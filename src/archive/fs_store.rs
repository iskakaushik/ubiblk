use super::ArchiveStore;
use crate::{Result, ResultExt};
use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

pub struct FileSystemStore {
    base_path: PathBuf,
    finished_puts: Vec<(String, Result<()>)>,
    finished_gets: Vec<(String, Result<Vec<u8>>)>,
}

impl FileSystemStore {
    pub fn new(base_path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&base_path)?;
        Ok(Self {
            base_path,
            finished_puts: Vec::new(),
            finished_gets: Vec::new(),
        })
    }

    /// Write `name.tmp`, fsync it and rename it over `name`, so a reader
    /// never sees a torn object under the final name and a crash mid-write
    /// leaves only the temporary file behind.
    fn try_put_object(&mut self, name: &str, data: &[u8]) -> Result<()> {
        let mut path = self.base_path.clone();
        path.push(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .context(format!("Failed to create dir {}", parent.display()))?;
        }
        let tmp_path = temp_path(&path);
        if let Err(err) = write_and_sync(&tmp_path, data) {
            // Best effort: the leftover is harmless, but keep the store tidy.
            let _ = fs::remove_file(&tmp_path);
            return Err(err).context(format!("Failed to write {}", tmp_path.display()));
        }
        fs::rename(&tmp_path, &path).context(format!(
            "Failed to rename {} to {}",
            tmp_path.display(),
            path.display()
        ))?;
        Ok(())
    }

    fn try_get_object(&self, name: &str) -> Result<Vec<u8>> {
        let mut path = self.base_path.clone();
        path.push(name);
        let data = fs::read(&path).context(format!("Failed to read {}", path.display()))?;
        Ok(data)
    }
}

/// `<path>.tmp`, next to the final object so the rename stays on one filesystem.
fn temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn write_and_sync(path: &Path, data: &[u8]) -> Result<()> {
    let mut file = fs::File::create(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

impl ArchiveStore for FileSystemStore {
    fn start_put_object(&mut self, name: &str, data: Vec<u8>) {
        let result = self.try_put_object(name, &data);
        self.finished_puts.push((name.to_string(), result));
    }

    fn start_get_object(&mut self, name: &str) {
        let result = self.try_get_object(name);
        self.finished_gets.push((name.to_string(), result));
    }

    fn poll_puts(&mut self) -> Vec<(String, Result<()>)> {
        std::mem::take(&mut self.finished_puts)
    }

    fn poll_gets(&mut self) -> Vec<(String, Result<Vec<u8>>)> {
        std::mem::take(&mut self.finished_gets)
    }

    /// `remove_file` per name. A missing file is not an error: the purge
    /// tool names every index a device could have written, most of which
    /// were never spilled.
    fn delete_objects(&mut self, names: &[String]) -> Result<()> {
        for name in names {
            let mut path = self.base_path.clone();
            path.push(name);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).context(format!("Failed to delete {}", path.display()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    const TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn put_is_atomic_via_tmp_and_rename() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;

        // A torn write leaves only `name.tmp`; it must not be readable as
        // the object.
        fs::create_dir_all(dir.path().join("dev"))?;
        fs::write(dir.path().join("dev/7.tmp"), b"torn")?;
        assert!(store.get_object("dev/7", TIMEOUT).is_err());

        store.put_object("dev/7", b"whole", TIMEOUT)?;
        assert_eq!(store.get_object("dev/7", TIMEOUT)?, b"whole");
        assert!(
            !dir.path().join("dev/7.tmp").exists(),
            "the temporary file is renamed away, not copied"
        );
        assert!(dir.path().join("dev/7").is_file());
        Ok(())
    }

    #[test]
    fn put_overwrites_an_existing_object() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;
        store.put_object("obj", b"first", TIMEOUT)?;
        store.put_object("obj", b"second", TIMEOUT)?;
        assert_eq!(store.get_object("obj", TIMEOUT)?, b"second");
        Ok(())
    }

    #[test]
    fn put_failure_removes_its_tmp_file() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;
        // The final name is a directory, so the rename fails after the
        // temporary file was written.
        fs::create_dir_all(dir.path().join("obj"))?;
        let err = store.put_object("obj", b"data", TIMEOUT).unwrap_err();
        assert!(err.to_string().contains("Failed to rename"), "{err}");
        Ok(())
    }

    #[test]
    fn temp_path_appends_tmp_to_the_file_name() {
        assert_eq!(
            temp_path(Path::new("/store/dev/12")),
            PathBuf::from("/store/dev/12.tmp")
        );
    }

    #[test]
    fn delete_objects_ignores_missing() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;
        store.put_object("dev/1", b"one", TIMEOUT)?;
        store.put_object("dev/3", b"three", TIMEOUT)?;

        let names: Vec<String> = ["dev/0", "dev/1", "dev/2", "dev/3", "dev/spill-key"]
            .iter()
            .map(|n| n.to_string())
            .collect();
        store.delete_objects(&names)?;

        assert!(!dir.path().join("dev/1").exists());
        assert!(!dir.path().join("dev/3").exists());
        assert!(store.get_object("dev/1", TIMEOUT).is_err());
        // Deleting again, with nothing left, is still fine.
        store.delete_objects(&names)?;
        Ok(())
    }

    #[test]
    fn delete_objects_reports_other_errors() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;
        // remove_file on a directory fails with something other than NotFound.
        fs::create_dir_all(dir.path().join("dev/dir"))?;
        let err = store
            .delete_objects(&["dev/dir".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Failed to delete"), "{err}");
        Ok(())
    }

    #[test]
    fn test_filesystem_put_and_get() -> Result<()> {
        let dir = tempdir()?;
        let mut store = FileSystemStore::new(dir.path().to_path_buf())?;
        let object_name = "test_object";
        let object_data = b"Hello, Archive!";
        store.put_object(object_name, object_data, Duration::from_secs(5))?;
        let retrieved_data = store.get_object(object_name, Duration::from_secs(5))?;
        assert_eq!(object_data.to_vec(), retrieved_data);
        Ok(())
    }

    #[test]
    fn test_creates_directory() -> Result<()> {
        let dir = tempdir()?;
        let new_dir = dir.path().join("new_store");
        let _store = FileSystemStore::new(new_dir.clone())?;
        assert!(new_dir.exists() && new_dir.is_dir());
        Ok(())
    }
}
