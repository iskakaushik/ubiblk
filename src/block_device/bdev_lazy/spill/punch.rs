//! Giving blocks back to the filesystem.

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use nix::errno::Errno;
use ubiblk_macros::error_context;

use crate::Result;

/// The two syscalls the evictor makes on data_path, behind a trait so the
/// evictor's tests run against a recorder on any filesystem.
pub trait HolePuncher: Send {
    /// fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len).
    fn punch(&mut self, offset: u64, len: u64) -> std::result::Result<(), Errno>;
    /// statfs(data_path): blocks_available * block_size.
    fn free_bytes(&mut self) -> Result<u64>;
}

#[derive(Debug)]
pub struct FilePuncher {
    file: File,
    path: PathBuf,
}

impl FilePuncher {
    /// Opens data_path O_RDWR | O_CLOEXEC. Refuses with InvalidParameter if it
    /// is not a regular file: a block device has nothing to punch and no
    /// ENOSPC to avoid.
    #[error_context("Failed to open the data path for hole punching")]
    pub fn open(data_path: &Path) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(data_path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(crate::ubiblk_error!(InvalidParameter {
                description: format!(
                    "spill needs data_path to be a regular file, but {} is not",
                    data_path.display()
                ),
            }));
        }
        Ok(FilePuncher {
            file,
            path: data_path.to_path_buf(),
        })
    }
}

#[cfg(target_os = "linux")]
impl HolePuncher for FilePuncher {
    fn punch(&mut self, offset: u64, len: u64) -> std::result::Result<(), Errno> {
        use nix::fcntl::{fallocate, FallocateFlags};

        let offset = libc::off_t::try_from(offset).map_err(|_| Errno::EOVERFLOW)?;
        let len = libc::off_t::try_from(len).map_err(|_| Errno::EOVERFLOW)?;
        fallocate(
            &self.file,
            FallocateFlags::FALLOC_FL_PUNCH_HOLE | FallocateFlags::FALLOC_FL_KEEP_SIZE,
            offset,
            len,
        )
    }

    fn free_bytes(&mut self) -> Result<u64> {
        let stat = nix::sys::statfs::statfs(&self.path).map_err(|e| {
            crate::ubiblk_error!(InvalidParameter {
                description: format!("Failed to statfs {}: {e}", self.path.display()),
            })
        })?;
        Ok(stat.blocks_available() * stat.block_size() as u64)
    }
}

/// Only Linux can punch holes; elsewhere the crate still type-checks and a
/// puncher reports the operation as unsupported.
#[cfg(not(target_os = "linux"))]
impl HolePuncher for FilePuncher {
    fn punch(&mut self, _offset: u64, _len: u64) -> std::result::Result<(), Errno> {
        let _ = &self.file;
        Err(Errno::EOPNOTSUPP)
    }

    fn free_bytes(&mut self) -> Result<u64> {
        Err(crate::ubiblk_error!(InvalidParameter {
            description: format!(
                "hole punching is not supported on this platform ({})",
                self.path.display()
            ),
        }))
    }
}

/// A puncher that records what it was asked instead of touching a file.
#[cfg(test)]
#[derive(Default)]
pub struct RecordingPuncher {
    pub punches: std::sync::Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
    pub free: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The next punch fails with EIO and clears this.
    pub fail_next: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl HolePuncher for RecordingPuncher {
    fn punch(&mut self, offset: u64, len: u64) -> std::result::Result<(), Errno> {
        if self
            .fail_next
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Errno::EIO);
        }
        self.punches.lock().unwrap().push((offset, len));
        Ok(())
    }

    fn free_bytes(&mut self) -> Result<u64> {
        Ok(self.free.load(std::sync::atomic::Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn recording_puncher_records_and_fails_once() {
        let mut puncher = RecordingPuncher::default();
        puncher.free.store(4096, Ordering::SeqCst);
        assert_eq!(puncher.free_bytes().unwrap(), 4096);

        puncher.punch(0, 512).unwrap();
        puncher.fail_next.store(true, Ordering::SeqCst);
        assert_eq!(puncher.punch(512, 512), Err(Errno::EIO));
        puncher.punch(1024, 512).unwrap();
        assert_eq!(
            *puncher.punches.lock().unwrap(),
            vec![(0, 512), (1024, 512)]
        );
    }

    #[test]
    fn file_puncher_refuses_non_regular_file() {
        let err = FilePuncher::open(Path::new("/dev/null"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("regular file"), "{err}");

        let dir = tempfile::tempdir().unwrap();
        let err = FilePuncher::open(dir.path()).unwrap_err().to_string();
        assert!(!err.is_empty());
    }

    #[test]
    fn file_puncher_opens_a_regular_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(1 << 20).unwrap();
        let puncher = FilePuncher::open(file.path()).unwrap();
        assert_eq!(puncher.path, file.path());
    }

    /// The real syscalls, on a file under `target/` because `/tmp` may be a
    /// tmpfs with its own ideas about allocation.
    #[cfg(target_os = "linux")]
    fn target_tempfile() -> tempfile::NamedTempFile {
        let dir = std::env::current_dir().unwrap().join("target");
        std::fs::create_dir_all(&dir).unwrap();
        tempfile::NamedTempFile::new_in(dir).unwrap()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn file_puncher_frees_blocks_and_reads_zero() {
        use std::io::{Read, Seek, SeekFrom, Write};
        use std::os::unix::fs::MetadataExt;

        let mut file = target_tempfile();
        let stripe = vec![0xABu8; 1 << 20];
        file.as_file_mut().write_all(&stripe).unwrap();
        file.as_file_mut().write_all(&stripe).unwrap();
        file.as_file().sync_all().unwrap();
        let blocks_before = file.as_file().metadata().unwrap().blocks();
        assert!(blocks_before > 0);

        let mut puncher = FilePuncher::open(file.path()).unwrap();
        puncher.punch(0, stripe.len() as u64).unwrap();

        let metadata = file.as_file().metadata().unwrap();
        assert_eq!(metadata.len(), 2 * stripe.len() as u64, "KEEP_SIZE");
        assert!(metadata.blocks() < blocks_before, "blocks were freed");

        let mut read_back = vec![1u8; stripe.len()];
        file.as_file_mut().seek(SeekFrom::Start(0)).unwrap();
        file.as_file_mut().read_exact(&mut read_back).unwrap();
        assert!(read_back.iter().all(|&b| b == 0));
        file.as_file_mut().read_exact(&mut read_back).unwrap();
        assert_eq!(read_back, stripe, "the second stripe is intact");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn file_puncher_free_bytes_matches_statfs() {
        let file = target_tempfile();
        let mut puncher = FilePuncher::open(file.path()).unwrap();
        let stat = nix::sys::statfs::statfs(file.path()).unwrap();
        let expected = stat.blocks_available() * stat.block_size() as u64;
        let reported = puncher.free_bytes().unwrap();
        // Other processes may allocate between the two calls.
        let slack = 64 * stat.block_size() as u64 * 1024;
        assert!(
            reported.abs_diff(expected) <= slack,
            "reported {reported}, statfs {expected}"
        );
    }
}
