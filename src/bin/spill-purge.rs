//! Delete every spill object a device may have left in its store.
//!
//! Run on fork destroy. Names every stripe index, not only the ones marked
//! IN_S3: a crash between a PUT and its header write leaves an object under an
//! index the metadata does not point at. Never opens `device.raw`.

use std::io::Write;

use clap::Parser;
use log::error;
use ubiblk::archive::ArchiveStore;
use ubiblk::backends::build_block_device;
use ubiblk::block_device::spill::codec::{spill_key_object_name, spill_object_name};
use ubiblk::block_device::UbiMetadata;
use ubiblk::cli::{load_config, CommonArgs};
use ubiblk::config::v2;
use ubiblk::stripe_source::StripeSourceBuilder;
use ubiblk::Result;

/// The most names one `ArchiveStore::delete_objects` call takes.
const DELETE_BATCH_SIZE: usize = 1000;

#[derive(Parser)]
#[command(
    name = "spill-purge",
    version,
    author,
    about = "Delete every spill object of a device from its [spill.store]"
)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,

    /// List the objects that would be deleted without deleting anything.
    #[arg(long = "dry-run", default_value_t = false)]
    dry_run: bool,
}

fn main() {
    env_logger::builder().format_timestamp(None).init();

    if let Err(err) = run() {
        error!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    let config = load_config(&args.common)?;
    let mut stdout = std::io::stdout();
    run_with(&config, args.dry_run, &mut stdout)
}

/// Every object the device could have written: one per stripe index plus the
/// wrapped spill key, in the order the store will be asked to delete them.
fn purge_object_names(device_id: &str, stripe_count: u64) -> Vec<String> {
    let mut names: Vec<String> = (0..stripe_count as usize)
        .map(|index| spill_object_name(device_id, index))
        .collect();
    names.push(spill_key_object_name(device_id));
    names
}

/// Delete `names` in batches of `DELETE_BATCH_SIZE`, or only list them when
/// `dry_run`. Returns how many names were handed to the store.
fn purge(
    store: &mut dyn ArchiveStore,
    names: &[String],
    dry_run: bool,
    out: &mut dyn Write,
) -> Result<usize> {
    if dry_run {
        for name in names {
            writeln!(out, "would delete {name}")?;
        }
        return Ok(0);
    }
    let mut deleted = 0;
    for batch in names.chunks(DELETE_BATCH_SIZE) {
        store.delete_objects(batch)?;
        deleted += batch.len();
    }
    Ok(deleted)
}

fn run_with(config: &v2::Config, dry_run: bool, out: &mut dyn Write) -> Result<()> {
    let spill = config.spill.as_ref().ok_or_else(|| {
        ubiblk::ubiblk_error!(InvalidParameter {
            description: "config has no [spill] section".to_string(),
        })
    })?;
    let store_config = spill.store.as_ref().ok_or_else(|| {
        ubiblk::ubiblk_error!(InvalidParameter {
            description: "[spill] has no store, so there is nothing to purge".to_string(),
        })
    })?;
    let metadata_path = config.device.metadata_path.as_ref().ok_or_else(|| {
        ubiblk::ubiblk_error!(InvalidParameter {
            description: "metadata_path is none".to_string(),
        })
    })?;

    let metadata_dev = build_block_device(metadata_path, config, true)?;
    let metadata = UbiMetadata::load_from_bdev(metadata_dev.as_ref())?;
    let names = purge_object_names(&config.device.device_id, metadata.stripe_count());

    // One worker: deletes are synchronous and batched, more would idle.
    let mut store = StripeSourceBuilder::build_object_store(store_config, &config.secrets, 1)?;

    writeln!(
        out,
        "device {}: {} stripes, {} objects to delete (every stripe index plus spill-key)",
        config.device.device_id,
        metadata.stripe_count(),
        names.len()
    )?;
    let deleted = purge(store.as_mut(), &names, dry_run, out)?;
    if dry_run {
        writeln!(out, "dry run: nothing deleted")?;
    } else {
        writeln!(
            out,
            "deleted {deleted} objects in {} batches",
            names.len().div_ceil(DELETE_BATCH_SIZE)
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, time::Duration};

    use tempfile::tempdir;
    use ubiblk::archive::FileSystemStore;
    use ubiblk::block_device::DEFAULT_STRIPE_SECTOR_COUNT_SHIFT;

    use super::*;

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A store that only remembers how big each delete batch was.
    #[derive(Default)]
    struct BatchRecorder {
        batches: Vec<usize>,
    }

    impl ArchiveStore for BatchRecorder {
        fn start_put_object(&mut self, _name: &str, _data: Vec<u8>) {}
        fn start_get_object(&mut self, _name: &str) {}
        fn poll_puts(&mut self) -> Vec<(String, Result<()>)> {
            Vec::new()
        }
        fn poll_gets(&mut self) -> Vec<(String, Result<Vec<u8>>)> {
            Vec::new()
        }
        fn delete_objects(&mut self, names: &[String]) -> Result<()> {
            self.batches.push(names.len());
            Ok(())
        }
    }

    #[test]
    fn purge_object_names_covers_every_index_and_the_key() {
        let names = purge_object_names("fork-1", 3);
        assert_eq!(
            names,
            ["fork-1/0", "fork-1/1", "fork-1/2", "fork-1/spill-key"]
        );
    }

    #[test]
    fn purge_batches_by_1000() {
        let names = purge_object_names("fork-1", 2000);
        let mut store = BatchRecorder::default();
        let mut out = Vec::new();
        let deleted = purge(&mut store, &names, false, &mut out).unwrap();
        assert_eq!(deleted, 2001);
        assert_eq!(store.batches, [1000, 1000, 1]);
        assert!(out.is_empty(), "a real purge does not list names");
    }

    #[test]
    fn purge_dry_run_touches_no_store() {
        let names = purge_object_names("fork-1", 2);
        let mut store = BatchRecorder::default();
        let mut out = Vec::new();
        let deleted = purge(&mut store, &names, true, &mut out).unwrap();
        assert_eq!(deleted, 0);
        assert!(store.batches.is_empty());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "would delete fork-1/0\nwould delete fork-1/1\nwould delete fork-1/spill-key\n"
        );
    }

    /// A device with `stripe_count` stripes: its config, a metadata file and
    /// a filesystem spill store under `dir`.
    fn device_fixture(dir: &Path, stripe_count: usize) -> v2::Config {
        let metadata = UbiMetadata::new(DEFAULT_STRIPE_SECTOR_COUNT_SHIFT, stripe_count, 0);
        let mut buf = vec![0u8; metadata.metadata_size()];
        metadata.write_to_buf(&mut buf).unwrap();
        fs::write(dir.join("device.meta"), &buf).unwrap();

        let toml = r#"
            [device]
            data_path = "device.raw"
            metadata_path = "device.meta"
            device_id = "fork-1"
            track_written = true
            [spill]
            max_local_bytes = 1073741824
            [spill.store]
            storage = "filesystem"
            path = "spill"
            [danger_zone]
            enabled = true
            allow_unencrypted_disk = true
        "#;
        let config_path = dir.join("config.toml");
        fs::write(&config_path, toml).unwrap();
        v2::Config::load(&config_path).unwrap()
    }

    fn seed_store(dir: &Path, names: &[&str]) -> FileSystemStore {
        let mut store = FileSystemStore::new(dir.join("spill")).unwrap();
        for name in names {
            store.put_object(name, b"object", TIMEOUT).unwrap();
        }
        store
    }

    #[test]
    fn purge_dry_run_lists_all_indices_and_key() {
        let dir = tempdir().unwrap();
        let config = device_fixture(dir.path(), 4);
        // Objects under index 1 and 3, the key, and another device's object.
        let store = seed_store(
            dir.path(),
            &["fork-1/1", "fork-1/3", "fork-1/spill-key", "other/1"],
        );

        let mut out = Vec::new();
        run_with(&config, true, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        for expected in [
            "fork-1/0",
            "fork-1/1",
            "fork-1/2",
            "fork-1/3",
            "fork-1/spill-key",
        ] {
            assert!(out.contains(&format!("would delete {expected}\n")), "{out}");
        }
        assert!(!out.contains("other/"), "{out}");
        assert!(out.contains("4 stripes, 5 objects to delete"), "{out}");
        assert!(out.contains("dry run: nothing deleted"), "{out}");
        for still_there in ["fork-1/1", "fork-1/3", "fork-1/spill-key", "other/1"] {
            assert!(
                dir.path().join("spill").join(still_there).is_file(),
                "{still_there} must survive a dry run"
            );
        }
        drop(store);
        assert!(
            !dir.path().join("device.raw").exists(),
            "the purge never creates or opens device.raw"
        );
    }

    #[test]
    fn purge_deletes_the_devices_objects_and_nothing_else() {
        let dir = tempdir().unwrap();
        let config = device_fixture(dir.path(), 4);
        let _store = seed_store(
            dir.path(),
            &["fork-1/1", "fork-1/3", "fork-1/spill-key", "other/1"],
        );

        let mut out = Vec::new();
        run_with(&config, false, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(out.contains("deleted 5 objects in 1 batches"), "{out}");
        for gone in ["fork-1/1", "fork-1/3", "fork-1/spill-key"] {
            assert!(!dir.path().join("spill").join(gone).exists(), "{gone}");
        }
        assert!(dir.path().join("spill/other/1").is_file());
    }

    #[test]
    fn purge_refuses_a_config_without_a_store() {
        let dir = tempdir().unwrap();
        let mut config = device_fixture(dir.path(), 1);
        let mut out = Vec::new();

        config.spill.as_mut().unwrap().store = None;
        let err = run_with(&config, true, &mut out).unwrap_err().to_string();
        assert!(err.contains("has no store"), "{err}");

        config.spill = None;
        let err = run_with(&config, true, &mut out).unwrap_err().to_string();
        assert!(err.contains("no [spill] section"), "{err}");
    }
}
