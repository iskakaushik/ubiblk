//! Turning `[spill]` into something the bgworker can run: the precondition
//! the config loader cannot check, metadata preallocation, the runtime the
//! evictor and the fetchers share, and the spill key written at init time.

use std::{path::Path, sync::Arc};

use log::info;
use ubiblk_macros::error_context;

use crate::{
    archive::DEFAULT_ARCHIVE_TIMEOUT,
    block_device::spill::{
        codec::{spill_kek, spill_key_object_name, unwrap_spill_key, wrap_spill_key},
        EvictorConfig, SpillCodec, SpillRuntime, StoreFactory,
    },
    config::v2::{
        self,
        secrets::{get_resolved_secret, ResolvedSecret},
        spill::{OnFull, SpillSection},
        stripe_source::ArchiveStorageConfig,
    },
    crypt::XtsBlockCipher,
    stripe_source::StripeSourceBuilder,
    Result, ResultExt,
};

/// Stripes the CLOCK hand examines per evictor tick.
const SWEEP_BATCH: usize = 4096;

/// `data_path` is a regular file; everything else was validated at config
/// load. A block device has nothing to punch and no ENOSPC to avoid.
#[error_context("Spill preconditions not met")]
pub fn check_spill_preconditions(config: &v2::Config) -> Result<()> {
    let path = &config.device.data_path;
    let stat = std::fs::metadata(path).context(format!("Failed to stat {}", path.display()))?;
    if !stat.file_type().is_file() {
        return Err(crate::ubiblk_error!(InvalidParameter {
            description: format!(
                "spill needs data_path to be a regular file, but {} is not",
                path.display()
            ),
        }));
    }
    Ok(())
}

/// fallocate(mode 0, 0, size) on the metadata file so metadata writes never
/// see ENOSPC: `ensure_metadata_file` only sets the length, which leaves the
/// blocks unallocated on a sparse filesystem, and the header write that
/// records an eviction is the one write that must not fail for lack of space.
#[error_context("Failed to preallocate metadata file {:?}", path)]
pub fn preallocate_metadata_file(path: &Path, size: usize) -> Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    preallocate(&file, size)?;
    // The allocation is filesystem metadata too; make it as durable as the
    // length `ensure_metadata_file` synced.
    file.sync_all()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn preallocate(file: &std::fs::File, size: usize) -> Result<()> {
    use nix::fcntl::{fallocate, FallocateFlags};

    let len = libc::off_t::try_from(size).map_err(|_| {
        crate::ubiblk_error!(InvalidParameter {
            description: format!("metadata size {size} does not fit an off_t"),
        })
    })?;
    fallocate(file, FallocateFlags::empty(), 0, len)
        .map_err(|e| crate::ubiblk_error!(IoError { source: e.into() }))
}

/// Only Linux has `fallocate`; elsewhere the crate still type-checks and the
/// caller learns that the guarantee cannot be given.
#[cfg(not(target_os = "linux"))]
fn preallocate(_file: &std::fs::File, _size: usize) -> Result<()> {
    Err(crate::ubiblk_error!(IoError {
        source: std::io::Error::from(nix::errno::Errno::EOPNOTSUPP),
    }))
}

/// `SpillRuntime` from the section: `EvictorConfig` from the section and the
/// device geometry, a store factory over `build_object_store`, and the codec
/// (with the spill key unwrapped from the store when `kek` is set; a
/// synchronous GET, construction time only).
#[error_context("Failed to build the spill runtime")]
pub fn build_spill_runtime(
    config: &v2::Config,
    section: &SpillSection,
    stripe_sector_count: u64,
    target_sector_count: u64,
    alignment: usize,
) -> Result<SpillRuntime> {
    let cfg = EvictorConfig {
        data_path: config.device.data_path.clone(),
        device_id: config.device.device_id.clone(),
        stripe_sector_count,
        target_sector_count,
        max_local_bytes: section.max_local_bytes,
        low_water_bytes: section.low_water_bytes,
        hard_margin_bytes: section.hard_margin_bytes,
        min_free_bytes: section.min_free_bytes,
        clean_eviction: section.clean_eviction,
        on_full: section.on_full,
        max_concurrent_evictions: section.max_concurrent_evictions,
        sweep_batch: SWEEP_BATCH,
        alignment,
    };

    let store_factory = section.store.as_ref().map(|store| {
        // The store's connection count is the budget both owners share: a
        // fetcher asks for its share of it, the evictor asks for
        // max_concurrent_evictions, and neither may exceed it.
        let budget = store_connections(store);
        let store = store.clone();
        let secrets = config.secrets.clone();
        let factory: Arc<StoreFactory> = Arc::new(move |workers: usize| {
            StripeSourceBuilder::build_object_store(&store, &secrets, workers.min(budget).max(1))
        });
        factory
    });

    let codec = build_spill_codec(
        section,
        &config.device.device_id,
        stripe_sector_count,
        &config.secrets,
    )?;

    Ok(SpillRuntime {
        cfg,
        device_id: config.device.device_id.clone(),
        store_factory,
        codec,
        #[cfg(test)]
        puncher_factory: None,
    })
}

/// init-metadata with `spill.kek`: generate a random XTS key, wrap it under the
/// KEK and put it at `<device_id>/spill-key`. Forks re-run init-metadata with a
/// truncated metadata file, so a fresh key per init is consistent: no object
/// written under the old key is ever read again. Without `kek` there is
/// nothing to write.
#[error_context("Failed to initialise the spill key")]
pub fn init_spill_key(config: &v2::Config, section: &SpillSection) -> Result<()> {
    let Some(kek_ref) = &section.kek else {
        return Ok(());
    };
    let store = store_for_key(section)?;
    let kek = spill_kek(get_resolved_secret(kek_ref, &config.secrets)?.as_bytes());
    let cipher = XtsBlockCipher::random()?;
    let wrapped = wrap_spill_key(&kek, &cipher)?;

    let name = spill_key_object_name(&config.device.device_id);
    let mut store = StripeSourceBuilder::build_object_store(store, &config.secrets, 1)?;
    store
        .put_object(&name, &wrapped, DEFAULT_ARCHIVE_TIMEOUT)
        .context(format!("Failed to write spill key object {name}"))?;
    info!("Wrote spill key object {name}");
    Ok(())
}

/// The startup summary of a spill configuration, one line an operator can
/// check against what they meant to deploy.
pub fn spill_summary(section: &SpillSection) -> String {
    let store = match &section.store {
        Some(ArchiveStorageConfig::S3 { bucket, prefix, .. }) => match prefix {
            Some(prefix) => format!("s3 {bucket}/{prefix}"),
            None => format!("s3 {bucket}"),
        },
        Some(ArchiveStorageConfig::Filesystem { path, .. }) => format!("fs {}", path.display()),
        None => "none (clean-only)".to_string(),
    };
    let on_off = |enabled: bool| if enabled { "on" } else { "off" };
    let on_full = match section.on_full {
        OnFull::Stall => "stall",
        OnFull::Fail => "fail",
    };
    format!(
        "ceiling {}, low water {}, hard margin {}, min free {}, store {store}, clean_eviction {}, on_full {on_full}, kek {}",
        section.max_local_bytes,
        section.low_water_bytes,
        section.hard_margin_bytes,
        section.min_free_bytes,
        on_off(section.clean_eviction),
        if section.kek.is_some() { "set" } else { "unset" },
    )
}

/// The codec for a device: compression from the section, cipher from the
/// wrapped key object when `kek` is set. A missing or undecryptable key object
/// fails construction rather than silently writing objects in the clear.
fn build_spill_codec(
    section: &SpillSection,
    device_id: &str,
    stripe_sector_count: u64,
    secrets: &std::collections::HashMap<String, ResolvedSecret>,
) -> Result<SpillCodec> {
    let cipher = match &section.kek {
        None => None,
        Some(kek_ref) => {
            let store = store_for_key(section)?;
            let kek = spill_kek(get_resolved_secret(kek_ref, secrets)?.as_bytes());
            let name = spill_key_object_name(device_id);
            let mut store = StripeSourceBuilder::build_object_store(store, secrets, 1)?;
            let wrapped = store
                .get_object(&name, DEFAULT_ARCHIVE_TIMEOUT)
                .context(format!(
                    "spill key object {name} is missing or unreadable; init-metadata writes it when spill.kek is set"
                ))?;
            Some(unwrap_spill_key(&kek, &wrapped).context(format!(
                "spill key object {name} does not decrypt under spill.kek"
            ))?)
        }
    };
    Ok(SpillCodec::new(
        section.compression.clone(),
        cipher,
        stripe_sector_count,
    ))
}

/// The wrapped key lives in the store, so a KEK without a store has nowhere
/// to put it.
fn store_for_key(section: &SpillSection) -> Result<&ArchiveStorageConfig> {
    section.store.as_ref().ok_or_else(|| {
        crate::ubiblk_error!(InvalidParameter {
            description: "spill.kek needs spill.store: the wrapped spill key is stored there"
                .to_string(),
        })
    })
}

fn store_connections(store: &ArchiveStorageConfig) -> usize {
    match store {
        ArchiveStorageConfig::S3 { connections, .. } => *connections,
        ArchiveStorageConfig::Filesystem { .. } => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::umask_guard::UMASK_LOCK;
    use crate::{
        archive::{ArchiveCompressionAlgorithm, ArchiveStore, FileSystemStore},
        block_device::spill::codec::{spill_flags, SpillObjectHeader, SPILL_HEADER_LEN},
        config::v2::{
            secrets::{resolve_secrets, SecretDef, SecretEncoding, SecretRef, SecretSource},
            DangerZone, DeviceSection,
        },
    };
    use base64::Engine;
    use std::collections::HashMap;

    const STRIPE_SECTORS: u64 = 8;
    const KEK_ID: &str = "spill-kek";

    fn kek_secrets(kek_bytes: [u8; 32]) -> HashMap<String, ResolvedSecret> {
        let kek_b64 = base64::engine::general_purpose::STANDARD.encode(kek_bytes);
        let defs = HashMap::from([(
            KEK_ID.to_string(),
            SecretDef {
                source: SecretSource::Inline(kek_b64),
                encrypted_by: None,
                encoding: SecretEncoding::Base64,
            },
        )]);
        let danger_zone = DangerZone {
            enabled: true,
            allow_inline_plaintext_secrets: true,
            ..Default::default()
        };
        resolve_secrets(&defs, &danger_zone).unwrap()
    }

    fn fs_store(path: &Path) -> ArchiveStorageConfig {
        ArchiveStorageConfig::Filesystem {
            path: path.to_path_buf(),
            archive_kek: None,
            autofetch: false,
        }
    }

    fn section(store: Option<ArchiveStorageConfig>, kek: bool) -> SpillSection {
        SpillSection {
            max_local_bytes: 1 << 20,
            low_water_bytes: 4096,
            hard_margin_bytes: 8192,
            min_free_bytes: 16384,
            on_full: OnFull::Fail,
            clean_eviction: store.is_none(),
            max_concurrent_evictions: 2,
            compression: ArchiveCompressionAlgorithm::None,
            kek: kek.then(|| SecretRef::Ref(KEK_ID.to_string())),
            store,
        }
    }

    fn config(data_path: &Path, secrets: HashMap<String, ResolvedSecret>) -> v2::Config {
        v2::Config {
            device: DeviceSection {
                snapshot_server: None,
                snapshot_source: None,
                snapshot_compression: Default::default(),
                data_path: data_path.to_path_buf(),
                metadata_path: None,
                vhost_socket: None,
                rpc_socket: None,
                device_id: "fork-1".to_string(),
                track_written: true,
            },
            tuning: Default::default(),
            encryption: None,
            danger_zone: Default::default(),
            stripe_source: None,
            spill: None,
            secrets,
        }
    }

    fn encoded_flags(codec: &mut SpillCodec) -> u16 {
        let data = vec![0x5Au8; STRIPE_SECTORS as usize * crate::backends::SECTOR_SIZE];
        let object = codec.encode(3, &data, None).unwrap();
        SpillObjectHeader::decode(&object[..SPILL_HEADER_LEN])
            .unwrap()
            .flags
    }

    #[test]
    fn preconditions_refuse_non_regular_data_path() {
        let err = check_spill_preconditions(&config(Path::new("/dev/null"), HashMap::new()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("regular file"), "{err}");

        let missing = check_spill_preconditions(&config(
            Path::new("/nonexistent/device.raw"),
            HashMap::new(),
        ));
        assert!(missing.is_err());
    }

    #[test]
    fn preconditions_accept_a_regular_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        check_spill_preconditions(&config(file.path(), HashMap::new())).unwrap();
    }

    /// Real fallocate, on a file under `target/` because `/tmp` may be a tmpfs
    /// with its own ideas about allocation.
    #[cfg(target_os = "linux")]
    #[test]
    fn preallocate_metadata_file_allocates_blocks() {
        use std::os::unix::fs::MetadataExt;

        let dir = std::env::current_dir().unwrap().join("target");
        std::fs::create_dir_all(&dir).unwrap();
        let file = tempfile::NamedTempFile::new_in(dir).unwrap();
        let size = 1usize << 20;
        file.as_file().set_len(size as u64).unwrap();
        assert_eq!(file.as_file().metadata().unwrap().blocks(), 0, "sparse");

        preallocate_metadata_file(file.path(), size).unwrap();

        let stat = file.as_file().metadata().unwrap();
        assert_eq!(stat.len(), size as u64);
        assert!(
            stat.blocks() * 512 >= size as u64,
            "{} blocks",
            stat.blocks()
        );
    }

    #[test]
    fn preallocate_metadata_file_fails_on_a_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let result = preallocate_metadata_file(&dir.path().join("missing"), 4096);
        assert!(result.is_err());
    }

    #[test]
    fn build_spill_runtime_without_store_is_clean_only() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let config = config(file.path(), HashMap::new());
        let section = section(None, false);

        let runtime = build_spill_runtime(&config, &section, STRIPE_SECTORS, 64, 4096).unwrap();

        assert!(runtime.store_factory.is_none());
        assert_eq!(runtime.device_id, "fork-1");
        assert_eq!(runtime.cfg.data_path, file.path());
        assert_eq!(runtime.cfg.stripe_sector_count, STRIPE_SECTORS);
        assert_eq!(runtime.cfg.target_sector_count, 64);
        assert_eq!(runtime.cfg.max_local_bytes, 1 << 20);
        assert_eq!(runtime.cfg.low_water_bytes, 4096);
        assert_eq!(runtime.cfg.hard_margin_bytes, 8192);
        assert_eq!(runtime.cfg.min_free_bytes, 16384);
        assert!(runtime.cfg.clean_eviction);
        assert_eq!(runtime.cfg.on_full, OnFull::Fail);
        assert_eq!(runtime.cfg.max_concurrent_evictions, 2);
        assert_eq!(runtime.cfg.sweep_batch, SWEEP_BATCH);
        assert_eq!(runtime.cfg.alignment, 4096);
        let mut codec = runtime.codec;
        assert_eq!(encoded_flags(&mut codec) & spill_flags::XTS, 0);
    }

    #[test]
    fn build_spill_runtime_with_store_builds_a_store_per_call() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = config(file.path(), HashMap::new());
        let section = section(Some(fs_store(dir.path())), false);

        let runtime = build_spill_runtime(&config, &section, STRIPE_SECTORS, 64, 4096).unwrap();

        let factory = runtime.store_factory.expect("store configured");
        let mut store = factory(4).unwrap();
        store
            .put_object("fork-1/7", b"object", DEFAULT_ARCHIVE_TIMEOUT)
            .unwrap();
        assert!(dir.path().join("fork-1").join("7").is_file());
        assert!(!runtime.cfg.clean_eviction);
    }

    #[test]
    fn build_spill_runtime_with_kek_unwraps_key() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = config(file.path(), kek_secrets([7u8; 32]));
        let section = section(Some(fs_store(dir.path())), true);
        init_spill_key(&config, &section).unwrap();

        let runtime = build_spill_runtime(&config, &section, STRIPE_SECTORS, 64, 4096).unwrap();

        // The runtime's codec encrypts, and with the key the store holds:
        // a codec built from the key object directly decodes its objects.
        let mut codec = runtime.codec;
        let data: Vec<u8> = (0..STRIPE_SECTORS as usize * crate::backends::SECTOR_SIZE)
            .map(|i| i as u8)
            .collect();
        let object = codec.encode(3, &data, None).unwrap();
        let header = SpillObjectHeader::decode(&object[..SPILL_HEADER_LEN]).unwrap();
        assert_ne!(header.flags & spill_flags::XTS, 0);

        let wrapped = std::fs::read(dir.path().join("fork-1").join("spill-key")).unwrap();
        let cipher = unwrap_spill_key(&spill_kek(&[7u8; 32]), &wrapped).unwrap();
        let mut from_store = SpillCodec::new(
            ArchiveCompressionAlgorithm::None,
            Some(cipher),
            STRIPE_SECTORS,
        );
        let mut decoded = vec![0u8; data.len()];
        from_store
            .decode_into(3, &object, &mut decoded, None)
            .unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn build_spill_runtime_fails_without_key_object() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = config(file.path(), kek_secrets([7u8; 32]));
        let section = section(Some(fs_store(dir.path())), true);

        let err = build_spill_runtime(&config, &section, STRIPE_SECTORS, 64, 4096)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("spill key object fork-1/spill-key"), "{err}");
    }

    #[test]
    fn build_spill_runtime_fails_with_wrong_kek() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let section = section(Some(fs_store(dir.path())), true);
        init_spill_key(&config(file.path(), kek_secrets([7u8; 32])), &section).unwrap();

        let wrong = config(file.path(), kek_secrets([8u8; 32]));
        let err = build_spill_runtime(&wrong, &section, STRIPE_SECTORS, 64, 4096)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("does not decrypt"), "{err}");
    }

    #[test]
    fn build_spill_runtime_rejects_kek_without_store() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let config = config(file.path(), kek_secrets([7u8; 32]));
        let section = section(None, true);

        let err = build_spill_runtime(&config, &section, STRIPE_SECTORS, 64, 4096)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("spill.kek needs spill.store"), "{err}");
        let err = init_spill_key(&config, &section).unwrap_err().to_string();
        assert!(err.contains("spill.kek needs spill.store"), "{err}");
    }

    #[test]
    fn init_spill_key_writes_key_object() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = config(file.path(), kek_secrets([9u8; 32]));
        let section = section(Some(fs_store(dir.path())), true);

        init_spill_key(&config, &section).unwrap();

        let mut store = FileSystemStore::new(dir.path().to_path_buf()).unwrap();
        let wrapped = store
            .get_object("fork-1/spill-key", DEFAULT_ARCHIVE_TIMEOUT)
            .unwrap();
        unwrap_spill_key(&spill_kek(&[9u8; 32]), &wrapped).expect("wrapped under the KEK");
        assert!(unwrap_spill_key(&spill_kek(&[1u8; 32]), &wrapped).is_err());

        // A second init writes a fresh key: forks start from a truncated
        // metadata file and nothing under the old key is read again.
        init_spill_key(&config, &section).unwrap();
        let rewrapped = store
            .get_object("fork-1/spill-key", DEFAULT_ARCHIVE_TIMEOUT)
            .unwrap();
        assert_ne!(rewrapped, wrapped);
    }

    #[test]
    fn init_spill_key_without_kek_writes_nothing() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config = config(file.path(), HashMap::new());
        let section = section(Some(fs_store(dir.path())), false);

        init_spill_key(&config, &section).unwrap();

        assert!(!dir.path().join("fork-1").exists());
    }

    #[test]
    fn spill_summary_names_every_setting() {
        let _umask_guard = UMASK_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let fs = spill_summary(&section(Some(fs_store(dir.path())), true));
        assert_eq!(
            fs,
            format!(
                "ceiling 1048576, low water 4096, hard margin 8192, min free 16384, store fs {}, clean_eviction off, on_full fail, kek set",
                dir.path().display()
            )
        );

        let mut clean_only = section(None, false);
        clean_only.on_full = OnFull::Stall;
        assert_eq!(
            spill_summary(&clean_only),
            "ceiling 1048576, low water 4096, hard margin 8192, min free 16384, store none (clean-only), clean_eviction on, on_full stall, kek unset"
        );

        let s3 = ArchiveStorageConfig::S3 {
            bucket: "forks".to_string(),
            prefix: Some("pg".to_string()),
            region: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint: None,
            connections: 3,
            connect_timeout_ms: 1,
            operation_attempt_timeout_ms: 1,
            max_attempts: 1,
            rate_limited_retry: Default::default(),
            archive_kek: None,
            autofetch: false,
        };
        assert!(spill_summary(&section(Some(s3), false)).contains("store s3 forks/pg,"));
    }
}
