//! The `[spill]` section: a ceiling on how much of a fork's overlay may stay
//! on the local disk, and where the rest goes.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use super::{
    load::resolve_path,
    secrets::{get_resolved_secret, ResolvedSecret, SecretRef},
    stripe_source::{ArchiveStorageConfig, StripeSourceConfig},
    tuning::TuningSection,
    DeviceSection,
};
use crate::{archive::ArchiveCompressionAlgorithm, ubiblk_error, Result};

/// What a guest write meets once the local disk is full and the evictor has
/// not caught up.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnFull {
    /// Queue the write until space is freed.
    #[default]
    Stall,
    /// Fail the write with an I/O error.
    Fail,
}

/// The `[spill]` section: when the local device is treated as a cache with a
/// ceiling, and where its overflow goes.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpillSection {
    /// Ceiling on resident stripes times the stripe size, in bytes.
    pub max_local_bytes: u64,
    /// Once over the ceiling, evict down to `max_local_bytes - low_water_bytes`.
    #[serde(default = "default_low_water_bytes")]
    pub low_water_bytes: u64,
    /// Gate guest writes above `max_local_bytes + hard_margin_bytes`.
    #[serde(default = "default_hard_margin_bytes")]
    pub hard_margin_bytes: u64,
    /// statfs watermark on the filesystem holding `data_path`; writes are
    /// gated below half of it.
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    /// What a guest write meets once the disk is full and the evictor has
    /// not caught up.
    #[serde(default)]
    pub on_full: OnFull,
    /// Drop clean stripes that the live snapshot can serve again instead of
    /// spilling them. Needs `snapshot_source`, which is what keeps track of
    /// whether the snapshot is still live.
    #[serde(default)]
    pub clean_eviction: bool,
    /// Also bounds PUTs in flight.
    #[serde(default = "default_max_concurrent_evictions")]
    pub max_concurrent_evictions: usize,
    /// Compression applied to spilled objects before encryption.
    #[serde(default = "default_spill_compression")]
    pub compression: ArchiveCompressionAlgorithm,
    /// 32-byte key-encryption key; enables AES-XTS on spilled objects.
    #[serde(default)]
    pub kek: Option<SecretRef>,
    /// Where dirty stripes go. Absent means clean-only, which then requires
    /// `clean_eviction`.
    #[serde(default)]
    pub store: Option<ArchiveStorageConfig>,
}

fn default_low_water_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_hard_margin_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_min_free_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_max_concurrent_evictions() -> usize {
    4
}

fn default_spill_compression() -> ArchiveCompressionAlgorithm {
    ArchiveCompressionAlgorithm::Zstd { level: 3 }
}

const DEFAULT_DEVICE_ID: &str = "ubiblk";

fn invalid(description: &str) -> crate::UbiblkError {
    ubiblk_error!(InvalidParameter {
        description: description.to_string(),
    })
}

impl SpillSection {
    /// Resolve the filesystem store path against the config directory.
    pub fn resolve_paths(&mut self, config_dir: &Path) {
        if let Some(ArchiveStorageConfig::Filesystem { path, .. }) = &mut self.store {
            *path = resolve_path(std::mem::take(path), config_dir);
        }
    }

    /// Every rule is an InvalidParameter naming the field it is about.
    pub fn validate(
        &self,
        device: &DeviceSection,
        stripe_source: Option<&StripeSourceConfig>,
        tuning: &TuningSection,
        secrets: &HashMap<String, ResolvedSecret>,
    ) -> Result<()> {
        if self.max_local_bytes == 0 {
            return Err(invalid("spill.max_local_bytes must be greater than 0"));
        }
        if self.low_water_bytes >= self.max_local_bytes {
            return Err(invalid(
                "spill.low_water_bytes must be below max_local_bytes",
            ));
        }
        if !(1..=64).contains(&self.max_concurrent_evictions) {
            return Err(invalid(
                "spill.max_concurrent_evictions must be between 1 and 64",
            ));
        }
        if device.metadata_path.is_none() {
            return Err(invalid("spill needs a device with metadata_path"));
        }
        if !device.track_written {
            return Err(invalid(
                "spill needs track_written = true to tell dirty from clean",
            ));
        }
        if device.device_id == DEFAULT_DEVICE_ID {
            return Err(invalid(
                "spill needs an explicit device_id; it is part of the object key",
            ));
        }
        // The snapshot server reads the local device for written or fetched
        // stripes, and an evicted one is a hole there.
        if device.snapshot_server.is_some() {
            return Err(invalid(
                "spill cannot be combined with snapshot_server: a served stripe may be a hole",
            ));
        }
        if let Some(stripe_source) = stripe_source {
            // A read of an evicted stripe must come back through the fetch
            // path; the image shortcut would serve the base image instead.
            if !stripe_source.copy_on_read() {
                return Err(invalid("spill needs copy_on_read = true"));
            }
            if stripe_source.autofetch() {
                return Err(invalid("spill needs autofetch = false"));
            }
        }
        if self.clean_eviction && device.snapshot_source.is_none() {
            return Err(invalid(
                "clean_eviction needs snapshot_source to track snapshot liveness",
            ));
        }
        match &self.store {
            None if !self.clean_eviction => {
                return Err(invalid(
                    "spill without a store can only evict clean stripes; set clean_eviction = true or configure spill.store",
                ));
            }
            None => {}
            Some(store) => {
                store.validate(secrets)?;
                let (archive_kek, autofetch) = match store {
                    ArchiveStorageConfig::Filesystem {
                        archive_kek,
                        autofetch,
                        ..
                    }
                    | ArchiveStorageConfig::S3 {
                        archive_kek,
                        autofetch,
                        ..
                    } => (archive_kek, *autofetch),
                };
                if archive_kek.is_some() || autofetch {
                    return Err(invalid(
                        "spill.store does not take archive_kek or autofetch",
                    ));
                }
            }
        }
        if let Some(kek) = &self.kek {
            let len = get_resolved_secret(kek, secrets)?.as_bytes().len();
            if len != 32 {
                return Err(ubiblk_error!(InvalidParameter {
                    description: format!(
                        "spill.kek secret must be exactly 32 bytes for AES-256-GCM (got {len} bytes)"
                    ),
                }));
            }
        }
        if tuning.num_queues * tuning.queue_size > u16::MAX as usize {
            return Err(invalid(
                "spill's per-stripe in-flight counter is 16 bits; reduce num_queues * queue_size",
            ));
        }
        Ok(())
    }

    /// The filesystem store path, if the store is one.
    pub fn store_path(&self) -> Option<&PathBuf> {
        match &self.store {
            Some(ArchiveStorageConfig::Filesystem { path, .. }) => Some(path),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::v2::{
        secrets::{resolve_secrets, SecretDef, SecretEncoding, SecretSource},
        stripe_source::RemoteStripeConfig,
        DangerZone,
    };
    use base64::Engine;

    const FULL_EXAMPLE: &str = r#"
        max_local_bytes = 12884901888
        low_water_bytes = 536870912
        hard_margin_bytes = 268435456
        min_free_bytes = 1073741824
        on_full = "stall"
        clean_eviction = false
        max_concurrent_evictions = 4
        compression = { zstd = { level = 3 } }
        kek = { ref = "spill-kek" }

        [store]
        storage = "s3"
        bucket = "pg-ubicloud-ci-forks"
        prefix = "forks"
        region = "us-west-2"
        connections = 16
    "#;

    fn device() -> DeviceSection {
        DeviceSection {
            data_path: "/data/device.raw".into(),
            metadata_path: Some("/data/device.meta".into()),
            vhost_socket: None,
            rpc_socket: None,
            device_id: "fork-3f9c".to_string(),
            track_written: true,
            snapshot_server: None,
            snapshot_source: Some("10.0.1.20:9500".to_string()),
            snapshot_compression: Default::default(),
        }
    }

    fn remote_source() -> StripeSourceConfig {
        StripeSourceConfig::Remote(RemoteStripeConfig {
            address: "10.0.1.20:9400".to_string(),
            psk: None,
            autofetch: false,
            connect_timeout_ms: 5_000,
            operation_attempt_timeout_ms: 20_000,
            connections: 4,
            compression: Default::default(),
        })
    }

    fn fs_store(path: &str) -> ArchiveStorageConfig {
        ArchiveStorageConfig::Filesystem {
            path: path.into(),
            archive_kek: None,
            autofetch: false,
        }
    }

    fn section() -> SpillSection {
        SpillSection {
            max_local_bytes: 12 * 1024 * 1024 * 1024,
            low_water_bytes: default_low_water_bytes(),
            hard_margin_bytes: default_hard_margin_bytes(),
            min_free_bytes: default_min_free_bytes(),
            on_full: OnFull::Stall,
            clean_eviction: false,
            max_concurrent_evictions: 4,
            compression: default_spill_compression(),
            kek: None,
            store: Some(fs_store("/mnt/other/spill")),
        }
    }

    fn secrets(kek_len: usize) -> HashMap<String, ResolvedSecret> {
        let def = SecretDef {
            source: SecretSource::Inline(
                base64::engine::general_purpose::STANDARD.encode(vec![0x42u8; kek_len]),
            ),
            encrypted_by: None,
            encoding: SecretEncoding::Base64,
        };
        let danger_zone = DangerZone {
            enabled: true,
            allow_inline_plaintext_secrets: true,
            ..Default::default()
        };
        resolve_secrets(
            &HashMap::from([("spill-kek".to_string(), def)]),
            &danger_zone,
        )
        .unwrap()
    }

    fn validate(section: &SpillSection, device: &DeviceSection) -> Result<()> {
        section.validate(
            device,
            Some(&remote_source()),
            &TuningSection::default(),
            &HashMap::new(),
        )
    }

    fn rejects(result: Result<()>, message: &str) {
        let err = result.expect_err("must be rejected").to_string();
        assert!(err.contains(message), "expected {message:?} in {err:?}");
    }

    #[test]
    fn spill_section_parses_full_example() {
        let section: SpillSection = toml::from_str(FULL_EXAMPLE).unwrap();
        assert_eq!(section.max_local_bytes, 12884901888);
        assert_eq!(section.low_water_bytes, 536870912);
        assert_eq!(section.hard_margin_bytes, 268435456);
        assert_eq!(section.min_free_bytes, 1073741824);
        assert_eq!(section.on_full, OnFull::Stall);
        assert!(!section.clean_eviction);
        assert_eq!(section.max_concurrent_evictions, 4);
        assert_eq!(
            section.compression,
            ArchiveCompressionAlgorithm::Zstd { level: 3 }
        );
        assert_eq!(section.kek, Some(SecretRef::Ref("spill-kek".to_string())));
        match &section.store {
            Some(ArchiveStorageConfig::S3 {
                bucket,
                prefix,
                region,
                connections,
                access_key_id,
                secret_access_key,
                ..
            }) => {
                assert_eq!(bucket, "pg-ubicloud-ci-forks");
                assert_eq!(prefix, &Some("forks".to_string()));
                assert_eq!(region, &Some("us-west-2".to_string()));
                assert_eq!(*connections, 16);
                assert_eq!(access_key_id, &None);
                assert_eq!(secret_access_key, &None);
            }
            other => panic!("expected an S3 store, got {other:?}"),
        }

        section
            .validate(
                &device(),
                Some(&remote_source()),
                &TuningSection::default(),
                &secrets(32),
            )
            .expect("the documented example must validate");
    }

    #[test]
    fn spill_defaults() {
        let section: SpillSection = toml::from_str("max_local_bytes = 1").unwrap();
        assert_eq!(section.max_local_bytes, 1);
        assert_eq!(section.low_water_bytes, 512 * 1024 * 1024);
        assert_eq!(section.hard_margin_bytes, 256 * 1024 * 1024);
        assert_eq!(section.min_free_bytes, 512 * 1024 * 1024);
        assert_eq!(section.on_full, OnFull::Stall);
        assert!(!section.clean_eviction);
        assert_eq!(section.max_concurrent_evictions, 4);
        assert_eq!(
            section.compression,
            ArchiveCompressionAlgorithm::Zstd { level: 3 }
        );
        assert_eq!(section.kek, None);
        assert_eq!(section.store, None);
        assert_eq!(section.store_path(), None);

        let section: SpillSection =
            toml::from_str("max_local_bytes = 1\non_full = \"fail\"\ncompression = \"none\"")
                .unwrap();
        assert_eq!(section.on_full, OnFull::Fail);
        assert_eq!(section.compression, ArchiveCompressionAlgorithm::None);
    }

    #[test]
    fn spill_rejects_unknown_field() {
        let result = toml::from_str::<SpillSection>("max_local_bytes = 1\nmax_local = 2");
        assert!(result.is_err());
    }

    #[test]
    fn spill_resolve_paths_resolves_filesystem_store() {
        let mut section = section();
        section.store = Some(fs_store("spill"));
        section.resolve_paths(Path::new("/etc/ubiblk"));
        assert_eq!(
            section.store_path(),
            Some(&PathBuf::from("/etc/ubiblk/spill"))
        );

        // Absolute paths and S3 stores are left alone.
        let mut section: SpillSection = toml::from_str(FULL_EXAMPLE).unwrap();
        let before = section.clone();
        section.resolve_paths(Path::new("/etc/ubiblk"));
        assert_eq!(section, before);
    }

    #[test]
    fn spill_accepts_a_valid_section() {
        validate(&section(), &device()).expect("valid");
        // Without a stripe source there is nothing to check about it.
        section()
            .validate(&device(), None, &TuningSection::default(), &HashMap::new())
            .expect("valid without a stripe source");
    }

    #[test]
    fn spill_rejects_zero_max_local_bytes() {
        let mut section = section();
        section.max_local_bytes = 0;
        rejects(
            validate(&section, &device()),
            "spill.max_local_bytes must be greater than 0",
        );
    }

    #[test]
    fn spill_rejects_low_water_at_or_above_max_local() {
        let mut section = section();
        section.low_water_bytes = section.max_local_bytes;
        rejects(
            validate(&section, &device()),
            "spill.low_water_bytes must be below max_local_bytes",
        );
    }

    #[test]
    fn spill_rejects_max_concurrent_evictions_out_of_range() {
        for out_of_range in [0, 65] {
            let mut section = section();
            section.max_concurrent_evictions = out_of_range;
            rejects(
                validate(&section, &device()),
                "spill.max_concurrent_evictions must be between 1 and 64",
            );
        }
    }

    #[test]
    fn spill_rejects_device_without_metadata_path() {
        let mut device = device();
        device.metadata_path = None;
        rejects(
            validate(&section(), &device),
            "spill needs a device with metadata_path",
        );
    }

    #[test]
    fn spill_rejects_track_written_off() {
        let mut device = device();
        device.track_written = false;
        rejects(
            validate(&section(), &device),
            "spill needs track_written = true to tell dirty from clean",
        );
    }

    #[test]
    fn spill_rejects_default_device_id() {
        let mut device = device();
        device.device_id = "ubiblk".to_string();
        rejects(
            validate(&section(), &device),
            "spill needs an explicit device_id; it is part of the object key",
        );
    }

    #[test]
    fn spill_rejects_snapshot_server() {
        let mut device = device();
        device.snapshot_server = Some("0.0.0.0:9500".to_string());
        rejects(
            validate(&section(), &device),
            "spill cannot be combined with snapshot_server: a served stripe may be a hole",
        );
    }

    #[test]
    fn spill_rejects_copy_on_read_false() {
        let raw = StripeSourceConfig::Raw {
            image_path: "/base.img".into(),
            autofetch: false,
            copy_on_read: false,
        };
        rejects(
            section().validate(
                &device(),
                Some(&raw),
                &TuningSection::default(),
                &HashMap::new(),
            ),
            "spill needs copy_on_read = true",
        );
    }

    #[test]
    fn spill_rejects_autofetch() {
        let mut source = remote_source();
        if let StripeSourceConfig::Remote(config) = &mut source {
            config.autofetch = true;
        }
        rejects(
            section().validate(
                &device(),
                Some(&source),
                &TuningSection::default(),
                &HashMap::new(),
            ),
            "spill needs autofetch = false",
        );
    }

    #[test]
    fn spill_rejects_clean_eviction_without_snapshot_source() {
        let mut section = section();
        section.clean_eviction = true;
        let mut device = device();
        device.snapshot_source = None;
        rejects(
            validate(&section, &device),
            "clean_eviction needs snapshot_source to track snapshot liveness",
        );
        // With a snapshot source it is fine.
        validate(&section, &self::device()).expect("valid");
    }

    #[test]
    fn spill_rejects_no_store_without_clean_eviction() {
        let mut section = section();
        section.store = None;
        rejects(
            validate(&section, &device()),
            "spill without a store can only evict clean stripes; set clean_eviction = true or configure spill.store",
        );
        section.clean_eviction = true;
        validate(&section, &device()).expect("clean-only is valid");
    }

    #[test]
    fn spill_rejects_store_with_archive_kek_or_autofetch() {
        let mut section = section();
        section.store = Some(ArchiveStorageConfig::Filesystem {
            path: "/mnt/other/spill".into(),
            archive_kek: None,
            autofetch: true,
        });
        rejects(
            validate(&section, &device()),
            "spill.store does not take archive_kek or autofetch",
        );

        section.store = Some(ArchiveStorageConfig::Filesystem {
            path: "/mnt/other/spill".into(),
            archive_kek: Some(SecretRef::Ref("spill-kek".to_string())),
            autofetch: false,
        });
        rejects(
            section.validate(
                &device(),
                Some(&remote_source()),
                &TuningSection::default(),
                &secrets(32),
            ),
            "spill.store does not take archive_kek or autofetch",
        );
    }

    #[test]
    fn spill_rejects_invalid_store() {
        let mut section: SpillSection = toml::from_str(FULL_EXAMPLE).unwrap();
        section.kek = None;
        if let Some(ArchiveStorageConfig::S3 { connections, .. }) = &mut section.store {
            *connections = 0;
        }
        rejects(
            validate(&section, &device()),
            "S3 connections must be greater than 0",
        );
    }

    #[test]
    fn spill_rejects_kek_of_wrong_length() {
        let mut section = section();
        section.kek = Some(SecretRef::Ref("spill-kek".to_string()));
        rejects(
            section.validate(
                &device(),
                Some(&remote_source()),
                &TuningSection::default(),
                &secrets(16),
            ),
            "spill.kek secret must be exactly 32 bytes for AES-256-GCM (got 16 bytes)",
        );
        rejects(validate(&section, &device()), "spill-kek");
        section
            .validate(
                &device(),
                Some(&remote_source()),
                &TuningSection::default(),
                &secrets(32),
            )
            .expect("a 32-byte kek is valid");
    }

    #[test]
    fn spill_rejects_inflight_counter_overflow() {
        let tuning = TuningSection {
            num_queues: 2,
            queue_size: 65536,
            ..Default::default()
        };
        rejects(
            section().validate(&device(), Some(&remote_source()), &tuning, &HashMap::new()),
            "spill's per-stripe in-flight counter is 16 bits; reduce num_queues * queue_size",
        );
        let tuning = TuningSection {
            num_queues: 1,
            queue_size: 65535,
            ..Default::default()
        };
        section()
            .validate(&device(), Some(&remote_source()), &tuning, &HashMap::new())
            .expect("exactly u16::MAX outstanding requests fit");
    }
}
