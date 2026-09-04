use std::sync::Arc;

use log::info;
use ubiblk_macros::error_context;

use crate::{
    archive::{ArchiveStore, FileSystemStore, S3Store},
    backends::build_raw_image_device,
    block_device::{spill::SpillRuntime, NullBlockDevice, SharedMetadataState},
    config::v2::{
        self,
        secrets::{get_resolved_secret, ResolvedSecret, SecretRef},
        stripe_source::ArchiveStorageConfig,
    },
    stripe_server::{connect_to_stripe_server, RemoteStripeProvider},
    utils::s3::{build_s3_client, create_runtime, RateLimitedRetry, S3ClientTuning},
    CipherMethod, KeyEncryptionCipher, Result,
};

use super::*;

/// What a builder needs to wrap the base source in a `SpillingStripeSource`:
/// the runtime the backend built from `[spill]`, and the state it routes by.
#[derive(Clone)]
pub struct SpillSourceParts {
    /// Store factory, codec and device id, as built from `[spill]`.
    pub runtime: SpillRuntime,
    /// The per-stripe state the composite source routes by.
    pub state: SharedMetadataState,
}

#[derive(Clone)]
pub struct StripeSourceBuilder {
    device_config: v2::Config,
    stripe_sector_count: u64,
    has_fetched_all_stripes: bool,
    spill: Option<SpillSourceParts>,
}

impl StripeSourceBuilder {
    /// `has_fetched_all_stripes` must be passed as false when spill is on: the
    /// null-source shortcut would leave clean re-pulls nowhere to go.
    pub fn new(
        device_config: v2::Config,
        stripe_sector_count: u64,
        has_fetched_all_stripes: bool,
        spill: Option<SpillSourceParts>,
    ) -> Self {
        Self {
            device_config,
            stripe_sector_count,
            has_fetched_all_stripes,
            spill,
        }
    }

    #[error_context("Failed to build stripe source")]
    pub fn build(&self) -> Result<Box<dyn StripeSource>> {
        self.build_with_connections(None)
    }

    /// Build a source that opens `connections` connections instead of what the
    /// config asks for. Ingest workers share the configured budget rather than
    /// each opening a full set. With spill configured the source is wrapped so
    /// evicted stripes can be routed to the spill store.
    #[error_context("Failed to build stripe source")]
    pub fn build_with_connections(
        &self,
        connection_override: Option<usize>,
    ) -> Result<Box<dyn StripeSource>> {
        let base = self.build_base_source(connection_override)?;
        let Some(parts) = &self.spill else {
            return Ok(base);
        };

        let spill = match &parts.runtime.store_factory {
            None => None,
            Some(factory) => {
                // Pool workers share the store's connection budget the way
                // they share a remote source's.
                let configured = match self
                    .device_config
                    .spill
                    .as_ref()
                    .and_then(|s| s.store.as_ref())
                {
                    Some(ArchiveStorageConfig::S3 { connections, .. }) => *connections,
                    _ => 1,
                };
                let connections = connection_override.unwrap_or(configured).max(1);
                Some(SpillStripeSource::new(
                    factory(connections)?,
                    parts.runtime.codec.clone(),
                    parts.runtime.device_id.clone(),
                    connections,
                    &parts.state,
                ))
            }
        };
        Ok(Box::new(SpillingStripeSource::new(
            base,
            spill,
            parts.state.clone(),
        )))
    }

    fn build_base_source(
        &self,
        connection_override: Option<usize>,
    ) -> Result<Box<dyn StripeSource>> {
        // If already fetched all stripes, no need to build a real source
        if self.has_fetched_all_stripes {
            info!("All stripes have been fetched; using NullBlockDevice as stripe source");
            return Ok(Box::new(BlockDeviceStripeSource::new(
                NullBlockDevice::new(),
                self.stripe_sector_count,
            )?));
        }

        if let Some(stripe_source) = self.device_config.stripe_source.as_ref() {
            match stripe_source {
                v2::stripe_source::StripeSourceConfig::Archive(config) => {
                    let store = Self::build_archive_store(config, &self.device_config.secrets)?;
                    let stripe_source = ArchiveStripeSource::new(
                        store,
                        Self::build_archive_kek(config, &self.device_config.secrets)?,
                    )?;
                    return Ok(Box::new(stripe_source));
                }
                v2::stripe_source::StripeSourceConfig::Remote(config) => {
                    // `connections` is validated (> 0) when the config is loaded;
                    // honor it as-is so a misconfiguration surfaces rather than
                    // being silently coerced.
                    let connections = connection_override.unwrap_or(config.connections).max(1);
                    // A factory that dials a fresh connection; reused both for
                    // the initial pool and for a worker to reconnect on failure.
                    let config = config.clone();
                    let secrets = self.device_config.secrets.clone();
                    let connect: Arc<ConnectFn> = Arc::new(move || {
                        connect_to_stripe_server(&config, &secrets)
                            .map(|client| Box::new(client) as Box<dyn RemoteStripeProvider + Send>)
                    });
                    let mut clients: Vec<Box<dyn RemoteStripeProvider + Send>> =
                        Vec::with_capacity(connections);
                    for _ in 0..connections {
                        clients.push(connect()?);
                    }
                    let stripe_source =
                        RemoteStripeSource::new(clients, connect, self.stripe_sector_count)?;
                    return Ok(Box::new(stripe_source));
                }
                v2::stripe_source::StripeSourceConfig::Raw { .. } => {}
            }
        }

        let source_block_device =
            build_raw_image_device(&self.device_config)?.unwrap_or(NullBlockDevice::new());

        Ok(Box::new(BlockDeviceStripeSource::new(
            source_block_device,
            self.stripe_sector_count,
        )?))
    }

    fn resolved_secret_to_string(
        secret_ref: &SecretRef,
        secrets: &std::collections::HashMap<String, ResolvedSecret>,
    ) -> Result<String> {
        let secret = get_resolved_secret(secret_ref, secrets)?;
        String::from_utf8(secret.as_bytes().to_vec()).map_err(|_| {
            crate::ubiblk_error!(InvalidParameter {
                description: format!("Secret '{}' is not valid UTF-8", secret_ref.id()),
            })
        })
    }

    /// Static credentials from the config, or `None` when both keys are
    /// omitted so the SDK runs its default provider chain (instance role,
    /// `AWS_*` environment).
    fn build_aws_credentials(
        access_key_id: Option<&SecretRef>,
        secret_access_key: Option<&SecretRef>,
        session_token: Option<&SecretRef>,
        secrets: &std::collections::HashMap<String, ResolvedSecret>,
    ) -> Result<Option<aws_sdk_s3::config::Credentials>> {
        let (access_key_id, secret_access_key) = match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => (access_key_id, secret_access_key),
            (None, None) => return Ok(None),
            _ => {
                return Err(crate::ubiblk_error!(InvalidParameter {
                    description: "S3 access_key_id and secret_access_key must be set together (omit both to use the instance role)".to_string(),
                }))
            }
        };
        let access_key_id = Self::resolved_secret_to_string(access_key_id, secrets)?;
        let secret_access_key = Self::resolved_secret_to_string(secret_access_key, secrets)?;
        let session_token = session_token
            .map(|t| Self::resolved_secret_to_string(t, secrets))
            .transpose()?;

        let mut credentials = aws_sdk_s3::config::Credentials::builder()
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key)
            .provider_name("ubiblk_archive");
        if let Some(session_token) = session_token {
            credentials = credentials.session_token(session_token);
        }

        Ok(Some(credentials.build()))
    }

    pub fn build_archive_kek(
        config: &ArchiveStorageConfig,
        secrets: &std::collections::HashMap<String, ResolvedSecret>,
    ) -> Result<KeyEncryptionCipher> {
        let archive_kek = match config {
            ArchiveStorageConfig::Filesystem { archive_kek, .. } => archive_kek,
            ArchiveStorageConfig::S3 { archive_kek, .. } => archive_kek,
        };

        let Some(archive_kek) = archive_kek else {
            return Ok(KeyEncryptionCipher::default());
        };

        let key = secrets
            .get(archive_kek.id())
            .ok_or_else(|| {
                crate::ubiblk_error!(InvalidParameter {
                    description: format!("Archive KEK secret '{}' not found", archive_kek.id()),
                })
            })?
            .as_bytes()
            .to_vec();
        Ok(KeyEncryptionCipher {
            method: CipherMethod::Aes256Gcm,
            key: Some(key),
            auth_data: Some(b"ubiblk_archive".to_vec()),
        })
    }

    pub fn build_archive_store(
        config: &ArchiveStorageConfig,
        secrets: &std::collections::HashMap<String, ResolvedSecret>,
    ) -> Result<Box<dyn ArchiveStore>> {
        let worker_threads = match config {
            ArchiveStorageConfig::S3 { connections, .. } => *connections,
            ArchiveStorageConfig::Filesystem { .. } => 1,
        };
        Self::build_object_store(config, secrets, worker_threads)
    }

    /// Same as `build_archive_store` with an explicit worker count: a spill
    /// store is built once per fetcher and once for the evictor, each with its
    /// own share of the connection budget.
    pub fn build_object_store(
        config: &ArchiveStorageConfig,
        secrets: &std::collections::HashMap<String, ResolvedSecret>,
        worker_threads: usize,
    ) -> Result<Box<dyn ArchiveStore>> {
        match config {
            ArchiveStorageConfig::Filesystem { path, .. } => {
                Ok(Box::new(FileSystemStore::new(path.into())?))
            }
            ArchiveStorageConfig::S3 {
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                endpoint,
                connect_timeout_ms,
                operation_attempt_timeout_ms,
                max_attempts,
                rate_limited_retry,
                ..
            } => {
                let decrypted_credentials = Self::build_aws_credentials(
                    access_key_id.as_ref(),
                    secret_access_key.as_ref(),
                    session_token.as_ref(),
                    secrets,
                )?;
                let runtime = create_runtime()?;
                let client = build_s3_client(
                    &runtime,
                    None,
                    endpoint.as_deref(),
                    region.as_deref(),
                    decrypted_credentials,
                    S3ClientTuning {
                        connect_timeout_ms: *connect_timeout_ms,
                        operation_attempt_timeout_ms: *operation_attempt_timeout_ms,
                        max_attempts: *max_attempts,
                        rate_limited_retry: rate_limited_retry.enabled.then(|| RateLimitedRetry {
                            min_delay: std::time::Duration::from_millis(
                                rate_limited_retry.min_delay_ms,
                            ),
                            jitter: std::time::Duration::from_millis(rate_limited_retry.jitter_ms),
                        }),
                    },
                )?;

                Ok(Box::new(S3Store::new(
                    client,
                    bucket.to_string(),
                    prefix.clone(),
                    worker_threads,
                )?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::v2::secrets::{resolve_secrets, SecretDef, SecretEncoding, SecretSource};
    use crate::config::v2::stripe_source::{
        ArchiveStorageConfig, RateLimitedRetryConfig, StripeSourceConfig,
    };
    use crate::config::v2::{DangerZone, DeviceSection};
    use base64::Engine;
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_config(remote: Option<String>, path: Option<PathBuf>) -> v2::Config {
        let stripe_source = path
            .map(|path| StripeSourceConfig::Raw {
                image_path: path,
                autofetch: false,
                copy_on_read: false,
            })
            .or_else(|| {
                remote.map(|remote| {
                    StripeSourceConfig::Remote(v2::stripe_source::RemoteStripeConfig {
                        address: remote,
                        psk: None,
                        autofetch: false,
                        connect_timeout_ms: 5_000,
                        operation_attempt_timeout_ms: 20_000,
                        connections: 1,
                        compression: Default::default(),
                    })
                })
            });

        v2::Config {
            device: DeviceSection {
                snapshot_server: None,
                snapshot_source: None,
                snapshot_compression: Default::default(),
                data_path: "/tmp/non-existent-disk".into(),
                metadata_path: None,
                vhost_socket: None,
                rpc_socket: None,
                device_id: "ubiblk".to_string(),
                track_written: false,
            },
            tuning: v2::tuning::TuningSection {
                queue_size: 64,
                ..Default::default()
            },
            encryption: None,
            danger_zone: v2::DangerZone {
                enabled: true,
                allow_unencrypted_disk: true,
                allow_inline_plaintext_secrets: true,
                allow_secret_over_regular_file: true,
                allow_unencrypted_connection: true,
                allow_env_secrets: false,
            },
            stripe_source,
            spill: None,
            secrets: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_build_defaults_to_null_device() {
        let config = create_test_config(None, None);
        let builder = StripeSourceBuilder::new(config, 4096, false, None);

        let result = builder.build();

        assert!(
            result.is_ok(),
            "Should successfully build a NullBlockDevice source when no paths provided"
        );
    }

    #[test]
    fn test_build_local_block_device() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.img");

        // Create a dummy 1MB file so build_block_device succeeds
        let f = File::create(&file_path).unwrap();
        f.set_len(1024 * 1024).unwrap();

        let config = create_test_config(None, Some(file_path));
        let builder = StripeSourceBuilder::new(config, 4096, false, None);

        let result = builder.build();
        assert!(
            result.is_ok(),
            "Should successfully build a BlockDeviceStripeSource with valid image_path"
        );
    }

    #[test]
    fn test_build_local_block_device_fails_on_missing_file() {
        let bad_path = PathBuf::from("/path/to/nonexistent/file.img");
        let config = create_test_config(None, Some(bad_path));
        let builder = StripeSourceBuilder::new(config, 4096, false, None);

        let result = builder.build();

        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(
            err_msg.to_lowercase().contains("not found")
                || err_msg.to_lowercase().contains("no such file"),
            "Should return file not found error. Got: {}",
            err_msg
        );
    }

    #[test]
    fn test_connect_to_invalid_remote_server() {
        let config = create_test_config(Some("127.0.0.1:99999".to_string()), None);
        let builder = StripeSourceBuilder::new(config, 4096, false, None);

        let result = builder.build();

        assert!(
            result.is_err(),
            "Should fail to connect to invalid remote server"
        );
    }

    #[test]
    fn test_skips_building_real_source_when_all_stripes_fetched() {
        let config = create_test_config(None, None);
        let builder = StripeSourceBuilder::new(config, 4096, true, None);

        let result = builder.build();
        assert!(
            result.is_ok(),
            "Should successfully build a NullBlockDevice source when all stripes fetched"
        );

        let stripe_source = result.unwrap();
        // NullBlockDevice has sector_count of 0
        assert_eq!(stripe_source.sector_count(), 0);
    }

    fn make_inline_secret(value: &str) -> SecretDef {
        SecretDef {
            source: SecretSource::Inline(
                base64::engine::general_purpose::STANDARD.encode(value.as_bytes()),
            ),
            encrypted_by: None,
            encoding: SecretEncoding::Base64,
        }
    }

    fn make_inline_secret_bytes(value: &[u8]) -> SecretDef {
        SecretDef {
            source: SecretSource::Inline(base64::engine::general_purpose::STANDARD.encode(value)),
            encrypted_by: None,
            encoding: SecretEncoding::Base64,
        }
    }

    fn danger_zone_permissive() -> DangerZone {
        DangerZone {
            enabled: true,
            allow_unencrypted_disk: true,
            allow_inline_plaintext_secrets: true,
            allow_secret_over_regular_file: true,
            allow_unencrypted_connection: true,
            allow_env_secrets: false,
        }
    }

    fn resolve(defs: HashMap<String, SecretDef>) -> HashMap<String, ResolvedSecret> {
        resolve_secrets(&defs, &danger_zone_permissive()).unwrap()
    }

    #[test]
    fn test_build_archive_kek_filesystem() {
        let kek_bytes = "0123456789abcdef0123456789abcdef";
        let secrets = resolve(HashMap::from([(
            "my_kek".to_string(),
            make_inline_secret(kek_bytes),
        )]));
        let config = ArchiveStorageConfig::Filesystem {
            path: "/tmp/archive".into(),
            archive_kek: Some(SecretRef::Ref("my_kek".to_string())),
            autofetch: false,
        };

        let result = StripeSourceBuilder::build_archive_kek(&config, &secrets);
        assert!(result.is_ok());
        let kek = result.unwrap();
        assert_eq!(kek.method, CipherMethod::Aes256Gcm);
        assert_eq!(kek.key.unwrap(), kek_bytes.as_bytes());
        assert_eq!(kek.auth_data.unwrap(), b"ubiblk_archive");
    }

    #[test]
    fn test_build_archive_kek_s3() {
        let kek_bytes = "0123456789abcdef0123456789abcdef";
        let secrets = resolve(HashMap::from([
            ("my_kek".to_string(), make_inline_secret(kek_bytes)),
            (
                "aws_key".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            ("aws_secret".to_string(), make_inline_secret("super-secret")),
        ]));
        let config = ArchiveStorageConfig::S3 {
            bucket: "test-bucket".to_string(),
            prefix: None,
            region: Some("us-east-1".to_string()),
            access_key_id: Some(SecretRef::Ref("aws_key".to_string())),
            secret_access_key: Some(SecretRef::Ref("aws_secret".to_string())),
            session_token: None,
            endpoint: None,
            connections: 4,
            connect_timeout_ms: 5_000,
            operation_attempt_timeout_ms: 20_000,
            max_attempts: 3,
            rate_limited_retry: RateLimitedRetryConfig::default(),
            archive_kek: Some(SecretRef::Ref("my_kek".to_string())),
            autofetch: false,
        };

        let result = StripeSourceBuilder::build_archive_kek(&config, &secrets);
        assert!(result.is_ok());
        let kek = result.unwrap();
        assert_eq!(kek.method, CipherMethod::Aes256Gcm);
    }

    #[test]
    fn test_build_archive_kek_missing_secret() {
        let secrets = HashMap::new();
        let config = ArchiveStorageConfig::Filesystem {
            path: "/tmp/archive".into(),
            archive_kek: Some(SecretRef::Ref("nonexistent".to_string())),
            autofetch: false,
        };

        let result = StripeSourceBuilder::build_archive_kek(&config, &secrets);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(err.contains("not found"), "Got: {err}");
    }

    #[test]
    fn test_build_aws_credentials_success() {
        let secrets = resolve(HashMap::from([
            (
                "key_id".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            (
                "secret_key".to_string(),
                make_inline_secret("my-secret-access-key"),
            ),
        ]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            None,
            &secrets,
        );
        assert!(result.is_ok());
        let creds = result.unwrap();
        assert!(creds.is_some());
    }

    #[test]
    fn build_with_connections_wraps_base_when_spill_parts_given() {
        use crate::{
            archive::{ArchiveCompressionAlgorithm, TestObjectStore},
            block_device::{
                spill::{EvictorConfig, SpillCodec},
                UbiMetadata,
            },
            config::v2::spill::OnFull,
        };
        use std::sync::{Arc, Mutex};

        let state = SharedMetadataState::new(&UbiMetadata::new(3, 4, 4));
        let factory_calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = factory_calls.clone();
        let runtime = SpillRuntime {
            cfg: EvictorConfig {
                data_path: "/tmp/device.raw".into(),
                device_id: "fork-1".to_string(),
                stripe_sector_count: 8,
                target_sector_count: 32,
                max_local_bytes: 1 << 20,
                low_water_bytes: 4096,
                hard_margin_bytes: 4096,
                min_free_bytes: 4096,
                clean_eviction: false,
                on_full: OnFull::Stall,
                max_concurrent_evictions: 1,
                sweep_batch: 4096,
                alignment: 4096,
            },
            device_id: "fork-1".to_string(),
            store_factory: Some(Arc::new(move |workers| {
                recorded.lock().unwrap().push(workers);
                Ok(Box::new(TestObjectStore::new()) as Box<dyn ArchiveStore>)
            })),
            codec: SpillCodec::new(ArchiveCompressionAlgorithm::None, None, 8),
            puncher_factory: None,
        };
        let parts = SpillSourceParts {
            runtime,
            state: state.clone(),
        };

        let builder =
            StripeSourceBuilder::new(create_test_config(None, None), 8, false, Some(parts));
        let source = builder.build_with_connections(Some(3)).unwrap();
        assert_eq!(*factory_calls.lock().unwrap(), vec![3]);
        // Null base plus the three spill connections.
        assert_eq!(source.max_concurrent_requests(), 1 + 3);
        assert_eq!(source.sector_count(), 0);

        // Without a store (clean-only) the base is still wrapped but no store
        // is built.
        let mut clean_only = builder.clone();
        if let Some(parts) = &mut clean_only.spill {
            parts.runtime.store_factory = None;
        }
        let source = clean_only.build().unwrap();
        assert_eq!(*factory_calls.lock().unwrap(), vec![3]);
        assert_eq!(source.max_concurrent_requests(), 1);
    }

    #[test]
    fn build_aws_credentials_none_when_keys_absent() {
        let result = StripeSourceBuilder::build_aws_credentials(None, None, None, &HashMap::new());
        assert!(result.unwrap().is_none(), "default provider chain");

        // One key without the other is a misconfiguration, not a fallback.
        let key = SecretRef::Ref("key_id".to_string());
        for (id, secret) in [(Some(&key), None), (None, Some(&key))] {
            let err = StripeSourceBuilder::build_aws_credentials(id, secret, None, &HashMap::new())
                .unwrap_err()
                .to_string();
            assert!(err.contains("must be set together"), "Got: {err}");
        }
    }

    #[test]
    fn build_object_store_filesystem_ignores_worker_count() {
        let dir = tempdir().unwrap();
        let config = ArchiveStorageConfig::Filesystem {
            path: dir.path().join("spill"),
            archive_kek: None,
            autofetch: false,
        };
        let mut store =
            StripeSourceBuilder::build_object_store(&config, &HashMap::new(), 7).unwrap();
        store
            .put_object("dev/1", b"data", std::time::Duration::from_secs(5))
            .unwrap();
        assert!(dir.path().join("spill/dev/1").is_file());
    }

    #[test]
    fn test_build_aws_credentials_with_session_token() {
        let secrets = resolve(HashMap::from([
            (
                "key_id".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            (
                "secret_key".to_string(),
                make_inline_secret("my-secret-access-key"),
            ),
            ("session".to_string(), make_inline_secret("session-token")),
        ]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            Some(&SecretRef::Ref("session".to_string())),
            &secrets,
        );
        assert!(result.is_ok());
        let creds = result.unwrap().unwrap();
        assert_eq!(creds.session_token(), Some("session-token"));
    }

    #[test]
    fn test_build_aws_credentials_missing_session_token() {
        let secrets = resolve(HashMap::from([
            (
                "key_id".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            (
                "secret_key".to_string(),
                make_inline_secret("my-secret-access-key"),
            ),
        ]));
        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            Some(&SecretRef::Ref("missing_session".to_string())),
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("missing_session") && err.contains("not found"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_aws_credentials_non_utf8_session_token() {
        let secrets = resolve(HashMap::from([
            (
                "key_id".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            (
                "secret_key".to_string(),
                make_inline_secret("my-secret-access-key"),
            ),
            (
                "session".to_string(),
                SecretDef {
                    source: SecretSource::Inline(
                        base64::engine::general_purpose::STANDARD.encode([0xFF, 0xFE, 0xFD]),
                    ),
                    encrypted_by: None,
                    encoding: SecretEncoding::Base64,
                },
            ),
        ]));
        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            Some(&SecretRef::Ref("session".to_string())),
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("session") && err.to_lowercase().contains("utf-8"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_aws_credentials_missing_access_key_id() {
        let secrets = resolve(HashMap::from([(
            "secret_key".to_string(),
            make_inline_secret("my-secret-access-key"),
        )]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("missing_key".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            None,
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("missing_key") && err.contains("not found"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_aws_credentials_missing_secret_access_key() {
        let secrets = resolve(HashMap::from([(
            "key_id".to_string(),
            make_inline_secret("AKIA1234567890123456"),
        )]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("missing_secret".to_string())),
            None,
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("missing_secret") && err.contains("not found"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_aws_credentials_non_utf8_access_key_id() {
        let secrets = resolve(HashMap::from([
            (
                "bad_key".to_string(),
                make_inline_secret_bytes(&[0xFF, 0xFE, 0xFD]),
            ),
            (
                "secret_key".to_string(),
                make_inline_secret("my-secret-access-key"),
            ),
        ]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("bad_key".to_string())),
            Some(&SecretRef::Ref("secret_key".to_string())),
            None,
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("bad_key") && err.contains("not valid UTF-8"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_aws_credentials_non_utf8_secret_access_key() {
        let secrets = resolve(HashMap::from([
            (
                "key_id".to_string(),
                make_inline_secret("AKIA1234567890123456"),
            ),
            (
                "bad_secret".to_string(),
                make_inline_secret_bytes(&[0xFF, 0xFE, 0xFD]),
            ),
        ]));

        let result = StripeSourceBuilder::build_aws_credentials(
            Some(&SecretRef::Ref("key_id".to_string())),
            Some(&SecretRef::Ref("bad_secret".to_string())),
            None,
            &secrets,
        );
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("bad_secret") && err.contains("not valid UTF-8"),
            "Got: {err}"
        );
    }

    #[test]
    fn test_build_archive_store_filesystem() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("archive");

        let secrets = resolve(HashMap::from([(
            "kek".to_string(),
            make_inline_secret("0123456789abcdef0123456789abcdef"),
        )]));
        let config = ArchiveStorageConfig::Filesystem {
            path: archive_path,
            archive_kek: Some(SecretRef::Ref("kek".to_string())),
            autofetch: false,
        };

        let result = StripeSourceBuilder::build_archive_store(&config, &secrets);
        assert!(result.is_ok());
    }
}
