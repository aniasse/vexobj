use std::path::PathBuf;
use std::sync::Arc;

use crate::audit::AuditLogger;
use crate::config::{self, Config};
use crate::metrics::Metrics;
use crate::ratelimit::RateLimiter;
use crate::webhooks::{WebhookConfig, WebhookSender};
use vexobj_auth::{AuthManager, PresignedUrlGenerator};
use vexobj_cache::Cache;
use vexobj_storage::{BlobStore, Encryptor, LocalBlobStore, S3BlobStore, S3Config, StorageEngine};

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub cache: Arc<Cache>,
    pub auth: Arc<AuthManager>,
    pub presigner: Arc<PresignedUrlGenerator>,
    pub config: Arc<Config>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub webhooks: Option<Arc<WebhookSender>>,
    pub metrics: Arc<Metrics>,
    pub audit: Arc<AuditLogger>,
}

impl AppState {
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        let data_dir = PathBuf::from(&config.storage.data_dir);
        let max_file_size = config::parse_size(&config.storage.max_file_size);

        let encryptor = if config.sse.enabled {
            if config.sse.master_key.is_empty() {
                anyhow::bail!("sse.enabled=true but sse.master_key is empty");
            }
            Some(Arc::new(
                Encryptor::from_hex(&config.sse.master_key)
                    .map_err(|e| anyhow::anyhow!("invalid SSE master key: {e}"))?,
            ))
        } else {
            None
        };

        // Select the blob backend. Default "local" preserves existing
        // deployments; "s3" requires [storage.s3] (or its env-var
        // equivalents) to be filled in.
        let blob_store: Arc<dyn BlobStore> = match config.storage.backend.as_str() {
            "local" => Arc::new(LocalBlobStore::new(data_dir.clone())),
            "s3" => {
                let s3 = config
                    .storage
                    .s3
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("storage.backend=s3 but [storage.s3] missing"))?;
                if s3.endpoint.is_empty() || s3.bucket.is_empty() {
                    anyhow::bail!("[storage.s3] endpoint and bucket are required");
                }
                tracing::info!(
                    endpoint = %s3.endpoint,
                    bucket = %s3.bucket,
                    "blob backend: s3"
                );
                Arc::new(S3BlobStore::new(S3Config {
                    endpoint: s3.endpoint.clone(),
                    bucket: s3.bucket.clone(),
                    access_key: s3.access_key.clone(),
                    secret_key: s3.secret_key.clone(),
                    region: s3.region.clone(),
                    path_style: s3.path_style,
                }))
            }
            other => anyhow::bail!("unknown storage.backend: {other} (want: local | s3)"),
        };

        let storage = StorageEngine::with_backend(
            data_dir.clone(),
            max_file_size,
            config.storage.deduplication,
            encryptor,
            blob_store,
        )?;

        let memory_size = config::parse_size(&config.cache.memory_size) as usize;
        let disk_size = config::parse_size(&config.cache.disk_size);
        let disk_path = config
            .cache
            .disk_path
            .as_ref()
            .map(PathBuf::from)
            .or_else(|| Some(data_dir.join("cache")));

        let cache = Cache::new(memory_size, disk_path, disk_size);

        let auth = AuthManager::open(&data_dir.join("auth.db"))?;

        // Load or generate presigning secret
        let secret_path = data_dir.join(".presign_secret");
        let secret = if secret_path.exists() {
            std::fs::read(&secret_path)?
        } else {
            use rand::Rng;
            let secret: Vec<u8> = (0..64).map(|_| rand::thread_rng().gen()).collect();
            std::fs::write(&secret_path, &secret)?;
            secret
        };
        let presigner = PresignedUrlGenerator::new(&secret);

        // Rate limiter
        let rate_limiter = if config.rate_limit.enabled {
            Some(Arc::new(RateLimiter::new(
                config.rate_limit.max_requests,
                config.rate_limit.window_secs,
            )))
        } else {
            None
        };

        // Webhooks
        let webhooks = if !config.webhooks.is_empty() {
            let configs: Vec<WebhookConfig> = config
                .webhooks
                .iter()
                .map(|w| WebhookConfig {
                    url: w.url.clone(),
                    events: w.events.clone(),
                    secret: w.secret.clone(),
                })
                .collect();
            Some(Arc::new(WebhookSender::new(configs)))
        } else {
            None
        };

        let metrics = Metrics::new();
        let audit = AuditLogger::open(&data_dir)?;

        Ok(Self {
            storage: Arc::new(storage),
            cache: Arc::new(cache),
            auth: Arc::new(auth),
            presigner: Arc::new(presigner),
            config: Arc::new(Config {
                server: config::ServerConfig {
                    bind: config.server.bind.clone(),
                },
                storage: config::StorageConfig {
                    data_dir: config.storage.data_dir.clone(),
                    max_file_size: config.storage.max_file_size.clone(),
                    deduplication: config.storage.deduplication,
                    backend: config.storage.backend.clone(),
                    s3: None, // don't propagate S3 credentials into shared state
                },
                cache: config::CacheConfig {
                    memory_size: config.cache.memory_size.clone(),
                    disk_size: config.cache.disk_size.clone(),
                    disk_path: config.cache.disk_path.clone(),
                },
                images: config::ImageConfig {
                    default_quality: config.images.default_quality,
                    max_transform_size: config.images.max_transform_size.clone(),
                },
                auth: config::AuthConfig {
                    enabled: config.auth.enabled,
                },
                tls: config::TlsConfig {
                    enabled: config.tls.enabled,
                    cert_path: config.tls.cert_path.clone(),
                    key_path: config.tls.key_path.clone(),
                },
                rate_limit: config::RateLimitConfig {
                    enabled: config.rate_limit.enabled,
                    max_requests: config.rate_limit.max_requests,
                    window_secs: config.rate_limit.window_secs,
                },
                quotas: config::QuotaConfig {
                    enabled: config.quotas.enabled,
                    default_max_storage: config.quotas.default_max_storage.clone(),
                    default_max_objects: config.quotas.default_max_objects,
                },
                webhooks: Vec::new(), // Don't store webhook secrets in shared state
                sse: config::SseConfig {
                    enabled: config.sse.enabled,
                    // Master key is already bound to Encryptor; don't copy
                    // plaintext into shared state that may get logged.
                    master_key: String::new(),
                },
                transcode: config::TranscodeConfig {
                    workers: config.transcode.workers,
                    max_pending: config.transcode.max_pending,
                    gc_after_days: config.transcode.gc_after_days,
                },
            }),
            rate_limiter,
            webhooks,
            metrics: Arc::new(metrics),
            audit: Arc::new(audit),
        })
    }
}
