use std::path::PathBuf;
use std::sync::Arc;

use crate::config::{self, Config};
use vaultfs_auth::{AuthManager, PresignedUrlGenerator};
use vaultfs_cache::Cache;
use vaultfs_storage::StorageEngine;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<StorageEngine>,
    pub cache: Arc<Cache>,
    pub auth: Arc<AuthManager>,
    pub presigner: Arc<PresignedUrlGenerator>,
    pub config: Arc<Config>,
}

impl AppState {
    pub fn new(config: &Config) -> anyhow::Result<Self> {
        let data_dir = PathBuf::from(&config.storage.data_dir);
        let max_file_size = config::parse_size(&config.storage.max_file_size);

        let storage = StorageEngine::new(data_dir.clone(), max_file_size, config.storage.deduplication)?;

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
            }),
        })
    }
}
