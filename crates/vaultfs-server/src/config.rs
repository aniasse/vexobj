use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub images: ImageConfig,
    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_max_file_size")]
    pub max_file_size: String,
    #[serde(default = "default_true")]
    pub deduplication: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            max_file_size: default_max_file_size(),
            deduplication: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_memory_size")]
    pub memory_size: String,
    #[serde(default = "default_disk_size")]
    pub disk_size: String,
    #[serde(default)]
    pub disk_path: Option<String>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            memory_size: default_memory_size(),
            disk_size: default_disk_size(),
            disk_path: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ImageConfig {
    #[serde(default = "default_quality")]
    pub default_quality: u8,
    #[serde(default = "default_max_transform_size")]
    pub max_transform_size: String,
}

impl Default for ImageConfig {
    fn default() -> Self {
        Self {
            default_quality: default_quality(),
            max_transform_size: default_max_transform_size(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

fn default_bind() -> String {
    "0.0.0.0:8000".into()
}
fn default_data_dir() -> String {
    "./data".into()
}
fn default_max_file_size() -> String {
    "5GB".into()
}
fn default_memory_size() -> String {
    "256MB".into()
}
fn default_disk_size() -> String {
    "2GB".into()
}
fn default_quality() -> u8 {
    85
}
fn default_max_transform_size() -> String {
    "50MB".into()
}
fn default_true() -> bool {
    true
}

impl Config {
    pub fn load() -> Result<Self> {
        // Try loading from config file, fall back to defaults
        let config_path = std::env::var("VAULTFS_CONFIG").unwrap_or_else(|_| "config.toml".into());

        if std::path::Path::new(&config_path).exists() {
            let content = std::fs::read_to_string(&config_path)?;
            Ok(toml::from_str(&content)?)
        } else {
            Ok(toml::from_str("")?)
        }
    }
}

pub fn parse_size(s: &str) -> u64 {
    let s = s.trim().to_uppercase();
    if let Some(n) = s.strip_suffix("GB") {
        n.trim().parse::<u64>().unwrap_or(1) * 1024 * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("MB") {
        n.trim().parse::<u64>().unwrap_or(1) * 1024 * 1024
    } else if let Some(n) = s.strip_suffix("KB") {
        n.trim().parse::<u64>().unwrap_or(1) * 1024
    } else {
        s.parse::<u64>().unwrap_or(1024 * 1024 * 1024)
    }
}
