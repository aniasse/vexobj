use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::debug;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

struct CacheEntry {
    data: Bytes,
    content_type: String,
    size: usize,
}

pub struct Cache {
    memory: Mutex<LruMap>,
    disk_path: Option<PathBuf>,
    memory_max: usize,
    disk_max: u64,
}

struct LruMap {
    entries: HashMap<String, CacheEntry>,
    order: Vec<String>,
    current_size: usize,
    max_size: usize,
}

impl LruMap {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: Vec::new(),
            current_size: 0,
            max_size,
        }
    }

    fn get(&mut self, key: &str) -> Option<(Bytes, String)> {
        if let Some(entry) = self.entries.get(key) {
            // Move to end (most recently used)
            self.order.retain(|k| k != key);
            self.order.push(key.to_string());
            Some((entry.data.clone(), entry.content_type.clone()))
        } else {
            None
        }
    }

    fn insert(&mut self, key: String, data: Bytes, content_type: String) {
        let size = data.len();

        // Evict until we have space
        while self.current_size + size > self.max_size && !self.order.is_empty() {
            let evict_key = self.order.remove(0);
            if let Some(entry) = self.entries.remove(&evict_key) {
                self.current_size -= entry.size;
            }
        }

        if size <= self.max_size {
            self.current_size += size;
            self.order.push(key.clone());
            self.entries.insert(key, CacheEntry {
                data,
                content_type,
                size,
            });
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.current_size -= entry.size;
            self.order.retain(|k| k != key);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.current_size = 0;
    }
}

impl Cache {
    pub fn new(memory_max: usize, disk_path: Option<PathBuf>, disk_max: u64) -> Self {
        if let Some(ref path) = disk_path {
            let _ = std::fs::create_dir_all(path);
        }
        Self {
            memory: Mutex::new(LruMap::new(memory_max)),
            disk_path,
            disk_max,
            memory_max,
        }
    }

    fn cache_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub async fn get(&self, key: &str) -> Option<(Bytes, String)> {
        let hash = Self::cache_key(key);

        // Check memory first
        {
            let mut mem = self.memory.lock().unwrap();
            if let Some(result) = mem.get(&hash) {
                debug!(key, "cache hit (memory)");
                return Some(result);
            }
        }

        // Check disk
        if let Some(ref disk_path) = self.disk_path {
            let data_path = disk_path.join(&hash);
            let meta_path = disk_path.join(format!("{hash}.meta"));

            if let (Ok(data), Ok(content_type)) = (
                tokio::fs::read(&data_path).await,
                tokio::fs::read_to_string(&meta_path).await,
            ) {
                let bytes = Bytes::from(data);
                // Promote to memory cache
                let mut mem = self.memory.lock().unwrap();
                mem.insert(hash, bytes.clone(), content_type.clone());
                debug!(key, "cache hit (disk)");
                return Some((bytes, content_type));
            }
        }

        None
    }

    pub async fn put(&self, key: &str, data: Bytes, content_type: &str) -> Result<(), CacheError> {
        let hash = Self::cache_key(key);

        // Store in memory
        {
            let mut mem = self.memory.lock().unwrap();
            mem.insert(hash.clone(), data.clone(), content_type.to_string());
        }

        // Store on disk
        if let Some(ref disk_path) = self.disk_path {
            let data_path = disk_path.join(&hash);
            let meta_path = disk_path.join(format!("{hash}.meta"));
            tokio::fs::write(&data_path, &data).await?;
            tokio::fs::write(&meta_path, content_type).await?;
        }

        Ok(())
    }

    pub async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        let hash = Self::cache_key(key);

        {
            let mut mem = self.memory.lock().unwrap();
            mem.remove(&hash);
        }

        if let Some(ref disk_path) = self.disk_path {
            let _ = tokio::fs::remove_file(disk_path.join(&hash)).await;
            let _ = tokio::fs::remove_file(disk_path.join(format!("{hash}.meta"))).await;
        }

        Ok(())
    }

    pub async fn clear(&self) -> Result<(), CacheError> {
        {
            let mut mem = self.memory.lock().unwrap();
            mem.clear();
        }

        if let Some(ref disk_path) = self.disk_path {
            let mut entries = tokio::fs::read_dir(disk_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let _ = tokio::fs::remove_file(entry.path()).await;
            }
        }

        Ok(())
    }
}
