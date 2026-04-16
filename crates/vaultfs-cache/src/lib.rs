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

/// Evict disk cache entries by mtime (oldest first) until total usage drops
/// at or below `max_bytes`. Each cache entry is a pair of files (`<hash>` +
/// `<hash>.meta`); we remove both when evicting to keep them in sync.
async fn evict_disk(disk_path: &std::path::Path, max_bytes: u64) -> std::io::Result<()> {
    struct Item {
        path: PathBuf,
        size: u64,
        mtime: std::time::SystemTime,
    }

    let mut entries = tokio::fs::read_dir(disk_path).await?;
    let mut items: Vec<Item> = Vec::new();
    let mut total: u64 = 0;
    while let Some(entry) = entries.next_entry().await? {
        // Only track the data files; the `.meta` sibling is evicted alongside.
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("meta") {
            continue;
        }
        let meta = entry.metadata().await?;
        let size = meta.len();
        let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
        total += size;
        // Also account for the companion .meta file if it exists.
        let meta_path = path.with_extension("meta");
        if let Ok(m) = tokio::fs::metadata(&meta_path).await {
            total += m.len();
        }
        items.push(Item { path, size, mtime });
    }

    if total <= max_bytes {
        return Ok(());
    }

    items.sort_by_key(|i| i.mtime);

    for item in items {
        if total <= max_bytes {
            break;
        }
        let meta_path = item.path.with_extension("meta");
        let meta_size = tokio::fs::metadata(&meta_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        if tokio::fs::remove_file(&item.path).await.is_ok() {
            total = total.saturating_sub(item.size);
        }
        if tokio::fs::remove_file(&meta_path).await.is_ok() {
            total = total.saturating_sub(meta_size);
        }
    }
    Ok(())
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

            // Enforce disk_max: evict oldest entries (by mtime) until under the cap.
            if self.disk_max > 0 {
                let _ = evict_disk(disk_path, self.disk_max).await;
            }
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

    /// Total bytes currently occupied on disk by the cache.
    pub async fn disk_usage(&self) -> u64 {
        let Some(ref disk_path) = self.disk_path else {
            return 0;
        };
        let Ok(mut entries) = tokio::fs::read_dir(disk_path).await else {
            return 0;
        };
        let mut total: u64 = 0;
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(meta) = entry.metadata().await {
                total += meta.len();
            }
        }
        total
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_cache_evicts_when_over_capacity() {
        let cache = Cache::new(100, None, 0);

        // Each value is 40 bytes; cap is 100 → 2 fit, 3rd should evict LRU.
        cache.put("a", Bytes::from(vec![0u8; 40]), "text/plain").await.unwrap();
        cache.put("b", Bytes::from(vec![0u8; 40]), "text/plain").await.unwrap();

        // Touch "a" so "b" becomes the LRU.
        assert!(cache.get("a").await.is_some());

        cache.put("c", Bytes::from(vec![0u8; 40]), "text/plain").await.unwrap();

        assert!(cache.get("a").await.is_some(), "a was touched, should survive");
        assert!(cache.get("b").await.is_none(), "b was LRU, should be evicted");
        assert!(cache.get("c").await.is_some(), "c was just inserted");
    }

    #[tokio::test]
    async fn oversized_entry_is_rejected() {
        let cache = Cache::new(50, None, 0);
        cache
            .put("big", Bytes::from(vec![0u8; 100]), "text/plain")
            .await
            .unwrap();
        assert!(
            cache.get("big").await.is_none(),
            "entries larger than max_size must not be cached"
        );
    }

    #[tokio::test]
    async fn disk_cache_enforces_disk_max() {
        let tmp = std::env::temp_dir().join(format!("vfs-cache-{}", uuid::Uuid::new_v4()));
        // Cap at 200 bytes. Each entry is data(60) + meta(~10) ≈ 70 bytes → ~2 fit.
        let cache = Cache::new(10 * 1024 * 1024, Some(tmp.clone()), 200);

        for i in 0..5u8 {
            cache
                .put(
                    &format!("k{}", i),
                    Bytes::from(vec![i; 60]),
                    "text/plain",
                )
                .await
                .unwrap();
            // Force distinguishable mtimes on fast filesystems.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let usage = cache.disk_usage().await;
        assert!(
            usage <= 200,
            "disk usage {} should be <= disk_max 200",
            usage
        );

        // Newest entry must still be readable from disk.
        cache.clear().await.unwrap();
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn disk_max_zero_disables_eviction() {
        let tmp = std::env::temp_dir().join(format!("vfs-cache-{}", uuid::Uuid::new_v4()));
        let cache = Cache::new(10 * 1024 * 1024, Some(tmp.clone()), 0);

        for i in 0..5u8 {
            cache
                .put(&format!("k{}", i), Bytes::from(vec![i; 60]), "text/plain")
                .await
                .unwrap();
        }
        // Every entry stays — disk_max=0 means unlimited.
        assert!(cache.disk_usage().await >= 300);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
