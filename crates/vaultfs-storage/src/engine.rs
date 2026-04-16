use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::db::Database;
use crate::error::StorageError;
use crate::models::*;

pub struct LifecycleResult {
    pub objects_expired: u64,
    pub bytes_freed: u64,
}

pub struct StorageEngine {
    db: Database,
    data_dir: PathBuf,
    max_file_size: u64,
    deduplication: bool,
}

impl StorageEngine {
    pub fn new(
        data_dir: PathBuf,
        max_file_size: u64,
        deduplication: bool,
    ) -> Result<Self, StorageError> {
        let db_path = data_dir.join("vaultfs.db");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(data_dir.join("blobs"))?;

        let db = Database::open(&db_path)?;

        Ok(Self {
            db,
            data_dir,
            max_file_size,
            deduplication,
        })
    }

    pub fn create_bucket(&self, req: &CreateBucketRequest) -> Result<Bucket, StorageError> {
        let bucket = self.db.create_bucket(req)?;
        info!(bucket = %bucket.name, "bucket created");
        Ok(bucket)
    }

    pub fn get_bucket(&self, name: &str) -> Result<Bucket, StorageError> {
        self.db.get_bucket(name)
    }

    pub fn list_buckets(&self) -> Result<Vec<Bucket>, StorageError> {
        self.db.list_buckets()
    }

    pub fn delete_bucket(&self, name: &str) -> Result<(), StorageError> {
        self.db.delete_bucket(name)?;
        info!(bucket = %name, "bucket deleted");
        Ok(())
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> Result<ObjectMeta, StorageError> {
        let size = data.len() as u64;
        if size > self.max_file_size {
            return Err(StorageError::ObjectTooLarge {
                size,
                max: self.max_file_size,
            });
        }

        // Compute hash
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let sha256 = hex::encode(hasher.finalize());

        // Content-addressable storage path
        let storage_path = self.blob_path(&sha256);

        // Deduplication: skip write if blob already exists
        if self.deduplication {
            if let Some(existing) = self.db.find_by_hash(&sha256)? {
                let existing_path = self.data_dir.join(&existing);
                if existing_path.exists() {
                    let content_type = content_type
                        .map(String::from)
                        .unwrap_or_else(|| Self::guess_content_type(key));

                    let meta = self.db.put_object(
                        bucket,
                        key,
                        size,
                        &content_type,
                        &sha256,
                        &existing,
                        &metadata.unwrap_or(serde_json::Value::Object(Default::default())),
                    )?;

                    info!(bucket, key, size, deduplicated = true, "object stored");
                    return Ok(meta);
                }
            }
        }

        // Write blob to disk
        let full_path = self.data_dir.join(&storage_path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full_path, &data).await?;

        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| Self::guess_content_type(key));

        let meta = self.db.put_object(
            bucket,
            key,
            size,
            &content_type,
            &sha256,
            &storage_path,
            &metadata.unwrap_or(serde_json::Value::Object(Default::default())),
        )?;

        info!(bucket, key, size, "object stored");
        Ok(meta)
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMeta, bytes::Bytes), StorageError> {
        let (meta, storage_path) = self.db.get_object(bucket, key)?;
        let full_path = self.data_dir.join(&storage_path);
        let data = tokio::fs::read(&full_path).await?;
        Ok((meta, bytes::Bytes::from(data)))
    }

    pub fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectMeta, StorageError> {
        let (meta, _) = self.db.get_object(bucket, key)?;
        Ok(meta)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let storage_path = self.db.delete_object(bucket, key)?;
        // Note: with dedup, we don't delete the blob as other objects may reference it
        if !self.deduplication {
            let full_path = self.data_dir.join(&storage_path);
            let _ = tokio::fs::remove_file(&full_path).await;
        }
        info!(bucket, key, "object deleted");
        Ok(())
    }

    pub fn list_objects(&self, req: &ListObjectsRequest) -> Result<ListObjectsResponse, StorageError> {
        self.db.list_objects(req)
    }

    pub fn object_data_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StorageError> {
        let (_, storage_path) = self.db.get_object(bucket, key)?;
        Ok(self.data_dir.join(storage_path))
    }

    fn blob_path(&self, sha256: &str) -> String {
        // Split hash into directory tiers: ab/cd/abcdef...
        format!("blobs/{}/{}/{}", &sha256[..2], &sha256[2..4], sha256)
    }

    fn guess_content_type(key: &str) -> String {
        mime_guess::from_path(key)
            .first_or_octet_stream()
            .to_string()
    }

    pub fn is_image(content_type: &str) -> bool {
        content_type.starts_with("image/")
    }

    /// Stream-upload: write body to a temp file while hashing, then move to content-addressed path.
    /// This avoids loading the entire file into RAM.
    pub async fn put_object_stream<S, E>(
        &self,
        bucket: &str,
        key: &str,
        mut stream: S,
        content_type: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> Result<ObjectMeta, StorageError>
    where
        S: futures::Stream<Item = Result<bytes::Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        use futures::StreamExt;

        // Verify bucket exists first
        self.db.get_bucket(bucket)?;

        let temp_path = self.data_dir.join(format!(".tmp-{}", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::File::create(&temp_path).await?;
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;

        // Stream body chunks directly to disk
        while let Some(chunk_result) = stream.next().await {
            let data = chunk_result.map_err(|e| {
                StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
            if size + data.len() as u64 > self.max_file_size {
                drop(file);
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(StorageError::ObjectTooLarge {
                    size: size + data.len() as u64,
                    max: self.max_file_size,
                });
            }
            hasher.update(&data);
            file.write_all(&data).await?;
            size += data.len() as u64;
        }
        file.flush().await?;
        drop(file);

        let sha256 = hex::encode(hasher.finalize());
        let storage_path = self.blob_path(&sha256);

        // Deduplication check
        if self.deduplication {
            if let Some(existing) = self.db.find_by_hash(&sha256)? {
                let existing_full = self.data_dir.join(&existing);
                if existing_full.exists() {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    let content_type = content_type
                        .map(String::from)
                        .unwrap_or_else(|| Self::guess_content_type(key));
                    let meta = self.db.put_object(
                        bucket, key, size, &content_type, &sha256, &existing,
                        &metadata.unwrap_or(serde_json::Value::Object(Default::default())),
                    )?;
                    info!(bucket, key, size, deduplicated = true, "object stored (stream)");
                    return Ok(meta);
                }
            }
        }

        // Move temp file to final content-addressed path
        let final_path = self.data_dir.join(&storage_path);
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::rename(&temp_path, &final_path).await?;

        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| Self::guess_content_type(key));

        let meta = self.db.put_object(
            bucket, key, size, &content_type, &sha256, &storage_path,
            &metadata.unwrap_or(serde_json::Value::Object(Default::default())),
        )?;

        info!(bucket, key, size, "object stored (stream)");

        // If versioning is enabled, save a version record
        if self.db.is_versioning_enabled(bucket) {
            let version_id = uuid::Uuid::new_v4().to_string();
            let _ = self.db.save_version(
                bucket,
                key,
                &version_id,
                size,
                &content_type,
                &sha256,
                &storage_path,
            );
        }

        Ok(meta)
    }

    /// Stream-download: returns a stream of file chunks instead of loading entire file into RAM.
    pub async fn get_object_stream(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectMeta, ReaderStream<tokio::fs::File>), StorageError> {
        let (meta, storage_path) = self.db.get_object(bucket, key)?;
        let full_path = self.data_dir.join(&storage_path);
        let file = tokio::fs::File::open(&full_path).await?;
        let stream = ReaderStream::new(file);
        Ok((meta, stream))
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    // ── Versioning helpers ──────────────────────────────────────────────

    pub fn list_versions(&self, bucket: &str, key: &str) -> Result<Vec<ObjectVersion>, StorageError> {
        self.db.list_versions(bucket, key)
    }

    pub async fn get_version_data(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectVersion, bytes::Bytes), StorageError> {
        let (version, storage_path) = self.db.get_version(bucket, key, version_id)?;
        let full_path = self.data_dir.join(&storage_path);
        let data = tokio::fs::read(&full_path).await?;
        Ok((version, bytes::Bytes::from(data)))
    }

    pub fn enable_versioning(&self, bucket: &str) -> Result<(), StorageError> {
        self.db.enable_versioning(bucket)
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    pub fn run_lifecycle(&self) -> Result<LifecycleResult, StorageError> {
        let expired = self.db.find_expired_objects()?;
        let mut objects_expired: u64 = 0;
        let mut bytes_freed: u64 = 0;

        for (bucket, key, storage_path) in &expired {
            // Get size before deleting
            if let Ok((meta, _)) = self.db.get_object(bucket, key) {
                bytes_freed += meta.size;
            }
            // Delete from database
            let _ = self.db.delete_object(bucket, key);
            // Delete from disk if dedup is off
            if !self.deduplication {
                let full_path = self.data_dir.join(storage_path);
                let _ = std::fs::remove_file(&full_path);
            }
            objects_expired += 1;
        }

        Ok(LifecycleResult {
            objects_expired,
            bytes_freed,
        })
    }
}
