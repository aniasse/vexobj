use sha2::{Digest, Sha256};
use std::path::PathBuf;
use tracing::info;

use crate::db::Database;
use crate::error::StorageError;
use crate::models::*;

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

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
}
