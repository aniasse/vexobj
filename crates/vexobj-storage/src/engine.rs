use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::db::Database;
use crate::encryption::Encryptor;
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
    encryptor: Option<Arc<Encryptor>>,
    /// Pluggable blob backend. `data_dir` still owns local-only state
    /// (SQLite, scratch, backups); blob bytes flow through the trait.
    blob_store: Arc<dyn crate::blob_store::BlobStore>,
    /// What ffmpeg-backed features the host can do (detected once at
    /// startup). Used to branch probe / thumbnail decisions.
    video_features: vexobj_processing::VideoFeatures,
}

impl StorageEngine {
    pub fn new(
        data_dir: PathBuf,
        max_file_size: u64,
        deduplication: bool,
    ) -> Result<Self, StorageError> {
        Self::with_encryption(data_dir, max_file_size, deduplication, None)
    }

    pub fn with_encryption(
        data_dir: PathBuf,
        max_file_size: u64,
        deduplication: bool,
        encryptor: Option<Arc<Encryptor>>,
    ) -> Result<Self, StorageError> {
        // Default to local blob storage — what VaultFS has always done.
        let blob_store = Arc::new(crate::LocalBlobStore::new(data_dir.clone()));
        Self::with_backend(data_dir, max_file_size, deduplication, encryptor, blob_store)
    }

    /// Constructor for callers that want to choose the blob backend
    /// (local disk, S3, R2, etc.). `data_dir` is still required —
    /// SQLite metadata, replication scratch, and backups live there
    /// regardless of where blobs go.
    pub fn with_backend(
        data_dir: PathBuf,
        max_file_size: u64,
        deduplication: bool,
        encryptor: Option<Arc<Encryptor>>,
        blob_store: Arc<dyn crate::blob_store::BlobStore>,
    ) -> Result<Self, StorageError> {
        let db_path = data_dir.join("vaultfs.db");
        std::fs::create_dir_all(&data_dir)?;
        // Only pre-create the blobs directory when the backend is
        // local — remote backends manage their own prefix layout.
        if blob_store.supports_local_path() {
            std::fs::create_dir_all(data_dir.join("blobs"))?;
        }

        let db = Database::open(&db_path)?;

        Ok(Self {
            db,
            data_dir,
            max_file_size,
            deduplication,
            encryptor,
            blob_store,
            video_features: vexobj_processing::VideoFeatures::detect(),
        })
    }

    pub fn blob_store(&self) -> &Arc<dyn crate::blob_store::BlobStore> {
        &self.blob_store
    }

    pub fn video_features(&self) -> &vexobj_processing::VideoFeatures {
        &self.video_features
    }

    pub fn encryption_enabled(&self) -> bool {
        self.encryptor.is_some()
    }

    /// Merge extracted video metadata into a user-provided metadata blob,
    /// when the content type looks probeable and the file parses. Returns
    /// the (possibly-enriched) metadata value ready to hand to the DB.
    ///
    /// Probing is best-effort: I/O or parse errors are swallowed so that
    /// unusual containers never fail an upload — the object just lacks
    /// video metadata. SSE-encrypted files can't be probed in place (the
    /// bytes on disk are ciphertext), so we probe from the plaintext
    /// buffer when one is available, or the decrypted stream in memory
    /// via `probe_bytes`.
    fn enrich_with_video_meta(
        &self,
        content_type: &str,
        storage_path: Option<&std::path::Path>,
        plaintext: Option<&[u8]>,
        user_meta: Option<serde_json::Value>,
    ) -> serde_json::Value {
        let base = user_meta.unwrap_or(serde_json::Value::Object(Default::default()));
        // `video/*` is the broad net; ffprobe covers way more container
        // types than the pure-Rust mp4 parser, so we try it on anything
        // that *claims* to be video rather than only MP4-family MIMEs.
        if !content_type.starts_with("video/") { return base; }

        let ffprobe_available = self.video_features.ffprobe;
        let is_mp4_family = vexobj_processing::is_probable_video(content_type);
        // Priority order:
        // 1. ffprobe on the local file (best coverage, needs a path)
        // 2. Pure-Rust mp4 parser on the local file (MP4/MOV only)
        // 3. Pure-Rust mp4 parser on the plaintext buffer (SSE path,
        //    or remote backend with bytes still in memory)
        let meta = if let Some(path) = storage_path {
            if ffprobe_available && self.encryptor.is_none() {
                vexobj_processing::probe_with_ffprobe(path)
                    .or_else(|| if is_mp4_family { vexobj_processing::probe_video_file(path) } else { None })
            } else if self.encryptor.is_some() {
                plaintext.and_then(vexobj_processing::probe_video_bytes)
            } else {
                vexobj_processing::probe_video_file(path)
            }
        } else {
            // Remote backend: no local file. Best we can do is the
            // pure-Rust mp4 parser against the in-memory buffer.
            plaintext.and_then(vexobj_processing::probe_video_bytes)
        };
        let Some(meta) = meta else { return base };

        let mut obj = match base {
            serde_json::Value::Object(m) => m,
            _ => serde_json::Map::new(),
        };
        obj.insert(
            "video".to_string(),
            serde_json::to_value(&meta).unwrap_or(serde_json::Value::Null),
        );
        serde_json::Value::Object(obj)
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

        // Deduplication: skip write if the blob already exists at the
        // canonical key. Both local and S3 backends dedup identically
        // via content-addressed paths.
        if self.deduplication {
            if let Some(existing) = self.db.find_by_hash(&sha256)? {
                if self.blob_store.exists_blob(&existing).await? {
                    let content_type = content_type
                        .map(String::from)
                        .unwrap_or_else(|| Self::guess_content_type(key));

                    // Enrichment wants a local path for ffprobe; with a
                    // remote backend we hand over the in-memory buffer
                    // instead so the probe still runs.
                    let existing_local = self.blob_store.local_path(&existing);
                    let enriched = self.enrich_with_video_meta(
                        &content_type,
                        existing_local.as_deref(),
                        Some(&data),
                        metadata,
                    );
                    let meta = self.db.put_object(
                        bucket,
                        key,
                        size,
                        &content_type,
                        &sha256,
                        &existing,
                        &enriched,
                    )?;

                    info!(bucket, key, size, deduplicated = true, "object stored");
                    return Ok(meta);
                }
            }
        }

        // Write blob via the backend. With SSE on, we still encrypt
        // in-process before handing the bytes over — the backend
        // never sees plaintext.
        let bytes_on_disk = match &self.encryptor {
            Some(enc) => enc.encrypt(&sha256, &data)?,
            None => data.to_vec(),
        };
        self.blob_store.put_blob(&storage_path, &bytes_on_disk).await?;

        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| Self::guess_content_type(key));

        let local_path = self.blob_store.local_path(&storage_path);
        let enriched = self.enrich_with_video_meta(
            &content_type,
            local_path.as_deref(),
            Some(&data),
            metadata,
        );
        let meta = self.db.put_object(
            bucket,
            key,
            size,
            &content_type,
            &sha256,
            &storage_path,
            &enriched,
        )?;

        info!(bucket, key, size, "object stored");

        // Replication event: one row per put. Appended *after* the
        // metadata write succeeded so replicas never see a ghost event.
        let _ = self.db.append_replication_event(
            "put", bucket, key, &sha256, None, size, &content_type,
        );

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
            let _ = self.db.append_replication_event(
                "version_put",
                bucket,
                key,
                &sha256,
                Some(&version_id),
                size,
                &content_type,
            );
        }

        Ok(meta)
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMeta, bytes::Bytes), StorageError> {
        let (meta, storage_path) = self.db.get_object(bucket, key)?;
        let raw = self.blob_store.get_blob(&storage_path).await?;
        let data = match &self.encryptor {
            Some(enc) => enc.decrypt(&meta.sha256, &raw)?,
            None => raw,
        };
        Ok((meta, bytes::Bytes::from(data)))
    }

    pub fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectMeta, StorageError> {
        let (meta, _) = self.db.get_object(bucket, key)?;
        Ok(meta)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        // Object-lock gate: refuse the delete if retention or legal hold is active.
        if let Ok(lock) = self.db.get_lock(bucket, key) {
            if lock.is_active(chrono::Utc::now()) {
                return Err(StorageError::ObjectLocked {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    reason: if lock.legal_hold {
                        "legal hold is in effect".into()
                    } else {
                        "retention period has not elapsed".into()
                    },
                });
            }
        }

        // If versioning is enabled, create a delete marker instead of hard-deleting
        if self.db.is_versioning_enabled(bucket) {
            let version_id = uuid::Uuid::new_v4().to_string();
            self.db.save_delete_marker(bucket, key, &version_id)?;
            let _ = self.db.append_replication_event(
                "delete_marker",
                bucket,
                key,
                "",
                Some(&version_id),
                0,
                "",
            );
        }

        let storage_path = self.db.delete_object(bucket, key)?;
        let _ = self
            .db
            .append_replication_event("delete", bucket, key, "", None, 0, "");
        // With dedup off, the blob is used by at most one object, so
        // deletion is safe. With dedup on, another row may still
        // reference the same content-addressed blob — leave it.
        if !self.deduplication {
            let _ = self.blob_store.delete_blob(&storage_path).await;
        }
        info!(bucket, key, "object deleted");
        Ok(())
    }

    pub fn list_objects(&self, req: &ListObjectsRequest) -> Result<ListObjectsResponse, StorageError> {
        self.db.list_objects(req)
    }

    /// Local filesystem path for the object's blob, when the active
    /// backend has one. Remote backends return None and callers that
    /// absolutely need a file (ffmpeg, SSE in-place ops) should
    /// either download to scratch or error out clearly.
    pub fn object_data_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StorageError> {
        let (_, storage_path) = self.db.get_object(bucket, key)?;
        self.blob_store
            .local_path(&storage_path)
            .ok_or_else(|| StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "active blob backend has no local path",
            )))
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
                if self.blob_store.exists_blob(&existing).await? {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    let content_type = content_type
                        .map(String::from)
                        .unwrap_or_else(|| Self::guess_content_type(key));
                    let existing_local = self.blob_store.local_path(&existing);
                    let enriched = self.enrich_with_video_meta(
                        &content_type,
                        existing_local.as_deref(),
                        None,
                        metadata,
                    );
                    let meta = self.db.put_object(
                        bucket, key, size, &content_type, &sha256, &existing,
                        &enriched,
                    )?;
                    info!(bucket, key, size, deduplicated = true, "object stored (stream)");
                    return Ok(meta);
                }
            }
        }

        // Finalize the blob through the backend. With SSE, we read the
        // plaintext temp file, encrypt in memory, then PUT. Without SSE,
        // we hand the temp file to `put_blob_from_file` which does a
        // rename on local and an upload on S3. Either way the temp is
        // consumed on success.
        if let Some(enc) = &self.encryptor {
            let plaintext = tokio::fs::read(&temp_path).await?;
            let ciphertext = enc.encrypt(&sha256, &plaintext)?;
            self.blob_store.put_blob(&storage_path, &ciphertext).await?;
            let _ = tokio::fs::remove_file(&temp_path).await;
        } else {
            self.blob_store
                .put_blob_from_file(&storage_path, &temp_path)
                .await?;
        }

        let content_type = content_type
            .map(String::from)
            .unwrap_or_else(|| Self::guess_content_type(key));

        let final_local = self.blob_store.local_path(&storage_path);
        let enriched = self.enrich_with_video_meta(
            &content_type,
            final_local.as_deref(),
            None,
            metadata,
        );
        let meta = self.db.put_object(
            bucket, key, size, &content_type, &sha256, &storage_path,
            &enriched,
        )?;

        info!(bucket, key, size, "object stored (stream)");

        let _ = self.db.append_replication_event(
            "put", bucket, key, &sha256, None, size, &content_type,
        );

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
            let _ = self.db.append_replication_event(
                "version_put",
                bucket,
                key,
                &sha256,
                Some(&version_id),
                size,
                &content_type,
            );
        }

        Ok(meta)
    }

    /// Stream-download: returns a stream of file chunks instead of loading
    /// the entire file into RAM. With SSE enabled we have to load the blob
    /// into memory to verify the auth tag before handing bytes to the client
    /// — the stream then yields the decrypted plaintext as one chunk.
    pub async fn get_object_stream(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<
        (
            ObjectMeta,
            futures::stream::BoxStream<'static, std::io::Result<bytes::Bytes>>,
        ),
        StorageError,
    > {
        let (meta, storage_path) = self.db.get_object(bucket, key)?;

        // SSE forces us to materialize the full blob to decrypt (the
        // GCM auth tag sits at the end). Anything else rides the
        // backend's native streaming.
        if let Some(enc) = &self.encryptor {
            let raw = self.blob_store.get_blob(&storage_path).await?;
            let plaintext = enc.decrypt(&meta.sha256, &raw)?;
            let stream = futures::stream::once(async move {
                Ok::<_, std::io::Error>(bytes::Bytes::from(plaintext))
            });
            return Ok((meta, Box::pin(stream)));
        }

        let stream = self.blob_store.stream_blob(&storage_path).await?;
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
        let raw = self.blob_store.get_blob(&storage_path).await?;
        let data = match &self.encryptor {
            Some(enc) => enc.decrypt(&version.sha256, &raw)?,
            None => raw,
        };
        Ok((version, bytes::Bytes::from(data)))
    }

    pub fn enable_versioning(&self, bucket: &str) -> Result<(), StorageError> {
        self.db.enable_versioning(bucket)
    }

    // ── Object lock ─────────────────────────────────────────────────────

    pub fn get_lock(&self, bucket: &str, key: &str) -> Result<ObjectLock, StorageError> {
        self.db.get_lock(bucket, key)
    }

    pub fn set_lock(
        &self,
        bucket: &str,
        key: &str,
        retain_until: Option<chrono::DateTime<chrono::Utc>>,
        legal_hold: bool,
    ) -> Result<ObjectLock, StorageError> {
        self.db.set_lock(bucket, key, retain_until, legal_hold)
    }

    pub fn clear_legal_hold(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        self.db.clear_legal_hold(bucket, key)
    }

    /// Delete a specific version of an object. If the version is a delete-marker,
    /// the row is just removed. Otherwise, the row is deleted and the blob is
    /// removed when no other object or version still references it.
    pub async fn delete_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), StorageError> {
        let (version, storage_path) = self.db.get_version(bucket, key, version_id)?;
        self.db.delete_version(bucket, key, version_id)?;

        if version.is_delete_marker || storage_path.is_empty() {
            return Ok(());
        }
        if self.deduplication {
            return Ok(());
        }
        if !self.db.is_storage_path_referenced(&storage_path)? {
            let _ = self.blob_store.delete_blob(&storage_path).await;
        }
        Ok(())
    }

    /// Hard-delete every version and delete-marker for a key, including the
    /// live object. Removes any blob that becomes orphaned (when dedup is off).
    pub async fn purge_versions(&self, bucket: &str, key: &str) -> Result<u64, StorageError> {
        // Object-lock gate — purge is strictly more destructive than delete,
        // so it must also refuse when the live object is locked.
        if let Ok(lock) = self.db.get_lock(bucket, key) {
            if lock.is_active(chrono::Utc::now()) {
                return Err(StorageError::ObjectLocked {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    reason: "purge blocked by active lock".into(),
                });
            }
        }

        let live_path = self.db.get_object(bucket, key).ok().map(|(_, p)| p);
        if live_path.is_some() {
            let _ = self.db.delete_object(bucket, key);
        }
        let paths = self.db.purge_versions(bucket, key)?;
        let mut all_paths: Vec<String> = paths;
        if let Some(p) = live_path {
            all_paths.push(p);
        }

        let mut removed: u64 = 0;
        if !self.deduplication {
            for path in all_paths.iter().collect::<std::collections::HashSet<_>>() {
                if !self.db.is_storage_path_referenced(path)? {
                    if self.blob_store.delete_blob(path).await.is_ok() {
                        removed += 1;
                    }
                }
            }
        }
        Ok(removed)
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    pub async fn run_lifecycle(&self) -> Result<LifecycleResult, StorageError> {
        let expired = self.db.find_expired_objects()?;
        let mut objects_expired: u64 = 0;
        let mut bytes_freed: u64 = 0;

        for (bucket, key, storage_path) in &expired {
            if let Ok((meta, _)) = self.db.get_object(bucket, key) {
                bytes_freed += meta.size;
            }
            let _ = self.db.delete_object(bucket, key);
            if !self.deduplication {
                // Blob cleanup goes through the backend so S3 /
                // remote stores stay in sync.
                let _ = self.blob_store.delete_blob(storage_path).await;
            }
            objects_expired += 1;
        }

        Ok(LifecycleResult {
            objects_expired,
            bytes_freed,
        })
    }
}
