use std::path::{Path, PathBuf};
use tracing::info;

use crate::db::Database;
use crate::error::StorageError;

/// Backup and restore operations for vexobj.
pub struct BackupManager {
    data_dir: PathBuf,
}

#[derive(Debug)]
pub struct BackupResult {
    pub path: PathBuf,
    pub db_size: u64,
    pub blobs_copied: u64,
    pub total_size: u64,
}

#[derive(Debug)]
pub struct RestoreResult {
    pub db_restored: bool,
    pub blobs_restored: u64,
}

impl BackupManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Create a full backup snapshot to the given directory.
    /// Copies the SQLite database (using VACUUM INTO for consistency) and all blobs.
    pub fn create_snapshot(&self, db: &Database, dest_dir: &Path) -> Result<BackupResult, StorageError> {
        std::fs::create_dir_all(dest_dir)?;

        // Backup SQLite database atomically
        let db_dest = dest_dir.join("vexobj.db");
        db.backup_to(&db_dest)?;
        let db_size = std::fs::metadata(&db_dest)
            .map(|m| m.len())
            .unwrap_or(0);

        // Copy blobs directory
        let blobs_src = self.data_dir.join("blobs");
        let blobs_dest = dest_dir.join("blobs");
        let mut blobs_copied = 0u64;
        let mut total_size = db_size;

        if blobs_src.exists() {
            blobs_copied = copy_dir_recursive(&blobs_src, &blobs_dest)?;
            total_size += dir_size(&blobs_dest)?;
        }

        // Copy auth database
        let auth_src = self.data_dir.join("auth.db");
        if auth_src.exists() {
            let auth_dest = dest_dir.join("auth.db");
            std::fs::copy(&auth_src, &auth_dest)?;
            total_size += std::fs::metadata(&auth_dest).map(|m| m.len()).unwrap_or(0);
        }

        // Copy presign secret
        let secret_src = self.data_dir.join(".presign_secret");
        if secret_src.exists() {
            std::fs::copy(&secret_src, dest_dir.join(".presign_secret"))?;
        }

        let result = BackupResult {
            path: dest_dir.to_path_buf(),
            db_size,
            blobs_copied,
            total_size,
        };

        info!(
            path = %dest_dir.display(),
            blobs = blobs_copied,
            total_size,
            "backup snapshot created"
        );

        Ok(result)
    }

    /// Restore from a backup snapshot.
    /// Warning: this will overwrite current data.
    pub fn restore_snapshot(&self, snapshot_dir: &Path) -> Result<RestoreResult, StorageError> {
        let db_src = snapshot_dir.join("vexobj.db");
        if !db_src.exists() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "backup snapshot missing vexobj.db",
            )));
        }

        std::fs::create_dir_all(&self.data_dir)?;

        // Restore database
        let db_dest = self.data_dir.join("vexobj.db");
        std::fs::copy(&db_src, &db_dest)?;

        // Restore auth database
        let auth_src = snapshot_dir.join("auth.db");
        if auth_src.exists() {
            std::fs::copy(&auth_src, self.data_dir.join("auth.db"))?;
        }

        // Restore presign secret
        let secret_src = snapshot_dir.join(".presign_secret");
        if secret_src.exists() {
            std::fs::copy(&secret_src, self.data_dir.join(".presign_secret"))?;
        }

        // Restore blobs
        let blobs_src = snapshot_dir.join("blobs");
        let blobs_dest = self.data_dir.join("blobs");
        let mut blobs_restored = 0u64;

        if blobs_src.exists() {
            blobs_restored = copy_dir_recursive(&blobs_src, &blobs_dest)?;
        }

        let result = RestoreResult {
            db_restored: true,
            blobs_restored,
        };

        info!(
            source = %snapshot_dir.display(),
            blobs = blobs_restored,
            "backup restored"
        );

        Ok(result)
    }

    /// Export a single bucket to a directory (bucket-level backup).
    pub fn export_bucket(
        &self,
        db: &Database,
        bucket: &str,
        dest_dir: &Path,
    ) -> Result<u64, StorageError> {
        use crate::models::ListObjectsRequest;

        std::fs::create_dir_all(dest_dir)?;

        let objects = db.list_objects(&ListObjectsRequest {
            bucket: bucket.to_string(),
            prefix: None,
            delimiter: None,
            max_keys: Some(1000000),
            continuation_token: None,
        })?;

        let mut count = 0u64;
        let meta_file = dest_dir.join("_manifest.json");
        let mut manifest = Vec::new();

        for obj in &objects.objects {
            // Get storage path and copy blob
            if let Ok((_, storage_path)) = db.get_object(bucket, &obj.key) {
                let blob_src = self.data_dir.join(&storage_path);
                if blob_src.exists() {
                    let blob_dest = dest_dir.join(&obj.key);
                    if let Some(parent) = blob_dest.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::copy(&blob_src, &blob_dest)?;
                    count += 1;
                }
            }

            manifest.push(serde_json::json!({
                "key": obj.key,
                "size": obj.size,
                "content_type": obj.content_type,
                "sha256": obj.sha256,
            }));
        }

        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        std::fs::write(&meta_file, manifest_json)?;

        info!(bucket, objects = count, "bucket exported");
        Ok(count)
    }
}

fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<u64, StorageError> {
    std::fs::create_dir_all(dest)?;
    let mut count = 0u64;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest_path = dest.join(entry.file_name());

        if file_type.is_dir() {
            count += copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            std::fs::copy(entry.path(), &dest_path)?;
            count += 1;
        }
    }

    Ok(count)
}

fn dir_size(path: &Path) -> Result<u64, StorageError> {
    let mut total = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                total += dir_size(&entry.path())?;
            } else {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
}
