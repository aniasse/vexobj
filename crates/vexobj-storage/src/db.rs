use chrono::Utc;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

use crate::error::StorageError;
use crate::models::*;

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use chrono::Duration as Cd;

    fn tempdb() -> Database {
        let p = std::env::temp_dir().join(format!("vfs-db-{}.sqlite", uuid::Uuid::new_v4()));
        Database::open(&p).unwrap()
    }

    #[test]
    fn gc_only_trims_terminal_jobs_past_the_cutoff() {
        let db = tempdb();
        db.create_bucket(&CreateBucketRequest {
            name: "b".into(),
            public: false,
        })
        .unwrap();

        // 4 jobs: pending, running, completed recent, completed old.
        // Only the "completed old" row should go.
        let _pending = db
            .create_transcode_job("b", "k1", "sha1", "mp4-480p", None)
            .unwrap();
        let _running = db
            .create_transcode_job("b", "k2", "sha2", "mp4-480p", None)
            .unwrap();
        let recent = db
            .create_transcode_job("b", "k3", "sha3", "mp4-480p", None)
            .unwrap();
        let old = db
            .create_transcode_job("b", "k4", "sha4", "mp4-480p", None)
            .unwrap();

        // Mark two as completed. The recent one stamps as "now",
        // the old one we nudge by poking the row directly.
        db.complete_transcode_job(&recent.id, "b", "k3.out", 100, 1)
            .unwrap();
        db.complete_transcode_job(&old.id, "b", "k4.out", 100, 1)
            .unwrap();
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE transcode_jobs SET completed_at = ?1 WHERE id = ?2",
                params![(Utc::now() - Cd::days(60)).to_rfc3339(), old.id],
            )
            .unwrap();
        }

        let removed = db.gc_transcode_jobs(Utc::now() - Cd::days(30)).unwrap();
        assert_eq!(removed, 1, "only the old-completed job should be GC'd");

        // Still 3 rows remain, and none of them is `old`.
        let all = db.list_transcode_jobs(None, 100).unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|j| j.id != old.id));
    }
}

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS buckets (
                id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL,
                public INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS objects (
                id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL REFERENCES buckets(name),
                key TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                metadata TEXT NOT NULL DEFAULT '{}',
                UNIQUE(bucket, key)
            );

            CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key);
            CREATE INDEX IF NOT EXISTS idx_objects_bucket_prefix ON objects(bucket, key);
            CREATE INDEX IF NOT EXISTS idx_objects_sha256 ON objects(sha256);

            CREATE TABLE IF NOT EXISTS object_versions (
                id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                version_id TEXT NOT NULL,
                size INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_latest INTEGER NOT NULL DEFAULT 1,
                is_delete_marker INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_versions_bucket_key ON object_versions(bucket, key, created_at DESC);

            CREATE TABLE IF NOT EXISTS lifecycle_rules (
                id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                prefix TEXT NOT NULL DEFAULT '',
                expire_days INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS replication_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                op TEXT NOT NULL,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                sha256 TEXT NOT NULL DEFAULT '',
                version_id TEXT,
                timestamp TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_replication_events_id
                ON replication_events(id);

            CREATE TABLE IF NOT EXISTS transcode_jobs (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                profile TEXT NOT NULL,
                output_bucket TEXT,
                output_key TEXT,
                output_size INTEGER,
                error TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                duration_ms INTEGER,
                requested_by TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_transcode_jobs_status
                ON transcode_jobs(status, created_at);
            CREATE INDEX IF NOT EXISTS idx_transcode_jobs_source
                ON transcode_jobs(bucket, key, profile);

            -- S3 multipart upload state. `multipart_uploads` is the parent
            -- record created by InitiateMultipartUpload; `multipart_parts`
            -- holds one row per UploadPart. On Complete or Abort both get
            -- dropped (ON DELETE CASCADE) and the scratch files are wiped.
            CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id TEXT PRIMARY KEY,
                bucket TEXT NOT NULL,
                key TEXT NOT NULL,
                content_type TEXT,
                initiated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_multipart_bucket_key
                ON multipart_uploads(bucket, key);

            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
                part_number INTEGER NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                PRIMARY KEY (upload_id, part_number)
            );
            ",
        )?;

        // Replication needs enough info in the log to rebuild an objects
        // row on the replica. Size + content_type are carried alongside
        // sha256 so `apply` does not have to round-trip to the primary
        // for every event.
        let _ = conn.execute_batch(
            "ALTER TABLE replication_events ADD COLUMN size INTEGER NOT NULL DEFAULT 0;
             ALTER TABLE replication_events ADD COLUMN content_type TEXT NOT NULL DEFAULT '';",
        );

        // Add versioning_enabled column if it doesn't exist (ALTER TABLE will fail if it already exists)
        let _ = conn.execute_batch(
            "ALTER TABLE buckets ADD COLUMN versioning_enabled INTEGER NOT NULL DEFAULT 0;",
        );

        // Object-lock columns — retain_until is an ISO-8601 timestamp; NULL means no retention.
        // legal_hold is 0/1. Lives on the `objects` row for the live object.
        let _ = conn.execute_batch(
            "ALTER TABLE objects ADD COLUMN retain_until TEXT;
             ALTER TABLE objects ADD COLUMN legal_hold INTEGER NOT NULL DEFAULT 0;",
        );

        Ok(())
    }

    pub fn create_bucket(&self, req: &CreateBucketRequest) -> Result<Bucket, StorageError> {
        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        conn.execute(
            "INSERT INTO buckets (id, name, created_at, public) VALUES (?1, ?2, ?3, ?4)",
            params![id, req.name, now.to_rfc3339(), req.public as i32],
        )
        .map_err(|e| match e {
            rusqlite::Error::SqliteFailure(err, _)
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                StorageError::BucketAlreadyExists(req.name.clone())
            }
            other => StorageError::Database(other),
        })?;

        Ok(Bucket {
            id,
            name: req.name.clone(),
            created_at: now,
            public: req.public,
        })
    }

    pub fn get_bucket(&self, name: &str) -> Result<Bucket, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, name, created_at, public FROM buckets WHERE name = ?1",
            params![name],
            |row| {
                Ok(Bucket {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row
                        .get::<_, String>(2)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    public: row.get::<_, i32>(3)? != 0,
                })
            },
        )
        .map_err(|_| StorageError::BucketNotFound(name.to_string()))
    }

    pub fn list_buckets(&self) -> Result<Vec<Bucket>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT id, name, created_at, public FROM buckets ORDER BY name")?;
        let buckets = stmt
            .query_map([], |row| {
                Ok(Bucket {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    created_at: row
                        .get::<_, String>(2)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    public: row.get::<_, i32>(3)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(buckets)
    }

    pub fn delete_bucket(&self, name: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE bucket = ?1",
            params![name],
            |row| row.get(0),
        )?;
        if count > 0 {
            return Err(StorageError::BucketNotFound(format!(
                "{name} (bucket not empty)"
            )));
        }
        let affected = conn.execute("DELETE FROM buckets WHERE name = ?1", params![name])?;
        if affected == 0 {
            return Err(StorageError::BucketNotFound(name.to_string()));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        content_type: &str,
        sha256: &str,
        storage_path: &str,
        metadata: &serde_json::Value,
    ) -> Result<ObjectMeta, StorageError> {
        // Verify bucket exists
        self.get_bucket(bucket)?;

        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        conn.execute(
            "INSERT INTO objects (id, bucket, key, size, content_type, sha256, storage_path, created_at, updated_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(bucket, key) DO UPDATE SET
                size = excluded.size,
                content_type = excluded.content_type,
                sha256 = excluded.sha256,
                storage_path = excluded.storage_path,
                updated_at = excluded.updated_at,
                metadata = excluded.metadata",
            params![
                id,
                bucket,
                key,
                size as i64,
                content_type,
                sha256,
                storage_path,
                now.to_rfc3339(),
                now.to_rfc3339(),
                serde_json::to_string(metadata).unwrap(),
            ],
        )?;

        Ok(ObjectMeta {
            id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            size,
            content_type: content_type.to_string(),
            sha256: sha256.to_string(),
            created_at: now,
            updated_at: now,
            metadata: metadata.clone(),
        })
    }

    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectMeta, String), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, bucket, key, size, content_type, sha256, storage_path, created_at, updated_at, metadata
             FROM objects WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            |row| {
                let meta = ObjectMeta {
                    id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    size: row.get::<_, i64>(3)? as u64,
                    content_type: row.get(4)?,
                    sha256: row.get(5)?,
                    created_at: row.get::<_, String>(7)?.parse().unwrap_or_else(|_| Utc::now()),
                    updated_at: row.get::<_, String>(8)?.parse().unwrap_or_else(|_| Utc::now()),
                    metadata: serde_json::from_str(&row.get::<_, String>(9)?).unwrap_or_default(),
                };
                let storage_path: String = row.get(6)?;
                Ok((meta, storage_path))
            },
        )
        .map_err(|_| StorageError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<String, StorageError> {
        let (_, storage_path) = self.get_object(bucket, key)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )?;
        Ok(storage_path)
    }

    pub fn list_objects(
        &self,
        req: &ListObjectsRequest,
    ) -> Result<ListObjectsResponse, StorageError> {
        let conn = self.conn.lock().unwrap();
        let max_keys = req.max_keys.unwrap_or(1000).min(1000) as usize;
        let prefix = req.prefix.as_deref().unwrap_or("");

        let mut stmt = conn.prepare(
            "SELECT id, bucket, key, size, content_type, sha256, created_at, updated_at, metadata
             FROM objects WHERE bucket = ?1 AND key >= ?2 AND key LIKE ?3
             ORDER BY key LIMIT ?4",
        )?;

        let like_pattern = format!("{prefix}%");
        let start = req.continuation_token.as_deref().unwrap_or(prefix);

        let objects: Vec<ObjectMeta> = stmt
            .query_map(
                params![req.bucket, start, like_pattern, max_keys as i64 + 1],
                |row| {
                    Ok(ObjectMeta {
                        id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        size: row.get::<_, i64>(3)? as u64,
                        content_type: row.get(4)?,
                        sha256: row.get(5)?,
                        created_at: row
                            .get::<_, String>(6)?
                            .parse()
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: row
                            .get::<_, String>(7)?
                            .parse()
                            .unwrap_or_else(|_| Utc::now()),
                        metadata: serde_json::from_str(&row.get::<_, String>(8)?)
                            .unwrap_or_default(),
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;

        let is_truncated = objects.len() > max_keys;
        let objects: Vec<ObjectMeta> = objects.into_iter().take(max_keys).collect();
        let next_token = if is_truncated {
            objects.last().map(|o| o.key.clone())
        } else {
            None
        };

        // Handle delimiter for common prefixes (virtual directories)
        let mut common_prefixes = Vec::new();
        let mut filtered_objects = Vec::new();

        if let Some(delimiter) = &req.delimiter {
            let mut seen_prefixes = std::collections::HashSet::new();
            for obj in objects {
                let after_prefix = &obj.key[prefix.len()..];
                if let Some(pos) = after_prefix.find(delimiter.as_str()) {
                    let common = format!("{}{}", prefix, &after_prefix[..=pos]);
                    if seen_prefixes.insert(common.clone()) {
                        common_prefixes.push(common);
                    }
                } else {
                    filtered_objects.push(obj);
                }
            }
        } else {
            filtered_objects = objects;
        }

        Ok(ListObjectsResponse {
            objects: filtered_objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: next_token,
        })
    }

    pub fn all_storage_paths(&self) -> Result<Vec<String>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT DISTINCT storage_path FROM objects")?;
        let paths = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(paths)
    }

    /// Create an atomic backup of the database using VACUUM INTO.
    pub fn backup_to(&self, dest: &std::path::Path) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let dest_str = dest.to_string_lossy();
        conn.execute_batch(&format!("VACUUM INTO '{}'", dest_str.replace('\'', "''")))?;
        Ok(())
    }

    /// Returns (total_size_bytes, object_count) for a given bucket.
    pub fn bucket_storage_stats(&self, bucket: &str) -> Result<(u64, u64), StorageError> {
        let conn = self.conn.lock().unwrap();
        let (total_size, count) = conn.query_row(
            "SELECT COALESCE(SUM(size), 0), COUNT(*) FROM objects WHERE bucket = ?1",
            params![bucket],
            |row| {
                let size: i64 = row.get(0)?;
                let count: i64 = row.get(1)?;
                Ok((size as u64, count as u64))
            },
        )?;
        Ok((total_size, count))
    }

    pub fn find_by_hash(&self, sha256: &str) -> Result<Option<String>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT storage_path FROM objects WHERE sha256 = ?1 LIMIT 1",
            params![sha256],
            |row| row.get::<_, String>(0),
        );
        match result {
            Ok(path) => Ok(Some(path)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Database(e)),
        }
    }

    // ── Versioning ──────────────────────────────────────────────────────

    pub fn enable_versioning(&self, bucket: &str) -> Result<(), StorageError> {
        self.get_bucket(bucket)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE buckets SET versioning_enabled = 1 WHERE name = ?1",
            params![bucket],
        )?;
        Ok(())
    }

    pub fn is_versioning_enabled(&self, bucket: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT versioning_enabled FROM buckets WHERE name = ?1",
            params![bucket],
            |row| row.get::<_, i32>(0),
        )
        .map(|v| v != 0)
        .unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn save_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        size: u64,
        content_type: &str,
        sha256: &str,
        storage_path: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        // Mark all previous versions of this bucket+key as not latest
        conn.execute(
            "UPDATE object_versions SET is_latest = 0 WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO object_versions (id, bucket, key, version_id, size, content_type, sha256, storage_path, created_at, is_latest, is_delete_marker)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, 0)",
            params![
                id,
                bucket,
                key,
                version_id,
                size as i64,
                content_type,
                sha256,
                storage_path,
                now.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn save_delete_marker(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        // Mark all previous versions as not latest
        conn.execute(
            "UPDATE object_versions SET is_latest = 0 WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO object_versions (id, bucket, key, version_id, size, content_type, sha256, storage_path, created_at, is_latest, is_delete_marker)
             VALUES (?1, ?2, ?3, ?4, 0, '', '', '', ?5, 1, 1)",
            params![id, bucket, key, version_id, now.to_rfc3339()],
        )?;
        Ok(())
    }

    pub fn list_versions(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<ObjectVersion>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, bucket, key, version_id, size, content_type, sha256, created_at, is_latest, is_delete_marker
             FROM object_versions WHERE bucket = ?1 AND key = ?2 ORDER BY created_at DESC",
        )?;
        let versions = stmt
            .query_map(params![bucket, key], |row| {
                Ok(ObjectVersion {
                    id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    version_id: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    content_type: row.get(5)?,
                    sha256: row.get(6)?,
                    created_at: row
                        .get::<_, String>(7)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    is_latest: row.get::<_, i32>(8)? != 0,
                    is_delete_marker: row.get::<_, i32>(9)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(versions)
    }

    pub fn get_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectVersion, String), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, bucket, key, version_id, size, content_type, sha256, storage_path, created_at, is_latest, is_delete_marker
             FROM object_versions WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id],
            |row| {
                let version = ObjectVersion {
                    id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    version_id: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    content_type: row.get(5)?,
                    sha256: row.get(6)?,
                    created_at: row
                        .get::<_, String>(8)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    is_latest: row.get::<_, i32>(9)? != 0,
                    is_delete_marker: row.get::<_, i32>(10)? != 0,
                };
                let storage_path: String = row.get(7)?;
                Ok((version, storage_path))
            },
        )
        .map_err(|_| StorageError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    pub fn delete_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let was_latest: i32 = conn
            .query_row(
                "SELECT is_latest FROM object_versions WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id],
                |row| row.get(0),
            )
            .map_err(|_| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        let affected = conn.execute(
            "DELETE FROM object_versions WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id],
        )?;
        if affected == 0 {
            return Err(StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        // If we removed the latest version, promote the newest remaining one.
        if was_latest != 0 {
            conn.execute(
                "UPDATE object_versions SET is_latest = 1
                 WHERE id = (
                     SELECT id FROM object_versions
                     WHERE bucket = ?1 AND key = ?2
                     ORDER BY created_at DESC LIMIT 1
                 )",
                params![bucket, key],
            )?;
        }
        Ok(())
    }

    /// Hard-delete every version and delete-marker for a key.
    /// Returns the distinct storage_paths that were referenced by those rows
    /// (caller decides whether to remove the blobs).
    pub fn purge_versions(&self, bucket: &str, key: &str) -> Result<Vec<String>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT storage_path FROM object_versions
             WHERE bucket = ?1 AND key = ?2 AND storage_path != ''",
        )?;
        let paths: Vec<String> = stmt
            .query_map(params![bucket, key], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);
        conn.execute(
            "DELETE FROM object_versions WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )?;
        Ok(paths)
    }

    // ── Replication event log ───────────────────────────────────────────

    /// Append a single event to the replication log. Called by the engine
    /// after every state-changing write so primaries and replicas stay
    /// in the same order. `size` and `content_type` are 0 / "" for events
    /// that don't carry a blob (delete, delete_marker).
    #[allow(clippy::too_many_arguments)]
    pub fn append_replication_event(
        &self,
        op: &str,
        bucket: &str,
        key: &str,
        sha256: &str,
        version_id: Option<&str>,
        size: u64,
        content_type: &str,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO replication_events
               (op, bucket, key, sha256, version_id, timestamp, size, content_type)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                op,
                bucket,
                key,
                sha256,
                version_id,
                Utc::now().to_rfc3339(),
                size as i64,
                content_type,
            ],
        )?;
        Ok(())
    }

    /// Return events with `id > since`, ordered by id ascending. `limit`
    /// caps the response size so replicas can paginate forever-long logs.
    pub fn list_replication_events(
        &self,
        since: i64,
        limit: u32,
    ) -> Result<Vec<ReplicationEvent>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let capped = limit.min(1000) as i64;
        let mut stmt = conn.prepare(
            "SELECT id, op, bucket, key, sha256, version_id, timestamp, size, content_type
             FROM replication_events
             WHERE id > ?1
             ORDER BY id ASC
             LIMIT ?2",
        )?;
        let events = stmt
            .query_map(params![since, capped], |row| {
                Ok(ReplicationEvent {
                    id: row.get(0)?,
                    op: row.get(1)?,
                    bucket: row.get(2)?,
                    key: row.get(3)?,
                    sha256: row.get(4)?,
                    version_id: row.get::<_, Option<String>>(5)?,
                    timestamp: row
                        .get::<_, String>(6)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    size: row.get::<_, i64>(7)? as u64,
                    content_type: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Newest event id — used by replicas/ops to see how far ahead the
    /// primary has moved without pulling every row.
    pub fn latest_replication_event_id(&self) -> Result<i64, StorageError> {
        let conn = self.conn.lock().unwrap();
        let id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM replication_events",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(id)
    }

    // ── Transcode jobs ──────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub fn create_transcode_job(
        &self,
        bucket: &str,
        key: &str,
        source_sha256: &str,
        profile: &str,
        requested_by: Option<&str>,
    ) -> Result<TranscodeJob, StorageError> {
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO transcode_jobs
               (id, status, bucket, key, source_sha256, profile, created_at, requested_by)
             VALUES (?1, 'pending', ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                bucket,
                key,
                source_sha256,
                profile,
                now.to_rfc3339(),
                requested_by,
            ],
        )?;
        Ok(TranscodeJob {
            id,
            status: "pending".to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            source_sha256: source_sha256.to_string(),
            profile: profile.to_string(),
            output_bucket: None,
            output_key: None,
            output_size: None,
            error: None,
            created_at: now,
            started_at: None,
            completed_at: None,
            duration_ms: None,
            requested_by: requested_by.map(String::from),
        })
    }

    pub fn get_transcode_job(&self, id: &str) -> Result<TranscodeJob, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, status, bucket, key, source_sha256, profile,
                    output_bucket, output_key, output_size, error,
                    created_at, started_at, completed_at, duration_ms, requested_by
             FROM transcode_jobs WHERE id = ?1",
            params![id],
            Self::read_transcode_row,
        )
        .map_err(|_| StorageError::ObjectNotFound {
            bucket: "transcode_jobs".to_string(),
            key: id.to_string(),
        })
    }

    pub fn list_transcode_jobs(
        &self,
        status: Option<&str>,
        limit: u32,
    ) -> Result<Vec<TranscodeJob>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let capped = limit.min(500) as i64;
        if let Some(s) = status {
            let mut stmt = conn.prepare(
                "SELECT id, status, bucket, key, source_sha256, profile,
                        output_bucket, output_key, output_size, error,
                        created_at, started_at, completed_at, duration_ms, requested_by
                 FROM transcode_jobs WHERE status = ?1
                 ORDER BY created_at DESC LIMIT ?2",
            )?;
            let rows = stmt
                .query_map(params![s, capped], Self::read_transcode_row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, status, bucket, key, source_sha256, profile,
                        output_bucket, output_key, output_size, error,
                        created_at, started_at, completed_at, duration_ms, requested_by
                 FROM transcode_jobs
                 ORDER BY created_at DESC LIMIT ?1",
            )?;
            let rows = stmt
                .query_map(params![capped], Self::read_transcode_row)?
                .collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        }
    }

    /// Atomically claim a pending job: marks the oldest one as
    /// `running` and returns it. Used by workers to poll the queue
    /// without races — UPDATE...WHERE status='pending' sees the row
    /// once and only once.
    pub fn claim_next_transcode_job(&self) -> Result<Option<TranscodeJob>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        // SQLite doesn't support UPDATE ... RETURNING on old versions,
        // so we do this in two steps inside a single transaction.
        let tx = conn.unchecked_transaction()?;
        let id_opt: Option<String> = tx
            .query_row(
                "SELECT id FROM transcode_jobs WHERE status = 'pending'
                 ORDER BY created_at ASC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok();
        let Some(id) = id_opt else {
            tx.commit()?;
            return Ok(None);
        };
        tx.execute(
            "UPDATE transcode_jobs SET status = 'running', started_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        let job = tx.query_row(
            "SELECT id, status, bucket, key, source_sha256, profile,
                    output_bucket, output_key, output_size, error,
                    created_at, started_at, completed_at, duration_ms, requested_by
             FROM transcode_jobs WHERE id = ?1",
            params![id],
            Self::read_transcode_row,
        )?;
        tx.commit()?;
        Ok(Some(job))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_transcode_job(
        &self,
        id: &str,
        output_bucket: &str,
        output_key: &str,
        output_size: u64,
        duration_ms: i64,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE transcode_jobs
             SET status = 'completed',
                 output_bucket = ?1, output_key = ?2, output_size = ?3,
                 completed_at = ?4, duration_ms = ?5
             WHERE id = ?6",
            params![
                output_bucket,
                output_key,
                output_size as i64,
                now,
                duration_ms,
                id
            ],
        )?;
        Ok(())
    }

    pub fn fail_transcode_job(
        &self,
        id: &str,
        error: &str,
        duration_ms: i64,
    ) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE transcode_jobs
             SET status = 'failed', error = ?1, completed_at = ?2, duration_ms = ?3
             WHERE id = ?4",
            params![error, now, duration_ms, id],
        )?;
        Ok(())
    }

    pub fn count_transcode_jobs_by_status(&self, status: &str) -> Result<u64, StorageError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM transcode_jobs WHERE status = ?1",
            params![status],
            |row| row.get(0),
        )?;
        Ok(count as u64)
    }

    /// Delete rows in terminal state (`completed` / `failed`) whose
    /// `completed_at` is older than `older_than`. Returns the number of
    /// rows removed. Used by the periodic GC task and the CLI.
    pub fn gc_transcode_jobs(
        &self,
        older_than: chrono::DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "DELETE FROM transcode_jobs
             WHERE status IN ('completed', 'failed')
               AND completed_at IS NOT NULL
               AND datetime(completed_at) < datetime(?1)",
            params![older_than.to_rfc3339()],
        )?;
        Ok(n as u64)
    }

    fn read_transcode_row(row: &rusqlite::Row) -> rusqlite::Result<TranscodeJob> {
        Ok(TranscodeJob {
            id: row.get(0)?,
            status: row.get(1)?,
            bucket: row.get(2)?,
            key: row.get(3)?,
            source_sha256: row.get(4)?,
            profile: row.get(5)?,
            output_bucket: row.get(6)?,
            output_key: row.get(7)?,
            output_size: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
            error: row.get(9)?,
            created_at: row
                .get::<_, String>(10)?
                .parse()
                .unwrap_or_else(|_| Utc::now()),
            started_at: row
                .get::<_, Option<String>>(11)?
                .and_then(|s| s.parse().ok()),
            completed_at: row
                .get::<_, Option<String>>(12)?
                .and_then(|s| s.parse().ok()),
            duration_ms: row.get(13)?,
            requested_by: row.get(14)?,
        })
    }

    // ── Object lock ─────────────────────────────────────────────────────

    pub fn get_lock(&self, bucket: &str, key: &str) -> Result<ObjectLock, StorageError> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT retain_until, legal_hold FROM objects WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
            |row| {
                let retain_until: Option<String> = row.get(0)?;
                let legal_hold: i32 = row.get(1)?;
                Ok(ObjectLock {
                    retain_until: retain_until.and_then(|s| s.parse().ok()),
                    legal_hold: legal_hold != 0,
                })
            },
        )
        .map_err(|_| StorageError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// Apply a lock to a live object. `retain_until` can only be extended —
    /// shortening (or clearing while still in the future) is rejected to
    /// preserve WORM semantics. `legal_hold` can be toggled freely.
    pub fn set_lock(
        &self,
        bucket: &str,
        key: &str,
        retain_until: Option<chrono::DateTime<Utc>>,
        legal_hold: bool,
    ) -> Result<ObjectLock, StorageError> {
        let current = self.get_lock(bucket, key)?;
        let now = Utc::now();

        // WORM: retention cannot be shortened while still active.
        if let Some(existing) = current.retain_until {
            if existing > now {
                match retain_until {
                    Some(new) if new < existing => {
                        return Err(StorageError::ObjectLocked {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            reason: "retention cannot be shortened while active".into(),
                        });
                    }
                    None => {
                        return Err(StorageError::ObjectLocked {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                            reason: "retention cannot be cleared while active".into(),
                        });
                    }
                    _ => {}
                }
            }
        }

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE objects SET retain_until = ?1, legal_hold = ?2 WHERE bucket = ?3 AND key = ?4",
            params![
                retain_until.map(|t| t.to_rfc3339()),
                legal_hold as i32,
                bucket,
                key,
            ],
        )?;
        Ok(ObjectLock {
            retain_until,
            legal_hold,
        })
    }

    /// Clear legal hold on a live object. Separate from `set_lock` so callers
    /// can toggle the hold without having to re-supply `retain_until`.
    pub fn clear_legal_hold(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        self.get_lock(bucket, key)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE objects SET legal_hold = 0 WHERE bucket = ?1 AND key = ?2",
            params![bucket, key],
        )?;
        Ok(())
    }

    /// True if any row in `objects` or `object_versions` still references this blob.
    pub fn is_storage_path_referenced(&self, storage_path: &str) -> Result<bool, StorageError> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT
                (SELECT COUNT(*) FROM objects WHERE storage_path = ?1)
              + (SELECT COUNT(*) FROM object_versions WHERE storage_path = ?1)",
            params![storage_path],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    // ── Lifecycle ───────────────────────────────────────────────────────

    pub fn create_lifecycle_rule(
        &self,
        bucket: &str,
        prefix: &str,
        expire_days: u64,
    ) -> Result<LifecycleRule, StorageError> {
        self.get_bucket(bucket)?;
        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        conn.execute(
            "INSERT INTO lifecycle_rules (id, bucket, prefix, expire_days, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, bucket, prefix, expire_days as i64, now.to_rfc3339()],
        )?;
        Ok(LifecycleRule {
            id,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            expire_days,
            created_at: now,
        })
    }

    pub fn list_lifecycle_rules(&self, bucket: &str) -> Result<Vec<LifecycleRule>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, bucket, prefix, expire_days, created_at FROM lifecycle_rules WHERE bucket = ?1 ORDER BY created_at",
        )?;
        let rules = stmt
            .query_map(params![bucket], |row| {
                Ok(LifecycleRule {
                    id: row.get(0)?,
                    bucket: row.get(1)?,
                    prefix: row.get(2)?,
                    expire_days: row.get::<_, i64>(3)? as u64,
                    created_at: row
                        .get::<_, String>(4)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rules)
    }

    pub fn delete_lifecycle_rule(&self, id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute("DELETE FROM lifecycle_rules WHERE id = ?1", params![id])?;
        if affected == 0 {
            return Err(StorageError::ObjectNotFound {
                bucket: "lifecycle_rules".to_string(),
                key: id.to_string(),
            });
        }
        Ok(())
    }

    /// Returns (bucket, key, storage_path) for objects whose created_at + expire_days < now.
    pub fn find_expired_objects(&self) -> Result<Vec<(String, String, String)>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.bucket, o.key, o.storage_path
             FROM objects o
             INNER JOIN lifecycle_rules r
               ON o.bucket = r.bucket AND o.key LIKE (r.prefix || '%')
             WHERE datetime(o.created_at) < datetime('now', '-' || r.expire_days || ' days')",
        )?;
        let results = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(results)
    }

    // ── S3 multipart uploads ───────────────────────────────────────────

    pub fn create_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> Result<MultipartUpload, StorageError> {
        // Surface BucketNotFound rather than a foreign-key failure — the
        // S3 NoSuchBucket error is more useful to clients.
        self.get_bucket(bucket)?;
        let now = Utc::now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket, key, content_type, initiated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![upload_id, bucket, key, content_type, now.to_rfc3339()],
        )?;
        Ok(MultipartUpload {
            upload_id: upload_id.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.map(|s| s.to_string()),
            initiated_at: now,
        })
    }

    pub fn get_multipart_upload(
        &self,
        upload_id: &str,
    ) -> Result<Option<MultipartUpload>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let row = conn.query_row(
            "SELECT bucket, key, content_type, initiated_at
             FROM multipart_uploads WHERE upload_id = ?1",
            params![upload_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        );
        match row {
            Ok((bucket, key, content_type, initiated_at)) => Ok(Some(MultipartUpload {
                upload_id: upload_id.to_string(),
                bucket,
                key,
                content_type,
                initiated_at: initiated_at.parse().unwrap_or_else(|_| chrono::Utc::now()),
            })),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(StorageError::Database(e)),
        }
    }

    pub fn save_multipart_part(
        &self,
        upload_id: &str,
        part_number: u32,
        size: u64,
        etag: &str,
    ) -> Result<MultipartPart, StorageError> {
        let now = Utc::now();
        let conn = self.conn.lock().unwrap();
        // UPSERT: re-uploading the same part number replaces it. S3 allows
        // this and clients do it for retries.
        conn.execute(
            "INSERT INTO multipart_parts (upload_id, part_number, size, etag, uploaded_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(upload_id, part_number) DO UPDATE SET
                size = excluded.size,
                etag = excluded.etag,
                uploaded_at = excluded.uploaded_at",
            params![upload_id, part_number, size, etag, now.to_rfc3339()],
        )?;
        Ok(MultipartPart {
            upload_id: upload_id.to_string(),
            part_number,
            size,
            etag: etag.to_string(),
            uploaded_at: now,
        })
    }

    pub fn list_multipart_parts(
        &self,
        upload_id: &str,
    ) -> Result<Vec<MultipartPart>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT part_number, size, etag, uploaded_at
             FROM multipart_parts WHERE upload_id = ?1 ORDER BY part_number",
        )?;
        let parts = stmt
            .query_map(params![upload_id], |row| {
                Ok(MultipartPart {
                    upload_id: upload_id.to_string(),
                    part_number: row.get::<_, u32>(0)?,
                    size: row.get::<_, u64>(1)?,
                    etag: row.get::<_, String>(2)?,
                    uploaded_at: row
                        .get::<_, String>(3)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(parts)
    }

    pub fn delete_multipart_upload(&self, upload_id: &str) -> Result<(), StorageError> {
        let conn = self.conn.lock().unwrap();
        // Explicitly drop parts first — the schema FK uses CASCADE, but
        // enable_foreign_keys isn't set on every connection path.
        conn.execute(
            "DELETE FROM multipart_parts WHERE upload_id = ?1",
            params![upload_id],
        )?;
        conn.execute(
            "DELETE FROM multipart_uploads WHERE upload_id = ?1",
            params![upload_id],
        )?;
        Ok(())
    }

    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
    ) -> Result<Vec<MultipartUpload>, StorageError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT upload_id, key, content_type, initiated_at
             FROM multipart_uploads WHERE bucket = ?1 ORDER BY initiated_at",
        )?;
        let uploads = stmt
            .query_map(params![bucket], |row| {
                Ok(MultipartUpload {
                    upload_id: row.get::<_, String>(0)?,
                    bucket: bucket.to_string(),
                    key: row.get::<_, String>(1)?,
                    content_type: row.get::<_, Option<String>>(2)?,
                    initiated_at: row
                        .get::<_, String>(3)?
                        .parse()
                        .unwrap_or_else(|_| chrono::Utc::now()),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(uploads)
    }
}
