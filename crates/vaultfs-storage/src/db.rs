use chrono::Utc;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

use crate::error::StorageError;
use crate::models::*;

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
            ",
        )?;
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
        let mut stmt = conn.prepare("SELECT id, name, created_at, public FROM buckets ORDER BY name")?;
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

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMeta, String), StorageError> {
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

    pub fn list_objects(&self, req: &ListObjectsRequest) -> Result<ListObjectsResponse, StorageError> {
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
            .query_map(params![req.bucket, start, like_pattern, max_keys as i64 + 1], |row| {
                Ok(ObjectMeta {
                    id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    size: row.get::<_, i64>(3)? as u64,
                    content_type: row.get(4)?,
                    sha256: row.get(5)?,
                    created_at: row.get::<_, String>(6)?.parse().unwrap_or_else(|_| Utc::now()),
                    updated_at: row.get::<_, String>(7)?.parse().unwrap_or_else(|_| Utc::now()),
                    metadata: serde_json::from_str(&row.get::<_, String>(8)?).unwrap_or_default(),
                })
            })?
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
}
