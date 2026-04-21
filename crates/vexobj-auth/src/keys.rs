use chrono::{DateTime, Utc};
use rand::Rng;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

use crate::error::AuthError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub name: String,
    pub key_prefix: String,
    pub created_at: DateTime<Utc>,
    pub permissions: Permissions,
    pub bucket_access: BucketAccess,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permissions {
    pub read: bool,
    pub write: bool,
    pub delete: bool,
    pub admin: bool,
}

impl Default for Permissions {
    fn default() -> Self {
        Self {
            read: true,
            write: true,
            delete: false,
            admin: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BucketAccess {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "specific")]
    Specific { buckets: Vec<String> },
}

impl Default for BucketAccess {
    fn default() -> Self {
        Self::All
    }
}

pub struct AuthManager {
    conn: Mutex<Connection>,
}

impl AuthManager {
    pub fn open(path: &Path) -> Result<Self, AuthError> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                key_hash TEXT UNIQUE NOT NULL,
                key_prefix TEXT NOT NULL,
                created_at TEXT NOT NULL,
                permissions TEXT NOT NULL DEFAULT '{}',
                bucket_access TEXT NOT NULL DEFAULT '{\"type\":\"all\"}'
            );
            ",
        )?;

        // SigV4 verification needs the plaintext secret to recompute HMACs.
        // Keys created before this column existed get an empty raw_key and
        // must authenticate via Bearer only — rotate to enable SigV4.
        let _ = conn.execute_batch(
            "ALTER TABLE api_keys ADD COLUMN raw_key TEXT NOT NULL DEFAULT '';",
        );

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn create_key(
        &self,
        name: &str,
        permissions: Permissions,
        bucket_access: BucketAccess,
    ) -> Result<(ApiKey, String), AuthError> {
        let id = uuid::Uuid::new_v4().to_string();
        let raw_key = generate_api_key();
        let key_hash = hash_key(&raw_key);
        let key_prefix = &raw_key[..12];
        let now = Utc::now();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO api_keys (id, name, key_hash, key_prefix, created_at, permissions, bucket_access, raw_key)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                id,
                name,
                key_hash,
                key_prefix,
                now.to_rfc3339(),
                serde_json::to_string(&permissions).unwrap(),
                serde_json::to_string(&bucket_access).unwrap(),
                raw_key,
            ],
        )?;

        let api_key = ApiKey {
            id,
            name: name.to_string(),
            key_prefix: key_prefix.to_string(),
            created_at: now,
            permissions,
            bucket_access,
        };

        // Return the full raw key only on creation
        Ok((api_key, raw_key))
    }

    pub fn verify_key(&self, raw_key: &str) -> Result<ApiKey, AuthError> {
        let key_hash = hash_key(raw_key);
        let conn = self.conn.lock().unwrap();

        conn.query_row(
            "SELECT id, name, key_prefix, created_at, permissions, bucket_access
             FROM api_keys WHERE key_hash = ?1",
            params![key_hash],
            |row| {
                Ok(ApiKey {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    key_prefix: row.get(2)?,
                    created_at: row
                        .get::<_, String>(3)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    permissions: serde_json::from_str(&row.get::<_, String>(4)?)
                        .unwrap_or_default(),
                    bucket_access: serde_json::from_str(&row.get::<_, String>(5)?)
                        .unwrap_or_default(),
                })
            },
        )
        .map_err(|_| AuthError::InvalidApiKey)
    }

    /// Look up a key by its SigV4 access-key identifier. The full raw API key
    /// is used as the access_key_id in the AWS Credential string, so we try
    /// an exact match first, then fall back to matching `key_prefix` for
    /// clients that truncate. Returns the ApiKey plus the stored plaintext
    /// secret (empty for legacy rows — caller must reject SigV4 for those).
    pub fn find_by_access_key(
        &self,
        access_key: &str,
    ) -> Result<(ApiKey, String), AuthError> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, name, key_prefix, created_at, permissions, bucket_access, raw_key
                 FROM api_keys WHERE raw_key = ?1 OR key_prefix = ?1",
                params![access_key],
                |row| {
                    let api_key = ApiKey {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        key_prefix: row.get(2)?,
                        created_at: row
                            .get::<_, String>(3)?
                            .parse()
                            .unwrap_or_else(|_| Utc::now()),
                        permissions: serde_json::from_str(&row.get::<_, String>(4)?)
                            .unwrap_or_default(),
                        bucket_access: serde_json::from_str(&row.get::<_, String>(5)?)
                            .unwrap_or_default(),
                    };
                    let raw_key: String = row.get(6)?;
                    Ok((api_key, raw_key))
                },
            )
            .map_err(|_| AuthError::InvalidApiKey)?;
        Ok(row)
    }

    pub fn list_keys(&self) -> Result<Vec<ApiKey>, AuthError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, key_prefix, created_at, permissions, bucket_access FROM api_keys ORDER BY created_at",
        )?;

        let keys = stmt
            .query_map([], |row| {
                Ok(ApiKey {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    key_prefix: row.get(2)?,
                    created_at: row
                        .get::<_, String>(3)?
                        .parse()
                        .unwrap_or_else(|_| Utc::now()),
                    permissions: serde_json::from_str(&row.get::<_, String>(4)?)
                        .unwrap_or_default(),
                    bucket_access: serde_json::from_str(&row.get::<_, String>(5)?)
                        .unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(keys)
    }

    pub fn delete_key(&self, id: &str) -> Result<(), AuthError> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute("DELETE FROM api_keys WHERE id = ?1", params![id])?;
        if affected == 0 {
            return Err(AuthError::KeyNotFound);
        }
        Ok(())
    }

    pub fn check_bucket_access(&self, key: &ApiKey, bucket: &str) -> Result<(), AuthError> {
        match &key.bucket_access {
            BucketAccess::All => Ok(()),
            BucketAccess::Specific { buckets } => {
                if buckets.iter().any(|b| b == bucket) {
                    Ok(())
                } else {
                    Err(AuthError::Forbidden(format!(
                        "key does not have access to bucket '{bucket}'"
                    )))
                }
            }
        }
    }
}

fn generate_api_key() -> String {
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
    format!("vex_{}", hex::encode(bytes))
}

fn hash_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}
