use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub content_type: String,
    pub sha256: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsRequest {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<u32>,
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectMeta>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBucketRequest {
    pub name: String,
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub size: u64,
    pub content_type: String,
    pub sha256: String,
    pub created_at: DateTime<Utc>,
    pub is_latest: bool,
    pub is_delete_marker: bool,
}

/// Object-lock state for a live object. Either retention (a timestamp until
/// which the object cannot be deleted) or a legal hold (boolean) or both.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectLock {
    pub retain_until: Option<DateTime<Utc>>,
    pub legal_hold: bool,
}

impl ObjectLock {
    /// True when retention is still in the future OR legal hold is on.
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        self.legal_hold || self.retain_until.map(|t| t > now).unwrap_or(false)
    }
}

/// A video transcoding job tracked in SQLite. Variant output is
/// stored as a first-class vaultfs object at `output_bucket/output_key`
/// once the job completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodeJob {
    pub id: String,
    /// `pending`, `running`, `completed`, or `failed`.
    pub status: String,
    pub bucket: String,
    pub key: String,
    pub source_sha256: String,
    /// Profile name (see vaultfs-processing::transcode_profiles).
    pub profile: String,
    pub output_bucket: Option<String>,
    pub output_key: Option<String>,
    pub output_size: Option<u64>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub requested_by: Option<String>,
}

/// A single replication event. Appended to the primary's event log on
/// every state-changing write; replicas poll `/v1/replication/events`
/// and advance a cursor as they apply each one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEvent {
    pub id: i64,
    pub op: String,
    pub bucket: String,
    pub key: String,
    pub sha256: String,
    pub version_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    /// 0 for delete / delete_marker; >0 for put / version_put.
    pub size: u64,
    /// Empty for delete / delete_marker.
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: String,
    pub bucket: String,
    pub prefix: String,
    pub expire_days: u64,
    pub created_at: DateTime<Utc>,
}
