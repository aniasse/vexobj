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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: String,
    pub bucket: String,
    pub prefix: String,
    pub expire_days: u64,
    pub created_at: DateTime<Utc>,
}
