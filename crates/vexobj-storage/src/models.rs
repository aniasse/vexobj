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
/// stored as a first-class vexobj object at `output_bucket/output_key`
/// once the job completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscodeJob {
    pub id: String,
    /// `pending`, `running`, `completed`, or `failed`.
    pub status: String,
    pub bucket: String,
    pub key: String,
    pub source_sha256: String,
    /// Profile name (see vexobj-processing::transcode_profiles).
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

/// S3-style multipart upload in progress. Rows here are created at
/// `InitiateMultipartUpload`, consulted by every `UploadPart` call, and
/// deleted at `CompleteMultipartUpload` / `AbortMultipartUpload`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub content_type: Option<String>,
    pub initiated_at: DateTime<Utc>,
}

/// One part of a multipart upload. Part bytes live in a scratch file on
/// disk; this row records the metadata needed to validate the client's
/// `CompleteMultipartUpload` request and reassemble in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPart {
    pub upload_id: String,
    pub part_number: u32,
    pub size: u64,
    pub etag: String,
    pub uploaded_at: DateTime<Utc>,
}

/// One CORS rule attached to a bucket. Semantics follow the AWS S3 shape
/// closely: a request is allowed iff at least one rule matches all three of
/// Origin, Method, and (for preflight) requested headers. `"*"` is a valid
/// entry in `allowed_origins` / `allowed_methods` / `allowed_headers` and
/// matches anything. Empty `allowed_origins` means the rule matches no
/// request, which is the no-op default we store when a bucket has never
/// had CORS configured.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CorsRule {
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub allowed_methods: Vec<String>,
    #[serde(default)]
    pub allowed_headers: Vec<String>,
    #[serde(default)]
    pub expose_headers: Vec<String>,
    /// How long browsers may cache the preflight result. 0 = don't emit
    /// Access-Control-Max-Age.
    #[serde(default)]
    pub max_age_seconds: u64,
}

impl CorsRule {
    /// True iff `origin` matches `allowed_origins` literally or via `"*"`.
    pub fn matches_origin(&self, origin: &str) -> bool {
        self.allowed_origins
            .iter()
            .any(|o| o == "*" || o.eq_ignore_ascii_case(origin))
    }

    /// True iff `method` matches `allowed_methods` literally or via `"*"`.
    /// Method names are compared case-insensitively to match HTTP's case
    /// tolerance in Origin/Access-Control-Request-Method.
    pub fn matches_method(&self, method: &str) -> bool {
        self.allowed_methods
            .iter()
            .any(|m| m == "*" || m.eq_ignore_ascii_case(method))
    }

    /// True iff every requested header is in `allowed_headers` (case-
    /// insensitive), or `allowed_headers` contains `"*"`. Called during
    /// preflight with the comma-separated list from
    /// Access-Control-Request-Headers.
    pub fn matches_headers(&self, requested: &[&str]) -> bool {
        if self.allowed_headers.iter().any(|h| h == "*") {
            return true;
        }
        requested.iter().all(|req| {
            self.allowed_headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case(req))
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: String,
    pub bucket: String,
    pub prefix: String,
    pub expire_days: u64,
    pub created_at: DateTime<Utc>,
}
