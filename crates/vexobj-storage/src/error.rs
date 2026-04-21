#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("object too large: {size} bytes (max: {max})")]
    ObjectTooLarge { size: u64, max: u64 },

    #[error("object is locked: {bucket}/{key} ({reason})")]
    ObjectLocked {
        bucket: String,
        key: String,
        reason: String,
    },

    #[error("bucket quota exceeded: {bucket} ({reason})")]
    QuotaExceeded { bucket: String, reason: String },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
}
