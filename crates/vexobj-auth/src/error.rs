#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("invalid api key")]
    InvalidApiKey,

    #[error("api key not found")]
    KeyNotFound,

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
}
