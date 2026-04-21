#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("image decode error: {0}")]
    Decode(#[from] image::ImageError),

    #[error("invalid transform parameters: {0}")]
    InvalidParams(String),
}
