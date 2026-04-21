//! Pluggable blob storage backend.
//!
//! The engine used to talk directly to the filesystem. Putting everything
//! behind a trait lets us ship S3 / R2 / B2 / Wasabi backends without
//! another refactor, and keeps the semantics testable in isolation
//! (unit tests stub the trait).
//!
//! # Contract
//!
//! - Keys are relative UTF-8 paths like `blobs/<aa>/<bb>/<sha256>`.
//!   The engine owns the hashing and path construction; backends just
//!   store bytes under whatever key they're given.
//! - `put_blob` overwrites existing content. Callers dedup via sha256.
//! - Every method is `async` so backends that do network I/O don't
//!   need to block a worker thread.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

use crate::error::StorageError;

#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Store `data` under `key`. Overwrites if the key already exists.
    async fn put_blob(&self, key: &str, data: &[u8]) -> Result<(), StorageError>;

    /// Move / upload a local file into the store under `key`. Local
    /// backends can implement this with `rename` (fast, no copy); S3
    /// backends stream the file body. The source file is consumed — on
    /// success the backend is free to move or unlink it.
    async fn put_blob_from_file(
        &self,
        key: &str,
        source: &std::path::Path,
    ) -> Result<(), StorageError>;

    /// Read the full blob into memory.
    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    /// Returns true when the key exists on the backend.
    async fn exists_blob(&self, key: &str) -> Result<bool, StorageError>;

    /// Remove the blob if present. Missing keys are NOT an error —
    /// deduplication means a single blob can back many object rows,
    /// and best-effort cleanup is the right default.
    async fn delete_blob(&self, key: &str) -> Result<(), StorageError>;

    /// Return a stream of the blob's bytes, for serving large files
    /// without buffering the whole thing into memory.
    async fn stream_blob(
        &self,
        key: &str,
    ) -> Result<BoxStream<'static, std::io::Result<Bytes>>, StorageError>;

    /// True when the backend stores blobs on a local filesystem that
    /// the server process can open directly (needed for ffmpeg, SSE
    /// in-place reads, etc.). Remote backends return false and those
    /// features fall back / return 501.
    fn supports_local_path(&self) -> bool {
        false
    }

    /// If `supports_local_path` is true, the absolute path where the
    /// blob lives. Otherwise None. Avoid round-tripping through this
    /// for anything that can use `get_blob` / `stream_blob`.
    fn local_path(&self, _key: &str) -> Option<std::path::PathBuf> {
        None
    }

    /// Human-readable tag for logs ("local", "s3", …).
    fn backend_name(&self) -> &'static str;
}
