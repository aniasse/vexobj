//! Local-filesystem implementation of `BlobStore`.
//!
//! This is what the engine used to do inline — now it lives behind
//! the trait so the rest of the code has one code path that works
//! against local disk, S3, R2, etc. Behavior is unchanged: blobs live
//! at `<data_dir>/<key>`, which the engine constructs as
//! `blobs/<aa>/<bb>/<sha256>`.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::path::{Path, PathBuf};
use tokio_util::io::ReaderStream;

use crate::blob_store::BlobStore;
use crate::error::StorageError;

pub struct LocalBlobStore {
    data_dir: PathBuf,
}

impl LocalBlobStore {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    fn full_path(&self, key: &str) -> PathBuf {
        self.data_dir.join(key)
    }

    async fn ensure_parent(path: &Path) -> Result<(), StorageError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn put_blob(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let dest = self.full_path(key);
        Self::ensure_parent(&dest).await?;
        tokio::fs::write(&dest, data).await?;
        Ok(())
    }

    async fn put_blob_from_file(&self, key: &str, source: &Path) -> Result<(), StorageError> {
        let dest = self.full_path(key);
        Self::ensure_parent(&dest).await?;
        // Try a cheap rename first — works when the temp file was written on
        // the same filesystem. Fall back to copy + delete when it isn't
        // (happens on some Docker / tmpfs setups).
        match tokio::fs::rename(source, &dest).await {
            Ok(()) => Ok(()),
            Err(_) => {
                tokio::fs::copy(source, &dest).await?;
                let _ = tokio::fs::remove_file(source).await;
                Ok(())
            }
        }
    }

    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        Ok(tokio::fs::read(self.full_path(key)).await?)
    }

    async fn exists_blob(&self, key: &str) -> Result<bool, StorageError> {
        // tokio::fs::try_exists returns a real error only on permission /
        // device failures — plain "missing" is Ok(false).
        Ok(tokio::fs::try_exists(self.full_path(key)).await?)
    }

    async fn delete_blob(&self, key: &str) -> Result<(), StorageError> {
        // Missing-is-fine semantics.
        let _ = tokio::fs::remove_file(self.full_path(key)).await;
        Ok(())
    }

    async fn stream_blob(
        &self,
        key: &str,
    ) -> Result<BoxStream<'static, std::io::Result<Bytes>>, StorageError> {
        let file = tokio::fs::File::open(self.full_path(key)).await?;
        let stream = ReaderStream::new(file);
        Ok(Box::pin(stream))
    }

    fn supports_local_path(&self) -> bool {
        true
    }

    fn local_path(&self, key: &str) -> Option<PathBuf> {
        Some(self.full_path(key))
    }

    fn backend_name(&self) -> &'static str {
        "local"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("vfs-blobs-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn round_trip_put_get() {
        let dir = tempdir();
        let store = LocalBlobStore::new(dir.clone());
        store.put_blob("blobs/aa/bb/hash", b"hello").await.unwrap();
        let got = store.get_blob("blobs/aa/bb/hash").await.unwrap();
        assert_eq!(got, b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn exists_and_delete() {
        let dir = tempdir();
        let store = LocalBlobStore::new(dir.clone());
        assert!(!store.exists_blob("missing").await.unwrap());
        store.put_blob("present", b"x").await.unwrap();
        assert!(store.exists_blob("present").await.unwrap());
        store.delete_blob("present").await.unwrap();
        assert!(!store.exists_blob("present").await.unwrap());
        // Deleting a missing key is a no-op.
        store.delete_blob("never-existed").await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_blob_from_file_uses_rename() {
        let dir = tempdir();
        let store = LocalBlobStore::new(dir.clone());
        let src = dir.join("source.tmp");
        tokio::fs::write(&src, b"contents").await.unwrap();
        store.put_blob_from_file("final/key", &src).await.unwrap();
        assert!(!src.exists(), "source should be consumed");
        let got = store.get_blob("final/key").await.unwrap();
        assert_eq!(got, b"contents");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
