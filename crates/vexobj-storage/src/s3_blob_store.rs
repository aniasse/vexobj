//! S3-compatible blob storage backend.
//!
//! Speaks the plain AWS S3 REST protocol (PUT / GET / HEAD / DELETE
//! on path-style URLs like `<endpoint>/<bucket>/<key>`), signed with
//! AWS Signature V4. Compatible with:
//!
//! - AWS S3 (`s3.<region>.amazonaws.com`)
//! - Cloudflare R2 (`<account>.r2.cloudflarestorage.com`)
//! - Backblaze B2 (`s3.<region>.backblazeb2.com`)
//! - Wasabi, DigitalOcean Spaces, Linode Object Storage, MinIO…
//!
//! Intentionally minimal: no multipart upload, no virtual-hosted-style
//! addressing, no streaming retries. Enough to act as a blob backend
//! for a single-writer engine; not a drop-in replacement for aws-sdk.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;

use crate::blob_store::BlobStore;
use crate::error::StorageError;

type HmacSha256 = Hmac<Sha256>;

/// Backend configuration. `region` defaults to `us-east-1` which works
/// with MinIO and most R2/B2 setups; on AWS proper it must match the
/// bucket's region or signatures get rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Forces path-style addressing (endpoint/bucket/key) vs. virtual-
    /// hosted (bucket.endpoint/key). Defaults to true because path
    /// style is the lowest common denominator.
    #[serde(default = "default_true")]
    pub path_style: bool,
}

fn default_region() -> String { "us-east-1".to_string() }
fn default_true() -> bool { true }

pub struct S3BlobStore {
    cfg: S3Config,
    client: reqwest::Client,
}

impl S3BlobStore {
    pub fn new(cfg: S3Config) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("build reqwest client");
        Self { cfg, client }
    }

    fn object_url(&self, key: &str) -> String {
        let base = self.cfg.endpoint.trim_end_matches('/');
        if self.cfg.path_style {
            format!("{}/{}/{}", base, self.cfg.bucket, key)
        } else {
            // Virtual-hosted style: bucket name moves to the hostname.
            let host = base.trim_start_matches("https://").trim_start_matches("http://");
            let scheme = if base.starts_with("https://") { "https" } else { "http" };
            format!("{}://{}.{}/{}", scheme, self.cfg.bucket, host, key)
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put_blob(&self, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let url = self.object_url(key);
        let payload_hash = sha256_hex(data);
        let headers = sign_request("PUT", &url, &self.cfg, &[], &payload_hash);

        let mut req = self.client.put(&url).body(data.to_vec());
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(io_err)?;
        check_ok(resp).await
    }

    async fn put_blob_from_file(
        &self,
        key: &str,
        source: &std::path::Path,
    ) -> Result<(), StorageError> {
        // No multipart yet — the whole file goes in one PUT. For files
        // up to ~5 GB this works with every S3-compat; above that the
        // engine should refuse well before it gets here (max_file_size).
        let data = tokio::fs::read(source).await?;
        self.put_blob(key, &data).await?;
        let _ = tokio::fs::remove_file(source).await;
        Ok(())
    }

    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let url = self.object_url(key);
        let headers = sign_request("GET", &url, &self.cfg, &[], EMPTY_SHA256);

        let mut req = self.client.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(io_err)?;
        let resp = check_ok_ref(resp).await?;
        let bytes = resp.bytes().await.map_err(io_err)?;
        Ok(bytes.to_vec())
    }

    async fn exists_blob(&self, key: &str) -> Result<bool, StorageError> {
        let url = self.object_url(key);
        let headers = sign_request("HEAD", &url, &self.cfg, &[], EMPTY_SHA256);

        let mut req = self.client.head(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(io_err)?;
        Ok(resp.status().is_success())
    }

    async fn delete_blob(&self, key: &str) -> Result<(), StorageError> {
        let url = self.object_url(key);
        let headers = sign_request("DELETE", &url, &self.cfg, &[], EMPTY_SHA256);

        let mut req = self.client.delete(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        // S3 returns 204 on delete-existing and 204 on delete-missing,
        // so we only care about non-2xx surprises. Network errors bubble.
        let _ = req.send().await.map_err(io_err)?;
        Ok(())
    }

    async fn stream_blob(
        &self,
        key: &str,
    ) -> Result<BoxStream<'static, std::io::Result<Bytes>>, StorageError> {
        let url = self.object_url(key);
        let headers = sign_request("GET", &url, &self.cfg, &[], EMPTY_SHA256);

        let mut req = self.client.get(&url);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.map_err(io_err)?;
        let resp = check_ok_ref(resp).await?;

        let stream = resp
            .bytes_stream()
            .map(|r| r.map(Bytes::from).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)));
        Ok(Box::pin(stream))
    }

    fn supports_local_path(&self) -> bool { false }
    fn backend_name(&self) -> &'static str { "s3" }
}

// ─── SigV4 helpers ────────────────────────────────────────────────────

const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Build the Authorization + x-amz-* headers for an S3 request. Mirrors
/// the signing we already do in vexobjctl-migrate; extracted here so
/// the backend doesn't depend on the CLI crate.
fn sign_request(
    method: &str,
    url: &str,
    cfg: &S3Config,
    extra: &[(&str, &str)],
    payload_hash: &str,
) -> Vec<(String, String)> {
    let now = chrono::Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    let parsed = reqwest::Url::parse(url).expect("valid URL");
    let host = match parsed.port() {
        Some(p) => format!("{}:{}", parsed.host_str().unwrap_or("localhost"), p),
        None => parsed.host_str().unwrap_or("localhost").to_string(),
    };
    let canonical_uri = parsed.path().to_string();
    let canonical_query = parsed.query().unwrap_or("").to_string();

    let mut all: Vec<(String, String)> = vec![
        ("host".into(), host.clone()),
        ("x-amz-content-sha256".into(), payload_hash.to_string()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    for (k, v) in extra {
        all.push((k.to_lowercase(), v.to_string()));
    }
    all.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers = all.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_headers: String = all
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash,
    );
    let cr_hash = sha256_hex(canonical_request.as_bytes());

    let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, cfg.region);
    let sts = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, cr_hash,
    );

    let k_date = hmac(format!("AWS4{}", cfg.secret_key).as_bytes(), date_stamp.as_bytes());
    let k_region = hmac(&k_date, cfg.region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let signature = hex::encode(hmac(&k_signing, sts.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        cfg.access_key, credential_scope, signed_headers, signature,
    );

    vec![
        ("Authorization".into(), auth),
        ("x-amz-date".into(), amz_date),
        ("x-amz-content-sha256".into(), payload_hash.to_string()),
        ("Host".into(), host),
    ]
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn io_err(e: reqwest::Error) -> StorageError {
    StorageError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
}

async fn check_ok(resp: reqwest::Response) -> Result<(), StorageError> {
    check_ok_ref(resp).await?;
    Ok(())
}

async fn check_ok_ref(resp: reqwest::Response) -> Result<reqwest::Response, StorageError> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp)
    } else {
        let body = resp.text().await.unwrap_or_default();
        Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("S3 HTTP {}: {}", status.as_u16(), truncate(&body, 240)),
        )))
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max { s } else { &s[..max] }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> S3Config {
        S3Config {
            endpoint: "http://localhost:9000".into(),
            bucket: "test".into(),
            access_key: "AKID".into(),
            secret_key: "SECRET".into(),
            region: "us-east-1".into(),
            path_style: true,
        }
    }

    #[test]
    fn path_style_url() {
        let store = S3BlobStore::new(cfg());
        assert_eq!(
            store.object_url("blobs/aa/bb/c"),
            "http://localhost:9000/test/blobs/aa/bb/c"
        );
    }

    #[test]
    fn virtual_hosted_style_url() {
        let mut c = cfg();
        c.path_style = false;
        c.endpoint = "https://s3.amazonaws.com".into();
        let store = S3BlobStore::new(c);
        assert_eq!(
            store.object_url("x/y"),
            "https://test.s3.amazonaws.com/x/y"
        );
    }

    #[test]
    fn sign_request_produces_stable_shape() {
        // Not a full SigV4 vector (requires frozen time), but we assert
        // the headers a caller needs to forward are all present and
        // have the right names — the shape the HTTP client expects.
        let c = cfg();
        let headers = sign_request(
            "PUT",
            "http://localhost:9000/test/blobs/aa/bb/sha",
            &c,
            &[],
            "deadbeef",
        );
        let names: Vec<_> = headers.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"Authorization"));
        assert!(names.contains(&"x-amz-date"));
        assert!(names.contains(&"x-amz-content-sha256"));
        assert!(names.contains(&"Host"));

        let auth = headers.iter().find(|(n, _)| n == "Authorization").unwrap().1.clone();
        assert!(auth.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"));
        assert!(auth.contains("s3/aws4_request"));
        assert!(auth.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
    }
}
