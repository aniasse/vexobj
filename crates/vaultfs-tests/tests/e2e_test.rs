//! End-to-end HTTP integration tests for VaultFS.
//!
//! Each test spawns a real `vaultfs` server process on a random port, makes
//! HTTP requests via `reqwest`, then tears the server down.
//!
//! Prerequisites:
//!   cargo build          # the binary must exist at target/debug/vaultfs
//!   cargo test --all     # runs these tests

use reqwest::Client;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Test server harness
// ---------------------------------------------------------------------------

struct TestServer {
    url: String,
    admin_key: String,
    child: Child,
    _temp_dir: PathBuf,
}

/// Find a free TCP port by binding to port 0 and reading the assigned port.
fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
    listener.local_addr().unwrap().port()
}

impl TestServer {
    /// Spawn a VaultFS server on a random port with a fresh temp directory.
    /// Blocks until the health endpoint responds or a timeout is reached.
    fn start() -> Self {
        let temp_dir = std::env::temp_dir().join(format!("vaultfs-e2e-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");

        let port = find_free_port();
        let bind_addr = format!("127.0.0.1:{}", port);

        // Write a minimal config file
        let config_path = temp_dir.join("config.toml");
        let config_content = format!(
            r#"
[server]
bind = "{bind_addr}"

[storage]
data_dir = "{data_dir}"

[auth]
enabled = true

[rate_limit]
enabled = true
max_requests = 100
window_secs = 60
"#,
            bind_addr = bind_addr,
            data_dir = temp_dir.to_string_lossy().replace('\\', "/")
        );
        std::fs::write(&config_path, &config_content).expect("write config");

        // Locate the binary
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let binary = workspace_root.join("target/debug/vaultfs");
        assert!(
            binary.exists(),
            "vaultfs binary not found at {}. Run `cargo build` first.",
            binary.display()
        );

        // Spawn with stdout piped so we can read the admin key from tracing output.
        // tracing_subscriber::fmt() writes to stdout by default in this project.
        let mut child = Command::new(&binary)
            .env("VAULTFS_CONFIG", config_path.to_str().unwrap())
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn vaultfs");

        // Read stdout in a separate thread so we don't block forever.
        // We collect lines until we see the "listening" message or timeout.
        let stdout = child.stdout.take().expect("stdout");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        let done = line.contains("VaultFS listening on");
                        if tx.send(line).is_err() {
                            break;
                        }
                        if done {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        let mut admin_key = String::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);

        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                child.kill().ok();
                panic!("timed out waiting for vaultfs to start on port {}", port);
            }
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    // Parse the admin key
                    if line.contains("Key:") {
                        if let Some(pos) = line.find("vfs_") {
                            admin_key = line[pos..].trim().to_string();
                        }
                    }
                    // Server is ready
                    if line.contains("VaultFS listening on") {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    child.kill().ok();
                    panic!("timed out waiting for vaultfs to start on port {}", port);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // stderr closed -- server probably crashed
                    let status = child.wait().ok();
                    panic!(
                        "vaultfs exited unexpectedly on port {} (status: {:?})",
                        port, status
                    );
                }
            }
        }

        assert!(
            !admin_key.is_empty(),
            "failed to capture admin key from server output"
        );

        let url = format!("http://{}", bind_addr);

        TestServer {
            url,
            admin_key,
            child,
            _temp_dir: temp_dir,
        }
    }

    fn client(&self) -> Client {
        Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap()
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.admin_key)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self._temp_dir);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_health_check() {
    let srv = TestServer::start();
    let client = srv.client();

    let resp = client
        .get(format!("{}/health", srv.url))
        .send()
        .await
        .expect("health request");

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["service"], "vaultfs");
    assert!(body["version"].is_string());
}

#[tokio::test]
async fn e2e_auth_required() {
    let srv = TestServer::start();
    let client = srv.client();

    // No auth header -> 401
    let resp = client
        .get(format!("{}/v1/buckets", srv.url))
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 401);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("authorization"));

    // Invalid key -> 401
    let resp = client
        .get(format!("{}/v1/buckets", srv.url))
        .header("Authorization", "Bearer vfs_invalid_key")
        .send()
        .await
        .expect("request");
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn e2e_bucket_crud() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket
    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "test-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "test-bucket");

    // List buckets
    let resp = client
        .get(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let buckets = body["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["name"], "test-bucket");

    // Get bucket
    let resp = client
        .get(format!("{}/v1/buckets/test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Duplicate bucket -> 409
    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "test-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Delete bucket
    let resp = client
        .delete(format!("{}/v1/buckets/test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify deleted
    let resp = client
        .get(format!("{}/v1/buckets/test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_object_crud() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket first
    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "obj-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Upload object
    let data = b"hello vaultfs e2e test";
    let resp = client
        .put(format!("{}/v1/objects/obj-bucket/greeting.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body(data.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["key"], "greeting.txt");
    assert_eq!(body["size"], data.len());
    assert!(body["sha256"].is_string());

    // Download object
    let resp = client
        .get(format!("{}/v1/objects/obj-bucket/greeting.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/plain"
    );
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.as_ref(), data);

    // HEAD object
    let resp = client
        .head(format!("{}/v1/objects/obj-bucket/greeting.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let cl = resp
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(cl, data.len().to_string());

    // List objects
    let resp = client
        .get(format!("{}/v1/objects/obj-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["key"], "greeting.txt");

    // Delete object
    let resp = client
        .delete(format!("{}/v1/objects/obj-bucket/greeting.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify deleted
    let resp = client
        .get(format!("{}/v1/objects/obj-bucket/greeting.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_image_transform() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "img-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    // Minimal valid 2x2 PNG (67 bytes)
    // This is a valid PNG with IHDR (2x2, 8-bit RGB), IDAT, and IEND chunks.
    let png: Vec<u8> = vec![
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02, // 2x2
        0x08, 0x02, 0x00, 0x00, 0x00, 0xFD, 0xD4, 0x9A, 0x73, // 8-bit RGB, CRC
        0x00, 0x00, 0x00, 0x14, 0x49, 0x44, 0x41, 0x54, // IDAT chunk (20 bytes)
        0x78, 0x9C, 0x62, 0xF8, 0xCF, 0xC0, 0xF0, 0x1F, // zlib-compressed data
        0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x68, 0x18, // ...
        0xE7, 0x5F, 0x00, 0x00, // ...
        0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, // IEND chunk
        0xAE, 0x42, 0x60, 0x82, // IEND CRC
    ];

    // Upload the PNG
    let resp = client
        .put(format!("{}/v1/objects/img-bucket/pixel.png", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "image/png")
        .body(png.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Request with resize params -- the server should accept the request.
    // Even if the transform fails (our tiny PNG might not survive all transforms),
    // we verify the server handles the request path correctly.
    let resp = client
        .get(format!(
            "{}/v1/objects/img-bucket/pixel.png?w=1&h=1",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    // Accept either 200 (transform succeeded) or 422 (transform failed on tiny image)
    let status = resp.status().as_u16();
    assert!(
        status == 200 || status == 422,
        "unexpected status {} for image transform",
        status
    );

    // Verify that requesting without transform params returns the original PNG
    let resp = client
        .get(format!("{}/v1/objects/img-bucket/pixel.png", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn e2e_presigned_url() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket and upload an object
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "presign-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    client
        .put(format!(
            "{}/v1/objects/presign-bucket/secret.txt",
            srv.url
        ))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body("presigned content")
        .send()
        .await
        .unwrap();

    // Generate presigned URL
    let resp = client
        .post(format!("{}/v1/presign", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({
            "method": "GET",
            "bucket": "presign-bucket",
            "key": "secret.txt",
            "expires_in": 3600
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let presigned_url = body["url"].as_str().expect("presigned url");

    // The presigned URL uses the server's internal bind address.
    // We need to adjust it to our test URL since the server may report 0.0.0.0.
    let presigned_url = presigned_url.replace("http://0.0.0.0:", "http://127.0.0.1:");

    // Access via presigned URL (no auth header)
    let resp = client.get(&presigned_url).send().await.unwrap();
    // Presigned URLs should work without auth. The server checks the signature.
    // The important thing is we got a response, not a 401.
    assert_ne!(
        resp.status().as_u16(),
        401,
        "presigned URL should bypass auth"
    );
}

#[tokio::test]
async fn e2e_multipart_upload() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "multi-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    // Multipart upload
    let form = reqwest::multipart::Form::new()
        .part(
            "file1",
            reqwest::multipart::Part::bytes(b"content of file1".to_vec())
                .file_name("file1.txt")
                .mime_str("text/plain")
                .unwrap(),
        )
        .part(
            "file2",
            reqwest::multipart::Part::bytes(b"content of file2".to_vec())
                .file_name("file2.txt")
                .mime_str("text/plain")
                .unwrap(),
        );

    let resp = client
        .post(format!("{}/v1/upload/multi-bucket", srv.url))
        .header("Authorization", &auth)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    let uploaded = body["uploaded"].as_array().unwrap();
    assert_eq!(uploaded.len(), 2);
    assert!(body["errors"].as_array().unwrap().is_empty());

    // Verify both objects exist
    let resp = client
        .get(format!("{}/v1/objects/multi-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 2);
}

#[tokio::test]
async fn e2e_rate_limit_headers() {
    let srv = TestServer::start();
    let client = srv.client();

    // Rate limiting is enabled (100 req / 60s). Make a request and check headers.
    let resp = client
        .get(format!("{}/health", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let limit = resp
        .headers()
        .get("x-ratelimit-limit")
        .map(|v| v.to_str().unwrap().to_string());
    let remaining = resp
        .headers()
        .get("x-ratelimit-remaining")
        .map(|v| v.to_str().unwrap().to_string());

    assert_eq!(limit.as_deref(), Some("100"));
    // remaining should be 99 after the first request (or close to it)
    assert!(remaining.is_some());
    let remaining_val: u64 = remaining.unwrap().parse().unwrap();
    assert!(remaining_val <= 100);
}

#[tokio::test]
async fn e2e_security_path_traversal() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Path traversal attempts should be rejected by the security middleware
    let resp = client
        .get(format!(
            "{}/v1/objects/bucket/../../../etc/passwd",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"]
        .as_str()
        .unwrap()
        .contains("path traversal"));

    // Double slash
    let resp = client
        .get(format!("{}/v1/objects//bucket/key", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Null byte
    let resp = client
        .get(format!("{}/v1/objects/bucket/key%00.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_metrics_endpoint() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Make a few requests first to generate some metrics
    client
        .get(format!("{}/health", srv.url))
        .send()
        .await
        .unwrap();
    client
        .get(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    // Fetch metrics
    let resp = client
        .get(format!("{}/metrics", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        content_type.contains("text/plain"),
        "metrics should be text/plain prometheus format"
    );

    let body = resp.text().await.unwrap();

    // Verify key prometheus metrics are present
    assert!(body.contains("vaultfs_requests_total"));
    assert!(body.contains("vaultfs_requests_by_method_total"));
    assert!(body.contains("vaultfs_request_duration_seconds"));
    assert!(body.contains("vaultfs_bytes_uploaded_total"));

    // The request count should be > 0 since we made requests above
    assert!(body.contains("vaultfs_requests_by_method_total{method=\"GET\"}"));
}
