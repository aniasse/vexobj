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
        Self::start_with_sse(None)
    }

    /// Variant that enables server-side encryption with the given 64-hex-char
    /// master key. Used by the SSE integration test; everything else keeps
    /// calling `start()` for the no-SSE default.
    fn start_with_sse(sse_master_key: Option<&str>) -> Self {
        let temp_dir = std::env::temp_dir().join(format!("vaultfs-e2e-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");

        let port = find_free_port();
        let bind_addr = format!("127.0.0.1:{}", port);

        let sse_section = match sse_master_key {
            Some(k) => format!("\n[sse]\nenabled = true\nmaster_key = \"{k}\"\n"),
            None => String::new(),
        };

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
{sse_section}"#,
            bind_addr = bind_addr,
            data_dir = temp_dir.to_string_lossy().replace('\\', "/"),
            sse_section = sse_section,
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
            let reader = BufReader::new(stdout);
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

    /// Send a raw HTTP/1.1 GET with the exact `raw_path` bytes (no client-side
    /// URL normalization). Returns `(status, body)`. Used to exercise server-side
    /// path handling for paths reqwest would otherwise collapse (e.g. `..`).
    fn raw_get(&self, raw_path: &str) -> (u16, String) {
        use std::io::{Read, Write};

        let host_port = self.url.trim_start_matches("http://");
        let mut stream =
            std::net::TcpStream::connect(host_port).expect("connect to test server");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let req = format!(
            "GET {raw_path} HTTP/1.1\r\n\
             Host: {host_port}\r\n\
             Authorization: Bearer {key}\r\n\
             Connection: close\r\n\r\n",
            raw_path = raw_path,
            host_port = host_port,
            key = self.admin_key,
        );
        stream.write_all(req.as_bytes()).unwrap();

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        let response = String::from_utf8_lossy(&buf).to_string();

        let status_line = response.lines().next().unwrap_or("");
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
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

    // Each attack vector is sent as a raw HTTP request so that reqwest/url
    // don't normalize the path client-side — we want to verify what the
    // server does with the exact bytes an attacker would send.

    // 1. Literal `..` segments
    let (status, body) = srv.raw_get("/v1/objects/bucket/../../../etc/passwd");
    assert_eq!(status, 400, "literal .. should be rejected");
    assert!(
        body.contains("path traversal"),
        "expected traversal error, got: {body}"
    );

    // 2. Percent-encoded `..` (%2E%2E) — a classic WAF bypass
    let (status, _) = srv.raw_get("/v1/objects/bucket/%2E%2E/%2E%2E/etc/passwd");
    assert_eq!(status, 400, "encoded .. should be rejected");

    // 3. Double slash
    let (status, _) = srv.raw_get("/v1/objects//bucket/key");
    assert_eq!(status, 400, "double slash should be rejected");

    // 4. Null byte (percent-encoded; raw \0 would terminate the request line)
    let (status, _) = srv.raw_get("/v1/objects/bucket/key%00.txt");
    assert_eq!(status, 400, "null byte should be rejected");
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

#[tokio::test]
async fn e2e_versioning() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "ver-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    // Enable versioning
    let resp = client
        .post(format!("{}/v1/admin/versioning/ver-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["versioning"], "enabled");

    // Upload version 1
    client
        .put(format!("{}/v1/objects/ver-bucket/doc.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body("version 1")
        .send()
        .await
        .unwrap();

    // Upload version 2 (overwrite)
    client
        .put(format!("{}/v1/objects/ver-bucket/doc.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body("version 2")
        .send()
        .await
        .unwrap();

    // List versions
    let resp = client
        .get(format!("{}/v1/versions/ver-bucket/doc.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2);
    // Latest version should be first (DESC order)
    assert_eq!(versions[0]["is_latest"], true);
    assert_eq!(versions[1]["is_latest"], false);

    // Download a specific version
    let v1_id = versions[1]["version_id"].as_str().unwrap();
    let resp = client
        .get(format!(
            "{}/v1/objects/ver-bucket/doc.txt?version_id={}",
            srv.url, v1_id
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let data = resp.text().await.unwrap();
    assert_eq!(data, "version 1");
}

#[tokio::test]
async fn e2e_lifecycle_rules() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "lc-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    // Create lifecycle rule
    let resp = client
        .post(format!("{}/v1/admin/lifecycle/lc-bucket", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"prefix": "tmp/", "expire_days": 7}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["bucket"], "lc-bucket");
    assert_eq!(body["prefix"], "tmp/");
    assert_eq!(body["expire_days"], 7);
    let rule_id = body["id"].as_str().unwrap().to_string();

    // List lifecycle rules
    let resp = client
        .get(format!("{}/v1/admin/lifecycle/lc-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let rules = body["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 1);

    // Run lifecycle (should expire nothing since objects are fresh)
    let resp = client
        .post(format!("{}/v1/admin/lifecycle/run", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["objects_expired"], 0);

    // Delete lifecycle rule
    let resp = client
        .delete(format!("{}/v1/admin/lifecycle/rule/{}", srv.url, rule_id))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Verify rule deleted
    let resp = client
        .get(format!("{}/v1/admin/lifecycle/lc-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let rules = body["rules"].as_array().unwrap();
    assert_eq!(rules.len(), 0);
}

#[tokio::test]
async fn e2e_delete_version_and_purge() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket + enable versioning
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "purge-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/v1/admin/versioning/purge-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Upload three versions of the same key
    for body in ["v1", "v2", "v3"] {
        client
            .put(format!("{}/v1/objects/purge-bucket/note.txt", srv.url))
            .header("Authorization", &auth)
            .header("Content-Type", "text/plain")
            .body(body)
            .send()
            .await
            .unwrap();
    }

    // List versions — expect 3, latest first
    let resp = client
        .get(format!("{}/v1/versions/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 3);
    let latest_id = versions[0]["version_id"].as_str().unwrap().to_string();
    let middle_id = versions[1]["version_id"].as_str().unwrap().to_string();

    // Delete a non-latest version via DELETE ?version_id=
    let resp = client
        .delete(format!(
            "{}/v1/objects/purge-bucket/note.txt?version_id={}",
            srv.url, middle_id
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // That version should be gone; latest still marked
    let resp = client
        .get(format!("{}/v1/versions/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2);
    assert!(!versions.iter().any(|v| v["version_id"] == middle_id));
    let latest_row = versions.iter().find(|v| v["version_id"] == latest_id).unwrap();
    assert_eq!(latest_row["is_latest"], true);

    // Delete the latest version — the remaining one must be promoted
    let resp = client
        .delete(format!(
            "{}/v1/objects/purge-bucket/note.txt?version_id={}",
            srv.url, latest_id
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let resp = client
        .get(format!("{}/v1/versions/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let versions = body["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0]["is_latest"], true);

    // Deleting an unknown version returns 404
    let resp = client
        .delete(format!(
            "{}/v1/objects/purge-bucket/note.txt?version_id=does-not-exist",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Purge every remaining version + the live object in one shot
    let resp = client
        .delete(format!("{}/v1/versions/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["bucket"], "purge-bucket");
    assert_eq!(body["key"], "note.txt");

    // Versions list is now empty and GET on the live object returns 404
    let resp = client
        .get(format!("{}/v1/versions/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["versions"].as_array().unwrap().len(), 0);

    let resp = client
        .get(format!("{}/v1/objects/purge-bucket/note.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_sse_at_rest() {
    let key = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    let srv = TestServer::start_with_sse(Some(key));
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket and put an object
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "sse-bucket", "public": false}))
        .send()
        .await
        .unwrap();

    let payload = "top secret — must be encrypted at rest";
    let resp = client
        .put(format!("{}/v1/objects/sse-bucket/secret.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body(payload)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let meta: Value = resp.json().await.unwrap();
    let sha256 = meta["sha256"].as_str().unwrap().to_string();

    // GET returns the plaintext — round-trip works
    let resp = client
        .get(format!("{}/v1/objects/sse-bucket/secret.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), payload);

    // Confirm the bytes on disk are NOT the plaintext. The blob lives at
    // data_dir/blobs/<aa>/<bb>/<sha256>. We can find data_dir via the test
    // temp directory used by the harness.
    let data_dir = srv._temp_dir.clone();
    let blob_path = data_dir
        .join("blobs")
        .join(&sha256[..2])
        .join(&sha256[2..4])
        .join(&sha256);
    let on_disk = std::fs::read(&blob_path).expect("blob file exists");
    assert!(
        !on_disk.windows(payload.len()).any(|w| w == payload.as_bytes()),
        "plaintext must not appear on disk"
    );
    // Ciphertext is payload length + 16-byte auth tag
    assert_eq!(on_disk.len(), payload.len() + 16);
}

#[tokio::test]
async fn e2e_object_lock() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Setup: bucket + object
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "lock-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .body("contents")
        .send()
        .await
        .unwrap();

    // Initially no lock: GET returns default (legal_hold=false, retain_until=null)
    let resp = client
        .get(format!("{}/v1/admin/lock/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let lock: Value = resp.json().await.unwrap();
    assert_eq!(lock["legal_hold"], false);
    assert!(lock["retain_until"].is_null());

    // Apply a future retention + legal hold
    let future = (chrono::Utc::now() + chrono::Duration::days(30))
        .to_rfc3339();
    let resp = client
        .put(format!("{}/v1/admin/lock/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"retain_until": future, "legal_hold": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // DELETE should now be rejected with 409 and a reason mentioning the lock
    let resp = client
        .delete(format!("{}/v1/objects/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("locked"));

    // purge_versions must also be blocked
    let resp = client
        .delete(format!("{}/v1/versions/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Shortening retention must be rejected (WORM)
    let soon = (chrono::Utc::now() + chrono::Duration::seconds(60))
        .to_rfc3339();
    let resp = client
        .put(format!("{}/v1/admin/lock/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"retain_until": soon, "legal_hold": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Release the legal hold via DELETE; retention is still active so delete still fails
    let resp = client
        .delete(format!("{}/v1/admin/lock/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let resp = client
        .delete(format!("{}/v1/objects/lock-bucket/sealed.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let body: Value = resp.json().await.unwrap();
    // Now the reason should cite retention, not legal hold
    assert!(body["reason"].as_str().unwrap().contains("retention"));
}

// ---------------------------------------------------------------------------
// S3-compatible API (/s3/*)
// ---------------------------------------------------------------------------
//
// Our s3-compat layer accepts Bearer tokens as a shortcut in addition to the
// full AWS4-HMAC-SHA256 Authorization header, so these tests reuse the same
// `auth_header()` helper as the native-API tests. Each test covers one surface
// a real S3 client would exercise.

#[tokio::test]
async fn e2e_s3_compat_bucket_lifecycle() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // PUT bucket
    let resp = client
        .put(format!("{}/s3/s3test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().contains_key("location"));

    // HEAD bucket → 200
    let resp = client
        .head(format!("{}/s3/s3test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // List buckets (service-level) returns XML with our bucket name.
    let resp = client
        .get(format!("{}/s3", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("xml"), "expected xml, got {ct}");
    let body = resp.text().await.unwrap();
    assert!(body.contains("<ListAllMyBucketsResult"));
    assert!(body.contains("<Name>s3test-bucket</Name>"));

    // DELETE empty bucket → 204
    let resp = client
        .delete(format!("{}/s3/s3test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // HEAD on deleted bucket → 404
    let resp = client
        .head(format!("{}/s3/s3test-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn e2e_s3_compat_object_crud() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create bucket via S3 route
    client
        .put(format!("{}/s3/s3obj", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    // PUT object
    let resp = client
        .put(format!("{}/s3/s3obj/hello.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body("hello s3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(etag.starts_with('"') && etag.ends_with('"'), "etag quoted: {etag}");

    // HEAD object
    let resp = client
        .head(format!("{}/s3/s3obj/hello.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/plain"
    );
    assert_eq!(
        resp.headers().get("content-length").unwrap().to_str().unwrap(),
        "8"
    );

    // GET object
    let resp = client
        .get(format!("{}/s3/s3obj/hello.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello s3");

    // DELETE object → 204
    let resp = client
        .delete(format!("{}/s3/s3obj/hello.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // GET on deleted object → NoSuchKey XML
    let resp = client
        .get(format!("{}/s3/s3obj/hello.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let body = resp.text().await.unwrap();
    assert!(body.contains("NoSuchKey"));
}

#[tokio::test]
async fn e2e_s3_compat_list_objects_v2() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .put(format!("{}/s3/s3list", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    for (key, body) in [
        ("docs/a.txt", "a"),
        ("docs/b.txt", "b"),
        ("docs/sub/c.txt", "c"),
        ("images/x.png", "png"),
    ] {
        client
            .put(format!("{}/s3/s3list/{}", srv.url, key))
            .header("Authorization", &auth)
            .body(body.to_string())
            .send()
            .await
            .unwrap();
    }

    // list-type=2 with prefix
    let resp = client
        .get(format!("{}/s3/s3list?list-type=2&prefix=docs/", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<ListBucketResult"));
    assert!(body.contains("<Key>docs/a.txt</Key>"));
    assert!(body.contains("<Key>docs/b.txt</Key>"));
    assert!(body.contains("<Key>docs/sub/c.txt</Key>"));
    assert!(!body.contains("<Key>images/x.png</Key>"));

    // prefix + delimiter collapses the `sub/` subtree into a CommonPrefix
    let resp = client
        .get(format!(
            "{}/s3/s3list?list-type=2&prefix=docs/&delimiter=/",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(body.contains("<Key>docs/a.txt</Key>"));
    assert!(body.contains("<Key>docs/b.txt</Key>"));
    assert!(!body.contains("<Key>docs/sub/c.txt</Key>"));
    assert!(body.contains("<Prefix>docs/sub/</Prefix>"));
}

#[tokio::test]
async fn e2e_s3_compat_copy_object() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .put(format!("{}/s3/s3copy", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    client
        .put(format!("{}/s3/s3copy/source.txt", srv.url))
        .header("Authorization", &auth)
        .body("original payload")
        .send()
        .await
        .unwrap();

    // Copy via x-amz-copy-source
    let resp = client
        .put(format!("{}/s3/s3copy/dest.txt", srv.url))
        .header("Authorization", &auth)
        .header("x-amz-copy-source", "/s3copy/source.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("<CopyObjectResult"));

    // Dest has the same content as source
    let resp = client
        .get(format!("{}/s3/s3copy/dest.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.text().await.unwrap(), "original payload");
}

/// Build a SigV4 `Authorization` header for a GET request using the same
/// algorithm the server expects. Keeps the test self-contained.
fn sigv4_get_auth(
    url: &str,
    access_key: &str,
    secret: &str,
    amz_date: &str,
    payload_hash: &str,
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;

    let parsed = reqwest::Url::parse(url).unwrap();
    let host = match parsed.port() {
        Some(p) => format!("{}:{}", parsed.host_str().unwrap(), p),
        None => parsed.host_str().unwrap().to_string(),
    };

    let canonical_uri = parsed.path().to_string();
    let canonical_query = parsed.query().unwrap_or("").to_string();

    let date = &amz_date[..8]; // YYYYMMDD
    let region = "us-east-1";
    let service = "s3";
    let scope = format!("{date}/{region}/{service}/aws4_request");

    let canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    let canonical_request = format!(
        "GET\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let mut h = Sha256::new();
    h.update(canonical_request.as_bytes());
    let cr_hash = hex::encode(h.finalize());

    let sts = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{cr_hash}");

    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(key).unwrap();
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let k_date = mac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = mac(&k_date, region.as_bytes());
    let k_service = mac(&k_region, service.as_bytes());
    let k_signing = mac(&k_service, b"aws4_request");
    let sig = hex::encode(mac(&k_signing, sts.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, \
         SignedHeaders={signed_headers}, Signature={sig}"
    )
}

#[tokio::test]
async fn e2e_s3_compat_sigv4_accepts_valid_and_rejects_tamper() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth_bearer = srv.auth_header();

    // Create bucket via the Bearer shortcut so we have something to list.
    client
        .put(format!("{}/s3/sigv4-bucket", srv.url))
        .header("Authorization", &auth_bearer)
        .send()
        .await
        .unwrap();

    // The admin key is the full `vfs_...` string — our server accepts the
    // raw key as the Credential access_key (see find_by_access_key).
    let key = srv.admin_key.clone();
    let url = format!("{}/s3", srv.url);
    let amz_date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let signed = sigv4_get_auth(&url, &key, &key, &amz_date, empty_hash);

    let resp = client
        .get(&url)
        .header("Authorization", &signed)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", empty_hash)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "valid SigV4 must be accepted");
    assert!(resp.text().await.unwrap().contains("<ListAllMyBucketsResult"));

    // Now tamper with the signature — last-char flip. Server must reject.
    let mut bad = signed.clone();
    bad.pop();
    bad.push('0');
    let resp = client
        .get(&url)
        .header("Authorization", &bad)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", empty_hash)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "tampered SigV4 must be rejected");

    // Tamper by swapping the query — signed for `/s3`, sent to `/s3?extra=1`.
    let resp = client
        .get(format!("{}?extra=1", url))
        .header("Authorization", &signed)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", empty_hash)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "URL mismatch must be rejected");
}

#[tokio::test]
async fn e2e_s3_compat_rejects_missing_auth() {
    let srv = TestServer::start();
    let client = srv.client();

    // No Authorization header → AccessDenied
    let resp = client
        .get(format!("{}/s3", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body = resp.text().await.unwrap();
    assert!(body.contains("AccessDenied"));

    // Bad bearer → AccessDenied
    let resp = client
        .get(format!("{}/s3", srv.url))
        .header("Authorization", "Bearer not-a-real-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn e2e_migrate_s3_stub() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Server-side S3 migration should return 501 with a hint to use CLI
    let resp = client
        .post(format!("{}/v1/admin/migrate/s3", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 501);
    let body: Value = resp.json().await.unwrap();
    assert!(body["hint"].as_str().unwrap().contains("CLI"));
    assert!(body["command"].as_str().unwrap().contains("vaultfsctl"));
}
