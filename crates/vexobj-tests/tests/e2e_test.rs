//! End-to-end HTTP integration tests for vexobj.
//!
//! Each test spawns a real `vexobj` server process on a random port, makes
//! HTTP requests via `reqwest`, then tears the server down.
//!
//! Prerequisites:
//!   cargo build          # the binary must exist at target/debug/vexobj
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
    /// Spawn a vexobj server on a random port with a fresh temp directory.
    /// Blocks until the health endpoint responds or a timeout is reached.
    fn start() -> Self {
        Self::start_with(None, &[])
    }

    fn start_with_sse(sse_master_key: Option<&str>) -> Self {
        Self::start_with(sse_master_key, &[])
    }

    /// Start a test server with arbitrary env vars — used by tests that
    /// need to tweak VEXOBJ_* knobs (rate limits, worker counts, etc.).
    fn start_with_env(extra_env: &[(&str, &str)]) -> Self {
        Self::start_with(None, extra_env)
    }

    /// Common path: writes a minimal config, spawns the binary with the
    /// given extra environment overrides, and blocks until /health
    /// answers.
    fn start_with(sse_master_key: Option<&str>, extra_env: &[(&str, &str)]) -> Self {
        let temp_dir = std::env::temp_dir().join(format!("vexobj-e2e-{}", uuid::Uuid::new_v4()));
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
        let binary = workspace_root.join("target/debug/vexobj");
        assert!(
            binary.exists(),
            "vexobj binary not found at {}. Run `cargo build` first.",
            binary.display()
        );

        // Spawn with stdout piped so we can read the admin key from tracing output.
        // tracing_subscriber::fmt() writes to stdout by default in this project.
        let mut cmd = Command::new(&binary);
        cmd.env("VEXOBJ_CONFIG", config_path.to_str().unwrap())
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn vexobj");

        // Read stdout in a separate thread so we don't block forever.
        // We collect lines until we see the "listening" message or timeout.
        let stdout = child.stdout.take().expect("stdout");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        let done = line.contains("vexobj listening on");
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
                panic!("timed out waiting for vexobj to start on port {}", port);
            }
            match rx.recv_timeout(remaining) {
                Ok(line) => {
                    // Parse the admin key
                    if line.contains("Key:") {
                        if let Some(pos) = line.find("vex_") {
                            admin_key = line[pos..].trim().to_string();
                        }
                    }
                    // Server is ready
                    if line.contains("vexobj listening on") {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    child.kill().ok();
                    panic!("timed out waiting for vexobj to start on port {}", port);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // stderr closed -- server probably crashed
                    let status = child.wait().ok();
                    panic!(
                        "vexobj exited unexpectedly on port {} (status: {:?})",
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
        let mut stream = std::net::TcpStream::connect(host_port).expect("connect to test server");
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
    assert_eq!(body["service"], "vexobj");
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
        .header("Authorization", "Bearer vex_invalid_key")
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
    let data = b"hello vexobj e2e test";
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
        .put(format!("{}/v1/objects/presign-bucket/secret.txt", srv.url))
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
    assert!(body.contains("vexobj_requests_total"));
    assert!(body.contains("vexobj_requests_by_method_total"));
    assert!(body.contains("vexobj_request_duration_seconds"));
    assert!(body.contains("vexobj_bytes_uploaded_total"));

    // The request count should be > 0 since we made requests above
    assert!(body.contains("vexobj_requests_by_method_total{method=\"GET\"}"));
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
    let latest_row = versions
        .iter()
        .find(|v| v["version_id"] == latest_id)
        .unwrap();
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
async fn e2e_replication_promote_clears_cursor() {
    // Simulate: primary is gone, replica caught up to some events,
    // operator runs `vexobjctl promote`. We verify the command runs
    // to success and deletes the cursor file so a later replicate
    // call can't silently replay from 0 against the dead primary.
    let primary = TestServer::start();
    let replica = TestServer::start();
    let client = primary.client();

    // Minimal primary activity so the replica has a non-zero cursor.
    client
        .post(format!("{}/v1/buckets", primary.url))
        .header("Authorization", primary.auth_header())
        .json(&serde_json::json!({"name": "promo-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/promo-bucket/note.txt", primary.url))
        .header("Authorization", primary.auth_header())
        .body("hello")
        .send()
        .await
        .unwrap();

    let cursor = std::env::temp_dir().join(format!("cursor-{}", uuid::Uuid::new_v4()));
    let binary = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/debug/vexobjctl");

    // Replicate once so the cursor file exists and holds a real id.
    let sync_out = Command::new(&binary)
        .args([
            "--url",
            &replica.url,
            "--key",
            &replica.admin_key,
            "replicate",
            "--primary",
            &primary.url,
            "--primary-key",
            &primary.admin_key,
            "--cursor-file",
        ])
        .arg(&cursor)
        .output()
        .expect("run replicate");
    assert!(
        sync_out.status.success(),
        "replicate must succeed: {}",
        String::from_utf8_lossy(&sync_out.stderr)
    );
    assert!(cursor.exists(), "replicate should have created cursor file");
    let before: i64 = std::fs::read_to_string(&cursor)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        before >= 1,
        "cursor should reflect at least one applied event"
    );

    // Now promote the replica. Cursor file should disappear.
    let promo_out = Command::new(&binary)
        .args([
            "--url",
            &replica.url,
            "--key",
            &replica.admin_key,
            "promote",
            "--cursor-file",
        ])
        .arg(&cursor)
        .output()
        .expect("run promote");
    assert!(
        promo_out.status.success(),
        "promote must succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&promo_out.stdout),
        String::from_utf8_lossy(&promo_out.stderr),
    );
    let stdout = String::from_utf8_lossy(&promo_out.stdout);
    assert!(
        stdout.contains("cursor file deleted"),
        "stdout should mention cursor deletion, got: {stdout}"
    );
    assert!(
        stdout.contains(&format!("last applied event id = {before}")),
        "stdout should report the checkpointed id, got: {stdout}"
    );
    assert!(!cursor.exists(), "cursor must be deleted post-promote");

    // --keep-cursor keeps the file even on success
    std::fs::write(&cursor, "42").unwrap();
    let keep_out = Command::new(&binary)
        .args([
            "--url",
            &replica.url,
            "--key",
            &replica.admin_key,
            "promote",
            "--keep-cursor",
            "--cursor-file",
        ])
        .arg(&cursor)
        .output()
        .expect("run promote --keep-cursor");
    assert!(keep_out.status.success());
    assert!(
        cursor.exists(),
        "--keep-cursor must leave the file in place"
    );
    let _ = std::fs::remove_file(&cursor);
}

/// Generate a tiny valid MP4 via ffmpeg for the video-metadata test.
/// Returns None when ffmpeg isn't on PATH so the caller can skip
/// gracefully on CI hosts that don't ship it.
fn ffmpeg_small_mp4() -> Option<PathBuf> {
    let out = std::env::temp_dir().join(format!("vfs-e2e-{}.mp4", uuid::Uuid::new_v4()));
    let status = Command::new("ffmpeg")
        .args([
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=0x10b981:size=320x240:duration=1",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-y",
        ])
        .arg(&out)
        .status()
        .ok()?;
    if !status.success() {
        return None;
    }
    Some(out)
}

#[tokio::test]
async fn e2e_storage_backend_config_parses_local() {
    // Minimal smoke test for the backend selector: a freshly-started
    // server with the default config should report "local" at boot.
    // We can't test the S3 path without a live S3-compatible endpoint,
    // which belongs in a separate docker-compose-driven suite.
    let srv = TestServer::start_with_env(&[("VEXOBJ_STORAGE_BACKEND", "local")]);
    let client = srv.client();
    let auth = srv.auth_header();

    // The health endpoint doesn't currently publish the backend name
    // (feature for a later commit); for now we just assert the server
    // comes up and serves objects normally under the new code path.
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "backend-test", "public": false}))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/backend-test/hello.txt", srv.url))
        .header("Authorization", &auth)
        .body("backend works")
        .send()
        .await
        .unwrap();
    let got = client
        .get(format!("{}/v1/objects/backend-test/hello.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(got, "backend works");

    // Unknown backends should fail at startup. We check this by
    // spawning a server with a bogus backend and expecting the
    // harness to panic during the startup wait — done in a
    // separate function so other tests don't inherit the env.
}

#[tokio::test]
async fn e2e_transcode_queue_cap_rejects_with_429() {
    // Start the server with max_pending=1 so a single unclaimed job
    // fills the queue. We block the worker by setting workers=0 via
    // env var — the pending job never drains, the second submission
    // sees the cap and gets rejected.
    let Some(mp4_path) = ffmpeg_small_mp4() else {
        eprintln!("SKIP: ffmpeg not available");
        return;
    };

    let srv = TestServer::start_with_env(&[
        ("VEXOBJ_TRANSCODE_WORKERS", "0"),
        ("VEXOBJ_TRANSCODE_MAX_PENDING", "1"),
    ]);
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "queue-cap", "public": false}))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/queue-cap/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "video/mp4")
        .body(std::fs::read(&mp4_path).unwrap())
        .send()
        .await
        .unwrap();

    // First submission goes through.
    let ok = client
        .post(format!("{}/v1/transcode/queue-cap/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"profile": "mp4-480p"}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 202);

    // Second submission hits the 1-job cap → 429 with retry-after.
    let blocked = client
        .post(format!("{}/v1/transcode/queue-cap/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"profile": "mp4-480p"}))
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), 429);
    assert_eq!(blocked.headers().get("retry-after").unwrap(), "30");
    let body: Value = blocked.json().await.unwrap();
    assert_eq!(body["max_pending"], 1);

    let _ = std::fs::remove_file(&mp4_path);
}

#[tokio::test]
async fn e2e_video_transcode_job_flow() {
    let Some(mp4_path) = ffmpeg_small_mp4() else {
        eprintln!("SKIP: ffmpeg not available");
        return;
    };

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "transcode-test", "public": false}))
        .send()
        .await
        .unwrap();

    // Profiles endpoint advertises at least the three built-ins.
    let profiles: Value = client
        .get(format!("{}/v1/transcode/profiles", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let names: Vec<&str> = profiles["profiles"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"webm-720p"));
    assert!(names.contains(&"mp4-480p"));
    assert!(names.contains(&"mp3-audio"));

    client
        .put(format!("{}/v1/objects/transcode-test/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "video/mp4")
        .body(std::fs::read(&mp4_path).unwrap())
        .send()
        .await
        .unwrap();

    // Submit a transcode to mp4-480p (faster than webm-720p in CI).
    let submit = client
        .post(format!("{}/v1/transcode/transcode-test/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"profile": "mp4-480p"}))
        .send()
        .await
        .unwrap();
    assert_eq!(submit.status(), 202);
    let job: Value = submit.json().await.unwrap();
    let job_id = job["id"].as_str().unwrap().to_string();
    assert_eq!(job["status"], "pending");

    // Poll until the worker finishes or times out. 60s is generous;
    // a 1s 320x240 clip transcodes in well under a second on modern
    // hardware even at preset=fast.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let final_job = loop {
        let j: Value = client
            .get(format!("{}/v1/transcode/jobs/{}", srv.url, job_id))
            .header("Authorization", &auth)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if j["status"] == "completed" || j["status"] == "failed" {
            break j;
        }
        if std::time::Instant::now() > deadline {
            panic!("transcode job never finished: last status = {j}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    assert_eq!(
        final_job["status"], "completed",
        "job should complete: {final_job}"
    );
    let output_key = final_job["output_key"].as_str().unwrap();
    assert!(output_key.ends_with(".mp4-480p.mp4"));

    // Variant is now a first-class object — fetch it and verify it's
    // actually an MP4 (starts with an ftyp box at offset 4).
    let resp = client
        .get(format!(
            "{}/v1/objects/transcode-test/{}",
            srv.url, output_key
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.headers().get("content-type").unwrap(), "video/mp4");
    let body = resp.bytes().await.unwrap();
    assert!(body.len() > 200, "variant suspiciously small");
    // MP4 containers start with a box: [size u32 BE][ftyp].
    assert_eq!(&body[4..8], b"ftyp", "not an MP4 container");

    // Submitting an unknown profile returns 400 with the list of
    // available names — no forever-pending job.
    let bad = client
        .post(format!("{}/v1/transcode/transcode-test/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"profile": "bogus"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    let _ = std::fs::remove_file(&mp4_path);
}

#[tokio::test]
async fn e2e_video_thumbnail_endpoint() {
    let Some(mp4_path) = ffmpeg_small_mp4() else {
        eprintln!("SKIP: ffmpeg not available");
        return;
    };

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // /health now surfaces capabilities — ffmpeg should be on for the
    // host that just ran ffmpeg_small_mp4 successfully.
    let health: Value = client
        .get(format!("{}/health", srv.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["capabilities"]["video_thumbnails"], true);
    assert_eq!(health["capabilities"]["ffmpeg"], true);

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "thumbs", "public": false}))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/thumbs/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "video/mp4")
        .body(std::fs::read(&mp4_path).unwrap())
        .send()
        .await
        .unwrap();

    // First request: cache miss, jpeg body, looks like JPEG (FFD8 FF).
    let r = client
        .get(format!(
            "{}/v1/objects/thumbs/clip.mp4?thumbnail=1&w=200&t=0.5",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers().get("content-type").unwrap(), "image/jpeg");
    assert_eq!(r.headers().get("x-vexobj-cache").unwrap(), "miss");
    let body = r.bytes().await.unwrap();
    // A 200×N JPEG of a solid-color test frame compresses to a few
    // hundred bytes; anything under ~100 B would be a truncated write.
    assert!(
        body.len() > 100,
        "thumbnail suspiciously small: {} bytes",
        body.len()
    );
    assert_eq!(&body[..3], &[0xFF, 0xD8, 0xFF], "not a JPEG");

    // Second request with the same params: cache hit.
    let r2 = client
        .get(format!(
            "{}/v1/objects/thumbs/clip.mp4?thumbnail=1&w=200&t=0.5",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.headers().get("x-vexobj-cache").unwrap(), "hit");

    // WebP variant — different cache key, different magic bytes.
    let r3 = client
        .get(format!(
            "{}/v1/objects/thumbs/clip.mp4?thumbnail=1&w=200&t=0.5&format=webp",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 200);
    assert_eq!(r3.headers().get("content-type").unwrap(), "image/webp");
    let body3 = r3.bytes().await.unwrap();
    // WebP starts with RIFF....WEBP.
    assert_eq!(&body3[..4], b"RIFF");
    assert_eq!(&body3[8..12], b"WEBP");

    // Thumbnail on a non-video → 400 with a clear error.
    client
        .put(format!("{}/v1/objects/thumbs/not-a-video.txt", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "text/plain")
        .body("hello")
        .send()
        .await
        .unwrap();
    let bad = client
        .get(format!(
            "{}/v1/objects/thumbs/not-a-video.txt?thumbnail=1",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let bad_body: Value = bad.json().await.unwrap();
    assert!(bad_body["error"].as_str().unwrap().contains("non-video"));

    let _ = std::fs::remove_file(&mp4_path);
}

#[tokio::test]
async fn e2e_video_metadata_on_upload() {
    let Some(mp4_path) = ffmpeg_small_mp4() else {
        eprintln!("SKIP: ffmpeg not available");
        return;
    };

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "videos", "public": false}))
        .send()
        .await
        .unwrap();

    let body = std::fs::read(&mp4_path).unwrap();
    let put = client
        .put(format!("{}/v1/objects/videos/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "video/mp4")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);
    let meta: Value = put.json().await.unwrap();

    // The server should have parsed and stashed the video metadata in
    // the object's JSON metadata blob.
    let video = &meta["metadata"]["video"];
    assert!(
        video.is_object(),
        "expected metadata.video to exist, got: {meta}"
    );
    assert_eq!(video["width"], 320);
    assert_eq!(video["height"], 240);
    assert!(
        (video["duration_secs"].as_f64().unwrap() - 1.0).abs() < 0.2,
        "duration should be ~1.0s, got {}",
        video["duration_secs"]
    );
    assert_eq!(video["codec"], "h264");
    assert_eq!(video["has_audio"], false);

    // HEAD should surface the same values via x-vexobj-video-* headers.
    let head = client
        .head(format!("{}/v1/objects/videos/clip.mp4", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200);
    let h = head.headers();
    assert_eq!(h.get("x-vexobj-video-width").unwrap(), "320");
    assert_eq!(h.get("x-vexobj-video-height").unwrap(), "240");
    assert_eq!(h.get("x-vexobj-video-codec").unwrap(), "h264");
    assert!(h.get("x-vexobj-video-duration").is_some());

    // A non-video upload must not grow any video metadata.
    let png_bytes = [
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44,
        0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90,
        0x77, 0x53, 0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x08, 0x99, 0x63, 0xF8,
        0xCF, 0xC0, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x5B, 0x0A, 0x3E, 0x42, 0x00, 0x00, 0x00,
        0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
    ];
    let resp = client
        .put(format!("{}/v1/objects/videos/pixel.png", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "image/png")
        .body(png_bytes.to_vec())
        .send()
        .await
        .unwrap();
    let m: Value = resp.json().await.unwrap();
    assert!(
        m["metadata"].get("video").is_none() || m["metadata"]["video"].is_null(),
        "non-video upload must not grow metadata.video, got: {m}"
    );

    let _ = std::fs::remove_file(&mp4_path);
}

#[tokio::test]
async fn e2e_replication_two_node_sync() {
    // Spin up two independent servers and drive a one-shot replicate
    // from "primary" to "replica" via the vexobjctl binary.
    let primary = TestServer::start();
    let replica = TestServer::start();
    let client = primary.client();

    // Write a variety of operations on the primary so the replica has
    // something to catch up on: bucket create → puts → overwrite →
    // delete. Each of these appends at least one event on the primary.
    client
        .post(format!("{}/v1/buckets", primary.url))
        .header("Authorization", primary.auth_header())
        .json(&serde_json::json!({"name": "mirror", "public": false}))
        .send()
        .await
        .unwrap();
    for (key, body) in [
        ("one.txt", "1111"),
        ("two.txt", "2222"),
        ("three.txt", "3333"),
    ] {
        client
            .put(format!("{}/v1/objects/mirror/{}", primary.url, key))
            .header("Authorization", primary.auth_header())
            .body(body.to_string())
            .send()
            .await
            .unwrap();
    }
    // Overwrite one key so we exercise multiple puts on the same row.
    client
        .put(format!("{}/v1/objects/mirror/one.txt", primary.url))
        .header("Authorization", primary.auth_header())
        .body("1111-v2".to_string())
        .send()
        .await
        .unwrap();
    // Delete one so we cover the delete event on the replica too.
    client
        .delete(format!("{}/v1/objects/mirror/two.txt", primary.url))
        .header("Authorization", primary.auth_header())
        .send()
        .await
        .unwrap();

    // Run vexobjctl replicate once. Primary and replica each have their
    // own admin key; we pass both explicitly.
    let cursor = std::env::temp_dir().join(format!("cursor-{}", uuid::Uuid::new_v4()));
    let binary = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/debug/vexobjctl");
    assert!(
        binary.exists(),
        "vexobjctl binary not found at {}. Run `cargo build` first.",
        binary.display()
    );

    let out = Command::new(&binary)
        .args([
            "--url",
            &replica.url,
            "--key",
            &replica.admin_key,
            "replicate",
            "--primary",
            &primary.url,
            "--primary-key",
            &primary.admin_key,
            "--cursor-file",
        ])
        .arg(&cursor)
        .output()
        .expect("run vexobjctl replicate");
    assert!(
        out.status.success(),
        "vexobjctl replicate failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // The replica should now mirror the primary: present keys present,
    // deleted key gone, overwritten key at its latest value.
    let one = client
        .get(format!("{}/v1/objects/mirror/one.txt", replica.url))
        .header("Authorization", replica.auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(one.status(), 200);
    assert_eq!(one.text().await.unwrap(), "1111-v2");

    let three = client
        .get(format!("{}/v1/objects/mirror/three.txt", replica.url))
        .header("Authorization", replica.auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(three.status(), 200);
    assert_eq!(three.text().await.unwrap(), "3333");

    let two = client
        .get(format!("{}/v1/objects/mirror/two.txt", replica.url))
        .header("Authorization", replica.auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(two.status(), 404, "deleted key should not exist on replica");

    // Cursor file should be set to >=5 (3 puts + 1 overwrite + 1 delete).
    let cursor_val: i64 = std::fs::read_to_string(&cursor)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(cursor_val >= 5, "cursor should advance, got {cursor_val}");
    let _ = std::fs::remove_file(&cursor);
}

#[tokio::test]
async fn e2e_replication_event_log() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Fresh server — no events yet
    let resp = client
        .get(format!("{}/v1/replication/cursor", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["latest_id"], 0);

    // Drive some writes — put creates events, delete creates one more
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({"name": "repl-bucket", "public": false}))
        .send()
        .await
        .unwrap();
    for (key, body) in [("a.txt", "alpha"), ("b.txt", "bravo"), ("c.txt", "charlie")] {
        client
            .put(format!("{}/v1/objects/repl-bucket/{}", srv.url, key))
            .header("Authorization", &auth)
            .body(body.to_string())
            .send()
            .await
            .unwrap();
    }
    client
        .delete(format!("{}/v1/objects/repl-bucket/b.txt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();

    // All events since cursor=0 — expect 3 puts + 1 delete, in order
    let resp = client
        .get(format!("{}/v1/replication/events?since=0", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let events = body["events"].as_array().unwrap();
    assert_eq!(events.len(), 4, "expected 3 puts + 1 delete");
    assert_eq!(events[0]["op"], "put");
    assert_eq!(events[0]["key"], "a.txt");
    assert!(events[0]["sha256"].as_str().unwrap().len() == 64);
    assert_eq!(events[3]["op"], "delete");
    assert_eq!(events[3]["key"], "b.txt");
    assert!(body["latest_id"].as_i64().unwrap() >= 4);

    // Pagination: since=2 should return only the 3rd put and the delete
    let resp = client
        .get(format!(
            "{}/v1/replication/events?since=2&limit=10",
            srv.url
        ))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let events = body["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["id"], 3);
    assert_eq!(events[1]["id"], 4);

    // Blob fetch by sha256 — use the hash from the first put
    let first_event = client
        .get(format!("{}/v1/replication/events?since=0&limit=1", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let sha = first_event["events"][0]["sha256"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = client
        .get(format!("{}/v1/replication/blob/{}", srv.url, sha))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body_bytes = resp.bytes().await.unwrap();
    // With SSE off (default), the bytes on disk are the raw plaintext
    assert_eq!(&body_bytes[..], b"alpha");

    // Malformed sha rejected
    let resp = client
        .get(format!("{}/v1/replication/blob/not-a-sha", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Unknown sha → 404
    let missing = "0".repeat(64);
    let resp = client
        .get(format!("{}/v1/replication/blob/{}", srv.url, missing))
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
        !on_disk
            .windows(payload.len())
            .any(|w| w == payload.as_bytes()),
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
    let future = (chrono::Utc::now() + chrono::Duration::days(30)).to_rfc3339();
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
    let soon = (chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339();
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
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "etag quoted: {etag}"
    );

    // HEAD object
    let resp = client
        .head(format!("{}/s3/s3obj/hello.txt", srv.url))
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
    assert_eq!(
        resp.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
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

    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
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

    // The admin key is the full `vex_...` string — our server accepts the
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
    assert!(resp
        .text()
        .await
        .unwrap()
        .contains("<ListAllMyBucketsResult"));

    // Now tamper with the signature — flip the last hex char to a
    // definitely-different one (0→1, anything-else→0) so we can't
    // accidentally re-emit the same signature.
    let mut bad = signed.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == '0' { '1' } else { '0' });
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
    let resp = client.get(format!("{}/s3", srv.url)).send().await.unwrap();
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
    assert!(body["command"].as_str().unwrap().contains("vexobjctl"));
}

/// Quota enforcement must apply to every write path — native streaming PUT,
/// multipart form upload, S3 PUT, and S3 CopyObject. Previously only the
/// native JSON route checked quotas, so clients using /s3/ or the form
/// upload could silently bust the cap.
#[tokio::test]
async fn e2e_quota_enforced_on_every_write_path() {
    use serde_json::json;

    let srv = TestServer::start_with_env(&[
        ("VEXOBJ_QUOTAS_ENABLED", "true"),
        ("VEXOBJ_QUOTAS_MAX_STORAGE", "2048"),
        ("VEXOBJ_QUOTAS_MAX_OBJECTS", "100"),
    ]);
    let client = srv.client();
    let auth = srv.auth_header();

    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "qtest", "public": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Push the bucket above its 2048-byte cap using native streaming PUTs.
    // Streaming can't know the final size up front, so the engine only
    // rejects when *already* over the cap — two 1500-byte writes land first
    // and leave the bucket at 3000 bytes.
    for i in 0..2 {
        let resp = client
            .put(format!("{}/v1/objects/qtest/seed-{i}", srv.url))
            .header("Authorization", &auth)
            .header("content-type", "application/octet-stream")
            .body(vec![b'x'; 1500])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201, "seed put {i} should succeed");
    }

    // 1. Native streaming PUT — over cap now, must be rejected with 507.
    let resp = client
        .put(format!("{}/v1/objects/qtest/blocked-native", srv.url))
        .header("Authorization", &auth)
        .body(vec![b'y'; 100])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 507, "native streaming PUT over quota");

    // 2. Multipart form upload — the outer response surfaces 507 when any
    //    file in the batch was quota-blocked.
    let form = reqwest::multipart::Form::new().part(
        "f",
        reqwest::multipart::Part::bytes(vec![b'm'; 100]).file_name("blocked.bin"),
    );
    let resp = client
        .post(format!("{}/v1/upload/qtest", srv.url))
        .header("Authorization", &auth)
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 507, "multipart form over quota");

    // 3. S3 PUT — maps to the `ServiceUnavailable` S3 error code with a
    //    507 status (storage full is the closest S3 idiom to a quota).
    let resp = client
        .put(format!("{}/s3/qtest/blocked-s3", srv.url))
        .header("Authorization", &auth)
        .body(vec![b's'; 100])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 507, "S3 PUT over quota");

    // 4. S3 CopyObject — the destination bucket is the same full one, so
    //    even a zero-payload copy must fail.
    let resp = client
        .put(format!("{}/s3/qtest/blocked-copy", srv.url))
        .header("Authorization", &auth)
        .header("x-amz-copy-source", "qtest/seed-0")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 507, "S3 copy over quota");
}

/// Full S3-spec multipart upload round-trip: InitiateMultipartUpload →
/// UploadPart × 3 (two 5 MiB parts + one small tail) → CompleteMultipartUpload
/// → HEAD verifies the assembled object matches. Also exercises AbortMultipartUpload
/// and the 5-MiB-minimum rule on non-last parts.
#[tokio::test]
async fn e2e_s3_multipart_upload_roundtrip() {
    use serde_json::json;

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // Create the bucket via the native API so we don't have to implement
    // CreateBucket in the test harness. The S3 mux shares the same storage.
    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "mpt", "public": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 1. InitiateMultipartUpload — POST /s3/mpt/big.bin?uploads
    let resp = client
        .post(format!("{}/s3/mpt/big.bin?uploads", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    let upload_id = extract_xml_tag(&body, "UploadId").expect("UploadId present");
    assert!(!upload_id.is_empty());

    // 2. Build three parts: 5 MiB, 5 MiB, 12 bytes.
    let part1 = vec![b'A'; 5 * 1024 * 1024];
    let part2 = vec![b'B'; 5 * 1024 * 1024];
    let part3 = b"tail-bytes!!".to_vec();
    let mut etags = Vec::new();
    for (pn, data) in [(1u32, &part1), (2u32, &part2), (3u32, &part3)] {
        let resp = client
            .put(format!(
                "{}/s3/mpt/big.bin?uploadId={}&partNumber={}",
                srv.url, upload_id, pn
            ))
            .header("Authorization", &auth)
            .body(data.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "UploadPart {pn}");
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .trim_matches('"')
            .to_string();
        assert_eq!(etag.len(), 64, "etag should be 64-char sha-256 hex");
        etags.push(etag);
    }

    // 3. ListParts should show all three parts in ascending order.
    let resp = client
        .get(format!("{}/s3/mpt/big.bin?uploadId={}", srv.url, upload_id))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let list_body = resp.text().await.unwrap();
    assert_eq!(list_body.matches("<PartNumber>").count(), 3);
    assert!(list_body.contains("<PartNumber>1</PartNumber>"));
    assert!(list_body.contains("<PartNumber>3</PartNumber>"));

    // 4. CompleteMultipartUpload — POST with XML listing the parts.
    let complete_body = format!(
        r#"<?xml version="1.0"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"{}"</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"{}"</ETag></Part>
  <Part><PartNumber>3</PartNumber><ETag>"{}"</ETag></Part>
</CompleteMultipartUpload>"#,
        etags[0], etags[1], etags[2]
    );
    let resp = client
        .post(format!("{}/s3/mpt/big.bin?uploadId={}", srv.url, upload_id))
        .header("Authorization", &auth)
        .header("Content-Type", "application/xml")
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "CompleteMultipartUpload");
    let complete_body = resp.text().await.unwrap();
    let final_etag =
        extract_xml_tag(&complete_body, "ETag").expect("final ETag in complete response");
    assert_eq!(
        final_etag.trim_matches('"').len(),
        64,
        "final etag is sha-256 hex"
    );

    // 5. GET the assembled object — must match part1 || part2 || part3.
    let resp = client
        .get(format!("{}/s3/mpt/big.bin", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let fetched = resp.bytes().await.unwrap();
    let expected_len = part1.len() + part2.len() + part3.len();
    assert_eq!(fetched.len(), expected_len, "reassembled length");
    assert_eq!(&fetched[..part1.len()], &part1[..]);
    assert_eq!(&fetched[part1.len()..part1.len() + part2.len()], &part2[..]);
    assert_eq!(&fetched[part1.len() + part2.len()..], &part3[..]);

    // 6. After Complete, the upload_id must be gone (NoSuchUpload on
    //    further UploadPart / ListParts).
    let resp = client
        .get(format!("{}/s3/mpt/big.bin?uploadId={}", srv.url, upload_id))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // 7. Abort path: start a new upload, upload one part, abort, confirm
    //    both the upload row and the scratch file go away.
    let resp = client
        .post(format!("{}/s3/mpt/abort-me?uploads", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    let abort_id = extract_xml_tag(&body, "UploadId").unwrap();
    let _ = client
        .put(format!(
            "{}/s3/mpt/abort-me?uploadId={}&partNumber=1",
            srv.url, abort_id
        ))
        .header("Authorization", &auth)
        .body(vec![b'Z'; 1024])
        .send()
        .await
        .unwrap();
    let resp = client
        .delete(format!("{}/s3/mpt/abort-me?uploadId={}", srv.url, abort_id))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 8. Enforce the 5-MiB-minimum rule on non-last parts. Initiate,
    //    upload two tiny parts, try to Complete — must be rejected.
    let resp = client
        .post(format!("{}/s3/mpt/tiny?uploads", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let tiny_id = extract_xml_tag(&resp.text().await.unwrap(), "UploadId").unwrap();
    let mut tiny_etags = Vec::new();
    for pn in [1u32, 2u32] {
        let resp = client
            .put(format!(
                "{}/s3/mpt/tiny?uploadId={}&partNumber={}",
                srv.url, tiny_id, pn
            ))
            .header("Authorization", &auth)
            .body(vec![b'T'; 100])
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .trim_matches('"')
            .to_string();
        tiny_etags.push(etag);
    }
    let complete = format!(
        r#"<?xml version="1.0"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"{}"</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"{}"</ETag></Part>
</CompleteMultipartUpload>"#,
        tiny_etags[0], tiny_etags[1]
    );
    let resp = client
        .post(format!("{}/s3/mpt/tiny?uploadId={}", srv.url, tiny_id))
        .header("Authorization", &auth)
        .header("Content-Type", "application/xml")
        .body(complete)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "Complete with tiny non-last parts must be rejected (InvalidPart)"
    );
}

/// Extract the inner text of the first `<tag>…</tag>` from an XML string.
/// Used only by the multipart test; keeps dep-tree free of a full parser.
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].trim().to_string())
}

/// Public buckets must allow anonymous GET/HEAD of their objects without an
/// API key — Mastodon / Peertube serve media directly from VexObj to
/// unauthenticated browsers, so the `public` flag on a bucket has to
/// actually waive auth for the narrow read path. Writes, lists, and access
/// to private buckets all stay locked.
#[tokio::test]
async fn e2e_public_bucket_allows_anonymous_object_reads() {
    use serde_json::json;

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    // One public, one private.
    for (name, is_public) in [("public-assets", true), ("private-docs", false)] {
        let resp = client
            .post(format!("{}/v1/buckets", srv.url))
            .header("Authorization", &auth)
            .json(&json!({ "name": name, "public": is_public }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201, "create {name}");
    }
    for name in ["public-assets", "private-docs"] {
        let resp = client
            .put(format!("{}/v1/objects/{}/logo.png", srv.url, name))
            .header("Authorization", &auth)
            .header("content-type", "image/png")
            .body(b"fake-png-bytes".to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
    }

    let anon = reqwest::Client::new();

    // Anonymous GET on the public bucket — body comes back intact.
    let resp = anon
        .get(format!("{}/v1/objects/public-assets/logo.png", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "anon GET on public native route");
    assert_eq!(&resp.bytes().await.unwrap()[..], b"fake-png-bytes");

    let resp = anon
        .head(format!("{}/v1/objects/public-assets/logo.png", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "anon HEAD on public native route");

    let resp = anon
        .get(format!("{}/s3/public-assets/logo.png", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "anon GET on public S3 route");
    assert_eq!(&resp.bytes().await.unwrap()[..], b"fake-png-bytes");

    // Private bucket stays locked.
    let resp = anon
        .get(format!("{}/v1/objects/private-docs/logo.png", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Listing a public bucket is NOT anonymously allowed — the `public`
    // flag unlocks reads by key, not the index.
    let resp = anon
        .get(format!("{}/v1/objects/public-assets", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "anon list of public bucket is forbidden"
    );

    // Writes to a public bucket require auth. Public ≠ writable.
    let resp = anon
        .put(format!("{}/v1/objects/public-assets/evil.png", srv.url))
        .body(b"anon-write".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "anon PUT on public bucket is forbidden");
}

/// Regression guard: `aws s3 cp` downloads big files in parallel byte-range
/// requests. When the S3 GET handler ignored `Range:` and served the full
/// body every time, clients wrote each chunk at the wrong offset and the
/// downloaded file corrupted silently. Every byte-range request must now
/// return 206 with only the requested slice.
#[tokio::test]
async fn e2e_s3_get_honors_range_header() {
    use serde_json::json;

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    let resp = client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "ranged", "public": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // 12 distinct byte values so we can spot a misaligned slice by eye.
    let body: Vec<u8> = (0..12).flat_map(|i| vec![i as u8; 1024]).collect();
    let resp = client
        .put(format!("{}/v1/objects/ranged/blob.bin", srv.url))
        .header("Authorization", &auth)
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Middle range: bytes 1024..=2047 → the second 1 KiB slab, all `1`s.
    let resp = client
        .get(format!("{}/s3/ranged/blob.bin", srv.url))
        .header("Authorization", &auth)
        .header("Range", "bytes=1024-2047")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "range requests must return 206");
    assert_eq!(
        resp.headers().get("content-range").unwrap(),
        format!("bytes 1024-2047/{}", body.len()).as_str()
    );
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 1024);
    assert!(bytes.iter().all(|&b| b == 1), "slab should be all 1s");

    // Open-ended range `bytes=N-` returns N..EOF.
    let resp = client
        .get(format!("{}/s3/ranged/blob.bin", srv.url))
        .header("Authorization", &auth)
        .header("Range", "bytes=10240-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), 2048); // last two slabs
    assert!(bytes[..1024].iter().all(|&b| b == 10));
    assert!(bytes[1024..].iter().all(|&b| b == 11));

    // Suffix form `bytes=-N` returns the last N bytes.
    let resp = client
        .get(format!("{}/s3/ranged/blob.bin", srv.url))
        .header("Authorization", &auth)
        .header("Range", "bytes=-1024")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert!(resp.bytes().await.unwrap().iter().all(|&b| b == 11));

    // Range entirely past EOF is 416 per RFC 9110.
    let resp = client
        .get(format!("{}/s3/ranged/blob.bin", srv.url))
        .header("Authorization", &auth)
        .header("Range", "bytes=99999-200000")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 416);
}

// ---------------------------------------------------------------------------
// S3 presigned-POST (browser-style upload)
// ---------------------------------------------------------------------------
//
// The presigned-POST flow is how Mastodon / Pixelfed / Cloudinary-style
// browser uploaders send files to S3 without exposing the secret key: the
// backend hands the browser a base64 policy + signature, the browser POSTs
// multipart/form-data to /s3/<bucket> with those fields + the file, and the
// server verifies everything before writing. No Authorization header.
//
// Spec reference: AWS "Creating a POST Policy"; our impl lives in
// crates/vexobj-s3-compat/src/presigned_post.rs.

/// Build a multipart/form-data body for a presigned POST. Returns a ready-to-send
/// `reqwest::multipart::Form`. Field order matches the AWS spec — `file` is the
/// last part so the server knows where the policy fields end.
///
/// `override_signature` and `override_algorithm` are for negative tests that
/// need to break exactly one piece without re-deriving everything.
#[allow(clippy::too_many_arguments)]
fn presigned_post_form(
    key: &str,
    access_key: &str,
    secret: &str,
    conditions: Vec<Value>,
    expiration: chrono::DateTime<chrono::Utc>,
    file_bytes: Vec<u8>,
    override_signature: Option<String>,
    override_algorithm: Option<&str>,
    override_amz_date: Option<String>,
) -> reqwest::multipart::Form {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let now = chrono::Utc::now();
    let amz_date = override_amz_date.unwrap_or_else(|| now.format("%Y%m%dT%H%M%SZ").to_string());
    let scope_date = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let service = "s3";
    let credential = format!("{access_key}/{scope_date}/{region}/{service}/aws4_request");
    let algorithm = override_algorithm.unwrap_or("AWS4-HMAC-SHA256");

    let policy_json = serde_json::json!({
        "expiration": expiration.to_rfc3339(),
        "conditions": conditions,
    });
    let policy_str = serde_json::to_string(&policy_json).unwrap();
    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy_str.as_bytes());

    let signature = override_signature.unwrap_or_else(|| {
        let mac = |k: &[u8], d: &[u8]| -> Vec<u8> {
            let mut m = HmacSha256::new_from_slice(k).unwrap();
            m.update(d);
            m.finalize().into_bytes().to_vec()
        };
        let k_date = mac(format!("AWS4{secret}").as_bytes(), scope_date.as_bytes());
        let k_region = mac(&k_date, region.as_bytes());
        let k_service = mac(&k_region, service.as_bytes());
        let k_signing = mac(&k_service, b"aws4_request");
        hex::encode(mac(&k_signing, policy_b64.as_bytes()))
    });

    reqwest::multipart::Form::new()
        .text("key", key.to_string())
        .text("x-amz-algorithm", algorithm.to_string())
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("policy", policy_b64)
        .text("x-amz-signature", signature)
        .part(
            "file",
            reqwest::multipart::Part::bytes(file_bytes).file_name("upload.bin"),
        )
}

/// Ensure a bucket exists so each test starts from a clean slate without
/// duplicating the native-API boilerplate.
async fn create_bucket(srv: &TestServer, name: &str) {
    use serde_json::json;
    let resp = srv
        .client()
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", srv.auth_header())
        .json(&json!({ "name": name, "public": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create bucket {name}");
}

#[tokio::test]
async fn e2e_s3_presigned_post_happy_path() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost").await;

    let body = b"hello from presigned POST".to_vec();
    let form = presigned_post_form(
        "uploads/hello.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost" }),
            serde_json::json!(["starts-with", "$key", "uploads/"]),
            serde_json::json!(["content-length-range", 1, body.len() as u64 + 10]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        body.clone(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 204, "valid presigned POST should 204");
    assert_eq!(
        resp.headers().get("location").unwrap(),
        "/ppost/uploads/hello.txt"
    );

    // The object must really exist with the right bytes.
    let dl = client
        .get(format!("{}/s3/ppost/uploads/hello.txt", srv.url))
        .header("Authorization", srv.auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(dl.status(), 200);
    assert_eq!(dl.bytes().await.unwrap().to_vec(), body);
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_bad_signature() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-bad-sig").await;

    let form = presigned_post_form(
        "f.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-bad-sig" }),
            serde_json::json!(["starts-with", "$key", ""]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"anything".to_vec(),
        Some("0".repeat(64)),
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-bad-sig", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "tampered signature must be rejected");
    assert!(resp.text().await.unwrap().contains("AccessDenied"));
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_expired_policy() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-expired").await;

    let form = presigned_post_form(
        "f.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-expired" }),
            serde_json::json!(["starts-with", "$key", ""]),
        ],
        // Well in the past.
        chrono::Utc::now() - chrono::Duration::hours(1),
        b"anything".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-expired", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "expired policy must be rejected");
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_bucket_mismatch() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-mismatch").await;
    create_bucket(&srv, "ppost-other").await;

    // Policy says "ppost-other", URL targets "ppost-mismatch".
    let form = presigned_post_form(
        "f.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-other" }),
            serde_json::json!(["starts-with", "$key", ""]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"anything".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-mismatch", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "bucket mismatch must be rejected");
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_missing_bucket_condition() {
    // A policy with no {"bucket": ...} condition can't be safely scoped to a
    // bucket — the handler must refuse it even if everything else is valid.
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-nobucket").await;

    let form = presigned_post_form(
        "f.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![serde_json::json!(["starts-with", "$key", ""])],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"anything".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-nobucket", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn e2e_s3_presigned_post_enforces_exact_key_rule() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-exact").await;

    // Policy pins the key, but the form field tries a different one.
    let form = presigned_post_form(
        "intruder.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-exact" }),
            serde_json::json!({ "key": "allowed.txt" }),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"x".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-exact", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "exact-key violation must be rejected");

    // And the matching key must succeed.
    let form_ok = presigned_post_form(
        "allowed.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-exact" }),
            serde_json::json!({ "key": "allowed.txt" }),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"x".to_vec(),
        None,
        None,
        None,
    );
    let resp = client
        .post(format!("{}/s3/ppost-exact", srv.url))
        .multipart(form_ok)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
}

#[tokio::test]
async fn e2e_s3_presigned_post_enforces_starts_with_key_rule() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-prefix").await;

    let form = presigned_post_form(
        "videos/a.mp4",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-prefix" }),
            serde_json::json!(["starts-with", "$key", "photos/"]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"x".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-prefix", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn e2e_s3_presigned_post_enforces_content_length_range() {
    // content-length-range is checked after the upload lands, because the
    // server only knows the real size once the stream closes. The handler
    // must then delete the over/undersized object so the bucket never
    // contains it.
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-size").await;

    // 5-byte file against a [10,1000] range → rejected.
    let form = presigned_post_form(
        "undersized.bin",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-size" }),
            serde_json::json!(["starts-with", "$key", ""]),
            serde_json::json!(["content-length-range", 10, 1000]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"small".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-size", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // And the object must NOT exist after the rejection — a HEAD should 404.
    let head = client
        .head(format!("{}/s3/ppost-size/undersized.bin", srv.url))
        .header("Authorization", srv.auth_header())
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.status(),
        404,
        "server must clean up oversized/undersized uploads"
    );
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_filename_placeholder() {
    // AWS's ${filename} placeholder is not implemented — clients must send
    // the concrete key. The handler should reject the placeholder with 400
    // rather than silently uploading to a bogus key.
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-ph").await;

    let form = presigned_post_form(
        "uploads/${filename}",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-ph" }),
            serde_json::json!(["starts-with", "$key", "uploads/"]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"x".to_vec(),
        None,
        None,
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-ph", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_unsupported_algorithm() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-alg").await;

    let form = presigned_post_form(
        "f.txt",
        &srv.admin_key,
        &srv.admin_key,
        vec![
            serde_json::json!({ "bucket": "ppost-alg" }),
            serde_json::json!(["starts-with", "$key", ""]),
        ],
        chrono::Utc::now() + chrono::Duration::minutes(15),
        b"x".to_vec(),
        None,
        Some("AWS4-HMAC-SHA512"), // not supported
        None,
    );

    let resp = client
        .post(format!("{}/s3/ppost-alg", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_s3_presigned_post_rejects_missing_file_field() {
    // Any multipart body that ends without a `file` field is malformed: the
    // handler buffers non-file fields and expects the file stream to close
    // the body. Missing file → InvalidRequest, not silent success.
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "ppost-nofile").await;

    // Build a valid-signature form then drop the file part.
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let scope_date = now.format("%Y%m%d").to_string();
    let credential = format!("{}/{}/us-east-1/s3/aws4_request", srv.admin_key, scope_date);
    let policy_json = serde_json::json!({
        "expiration": (now + chrono::Duration::minutes(15)).to_rfc3339(),
        "conditions": [
            { "bucket": "ppost-nofile" },
            ["starts-with", "$key", ""]
        ],
    });
    let policy_b64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&policy_json).unwrap().as_bytes());
    let mac = |k: &[u8], d: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(k).unwrap();
        m.update(d);
        m.finalize().into_bytes().to_vec()
    };
    let k_date = mac(
        format!("AWS4{}", srv.admin_key).as_bytes(),
        scope_date.as_bytes(),
    );
    let k_region = mac(&k_date, b"us-east-1");
    let k_service = mac(&k_region, b"s3");
    let k_signing = mac(&k_service, b"aws4_request");
    let signature = hex::encode(mac(&k_signing, policy_b64.as_bytes()));

    let form = reqwest::multipart::Form::new()
        .text("key", "whatever.txt")
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("policy", policy_b64)
        .text("x-amz-signature", signature);
    // No `file` part.

    let resp = client
        .post(format!("{}/s3/ppost-nofile", srv.url))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

// ---------------------------------------------------------------------------
// Per-bucket CORS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_cors_get_returns_configured_rules() {
    // Round-trip: PUT a rule set, GET it back, DELETE it, GET returns empty.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsget").await;

    // Initially no rules.
    let resp = client
        .get(format!("{}/v1/buckets/corsget/cors", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["rules"].as_array().unwrap().len(), 0);

    // Set a rule.
    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["POST", "PUT"],
            "allowed_headers": ["*"],
            "expose_headers": ["ETag"],
            "max_age_seconds": 600u64
        }]
    });
    let resp = client
        .put(format!("{}/v1/buckets/corsget/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET now shows them.
    let resp = client
        .get(format!("{}/v1/buckets/corsget/cors", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["rules"][0]["allowed_origins"][0],
        "https://social.example"
    );
    assert_eq!(body["rules"][0]["max_age_seconds"], 600);

    // DELETE clears.
    let resp = client
        .delete(format!("{}/v1/buckets/corsget/cors", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let resp = client
        .get(format!("{}/v1/buckets/corsget/cors", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["rules"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn e2e_cors_preflight_permissive_when_no_rules() {
    // Without explicit rules, any Origin should get the wildcard response so
    // we don't break existing clients (dashboards, SDK examples, etc.).
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "corsnone").await;

    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{}/s3/corsnone", srv.url))
        .header("Origin", "https://random.example")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
}

#[tokio::test]
async fn e2e_cors_preflight_honors_matching_rule() {
    // With rules set, only matching origins get an allow response.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsok").await;

    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["POST"],
            "allowed_headers": ["x-amz-date", "authorization"],
            "max_age_seconds": 3000u64
        }]
    });
    client
        .put(format!("{}/v1/buckets/corsok/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();

    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{}/s3/corsok", srv.url))
        .header("Origin", "https://social.example")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "x-amz-date, authorization",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://social.example"
    );
    assert_eq!(
        resp.headers().get("access-control-allow-methods").unwrap(),
        "POST"
    );
    assert_eq!(
        resp.headers().get("access-control-max-age").unwrap(),
        "3000"
    );
}

#[tokio::test]
async fn e2e_cors_preflight_rejects_non_matching_origin() {
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsevil").await;

    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["POST"],
            "allowed_headers": ["*"]
        }]
    });
    client
        .put(format!("{}/v1/buckets/corsevil/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();

    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{}/s3/corsevil", srv.url))
        .header("Origin", "https://evil.example")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn e2e_cors_preflight_rejects_non_matching_method() {
    // Origin allowed, but method isn't in the rule → preflight must 403.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsmethod").await;

    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["POST"]
        }]
    });
    client
        .put(format!("{}/v1/buckets/corsmethod/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();

    let resp = client
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/s3/corsmethod", srv.url),
        )
        .header("Origin", "https://social.example")
        .header("Access-Control-Request-Method", "DELETE")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn e2e_cors_actual_request_echoes_origin_when_rule_matches() {
    // A real (non-preflight) request with a matching Origin gets the echoed
    // origin in the response, plus Vary: Origin so CDNs cache correctly.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsreal").await;

    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["GET", "HEAD"],
            "expose_headers": ["ETag"]
        }]
    });
    client
        .put(format!("{}/v1/buckets/corsreal/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();

    // Seed one object so GET is meaningful.
    client
        .put(format!("{}/v1/objects/corsreal/hello.txt", srv.url))
        .header("Authorization", &auth)
        .body(b"hi".to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{}/s3/corsreal/hello.txt", srv.url))
        .header("Authorization", &auth)
        .header("Origin", "https://social.example")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://social.example"
    );
    assert_eq!(resp.headers().get("vary").unwrap(), "origin");
    assert_eq!(
        resp.headers().get("access-control-expose-headers").unwrap(),
        "ETag"
    );
}

#[tokio::test]
async fn e2e_cors_actual_request_omits_origin_when_no_rule_matches() {
    // When rules are set but none admits the Origin, the server still runs
    // the request (so non-browser clients work normally) but emits no CORS
    // headers — browsers will refuse to expose the response. This is the
    // standard "blocked by CORS" shape.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsblock").await;

    let put = serde_json::json!({
        "rules": [{
            "allowed_origins": ["https://social.example"],
            "allowed_methods": ["GET"]
        }]
    });
    client
        .put(format!("{}/v1/buckets/corsblock/cors", srv.url))
        .header("Authorization", &auth)
        .json(&put)
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/corsblock/hello.txt", srv.url))
        .header("Authorization", &auth)
        .body(b"hi".to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{}/s3/corsblock/hello.txt", srv.url))
        .header("Authorization", &auth)
        .header("Origin", "https://evil.example")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn e2e_cors_non_s3_path_is_permissive() {
    // Admin endpoints under /v1/ should not be affected by bucket-level rules.
    // A dashboard pulling from the native API must still work from any origin.
    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    create_bucket(&srv, "corsv1").await;

    // Even with strict rules on the bucket,
    client
        .put(format!("{}/v1/buckets/corsv1/cors", srv.url))
        .header("Authorization", &auth)
        .json(&serde_json::json!({
            "rules": [{
                "allowed_origins": ["https://social.example"],
                "allowed_methods": ["POST"]
            }]
        }))
        .send()
        .await
        .unwrap();

    // …a preflight on /v1/buckets is still permissive.
    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{}/v1/buckets", srv.url))
        .header("Origin", "https://random.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
}

#[tokio::test]
async fn e2e_cors_put_on_unknown_bucket_returns_404() {
    let srv = TestServer::start();
    let client = srv.client();

    let resp = client
        .put(format!("{}/v1/buckets/nonexistent/cors", srv.url))
        .header("Authorization", srv.auth_header())
        .json(&serde_json::json!({ "rules": [] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// ---------------------------------------------------------------------------
// S3 query-string presigned URLs + multipart upload
// ---------------------------------------------------------------------------
//
// "Give the browser a URL, let it PUT directly" for big files: backend
// initiates multipart with its API key, hands the browser one presigned PUT
// URL per part, browser uploads each, backend completes. The test walks that
// full flow with reqwest, asserting the handler accepts presigned PUTs
// WITHOUT an Authorization header.

fn percent_encode_sigv4(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build a presigned URL the server's `verify_sigv4_presigned()` will accept.
/// `extra_query` is the bag of non-signature params the URL carries (e.g.
/// `"uploadId=abc&partNumber=1"`).
fn presigned_put_url(
    srv: &TestServer,
    path: &str,
    extra_query: &str,
    expires_seconds: u32,
) -> String {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;

    let host = srv.url.trim_start_matches("http://");
    let access_key = &srv.admin_key;
    let secret = &srv.admin_key;
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let region = "us-east-1";
    let service = "s3";
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let credential = format!("{access_key}/{scope}");

    let mut params: Vec<(String, String)> = Vec::new();
    for pair in extra_query.split('&').filter(|s| !s.is_empty()) {
        if let Some((k, v)) = pair.split_once('=') {
            params.push((k.to_string(), v.to_string()));
        }
    }
    params.push(("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()));
    params.push(("X-Amz-Credential".into(), credential.clone()));
    params.push(("X-Amz-Date".into(), amz_date.clone()));
    params.push(("X-Amz-Expires".into(), expires_seconds.to_string()));
    params.push(("X-Amz-SignedHeaders".into(), "host".into()));

    let mut canonical: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| (percent_encode_sigv4(k), percent_encode_sigv4(v)))
        .collect();
    canonical.sort();
    let canonical_query = canonical
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_uri = path
        .split('/')
        .map(percent_encode_sigv4)
        .collect::<Vec<_>>()
        .join("/");

    let canonical_request =
        format!("PUT\n{canonical_uri}\n{canonical_query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD");
    let cr_hash = {
        let mut h = Sha256::new();
        h.update(canonical_request.as_bytes());
        hex::encode(h.finalize())
    };
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
    let signature = hex::encode(mac(&k_signing, sts.as_bytes()));

    format!(
        "{base}{path}?{canonical_query}&X-Amz-Signature={signature}",
        base = srv.url,
    )
}

/// Full multipart-via-presigned-URL roundtrip, mirroring what a Mastodon
/// backend would orchestrate for a browser-uploaded large media file:
///   1. server-side InitiateMultipartUpload (auth'd)
///   2. server hands the browser one presigned PUT URL per part
///   3. browser PUTs each part to those URLs with NO Authorization header
///   4. server-side CompleteMultipartUpload (auth'd)
///   5. download and verify byte-accurate roundtrip
#[tokio::test]
async fn e2e_s3_multipart_with_presigned_put_urls() {
    let srv = TestServer::start();
    let auth = srv.auth_header();
    let client = srv.client();
    create_bucket(&srv, "mpp").await;

    // 1. Initiate.
    let resp = client
        .post(format!("{}/s3/mpp/big.bin?uploads", srv.url))
        .header("Authorization", &auth)
        .header("Content-Type", "application/octet-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let upload_id = extract_xml_tag(&resp.text().await.unwrap(), "UploadId").unwrap();
    assert!(!upload_id.is_empty());

    // 2. Two 5 MiB parts + a small tail, uploaded via presigned URLs.
    let part1 = vec![b'A'; 5 * 1024 * 1024];
    let part2 = vec![b'B'; 5 * 1024 * 1024];
    let tail = b"TAIL!!".to_vec();
    let mut etags = Vec::new();

    for (pn, data) in [(1u32, &part1), (2u32, &part2), (3u32, &tail)] {
        let url = presigned_put_url(
            &srv,
            "/s3/mpp/big.bin",
            &format!("uploadId={upload_id}&partNumber={pn}"),
            900,
        );
        // No Authorization header — the URL is its own credential.
        let resp = client.put(&url).body(data.clone()).send().await.unwrap();
        assert_eq!(resp.status(), 200, "presigned UploadPart {pn} must succeed");
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .trim_matches('"')
            .to_string();
        etags.push(etag);
    }

    // 3. Complete.
    let complete_body = format!(
        r#"<?xml version="1.0"?>
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"{}"</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"{}"</ETag></Part>
  <Part><PartNumber>3</PartNumber><ETag>"{}"</ETag></Part>
</CompleteMultipartUpload>"#,
        etags[0], etags[1], etags[2]
    );
    let resp = client
        .post(format!("{}/s3/mpp/big.bin?uploadId={}", srv.url, upload_id))
        .header("Authorization", &auth)
        .header("content-type", "application/xml")
        .body(complete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 4. Byte-accurate download. Total size = 2 * 5 MiB + 6 bytes.
    let resp = client
        .get(format!("{}/s3/mpp/big.bin", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.bytes().await.unwrap();
    assert_eq!(bytes.len(), part1.len() + part2.len() + tail.len());
    assert!(bytes[..part1.len()].iter().all(|&b| b == b'A'));
    assert!(bytes[part1.len()..part1.len() + part2.len()]
        .iter()
        .all(|&b| b == b'B'));
    assert_eq!(&bytes[part1.len() + part2.len()..], tail.as_slice());
}

#[tokio::test]
async fn e2e_s3_presigned_put_url_rejects_bad_signature() {
    let srv = TestServer::start();
    let client = srv.client();
    create_bucket(&srv, "bpu").await;

    let url = presigned_put_url(&srv, "/s3/bpu/file.bin", "", 300);
    // Flip one hex character in the signature.
    let mut bad = url.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == '0' { '1' } else { '0' });

    let resp = client.put(&bad).body(b"hi".to_vec()).send().await.unwrap();
    assert_eq!(resp.status(), 403);

    // The untampered URL must still succeed.
    let resp = client.put(&url).body(b"hi".to_vec()).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------------------
// X-Request-Id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_request_id_generated_when_absent() {
    // No X-Request-Id on the wire → server mints a UUID-shaped id and
    // echoes it. 36-char with 4 dashes is a reasonable sanity shape
    // without pulling in a UUID parser just for the test.
    let srv = TestServer::start();
    let resp = srv
        .client()
        .get(format!("{}/health", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let id = resp.headers().get("x-request-id").expect("echo header");
    let s = id.to_str().unwrap();
    assert_eq!(s.len(), 36);
    assert_eq!(s.matches('-').count(), 4);
}

#[tokio::test]
async fn e2e_request_id_echoes_well_formed_client_id() {
    let srv = TestServer::start();
    let resp = srv
        .client()
        .get(format!("{}/health", srv.url))
        .header("X-Request-Id", "abc-123-trace")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.headers().get("x-request-id").unwrap(), "abc-123-trace");
}

#[tokio::test]
async fn e2e_request_id_replaces_malformed_client_id() {
    // A 200-byte client-supplied id is over our 128-byte cap — the server
    // must silently replace it with a minted one rather than echo a
    // possibly-attacker-controlled identifier back.
    let srv = TestServer::start();
    let bad = "a".repeat(200);
    let resp = srv
        .client()
        .get(format!("{}/health", srv.url))
        .header("X-Request-Id", bad)
        .send()
        .await
        .unwrap();
    let echoed = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    // 36 chars (UUID shape) is the replacement-happened tell. A generated
    // UUID can start with any hex digit so we can't assert on the prefix.
    assert_eq!(echoed.len(), 36);
    assert_eq!(echoed.matches('-').count(), 4);
}

// ---------------------------------------------------------------------------
// Kubernetes probes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_livez_returns_ok() {
    // /livez must succeed whenever the HTTP loop is alive. No auth, no DB
    // dependency — if this ever fails under normal operation we'd be in a
    // restart loop.
    let srv = TestServer::start();
    let resp = srv
        .client()
        .get(format!("{}/livez", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn e2e_readyz_checks_storage() {
    // Healthy storage → 200. We don't have an easy way to simulate a
    // broken DB in-process, so this test is the positive case only; the
    // error path is exercised by the shared `match` in readyz().
    let srv = TestServer::start();
    let resp = srv
        .client()
        .get(format!("{}/readyz", srv.url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn e2e_probes_need_no_auth() {
    // Operators deploy VexObj behind auth-enforcing proxies. The probes
    // have to pass unauthenticated or K8s will kill every pod.
    let srv = TestServer::start();
    for path in ["/livez", "/readyz", "/health"] {
        let resp = srv
            .client()
            .get(format!("{}{}", srv.url, path))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{path} must respond without auth");
    }
}

// ---------------------------------------------------------------------------
// vexobjctl migrate s3
// ---------------------------------------------------------------------------
//
// Uses a second VexObj instance as the S3 source — our /s3 surface is
// SigV4-compatible, so driving vexobjctl against it exercises the real
// signing path end-to-end without requiring MinIO in CI.

async fn run_vexobjctl(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let cli = workspace_root.join("target/debug/vexobjctl");
    assert!(
        cli.exists(),
        "vexobjctl not built; run `cargo build --bins`"
    );
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    tokio::task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(&cli);
        for (k, v) in env {
            cmd.env(k, v);
        }
        for a in args {
            cmd.arg(a);
        }
        cmd.output().expect("spawn vexobjctl")
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn e2e_vexobjctl_migrate_s3_round_trips_bytes_with_hash_verified() {
    use serde_json::json;

    let source = TestServer::start();
    let dest = TestServer::start();
    let sc = source.client();
    let dc = dest.client();

    // Seed three objects on the source with varying shapes so the test can't
    // pass by accident (e.g. empty-body pipeline).
    sc.post(format!("{}/v1/buckets", source.url))
        .header("Authorization", source.auth_header())
        .json(&json!({ "name": "src", "public": false }))
        .send()
        .await
        .unwrap();
    let objects: &[(&str, &[u8])] = &[
        ("a.txt", b"alpha content"),
        ("nested/b.bin", &[0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ("docs/readme.md", b"# hello\n\nmigration smoke test\n"),
    ];
    for (k, body) in objects {
        let resp = sc
            .put(format!("{}/v1/objects/src/{k}", source.url))
            .header("Authorization", source.auth_header())
            .body(body.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201, "seed {k}");
    }

    // Destination bucket exists but is empty.
    dc.post(format!("{}/v1/buckets", dest.url))
        .header("Authorization", dest.auth_header())
        .json(&json!({ "name": "dst", "public": false }))
        .send()
        .await
        .unwrap();

    let source_endpoint = format!("{}/s3", source.url);
    let out = run_vexobjctl(
        &[
            "migrate",
            "s3",
            "--source-endpoint",
            &source_endpoint,
            "--source-bucket",
            "src",
            "--source-access-key",
            &source.admin_key,
            "--source-secret-key",
            &source.admin_key,
            "--dest-bucket",
            "dst",
            "--region",
            "us-east-1",
        ],
        &[("VEXOBJ_URL", &dest.url), ("VEXOBJ_KEY", &dest.admin_key)],
    )
    .await;
    assert!(
        out.status.success(),
        "migrate failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("sha256 verified"),
        "expected per-object hash verification in output:\n{stdout}"
    );

    // Every object must exist on dest with the exact source bytes.
    for (k, body) in objects {
        let resp = dc
            .get(format!("{}/v1/objects/dst/{k}", dest.url))
            .header("Authorization", dest.auth_header())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "dest missing {k}");
        let got = resp.bytes().await.unwrap();
        assert_eq!(&got[..], *body, "byte mismatch for {k}");
    }
}

#[tokio::test]
async fn e2e_vexobjctl_migrate_s3_skip_existing_resumes_partial_run() {
    // Simulates a re-run: pre-seed one key on the destination, then invoke
    // migrate with --skip-existing. That key must be skipped; the others
    // must be transferred. Confirms --skip-existing is an effective resume
    // primitive (checks HEAD on dest before transferring).
    use serde_json::json;

    let source = TestServer::start();
    let dest = TestServer::start();
    let sc = source.client();
    let dc = dest.client();

    sc.post(format!("{}/v1/buckets", source.url))
        .header("Authorization", source.auth_header())
        .json(&json!({ "name": "src", "public": false }))
        .send()
        .await
        .unwrap();
    for k in ["one.txt", "two.txt", "three.txt"] {
        sc.put(format!("{}/v1/objects/src/{k}", source.url))
            .header("Authorization", source.auth_header())
            .body(format!("content of {k}").into_bytes())
            .send()
            .await
            .unwrap();
    }

    dc.post(format!("{}/v1/buckets", dest.url))
        .header("Authorization", dest.auth_header())
        .json(&json!({ "name": "dst", "public": false }))
        .send()
        .await
        .unwrap();
    // Pre-seed `two.txt` with DIFFERENT content — we want to prove skip
    // really means skip (not overwrite with source content).
    dc.put(format!("{}/v1/objects/dst/two.txt", dest.url))
        .header("Authorization", dest.auth_header())
        .body(b"preserved content".to_vec())
        .send()
        .await
        .unwrap();

    let source_endpoint = format!("{}/s3", source.url);
    let out = run_vexobjctl(
        &[
            "migrate",
            "s3",
            "--source-endpoint",
            &source_endpoint,
            "--source-bucket",
            "src",
            "--source-access-key",
            &source.admin_key,
            "--source-secret-key",
            &source.admin_key,
            "--dest-bucket",
            "dst",
            "--skip-existing",
        ],
        &[("VEXOBJ_URL", &dest.url), ("VEXOBJ_KEY", &dest.admin_key)],
    )
    .await;
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("SKIP (exists) two.txt"),
        "expected two.txt to be skipped:\n{stdout}"
    );
    assert!(
        stdout.contains("Migrated:    2 object"),
        "expected two migrations:\n{stdout}"
    );
    assert!(
        stdout.contains("Skipped:     1 object"),
        "expected one skip in summary:\n{stdout}"
    );

    // two.txt on dest must still have the pre-seeded content.
    let body = dc
        .get(format!("{}/v1/objects/dst/two.txt", dest.url))
        .header("Authorization", dest.auth_header())
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&body[..], b"preserved content");
}

// ---------------------------------------------------------------------------
// Upload from URL
// ---------------------------------------------------------------------------

/// Spawn a tiny HTTP/1.1 server on 127.0.0.1 that responds to every request
/// with `body` (Content-Type application/octet-stream). Returns the port
/// and a JoinHandle whose lifetime the caller must hold.
async fn serve_payload(body: Vec<u8>) -> (u16, tokio::task::JoinHandle<()>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
                    body.len(),
                );
                let _ = sock.write_all(header.as_bytes()).await;
                let _ = sock.write_all(&body).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    (port, handle)
}

#[tokio::test]
async fn e2e_upload_from_url_round_trips_bytes() {
    // Happy path: admin opts into allow-private (env flag), server fetches
    // the localhost test URL, bytes land in the bucket verbatim.
    use serde_json::json;

    let srv = TestServer::start_with_env(&[("VEXOBJ_ALLOW_PRIVATE_SOURCE_URLS", "true")]);
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "uurl", "public": false }))
        .send()
        .await
        .unwrap();

    let payload = b"fetched via upload-from-URL".to_vec();
    let (port, _handle) = serve_payload(payload.clone()).await;
    let source = format!("http://127.0.0.1:{port}/media.bin");

    let resp = client
        .post(format!("{}/v1/objects/uurl/from-url.bin", srv.url))
        .header("Authorization", &auth)
        .query(&[("source", source.as_str())])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let got = client
        .get(format!("{}/v1/objects/uurl/from-url.bin", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(got.status(), 200);
    assert_eq!(got.bytes().await.unwrap().to_vec(), payload);
}

#[tokio::test]
async fn e2e_upload_from_url_ssrf_blocks_localhost_by_default() {
    // No env opt-in → loopback must be refused with 403. Default for
    // any VexObj exposed to the internet.
    use serde_json::json;

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "ssrf", "public": false }))
        .send()
        .await
        .unwrap();

    let (port, _handle) = serve_payload(b"x".to_vec()).await;
    let source = format!("http://127.0.0.1:{port}/x");

    let resp = client
        .post(format!("{}/v1/objects/ssrf/x.bin", srv.url))
        .header("Authorization", &auth)
        .query(&[("source", source.as_str())])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("private"));
}

#[tokio::test]
async fn e2e_upload_from_url_rejects_non_http_scheme() {
    // file:// and other non-http(s) schemes are rejected up front with 400
    // (malformed request) rather than 403 (policy) — clear distinction.
    use serde_json::json;

    let srv = TestServer::start();
    let client = srv.client();
    let auth = srv.auth_header();
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "sch", "public": false }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v1/objects/sch/x.bin", srv.url))
        .header("Authorization", &auth)
        .query(&[("source", "file:///etc/passwd")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn e2e_upload_from_url_blocks_cloud_metadata_even_when_private_allowed() {
    // 169.254.169.254 (AWS / GCP instance metadata) stays blocked even
    // when the admin opts into private-network sources. An SSRF-relay
    // escape hatch that doesn't close the metadata hole is worthless.
    use serde_json::json;

    let srv = TestServer::start_with_env(&[("VEXOBJ_ALLOW_PRIVATE_SOURCE_URLS", "true")]);
    let client = srv.client();
    let auth = srv.auth_header();
    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "meta", "public": false }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/v1/objects/meta/x.bin", srv.url))
        .header("Authorization", &auth)
        .query(&[("source", "http://169.254.169.254/latest/meta-data/")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("cloud-metadata"));
}

// ---------------------------------------------------------------------------
// S3 compat matrix — operations Mastodon / PeerTube actually invoke
// ---------------------------------------------------------------------------
//
// Each test below is named after the S3 API op it exercises and documents
// the fediverse use case in the header comment. Together they form the
// contract documented in docs/s3-compat.md.

/// HeadBucket — Mastodon/PeerTube startup probe ("does the configured
/// bucket exist?"). Must return 200 for a real bucket and 404 otherwise,
/// NOT 403: clients use the status code to branch between "configure
/// differently" and "create the bucket".
#[tokio::test]
async fn e2e_s3_compat_head_bucket() {
    use serde_json::json;

    let srv = TestServer::start();
    let auth = srv.auth_header();

    srv.client()
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "bckt", "public": false }))
        .send()
        .await
        .unwrap();

    // Real bucket → 200.
    let resp = srv
        .client()
        .head(format!("{}/s3/bckt", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Missing bucket → 404 (NoSuchBucket).
    let resp = srv
        .client()
        .head(format!("{}/s3/no-such-bucket", srv.url))
        .header("Authorization", &auth)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// DeleteObjects (bulk delete) — PeerTube uses this to clean up video
/// transcodes and thumbnails in batches. S3 guarantees the op is
/// idempotent: a key that doesn't exist is still returned in <Deleted>.
#[tokio::test]
async fn e2e_s3_compat_delete_objects_batch() {
    use serde_json::json;

    let srv = TestServer::start();
    let auth = srv.auth_header();
    let client = srv.client();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "bulk", "public": false }))
        .send()
        .await
        .unwrap();
    for k in ["a", "b", "c"] {
        client
            .put(format!("{}/v1/objects/bulk/{k}", srv.url))
            .header("Authorization", &auth)
            .body(format!("body-{k}").into_bytes())
            .send()
            .await
            .unwrap();
    }

    // Delete a, b, and a missing key "ghost". All three must come back in
    // <Deleted> (idempotency), with no <Error> entries.
    let body = r#"<?xml version="1.0"?>
<Delete>
  <Object><Key>a</Key></Object>
  <Object><Key>b</Key></Object>
  <Object><Key>ghost</Key></Object>
</Delete>"#;
    let resp = client
        .post(format!("{}/s3/bulk?delete", srv.url))
        .header("Authorization", &auth)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    assert_eq!(
        xml.matches("<Deleted>").count(),
        3,
        "all three keys must appear in <Deleted>: {xml}"
    );
    assert_eq!(xml.matches("<Error>").count(), 0, "no errors expected");

    // `a` and `b` should be gone; `c` must survive.
    for (k, want) in [("a", 404), ("b", 404), ("c", 200)] {
        let resp = client
            .get(format!("{}/s3/bulk/{k}", srv.url))
            .header("Authorization", &auth)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            want,
            "after DeleteObjects, /{k} should {want}"
        );
    }
}

/// DeleteObjects with <Quiet>true</Quiet> — response omits the <Deleted>
/// block and only surfaces errors. PeerTube often runs in quiet mode to
/// keep response bodies small for big cleanup batches.
#[tokio::test]
async fn e2e_s3_compat_delete_objects_quiet_mode() {
    use serde_json::json;

    let srv = TestServer::start();
    let auth = srv.auth_header();
    let client = srv.client();

    client
        .post(format!("{}/v1/buckets", srv.url))
        .header("Authorization", &auth)
        .json(&json!({ "name": "quiet", "public": false }))
        .send()
        .await
        .unwrap();
    client
        .put(format!("{}/v1/objects/quiet/x", srv.url))
        .header("Authorization", &auth)
        .body(b"x".to_vec())
        .send()
        .await
        .unwrap();

    let body = r#"<?xml version="1.0"?>
<Delete>
  <Quiet>true</Quiet>
  <Object><Key>x</Key></Object>
</Delete>"#;
    let resp = client
        .post(format!("{}/s3/quiet?delete", srv.url))
        .header("Authorization", &auth)
        .header("content-type", "application/xml")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let xml = resp.text().await.unwrap();
    assert!(
        !xml.contains("<Deleted>"),
        "quiet mode must suppress <Deleted> entries: {xml}"
    );
}
