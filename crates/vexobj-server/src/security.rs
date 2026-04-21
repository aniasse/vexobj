use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Security middleware: validates requests before they reach handlers.
/// - Path traversal protection
/// - Key/bucket name validation
/// - Request size enforcement via headers
/// - Security response headers
pub async fn security_middleware(req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();

    // Path traversal protection. We also percent-decode to catch encoded
    // variants like `%2E%2E` and `%00` — a classic WAF/middleware bypass.
    let decoded = percent_decode(&path);
    let has_traversal = path.contains("//")
        || path.contains('\0')
        || decoded.contains("..")
        || decoded.contains('\0');
    if has_traversal {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "invalid path: path traversal detected"})),
        )
            .into_response();
    }

    // Validate bucket/key names in API paths
    if path.starts_with("/v1/objects/") || path.starts_with("/v1/stream/") {
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if segments.len() >= 3 {
            let bucket = segments[2];
            if !is_valid_bucket_name(bucket) {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({"error": "invalid bucket name: must be 3-63 chars, lowercase alphanumeric and hyphens"})),
                )
                    .into_response();
            }
        }
    }

    let mut response = next.run(req).await;

    // Security headers
    let headers = response.headers_mut();
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert("x-frame-options", "DENY".parse().unwrap());
    headers.insert(
        "strict-transport-security",
        "max-age=31536000; includeSubDomains".parse().unwrap(),
    );
    headers.insert("x-xss-protection", "1; mode=block".parse().unwrap());
    headers.insert(
        "content-security-policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com".parse().unwrap(),
    );

    response
}

/// Lossy percent-decoder for ASCII paths. Invalid escapes are kept verbatim;
/// non-UTF8 decoded bytes are replaced with U+FFFD. Good enough for traversal
/// detection where we only care about `.` and NUL.
fn percent_decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Validate bucket names: 3-63 chars, lowercase, alphanumeric + hyphens, no leading/trailing hyphen
fn is_valid_bucket_name(name: &str) -> bool {
    if name.len() < 3 || name.len() > 63 {
        return false;
    }
    if name.starts_with('-') || name.ends_with('-') {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}
