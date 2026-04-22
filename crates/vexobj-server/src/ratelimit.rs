use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::state::AppState;

pub struct RateLimiter {
    windows: Mutex<HashMap<String, Window>>,
    max_requests: u64,
    window_secs: u64,
}

struct Window {
    count: u64,
    started: Instant,
}

impl RateLimiter {
    pub fn new(max_requests: u64, window_secs: u64) -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
            max_requests,
            window_secs,
        }
    }

    pub fn check(&self, key: &str) -> RateLimitResult {
        let mut windows = self.windows.lock().unwrap();
        let now = Instant::now();

        let window = windows.entry(key.to_string()).or_insert(Window {
            count: 0,
            started: now,
        });

        // Reset window if expired
        if now.duration_since(window.started).as_secs() >= self.window_secs {
            window.count = 0;
            window.started = now;
        }

        window.count += 1;

        if window.count > self.max_requests {
            let retry_after = self.window_secs - now.duration_since(window.started).as_secs();
            RateLimitResult::Limited {
                retry_after,
                limit: self.max_requests,
            }
        } else {
            RateLimitResult::Allowed {
                remaining: self.max_requests - window.count,
                limit: self.max_requests,
            }
        }
    }

    pub fn cleanup(&self) {
        let mut windows = self.windows.lock().unwrap();
        let now = Instant::now();
        windows.retain(|_, w| now.duration_since(w.started).as_secs() < self.window_secs * 2);
    }
}

pub enum RateLimitResult {
    Allowed { remaining: u64, limit: u64 },
    Limited { retry_after: u64, limit: u64 },
}

/// Derive a rate-limit bucket identifier from a request. API keys get
/// their own bucket (so one compromised key can't saturate the server
/// for every other key); anonymous requests fall back to client IP
/// (proxy-aware), which gives each caller its own bucket instead of a
/// single shared "anonymous" window.
fn rate_limit_key(req: &Request) -> String {
    if let Some(auth) = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
    {
        // "Bearer <key>" — isolate per key.
        if let Some(k) = auth.strip_prefix("Bearer ") {
            let trimmed = &k[..k.len().min(32)];
            return format!("key:{trimmed}");
        }
        // "AWS4-HMAC-SHA256 Credential=<access>/date/region/service/aws4_request, ..."
        // Parse out the access key — without that split, every SigV4
        // client shared one window regardless of identity.
        if let Some(after_algo) = auth.strip_prefix("AWS4-HMAC-SHA256 ") {
            for part in after_algo.split(',') {
                if let Some(cred) = part.trim().strip_prefix("Credential=") {
                    if let Some(access) = cred.split('/').next() {
                        if !access.is_empty() {
                            let trimmed = &access[..access.len().min(32)];
                            return format!("key:{trimmed}");
                        }
                    }
                }
            }
        }
        // Raw API key in Authorization (no prefix) — also per key.
        let trimmed = &auth[..auth.len().min(32)];
        return format!("raw:{trimmed}");
    }

    // No auth: bucket by client IP. Behind a reverse proxy (Caddy /
    // nginx / Cloudflare) the real client IP lands in X-Real-IP or
    // the first X-Forwarded-For hop; without that we don't have a
    // peer addr in this layer so fall back to a shared "anonymous"
    // bucket — conservative but limits total anonymous volume.
    let ip = req
        .headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| {
            req.headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.split(',').next())
                .map(|s| s.trim().to_string())
        });
    match ip {
        Some(s) if !s.is_empty() => format!("ip:{s}"),
        _ => "anonymous".to_string(),
    }
}

pub async fn rate_limit_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let limiter = match &state.rate_limiter {
        Some(l) => l,
        None => return next.run(req).await,
    };

    let key = rate_limit_key(&req);

    match limiter.check(&key) {
        RateLimitResult::Allowed { remaining, limit } => {
            let mut response = next.run(req).await;
            let headers = response.headers_mut();
            headers.insert("x-ratelimit-limit", limit.into());
            headers.insert("x-ratelimit-remaining", remaining.into());
            response
        }
        RateLimitResult::Limited { retry_after, limit } => (
            StatusCode::TOO_MANY_REQUESTS,
            [
                ("retry-after", retry_after.to_string()),
                ("x-ratelimit-limit", limit.to_string()),
                ("x-ratelimit-remaining", "0".to_string()),
            ],
            axum::Json(json!({"error": "rate limit exceeded", "retry_after": retry_after})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod key_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;

    fn req_with(headers: &[(&str, &str)]) -> Request<Body> {
        let mut b = Request::builder().method("GET").uri("/");
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        b.body(Body::empty()).unwrap()
    }

    #[test]
    fn bearer_key_isolated_per_key() {
        let a = rate_limit_key(&req_with(&[("authorization", "Bearer vex_aaaaaaa")]));
        let b = rate_limit_key(&req_with(&[("authorization", "Bearer vex_bbbbbbb")]));
        assert_ne!(a, b);
        assert!(a.starts_with("key:"));
    }

    #[test]
    fn sigv4_key_extracts_access_key() {
        let auth = "AWS4-HMAC-SHA256 Credential=AKID123/20260422/us-east-1/s3/aws4_request, \
                    SignedHeaders=host;x-amz-date, Signature=deadbeef";
        let k = rate_limit_key(&req_with(&[("authorization", auth)]));
        assert_eq!(k, "key:AKID123");
    }

    #[test]
    fn sigv4_different_keys_land_in_different_buckets() {
        let a = rate_limit_key(&req_with(&[(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKID_A/d/r/s3/aws4_request, SignedHeaders=h, Signature=s",
        )]));
        let b = rate_limit_key(&req_with(&[(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKID_B/d/r/s3/aws4_request, SignedHeaders=h, Signature=s",
        )]));
        assert_ne!(a, b);
    }

    #[test]
    fn anonymous_buckets_by_client_ip() {
        let a = rate_limit_key(&req_with(&[("x-real-ip", "1.2.3.4")]));
        let b = rate_limit_key(&req_with(&[("x-real-ip", "5.6.7.8")]));
        assert_eq!(a, "ip:1.2.3.4");
        assert_ne!(a, b);
    }

    #[test]
    fn x_forwarded_for_uses_first_hop() {
        let k = rate_limit_key(&req_with(&[("x-forwarded-for", "203.0.113.1, 10.0.0.1")]));
        assert_eq!(k, "ip:203.0.113.1");
    }

    #[test]
    fn no_auth_no_proxy_headers_falls_back() {
        let k = rate_limit_key(&req_with(&[]));
        assert_eq!(k, "anonymous");
    }
}
