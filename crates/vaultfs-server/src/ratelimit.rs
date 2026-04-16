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

pub async fn rate_limit_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let limiter = match &state.rate_limiter {
        Some(l) => l,
        None => return next.run(req).await,
    };

    // Use API key prefix or IP as rate limit key
    let key = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|h| {
            if h.len() > 20 {
                h[..20].to_string()
            } else {
                h.to_string()
            }
        })
        .unwrap_or_else(|| "anonymous".to_string());

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
