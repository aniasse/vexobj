use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct Metrics {
    /// Total requests by (method, path, status) — we track aggregated counters
    pub requests_total: Arc<AtomicU64>,
    pub requests_2xx: Arc<AtomicU64>,
    pub requests_3xx: Arc<AtomicU64>,
    pub requests_4xx: Arc<AtomicU64>,
    pub requests_5xx: Arc<AtomicU64>,

    pub requests_get: Arc<AtomicU64>,
    pub requests_put: Arc<AtomicU64>,
    pub requests_post: Arc<AtomicU64>,
    pub requests_delete: Arc<AtomicU64>,
    pub requests_head: Arc<AtomicU64>,

    /// Request duration tracking (histogram approximation via sum + count)
    pub request_duration_sum_us: Arc<AtomicU64>,
    pub request_duration_count: Arc<AtomicU64>,

    /// Duration histogram buckets (cumulative, in microseconds)
    pub duration_le_1ms: Arc<AtomicU64>,
    pub duration_le_10ms: Arc<AtomicU64>,
    pub duration_le_50ms: Arc<AtomicU64>,
    pub duration_le_100ms: Arc<AtomicU64>,
    pub duration_le_500ms: Arc<AtomicU64>,
    pub duration_le_1s: Arc<AtomicU64>,
    pub duration_le_5s: Arc<AtomicU64>,
    pub duration_le_inf: Arc<AtomicU64>,

    /// Object/byte counters
    pub objects_uploaded_total: Arc<AtomicU64>,
    pub bytes_uploaded_total: Arc<AtomicU64>,
    pub bytes_downloaded_total: Arc<AtomicU64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests_total: Arc::new(AtomicU64::new(0)),
            requests_2xx: Arc::new(AtomicU64::new(0)),
            requests_3xx: Arc::new(AtomicU64::new(0)),
            requests_4xx: Arc::new(AtomicU64::new(0)),
            requests_5xx: Arc::new(AtomicU64::new(0)),
            requests_get: Arc::new(AtomicU64::new(0)),
            requests_put: Arc::new(AtomicU64::new(0)),
            requests_post: Arc::new(AtomicU64::new(0)),
            requests_delete: Arc::new(AtomicU64::new(0)),
            requests_head: Arc::new(AtomicU64::new(0)),
            request_duration_sum_us: Arc::new(AtomicU64::new(0)),
            request_duration_count: Arc::new(AtomicU64::new(0)),
            duration_le_1ms: Arc::new(AtomicU64::new(0)),
            duration_le_10ms: Arc::new(AtomicU64::new(0)),
            duration_le_50ms: Arc::new(AtomicU64::new(0)),
            duration_le_100ms: Arc::new(AtomicU64::new(0)),
            duration_le_500ms: Arc::new(AtomicU64::new(0)),
            duration_le_1s: Arc::new(AtomicU64::new(0)),
            duration_le_5s: Arc::new(AtomicU64::new(0)),
            duration_le_inf: Arc::new(AtomicU64::new(0)),
            objects_uploaded_total: Arc::new(AtomicU64::new(0)),
            bytes_uploaded_total: Arc::new(AtomicU64::new(0)),
            bytes_downloaded_total: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_request(&self, method: &str, status: u16, duration_us: u64) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);

        match status / 100 {
            2 => { self.requests_2xx.fetch_add(1, Ordering::Relaxed); }
            3 => { self.requests_3xx.fetch_add(1, Ordering::Relaxed); }
            4 => { self.requests_4xx.fetch_add(1, Ordering::Relaxed); }
            5 => { self.requests_5xx.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }

        match method {
            "GET" => { self.requests_get.fetch_add(1, Ordering::Relaxed); }
            "PUT" => { self.requests_put.fetch_add(1, Ordering::Relaxed); }
            "POST" => { self.requests_post.fetch_add(1, Ordering::Relaxed); }
            "DELETE" => { self.requests_delete.fetch_add(1, Ordering::Relaxed); }
            "HEAD" => { self.requests_head.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }

        self.request_duration_sum_us.fetch_add(duration_us, Ordering::Relaxed);
        self.request_duration_count.fetch_add(1, Ordering::Relaxed);

        // Histogram buckets (cumulative)
        if duration_us <= 1_000 { self.duration_le_1ms.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 10_000 { self.duration_le_10ms.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 50_000 { self.duration_le_50ms.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 100_000 { self.duration_le_100ms.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 500_000 { self.duration_le_500ms.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 1_000_000 { self.duration_le_1s.fetch_add(1, Ordering::Relaxed); }
        if duration_us <= 5_000_000 { self.duration_le_5s.fetch_add(1, Ordering::Relaxed); }
        self.duration_le_inf.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_upload(&self, bytes: u64) {
        self.objects_uploaded_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_uploaded_total.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_download(&self, bytes: u64) {
        self.bytes_downloaded_total.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);

        // requests_total by method
        out.push_str("# HELP vaultfs_requests_total Total number of HTTP requests.\n");
        out.push_str("# TYPE vaultfs_requests_total counter\n");
        out.push_str(&format!("vaultfs_requests_total {{}} {}\n", self.requests_total.load(Ordering::Relaxed)));

        out.push_str("# HELP vaultfs_requests_by_method_total HTTP requests by method.\n");
        out.push_str("# TYPE vaultfs_requests_by_method_total counter\n");
        for (method, counter) in [
            ("GET", &self.requests_get),
            ("PUT", &self.requests_put),
            ("POST", &self.requests_post),
            ("DELETE", &self.requests_delete),
            ("HEAD", &self.requests_head),
        ] {
            out.push_str(&format!(
                "vaultfs_requests_by_method_total{{method=\"{}\"}} {}\n",
                method,
                counter.load(Ordering::Relaxed),
            ));
        }

        out.push_str("# HELP vaultfs_requests_by_status_total HTTP requests by status class.\n");
        out.push_str("# TYPE vaultfs_requests_by_status_total counter\n");
        for (status, counter) in [
            ("2xx", &self.requests_2xx),
            ("3xx", &self.requests_3xx),
            ("4xx", &self.requests_4xx),
            ("5xx", &self.requests_5xx),
        ] {
            out.push_str(&format!(
                "vaultfs_requests_by_status_total{{status=\"{}\"}} {}\n",
                status,
                counter.load(Ordering::Relaxed),
            ));
        }

        // Duration histogram
        let count = self.request_duration_count.load(Ordering::Relaxed);
        let sum_us = self.request_duration_sum_us.load(Ordering::Relaxed);
        let sum_secs = sum_us as f64 / 1_000_000.0;

        out.push_str("# HELP vaultfs_request_duration_seconds HTTP request duration in seconds.\n");
        out.push_str("# TYPE vaultfs_request_duration_seconds histogram\n");
        for (le, counter) in [
            ("0.001", &self.duration_le_1ms),
            ("0.01", &self.duration_le_10ms),
            ("0.05", &self.duration_le_50ms),
            ("0.1", &self.duration_le_100ms),
            ("0.5", &self.duration_le_500ms),
            ("1", &self.duration_le_1s),
            ("5", &self.duration_le_5s),
            ("+Inf", &self.duration_le_inf),
        ] {
            out.push_str(&format!(
                "vaultfs_request_duration_seconds_bucket{{le=\"{}\"}} {}\n",
                le,
                counter.load(Ordering::Relaxed),
            ));
        }
        out.push_str(&format!("vaultfs_request_duration_seconds_sum {:.6}\n", sum_secs));
        out.push_str(&format!("vaultfs_request_duration_seconds_count {}\n", count));

        // Upload/download counters
        out.push_str("# HELP vaultfs_objects_uploaded_total Total objects uploaded.\n");
        out.push_str("# TYPE vaultfs_objects_uploaded_total counter\n");
        out.push_str(&format!("vaultfs_objects_uploaded_total {}\n", self.objects_uploaded_total.load(Ordering::Relaxed)));

        out.push_str("# HELP vaultfs_bytes_uploaded_total Total bytes uploaded.\n");
        out.push_str("# TYPE vaultfs_bytes_uploaded_total counter\n");
        out.push_str(&format!("vaultfs_bytes_uploaded_total {}\n", self.bytes_uploaded_total.load(Ordering::Relaxed)));

        out.push_str("# HELP vaultfs_bytes_downloaded_total Total bytes downloaded.\n");
        out.push_str("# TYPE vaultfs_bytes_downloaded_total counter\n");
        out.push_str(&format!("vaultfs_bytes_downloaded_total {}\n", self.bytes_downloaded_total.load(Ordering::Relaxed)));

        out
    }
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/metrics", get(metrics_handler))
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let body = state.metrics.render_prometheus();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

pub async fn metrics_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().to_string();
    let start = Instant::now();
    let response = next.run(req).await;
    let duration = start.elapsed();
    let status = response.status().as_u16();
    let duration_us = duration.as_micros() as u64;

    state.metrics.record_request(&method, status, duration_us);

    response
}
