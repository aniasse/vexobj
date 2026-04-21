use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use std::path::Path;

use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/stats", get(get_stats))
}

async fn get_stats(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let buckets = state.storage.list_buckets().unwrap_or_default();
    let mut total_objects: u64 = 0;
    let mut total_size: u64 = 0;
    let mut bucket_stats = Vec::new();

    for bucket in &buckets {
        let req = vexobj_storage::ListObjectsRequest {
            bucket: bucket.name.clone(),
            prefix: None,
            delimiter: None,
            max_keys: Some(1000),
            continuation_token: None,
        };
        if let Ok(resp) = state.storage.list_objects(&req) {
            let count = resp.objects.len() as u64;
            let size: u64 = resp.objects.iter().map(|o| o.size).sum();
            total_objects += count;
            total_size += size;
            bucket_stats.push(json!({
                "name": bucket.name,
                "objects": count,
                "size": size,
                "size_human": human_size(size),
            }));
        }
    }

    // Disk usage
    let data_dir = &state.config.storage.data_dir;
    let disk_usage = dir_size(Path::new(data_dir)).unwrap_or(0);

    (
        StatusCode::OK,
        Json(json!({
            "buckets": buckets.len(),
            "total_objects": total_objects,
            "total_size": total_size,
            "total_size_human": human_size(total_size),
            "disk_usage": disk_usage,
            "disk_usage_human": human_size(disk_usage),
            "bucket_details": bucket_stats,
            "version": env!("CARGO_PKG_VERSION"),
        })),
    )
        .into_response()
}

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{:.1} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.1} PB", size)
}

fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                total += dir_size(&entry.path())?;
            } else {
                total += metadata.len();
            }
        }
    }
    Ok(total)
}
