//! Replication endpoints. Replicas pull events and blobs from the
//! primary via these routes. Both require the `admin` permission.

use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tokio_util::io::ReaderStream;

use crate::middleware::require_permission;
use crate::state::AppState;
use vaultfs_auth::ApiKey;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/replication/events", get(list_events))
        .route("/v1/replication/cursor", get(get_cursor))
        .route("/v1/replication/blob/{sha256}", get(get_blob))
}

#[derive(Deserialize, Default)]
struct EventsQuery {
    #[serde(default)]
    since: i64,
    #[serde(default)]
    limit: Option<u32>,
}

/// Return replication events with `id > since`, up to `limit` rows
/// (default 100, max 1000). Ordered ascending so replicas apply in
/// primary-order.
async fn list_events(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Query(query): Query<EventsQuery>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let limit = query.limit.unwrap_or(100).min(1000);
    match state
        .storage
        .db()
        .list_replication_events(query.since, limit)
    {
        Ok(events) => {
            let latest = state
                .storage
                .db()
                .latest_replication_event_id()
                .unwrap_or(0);
            Json(json!({
                "events": events,
                "latest_id": latest,
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Fast probe of how far ahead the primary is. Replicas call this
/// before a full `list_events` to skip polling when there's nothing to
/// pull.
async fn get_cursor(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }
    match state.storage.db().latest_replication_event_id() {
        Ok(id) => Json(json!({"latest_id": id})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Stream a blob by its SHA-256. If SSE is on, the bytes returned are
/// the ciphertext — the replica must hold the same master key to
/// decrypt on read. We bypass the usual object route to avoid
/// (bucket, key) lookups; replication only cares about content hashes.
async fn get_blob(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(sha256): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    // Reject anything that isn't a plain SHA-256 hex digest — the path
    // is used to build a filesystem location, so cheap validation first.
    if sha256.len() != 64 || !sha256.chars().all(|c| c.is_ascii_hexdigit()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid sha256"})),
        )
            .into_response();
    }

    let relative = format!("blobs/{}/{}/{}", &sha256[..2], &sha256[2..4], sha256);
    let full_path = state.storage.data_dir().join(&relative);

    match tokio::fs::File::open(&full_path).await {
        Ok(file) => {
            let size = file
                .metadata()
                .await
                .map(|m| m.len().to_string())
                .unwrap_or_default();
            let stream = ReaderStream::new(file);
            (
                StatusCode::OK,
                [
                    ("content-type", "application/octet-stream".to_string()),
                    ("content-length", size),
                    ("x-vaultfs-sha256", sha256),
                ],
                Body::from_stream(stream),
            )
                .into_response()
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "blob not found"})),
        )
            .into_response(),
    }
}
