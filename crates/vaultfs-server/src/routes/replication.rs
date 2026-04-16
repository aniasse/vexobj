//! Replication endpoints. Replicas pull events and blobs from the
//! primary via these routes. Both require the `admin` permission.

use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use crate::middleware::require_permission;
use crate::state::AppState;
use vaultfs_auth::ApiKey;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/replication/events", get(list_events))
        .route("/v1/replication/cursor", get(get_cursor))
        .route(
            "/v1/replication/blob/{sha256}",
            get(get_blob).put(import_blob),
        )
        .route("/v1/replication/apply", axum::routing::post(apply_event))
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

fn valid_sha256(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn blob_path_for(sha256: &str) -> String {
    format!("blobs/{}/{}/{}", &sha256[..2], &sha256[2..4], sha256)
}

/// Import a blob from a primary. Replicas call this before `apply` so
/// the content-addressed file exists locally. In non-SSE mode we verify
/// the incoming bytes hash to the expected sha256 (prevents poisoning).
/// In SSE mode the bytes are the primary's ciphertext — we trust the
/// primary and skip verification, since hashing ciphertext wouldn't
/// match the plaintext sha we're keyed on.
async fn import_blob(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(sha256): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }
    if !valid_sha256(&sha256) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid sha256"})),
        )
            .into_response();
    }

    if !state.storage.encryption_enabled() {
        let mut h = Sha256::new();
        h.update(&body);
        let actual = hex::encode(h.finalize());
        if actual != sha256 {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "body sha256 does not match path",
                    "expected": sha256,
                    "actual": actual,
                })),
            )
                .into_response();
        }
    }

    let rel = blob_path_for(&sha256);
    let full_path = state.storage.data_dir().join(&rel);
    if let Some(parent) = full_path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("mkdir failed: {e}")})),
            )
                .into_response();
        }
    }

    // Atomic-ish write via temp + rename. A concurrent importer could
    // race on the rename, but both writers would produce identical
    // content-addressed bytes, so the loser just wastes I/O.
    let tmp = state
        .storage
        .data_dir()
        .join(format!(".tmp-import-{}", uuid::Uuid::new_v4()));
    let mut file = match tokio::fs::File::create(&tmp).await {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("create tmp: {e}")})),
            )
                .into_response();
        }
    };
    if let Err(e) = file.write_all(&body).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("write tmp: {e}")})),
        )
            .into_response();
    }
    drop(file);
    if let Err(e) = tokio::fs::rename(&tmp, &full_path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("rename: {e}")})),
        )
            .into_response();
    }

    (StatusCode::NO_CONTENT, ()).into_response()
}

#[derive(Deserialize)]
struct ApplyEvent {
    op: String,
    bucket: String,
    key: String,
    #[serde(default)]
    sha256: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    content_type: String,
}

/// Apply a single replication event from a primary. Writes go straight
/// to the database — we deliberately skip the engine's write path so
/// the replica does not re-append to its own `replication_events` log.
/// The primary's exact sha256 / version_id are preserved.
async fn apply_event(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Json(event): Json<ApplyEvent>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let db = state.storage.db();

    match event.op.as_str() {
        "put" | "version_put" => {
            if !valid_sha256(&event.sha256) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid sha256 for put event"})),
                )
                    .into_response();
            }
            // Refuse to apply before the blob was imported — without the
            // file on disk a subsequent GET would return garbage.
            let rel = blob_path_for(&event.sha256);
            if !state.storage.data_dir().join(&rel).exists() {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({"error": "blob not found on replica — import first"})),
                )
                    .into_response();
            }

            // Ensure bucket exists (primaries could have created it before
            // replication started).
            let _ = state
                .storage
                .create_bucket(&vaultfs_storage::CreateBucketRequest {
                    name: event.bucket.clone(),
                    public: false,
                });

            if event.op == "put" {
                match db.put_object(
                    &event.bucket,
                    &event.key,
                    event.size,
                    &event.content_type,
                    &event.sha256,
                    &rel,
                    &serde_json::Value::Object(Default::default()),
                ) {
                    Ok(_) => (StatusCode::NO_CONTENT, ()).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            } else {
                let vid = match &event.version_id {
                    Some(v) => v.clone(),
                    None => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "version_put needs version_id"})),
                        )
                            .into_response()
                    }
                };
                match db.save_version(
                    &event.bucket,
                    &event.key,
                    &vid,
                    event.size,
                    &event.content_type,
                    &event.sha256,
                    &rel,
                ) {
                    Ok(()) => (StatusCode::NO_CONTENT, ()).into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response(),
                }
            }
        }
        "delete" => match db.delete_object(&event.bucket, &event.key) {
            Ok(_) => (StatusCode::NO_CONTENT, ()).into_response(),
            // Idempotent: already gone = success. Primaries that retry
            // should not cause spurious 4xx on the replica.
            Err(_) => (StatusCode::NO_CONTENT, ()).into_response(),
        },
        "delete_marker" => {
            let vid = match &event.version_id {
                Some(v) => v.clone(),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "delete_marker needs version_id"})),
                    )
                        .into_response()
                }
            };
            match db.save_delete_marker(&event.bucket, &event.key, &vid) {
                Ok(()) => (StatusCode::NO_CONTENT, ()).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response(),
            }
        }
        other => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("unknown op: {other}")})),
        )
            .into_response(),
    }
}
