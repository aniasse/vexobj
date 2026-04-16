use axum::body::Body;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::put;
use axum::{Json, Router};
use futures::TryStreamExt;
use serde_json::json;

use crate::middleware::require_permission;
use crate::state::AppState;
use vaultfs_auth::ApiKey;

/// Streaming endpoints for large files.
/// - PUT streams body directly to disk (constant RAM usage)
/// - GET streams file from disk to client (constant RAM usage)
pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/v1/stream/{bucket}/{*key}",
        put(stream_put).get(stream_get),
    )
}

async fn stream_put(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "write").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Convert axum Body into a Stream<Item = Result<Bytes, _>>
    let stream = body
        .into_data_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    match state
        .storage
        .put_object_stream(&bucket, &key, stream, content_type.as_deref(), None)
        .await
    {
        Ok(meta) => {
            if let Some(ref wh) = state.webhooks {
                wh.send("object.created", json!({
                    "bucket": meta.bucket,
                    "key": meta.key,
                    "size": meta.size,
                    "content_type": meta.content_type,
                    "sha256": meta.sha256,
                }));
            }
            (StatusCode::CREATED, Json(json!(meta))).into_response()
        }
        Err(e) => {
            let status = match &e {
                vaultfs_storage::StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                vaultfs_storage::StorageError::ObjectTooLarge { .. } => {
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn stream_get(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    match state.storage.get_object_stream(&bucket, &key).await {
        Ok((meta, stream)) => {
            let body = Body::from_stream(stream);
            (
                StatusCode::OK,
                [
                    ("content-type", meta.content_type),
                    ("content-length", meta.size.to_string()),
                    ("etag", format!("\"{}\"", meta.sha256)),
                    ("accept-ranges", "bytes".to_string()),
                ],
                body,
            )
                .into_response()
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "object not found"})),
        )
            .into_response(),
    }
}
