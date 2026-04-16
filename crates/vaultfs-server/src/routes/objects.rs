use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::json;

use crate::middleware::require_permission;
use crate::state::AppState;
use vaultfs_auth::ApiKey;
use vaultfs_processing::{
    best_format_from_accept, transform_image, FitMode, OutputFormat, TransformParams,
};
use vaultfs_storage::{ListObjectsRequest, StorageEngine};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/objects/{bucket}", get(list_objects))
        .route(
            "/v1/objects/{bucket}/{*key}",
            put(put_object)
                .get(get_object)
                .delete(delete_object)
                .head(head_object),
        )
}

async fn put_object(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
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

    match state
        .storage
        .put_object(&bucket, &key, body, content_type.as_deref(), None)
        .await
    {
        Ok(meta) => (StatusCode::CREATED, Json(json!(meta))).into_response(),
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

#[derive(Deserialize, Default)]
struct GetObjectQuery {
    w: Option<u32>,
    h: Option<u32>,
    format: Option<String>,
    quality: Option<u8>,
    fit: Option<String>,
    // Presigned URL params
    #[allow(dead_code)]
    expires: Option<i64>,
    #[allow(dead_code)]
    signature: Option<String>,
}

async fn get_object(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<GetObjectQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    let (meta, data) = match state.storage.get_object(&bucket, &key).await {
        Ok(result) => result,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "object not found"})),
            )
                .into_response()
        }
    };

    let is_image = StorageEngine::is_image(&meta.content_type);
    let has_transform = query.w.is_some()
        || query.h.is_some()
        || query.format.is_some()
        || query.quality.is_some();

    if is_image && has_transform {
        let accept = headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let format = query
            .format
            .as_deref()
            .and_then(OutputFormat::from_str)
            .or_else(|| best_format_from_accept(accept));

        let params = TransformParams {
            width: query.w,
            height: query.h,
            format,
            quality: query.quality,
            fit: match query.fit.as_deref() {
                Some("contain") => FitMode::Contain,
                Some("fill") => FitMode::Fill,
                _ => FitMode::Cover,
            },
        };

        let cache_key = format!("{}/{}/{}", bucket, key, params.cache_key());
        if let Some((cached_data, cached_type)) = state.cache.get(&cache_key).await {
            return (
                StatusCode::OK,
                [
                    ("content-type", cached_type),
                    ("x-vaultfs-cache", "hit".to_string()),
                ],
                cached_data,
            )
                .into_response();
        }

        match transform_image(&data, &params) {
            Ok((transformed, content_type)) => {
                let bytes = Bytes::from(transformed);
                let _ = state.cache.put(&cache_key, bytes.clone(), &content_type).await;
                (
                    StatusCode::OK,
                    [
                        ("content-type", content_type),
                        ("x-vaultfs-cache", "miss".to_string()),
                    ],
                    bytes,
                )
                    .into_response()
            }
            Err(e) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error": e.to_string()})),
            )
                .into_response(),
        }
    } else {
        (
            StatusCode::OK,
            [
                ("content-type", meta.content_type.clone()),
                ("content-length", meta.size.to_string()),
                ("etag", format!("\"{}\"", meta.sha256)),
            ],
            data,
        )
            .into_response()
    }
}

async fn head_object(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }

    match state.storage.get_object_meta(&bucket, &key) {
        Ok(meta) => (
            StatusCode::OK,
            [
                ("content-type", meta.content_type),
                ("content-length", meta.size.to_string()),
                ("etag", format!("\"{}\"", meta.sha256)),
                ("last-modified", meta.updated_at.to_rfc2822()),
            ],
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_object(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "delete").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    match state.storage.delete_object(&bucket, &key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "object not found"})),
        )
            .into_response(),
    }
}

#[derive(Deserialize, Default)]
struct ListQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    max_keys: Option<u32>,
    continuation_token: Option<String>,
}

async fn list_objects(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }

    match state.storage.list_objects(&ListObjectsRequest {
        bucket,
        prefix: query.prefix,
        delimiter: query.delimiter,
        max_keys: query.max_keys,
        continuation_token: query.continuation_token,
    }) {
        Ok(response) => Json(json!(response)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
