use axum::body::Body;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::{Json, Router};
use bytes::Bytes;
use futures::TryStreamExt;
use serde::Deserialize;
use serde_json::json;

use crate::audit::{extract_ip, key_prefix};
use crate::config::parse_size;
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
        .route("/v1/versions/{bucket}/{*key}", get(list_versions))
}

/// PUT now streams to disk by default — constant RAM regardless of file size.
/// Only falls back to in-memory for small files that need image transforms.
async fn put_object(
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

    // Quota check before upload
    if state.config.quotas.enabled {
        match state.storage.db().bucket_storage_stats(&bucket) {
            Ok((total_size, object_count)) => {
                let max_storage = parse_size(&state.config.quotas.default_max_storage);
                let max_objects = state.config.quotas.default_max_objects;

                if total_size >= max_storage {
                    return (
                        StatusCode::INSUFFICIENT_STORAGE,
                        Json(json!({
                            "error": "bucket storage quota exceeded",
                            "current_size": total_size,
                            "max_size": max_storage,
                        })),
                    )
                        .into_response();
                }
                if object_count >= max_objects {
                    return (
                        StatusCode::INSUFFICIENT_STORAGE,
                        Json(json!({
                            "error": "bucket object count quota exceeded",
                            "current_objects": object_count,
                            "max_objects": max_objects,
                        })),
                    )
                        .into_response();
                }
            }
            Err(_) => {
                // If we can't check quota (e.g. bucket doesn't exist), let the put fail naturally
            }
        }
    }

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let ip = extract_ip(&headers);

    // Stream body to disk (constant RAM)
    let stream = body
        .into_data_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

    match state
        .storage
        .put_object_stream(&bucket, &key, stream, content_type.as_deref(), None)
        .await
    {
        Ok(meta) => {
            state.metrics.record_upload(meta.size);
            state.audit.log(
                &key_prefix(&caller),
                "object.create",
                &format!("{}/{}", meta.bucket, meta.key),
                &json!({"size": meta.size, "content_type": meta.content_type, "sha256": meta.sha256}),
                &ip,
            );
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

#[derive(Deserialize, Default)]
struct GetObjectQuery {
    w: Option<u32>,
    h: Option<u32>,
    format: Option<String>,
    quality: Option<u8>,
    fit: Option<String>,
    #[allow(dead_code)]
    expires: Option<i64>,
    #[allow(dead_code)]
    signature: Option<String>,
    version_id: Option<String>,
}

/// GET now streams from disk by default for non-image files.
/// Images with transforms still load into memory for processing.
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

    // If a specific version is requested, serve it directly
    if let Some(ref vid) = query.version_id {
        return match state.storage.get_version_data(&bucket, &key, vid).await {
            Ok((version, data)) => {
                state.metrics.record_download(version.size);
                (
                    StatusCode::OK,
                    [
                        ("content-type", version.content_type),
                        ("content-length", version.size.to_string()),
                        ("etag", format!("\"{}\"", version.sha256)),
                        ("x-vaultfs-version-id", version.version_id),
                    ],
                    data,
                )
                    .into_response()
            }
            Err(_) => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "version not found"})),
            )
                .into_response(),
        };
    }

    let has_transform = query.w.is_some()
        || query.h.is_some()
        || query.format.is_some()
        || query.quality.is_some();

    let has_range = headers.get("range").is_some();

    // For image transforms or range requests, we need the data in memory
    if has_transform || has_range {
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
                    return (
                        StatusCode::OK,
                        [
                            ("content-type", content_type),
                            ("x-vaultfs-cache", "miss".to_string()),
                        ],
                        bytes,
                    )
                        .into_response();
                }
                Err(e) => {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            }
        }

        // Range request
        if let Some(range_header) = headers.get("range").and_then(|v| v.to_str().ok()) {
            if let Some((start, end)) = parse_range(range_header, data.len() as u64) {
                let slice = data.slice(start as usize..end as usize);
                let content_range = format!("bytes {}-{}/{}", start, end - 1, meta.size);
                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        ("content-type", meta.content_type.clone()),
                        ("content-length", slice.len().to_string()),
                        ("content-range", content_range),
                        ("accept-ranges", "bytes".to_string()),
                        ("etag", format!("\"{}\"", meta.sha256)),
                    ],
                    slice,
                )
                    .into_response();
            }
        }

        // Fallback: serve in-memory
        return (
            StatusCode::OK,
            [
                ("content-type", meta.content_type.clone()),
                ("content-length", meta.size.to_string()),
                ("etag", format!("\"{}\"", meta.sha256)),
                ("accept-ranges", "bytes".to_string()),
            ],
            data,
        )
            .into_response();
    }

    // Default: stream from disk (constant RAM)
    match state.storage.get_object_stream(&bucket, &key).await {
        Ok((meta, stream)) => {
            state.metrics.record_download(meta.size);
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

fn parse_range(header: &str, total: u64) -> Option<(u64, u64)> {
    let range = header.strip_prefix("bytes=")?;
    let (start_str, end_str) = range.split_once('-')?;

    if start_str.is_empty() {
        let suffix: u64 = end_str.parse().ok()?;
        let start = total.saturating_sub(suffix);
        Some((start, total))
    } else if end_str.is_empty() {
        let start: u64 = start_str.parse().ok()?;
        if start >= total { return None; }
        Some((start, total))
    } else {
        let start: u64 = start_str.parse().ok()?;
        let end: u64 = end_str.parse().ok()?;
        if start >= total { return None; }
        Some((start, (end + 1).min(total)))
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
                ("accept-ranges", "bytes".to_string()),
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
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "delete").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    let ip = extract_ip(&headers);

    match state.storage.delete_object(&bucket, &key).await {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "object.delete",
                &format!("{}/{}", bucket, key),
                &json!({}),
                &ip,
            );
            if let Some(ref wh) = state.webhooks {
                wh.send("object.deleted", json!({"bucket": bucket, "key": key}));
            }
            StatusCode::NO_CONTENT.into_response()
        }
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

async fn list_versions(
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

    match state.storage.list_versions(&bucket, &key) {
        Ok(versions) => Json(json!({"versions": versions})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
