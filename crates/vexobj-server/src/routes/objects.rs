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
use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;
use vexobj_processing::{
    best_format_from_accept, transform_image, FitMode, OutputFormat, TransformParams,
};
use vexobj_storage::{ListObjectsRequest, StorageEngine};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/objects/{bucket}", get(list_objects))
        .route(
            "/v1/objects/{bucket}/{*key}",
            put(put_object)
                // POST with ?source=<url>: server-side fetch of a remote URL,
                // streamed into the bucket under the given key. Useful for
                // Mastodon media backfill without pulling bytes through the
                // client.
                .post(post_object_from_url)
                .get(get_object)
                .delete(delete_object)
                .head(head_object),
        )
        .route(
            "/v1/versions/{bucket}/{*key}",
            get(list_versions).delete(purge_versions),
        )
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

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let ip = extract_ip(&headers);

    // Stream body to disk (constant RAM)
    let stream = body.into_data_stream().map_err(std::io::Error::other);

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
                wh.send(
                    "object.created",
                    json!({
                        "bucket": meta.bucket,
                        "key": meta.key,
                        "size": meta.size,
                        "content_type": meta.content_type,
                        "sha256": meta.sha256,
                    }),
                );
            }
            (StatusCode::CREATED, Json(json!(meta))).into_response()
        }
        Err(e) => {
            let status = match &e {
                vexobj_storage::StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                vexobj_storage::StorageError::ObjectTooLarge { .. } => {
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                vexobj_storage::StorageError::QuotaExceeded { .. } => {
                    StatusCode::INSUFFICIENT_STORAGE
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

#[derive(Deserialize, Default)]
struct SourceUrlQuery {
    source: Option<String>,
}

/// POST /v1/objects/{bucket}/{key}?source=<url>
///
/// Server-side fetch-and-store. The remote URL is streamed in 16 KiB
/// chunks via put_object_stream, so a multi-GB source doesn't load into
/// RAM. Only http/https schemes are accepted; by default URLs resolving
/// to private / loopback / link-local addresses are refused (SSRF
/// protection). The AWS metadata IP 169.254.169.254 is always refused.
async fn post_object_from_url(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<SourceUrlQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    use futures::TryStreamExt;

    if let Err(resp) = require_permission(&caller, "write").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }
    let Some(url_str) = q.source else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing ?source=<url>"})),
        )
            .into_response();
    };
    let url = match url::Url::parse(&url_str) {
        Ok(u) => u,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "malformed source URL"})),
            )
                .into_response();
        }
    };
    if !matches!(url.scheme(), "http" | "https") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "only http/https schemes are supported"})),
        )
            .into_response();
    }
    if let Err(reason) = ssrf_check(&url, state.config.storage.allow_private_source_urls).await {
        return (StatusCode::FORBIDDEN, Json(json!({"error": reason}))).into_response();
    }

    // Bounded timeout so a slow source doesn't wedge a server thread.
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("client: {e}")})),
            )
                .into_response();
        }
    };
    let resp = match client.get(url.as_str()).send().await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": format!("fetch failed: {e}")})),
            )
                .into_response();
        }
    };
    if !resp.status().is_success() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": format!("source responded {}", resp.status().as_u16())})),
        )
            .into_response();
    }
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let stream = resp.bytes_stream().map_err(std::io::Error::other);
    let ip = extract_ip(&headers);

    match state
        .storage
        .put_object_stream(&bucket, &key, stream, content_type.as_deref(), None)
        .await
    {
        Ok(meta) => {
            state.metrics.record_upload(meta.size);
            state.audit.log(
                &key_prefix(&caller),
                "object.create.from_url",
                &format!("{}/{}", meta.bucket, meta.key),
                &json!({"size": meta.size, "content_type": meta.content_type, "source": url_str}),
                &ip,
            );
            (StatusCode::CREATED, Json(json!(meta))).into_response()
        }
        Err(e) => {
            let status = match &e {
                vexobj_storage::StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                vexobj_storage::StorageError::ObjectTooLarge { .. } => {
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                vexobj_storage::StorageError::QuotaExceeded { .. } => {
                    StatusCode::INSUFFICIENT_STORAGE
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

/// Resolve the URL's host and reject addresses that would expose the
/// VexObj process as an SSRF relay. 169.254.169.254 (AWS instance
/// metadata) is always refused regardless of the allow-private flag.
async fn ssrf_check(url: &url::Url, allow_private: bool) -> Result<(), String> {
    use std::net::{IpAddr, Ipv4Addr};

    let host = url.host_str().ok_or("source URL has no host")?;
    let port = url.port_or_known_default().unwrap_or(80);
    let addrs = tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("dns lookup: {e}"))?;
    let mut seen = false;
    for addr in addrs {
        seen = true;
        let ip = addr.ip();
        // Always refuse the cloud-metadata address — it's never a
        // legitimate source for a media upload.
        if ip == IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)) {
            return Err("source URL resolves to cloud-metadata address".into());
        }
        if !allow_private && is_private_or_local(&ip) {
            return Err("source URL resolves to a private-network address".into());
        }
    }
    if !seen {
        return Err("source URL did not resolve".into());
    }
    Ok(())
}

/// True for any IP the VexObj process shouldn't follow outbound requests
/// to in a typical production deployment.
fn is_private_or_local(ip: &std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                // Unique-local fc00::/7
                || (v6.segments()[0] & 0xfe00 == 0xfc00)
                // Link-local fe80::/10
                || (v6.segments()[0] & 0xffc0 == 0xfe80)
                // IPv4-mapped — check the mapped v4
                || v6.to_ipv4_mapped()
                    .is_some_and(|v4| {
                        v4.is_loopback() || v4.is_private() || v4.is_link_local()
                    })
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
    /// Video thumbnail request: when "1"/"true", extract a frame at
    /// `t` seconds (default 1.0), scaled to `w` pixels wide, encoded
    /// as `format` (jpeg default, webp supported).
    thumbnail: Option<String>,
    t: Option<f64>,
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

    // Video thumbnail request: branch out of the object-serve path
    // completely. ffmpeg does the work, the result is cached by
    // (sha256, t, w, format, quality) so repeats hit the LRU.
    if matches!(query.thumbnail.as_deref(), Some("1") | Some("true")) {
        return serve_video_thumbnail(&state, &bucket, &key, &query).await;
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
                        ("x-vexobj-version-id", version.version_id),
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

    let has_transform =
        query.w.is_some() || query.h.is_some() || query.format.is_some() || query.quality.is_some();

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
                .and_then(OutputFormat::parse)
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
                        ("x-vexobj-cache", "hit".to_string()),
                    ],
                    cached_data,
                )
                    .into_response();
            }

            match transform_image(&data, &params) {
                Ok((transformed, content_type)) => {
                    let bytes = Bytes::from(transformed);
                    let _ = state
                        .cache
                        .put(&cache_key, bytes.clone(), &content_type)
                        .await;
                    return (
                        StatusCode::OK,
                        [
                            ("content-type", content_type),
                            ("x-vexobj-cache", "miss".to_string()),
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
        if start >= total {
            return None;
        }
        Some((start, total))
    } else {
        let start: u64 = start_str.parse().ok()?;
        let end: u64 = end_str.parse().ok()?;
        if start >= total {
            return None;
        }
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
        Ok(meta) => {
            // Build the always-present headers first, then optionally
            // append video-specific headers when the object carries them.
            // Axum's tuple-of-array builder wants a fixed shape, so we
            // collect into a Vec and hand it over at the end.
            let mut headers: Vec<(&'static str, String)> = vec![
                ("content-type", meta.content_type.clone()),
                ("content-length", meta.size.to_string()),
                ("etag", format!("\"{}\"", meta.sha256)),
                ("last-modified", meta.updated_at.to_rfc2822()),
                ("accept-ranges", "bytes".to_string()),
            ];

            if let Some(video) = meta.metadata.get("video") {
                if let Some(d) = video.get("duration_secs").and_then(|v| v.as_f64()) {
                    headers.push(("x-vexobj-video-duration", format!("{d:.3}")));
                }
                if let Some(w) = video.get("width").and_then(|v| v.as_u64()) {
                    headers.push(("x-vexobj-video-width", w.to_string()));
                }
                if let Some(h) = video.get("height").and_then(|v| v.as_u64()) {
                    headers.push(("x-vexobj-video-height", h.to_string()));
                }
                if let Some(c) = video.get("codec").and_then(|v| v.as_str()) {
                    headers.push(("x-vexobj-video-codec", c.to_string()));
                }
                if let Some(a) = video.get("has_audio").and_then(|v| v.as_bool()) {
                    headers.push(("x-vexobj-video-has-audio", a.to_string()));
                }
            }

            let mut resp = StatusCode::OK.into_response();
            let h = resp.headers_mut();
            for (name, value) in headers {
                if let Ok(v) = axum::http::HeaderValue::from_str(&value) {
                    h.insert(name, v);
                }
            }
            resp
        }
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize, Default)]
struct DeleteObjectQuery {
    version_id: Option<String>,
}

async fn delete_object(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<DeleteObjectQuery>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "delete").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    let ip = extract_ip(&headers);

    if let Some(ref vid) = query.version_id {
        return match state.storage.delete_version(&bucket, &key, vid).await {
            Ok(()) => {
                state.audit.log(
                    &key_prefix(&caller),
                    "object.version.delete",
                    &format!("{}/{}", bucket, key),
                    &json!({"version_id": vid}),
                    &ip,
                );
                StatusCode::NO_CONTENT.into_response()
            }
            Err(_) => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "version not found"})),
            )
                .into_response(),
        };
    }

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
        Err(vexobj_storage::StorageError::ObjectLocked { reason, .. }) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "object is locked", "reason": reason})),
        )
            .into_response(),
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

/// Serve a video thumbnail via ffmpeg. 501 if ffmpeg isn't on PATH.
/// Errors from ffmpeg itself (corrupt file, unreadable container) are
/// surfaced as 422 so callers can distinguish "server missing a tool"
/// from "this particular video can't be thumbnailed".
async fn serve_video_thumbnail(
    state: &AppState,
    bucket: &str,
    key: &str,
    query: &GetObjectQuery,
) -> axum::response::Response {
    if !state.storage.video_features().ffmpeg {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": "video thumbnails require ffmpeg on the server's PATH",
                "hint": "install ffmpeg and restart vexobj; no other config needed",
            })),
        )
            .into_response();
    }

    // Shape of inputs is sanitized inside ThumbRequest; any out-of-range
    // values are clamped rather than rejected so clients don't need to
    // know the limits.
    let req = vexobj_processing::ThumbRequest::sanitized(
        query.t,
        query.w,
        query.format.as_deref(),
        query.quality,
    );

    // Pull meta first — we need the sha256 for the cache key and the
    // content_type to reject obvious non-videos before shelling out.
    let meta = match state.storage.get_object_meta(bucket, key) {
        Ok(m) => m,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "object not found"})),
            )
                .into_response();
        }
    };
    if !meta.content_type.starts_with("video/") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "thumbnail requested on a non-video object",
                "content_type": meta.content_type,
            })),
        )
            .into_response();
    }

    let cache_key = req.cache_key(&meta.sha256);

    // Fast path: cache hit.
    if let Some((bytes, ct)) = state.cache.get(&cache_key).await {
        return (
            StatusCode::OK,
            [("content-type", ct), ("x-vexobj-cache", "hit".to_string())],
            bytes,
        )
            .into_response();
    }

    // Cache miss: resolve the file path, generate the thumbnail on a
    // blocking thread (ffmpeg is synchronous), cache the result.
    let src_path = match state.storage.object_data_path(bucket, key) {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "object not found"})),
            )
                .into_response();
        }
    };

    // SSE-on stores ciphertext — ffmpeg would choke on that. We could
    // decrypt to a temp file first, but it's a real perf hit and the
    // docs already flag video features as plaintext-only. Bail clearly.
    if state.storage.encryption_enabled() {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": "thumbnails aren't available when SSE-at-rest is enabled",
                "hint": "serve videos from an instance without sse.enabled=true",
            })),
        )
            .into_response();
    }

    let req_clone = req.clone();
    let thumbnail_result = tokio::task::spawn_blocking(move || {
        vexobj_processing::generate_thumbnail(&src_path, &req_clone)
    })
    .await;

    let bytes = match thumbnail_result {
        Ok(Ok(b)) => b,
        Ok(Err(vexobj_processing::ThumbError::FfmpegMissing)) => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                Json(json!({"error": "ffmpeg not available"})),
            )
                .into_response();
        }
        Ok(Err(vexobj_processing::ThumbError::Timeout)) => {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({"error": "thumbnail generation timed out"})),
            )
                .into_response();
        }
        Ok(Err(e)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error": format!("{e}")})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("task panicked: {e}")})),
            )
                .into_response();
        }
    };

    let body = bytes::Bytes::from(bytes);
    let ct = req.format.mime().to_string();
    let _ = state.cache.put(&cache_key, body.clone(), &ct).await;

    (
        StatusCode::OK,
        [("content-type", ct), ("x-vexobj-cache", "miss".to_string())],
        body,
    )
        .into_response()
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

async fn purge_versions(
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

    match state.storage.purge_versions(&bucket, &key).await {
        Ok(blobs_removed) => {
            state.audit.log(
                &key_prefix(&caller),
                "object.versions.purge",
                &format!("{}/{}", bucket, key),
                &json!({"blobs_removed": blobs_removed}),
                &ip,
            );
            Json(json!({"bucket": bucket, "key": key, "blobs_removed": blobs_removed}))
                .into_response()
        }
        Err(vexobj_storage::StorageError::ObjectLocked { reason, .. }) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "object is locked", "reason": reason})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
