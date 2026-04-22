use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use futures::TryStreamExt;
use serde::Deserialize;

use crate::error::S3Error;
use crate::signature::{
    parse_auth_header, parse_presign_query, verify_sigv4, verify_sigv4_presigned,
};
use crate::xml;
use vexobj_auth::AuthManager;
use vexobj_storage::StorageEngine;

/// Ceiling for non-multipart S3 PUT bodies buffered into memory (16 MiB).
/// Above this clients must use the multipart protocol, which streams each
/// part to disk.
const S3_SINGLEPUT_MAX: usize = 16 * 1024 * 1024;

/// Cap on the CompleteMultipartUpload request body. The XML is tiny — a
/// few hundred bytes per part × 10 000 parts max.
const S3_COMPLETE_MAX: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct S3State {
    pub storage: Arc<StorageEngine>,
    pub auth: Arc<AuthManager>,
}

/// Create the S3-compatible router.
/// Mount this at the root alongside the vexobj native API.
pub fn s3_router(storage: Arc<StorageEngine>, auth: Arc<AuthManager>) -> Router {
    let state = S3State { storage, auth };

    Router::new()
        // Service-level (list buckets)
        .route("/s3", any(s3_service))
        .route("/s3/", any(s3_service))
        // Bucket-level operations
        .route("/s3/{bucket}", any(s3_bucket))
        // Object-level operations
        .route("/s3/{bucket}/{*key}", any(s3_object))
        .with_state(state)
}

/// Authenticate an S3 request.
///
/// Supports two modes:
/// - `Authorization: AWS4-HMAC-SHA256 ...` — full SigV4 signature check: we
///   recompute the canonical request from what the server actually received
///   and compare HMACs. Rejects requests where the signed bytes differ from
///   the received bytes (tamper / replay different URL etc.).
/// - `Authorization: Bearer <key>` — convenience shortcut, no signature
///   verification. Useful for `curl` / development but NOT for untrusted
///   networks; callers who care should use SigV4.
fn authenticate(
    state: &S3State,
    method: &str,
    uri_path: &str,
    query: &str,
    headers: &HeaderMap,
) -> Result<(), S3Error> {
    // Query-string presigned URL: the client has no Authorization header and
    // the request carries X-Amz-Signature in the URL. Verify that branch
    // before we'd otherwise error on the missing header.
    if query.to_ascii_lowercase().contains("x-amz-signature=") {
        let parsed = parse_presign_query(query).ok_or_else(S3Error::access_denied)?;
        let (_api_key, secret) = state
            .auth
            .find_by_access_key(&parsed.access_key)
            .map_err(|_| S3Error::access_denied())?;
        if secret.is_empty() {
            return Err(S3Error::access_denied());
        }
        let header_pairs: Vec<(String, String)> = headers
            .iter()
            .filter_map(|(n, v)| {
                v.to_str()
                    .ok()
                    .map(|s| (n.as_str().to_string(), s.to_string()))
            })
            .collect();
        if !verify_sigv4_presigned(method, uri_path, query, &header_pairs, &secret, &parsed) {
            return Err(S3Error::access_denied());
        }
        return Ok(());
    }

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(S3Error::access_denied)?;

    if auth_header.starts_with("AWS4-HMAC-SHA256") {
        let parsed = parse_auth_header(auth_header).ok_or_else(S3Error::access_denied)?;
        let (_api_key, secret) = state
            .auth
            .find_by_access_key(&parsed.access_key)
            .map_err(|_| S3Error::access_denied())?;
        if secret.is_empty() {
            // Legacy row with no stored plaintext — can't verify SigV4. The
            // caller must rotate to a freshly-issued key to use SigV4.
            return Err(S3Error::access_denied());
        }

        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if payload_hash.is_empty() {
            return Err(S3Error::access_denied());
        }

        let header_pairs: Vec<(String, String)> = headers
            .iter()
            .filter_map(|(n, v)| {
                v.to_str()
                    .ok()
                    .map(|s| (n.as_str().to_string(), s.to_string()))
            })
            .collect();

        if !verify_sigv4(
            method,
            uri_path,
            query,
            &header_pairs,
            payload_hash,
            &secret,
            &parsed,
        ) {
            return Err(S3Error::access_denied());
        }
        Ok(())
    } else if let Some(key) = auth_header.strip_prefix("Bearer ") {
        state
            .auth
            .verify_key(key)
            .map_err(|_| S3Error::access_denied())?;
        Ok(())
    } else {
        Err(S3Error::access_denied())
    }
}

// ─── Service Level ───────────────────────────────────────────

async fn s3_service(
    State(state): State<S3State>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    if let Err(e) = authenticate(
        &state,
        "GET",
        uri.path(),
        uri.query().unwrap_or(""),
        &headers,
    ) {
        return e.into_response();
    }

    match state.storage.list_buckets() {
        Ok(buckets) => {
            let body = xml::list_buckets_xml(&buckets, "vexobj");
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

// ─── Bucket Level ────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct BucketQuery {
    prefix: Option<String>,
    delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    max_keys: Option<u32>,
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
}

async fn s3_bucket(
    State(state): State<S3State>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path(bucket): Path<String>,
    Query(query): Query<BucketQuery>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // POST with multipart/form-data is the S3 presigned-POST upload flow.
    // It's authenticated entirely by fields inside the form (policy +
    // x-amz-signature), not by an Authorization header, so the regular
    // `authenticate()` path has to be skipped for this case.
    if method == Method::POST {
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if ct.starts_with("multipart/form-data") {
            return crate::presigned_post::handle_presigned_post(&state, &bucket, headers, body)
                .await;
        }
    }

    if let Err(e) = authenticate(
        &state,
        method.as_str(),
        uri.path(),
        uri.query().unwrap_or(""),
        &headers,
    ) {
        return e.into_response();
    }

    // POST /<bucket>?delete is the S3 DeleteObjects (bulk-delete) op.
    // PeerTube leans on it heavily when cleaning up old videos and thumbs.
    if method == Method::POST {
        let q = uri.query().unwrap_or("");
        if q.split('&')
            .any(|p| p == "delete" || p.starts_with("delete="))
        {
            return delete_objects(&state, &bucket, body).await;
        }
    }

    match method {
        Method::PUT => create_bucket(&state, &bucket).await,
        Method::DELETE => delete_bucket(&state, &bucket).await,
        Method::HEAD => head_bucket(&state, &bucket).await,
        Method::GET => list_objects_v2(&state, &bucket, query).await,
        _ => S3Error::invalid_request("Method not allowed").into_response(),
    }
}

async fn create_bucket(state: &S3State, bucket: &str) -> Response {
    use vexobj_storage::CreateBucketRequest;

    match state.storage.create_bucket(&CreateBucketRequest {
        name: bucket.to_string(),
        public: false,
    }) {
        Ok(_) => (StatusCode::OK, [("location", format!("/{bucket}"))], "").into_response(),
        Err(vexobj_storage::StorageError::BucketAlreadyExists(_)) => {
            S3Error::bucket_already_exists(bucket).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn delete_bucket(state: &S3State, bucket: &str) -> Response {
    match state.storage.delete_bucket(bucket) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(vexobj_storage::StorageError::BucketNotFound(msg)) if msg.contains("not empty") => {
            S3Error::bucket_not_empty(bucket).into_response()
        }
        Err(_) => S3Error::no_such_bucket(bucket).into_response(),
    }
}

async fn head_bucket(state: &S3State, bucket: &str) -> Response {
    match state.storage.get_bucket(bucket) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => S3Error::no_such_bucket(bucket).into_response(),
    }
}

/// Handle `POST /<bucket>?delete`. Up to 1000 keys (S3 caps at 1000 too);
/// request bodies are tiny XML so the 1 MiB cap on to_bytes is ample.
/// Idempotent: deleting a missing key is a success (S3 returns it in
/// <Deleted> even when the object never existed).
async fn delete_objects(state: &S3State, bucket: &str, body: Body) -> Response {
    const MAX_BODY: usize = 1024 * 1024;

    let bytes = match to_bytes(body, MAX_BODY).await {
        Ok(b) => b,
        Err(_) => return S3Error::malformed_xml().into_response(),
    };
    let xml_str = match std::str::from_utf8(&bytes) {
        Ok(s) => s,
        Err(_) => return S3Error::malformed_xml().into_response(),
    };
    let (keys, quiet) = xml::parse_delete_request(xml_str);
    if keys.is_empty() {
        return S3Error::malformed_xml().into_response();
    }
    if keys.len() > 1000 {
        return S3Error::invalid_request("DeleteObjects accepts at most 1000 keys").into_response();
    }

    let mut deleted: Vec<String> = Vec::with_capacity(keys.len());
    let mut errors: Vec<(String, String, String)> = Vec::new();
    for key in keys {
        match state.storage.delete_object(bucket, &key).await {
            Ok(()) => deleted.push(key),
            Err(vexobj_storage::StorageError::ObjectNotFound { .. }) => {
                // Treat as a success, matching the idempotency S3 itself
                // documents. PeerTube relies on this to clean up lists
                // that may contain already-deleted keys.
                deleted.push(key);
            }
            Err(vexobj_storage::StorageError::BucketNotFound(_)) => {
                return S3Error::no_such_bucket(bucket).into_response();
            }
            Err(e) => errors.push((key, "InternalError".to_string(), e.to_string())),
        }
    }
    let body = xml::delete_result_xml(&deleted, &errors, quiet);
    (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
}

async fn list_objects_v2(state: &S3State, bucket: &str, query: BucketQuery) -> Response {
    let max_keys = query.max_keys.unwrap_or(1000);
    let prefix = query.prefix.as_deref().unwrap_or("");
    let delimiter = query.delimiter.as_deref().unwrap_or("");

    let req = vexobj_storage::ListObjectsRequest {
        bucket: bucket.to_string(),
        prefix: Some(prefix.to_string()),
        delimiter: if delimiter.is_empty() {
            None
        } else {
            Some(delimiter.to_string())
        },
        max_keys: Some(max_keys),
        continuation_token: query.continuation_token,
    };

    match state.storage.list_objects(&req) {
        Ok(resp) => {
            let body = xml::list_objects_v2_xml(bucket, prefix, &resp, max_keys, delimiter);
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

// ─── Object Level ────────────────────────────────────────────

async fn s3_object(
    State(state): State<S3State>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // Public-bucket read bypass — mirror of the native middleware logic.
    // GET/HEAD on `/s3/{bucket}/{*key}` goes through without a signature
    // when the bucket is marked public, so Mastodon/Peertube-style
    // browser clients can fetch media directly.
    let is_public_read = matches!(method, Method::GET | Method::HEAD)
        && state
            .storage
            .get_bucket(&bucket)
            .map(|b| b.public)
            .unwrap_or(false);

    if !is_public_read {
        if let Err(e) = authenticate(
            &state,
            method.as_str(),
            uri.path(),
            uri.query().unwrap_or(""),
            &headers,
        ) {
            return e.into_response();
        }
    }

    // ── Multipart sub-resources ────────────────────────────────────────
    //
    // S3 overloads `/bucket/key` with query-string verbs. We check them up
    // front so the standard verb handlers never see a multipart request.
    let qs = uri.query().unwrap_or("");
    let has_uploads = qs == "uploads" || qs.starts_with("uploads&") || qs.contains("&uploads");
    let upload_id = extract_query_param(qs, "uploadId");
    let part_number = extract_query_param(qs, "partNumber").and_then(|v| v.parse::<u32>().ok());

    if method == Method::POST && has_uploads {
        return initiate_multipart(&state, &bucket, &key, &headers).await;
    }
    if let Some(id) = &upload_id {
        return match (method.clone(), part_number) {
            (Method::PUT, Some(pn)) => upload_part(&state, &bucket, id, pn, body).await,
            (Method::POST, None) => complete_multipart(&state, &bucket, id, body, &uri).await,
            (Method::DELETE, None) => abort_multipart(&state, id).await,
            (Method::GET, None) => list_parts(&state, &bucket, &key, id).await,
            _ => S3Error::invalid_request(
                "Invalid combination of method and multipart sub-resources",
            )
            .into_response(),
        };
    }

    // ── Standard verbs ──────────────────────────────────────────────────
    match method {
        Method::PUT => {
            // Buffer non-multipart PUTs into memory up to the cap, then
            // hand to the existing put_object path. Clients needing more
            // than 16 MiB should use the multipart protocol.
            match to_bytes(body, S3_SINGLEPUT_MAX).await {
                Ok(b) => put_object(&state, &bucket, &key, headers, b).await,
                Err(_) => S3Error::entity_too_large().into_response(),
            }
        }
        Method::GET => get_object(&state, &bucket, &key, &headers).await,
        Method::HEAD => head_object(&state, &bucket, &key).await,
        Method::DELETE => delete_object(&state, &bucket, &key).await,
        _ => S3Error::invalid_request("Method not allowed").into_response(),
    }
}

/// Minimal query-string parser that handles valueless keys like `?uploads`
/// as well as `foo=bar` pairs. Not URL-decoding values — S3 part numbers
/// and upload IDs don't need it.
fn extract_query_param<'a>(qs: &'a str, name: &str) -> Option<&'a str> {
    for pair in qs.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == name {
                return Some(v);
            }
        } else if pair == name {
            return Some("");
        }
    }
    None
}

async fn put_object(
    state: &S3State,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for copy operation
    if let Some(copy_source) = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
    {
        return copy_object(state, bucket, key, copy_source).await;
    }

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match state
        .storage
        .put_object(bucket, key, body, content_type.as_deref(), None)
        .await
    {
        Ok(meta) => (
            StatusCode::OK,
            [("etag", format!("\"{}\"", meta.sha256))],
            "",
        )
            .into_response(),
        Err(vexobj_storage::StorageError::BucketNotFound(_)) => {
            S3Error::no_such_bucket(bucket).into_response()
        }
        Err(vexobj_storage::StorageError::ObjectTooLarge { .. }) => {
            S3Error::entity_too_large().into_response()
        }
        Err(vexobj_storage::StorageError::QuotaExceeded { reason, .. }) => {
            S3Error::quota_exceeded(&reason).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn copy_object(state: &S3State, dest_bucket: &str, dest_key: &str, source: &str) -> Response {
    // source format: /bucket/key or bucket/key
    let source = source.strip_prefix('/').unwrap_or(source);
    let (src_bucket, src_key) = match source.split_once('/') {
        Some(pair) => pair,
        None => return S3Error::invalid_request("Invalid x-amz-copy-source").into_response(),
    };

    let (_, data) = match state.storage.get_object(src_bucket, src_key).await {
        Ok(result) => result,
        Err(_) => return S3Error::no_such_key(src_key).into_response(),
    };

    match state
        .storage
        .put_object(dest_bucket, dest_key, data, None, None)
        .await
    {
        Ok(meta) => {
            let body = xml::copy_object_xml(&meta);
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(vexobj_storage::StorageError::QuotaExceeded { reason, .. }) => {
            S3Error::quota_exceeded(&reason).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn get_object(state: &S3State, bucket: &str, key: &str, headers: &HeaderMap) -> Response {
    let (meta, data) = match state.storage.get_object(bucket, key).await {
        Ok(r) => r,
        Err(_) => return S3Error::no_such_key(key).into_response(),
    };

    // Honor `Range: bytes=start-end` so clients like `aws s3 cp` that
    // download in parallel ranges don't overwrite each chunk with a
    // full-body response. Syntactically we support a single range spec
    // (`bytes=N-`, `bytes=N-M`, `bytes=-N`) — multi-range 206 responses
    // are not used by any S3 SDK in the wild.
    if let Some(range_header) = headers.get("range").and_then(|v| v.to_str().ok()) {
        match parse_single_byte_range(range_header, meta.size) {
            Some((start, end)) => {
                let slice = data.slice(start as usize..(end + 1) as usize);
                let len = slice.len();
                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        ("content-type", meta.content_type),
                        ("content-length", len.to_string()),
                        ("etag", format!("\"{}\"", meta.sha256)),
                        ("last-modified", meta.updated_at.to_rfc2822()),
                        ("accept-ranges", "bytes".to_string()),
                        (
                            "content-range",
                            format!("bytes {start}-{end}/{}", meta.size),
                        ),
                    ],
                    slice,
                )
                    .into_response();
            }
            None => {
                // S3 returns 416 for ranges outside the object size.
                return (
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    [("content-range", format!("bytes */{}", meta.size))],
                )
                    .into_response();
            }
        }
    }

    (
        StatusCode::OK,
        [
            ("content-type", meta.content_type),
            ("content-length", meta.size.to_string()),
            ("etag", format!("\"{}\"", meta.sha256)),
            ("last-modified", meta.updated_at.to_rfc2822()),
            ("accept-ranges", "bytes".to_string()),
        ],
        data,
    )
        .into_response()
}

/// Parse an HTTP `Range: bytes=…` header against a known object size.
/// Returns the inclusive [start, end] pair actually served, or `None`
/// when the range is unsatisfiable or syntactically bad.
fn parse_single_byte_range(header: &str, size: u64) -> Option<(u64, u64)> {
    let spec = header.strip_prefix("bytes=")?;
    // Reject multi-range (`bytes=0-10,20-30`) — single range only.
    if spec.contains(',') {
        return None;
    }
    let (start_s, end_s) = spec.split_once('-')?;
    let start_s = start_s.trim();
    let end_s = end_s.trim();

    let (start, end) = if start_s.is_empty() {
        // Suffix form `bytes=-N` — last N bytes.
        let n: u64 = end_s.parse().ok()?;
        if n == 0 || size == 0 {
            return None;
        }
        let start = size.saturating_sub(n);
        (start, size - 1)
    } else {
        let start: u64 = start_s.parse().ok()?;
        let end: u64 = if end_s.is_empty() {
            size.checked_sub(1)?
        } else {
            end_s.parse().ok()?
        };
        if end >= size || start > end {
            return None;
        }
        (start, end)
    };
    Some((start, end))
}

async fn head_object(state: &S3State, bucket: &str, key: &str) -> Response {
    match state.storage.get_object_meta(bucket, key) {
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
        Err(_) => S3Error::no_such_key(key).into_response(),
    }
}

async fn delete_object(state: &S3State, bucket: &str, key: &str) -> Response {
    match state.storage.delete_object(bucket, key).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => {
            // S3 returns 204 even if object doesn't exist
            StatusCode::NO_CONTENT.into_response()
        }
    }
}

// ─── Multipart Upload ─────────────────────────────────────────────────

async fn initiate_multipart(
    state: &S3State,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    match state
        .storage
        .initiate_multipart(bucket, key, content_type.as_deref())
        .await
    {
        Ok(upload_id) => {
            let body = xml::initiate_multipart_xml(bucket, key, &upload_id);
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(vexobj_storage::StorageError::BucketNotFound(_)) => {
            S3Error::no_such_bucket(bucket).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn upload_part(
    state: &S3State,
    bucket: &str,
    upload_id: &str,
    part_number: u32,
    body: Body,
) -> Response {
    // Verify the upload belongs to this bucket — clients can't poke a
    // different bucket's upload id into the URL and write there.
    match state.storage.get_multipart_upload(upload_id) {
        Ok(Some(u)) if u.bucket == bucket => {}
        Ok(_) => return S3Error::no_such_upload(upload_id).into_response(),
        Err(e) => return S3Error::internal(&e.to_string()).into_response(),
    }

    let stream = body.into_data_stream().map_err(std::io::Error::other);
    match state
        .storage
        .upload_part(upload_id, part_number, stream)
        .await
    {
        Ok(part) => (StatusCode::OK, [("etag", format!("\"{}\"", part.etag))], "").into_response(),
        Err(vexobj_storage::StorageError::ObjectTooLarge { .. }) => {
            S3Error::entity_too_large().into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn complete_multipart(
    state: &S3State,
    bucket: &str,
    upload_id: &str,
    body: Body,
    uri: &axum::http::Uri,
) -> Response {
    match state.storage.get_multipart_upload(upload_id) {
        Ok(Some(u)) if u.bucket == bucket => {}
        Ok(_) => return S3Error::no_such_upload(upload_id).into_response(),
        Err(e) => return S3Error::internal(&e.to_string()).into_response(),
    }

    let body_bytes = match to_bytes(body, S3_COMPLETE_MAX).await {
        Ok(b) => b,
        Err(_) => {
            return S3Error::invalid_request("CompleteMultipartUpload body too large")
                .into_response();
        }
    };
    let body_str = match std::str::from_utf8(&body_bytes) {
        Ok(s) => s,
        Err(_) => return S3Error::malformed_xml().into_response(),
    };
    let Some(claimed_parts) = xml::parse_complete_multipart(body_str) else {
        return S3Error::malformed_xml().into_response();
    };

    match state
        .storage
        .complete_multipart(upload_id, claimed_parts)
        .await
    {
        Ok(meta) => {
            let location = format!("{}/{}/{}", uri.path(), bucket, meta.key);
            let body =
                xml::complete_multipart_xml(&meta.bucket, &meta.key, &meta.sha256, &location);
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(vexobj_storage::StorageError::QuotaExceeded { reason, .. }) => {
            S3Error::quota_exceeded(&reason).into_response()
        }
        Err(vexobj_storage::StorageError::Io(e)) => {
            S3Error::invalid_part(&e.to_string()).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn abort_multipart(state: &S3State, upload_id: &str) -> Response {
    match state.storage.abort_multipart(upload_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn list_parts(state: &S3State, bucket: &str, key: &str, upload_id: &str) -> Response {
    match state.storage.get_multipart_upload(upload_id) {
        Ok(Some(u)) if u.bucket == bucket => {}
        Ok(_) => return S3Error::no_such_upload(upload_id).into_response(),
        Err(e) => return S3Error::internal(&e.to_string()).into_response(),
    }
    match state.storage.list_multipart_parts(upload_id) {
        Ok(parts) => {
            let body = xml::list_parts_xml(bucket, key, upload_id, &parts, 1000);
            (StatusCode::OK, [("content-type", "application/xml")], body).into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}
