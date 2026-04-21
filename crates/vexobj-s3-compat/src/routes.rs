use std::sync::Arc;

use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use bytes::Bytes;
use serde::Deserialize;

use crate::error::S3Error;
use crate::signature::{parse_auth_header, verify_sigv4};
use crate::xml;
use vexobj_auth::AuthManager;
use vexobj_storage::StorageEngine;

#[derive(Clone)]
pub struct S3State {
    pub storage: Arc<StorageEngine>,
    pub auth: Arc<AuthManager>,
}

/// Create the S3-compatible router.
/// Mount this at the root alongside the VaultFS native API.
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
            .filter_map(|(n, v)| v.to_str().ok().map(|s| (n.as_str().to_string(), s.to_string())))
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
            let body = xml::list_buckets_xml(&buckets, "vaultfs");
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                body,
            )
                .into_response()
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
) -> Response {
    if let Err(e) = authenticate(
        &state,
        method.as_str(),
        uri.path(),
        uri.query().unwrap_or(""),
        &headers,
    ) {
        return e.into_response();
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
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                body,
            )
                .into_response()
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
    body: Bytes,
) -> Response {
    if let Err(e) = authenticate(
        &state,
        method.as_str(),
        uri.path(),
        uri.query().unwrap_or(""),
        &headers,
    ) {
        return e.into_response();
    }

    match method {
        Method::PUT => put_object(&state, &bucket, &key, headers, body).await,
        Method::GET => get_object(&state, &bucket, &key).await,
        Method::HEAD => head_object(&state, &bucket, &key).await,
        Method::DELETE => delete_object(&state, &bucket, &key).await,
        _ => S3Error::invalid_request("Method not allowed").into_response(),
    }
}

async fn put_object(
    state: &S3State,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for copy operation
    if let Some(copy_source) = headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok()) {
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

    match state.storage.put_object(dest_bucket, dest_key, data, None, None).await {
        Ok(meta) => {
            let body = xml::copy_object_xml(&meta);
            (
                StatusCode::OK,
                [("content-type", "application/xml")],
                body,
            )
                .into_response()
        }
        Err(e) => S3Error::internal(&e.to_string()).into_response(),
    }
}

async fn get_object(state: &S3State, bucket: &str, key: &str) -> Response {
    match state.storage.get_object(bucket, key).await {
        Ok((meta, data)) => (
            StatusCode::OK,
            [
                ("content-type", meta.content_type),
                ("content-length", meta.size.to_string()),
                ("etag", format!("\"{}\"", meta.sha256)),
                ("last-modified", meta.updated_at.to_rfc2822()),
            ],
            data,
        )
            .into_response(),
        Err(_) => S3Error::no_such_key(key).into_response(),
    }
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
