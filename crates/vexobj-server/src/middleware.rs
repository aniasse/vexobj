use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use serde_json::json;

use crate::state::AppState;
use vexobj_auth::{ApiKey, BucketAccess, Permissions};

const AUTH_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

/// Look at the request and decide whether an anonymous client should be
/// allowed through. The rule is deliberately narrow: GET / HEAD of an
/// object inside a bucket whose `public` flag is on. Anything that
/// mutates state, lists buckets, or touches /v1/admin/* still needs a
/// real key.
///
/// Returns the bucket name if we decided to waive auth — the caller uses
/// that to scope the synthetic `ApiKey` that gets injected downstream.
fn public_bucket_read_target(method: &str, path: &str, state: &AppState) -> Option<String> {
    if !matches!(method, "GET" | "HEAD") {
        return None;
    }
    // Only object paths are eligible — not `/v1/objects/{bucket}` (which
    // *lists* and leaks metadata) and not any admin/versioning routes.
    let rest = path
        .strip_prefix("/v1/objects/")
        .or_else(|| path.strip_prefix("/v1/stream/"))?;
    let (bucket, key) = rest.split_once('/')?;
    if key.is_empty() {
        return None;
    }
    match state.storage.get_bucket(bucket) {
        Ok(b) if b.public => Some(b.name),
        _ => None,
    }
}

/// Build the synthetic `ApiKey` that stands in for an anonymous request
/// that passed the `public_bucket_read_target` check. Read-only, scoped
/// to the one bucket, so downstream permission checks still work.
fn anonymous_key_for(bucket: &str) -> ApiKey {
    ApiKey {
        id: "anonymous".to_string(),
        name: "public-bucket".to_string(),
        key_prefix: "anon".to_string(),
        created_at: Utc::now(),
        permissions: Permissions {
            read: true,
            write: false,
            delete: false,
            admin: false,
        },
        bucket_access: BucketAccess::Specific {
            buckets: vec![bucket.to_string()],
        },
    }
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    if !state.config.auth.enabled {
        return next.run(req).await;
    }

    // Check for presigned URL signature (skip auth for valid presigned requests)
    let uri = req.uri().clone();
    if let Some(query) = uri.query() {
        if query.contains("signature=") && query.contains("expires=") {
            // Presigned URL — validated in the handler
            return next.run(req).await;
        }
    }

    // Anonymous read of a public bucket: inject a synthetic key and let
    // the handler proceed. Without this, Mastodon / Peertube can't serve
    // media directly from VexObj to unauthenticated browsers.
    if let Some(bucket) = public_bucket_read_target(req.method().as_str(), uri.path(), &state) {
        req.extensions_mut().insert(anonymous_key_for(&bucket));
        return next.run(req).await;
    }

    let auth_header = req.headers().get(AUTH_HEADER).and_then(|v| v.to_str().ok());

    let raw_key = match auth_header {
        Some(h) if h.starts_with(BEARER_PREFIX) => &h[BEARER_PREFIX.len()..],
        Some(h) => h,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                axum::Json(json!({"error": "missing authorization header"})),
            )
                .into_response();
        }
    };

    match state.auth.verify_key(raw_key) {
        Ok(api_key) => {
            req.extensions_mut().insert(api_key);
            next.run(req).await
        }
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            axum::Json(json!({"error": "invalid api key"})),
        )
            .into_response(),
    }
}

/// Extract the authenticated API key from request extensions
pub async fn require_permission(key: &ApiKey, permission: &str) -> Result<(), Response> {
    let allowed = match permission {
        "read" => key.permissions.read,
        "write" => key.permissions.write,
        "delete" => key.permissions.delete,
        "admin" => key.permissions.admin,
        _ => false,
    };

    if allowed {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            axum::Json(json!({"error": format!("missing '{}' permission", permission)})),
        )
            .into_response())
    }
}
