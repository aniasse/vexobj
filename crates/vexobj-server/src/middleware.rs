use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::state::AppState;
use vexobj_auth::ApiKey;

const AUTH_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

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
