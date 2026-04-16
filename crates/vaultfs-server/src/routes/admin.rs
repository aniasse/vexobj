use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::middleware::require_permission;
use crate::state::AppState;
use vaultfs_auth::{ApiKey, BucketAccess, Permissions, PresignRequest};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/admin/keys", get(list_keys).post(create_key))
        .route("/v1/admin/keys/{id}", axum::routing::delete(delete_key))
        .route("/v1/presign", axum::routing::post(create_presigned_url))
}

#[derive(Deserialize)]
struct CreateKeyBody {
    name: String,
    #[serde(default)]
    permissions: Option<Permissions>,
    #[serde(default)]
    bucket_access: Option<BucketAccess>,
}

async fn create_key(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Json(body): Json<CreateKeyBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let permissions = body.permissions.unwrap_or_default();
    let bucket_access = body.bucket_access.unwrap_or_default();

    match state.auth.create_key(&body.name, permissions, bucket_access) {
        Ok((key, raw_key)) => (
            StatusCode::CREATED,
            Json(json!({
                "key": key,
                "secret": raw_key,
                "warning": "Store this secret securely. It cannot be retrieved again."
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_keys(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state.auth.list_keys() {
        Ok(keys) => Json(json!({"keys": keys})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_key(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state.auth.delete_key(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "key not found"})),
        )
            .into_response(),
    }
}

async fn create_presigned_url(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Json(body): Json<PresignRequest>,
) -> impl IntoResponse {
    // Verify the caller has the right permission for the requested method
    let perm = match body.method.to_uppercase().as_str() {
        "GET" | "HEAD" => "read",
        "PUT" => "write",
        "DELETE" => "delete",
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid method, use GET, PUT, HEAD, or DELETE"})),
            )
                .into_response()
        }
    };

    if let Err(resp) = require_permission(&caller, perm).await {
        return resp;
    }

    // Check bucket access
    if let Err(e) = state.auth.check_bucket_access(&caller, &body.bucket) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }

    let base_url = format!("http://{}", state.config.server.bind);
    let presigned = state.presigner.generate(&base_url, &body);

    (StatusCode::OK, Json(json!(presigned))).into_response()
}
