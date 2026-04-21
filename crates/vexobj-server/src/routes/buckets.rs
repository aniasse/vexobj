use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{extract_ip, key_prefix};
use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;
use vexobj_storage::CreateBucketRequest;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/buckets", get(list_buckets).post(create_bucket))
        .route(
            "/v1/buckets/{name}",
            get(get_bucket).delete(delete_bucket),
        )
}

#[derive(Deserialize)]
struct CreateBucketBody {
    name: String,
    #[serde(default)]
    public: bool,
}

async fn create_bucket(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    headers: HeaderMap,
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state.storage.create_bucket(&CreateBucketRequest {
        name: body.name.clone(),
        public: body.public,
    }) {
        Ok(bucket) => {
            state.audit.log(
                &key_prefix(&caller),
                "bucket.create",
                &body.name,
                &json!({"public": body.public}),
                &ip,
            );
            (StatusCode::CREATED, Json(json!(bucket))).into_response()
        }
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_buckets(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }

    match state.storage.list_buckets() {
        Ok(buckets) => Json(json!({"buckets": buckets})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn get_bucket(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }

    match state.storage.get_bucket(&name) {
        Ok(bucket) => Json(json!(bucket)).into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "bucket not found"})),
        )
            .into_response(),
    }
}

async fn delete_bucket(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state.storage.delete_bucket(&name) {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "bucket.delete",
                &name,
                &json!({}),
                &ip,
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
