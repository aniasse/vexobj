use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{extract_ip, key_prefix};
use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;
use vexobj_storage::{CorsRule, CreateBucketRequest};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/buckets", get(list_buckets).post(create_bucket))
        .route("/v1/buckets/{name}", get(get_bucket).delete(delete_bucket))
        .route(
            "/v1/buckets/{name}/cors",
            put(put_bucket_cors)
                .get(get_bucket_cors)
                .delete(delete_bucket_cors),
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
        Err(e) => (StatusCode::CONFLICT, Json(json!({"error": e.to_string()}))).into_response(),
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

// ── CORS config ─────────────────────────────────────────────────────

#[derive(Deserialize)]
struct PutCorsBody {
    rules: Vec<CorsRule>,
}

/// PUT /v1/buckets/{name}/cors — replace the whole rule set. We don't expose
/// per-rule patch semantics on purpose: admins edit the config in one shot,
/// same as every other per-bucket config field.
async fn put_bucket_cors(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<PutCorsBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }
    let ip = extract_ip(&headers);

    match state.storage.set_bucket_cors(&name, &body.rules) {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "bucket.cors.set",
                &name,
                &json!({ "rules_count": body.rules.len() }),
                &ip,
            );
            (StatusCode::OK, Json(json!({ "rules": body.rules }))).into_response()
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_bucket_cors(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }
    // Existence check: silent empty on unknown bucket is confusing.
    if state.storage.get_bucket(&name).is_err() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "bucket not found" })),
        )
            .into_response();
    }
    let rules = state.storage.get_bucket_cors(&name);
    Json(json!({ "rules": rules })).into_response()
}

async fn delete_bucket_cors(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }
    let ip = extract_ip(&headers);

    match state.storage.set_bucket_cors(&name, &[]) {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "bucket.cors.clear",
                &name,
                &json!({}),
                &ip,
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
