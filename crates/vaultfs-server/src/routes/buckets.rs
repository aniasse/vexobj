use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;
use vaultfs_storage::CreateBucketRequest;

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
    Json(body): Json<CreateBucketBody>,
) -> impl IntoResponse {
    match state.storage.create_bucket(&CreateBucketRequest {
        name: body.name,
        public: body.public,
    }) {
        Ok(bucket) => (StatusCode::CREATED, Json(json!(bucket))).into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_buckets(State(state): State<AppState>) -> impl IntoResponse {
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
    Path(name): Path<String>,
) -> impl IntoResponse {
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
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.storage.delete_bucket(&name) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
