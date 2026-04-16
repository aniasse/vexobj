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
        .route("/v1/admin/gc", axum::routing::post(run_gc))
        .route("/v1/admin/backup", axum::routing::post(create_backup))
        .route("/v1/admin/backup/export/{bucket}", axum::routing::post(export_bucket))
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

async fn run_gc(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let gc = vaultfs_storage::GarbageCollector::new(state.storage.data_dir().to_path_buf());
    match gc.collect(state.storage.db()) {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({
                "blobs_scanned": result.blobs_scanned,
                "orphans_removed": result.orphans_removed,
                "bytes_freed": result.bytes_freed,
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

async fn create_backup(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let data_dir = state.storage.data_dir().to_path_buf();
    let backup_dir = data_dir.join(format!(
        "backups/snapshot-{}",
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    ));

    let bm = vaultfs_storage::BackupManager::new(data_dir);
    match bm.create_snapshot(state.storage.db(), &backup_dir) {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({
                "path": result.path.to_string_lossy(),
                "db_size": result.db_size,
                "blobs_copied": result.blobs_copied,
                "total_size": result.total_size,
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

async fn export_bucket(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let data_dir = state.storage.data_dir().to_path_buf();
    let export_dir = data_dir.join(format!(
        "exports/{}-{}",
        bucket,
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    ));

    let bm = vaultfs_storage::BackupManager::new(data_dir);
    match bm.export_bucket(state.storage.db(), &bucket, &export_dir) {
        Ok(count) => (
            StatusCode::OK,
            Json(json!({
                "bucket": bucket,
                "objects_exported": count,
                "path": export_dir.to_string_lossy(),
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
