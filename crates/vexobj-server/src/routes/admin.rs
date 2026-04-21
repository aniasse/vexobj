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
use vexobj_auth::{ApiKey, BucketAccess, Permissions, PresignRequest};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/admin/keys", get(list_keys).post(create_key))
        .route("/v1/admin/keys/{id}", axum::routing::delete(delete_key))
        .route("/v1/presign", axum::routing::post(create_presigned_url))
        .route("/v1/admin/gc", axum::routing::post(run_gc))
        .route("/v1/admin/backup", axum::routing::post(create_backup))
        .route("/v1/admin/backup/export/{bucket}", axum::routing::post(export_bucket))
        .route("/v1/admin/versioning/{bucket}", axum::routing::post(enable_versioning))
        .route(
            "/v1/admin/lifecycle/{bucket}",
            axum::routing::post(create_lifecycle_rule).get(list_lifecycle_rules),
        )
        .route("/v1/admin/lifecycle/run", axum::routing::post(run_lifecycle))
        .route("/v1/admin/lifecycle/rule/{id}", axum::routing::delete(delete_lifecycle_rule))
        .route("/v1/admin/migrate/s3", axum::routing::post(migrate_s3_stub))
        .route(
            "/v1/admin/lock/{bucket}/{*key}",
            get(get_object_lock)
                .put(set_object_lock)
                .delete(release_legal_hold),
        )
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
    headers: HeaderMap,
    Json(body): Json<CreateKeyBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);
    let permissions = body.permissions.unwrap_or_default();
    let bucket_access = body.bucket_access.unwrap_or_default();

    match state.auth.create_key(&body.name, permissions, bucket_access) {
        Ok((key, raw_key)) => {
            state.audit.log(
                &key_prefix(&caller),
                "key.create",
                &body.name,
                &json!({"key_id": key.id}),
                &ip,
            );
            (
                StatusCode::CREATED,
                Json(json!({
                    "key": key,
                    "secret": raw_key,
                    "warning": "Store this secret securely. It cannot be retrieved again."
                })),
            )
                .into_response()
        }
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
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state.auth.delete_key(&id) {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "key.delete",
                &id,
                &json!({}),
                &ip,
            );
            StatusCode::NO_CONTENT.into_response()
        }
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
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    let gc = vexobj_storage::GarbageCollector::new(state.storage.data_dir().to_path_buf());
    match gc.collect(state.storage.db()) {
        Ok(result) => {
            state.audit.log(
                &key_prefix(&caller),
                "gc.run",
                "gc",
                &json!({
                    "blobs_scanned": result.blobs_scanned,
                    "orphans_removed": result.orphans_removed,
                    "bytes_freed": result.bytes_freed,
                }),
                &ip,
            );
            (
                StatusCode::OK,
                Json(json!({
                    "blobs_scanned": result.blobs_scanned,
                    "orphans_removed": result.orphans_removed,
                    "bytes_freed": result.bytes_freed,
                })),
            )
                .into_response()
        }
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
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    let data_dir = state.storage.data_dir().to_path_buf();
    let backup_dir = data_dir.join(format!(
        "backups/snapshot-{}",
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    ));

    let bm = vexobj_storage::BackupManager::new(data_dir);
    match bm.create_snapshot(state.storage.db(), &backup_dir) {
        Ok(result) => {
            state.audit.log(
                &key_prefix(&caller),
                "backup.create",
                &result.path.to_string_lossy(),
                &json!({
                    "db_size": result.db_size,
                    "blobs_copied": result.blobs_copied,
                    "total_size": result.total_size,
                }),
                &ip,
            );
            (
                StatusCode::OK,
                Json(json!({
                    "path": result.path.to_string_lossy(),
                    "db_size": result.db_size,
                    "blobs_copied": result.blobs_copied,
                    "total_size": result.total_size,
                })),
            )
                .into_response()
        }
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

    let bm = vexobj_storage::BackupManager::new(data_dir);
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

// ── Versioning ──────────────────────────────────────────────────────────

async fn enable_versioning(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state.storage.enable_versioning(&bucket) {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({"bucket": bucket, "versioning": "enabled"})),
        )
            .into_response(),
        Err(e) => {
            let status = match &e {
                vexobj_storage::StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

// ── Lifecycle ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateLifecycleBody {
    #[serde(default)]
    prefix: String,
    expire_days: u64,
}

async fn create_lifecycle_rule(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
    Json(body): Json<CreateLifecycleBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state
        .storage
        .db()
        .create_lifecycle_rule(&bucket, &body.prefix, body.expire_days)
    {
        Ok(rule) => (StatusCode::CREATED, Json(json!(rule))).into_response(),
        Err(e) => {
            let status = match &e {
                vexobj_storage::StorageError::BucketNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn list_lifecycle_rules(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state.storage.db().list_lifecycle_rules(&bucket) {
        Ok(rules) => Json(json!({"rules": rules})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_lifecycle_rule(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    match state.storage.db().delete_lifecycle_rule(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "lifecycle rule not found"})),
        )
            .into_response(),
    }
}

async fn run_lifecycle(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state.storage.run_lifecycle().await {
        Ok(result) => {
            state.audit.log(
                &key_prefix(&caller),
                "lifecycle.run",
                "lifecycle",
                &json!({
                    "objects_expired": result.objects_expired,
                    "bytes_freed": result.bytes_freed,
                }),
                &ip,
            );
            (
                StatusCode::OK,
                Json(json!({
                    "objects_expired": result.objects_expired,
                    "bytes_freed": result.bytes_freed,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ── Object lock ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SetLockBody {
    /// ISO-8601 timestamp (UTC). Omit or set null to leave retention unset.
    #[serde(default)]
    retain_until: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    legal_hold: bool,
}

async fn get_object_lock(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }
    match state.storage.get_lock(&bucket, &key) {
        Ok(lock) => (StatusCode::OK, Json(json!(lock))).into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "object not found"})),
        )
            .into_response(),
    }
}

async fn set_object_lock(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<SetLockBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state
        .storage
        .set_lock(&bucket, &key, body.retain_until, body.legal_hold)
    {
        Ok(lock) => {
            state.audit.log(
                &key_prefix(&caller),
                "object.lock.set",
                &format!("{}/{}", bucket, key),
                &json!(lock),
                &ip,
            );
            (StatusCode::OK, Json(json!(lock))).into_response()
        }
        Err(e) => {
            let status = match &e {
                vexobj_storage::StorageError::ObjectNotFound { .. } => StatusCode::NOT_FOUND,
                vexobj_storage::StorageError::ObjectLocked { .. } => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn release_legal_hold(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let ip = extract_ip(&headers);

    match state.storage.clear_legal_hold(&bucket, &key) {
        Ok(()) => {
            state.audit.log(
                &key_prefix(&caller),
                "object.lock.legal_hold.release",
                &format!("{}/{}", bucket, key),
                &json!({}),
                &ip,
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "object not found"})),
        )
            .into_response(),
    }
}

// ── S3 Migration Stub ─────────────────────────────────────────────────

async fn migrate_s3_stub(
    Extension(caller): Extension<ApiKey>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "server-side S3 migration is not implemented",
            "hint": "Use the CLI tool instead:",
            "command": "vexobjctl migrate s3 --source-endpoint <ENDPOINT> --source-bucket <BUCKET> --source-access-key <KEY> --source-secret-key <SECRET> --dest-bucket <DEST>"
        })),
    )
        .into_response()
}
