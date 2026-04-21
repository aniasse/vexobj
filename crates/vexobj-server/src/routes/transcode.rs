//! Transcoding HTTP API. Submissions create rows in `transcode_jobs`;
//! the background worker (see `crate::transcode_worker`) picks them up.

use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::audit::{extract_ip, key_prefix};
use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/transcode/profiles", get(list_profiles))
        .route("/v1/transcode/jobs", get(list_jobs))
        .route("/v1/transcode/jobs/{id}", get(get_job))
        .route("/v1/transcode/{bucket}/{*key}", post(submit_job))
}

#[derive(Deserialize)]
struct SubmitBody {
    profile: String,
}

async fn submit_job(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<SubmitBody>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "write").await {
        return resp;
    }
    if let Err(e) = state.auth.check_bucket_access(&caller, &bucket) {
        return (StatusCode::FORBIDDEN, Json(json!({"error": e.to_string()}))).into_response();
    }

    // Fail fast if the requested profile doesn't exist — otherwise the
    // worker would just mark the job failed and the caller would poll
    // for nothing.
    if vexobj_processing::transcode_profile(&body.profile).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("unknown profile '{}'", body.profile),
                "available": vexobj_processing::TRANSCODE_PROFILES
                    .iter().map(|p| p.name).collect::<Vec<_>>(),
            })),
        )
            .into_response();
    }

    // And refuse when ffmpeg isn't available — the worker wouldn't
    // even start, and clients deserve a direct 501 rather than a
    // forever-pending job.
    if !state.storage.video_features().ffmpeg {
        return (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "error": "transcoding requires ffmpeg on the server's PATH",
            })),
        )
            .into_response();
    }

    // Backpressure: reject submissions once the pending queue is full.
    // Without this, a misbehaved client could queue thousands of jobs
    // and force the worker pool into permanent overload. 429 is the
    // right signal — clients are expected to back off and retry.
    let cap = state.config.transcode.max_pending;
    if cap > 0 {
        match state.storage.db().count_transcode_jobs_by_status("pending") {
            Ok(pending) if pending >= cap as u64 => {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [("retry-after", "30")],
                    Json(json!({
                        "error": "transcode queue is full",
                        "pending": pending,
                        "max_pending": cap,
                    })),
                )
                    .into_response();
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("pending-count query failed: {e}"),
        }
    }

    // Resolve the source so we can capture its sha256 on the job row
    // (survives future edits to the source key).
    let meta = match state.storage.get_object_meta(&bucket, &key) {
        Ok(m) => m,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "source object not found"})),
            )
                .into_response();
        }
    };
    if !meta.content_type.starts_with("video/") {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "transcoding requested on a non-video object",
                "content_type": meta.content_type,
            })),
        )
            .into_response();
    }

    let ip = extract_ip(&headers);
    let caller_prefix = key_prefix(&caller);

    match state.storage.db().create_transcode_job(
        &bucket,
        &key,
        &meta.sha256,
        &body.profile,
        Some(&caller_prefix),
    ) {
        Ok(job) => {
            state.audit.log(
                &caller_prefix,
                "transcode.submit",
                &format!("{}/{}", bucket, key),
                &json!({"profile": body.profile, "job_id": job.id}),
                &ip,
            );
            (StatusCode::ACCEPTED, Json(json!(job))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn get_job(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }
    match state.storage.db().get_transcode_job(&id) {
        Ok(job) => (StatusCode::OK, Json(json!(job))).into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "job not found"})),
        )
            .into_response(),
    }
}

#[derive(Deserialize, Default)]
struct ListQuery {
    status: Option<String>,
    limit: Option<u32>,
}

async fn list_jobs(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "read").await {
        return resp;
    }
    let limit = q.limit.unwrap_or(50);
    match state
        .storage
        .db()
        .list_transcode_jobs(q.status.as_deref(), limit)
    {
        Ok(jobs) => Json(json!({"jobs": jobs})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn list_profiles() -> impl IntoResponse {
    let items: Vec<_> = vexobj_processing::TRANSCODE_PROFILES
        .iter()
        .map(|p| {
            json!({
                "name": p.name,
                "description": p.description,
                "extension": p.extension,
                "content_type": p.content_type,
                "timeout_secs": p.timeout_secs,
            })
        })
        .collect();
    Json(json!({"profiles": items}))
}
