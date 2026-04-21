use axum::extract::{Extension, Multipart, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use bytes::BytesMut;
use serde_json::json;

use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/upload/{bucket}", post(multipart_upload))
        .route(
            "/v1/upload/{bucket}/{*prefix}",
            post(multipart_upload_prefix),
        )
}

async fn multipart_upload(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path(bucket): Path<String>,
    multipart: Multipart,
) -> Response {
    if let Err(resp) = require_permission(&caller, "write").await {
        return resp;
    }
    handle_multipart(state, &bucket, "", multipart).await
}

async fn multipart_upload_prefix(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Path((bucket, prefix)): Path<(String, String)>,
    multipart: Multipart,
) -> Response {
    if let Err(resp) = require_permission(&caller, "write").await {
        return resp;
    }
    handle_multipart(state, &bucket, &prefix, multipart).await
}

async fn handle_multipart(
    state: AppState,
    bucket: &str,
    prefix: &str,
    mut multipart: Multipart,
) -> Response {
    let mut uploaded = Vec::new();
    let mut errors = Vec::new();

    while let Ok(Some(field)) = multipart.next_field().await {
        let file_name = match field.file_name() {
            Some(name) => name.to_string(),
            None => {
                errors.push(json!({"error": "field missing filename"}));
                continue;
            }
        };

        let content_type = field.content_type().map(|s| s.to_string());

        let mut data = BytesMut::new();
        let mut field = field;
        while let Ok(Some(chunk)) = field.chunk().await {
            data.extend_from_slice(&chunk);
        }

        let key = if prefix.is_empty() {
            file_name.clone()
        } else {
            let p = prefix.trim_end_matches('/');
            format!("{}/{}", p, file_name)
        };

        match state
            .storage
            .put_object(bucket, &key, data.freeze(), content_type.as_deref(), None)
            .await
        {
            Ok(meta) => uploaded.push(json!(meta)),
            Err(e) => errors.push(json!({
                "file": file_name,
                "error": e.to_string(),
            })),
        }
    }

    let status = if errors.is_empty() {
        StatusCode::CREATED
    } else if uploaded.is_empty() {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::MULTI_STATUS
    };

    (
        status,
        Json(json!({
            "uploaded": uploaded,
            "errors": errors,
        })),
    )
        .into_response()
}
