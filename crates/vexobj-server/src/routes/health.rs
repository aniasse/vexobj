use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        // Process-up check: used by Kubernetes livenessProbe. Cheap — it
        // says nothing about whether traffic can actually be served, just
        // that the HTTP loop is alive. If this fails we *want* the pod
        // killed; anything more complex risks a death loop during a brief
        // DB hiccup.
        .route("/livez", get(livez))
        // Ready-for-traffic check: used by Kubernetes readinessProbe. Does
        // a cheap DB read to confirm the metadata store is reachable. If
        // this fails K8s stops sending traffic but doesn't kill the pod,
        // which gives the storage layer time to recover.
        .route("/readyz", get(readyz))
        // Existing external monitoring / dashboard / SDK health banner.
        // Same contract as /readyz so external health checkers need no
        // migration, but adds capability metadata for feature discovery.
        .route("/health", get(health_check))
        .route("/openapi.yaml", get(openapi_spec))
        .route("/docs", get(swagger_ui))
}

async fn livez() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    match state.storage.list_buckets() {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ok" }))).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "reason": e.to_string() })),
        )
            .into_response(),
    }
}

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    // Capabilities let clients avoid asking for features the host
    // can't provide (thumbnails without ffmpeg, etc.).
    let vf = state.storage.video_features();
    let body = json!({
        "status": "ok",
        "service": "vexobj",
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": {
            "sse_at_rest":        state.storage.encryption_enabled(),
            "video_metadata":     true,
            "video_thumbnails":   vf.ffmpeg,
            "ffprobe":            vf.ffprobe,
            "ffmpeg":             vf.ffmpeg,
        }
    });
    // Fail /health the same way /readyz does so anything still pointed at
    // the old endpoint sees the right status when metadata goes down.
    match state.storage.list_buckets() {
        Ok(_) => (StatusCode::OK, Json(body)).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable", "reason": e.to_string() })),
        )
            .into_response(),
    }
}

async fn openapi_spec() -> impl IntoResponse {
    (
        [("content-type", "text/yaml")],
        include_str!("../../../../openapi.yaml"),
    )
}

async fn swagger_ui() -> Html<&'static str> {
    Html(
        r#"<!DOCTYPE html>
<html>
<head>
<title>vexobj API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui', deepLinking: true });
</script>
</body>
</html>"#,
    )
}
