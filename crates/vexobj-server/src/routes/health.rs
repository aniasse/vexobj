use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::state::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health_check))
        .route("/openapi.yaml", get(openapi_spec))
        .route("/docs", get(swagger_ui))
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    // Capabilities let clients avoid asking for features the host
    // can't provide (thumbnails without ffmpeg, etc.).
    let vf = state.storage.video_features();
    Json(json!({
        "status": "ok",
        "service": "vaultfs",
        "version": env!("CARGO_PKG_VERSION"),
        "capabilities": {
            "sse_at_rest":        state.storage.encryption_enabled(),
            "video_metadata":     true,
            "video_thumbnails":   vf.ffmpeg,
            "ffprobe":            vf.ffprobe,
            "ffmpeg":             vf.ffmpeg,
        }
    }))
}

async fn openapi_spec() -> impl IntoResponse {
    (
        [("content-type", "text/yaml")],
        include_str!("../../../../openapi.yaml"),
    )
}

async fn swagger_ui() -> Html<&'static str> {
    Html(r#"<!DOCTYPE html>
<html>
<head>
<title>VaultFS API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui', deepLinking: true });
</script>
</body>
</html>"#)
}
