mod admin;
mod buckets;
mod dashboard;
mod health;
mod multipart;
mod objects;
mod stats;
mod stream;

use axum::middleware as axum_mw;
use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::middleware::auth_middleware;
use crate::ratelimit::rate_limit_middleware;
use crate::security::security_middleware;
use crate::state::AppState;

pub fn create_router(state: AppState) -> Router {
    // Public routes (no auth)
    let public = Router::new()
        .merge(health::routes())
        .merge(dashboard::routes());

    // Protected routes (auth required)
    let protected = Router::new()
        .merge(buckets::routes())
        .merge(objects::routes())
        .merge(multipart::routes())
        .merge(admin::routes())
        .merge(stats::routes())
        .merge(stream::routes())
        .route_layer(axum_mw::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // S3-compatible API (has its own state and auth)
    let s3 = vaultfs_s3_compat::s3_router(state.storage.clone(), state.auth.clone());

    let mut app = Router::new()
        .merge(public)
        .merge(protected)
        .with_state(state.clone())
        .merge(s3);

    // Rate limiting layer (wraps everything)
    if state.rate_limiter.is_some() {
        app = app.layer(axum_mw::from_fn_with_state(state, rate_limit_middleware));
    }

    app.layer(axum::middleware::from_fn(security_middleware))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}
