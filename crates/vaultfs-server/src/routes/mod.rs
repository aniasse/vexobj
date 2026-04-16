mod admin;
mod buckets;
mod health;
mod multipart;
mod objects;

use axum::middleware as axum_mw;
use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::middleware::auth_middleware;
use crate::state::AppState;

pub fn create_router(state: AppState) -> Router {
    // Public routes (no auth)
    let public = Router::new().merge(health::routes());

    // Protected routes (auth required)
    let protected = Router::new()
        .merge(buckets::routes())
        .merge(objects::routes())
        .merge(multipart::routes())
        .merge(admin::routes())
        .route_layer(axum_mw::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .merge(public)
        .merge(protected)
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
