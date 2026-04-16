mod buckets;
mod health;
mod objects;

use axum::Router;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .merge(health::routes())
        .merge(buckets::routes())
        .merge(objects::routes())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
