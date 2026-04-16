mod config;
mod routes;
mod state;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = config::Config::load()?;
    info!(
        bind = %config.server.bind,
        data_dir = %config.storage.data_dir,
        "starting VaultFS"
    );

    let state = state::AppState::new(&config)?;
    let app = routes::create_router(state);

    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    info!("VaultFS listening on {}", config.server.bind);

    axum::serve(listener, app).await?;
    Ok(())
}
