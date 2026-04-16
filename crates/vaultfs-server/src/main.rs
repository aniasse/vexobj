mod config;
mod middleware;
mod routes;
mod state;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vaultfs_auth::{BucketAccess, Permissions};

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

    // Bootstrap: create admin key if no keys exist
    if config.auth.enabled {
        match state.auth.list_keys() {
            Ok(keys) if keys.is_empty() => {
                let perms = Permissions {
                    read: true,
                    write: true,
                    delete: true,
                    admin: true,
                };
                match state.auth.create_key("admin", perms, BucketAccess::All) {
                    Ok((key, secret)) => {
                        info!("==========================================================");
                        info!("  No API keys found. Created initial admin key.");
                        info!("  Name:   {}", key.name);
                        info!("  Key:    {}", secret);
                        info!("  SAVE THIS KEY — it will not be shown again.");
                        info!("==========================================================");
                    }
                    Err(e) => tracing::warn!("failed to create bootstrap key: {}", e),
                }
            }
            _ => {}
        }
    }

    let app = routes::create_router(state);

    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    info!("VaultFS listening on {}", config.server.bind);

    axum::serve(listener, app).await?;
    Ok(())
}
