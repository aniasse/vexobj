mod audit;
mod config;
mod cors;
mod metrics;
mod middleware;
mod ratelimit;
mod routes;
mod security;
mod state;
mod transcode_worker;
mod webhooks;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vexobj_auth::{BucketAccess, Permissions};

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
        "starting vexobj"
    );

    if config.tls.enabled {
        info!("TLS enabled");
    }
    if config.rate_limit.enabled {
        info!(
            max = config.rate_limit.max_requests,
            window = config.rate_limit.window_secs,
            "rate limiting enabled"
        );
    }
    if !config.webhooks.is_empty() {
        info!(count = config.webhooks.len(), "webhooks configured");
    }

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

    // Lifecycle expiration background task (runs every hour)
    let storage_for_lifecycle = state.storage.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            if let Ok(result) = storage_for_lifecycle.run_lifecycle().await {
                if result.objects_expired > 0 {
                    tracing::info!(
                        expired = result.objects_expired,
                        bytes = result.bytes_freed,
                        "lifecycle cleanup"
                    );
                }
            }
        }
    });

    // Transcode worker — only spins up when ffmpeg is detected on PATH.
    transcode_worker::spawn(
        state.storage.clone(),
        transcode_worker::TranscodeWorkerConfig {
            workers: config.transcode.workers,
            gc_after_days: config.transcode.gc_after_days,
            ..Default::default()
        },
    );

    // Rate limiter cleanup task
    if let Some(ref limiter) = state.rate_limiter {
        let limiter = limiter.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                limiter.cleanup();
            }
        });
    }

    let app = routes::create_router(state);

    if config.tls.enabled {
        let cert_path = config
            .tls
            .cert_path
            .as_deref()
            .expect("tls.cert_path required when TLS is enabled");
        let key_path = config
            .tls
            .key_path
            .as_deref()
            .expect("tls.key_path required when TLS is enabled");

        info!("vexobj listening on {} (HTTPS)", config.server.bind);

        let tls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await?;

        axum_server::bind_rustls(config.server.bind.parse()?, tls_config)
            .serve(app.into_make_service())
            .await?;
    } else {
        let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
        info!("vexobj listening on {}", config.server.bind);
        axum::serve(listener, app).await?;
    }

    Ok(())
}
