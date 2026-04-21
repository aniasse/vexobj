use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    pub events: Vec<String>,
    #[serde(default)]
    pub secret: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebhookEvent {
    pub event: String,
    pub timestamp: String,
    pub data: serde_json::Value,
}

pub struct WebhookSender {
    tx: mpsc::UnboundedSender<WebhookEvent>,
}

impl WebhookSender {
    pub fn new(configs: Vec<WebhookConfig>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        if !configs.is_empty() {
            tokio::spawn(webhook_worker(rx, configs));
        }

        Self { tx }
    }

    pub fn send(&self, event: &str, data: serde_json::Value) {
        let evt = WebhookEvent {
            event: event.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            data,
        };
        let _ = self.tx.send(evt);
    }
}

async fn webhook_worker(
    mut rx: mpsc::UnboundedReceiver<WebhookEvent>,
    configs: Vec<WebhookConfig>,
) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    info!(count = configs.len(), "webhook worker started");

    while let Some(event) = rx.recv().await {
        for config in &configs {
            if config.events.contains(&"*".to_string())
                || config.events.contains(&event.event)
            {
                let body = serde_json::to_string(&event).unwrap_or_default();
                let mut req = client
                    .post(&config.url)
                    .header("content-type", "application/json")
                    .header("x-vexobj-event", &event.event);

                if let Some(ref secret) = config.secret {
                    use hmac::{Hmac, Mac};
                    use sha2::Sha256;
                    type HmacSha256 = Hmac<Sha256>;

                    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
                    mac.update(body.as_bytes());
                    let sig = hex::encode(mac.finalize().into_bytes());
                    req = req.header("x-vexobj-signature", sig);
                }

                match req.body(body).send().await {
                    Ok(resp) => debug!(
                        url = %config.url,
                        event = %event.event,
                        status = %resp.status(),
                        "webhook delivered"
                    ),
                    Err(e) => error!(
                        url = %config.url,
                        event = %event.event,
                        error = %e,
                        "webhook delivery failed"
                    ),
                }
            }
        }
    }
}
