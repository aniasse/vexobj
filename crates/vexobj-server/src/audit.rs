use std::path::Path;
use std::sync::Arc;

use axum::extract::{Extension, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Mutex;

use crate::middleware::require_permission;
use crate::state::AppState;
use vexobj_auth::ApiKey;

#[derive(Clone)]
pub struct AuditLogger {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize)]
pub struct AuditEntry {
    pub id: i64,
    pub timestamp: String,
    pub api_key_prefix: String,
    pub action: String,
    pub resource: String,
    pub details: serde_json::Value,
    pub ip_address: String,
}

impl AuditLogger {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        let db_path = data_dir.join("audit.db");
        let conn = Connection::open(db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                api_key_prefix TEXT NOT NULL,
                action TEXT NOT NULL,
                resource TEXT NOT NULL,
                details TEXT NOT NULL DEFAULT '{}',
                ip_address TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp);
            CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn log(
        &self,
        api_key_prefix: &str,
        action: &str,
        resource: &str,
        details: &serde_json::Value,
        ip_address: &str,
    ) {
        let timestamp = chrono::Utc::now().to_rfc3339();
        let details_str = serde_json::to_string(details).unwrap_or_default();
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                "INSERT INTO audit_log (timestamp, api_key_prefix, action, resource, details, ip_address)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![timestamp, api_key_prefix, action, resource, details_str, ip_address],
            );
        }
    }

    pub fn query(&self, limit: i64, offset: i64) -> anyhow::Result<Vec<AuditEntry>> {
        let conn = self.conn.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
        let mut stmt = conn.prepare(
            "SELECT id, timestamp, api_key_prefix, action, resource, details, ip_address
             FROM audit_log ORDER BY id DESC LIMIT ?1 OFFSET ?2",
        )?;
        let entries = stmt
            .query_map(params![limit, offset], |row| {
                let details_str: String = row.get(5)?;
                let details: serde_json::Value =
                    serde_json::from_str(&details_str).unwrap_or(json!({}));
                Ok(AuditEntry {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    api_key_prefix: row.get(2)?,
                    action: row.get(3)?,
                    resource: row.get(4)?,
                    details,
                    ip_address: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }
}

/// Extract the client IP from x-forwarded-for or fall back to empty string.
pub fn extract_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or("").trim().to_string())
        .unwrap_or_default()
}

/// Extract a short prefix from the API key name for audit logging.
pub fn key_prefix(key: &ApiKey) -> String {
    format!("{}:{}", key.name, &key.id[..8.min(key.id.len())])
}

// Routes

pub fn routes() -> Router<AppState> {
    Router::new().route("/v1/admin/audit", get(get_audit_log))
}

#[derive(Deserialize)]
struct AuditQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 {
    50
}

async fn get_audit_log(
    State(state): State<AppState>,
    Extension(caller): Extension<ApiKey>,
    Query(query): Query<AuditQuery>,
) -> impl IntoResponse {
    if let Err(resp) = require_permission(&caller, "admin").await {
        return resp;
    }

    let limit = query.limit.min(1000).max(1);
    let offset = query.offset.max(0);

    match state.audit.query(limit, offset) {
        Ok(entries) => Json(json!({
            "entries": entries,
            "limit": limit,
            "offset": offset,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
