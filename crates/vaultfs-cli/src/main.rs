use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::Value;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "vaultfsctl", version, about = "VaultFS admin CLI")]
struct Cli {
    /// VaultFS server URL
    #[arg(long, env = "VAULTFS_URL", default_value = "http://localhost:8000")]
    url: String,

    /// API key for authentication
    #[arg(long, env = "VAULTFS_KEY")]
    key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Bucket operations
    Bucket {
        #[command(subcommand)]
        action: BucketAction,
    },
    /// Object operations
    Object {
        #[command(subcommand)]
        action: ObjectAction,
    },
    /// API key management
    Key {
        #[command(subcommand)]
        action: KeyAction,
    },
    /// Show storage statistics
    Stats,
    /// Run garbage collection
    Gc,
    /// Create a backup snapshot
    Backup,
    /// Export a bucket
    Export {
        /// Bucket name to export
        bucket: String,
    },
    /// Health check
    Health,
}

#[derive(Subcommand)]
enum BucketAction {
    /// List all buckets
    List,
    /// Create a bucket
    Create {
        /// Bucket name
        name: String,
        /// Make bucket public
        #[arg(long)]
        public: bool,
    },
    /// Delete a bucket
    Delete {
        /// Bucket name
        name: String,
    },
}

#[derive(Subcommand)]
enum ObjectAction {
    /// List objects in a bucket
    List {
        /// Bucket name
        bucket: String,
        /// Filter by prefix
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Upload a file
    Put {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
        /// Path to local file
        file: PathBuf,
    },
    /// Download a file
    Get {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
        /// Destination file path (stdout if omitted)
        dest: Option<PathBuf>,
    },
    /// Delete an object
    Delete {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
    },
    /// Show object metadata
    Head {
        /// Bucket name
        bucket: String,
        /// Object key
        key: String,
    },
}

#[derive(Subcommand)]
enum KeyAction {
    /// List API keys
    List,
    /// Create an API key
    Create {
        /// Key name
        name: String,
        /// Grant read permission
        #[arg(long)]
        read: bool,
        /// Grant write permission
        #[arg(long)]
        write: bool,
        /// Grant delete permission
        #[arg(long)]
        delete: bool,
        /// Grant admin permission
        #[arg(long)]
        admin: bool,
    },
    /// Revoke an API key
    Delete {
        /// Key ID
        id: String,
    },
}

struct ApiClient {
    client: Client,
    base_url: String,
    api_key: Option<String>,
}

impl ApiClient {
    fn new(base_url: String, api_key: Option<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut req = self.client.request(method, self.url(path));
        if let Some(ref key) = self.api_key {
            req = req.header("Authorization", format!("Bearer {}", key));
        }
        req
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, path)
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, path)
    }

    fn put(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::PUT, path)
    }

    fn delete(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::DELETE, path)
    }

    fn head(&self, path: &str) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::HEAD, path)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let api = ApiClient::new(cli.url, cli.key);

    match cli.command {
        Commands::Bucket { action } => match action {
            BucketAction::List => cmd_bucket_list(&api).await,
            BucketAction::Create { name, public } => cmd_bucket_create(&api, &name, public).await,
            BucketAction::Delete { name } => cmd_bucket_delete(&api, &name).await,
        },
        Commands::Object { action } => match action {
            ObjectAction::List { bucket, prefix } => {
                cmd_object_list(&api, &bucket, prefix.as_deref()).await
            }
            ObjectAction::Put { bucket, key, file } => {
                cmd_object_put(&api, &bucket, &key, &file).await
            }
            ObjectAction::Get { bucket, key, dest } => {
                cmd_object_get(&api, &bucket, &key, dest.as_deref()).await
            }
            ObjectAction::Delete { bucket, key } => {
                cmd_object_delete(&api, &bucket, &key).await
            }
            ObjectAction::Head { bucket, key } => cmd_object_head(&api, &bucket, &key).await,
        },
        Commands::Key { action } => match action {
            KeyAction::List => cmd_key_list(&api).await,
            KeyAction::Create {
                name,
                read,
                write,
                delete,
                admin,
            } => cmd_key_create(&api, &name, read, write, delete, admin).await,
            KeyAction::Delete { id } => cmd_key_delete(&api, &id).await,
        },
        Commands::Stats => cmd_stats(&api).await,
        Commands::Gc => cmd_gc(&api).await,
        Commands::Backup => cmd_backup(&api).await,
        Commands::Export { bucket } => cmd_export(&api, &bucket).await,
        Commands::Health => cmd_health(&api).await,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn human_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{:.1} {}", size, unit);
        }
        size /= 1024.0;
    }
    format!("{:.1} PB", size)
}

/// Check the HTTP response and return the JSON body, or print an error and bail.
async fn check_response(resp: reqwest::Response) -> Result<Value> {
    let status = resp.status();
    if status.is_success() {
        let body = resp
            .json::<Value>()
            .await
            .unwrap_or(Value::Null);
        Ok(body)
    } else {
        let body = resp.text().await.unwrap_or_default();
        let msg = if let Ok(v) = serde_json::from_str::<Value>(&body) {
            v.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or(&body)
                .to_string()
        } else {
            body
        };
        anyhow::bail!("HTTP {} - {}", status.as_u16(), msg);
    }
}

/// Print a simple table: each row is a Vec of column values.
fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(no results)");
        return;
    }

    let col_count = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < col_count {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Header
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h.to_uppercase(), width = widths[i]))
        .collect();
    println!("{}", header_line.join("  "));

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("{}", sep.join("  "));

    // Rows
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = widths.get(i).copied().unwrap_or(0);
                format!("{:<width$}", c, width = w)
            })
            .collect();
        println!("{}", cells.join("  "));
    }
}

// ---------------------------------------------------------------------------
// Bucket commands
// ---------------------------------------------------------------------------

async fn cmd_bucket_list(api: &ApiClient) -> Result<()> {
    let resp = api.get("/v1/buckets").send().await.context("request failed")?;
    let body = check_response(resp).await?;
    let buckets = body
        .get("buckets")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let rows: Vec<Vec<String>> = buckets
        .iter()
        .map(|b| {
            vec![
                b.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
                b.get("public")
                    .and_then(|v| v.as_bool())
                    .map(|p| if p { "yes" } else { "no" })
                    .unwrap_or("-")
                    .to_string(),
                b.get("created_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
            ]
        })
        .collect();

    print_table(&["Name", "Public", "Created"], &rows);
    Ok(())
}

async fn cmd_bucket_create(api: &ApiClient, name: &str, public: bool) -> Result<()> {
    let resp = api
        .post("/v1/buckets")
        .json(&serde_json::json!({ "name": name, "public": public }))
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;
    println!("Bucket created:");
    println!("{}", serde_json::to_string_pretty(&body)?);
    Ok(())
}

async fn cmd_bucket_delete(api: &ApiClient, name: &str) -> Result<()> {
    let resp = api
        .delete(&format!("/v1/buckets/{}", name))
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    if status.is_success() {
        println!("Bucket '{}' deleted.", name);
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {} - {}", status.as_u16(), body);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Object commands
// ---------------------------------------------------------------------------

async fn cmd_object_list(api: &ApiClient, bucket: &str, prefix: Option<&str>) -> Result<()> {
    let mut url = format!("/v1/objects/{}", bucket);
    if let Some(p) = prefix {
        url.push_str(&format!("?prefix={}", p));
    }
    let resp = api.get(&url).send().await.context("request failed")?;
    let body = check_response(resp).await?;
    let objects = body
        .get("objects")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let rows: Vec<Vec<String>> = objects
        .iter()
        .map(|o| {
            let size = o
                .get("size")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            vec![
                o.get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
                human_size(size),
                o.get("content_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
                o.get("updated_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
            ]
        })
        .collect();

    print_table(&["Key", "Size", "Type", "Modified"], &rows);
    println!("\n{} object(s)", objects.len());
    Ok(())
}

async fn cmd_object_put(api: &ApiClient, bucket: &str, key: &str, file: &PathBuf) -> Result<()> {
    let data = tokio::fs::read(file)
        .await
        .with_context(|| format!("failed to read {}", file.display()))?;
    let content_type = mime_guess::from_path(file)
        .first_or_octet_stream()
        .to_string();
    let size = data.len();

    let resp = api
        .put(&format!("/v1/objects/{}/{}", bucket, key))
        .header("Content-Type", &content_type)
        .body(data)
        .send()
        .await
        .context("upload failed")?;
    let body = check_response(resp).await?;
    let sha = body
        .get("sha256")
        .and_then(|v| v.as_str())
        .unwrap_or("-");
    println!(
        "Uploaded {}/{} ({}, {})",
        bucket,
        key,
        human_size(size as u64),
        content_type
    );
    println!("SHA-256: {}", sha);
    Ok(())
}

async fn cmd_object_get(
    api: &ApiClient,
    bucket: &str,
    key: &str,
    dest: Option<&std::path::Path>,
) -> Result<()> {
    let resp = api
        .get(&format!("/v1/objects/{}/{}", bucket, key))
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {} - {}", status.as_u16(), body);
    }

    let bytes = resp.bytes().await.context("failed to read response body")?;

    match dest {
        Some(path) => {
            tokio::fs::write(path, &bytes)
                .await
                .with_context(|| format!("failed to write {}", path.display()))?;
            println!(
                "Downloaded {}/{} -> {} ({})",
                bucket,
                key,
                path.display(),
                human_size(bytes.len() as u64)
            );
        }
        None => {
            use std::io::Write;
            std::io::stdout()
                .write_all(&bytes)
                .context("failed to write to stdout")?;
        }
    }
    Ok(())
}

async fn cmd_object_delete(api: &ApiClient, bucket: &str, key: &str) -> Result<()> {
    let resp = api
        .delete(&format!("/v1/objects/{}/{}", bucket, key))
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    if status.is_success() {
        println!("Deleted {}/{}.", bucket, key);
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {} - {}", status.as_u16(), body);
    }
    Ok(())
}

async fn cmd_object_head(api: &ApiClient, bucket: &str, key: &str) -> Result<()> {
    let resp = api
        .head(&format!("/v1/objects/{}/{}", bucket, key))
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("HTTP {} - object not found", status.as_u16());
    }

    let headers = resp.headers();
    let pairs: Vec<(&str, String)> = vec![
        (
            "Content-Type",
            headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "Content-Length",
            headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "ETag",
            headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "Last-Modified",
            headers
                .get("last-modified")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "Accept-Ranges",
            headers
                .get("accept-ranges")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string(),
        ),
    ];

    let max_label = pairs.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    for (label, value) in &pairs {
        println!("{:>width$}: {}", label, value, width = max_label);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Key commands
// ---------------------------------------------------------------------------

async fn cmd_key_list(api: &ApiClient) -> Result<()> {
    let resp = api
        .get("/v1/admin/keys")
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;
    let keys = body
        .get("keys")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let rows: Vec<Vec<String>> = keys
        .iter()
        .map(|k| {
            let perms = k.get("permissions");
            let perm_str = [
                if perms
                    .and_then(|p| p.get("read"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    "R"
                } else {
                    "-"
                },
                if perms
                    .and_then(|p| p.get("write"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    "W"
                } else {
                    "-"
                },
                if perms
                    .and_then(|p| p.get("delete"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    "D"
                } else {
                    "-"
                },
                if perms
                    .and_then(|p| p.get("admin"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    "A"
                } else {
                    "-"
                },
            ]
            .join("");

            vec![
                k.get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
                k.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
                perm_str,
                k.get("created_at")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-")
                    .to_string(),
            ]
        })
        .collect();

    print_table(&["ID", "Name", "Perms", "Created"], &rows);
    Ok(())
}

async fn cmd_key_create(
    api: &ApiClient,
    name: &str,
    read: bool,
    write: bool,
    delete: bool,
    admin: bool,
) -> Result<()> {
    let resp = api
        .post("/v1/admin/keys")
        .json(&serde_json::json!({
            "name": name,
            "permissions": {
                "read": read,
                "write": write,
                "delete": delete,
                "admin": admin,
            }
        }))
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;

    println!("API key created:");
    if let Some(secret) = body.get("secret").and_then(|v| v.as_str()) {
        println!("  Secret: {}", secret);
        println!("  (store this securely -- it cannot be retrieved again)");
    }
    if let Some(key_obj) = body.get("key") {
        println!(
            "  ID:   {}",
            key_obj
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
        );
        println!(
            "  Name: {}",
            key_obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
        );
    }
    Ok(())
}

async fn cmd_key_delete(api: &ApiClient, id: &str) -> Result<()> {
    let resp = api
        .delete(&format!("/v1/admin/keys/{}", id))
        .send()
        .await
        .context("request failed")?;
    let status = resp.status();
    if status.is_success() {
        println!("Key '{}' revoked.", id);
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {} - {}", status.as_u16(), body);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Admin commands
// ---------------------------------------------------------------------------

async fn cmd_stats(api: &ApiClient) -> Result<()> {
    let resp = api.get("/v1/stats").send().await.context("request failed")?;
    let body = check_response(resp).await?;

    println!("VaultFS Storage Statistics");
    println!("==========================");
    println!(
        "Buckets:       {}",
        body.get("buckets")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!(
        "Total objects: {}",
        body.get("total_objects")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!(
        "Total size:    {}",
        body.get("total_size_human")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    println!(
        "Disk usage:    {}",
        body.get("disk_usage_human")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    println!(
        "Version:       {}",
        body.get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );

    if let Some(details) = body.get("bucket_details").and_then(|v| v.as_array()) {
        if !details.is_empty() {
            println!();
            let rows: Vec<Vec<String>> = details
                .iter()
                .map(|b| {
                    vec![
                        b.get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                        b.get("objects")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0)
                            .to_string(),
                        b.get("size_human")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                    ]
                })
                .collect();
            print_table(&["Bucket", "Objects", "Size"], &rows);
        }
    }
    Ok(())
}

async fn cmd_gc(api: &ApiClient) -> Result<()> {
    println!("Running garbage collection...");
    let resp = api
        .post("/v1/admin/gc")
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;

    println!(
        "Blobs scanned:   {}",
        body.get("blobs_scanned")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!(
        "Orphans removed: {}",
        body.get("orphans_removed")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    let freed = body
        .get("bytes_freed")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    println!("Bytes freed:     {}", human_size(freed));
    Ok(())
}

async fn cmd_backup(api: &ApiClient) -> Result<()> {
    println!("Creating backup snapshot...");
    let resp = api
        .post("/v1/admin/backup")
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;

    println!(
        "Path:         {}",
        body.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    let db_size = body.get("db_size").and_then(|v| v.as_u64()).unwrap_or(0);
    println!("DB size:      {}", human_size(db_size));
    println!(
        "Blobs copied: {}",
        body.get("blobs_copied")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    let total = body
        .get("total_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    println!("Total size:   {}", human_size(total));
    Ok(())
}

async fn cmd_export(api: &ApiClient, bucket: &str) -> Result<()> {
    println!("Exporting bucket '{}'...", bucket);
    let resp = api
        .post(&format!("/v1/admin/backup/export/{}", bucket))
        .send()
        .await
        .context("request failed")?;
    let body = check_response(resp).await?;

    println!(
        "Bucket:           {}",
        body.get("bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    println!(
        "Objects exported: {}",
        body.get("objects_exported")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );
    println!(
        "Path:             {}",
        body.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("-")
    );
    Ok(())
}

async fn cmd_health(api: &ApiClient) -> Result<()> {
    let resp = api
        .get("/health")
        .send()
        .await
        .context("cannot reach server")?;
    let status = resp.status();
    let body = resp.json::<Value>().await.unwrap_or(Value::Null);

    let srv_status = body
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let version = body
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if status.is_success() && srv_status == "ok" {
        println!("VaultFS is healthy (v{})", version);
    } else {
        println!("VaultFS health check failed (HTTP {}, status={})", status, srv_status);
        std::process::exit(1);
    }
    Ok(())
}
