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
    /// Migrate data from external sources into VaultFS
    Migrate {
        #[command(subcommand)]
        source: MigrateSource,
    },
    /// Promote this replica to a new primary after the old primary failed.
    /// Prints the checkpoint cursor, deletes the cursor file so nothing
    /// accidentally reconnects to the dead primary, and runs a sanity
    /// probe against the local server. See docs/failover.md.
    Promote {
        /// Cursor file written by `vaultfsctl replicate`. Deleted on
        /// success so a future replicate call against the dead primary
        /// fails loudly instead of rewinding to 0.
        #[arg(long, default_value = "./vaultfs-replica.cursor")]
        cursor_file: PathBuf,
        /// Skip deletion of the cursor file (handy in tests or if you
        /// want to inspect the last-applied id without side effects).
        #[arg(long)]
        keep_cursor: bool,
    },
    /// Pull replication events from a primary VaultFS and apply them to the
    /// local server. Designed to run as a one-shot or a tight loop.
    Replicate {
        /// Primary VaultFS URL (e.g. https://vaultfs-primary.example.com)
        #[arg(long)]
        primary: String,
        /// Admin API key on the primary (read-only keys are not enough)
        #[arg(long, env = "VAULTFS_PRIMARY_KEY")]
        primary_key: String,
        /// Local (replica) VaultFS URL. Defaults to --url.
        #[arg(long)]
        local: Option<String>,
        /// Admin API key on the local replica. Defaults to --key.
        #[arg(long, env = "VAULTFS_LOCAL_KEY")]
        local_key: Option<String>,
        /// Cursor file that records the last applied event id. A missing
        /// file means start from event 0 (full catch-up).
        #[arg(long, default_value = "./vaultfs-replica.cursor")]
        cursor_file: PathBuf,
        /// Poll interval in seconds. If 0, apply once and exit.
        #[arg(long, default_value_t = 0u64)]
        interval: u64,
        /// Max events pulled per batch (server caps at 1000).
        #[arg(long, default_value_t = 100u32)]
        batch_size: u32,
    },
}

#[derive(Subcommand)]
enum MigrateSource {
    /// Import objects from an S3-compatible source (AWS S3, MinIO, etc.)
    S3 {
        /// S3 endpoint URL (e.g. https://s3.amazonaws.com or https://minio.example.com)
        #[arg(long)]
        source_endpoint: String,
        /// Source S3 bucket name
        #[arg(long)]
        source_bucket: String,
        /// S3 access key ID
        #[arg(long)]
        source_access_key: String,
        /// S3 secret access key
        #[arg(long)]
        source_secret_key: String,
        /// Destination VaultFS bucket name
        #[arg(long)]
        dest_bucket: String,
        /// Only migrate objects with this key prefix
        #[arg(long)]
        prefix: Option<String>,
        /// List objects that would be migrated without actually migrating
        #[arg(long)]
        dry_run: bool,
    },
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
        Commands::Migrate { source } => match source {
            MigrateSource::S3 {
                source_endpoint,
                source_bucket,
                source_access_key,
                source_secret_key,
                dest_bucket,
                prefix,
                dry_run,
            } => {
                cmd_migrate_s3(
                    &api,
                    &source_endpoint,
                    &source_bucket,
                    &source_access_key,
                    &source_secret_key,
                    &dest_bucket,
                    prefix.as_deref(),
                    dry_run,
                )
                .await
            }
        },
        Commands::Promote {
            cursor_file,
            keep_cursor,
        } => cmd_promote(&api, &cursor_file, keep_cursor).await,
        Commands::Replicate {
            primary,
            primary_key,
            local,
            local_key,
            cursor_file,
            interval,
            batch_size,
        } => {
            let local_url = local.unwrap_or_else(|| api.base_url.clone());
            let local_key_val = local_key
                .or_else(|| api.api_key.clone())
                .ok_or_else(|| anyhow::anyhow!("no local key — pass --local-key or --key"))?;
            cmd_replicate(
                &primary,
                &primary_key,
                &local_url,
                &local_key_val,
                &cursor_file,
                interval,
                batch_size,
            )
            .await
        }
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

// ---------------------------------------------------------------------------
// S3 Migration
// ---------------------------------------------------------------------------

/// Represents an object discovered in the S3 source bucket.
struct S3Object {
    key: String,
    size: u64,
}

/// Parse the ListObjectsV2 XML response using simple string matching.
/// Extracts <Key>, <Size> for each <Contents> entry, and the
/// <NextContinuationToken> if present.
fn parse_s3_list_response(xml: &str) -> (Vec<S3Object>, Option<String>) {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    // Extract <NextContinuationToken>
    if let Some(start) = xml.find("<NextContinuationToken>") {
        let after = &xml[start + "<NextContinuationToken>".len()..];
        if let Some(end) = after.find("</NextContinuationToken>") {
            continuation_token = Some(after[..end].to_string());
        }
    }

    // Extract each <Contents> block
    let mut search = xml;
    while let Some(start) = search.find("<Contents>") {
        let after = &search[start..];
        let end = match after.find("</Contents>") {
            Some(e) => e + "</Contents>".len(),
            None => break,
        };
        let block = &after[..end];

        let key = extract_xml_tag(block, "Key").unwrap_or_default();
        let size: u64 = extract_xml_tag(block, "Size")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if !key.is_empty() {
            objects.push(S3Object { key, size });
        }

        search = &search[start + end..];
    }

    (objects, continuation_token)
}

/// Extract the text content of a simple XML tag like <Tag>value</Tag>.
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(xml[start..end].to_string())
}

/// Sign an S3 request using AWS Signature Version 4 (simplified).
/// This produces the Authorization header for a given request.
fn s3_sign_request(
    method: &str,
    url: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    headers: &[(&str, &str)],
    payload_hash: &str,
) -> Vec<(String, String)> {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    let now = chrono::Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    // Parse the URL
    let parsed = url::Url::parse(url).expect("invalid URL");
    let host = parsed.host_str().unwrap_or("localhost");
    let host_with_port = if let Some(port) = parsed.port() {
        format!("{}:{}", host, port)
    } else {
        host.to_string()
    };
    let canonical_uri = parsed.path().to_string();
    let canonical_querystring = parsed.query().unwrap_or("").to_string();

    // Build signed headers: host + x-amz-content-sha256 + x-amz-date + any extras
    let mut all_headers: Vec<(String, String)> = vec![
        ("host".to_string(), host_with_port.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    for (k, v) in headers {
        all_headers.push((k.to_lowercase(), v.to_string()));
    }
    all_headers.sort_by(|a, b| a.0.cmp(&b.0));

    let signed_headers: String = all_headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = all_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash,
    );

    let mut hasher = Sha256::new();
    hasher.update(canonical_request.as_bytes());
    let canonical_request_hash = hex::encode(hasher.finalize());

    let credential_scope = format!("{}/{}/s3/aws4_request", date_stamp, region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date, credential_scope, canonical_request_hash,
    );

    // Derive signing key
    type HmacSha256 = Hmac<Sha256>;
    let k_date = {
        let mut mac = HmacSha256::new_from_slice(format!("AWS4{}", secret_key).as_bytes()).unwrap();
        mac.update(date_stamp.as_bytes());
        mac.finalize().into_bytes()
    };
    let k_region = {
        let mut mac = HmacSha256::new_from_slice(&k_date).unwrap();
        mac.update(region.as_bytes());
        mac.finalize().into_bytes()
    };
    let k_service = {
        let mut mac = HmacSha256::new_from_slice(&k_region).unwrap();
        mac.update(b"s3");
        mac.finalize().into_bytes()
    };
    let k_signing = {
        let mut mac = HmacSha256::new_from_slice(&k_service).unwrap();
        mac.update(b"aws4_request");
        mac.finalize().into_bytes()
    };

    let signature = {
        let mut mac = HmacSha256::new_from_slice(&k_signing).unwrap();
        mac.update(string_to_sign.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    };

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key, credential_scope, signed_headers, signature,
    );

    vec![
        ("Authorization".to_string(), authorization),
        ("x-amz-date".to_string(), amz_date),
        (
            "x-amz-content-sha256".to_string(),
            payload_hash.to_string(),
        ),
        ("Host".to_string(), host_with_port),
    ]
}

#[allow(clippy::too_many_arguments)]
async fn cmd_migrate_s3(
    api: &ApiClient,
    source_endpoint: &str,
    source_bucket: &str,
    access_key: &str,
    secret_key: &str,
    dest_bucket: &str,
    prefix: Option<&str>,
    dry_run: bool,
) -> Result<()> {
    let client = Client::new();
    let endpoint = source_endpoint.trim_end_matches('/');
    let region = "us-east-1"; // Default region; works for MinIO and most S3-compatible services.

    println!(
        "Discovering objects in s3://{}/{}...",
        source_bucket,
        prefix.unwrap_or("")
    );

    // Phase 1: List all objects with pagination
    let mut all_objects: Vec<S3Object> = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut query = format!("list-type=2&max-keys=1000");
        if let Some(p) = prefix {
            query.push_str(&format!("&prefix={}", p));
        }
        if let Some(ref token) = continuation_token {
            query.push_str(&format!("&continuation-token={}", token));
        }

        let list_url = format!("{}/{}?{}", endpoint, source_bucket, query);
        let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // SHA-256 of empty body

        let sig_headers =
            s3_sign_request("GET", &list_url, access_key, secret_key, region, &[], payload_hash);

        let mut req = client.get(&list_url);
        for (k, v) in &sig_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let resp = req.send().await.context("failed to list S3 objects")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "S3 ListObjectsV2 failed (HTTP {}): {}",
                status.as_u16(),
                body
            );
        }

        let xml = resp.text().await.context("failed to read S3 list response")?;
        let (objects, next_token) = parse_s3_list_response(&xml);

        all_objects.extend(objects);
        continuation_token = next_token;

        if continuation_token.is_none() {
            break;
        }
    }

    if all_objects.is_empty() {
        println!("No objects found.");
        return Ok(());
    }

    let total = all_objects.len();
    let total_size: u64 = all_objects.iter().map(|o| o.size).sum();
    println!(
        "Found {} object(s) ({} total)",
        total,
        human_size(total_size)
    );

    if dry_run {
        println!("\n--- DRY RUN (no data will be transferred) ---\n");
        for (i, obj) in all_objects.iter().enumerate() {
            println!(
                "[{}/{}] {} ({})",
                i + 1,
                total,
                obj.key,
                human_size(obj.size)
            );
        }
        println!(
            "\nWould migrate {} object(s), {} total.",
            total,
            human_size(total_size)
        );
        return Ok(());
    }

    // Phase 2: Download each object from S3 and upload to VaultFS
    let mut migrated = 0u64;
    let mut failed = 0u64;
    let mut bytes_transferred = 0u64;

    for (i, obj) in all_objects.iter().enumerate() {
        let object_url = format!("{}/{}/{}", endpoint, source_bucket, obj.key);
        let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let sig_headers = s3_sign_request(
            "GET",
            &object_url,
            access_key,
            secret_key,
            region,
            &[],
            payload_hash,
        );

        let mut req = client.get(&object_url);
        for (k, v) in &sig_headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let download = match req.send().await {
            Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                Ok(data) => data,
                Err(e) => {
                    eprintln!(
                        "[{}/{}] FAILED to read {} : {}",
                        i + 1,
                        total,
                        obj.key,
                        e
                    );
                    failed += 1;
                    continue;
                }
            },
            Ok(resp) => {
                let status = resp.status();
                eprintln!(
                    "[{}/{}] FAILED to download {} (HTTP {})",
                    i + 1,
                    total,
                    obj.key,
                    status.as_u16()
                );
                failed += 1;
                continue;
            }
            Err(e) => {
                eprintln!(
                    "[{}/{}] FAILED to download {} : {}",
                    i + 1,
                    total,
                    obj.key,
                    e
                );
                failed += 1;
                continue;
            }
        };

        // Guess content type from the key
        let content_type = mime_guess::from_path(&obj.key)
            .first_or_octet_stream()
            .to_string();

        // Upload to VaultFS
        let upload_path = format!("/v1/objects/{}/{}", dest_bucket, obj.key);
        let upload_resp = api
            .put(&upload_path)
            .header("Content-Type", &content_type)
            .body(download.to_vec())
            .send()
            .await;

        match upload_resp {
            Ok(resp) if resp.status().is_success() => {
                migrated += 1;
                bytes_transferred += obj.size;
                println!(
                    "[{}/{}] Migrated {} ({})",
                    i + 1,
                    total,
                    obj.key,
                    human_size(obj.size)
                );
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!(
                    "[{}/{}] FAILED to upload {} (HTTP {}): {}",
                    i + 1,
                    total,
                    obj.key,
                    status.as_u16(),
                    body
                );
                failed += 1;
            }
            Err(e) => {
                eprintln!(
                    "[{}/{}] FAILED to upload {} : {}",
                    i + 1,
                    total,
                    obj.key,
                    e
                );
                failed += 1;
            }
        }
    }

    println!("\n--- Migration Complete ---");
    println!("Migrated:    {} object(s)", migrated);
    println!("Failed:      {} object(s)", failed);
    println!("Transferred: {}", human_size(bytes_transferred));

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct ReplEvent {
    id: i64,
    op: String,
    bucket: String,
    key: String,
    #[serde(default)]
    sha256: String,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    content_type: String,
}

#[derive(serde::Deserialize)]
struct EventsPage {
    events: Vec<ReplEvent>,
}

fn load_cursor(path: &std::path::Path) -> i64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn save_cursor(path: &std::path::Path, id: i64) -> Result<()> {
    std::fs::write(path, id.to_string())
        .with_context(|| format!("failed to persist cursor to {}", path.display()))
}

async fn sync_once(
    primary: &str,
    primary_key: &str,
    local: &str,
    local_key: &str,
    cursor_file: &std::path::Path,
    batch_size: u32,
) -> Result<(u64, i64)> {
    let client = Client::new();
    let mut cursor = load_cursor(cursor_file);
    let mut applied: u64 = 0;

    loop {
        // Pull the next batch
        let url = format!(
            "{}/v1/replication/events?since={}&limit={}",
            primary.trim_end_matches('/'),
            cursor,
            batch_size
        );
        let resp = client
            .get(&url)
            .header("Authorization", format!("Bearer {}", primary_key))
            .send()
            .await
            .context("fetch events")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("primary events HTTP {}: {}", status, body);
        }
        let page: EventsPage = resp.json().await.context("parse events")?;
        if page.events.is_empty() {
            return Ok((applied, cursor));
        }

        for event in &page.events {
            apply_one(primary, primary_key, local, local_key, event)
                .await
                .with_context(|| format!("apply event id={}", event.id))?;
            cursor = event.id;
            save_cursor(cursor_file, cursor)?;
            applied += 1;
        }
    }
}

async fn apply_one(
    primary: &str,
    primary_key: &str,
    local: &str,
    local_key: &str,
    event: &ReplEvent,
) -> Result<()> {
    let client = Client::new();

    // Ensure the blob is on the replica before we publish a metadata row
    // that references it. Idempotent: if the local already has the blob
    // the import endpoint just overwrites with identical bytes.
    if matches!(event.op.as_str(), "put" | "version_put") && !event.sha256.is_empty() {
        let blob_url = format!(
            "{}/v1/replication/blob/{}",
            primary.trim_end_matches('/'),
            event.sha256
        );
        let resp = client
            .get(&blob_url)
            .header("Authorization", format!("Bearer {}", primary_key))
            .send()
            .await
            .context("fetch blob")?;
        if !resp.status().is_success() {
            anyhow::bail!(
                "primary blob {} HTTP {}",
                &event.sha256,
                resp.status().as_u16()
            );
        }
        let bytes = resp.bytes().await.context("read blob body")?;

        let import_url = format!(
            "{}/v1/replication/blob/{}",
            local.trim_end_matches('/'),
            event.sha256
        );
        let resp = client
            .put(&import_url)
            .header("Authorization", format!("Bearer {}", local_key))
            .body(bytes)
            .send()
            .await
            .context("import blob to local")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("local import blob HTTP {}: {}", status, body);
        }
    }

    let apply_url = format!("{}/v1/replication/apply", local.trim_end_matches('/'));
    let resp = client
        .post(&apply_url)
        .header("Authorization", format!("Bearer {}", local_key))
        .json(&serde_json::json!({
            "op": event.op,
            "bucket": event.bucket,
            "key": event.key,
            "sha256": event.sha256,
            "version_id": event.version_id,
            "size": event.size,
            "content_type": event.content_type,
        }))
        .send()
        .await
        .context("apply to local")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("local apply HTTP {}: {}", status, body);
    }
    Ok(())
}

async fn cmd_replicate(
    primary: &str,
    primary_key: &str,
    local: &str,
    local_key: &str,
    cursor_file: &std::path::Path,
    interval: u64,
    batch_size: u32,
) -> Result<()> {
    if interval == 0 {
        println!("Replicating {} → {} (one-shot)...", primary, local);
        let (n, cursor) = sync_once(
            primary, primary_key, local, local_key, cursor_file, batch_size,
        )
        .await?;
        println!("Applied {} event(s). Cursor at {}.", n, cursor);
        return Ok(());
    }

    println!(
        "Replicating {} → {} every {}s (Ctrl-C to stop)...",
        primary, local, interval
    );
    loop {
        match sync_once(
            primary, primary_key, local, local_key, cursor_file, batch_size,
        )
        .await
        {
            Ok((0, _)) => {}
            Ok((n, cursor)) => println!("[{}] +{} events (cursor={})", chrono::Utc::now(), n, cursor),
            Err(e) => eprintln!("[{}] replication error: {}", chrono::Utc::now(), e),
        }
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
    }
}

// ---------------------------------------------------------------------------
// Promote
// ---------------------------------------------------------------------------

async fn cmd_promote(
    api: &ApiClient,
    cursor_file: &std::path::Path,
    keep_cursor: bool,
) -> Result<()> {
    println!("=== VaultFS promote ===");
    println!("Local server: {}", api.base_url);
    println!();

    // Sanity probe — the new primary must answer /health and have an
    // authenticated caller. Failing fast here prevents an operator from
    // updating DNS only to discover the node is down.
    let resp = api
        .get("/health")
        .send()
        .await
        .context("local server did not respond to /health")?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "local /health returned HTTP {} — refusing to promote an unhealthy node",
            resp.status().as_u16()
        );
    }
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    println!(
        "  ✓ local server healthy (version={})",
        body.get("version").and_then(|v| v.as_str()).unwrap_or("?")
    );

    let stats_resp = api
        .get("/v1/stats")
        .send()
        .await
        .context("local /v1/stats request failed")?;
    if !stats_resp.status().is_success() {
        anyhow::bail!(
            "local /v1/stats returned HTTP {} — the admin API key looks wrong",
            stats_resp.status().as_u16()
        );
    }
    let stats: Value = stats_resp.json().await.unwrap_or(Value::Null);
    println!(
        "  ✓ local admin auth works  buckets={} objects={} disk={}",
        stats.get("buckets").and_then(|v| v.as_u64()).unwrap_or(0),
        stats.get("total_objects").and_then(|v| v.as_u64()).unwrap_or(0),
        stats
            .get("disk_usage_human")
            .and_then(|v| v.as_str())
            .unwrap_or("?"),
    );

    // Checkpoint: report the last-applied event id, then (unless
    // --keep-cursor) delete the file so a future `replicate` against
    // the dead primary errors instead of silently replaying from 0.
    let cursor = std::fs::read_to_string(cursor_file)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    println!(
        "  ✓ cursor file: {} (last applied event id = {})",
        cursor_file.display(),
        cursor
    );

    if keep_cursor {
        println!("  — keeping cursor file (you passed --keep-cursor)");
    } else if cursor_file.exists() {
        std::fs::remove_file(cursor_file).with_context(|| {
            format!("failed to delete cursor file {}", cursor_file.display())
        })?;
        println!("  ✓ cursor file deleted");
    } else {
        println!("  — cursor file already absent");
    }

    // Operator checklist. Promotion itself is not a state change on the
    // server — the server always writes to its own replication log.
    // What changes is *who the world talks to*, which the operator
    // must reconfigure outside VaultFS.
    println!();
    println!("Promotion checklist (complete these next):");
    println!("  1. Point clients at this node (DNS, load balancer, SDK base_url).");
    println!("  2. Revoke the old primary's admin key (in case it comes back online).");
    println!("  3. Start `vaultfsctl replicate` on each remaining replica with");
    println!("     --primary={} and a fresh cursor file.", api.base_url);
    println!("  4. Investigate why the old primary failed before reusing the node.");
    println!();
    println!("See docs/failover.md for the full runbook.");
    Ok(())
}
