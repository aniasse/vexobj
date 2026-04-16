# VaultFS

High-performance, self-hosted object storage with built-in image processing. A single binary alternative to S3 + Cloudinary that runs on a simple VPS.

→ **Landing page**: https://aniasse.github.io/vaultfs/
→ **Release**: [v0.1.0](https://github.com/aniasse/vaultfs/releases/tag/v0.1.0) (Linux amd64, macOS arm64, Docker `ghcr.io/aniasse/vaultfs:latest`)

## Features

- **Universal object storage** — any file type (images, PDFs, videos, archives…)
- **Image processing on the fly** — resize / crop / convert via URL query parameters
- **Auto format negotiation** — AVIF / WebP served based on the browser `Accept` header
- **Content-addressable deduplication** — identical files stored once
- **Versioning & delete-markers** — history per object, with `?version_id=` on GET/DELETE and a one-shot purge endpoint
- **Object lock (WORM)** — per-object retention + legal hold; blocks deletes with HTTP 409
- **Lifecycle rules** — expire objects by prefix + age, on-demand or scheduled
- **Server-side encryption at rest** — AES-256-GCM per-blob keys derived from a master key; dedup still works
- **Multi-level cache** — in-memory LRU + disk cache (both enforced) for transformed images
- **API key auth** — per-key permissions (read / write / delete / admin) and bucket scoping
- **S3-compatible API** with **real AWS Signature V4 verification** — plug in any S3 SDK
- **Presigned URLs** + **multipart upload**
- **Admin dashboard** at `/dashboard`, Prometheus **metrics** at `/metrics`, OpenAPI spec at `/openapi.yaml`
- **Single ~14 MB binary**, SQLite metadata, no external dependencies
- **Official SDKs** — TypeScript, Python, Go

## Quick Start

```bash
# Build from source
cargo build --release

# Run with defaults (listens on :8000, stores in ./data)
./target/release/vaultfs

# Or with a config file
VAULTFS_CONFIG=config.toml ./target/release/vaultfs
```

On first launch with auth enabled, VaultFS generates an admin API key and prints it to the logs. Save this key.

## API

All endpoints (except `/health`) require authentication via `Authorization: Bearer <api-key>` header.

### Buckets

```bash
# Create a bucket (requires admin)
curl -X POST http://localhost:8000/v1/buckets \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "photos", "public": false}'

# List buckets
curl http://localhost:8000/v1/buckets \
  -H "Authorization: Bearer $API_KEY"
```

### Objects

```bash
# Upload any file
curl -X PUT http://localhost:8000/v1/objects/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: image/jpeg" \
  --data-binary @beach.jpg

# Upload a PDF
curl -X PUT http://localhost:8000/v1/objects/docs/report.pdf \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/pdf" \
  --data-binary @report.pdf

# Download
curl http://localhost:8000/v1/objects/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY" -o beach.jpg

# Download with image transformation
curl "http://localhost:8000/v1/objects/photos/vacation/beach.jpg?w=300&h=200&format=webp&quality=80" \
  -H "Authorization: Bearer $API_KEY"

# Get metadata
curl -I http://localhost:8000/v1/objects/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY"

# List objects with virtual directories
curl "http://localhost:8000/v1/objects/photos?prefix=vacation/&delimiter=/" \
  -H "Authorization: Bearer $API_KEY"

# Delete
curl -X DELETE http://localhost:8000/v1/objects/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY"
```

### Multipart Upload

```bash
# Upload multiple files at once
curl -X POST http://localhost:8000/v1/upload/photos/vacation \
  -H "Authorization: Bearer $API_KEY" \
  -F "file1=@beach.jpg" \
  -F "file2=@sunset.png" \
  -F "file3=@notes.txt"
```

### Presigned URLs

```bash
# Generate a temporary download URL (no auth needed to use it)
curl -X POST http://localhost:8000/v1/presign \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"method": "GET", "bucket": "photos", "key": "beach.jpg", "expires_in": 3600}'
```

### API Key Management

```bash
# Create a read-only key (requires admin)
curl -X POST http://localhost:8000/v1/admin/keys \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "frontend-app",
    "permissions": {"read": true, "write": false, "delete": false, "admin": false},
    "bucket_access": {"type": "specific", "buckets": ["public-assets"]}
  }'

# List keys
curl http://localhost:8000/v1/admin/keys \
  -H "Authorization: Bearer $ADMIN_KEY"

# Delete a key
curl -X DELETE http://localhost:8000/v1/admin/keys/{key-id} \
  -H "Authorization: Bearer $ADMIN_KEY"
```

### S3-Compatible API

VaultFS exposes an S3-compatible API under `/s3/`. Use it with any S3 SDK by pointing to your VaultFS instance.

```bash
# List buckets
curl http://localhost:8000/s3/ -H "Authorization: Bearer $API_KEY"

# Create bucket
curl -X PUT http://localhost:8000/s3/my-bucket -H "Authorization: Bearer $API_KEY"

# Put object
curl -X PUT http://localhost:8000/s3/my-bucket/file.txt \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary @file.txt

# Get object
curl http://localhost:8000/s3/my-bucket/file.txt -H "Authorization: Bearer $API_KEY"

# Copy object
curl -X PUT http://localhost:8000/s3/my-bucket/copy.txt \
  -H "Authorization: Bearer $API_KEY" \
  -H "x-amz-copy-source: my-bucket/file.txt"

# List objects (ListObjectsV2)
curl "http://localhost:8000/s3/my-bucket?list-type=2&prefix=docs/" \
  -H "Authorization: Bearer $API_KEY"

# Delete object
curl -X DELETE http://localhost:8000/s3/my-bucket/file.txt -H "Authorization: Bearer $API_KEY"
```

### Stats

```bash
# Get storage stats (requires admin)
curl http://localhost:8000/v1/stats -H "Authorization: Bearer $ADMIN_KEY"
```

### Dashboard

Access the built-in admin dashboard at `http://localhost:8000/dashboard`. Enter your admin API key to view buckets, objects, API keys, and storage stats.

### Image Transform Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `w` | Width in pixels | `?w=300` |
| `h` | Height in pixels | `?h=200` |
| `format` | Output format (jpeg, png, webp, avif, gif) | `?format=webp` |
| `quality` | Compression quality (1-100) | `?quality=80` |
| `fit` | Resize mode (cover, contain, fill) | `?fit=contain` |

### Versioning

```bash
# Enable versioning on a bucket (admin)
curl -X POST http://localhost:8000/v1/admin/versioning/photos \
  -H "Authorization: Bearer $ADMIN_KEY"

# Every subsequent PUT creates a new version. List them (newest first):
curl http://localhost:8000/v1/versions/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY"

# Fetch a specific historical version
curl "http://localhost:8000/v1/objects/photos/vacation/beach.jpg?version_id=<id>" \
  -H "Authorization: Bearer $API_KEY"

# Remove a single version (newest remaining becomes is_latest)
curl -X DELETE "http://localhost:8000/v1/objects/photos/vacation/beach.jpg?version_id=<id>" \
  -H "Authorization: Bearer $API_KEY"

# Purge every version and the live object in one call
curl -X DELETE http://localhost:8000/v1/versions/photos/vacation/beach.jpg \
  -H "Authorization: Bearer $API_KEY"
```

### Object lock (retention + legal hold)

```bash
# Lock an object for 30 days, also set a legal hold
curl -X PUT http://localhost:8000/v1/admin/lock/photos/contract.pdf \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"retain_until\": \"$(date -u -d '+30 days' +%FT%TZ)\", \"legal_hold\": true}"

# DELETE now returns 409 while the lock is active
curl -X DELETE http://localhost:8000/v1/objects/photos/contract.pdf \
  -H "Authorization: Bearer $API_KEY"
# → HTTP 409 {"error":"object is locked","reason":"legal hold is in effect"}

# Legal hold can be released; retention cannot be shortened while active
curl -X DELETE http://localhost:8000/v1/admin/lock/photos/contract.pdf \
  -H "Authorization: Bearer $ADMIN_KEY"
```

### Lifecycle rules

```bash
# Expire everything under tmp/ after 7 days
curl -X POST http://localhost:8000/v1/admin/lifecycle/photos \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"prefix": "tmp/", "expire_days": 7}'

# Run the sweep now (instead of waiting for the schedule)
curl -X POST http://localhost:8000/v1/admin/lifecycle/run \
  -H "Authorization: Bearer $ADMIN_KEY"
```

### Migrate from S3 / MinIO

```bash
# Stream an entire bucket from any S3-compatible source (uses AWS SigV4)
vaultfsctl migrate s3 \
  --source-endpoint https://s3.amazonaws.com \
  --source-bucket old-photos \
  --source-access-key AKID... \
  --source-secret-key SECRET... \
  --dest-bucket photos
```

### S3-compatible API with SigV4

The `/s3/*` routes verify `AWS4-HMAC-SHA256` signatures (not just the access key) — tampered URLs and mutated signatures are rejected. Point any S3 SDK at the VaultFS endpoint and use a VaultFS API key as both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. A `Bearer` header is also accepted as a convenience shortcut.

## Configuration

```toml
[server]
bind = "0.0.0.0:8000"

[storage]
data_dir = "./data"
max_file_size = "5GB"
deduplication = true

[cache]
memory_size = "256MB"
disk_size = "2GB"

[images]
default_quality = 85
max_transform_size = "50MB"

[auth]
enabled = true

# Optional: encrypt blobs at rest. master_key is 64 hex chars (32 bytes).
# Prefer VAULTFS_SSE_MASTER_KEY env var in production so it stays out of
# the config file.
[sse]
enabled = false
master_key = ""
```

## Docker

```bash
# Build and run
docker compose up -d

# Or standalone
docker build -t vaultfs .
docker run -p 8000:8000 -v vaultfs-data:/data vaultfs
```

## Architecture

```
vaultfs/
├── crates/
│   ├── vaultfs-server/       # HTTP server (axum) + middleware + routes
│   ├── vaultfs-storage/      # Storage engine, SQLite metadata, SSE
│   ├── vaultfs-processing/   # Image transformation
│   ├── vaultfs-cache/        # Multi-level LRU cache (memory + disk)
│   ├── vaultfs-auth/         # API keys, permissions, presigned URLs
│   ├── vaultfs-s3-compat/    # S3-compatible API with SigV4
│   ├── vaultfs-cli/          # vaultfsctl admin CLI
│   └── vaultfs-tests/        # End-to-end integration tests
├── sdks/
│   ├── js/                   # TypeScript / JavaScript SDK
│   ├── python/               # Python SDK (httpx)
│   └── go/                   # Go SDK (net/http)
├── openapi.yaml              # OpenAPI 3.1 spec (served at /openapi.yaml)
├── Dockerfile + docker-compose.yml
└── deploy/helm/              # Kubernetes deployment
```

## Performance

Current single-core numbers on an Intel i5-10300H (no SHA-NI):

- AES-256-GCM (SSE encrypt/decrypt): **~1.25 GB/s** at 64 KB and above
- SHA-256 hashing: **~240 MB/s** (hardware-accelerated on AMD / Ice-Lake+)
- SigV4 verification: **~100k req/s per core**

Full methodology and per-size numbers in [docs/benchmarks.md](docs/benchmarks.md).
Reproducible with `cargo bench -p vaultfs-storage` and
`cargo bench -p vaultfs-s3-compat`.

## License

MIT
