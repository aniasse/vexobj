# VexObj

Self-hosted S3-compatible object storage with media processing, server-side encryption, async replication, and a SQLite metadata store — all in a single ~14 MB Rust binary.

→ **Landing page**: https://vortex-soft.github.io/vexobj/
→ **Release**: [v0.1.0](https://github.com/vortex-soft/vexobj/releases/tag/v0.1.0) (Linux amd64, macOS arm64, Docker `ghcr.io/vortex-soft/vexobj:latest`)

### Drop-in object storage for the fediverse

VexObj is designed first for Mastodon / PeerTube / Pixelfed admins who'd rather not run MinIO + Cloudinary side by side. Media, image transforms, video thumbnails, replication, and a quota-aware admin UI all fit in one process.

- **Mastodon** → [docs/guides/mastodon.md](docs/guides/mastodon.md)
- **PeerTube** → [docs/guides/peertube.md](docs/guides/peertube.md)
- **Pixelfed / Friendica / custom** — the patterns in the Mastodon guide apply; S3 config, public buckets, on-the-fly image variants, and content-addressable dedup are the same knobs.

### Why not just MinIO (or SeaweedFS, or Garage)?

Those are excellent if all you need is *pure storage*. For a fediverse instance you also need image resizing, AVIF/WebP negotiation, video thumbnails, and (often) transcoding — and those always live in a second service (ImgProxy / Cloudinary / a custom ffmpeg worker). VexObj rolls them into the same binary, reuses the same SQLite metadata, and caches transformed variants so repeat loads skip the encode entirely. You lose Ceph-style multi-node sharding (VexObj scales vertically and delegates to an S3-compatible blob layer once the local disk is full), but you gain an ops surface orders of magnitude smaller.

## Features

- **Universal object storage** — any file type (images, PDFs, videos, archives…)
- **Image processing on the fly** — resize / crop / convert via URL query parameters
- **Auto format negotiation** — AVIF / WebP served based on the browser `Accept` header
- **Video support** — metadata extraction (pure Rust for MP4/MOV, `ffprobe` for WebM / MKV / AVI), server-side thumbnails via `ffmpeg` when installed ([docs/video.md](docs/video.md))
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

## What it does

VexObj is a self-hosted object storage platform shipped as a single ~14 MB Rust binary. It exposes both a native REST API (`/v1/*`) and an S3-compatible API (`/s3/*`) with real AWS Signature V4 verification, so any standard S3 client — `aws-sdk`, `boto3`, `mc`, `rclone` — talks to it unmodified.

Three things the server does that no single competitor covers in one process:

1. **Object storage** — buckets, versioning, WORM object lock with retention, lifecycle rules, async primary-replica replication, AES-256-GCM encryption at rest, content-addressable deduplication.
2. **Media processing** — on-the-fly image transforms (resize / crop / format conversion with automatic AVIF/WebP/JPEG negotiation), video metadata extraction, thumbnail generation, and a SQLite-backed transcoding job queue (requires `ffmpeg` on the host).
3. **Pluggable blob backend** — blobs live on local disk by default, or delegate to any S3-compatible service (AWS S3, Cloudflare R2, Backblaze B2, Wasabi, MinIO) via a one-line config toggle. Metadata stays in SQLite.

## Why use it

- **vs. managed (S3 + Cloudinary + Mux)** — you own the keys, the data, and the logs. No per-gigabyte, per-transformation, per-second-of-transcoding billing.
- **vs. MinIO / Garage / SeaweedFS** — those are excellent for pure storage but lack media processing. Pairing them with Cloudinary or ImgProxy adds a second service, a second invoice, a second failure point. VexObj keeps it all in one process.
- **vs. Ceph / MinIO cluster** — those target distributed multi-node scale and demand serious ops expertise. VexObj takes the opposite bet: one binary, 4-line config, scale vertically on one machine and then delegate to a cloud blob layer. The operational effort is orders of magnitude lower.
- **vs. rolling your own** — SigV4, WORM, `is_latest` promotion on versioning, content-addressable dedup, replication with atomic cursors — each of those is weeks of work. VexObj ships them tested.

## When to use it

Good fit for:

- **Federated instances (primary target)** — Mastodon, PeerTube, Pixelfed, Matrix, Friendica. Public-bucket reads let browsers fetch media directly, image variants replace a Cloudinary layer, replication gives you a warm standby per instance. See the [Mastodon](docs/guides/mastodon.md) and [PeerTube](docs/guides/peertube.md) guides.
- **Independent media platforms** — podcasts, video, photo galleries. End-to-end ingestion, transformation, and transcoding in one process.
- **B2B / B2C SaaS up to a few million users** — a single binary plus an S3 backend handles the load without custom sharding.
- **Enterprise document backends** — WORM object lock + audit log + immutable retention. Healthcare, finance, legal archival.
- **Dev / staging environments** — mimics an S3 instance locally in seconds, same auth model, same SDK compatibility.

Not a fit for:

- **Consumer-social-network scale** (Meta, TikTok, Snap) — SQLite single-writer metadata tops out around ~100 TB / billions of objects; past that you need a distributed metadata layer (FoundationDB, TiKV), which is a separate roadmap item.
- **Multi-region active-active** with simultaneous writes on both sides — replication is one-way async, no Raft consensus.
- **Automated moderation pipelines** (NSFW / DMCA / copyright matching) — build those layers alongside, not inside.
- **Drop-in replacement for the full AWS ecosystem** (fine-grained IAM, JSON policies, multi-account / SCP) — auth here is intentionally simpler.

## Quick Start

```bash
# Build from source
cargo build --release

# Run with defaults (listens on :8000, stores in ./data)
./target/release/vexobj

# Or with a config file
VEXOBJ_CONFIG=config.toml ./target/release/vexobj
```

On first launch with auth enabled, VexObj generates an admin API key and prints it to the logs. Save this key.

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

VexObj exposes an S3-compatible API under `/s3/`. Use it with any S3 SDK by pointing to your VexObj instance.

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
vexobjctl migrate s3 \
  --source-endpoint https://s3.amazonaws.com \
  --source-bucket old-photos \
  --source-access-key AKID... \
  --source-secret-key SECRET... \
  --dest-bucket photos
```

### S3-compatible API with SigV4

The `/s3/*` routes verify `AWS4-HMAC-SHA256` signatures (not just the access key) — tampered URLs and mutated signatures are rejected. Point any S3 SDK at the VexObj endpoint and use a VexObj API key as both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. A `Bearer` header is also accepted as a convenience shortcut.

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
# Prefer VEXOBJ_SSE_MASTER_KEY env var in production so it stays out of
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
docker build -t vexobj .
docker run -p 8000:8000 -v vexobj-data:/data vexobj
```

## Architecture

```
vexobj/
├── crates/
│   ├── vexobj-server/       # HTTP server (axum) + middleware + routes
│   ├── vexobj-storage/      # Storage engine, SQLite metadata, SSE
│   ├── vexobj-processing/   # Image transformation
│   ├── vexobj-cache/        # Multi-level LRU cache (memory + disk)
│   ├── vexobj-auth/         # API keys, permissions, presigned URLs
│   ├── vexobj-s3-compat/    # S3-compatible API with SigV4
│   ├── vexobj-cli/          # vexobjctl admin CLI
│   └── vexobj-tests/        # End-to-end integration tests
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
Reproducible with `cargo bench -p vexobj-storage` and
`cargo bench -p vexobj-s3-compat`.

## License

MIT
