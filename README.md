# VaultFS

High-performance, self-hosted object storage with built-in image processing. A single binary alternative to S3 + Cloudinary that runs on a simple VPS.

## Features

- **Universal object storage** — store any file type (images, PDFs, videos, documents, archives)
- **Image processing on the fly** — resize, convert, crop via URL query parameters
- **Auto format negotiation** — serves WebP based on browser `Accept` header
- **Content-addressable deduplication** — identical files stored once
- **Multi-level cache** — in-memory LRU + disk cache for transformed images
- **API key authentication** — per-key permissions (read/write/delete/admin) and bucket access control
- **Presigned URLs** — temporary signed URLs for secure upload/download
- **Multipart upload** — upload multiple files in a single request
- **Auto bootstrap** — first admin key generated on startup
- **S3-compatible API** — drop-in replacement for AWS S3 (coming soon)
- **Single binary** — no external dependencies, no Docker required
- **SQLite metadata** — zero-ops database, backup = copy a file
- **14 MB binary** — lightweight, fast startup

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

### Image Transform Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `w` | Width in pixels | `?w=300` |
| `h` | Height in pixels | `?h=200` |
| `format` | Output format (jpeg, png, webp, gif) | `?format=webp` |
| `quality` | Compression quality (1-100) | `?quality=80` |
| `fit` | Resize mode (cover, contain, fill) | `?fit=contain` |

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
```

## Architecture

```
vaultfs/
├── crates/
│   ├── vaultfs-server/       # HTTP server (axum) + auth middleware
│   ├── vaultfs-storage/      # Storage engine + SQLite metadata
│   ├── vaultfs-processing/   # Image transformation
│   ├── vaultfs-cache/        # Multi-level LRU cache
│   ├── vaultfs-auth/         # API keys, permissions, presigned URLs
│   └── vaultfs-s3-compat/    # S3-compatible API layer
```

## License

MIT
