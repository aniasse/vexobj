# VaultFS

High-performance, self-hosted object storage with built-in image processing. A single binary alternative to S3 + Cloudinary that runs on a simple VPS.

## Features

- **Universal object storage** — store any file type (images, PDFs, videos, documents, archives)
- **Image processing on the fly** — resize, convert, crop via URL query parameters
- **Auto format negotiation** — serves WebP/AVIF based on browser `Accept` header
- **Content-addressable deduplication** — identical files stored once
- **Multi-level cache** — in-memory LRU + disk cache for transformed images
- **S3-compatible API** — drop-in replacement for AWS S3 (coming soon)
- **API key authentication** — per-key permissions and bucket access control
- **Single binary** — no external dependencies, no Docker required
- **SQLite metadata** — zero-ops database, backup = copy a file

## Quick Start

```bash
# Build from source
cargo build --release

# Run with defaults (listens on :8000, stores in ./data)
./target/release/vaultfs

# Or with a config file
VAULTFS_CONFIG=config.toml ./target/release/vaultfs
```

## API

### Buckets

```bash
# Create a bucket
curl -X POST http://localhost:8000/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name": "photos", "public": false}'

# List buckets
curl http://localhost:8000/v1/buckets
```

### Objects

```bash
# Upload any file
curl -X PUT http://localhost:8000/v1/objects/photos/vacation/beach.jpg \
  -H "Content-Type: image/jpeg" \
  --data-binary @beach.jpg

# Upload a PDF
curl -X PUT http://localhost:8000/v1/objects/docs/report.pdf \
  -H "Content-Type: application/pdf" \
  --data-binary @report.pdf

# Download
curl http://localhost:8000/v1/objects/photos/vacation/beach.jpg -o beach.jpg

# Download with image transformation
curl "http://localhost:8000/v1/objects/photos/vacation/beach.jpg?w=300&h=200&format=webp&quality=80"

# Get metadata
curl -I http://localhost:8000/v1/objects/photos/vacation/beach.jpg

# List objects
curl "http://localhost:8000/v1/objects/photos?prefix=vacation/&delimiter=/"

# Delete
curl -X DELETE http://localhost:8000/v1/objects/photos/vacation/beach.jpg
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
│   ├── vaultfs-server/       # HTTP server (axum)
│   ├── vaultfs-storage/      # Storage engine + SQLite metadata
│   ├── vaultfs-processing/   # Image transformation
│   ├── vaultfs-cache/        # Multi-level LRU cache
│   ├── vaultfs-auth/         # API key management
│   └── vaultfs-s3-compat/    # S3-compatible API layer
```

## License

MIT
