# Changelog

All notable changes to VaultFS are documented here.

## [0.1.0] - 2026-04-16

### Core Storage
- Universal object storage for any file type
- Content-addressable deduplication (SHA256)
- Streaming upload/download with constant RAM usage
- Virtual directory listing with prefix/delimiter support
- SQLite metadata database (WAL mode)

### Image Processing
- On-the-fly resize, crop, convert via URL query parameters
- Output formats: JPEG, PNG, WebP, AVIF, GIF
- Auto format negotiation from Accept header (AVIF > WebP)
- Multi-level cache (memory LRU + disk) for transformed images

### Authentication & Authorization
- API key authentication with Bearer tokens
- Per-key permissions: read, write, delete, admin
- Per-key bucket access control (all or specific buckets)
- Auto-bootstrap: admin key generated on first startup
- Presigned URLs with HMAC-SHA256 signatures

### S3-Compatible API
- Mounted at /s3/ prefix
- ListBuckets, CreateBucket, DeleteBucket, HeadBucket
- PutObject, GetObject, HeadObject, DeleteObject
- CopyObject via x-amz-copy-source header
- ListObjectsV2 with prefix/delimiter
- XML responses matching AWS S3 format

### Admin & Operations
- Dashboard: embedded web UI at /dashboard
- Stats endpoint: GET /v1/stats
- Garbage collection: POST /v1/admin/gc
- Backup/restore: POST /v1/admin/backup
- Bucket export: POST /v1/admin/backup/export/{bucket}
- Audit log: all write operations logged to SQLite
- Audit query: GET /v1/admin/audit with pagination

### Observability
- Prometheus metrics at /metrics
- Request counters by method/status
- Upload/download byte counters
- Request duration histogram
- Structured tracing with env-filter

### Security
- Path traversal protection
- Bucket name validation
- Security headers (HSTS, CSP, X-Frame-Options, etc.)
- Rate limiting per API key (configurable)
- TLS support via rustls

### Infrastructure
- Single 14MB binary, no external dependencies
- Docker + docker-compose
- Helm chart for Kubernetes
- CI/CD: GitHub Actions (check, test, fmt, clippy, release)
- Cross-platform release builds (Linux amd64, macOS arm64)

### Configuration
- TOML config file
- Environment variable overrides (VAULTFS_* prefix)
- Multitenancy quotas (max storage, max objects per bucket)
- Webhook notifications with HMAC signatures

### SDKs
- JavaScript/TypeScript (zero-dependency, native fetch)
- Python (httpx-based, typed dataclasses)
- Go (zero-dependency, standard net/http)

### CLI
- vaultfsctl: full admin CLI tool
- Bucket, object, key management
- Stats, GC, backup, export commands
- Environment variable support (VAULTFS_URL, VAULTFS_KEY)

### Documentation
- OpenAPI 3.1 spec served at /openapi.yaml
- Swagger UI at /docs
- Comprehensive README with API examples
