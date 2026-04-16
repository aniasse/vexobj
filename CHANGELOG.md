# Changelog

All notable changes to VaultFS are documented here. The project stays
on 0.1.x until it has real users — additive features land here rather
than cutting new minor versions.

## [0.1.0] - 2026-04-16

### Core storage
- Universal object storage (any file type)
- Content-addressable deduplication via SHA-256
- Streaming upload/download with constant RAM usage
- Virtual directory listing (prefix + delimiter)
- SQLite metadata store (WAL mode)

### Versioning & retention
- Per-bucket versioning with `is_latest` promotion and delete-markers
- `GET ?version_id=…` to fetch a historical version
- `DELETE ?version_id=…` to remove a specific version (the newest
  remaining version is promoted to `is_latest`)
- `DELETE /v1/versions/{bucket}/{key}` to hard-purge every version and
  the live object; orphan blobs removed when dedup is off
- Object lock: per-object retention timestamp + legal hold; both block
  delete and purge with HTTP 409. Retention cannot be shortened while
  active (WORM)
- Lifecycle rules: expire objects by prefix + age, manual or scheduled

### Image processing
- On-the-fly resize / crop / convert via URL query parameters
- Output formats: JPEG, PNG, WebP, AVIF, GIF
- Automatic format negotiation from the `Accept` header
  (AVIF preferred, WebP fallback)
- Multi-level cache (memory LRU + disk) for transformed images with
  enforced memory and disk caps

### Authentication & authorization
- API key auth via `Authorization: Bearer <vfs_...>` on the native API
- Per-key permissions (read / write / delete / admin)
- Per-key bucket access control (all or specific)
- Presigned URLs with HMAC-SHA256 signatures
- Auto-bootstrap: admin key generated and printed on first startup

### S3-compatible API (`/s3/*`)
- ListBuckets, CreateBucket, DeleteBucket, HeadBucket
- PutObject, GetObject, HeadObject, DeleteObject
- CopyObject via `x-amz-copy-source`
- ListObjectsV2 with prefix + delimiter + continuation-token
- AWS-style XML error / response bodies
- **Full AWS Signature V4 verification** — canonical request rebuilt
  from what the server received, HMACs compared in constant time.
  `AWS4-HMAC-SHA256` Authorization headers are validated; tampered
  URLs and mutated signatures are rejected. `Bearer` is still accepted
  as a convenience shortcut

### Encryption at rest (SSE)
- AES-256-GCM with per-blob keys derived via HKDF from a master key
  and the plaintext SHA-256 — deterministic so content-addressable
  dedup keeps working
- Applies transparently to put/get, streaming upload/download, and
  versioned reads
- Enabled via `[sse] enabled = true` + `master_key` (64 hex chars) in
  config, or `VAULTFS_SSE_ENABLED` / `VAULTFS_SSE_MASTER_KEY`

### Admin & operations
- Embedded dashboard at `/dashboard`
- `GET /v1/stats` — storage and per-bucket stats
- `POST /v1/admin/gc` — orphan blob garbage collection
- `POST /v1/admin/backup` — full backup snapshot
- `POST /v1/admin/backup/export/{bucket}` — per-bucket export
- `POST /v1/admin/versioning/{bucket}` — enable versioning
- `POST|GET|DELETE /v1/admin/lifecycle/{bucket}` — rule management
- `POST /v1/admin/lifecycle/run` — manual sweep
- `GET|PUT|DELETE /v1/admin/lock/{bucket}/{key}` — object lock
- `POST /v1/admin/migrate/s3` — stub that redirects callers to
  `vaultfsctl migrate s3`
- SQLite-backed audit log: every write operation logged with actor,
  action, target, and IP; query via `GET /v1/admin/audit`

### Observability
- Prometheus metrics at `/metrics`
- Request counters by method and status
- Upload / download byte counters
- Request duration histogram
- Structured tracing via `tracing` + env-filter

### Security
- Path-traversal protection — blocks both literal `..` and
  percent-encoded `%2E%2E`, plus `%00` / NUL
- Bucket name validation
- Security headers on every response (HSTS, CSP, X-Frame-Options,
  X-Content-Type-Options, X-XSS-Protection)
- Per-key rate limiting (configurable)
- TLS support via rustls

### Infrastructure
- Single ~14 MB binary, no external dependencies
- `Dockerfile` + `docker-compose.yml`
- Helm chart for Kubernetes
- GitHub Actions: check / test / fmt / clippy / release
- Cross-platform release builds (Linux amd64, macOS arm64)

### Configuration
- TOML config file with `VAULTFS_CONFIG=` override
- Environment variable overrides (`VAULTFS_*` prefix)
- Multitenancy quotas (max storage and max objects per bucket)
- Webhook notifications with HMAC signatures

### SDKs
- **TypeScript / JavaScript** — zero-dependency, native `fetch`
- **Python** — `httpx`-based, typed dataclasses
- **Go** — zero-dependency, standard `net/http`
- All three cover bucket/object CRUD, streaming, presign, admin,
  versioning, object lock, and lifecycle

### Replication
- Primary-replica async replication with a persistent event log
- `GET /v1/replication/{events,cursor,blob/:sha256}` — primary side
- `PUT /v1/replication/blob/:sha256` + `POST /v1/replication/apply` —
  replica-side import with hash verification (non-SSE) and direct-to-DB
  apply that skips the local log to avoid cascade loops
- `vaultfsctl replicate` — one-shot or polling loop with persisted cursor
- `vaultfsctl promote` — replica-to-primary checkpoint + cursor cleanup
- Full runbook at [docs/failover.md](docs/failover.md)

### Monitoring
- Ready-to-import Grafana dashboard at
  `deploy/grafana/vaultfs-dashboard.json` (request rate, 5xx ratio,
  p50/p95/p99, throughput)
- Reference alert rules and scrape config in [docs/monitoring.md](docs/monitoring.md)

### Benchmarks
- Criterion benches for the three CPU hot paths (SHA-256, AES-256-GCM,
  SigV4)
- Published numbers in [docs/benchmarks.md](docs/benchmarks.md)

### CLI (`vaultfsctl`)
- Full admin CLI: bucket / object / key / stats / GC / backup / export
- `vaultfsctl migrate s3` — streams objects from any S3-compatible
  source (AWS S3, MinIO, etc.) into VaultFS with AWS Sigv4 signing
- `vaultfsctl replicate` + `vaultfsctl promote` for multi-node ops
- `VAULTFS_URL` / `VAULTFS_KEY` env-var support

### Documentation
- OpenAPI 3.1 spec served at `/openapi.yaml` — covers every documented
  endpoint including versioning, lock, lifecycle, S3-compat, and SSE
- Swagger UI at `/docs`
- Comprehensive README with runnable examples
