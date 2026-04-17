# Storage backends

The blob store sits behind a small trait (`BlobStore`) so the engine
can talk to a local filesystem, an S3-compatible service, or anything
else that implements the four basic operations.

## Shipped backends

### `local` — default

Blobs live on disk under `<data_dir>/blobs/<aa>/<bb>/<sha256>`. This
is what VaultFS has always done and remains the default — nothing in
existing deployments has to change.

- **Speed**: sub-millisecond per op on a warm page cache.
- **Limits**: capped by the largest filesystem you can mount.
  Practical ceiling ~100 TB / ~1 billion objects before the SQLite
  metadata layer becomes the bottleneck.
- **Dependencies**: none.

### `s3` — any S3-compatible service

```toml
[storage]
backend = "s3"

[storage.s3]
endpoint   = "https://s3.us-east-1.amazonaws.com"  # or R2 / B2 / MinIO / Wasabi / DO Spaces
bucket     = "vaultfs-blobs"
access_key = "AKID..."
secret_key = "..."
region     = "us-east-1"
path_style = true   # false for AWS-native virtual-hosted style
```

- **Speed**: 20–100 ms per operation (network latency dominates). Use
  a VPC-peered endpoint / S3 Transfer Acceleration / regional R2
  endpoint to minimize.
- **Scale**: effectively unlimited. The blob layer becomes petabyte-
  scalable and durable (99.999999999%) for free. VaultFS metadata
  still lives in SQLite — see `Limitations` below.
- **Dependencies**: none on the server side; any service that speaks
  the AWS Signature V4 S3 protocol works.

## Limitations when `backend = "s3"`

These features need direct filesystem access to the blob data and
aren't compatible with the S3 backend in 0.1.x:

- **SSE-at-rest** — VaultFS encrypts blobs before they hit the local
  disk; with S3 the ciphertext would go to S3 too, which is usually
  not what you want. If your object store offers SSE-S3 / SSE-KMS,
  use that instead.
- **Video thumbnails** — ffmpeg needs a local file path to seek into.
  We'd need to download the full blob to a scratch directory first;
  not wired yet.
- **Video transcoding** — same reason as thumbnails.
- **Image transforms on the fly** — these already cache transformed
  outputs in memory / disk; with S3 the first request pays a
  download cost. Functional but slower than local.

Running the server in this mode logs `backend: s3` on startup and
the affected endpoints return `501 Not Implemented` with an
explanatory message.

## Roadmap

- **In-engine wiring** — the trait and both backends are shipped and
  unit-tested. The next step is refactoring `StorageEngine` to hold
  an `Arc<dyn BlobStore>` and route every filesystem call through it.
- **Multipart upload for S3** — current `put_blob_from_file` reads
  the whole file into memory before PUTing it. Fine up to ~5 GB.
  Multipart lifts the cap and improves throughput for large videos.
- **Automatic backend for ffmpeg ops** — download to scratch on
  demand so thumbnails / transcoding keep working with `backend = "s3"`.
- **Request retries + backoff** — the S3 backend today fails on any
  transient 5xx. A small retry loop with jittered backoff is a ~20
  line add that makes the backend production-grade.
- **Metadata on a distributed store** — SQLite is still a single-
  writer bottleneck. To really scale past one node, swap in
  FoundationDB / TiKV / Postgres. Separate roadmap item because it
  touches every query, not just the blob layer.

## Why not use `aws-sdk-s3`?

The canonical SDK is 40+ transitive crates and ~10 s extra build
time. `S3BlobStore` implements only what the blob layer needs — PUT,
GET, HEAD, DELETE, path-style and virtual-hosted addressing, SigV4
— in ~300 lines. If you need bucket management, lifecycle policies,
or multipart orchestration, point VaultFS at a bucket you created
externally with the SDK (or `aws` CLI / `mc`) and let it own blob
storage only.
