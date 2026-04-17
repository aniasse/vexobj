# Video support

VaultFS ships three tiers of video support, layered so you get something
useful even without external tools and get more when you install them.

## Feature matrix

| Feature                               | Without ffmpeg | With `ffprobe` | With `ffmpeg` |
|---------------------------------------|----------------|----------------|---------------|
| Upload and stream any container       | ✅             | ✅             | ✅            |
| Range requests (seek during playback) | ✅             | ✅             | ✅            |
| Versioning / object lock / replication| ✅             | ✅             | ✅            |
| MP4 / MOV metadata (duration, codec, resolution) | ✅ (pure Rust) | ✅          | ✅            |
| WebM / MKV / AVI / MPEG-TS metadata   | —              | ✅             | ✅            |
| Thumbnail generation                  | —              | —              | ✅            |
| Transcoding (profiles → variants)     | —              | —              | ✅            |

Capability flags are served at `GET /health`:

```json
{
  "capabilities": {
    "video_metadata":  true,
    "video_thumbnails": true,
    "ffprobe": true,
    "ffmpeg":  true,
    "sse_at_rest": false
  }
}
```

Clients should inspect this before asking for features that require
`ffmpeg` — the server refuses cleanly (501) rather than timing out.

## Installing ffmpeg

VaultFS looks for `ffmpeg` and `ffprobe` on the host's `PATH` **at
startup**. No config flag, no binding. Restart the server after
installing.

```bash
# Debian / Ubuntu
sudo apt-get install ffmpeg

# macOS
brew install ffmpeg

# Alpine / Docker
apk add --no-cache ffmpeg
```

The official `ghcr.io/aniasse/vaultfs:latest` image ships **without**
ffmpeg to keep the image small (~40 MB). To enable video features in
Docker, extend the image:

```dockerfile
FROM ghcr.io/aniasse/vaultfs:latest
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
  && rm -rf /var/lib/apt/lists/*
USER vaultfs
```

## Thumbnails

`GET /v1/objects/{bucket}/{key}?thumbnail=1&w=<px>&t=<s>&format=<jpeg|webp>&quality=<1..100>`

| Param       | Default | Bounds         |
|-------------|---------|----------------|
| `thumbnail` | —       | `1` / `true`   |
| `w`         | 320     | 32 – 1920      |
| `t`         | 1.0 s   | 0 – 86400      |
| `format`    | jpeg    | `jpeg`, `webp` |
| `quality`   | 70      | 1 – 100        |

Responses are cached by `(sha256, t, w, format, quality)` in the
existing two-level cache, so repeated requests for the same thumbnail
hit RAM / disk before they reach ffmpeg. Each generation is bounded
by a 15 second wall clock — a pathological file can't stall a worker.

### When thumbnails fail

- **`501 Not Implemented`** — ffmpeg isn't on the host's PATH. Install
  it, restart.
- **`422 Unprocessable Entity`** — the file parses, but ffmpeg can't
  seek to the requested timestamp or decode the stream. Response body
  echoes ffmpeg's stderr (truncated).
- **`504 Gateway Timeout`** — ffmpeg exceeded the 15 second budget.
  Typically means the source is corrupt or too large for a one-shot
  seek-decode.

### SSE-at-rest interaction

When server-side encryption is enabled, the bytes on disk are
ciphertext and ffmpeg can't read them. Thumbnail requests return
`501` with a hint. A future version may decrypt to a temp file first
— not worth the perf hit for 0.1.x.

## Metadata probing

The engine probes every `video/*` upload, best-effort:

1. If `ffprobe` is on PATH and SSE is off → `ffprobe -show_streams`,
   parses JSON, extracts duration/width/height/codec/has_audio. Covers
   every container ffmpeg supports.
2. Else if the MIME is MP4-family (`video/mp4`, `video/quicktime`,
   `video/x-m4v`) → pure-Rust `mp4` crate parses the `moov` atom.
3. Else → no metadata stored (object upload still succeeds).

The result is merged into `object.metadata.video`:

```json
{
  "video": {
    "duration_secs": 42.5,
    "width": 1920,
    "height": 1080,
    "codec": "h264",
    "has_audio": true
  }
}
```

Same fields are exposed on `HEAD` as `x-vaultfs-video-*` headers.

## Transcoding

Submissions go into a SQLite-backed queue that a background tokio
worker drains. Variants are stored as first-class vaultfs objects, so
they get versioning, lifecycle, ACLs, and replication for free — the
same way every other object does.

### Built-in profiles

```bash
curl http://localhost:8000/v1/transcode/profiles
```

| Profile      | Output  | Use case                                       |
|--------------|---------|------------------------------------------------|
| `mp4-480p`   | H.264 + AAC, max 480p, faststart flag | Web-ready compatibility        |
| `webm-720p`  | VP9 + Opus, max 720p                  | Modern browsers, smaller files |
| `mp3-audio`  | MP3 192 kbps, video stripped          | Podcasts, voice extraction     |

Custom ffmpeg args aren't exposed in 0.1.x — arbitrary argument
passthrough is a CVE waiting to happen without sandboxing. Add a new
preset in `vaultfs-processing/src/transcode.rs` and rebuild instead.

### Submit a job

```bash
curl -X POST http://localhost:8000/v1/transcode/videos/clip.mp4 \
  -H "Authorization: Bearer $VFS_KEY" \
  -H "Content-Type: application/json" \
  -d '{"profile": "mp4-480p"}'
```

Returns `202 Accepted` with a job row:

```json
{
  "id": "47f1...",
  "status": "pending",
  "bucket": "videos",
  "key": "clip.mp4",
  "profile": "mp4-480p",
  "created_at": "2026-04-17T12:00:00Z"
}
```

### Poll for status

```bash
curl http://localhost:8000/v1/transcode/jobs/47f1... \
  -H "Authorization: Bearer $VFS_KEY"
```

Once `status` is `completed`, the response carries `output_bucket`
and `output_key` — the variant is a normal object at that location,
fetched via the standard `/v1/objects/...` route. Output keys follow
the convention `<source_key>.<profile>.<ext>` so they're
predictable without polling if the source is already known.

### Failure modes

- **`pending` forever** — check `/health` for `"ffmpeg": true`. The
  worker skips startup if ffmpeg isn't on PATH.
- **`failed` with a short error** — the last few lines of ffmpeg's
  stderr. Usually means the source is corrupt or uses a codec the
  host's ffmpeg was built without.
- **Timeout** — each profile has a per-job wall-clock cap (default
  600 s). A one-hour 4K source will hit it on `webm-720p` — tune the
  preset's `timeout_secs` if you routinely transcode long files.

### List recent jobs

```bash
# All states
curl "http://localhost:8000/v1/transcode/jobs?limit=100" \
  -H "Authorization: Bearer $VFS_KEY"

# Just the ones currently processing
curl "http://localhost:8000/v1/transcode/jobs?status=running" \
  -H "Authorization: Bearer $VFS_KEY"
```

### Configuration

```toml
[transcode]
workers       = 2      # concurrent jobs (default 2)
max_pending   = 100    # reject new submissions over this count with 429
gc_after_days = 30     # GC terminal rows older than this; 0 disables
```

Environment-variable overrides:

- `VAULTFS_TRANSCODE_WORKERS`
- `VAULTFS_TRANSCODE_MAX_PENDING`
- `VAULTFS_TRANSCODE_GC_DAYS`

### Backpressure

Submissions that would push `pending` above `max_pending` are
rejected immediately with `429 Too Many Requests` and a `Retry-After:
30` header. Clients should implement exponential backoff. The
underlying `claim_next_transcode_job` DB method is an atomic
transaction so multiple workers never race on the same row.

### Queue hygiene

A background task runs hourly and deletes rows in `completed` or
`failed` state whose `completed_at` is older than `gc_after_days`.
Set it to `0` to disable. Running jobs are never touched.

For on-demand inspection:

```bash
vaultfsctl transcode jobs --status=running --limit=20
vaultfsctl transcode get <job-id>
vaultfsctl transcode profiles
```

### What's still on the roadmap

- **Priority lanes** — admins should be able to jump a job past
  long-running batch work.
- **Per-bucket transcode quotas** — protect against a runaway client
  queueing 10 GB of work per second (the global `max_pending` cap is
  a blunt instrument).
- **Re-transcode on source version bump** — currently variants are
  detached from their source's version_id; a new upload doesn't
  invalidate them.
- **SSE support** — transcoding is disabled when `sse.enabled=true`
  because the on-disk bytes are ciphertext. Fixing this cleanly
  needs a decrypt-to-scratch step with the right accounting.
