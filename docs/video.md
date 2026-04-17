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
| Transcoding (MP4 ↔ WebM / variants)   | —              | —              | **not yet**   |

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

## Transcoding — deliberately deferred

Transcoding is the remaining piece, and it's the hardest one to do
right at a small-tool scale. Sketch of what a proper implementation
looks like:

- **Async job queue** — transcodes take seconds to minutes, not the
  ~100 ms budget of a request. We need a workers pool, a job table in
  SQLite, retry semantics, and a client polling endpoint.
- **Storage of variants** — each transcode produces a new blob
  content-addressed by its own sha. Versioning applies per variant,
  and the lifecycle rules need to know how to expire them.
- **Invalidation** — if the source object is replaced (versioning
  on), do we re-transcode? Link the variant to `version_id`?
- **Backpressure** — a naive implementation lets users queue 10 GB
  of work per request. Queue caps and admin quotas are non-optional.
- **Cost accounting** — `/v1/admin/jobs` with status, duration, CPU
  and peak memory per job is what makes this operable at all.

That's a feature release, not a patch. It's on the roadmap under
`vaultfs-transcode` — a separate crate so operators who never need it
don't pay for the code or deps.
