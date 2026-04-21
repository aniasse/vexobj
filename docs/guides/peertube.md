# VexObj as PeerTube object storage

PeerTube's object-storage feature offloads raw video files, HLS playlists,
and streaming fragments to an S3-compatible backend. Plugging VexObj in
gives a PeerTube instance exactly what it needs (multipart uploads,
public media serving, content-addressable dedup for identical uploads)
from a single Rust binary, plus one thing most S3-compatibles don't
have: server-side video thumbnails.

## Why this combination

- **Multipart upload actually works.** Video files regularly cross the
  5 GiB single-PUT ceiling; VexObj speaks the full InitiateMultipartUpload
  → UploadPart → CompleteMultipartUpload protocol (see
  [../../openapi.yaml](../../openapi.yaml)), so `aws s3 cp`, `rclone`,
  and PeerTube's own uploader all work unmodified.
- **Thumbnails without a separate service.** Point `?thumbnail=1&t=30`
  at any video and get a JPEG/WebP frame back. Most PeerTube setups
  bolt ffmpeg onto a worker queue to do this; VexObj can do it inline
  with the same ffmpeg binary already on the host.
- **Bandwidth savings from content-addressable dedup.** A reupload of
  the exact same video file only writes once.

## Prerequisites

- VexObj binary running at a reachable hostname. `ffmpeg` + `ffprobe`
  should be on `PATH` so thumbnails and metadata work — check
  `GET /health` for the `video.ffmpeg` / `video.ffprobe` capability
  flags.
- PeerTube v5.x or later (the `object_storage` config section is
  stable as of 5.0).
- A public hostname for streaming (`videos.example.tube`) that maps to
  VexObj.

## 1. Create two buckets

PeerTube splits raw video files from streaming (HLS) output so they can
have different retention. VexObj handles either via the same API.

```bash
export VEXOBJ=https://vexobj.internal.example.tube
export ADMIN=vex_your_admin_key

for b in peertube-videos peertube-streaming; do
  curl -s -X POST "$VEXOBJ/v1/buckets" \
    -H "Authorization: Bearer $ADMIN" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$b\",\"public\":true}"
done

# Scoped write key — no admin, only these two buckets.
curl -s -X POST "$VEXOBJ/v1/admin/keys" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"peertube",
    "permissions":{"read":true,"write":true,"delete":true,"admin":false},
    "bucket_access":{"type":"specific","buckets":["peertube-videos","peertube-streaming"]}
  }'
```

## 2. PeerTube `production.yaml`

```yaml
object_storage:
  enabled: true

  endpoint: 'https://vexobj.internal.example.tube'
  region: 'us-east-1'

  upload_acl:
    public: 'public-read'
    private: 'private'

  credentials:
    access_key_id: 'vex_scoped_peertube_key'
    secret_access_key: 'vex_scoped_peertube_key'

  # PeerTube uploads large files in 100 MiB parts via the S3 multipart
  # protocol. VexObj caps individual parts at `storage.max_file_size`
  # and enforces the S3 5 MiB minimum on non-last parts.
  max_upload_part: 104857600

  videos:
    bucket_name: 'peertube-videos'
    prefix: ''
    base_url: 'https://videos.example.tube/s3/peertube-videos'

  streaming_playlists:
    bucket_name: 'peertube-streaming'
    prefix: ''
    base_url: 'https://videos.example.tube/s3/peertube-streaming'
```

`base_url` is what PeerTube embeds in the page — browsers hit it
directly. Since both buckets are `public: true`, VexObj serves the
objects without an `Authorization` header (see
[mastodon.md](mastodon.md#public-bucket-semantics) for the exact
scope of that bypass).

## 3. Server-side thumbnails

PeerTube generates its own poster frames, but you can also let VexObj
do it on demand. For any object with a `video/*` content-type:

```
GET /v1/objects/peertube-videos/videos/foo.mp4?thumbnail=1&t=5&w=480&format=webp
```

- `t` — seek time in seconds (default 1.0)
- `w` — output width in pixels (default 320, capped at 1920)
- `format` — `jpeg` (default) or `webp`
- `quality` — 1–100 (default 70)

Results are cached by `(sha256, t, w, format, quality)` so repeat
requests hit the LRU. Details in [../video.md](../video.md).

If `ffmpeg` isn't installed, the endpoint returns 501 and the capability
flag at `GET /health` is `false`; nothing silently breaks.

## 4. Capacity planning

Quotas and lifecycle rules compose naturally:

```bash
# Hard cap on raw originals — set to whatever fits your volume.
# Runtime env: VEXOBJ_QUOTAS_ENABLED=true VEXOBJ_QUOTAS_MAX_STORAGE=5TB

# Auto-expire HLS fragments for videos older than 180 days (keeps
# originals intact in the other bucket).
curl -X POST "$VEXOBJ/v1/admin/lifecycle/peertube-streaming" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{"prefix":"hls/","expire_days":180}'
```

Dedup works across both buckets: two identical originals share a single
blob on disk. Enable encryption at rest (`sse.enabled = true`) to store
ciphertext — deterministic AEAD preserves dedup, see
[../../README.md](../../README.md#server-side-encryption-at-rest).

## 5. Two-node setup

PeerTube instances that pair an active node with a replica can mirror
everything through VexObj's replication log:

```bash
# On the replica:
vexobjctl replicate \
  --primary https://vexobj.internal.example.tube \
  --primary-key "$PRIMARY_ADMIN_KEY" \
  --interval 10
```

Ordered apply + atomic cursor — detailed in
[../replication.md](../replication.md) and
[../failover.md](../failover.md).

## Known gaps vs. a full S3 migration

- **No presigned POST uploads.** PeerTube uses PUT-based multipart;
  both work. If you have a client that relies on `POST` with a
  signature policy document, it's not supported yet.
- **No object ACL API.** Public-vs-private is per-bucket, not per-object.
  PeerTube doesn't depend on per-object ACLs in practice.
- **Virtual-hosted addressing absent.** Requests are path-style.
  PeerTube's S3 client uses path-style when `endpoint` is an
  explicit URL, which is the supported mode here.
