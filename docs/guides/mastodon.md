# VexObj as Mastodon media storage

This guide shows how to use a single VexObj binary as the media backend for a
Mastodon instance — replacing both the local filesystem and a MinIO+Cloudinary
or S3+ImgProxy pair. Mastodon talks to VexObj through its S3-compatible API,
public media is served directly to browsers, and image resizes are generated
on the fly by VexObj instead of Paperclip.

## Why this combination

- **One process to run.** Mastodon's Ruby side already handles database, Redis,
  Sidekiq — removing a separate MinIO daemon and a separate image-resize
  service is genuinely less to operate.
- **Smaller disk usage.** VexObj's content-addressable dedup collapses repeat
  avatars, headers, and re-uploaded images. A modest instance can see
  10–20% savings without changing anything.
- **Media stays owned.** Unlike Cloudinary, every byte and every transform
  lives on your machine. GDPR compliance, right-to-deletion, and audit logs
  don't involve a third party.

## Prerequisites

- A reachable VexObj server (single binary, see [the root README](../../README.md)
  for install).
- A Mastodon instance at v4.x. These variables match `docs/production-guide.md`
  from the Mastodon project.
- A hostname for media — usually a subdomain like `files.example.social`
  pointing at VexObj (either directly or through a CDN). This goes into
  `S3_ALIAS_HOST` so Mastodon emits URLs browsers can resolve.

## 1. Create the bucket and an API key

```bash
export VEXOBJ=https://vexobj.internal.example.social
export ADMIN=vex_your_admin_key_from_first_boot

# Public bucket — Mastodon serves avatars/headers directly to browsers.
curl -s -X POST "$VEXOBJ/v1/buckets" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{"name":"mastodon-media","public":true}'

# A scoped write key for the Mastodon process — no admin privilege, no
# access to other buckets.
curl -s -X POST "$VEXOBJ/v1/admin/keys" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{
    "name":"mastodon",
    "permissions":{"read":true,"write":true,"delete":true,"admin":false},
    "bucket_access":{"type":"specific","buckets":["mastodon-media"]}
  }'
# → {"secret":"vex_...", ...}  ← copy the secret, it is only shown once
```

The `public: true` flag is what makes browsers able to GET `/avatars/…`
without an `Authorization` header. Writes, listing, and anything touching
another bucket still require the API key. See
[public-bucket semantics](#public-bucket-semantics) below.

## 2. Mastodon `.env.production`

```env
# --- S3 storage --------------------------------------------------------
S3_ENABLED=true
S3_BUCKET=mastodon-media
S3_REGION=us-east-1

# VexObj endpoint. Use an internal hostname if the Mastodon Ruby process
# and VexObj live on the same VPS or LAN.
S3_ENDPOINT=https://vexobj.internal.example.social
S3_HOSTNAME=vexobj.internal.example.social
S3_PROTOCOL=https

# Use the same API-key secret for both fields — VexObj verifies SigV4
# against the plaintext key.
AWS_ACCESS_KEY_ID=vex_scoped_key_from_step_1
AWS_SECRET_ACCESS_KEY=vex_scoped_key_from_step_1

# Path-style addressing — VexObj doesn't (yet) do virtual-hosted buckets.
S3_FORCE_SINGLE_REQUEST=true
S3_PERMISSION=public-read

# Public-facing hostname for media. Browsers hit this directly; Mastodon
# embeds it into the URLs it serves in timelines.
S3_ALIAS_HOST=files.example.social
```

Then make `files.example.social` reach VexObj's `/s3/mastodon-media/…`
path. The simplest layout is an nginx/Caddy in front of VexObj stripping
the `/s3/mastodon-media` prefix — a minimal Caddy example:

```caddyfile
files.example.social {
    reverse_proxy /s3/mastodon-media/* vexobj.internal.example.social
    rewrite * /s3/mastodon-media{uri}
}
```

Restart Mastodon (`systemctl restart mastodon-web mastodon-sidekiq`) and
upload a new avatar. The object shows up as
`GET /s3/mastodon-media/accounts/…` in VexObj's logs.

## 3. On-the-fly image transforms (optional but recommended)

Mastodon resizes once at upload time. With VexObj you can skip that step
and pass a raw image through, then request variants at read time:

```
<img srcset="
  https://files.example.social/avatars/123/original/avatar.png?w=48&format=auto 48w,
  https://files.example.social/avatars/123/original/avatar.png?w=96&format=auto 96w,
  https://files.example.social/avatars/123/original/avatar.png?w=200&format=auto 200w
">
```

`format=auto` honors the browser's `Accept` header — Chrome gets AVIF,
Safari gets WebP, old IE gets JPEG — and every variant is cached so
repeat loads never re-encode. The original upload stays untouched.

If you want Mastodon itself to stop pre-resizing, set
`MASTODON_MEDIA_PROCESSING_DISABLED=1` (community patch; upstream still
requires small thumbnails to be generated). For a stock Mastodon, leave
that flag alone and just use the VexObj URLs in your theme/front-end.

## 4. Quotas and lifecycle rules

Cap the bucket and auto-expire old media:

```bash
# Hard cap on `mastodon-media` — set to a value below your filesystem
# free space. In values.yaml / TOML: quotas.default_max_storage.
# At runtime, set VEXOBJ_QUOTAS_ENABLED=true and
# VEXOBJ_QUOTAS_MAX_STORAGE=200GB.

# Expire everything under cache/ after 30 days.
curl -X POST "$VEXOBJ/v1/admin/lifecycle/mastodon-media" \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{"prefix":"cache/","expire_days":30}'
```

## 5. Replication to a backup node

Mastodon's media is the first thing you lose in a disk failure. VexObj
can stream every object to a warm standby:

```bash
# On the replica machine:
vexobjctl replicate \
  --primary https://vexobj.internal.example.social \
  --primary-key "$PRIMARY_ADMIN_KEY" \
  --interval 5
```

Objects are applied in order; if the primary dies, `vexobjctl promote`
on the replica clears the cursor so nothing reconnects. See
[../failover.md](../failover.md).

## Public-bucket semantics

When a bucket is created with `public: true`:

- `GET /v1/objects/<bucket>/<key>` and `GET /s3/<bucket>/<key>` — served
  without an API key.
- `HEAD` of the same paths — served without an API key.
- `PUT`, `DELETE`, `POST` — still require a valid API key with the right
  permission.
- `GET /v1/objects/<bucket>` (bucket-level listing) — **still requires
  auth**. The flag unlocks reads *by key*, not the index. Otherwise any
  visitor could enumerate every object.
- `/v1/admin/*`, `/v1/stats`, presigned operations — unchanged.

This matches S3's `public-read` ACL on a bucket: you can fetch an object
if you know its key, but the index is not browseable.

## Known gaps vs. a full S3 migration

- **No `Bucket ACL` API yet.** Public-vs-private is a bucket property
  managed through VexObj's native API (or the admin dashboard), not
  through S3 `PutBucketACL` / `PutObjectACL`. Mastodon doesn't call
  those per-object, so in practice this isn't visible.
- **No virtual-hosted-style addressing.** Requests must be path-style
  (`<endpoint>/<bucket>/<key>`). Mastodon 4.x supports this via
  `S3_FORCE_SINGLE_REQUEST=true`.
- **Versioning off by default.** Mastodon doesn't need it, but it's one
  `POST /v1/admin/versioning/<bucket>` away if you want per-object
  history.
