# S3 Compatibility Matrix

This document is the **formal contract** of VexObj's S3-compatible API surface. Each row lists an S3 operation, its support status, the e2e test that validates it, and the fediverse workload that exercises it in production.

All tests referenced below live in `crates/vexobj-tests/tests/e2e_test.rs` and run under `cargo test --all` in CI.

> **Scope note**: VexObj's goal is to be S3-compatible enough to host Mastodon, Pixelfed, and PeerTube without code changes on the client side. Operations outside the fediverse need are not a priority and may be unsupported — that's deliberate.

---

## Authentication

| Auth mode | Status | Test | Notes |
|---|---|---|---|
| `Authorization: AWS4-HMAC-SHA256 ...` (header-signed SigV4) | ✅ | `e2e_s3_compat_sigv4_accepts_valid_and_rejects_tamper` | Full canonical-request re-derivation; constant-time signature compare |
| Query-string presigned URL (`?X-Amz-Signature=...`) | ✅ | `e2e_s3_multipart_with_presigned_put_urls`, `e2e_s3_presigned_put_url_rejects_bad_signature` | GET and PUT; UNSIGNED-PAYLOAD; expiry enforced |
| Presigned POST (browser-upload form) | ✅ | `e2e_s3_presigned_post_*` (10 tests) | Policy + signature in form fields; 10 scenarios covered |
| `Authorization: Bearer <vex_...>` (convenience) | ✅ | `e2e_s3_compat_object_crud` | Not SigV4; for curl/dev; API-key plaintext only |
| Anonymous read of a public bucket | ✅ | `e2e_public_bucket_allows_anonymous_object_reads` | GET + HEAD only; write ops always require auth |

---

## Service

| Operation | HTTP shape | Status | Test |
|---|---|---|---|
| ListBuckets | `GET /` | ✅ | `e2e_s3_compat_sigv4_accepts_valid_and_rejects_tamper` (happy path asserts `<ListAllMyBucketsResult>`) |

---

## Bucket

| Operation | HTTP shape | Status | Test | Notes |
|---|---|---|---|---|
| CreateBucket | `PUT /<bucket>` | ✅ | `e2e_s3_compat_bucket_lifecycle` | Returns `Location`; private by default |
| DeleteBucket | `DELETE /<bucket>` | ✅ | `e2e_s3_compat_bucket_lifecycle` | 409 BucketNotEmpty when objects remain |
| HeadBucket | `HEAD /<bucket>` | ✅ | `e2e_s3_compat_head_bucket` | Mastodon + PeerTube startup probe; 200/404, no 403 |
| ListObjectsV2 | `GET /<bucket>?list-type=2&prefix=…&delimiter=…&max-keys=…&continuation-token=…` | ✅ | `e2e_s3_compat_list_objects_v2` | Full pagination; `CommonPrefixes` emitted on delimiter |
| ListObjects (v1) | `GET /<bucket>` | ⚠️ | — | Falls through to ListObjectsV2 XML shape; no `Marker` |
| DeleteObjects (batch) | `POST /<bucket>?delete` | ✅ | `e2e_s3_compat_delete_objects_batch`, `e2e_s3_compat_delete_objects_quiet_mode` | Up to 1000 keys per request; `<Quiet>true</Quiet>` honored; missing keys succeed (S3 idempotency) |
| GetBucketLocation | `GET /<bucket>?location` | ❌ | — | Always returns `us-east-1` via SigV4 scope; endpoint not implemented |
| PutBucketCors (S3 XML) | `PUT /<bucket>?cors` | ❌ | — | Use the JSON endpoint `PUT /v1/buckets/<name>/cors` instead |

---

## Object

| Operation | HTTP shape | Status | Test | Notes |
|---|---|---|---|---|
| PutObject (single) | `PUT /<bucket>/<key>` | ✅ | `e2e_s3_compat_object_crud` | Body capped at 16 MiB for single-PUT; above that use multipart |
| GetObject | `GET /<bucket>/<key>` | ✅ | `e2e_s3_compat_object_crud` | |
| GetObject with Range | `GET /<bucket>/<key>` + `Range: bytes=X-Y` | ✅ | `e2e_s3_get_honors_range_header` | Single range only (no multi-range); `206` + `Content-Range`; `416` on unsatisfiable; `bytes=N-`, `bytes=-N`, `bytes=N-M` all honored |
| HeadObject | `HEAD /<bucket>/<key>` | ✅ | Covered implicitly by CORS, presigned, and batch-delete tests | Returns `etag` (hex sha256), `content-length`, `last-modified`, `accept-ranges`, optional video metadata |
| DeleteObject | `DELETE /<bucket>/<key>` | ✅ | `e2e_s3_compat_object_crud`, `e2e_s3_compat_delete_objects_batch` | Idempotent — 204 even on missing |
| CopyObject | `PUT /<bucket>/<key>` + `x-amz-copy-source` | ✅ | `e2e_s3_compat_copy_object` | Cross-bucket supported; server-side copy via storage engine (no round-trip bytes) |

---

## Multipart upload

| Operation | HTTP shape | Status | Test |
|---|---|---|---|
| InitiateMultipartUpload | `POST /<bucket>/<key>?uploads` | ✅ | `e2e_s3_multipart_upload_roundtrip` |
| UploadPart | `PUT /<bucket>/<key>?uploadId=…&partNumber=…` | ✅ | `e2e_s3_multipart_upload_roundtrip`, `e2e_s3_multipart_with_presigned_put_urls` |
| CompleteMultipartUpload | `POST /<bucket>/<key>?uploadId=…` | ✅ | `e2e_s3_multipart_upload_roundtrip` |
| AbortMultipartUpload | `DELETE /<bucket>/<key>?uploadId=…` | ✅ | `e2e_s3_multipart_upload_roundtrip` |
| ListParts | `GET /<bucket>/<key>?uploadId=…` | ✅ | `e2e_s3_multipart_upload_roundtrip` |
| ListMultipartUploads | `GET /<bucket>?uploads` | ❌ | — | Not used by Mastodon or PeerTube in the current integrations; adds on request |

**Enforced rules**: non-last parts must be ≥5 MiB (S3 minimum); `CompleteMultipartUpload` validates every claimed ETag against the DB before concatenation; an upload is bound to a single `<bucket>/<key>` — a successful Complete invalidates the `upload_id`.

---

## CORS

Browser uploads require the right CORS response. VexObj supports per-bucket rules via a **native JSON API** (not the S3-XML `PutBucketCors` op, which remains out of scope).

| Feature | Status | Test | Notes |
|---|---|---|---|
| Per-bucket CRUD: `GET/PUT/DELETE /v1/buckets/<name>/cors` | ✅ | `e2e_cors_get_returns_configured_rules` | JSON body of `{ rules: [...] }` |
| Preflight (`OPTIONS`) matching | ✅ | `e2e_cors_preflight_honors_matching_rule` | Echoes `Access-Control-Allow-Origin`, includes `Max-Age`, `Allow-Headers`, `Allow-Methods` |
| Preflight rejection | ✅ | `e2e_cors_preflight_rejects_non_matching_origin`, `_non_matching_method` | 403 with no CORS headers |
| Non-matching actual request | ✅ | `e2e_cors_actual_request_omits_origin_when_no_rule_matches` | Handler runs, CORS header omitted — browser blocks page-side read |
| Default fallback (no rules) | ✅ | `e2e_cors_preflight_permissive_when_no_rules` | Wildcard `*` preserved for zero-config clients |
| Non-S3 paths unaffected | ✅ | `e2e_cors_non_s3_path_is_permissive` | `/v1/...` dashboard + admin stays permissive |

---

## Error codes

| S3 error | HTTP | When |
|---|---|---|
| `NoSuchBucket` | 404 | HeadBucket / any op on missing bucket |
| `NoSuchKey` | 404 | GetObject / HeadObject on missing key |
| `BucketAlreadyExists` | 409 | CreateBucket collision |
| `BucketNotEmpty` | 409 | DeleteBucket on non-empty bucket |
| `AccessDenied` | 403 | Missing / bad SigV4, presigned mismatch, expired policy |
| `InvalidRequest` | 400 | Malformed policy, unsupported algo, `${filename}` placeholder |
| `EntityTooLarge` | 400 | Object exceeds configured max size |
| `QuotaExceeded` | 507 | Bucket quota hit (VexObj-specific; maps from `StorageError::QuotaExceeded`) |
| `NoSuchUpload` | 404 | UploadPart / Complete / Abort with unknown `upload_id` |
| `InvalidPart` | 400 | Complete references a part ETag that doesn't match the server-side record |
| `MalformedXML` | 400 | Complete / DeleteObjects body doesn't parse |

---

## Known non-goals

- **Object ACL (`PUT /<bucket>/<key>?acl`)** — VexObj uses bucket-level `public` + per-bucket CORS. Per-object ACLs aren't on the roadmap.
- **Inventory, Logging, Analytics, Metrics subresources** — out of scope. Use the Prometheus endpoint (`/metrics`).
- **SSE-C / SSE-KMS per-request headers** — VexObj SSE is engine-wide (master-key derivation). Per-request customer-provided keys aren't implemented.
- **Bucket lifecycle via S3 XML** — use the native `/v1/admin/lifecycle` endpoints.
- **Bucket versioning via S3 XML** — use the native `POST /v1/admin/versioning/<bucket>/enable` endpoint.
- **ListMultipartUploads** — not used by supported clients; reopen if a real user needs it.

---

## How this document stays honest

Every row with a ✅ cites an e2e test. If the test is removed or renamed without updating this doc, `cargo test --all` still passes, so changes here should be paired with code review. In practice: when adding or changing an S3 op, update both the test list in this file and the test name reference.

Unsupported operations return HTTP 400 `InvalidRequest` with a clear `<Message>` explaining the gap — no silent 500s for "not implemented".
