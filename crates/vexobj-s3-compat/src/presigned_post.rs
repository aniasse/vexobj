//! Browser-style S3 presigned-POST uploads.
//!
//! The client submits a `multipart/form-data` POST to `/s3/<bucket>`.
//! Auth is embedded in the form fields — no Authorization header — so this
//! handler runs *before* the normal SigV4 path in `routes.rs::s3_bucket`.
//!
//! Fields expected (names match the AWS spec exactly):
//!
//!   key                 — target object key. Literal, or constrained by
//!                         the policy via `{"key": "..."}` /
//!                         `["starts-with", "$key", "prefix/"]`.
//!   policy              — base64-encoded JSON policy document.
//!   x-amz-algorithm     — must be `AWS4-HMAC-SHA256`.
//!   x-amz-credential    — `<access-key-id>/<date>/<region>/s3/aws4_request`.
//!   x-amz-date          — `yyyyMMdd'T'HHmmss'Z'`.
//!   x-amz-signature     — hex HMAC-SHA256 of the base64 policy, using the
//!                         SigV4 signing key derived from the access key's
//!                         secret + the scope in x-amz-credential.
//!   file                — the object bytes. MUST be the last field (S3
//!                         parsers stop after this field).
//!
//! Conditions we enforce:
//!   - `expiration`          (ISO-8601 timestamp, must be in the future)
//!   - `{"bucket": …}`       (must match the URL bucket)
//!   - `{"key": …}`          (literal match)
//!   - `["eq", "$key", …]`   (literal match)
//!   - `["starts-with", "$key", …]`  (prefix match)
//!   - `["content-length-range", min, max]`
//!
//! Other condition shapes are accepted but not validated — we log a trace
//! and proceed. That's safe for ACL-style fields we don't expose anyway.

use axum::body::Body;
use axum::extract::Multipart;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::error::S3Error;
use crate::routes::S3State;

type HmacSha256 = Hmac<Sha256>;

const MAX_POLICY_SIZE: usize = 64 * 1024;

/// Entry point from `routes::s3_bucket` when a POST with `multipart/form-data`
/// arrives. Drives the whole presigned-POST flow and returns the S3 response.
pub async fn handle_presigned_post(
    state: &S3State,
    bucket: &str,
    headers: HeaderMap,
    body: Body,
) -> Response {
    // Rebuild a Request so axum's Multipart extractor has what it needs.
    // The original request arrived at `s3_bucket` as method + headers + Body;
    // Multipart::from_request wants an http::Request.
    let mut req_builder = Request::builder().method("POST").uri("/");
    for (k, v) in headers.iter() {
        req_builder = req_builder.header(k, v);
    }
    let req = match req_builder.body(body) {
        Ok(r) => r,
        Err(_) => return S3Error::invalid_request("malformed request").into_response(),
    };

    let multipart = match Multipart::from_request(req, &()).await {
        Ok(m) => m,
        Err(_) => return S3Error::invalid_request("malformed multipart body").into_response(),
    };

    match process(state, bucket, multipart).await {
        Ok(key) => (
            StatusCode::NO_CONTENT,
            [("location", format!("/{bucket}/{key}"))],
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// Consume the multipart stream, validate the policy, stream the file field
/// through the storage engine, and return the created key. Errors short-
/// circuit into an S3Error the caller maps to XML.
async fn process(
    state: &S3State,
    bucket: &str,
    mut mp: Multipart,
) -> Result<String, S3Error> {
    // Gather every field EXCEPT `file` into memory — they're tiny. When we
    // hit `file`, we stop gathering and stream the body to disk through
    // the engine's put_object_stream path.
    let mut fields: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    loop {
        let field = mp
            .next_field()
            .await
            .map_err(|e| S3Error::invalid_request(&format!("multipart parse: {e}")))?;
        let Some(field) = field else {
            return Err(S3Error::invalid_request(
                "multipart body ended before `file` field",
            ));
        };
        let name = field.name().unwrap_or_default().to_lowercase();

        if name == "file" {
            // Validate everything we gathered so far, then stream.
            let policy = parse_and_verify_policy(state, bucket, &fields)?;
            let key = resolve_key(&fields, &policy)?;

            // Stream the file field into the engine. Returning a Stream of
            // Bytes straight from the field lets put_object_stream write
            // to disk without buffering the whole file.
            let stream = field
                .into_stream()
                .map_err(|e| std::io::Error::other(e.to_string()));

            match state
                .storage
                .put_object_stream(
                    bucket,
                    &key,
                    stream,
                    fields.get("content-type").map(|s| s.as_str()),
                    None,
                )
                .await
            {
                Ok(meta) => {
                    // Post-write: enforce content-length-range now that we
                    // know the real size. If it's outside the allowed
                    // window we delete the object and surface InvalidRequest.
                    if let Some((min, max)) = policy.content_length_range {
                        if meta.size < min || meta.size > max {
                            let _ = state.storage.delete_object(bucket, &key).await;
                            return Err(S3Error::invalid_request(&format!(
                                "file size {} outside policy range [{min}, {max}]",
                                meta.size
                            )));
                        }
                    }
                    return Ok(meta.key);
                }
                Err(vexobj_storage::StorageError::BucketNotFound(_)) => {
                    return Err(S3Error::no_such_bucket(bucket));
                }
                Err(vexobj_storage::StorageError::ObjectTooLarge { .. }) => {
                    return Err(S3Error::entity_too_large());
                }
                Err(vexobj_storage::StorageError::QuotaExceeded { reason, .. }) => {
                    return Err(S3Error::quota_exceeded(&reason));
                }
                Err(e) => return Err(S3Error::internal(&e.to_string())),
            }
        } else {
            // Any non-file field: buffer up to a reasonable cap.
            let bytes = field
                .bytes()
                .await
                .map_err(|e| S3Error::invalid_request(&format!("multipart: {e}")))?;
            if bytes.len() > MAX_POLICY_SIZE {
                return Err(S3Error::invalid_request(&format!(
                    "field `{name}` exceeds {MAX_POLICY_SIZE} bytes"
                )));
            }
            let value = String::from_utf8(bytes.to_vec()).map_err(|_| {
                S3Error::invalid_request(&format!("field `{name}` is not valid UTF-8"))
            })?;
            fields.insert(name, value);
        }
    }
}

/// Cleaned-up view of the policy JSON, with only the conditions we actually
/// validate preserved.
struct PolicyValidated {
    content_length_range: Option<(u64, u64)>,
    key_rule: KeyRule,
}

enum KeyRule {
    /// `{"key": "literal"}` or `["eq", "$key", "literal"]`.
    Exact(String),
    /// `["starts-with", "$key", "prefix"]`. Empty prefix = any key.
    StartsWith(String),
}

fn parse_and_verify_policy(
    state: &S3State,
    bucket: &str,
    fields: &std::collections::HashMap<String, String>,
) -> Result<PolicyValidated, S3Error> {
    let policy_b64 = fields
        .get("policy")
        .ok_or_else(|| S3Error::invalid_request("missing policy field"))?;
    let algorithm = fields
        .get("x-amz-algorithm")
        .ok_or_else(|| S3Error::invalid_request("missing x-amz-algorithm"))?;
    let credential = fields
        .get("x-amz-credential")
        .ok_or_else(|| S3Error::invalid_request("missing x-amz-credential"))?;
    let amz_date = fields
        .get("x-amz-date")
        .ok_or_else(|| S3Error::invalid_request("missing x-amz-date"))?;
    let signature = fields
        .get("x-amz-signature")
        .ok_or_else(|| S3Error::invalid_request("missing x-amz-signature"))?;

    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(S3Error::invalid_request("unsupported algorithm"));
    }

    // Parse the credential: AKID/yyyyMMdd/region/service/aws4_request
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts[4] != "aws4_request" {
        return Err(S3Error::invalid_request("malformed x-amz-credential"));
    }
    let access_key = parts[0];
    let date = parts[1];
    let region = parts[2];
    let service = parts[3];

    // Look up the API key's plaintext secret.
    let (_api_key, secret) = state
        .auth
        .find_by_access_key(access_key)
        .map_err(|_| S3Error::access_denied())?;
    if secret.is_empty() {
        return Err(S3Error::access_denied());
    }

    // Recompute the HMAC the same way the client did: signing-key(secret,
    // date, region, service) then HMAC-SHA256 over the base64 policy.
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let expected = hex::encode(hmac(&k_signing, policy_b64.as_bytes()));
    if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
        return Err(S3Error::access_denied());
    }
    // The x-amz-date field MUST match the scope date so clients can't reuse
    // a policy with an out-of-scope date.
    if amz_date.len() < 8 || &amz_date[..8] != date {
        return Err(S3Error::access_denied());
    }

    // Decode and parse the policy JSON.
    let raw = base64::engine::general_purpose::STANDARD
        .decode(policy_b64.as_bytes())
        .map_err(|_| S3Error::invalid_request("policy is not valid base64"))?;
    if raw.len() > MAX_POLICY_SIZE {
        return Err(S3Error::invalid_request("policy too large"));
    }
    let json: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|_| S3Error::invalid_request("policy JSON parse"))?;

    // Expiration: ISO-8601, must be in the future.
    let expiration = json
        .get("expiration")
        .and_then(|v| v.as_str())
        .ok_or_else(|| S3Error::invalid_request("policy missing expiration"))?;
    let expires: chrono::DateTime<chrono::Utc> = expiration
        .parse()
        .map_err(|_| S3Error::invalid_request("expiration not ISO-8601"))?;
    if expires <= chrono::Utc::now() {
        return Err(S3Error::access_denied());
    }

    let conditions = json
        .get("conditions")
        .and_then(|v| v.as_array())
        .ok_or_else(|| S3Error::invalid_request("policy.conditions missing"))?;

    let mut bucket_ok = false;
    let mut key_rule = KeyRule::StartsWith(String::new());
    let mut content_length_range: Option<(u64, u64)> = None;

    for cond in conditions {
        match cond {
            // `{"name": "value"}` — exact-match on a single field.
            serde_json::Value::Object(obj) if obj.len() == 1 => {
                let (k, v) = obj.iter().next().unwrap();
                let Some(v) = v.as_str() else { continue };
                match k.as_str() {
                    "bucket" => {
                        if v != bucket {
                            return Err(S3Error::access_denied());
                        }
                        bucket_ok = true;
                    }
                    "key" => key_rule = KeyRule::Exact(v.to_string()),
                    _ => {
                        // Other fields (acl, content-type, success_action_*)
                        // are accepted but not checked. Clients that depend
                        // on them for security shouldn't — they're just
                        // form-field filters.
                    }
                }
            }
            serde_json::Value::Array(arr) if arr.len() == 3 => {
                // Policy array forms:
                //   ["eq",               "$key", "literal"]
                //   ["starts-with",      "$key", "prefix"]
                //   ["content-length-range", <min>, <max>]   (numbers, not strings)
                let op = arr[0].as_str().unwrap_or("");
                match op {
                    "content-length-range" => {
                        let min = arr[1].as_u64().unwrap_or(0);
                        let max = arr[2].as_u64().unwrap_or(u64::MAX);
                        content_length_range = Some((min, max));
                    }
                    "eq" | "starts-with" => {
                        let field = arr[1].as_str().unwrap_or("");
                        let value = arr[2].as_str().unwrap_or("").to_string();
                        match (op, field) {
                            ("eq", "$key") => key_rule = KeyRule::Exact(value),
                            ("starts-with", "$key") => key_rule = KeyRule::StartsWith(value),
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    if !bucket_ok {
        return Err(S3Error::access_denied());
    }
    let _ = (region, service); // currently only validated as part of the scope string

    Ok(PolicyValidated {
        content_length_range,
        key_rule,
    })
}

fn resolve_key(
    fields: &std::collections::HashMap<String, String>,
    policy: &PolicyValidated,
) -> Result<String, S3Error> {
    let key = fields
        .get("key")
        .cloned()
        .ok_or_else(|| S3Error::invalid_request("missing key field"))?;
    if key.is_empty() {
        return Err(S3Error::invalid_request("empty key field"));
    }
    // S3 allows `${filename}` as a placeholder expanded to the uploaded
    // file's name. We don't expand it — the client has to provide a
    // concrete key. Most browser flows do so already; Mastodon certainly
    // does.
    if key.contains("${filename}") {
        return Err(S3Error::invalid_request(
            "${filename} placeholder is not supported",
        ));
    }

    match &policy.key_rule {
        KeyRule::Exact(expected) => {
            if &key != expected {
                return Err(S3Error::access_denied());
            }
        }
        KeyRule::StartsWith(prefix) => {
            if !key.starts_with(prefix) {
                return Err(S3Error::access_denied());
            }
        }
    }
    Ok(key)
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// The extractor on axum::extract::Multipart needs `FromRequest`, which is
// in scope via this re-import.
use axum::extract::FromRequest;
