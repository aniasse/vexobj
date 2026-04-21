//! AWS Signature V4 verification for the S3-compat layer.
//!
//! The spec: https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
//!
//! We verify the `Authorization` header, recomputing the canonical request
//! from the actual request the server received. The signing secret is the
//! plaintext vexobj API key (stored on key creation), so the `access_key_id`
//! in the Credential string is expected to be either the full `vex_...` key
//! or its 12-char prefix.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Parsed components of an `Authorization: AWS4-HMAC-SHA256 ...` header.
pub struct ParsedAuth {
    pub access_key: String,
    pub scope: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

/// Parse an AWS4-HMAC-SHA256 Authorization header. Returns None for any
/// malformed input — callers should treat that as AccessDenied.
pub fn parse_auth_header(header: &str) -> Option<ParsedAuth> {
    let header = header.strip_prefix("AWS4-HMAC-SHA256 ")?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in header.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val.to_string());
        }
    }

    let credential = credential?;
    let (access_key, scope) = credential.split_once('/')?;
    let signed_headers: Vec<String> = signed_headers?
        .split(';')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if access_key.is_empty() || signed_headers.is_empty() {
        return None;
    }

    Some(ParsedAuth {
        access_key: access_key.to_string(),
        scope: scope.to_string(),
        signed_headers,
        signature: signature?,
    })
}

/// Verify a SigV4 request against the expected secret. Returns true only when
/// the computed HMAC matches the signature in the Authorization header.
///
/// `headers` contains every header in the original request; we look up only
/// those listed in `signed_headers` (the other headers were not part of what
/// the client signed). `payload_hash` is the value of `x-amz-content-sha256`
/// — `UNSIGNED-PAYLOAD` is honored as-is per the AWS spec.
pub fn verify_sigv4(
    method: &str,
    uri_path: &str,
    query_string: &str,
    headers: &[(String, String)],
    payload_hash: &str,
    secret: &str,
    parsed: &ParsedAuth,
) -> bool {
    // Canonical headers: lowercase name, trimmed value, sorted by name, one
    // per line, terminated by "\n". Only headers listed in SignedHeaders.
    let mut picked: Vec<(String, String)> = Vec::new();
    for want in &parsed.signed_headers {
        // If a signed header is absent from the request, the signature cannot
        // be valid — short-circuit rather than signing an empty value.
        let Some((_, v)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(want))
        else {
            return false;
        };
        picked.push((want.clone(), collapse_whitespace(v.trim())));
    }
    picked.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers: String = picked
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_headers_str = parsed.signed_headers.join(";");

    let canonical_request = format!(
        "{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload}",
        method = method,
        uri = canonical_uri(uri_path),
        query = canonical_query(query_string),
        canonical_headers = canonical_headers,
        signed_headers = signed_headers_str,
        payload = payload_hash,
    );

    let cr_hash = {
        let mut h = Sha256::new();
        h.update(canonical_request.as_bytes());
        hex::encode(h.finalize())
    };

    // x-amz-date is mandatory for SigV4 and was just verified present via the
    // signed-headers loop above; unwrap can't fail here but we stay defensive.
    let amz_date = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("x-amz-date"))
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    if amz_date.is_empty() {
        return false;
    }

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{hash}",
        date = amz_date,
        scope = parsed.scope,
        hash = cr_hash,
    );

    // Derive signing key from the credential scope: AWS4<secret> → date →
    // region → service → aws4_request, each HMAC-SHA256.
    let scope_parts: Vec<&str> = parsed.scope.split('/').collect();
    if scope_parts.len() < 4 {
        return false;
    }
    let date = scope_parts[0];
    let region = scope_parts[1];
    let service = scope_parts[2];

    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    let k_signing = hmac(&k_service, b"aws4_request");
    let expected = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));

    // Constant-time compare to avoid leaking signature bytes via timing.
    constant_time_eq(expected.as_bytes(), parsed.signature.as_bytes())
}

/// URI-encode the path per SigV4 rules: each segment is percent-encoded, but
/// `/` between segments is preserved. Unreserved chars (letters, digits,
/// `-_.~`) are kept verbatim.
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    path.split('/')
        .map(percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// Canonical query string: each `key=value` pair is individually percent-
/// encoded and the pairs are sorted by key, then by value on tie.
fn canonical_query(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (percent_encode(k), percent_encode(v)),
            None => (percent_encode(pair), String::new()),
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Collapse runs of whitespace inside a header value to a single space,
/// matching AWS's canonical-header rule. Leading/trailing whitespace should
/// already be stripped by the caller.
fn collapse_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for c in s.chars() {
        if c.is_ascii_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_auth_header() {
        let h = "AWS4-HMAC-SHA256 Credential=AKID/20260416/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                 Signature=abc123";
        let p = parse_auth_header(h).unwrap();
        assert_eq!(p.access_key, "AKID");
        assert_eq!(p.scope, "20260416/us-east-1/s3/aws4_request");
        assert_eq!(
            p.signed_headers,
            vec!["host", "x-amz-content-sha256", "x-amz-date"]
        );
        assert_eq!(p.signature, "abc123");
    }

    #[test]
    fn rejects_missing_fields() {
        assert!(parse_auth_header("Bearer foo").is_none());
        assert!(parse_auth_header("AWS4-HMAC-SHA256 Credential=k").is_none());
        assert!(parse_auth_header(
            "AWS4-HMAC-SHA256 Credential=k/s, SignedHeaders=, Signature=x"
        )
        .is_none());
    }

    #[test]
    fn canonical_query_sorts_and_encodes() {
        assert_eq!(canonical_query("b=2&a=1"), "a=1&b=2");
        assert_eq!(canonical_query("list-type=2&prefix=d/"), "list-type=2&prefix=d%2F");
        assert_eq!(canonical_query(""), "");
    }

    #[test]
    fn canonical_uri_preserves_slashes() {
        assert_eq!(canonical_uri("/foo/bar"), "/foo/bar");
        assert_eq!(canonical_uri("/foo/b ar"), "/foo/b%20ar");
        assert_eq!(canonical_uri(""), "/");
    }

    /// Sign then verify a minimal request using the same helpers. Any change
    /// to the canonicalization on either side breaks this round-trip.
    #[test]
    fn round_trip_sign_and_verify() {
        let secret = "vex_secretsecret";
        let method = "GET";
        let uri = "/bucket/key.txt";
        let query = "";
        let date = "20260416";
        let region = "us-east-1";
        let service = "s3";
        let amz_date = "20260416T120000Z";
        let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        let headers = vec![
            ("host".to_string(), "localhost:8000".to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.to_string()),
            ("x-amz-date".to_string(), amz_date.to_string()),
        ];
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let signed_headers = vec![
            "host".to_string(),
            "x-amz-content-sha256".to_string(),
            "x-amz-date".to_string(),
        ];

        // Build canonical request + string-to-sign same way the verifier does
        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect();
        let canonical = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri(uri),
            canonical_query(query),
            canonical_headers,
            signed_headers.join(";"),
            payload_hash,
        );
        let cr_hash = {
            let mut h = Sha256::new();
            h.update(canonical.as_bytes());
            hex::encode(h.finalize())
        };
        let sts = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{cr_hash}");

        let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
        let k_region = hmac(&k_date, region.as_bytes());
        let k_service = hmac(&k_region, service.as_bytes());
        let k_signing = hmac(&k_service, b"aws4_request");
        let signature = hex::encode(hmac(&k_signing, sts.as_bytes()));

        let parsed = ParsedAuth {
            access_key: "AKID".into(),
            scope,
            signed_headers,
            signature: signature.clone(),
        };

        assert!(verify_sigv4(
            method, uri, query, &headers, payload_hash, secret, &parsed
        ));

        // A one-byte flip in the signature must cause verification to fail.
        let mut bad = parsed;
        bad.signature = format!("{}x", &signature[..signature.len() - 1]);
        assert!(!verify_sigv4(
            method, uri, query, &headers, payload_hash, secret, &bad
        ));
    }
}
