use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Verify AWS Signature V4 Authorization header.
///
/// Header format:
/// AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/s3/aws4_request,
/// SignedHeaders=<headers>, Signature=<signature>
///
/// For VaultFS, we use a simplified verification:
/// - access_key = the VaultFS API key prefix (first 20 chars)
/// - We verify the signature was created with the full API key as the secret
pub fn parse_auth_header(header: &str) -> Option<ParsedAuth> {
    let header = header.strip_prefix("AWS4-HMAC-SHA256 ")?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in header.split(", ") {
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val.to_string());
        }
    }

    let credential = credential?;
    let parts: Vec<&str> = credential.splitn(2, '/').collect();
    let access_key = parts.first()?.to_string();
    let credential_scope = parts.get(1).unwrap_or(&"").to_string();

    Some(ParsedAuth {
        access_key,
        credential_scope,
        signed_headers: signed_headers?,
        signature: signature?,
    })
}

pub struct ParsedAuth {
    pub access_key: String,
    pub credential_scope: String,
    pub signed_headers: String,
    pub signature: String,
}

/// Compute the SHA256 hash of a payload
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// HMAC-SHA256 signing key derivation (AWS v4 style)
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret);
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Create canonical request string
pub fn canonical_request(
    method: &str,
    uri: &str,
    query: &str,
    headers: &[(String, String)],
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k.to_lowercase(), v.trim()))
        .collect();

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        uri_encode_path(uri),
        query,
        canonical_headers,
        signed_headers,
        payload_hash
    )
}

/// Create string to sign
pub fn string_to_sign(datetime: &str, scope: &str, canonical_request_hash: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        datetime, scope, canonical_request_hash
    )
}

/// Sign the string with the derived key
pub fn sign(key: &[u8], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(key, string_to_sign.as_bytes()))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn uri_encode_path(path: &str) -> String {
    // For S3, path segments are individually encoded but / is preserved
    path.split('/')
        .map(|segment| {
            percent_encode(segment)
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn percent_encode(s: &str) -> String {
    let mut result = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}
