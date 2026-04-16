/// Parse an AWS4-HMAC-SHA256 Authorization header and extract the access key.
///
/// Format: `AWS4-HMAC-SHA256 Credential=<access_key>/<date>/<region>/s3/aws4_request,
/// SignedHeaders=<headers>, Signature=<signature>`
///
/// VaultFS only uses the `access_key` portion: it is treated as a VaultFS API
/// key and looked up directly. Full SigV4 signature verification is not
/// performed.
pub fn parse_auth_header(header: &str) -> Option<ParsedAuth> {
    let header = header.strip_prefix("AWS4-HMAC-SHA256 ")?;

    let credential = header
        .split(", ")
        .find_map(|part| part.strip_prefix("Credential="))?;

    let access_key = credential.split('/').next()?.to_string();
    if access_key.is_empty() {
        return None;
    }
    Some(ParsedAuth { access_key })
}

pub struct ParsedAuth {
    pub access_key: String,
}
