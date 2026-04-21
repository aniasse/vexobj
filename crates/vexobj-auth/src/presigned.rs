use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedUrl {
    pub url: String,
    pub method: String,
    pub bucket: String,
    pub key: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PresignRequest {
    pub method: String,
    pub bucket: String,
    pub key: String,
    pub expires_in: Option<u64>,
    pub content_type: Option<String>,
}

pub struct PresignedUrlGenerator {
    secret: Vec<u8>,
}

impl PresignedUrlGenerator {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            secret: secret.to_vec(),
        }
    }

    pub fn generate(&self, base_url: &str, req: &PresignRequest) -> PresignedUrl {
        let expires_in = req.expires_in.unwrap_or(3600).min(86400);
        let expires_at = Utc::now() + Duration::seconds(expires_in as i64);
        let expires_ts = expires_at.timestamp();

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            req.method.to_uppercase(),
            req.bucket,
            req.key,
            expires_ts,
        );

        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key");
        mac.update(string_to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let url = format!(
            "{}/v1/objects/{}/{}?expires={}&signature={}",
            base_url.trim_end_matches('/'),
            req.bucket,
            req.key,
            expires_ts,
            signature,
        );

        PresignedUrl {
            url,
            method: req.method.to_uppercase(),
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            expires_at,
        }
    }

    pub fn verify(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        expires_ts: i64,
        signature: &str,
    ) -> bool {
        // Check expiry
        let now = Utc::now().timestamp();
        if now > expires_ts {
            return false;
        }

        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            method.to_uppercase(),
            bucket,
            key,
            expires_ts,
        );

        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("HMAC key");
        mac.update(string_to_sign.as_bytes());
        let expected = hex::encode(mac.finalize().into_bytes());

        constant_time_eq(signature.as_bytes(), expected.as_bytes())
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}
