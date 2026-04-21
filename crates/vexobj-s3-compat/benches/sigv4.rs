//! Measures how fast the SigV4 verifier can check one request. The
//! full HMAC chain (derive signing key → hash canonical request → HMAC
//! string-to-sign) runs once per S3 call, so its cost sets the upper
//! bound on requests-per-core for the /s3 layer.
//!
//! Run with: `cargo bench -p vexobj-s3-compat`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use vexobj_s3_compat::signature::{parse_auth_header, verify_sigv4, ParsedAuth};

type HmacSha256 = Hmac<Sha256>;

/// Build the same signature the server will later verify so the bench
/// measures the verification path end-to-end, not just failure cases.
fn make_signed_request() -> (String, ParsedAuth, Vec<(String, String)>) {
    let secret = "vex_secretsecretsecretsecret";
    let method = "GET";
    let uri = "/bucket/path/to/some/object.bin";
    // Query with no chars that need canonical percent-encoding, so the
    // bench can hand-build the canonical request without reimplementing
    // canonical_query here.
    let query = "max-keys=1000";
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
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    let canonical_headers: String = headers.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let canonical =
        format!("{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let mut h = Sha256::new();
    h.update(canonical.as_bytes());
    let cr_hash = hex::encode(h.finalize());
    let sts = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{cr_hash}");

    let mac = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut m = HmacSha256::new_from_slice(key).unwrap();
        m.update(data);
        m.finalize().into_bytes().to_vec()
    };
    let k_date = mac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = mac(&k_date, region.as_bytes());
    let k_service = mac(&k_region, service.as_bytes());
    let k_signing = mac(&k_service, b"aws4_request");
    let sig = hex::encode(mac(&k_signing, sts.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential=AKID/{scope}, \
         SignedHeaders={signed_headers}, Signature={sig}"
    );
    let parsed = parse_auth_header(&auth_header).unwrap();
    (secret.to_string(), parsed, headers)
}

fn verify_full_request(c: &mut Criterion) {
    let (secret, parsed, headers) = make_signed_request();
    let method = "GET";
    let uri = "/bucket/path/to/some/object.bin";
    let query = "max-keys=1000";
    let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    c.bench_function("sigv4_verify", |b| {
        b.iter(|| {
            let ok = verify_sigv4(
                black_box(method),
                black_box(uri),
                black_box(query),
                black_box(&headers),
                black_box(payload_hash),
                black_box(&secret),
                black_box(&parsed),
            );
            assert!(ok);
        });
    });
}

fn parse_header_only(c: &mut Criterion) {
    let header = "AWS4-HMAC-SHA256 Credential=AKID/20260416/us-east-1/s3/aws4_request, \
                  SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
                  Signature=abc123deadbeef";
    c.bench_function("sigv4_parse_header", |b| {
        b.iter(|| {
            let _ = black_box(parse_auth_header(black_box(header)));
        });
    });
}

criterion_group!(benches, verify_full_request, parse_header_only);
criterion_main!(benches);
