//! Custom CORS middleware.
//!
//! For `/s3/<bucket>*` requests, per-bucket rules (stored in SQLite, set via
//! `PUT /v1/buckets/<name>/cors`) override the default permissive behavior.
//! When a bucket has no rules, or the request targets a non-S3 path
//! (dashboard, `/v1/...`, `/metrics`, `/healthz`), we fall back to the pre-
//! existing permissive policy so nothing else in the system has to care.
//!
//! The middleware sits at the same position in the layer stack as the
//! `tower_http::cors::CorsLayer` it replaced: outside `auth_middleware` so
//! preflight requests never see a 401 when they lack an Authorization
//! header.
//!
//! Policy semantics follow AWS S3: a request is admitted iff at least one
//! rule admits all of Origin + Method (+ headers for preflight). Matching
//! supports literal strings and `"*"`. No CORS response headers are added
//! when no rule matches — the browser sees the response but refuses to
//! expose it to the page, which is the standard way to deny.

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::state::AppState;
use vexobj_storage::CorsRule;

/// Peel the bucket name out of an `/s3/<bucket>[/...]` path. Returns `None`
/// for `/s3` / `/s3/` (service-level) and anything outside the /s3 prefix.
fn extract_s3_bucket(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/s3/")?;
    let bucket = rest.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

/// First rule that admits `(origin, method, headers?)`. `requested_headers` is
/// `Some` for preflight (comma-separated Access-Control-Request-Headers) and
/// `None` for non-preflight — actual requests can't advertise headers, so
/// header matching is only meaningful at preflight time.
fn find_match<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    requested_headers: Option<&[&str]>,
) -> Option<&'a CorsRule> {
    rules.iter().find(|r| {
        r.matches_origin(origin)
            && r.matches_method(method)
            && requested_headers.is_none_or(|hs| r.matches_headers(hs))
    })
}

pub async fn cors_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let bucket_rules = extract_s3_bucket(&path)
        .map(|b| state.storage.get_bucket_cors(b))
        .unwrap_or_default();
    let has_rules = !bucket_rules.is_empty();

    // ---------------------------------------------------------------
    // Preflight
    // ---------------------------------------------------------------
    if method == Method::OPTIONS && origin.is_some() {
        let origin = origin.as_deref().unwrap();
        let req_method = req
            .headers()
            .get("access-control-request-method")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("GET")
            .to_string();
        let req_headers_raw = req
            .headers()
            .get("access-control-request-headers")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let req_headers: Vec<&str> = req_headers_raw
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if has_rules {
            if let Some(rule) = find_match(&bucket_rules, origin, &req_method, Some(&req_headers)) {
                return preflight_from_rule(rule, origin);
            }
            return StatusCode::FORBIDDEN.into_response();
        }
        return permissive_preflight();
    }

    // ---------------------------------------------------------------
    // Actual request — handler runs, we decorate the response
    // ---------------------------------------------------------------
    let mut resp = next.run(req).await;
    let Some(origin) = origin else { return resp };
    let hdrs = resp.headers_mut();

    if has_rules {
        if let Some(rule) = find_match(&bucket_rules, &origin, method.as_str(), None) {
            if let Ok(v) = HeaderValue::from_str(&origin) {
                hdrs.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
            }
            hdrs.insert(header::VARY, HeaderValue::from_static("origin"));
            if !rule.expose_headers.is_empty() {
                if let Ok(v) = HeaderValue::from_str(&rule.expose_headers.join(", ")) {
                    hdrs.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
                }
            }
        }
        // No rule matched → no CORS headers; browsers block the page-side
        // read. The handler's status code (2xx/4xx) is preserved so that
        // non-browser clients like curl/SDKs still see the real result.
    } else {
        hdrs.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
    }
    resp
}

fn preflight_from_rule(rule: &CorsRule, origin: &str) -> Response {
    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, v);
    }
    headers.insert(header::VARY, HeaderValue::from_static("origin"));
    if !rule.allowed_methods.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&rule.allowed_methods.join(", ")) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, v);
        }
    }
    if !rule.allowed_headers.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&rule.allowed_headers.join(", ")) {
            headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, v);
        }
    }
    if !rule.expose_headers.is_empty() {
        if let Ok(v) = HeaderValue::from_str(&rule.expose_headers.join(", ")) {
            headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
        }
    }
    if rule.max_age_seconds > 0 {
        if let Ok(v) = HeaderValue::from_str(&rule.max_age_seconds.to_string()) {
            headers.insert(header::ACCESS_CONTROL_MAX_AGE, v);
        }
    }
    (StatusCode::NO_CONTENT, headers).into_response()
}

fn permissive_preflight() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, HEAD, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("*"),
    );
    (StatusCode::NO_CONTENT, headers).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_extraction() {
        assert_eq!(extract_s3_bucket("/s3/foo"), Some("foo"));
        assert_eq!(extract_s3_bucket("/s3/foo/bar/baz"), Some("foo"));
        assert_eq!(extract_s3_bucket("/s3/"), None);
        assert_eq!(extract_s3_bucket("/s3"), None);
        assert_eq!(extract_s3_bucket("/v1/buckets/foo"), None);
    }

    #[test]
    fn match_picks_first_admitting_rule() {
        let rules = vec![
            CorsRule {
                allowed_origins: vec!["https://a.com".into()],
                allowed_methods: vec!["GET".into()],
                ..Default::default()
            },
            CorsRule {
                allowed_origins: vec!["https://b.com".into()],
                allowed_methods: vec!["POST".into()],
                ..Default::default()
            },
        ];
        assert!(find_match(&rules, "https://a.com", "GET", None).is_some());
        assert!(find_match(&rules, "https://b.com", "POST", None).is_some());
        assert!(find_match(&rules, "https://b.com", "GET", None).is_none());
        assert!(find_match(&rules, "https://c.com", "GET", None).is_none());
    }

    #[test]
    fn wildcard_origin_admits_any() {
        let rules = vec![CorsRule {
            allowed_origins: vec!["*".into()],
            allowed_methods: vec!["GET".into()],
            ..Default::default()
        }];
        assert!(find_match(&rules, "https://anywhere.example", "GET", None).is_some());
    }

    #[test]
    fn header_match_requires_all_unless_wildcard() {
        let rule = CorsRule {
            allowed_origins: vec!["*".into()],
            allowed_methods: vec!["*".into()],
            allowed_headers: vec!["x-amz-date".into(), "authorization".into()],
            ..Default::default()
        };
        let rules = [rule];
        assert!(find_match(
            &rules,
            "https://x",
            "POST",
            Some(&["x-amz-date", "authorization"])
        )
        .is_some());
        assert!(find_match(&rules, "https://x", "POST", Some(&["x-amz-date"])).is_some());
        assert!(find_match(&rules, "https://x", "POST", Some(&["x-other"])).is_none());

        let wild = CorsRule {
            allowed_origins: vec!["*".into()],
            allowed_methods: vec!["*".into()],
            allowed_headers: vec!["*".into()],
            ..Default::default()
        };
        let wild_rules = [wild];
        assert!(find_match(&wild_rules, "https://x", "POST", Some(&["x-anything"])).is_some());
    }
}
