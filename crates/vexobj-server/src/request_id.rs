//! Per-request identifier middleware.
//!
//! On every request we either accept a client-supplied `X-Request-Id`
//! (when it's well-formed) or mint a fresh UUIDv4. The id lives in
//! request extensions for handlers to read, is attached to a tracing
//! span so downstream log lines are auto-correlated, and is echoed back
//! in the response header so the operator can grep logs from a user's
//! failure report.

use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::Response;
use tracing::Instrument;

pub const X_REQUEST_ID: &str = "x-request-id";

/// Max accepted length of a client-supplied id. Anything longer is rejected
/// and we fall back to minting our own. 128 bytes comfortably covers UUIDs,
/// ULIDs, and the opentelemetry trace-id-as-string forms, while blocking
/// log-spam attempts via multi-KB headers.
const MAX_LEN: usize = 128;

/// Per-request id made available through request extensions. Handlers that
/// need to surface the id in a log line can `req.extensions().get::<RequestId>()`.
#[derive(Clone, Debug)]
#[allow(dead_code)] // accessed via request extensions; keeping the field public
pub struct RequestId(pub String);

pub async fn request_id_middleware(mut req: Request<Body>, next: Next) -> Response {
    let id = client_supplied_id(&req).unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    req.extensions_mut().insert(RequestId(id.clone()));

    // Use .instrument() rather than .enter() — span guards are !Send and
    // can't be held across an .await.
    let span = tracing::info_span!("request", request_id = %id);
    let mut resp = next.run(req).instrument(span).await;

    if let Ok(hv) = HeaderValue::from_str(&id) {
        resp.headers_mut()
            .insert(HeaderName::from_static(X_REQUEST_ID), hv);
    }
    resp
}

/// Accept the client id when it's present, non-empty, under the size cap, and
/// purely printable ASCII — so a hostile header can't smuggle control chars
/// into our logs or response.
fn client_supplied_id(req: &Request<Body>) -> Option<String> {
    let v = req.headers().get(X_REQUEST_ID)?.to_str().ok()?;
    if v.is_empty() || v.len() > MAX_LEN {
        return None;
    }
    if !v.chars().all(|c| c.is_ascii_graphic()) {
        return None;
    }
    Some(v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn rejects_empty_id() {
        let req = Request::builder()
            .header(X_REQUEST_ID, "")
            .body(Body::empty())
            .unwrap();
        assert!(client_supplied_id(&req).is_none());
    }

    #[test]
    fn rejects_whitespace_inside_value() {
        // A space is a valid HeaderValue byte but fails our is_ascii_graphic
        // filter — prevents odd id shapes like "foo bar" that would log
        // oddly and break string-grep workflows.
        let req = Request::builder()
            .header(X_REQUEST_ID, "abc def")
            .body(Body::empty())
            .unwrap();
        assert!(client_supplied_id(&req).is_none());
    }

    #[test]
    fn rejects_overlong_id() {
        let long = "a".repeat(MAX_LEN + 1);
        let req = Request::builder()
            .header(X_REQUEST_ID, long)
            .body(Body::empty())
            .unwrap();
        assert!(client_supplied_id(&req).is_none());
    }

    #[test]
    fn accepts_uuid_like() {
        let req = Request::builder()
            .header(X_REQUEST_ID, "6b86b273-f34e-4a6f-bc1f-04af3c3d18a0")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            client_supplied_id(&req).unwrap(),
            "6b86b273-f34e-4a6f-bc1f-04af3c3d18a0"
        );
    }
}
