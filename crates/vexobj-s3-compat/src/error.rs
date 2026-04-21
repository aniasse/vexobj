use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::xml::error_xml;

#[derive(Debug)]
pub struct S3Error {
    pub code: &'static str,
    pub message: String,
    pub status: StatusCode,
}

impl S3Error {
    pub fn no_such_bucket(name: &str) -> Self {
        Self {
            code: "NoSuchBucket",
            message: format!("The specified bucket does not exist: {name}"),
            status: StatusCode::NOT_FOUND,
        }
    }

    pub fn no_such_key(key: &str) -> Self {
        Self {
            code: "NoSuchKey",
            message: format!("The specified key does not exist: {key}"),
            status: StatusCode::NOT_FOUND,
        }
    }

    pub fn bucket_already_exists(name: &str) -> Self {
        Self {
            code: "BucketAlreadyOwnedByYou",
            message: format!("Your previous request to create the named bucket succeeded: {name}"),
            status: StatusCode::CONFLICT,
        }
    }

    pub fn access_denied() -> Self {
        Self {
            code: "AccessDenied",
            message: "Access Denied".into(),
            status: StatusCode::FORBIDDEN,
        }
    }

    pub fn invalid_request(msg: &str) -> Self {
        Self {
            code: "InvalidRequest",
            message: msg.to_string(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    pub fn internal(msg: &str) -> Self {
        Self {
            code: "InternalError",
            message: msg.to_string(),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn entity_too_large() -> Self {
        Self {
            code: "EntityTooLarge",
            message: "Your proposed upload exceeds the maximum allowed size.".into(),
            status: StatusCode::BAD_REQUEST,
        }
    }

    pub fn bucket_not_empty(name: &str) -> Self {
        Self {
            code: "BucketNotEmpty",
            message: format!("The bucket you tried to delete is not empty: {name}"),
            status: StatusCode::CONFLICT,
        }
    }

    pub fn quota_exceeded(reason: &str) -> Self {
        Self {
            code: "ServiceUnavailable",
            message: format!("Bucket quota exceeded: {reason}"),
            status: StatusCode::INSUFFICIENT_STORAGE,
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = error_xml(self.code, &self.message);
        (self.status, [("content-type", "application/xml")], body).into_response()
    }
}
