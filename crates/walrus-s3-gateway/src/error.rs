// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Error types for the S3 gateway.

use axum::response::{IntoResponse, Response};
use axum::http::StatusCode;
use serde::Serialize;
use std::fmt;

/// Result type for S3 operations.
pub type S3Result<T> = Result<T, S3Error>;

/// S3 error types that correspond to AWS S3 error codes.
#[derive(Debug, Clone, Serialize)]
pub enum S3Error {
    /// The specified bucket does not exist.
    NoSuchBucket,
    
    /// The specified key does not exist.
    NoSuchKey,
    
    /// Access denied.
    AccessDenied(String),
    
    /// Invalid access key ID.
    InvalidAccessKeyId,
    
    /// Signature does not match.
    SignatureDoesNotMatch,
    
    /// Request time too skewed.
    RequestTimeTooSkewed,
    
    /// Invalid request.
    InvalidRequest(String),
    
    /// Bad request.
    BadRequest(String),
    
    /// Internal server error.
    InternalError(String),
    
    /// Method not allowed.
    MethodNotAllowed,
    
    /// Service unavailable.
    ServiceUnavailable(String),
    
    /// Request timeout.
    RequestTimeout,
    
    /// Entity too large.
    EntityTooLarge,
    
    /// Bucket already exists.
    BucketAlreadyExists,
    
    /// Bucket not empty.
    BucketNotEmpty,
    
    /// Not implemented.
    NotImplemented,
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3Error::NoSuchBucket => write!(f, "NoSuchBucket"),
            S3Error::NoSuchKey => write!(f, "NoSuchKey"),
            S3Error::AccessDenied(msg) => write!(f, "AccessDenied: {}", msg),
            S3Error::InvalidAccessKeyId => write!(f, "InvalidAccessKeyId"),
            S3Error::SignatureDoesNotMatch => write!(f, "SignatureDoesNotMatch"),
            S3Error::RequestTimeTooSkewed => write!(f, "RequestTimeTooSkewed"),
            S3Error::InvalidRequest(msg) => write!(f, "InvalidRequest: {}", msg),
            S3Error::BadRequest(msg) => write!(f, "BadRequest: {}", msg),
            S3Error::InternalError(msg) => write!(f, "InternalError: {}", msg),
            S3Error::MethodNotAllowed => write!(f, "MethodNotAllowed"),
            S3Error::ServiceUnavailable(msg) => write!(f, "ServiceUnavailable: {}", msg),
            S3Error::RequestTimeout => write!(f, "RequestTimeout"),
            S3Error::EntityTooLarge => write!(f, "EntityTooLarge"),
            S3Error::BucketAlreadyExists => write!(f, "BucketAlreadyExists"),
            S3Error::BucketNotEmpty => write!(f, "BucketNotEmpty"),
            S3Error::NotImplemented => write!(f, "NotImplemented"),
        }
    }
}

impl std::error::Error for S3Error {}

impl S3Error {
    /// Get the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            S3Error::NoSuchBucket => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey => StatusCode::NOT_FOUND,
            S3Error::AccessDenied(_) => StatusCode::FORBIDDEN,
            S3Error::InvalidAccessKeyId => StatusCode::FORBIDDEN,
            S3Error::SignatureDoesNotMatch => StatusCode::FORBIDDEN,
            S3Error::RequestTimeTooSkewed => StatusCode::FORBIDDEN,
            S3Error::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            S3Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            S3Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            S3Error::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            S3Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            S3Error::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            S3Error::EntityTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            S3Error::BucketAlreadyExists => StatusCode::CONFLICT,
            S3Error::BucketNotEmpty => StatusCode::CONFLICT,
            S3Error::NotImplemented => StatusCode::NOT_IMPLEMENTED,
        }
    }
    
    /// Get the S3 error code.
    pub fn error_code(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket => "NoSuchBucket",
            S3Error::NoSuchKey => "NoSuchKey",
            S3Error::AccessDenied => "AccessDenied",
            S3Error::InvalidAccessKeyId => "InvalidAccessKeyId",
            S3Error::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            S3Error::RequestTimeTooSkewed => "RequestTimeTooSkewed",
            S3Error::InvalidRequest(_) => "InvalidRequest",
            S3Error::InternalError(_) => "InternalError",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
            S3Error::ServiceUnavailable => "ServiceUnavailable",
            S3Error::RequestTimeout => "RequestTimeout",
            S3Error::EntityTooLarge => "EntityTooLarge",
            S3Error::BucketAlreadyExists => "BucketAlreadyExists",
            S3Error::BucketNotEmpty => "BucketNotEmpty",
            S3Error::NotImplemented => "NotImplemented",
        }
    }
    
    /// Get the error message.
    pub fn message(&self) -> String {
        match self {
            S3Error::NoSuchBucket => "The specified bucket does not exist".to_string(),
            S3Error::NoSuchKey => "The specified key does not exist".to_string(),
            S3Error::AccessDenied => "Access Denied".to_string(),
            S3Error::InvalidAccessKeyId => "The access key ID you provided does not exist in our records".to_string(),
            S3Error::SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided".to_string(),
            S3Error::RequestTimeTooSkewed => "The difference between the request time and the server's time is too large".to_string(),
            S3Error::InvalidRequest(msg) => msg.clone(),
            S3Error::InternalError(msg) => msg.clone(),
            S3Error::MethodNotAllowed => "The specified method is not allowed against this resource".to_string(),
            S3Error::ServiceUnavailable => "Please reduce your request rate".to_string(),
            S3Error::RequestTimeout => "Your socket connection to the server was not read from or written to within the timeout period".to_string(),
            S3Error::EntityTooLarge => "Your proposed upload exceeds the maximum allowed size".to_string(),
            S3Error::BucketAlreadyExists => "The named bucket already exists".to_string(),
            S3Error::BucketNotEmpty => "The bucket you tried to delete is not empty".to_string(),
            S3Error::NotImplemented => "The requested operation is not implemented".to_string(),
        }
    }
    
    /// Create an XML error response.
    pub fn to_xml(&self) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
    <RequestId>{}</RequestId>
</Error>"#,
            self.error_code(),
            self.message(),
            uuid::Uuid::new_v4()
        )
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let xml_body = self.to_xml();
        
        (
            status,
            [("Content-Type", "application/xml")],
            xml_body,
        ).into_response()
    }
}

impl From<anyhow::Error> for S3Error {
    fn from(err: anyhow::Error) -> Self {
        S3Error::InternalError(err.to_string())
    }
}

impl From<walrus_sdk::error::ClientError> for S3Error {
    fn from(err: walrus_sdk::error::ClientError) -> Self {
        match err.kind() {
            walrus_sdk::error::ClientErrorKind::BlobIdDoesNotExist => S3Error::NoSuchKey,
            _ => S3Error::InternalError(err.to_string()),
        }
    }
}
