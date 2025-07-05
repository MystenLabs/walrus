// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3-compatible gateway for Walrus storage.
//!
//! This crate provides an S3-compatible API that translates S3 requests into Walrus operations.
//! It supports the core S3 operations like PutObject, GetObject, DeleteObject, ListObjects, etc.
//! and uses SIGv4 authentication.

pub mod auth;
pub mod config;
pub mod credentials;
pub mod error;
pub mod handlers;
pub mod metadata;
pub mod s3_types;
pub mod server;
pub mod utils;

pub use config::Config;
pub use error::{S3Error, S3Result};
pub use server::S3GatewayServer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = Config::default();
        assert_eq!(config.access_key, "walrus-access-key");
        assert_eq!(config.secret_key, "walrus-secret-key");
        assert_eq!(config.region, "us-east-1");
        assert!(!config.enable_tls);
        assert!(config.enable_cors);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        
        // Valid config should pass
        assert!(config.validate().is_ok());
        
        // Empty access key should fail
        config.access_key = "".to_string();
        assert!(config.validate().is_err());
        
        // Empty secret key should fail
        config.access_key = "valid".to_string();
        config.secret_key = "".to_string();
        assert!(config.validate().is_err());
        
        // TLS enabled but no cert path should fail
        config.secret_key = "valid".to_string();
        config.enable_tls = true;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_s3_error_codes() {
        assert_eq!(S3Error::NoSuchBucket.error_code(), "NoSuchBucket");
        assert_eq!(S3Error::NoSuchKey.error_code(), "NoSuchKey");
        assert_eq!(S3Error::AccessDenied("test".to_string()).error_code(), "AccessDenied");
        assert_eq!(S3Error::InvalidAccessKeyId.error_code(), "InvalidAccessKeyId");
        assert_eq!(S3Error::SignatureDoesNotMatch.error_code(), "SignatureDoesNotMatch");
    }

    #[test]
    fn test_s3_error_status_codes() {
        use axum::http::StatusCode;
        
        assert_eq!(S3Error::NoSuchBucket.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::NoSuchKey.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::AccessDenied("test".to_string()).status_code(), StatusCode::FORBIDDEN);
        assert_eq!(S3Error::InvalidAccessKeyId.status_code(), StatusCode::FORBIDDEN);
        assert_eq!(S3Error::SignatureDoesNotMatch.status_code(), StatusCode::FORBIDDEN);
        assert_eq!(S3Error::InternalError("test".to_string()).status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_s3_error_xml_generation() {
        let error = S3Error::NoSuchBucket;
        let xml = error.to_xml();
        
        assert!(xml.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<Error>"));
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>The specified bucket does not exist</Message>"));
        assert!(xml.contains("</Error>"));
    }
}
