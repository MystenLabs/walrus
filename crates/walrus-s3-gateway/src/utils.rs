// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for the S3 gateway.

use crate::error::{S3Error, S3Result};
use axum::http::HeaderMap;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Extract query parameters from a URI.
pub fn extract_query_params(uri: &axum::http::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

/// Parse S3 path (bucket and key) from URI.
pub fn parse_s3_path(uri: &axum::http::Uri) -> S3Result<(String, Option<String>)> {
    let path = uri.path();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    
    if parts.is_empty() || parts[0].is_empty() {
        return Err(S3Error::InvalidRequest("Invalid S3 path".to_string()));
    }
    
    let bucket = parts[0].to_string();
    let key = if parts.len() > 1 {
        Some(parts[1..].join("/"))
    } else {
        None
    };
    
    Ok((bucket, key))
}

/// Calculate MD5 hash of data.
pub fn calculate_md5(data: &[u8]) -> String {
    let digest = md5::compute(data);
    format!("{:x}", digest)
}

/// Calculate SHA256 hash of data.
pub fn calculate_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    hex::encode(digest)
}

/// Generate a random ETag.
pub fn generate_etag() -> String {
    let uuid = uuid::Uuid::new_v4();
    format!("\"{}\"", uuid.to_string().replace('-', ""))
}

/// Parse content type from headers.
pub fn parse_content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Parse content length from headers.
pub fn parse_content_length(headers: &HeaderMap) -> S3Result<Option<u64>> {
    if let Some(cl) = headers.get("content-length") {
        let cl_str = cl.to_str()
            .map_err(|_| S3Error::InvalidRequest("Invalid content-length header".to_string()))?;
        let length = cl_str.parse::<u64>()
            .map_err(|_| S3Error::InvalidRequest("Invalid content-length value".to_string()))?;
        Ok(Some(length))
    } else {
        Ok(None)
    }
}

/// Extract custom metadata from headers.
pub fn extract_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name_str = name.as_str();
            if name_str.starts_with("x-amz-meta-") {
                let key = name_str.strip_prefix("x-amz-meta-").unwrap().to_string();
                let value_str = value.to_str().ok()?.to_string();
                Some((key, value_str))
            } else {
                None
            }
        })
        .collect()
}

/// Create response headers for S3 object.
pub fn create_object_headers(
    size: u64,
    content_type: Option<&str>,
    etag: &str,
    last_modified: &DateTime<Utc>,
    metadata: &HashMap<String, String>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();
    
    // Set content length
    headers.insert("content-length", size.to_string().parse().unwrap());
    
    // Set content type
    if let Some(ct) = content_type {
        headers.insert("content-type", ct.parse().unwrap());
    }
    
    // Set ETag
    headers.insert("etag", etag.parse().unwrap());
    
    // Set last modified
    headers.insert(
        "last-modified",
        last_modified.format("%a, %d %b %Y %H:%M:%S GMT").to_string().parse().unwrap(),
    );
    
    // Set custom metadata
    for (key, value) in metadata {
        let header_name = format!("x-amz-meta-{}", key);
        if let Ok(name) = header_name.parse::<axum::http::HeaderName>() {
            if let Ok(val) = value.parse() {
                headers.insert(name, val);
            }
        }
    }
    
    headers
}

/// Validate bucket name according to S3 rules.
pub fn validate_bucket_name(bucket: &str) -> S3Result<()> {
    if bucket.is_empty() {
        return Err(S3Error::InvalidRequest("Bucket name cannot be empty".to_string()));
    }
    
    if bucket.len() < 3 || bucket.len() > 63 {
        return Err(S3Error::InvalidRequest(
            "Bucket name must be between 3 and 63 characters".to_string(),
        ));
    }
    
    if !bucket.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.') {
        return Err(S3Error::InvalidRequest(
            "Bucket name contains invalid characters".to_string(),
        ));
    }
    
    if bucket.starts_with('-') || bucket.ends_with('-') {
        return Err(S3Error::InvalidRequest(
            "Bucket name cannot start or end with a hyphen".to_string(),
        ));
    }
    
    Ok(())
}

/// Validate object key according to S3 rules.
pub fn validate_object_key(key: &str) -> S3Result<()> {
    if key.is_empty() {
        return Err(S3Error::InvalidRequest("Object key cannot be empty".to_string()));
    }
    
    if key.len() > 1024 {
        return Err(S3Error::InvalidRequest(
            "Object key cannot be longer than 1024 characters".to_string(),
        ));
    }
    
    // Check for invalid characters
    for c in key.chars() {
        if c.is_control() && c != '\t' {
            return Err(S3Error::InvalidRequest(
                "Object key contains invalid control characters".to_string(),
            ));
        }
    }
    
    Ok(())
}

/// Convert Walrus blob ID to S3 ETag format.
pub fn blob_id_to_etag(blob_id: &str) -> String {
    format!("\"{}\"", blob_id)
}

/// Convert S3 ETag to Walrus blob ID.
pub fn etag_to_blob_id(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

/// Parse multipart upload ID from ETag.
pub fn parse_upload_id(etag: &str) -> S3Result<String> {
    let blob_id = etag_to_blob_id(etag);
    // For now, we'll use the blob ID as the upload ID
    // In a real implementation, you might want to use a different scheme
    Ok(blob_id)
}

/// Format timestamp for S3 responses.
pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Format timestamp for HTTP headers.
pub fn format_http_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Uri;
    
    #[test]
    fn test_parse_s3_path() {
        let uri: Uri = "/mybucket/mykey".parse().unwrap();
        let (bucket, key) = parse_s3_path(&uri).unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, Some("mykey".to_string()));
        
        let uri: Uri = "/mybucket/path/to/object".parse().unwrap();
        let (bucket, key) = parse_s3_path(&uri).unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, Some("path/to/object".to_string()));
        
        let uri: Uri = "/mybucket".parse().unwrap();
        let (bucket, key) = parse_s3_path(&uri).unwrap();
        assert_eq!(bucket, "mybucket");
        assert_eq!(key, None);
    }
    
    #[test]
    fn test_validate_bucket_name() {
        assert!(validate_bucket_name("valid-bucket").is_ok());
        assert!(validate_bucket_name("valid.bucket").is_ok());
        assert!(validate_bucket_name("validbucket123").is_ok());
        
        assert!(validate_bucket_name("").is_err());
        assert!(validate_bucket_name("ab").is_err());
        assert!(validate_bucket_name("a".repeat(64).as_str()).is_err());
        assert!(validate_bucket_name("-invalid").is_err());
        assert!(validate_bucket_name("invalid-").is_err());
        assert!(validate_bucket_name("invalid@bucket").is_err());
    }
    
    #[test]
    fn test_validate_object_key() {
        assert!(validate_object_key("valid/key").is_ok());
        assert!(validate_object_key("valid-key.txt").is_ok());
        assert!(validate_object_key("valid_key").is_ok());
        
        assert!(validate_object_key("").is_err());
        assert!(validate_object_key(&"a".repeat(1025)).is_err());
    }
    
    #[test]
    fn test_blob_id_etag_conversion() {
        let blob_id = "test-blob-id";
        let etag = blob_id_to_etag(blob_id);
        assert_eq!(etag, "\"test-blob-id\"");
        
        let converted_back = etag_to_blob_id(&etag);
        assert_eq!(converted_back, blob_id);
    }
}
