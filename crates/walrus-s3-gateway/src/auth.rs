// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 authentication using AWS Signature Version 4.

use crate::error::{S3Error, S3Result};
use axum::http::{HeaderMap, HeaderValue, Method, Uri};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::str::FromStr;

type HmacSha256 = Hmac<Sha256>;

/// AWS Signature Version 4 authenticator.
#[derive(Debug, Clone)]
pub struct SigV4Authenticator {
    access_key: String,
    secret_key: String,
    region: String,
    service: String,
}

impl SigV4Authenticator {
    /// Create a new SigV4 authenticator.
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        Self {
            access_key,
            secret_key,
            region,
            service: "s3".to_string(),
        }
    }
    
    /// Authenticate a request using AWS Signature Version 4.
    pub fn authenticate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> S3Result<()> {
        // Extract authorization header
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| S3Error::AccessDenied("Missing authorization header".to_string()))?;
        
        // Parse the authorization header
        let auth_parts = self.parse_authorization_header(auth_header)?;
        
        // Validate access key
        if auth_parts.access_key != self.access_key {
            return Err(S3Error::InvalidAccessKeyId);
        }
        
        // Get the timestamp
        let timestamp = self.get_timestamp(headers)?;
        
        // Validate timestamp (within 15 minutes)
        let now = Utc::now();
        let diff = (now - timestamp).num_seconds().abs();
        if diff > 900 {
            return Err(S3Error::RequestTimeTooSkewed);
        }
        
        // Calculate the expected signature
        let expected_signature = self.calculate_signature(
            method,
            uri,
            headers,
            body,
            &timestamp,
            &auth_parts.signed_headers,
        )?;
        
        // Compare signatures
        if expected_signature != auth_parts.signature {
            return Err(S3Error::SignatureDoesNotMatch);
        }
        
        Ok(())
    }
    
    /// Extract credentials from authorization header for creating per-request clients.
    /// Returns (access_key, secret_key) if extraction is successful.
    /// Note: This is a simplified implementation - in production you'd want to
    /// securely map access keys to corresponding secrets.
    pub fn extract_credentials(&self, headers: &HeaderMap) -> S3Result<(String, String)> {
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| S3Error::AccessDenied("Missing authorization header".to_string()))?;
        
        let auth_parts = self.parse_authorization_header(auth_header)?;
        
        // In a real implementation, you would:
        // 1. Look up the secret key from a secure store using the access key
        // 2. Validate the signature against the looked-up secret
        // 3. Return the actual credentials
        
        // For now, we'll return the configured credentials if the access key matches
        if auth_parts.access_key == self.access_key {
            Ok((self.access_key.clone(), self.secret_key.clone()))
        } else {
            Err(S3Error::InvalidAccessKeyId)
        }
    }
    
    /// Authenticate and extract credentials in one step.
    pub fn authenticate_and_extract(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> S3Result<(String, String)> {
        // First authenticate the request
        self.authenticate(method, uri, headers, body)?;
        
        // Then extract credentials
        self.extract_credentials(headers)
    }
    
    /// Parse the authorization header.
    fn parse_authorization_header(&self, header: &str) -> S3Result<AuthorizationParts> {
        if !header.starts_with("AWS4-HMAC-SHA256") {
            return Err(S3Error::InvalidRequest("Invalid authorization header".to_string()));
        }
        
        let parts: HashMap<&str, &str> = header
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .unwrap_or("")
            .split(", ")
            .filter_map(|part| {
                let mut split = part.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(key), Some(value)) => Some((key, value)),
                    _ => None,
                }
            })
            .collect();
        
        let credential = parts.get("Credential").ok_or(S3Error::InvalidRequest("Missing Credential".to_string()))?;
        let signed_headers = parts.get("SignedHeaders").ok_or(S3Error::InvalidRequest("Missing SignedHeaders".to_string()))?;
        let signature = parts.get("Signature").ok_or(S3Error::InvalidRequest("Missing Signature".to_string()))?;
        
        // Parse credential
        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() != 5 {
            return Err(S3Error::InvalidRequest("Invalid credential format".to_string()));
        }
        
        Ok(AuthorizationParts {
            access_key: cred_parts[0].to_string(),
            date: cred_parts[1].to_string(),
            region: cred_parts[2].to_string(),
            service: cred_parts[3].to_string(),
            signed_headers: signed_headers.to_string(),
            signature: signature.to_string(),
        })
    }
    
    /// Get the timestamp from headers.
    fn get_timestamp(&self, headers: &HeaderMap) -> S3Result<DateTime<Utc>> {
        let timestamp_str = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or(S3Error::InvalidRequest("Missing x-amz-date header".to_string()))?;
        
        // Parse AWS timestamp format: YYYYMMDDTHHMMSSZ
        DateTime::parse_from_str(timestamp_str, "%Y%m%dT%H%M%SZ")
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|_| S3Error::InvalidRequest("Invalid timestamp format".to_string()))
    }
    
    /// Calculate the expected signature.
    fn calculate_signature(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
        timestamp: &DateTime<Utc>,
        signed_headers: &str,
    ) -> S3Result<String> {
        // Step 1: Create canonical request
        let canonical_request = self.create_canonical_request(method, uri, headers, body, signed_headers)?;
        
        // Step 2: Create string to sign
        let string_to_sign = self.create_string_to_sign(&canonical_request, timestamp)?;
        
        // Step 3: Calculate signing key
        let signing_key = self.calculate_signing_key(timestamp)?;
        
        // Step 4: Calculate signature
        let signature = self.calculate_signature_hmac(&signing_key, &string_to_sign)?;
        
        Ok(hex::encode(signature))
    }
    
    /// Create canonical request.
    fn create_canonical_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
        signed_headers: &str,
    ) -> S3Result<String> {
        let canonical_method = method.as_str();
        let canonical_uri = uri.path();
        let canonical_query = uri.query().unwrap_or("");
        
        // Create canonical headers
        let header_names: Vec<&str> = signed_headers.split(';').collect();
        let mut canonical_headers = String::new();
        for header_name in &header_names {
            let header_value = headers
                .get(*header_name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            canonical_headers.push_str(&format!("{}:{}\n", header_name, header_value.trim()));
        }
        
        // Create payload hash
        let payload_hash = hex::encode(Sha256::digest(body));
        
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            canonical_method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        );
        
        Ok(canonical_request)
    }
    
    /// Create string to sign.
    fn create_string_to_sign(&self, canonical_request: &str, timestamp: &DateTime<Utc>) -> S3Result<String> {
        let algorithm = "AWS4-HMAC-SHA256";
        let timestamp_str = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            timestamp.format("%Y%m%d"),
            self.region,
            self.service
        );
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
        
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            algorithm,
            timestamp_str,
            credential_scope,
            canonical_request_hash
        );
        
        Ok(string_to_sign)
    }
    
    /// Calculate signing key.
    fn calculate_signing_key(&self, timestamp: &DateTime<Utc>) -> S3Result<Vec<u8>> {
        let date = timestamp.format("%Y%m%d").to_string();
        let k_secret = format!("AWS4{}", self.secret_key);
        
        let k_date = self.hmac_sha256(k_secret.as_bytes(), date.as_bytes())?;
        let k_region = self.hmac_sha256(&k_date, self.region.as_bytes())?;
        let k_service = self.hmac_sha256(&k_region, self.service.as_bytes())?;
        let k_signing = self.hmac_sha256(&k_service, b"aws4_request")?;
        
        Ok(k_signing)
    }
    
    /// Calculate signature using HMAC.
    fn calculate_signature_hmac(&self, key: &[u8], data: &str) -> S3Result<Vec<u8>> {
        self.hmac_sha256(key, data.as_bytes())
    }
    
    /// HMAC-SHA256 helper.
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> S3Result<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|_| S3Error::InternalError("Failed to create HMAC".to_string()))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

/// Parsed authorization header parts.
#[derive(Debug)]
struct AuthorizationParts {
    access_key: String,
    date: String,
    region: String,
    service: String,
    signed_headers: String,
    signature: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Method, Uri};
    
    #[test]
    fn test_parse_authorization_header() {
        let auth = SigV4Authenticator::new(
            "test_access_key".to_string(),
            "test_secret_key".to_string(),
            "us-east-1".to_string(),
        );
        
        let header = "AWS4-HMAC-SHA256 Credential=test_access_key/20231201/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=test_signature";
        
        let parts = auth.parse_authorization_header(header).unwrap();
        assert_eq!(parts.access_key, "test_access_key");
        assert_eq!(parts.date, "20231201");
        assert_eq!(parts.region, "us-east-1");
        assert_eq!(parts.service, "s3");
        assert_eq!(parts.signed_headers, "host;x-amz-date");
        assert_eq!(parts.signature, "test_signature");
    }
}
