use anyhow::Result;
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::Response,
    Router,
};
use chrono::{DateTime, Utc};
use hex;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

/// AWS Signature V4 test helper
pub struct AwsSigV4TestClient {
    access_key: String,
    secret_key: String,
    region: String,
    service: String,
}

impl AwsSigV4TestClient {
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        Self {
            access_key,
            secret_key,
            region,
            service: "s3".to_string(),
        }
    }

    /// Create a signed request
    pub fn create_signed_request(
        &self,
        method: &Method,
        uri: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<(HeaderMap, Vec<u8>)> {
        let parsed_url = url::Url::parse(&format!("http://localhost{}", uri))?;
        let host = parsed_url.host_str().unwrap_or("localhost");
        let path = parsed_url.path();
        let query = parsed_url.query().unwrap_or("");

        // Create timestamp
        let timestamp = Utc::now();
        let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();

        // Create headers
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_str(host)?);
        headers.insert("x-amz-date", HeaderValue::from_str(&amz_date)?);

        if let Some(ct) = content_type {
            headers.insert("content-type", HeaderValue::from_str(ct)?);
        }

        // Create canonical request
        let canonical_request = self.create_canonical_request(method, path, query, &headers, body)?;

        // Create string to sign
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            timestamp.format("%Y%m%d"),
            self.region,
            self.service
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        // Calculate signature
        let signing_key = self.calculate_signing_key(&timestamp)?;
        let signature = hex::encode(
            HmacSha256::new_from_slice(&signing_key)?
                .chain_update(string_to_sign.as_bytes())
                .finalize()
                .into_bytes(),
        );

        // Create authorization header
        let signed_headers = self.get_signed_headers(&headers);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, credential_scope, signed_headers, signature
        );

        headers.insert("authorization", HeaderValue::from_str(&authorization)?);

        Ok((headers, body.to_vec()))
    }

    fn create_canonical_request(
        &self,
        method: &Method,
        path: &str,
        query: &str,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<String> {
        let canonical_method = method.as_str();
        let canonical_uri = urlencoding::encode(path);

        // Canonical query
        let mut query_params: Vec<(String, String)> = if !query.is_empty() {
            query
                .split('&')
                .filter_map(|param| {
                    let mut parts = param.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(key), Some(value)) => Some((
                            urlencoding::encode(key).to_string(),
                            urlencoding::encode(value).to_string(),
                        )),
                        (Some(key), None) => Some((urlencoding::encode(key).to_string(), String::new())),
                        _ => None,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        query_params.sort();
        let canonical_query = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        // Canonical headers
        let mut canonical_headers = String::new();
        let mut header_names: Vec<String> = headers
            .keys()
            .map(|name| name.as_str().to_lowercase())
            .collect();
        header_names.sort();

        for name in &header_names {
            if let Some(value) = headers.get(name) {
                canonical_headers.push_str(&format!("{}:{}\n", name, value.to_str()?));
            }
        }

        let signed_headers = header_names.join(";");
        let payload_hash = hex::encode(Sha256::digest(body));

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            canonical_method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash
        ))
    }

    fn get_signed_headers(&self, headers: &HeaderMap) -> String {
        let mut header_names: Vec<String> = headers
            .keys()
            .map(|name| name.as_str().to_lowercase())
            .collect();
        header_names.sort();
        header_names.join(";")
    }

    fn calculate_signing_key(&self, timestamp: &DateTime<Utc>) -> Result<Vec<u8>> {
        let date = timestamp.format("%Y%m%d").to_string();
        let k_secret = format!("AWS4{}", self.secret_key);

        let k_date = HmacSha256::new_from_slice(k_secret.as_bytes())?
            .chain_update(date.as_bytes())
            .finalize()
            .into_bytes();

        let k_region = HmacSha256::new_from_slice(&k_date)?
            .chain_update(self.region.as_bytes())
            .finalize()
            .into_bytes();

        let k_service = HmacSha256::new_from_slice(&k_region)?
            .chain_update(self.service.as_bytes())
            .finalize()
            .into_bytes();

        let k_signing = HmacSha256::new_from_slice(&k_service)?
            .chain_update(b"aws4_request")
            .finalize()
            .into_bytes();

        Ok(k_signing.to_vec())
    }
}

/// Test helper for creating mock Walrus S3 Gateway application
pub async fn create_test_app() -> Result<Router> {
    use axum::routing::{get, put, post, delete, head};
    use axum::extract::{Path, Query};
    use axum::response::Json;
    use axum::http::StatusCode;
    use std::collections::HashMap;
    
    // Mock handlers for testing
    async fn mock_list_buckets() -> Json<serde_json::Value> {
        Json(json!({
            "ListAllMyBucketsResult": {
                "Buckets": {
                    "Bucket": []
                }
            }
        }))
    }
    
    async fn mock_get_object(Path((bucket, key)): Path<(String, String)>) -> Result<String, StatusCode> {
        let _ = (bucket, key);
        Ok("test-object-content".to_string())
    }
    
    async fn mock_put_object(Path((bucket, key)): Path<(String, String)>) -> Result<Json<serde_json::Value>, StatusCode> {
        let _ = (bucket, key);
        Ok(Json(json!({
            "ETag": "\"test-etag\"",
            "Location": "test-location"
        })))
    }
    
    async fn mock_delete_object(Path((bucket, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
        let _ = (bucket, key);
        Ok(StatusCode::NO_CONTENT)
    }
    
    async fn mock_head_object(Path((bucket, key)): Path<(String, String)>) -> Result<StatusCode, StatusCode> {
        let _ = (bucket, key);
        Ok(StatusCode::OK)
    }
    
    async fn mock_list_objects(Path(bucket): Path<String>) -> Json<serde_json::Value> {
        let _ = bucket;
        Json(json!({
            "ListBucketResult": {
                "Contents": []
            }
        }))
    }
    
    // For testing purposes, we'll create a simple router that mimics the S3 gateway
    // without requiring a real Walrus client
    let router = Router::new()
        // Root endpoint - list buckets
        .route("/", get(mock_list_buckets))
        
        // Bucket operations
        .route("/{bucket}", get(mock_list_objects))
        .route("/{bucket}", put(|| async { StatusCode::OK }))
        .route("/{bucket}", delete(|| async { StatusCode::NO_CONTENT }))
        .route("/{bucket}", head(|| async { StatusCode::OK }))
        
        // Object operations
        .route("/{bucket}/{*key}", get(mock_get_object))
        .route("/{bucket}/{*key}", put(mock_put_object))
        .route("/{bucket}/{*key}", delete(mock_delete_object))
        .route("/{bucket}/{*key}", head(mock_head_object))
        .route("/{bucket}/{*key}", post(|| async { StatusCode::OK }));
    
    Ok(router)
}

/// Create a mock Walrus client for testing
async fn create_mock_walrus_client() -> Result<walrus_sdk::client::Client<walrus_sui::client::SuiReadClient>> {
    // This is a placeholder - in a real test, you'd create a mock client
    // For now, we'll return an error since we can't create a real client without proper setup
    Err(anyhow::anyhow!("Mock client not implemented"))
}

/// Create a test client with default credentials
pub fn create_test_client() -> AwsSigV4TestClient {
    AwsSigV4TestClient::new(
        "test-access-key".to_string(),
        "test-secret-key".to_string(),
        "us-east-1".to_string(),
    )
}

/// Helper to extract JSON from response
pub async fn extract_json_response(response: Response<Body>) -> Result<Value> {
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body_bytes)?;
    Ok(json)
}

/// Helper to extract text from response
pub async fn extract_text_response(response: Response<Body>) -> Result<String> {
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    Ok(String::from_utf8(body_bytes.to_vec())?)
}

/// Test data generators
pub struct TestData;

impl TestData {
    pub fn sample_blob_content() -> &'static str {
        "Hello, Walrus S3 Gateway with Client-Side Signing!"
    }

    pub fn sample_bucket_name() -> String {
        format!("test-bucket-{}", Uuid::new_v4().simple())
    }

    pub fn sample_object_key() -> String {
        format!("test-object-{}.txt", Uuid::new_v4().simple())
    }

    pub fn sample_transaction_template() -> Value {
        json!({
            "transaction_data": "0x1234567890abcdef",
            "gas_object": "0xabcdef1234567890",
            "gas_budget": 10000000,
            "gas_price": 1000,
            "sender": "0x1111111111111111111111111111111111111111111111111111111111111111"
        })
    }

    pub fn sample_signed_transaction() -> Value {
        json!({
            "transaction_data": "0x1234567890abcdef",
            "signatures": ["0xsignature1", "0xsignature2"],
            "gas_object": "0xabcdef1234567890"
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aws_signature_creation() {
        let client = create_test_client();
        let result = client.create_signed_request(
            &Method::GET,
            "/test-bucket/test-object",
            b"",
            None,
        );

        assert!(result.is_ok());
        let (headers, _) = result.unwrap();

        assert!(headers.contains_key("host"));
        assert!(headers.contains_key("x-amz-date"));
        assert!(headers.contains_key("authorization"));

        let auth_header = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256"));
        assert!(auth_header.contains("Credential=test-access-key"));
    }

    #[test]
    fn test_canonical_request_creation() {
        let client = create_test_client();
        let headers = HeaderMap::new();
        
        let result = client.create_canonical_request(
            &Method::GET,
            "/test-bucket/test-object",
            "",
            &headers,
            b"test content",
        );

        assert!(result.is_ok());
        let canonical = result.unwrap();
        assert!(canonical.contains("GET"));
        assert!(canonical.contains("/test-bucket/test-object"));
    }

    #[test]
    fn test_data_generators() {
        assert!(!TestData::sample_blob_content().is_empty());
        assert!(TestData::sample_bucket_name().starts_with("test-bucket-"));
        assert!(TestData::sample_object_key().starts_with("test-object-"));
        
        let template = TestData::sample_transaction_template();
        assert!(template.get("transaction_data").is_some());
        
        let signed_tx = TestData::sample_signed_transaction();
        assert!(signed_tx.get("signatures").is_some());
    }
}
