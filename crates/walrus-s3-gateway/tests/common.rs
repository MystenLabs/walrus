//! Common test utilities for Walrus S3 Gateway integration tests

use anyhow::Result;
use base64::Engine;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::process::Command;
use uuid::Uuid;

/// Test client configuration
pub struct TestConfig {
    pub base_url: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub timeout: Duration,
    pub wal_tokens_needed: u64,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            base_url: std::env::var("GATEWAY_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string()),
            access_key: std::env::var("ACCESS_KEY")
                .unwrap_or_else(|_| "walrus-access-key".to_string()),
            secret_key: std::env::var("SECRET_KEY")
                .unwrap_or_else(|_| "walrus-secret-key".to_string()),
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            timeout: Duration::from_secs(
                std::env::var("REQUEST_TIMEOUT")
                    .unwrap_or_else(|_| "30".to_string())
                    .parse()
                    .unwrap_or(30),
            ),
            wal_tokens_needed: std::env::var("WAL_TOKENS_NEEDED")
                .unwrap_or_else(|_| "1000000000".to_string()) // 1 WAL token in FROST
                .parse()
                .unwrap_or(1000000000),
        }
    }
}

/// AWS SigV4 test client for signing requests
pub struct AwsSigV4TestClient {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub service: String,
    pub client: Client,
}

impl AwsSigV4TestClient {
    pub fn new(access_key: String, secret_key: String, region: String) -> Self {
        Self {
            access_key,
            secret_key,
            region,
            service: "s3".to_string(),
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Create a signed HTTP request
    pub fn create_signed_request(
        &self,
        method: &str,
        path: &str,
        payload: &[u8],
        content_type: Option<&str>,
    ) -> Result<(HashMap<String, String>, Vec<u8>)> {
        let now = Utc::now();
        let date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        // Calculate payload hash first
        let payload_hash = hex::encode(Sha256::digest(payload));

        let mut headers = HashMap::new();
        headers.insert("host".to_string(), "127.0.0.1:8080".to_string());
        headers.insert("x-amz-date".to_string(), date.clone());
        headers.insert("x-amz-content-sha256".to_string(), payload_hash.clone());

        if let Some(ct) = content_type {
            headers.insert("content-type".to_string(), ct.to_string());
        }

        // Create canonical headers in the correct format
        let mut canonical_headers = String::new();
        let mut signed_headers_vec = Vec::new();

        let mut header_keys: Vec<&String> = headers.keys().collect();
        header_keys.sort();

        for key in &header_keys {
            let normalized_key = key.to_lowercase();
            let normalized_value = headers[*key].trim();
            canonical_headers.push_str(&format!("{}:{}\n", normalized_key, normalized_value));
            signed_headers_vec.push(normalized_key);
        }

        let signed_headers = signed_headers_vec.join(";");

        // Ensure path starts with /
        let canonical_uri = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };

        // Create canonical request following AWS specification exactly
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.to_uppercase(),
            canonical_uri,
            "", // Empty query string
            canonical_headers,
            signed_headers,
            payload_hash
        );

        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

        // Create string to sign
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date, credential_scope, canonical_request_hash
        );

        // Calculate signature
        let signature = self.calculate_signature(&string_to_sign, &date_stamp)?;

        // Create authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, credential_scope, signed_headers, signature
        );

        headers.insert("authorization".to_string(), authorization);

        Ok((headers, payload.to_vec()))
    }

    /// Calculate AWS SigV4 signature
    fn calculate_signature(&self, string_to_sign: &str, date_stamp: &str) -> Result<String> {
        let date_key = Self::hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let date_region_key = Self::hmac_sha256(&date_key, self.region.as_bytes());
        let date_region_service_key = Self::hmac_sha256(&date_region_key, self.service.as_bytes());
        let signing_key = Self::hmac_sha256(&date_region_service_key, b"aws4_request");

        let signature = Self::hmac_sha256(&signing_key, string_to_sign.as_bytes());
        Ok(hex::encode(signature))
    }

    /// HMAC-SHA256 helper
    fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Make a signed HTTP request
    pub async fn make_request(
        &self,
        method: &str,
        base_url: &str,
        path: &str,
        payload: &[u8],
        content_type: Option<&str>,
    ) -> Result<reqwest::Response> {
        let (headers, body) = self.create_signed_request(method, path, payload, content_type)?;

        let mut request_builder = self
            .client
            .request(method.parse()?, &format!("{}{}", base_url, path));

        for (key, value) in headers {
            request_builder = request_builder.header(key, value);
        }

        if !body.is_empty() {
            request_builder = request_builder.body(body);
        }

        let response = request_builder.send().await?;
        Ok(response)
    }
}

/// Test data utilities
pub struct TestData;

impl TestData {
    pub fn sample_bucket_name() -> String {
        std::env::var("TEST_BUCKET").unwrap_or_else(|_| {
            format!(
                "test-bucket-{}",
                Uuid::new_v4().to_string()[..8].to_lowercase()
            )
        })
    }

    pub fn sample_object_key() -> String {
        std::env::var("TEST_OBJECT").unwrap_or_else(|_| {
            format!(
                "test-object-{}.txt",
                Uuid::new_v4().to_string()[..8].to_lowercase()
            )
        })
    }

    pub fn sample_blob_content() -> String {
        format!("Test blob content created at {}", Utc::now().to_rfc3339())
    }

    pub fn sample_signed_transaction() -> String {
        // This would be a real signed transaction in a real test
        base64::engine::general_purpose::STANDARD
            .encode(format!("mock_signed_transaction_{}", Uuid::new_v4()).as_bytes())
    }

    pub fn large_blob_content(size_mb: usize) -> Vec<u8> {
        let size_bytes = size_mb * 1024 * 1024;
        let mut content = Vec::with_capacity(size_bytes);
        let pattern = b"WALRUS_TEST_DATA_PATTERN_";

        for i in 0..size_bytes {
            content.push(pattern[i % pattern.len()]);
        }

        content
    }
}

/// Test environment setup
pub struct TestEnvironment {
    pub config: TestConfig,
    pub client: AwsSigV4TestClient,
}

impl TestEnvironment {
    pub async fn new() -> Result<Self> {
        let config = TestConfig::default();
        let client = AwsSigV4TestClient::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            config.region.clone(),
        );

        // Verify server is accessible
        Self::check_server_connectivity(&config).await?;

        Ok(Self { config, client })
    }

    /// Create a new test environment with WAL token funding
    pub async fn new_with_wal_funding() -> Result<Self> {
        let config = TestConfig::default();
        let client = AwsSigV4TestClient::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            config.region.clone(),
        );

        // Verify server is accessible
        Self::check_server_connectivity(&config).await?;

        // Try to fund the test account with WAL tokens
        fund_test_account_with_wal_tokens().await?;

        Ok(Self { config, client })
    }

    /// Check if the server is accessible
    async fn check_server_connectivity(config: &TestConfig) -> Result<()> {
        let client = Client::builder().timeout(config.timeout).build()?;

        // Try a simple request to verify the server is running
        let response = client
            .get(&format!("{}/health", config.base_url))
            .send()
            .await;

        match response {
            Ok(_) => Ok(()),
            Err(_) => {
                // Try root endpoint as fallback
                let response = client
                    .get(&config.base_url)
                    .send()
                    .await
                    .map_err(|e| anyhow::anyhow!(
                        "Server not accessible at {}. Make sure the Walrus S3 Gateway is running. Error: {}",
                        config.base_url, e
                    ))?;

                if response.status().is_server_error() {
                    return Err(anyhow::anyhow!(
                        "Server error at {}. Server returned status: {}",
                        config.base_url,
                        response.status()
                    ));
                }

                Ok(())
            }
        }
    }

    pub fn base_url(&self) -> &str {
        &self.config.base_url
    }
}

/// Helper for testing with timeout
pub async fn with_timeout<F, R>(duration: Duration, f: F) -> Result<R>
where
    F: std::future::Future<Output = Result<R>>,
{
    match tokio::time::timeout(duration, f).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("Operation timed out after {:?}", duration)),
    }
}

/// Print test configuration
pub fn print_test_configuration() {
    let config = TestConfig::default();
    println!("🔧 Test Configuration:");
    println!("   Gateway URL: {}", config.base_url);
    println!("   Access Key: {}", config.access_key);
    println!(
        "   Secret Key: {}***",
        &config.secret_key[..3.min(config.secret_key.len())]
    );
    println!("   Region: {}", config.region);
    println!("   Timeout: {:?}", config.timeout);
    println!("   WAL Tokens Needed: {}", config.wal_tokens_needed);
    println!("   Test Bucket: {}", TestData::sample_bucket_name());
    println!("   Test Object: {}", TestData::sample_object_key());
    println!();
}

/// Fund test account with WAL tokens for testing
pub async fn fund_test_account_with_wal_tokens() -> Result<()> {
    let config = TestConfig::default();
    
    println!("💰 Funding test account with {} WAL tokens...", config.wal_tokens_needed);
    
    // Check if we're in testnet mode
    let network_check = std::env::var("WALRUS_NETWORK").unwrap_or_else(|_| "testnet".to_string());
    
    if network_check != "testnet" {
        println!("⚠️  WAL token funding is only available on testnet. Skipping funding...");
        return Ok(());
    }
    
    // Try to get WAL tokens using the walrus CLI
    let get_wal_result = Command::new("walrus")
        .args(&["get-wal", "--amount", &config.wal_tokens_needed.to_string()])
        .output()
        .await;
    
    match get_wal_result {
        Ok(output) => {
            if output.status.success() {
                println!("✅ Successfully funded test account with WAL tokens");
                println!("   Output: {}", String::from_utf8_lossy(&output.stdout));
            } else {
                println!("⚠️  Failed to fund account with WAL tokens (this is expected in some test environments)");
                println!("   Error: {}", String::from_utf8_lossy(&output.stderr));
                // Don't fail the test, just warn
            }
        }
        Err(e) => {
            println!("⚠️  Could not execute 'walrus get-wal' command: {}", e);
            println!("   This is expected if walrus CLI is not available or not in PATH");
            // Don't fail the test, just warn
        }
    }
    
    // Try to check balance
    let balance_result = Command::new("sui")
        .args(&["client", "balance"])
        .output()
        .await;
    
    match balance_result {
        Ok(output) => {
            if output.status.success() {
                println!("💰 Current balance:");
                println!("{}", String::from_utf8_lossy(&output.stdout));
            }
        }
        Err(_) => {
            println!("⚠️  Could not check balance (sui CLI not available)");
        }
    }
    
    Ok(())
}

/// Check if test account has sufficient WAL tokens
pub async fn check_wal_token_balance() -> Result<bool> {
    let balance_result = Command::new("sui")
        .args(&["client", "balance"])
        .output()
        .await;
    
    match balance_result {
        Ok(output) => {
            if output.status.success() {
                let balance_output = String::from_utf8_lossy(&output.stdout);
                // Look for WAL token balance
                if balance_output.contains("WAL") {
                    println!("✅ Found WAL tokens in account balance");
                    return Ok(true);
                }
            }
        }
        Err(_) => {
            println!("⚠️  Could not check balance (sui CLI not available)");
        }
    }
    
    Ok(false)
}
