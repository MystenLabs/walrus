// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the S3 gateway.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Configuration for the S3 gateway server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// The address to bind the server to.
    pub bind_address: SocketAddr,
    
    /// Access key for S3 authentication.
    pub access_key: String,
    
    /// Secret key for S3 authentication.
    pub secret_key: String,
    
    /// Default region for S3 operations.
    pub region: String,
    
    /// Path to the Walrus client configuration file.
    pub walrus_config_path: Option<PathBuf>,
    
    /// Maximum request body size in bytes.
    pub max_body_size: usize,
    
    /// Request timeout in seconds.
    pub request_timeout: u64,
    
    /// Whether to enable CORS headers.
    pub enable_cors: bool,
    
    /// Whether to enable TLS.
    pub enable_tls: bool,
    
    /// Path to TLS certificate file (if TLS is enabled).
    pub tls_cert_path: Option<PathBuf>,
    
    /// Path to TLS private key file (if TLS is enabled).
    pub tls_key_path: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            access_key: "walrus-access-key".to_string(),
            secret_key: "walrus-secret-key".to_string(),
            region: "us-east-1".to_string(),
            walrus_config_path: None,
            max_body_size: 64 * 1024 * 1024, // 64MB
            request_timeout: 300, // 5 minutes
            enable_cors: true,
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
        }
    }
}

impl Config {
    /// Load configuration from a file.
    pub fn from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }
    
    /// Save configuration to a file.
    pub fn to_file(&self, path: &PathBuf) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
    
    /// Validate the configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.access_key.is_empty() {
            return Err(anyhow::anyhow!("access_key cannot be empty"));
        }
        
        if self.secret_key.is_empty() {
            return Err(anyhow::anyhow!("secret_key cannot be empty"));
        }
        
        if self.enable_tls {
            if self.tls_cert_path.is_none() {
                return Err(anyhow::anyhow!("tls_cert_path is required when TLS is enabled"));
            }
            if self.tls_key_path.is_none() {
                return Err(anyhow::anyhow!("tls_key_path is required when TLS is enabled"));
            }
        }
        
        Ok(())
    }
}
