// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the S3 gateway.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use walrus_sui::config::WalletConfig;

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
    
    /// Walrus-specific configuration.
    pub walrus: WalrusConfig,
    
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
            walrus: WalrusConfig::default(),
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
        
        // Validate Walrus configuration
        self.walrus.validate()?;
        
        Ok(())
    }
}

/// Configuration for the Walrus storage system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalrusConfig {
    /// Sui RPC endpoint URLs.
    /// Example: ["https://sui-testnet-rpc.mystenlabs.com:443"]
    pub sui_rpc_urls: Vec<String>,
    
    /// Walrus storage node URLs.
    /// These are the storage node endpoints for reading/writing data.
    pub storage_nodes: Vec<String>,
    
    /// Committee refresh interval in seconds.
    /// How often to refresh the committee information from Sui.
    pub committee_refresh_interval: Option<u64>,
    
    /// Request timeout in seconds.
    pub request_timeout: Option<u64>,
    
    /// Wallet configuration for interacting with Sui.
    /// This includes the path to the wallet keystore and active address/environment.
    pub wallet_config: Option<WalletConfig>,
    
    /// Whether to enable metrics collection.
    pub enable_metrics: bool,
    
    /// Metrics port (if metrics are enabled).
    pub metrics_port: Option<u16>,
}

impl Default for WalrusConfig {
    fn default() -> Self {
        Self {
            sui_rpc_urls: vec![
                "https://sui-testnet-rpc.mystenlabs.com:443".to_string(),
                "https://sui-testnet.publicnode.com:443".to_string(),
            ],
            storage_nodes: vec![
                "https://walrus-testnet.nodes.guru:11444".to_string(),
                "https://walrus-testnet-storage.stakin-nodes.com:11444".to_string(),
            ],
            committee_refresh_interval: Some(300), // 5 minutes
            request_timeout: Some(30),
            wallet_config: None,
            enable_metrics: false,
            metrics_port: None,
        }
    }
}

impl WalrusConfig {
    /// Validate the Walrus configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.sui_rpc_urls.is_empty() {
            return Err(anyhow::anyhow!("At least one Sui RPC URL must be provided"));
        }
        
        for url in &self.sui_rpc_urls {
            if url.is_empty() {
                return Err(anyhow::anyhow!("Sui RPC URL cannot be empty"));
            }
        }
        
        if self.storage_nodes.is_empty() {
            return Err(anyhow::anyhow!("At least one storage node URL must be provided"));
        }
        
        for url in &self.storage_nodes {
            if url.is_empty() {
                return Err(anyhow::anyhow!("Storage node URL cannot be empty"));
            }
        }
        
        Ok(())
    }
}
