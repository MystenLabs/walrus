// Copyright (c) Walrus Foundation  
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the Walrus S3 Gateway with Client-Side Signing.

use crate::credentials::ClientSigningConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use sui_types::base_types::SuiAddress;

/// Main configuration for the S3 Gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server configuration
    pub server: ServerConfig,
    /// Client-side signing configuration
    pub client_signing: ClientSigningConfig,
    /// Walrus client configuration path
    pub walrus_client_config: Option<PathBuf>,
    /// Walrus-specific configuration
    pub walrus: WalrusConfig,
    /// Registered client credentials
    pub client_credentials: HashMap<String, ClientCredentialConfig>,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Address to bind the server to
    pub bind_address: SocketAddr,
    /// S3 region identifier
    pub region: String,
    /// Enable TLS
    pub enable_tls: bool,
    /// TLS certificate file path
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private key file path
    pub tls_key_path: Option<PathBuf>,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Enable CORS headers
    pub enable_cors: bool,
}

/// Client credential configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCredentialConfig {
    /// The Sui address for this client
    pub sui_address: String, // We'll use String for TOML compatibility
    /// Permissions granted to this client
    pub permissions: Vec<String>,
    /// Optional description for this credential
    pub description: Option<String>,
    /// Whether this credential is active
    pub active: bool,
}

/// Configuration for the Walrus storage system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalrusConfig {
    /// Sui RPC endpoint URLs
    pub sui_rpc_urls: Vec<String>,
    /// Walrus storage node URLs
    pub storage_nodes: Vec<String>,
    /// Committee refresh interval in seconds
    pub committee_refresh_interval: Option<u64>,
    /// Request timeout in seconds
    pub request_timeout: Option<u64>,
    /// Whether to enable metrics collection
    pub enable_metrics: bool,
    /// Metrics port (if metrics are enabled)
    pub metrics_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        let mut client_credentials = HashMap::new();
        
        // Add a default credential for testing
        client_credentials.insert(
            "walrus-access-key".to_string(),
            ClientCredentialConfig {
                sui_address: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
                permissions: vec!["read".to_string(), "write".to_string()],
                description: Some("Default test credential".to_string()),
                active: true,
            }
        );

        Self {
            server: ServerConfig::default(),
            client_signing: ClientSigningConfig::default(),
            walrus_client_config: None,
            walrus: WalrusConfig::default(),
            client_credentials,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            region: "us-east-1".to_string(),
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            request_timeout: 300, // 5 minutes
            max_body_size: 64 * 1024 * 1024, // 64MB
            enable_cors: true,
        }
    }
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
            enable_metrics: false,
            metrics_port: None,
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Validate the configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        // Validate server config
        if self.server.region.is_empty() {
            return Err(anyhow::anyhow!("region cannot be empty"));
        }

        // Validate TLS configuration
        if self.server.enable_tls {
            if self.server.tls_cert_path.is_none() || self.server.tls_key_path.is_none() {
                return Err(anyhow::anyhow!("TLS enabled but cert or key not provided"));
            }
        }

        // Validate client credentials
        if self.client_credentials.is_empty() {
            return Err(anyhow::anyhow!("At least one client credential must be configured"));
        }

        for (access_key, cred) in &self.client_credentials {
            if access_key.is_empty() {
                return Err(anyhow::anyhow!("Access key cannot be empty"));
            }
            
            if cred.permissions.is_empty() {
                return Err(anyhow::anyhow!("Client {} must have at least one permission", access_key));
            }
            
            // Validate Sui address format
            if let Err(_) = cred.sui_address.parse::<SuiAddress>() {
                return Err(anyhow::anyhow!("Invalid Sui address for client {}: {}", access_key, cred.sui_address));
            }
        }

        // Validate Walrus config
        self.walrus.validate()?;

        Ok(())
    }

    /// Get active client credentials
    pub fn get_active_credentials(&self) -> HashMap<String, &ClientCredentialConfig> {
        self.client_credentials
            .iter()
            .filter(|(_, cred)| cred.active)
            .map(|(key, cred)| (key.clone(), cred))
            .collect()
    }

    /// Parse a client credential's Sui address
    pub fn parse_sui_address(&self, access_key: &str) -> anyhow::Result<SuiAddress> {
        let cred = self.client_credentials
            .get(access_key)
            .ok_or_else(|| anyhow::anyhow!("Access key not found: {}", access_key))?;
        
        cred.sui_address
            .parse::<SuiAddress>()
            .map_err(|e| anyhow::anyhow!("Invalid Sui address: {}", e))
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
    
    /// Whether to enable metrics collection.
    pub enable_metrics: bool,
    
    /// Metrics port (if metrics are enabled).
    pub metrics_port: Option<u16>,
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
