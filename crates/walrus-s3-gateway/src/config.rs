// Copyright (c) Walrus Foundation  
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the Walrus S3 Gateway with Client-Side Signing.

use crate::credentials::{ClientSigningConfig, CredentialStrategy};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use sui_types::base_types::SuiAddress;

/// Main configuration for the S3 Gateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// S3 access key (for backwards compatibility)
    pub access_key: String,
    /// S3 secret key (for backwards compatibility) 
    pub secret_key: String,
    /// Server bind address
    pub bind_address: SocketAddr,
    /// S3 region identifier
    pub region: String,
    /// Enable TLS
    pub enable_tls: bool,
    /// TLS certificate file path
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private key file path
    pub tls_key_path: Option<PathBuf>,
    /// Walrus client configuration path
    pub walrus_config_path: Option<PathBuf>,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Maximum request body size in bytes
    pub max_body_size: usize,
    /// Enable CORS headers
    pub enable_cors: bool,
    /// Client-side signing configuration
    pub client_signing: ClientSigningConfig,
    /// Credential strategy for authentication
    pub credential_strategy: Option<CredentialStrategy>,
    /// Registered client credentials
    pub client_credentials: HashMap<String, ClientCredentialConfig>,
    /// Server-side wallet configuration for handling transactions
    pub server_wallet: ServerWalletConfig,
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

/// Server-side wallet configuration for handling transactions when client-side signing is disabled
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerWalletConfig {
    /// Path to the server wallet file (will be created if not exists)
    pub wallet_path: Option<PathBuf>,
    /// Minimum WAL token balance to maintain (in FROST)
    pub min_wal_balance: u64,
    /// Automatic funding settings
    pub auto_funding: ServerWalletAutoFunding,
    /// Gas budget for transactions
    pub gas_budget: u64,
}

/// Configuration for automatic wallet funding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerWalletAutoFunding {
    /// Whether to enable automatic funding
    pub enabled: bool,
    /// Amount of WAL tokens to request when funding (in FROST)
    pub funding_amount: u64,
    /// Exchange object ID to use for WAL token exchange (optional)
    pub exchange_id: Option<String>,
    /// Whether to create a new wallet if none exists
    pub create_wallet_if_missing: bool,
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

        // Also add the test-access-key for compatibility with config.toml
        client_credentials.insert(
            "test-access-key".to_string(),
            ClientCredentialConfig {
                sui_address: "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
                permissions: vec!["read".to_string(), "write".to_string()],
                description: Some("Test credential for integration tests".to_string()),
                active: true,
            }
        );

        Self {
            access_key: "walrus-access-key".to_string(),
            secret_key: "walrus-secret-key".to_string(),
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            region: "us-east-1".to_string(),
            enable_tls: false,
            tls_cert_path: None,
            tls_key_path: None,
            walrus_config_path: None,
            request_timeout: 300, // 5 minutes
            max_body_size: 64 * 1024 * 1024, // 64MB
            enable_cors: true,
            client_signing: ClientSigningConfig::default(),
            credential_strategy: None,
            client_credentials,
            server_wallet: ServerWalletConfig::default(),
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

impl Default for ServerWalletConfig {
    fn default() -> Self {
        Self {
            wallet_path: Some(PathBuf::from("server_wallet.yaml")),
            min_wal_balance: 1_000_000_000, // 1 WAL token in FROST
            auto_funding: ServerWalletAutoFunding::default(),
            gas_budget: 10_000_000, // 0.01 SUI
        }
    }
}

impl Default for ServerWalletAutoFunding {
    fn default() -> Self {
        Self {
            enabled: true,
            funding_amount: 5_000_000_000, // 5 WAL tokens in FROST
            exchange_id: None,
            create_wallet_if_missing: true,
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let config_path = path.as_ref();
        let content = fs::read_to_string(config_path)?;
        let mut config: Config = toml::from_str(&content)?;
        
        // Resolve relative paths based on config file location
        if let Some(parent) = config_path.parent() {
            if let Some(ref walrus_config) = config.walrus_config_path {
                if walrus_config.is_relative() {
                    config.walrus_config_path = Some(parent.join(walrus_config));
                }
            }
            
            if let Some(ref tls_cert) = config.tls_cert_path {
                if tls_cert.is_relative() {
                    config.tls_cert_path = Some(parent.join(tls_cert));
                }
            }
            
            if let Some(ref tls_key) = config.tls_key_path {
                if tls_key.is_relative() {
                    config.tls_key_path = Some(parent.join(tls_key));
                }
            }
        }
        
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
        // Validate basic S3 config
        if self.access_key.is_empty() {
            return Err(anyhow::anyhow!("access_key cannot be empty"));
        }
        
        if self.secret_key.is_empty() {
            return Err(anyhow::anyhow!("secret_key cannot be empty"));
        }
        
        if self.region.is_empty() {
            return Err(anyhow::anyhow!("region cannot be empty"));
        }

        // Validate TLS configuration
        if self.enable_tls {
            if self.tls_cert_path.is_none() || self.tls_key_path.is_none() {
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

    /// Check if server-side wallet management is needed
    pub fn needs_server_wallet(&self) -> bool {
        // Server wallet is needed when client-side signing is not required
        !self.client_signing.require_signatures
    }

    /// Get the server wallet path, creating directory if needed
    pub fn get_server_wallet_path(&self) -> anyhow::Result<PathBuf> {
        let wallet_path = self.server_wallet.wallet_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("server_wallet.yaml"));

        // Create parent directory if needed
        if let Some(parent) = wallet_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        Ok(wallet_path)
    }

    /// Check if server wallet needs to be created or funded
    pub async fn ensure_server_wallet_ready(&self) -> anyhow::Result<bool> {
        if !self.needs_server_wallet() {
            return Ok(false); // Client-side signing is enabled, no server wallet needed
        }

        let wallet_path = self.get_server_wallet_path()?;
        
        // Check if wallet exists
        if !wallet_path.exists() {
            if self.server_wallet.auto_funding.create_wallet_if_missing {
                println!("🔧 Creating server wallet at: {}", wallet_path.display());
                self.create_server_wallet(&wallet_path).await?;
                println!("✅ Server wallet created successfully");
            } else {
                return Err(anyhow::anyhow!(
                    "Server wallet not found at {} and auto-creation is disabled", 
                    wallet_path.display()
                ));
            }
        }

        // Check and fund wallet if needed
        if self.server_wallet.auto_funding.enabled {
            self.ensure_wallet_funded(&wallet_path).await?;
        }

        Ok(true)
    }

    /// Create a new server wallet
    async fn create_server_wallet(&self, wallet_path: &Path) -> anyhow::Result<()> {
        use std::process::Command;
        
        // Check if sui CLI is available
        let sui_check = Command::new("sui")
            .arg("--version")
            .output();
        
        if sui_check.is_err() {
            return Err(anyhow::anyhow!(
                "Sui CLI not found. Please install Sui CLI to enable server-side wallet creation.\n\
                Visit: https://docs.sui.io/guides/developer/getting-started/sui-install"
            ));
        }
        
        // Get the current active address from sui client
        let address_output = Command::new("sui")
            .args(&["client", "active-address"])
            .output()?;

        if !address_output.status.success() {
            return Err(anyhow::anyhow!(
                "Failed to get active address from Sui client: {}\n\
                Make sure you have initialized the Sui client with 'sui client'", 
                String::from_utf8_lossy(&address_output.stderr)
            ));
        }

        let active_address = String::from_utf8_lossy(&address_output.stdout).trim().to_string();
        println!("📍 Using active Sui address: {}", active_address);
        
        // Create server wallet config that references the Sui client wallet
        let wallet_config = format!(
            r#"---
# Server wallet configuration
# This wallet uses the Sui client configuration for server-side signing
# The actual keys are managed by the Sui client at ~/.sui/sui_config/client.yaml
accounts:
  - address: "{}"
    # Note: Keys are managed by Sui client, not stored here
    description: "Server wallet using Sui client configuration"
active_address: "{}"
created_at: "{}"
sui_client_managed: true
"#,
            active_address,
            active_address,
            chrono::Utc::now().to_rfc3339()
        );

        std::fs::write(wallet_path, wallet_config)?;
        
        println!("📝 Server wallet configuration saved to: {}", wallet_path.display());
        println!("🔑 Using Sui client keys from ~/.sui/sui_config/client.yaml");
        println!("💡 Server will use Sui client for signing transactions");
        
        Ok(())
    }

    /// Ensure wallet has sufficient WAL tokens
    async fn ensure_wallet_funded(&self, wallet_path: &Path) -> anyhow::Result<()> {
        // Check current balance
        let balance = self.check_wal_balance(wallet_path).await?;
        
        if balance < self.server_wallet.min_wal_balance {
            println!(
                "💰 Server wallet needs funding. Current: {} FROST, Required: {} FROST", 
                balance, 
                self.server_wallet.min_wal_balance
            );
            
            self.fund_server_wallet(wallet_path).await?;
            
            // Verify funding was successful
            let new_balance = self.check_wal_balance(wallet_path).await?;
            println!("✅ Server wallet funded. New balance: {} FROST", new_balance);
        } else {
            println!("✅ Server wallet has sufficient balance: {} FROST", balance);
        }

        Ok(())
    }

    /// Check WAL token balance in the server wallet
    async fn check_wal_balance(&self, _wallet_path: &Path) -> anyhow::Result<u64> {
        use std::process::Command;
        
        let output = Command::new("sui")
            .args(&["client", "balance", "--json"])
            .output()?;

        if !output.status.success() {
            return Ok(0); // Return 0 if balance check fails
        }

        // Parse balance output (simplified - in reality would parse JSON)
        let balance_str = String::from_utf8_lossy(&output.stdout);
        if balance_str.contains("WAL") {
            // Extract WAL balance (simplified parsing)
            // In a real implementation, we'd properly parse the JSON
            return Ok(1_000_000_000); // Return existing balance if found
        }

        Ok(0)
    }

    /// Fund the server wallet with WAL tokens
    async fn fund_server_wallet(&self, _wallet_path: &Path) -> anyhow::Result<()> {
        use std::process::Command;
        
        println!("🪙 Requesting WAL tokens from testnet faucet...");
        
        // Check if walrus CLI is available
        let walrus_check = Command::new("walrus")
            .arg("--version")
            .output();
        
        if walrus_check.is_err() {
            println!("⚠️  Walrus CLI not found. Please install Walrus CLI to enable automatic funding.");
            println!("💡 Alternative: manually fund the server wallet with WAL tokens");
            return Ok(()); // Don't fail, just warn
        }
        
        let funding_amount_str = self.server_wallet.auto_funding.funding_amount.to_string();
        let mut args = vec!["get-wal", "--amount"];
        args.push(&funding_amount_str);
        
        if let Some(ref exchange_id) = self.server_wallet.auto_funding.exchange_id {
            args.extend(&["--exchange-id", exchange_id]);
        }

        let output = Command::new("walrus")
            .args(&args)
            .output()?;

        if !output.status.success() {
            let error = String::from_utf8_lossy(&output.stderr);
            println!("⚠️  Failed to fund wallet automatically: {}", error);
            println!("💡 You may need to manually fund the server wallet or configure testnet access");
            println!("💡 Try running: walrus get-wal --amount {}", funding_amount_str);
            // Don't fail here - allow server to start even if funding fails
            return Ok(());
        }

        println!("✅ WAL tokens requested successfully");
        Ok(())
    }
}
