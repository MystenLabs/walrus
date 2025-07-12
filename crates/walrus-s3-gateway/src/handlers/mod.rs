// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 API handlers for Client-Side Signing.

pub mod bucket;
pub mod object;
pub mod signing;

use crate::auth::SigV4Authenticator;
use crate::metadata::MetadataStore;
use crate::config::Config;
use crate::error::{S3Error, S3Result};
use crate::credentials::{CredentialManager, ClientSigningManager, SignedTransactionRequest, UnsignedTransactionTemplate};
use axum::http::{HeaderMap, Method, Uri};
use std::sync::Arc;
use walrus_sdk::client::Client;
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Shared state for S3 handlers with Client-Side Signing support.
#[derive(Clone)]
pub struct S3State {
    /// Read-only Walrus client for operations that don't require authentication.
    pub read_client: Arc<Client<walrus_sui::client::SuiReadClient>>,
    
    /// SigV4 authenticator for S3 compatibility.
    pub authenticator: SigV4Authenticator,
    
    /// Default bucket name (since Walrus doesn't have bucket concept).
    pub default_bucket: String,
    
    /// Metadata store for S3 objects.
    pub metadata_store: MetadataStore,
    
    /// Gateway configuration.
    pub config: Arc<Config>,
    
    /// Credential manager for authentication strategies.
    pub credential_manager: Arc<CredentialManager>,
    
    /// Client-side signing manager.
    pub signing_manager: Arc<tokio::sync::RwLock<ClientSigningManager>>,
}

impl S3State {
    /// Create new S3 state.
    pub fn new(
        read_client: Client<walrus_sui::client::SuiReadClient>,
        authenticator: SigV4Authenticator,
        default_bucket: String,
        config: Config,
        credential_manager: CredentialManager,
    ) -> S3Result<Self> {
        let metadata_store = MetadataStore::new();
        
        // Initialize client signing manager
        let signing_manager = ClientSigningManager::new(config.client_signing.clone());
        
        Ok(Self {
            read_client: Arc::new(read_client),
            authenticator,
            default_bucket,
            metadata_store,
            config: Arc::new(config),
            credential_manager: Arc::new(credential_manager),
            signing_manager: Arc::new(tokio::sync::RwLock::new(signing_manager)),
        })
    }

    /// Initialize client credentials from configuration
    pub async fn initialize_credentials(&self) -> S3Result<()> {
        let mut manager = self.signing_manager.write().await;
        
        for (access_key, cred_config) in &self.config.client_credentials {
            if !cred_config.active {
                continue;
            }

            let sui_address = cred_config.sui_address
                .parse()
                .map_err(|e| S3Error::BadRequest(format!("Invalid Sui address: {}", e)))?;

            let permissions = cred_config.permissions
                .iter()
                .filter_map(|p| match p.as_str() {
                    "read" => Some(crate::credentials::Permission::Read),
                    "write" => Some(crate::credentials::Permission::Write),
                    "delete" => Some(crate::credentials::Permission::Delete),
                    "list_buckets" => Some(crate::credentials::Permission::ListBuckets),
                    "create_bucket" => Some(crate::credentials::Permission::CreateBucket),
                    _ => None,
                })
                .collect();

            manager.register_credential(
                access_key.clone(),
                sui_address,
                permissions,
            )?;
        }

        Ok(())
    }

    /// Authenticate a request and extract access key
    pub fn authenticate_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> S3Result<String> {
        // Extract the access key from the authorization header
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| S3Error::AccessDenied("Missing authorization header".to_string()))?;
        
        // Parse AWS4-HMAC-SHA256 Credential=ACCESS_KEY/...
        if let Some(credential_part) = auth_header.split("Credential=").nth(1) {
            if let Some(access_key) = credential_part.split('/').next() {
                // Verify the signature
                self.authenticator.authenticate(method, uri, headers, body)?;
                return Ok(access_key.to_string());
            }
        }
        
        Err(S3Error::AccessDenied("Missing access key".to_string()))
    }

    /// Authenticate a request (wrapper for backwards compatibility)
    pub fn authenticate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> S3Result<()> {
        self.authenticate_request(method, uri, headers, body).map(|_| ())
    }

    /// Validate a signed transaction from a client
    pub async fn validate_signed_transaction(
        &self,
        access_key: &str,
        request: &SignedTransactionRequest,
    ) -> S3Result<()> {
        let mut manager = self.signing_manager.write().await;
        manager.validate_signed_transaction(access_key, request)
    }

    /// Generate an unsigned transaction template for client signing
    pub async fn generate_transaction_template(
        &self,
        access_key: &str,
        purpose: crate::credentials::TransactionPurpose,
    ) -> S3Result<UnsignedTransactionTemplate> {
        let manager = self.signing_manager.read().await;
        
        let credential = manager.get_credential(access_key)
            .ok_or_else(|| S3Error::AccessDenied("Invalid access key".to_string()))?;

        manager.generate_transaction_template(purpose, credential.sui_address)
    }

    /// Submit a signed transaction to the Sui network (placeholder implementation)
    pub async fn submit_signed_transaction(
        &self,
        _signed_tx: &SignedTransactionRequest,
    ) -> S3Result<String> {
        // For now, we'll return a placeholder transaction hash
        // In a real implementation, this would submit the transaction to Sui
        let tx_hash = format!("tx_{}", uuid::Uuid::new_v4());
        Ok(tx_hash)
    }

    /// Create a write client
    pub async fn create_write_client(
        &self,
        _access_key: &str,
        _secret_key: &str,
        _authorization_header: Option<&str>,
    ) -> S3Result<walrus_sdk::client::Client<walrus_sui::client::SuiContractClient>> {
        // If client signing is required, we cannot perform server-side operations
        if self.config.client_signing.require_signatures {
            return Err(S3Error::BadRequest("Server-side operations not allowed when client signing is required".to_string()));
        }

        // For server-side operations, we need to create a contract client with a server wallet
        // For now, create a temporary wallet for testing
        let temp_dir = tempfile::tempdir()
            .map_err(|e| S3Error::InternalError(format!("Failed to create temp dir: {}", e)))?;
        
        let wallet_path = temp_dir.path().join("server_wallet.yaml");
        
        // Create a wallet for the server
        let wallet = walrus_sui::utils::create_wallet(
            &wallet_path,
            walrus_sui::utils::SuiNetwork::Testnet.env(),
            Some("server.keystore"),
            Some(std::time::Duration::from_secs(30)),
        ).map_err(|e| S3Error::InternalError(format!("Failed to create wallet: {}", e)))?;

        // Create contract client configuration from the read client's configuration
        let contract_config = self.read_client.config().contract_config.clone();
        
        // Get RPC URLs from the Walrus config if available, otherwise use default
        let rpc_urls = if let Some(walrus_config_path) = &self.config.walrus_config_path {
            // Try to read Walrus client config
            if let Ok(content) = std::fs::read_to_string(walrus_config_path) {
                if let Ok(walrus_config) = serde_yaml::from_str::<serde_yaml::Value>(&content) {
                    if let Some(rpc_urls) = walrus_config.get("contexts")
                        .and_then(|c| c.get("testnet"))
                        .and_then(|t| t.get("rpc_urls"))
                        .and_then(|r| r.as_sequence())
                    {
                        rpc_urls.iter()
                            .filter_map(|url| url.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    } else {
                        vec!["https://fullnode.testnet.sui.io:443".to_string()]
                    }
                } else {
                    vec!["https://fullnode.testnet.sui.io:443".to_string()]
                }
            } else {
                vec!["https://fullnode.testnet.sui.io:443".to_string()]
            }
        } else {
            vec!["https://fullnode.testnet.sui.io:443".to_string()]
        };

        // Create the SuiContractClient
        let contract_client = walrus_sui::client::SuiContractClient::new(
            wallet,
            &rpc_urls,
            &contract_config,
            ExponentialBackoffConfig::default(),
            None, // gas_budget
        ).await.map_err(|e| S3Error::InternalError(format!("Failed to create contract client: {}", e)))?;

        // Create the Walrus SDK client with a default config
        let client_config = walrus_sdk::config::ClientConfig {
            contract_config,
            exchange_objects: vec![],
            wallet_config: None,
            rpc_urls,
            communication_config: walrus_sdk::config::ClientCommunicationConfig::default(),
            refresh_config: walrus_sdk::config::CommitteesRefreshConfig::default(),
        };

        // Create the committees refresher handle
        let committees_handle = client_config
            .refresh_config
            .build_refresher_and_run(contract_client.read_client().clone())
            .await
            .map_err(|e| S3Error::InternalError(format!("Failed to create committees refresher: {}", e)))?;

        // Create the final Walrus client
        let client = walrus_sdk::client::Client::new_contract_client(
            client_config,
            committees_handle,
            contract_client,
        ).await.map_err(|e| S3Error::InternalError(format!("Failed to create Walrus client: {}", e)))?;

        Ok(client)
    }
}
