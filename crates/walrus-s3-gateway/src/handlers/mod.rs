// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 API handlers for Client-Side Signing.

pub mod bucket;
pub mod object;

use crate::auth::SigV4Authenticator;
use crate::metadata::MetadataStore;
use crate::config::Config;
use crate::error::{S3Error, S3Result};
use crate::credentials::{ClientSigningManager, SignedTransactionRequest, UnsignedTransactionTemplate};
use axum::http::{HeaderMap, Method, Uri};
use std::sync::Arc;
use walrus_sdk::client::Client;
use walrus_sdk::config::ClientConfig;
use walrus_sui::client::{SuiContractClient, retry_client::RetriableSuiClient};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Shared state for S3 handlers with Client-Side Signing.
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
    
    /// Client-side signing manager.
    pub signing_manager: Arc<tokio::sync::RwLock<ClientSigningManager>>,
}

/// Shared state for S3 handlers.
#[derive(Clone)]
pub struct S3State {
    /// Read-only Walrus client for operations that don't require authentication.
    pub read_client: Arc<Client<walrus_sui::client::SuiReadClient>>,
    
    /// SigV4 authenticator.
    pub authenticator: SigV4Authenticator,
    
    /// Default bucket name (since Walrus doesn't have bucket concept).
    pub default_bucket: String,
    
    /// Metadata store for S3 objects.
    pub metadata_store: MetadataStore,
    
    /// Gateway configuration for creating per-request clients.
    pub config: Arc<Config>,
    
    /// Credential manager for secure authentication strategies.
    pub credential_manager: Arc<CredentialManager>,
}

impl S3State {
    /// Create new S3 state.
    pub fn new(
        read_client: Client<walrus_sui::client::SuiReadClient>,
        authenticator: SigV4Authenticator,
        default_bucket: String,
        config: Config,
    ) -> S3Result<Self> {
        let metadata_store = MetadataStore::new()?;
        
        // Initialize client signing manager
        let signing_manager = ClientSigningManager::new(config.client_signing.clone());
        
        Ok(Self {
            read_client: Arc::new(read_client),
            authenticator,
            default_bucket,
            metadata_store,
            config: Arc::new(config),
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

    /// Authenticate a request and return the access key
    pub fn authenticate_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
    ) -> S3Result<String> {
        self.authenticator.authenticate_request(method, uri, headers)
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

    /// Create a Walrus client for read operations (doesn't require credentials)
    pub fn create_read_client(&self) -> S3Result<Client<walrus_sui::client::SuiReadClient>> {
        // Use the already initialized read client
        Ok((*self.read_client).clone())
    }

    /// Submit a signed transaction to the Sui network
    pub async fn submit_signed_transaction(
        &self,
        signed_tx: &SignedTransactionRequest,
    ) -> S3Result<String> {
        // Create a client for transaction submission
        let config = ClientConfig {
            sui_rpc_url: self.config.walrus.sui_rpc_urls[0].clone(),
            storage_nodes: self.config.walrus.storage_nodes.clone(),
            committee_refresh_interval: self.config.walrus.committee_refresh_interval,
            backoff_config: ExponentialBackoffConfig::default(),
        };

        let client = Client::new(config)
            .map_err(|e| S3Error::ServiceUnavailable(format!("Failed to create client: {}", e)))?;

        // Submit the transaction
        // Note: This is a simplified implementation
        // In reality, you'd need to properly handle the transaction submission
        
        // For now, we'll return a placeholder transaction hash
        let tx_hash = format!("tx_{}", uuid::Uuid::new_v4());
        
        Ok(tx_hash)
    }
}
