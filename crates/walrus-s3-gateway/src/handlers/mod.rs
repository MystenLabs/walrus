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

    /// Create a write client (for client-side signing, this returns a read client)
    pub async fn create_write_client(
        &self,
        _access_key: &str,
        _secret_key: &str,
        _authorization_header: Option<&str>,
    ) -> S3Result<Client<walrus_sui::client::SuiReadClient>> {
        // For client-side signing, we only support read operations on the server side
        // Write operations require client-signed transactions
        Ok((*self.read_client).clone())
    }
}
