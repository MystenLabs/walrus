// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 API handlers.

pub mod bucket;
pub mod object;

use crate::auth::SigV4Authenticator;
use crate::metadata::MetadataStore;
use crate::config::Config;
use crate::error::{S3Error, S3Result};
use crate::credentials::CredentialManager;
use axum::http::{HeaderMap, Method, Uri};
use std::sync::Arc;
use walrus_sdk::client::Client;
use walrus_sdk::config::ClientConfig;
use walrus_sui::client::{SuiContractClient, retry_client::RetriableSuiClient};
use walrus_sui::config::WalletConfig;
use walrus_utils::backoff::ExponentialBackoffConfig;

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
        credential_manager: CredentialManager,
    ) -> Self {
        Self {
            read_client: Arc::new(read_client),
            authenticator,
            default_bucket,
            metadata_store: MetadataStore::new(),
            config: Arc::new(config),
            credential_manager: Arc::new(credential_manager),
        }
    }
    
    /// Authenticate a request and extract credentials.
    pub fn authenticate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Result<(), crate::error::S3Error> {
        self.authenticator.authenticate(method, uri, headers, body)
    }
    
    /// Create a Walrus client for write operations using user credentials.
    /// This creates a new client per request with the user's wallet configuration.
    pub async fn create_write_client(
        &self,
        user_access_key: &str,
        user_secret_key: &str,
        authorization_header: Option<&str>,
    ) -> S3Result<Client<SuiContractClient>> {
        // Use credential manager to resolve credentials
        let resolved_cred = self.credential_manager
            .resolve_credentials(user_access_key, user_secret_key, authorization_header)
            .await?;
        
        // Check if we can perform server-side signing
        if !resolved_cred.supports_server_signing() {
            return Err(S3Error::InternalError(
                "Server-side signing not supported for this credential type".to_string()
            ));
        }
        
        let wallet_config = resolved_cred.wallet_config
            .ok_or_else(|| S3Error::InternalError("No wallet config available".to_string()))?;
        
        // Create a new client configuration with the user's wallet
        let mut client_config = self.create_client_config()?;
        client_config.wallet_config = Some(wallet_config.clone());
        
        // Create Sui client
        let _sui_client = RetriableSuiClient::new_for_rpc_urls(
            &self.config.walrus.sui_rpc_urls,
            ExponentialBackoffConfig::default(),
            self.config.walrus.request_timeout.map(std::time::Duration::from_secs),
        )
        .await
        .map_err(|e| S3Error::InternalError(format!("Failed to create Sui client: {}", e)))?;
        
        // Create wallet from configuration
        let wallet = WalletConfig::load_wallet(Some(&wallet_config), None)
            .map_err(|e| S3Error::InternalError(format!("Failed to load wallet: {}", e)))?;
        
        // Create SuiContractClient for write operations
        let contract_client = client_config
            .new_contract_client(wallet, None) // None for gas budget (use default)
            .await
            .map_err(|e| S3Error::InternalError(format!("Failed to create contract client: {}", e)))?;
        
        // Create Walrus client with contract client
        let walrus_client = Client::new_contract_client_with_refresher(client_config, contract_client)
            .await
            .map_err(|e| S3Error::InternalError(format!("Failed to create Walrus client: {}", e)))?;
        
        Ok(walrus_client)
    }
    
    /// Map S3 credentials to Walrus wallet configuration.
    /// This is where you can implement your credential mapping logic.
    fn map_s3_to_walrus_credentials(
        &self,
        access_key: &str,
        _secret_key: &str,
    ) -> S3Result<WalletConfig> {
        // For now, we'll use a simple mapping strategy.
        // In production, you might want to:
        // 1. Use the secret_key as a seed for wallet derivation
        // 2. Store user wallet configs in a database indexed by access_key
        // 3. Use some other secure mapping mechanism
        
        // For demonstration, we'll try to derive a wallet path from the access key
        let keystore_path = format!("~/.sui/sui_config/sui.keystore.{}", access_key);
        
        Ok(WalletConfig::OptionalPathWithOverride {
            path: Some(keystore_path.into()),
            active_env: Some("testnet".to_string()),
            active_address: None, // Will be determined from keystore
        })
    }
    
    /// Create a client configuration using the gateway's settings.
    fn create_client_config(&self) -> S3Result<ClientConfig> {
        use sui_types::base_types::ObjectID;
        use std::str::FromStr;
        use walrus_sdk::config::{ClientCommunicationConfig, CommitteesRefreshConfig};
        use walrus_sui::client::contract_config::ContractConfig;
        
        // Use actual testnet contract configuration
        let contract_config = ContractConfig {
            system_object: ObjectID::from_str("0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af")
                .map_err(|e| S3Error::InternalError(format!("Invalid system object ID: {}", e)))?,
            staking_object: ObjectID::from_str("0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3")
                .map_err(|e| S3Error::InternalError(format!("Invalid staking object ID: {}", e)))?,
            subsidies_object: Some(ObjectID::from_str("0xda799d85db0429765c8291c594d334349ef5bc09220e79ad397b30106161a0af")
                .map_err(|e| S3Error::InternalError(format!("Invalid subsidies object ID: {}", e)))?),
            credits_object: None,
        };
        
        // Create exchange objects for testnet
        let exchange_objects = vec![
            ObjectID::from_str("0xf4d164ea2def5fe07dc573992a029e010dba09b1a8dcbc44c5c2e79567f39073")
                .map_err(|e| S3Error::InternalError(format!("Invalid exchange object ID: {}", e)))?,
            ObjectID::from_str("0x19825121c52080bb1073662231cfea5c0e4d905fd13e95f21e9a018f2ef41862")
                .map_err(|e| S3Error::InternalError(format!("Invalid exchange object ID: {}", e)))?,
            ObjectID::from_str("0x83b454e524c71f30803f4d6c302a86fb6a39e96cdfb873c2d1e93bc1c26a3bc5")
                .map_err(|e| S3Error::InternalError(format!("Invalid exchange object ID: {}", e)))?,
            ObjectID::from_str("0x8d63209cf8589ce7aef8f262437163c67577ed09f3e636a9d8e0813843fb8bf1")
                .map_err(|e| S3Error::InternalError(format!("Invalid exchange object ID: {}", e)))?,
        ];
        
        Ok(ClientConfig {
            contract_config,
            exchange_objects,
            wallet_config: None, // Will be set by create_write_client
            rpc_urls: self.config.walrus.sui_rpc_urls.clone(),
            communication_config: ClientCommunicationConfig::default(),
            refresh_config: CommitteesRefreshConfig::default(),
        })
    }
}
