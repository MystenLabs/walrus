// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! S3 gateway server implementation.

use crate::auth::SigV4Authenticator;
use crate::config::Config;
use crate::error::{S3Error, S3Result};
use crate::handlers::{bucket, object, S3State};
use crate::credentials::{CredentialManager, CredentialStrategy};
use axum::extract::{DefaultBodyLimit, Query, State};
use axum::http::{HeaderMap, Method, Uri};
use axum::response::Response;
use axum::routing::{delete, get, head, post, put};
use axum::Router;
use bytes::Bytes;
use std::collections::HashMap;
use tokio::signal;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use walrus_sdk::{client::Client, config::{ClientConfig, ClientCommunicationConfig, CommitteesRefreshConfig}};
use walrus_sui::client::{retry_client::RetriableSuiClient, contract_config::ContractConfig};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// S3 gateway server.
pub struct S3GatewayServer {
    config: Config,
    app: Router,
}

impl S3GatewayServer {
    /// Create a new S3 gateway server.
    pub async fn new(config: Config, walrus_client: Client<walrus_sui::client::SuiReadClient>) -> S3Result<Self> {
        // Validate configuration
        config.validate().map_err(S3Error::from)?;
        
        // Create authenticator
        let authenticator = SigV4Authenticator::new(
            config.access_key.clone(),
            config.secret_key.clone(),
            config.region.clone(),
        );
        
        // Create credential manager from configuration
        let credential_strategy = config.credential_strategy.clone()
            .unwrap_or_else(|| CredentialStrategy::DirectMapping { 
                mapping: HashMap::new() 
            });
        let credential_manager = CredentialManager::new(credential_strategy);
        
        // Create shared state
        let state = S3State::new(
            walrus_client,
            authenticator,
            "walrus-bucket".to_string(), // Default bucket name
            config.clone(),
            credential_manager,
        );
        
        // Build the router
        let app = Self::build_router(state, &config).await?;
        
        Ok(Self { config, app })
    }
    
    /// Build the Axum router with all S3 endpoints.
    async fn build_router(state: S3State, config: &Config) -> S3Result<Router> {
        let router = Router::new()
            // Root endpoint - list buckets
            .route("/", get(bucket::list_buckets))
            
            // Bucket operations
            .route("/{bucket}", get(Self::handle_bucket_get))
            .route("/{bucket}", put(bucket::create_bucket))
            .route("/{bucket}", delete(bucket::delete_bucket))
            .route("/{bucket}", head(bucket::head_bucket))
            
            // Object operations
            .route("/{bucket}/{*key}", get(object::get_object))
            .route("/{bucket}/{*key}", put(Self::handle_object_put))
            .route("/{bucket}/{*key}", delete(object::delete_object))
            .route("/{bucket}/{*key}", head(object::head_object))
            .route("/{bucket}/{*key}", post(Self::handle_object_post))
            
            // Add state
            .with_state(state)
            
            // Add middleware
            .layer(
                ServiceBuilder::new()
                    .layer(TraceLayer::new_for_http())
                    .layer(DefaultBodyLimit::max(config.max_body_size))
                    .layer(if config.enable_cors {
                        CorsLayer::very_permissive()
                    } else {
                        CorsLayer::new()
                    }),
            );
        
        Ok(router)
    }
    
    /// Handle bucket GET requests (can be list objects or bucket-specific operations).
    async fn handle_bucket_get(
        State(state): State<S3State>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        Query(params): Query<HashMap<String, String>>,
    ) -> S3Result<Response> {
        // Check query parameters to determine the operation
        if params.contains_key("location") {
            bucket::get_bucket_location(State(state), method, uri, headers).await
        } else if params.contains_key("versioning") {
            bucket::get_bucket_versioning(State(state), method, uri, headers).await
        } else if params.contains_key("acl") {
            bucket::get_bucket_acl(State(state), method, uri, headers).await
        } else {
            // Default to list objects
            object::list_objects(State(state), method, uri, headers, Query(params)).await
        }
    }
    
    /// Handle object PUT requests (can be put object or upload part).
    async fn handle_object_put(
        State(state): State<S3State>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        Query(params): Query<HashMap<String, String>>,
        body: Bytes,
    ) -> S3Result<Response> {
        // Check if this is a multipart upload part
        if params.contains_key("partNumber") && params.contains_key("uploadId") {
            object::upload_part(State(state), method, uri, headers, body, Query(params)).await
        } else if headers.contains_key("x-amz-copy-source") {
            object::copy_object(State(state), method, uri, headers).await
        } else {
            // Regular put object
            object::put_object(State(state), method, uri, headers, body).await
        }
    }
    
    /// Handle object POST requests (multipart upload operations).
    async fn handle_object_post(
        State(state): State<S3State>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        Query(params): Query<HashMap<String, String>>,
        body: Bytes,
    ) -> S3Result<Response> {
        if params.contains_key("uploads") {
            // Initiate multipart upload
            object::create_multipart_upload(State(state), method, uri, headers).await
        } else if params.contains_key("uploadId") {
            // Complete multipart upload
            object::complete_multipart_upload(State(state), method, uri, headers, body, Query(params)).await
        } else {
            Err(S3Error::InvalidRequest("Invalid POST operation".to_string()))
        }
    }
    
    /// Start the server.
    pub async fn serve(self) -> S3Result<()> {
        let addr = self.config.bind_address;
        
        info!("Starting S3 gateway server on {}", addr);
        info!("Access Key: {}", self.config.access_key);
        info!("Region: {}", self.config.region);
        
        // Create the server future
        let server_future: std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), _>> + Send>> = if self.config.enable_tls {
            // TLS configuration
            let cert_path = self.config.tls_cert_path.as_ref()
                .ok_or_else(|| S3Error::InternalError("TLS certificate path not configured".to_string()))?;
            let key_path = self.config.tls_key_path.as_ref()
                .ok_or_else(|| S3Error::InternalError("TLS private key path not configured".to_string()))?;
            
            info!("Starting server with TLS enabled");
            
            let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
                .await
                .map_err(|e| S3Error::InternalError(format!("Failed to load TLS config: {}", e)))?;
            
            Box::pin(axum_server::bind_rustls(addr, config)
                .serve(self.app.into_make_service()))
        } else {
            info!("Starting server without TLS");
            
            Box::pin(axum_server::bind(addr)
                .serve(self.app.into_make_service()))
        };
        
        // Handle graceful shutdown
        tokio::select! {
            result = server_future => {
                if let Err(e) = result {
                    return Err(S3Error::InternalError(format!("Server error: {}", e)));
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal, stopping server...");
            }
        }
        
        info!("S3 gateway server stopped");
        Ok(())
    }
}

/// Create a Walrus client from configuration.
pub async fn create_walrus_client(config: &Config) -> S3Result<Client<walrus_sui::client::SuiReadClient>> {
    // Create Walrus client
    info!("Setting up Walrus client...");
    
    // Load Walrus client configuration
    let walrus_config = if let Some(_config_path) = &config.walrus_config_path {
        // Load from file (we'll implement this later)
        create_default_walrus_config(&config)?
    } else {
        // Create default configuration from our settings
        create_default_walrus_config(&config)?
    };
    
    // Create Sui client
    info!("Setting up Sui client with endpoints: {:?}", config.walrus.sui_rpc_urls);
    let sui_client = RetriableSuiClient::new_for_rpc_urls(
        &config.walrus.sui_rpc_urls,
        ExponentialBackoffConfig::default(),
        config.walrus.request_timeout.map(std::time::Duration::from_secs),
    )
    .await
    .map_err(|e| S3Error::InternalError(format!("Failed to create Sui client: {}", e)))?;
    
    // Create SuiReadClient instead of SuiContractClient for now
    // This allows read-only access without requiring a wallet
    let sui_read_client = walrus_config
        .new_read_client(sui_client)
        .await
        .map_err(|e| S3Error::InternalError(format!("Failed to create Sui read client: {}", e)))?;
    
    // Create Walrus client with read client
    let walrus_client = Client::new_read_client_with_refresher(walrus_config, sui_read_client)
        .await
        .map_err(|e| S3Error::InternalError(format!("Failed to create Walrus client: {}", e)))?;
    
    info!("Walrus client created successfully");
    Ok(walrus_client)
}

/// Create a default Walrus client configuration from our gateway configuration.
fn create_default_walrus_config(config: &Config) -> S3Result<ClientConfig> {
    use sui_types::base_types::ObjectID;
    use std::str::FromStr;
    
    // Use actual testnet contract configuration
    let contract_config = ContractConfig {
        system_object: ObjectID::from_str("0x6c2547cbbc38025cf3adac45f63cb0a8d12ecf777cdc75a4971612bf97fdf6af")
            .map_err(|e| S3Error::InternalError(format!("Invalid system object ID: {}", e)))?,
        staking_object: ObjectID::from_str("0xbe46180321c30aab2f8b3501e24048377287fa708018a5b7c2792b35fe339ee3")
            .map_err(|e| S3Error::InternalError(format!("Invalid staking object ID: {}", e)))?,
        subsidies_object: Some(ObjectID::from_str("0xda799d85db0429765c8291c594d334349ef5bc09220e79ad397b30106161a0af")
            .map_err(|e| S3Error::InternalError(format!("Invalid subsidies object ID: {}", e)))?),
        credits_object: None, // Not used in testnet
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
    
    // Create communication configuration  
    let communication_config = ClientCommunicationConfig::default();
    
    // Create committee refresh configuration
    let refresh_config = CommitteesRefreshConfig::default();
    
    Ok(ClientConfig {
        contract_config,
        exchange_objects,
        wallet_config: config.walrus.wallet_config.clone(),
        rpc_urls: config.walrus.sui_rpc_urls.clone(),
        communication_config,
        refresh_config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::net::SocketAddr;
    
    #[tokio::test]
    async fn test_server_creation() {
        let config = Config::default();
        
        // Note: This test would need a mock Walrus client
        // let client = create_mock_walrus_client().await;
        // let server = S3GatewayServer::new(config, client).await.unwrap();
        // assert!(server.app.is_some());
    }
}
