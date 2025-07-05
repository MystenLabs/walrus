// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client module for managing Sui RPC and client connections with retry capabilities.
use std::{sync::Arc, time::Duration};

use anyhow::Result;
use walrus_sui::client::{
    SuiClientMetricSet,
    retry_client::{
        RetriableRpcClient,
        RetriableSuiClient,
        retriable_rpc_client::LazyFallibleRpcClientBuilder,
        retriable_sui_client::LazySuiClientBuilder,
    },
    rpc_config::RpcFallbackConfig,
};
use walrus_utils::{backoff::ExponentialBackoffConfig, metrics::Registry};

use crate::event::event_processor::config::SuiClientSet;

/// A manager for handling Sui RPC and client connections with retry capabilities.
/// This struct manages both the RPC client and Sui client instances, providing
/// a unified interface for interacting with the Sui network.
#[derive(Clone)]
pub struct ClientManager {
    /// The RPC client with retry capabilities for making RPC calls to the Sui network.
    pub rpc_client: RetriableRpcClient,
    /// The Sui client with retry capabilities for making Sui-specific API calls.
    pub sui_client: RetriableSuiClient,
}

impl std::fmt::Debug for ClientManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientManager")
            .field("client", &self.rpc_client)
            .field("sui_client", &"RetriableSuiClient")
            .finish()
    }
}

impl ClientManager {
    /// Creates a new instance of the client manager.
    ///
    /// # Arguments
    ///
    /// * `rest_urls` - A slice of REST API URLs to connect to
    /// * `request_timeout` - The timeout duration for requests
    /// * `rpc_fallback_config` - Optional configuration for RPC fallback behavior
    /// * `registry` - The metrics registry for tracking client metrics
    ///
    /// # Returns
    ///
    /// A Result containing either the new ClientManager instance or an
    /// error if initialization fails.
    pub async fn new(
        rest_urls: &[String],
        request_timeout: Duration,
        rpc_fallback_config: Option<&RpcFallbackConfig>,
        registry: &Registry,
    ) -> Result<Self, anyhow::Error> {
        let metrics = Arc::new(SuiClientMetricSet::new(registry));
        let lazy_client_builders = rest_urls
            .iter()
            .map(|url| LazyFallibleRpcClientBuilder::Url {
                rpc_url: url.clone(),
                ensure_experimental_rest_endpoint: true,
            })
            .collect::<Vec<_>>();

        let client = RetriableRpcClient::new(
            lazy_client_builders,
            request_timeout,
            ExponentialBackoffConfig::default(),
            rpc_fallback_config.cloned(),
            Some(metrics.clone()),
        )
        .await?;

        let sui_client = RetriableSuiClient::new(
            rest_urls
                .iter()
                .map(|rpc_url| LazySuiClientBuilder::new(rpc_url, Some(request_timeout)))
                .collect(),
            ExponentialBackoffConfig::default(),
        )
        .await?;

        Ok(Self {
            rpc_client: client,
            sui_client,
        })
    }

    /// Returns a reference to the RPC client.
    ///
    /// # Returns
    ///
    /// A reference to the RetriableRpcClient instance.
    pub fn get_client(&self) -> &RetriableRpcClient {
        &self.rpc_client
    }

    /// Returns a reference to the Sui client.
    ///
    /// # Returns
    ///
    /// A reference to the RetriableSuiClient instance.
    pub fn get_sui_client(&self) -> &RetriableSuiClient {
        &self.sui_client
    }

    /// Converts the ClientManager into a SuiClientSet.
    ///
    /// # Returns
    ///
    /// A SuiClientSet containing the RPC and Sui clients.
    pub fn into_client_set(&self) -> SuiClientSet {
        SuiClientSet::new(self.sui_client.clone(), self.rpc_client.clone())
    }
}
