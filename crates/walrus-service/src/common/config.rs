// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Common configuration module.

use std::{iter::once, sync::Arc, time::Duration};

use indexmap::IndexSet;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};
use walrus_sui::{
    client::{
        SuiClientError,
        SuiClientMetricSet,
        SuiContractClient,
        SuiReadClient,
        contract_config::ContractConfig,
        rpc_config::RpcFallbackConfig,
    },
    config::WalletConfig,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

/// Sui-specific configuration for Walrus
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SuiConfig {
    /// HTTP URL of the Sui full-node RPC endpoint (including scheme). This is used in the event
    /// processor and some other read operations; for all write operations, the RPC URL from the
    /// wallet is used.
    pub rpc: String,
    /// Configuration of the contract packages and shared objects.
    #[serde(flatten)]
    pub contract_config: ContractConfig,
    /// Interval with which events are polled, in milliseconds.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(
        rename = "event_polling_interval_millis",
        default = "defaults::polling_interval"
    )]
    pub event_polling_interval: Duration,
    /// Location of the wallet config.
    pub wallet_config: WalletConfig,
    /// The configuration for the backoff strategy used for retries.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub backoff_config: ExponentialBackoffConfig,
    /// Gas budget for transactions.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub gas_budget: Option<u64>,
    /// The config for rpc fallback.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub rpc_fallback_config: Option<RpcFallbackConfig>,
    /// Additional RPC endpoints to use for the event processor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_rpc_endpoints: Vec<String>,
    /// The request timeout for communicating with Sui network.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub request_timeout: Option<Duration>,
}

/// Combines the main RPC URL with additional RPC endpoints, ensuring uniqueness of each URL string.
pub fn combine_rpc_urls(rpc: &str, additional_rpc_endpoints: &[String]) -> Vec<String> {
    once(rpc.to_string())
        .chain(additional_rpc_endpoints.iter().cloned())
        .collect::<IndexSet<String>>()
        .into_iter()
        .collect::<Vec<_>>()
}

impl SuiConfig {
    /// Creates a new [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client_with_refresher(&self) -> Result<SuiReadClient, SuiClientError> {
        let combined_rpc_urls = combine_rpc_urls(&self.rpc, &self.additional_rpc_endpoints);
        SuiReadClient::new_for_rpc_urls(
            &combined_rpc_urls,
            &self.contract_config,
            self.backoff_config.clone(),
        )
        .await
    }

    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(
        &self,
        metrics: Option<Arc<SuiClientMetricSet>>,
    ) -> Result<SuiContractClient, SuiClientError> {
        if let Some(metrics) = metrics {
            SuiContractClient::new_from_wallet_with_metrics(
                WalletConfig::load_wallet_context(Some(&self.wallet_config), self.request_timeout)?,
                &self.contract_config,
                self.backoff_config.clone(),
                self.gas_budget,
                metrics,
            )
            .await
        } else {
            SuiContractClient::new(
                WalletConfig::load_wallet_context(Some(&self.wallet_config), self.request_timeout)?,
                &self.contract_config,
                self.backoff_config.clone(),
                self.gas_budget,
            )
            .await
        }
    }
}

impl From<&SuiConfig> for SuiReaderConfig {
    fn from(config: &SuiConfig) -> Self {
        Self {
            rpc: config.rpc.clone(),
            contract_config: config.contract_config.clone(),
            event_polling_interval: config.event_polling_interval,
            backoff_config: config.backoff_config.clone(),
            rpc_fallback_config: config.rpc_fallback_config.clone(),
            additional_rpc_endpoints: config.additional_rpc_endpoints.clone(),
            request_timeout: config.request_timeout,
        }
    }
}

/// Reader-specific configuration for Sui.
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SuiReaderConfig {
    /// HTTP URL of the Sui full-node RPC endpoint (including scheme). This is used in the event
    /// processor and some other read operations; for all write operations, the RPC URL from the
    /// wallet is used.
    pub rpc: String,
    /// Configuration of the contract packages and shared objects.
    #[serde(flatten)]
    pub contract_config: ContractConfig,
    /// Interval with which events are polled, in milliseconds.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(
        rename = "event_polling_interval_millis",
        default = "defaults::polling_interval"
    )]
    pub event_polling_interval: Duration,
    /// The configuration for the backoff strategy used for retries.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub backoff_config: ExponentialBackoffConfig,
    /// The URL of the checkpoint download fallback endpoint.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub rpc_fallback_config: Option<RpcFallbackConfig>,
    /// Additional RPC endpoints to use for failover.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_rpc_endpoints: Vec<String>,
    /// The request timeout for communicating with Sui network.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub request_timeout: Option<Duration>,
}

impl SuiReaderConfig {
    /// Creates a new [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client_with_refresher(&self) -> Result<SuiReadClient, SuiClientError> {
        let combined_rpc_urls = combine_rpc_urls(&self.rpc, &self.additional_rpc_endpoints);
        SuiReadClient::new_for_rpc_urls(
            &combined_rpc_urls,
            &self.contract_config,
            self.backoff_config.clone(),
        )
        .await
    }
}

/// Shared configuration defaults.
pub mod defaults {
    use super::*;

    /// Default polling interval in milliseconds.
    pub const POLLING_INTERVAL_MS: u64 = 400;

    /// Returns the default polling interval.
    pub fn polling_interval() -> Duration {
        Duration::from_millis(POLLING_INTERVAL_MS)
    }

    /// Returns true iff the value is the default and we don't run in test mode.
    pub fn is_default<T: PartialEq + Default>(t: &T) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t == &T::default()
    }

    /// Returns true iff the value is `None` and we don't run in test mode.
    pub fn is_none<T>(t: &Option<T>) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t.is_none()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_combine_rpc_urls() {
        let rpc = "http://localhost:1".to_string();
        let rpc_urls = vec![
            "http://localhost:2".to_string(),
            "http://localhost:2".to_string(),
            "http://localhost:3".to_string(),
            "http://localhost:1".to_string(),
            "http://localhost:3".to_string(),
        ];

        // Check that the duplicates are removed and the order is preserved.
        let combined = super::combine_rpc_urls(&rpc, &rpc_urls);
        assert_eq!(combined.len(), 3);
        assert_eq!(combined[0], "http://localhost:1");
        assert_eq!(combined[1], "http://localhost:2");
        assert_eq!(combined[2], "http://localhost:3");
    }
}
