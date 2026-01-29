// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use tokio::{sync::RwLock, task::JoinHandle};

use crate::node::metrics::NodeMetricSet;

/// CoinGecko API URL for fetching WAL price
const COINGECKO_API_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=walrus-2&vs_currencies=usd";

/// Helper struct for fetching prices from CoinGecko with metrics
#[derive(Clone)]
struct CoinGeckoPriceFetcher {
    metrics: Arc<NodeMetricSet>,
}

impl CoinGeckoPriceFetcher {
    fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self { metrics }
    }

    /// Fetches WAL price from CoinGecko API
    async fn fetch(&self) -> Result<f64, anyhow::Error> {
        let client = reqwest::Client::new();
        let response = client
            .get(COINGECKO_API_URL)
            .header(reqwest::header::USER_AGENT, "Walrus (walrus.xyz)")
            .send()
            .await?;

        let json_response: serde_json::Value = response.json().await?;

        let price = json_response
            .get("walrus-2")
            .and_then(|v| v.get("usd"))
            .and_then(|v| v.as_f64())
            .ok_or(anyhow::anyhow!(
                "Failed to parse price from CoinGecko response"
            ))?;

        tracing::debug!("Fetched WAL price from CoinGecko: ${:.6}", price);

        // Update metrics
        self.metrics
            .current_monitored_wal_price
            .with_label_values(&["coingecko"])
            .set(price);

        Ok(price)
    }
}

/// Configuration for the WAL price monitor
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WalPriceMonitorConfig {
    /// How often to check the WAL price
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "check_interval_secs")]
    pub check_interval: Duration,
}

impl Default for WalPriceMonitorConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(60), // Default: check every minute
        }
    }
}

/// Monitors the WAL token price periodically
pub struct WalPriceMonitor {
    /// Current price (if available)
    _current_price: Arc<RwLock<Option<f64>>>,
    /// Background task handle
    task_handle: JoinHandle<()>,
}

impl WalPriceMonitor {
    /// Creates and starts a new WAL price monitor
    pub fn start(config: WalPriceMonitorConfig, metrics: Arc<NodeMetricSet>) -> Self {
        let current_price = Arc::new(RwLock::new(None));

        tracing::info!(
            "WAL price monitor started with check interval: {:?}",
            config.check_interval
        );

        let task_handle =
            Self::spawn_monitoring_task(config, current_price.clone(), metrics.clone());

        Self {
            _current_price: current_price,
            task_handle,
        }
    }

    /// Spawns the background monitoring task
    fn spawn_monitoring_task(
        config: WalPriceMonitorConfig,
        current_price: Arc<RwLock<Option<f64>>>,
        metrics: Arc<NodeMetricSet>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Create the list of WAL price fetchers
            let coin_gecko_fetcher = CoinGeckoPriceFetcher::new(metrics);

            loop {
                interval.tick().await;

                match coin_gecko_fetcher.fetch().await {
                    Ok(price) => {
                        *current_price.write().await = Some(price);
                    }
                    Err(e) => {
                        tracing::error!("Failed to fetch WAL price: {}", e);
                    }
                }
            }
        })
    }
}

impl std::fmt::Debug for WalPriceMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalPriceMonitor")
            .field("metrics", &"<NodeMetricSet>")
            .field("task_handle", &"<JoinHandle>")
            .finish()
    }
}

impl Drop for WalPriceMonitor {
    fn drop(&mut self) {
        // Abort the task on drop
        self.task_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The goal of this test is to catch any issues with the CoinGecko API or the price fetching
    // logic itself.
    #[tokio::test]
    async fn test_fetch_from_coingecko() {
        // This test makes a real API call to CoinGecko
        use walrus_utils::metrics::Registry;

        // Set up test environment
        let registry = Registry::default();
        let metrics = Arc::new(NodeMetricSet::new(&registry));

        // Create CoinGecko fetcher and fetch the price
        let fetcher = CoinGeckoPriceFetcher::new(metrics.clone());
        let result = fetcher.fetch().await;

        // Assert that the fetch was successful
        assert!(
            result.is_ok(),
            "Failed to fetch WAL price from CoinGecko: {:?}",
            result.err()
        );

        let price = result.unwrap();

        // Assert that the price is a positive number
        assert!(price > 0.0, "WAL price should be positive, got: {}", price);

        // Assert that the price is within a reasonable range (e.g., between $0.001 and $1000)
        assert!(
            (0.001..=100.0).contains(&price),
            "WAL price seems unreasonable: ${}",
            price
        );

        // Verify that the metric was updated
        let metric_value = metrics
            .current_monitored_wal_price
            .with_label_values(&["coingecko"])
            .get();
        assert_eq!(
            metric_value, price,
            "Metric value should match fetched price"
        );

        tracing::info!("Successfully fetched WAL price: ${:.6}", price);
    }
}
