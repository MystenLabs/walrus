// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use tokio::{sync::RwLock, task::JoinHandle};

use crate::node::metrics::NodeMetricSet;

/// CoinGecko API URL for fetching WAL price
const COINGECKO_API_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=walrus-2&vs_currencies=usd";

/// Trait for fetching WAL token prices from different data sources
trait WalPriceFetcher: Send + Sync {
    /// Returns the name of the price source
    fn source(&self) -> &str;

    /// Fetches the current WAL price in USD
    fn fetch(&self) -> Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>>;
}

/// Helper struct for fetching prices from CoinGecko with metrics
#[derive(Clone)]
struct CoinGeckoPriceFetcher {
    metrics: Arc<NodeMetricSet>,
}

impl CoinGeckoPriceFetcher {
    fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self { metrics }
    }
}

impl WalPriceFetcher for CoinGeckoPriceFetcher {
    fn source(&self) -> &str {
        "coingecko"
    }

    fn fetch(&self) -> Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>> {
        Box::pin(async move {
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

            tracing::debug!("Fetched WAL price from {}: ${:.6}", self.source(), price);

            // Update metrics
            self.metrics
                .current_monitored_wal_price
                .with_label_values(&[self.source()])
                .set(price);

            Ok(price)
        })
    }
}

/// Configuration for the WAL price monitor
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WalPriceMonitorConfig {
    /// Whether to enable WAL price monitoring
    pub enable_wal_price_monitor: bool,
    /// How often to check the WAL price
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "check_interval_secs")]
    pub check_interval: Duration,
}

impl Default for WalPriceMonitorConfig {
    fn default() -> Self {
        Self {
            enable_wal_price_monitor: false,
            check_interval: Duration::from_secs(600), // Default: check every 10 minutes
        }
    }
}

/// Calculates the median value from a list of prices
fn calculate_median(mut prices: Vec<f64>) -> Option<f64> {
    if prices.is_empty() {
        return None;
    }

    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let len = prices.len();
    let median = if len.is_multiple_of(2) {
        // Even number of elements: average of the two middle values
        (prices[len / 2 - 1] + prices[len / 2]) / 2.0
    } else {
        // Odd number of elements: middle value
        prices[len / 2]
    };

    Some(median)
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

        // Create the list of WAL price fetchers
        let fetchers: Vec<Box<dyn WalPriceFetcher>> =
            vec![Box::new(CoinGeckoPriceFetcher::new(metrics.clone()))];

        tracing::info!(
            "WAL price monitor started with {} fetcher(s) and check interval: {:?}",
            fetchers.len(),
            config.check_interval
        );

        let task_handle =
            Self::spawn_monitoring_task(config, current_price.clone(), fetchers, metrics.clone());

        Self {
            _current_price: current_price,
            task_handle,
        }
    }

    /// Spawns the background monitoring task
    fn spawn_monitoring_task(
        config: WalPriceMonitorConfig,
        current_price: Arc<RwLock<Option<f64>>>,
        fetchers: Vec<Box<dyn WalPriceFetcher>>,
        metrics: Arc<NodeMetricSet>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Fetch prices from all sources concurrently
                let fetch_tasks: Vec<_> = fetchers.iter().map(|f| f.fetch()).collect();
                let results = futures::future::join_all(fetch_tasks).await;

                // Collect successful price fetches
                let mut prices = Vec::new();
                for (fetcher, result) in fetchers.iter().zip(results.into_iter()) {
                    match result {
                        Ok(price) => {
                            tracing::debug!(
                                "Fetcher '{}' returned price: ${:.6}",
                                fetcher.source(),
                                price
                            );
                            prices.push(price);
                        }
                        Err(e) => {
                            tracing::error!(
                                "Fetcher '{}' failed to fetch WAL price: {}",
                                fetcher.source(),
                                e
                            );
                        }
                    }
                }

                // Calculate median if we have at least one successful fetch
                if let Some(median_price) = calculate_median(prices.clone()) {
                    tracing::info!(
                        "Calculated median WAL price from {} source(s): ${:.6}",
                        prices.len(),
                        median_price
                    );

                    // Update current price
                    *current_price.write().await = Some(median_price);

                    // Update aggregate metric
                    metrics
                        .current_monitored_wal_price
                        .with_label_values(&["median"])
                        .set(median_price);
                } else {
                    tracing::warn!("No successful price fetches from any source");
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

    #[test]
    fn test_calculate_median() {
        // Test with empty list
        assert_eq!(calculate_median(vec![]), None);

        // Test with single value
        assert_eq!(calculate_median(vec![5.0]), Some(5.0));

        // Test with odd number of values
        assert_eq!(calculate_median(vec![1.0, 3.0, 5.0]), Some(3.0));
        assert_eq!(calculate_median(vec![5.0, 1.0, 3.0]), Some(3.0)); // Unsorted

        // Test with even number of values
        assert_eq!(calculate_median(vec![1.0, 2.0, 3.0, 4.0]), Some(2.5));
        assert_eq!(calculate_median(vec![4.0, 1.0, 3.0, 2.0]), Some(2.5)); // Unsorted

        // Test with duplicate values
        assert_eq!(calculate_median(vec![2.0, 2.0, 2.0]), Some(2.0));
    }
}
