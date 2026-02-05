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

/// Coinbase API URL for fetching WAL price
const COINBASE_API_URL: &str = "https://api.coinbase.com/v2/prices/WAL-USD/spot";

/// Binance API URL for fetching WAL price
const BINANCE_API_URL: &str = "https://api.binance.com/api/v3/ticker/price?symbol=WALUSDC";

/// Pyth Hermes API URL for fetching WAL price
const PYTH_HERMES_API_URL: &str = concat!(
    "https://hermes.pyth.network/v2/updates/price/latest?ids[]=",
    "eba0732395fae9dec4bae12e52760b35fc1c5671e2da8b449c9af4efe5d54341"
);

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
                    "Failed to parse price from CoinGecko response: {}",
                    json_response
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

/// Helper struct for fetching prices from Coinbase with metrics
#[derive(Clone)]
struct CoinbasePriceFetcher {
    metrics: Arc<NodeMetricSet>,
}

impl CoinbasePriceFetcher {
    fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self { metrics }
    }
}

impl WalPriceFetcher for CoinbasePriceFetcher {
    fn source(&self) -> &str {
        "coinbase"
    }

    fn fetch(&self) -> Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>> {
        Box::pin(async move {
            let client = reqwest::Client::new();
            let response = client
                .get(COINBASE_API_URL)
                .header(reqwest::header::USER_AGENT, "Walrus (walrus.xyz)")
                .send()
                .await?;

            let json_response: serde_json::Value = response.json().await?;

            let price = json_response
                .get("data")
                .and_then(|v| v.get("amount"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(anyhow::anyhow!(
                    "Failed to parse price from Coinbase response: {}",
                    json_response
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

/// Helper struct for fetching prices from Binance with metrics
#[derive(Clone)]
struct BinancePriceFetcher {
    metrics: Arc<NodeMetricSet>,
}

impl BinancePriceFetcher {
    fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self { metrics }
    }
}

impl WalPriceFetcher for BinancePriceFetcher {
    fn source(&self) -> &str {
        "binance"
    }

    fn fetch(&self) -> Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>> {
        Box::pin(async move {
            let client = reqwest::Client::new();
            let response = client
                .get(BINANCE_API_URL)
                .header(reqwest::header::USER_AGENT, "Walrus (walrus.xyz)")
                .send()
                .await?;

            let json_response: serde_json::Value = response.json().await?;

            let price = json_response
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or(anyhow::anyhow!(
                    "Failed to parse price from Binance response: {}",
                    json_response
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

/// Helper struct for fetching prices from Pyth Hermes with metrics
#[derive(Clone)]
struct PythHermesPriceFetcher {
    metrics: Arc<NodeMetricSet>,
}

impl PythHermesPriceFetcher {
    fn new(metrics: Arc<NodeMetricSet>) -> Self {
        Self { metrics }
    }
}

impl WalPriceFetcher for PythHermesPriceFetcher {
    fn source(&self) -> &str {
        "pyth_hermes"
    }

    fn fetch(&self) -> Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>> {
        Box::pin(async move {
            let client = reqwest::Client::new();
            let response = client
                .get(PYTH_HERMES_API_URL)
                .header(reqwest::header::USER_AGENT, "Walrus (walrus.xyz)")
                .send()
                .await?;

            let json_response: serde_json::Value = response.json().await?;

            let (price_mantissa, expo) = json_response
                .get("parsed")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|parsed| parsed.get("price"))
                .and_then(|price_obj| {
                    Some((
                        price_obj.get("price")?.as_str()?.parse::<f64>().ok()?,
                        price_obj.get("expo")?.as_i64()?.try_into().ok()?,
                    ))
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to parse price from Pyth Hermes response: {}",
                        json_response
                    )
                })?;
            let price = price_mantissa * 10_f64.powi(expo);

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

/// Gets the current WAL price based on the list of fetched prices
fn get_current_wal_price(prices: Vec<f64>) -> Option<f64> {
    // We are currently using the median price as the current WAL price, but the algorithm here
    // can be more sophisticated when we have more data points. We can detect abnormal prices and
    // outliers to make the WAL price more accurate.
    calculate_median(prices)
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
        let fetchers: Vec<Box<dyn WalPriceFetcher>> = vec![
            Box::new(CoinGeckoPriceFetcher::new(metrics.clone())),
            Box::new(CoinbasePriceFetcher::new(metrics.clone())),
            Box::new(BinancePriceFetcher::new(metrics.clone())),
            Box::new(PythHermesPriceFetcher::new(metrics.clone())),
        ];

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
                            // It is not guaranteed that all fetchers will work in all the places
                            // around the world.
                            tracing::debug!(
                                "Fetcher '{}' failed to fetch WAL price: {}",
                                fetcher.source(),
                                e
                            );
                        }
                    }
                }

                if let Some(price) = get_current_wal_price(prices.clone()) {
                    tracing::info!(
                        "Calculated WAL price from {} source(s): ${:.6}",
                        prices.len(),
                        price
                    );

                    // Update current price
                    *current_price.write().await = Some(price);

                    // Update aggregate metric
                    metrics
                        .current_monitored_wal_price
                        .with_label_values(&["aggregated_price"])
                        .set(price);
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

    /// Expands to a #[tokio::test] that fetches WAL price from a source and checks that the
    /// associated metric was updated correctly.
    /// Note: this test is ignored by default because it hits external APIs. They will be run
    /// in CI to make sure that the output from these data sources are parsed correctly.
    macro_rules! test_fetch_from_source {
        ($test_name:ident, $fetcher_type:ty, $source:expr) => {
            #[tokio::test]
            #[ignore = "ignore tests that hit external APIs"]
            async fn $test_name() {
                use walrus_utils::metrics::Registry;

                let _ = tracing_subscriber::fmt::try_init();

                let registry = Registry::default();
                let metrics = Arc::new(NodeMetricSet::new(&registry));

                let fetcher = <$fetcher_type>::new(metrics.clone());
                let result = fetcher.fetch().await;

                assert!(
                    result.is_ok(),
                    "Failed to fetch WAL price from {}: {:?}",
                    $source,
                    result.err()
                );

                let price = result.unwrap();

                assert!(price > 0.0, "WAL price should be positive, got: {}", price);
                assert!(
                    (0.001..=5.0).contains(&price),
                    "WAL price seems unreasonable: ${}",
                    price
                );

                let metric_value = metrics
                    .current_monitored_wal_price
                    .with_label_values(&[$source])
                    .get();
                assert_eq!(
                    metric_value, price,
                    "Metric value should match fetched price"
                );

                tracing::info!(
                    "Successfully fetched WAL price from {}: ${:.6}",
                    $source,
                    price
                );
            }
        };
    }

    test_fetch_from_source!(
        test_fetch_from_coingecko,
        CoinGeckoPriceFetcher,
        "coingecko"
    );
    test_fetch_from_source!(test_fetch_from_coinbase, CoinbasePriceFetcher, "coinbase");
    test_fetch_from_source!(
        test_fetch_from_pyth_hermes,
        PythHermesPriceFetcher,
        "pyth_hermes"
    );

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
