// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use anyhow::bail;
use itertools::Itertools;
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds};
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use walrus_core::encoding::{EncodingConfig, Primary};
use walrus_sui::client::{
    contract_config::ContractConfig,
    retry_client::RetriableSuiClient,
    SuiClientError,
    SuiContractClient,
    SuiReadClient,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::common::utils::{self, LoadConfig};

/// Config for the client.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Config {
    /// The Walrus contract config.
    #[serde(flatten)]
    pub contract_config: ContractConfig,
    /// The WAL exchange object ID.
    #[serde(default)]
    pub exchange_objects: Vec<ObjectID>,
    /// Path to the wallet configuration.
    #[serde(default, deserialize_with = "utils::resolve_home_dir_option")]
    pub wallet_config: Option<PathBuf>,
    /// Configuration for the client's network communication.
    #[serde(default)]
    pub communication_config: ClientCommunicationConfig,
}

impl Config {
    /// Creates a [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client(
        &self,
        sui_client: RetriableSuiClient,
    ) -> Result<SuiReadClient, SuiClientError> {
        SuiReadClient::new(sui_client, &self.contract_config).await
    }

    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(
        &self,
        wallet: WalletContext,
        gas_budget: u64,
    ) -> Result<SuiContractClient, SuiClientError> {
        SuiContractClient::new(
            wallet,
            &self.contract_config,
            self.backoff_config().clone(),
            gas_budget,
        )
        .await
    }

    /// Creates a [`SuiContractClient`] with a wallet configured in the client config.
    ///
    /// Returns an error if the client configuration does not contain a path to a valid Sui wallet
    /// configuration.
    pub async fn new_contract_client_with_wallet_in_config(
        &self,
        gas_budget: u64,
    ) -> anyhow::Result<SuiContractClient> {
        let Some(wallet_config) = self.wallet_config.as_ref() else {
            bail!(
                "path to Sui wallet must be specified in client config through the 'wallet_config' \
                field"
            );
        };
        let wallet = WalletContext::new(wallet_config, None, None)?;
        Ok(self.new_contract_client(wallet, gas_budget).await?)
    }

    /// Returns a reference to the backoff configuration.
    pub fn backoff_config(&self) -> &ExponentialBackoffConfig {
        &self.communication_config.request_rate_config.backoff_config
    }
}

impl LoadConfig for Config {}

/// Configuration for the communication parameters of the client
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct ClientCommunicationConfig {
    /// The maximum number of open connections the client can have at any one time for writes.
    ///
    /// If `None`, the value is set by the client to optimize the write speed while avoiding running
    /// out of memory.
    pub max_concurrent_writes: Option<usize>,
    /// The maximum number of slivers the client requests in parallel. If `None`, the value is set
    /// by the client to `n - 2f`, depending on the number of shards `n`.
    pub max_concurrent_sliver_reads: Option<usize>,
    /// The maximum number of nodes the client contacts to get the blob metadata in parallel.
    pub max_concurrent_metadata_reads: usize,
    /// The maximum number of nodes the client contacts to get a blob status in parallel.
    pub max_concurrent_status_reads: Option<usize>,
    /// The maximum amount of data (in bytes) associated with concurrent requests.
    pub max_data_in_flight: Option<usize>,
    /// The configuration for the `reqwest` client.
    pub reqwest_config: ReqwestConfig,
    /// The configuration specific to each node connection.
    pub request_rate_config: RequestRateConfig,
    /// Disable the use of system proxies for communication.
    pub disable_proxy: bool,
    /// Disable the use of operating system certificates for authenticating the communication.
    pub disable_native_certs: bool,
    /// The extra time allowed for sliver writes.
    pub sliver_write_extra_time: SliverWriteExtraTime,
    /// The delay for which the client waits before storing data to ensure that storage nodes have
    /// seen the registration event.
    #[serde(rename = "registration_delay_millis")]
    #[serde_as(as = "DurationMilliSeconds")]
    pub registration_delay: Duration,
    /// The maximum total blob size allowed to store if multiple blobs are uploaded.
    pub max_total_blob_size: usize,
}

impl Default for ClientCommunicationConfig {
    fn default() -> Self {
        Self {
            disable_native_certs: true,
            max_concurrent_writes: Default::default(),
            max_concurrent_sliver_reads: Default::default(),
            max_concurrent_metadata_reads: default::max_concurrent_metadata_reads(),
            max_concurrent_status_reads: Default::default(),
            max_data_in_flight: Default::default(),
            reqwest_config: Default::default(),
            request_rate_config: Default::default(),
            disable_proxy: Default::default(),
            sliver_write_extra_time: Default::default(),
            registration_delay: Duration::from_millis(200),
            max_total_blob_size: 1024 * 1024 * 1024, // 1GiB
        }
    }
}

impl ClientCommunicationConfig {
    /// Provides a config with lower number of retries to speed up integration testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn default_for_test() -> Self {
        use walrus_utils::backoff::ExponentialBackoffConfig;

        #[cfg(msim)]
        let max_retries = Some(3);
        #[cfg(not(msim))]
        let max_retries = Some(1);
        ClientCommunicationConfig {
            disable_proxy: true,
            disable_native_certs: true,
            request_rate_config: RequestRateConfig {
                max_node_connections: 10,
                backoff_config: ExponentialBackoffConfig {
                    max_retries,
                    min_backoff: Duration::from_secs(2),
                    max_backoff: Duration::from_secs(10),
                },
            },
            ..Default::default()
        }
    }

    /// Provides a config with lower number of retries and a custom timeout to speed up integration
    /// testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn default_for_test_with_reqwest_timeout(timeout: Duration) -> Self {
        let mut config = Self::default_for_test();
        config.reqwest_config.total_timeout = timeout;
        config
    }
}

/// Communication limits in the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommunicationLimits {
    pub max_concurrent_writes: usize,
    pub max_concurrent_sliver_reads: usize,
    pub max_concurrent_metadata_reads: usize,
    pub max_concurrent_status_reads: usize,
    pub max_data_in_flight: usize,
}

impl CommunicationLimits {
    pub fn new(communication_config: &ClientCommunicationConfig, n_shards: NonZeroU16) -> Self {
        let max_concurrent_writes = communication_config
            .max_concurrent_writes
            .unwrap_or(default::max_concurrent_writes(n_shards));
        let max_concurrent_sliver_reads = communication_config
            .max_concurrent_sliver_reads
            .unwrap_or(default::max_concurrent_sliver_reads(n_shards));
        let max_concurrent_metadata_reads = communication_config.max_concurrent_metadata_reads;
        let max_concurrent_status_reads = communication_config
            .max_concurrent_status_reads
            .unwrap_or(default::max_concurrent_status_reads(n_shards));
        let max_data_in_flight = communication_config
            .max_data_in_flight
            .unwrap_or(default::max_data_in_flight());

        Self {
            max_concurrent_writes,
            max_concurrent_sliver_reads,
            max_concurrent_metadata_reads,
            max_concurrent_status_reads,
            max_data_in_flight,
        }
    }

    fn max_connections_for_request_and_blob_size(
        &self,
        request_size: NonZeroUsize,
        max_connections: usize,
    ) -> usize {
        (self.max_data_in_flight / request_size.get())
            .min(max_connections)
            .max(1)
    }

    fn sliver_size_for_blob(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
    ) -> NonZeroUsize {
        encoding_config
            .sliver_size_for_blob::<Primary>(blob_size)
            .expect("blob must not be too large to be encoded")
            .try_into()
            .expect("we assume at least a 32-bit architecture")
    }

    /// This computes the maximum number of concurrent sliver writes based on the unencoded blob
    /// size.
    ///
    /// This applies two limits:
    /// 1. The result is at most [`self.max_concurrent_writes`][Self::max_concurrent_writes].
    /// 2. The result multiplied with the primary sliver size does not exceed
    ///    `self.max_data_in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if the provided blob size is too large to be encoded, see
    /// [EncodingConfig::sliver_size_for_blob].
    pub fn max_concurrent_sliver_writes_for_blob_size(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
    ) -> usize {
        self.max_connections_for_request_and_blob_size(
            self.sliver_size_for_blob(blob_size, encoding_config),
            self.max_concurrent_writes,
        )
    }

    /// This computes the maximum number of concurrent sliver writes based on the unencoded blob
    /// size.
    ///
    /// This applies two limits:
    /// 1. The result is at most
    ///    [`self.max_concurrent_sliver_reads`][Self::max_concurrent_sliver_reads].
    /// 2. The result multiplied with the primary sliver size does not exceed
    ///    `self.max_data_in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if the provided blob size is too large to be encoded, see
    /// [EncodingConfig::sliver_size_for_blob].
    pub fn max_concurrent_sliver_reads_for_blob_size(
        &self,
        blob_size: u64,
        encoding_config: &EncodingConfig,
    ) -> usize {
        self.max_connections_for_request_and_blob_size(
            self.sliver_size_for_blob(blob_size, encoding_config),
            self.max_concurrent_sliver_reads,
        )
    }
}

/// Configuration for retries towards the storage nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct RequestRateConfig {
    /// The maximum number of connections the client can open towards each node.
    pub max_node_connections: usize,
    /// The configuration for the backoff strategy.
    pub backoff_config: ExponentialBackoffConfig,
}

impl Default for RequestRateConfig {
    fn default() -> Self {
        Self {
            max_node_connections: 10,
            backoff_config: Default::default(),
        }
    }
}

/// Configuration for the parameters of the `reqwest` client.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct ReqwestConfig {
    /// Total request timeout, applied from when the request starts connecting until the response
    /// body has finished.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "total_timeout_millis")]
    pub total_timeout: Duration,
    /// Timeout for idle sockets to be kept alive. Pass `None` to disable.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    #[serde(rename = "pool_idle_timeout_millis")]
    pub pool_idle_timeout: Option<Duration>,
    /// Timeout for receiving an acknowledgement of the keep-alive ping.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "http2_keep_alive_timeout_millis")]
    pub http2_keep_alive_timeout: Duration,
    /// Ping every such interval to keep the connection alive.
    #[serde_as(as = "Option<DurationMilliSeconds>")]
    #[serde(rename = "http2_keep_alive_interval_millis")]
    pub http2_keep_alive_interval: Option<Duration>,
    /// Sets whether HTTP2 keep-alive should apply while the connection is idle.
    pub http2_keep_alive_while_idle: bool,
}

impl Default for ReqwestConfig {
    fn default() -> Self {
        Self {
            total_timeout: default::total_timeout(),
            pool_idle_timeout: default::pool_idle_timeout(),
            http2_keep_alive_timeout: default::http2_keep_alive_timeout(),
            http2_keep_alive_interval: default::http2_keep_alive_interval(),
            http2_keep_alive_while_idle: default::http2_keep_alive_while_idle(),
        }
    }
}

impl ReqwestConfig {
    /// Applies the configurations in [`Self`] to the provided client builder.
    pub fn apply(&self, builder: ClientBuilder) -> ClientBuilder {
        builder
            .timeout(self.total_timeout)
            .pool_idle_timeout(self.pool_idle_timeout)
            .http2_prior_knowledge()
            .http2_keep_alive_timeout(self.http2_keep_alive_timeout)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .http2_keep_alive_while_idle(self.http2_keep_alive_while_idle)
    }
}

/// Returns the default paths for the Walrus configuration file.
pub fn default_configuration_paths() -> Vec<PathBuf> {
    const WALRUS_CONFIG_FILE_NAMES: [&str; 2] = ["client_config.yaml", "client_config.yml"];
    let mut directories = vec![PathBuf::from(".")];
    if let Ok(xdg_config_dir) = env::var("XDG_CONFIG_HOME") {
        directories.push(xdg_config_dir.into());
    }
    if let Some(home_dir) = home::home_dir() {
        directories.push(home_dir.join(".config").join("walrus"));
        directories.push(home_dir.join(".walrus"));
    }
    directories
        .into_iter()
        .cartesian_product(WALRUS_CONFIG_FILE_NAMES)
        .map(|(directory, file_name)| directory.join(file_name))
        .collect()
}

pub(crate) mod default {
    use std::{num::NonZeroU16, time::Duration};

    use walrus_core::bft;

    pub fn max_concurrent_writes(n_shards: NonZeroU16) -> usize {
        // No limit as we anyway want to store as many slivers as possible.
        n_shards.get().into()
    }

    pub fn max_concurrent_sliver_reads(n_shards: NonZeroU16) -> usize {
        // Read up to `n-2f` slivers concurrently to avoid wasted work on the storage nodes.
        (n_shards.get() - 2 * bft::max_n_faulty(n_shards)).into()
    }

    pub fn max_concurrent_status_reads(n_shards: NonZeroU16) -> usize {
        // No limit as we need 2f+1 responses and requests are small.
        n_shards.get().into()
    }

    pub fn max_concurrent_metadata_reads() -> usize {
        3
    }

    /// This corresponds to 100Mb, i.e., 1 second on a 100 Mbps connection.
    pub fn max_data_in_flight() -> usize {
        12_500_000
    }

    /// Allows for enough time to transfer big slivers on the other side of the world.
    pub fn total_timeout() -> Duration {
        Duration::from_secs(30)
    }

    /// Disabled by default, i.e., connections are kept alive.
    pub fn pool_idle_timeout() -> Option<Duration> {
        None
    }

    /// Close the connection if the answer to the ping is not received within this deadline.
    pub fn http2_keep_alive_timeout() -> Duration {
        Duration::from_secs(5)
    }

    /// Ping every 30 secs.
    pub fn http2_keep_alive_interval() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    /// Keep-alive pings are sent to idle connections.
    pub fn http2_keep_alive_while_idle() -> bool {
        true
    }
}

/// The additional time allowed to sliver writes, to allow for more nodes to receive them.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(default)]
pub struct SliverWriteExtraTime {
    /// The multiplication factor for the time it took to store n-f sliver.
    pub factor: f64,
    /// The minimum extra time.
    #[serde(rename = "base_millis")]
    #[serde_as(as = "DurationMilliSeconds")]
    pub base: Duration,
}

impl SliverWriteExtraTime {
    /// Returns the extra time for the given time.
    ///
    /// The extra time is computed as `store_time * factor + base`.
    pub fn extra_time(&self, store_time: Duration) -> Duration {
        let extra_time = Duration::from_nanos((store_time.as_nanos() as f64 * self.factor) as u64);
        self.base + extra_time
    }
}

impl Default for SliverWriteExtraTime {
    fn default() -> Self {
        Self {
            factor: 0.5,                      // 1/2 of the time it took to store n-f slivers.
            base: Duration::from_millis(500), // +0.5s every time.
        }
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rand::{rngs::StdRng, SeedableRng as _};
    use walrus_test_utils::Result as TestResult;

    use super::*;

    /// Serializes a default config to the example file when tests are run.
    ///
    /// This test ensures that the `client_config_example.yaml` is kept in sync with the config
    /// struct in this file.
    #[test]
    fn check_and_update_example_config() -> TestResult {
        const EXAMPLE_CONFIG_PATH: &str = "client_config_example.yaml";

        let mut rng = StdRng::seed_from_u64(42);
        let contract_config = ContractConfig {
            system_object: ObjectID::random_from_rng(&mut rng),
            staking_object: ObjectID::random_from_rng(&mut rng),
        };
        let config = Config {
            contract_config,
            exchange_objects: vec![
                ObjectID::random_from_rng(&mut rng),
                ObjectID::random_from_rng(&mut rng),
            ],
            wallet_config: None,
            communication_config: Default::default(),
        };

        let serialized = serde_yaml::to_string(&config).unwrap();
        let configs_are_in_sync = std::fs::read_to_string(EXAMPLE_CONFIG_PATH)? == serialized;
        std::fs::write(EXAMPLE_CONFIG_PATH, serialized.clone()).unwrap();
        assert!(
            configs_are_in_sync,
            "example configuration was out of sync; was updated automatically"
        );

        Ok(())
    }

    #[test]
    fn parses_minimal_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
        "};

        let _: Config = serde_yaml::from_str(yaml)?;

        Ok(())
    }

    #[test]
    fn parses_no_exchange_object_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects: []
        "};

        let config: Config = serde_yaml::from_str(yaml)?;
        assert!(config.exchange_objects.is_empty());

        Ok(())
    }

    #[test]
    fn parses_single_exchange_object_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
        "};

        let config: Config = serde_yaml::from_str(yaml)?;
        assert_eq!(config.exchange_objects.len(), 1);

        Ok(())
    }

    #[test]
    fn parses_multiple_exchange_objects_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
                - 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
        "};

        let config: Config = serde_yaml::from_str(yaml)?;
        assert_eq!(config.exchange_objects.len(), 2);

        Ok(())
    }

    #[test]
    fn parses_partial_config_file() -> TestResult {
        let yaml = indoc! {"
            system_object: 0xa2637d13d171b278eadfa8a3fbe8379b5e471e1f3739092e5243da17fc8090eb
            staking_object: 0xca7cf321e47a1fc9bfd032abc31b253f5063521fd5b4c431f2cdd3fee1b4ec00
            exchange_objects:
                - 0xa9b00f69d3b033e7b64acff2672b54fbb7c31361954251e235395dea8bd6dcac
            wallet_config: path/to/wallet
            communication_config:
                max_concurrent_writes: 42
                max_data_in_flight: 1000
                reqwest_config:
                    total_timeout_millis: 30000
                    http2_keep_alive_while_idle: false
                request_rate_config:
                    max_node_connections: 10
                    backoff_config:
                        min_backoff_millis: 1000
                disable_proxy: false
                sliver_write_extra_time:
                    factor: 0.5
                max_total_blob_size: 1073741824
        "};

        let _: Config = serde_yaml::from_str(yaml)?;

        Ok(())
    }
}
