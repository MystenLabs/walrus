// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Storage client configuration module.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr as _,
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_with::{
    base64::Base64,
    de::DeserializeAsWrap,
    ser::SerializeAsWrap,
    serde_as,
    DeserializeAs,
    DurationMilliSeconds,
    DurationSeconds,
    SerializeAs,
};
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::SuiAddress;
use walrus_core::{
    keys::{KeyPairParseError, NetworkKeyPair, ProtocolKeyPair, SupportedKeyPair, TaggedKeyPair},
    messages::ProofOfPossession,
};
use walrus_sui::{
    client::{contract_config::ContractConfig, SuiClientError, SuiContractClient, SuiReadClient},
    types::{move_structs::VotingParams, NetworkAddress, NodeMetadata, NodeRegistrationParams},
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use super::storage::DatabaseConfig;
use crate::{
    common::utils::{self, LoadConfig},
    node::events::EventProcessorConfig,
};

/// Configuration of a Walrus storage node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct StorageNodeConfig {
    /// The name of the storage node that is set in the staking pool on chain.
    pub name: String,
    /// Directory in which to persist the database.
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub storage_path: PathBuf,
    /// File path to the blocklist.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub blocklist_path: Option<PathBuf>,
    /// Optional "config" to tune storage database.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub db_config: DatabaseConfig,
    /// Key pair used in Walrus protocol messages.
    #[serde_as(as = "PathOrInPlace<Base64>")]
    pub protocol_key_pair: PathOrInPlace<ProtocolKeyPair>,
    /// Key pair used to authenticate nodes in network communication.
    #[serde_as(as = "PathOrInPlace<Base64>")]
    pub network_key_pair: PathOrInPlace<NetworkKeyPair>,
    /// The host name or public IP address of the node. This is used in the on-chain data and for
    /// generating self-signed certificates if TLS is enabled.
    pub public_host: String,
    /// The port on which the storage node will serve requests.
    pub public_port: u16,
    /// Socket address on which the Prometheus server should export its metrics.
    #[serde(default = "defaults::metrics_address")]
    pub metrics_address: SocketAddr,
    /// Socket address on which the REST API listens.
    #[serde(default = "defaults::rest_api_address")]
    pub rest_api_address: SocketAddr,
    /// Duration for which to wait for connections to close before shutting down.
    ///
    /// Set explicitly to None to wait indefinitely.
    #[serde(
        default = "defaults::rest_graceful_shutdown_period_secs",
        skip_serializing_if = "defaults::is_none",
        with = "serde_with::rust::double_option"
    )]
    pub rest_graceful_shutdown_period_secs: Option<Option<u64>>,
    /// Sui config for the node
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub sui: Option<SuiConfig>,
    /// Configuration of blob synchronization
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub blob_recovery: BlobRecoveryConfig,
    /// Configuration for TLS of the rest API.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub tls: TlsConfig,
    /// Configuration for shard synchronization.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub shard_sync_config: ShardSyncConfig,
    /// Configuration for the event processor.
    ///
    /// This is ignored if `use_legacy_event_provider` is set to `true`.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub event_processor_config: EventProcessorConfig,
    /// Use the legacy event provider.
    ///
    /// This is deprecated and will be removed in the future.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub use_legacy_event_provider: bool,
    /// Disable the event-blob writer
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub disable_event_blob_writer: bool,
    /// The commission rate of the storage node, in basis points.
    #[serde(default)]
    pub commission_rate: u16,
    /// The parameters for the staking pool.
    pub voting_params: VotingParams,
    /// Metadata of the storage node.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub metadata: NodeMetadata,
    /// Metric push configuration.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub metrics_push: Option<MetricsPushConfig>,
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("/opt/walrus/db"),
            blocklist_path: Default::default(),
            db_config: Default::default(),
            protocol_key_pair: PathOrInPlace::from_path("/opt/walrus/config/protocol.key"),
            network_key_pair: PathOrInPlace::from_path("/opt/walrus/config/network.key"),
            public_host: defaults::rest_api_address().ip().to_string(),
            public_port: defaults::rest_api_port(),
            metrics_address: defaults::metrics_address(),
            rest_api_address: defaults::rest_api_address(),
            rest_graceful_shutdown_period_secs: defaults::rest_graceful_shutdown_period_secs(),
            sui: Default::default(),
            blob_recovery: Default::default(),
            tls: Default::default(),
            shard_sync_config: Default::default(),
            event_processor_config: Default::default(),
            use_legacy_event_provider: false,
            disable_event_blob_writer: Default::default(),
            commission_rate: 0,
            voting_params: VotingParams {
                storage_price: defaults::storage_price(),
                write_price: defaults::write_price(),
                node_capacity: 250_000_000_000,
            },
            name: Default::default(),
            metrics_push: None,
            metadata: Default::default(),
        }
    }
}

impl StorageNodeConfig {
    /// Loads the keys from disk into memory.
    pub fn load_keys(&mut self) -> Result<(), anyhow::Error> {
        self.protocol_key_pair.load()?;
        self.network_key_pair.load()?;
        Ok(())
    }

    /// Returns the network key pair.
    ///
    /// # Panics
    ///
    /// Panics if the key has not yet been loaded from disk.
    pub fn network_key_pair(&self) -> &NetworkKeyPair {
        self.network_key_pair
            .get()
            .expect("key pair should already be loaded into memory")
    }

    /// Returns the protocol key pair.
    ///
    /// # Panics
    ///
    /// Panics if the key has not yet been loaded from disk.
    pub fn protocol_key_pair(&self) -> &ProtocolKeyPair {
        self.protocol_key_pair
            .get()
            .expect("key pair should already be loaded into memory")
    }

    /// Converts the configuration into registration parameters used for node registration.
    pub fn to_registration_params(&self) -> NodeRegistrationParams {
        let network_key_pair = self.network_key_pair();
        let protocol_key_pair = self.protocol_key_pair();
        let public_port = self.public_port;
        let public_address = if let Ok(ip_addr) = IpAddr::from_str(&self.public_host) {
            NetworkAddress(SocketAddr::new(ip_addr, public_port).to_string())
        } else {
            NetworkAddress(format!("{}:{}", self.public_host, public_port))
        };
        NodeRegistrationParams {
            name: self.name.clone(),
            network_address: public_address,
            public_key: protocol_key_pair.public().clone(),
            network_public_key: network_key_pair.public().clone(),
            commission_rate: self.commission_rate,
            storage_price: self.voting_params.storage_price,
            write_price: self.voting_params.write_price,
            node_capacity: self.voting_params.node_capacity,
            metadata: self.metadata.clone(),
        }
    }
}

/// Configuration for metric push.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetricsPushConfig {
    /// The interval of time we will allow to elapse before pushing metrics.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(
        rename = "push_interval_secs",
        default = "push_interval_default",
        skip_serializing_if = "defaults::is_default"
    )]
    pub push_interval: Duration,
    /// The URL that we will push metrics to.
    pub push_url: String,
    /// Static labels to provide to the push process.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub labels: Option<HashMap<String, String>>,
}

/// Configure the default push interval for metrics.
fn push_interval_default() -> Duration {
    Duration::from_secs(60)
}

/// Configuration for TLS of the rest API.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
pub struct TlsConfig {
    /// Do not use TLS on the REST API.
    ///
    /// Should only be disabled if TLS encryption is being offloaded to another
    /// service in the network.
    pub disable_tls: bool,
    /// Paths to the certificate and key used to secure the REST API.
    pub pem_files: Option<TlsCertificateAndKey>,
}

/// Paths to a TLS certificate and key.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct TlsCertificateAndKey {
    /// Path to the PEM-encoded x509 certificate.
    pub certificate_path: PathBuf,
    /// Path to the PEM-encoded PKCS8 certificate private key.
    pub key_path: PathBuf,
}

impl LoadConfig for StorageNodeConfig {}

/// Configuration of a Walrus storage node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct BlobRecoveryConfig {
    /// The number of in-parallel blobs synchronized
    pub max_concurrent_blob_syncs: usize,
    /// The number of in-parallel slivers synchronized
    pub max_concurrent_sliver_syncs: usize,
    /// Configuration of the committee service timeouts and retries
    #[serde(flatten)]
    pub committee_service_config: CommitteeServiceConfig,
}

impl Default for BlobRecoveryConfig {
    fn default() -> Self {
        Self {
            max_concurrent_blob_syncs: 100,
            max_concurrent_sliver_syncs: 2_000,
            committee_service_config: CommitteeServiceConfig::default(),
        }
    }
}

/// Configuration of a Walrus storage node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct CommitteeServiceConfig {
    /// The minimum number of seconds to wait before retrying an operation.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "retry_interval_min_secs")]
    pub retry_interval_min: Duration,
    /// The maximum number of seconds to wait before retrying an operation.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "retry_interval_max_secs")]
    pub retry_interval_max: Duration,
    /// The timeout when requesting metadata from a storage node, before contacting a separate node.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "metadata_request_timeout_secs")]
    pub metadata_request_timeout: Duration,
    /// The number of concurrent metadata requests
    pub max_concurrent_metadata_requests: NonZeroUsize,
    /// The timeout when requesting slivers from a storage node.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "sliver_request_timeout_secs")]
    pub sliver_request_timeout: Duration,
    /// The timeout when syncing invalidity certificates to a storage node
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "invalidity_sync_timeout_secs")]
    pub invalidity_sync_timeout: Duration,
    /// The timeout when connecting to remote storage nodes.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "node_connect_timeout_secs")]
    pub node_connect_timeout: Duration,
}

impl Default for CommitteeServiceConfig {
    fn default() -> Self {
        Self {
            retry_interval_min: Duration::from_secs(1),
            retry_interval_max: Duration::from_secs(3600),
            metadata_request_timeout: Duration::from_secs(5),
            sliver_request_timeout: Duration::from_secs(300),
            invalidity_sync_timeout: Duration::from_secs(300),
            max_concurrent_metadata_requests: NonZeroUsize::new(1).unwrap(),
            node_connect_timeout: Duration::from_secs(1),
        }
    }
}

/// Configuration for Walrus storage node shard synchronization.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct ShardSyncConfig {
    /// The number of slivers to fetch in a single sync shard request.
    pub sliver_count_per_sync_request: u64,
    /// The minimum backoff time for shard sync retries.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "shard_sync_retry_min_backoff_secs")]
    pub shard_sync_retry_min_backoff: Duration,
    /// The maximum backoff time for shard sync retries.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "shard_sync_retry_max_backoff_secs")]
    pub shard_sync_retry_max_backoff: Duration,
    /// The maximum number of concurrent blob recoveries during shard recovery.
    pub max_concurrent_blob_recovery_during_shard_recovery: usize,
    /// The interval to check if the blob is still certified during recovery.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "blob_certified_check_interval_secs")]
    pub blob_certified_check_interval: Duration,
    /// The number of metadata to fetch in parallel.
    pub max_concurrent_metadata_fetch: usize,
}

impl Default for ShardSyncConfig {
    fn default() -> Self {
        Self {
            sliver_count_per_sync_request: 10,
            shard_sync_retry_min_backoff: Duration::from_secs(60),
            shard_sync_retry_max_backoff: Duration::from_secs(600),
            max_concurrent_blob_recovery_during_shard_recovery: 5,
            blob_certified_check_interval: Duration::from_secs(60),
            max_concurrent_metadata_fetch: 10,
        }
    }
}

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
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub wallet_config: PathBuf,
    /// The configuration for the backoff strategy used for retries.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub backoff_config: ExponentialBackoffConfig,
    /// Gas budget for transactions.
    #[serde(default = "defaults::gas_budget")]
    pub gas_budget: u64,
}

impl SuiConfig {
    /// Creates a new [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client(&self) -> Result<SuiReadClient, SuiClientError> {
        SuiReadClient::new_for_rpc(
            &self.rpc,
            &self.contract_config,
            self.backoff_config.clone(),
        )
        .await
    }

    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(&self) -> Result<SuiContractClient, SuiClientError> {
        SuiContractClient::new(
            WalletContext::new(&self.wallet_config, None, None)?,
            &self.contract_config,
            self.backoff_config.clone(),
            self.gas_budget,
        )
        .await
    }
}

impl LoadConfig for SuiConfig {}

/// Default values for the storage-node configuration.
pub mod defaults {
    use std::net::Ipv4Addr;

    use walrus_sui::utils::SuiNetwork;

    use super::*;

    /// Default metrics port.
    pub const METRICS_PORT: u16 = 9184;
    /// Default REST API port.
    pub const REST_API_PORT: u16 = 9185;
    /// Default polling interval in milliseconds.
    pub const POLLING_INTERVAL_MS: u64 = 400;
    /// Default number of seconds to wait for graceful shutdown.
    pub const REST_GRACEFUL_SHUTDOWN_PERIOD_SECS: u64 = 60;

    /// Returns the default metrics port.
    pub fn metrics_port() -> u16 {
        METRICS_PORT
    }

    /// Returns the default REST API port.
    pub fn rest_api_port() -> u16 {
        REST_API_PORT
    }

    /// Returns the default metrics address.
    pub fn metrics_address() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, METRICS_PORT).into()
    }

    /// Returns the default REST API address.
    pub fn rest_api_address() -> SocketAddr {
        (Ipv4Addr::UNSPECIFIED, REST_API_PORT).into()
    }

    /// Returns the default polling interval.
    pub fn polling_interval() -> Duration {
        Duration::from_millis(POLLING_INTERVAL_MS)
    }

    /// Returns the default gas budget.
    pub fn gas_budget() -> u64 {
        500_000_000
    }

    /// Returns the default network ([`SuiNetwork::Devnet`])
    pub fn network() -> SuiNetwork {
        SuiNetwork::Devnet
    }

    pub(super) const fn rest_graceful_shutdown_period_secs() -> Option<Option<u64>> {
        Some(Some(REST_GRACEFUL_SHUTDOWN_PERIOD_SECS))
    }

    /// The default vote for the storage price.
    pub fn storage_price() -> u64 {
        100
    }

    /// The default vote for the write price.
    pub fn write_price() -> u64 {
        2000
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

/// Enum that represents a configuration value being preset or at a path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PathOrInPlace<T> {
    /// The value was present in-place in the config, without a filename.
    InPlace(T),

    /// A value that is not present in the config, but at a path on the filesystem.
    Path {
        /// The path from which the value can be loaded.
        #[serde(rename = "path", deserialize_with = "utils::resolve_home_dir")]
        path: PathBuf,
        /// The value loaded from the specified path.
        #[serde(skip, default = "Option::default")]
        value: Option<T>,
    },
}

impl<T> PathOrInPlace<T> {
    /// Creates a new `PathOrInPlace::Path` from the provided path.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        Self::Path {
            path: path.as_ref().to_owned(),
            value: None,
        }
    }

    /// Returns true iff the value has already been loaded into memory.
    pub const fn is_loaded(&self) -> bool {
        matches!(
            self,
            PathOrInPlace::InPlace(_) | PathOrInPlace::Path { value: Some(_), .. }
        )
    }

    /// Gets the value, if already loaded into memory, otherwise returns None.
    pub const fn get(&self) -> Option<&T> {
        if let PathOrInPlace::InPlace(value)
        | PathOrInPlace::Path {
            value: Some(value), ..
        } = self
        {
            Some(value)
        } else {
            None
        }
    }

    /// Returns true iff the value is a path.
    pub const fn is_path(&self) -> bool {
        matches!(self, PathOrInPlace::Path { .. })
    }
}

impl<T> From<T> for PathOrInPlace<T> {
    fn from(value: T) -> Self {
        PathOrInPlace::InPlace(value)
    }
}

impl<T: SupportedKeyPair> PathOrInPlace<TaggedKeyPair<T>> {
    /// Loads and returns a key pair from the path on disk.
    ///
    /// If the value was already loaded, it is returned instead.
    pub fn load(&mut self) -> Result<&TaggedKeyPair<T>, anyhow::Error> {
        if let PathOrInPlace::Path {
            path,
            value: value @ None,
        } = self
        {
            let base64_string = std::fs::read_to_string(path.as_path())
                .context(format!("unable to read key from '{}'", path.display()))?;
            let decoded: TaggedKeyPair<T> = base64_string
                .parse()
                .map_err(|err: KeyPairParseError| anyhow::anyhow!(err.to_string()))?;
            *value = Some(decoded)
        };
        Ok(self
            .get()
            .expect("we just made sure that the value is some"))
    }
}

impl<'de, T> DeserializeAs<'de, PathOrInPlace<T>> for PathOrInPlace<Base64>
where
    Base64: DeserializeAs<'de, T>,
{
    fn deserialize_as<D>(deserializer: D) -> Result<PathOrInPlace<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match PathOrInPlace::<DeserializeAsWrap<T, Base64>>::deserialize(deserializer)? {
            PathOrInPlace::InPlace(value) => Ok(PathOrInPlace::InPlace(value.into_inner())),
            PathOrInPlace::Path { path, value } => Ok(PathOrInPlace::Path {
                path,
                value: value.map(DeserializeAsWrap::into_inner),
            }),
        }
    }
}

impl<T> SerializeAs<PathOrInPlace<T>> for PathOrInPlace<Base64>
where
    Base64: SerializeAs<T>,
{
    fn serialize_as<S>(source: &PathOrInPlace<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let wrapper = match source {
            PathOrInPlace::InPlace(value) => {
                PathOrInPlace::InPlace(SerializeAsWrap::<T, Base64>::new(value))
            }
            PathOrInPlace::Path { path, value } => PathOrInPlace::Path {
                path: path.to_path_buf(),
                value: value.as_ref().map(SerializeAsWrap::new),
            },
        };
        wrapper.serialize(serializer)
    }
}

/// Parameters that allow registering a node with a third party.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistrationParamsForThirdPartyRegistration {
    /// The node registration parameters.
    pub node_registration_params: NodeRegistrationParams,
    /// The proof of possession authorizing the third party to register the node.
    pub proof_of_possession: ProofOfPossession,
    /// The wallet address of the node. This is required to send the storage-node capability to the
    /// node.
    pub wallet_address: SuiAddress,
}

impl LoadConfig for NodeRegistrationParamsForThirdPartyRegistration {}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, str::FromStr};

    use indoc::indoc;
    use rand::{rngs::StdRng, SeedableRng as _};
    use sui_types::base_types::ObjectID;
    use tempfile::NamedTempFile;
    use walrus_core::test_utils;
    use walrus_test_utils::Result as TestResult;

    use super::*;

    /// Serializes a default config to the example file when tests are run.
    ///
    /// This test ensures that the `node_config_example.yaml` is kept in sync with the config struct
    /// in this file.
    #[test]
    fn check_and_update_example_config() -> TestResult {
        const EXAMPLE_CONFIG_PATH: &str = "node_config_example.yaml";

        let mut rng = StdRng::seed_from_u64(42);
        let contract_config = ContractConfig {
            system_object: ObjectID::random_from_rng(&mut rng),
            staking_object: ObjectID::random_from_rng(&mut rng),
        };
        let config = StorageNodeConfig {
            sui: Some(SuiConfig {
                rpc: "https://fullnode.testnet.sui.io:443".to_string(),
                contract_config,
                event_polling_interval: defaults::polling_interval(),
                wallet_config: PathBuf::from("/opt/walrus/config/sui_config.yaml"),
                backoff_config: Default::default(),
                gas_budget: defaults::gas_budget(),
            }),
            ..Default::default()
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
    fn path_or_in_place_parses_value() -> TestResult {
        assert_eq!(
            serde_yaml::from_str::<PathOrInPlace<u64>>("2048")?,
            PathOrInPlace::InPlace(2048)
        );
        Ok(())
    }

    #[test]
    fn path_or_in_place_parses_path() -> TestResult {
        let path: PathBuf = "/path/to/value.txt".parse()?;
        assert_eq!(
            serde_yaml::from_str::<PathOrInPlace<u64>>(&format!("path: {}", path.display()))?,
            PathOrInPlace::from_path(path)
        );
        Ok(())
    }

    #[test]
    fn path_or_in_place_deserializes_from_base64() -> TestResult {
        let expected_keypair = test_utils::protocol_key_pair();
        let yaml_contents = expected_keypair.to_base64();

        let deserializer = serde_yaml::Deserializer::from_str(&yaml_contents);
        let decoded: PathOrInPlace<ProtocolKeyPair> =
            PathOrInPlace::<Base64>::deserialize_as(deserializer)?;

        assert_eq!(decoded, PathOrInPlace::InPlace(expected_keypair));

        Ok(())
    }

    #[test]
    fn path_or_in_place_serializes_to_base64() -> TestResult {
        let keypair = test_utils::protocol_key_pair();
        let expected_yaml = keypair.to_base64() + "\n";

        let mut written_yaml = vec![];
        let mut serializer = serde_yaml::Serializer::new(&mut written_yaml);

        let in_place = PathOrInPlace::<ProtocolKeyPair>::InPlace(keypair);
        PathOrInPlace::<Base64>::serialize_as(&in_place, &mut serializer)?;

        assert_eq!(String::from_utf8(written_yaml)?, expected_yaml);

        Ok(())
    }

    #[test]
    fn loads_base64_protocol_keypair() -> TestResult {
        let key = test_utils::protocol_key_pair();
        let key_file = NamedTempFile::new()?;

        key_file.as_file().write_all(key.to_base64().as_bytes())?;

        let mut path = PathOrInPlace::<ProtocolKeyPair>::from_path(key_file.path());

        assert_eq!(*path.load()?, key);

        Ok(())
    }

    #[test]
    fn parses_minimal_config_file() -> TestResult {
        let yaml = indoc! {"
            name: node-1
            storage_path: target/storage
            protocol_key_pair: BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj
            network_key_pair: As5tqQFRGrjPSvcZeKfBX98NwDuCUtZyJdzWR2bUn0oY
            public_host: 31.41.59.26
            public_port: 12345
            voting_params:
                storage_price: 5
                write_price: 1
                node_capacity: 250000000000
        "};

        let config: StorageNodeConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(
            config.protocol_key_pair(),
            &ProtocolKeyPair::from_str("BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj")?
        );

        Ok(())
    }

    #[test]
    fn parses_partial_config_file() -> TestResult {
        let yaml = indoc! {"
            name: node-1
            storage_path: /opt/walrus/db
            db_config:
                metadata:
                    target_file_size_base: 4194304
                default:
                    blob_compression_type: none
                    enable_blob_garbage_collection: false
                optimized_for_blobs:
                    enable_blob_files: true
                    min_blob_size: 0
                    blob_file_size: 1000
                blob_info:
                    enable_blob_files: false
                event_cursor:
                    enable_blob_files: false
                shard:
                    blob_garbage_collection_force_threshold: 0.5
                shard_status:
                    blob_garbage_collection_age_cutoff: 0.0
            protocol_key_pair: BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj
            network_key_pair: As5tqQFRGrjPSvcZeKfBX98NwDuCUtZyJdzWR2bUn0oY
            public_host: node.walrus.space
            public_port: 9185
            metrics_address: 173.199.90.181:9184
            rest_api_address: 173.199.90.181:9185
            sui:
                rpc: https://fullnode.testnet.sui.io:443
                system_object: 0x6c957cf363ec968582f24e3e1a638c968cec1fa228999c560ec7925994906315
                staking_object: 0x2be0418db0dc7b07fe4c32bf80e250b8993cef130ce8c51ad8f12aa91def42df
                event_polling_interval_millis: 400
                wallet_config: /opt/walrus/config/dryrun-node-1-sui.yaml
                gas_budget: 500000000
                backoff_config:
                    min_backoff_millis: 1000
            blob_recovery:
                invalidity_sync_timeout_secs: 300
            voting_params:
                storage_price: 5
                write_price: 1
                node_capacity: 250000000000
            tls:
                disable_tls: true
            shard_sync_config:
                sliver_count_per_sync_request: 10
                shard_sync_retry_min_backoff_secs: 60
            event_processor_config:
                pruning_interval_secs: 3600
                adaptive_downloader_config:
                    max_workers: 5
                    scale_down_lag_threshold: 10
                    base_config:
                        max_delay_millis: 1000
        "};

        let _: StorageNodeConfig = serde_yaml::from_str(yaml)?;

        Ok(())
    }
}
