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

use anyhow::{anyhow, Context};
use p256::pkcs8::DecodePrivateKey;
use serde::{Deserialize, Serialize};
use serde_with::{
    base64::Base64,
    de::DeserializeAsWrap,
    ser::SerializeAsWrap,
    serde_as,
    DeserializeAs,
    DurationSeconds,
    SerializeAs,
};
use sui_types::base_types::{ObjectID, SuiAddress};
use walrus_core::{
    keys::{KeyPairParseError, NetworkKeyPair, ProtocolKeyPair},
    messages::ProofOfPossession,
    NetworkPublicKey,
};
use walrus_sui::types::{
    move_structs::VotingParams,
    NetworkAddress,
    NodeMetadata,
    NodeRegistrationParams,
    NodeUpdateParams,
};

use super::storage::DatabaseConfig;
use crate::{
    common::{config::SuiConfig, utils},
    node::events::EventProcessorConfig,
};

/// Configuration for the config synchronizer.
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSynchronizerConfig {
    /// Interval between config monitoring checks.
    #[serde_as(as = "DurationSeconds<u64>")]
    #[serde(rename = "interval_secs")]
    pub interval: Duration,
    /// Enable the config monitor.
    pub enabled: bool,
}

impl Default for ConfigSynchronizerConfig {
    fn default() -> Self {
        Self {
            interval: defaults::config_synchronizer_interval(),
            enabled: defaults::config_synchronizer_enabled(),
        }
    }
}

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
    // Important: this name should be in-sync with the name used in `rotate_protocol_key_pair()`
    #[serde_as(as = "PathOrInPlace<Base64>")]
    pub protocol_key_pair: PathOrInPlace<ProtocolKeyPair>,
    /// The next protocol key pair to use for the storage node.
    // Important: this name should be in-sync with the name used in `rotate_protocol_key_pair()`
    #[serde_as(as = "Option<PathOrInPlace<Base64>>")]
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub next_protocol_key_pair: Option<PathOrInPlace<ProtocolKeyPair>>,
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
    /// Configuration for the connections establishing in the REST API.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub rest_server: RestServerConfig,
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
    /// Configuration for the config synchronizer.
    #[serde(default, skip_serializing_if = "defaults::is_default")]
    pub config_synchronizer: ConfigSynchronizerConfig,
    /// The capability object ID of the storage node.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub storage_node_cap: Option<ObjectID>,
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("/opt/walrus/db"),
            blocklist_path: Default::default(),
            db_config: Default::default(),
            protocol_key_pair: PathOrInPlace::from_path("/opt/walrus/config/protocol.key"),
            next_protocol_key_pair: None,
            network_key_pair: PathOrInPlace::from_path("/opt/walrus/config/network.key"),
            public_host: defaults::rest_api_address().ip().to_string(),
            public_port: defaults::rest_api_port(),
            metrics_address: defaults::metrics_address(),
            rest_api_address: defaults::rest_api_address(),
            rest_graceful_shutdown_period_secs: defaults::rest_graceful_shutdown_period_secs(),
            rest_server: Default::default(),
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
            config_synchronizer: Default::default(),
            storage_node_cap: None,
        }
    }
}

impl StorageNodeConfig {
    /// Rotates the protocol key pair.
    pub fn rotate_protocol_key_pair(&mut self) {
        if let Some(next_key_pair) = self.next_protocol_key_pair.clone() {
            self.protocol_key_pair = next_key_pair;
            self.next_protocol_key_pair = None;
        }
    }

    /// Rotates the protocol key pair and persists the config to disk.
    /// This happens when the on-chain protocol key pair has been rotated.
    /// The protocol_key_pair is set to the new key pair, and the
    /// next_protocol_key_pair is cleared.
    pub fn rotate_protocol_key_pair_persist(path: impl AsRef<Path>) -> anyhow::Result<()> {
        // Load config from path, preserving the raw Value to maintain all fields
        let config_str = std::fs::read_to_string(path.as_ref())?;
        let mut original_value: serde_yaml::Value = serde_yaml::from_str(&config_str)?;
        let mut config: StorageNodeConfig = serde_yaml::from_str(&config_str)?;
        // Constants for config key strings
        const PROTOCOL_KEY_PAIR_KEY: &str = "protocol_key_pair";
        const NEXT_PROTOCOL_KEY_PAIR_KEY: &str = "next_protocol_key_pair";

        if config.next_protocol_key_pair.is_none() {
            return Err(anyhow::anyhow!("{} is not set", NEXT_PROTOCOL_KEY_PAIR_KEY));
        }

        // Rotate the protocol key pair
        config.rotate_protocol_key_pair();

        // Update only the relevant fields in the original Value
        if let serde_yaml::Value::Mapping(ref mut map) = original_value {
            // Update protocol_key_pair
            if let Ok(protocol_key_pair) = serde_yaml::to_value(&config.protocol_key_pair) {
                map.insert(
                    serde_yaml::Value::String(PROTOCOL_KEY_PAIR_KEY.to_string()),
                    protocol_key_pair,
                );
            }
            // Clear next_protocol_key_pair
            map.remove(serde_yaml::Value::String(
                NEXT_PROTOCOL_KEY_PAIR_KEY.to_string(),
            ));
        }

        // Write to temporary file first
        let temp_path = path.as_ref().with_extension("tmp");
        let config_str = serde_yaml::to_string(&original_value)
            .map_err(|e| anyhow::anyhow!("failed to serialize config: {e}"))?;
        std::fs::write(&temp_path, config_str)
            .map_err(|e| anyhow::anyhow!("failed to write temporary config file: {e}"))?;

        // Remove old file if it exists
        if path.as_ref().exists() {
            std::fs::remove_file(path.as_ref())?;
        }

        // Rename temporary file to actual config file
        std::fs::rename(&temp_path, path.as_ref())?;

        Ok(())
    }

    /// Loads the keys from disk into memory.
    pub fn load_keys(&mut self) -> Result<(), anyhow::Error> {
        self.protocol_key_pair.load()?;
        if let Some(next_protocol_key_pair) = self.next_protocol_key_pair.as_mut() {
            next_protocol_key_pair.load()?;
        }
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

    /// Returns the next protocol key pair, if it exists.
    ///
    /// # Panics
    ///
    /// Panics if the next protocol key pair exists but hasn't been loaded into memory yet.
    pub fn next_protocol_key_pair(&self) -> Option<&ProtocolKeyPair> {
        self.next_protocol_key_pair.as_ref().map(|k| {
            k.get()
                .expect("next protocol key pair should already be loaded into memory")
        })
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

    /// Compares the current node parameters with the passed-in parameters and generates the
    /// update params if there are any changes, so that the source of the passed-in parameters
    /// can be updated to the node parameters.
    pub fn generate_update_params(
        &self,
        name: &str,
        network_address: &str,
        network_public_key: &NetworkPublicKey,
        voting_params: &VotingParams,
    ) -> NodeUpdateParams {
        let local_network_public_key = self.network_key_pair().public();
        let local_public_address =
            NetworkAddress(format!("{}:{}", self.public_host, self.public_port));

        NodeUpdateParams {
            name: (name != self.name).then_some(self.name.clone()),
            network_address: (network_address != local_public_address.0)
                .then_some(local_public_address),
            network_public_key: (network_public_key != local_network_public_key)
                .then_some(local_network_public_key.clone()),
            update_public_key: None,
            storage_price: (voting_params.storage_price != self.voting_params.storage_price)
                .then_some(self.voting_params.storage_price),
            write_price: (voting_params.write_price != self.voting_params.write_price)
                .then_some(self.voting_params.write_price),
            node_capacity: (voting_params.node_capacity != self.voting_params.node_capacity)
                .then_some(self.voting_params.node_capacity),
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
        default = "defaults::push_interval",
        skip_serializing_if = "defaults::is_push_interval_default"
    )]
    pub push_interval: Duration,
    /// The URL that we will push metrics to.
    pub push_url: String,
    /// Static labels to provide to the push process.
    #[serde(default, skip_serializing_if = "defaults::is_none")]
    pub labels: Option<HashMap<String, String>>,
}

impl MetricsPushConfig {
    /// Creates a new `MetricsPushConfig` with the provided URL and otherwise default values.
    pub fn new_for_url(url: String) -> Self {
        Self {
            push_interval: defaults::push_interval(),
            push_url: url,
            labels: None,
        }
    }

    /// Sets the 'name' label to `name` and the 'host' label to the machine's hostname; if the
    /// hostname cannot be determined, `name` is used as a fallback.
    pub fn set_name_and_host_label(&mut self, name: &str) {
        self.labels
            .get_or_insert_with(HashMap::new)
            .entry("name".into())
            .or_insert_with(|| name.into());

        let host =
            hostname::get().map_or_else(|_| name.into(), |v| v.to_string_lossy().to_string());
        self.set_host(host);
    }

    /// Sets the 'host' label to `host`.
    fn set_host(&mut self, host: String) {
        self.labels
            .get_or_insert_with(HashMap::new)
            .entry("host".into())
            .or_insert_with(|| host);
    }
}

/// Configuration for TLS of the rest API.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct TlsConfig {
    /// Do not use TLS on the REST API.
    ///
    /// Should only be disabled if TLS encryption is being offloaded to another
    /// service in the network.
    pub disable_tls: bool,
    /// Path to the PEM-encoded x509 certificate.
    pub certificate_path: Option<PathBuf>,
}

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
    /// Use the experimental batch recovery service endpoint.
    pub experimental_batch_symbol_recovery: bool,
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
            experimental_batch_symbol_recovery: true,
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
    /// Maximum number of concurrent shard syncs allowed per node.
    pub shard_sync_concurrency: usize,
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
            shard_sync_concurrency: 10,
        }
    }
}

/// Default values for the storage-node configuration.
pub mod defaults {
    use std::net::Ipv4Addr;

    use walrus_sui::utils::SuiNetwork;

    use super::*;
    pub use crate::common::config::defaults::{is_default, polling_interval};

    /// Default metrics port.
    pub const METRICS_PORT: u16 = 9184;
    /// Default REST API port.
    pub const REST_API_PORT: u16 = 9185;
    /// Default number of seconds to wait for graceful shutdown.
    pub const REST_GRACEFUL_SHUTDOWN_PERIOD_SECS: u64 = 60;
    /// Default interval between config monitoring checks in seconds.
    pub const CONFIG_SYNCHRONIZER_INTERVAL_SECS: u64 = 900;

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

    /// Configure the default push interval for metrics.
    pub fn push_interval() -> Duration {
        Duration::from_secs(60)
    }

    /// Returns true if the `duration` is equal to the default push interval for metrics.
    pub fn is_push_interval_default(duration: &Duration) -> bool {
        duration == &push_interval()
    }

    /// Returns true iff the value is `None` and we don't run in test mode.
    pub fn is_none<T>(t: &Option<T>) -> bool {
        // The `cfg!(test)` check is there to allow serializing the full configuration, specifically
        // to generate the example configuration files.
        !cfg!(test) && t.is_none()
    }

    /// The default interval between config monitoring checks
    pub fn config_synchronizer_interval() -> Duration {
        Duration::from_secs(CONFIG_SYNCHRONIZER_INTERVAL_SECS)
    }

    /// Returns false in test mode.
    pub fn config_synchronizer_enabled() -> bool {
        !cfg!(test)
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

    /// Returns the path, if any.
    pub fn path(&self) -> Option<&Path> {
        if let PathOrInPlace::Path { path, .. } = self {
            Some(path)
        } else {
            None
        }
    }
}

impl<T> From<T> for PathOrInPlace<T> {
    fn from(value: T) -> Self {
        PathOrInPlace::InPlace(value)
    }
}

/// Trait for simplifying the loading of different representations from the file.
pub trait LoadsFromPath: Sized {
    /// Loads the value from the specified filesystem path.
    fn load(path: &Path) -> Result<Self, anyhow::Error>;
}

impl LoadsFromPath for ProtocolKeyPair {
    fn load(path: &Path) -> Result<Self, anyhow::Error> {
        let base64_string = std::fs::read_to_string(path)
            .context(format!("unable to read key from '{}'", path.display()))?;
        base64_string
            .parse()
            .map_err(|err: KeyPairParseError| anyhow!(err.to_string()))
    }
}

impl LoadsFromPath for NetworkKeyPair {
    fn load(path: &Path) -> Result<Self, anyhow::Error> {
        let _span = tracing::info_span!("load", path = %path.display()).entered();

        let file_contents = std::fs::read_to_string(path)
            .context(format!("unable to read key from '{}'", path.display()))?;

        NetworkKeyPair::from_pkcs8_pem(&file_contents)
            .inspect(|_| tracing::info!("loaded network private key in PKCS#8 format"))
            .or_else(|error| {
                tracing::debug!(
                    ?error,
                    "failed to load network key in PKCS#8 format, trying tagged"
                );

                NetworkKeyPair::from_str(&file_contents)
                    .inspect(|_| {
                        tracing::info!("loaded network private key in tagged format");
                    })
                    .map_err(|error2| {
                        anyhow!(
                            "unsupported network private key format: key is neither in PKCS#8 \
                            format ({error}), nor in \"tagged\" format ({error2})"
                        )
                    })
            })
    }
}

impl LoadsFromPath for Vec<u8> {
    fn load(path: &Path) -> Result<Self, anyhow::Error> {
        std::fs::read(path).map_err(|error| anyhow!(error))
    }
}

impl<T: LoadsFromPath> PathOrInPlace<T> {
    /// Loads and returns the value from the filesystem path.
    ///
    /// If the value was already loaded, it is returned instead.
    pub fn load(&mut self) -> Result<&T, anyhow::Error> {
        if let PathOrInPlace::Path {
            path,
            value: value @ None,
        } = self
        {
            *value = Some(T::load(path)?)
        };

        Ok(self
            .get()
            .expect("we just made sure that the value is some"))
    }

    /// Loads and returns the value from the filesystem path, or returns the value if it is already
    /// loaded.
    ///
    /// This does not update the stored value, and so can be called with only a shared reference.
    pub fn load_transient(&self) -> Result<T, anyhow::Error>
    where
        T: Clone,
    {
        match self {
            PathOrInPlace::InPlace(value) => Ok(value.clone()),
            PathOrInPlace::Path { path, .. } => T::load(path),
        }
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

/// Configuration for the REST server.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RestServerConfig {
    /// Configuration for incoming HTTP/2 connections.
    #[serde(flatten, skip_serializing_if = "defaults::is_default")]
    pub http2_config: Http2Config,
}

/// Configuration of the HTTP/2 connections established by the REST API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Http2Config {
    /// The maximum number of concurrent streams that a client can open
    /// over a connection to the server.
    pub http2_max_concurrent_streams: u32,
    /// Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control.
    #[serde(skip_serializing_if = "defaults::is_none")]
    pub http2_initial_stream_window_size: Option<u32>,
    /// Sets the max connection-level flow control for HTTP2.
    #[serde(skip_serializing_if = "defaults::is_none")]
    pub http2_initial_connection_window_size: Option<u32>,
    /// Use adaptive flow control, overriding the `http2_initial_stream_window_size` and
    /// `http2_initial_connection_window_size` settings.
    pub http2_adaptive_window: bool,
}

impl Default for Http2Config {
    fn default() -> Self {
        Self {
            http2_max_concurrent_streams: 1000,
            http2_initial_stream_window_size: None,
            http2_initial_connection_window_size: None,
            http2_adaptive_window: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, str::FromStr};

    use indoc::indoc;
    use p256::{pkcs8, pkcs8::EncodePrivateKey};
    use rand::{rngs::StdRng, SeedableRng as _};
    use sui_types::base_types::ObjectID;
    use tempfile::{NamedTempFile, TempDir};
    use walrus_core::test_utils;
    use walrus_sui::client::contract_config::ContractConfig;
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
                gas_budget: None,
            }),
            config_synchronizer: ConfigSynchronizerConfig {
                interval: Duration::from_secs(defaults::CONFIG_SYNCHRONIZER_INTERVAL_SECS),
                enabled: true,
            },
            ..Default::default()
        };

        walrus_test_utils::overwrite_file_and_fail_if_not_equal(
            EXAMPLE_CONFIG_PATH,
            serde_yaml::to_string(&config)?,
        )?;

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
    fn loads_pem_network_keypair() -> TestResult {
        let key = test_utils::network_key_pair();
        let pem_string = key
            .to_pkcs8_pem(pkcs8::LineEnding::default())
            .expect("key can be serialized as pem");

        let key_file = NamedTempFile::new()?;
        key_file.as_file().write_all(pem_string.as_ref())?;

        let mut path = PathOrInPlace::<NetworkKeyPair>::from_path(key_file.path());
        let loaded_key = path.load().expect("key should load successfully");

        assert_eq!(*loaded_key, key);

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

    #[test]
    fn test_generate_update_params() -> TestResult {
        // Create a config with the new desired values
        let new_voting_params = VotingParams {
            storage_price: 150,
            write_price: 250,
            node_capacity: 2000,
        };
        let mut config = StorageNodeConfig {
            name: "new-name".to_string(),
            public_host: "192.168.1.1".to_string(),
            public_port: 9090,
            network_key_pair: PathOrInPlace::InPlace(NetworkKeyPair::generate()),
            voting_params: new_voting_params,
            ..Default::default()
        };

        // Test 1: No changes needed - current values match config
        let current_addr = "192.168.1.1:9090";
        let result = config.generate_update_params(
            &config.name,
            current_addr,
            config.network_key_pair().public(),
            &config.voting_params,
        );
        assert!(
            !result.needs_update(),
            "Expected no updates when all values match"
        );

        // Test 2: All fields need updating - current values are all different
        let old_network_keypair = NetworkKeyPair::generate();
        let old_voting_params = VotingParams {
            storage_price: 100,
            write_price: 200,
            node_capacity: 1000,
        };
        let old_name = "old-name".to_string();
        let old_network_address = "127.0.0.1:8080";

        let result = config.generate_update_params(
            &old_name,
            old_network_address,
            old_network_keypair.public(),
            &old_voting_params,
        );

        let expected_update_params = NodeUpdateParams {
            name: Some(config.name.clone()),
            network_address: Some(NetworkAddress(format!(
                "{}:{}",
                config.public_host, config.public_port
            ))),
            network_public_key: Some(config.network_key_pair().public().clone()),
            update_public_key: None,
            storage_price: Some(config.voting_params.storage_price),
            write_price: Some(config.voting_params.write_price),
            node_capacity: Some(config.voting_params.node_capacity),
        };
        assert_eq!(result, expected_update_params);

        // Test 3: Only voting params need updating
        let result = config.generate_update_params(
            &config.name,
            &format!("{}:{}", config.public_host, config.public_port),
            config.network_key_pair().public(),
            &old_voting_params,
        );

        let expected_update_params = NodeUpdateParams {
            name: None,
            network_address: None,
            network_public_key: None,
            update_public_key: None,
            storage_price: Some(config.voting_params.storage_price),
            write_price: Some(config.voting_params.write_price),
            node_capacity: Some(config.voting_params.node_capacity),
        };
        assert_eq!(result, expected_update_params);

        // Test 4: Test hostname instead of IP
        config.public_host = "example.com".to_string();
        let result = config.generate_update_params(
            &config.name,
            "old-domain.com:8080",
            config.network_key_pair().public(),
            &config.voting_params,
        );

        assert_eq!(
            result.network_address.map(|addr| addr.0),
            Some(format!("{}:{}", config.public_host, config.public_port))
        );

        Ok(())
    }

    #[test]
    fn test_rotate_protocol_key_pair_persist() -> TestResult {
        // Create temporary directory for test
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("config.yaml");
        let key_path = temp_dir.path().join("protocol_key.key");
        let next_key_path = temp_dir.path().join("next_protocol_key.key");
        create_protocol_key_file(&key_path)?;
        create_protocol_key_file(&next_key_path)?;

        let config = StorageNodeConfig {
            protocol_key_pair: PathOrInPlace::from_path(key_path),
            next_protocol_key_pair: Some(PathOrInPlace::from_path(next_key_path.clone())),
            name: "test-node".to_string(),
            storage_path: temp_dir.path().to_path_buf(),
            network_key_pair: PathOrInPlace::InPlace(test_utils::network_key_pair()),
            public_host: "localhost".to_string(),
            public_port: 9185,
            voting_params: VotingParams {
                storage_price: 100,
                write_price: 2000,
                node_capacity: 250_000_000,
            },
            ..Default::default()
        };

        // Write config to file
        let config_str = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, config_str)?;

        // Call rotate_protocol_key_pair_persist
        StorageNodeConfig::rotate_protocol_key_pair_persist(&config_path)?;

        // Read back the config and verify the rotation
        let config_content = std::fs::read_to_string(&config_path)?;
        let loaded_config: StorageNodeConfig = serde_yaml::from_str(&config_content)?;

        // Verify that the protocol key pair was rotated
        assert_eq!(
            loaded_config.protocol_key_pair,
            PathOrInPlace::from_path(next_key_path.clone()),
            "Protocol key pair should be rotated to next key pair"
        );
        assert_eq!(
            loaded_config.next_protocol_key_pair, None,
            "Next protocol key pair should be cleared after rotation"
        );

        Ok(())
    }

    fn create_protocol_key_file(path: &Path) -> Result<(), anyhow::Error> {
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("Cannot create the keyfile '{}'", path.display()))?;

        file.write_all(ProtocolKeyPair::generate().to_base64().as_bytes())?;

        Ok(())
    }
}
