// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Storage client configuration module.

use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
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
    DurationSeconds,
    SerializeAs,
};
use sui_sdk::{types::base_types::ObjectID, wallet_context::WalletContext};
use walrus_core::keys::{
    KeyPairParseError,
    NetworkKeyPair,
    ProtocolKeyPair,
    SupportedKeyPair,
    TaggedKeyPair,
};
use walrus_event::EventProcessorConfig;
use walrus_sui::{
    client::{SuiClientError, SuiContractClient, SuiReadClient},
    types::{move_structs::VotingParams, NetworkAddress, NodeRegistrationParams},
};

use super::storage::DatabaseConfig;
use crate::common::utils::{self, LoadConfig};

/// Configuration of a Walrus storage node.
#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageNodeConfig {
    /// Directory in which to persist the database
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub storage_path: PathBuf,
    /// Option config to tune storage db
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_config: Option<DatabaseConfig>,
    /// Key pair used in Walrus protocol messages
    #[serde_as(as = "PathOrInPlace<Base64>")]
    pub protocol_key_pair: PathOrInPlace<ProtocolKeyPair>,
    /// Key pair used to authenticate nodes in network communication.
    #[serde_as(as = "PathOrInPlace<Base64>")]
    pub network_key_pair: PathOrInPlace<NetworkKeyPair>,
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
        skip_serializing_if = "Option::is_none",
        with = "serde_with::rust::double_option"
    )]
    pub rest_graceful_shutdown_period_secs: Option<Option<u64>>,
    /// Sui config for the node
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// Configuration for running checkpoint processor
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_processor_config: Option<EventProcessorConfig>,
    /// The commission rate of the storage node, in basis points.
    #[serde(default)]
    pub commission_rate: u64,
    /// The parameters for the staking pool.
    pub voting_params: VotingParams,
    /// Name of the storage node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("db"),
            db_config: Default::default(),
            protocol_key_pair: PathOrInPlace::InPlace(ProtocolKeyPair::generate()),
            network_key_pair: PathOrInPlace::InPlace(NetworkKeyPair::generate()),
            metrics_address: defaults::metrics_address(),
            rest_api_address: defaults::rest_api_address(),
            rest_graceful_shutdown_period_secs: defaults::rest_graceful_shutdown_period_secs(),
            sui: Default::default(),
            blob_recovery: Default::default(),
            tls: Default::default(),
            shard_sync_config: Default::default(),
            event_processor_config: Default::default(),
            commission_rate: 0,
            voting_params: VotingParams {
                storage_price: defaults::storage_price(),
                write_price: defaults::write_price(),
                node_capacity: 250_000_000_000,
            },
            name: Default::default(),
        }
    }
}

impl StorageNodeConfig {
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
    pub fn to_registration_params(
        &self,
        public_address: NetworkAddress,
        name: String,
    ) -> NodeRegistrationParams {
        let network_key_pair = self.network_key_pair();
        let protocol_key_pair = self.protocol_key_pair();
        NodeRegistrationParams {
            name,
            network_address: public_address,
            public_key: protocol_key_pair.public().clone(),
            network_public_key: network_key_pair.public().clone(),
            commission_rate: self.commission_rate,
            storage_price: self.voting_params.storage_price,
            write_price: self.voting_params.write_price,
            node_capacity: self.voting_params.node_capacity,
        }
    }
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
    /// The server name add to self-signed certificates, if used. If not provided, any self-signed
    /// certificates will use the IP address as the subject name.
    pub server_name: Option<String>,
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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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
}

impl Default for ShardSyncConfig {
    fn default() -> Self {
        Self {
            sliver_count_per_sync_request: 10,
            shard_sync_retry_min_backoff: Duration::from_secs(60),
            shard_sync_retry_max_backoff: Duration::from_secs(600),
            max_concurrent_blob_recovery_during_shard_recovery: 5,
        }
    }
}

/// Sui-specific configuration for Walrus
#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SuiConfig {
    /// HTTP URL of the Sui full-node RPC endpoint (including scheme).
    pub rpc: String,
    /// Object ID of the Walrus system object.
    pub system_object: ObjectID,
    /// Object ID of the Walrus staking object.
    pub staking_object: ObjectID,
    /// Interval with which events are polled, in milliseconds.
    #[serde_as(as = "serde_with::DurationMilliSeconds")]
    #[serde(
        rename = "event_polling_interval_millis",
        default = "defaults::polling_interval"
    )]
    pub event_polling_interval: Duration,
    /// Location of the wallet config.
    #[serde(deserialize_with = "utils::resolve_home_dir")]
    pub wallet_config: PathBuf,
    /// Gas budget for transactions.
    #[serde(default = "defaults::gas_budget")]
    pub gas_budget: u64,
}

impl SuiConfig {
    /// Creates a new [`SuiReadClient`] based on the configuration.
    pub async fn new_read_client(&self) -> Result<SuiReadClient, SuiClientError> {
        SuiReadClient::new_for_rpc(&self.rpc, self.system_object, self.staking_object).await
    }

    /// Creates a [`SuiContractClient`] based on the configuration.
    pub async fn new_contract_client(&self) -> Result<SuiContractClient, SuiClientError> {
        SuiContractClient::new(
            WalletContext::new(&self.wallet_config, None, None)?,
            self.system_object,
            self.staking_object,
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
        (Ipv4Addr::UNSPECIFIED, METRICS_PORT).into()
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
        5
    }

    /// The default vote for the write price.
    pub fn write_price() -> u64 {
        1
    }

    /// Returns true iff the value is the default.
    pub fn is_default<T: PartialEq + Default>(t: &T) -> bool {
        t == &T::default()
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

#[cfg(test)]
mod tests {
    use std::{io::Write as _, str::FromStr};

    use tempfile::NamedTempFile;
    use walrus_core::test_utils;
    use walrus_test_utils::Result as TestResult;

    use super::*;

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
    fn parses_config_file() -> TestResult {
        let yaml = "---\n\
        name: node-1\n\
        storage_path: target/storage\n\
        protocol_key_pair:\n  BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj\n\
        network_key_pair:\n  As5tqQFRGrjPSvcZeKfBX98NwDuCUtZyJdzWR2bUn0oY\n\
        voting_params:
            storage_price: 5
            write_price: 1
            node_capacity: 250000000000";

        ProtocolKeyPair::from_str("BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj")?;

        let _: StorageNodeConfig = serde_yaml::from_str(yaml)?;

        Ok(())
    }

    #[test]
    fn deserialize_partial_config() -> TestResult {
        // editorconfig-checker-disable
        let yaml = "\
name: node-1
storage_path: /opt/walrus/db
db_config:
  metadata:
    target_file_size_base: 4194304
  blob_info:
    enable_blob_files: false
  event_cursor:
    enable_blob_files: false
  shard:
    blob_garbage_collection_force_threshold: 0.5
  shard_status:
    blob_garbage_collection_age_cutoff: 0.0
protocol_key_pair: BBlm7tRefoPuaKoVoxVtnUBBDCfy+BGPREM8B6oSkOEj
network_key_pair:  As5tqQFRGrjPSvcZeKfBX98NwDuCUtZyJdzWR2bUn0oY
metrics_address: 173.199.90.181:9184
rest_api_address: 173.199.90.181:9185
sui:
  rpc: https://fullnode.testnet.sui.io:443
  system_object: 0x6c957cf363ec968582f24e3e1a638c968cec1fa228999c560ec7925994906315
  staking_object: 0x2be0418db0dc7b07fe4c32bf80e250b8993cef130ce8c51ad8f12aa91def42df
  event_polling_interval_millis: 400
  wallet_config: /opt/walrus/config/dryrun-node-1-sui.yaml
  gas_budget: 500000000
blob_recovery:
  invalidity_sync_timeout_secs: 300
voting_params:
  storage_price: 5
  write_price: 1
  node_capacity: 250000000000
  ";
        // editorconfig-checker-enable
        let _: StorageNodeConfig = serde_yaml::from_str(yaml)?;
        Ok(())
    }
}
