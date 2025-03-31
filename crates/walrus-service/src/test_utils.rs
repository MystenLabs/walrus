// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for using storage nodes in tests.
//!
//! For creating an instance of a single storage node in a test, see [`StorageNodeHandleBuilder`] .
//!
//! For creating a cluster of test storage nodes, see [`TestClusterBuilder`].

use std::{
    borrow::Borrow,
    default::Default,
    net::{SocketAddr, TcpStream},
    num::NonZeroU16,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use futures::{future, stream::FuturesUnordered, StreamExt};
use prometheus::Registry;
use sui_macros::nondeterministic;
use sui_types::base_types::ObjectID;
use tempfile::TempDir;
#[cfg(msim)]
use tokio::sync::RwLock;
use tokio::{task::JoinHandle, time::Duration};
use tokio_stream::{wrappers::BroadcastStream, Stream};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;
use typed_store::{rocks::MetricConf, Map};
use walrus_core::{
    encoding::EncodingConfig,
    keys::{NetworkKeyPair, ProtocolKeyPair},
    merkle::MerkleProof,
    messages::InvalidBlobCertificate,
    metadata::VerifiedBlobMetadataWithId,
    BlobId,
    Epoch,
    InconsistencyProof as InconsistencyProofEnum,
    NetworkPublicKey,
    PublicKey,
    ShardIndex,
    Sliver,
    SliverPairIndex,
    SliverType,
};
use walrus_sdk::client::Client;
use walrus_sui::{
    client::{
        retry_client::RetriableRpcClient,
        BlobObjectMetadata,
        FixedSystemParameters,
        SuiClientError,
    },
    test_utils::system_setup::SystemContext,
    types::{
        move_structs::{EpochState, EventBlob, NodeMetadata, VotingParams},
        Committee,
        ContractEvent,
        NetworkAddress,
        NodeRegistrationParams,
        StorageNode as SuiStorageNode,
        StorageNodeCap,
        GENESIS_EPOCH,
    },
};
use walrus_test_utils::WithTempDir;
use walrus_utils::backoff::ExponentialBackoffConfig;

#[cfg(msim)]
use crate::common::config::SuiConfig;
#[cfg(msim)]
use crate::node::ConfigLoader;
use crate::{
    common::active_committees::ActiveCommittees,
    node::{
        committee::{
            BeginCommitteeChangeError,
            CommitteeLookupService,
            CommitteeService,
            DefaultNodeServiceFactory,
            EndCommitteeChangeError,
            NodeCommitteeService,
        },
        config::{self, ConfigSynchronizerConfig, ShardSyncConfig, StorageNodeConfig},
        contract_service::SystemContractService,
        errors::{SyncNodeConfigError, SyncShardClientError},
        events::{
            event_processor::EventProcessor,
            CheckpointEventPosition,
            EventStreamCursor,
            InitState,
            PositionedStreamEvent,
        },
        server::{RestApiConfig, RestApiServer},
        system_events::{EventManager, EventRetentionManager, SystemEventProvider},
        DatabaseConfig,
        Storage,
        StorageNode,
    },
};

/// Default buyer subsidy rate (5%)
const DEFAULT_BUYER_SUBSIDY_RATE: u16 = 500;
/// Default system subsidy rate (6%)
const DEFAULT_SYSTEM_SUBSIDY_RATE: u16 = 600;
/// Default initial subsidy funds amount
pub const DEFAULT_SUBSIDY_FUNDS: u64 = 1_000_000;

/// A system event manager that provides events from a stream. It does not support dropping events.
#[derive(Debug)]
pub struct DefaultSystemEventManager {
    event_provider: Box<dyn SystemEventProvider>,
}

impl DefaultSystemEventManager {
    /// Creates a new system event manager with the provided event provider.
    pub fn new(event_provider: Box<dyn SystemEventProvider>) -> Self {
        Self { event_provider }
    }
}

#[async_trait]
impl SystemEventProvider for DefaultSystemEventManager {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> anyhow::Result<
        Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>,
        anyhow::Error,
    > {
        self.event_provider.events(cursor).await
    }
    async fn init_state(
        &self,
        _from: EventStreamCursor,
    ) -> Result<Option<InitState>, anyhow::Error> {
        Ok(None)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl EventRetentionManager for DefaultSystemEventManager {
    async fn drop_events_before(&self, _cursor: EventStreamCursor) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
impl EventManager for DefaultSystemEventManager {}

/// Trait representing a storage node handle.
/// The trait is used to abstract over the different types of storage node handles.
/// More specifically, there are two types:
///   - a node handle that the tester has control over all the internal components of the node.
///   - a node handle that is used in the simulation tests which supports crash and recovery.
pub trait StorageNodeHandleTrait {
    /// Cancels the node's event loop and REST API.
    fn cancel(&self);

    /// Returns the client that can be used to communicate with the node.
    fn client(&self) -> &Client;

    /// Builds a new storage node handle, and starts the node.
    fn build_and_run(
        builder: StorageNodeHandleBuilder,
        system_context: Option<SystemContext>,
        sui_rpc_urls: Vec<String>,
        storage_dir: TempDir,
        start_node: bool,
        disable_event_blob_writer: bool,
    ) -> impl std::future::Future<Output = anyhow::Result<Self>> + Send
    where
        Self: Sized;

    /// Returns the storage node.
    fn storage_node(&self) -> &Arc<StorageNode>;

    /// Returns the node's protocol public key.
    fn public_key(&self) -> &PublicKey;

    /// Returns whether the storage node should use a distinct IP address.
    fn use_distinct_ip() -> bool;
}

/// Configuration for test node setup
#[derive(Debug, Clone, Default)]
pub struct TestNodesConfig {
    /// The weights of the nodes in the cluster.
    pub node_weights: Vec<u16>,
    /// Whether to use the legacy event processor.
    pub use_legacy_event_processor: bool,
    /// Whether to disable the event blob writer.
    pub disable_event_blob_writer: bool,
    /// The directory to store the blocklist.
    pub blocklist_dir: Option<PathBuf>,
    /// Whether to enable the node config monitor.
    pub enable_node_config_synchronizer: bool,
}

/// A storage node and associated data for testing.
#[derive(Debug)]
pub struct StorageNodeHandle {
    /// The wrapped storage node.
    pub storage_node: Arc<StorageNode>,
    /// The temporary directory containing the node's storage.
    pub storage_directory: TempDir,
    /// The node's protocol public key.
    pub public_key: PublicKey,
    /// The node's protocol public key.
    pub network_public_key: NetworkPublicKey,
    /// The address of the REST API.
    pub rest_api_address: SocketAddr,
    /// The address of the metric service.
    pub metrics_address: SocketAddr,
    /// Handle the REST API.
    pub rest_api: Arc<RestApiServer<StorageNode>>,
    /// Cancellation token for the REST API.
    pub cancel: CancellationToken,
    /// Client that can be used to communicate with the node.
    pub client: Client,
    /// The storage capability object of the node.
    pub storage_node_capability: Option<StorageNodeCap>,
    /// The handle to the node runtime.
    pub node_runtime_handle: Option<JoinHandle<()>>,
}

impl StorageNodeHandleTrait for StorageNodeHandle {
    fn cancel(&self) {
        self.cancel.cancel();
    }

    fn client(&self) -> &Client {
        &self.client
    }

    fn storage_node(&self) -> &Arc<StorageNode> {
        &self.storage_node
    }

    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    async fn build_and_run(
        builder: StorageNodeHandleBuilder,
        _system_context: Option<SystemContext>,
        _sui_rpc_urls: Vec<String>,
        _storage_dir: TempDir,
        _start_node: bool,
        _disable_event_blob_writer: bool,
    ) -> anyhow::Result<Self> {
        builder.build().await
    }

    // StorageNodeHandle is only used in integration test without crash and recovery. No need to
    // use distinct IP.
    fn use_distinct_ip() -> bool {
        false
    }
}

impl StorageNodeHandle {
    /// Creates a new builder.
    pub fn builder() -> StorageNodeHandleBuilder {
        StorageNodeHandleBuilder::default()
    }
}

impl AsRef<StorageNode> for StorageNodeHandle {
    fn as_ref(&self) -> &StorageNode {
        &self.storage_node
    }
}

#[cfg(msim)]
#[derive(Debug)]
struct SimStorageNodeConfigLoader {
    config: Arc<RwLock<StorageNodeConfig>>,
}

#[cfg(msim)]
impl SimStorageNodeConfigLoader {
    pub fn new(config: Arc<RwLock<StorageNodeConfig>>) -> Self {
        Self { config }
    }
}

#[cfg(msim)]
#[async_trait]
impl ConfigLoader for SimStorageNodeConfigLoader {
    async fn load_storage_node_config(&self) -> anyhow::Result<StorageNodeConfig> {
        Ok(self.config.read().await.clone())
    }
}

/// A storage node handle for simulation tests. Comparing to StorageNodeHandle, this struct
/// removes any storage node internal state, and adds a simulator node id for node management.
#[cfg(msim)]
#[derive(Debug)]
pub struct SimStorageNodeHandle {
    /// The temporary directory containing the node's storage.
    pub storage_directory: TempDir,
    /// The node's protocol public key.
    pub public_key: PublicKey,
    /// The node's protocol public key.
    pub network_public_key: NetworkPublicKey,
    /// The address of the REST API.
    pub rest_api_address: SocketAddr,
    /// The address of the metric service.
    pub metrics_address: SocketAddr,
    /// Cancellation token for the node.
    pub cancel_token: CancellationToken,
    /// The wrapped simulator node id.
    pub node_id: Option<sui_simulator::task::NodeId>,
    /// The storage capability object of the node.
    pub storage_node_capability: Option<StorageNodeCap>,
    /// The storage node config.
    pub storage_node_config: StorageNodeConfig,
    /// A reference to the storage node config used by the storage node.
    pub node_config_arc: Arc<RwLock<StorageNodeConfig>>,
}

#[cfg(msim)]
impl SimStorageNodeHandle {
    /// Starts and runs a storage node with the provided configuration in a dedicated simulator
    /// node.
    pub async fn spawn_node(
        config: Arc<RwLock<StorageNodeConfig>>,
        num_checkpoints_per_blob: Option<u32>,
        cancel_token: CancellationToken,
    ) -> sui_simulator::runtime::NodeHandle {
        let (startup_sender, mut startup_receiver) = tokio::sync::watch::channel(false);

        // Extract the IP address from the configuration.
        let ip = match config.read().await.rest_api_address {
            SocketAddr::V4(v4) => std::net::IpAddr::V4(*v4.ip()),
            _ => panic!("unsupported protocol"),
        };

        let config_loader = Arc::new(SimStorageNodeConfigLoader::new(config.clone()));

        let startup_sender = Arc::new(startup_sender);
        let handle = sui_simulator::runtime::Handle::current();
        let builder = handle.create_node();

        // This is the entry point of node restart.
        let node_handle = builder
            .ip(ip)
            .name(&format!(
                "{:?}",
                config
                    .read()
                    .await
                    .protocol_key_pair
                    .get()
                    .expect("config must contain protocol key pair")
                    .public()
            ))
            .init(move || {
                tracing::info!(?ip, "starting simulator node");

                let config = config.clone();
                let cancel_token = cancel_token.clone();
                let startup_sender = startup_sender.clone();
                let config_loader = config_loader.clone();
                async move {
                    let result = async {
                        let (rest_api_handle, node_handle, event_processor_handle) =
                            Self::build_and_run_node(
                                config.clone(),
                                num_checkpoints_per_blob,
                                config_loader,
                                cancel_token.clone(),
                            )
                            .await?;

                        startup_sender.send(true).ok();

                        tokio::select! {
                            _ = rest_api_handle => {
                                tracing::info!("REST API stopped");
                                Ok(())
                            }
                            result = node_handle => {
                                tracing::info!("node handle completed");
                                result?
                            }
                            _ = event_processor_handle => {
                                tracing::info!("event processor stopped");
                                Ok(())
                            }
                            _ = cancel_token.cancelled() => {
                                tracing::info!("node stopped by cancellation");
                                Ok(())
                            }
                        }
                    }
                    .await;

                    match result {
                        Err(e) => {
                            if matches!(
                                e.downcast_ref::<SyncNodeConfigError>(),
                                Some(SyncNodeConfigError::ProtocolKeyPairRotationRequired)
                            ) {
                                let mut config_guard = config.write().await;
                                tracing::info!(
                                    protocol_key = ?config_guard.protocol_key_pair().public(),
                                    next_protocol_key = ?config_guard
                                        .next_protocol_key_pair()
                                        .map(|kp| kp.public()),
                                    "rotating protocol key pair"
                                );
                                config_guard.rotate_protocol_key_pair();
                                sui_simulator::task::kill_current_node(Some(Duration::from_secs(
                                    10,
                                )));
                            } else if matches!(
                                e.downcast_ref::<SyncNodeConfigError>(),
                                Some(SyncNodeConfigError::NodeNeedsReboot)
                            ) {
                                tracing::info!("node needs reboot, killing current node");
                                sui_simulator::task::kill_current_node(Some(Duration::from_secs(
                                    10,
                                )));
                            } else {
                                tracing::info!("node stopped with error: {e}");
                            }
                        }
                        Ok(()) => {
                            tracing::info!("node stopped");
                        }
                    }
                }
            })
            .build();

        // Wait for the node to start and running.
        startup_receiver
            .changed()
            .await
            .expect("waiting for node to start failed");

        node_handle
    }

    /// Builds and runs a storage node with the provided configuration. Returns the handles to the
    /// REST API, the node, and the event processor.
    async fn build_and_run_node(
        config: Arc<RwLock<StorageNodeConfig>>,
        num_checkpoints_per_blob: Option<u32>,
        config_loader: Arc<dyn ConfigLoader>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<(
        tokio::task::JoinHandle<Result<(), anyhow::Error>>,
        tokio::task::JoinHandle<Result<(), anyhow::Error>>,
        tokio::task::JoinHandle<Result<(), anyhow::Error>>,
    )> {
        // TODO(#996): extract the common code to start the code, and use it here as well as in
        // StorageNodeRuntime::start.
        let config = config.read().await.clone();

        let metrics_registry = Registry::default();

        let sui_config = config
            .sui
            .clone()
            .expect("simulation must set sui config in storage node config");

        // Starts the event processor thread if the node is configured to use the checkpoint
        // based event processor.
        let sui_read_client = sui_config.new_read_client().await?;
        let event_provider: Box<dyn EventManager> = if config.use_legacy_event_provider {
            Box::new(crate::node::system_events::SuiSystemEventProvider::new(
                sui_read_client.clone(),
                Duration::from_millis(100),
            ))
        } else {
            let processor_config =
                crate::node::events::event_processor::EventProcessorRuntimeConfig {
                    rpc_addresses: sui_config
                        .additional_rpc_endpoints
                        .iter()
                        .chain(std::iter::once(&sui_config.rpc))
                        .cloned()
                        .collect(),
                    event_polling_interval: Duration::from_millis(100),
                    db_path: nondeterministic!(tempfile::tempdir()
                        .expect("temporary directory creation must succeed")
                        .path()
                        .to_path_buf()),
                    rpc_fallback_config: None,
                };
            let system_config = crate::node::events::event_processor::SystemConfig {
                system_object_id: sui_config.contract_config.system_object,
                staking_object_id: sui_config.contract_config.staking_object,
                system_pkg_id: sui_read_client.get_system_package_id(),
            };
            Box::new(
                EventProcessor::new(
                    &config.event_processor_config,
                    processor_config,
                    system_config,
                    &metrics_registry,
                )
                .await?,
            )
        };

        // Starts the event processor thread if it is configured, otherwise it produces a JoinHandle
        // that never returns.
        let event_processor_handle = if let Some(event_processor) =
            event_provider.as_any().downcast_ref::<EventProcessor>()
        {
            let cloned_cancel_token = cancel_token.clone();
            let cloned_event_processor = event_processor.clone();
            tokio::spawn(
                async move { cloned_event_processor.start(cloned_cancel_token).await }
                    .instrument(tracing::info_span!("cluster-event-processor",
                    address = %config.rest_api_address)),
            )
        } else {
            tokio::spawn(async { std::future::pending::<Result<(), anyhow::Error>>().await })
        };

        // Build storage node with the current configuration and event manager.
        let mut builder = StorageNode::builder();
        if let Some(num_checkpoints_per_blob) = num_checkpoints_per_blob {
            builder = builder.with_num_checkpoints_per_blob(num_checkpoints_per_blob);
        };
        builder = builder.with_config_loader(Some(config_loader));
        let node = builder
            .with_system_event_manager(event_provider)
            .build(&config, metrics_registry.clone())
            .await?;
        let node = Arc::new(node);

        // Starts rest api and node threads.
        let rest_api = Arc::new(RestApiServer::new(
            node.clone(),
            cancel_token.clone(),
            RestApiConfig::from(&config),
            &metrics_registry,
        ));

        let rest_api_handle = tokio::task::spawn(async move { rest_api.run().await }.instrument(
            tracing::info_span!("cluster-rest-api", address = %config.rest_api_address),
        ));

        let node_handle =
            tokio::task::spawn(async move { node.run(cancel_token).await }.instrument(
                tracing::info_span!("cluster-node", address = %config.rest_api_address),
            ));

        Ok((rest_api_handle, node_handle, event_processor_handle))
    }
}

/// A storage node handle builder for simulation tests. The main difference between
/// SimStorageNodeHandle and StorageNodeHandle is that SimStorageNodeHandle is used in crash
/// recovery simulation and therefore the StorageNode instance may change overtime.
#[cfg(msim)]
impl StorageNodeHandleTrait for SimStorageNodeHandle {
    fn cancel(&self) {
        unimplemented!("cannot directly cancel a storage proactively.")
    }

    fn client(&self) -> &Client {
        unimplemented!("simulation test does not use the pre-built storage node client.")
    }

    fn storage_node(&self) -> &Arc<StorageNode> {
        // Storage node state changes everytime the node restarts.
        unimplemented!("simulation test does not have a stable internal state.")
    }

    fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    async fn build_and_run(
        builder: StorageNodeHandleBuilder,
        system_context: Option<SystemContext>,
        sui_rpc_urls: Vec<String>,
        storage_dir: TempDir,
        start_node: bool,
        disable_event_blob_writer: bool,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let num_checkpoints_per_blob = builder.num_checkpoints_per_blob.as_ref().cloned();
        let node_capability = builder.storage_node_capability.as_ref().cloned();
        builder
            .start_node(
                system_context.expect("System context must be provided"),
                sui_rpc_urls,
                storage_dir,
                start_node,
                disable_event_blob_writer,
                num_checkpoints_per_blob,
                node_capability,
            )
            .await
    }

    // Storage node in simulation requires to have distinct IP since ip represents the id of
    // the simulator node.
    fn use_distinct_ip() -> bool {
        true
    }
}

#[cfg(msim)]
impl Drop for SimStorageNodeHandle {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Builds a new [`StorageNodeHandle`] with custom configuration values.
///
/// Can be created with the methods [`StorageNodeHandle::builder()`] or with
/// [`StorageNodeHandleBuilder::new()`].
///
/// Methods can be chained in order to set the configuration values, with the `StorageNode` being
/// constructed by calling [`build`][Self::build`].
///
/// See function level documentation for details on the various configuration settings.
#[derive(Debug)]
pub struct StorageNodeHandleBuilder {
    name: Option<String>,
    storage: Option<WithTempDir<Storage>>,
    blocklist_path: Option<PathBuf>,
    event_provider: Box<dyn SystemEventProvider>,
    committee_service: Option<Arc<dyn CommitteeService>>,
    contract_service: Option<Arc<dyn SystemContractService>>,
    run_rest_api: bool,
    run_node: bool,
    disable_event_blob_writer: bool,
    test_config: Option<StorageNodeTestConfig>,
    shard_sync_config: Option<ShardSyncConfig>,
    initial_epoch: Option<Epoch>,
    storage_node_capability: Option<StorageNodeCap>,
    node_wallet_dir: Option<PathBuf>,
    num_checkpoints_per_blob: Option<u32>,
    enable_node_config_synchronizer: bool,
}

impl StorageNodeHandleBuilder {
    /// Creates a new builder, which by default creates a storage node without any assigned shards
    /// and without its REST API or event loop running.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the storage associated with the node.
    ///
    /// If a committee service is *not* provided with [`Self::with_committee_service`], then the
    /// storage also dictates the shard assignment to this storage node in the created committee.
    pub fn with_storage(mut self, storage: WithTempDir<Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Enable or disable the node's config monitor.
    pub fn with_enable_node_config_synchronizer(mut self, enable: bool) -> Self {
        self.enable_node_config_synchronizer = enable;
        self
    }

    /// Sets the shard sync config for the node.
    pub fn with_shard_sync_config(mut self, shard_sync_config: ShardSyncConfig) -> Self {
        self.shard_sync_config = Some(shard_sync_config);
        self
    }

    /// Sets the service providing events to the storage node.
    pub fn with_system_event_provider<T>(self, event_provider: T) -> Self
    where
        T: SystemEventProvider + Into<Box<T>> + 'static,
    {
        self.with_boxed_system_event_provider(event_provider.into())
    }

    /// Sets the service providing events to the storage node.
    pub fn with_boxed_system_event_provider(
        mut self,
        event_provider: Box<dyn SystemEventProvider>,
    ) -> Self {
        self.event_provider = event_provider;
        self
    }

    /// Sets the [`CommitteeService`] used with the node.
    ///
    /// If not provided, defaults to [`StubCommitteeService`] created with a valid committee
    /// constructed over at most 1 other node. Note that if the node has no shards assigned to it
    /// (as inferred from the storage), it will not be in the committee.
    pub fn with_committee_service(mut self, service: Arc<dyn CommitteeService>) -> Self {
        self.committee_service = Some(service);
        self
    }

    /// Sets the [`SystemContractService`] to be used with the node.
    ///
    /// If not provided, defaults to a [`StubContractService`].
    pub fn with_system_contract_service(
        mut self,
        contract_service: Arc<dyn SystemContractService>,
    ) -> Self {
        self.contract_service = Some(contract_service);
        self
    }

    /// Enable or disable the node's event loop being started on build.
    pub fn with_node_started(mut self, run_node: bool) -> Self {
        self.run_node = run_node;
        self
    }

    /// Enable or disable event blob writer on the node.
    pub fn with_disabled_event_blob_writer(mut self, disable: bool) -> Self {
        self.disable_event_blob_writer = disable;
        self
    }

    /// Enable or disable the REST API being started on build.
    pub fn with_rest_api_started(mut self, run_rest_api: bool) -> Self {
        self.run_rest_api = run_rest_api;
        self
    }

    /// Sets the number of checkpoints per blob.
    pub fn with_num_checkpoints_per_blob(mut self, num_checkpoints_per_blob: u32) -> Self {
        self.num_checkpoints_per_blob = Some(num_checkpoints_per_blob);
        self
    }

    /// Specify the shard assignment for this node.
    ///
    /// If specified, it will determine the the shard assignment for the node in the committee. If
    /// not, the shard assignment will be inferred from the shards present in the storage.
    ///
    /// Resets any prior calls to [`Self::with_test_config`].
    pub fn with_shard_assignment(mut self, shards: &[ShardIndex]) -> Self {
        self.test_config = Some(StorageNodeTestConfig::new(shards.into(), false));
        self
    }

    /// Specify the test config for this node.
    ///
    /// If specified, it will determine the the shard assignment for the node in the committee
    /// as well as the network address and the protocol key.
    ///
    /// Resets any prior calls to [`Self::with_shard_assignment`].
    pub fn with_test_config(mut self, test_config: StorageNodeTestConfig) -> Self {
        self.test_config = Some(test_config);
        self
    }

    /// Specify the initial epoch for the node.
    ///
    /// If specified, the committee service will be created with the provided epoch.
    pub fn with_initial_epoch(mut self, epoch: Option<Epoch>) -> Self {
        self.initial_epoch = epoch;
        self
    }

    /// Specify the storage capability for the node.
    pub fn with_storage_node_capability(
        mut self,
        storage_node_capability: Option<StorageNodeCap>,
    ) -> Self {
        self.storage_node_capability = storage_node_capability;
        self
    }

    /// Specify the blocklist file for the node.
    pub fn with_blocklist_file(mut self, blocklist_file: Option<PathBuf>) -> Self {
        self.blocklist_path = blocklist_file;
        self
    }

    /// Specify the sui wallet directory for the node.
    pub fn with_node_wallet_dir(mut self, node_wallet_dir: Option<PathBuf>) -> Self {
        self.node_wallet_dir = node_wallet_dir;
        self
    }

    /// Specify the node's name.
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Creates the configured [`StorageNodeHandle`].
    pub async fn build(self) -> anyhow::Result<StorageNodeHandle> {
        // Identify the storage being used, as it allows us to extract the shards
        // that should be assigned to this storage node.
        let WithTempDir {
            inner: storage,
            temp_dir,
        } = {
            if let Some(storage) = self.storage {
                storage
            } else {
                empty_storage_with_shards(&[]).await
            }
        };

        let node_info = {
            if let Some(test_config) = self.test_config {
                test_config
            } else {
                let shards = storage.shards_present().await;
                StorageNodeTestConfig::new(shards, false)
            }
        };

        // To be in the committee, the node must have at least one shard assigned to it.
        let is_in_committee = !node_info.shards.is_empty();

        let public_key = node_info.key_pair.public().clone();
        let network_public_key = node_info.network_key_pair.public().clone();

        let committee_service = self.committee_service.unwrap_or_else(|| {
            // Create a list of the committee members, that contains one or two nodes.
            let committee_members = [
                is_in_committee.then(|| node_info.to_storage_node_info("node-under-test")),
                committee_partner(&node_info).map(|info| info.to_storage_node_info("other-node")),
            ];
            debug_assert!(committee_members[0].is_some() || committee_members[1].is_some());

            let committee = committee_from_members(
                // Remove the possible None in the members list
                committee_members.into_iter().flatten().collect(),
                self.initial_epoch,
            );
            Arc::new(StubCommitteeService {
                encoding: EncodingConfig::new(committee.n_shards()).into(),
                committee: committee.into(),
            })
        });

        let contract_service = self.contract_service.unwrap_or_else(|| {
            Arc::new(StubContractService {
                system_parameters: FixedSystemParameters {
                    n_shards: committee_service.active_committees().n_shards(),
                    max_epochs_ahead: 200,
                    epoch_duration: Duration::from_secs(600),
                    epoch_zero_end: Utc::now() + Duration::from_secs(60),
                },
                node_capability_object: self
                    .storage_node_capability
                    .clone()
                    .unwrap_or_else(StorageNodeCap::new_for_testing),
            })
        });

        // Create the node's config using the previously generated keypair and address.
        let config = StorageNodeConfig {
            name: self.name.unwrap_or_else(|| "node".to_string()),
            storage_path: temp_dir.path().to_path_buf(),
            protocol_key_pair: node_info.key_pair.into(),
            next_protocol_key_pair: None,
            network_key_pair: node_info.network_key_pair.into(),
            rest_api_address: node_info.rest_api_address,
            public_host: node_info.rest_api_address.ip().to_string(),
            public_port: node_info.rest_api_address.port(),
            blocklist_path: self.blocklist_path,
            shard_sync_config: self.shard_sync_config.unwrap_or_default(),
            disable_event_blob_writer: self.disable_event_blob_writer,
            config_synchronizer: ConfigSynchronizerConfig {
                interval: Duration::from_secs(5),
                enabled: self.enable_node_config_synchronizer,
            },
            storage_node_cap: self.storage_node_capability.clone().map(|cap| cap.id),
            ..storage_node_config().inner
        };

        let cancel_token = CancellationToken::new();

        if let Some(event_processor) = self
            .event_provider
            .as_any()
            .downcast_ref::<EventProcessor>()
        {
            let cloned_cancel_token = cancel_token.clone();
            let cloned_event_processor = event_processor.clone();
            spawn_event_processor(
                cloned_event_processor,
                cloned_cancel_token,
                config.rest_api_address.to_string(),
            );
            wait_for_event_processor_to_start(
                event_processor.clone(),
                event_processor.client.clone(),
            )
            .await?;
        }

        let metrics_registry = Registry::default();
        let mut builder = StorageNode::builder();
        if let Some(num_checkpoints_per_blob) = self.num_checkpoints_per_blob {
            builder = builder.with_num_checkpoints_per_blob(num_checkpoints_per_blob);
        };
        let node = builder
            .with_storage(storage)
            .with_system_event_manager(Box::new(DefaultSystemEventManager::new(
                self.event_provider,
            )))
            .with_committee_service(committee_service)
            .with_system_contract_service(contract_service)
            .build(&config, metrics_registry.clone())
            .await?;
        let node = Arc::new(node);

        let rest_api = Arc::new(RestApiServer::new(
            node.clone(),
            cancel_token.clone(),
            RestApiConfig::from(&config),
            &metrics_registry,
        ));

        if self.run_rest_api {
            let rest_api_clone = rest_api.clone();

            tokio::task::spawn(
                async move {
                    let status = rest_api_clone.run().await;
                    if let Err(error) = status {
                        tracing::error!(?error, "REST API stopped with an error");
                        std::process::exit(1);
                    }
                }
                .instrument(
                    tracing::info_span!("cluster-node", address = %config.rest_api_address),
                ),
            );
        }

        let node_runtime_handle = if self.run_node {
            let node = node.clone();
            let cancel_token = cancel_token.clone();

            Some(tokio::task::spawn(
                async move {
                    let status = node.run(cancel_token).await;
                    if let Err(error) = status {
                        tracing::error!(?error, "node stopped with an error");
                        std::process::exit(1);
                    }
                }
                .instrument(
                    tracing::info_span!("cluster-node", address = %config.rest_api_address),
                ),
            ))
        } else {
            None
        };

        let client = Client::builder()
            .authenticate_with_public_key(network_public_key.clone())
            // Disable proxy and root certs from the OS for tests.
            .no_proxy()
            .tls_built_in_root_certs(false)
            .build_for_remote_ip(config.rest_api_address)?;

        if self.run_rest_api {
            wait_for_rest_api_ready(&client).await?;
        }

        Ok(StorageNodeHandle {
            storage_node: node,
            storage_directory: temp_dir,
            public_key,
            network_public_key,
            rest_api_address: config.rest_api_address,
            metrics_address: config.metrics_address,
            rest_api,
            cancel: cancel_token,
            client,
            storage_node_capability: self.storage_node_capability,
            node_runtime_handle,
        })
    }

    /// Starts and running a storage node with the provided configuration.
    #[cfg(msim)]
    pub async fn start_node(
        self,
        system_context: SystemContext,
        sui_rpc_urls: Vec<String>,
        storage_dir: TempDir,
        start_node: bool,
        disable_event_blob_writer: bool,
        num_checkpoints_per_blob: Option<u32>,
        node_capability: Option<StorageNodeCap>,
    ) -> anyhow::Result<SimStorageNodeHandle> {
        use walrus_sui::client::contract_config::ContractConfig;

        let node_info = self
            .test_config
            .expect("test config must be provided to spawn a storage node");

        // Builds the storage node config used to run the node.
        let storage_node_config = StorageNodeConfig {
            storage_path: storage_dir.path().to_path_buf(),
            protocol_key_pair: node_info.key_pair.into(),
            next_protocol_key_pair: None,
            network_key_pair: node_info.network_key_pair.into(),
            rest_api_address: node_info.rest_api_address,
            public_host: node_info.rest_api_address.ip().to_string(),
            public_port: node_info.rest_api_address.port(),
            event_processor_config: Default::default(),
            use_legacy_event_provider: false,
            disable_event_blob_writer,
            sui: Some(SuiConfig {
                rpc: sui_rpc_urls
                    .first()
                    .expect("SUI RPC URL must be provided in integration test")
                    .to_string(),
                contract_config: ContractConfig::new(
                    system_context.system_object,
                    system_context.staking_object,
                ),
                wallet_config: walrus_sui::config::WalletConfig::from_path(
                    self.node_wallet_dir.unwrap().join("wallet_config.yaml"),
                ),
                event_polling_interval: config::defaults::polling_interval(),
                backoff_config: ExponentialBackoffConfig::default(),
                gas_budget: None,
                rpc_fallback_config: None,
                additional_rpc_endpoints: sui_rpc_urls
                    .iter()
                    .skip(1)
                    .map(|url| url.clone())
                    .collect::<Vec<String>>(),
            }),
            config_synchronizer: ConfigSynchronizerConfig {
                interval: Duration::from_secs(5),
                enabled: self.enable_node_config_synchronizer,
            },
            storage_node_cap: node_capability.map(|cap| cap.id),
            ..storage_node_config().inner
        };

        let cancel_token = CancellationToken::new();
        let node_config_arc = Arc::new(RwLock::new(storage_node_config.clone()));

        let node_id = if start_node {
            Some(
                SimStorageNodeHandle::spawn_node(
                    node_config_arc.clone(),
                    num_checkpoints_per_blob,
                    cancel_token.clone(),
                )
                .await
                .id(),
            )
        } else {
            None
        };

        Ok(SimStorageNodeHandle {
            storage_directory: storage_dir,
            public_key: storage_node_config
                .protocol_key_pair
                .get()
                .expect("protocol key must be set")
                .public()
                .clone(),
            network_public_key: storage_node_config
                .network_key_pair
                .get()
                .expect("network key must be set")
                .public()
                .clone(),
            rest_api_address: storage_node_config.rest_api_address,
            metrics_address: storage_node_config.metrics_address,
            cancel_token,
            node_id,
            storage_node_capability: self.storage_node_capability,
            storage_node_config,
            node_config_arc,
        })
    }
}

impl Default for StorageNodeHandleBuilder {
    fn default() -> Self {
        Self {
            name: None,
            shard_sync_config: None,
            event_provider: Box::<Vec<ContractEvent>>::default(),
            blocklist_path: None,
            committee_service: None,
            storage: Default::default(),
            run_rest_api: Default::default(),
            run_node: Default::default(),
            disable_event_blob_writer: Default::default(),
            contract_service: None,
            test_config: None,
            initial_epoch: None,
            storage_node_capability: None,
            node_wallet_dir: None,
            num_checkpoints_per_blob: None,
            enable_node_config_synchronizer: false,
        }
    }
}

/// Waits until the node is ready by querying the node's health info endpoint using the node
/// client.
async fn wait_for_rest_api_ready(client: &Client) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Err(err) = client.get_server_health_info(false).await {
            tracing::trace!(%err, "node is not ready yet, retrying...");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    })
    .await?
}

/// Returns with a test config for a storage node that would make a valid committee when paired
/// with the provided node, if necessary.
///
/// The number of shards in the system inferred from the shards assigned in the provided config.
/// It is at least 4 and is defined as `n = max(max(shard_ids) + 1, 4)`. If the shards `0..n` are
/// assigned to the existing node, then this function returns `None`. Otherwise, there must be a
/// second node in the committee with the shards not managed by the provided node.
fn committee_partner(node_config: &StorageNodeTestConfig) -> Option<StorageNodeTestConfig> {
    const MIN_SHARDS: u16 = 4;
    let n_shards = node_config
        .shards
        .iter()
        .max()
        .map(|index| index.get() + 1)
        .unwrap_or(MIN_SHARDS)
        .max(MIN_SHARDS);

    let other_shards: Vec<_> = ShardIndex::range(..n_shards)
        .filter(|id| !node_config.shards.contains(id))
        .collect();

    if !other_shards.is_empty() {
        Some(StorageNodeTestConfig::new(other_shards, false))
    } else {
        None
    }
}

#[cfg(not(msim))]
fn spawn_event_processor(
    event_processor: EventProcessor,
    cancellation_token: CancellationToken,
    rest_api_address: String,
) -> JoinHandle<()> {
    get_runtime().spawn(
        async move {
            let status = event_processor.start(cancellation_token).await;
            if let Err(error) = status {
                tracing::error!(?error, "event processor stopped with anerror");
                std::process::exit(1);
            }
        }
        .instrument(tracing::info_span!("cluster-node", address = %rest_api_address)),
    )
}

#[cfg(msim)]
fn spawn_event_processor(
    event_processor: EventProcessor,
    cancellation_token: CancellationToken,
    rest_api_address: String,
) -> JoinHandle<()> {
    tokio::task::spawn(
        async move {
            let status = event_processor.start(cancellation_token).await;
            if let Err(error) = status {
                tracing::error!(?error, "event processor stopped with an error");
                std::process::exit(1);
            }
        }
        .instrument(tracing::info_span!("cluster-node", address = %rest_api_address)),
    )
}

/// Returns a runtime that can be used to spawn tasks.
#[cfg(not(msim))]
fn get_runtime() -> std::sync::MutexGuard<'static, tokio::runtime::Runtime> {
    use std::sync::OnceLock;

    static CLUSTER: OnceLock<std::sync::Mutex<tokio::runtime::Runtime>> = OnceLock::new();
    CLUSTER
        .get_or_init(|| {
            std::sync::Mutex::new(
                std::thread::spawn(move || {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("should be able to build runtime")
                })
                .join()
                .expect("should be able to wait for thread to finish"),
            )
        })
        .lock()
        .unwrap()
}

fn create_previous_committee(committee: &Committee) -> Option<Committee> {
    if committee.epoch == GENESIS_EPOCH {
        None
    } else {
        let members = if committee.epoch - 1 == GENESIS_EPOCH {
            vec![]
        } else {
            committee.members().to_vec()
        };

        Some(
            Committee::new(members, committee.epoch - 1, committee.n_shards())
                .expect("all fields are valid"),
        )
    }
}

/// A stub `CommitteeLookupService`.
///
/// Does not perform any network operations.
#[derive(Debug, Clone)]
pub struct StubLookupService {
    committees: Arc<std::sync::Mutex<ActiveCommittees>>,
}

impl StubLookupService {
    /// Create a new `StubLookupService` with the specified committee.
    ///
    /// The previous committee is set to the same committee but with an epoch of 1 less. If the
    /// provided committee is at epoch 0 or 1, then the prior committee is non-existent or the empty
    /// committee respectively.
    pub fn new(committee: Committee) -> Self {
        let previous = create_previous_committee(&committee);
        Self {
            committees: Arc::new(std::sync::Mutex::new(ActiveCommittees::new(
                committee, previous,
            ))),
        }
    }

    /// Returns a handle that can be used to modify the ActiveCommittees returned by the
    /// lookup service.
    pub fn handle(&self) -> StubLookupServiceHandle {
        StubLookupServiceHandle {
            committees: self.committees.clone(),
        }
    }
}

/// A handle to a [`StubLookupService`] which can be used to modify the value of the services.
#[derive(Debug, Clone)]
pub struct StubLookupServiceHandle {
    /// The active committees.
    pub committees: Arc<std::sync::Mutex<ActiveCommittees>>,
}

impl StubLookupServiceHandle {
    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.committees.lock().unwrap().epoch()
    }

    /// Advances the epoch to the next epoch, skipping the transitioning phase.
    pub fn advance_epoch(&self) -> Epoch {
        let mut committees = self.committees.lock().unwrap();
        let current = (**committees.current_committee()).clone();
        let next_epoch = current.epoch + 1;
        let next = if let Some(next_committee) = committees.next_committee() {
            (**next_committee).clone()
        } else {
            Committee::new(current.members().to_vec(), next_epoch, current.n_shards())
                .expect("static committee is valid")
        };
        *committees = ActiveCommittees::new(next, Some(current));
        next_epoch
    }

    /// Sets the next epoch committee.
    pub fn set_next_epoch_committee(&self, next_committee: Committee) {
        let mut committees = self.committees.lock().unwrap();
        *committees = ActiveCommittees::new_with_next(
            committees.current_committee().clone(),
            committees.previous_committee().cloned(),
            Some(Arc::new(next_committee)),
            true,
        )
    }
}

#[async_trait]
impl CommitteeLookupService for StubLookupService {
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        Ok(self.committees.lock().unwrap().clone())
    }
}

/// A stub [`CommitteeService`].
///
/// Does not perform any network operations.
#[derive(Debug)]
pub struct StubCommitteeService {
    /// The current committee.
    pub committee: Arc<Committee>,
    /// The system's encoding config.
    pub encoding: Arc<EncodingConfig>,
}

#[async_trait]
impl CommitteeService for StubCommitteeService {
    fn get_epoch(&self) -> Epoch {
        1
    }

    fn get_shard_count(&self) -> NonZeroU16 {
        self.committee.n_shards()
    }

    fn encoding_config(&self) -> &Arc<EncodingConfig> {
        &self.encoding
    }

    async fn get_and_verify_metadata(
        &self,
        _blob_id: BlobId,
        _certified_epoch: Epoch,
    ) -> VerifiedBlobMetadataWithId {
        std::future::pending().await
    }

    async fn recover_sliver(
        &self,
        _metadata: Arc<VerifiedBlobMetadataWithId>,
        _sliver_id: SliverPairIndex,
        _sliver_type: SliverType,
        _certified_epoch: Epoch,
    ) -> Result<Sliver, InconsistencyProofEnum<MerkleProof>> {
        std::future::pending().await
    }

    async fn get_invalid_blob_certificate(
        &self,
        _blob_id: BlobId,
        _inconsistency_proof: &InconsistencyProofEnum,
    ) -> InvalidBlobCertificate {
        std::future::pending().await
    }

    async fn sync_shard_before_epoch(
        &self,
        _shard_index: ShardIndex,
        _starting_blob_id: BlobId,
        _sliver_type: SliverType,
        _sliver_count: u64,
        _epoch: Epoch,
        _key_pair: &ProtocolKeyPair,
    ) -> Result<Vec<(BlobId, Sliver)>, SyncShardClientError> {
        std::future::pending().await
    }

    fn active_committees(&self) -> ActiveCommittees {
        ActiveCommittees::new(
            self.committee.as_ref().clone(),
            create_previous_committee(&self.committee),
        )
    }

    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool {
        self.committee
            .members()
            .iter()
            .any(|node| node.public_key == *public_key)
    }

    async fn begin_committee_change(
        &self,
        _new_epoch: Epoch,
    ) -> Result<(), BeginCommitteeChangeError> {
        // Return immediately in test since begin committee change is a required step in starting
        // new storage node.
        Ok(())
    }

    fn end_committee_change(&self, _epoch: Epoch) -> Result<(), EndCommitteeChangeError> {
        Ok(())
    }

    async fn begin_committee_change_to_latest_committee(
        &self,
    ) -> Result<(), BeginCommitteeChangeError> {
        Ok(())
    }

    async fn sync_committee_members(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// A stub [`SystemContractService`].
///
/// Performs a no-op when calling [`invalidate_blob_id()`][Self::invalidate_blob_id]
#[derive(Debug)]
pub struct StubContractService {
    pub(crate) system_parameters: FixedSystemParameters,
    pub(crate) node_capability_object: StorageNodeCap,
}

#[async_trait]
impl SystemContractService for StubContractService {
    async fn sync_node_params(
        &self,
        _config: &StorageNodeConfig,
        _node_capability_object_id: ObjectID,
    ) -> Result<(), SyncNodeConfigError> {
        Ok(())
    }

    async fn invalidate_blob_id(&self, _certificate: &InvalidBlobCertificate) {}

    async fn epoch_sync_done(&self, _epoch: Epoch, _node_capability_object_id: ObjectID) {}

    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        anyhow::bail!("stub service does not store the epoch or state")
    }

    fn current_epoch(&self) -> Epoch {
        1
    }

    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error> {
        Ok(self.system_parameters.clone())
    }

    async fn end_voting(&self) -> Result<(), anyhow::Error> {
        anyhow::bail!("stub service cannot end voting")
    }

    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error> {
        anyhow::bail!("stub service cannot initiate epoch change")
    }

    async fn certify_event_blob(
        &self,
        _blob_metadata: BlobObjectMetadata,
        _ending_checkpoint_seq_num: u64,
        _epoch: u32,
        _node_capability_object_id: ObjectID,
    ) -> Result<(), SuiClientError> {
        Ok(())
    }

    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error> {
        anyhow::bail!("stub service cannot refresh contract package")
    }

    async fn get_node_capability_object(
        &self,
        _node_capability_object_id: Option<ObjectID>,
    ) -> Result<StorageNodeCap, SuiClientError> {
        Ok(self.node_capability_object.clone())
    }

    async fn get_system_object_version(&self) -> Result<u64, SuiClientError> {
        Ok(1)
    }

    async fn last_certified_event_blob(&self) -> Result<Option<EventBlob>, SuiClientError> {
        Ok(None)
    }
}

/// Returns a socket address that is not currently in use on the system.
///
/// distinct_ip: If true, the returned address will have a distinct IP address from the local
/// machine.
pub fn unused_socket_address(distinct_ip: bool) -> SocketAddr {
    if distinct_ip {
        try_unused_socket_address_with_distinct_ip()
            .expect("unused socket address with distinct ip to be available")
    } else {
        try_unused_socket_address().expect("unused socket address to be available")
    }
}

fn try_unused_socket_address() -> anyhow::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;

    // Create and accept a connection to force the port into the TIME_WAIT state
    let _client_stream = TcpStream::connect(address)?;
    let _server_stream = listener.accept()?;
    Ok(address)
}

fn try_unused_socket_address_with_distinct_ip() -> anyhow::Result<SocketAddr> {
    Ok(SocketAddr::from_str(
        (sui_config::local_ip_utils::get_new_ip() + ":1314").as_str(),
    )?)
}

#[async_trait::async_trait]
impl SystemEventProvider for Vec<ContractEvent> {
    async fn events(
        &self,
        _cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>, anyhow::Error>
    {
        Ok(Box::new(
            tokio_stream::iter(
                self.clone()
                    .into_iter()
                    .map(|c| PositionedStreamEvent::new(c, CheckpointEventPosition::new(0, 0))),
            )
            .chain(tokio_stream::pending()),
        ))
    }

    async fn init_state(
        &self,
        _from: EventStreamCursor,
    ) -> Result<Option<InitState>, anyhow::Error> {
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait::async_trait]
impl SystemEventProvider for tokio::sync::broadcast::Sender<ContractEvent> {
    async fn events(
        &self,
        _cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>, anyhow::Error>
    {
        Ok(Box::new(BroadcastStream::new(self.subscribe()).map(
            |value| {
                PositionedStreamEvent::new(
                    value.expect("should not return errors in test"),
                    CheckpointEventPosition::new(0, 0),
                )
            },
        )))
    }

    async fn init_state(
        &self,
        _from: EventStreamCursor,
    ) -> Result<Option<InitState>, anyhow::Error> {
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A cluster of [`StorageNodeHandle`]s corresponding to several running storage nodes.
#[derive(Debug)]
pub struct TestCluster<T: StorageNodeHandleTrait = StorageNodeHandle> {
    /// The running storage nodes.
    pub nodes: Vec<T>,
    /// A handle to the stub lookup service, is used.
    pub lookup_service_handle: Option<StubLookupServiceHandle>,
    /// The number of shards in the system.
    pub n_shards: usize,
}

impl<T: StorageNodeHandleTrait> TestCluster<T> {
    /// Returns a new builder to create the [`TestCluster`].
    pub fn builder() -> TestClusterBuilder {
        TestClusterBuilder::new(T::use_distinct_ip())
    }

    /// Returns an encoding config valid for use with the storage nodes.
    pub fn encoding_config(&self) -> EncodingConfig {
        let n_shards: u16 = self.n_shards.try_into().expect("valid number of shards");
        EncodingConfig::new(NonZeroU16::new(n_shards).expect("more than 1 shard"))
    }

    /// Stops the storage node with index `idx` by cancelling its task.
    pub fn cancel_node(&mut self, idx: usize) {
        assert!(
            idx < self.nodes.len(),
            "the index of the node to be dropped must be within the node vector"
        );
        self.nodes[idx].cancel();
    }

    /// Returns the client for the node at the specified index.
    pub fn client(&self, index: usize) -> &Client {
        self.nodes[index].client()
    }

    /// Wait for all nodes to arrive at at least the specified epoch.
    pub async fn wait_for_nodes_to_reach_epoch(&self, epoch: Epoch) {
        let waits: FuturesUnordered<_> = self
            .nodes
            .iter()
            .map(|handle| handle.storage_node().wait_for_epoch(epoch))
            .collect();
        waits.for_each(|_| std::future::ready(())).await;
    }
}

/// Builds a new [`TestCluster`] with custom configuration values.
///
/// Methods can be chained in order to set the configuration values, with the `TestCluster` being
/// constructed by calling [`build`][Self::build`].
///
/// Without further configuration, this will build a test cluster of 4 storage nodes with shards
/// being assigned as {0}, {1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10, 11, 12} to the nodes.
///
/// See function level documentation for details on the various configuration settings.
#[derive(Debug)]
pub struct TestClusterBuilder {
    storage_node_configs: Vec<StorageNodeTestConfig>,
    shard_sync_config: Option<ShardSyncConfig>,
    system_context: Option<SystemContext>,
    sui_rpc_urls: Vec<String>,
    use_distinct_ip: bool,
    // INV: Reset if shard_assignment is changed.
    event_providers: Vec<Option<Box<dyn SystemEventProvider>>>,
    committee_services: Vec<Option<Arc<dyn CommitteeService>>>,
    contract_services: Vec<Option<Arc<dyn SystemContractService>>>,
    storage_capabilities: Vec<Option<StorageNodeCap>>,
    node_wallet_dirs: Vec<Option<PathBuf>>,
    start_node_from_beginning: Vec<bool>,
    num_checkpoints_per_blob: Option<u32>,
    blocklist_files: Vec<Option<PathBuf>>,
    disable_event_blob_writer: Vec<bool>,
    enable_node_config_synchronizer: bool,
}

impl TestClusterBuilder {
    /// Returns a new default builder.
    pub fn new(use_distinct_ip: bool) -> Self {
        Self {
            storage_node_configs: TestClusterBuilder::default()
                .storage_node_configs
                .into_iter()
                .map(|config| StorageNodeTestConfig::new(config.shards, use_distinct_ip))
                .collect(),
            use_distinct_ip,
            ..Default::default()
        }
    }

    /// Returns a reference to the storage node test configs of the builder.
    pub fn storage_node_test_configs(&self) -> &Vec<StorageNodeTestConfig> {
        &self.storage_node_configs
    }

    /// Sets the enable storage node config monitor flag for each storage node.
    pub fn with_enable_node_config_synchronizer(
        mut self,
        enable_node_config_synchronizer: bool,
    ) -> Self {
        self.enable_node_config_synchronizer = enable_node_config_synchronizer;
        self
    }

    /// Sets the shard sync config for the cluster.
    pub fn with_shard_sync_config(mut self, shard_sync_config: ShardSyncConfig) -> Self {
        self.shard_sync_config = Some(shard_sync_config);
        self
    }

    /// Sets the number of storage nodes and their shard assignments from a sequence of the shards
    /// assigned to each storage.
    ///
    /// Resets any prior calls to [`Self::with_test_configs`],
    /// [`Self::with_system_event_providers`], and [`Self::with_committee_services`].
    pub fn with_shard_assignment<S, I>(mut self, assignment: &[S]) -> Self
    where
        S: Borrow<[I]>,
        for<'a> &'a I: Into<ShardIndex>,
    {
        let configs =
            storage_node_test_configs_from_shard_assignment(assignment, self.use_distinct_ip);

        self.event_providers = configs.iter().map(|_| None).collect();
        self.committee_services = configs.iter().map(|_| None).collect();
        self.storage_capabilities = configs.iter().map(|_| None).collect();
        self.node_wallet_dirs = configs.iter().map(|_| None).collect();
        self.storage_node_configs = configs;
        self
    }

    /// Sets the configurations for each storage node based on `configs`.
    ///
    /// Resets any prior calls to [`Self::with_shard_assignment`],
    /// [`Self::with_system_event_providers`], [`Self::with_committee_services`],
    /// and [`Self::with_system_contract_services`].
    pub fn with_test_configs(mut self, configs: Vec<StorageNodeTestConfig>) -> Self {
        self.event_providers = configs.iter().map(|_| None).collect();
        self.committee_services = configs.iter().map(|_| None).collect();
        self.contract_services = configs.iter().map(|_| None).collect();
        self.storage_capabilities = configs.iter().map(|_| None).collect();
        self.node_wallet_dirs = configs.iter().map(|_| None).collect();
        self.storage_node_configs = configs;
        self
    }

    /// Clones an event provider to be used with each storage node.
    ///
    /// Should be called after the storage nodes have been specified.
    pub fn with_system_event_providers<T>(mut self, event_provider: T) -> Self
    where
        T: SystemEventProvider + Clone + 'static,
    {
        self.event_providers = self
            .storage_node_configs
            .iter()
            .map(|_| Some(Box::new(event_provider.clone()) as _))
            .collect();
        self
    }

    /// Sets the individual event providers for each storage node.
    /// Requires: `event_providers.len() == storage_node_configs.len()`.
    pub fn with_individual_system_event_providers<T>(mut self, event_providers: &[T]) -> Self
    where
        T: SystemEventProvider + Clone + 'static,
    {
        assert_eq!(event_providers.len(), self.storage_node_configs.len());
        self.event_providers = event_providers
            .iter()
            .map(|provider| Some(Box::new(provider.clone()) as _))
            .collect();
        self
    }

    /// Sets the [`CommitteeService`] used for each storage node.
    ///
    /// Should be called after the storage nodes have been specified.
    pub fn with_committee_services(
        mut self,
        committee_services: &[Arc<dyn CommitteeService>],
    ) -> Self {
        assert_eq!(committee_services.len(), self.storage_node_configs.len());
        self.committee_services = committee_services
            .iter()
            .map(|service| Some(service.clone()))
            .collect();
        self
    }

    /// Sets the [`SystemContractService`] used for each storage node.
    ///
    /// Should be called after the storage nodes have been specified.
    pub fn with_system_contract_services<T, U>(mut self, contract_services: T) -> Self
    where
        T: IntoIterator<Item = U>,
        U: SystemContractService + Clone + 'static,
    {
        let contract_services: Vec<_> = contract_services
            .into_iter()
            .map(|service| Some(Arc::new(service.clone()) as _))
            .collect();
        assert_eq!(contract_services.len(), self.storage_node_configs.len());
        self.contract_services = contract_services;
        self
    }

    /// Sets the system context for the cluster.
    pub fn with_system_context(mut self, system_context: SystemContext) -> Self {
        self.system_context = Some(system_context);
        self
    }

    /// Sets the SUI RPC URL for the cluster.
    ///
    /// This is required for the storage nodes to connect to the SUI network.
    pub fn with_sui_rpc_urls(mut self, sui_rpc_urls: Vec<String>) -> Self {
        self.sui_rpc_urls = sui_rpc_urls;
        self
    }

    /// Sets the number of checkpoints per event blob for the cluster.
    pub fn with_num_checkpoints_per_blob(mut self, num_checkpoints_per_blob: u32) -> Self {
        self.num_checkpoints_per_blob = Some(num_checkpoints_per_blob);
        self
    }

    /// Sets the storage capabilities for each storage node.
    pub fn with_storage_capabilities(mut self, capabilities: Vec<StorageNodeCap>) -> Self {
        self.storage_capabilities = capabilities.into_iter().map(Some).collect();
        self
    }

    /// Sets the sui wallet config directory for each storage node.
    pub fn with_node_wallet_dirs(mut self, wallet_dirs: Vec<PathBuf>) -> Self {
        self.node_wallet_dirs = wallet_dirs.into_iter().map(Some).collect();
        self
    }

    /// Sets the start node from beginning flag for each storage node.
    pub fn with_start_node_from_beginning(mut self, start_node_from_beginning: Vec<bool>) -> Self {
        self.start_node_from_beginning = start_node_from_beginning;
        self
    }

    /// Sets the disable event blob writer flag for each storage node.
    pub fn with_disable_event_blob_writer(mut self, disable_event_blob_writer: Vec<bool>) -> Self {
        self.disable_event_blob_writer = disable_event_blob_writer;
        self
    }

    /// Sets the sui wallet config directory for each storage node.
    pub fn with_blocklist_files(mut self, blocklist_files: Vec<PathBuf>) -> Self {
        self.blocklist_files = blocklist_files.into_iter().map(Some).collect();
        self
    }

    /// Creates the configured `TestCluster`.
    pub async fn build<T: StorageNodeHandleTrait>(self) -> anyhow::Result<TestCluster<T>> {
        let committee_members: Vec<_> = self
            .storage_node_configs
            .iter()
            .enumerate()
            .map(|(i, info)| info.to_storage_node_info(&format!("node-{i}")))
            .collect();

        let n_shards = self
            .storage_node_configs
            .iter()
            .map(|config| config.shards.len())
            .sum::<usize>();

        // Create the stub lookup service and handles that may be used if none is provided.
        let mut lookup_service_and_handle = None;
        let mut node_futures = vec![];

        for (
            idx,
            (
                (
                    (
                        (
                            ((((config, event_provider), service), contract_service), capability),
                            node_wallet_dir,
                        ),
                        start_node_from_beginning,
                    ),
                    blocklist_file,
                ),
                disable_event_blob_writer,
            ),
        ) in self
            .storage_node_configs
            .into_iter()
            .zip(self.event_providers.into_iter())
            .zip(self.committee_services.into_iter())
            .zip(self.contract_services.into_iter())
            .zip(self.storage_capabilities.into_iter())
            .zip(self.node_wallet_dirs.into_iter())
            .zip(self.start_node_from_beginning.into_iter())
            .zip(self.blocklist_files.into_iter())
            .zip(self.disable_event_blob_writer.into_iter())
            .enumerate()
        {
            let storage = empty_storage_with_shards(&config.shards).await;
            let local_identity = config.key_pair.public().clone();
            let builder = StorageNodeHandle::builder()
                .with_storage(storage)
                .with_test_config(config)
                .with_rest_api_started(true)
                .with_node_started(true)
                .with_storage_node_capability(capability)
                .with_node_wallet_dir(node_wallet_dir)
                .with_blocklist_file(blocklist_file)
                .with_shard_sync_config(self.shard_sync_config.clone().unwrap_or_default())
                .with_disabled_event_blob_writer(disable_event_blob_writer)
                .with_enable_node_config_synchronizer(self.enable_node_config_synchronizer)
                .with_name(format!("node-{}", idx));
            tracing::info!(
                "test cluster builder build enable_node_config_synchronizer: {}",
                self.enable_node_config_synchronizer
            );

            let mut builder = if let Some(num_checkpoints_per_blob) = self.num_checkpoints_per_blob
            {
                builder.with_num_checkpoints_per_blob(num_checkpoints_per_blob)
            } else {
                builder
            };

            if let Some(provider) = event_provider {
                builder = builder.with_boxed_system_event_provider(provider);
            }

            builder = if let Some(service) = service {
                builder.with_committee_service(service)
            } else {
                let (lookup_service, _) = lookup_service_and_handle.get_or_insert_with(|| {
                    let committee = committee_from_members(committee_members.clone(), Some(1));
                    let lookup_service = StubLookupService::new(committee.clone());
                    let lookup_service_handle = lookup_service.handle();
                    (lookup_service, lookup_service_handle)
                });

                let service = NodeCommitteeService::builder()
                    .local_identity(local_identity)
                    .build_with_factory(
                        lookup_service.clone(),
                        DefaultNodeServiceFactory::avoid_system_services(),
                    )
                    .await?;
                builder.with_committee_service(Arc::new(service))
            };

            if let Some(service) = contract_service {
                builder = builder.with_system_contract_service(service);
            }

            // Build and run the storage nodes in parallel.
            node_futures.push(T::build_and_run(
                builder,
                self.system_context.clone(),
                self.sui_rpc_urls.clone(),
                nondeterministic!(
                    tempfile::tempdir().expect("temporary directory creation must succeed")
                ),
                start_node_from_beginning,
                disable_event_blob_writer,
            ));
        }

        // Returns error if any storage node fails to build and start.
        let nodes = future::join_all(node_futures)
            .await
            .into_iter()
            .enumerate()
            .map(|(idx, result)| {
                result.with_context(|| format!("Failed to start storage node {}", idx))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TestCluster {
            nodes,
            lookup_service_handle: lookup_service_and_handle.map(|(_, handle)| handle),
            n_shards,
        })
    }
}

/// Configuration for a test cluster storage node.
#[derive(Debug)]
pub struct StorageNodeTestConfig {
    key_pair: ProtocolKeyPair,
    network_key_pair: NetworkKeyPair,
    shards: Vec<ShardIndex>,
    rest_api_address: SocketAddr,
}

impl StorageNodeTestConfig {
    fn new(shards: Vec<ShardIndex>, use_distinct_ip: bool) -> Self {
        Self {
            key_pair: ProtocolKeyPair::generate(),
            network_key_pair: NetworkKeyPair::generate(),
            rest_api_address: unused_socket_address(use_distinct_ip),
            shards,
        }
    }

    /// Creates a `SuiStorageNode` from `self`.
    pub fn to_storage_node_info(&self, name: &str) -> SuiStorageNode {
        SuiStorageNode {
            node_id: ObjectID::random(),
            name: name.into(),
            network_address: NetworkAddress(self.rest_api_address.to_string()),
            public_key: self.key_pair.public().clone(),
            next_epoch_public_key: None,
            network_public_key: self.network_key_pair.public().clone(),
            shard_ids: self.shards.clone(),
            metadata: ObjectID::random(),
        }
    }

    /// Creates `NodeRegistrationParams` from `self`.
    pub fn to_node_registration_params(&self, name: &str) -> NodeRegistrationParams {
        NodeRegistrationParams {
            name: name.into(),
            network_address: NetworkAddress(self.rest_api_address.to_string()),
            public_key: self.key_pair.public().clone(),
            network_public_key: self.network_key_pair.public().clone(),
            commission_rate: 0,
            storage_price: 5,
            write_price: 1,
            node_capacity: 1_000_000_000,
            metadata: NodeMetadata::default(),
        }
    }
}

fn test_config_from_node_shard_assignment<I>(
    shards: &[I],
    use_distinct_ip: bool,
) -> StorageNodeTestConfig
where
    for<'a> &'a I: Into<ShardIndex>,
{
    let shards: Vec<ShardIndex> = shards.iter().map(|i| i.into()).collect();
    StorageNodeTestConfig::new(shards, use_distinct_ip)
}

/// Returns storage node test configs for the given `shard_assignment`.
pub fn storage_node_test_configs_from_shard_assignment<S, I>(
    shard_assignment: &[S],
    use_distinct_ip: bool,
) -> Vec<StorageNodeTestConfig>
where
    S: Borrow<[I]>,
    for<'a> &'a I: Into<ShardIndex>,
{
    let storage_node_configs: Vec<_> = shard_assignment
        .iter()
        .map(|node_shard_assignment| {
            test_config_from_node_shard_assignment(node_shard_assignment.borrow(), use_distinct_ip)
        })
        .collect();

    assert!(
        !storage_node_configs.is_empty(),
        "assignments for at least 1 node must be specified"
    );

    storage_node_configs
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        let shard_assignment = vec![
            vec![ShardIndex(0)],
            vec![ShardIndex(1), ShardIndex(2)],
            ShardIndex::range(3..6).collect(),
            ShardIndex::range(6..9).collect(),
            ShardIndex::range(9..13).collect(),
        ];
        Self {
            shard_sync_config: None,
            event_providers: shard_assignment.iter().map(|_| None).collect(),
            committee_services: shard_assignment.iter().map(|_| None).collect(),
            contract_services: shard_assignment.iter().map(|_| None).collect(),
            storage_capabilities: shard_assignment.iter().map(|_| None).collect(),
            node_wallet_dirs: shard_assignment.iter().map(|_| None).collect(),
            start_node_from_beginning: shard_assignment.iter().map(|_| true).collect(),
            blocklist_files: shard_assignment.iter().map(|_| None).collect(),
            disable_event_blob_writer: shard_assignment.iter().map(|_| false).collect(),
            storage_node_configs: shard_assignment
                .into_iter()
                .map(|shards| StorageNodeTestConfig::new(shards, false))
                .collect(),
            system_context: None,
            sui_rpc_urls: vec![],
            use_distinct_ip: false,
            num_checkpoints_per_blob: None,
            enable_node_config_synchronizer: false,
        }
    }
}

#[async_trait]
impl<T> SystemContractService for Arc<WithTempDir<T>>
where
    T: SystemContractService,
{
    async fn sync_node_params(
        &self,
        config: &StorageNodeConfig,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SyncNodeConfigError> {
        self.as_ref()
            .inner
            .sync_node_params(config, node_capability_object_id)
            .await
    }

    async fn end_voting(&self) -> Result<(), anyhow::Error> {
        self.as_ref().inner.end_voting().await
    }

    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate) {
        self.as_ref().inner.invalidate_blob_id(certificate).await
    }

    async fn epoch_sync_done(&self, epoch: Epoch, node_capability_object_id: ObjectID) {
        self.as_ref()
            .inner
            .epoch_sync_done(epoch, node_capability_object_id)
            .await
    }

    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        self.as_ref().inner.get_epoch_and_state().await
    }

    fn current_epoch(&self) -> Epoch {
        self.as_ref().inner.current_epoch()
    }

    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error> {
        self.as_ref().inner.fixed_system_parameters().await
    }

    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error> {
        self.as_ref().inner.initiate_epoch_change().await
    }

    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SuiClientError> {
        self.as_ref()
            .inner
            .certify_event_blob(
                blob_metadata,
                ending_checkpoint_seq_num,
                epoch,
                node_capability_object_id,
            )
            .await
    }

    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error> {
        self.as_ref().inner.refresh_contract_package().await
    }

    async fn get_node_capability_object(
        &self,
        node_capability_object_id: Option<ObjectID>,
    ) -> Result<StorageNodeCap, SuiClientError> {
        self.as_ref()
            .inner
            .get_node_capability_object(node_capability_object_id)
            .await
    }

    async fn get_system_object_version(&self) -> Result<u64, SuiClientError> {
        self.as_ref().inner.get_system_object_version().await
    }

    async fn last_certified_event_blob(&self) -> Result<Option<EventBlob>, SuiClientError> {
        self.as_ref().inner.last_certified_event_blob().await
    }
}

/// Returns a test-committee with members with the specified number of shards ehortach.
#[cfg(test)]
#[allow(unused)]
pub(crate) fn test_committee(weights: &[u16]) -> Committee {
    test_committee_with_epoch(weights, 1)
}

#[cfg(test)]
#[allow(unused)]
pub(crate) fn test_committee_with_epoch(weights: &[u16], epoch: Epoch) -> Committee {
    use sui_types::base_types::ObjectID;

    let n_shards: u16 = weights.iter().sum();
    let mut shards = 0..n_shards;

    let members = weights
        .iter()
        .map(|&node_shard_count| SuiStorageNode {
            node_id: ObjectID::random(),
            shard_ids: (&mut shards)
                .take(node_shard_count.into())
                .map(ShardIndex)
                .collect(),
            public_key: ProtocolKeyPair::generate().public().clone(),
            next_epoch_public_key: None,
            network_public_key: NetworkKeyPair::generate().public().clone(),
            name: String::new(),
            network_address: NetworkAddress("host:0".to_owned()),
            metadata: ObjectID::random(),
        })
        .collect();

    Committee::new(members, epoch, NonZeroU16::new(n_shards).unwrap()).unwrap()
}

/// A module for creating a Walrus test cluster.
#[cfg(all(feature = "client", feature = "node"))]
pub mod test_cluster {
    use std::sync::OnceLock;

    use futures::future;
    use tokio::sync::Mutex as TokioMutex;
    use walrus_sui::{
        client::{SuiContractClient, SuiReadClient},
        test_utils::{
            self,
            system_setup::{
                create_and_init_system_for_test,
                end_epoch_zero,
                register_committee_and_stake,
            },
            TestClusterHandle,
        },
        types::move_structs::Authorized,
    };

    use super::*;
    use crate::{
        client::{self, ClientCommunicationConfig, Config},
        node::{
            committee::DefaultNodeServiceFactory,
            contract_service::SuiSystemContractService,
            events::{
                event_processor::{EventProcessorRuntimeConfig, SystemConfig},
                EventProcessorConfig,
            },
        },
    };

    /// The weight of each storage node in the test cluster.
    pub const FROST_PER_NODE_WEIGHT: u64 = 1_000_000_000_000;

    /// Performs the default setup for the test cluster using StorageNodeHandle as default storage
    /// node handle.
    pub async fn default_setup() -> anyhow::Result<(
        Arc<TokioMutex<TestClusterHandle>>,
        TestCluster,
        WithTempDir<client::Client<SuiContractClient>>,
    )> {
        default_setup_with_epoch_duration(Duration::from_secs(60 * 60), false).await
    }

    /// Performs the default setup for the test cluster using StorageNodeHandle as default storage
    /// node handle.
    pub async fn default_setup_with_subsidies() -> anyhow::Result<(
        Arc<TokioMutex<TestClusterHandle>>,
        TestCluster,
        WithTempDir<client::Client<SuiContractClient>>,
    )> {
        default_setup_with_epoch_duration(Duration::from_secs(60 * 60), true).await
    }

    /// Performs the default setup with the input epoch duration for the test cluster using
    /// StorageNodeHandle as default storage node handle.
    pub async fn default_setup_with_epoch_duration(
        epoch_duration: Duration,
        with_subsidies: bool,
    ) -> anyhow::Result<(
        Arc<TokioMutex<TestClusterHandle>>,
        TestCluster,
        WithTempDir<client::Client<SuiContractClient>>,
    )> {
        let test_nodes_config = TestNodesConfig {
            node_weights: vec![1, 2, 3, 3, 4],
            use_legacy_event_processor: true,
            disable_event_blob_writer: false,
            blocklist_dir: None,
            enable_node_config_synchronizer: false,
        };
        default_setup_with_num_checkpoints_generic::<StorageNodeHandle>(
            epoch_duration,
            test_nodes_config,
            Some(10),
            ClientCommunicationConfig::default_for_test(),
            with_subsidies,
            None,
        )
        .await
    }

    /// Performs the default setup with the input epoch duration for the test cluster with the
    /// specified storage node handle.
    pub async fn default_setup_with_num_checkpoints_generic<T: StorageNodeHandleTrait>(
        epoch_duration: Duration,
        test_nodes_config: TestNodesConfig,
        num_checkpoints_per_blob: Option<u32>,
        communication_config: ClientCommunicationConfig,
        with_subsidies: bool,
        num_additional_fullnodes: Option<usize>,
    ) -> anyhow::Result<(
        Arc<TokioMutex<TestClusterHandle>>,
        TestCluster<T>,
        WithTempDir<client::Client<SuiContractClient>>,
    )> {
        let (handle, cluster, client, _) = default_setup_with_deploy_directory_generic(
            epoch_duration,
            test_nodes_config,
            num_checkpoints_per_blob,
            communication_config,
            with_subsidies,
            None,
            false,
            num_additional_fullnodes,
        )
        .await?;
        Ok((handle, cluster, client))
    }

    // TODO(WAL-653): Refactor with builder pattern to make selecting different options cleaner.
    /// Performs the default setup with the input epoch duration for the test cluster with the
    /// specified storage node handle.
    #[allow(unused)]
    #[allow(clippy::too_many_arguments)]
    pub async fn default_setup_with_deploy_directory_generic<T: StorageNodeHandleTrait>(
        epoch_duration: Duration,
        test_nodes_config: TestNodesConfig,
        num_checkpoints_per_blob: Option<u32>,
        communication_config: ClientCommunicationConfig,
        with_subsidies: bool,
        deploy_directory: Option<PathBuf>,
        delegate_governance_to_admin_wallet: bool,
        num_additional_fullnodes: Option<usize>,
    ) -> anyhow::Result<(
        Arc<TokioMutex<TestClusterHandle>>,
        TestCluster<T>,
        WithTempDir<client::Client<SuiContractClient>>,
        SystemContext,
    )> {
        #[cfg(not(msim))]
        let sui_cluster = test_utils::using_tokio::global_sui_test_cluster();
        #[cfg(msim)]
        let sui_cluster =
            test_utils::using_msim::global_sui_test_cluster_with_additional_fullnodes(
                num_additional_fullnodes,
            )
            .await;

        let sui_rpc_urls: Vec<String> = {
            let cluster = sui_cluster.lock().await;
            vec![cluster.rpc_url().to_string()]
                .into_iter()
                .chain(cluster.additional_rpc_urls().into_iter())
                .collect()
        };

        // Get a wallet on the global sui test cluster
        let mut admin_wallet =
            test_utils::new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;

        // Specify an empty assignment to ensure that storage nodes are not created with invalid
        // shard assignments.
        let n_shards = NonZeroU16::new(test_nodes_config.node_weights.iter().sum())
            .expect("sum of non-zero weights is not zero");
        let cluster_builder = TestCluster::<T>::builder()
            .with_shard_assignment(&vec![[]; test_nodes_config.node_weights.len()]);

        // Get the default committee from the test cluster builder
        let (members, protocol_keypairs): (Vec<_>, Vec<_>) = cluster_builder
            .storage_node_test_configs()
            .iter()
            .enumerate()
            .map(|(i, info)| {
                (
                    info.to_node_registration_params(&format!("node-{i}")),
                    info.key_pair.to_owned(),
                )
            })
            .unzip();

        let system_ctx = create_and_init_system_for_test(
            &mut admin_wallet.inner,
            n_shards,
            Duration::from_secs(0),
            epoch_duration,
            None,
            with_subsidies,
            deploy_directory,
        )
        .await
        .context("failed to create and init system for test")?;

        let n_nodes = members.len();
        let mut contract_clients = Vec::with_capacity(n_nodes);
        let mut node_wallet_dirs = Vec::with_capacity(n_nodes);
        let mut blocklist_files = Vec::with_capacity(n_nodes);
        let mut disable_event_blob_writers = Vec::with_capacity(n_nodes);

        for (i, wallet) in
            test_utils::create_and_fund_wallets_on_cluster(sui_cluster.clone(), n_nodes)
                .await?
                .into_iter()
                .enumerate()
        {
            let client = wallet
                .and_then_async(|wallet| {
                    system_ctx.new_contract_client(wallet, Default::default(), None)
                })
                .await?;
            let temp_dir = client.temp_dir.path().to_owned();
            node_wallet_dirs.push(temp_dir.clone());
            contract_clients.push(client.inner);
            let blocklist_dir = test_nodes_config.blocklist_dir.clone().unwrap_or(temp_dir);
            blocklist_files.push(blocklist_dir.join(format!("blocklist-{i}.yaml")));
            disable_event_blob_writers.push(test_nodes_config.disable_event_blob_writer);
            // In simtest, storage nodes load the Sui wallet config from the `temp_dir`. We
            // need to keep the directory alive throughout the test.
            #[cfg(msim)]
            Box::leak(Box::new(client.temp_dir));
        }
        let contract_clients_refs = contract_clients.iter().collect::<Vec<_>>();

        let contract_config = system_ctx.contract_config();

        let admin_contract_client = admin_wallet
            .and_then_async(|wallet| {
                SuiContractClient::new(
                    wallet,
                    &contract_config,
                    ExponentialBackoffConfig::default(),
                    None,
                )
            })
            .await?;

        if let Some(subsidies_pkg_id) = system_ctx.subsidies_pkg_id {
            let (subsidies_object_id, _) = admin_contract_client
                .as_ref()
                .create_and_fund_subsidies(
                    subsidies_pkg_id,
                    DEFAULT_BUYER_SUBSIDY_RATE,
                    DEFAULT_SYSTEM_SUBSIDY_RATE,
                    DEFAULT_SUBSIDY_FUNDS,
                )
                .await?;

            admin_contract_client
                .inner
                .read_client()
                .set_subsidies_object(subsidies_object_id)
                .await?;
        }

        let amounts_to_stake = test_nodes_config
            .node_weights
            .iter()
            .map(|&weight| FROST_PER_NODE_WEIGHT * weight as u64)
            .collect::<Vec<_>>();
        let storage_capabilities = register_committee_and_stake(
            admin_contract_client.as_ref(),
            &members,
            &protocol_keypairs,
            &contract_clients_refs,
            &amounts_to_stake,
        )
        .await?;

        if delegate_governance_to_admin_wallet {
            let authorized = Authorized::Address(admin_contract_client.as_ref().address());
            for (cap, client) in storage_capabilities
                .iter()
                .zip(contract_clients_refs.iter())
            {
                client
                    .set_governance_authorized(cap.node_id, authorized.clone())
                    .await?;
            }
        }

        end_epoch_zero(contract_clients_refs.first().unwrap()).await?;

        // Build the walrus cluster
        let sui_read_client = admin_contract_client.as_ref().read_client().clone();

        let committee_services = future::join_all(contract_clients.iter().map(|_| async {
            let service: Arc<dyn CommitteeService> = Arc::new(
                NodeCommitteeService::builder()
                    .build_with_factory(
                        sui_read_client.clone(),
                        DefaultNodeServiceFactory::avoid_system_services(),
                    )
                    .await
                    .expect("service construction must succeed in tests"),
            );
            service
        }))
        .await;

        // Create a contract service for the storage nodes using a wallet in a temp dir.
        let node_contract_services = contract_clients
            .into_iter()
            .zip(committee_services.iter())
            .map(|(client, committee_service)| {
                SuiSystemContractService::builder().build(client, committee_service.clone())
            });

        let cluster_builder = cluster_builder
            .with_committee_services(&committee_services)
            .with_system_contract_services(node_contract_services);

        let event_processor_config = Default::default();
        let cluster_builder = if test_nodes_config.use_legacy_event_processor {
            setup_legacy_event_processors(sui_read_client.clone(), cluster_builder).await?
        } else {
            setup_checkpoint_based_event_processors(
                &event_processor_config,
                sui_rpc_urls.as_slice(),
                sui_read_client.clone(),
                cluster_builder,
                system_ctx.system_object,
                system_ctx.staking_object,
            )
            .await?
        };

        let cluster_builder = if let Some(num_checkpoints_per_blob) = num_checkpoints_per_blob {
            cluster_builder.with_num_checkpoints_per_blob(num_checkpoints_per_blob)
        } else {
            cluster_builder
        };

        let cluster_builder = cluster_builder
            .with_system_context(system_ctx.clone())
            .with_sui_rpc_urls(sui_rpc_urls)
            .with_storage_capabilities(storage_capabilities)
            .with_node_wallet_dirs(node_wallet_dirs)
            .with_start_node_from_beginning(
                amounts_to_stake
                    .iter()
                    .map(|&initial_staking_amount| initial_staking_amount > 0)
                    .collect(),
            )
            .with_disable_event_blob_writer(disable_event_blob_writers)
            .with_blocklist_files(blocklist_files)
            .with_enable_node_config_synchronizer(
                test_nodes_config.enable_node_config_synchronizer,
            );
        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            cluster_builder.build().await?
        };

        // Create the client with the admin wallet to ensure that we have some WAL.
        let config = Config {
            contract_config,
            exchange_objects: vec![],
            wallet_config: None,
            communication_config,
            refresh_config: Default::default(),
        };

        let client = admin_contract_client
            .and_then_async(|contract_client| {
                client::Client::new_contract_client_with_refresher(config, contract_client)
            })
            .await?;

        Ok((sui_cluster, cluster, client, system_ctx))
    }

    async fn setup_checkpoint_based_event_processors(
        event_processor_config: &EventProcessorConfig,
        rpc_urls: &[String],
        sui_read_client: SuiReadClient,
        test_cluster_builder: TestClusterBuilder,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
    ) -> anyhow::Result<TestClusterBuilder> {
        let mut event_processors = vec![];
        for _ in test_cluster_builder.storage_node_test_configs().iter() {
            let processor_config = EventProcessorRuntimeConfig {
                rpc_addresses: rpc_urls.to_vec(),
                event_polling_interval: Duration::from_millis(100),
                db_path: nondeterministic!(tempfile::tempdir()
                    .expect("temporary directory creation must succeed")
                    .path()
                    .to_path_buf()),
                rpc_fallback_config: None,
            };
            let system_config = SystemConfig {
                system_pkg_id: sui_read_client.get_system_package_id(),
                system_object_id,
                staking_object_id,
            };
            let event_processor = EventProcessor::new(
                event_processor_config,
                processor_config,
                system_config,
                &Registry::default(),
            )
            .await?;
            event_processors.push(event_processor.clone());
        }
        let res = test_cluster_builder.with_individual_system_event_providers(&event_processors);
        Ok(res)
    }

    async fn setup_legacy_event_processors(
        sui_read_client: SuiReadClient,
        test_cluster_builder: TestClusterBuilder,
    ) -> anyhow::Result<TestClusterBuilder> {
        let event_provider = crate::node::system_events::SuiSystemEventProvider::new(
            sui_read_client.clone(),
            Duration::from_millis(100),
        );
        let res = test_cluster_builder.with_system_event_providers(event_provider);
        Ok(res)
    }

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    fn global_test_lock() -> &'static TokioMutex<()> {
        static LOCK: OnceLock<TokioMutex<()>> = OnceLock::new();
        LOCK.get_or_init(TokioMutex::default)
    }
}

/// Creates a new [`StorageNodeConfig`] object for testing.
pub fn storage_node_config() -> WithTempDir<StorageNodeConfig> {
    let temp_dir = nondeterministic!(TempDir::new().expect("able to create a temporary directory"));
    let rest_api_address = unused_socket_address(false);
    WithTempDir {
        inner: StorageNodeConfig {
            name: "node".to_string(),
            protocol_key_pair: walrus_core::test_utils::protocol_key_pair().into(),
            next_protocol_key_pair: None,
            network_key_pair: walrus_core::test_utils::network_key_pair().into(),
            rest_api_address,
            metrics_address: unused_socket_address(false),
            storage_path: temp_dir.path().to_path_buf(),
            db_config: Default::default(),
            rest_server: Default::default(),
            blocklist_path: None,
            sui: None,
            blob_recovery: Default::default(),
            tls: Default::default(),
            rest_graceful_shutdown_period_secs: Some(Some(0)),
            shard_sync_config: config::ShardSyncConfig {
                shard_sync_retry_min_backoff: Duration::from_secs(1),
                shard_sync_retry_max_backoff: Duration::from_secs(3),
                ..Default::default()
            },
            event_processor_config: Default::default(),
            use_legacy_event_provider: false,
            disable_event_blob_writer: false,
            commission_rate: 0,
            voting_params: VotingParams {
                storage_price: 5,
                write_price: 1,
                node_capacity: 1_000_000_000,
            },
            public_host: rest_api_address.ip().to_string(),
            public_port: rest_api_address.port(),
            metrics_push: None,
            metadata: Default::default(),
            config_synchronizer: Default::default(),
            storage_node_cap: None,
            num_uncertified_blob_threshold: Some(3),
            balance_check: Default::default(),
            thread_pool: Default::default(),
        },
        temp_dir,
    }
}

async fn wait_for_event_processor_to_start(
    event_processor: EventProcessor,
    client: RetriableRpcClient,
) -> anyhow::Result<()> {
    // Wait until event processor is actually running and downloaded a few checkpoints
    tokio::time::sleep(Duration::from_secs(5)).await;
    let checkpoint = client.get_latest_checkpoint_summary().await?;
    while let Some(event_processor_checkpoint) = event_processor.stores.checkpoint_store.get(&())? {
        if event_processor_checkpoint.inner().sequence_number >= checkpoint.sequence_number {
            break;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Ok(())
}

/// Returns an empty storage, with the column families for the specified shards already created.
pub async fn empty_storage_with_shards(shards: &[ShardIndex]) -> WithTempDir<Storage> {
    let temp_dir =
        nondeterministic!(tempfile::tempdir().expect("temporary directory creation must succeed"));
    let db_config = DatabaseConfig::default();
    let storage = Storage::open(
        temp_dir.path(),
        db_config,
        MetricConf::default(),
        Registry::default(),
    )
    .expect("storage creation must succeed");

    storage
        .create_storage_for_shards(shards)
        .await
        .expect("should be able to create storage for shards");

    WithTempDir {
        inner: storage,
        temp_dir,
    }
}

fn committee_from_members(members: Vec<SuiStorageNode>, initial_epoch: Option<Epoch>) -> Committee {
    let n_shards = NonZeroU16::new(members.iter().map(|node| node.shard_ids.len() as u16).sum())
        .expect("committee cannot have zero shards");
    Committee::new(members, initial_epoch.unwrap_or(1), n_shards)
        .expect("valid members to be provided for tests")
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::unused_socket_address;

    #[test]
    #[ignore = "ignore to not bind sockets unnecessarily"]
    fn test_unused_socket_addr() {
        let n = 1000;
        assert_eq!(
            (0..n)
                .map(|_| unused_socket_address(false))
                .collect::<HashSet<_>>()
                .len(),
            n
        )
    }
}
