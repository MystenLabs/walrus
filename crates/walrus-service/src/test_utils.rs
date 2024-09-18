// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test utilities for using storage nodes in tests.
//!
//! For creating an instance of a single storage node in a test, see [`StorageNodeHandleBuilder`] .
//!
//! For creating a cluster of test storage nodes, see [`TestClusterBuilder`].

use std::{
    borrow::Borrow,
    net::{SocketAddr, TcpStream},
    num::NonZeroU16,
    sync::Arc,
};

use async_trait::async_trait;
use futures::StreamExt;
use prometheus::Registry;
use sui_types::base_types::ObjectID;
use tempfile::TempDir;
use tokio::time::Duration;
use tokio_stream::{wrappers::BroadcastStream, Stream};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use typed_store::rocks::MetricConf;
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
use walrus_event::{EventSequenceNumber, EventStreamCursor, IndexedStreamElement};
use walrus_sdk::client::Client;
use walrus_sui::types::{
    Committee,
    ContractEvent,
    NetworkAddress,
    NodeRegistrationParams,
    StorageNode as SuiStorageNode,
};
use walrus_test_utils::WithTempDir;

use crate::node::{
    committee::{ActiveCommittees, CommitteeLookupService, CommitteeService, NodeCommitteeService},
    config::StorageNodeConfig,
    contract_service::SystemContractService,
    errors::SyncShardClientError,
    server::{UserServer, UserServerConfig},
    system_events::{EventManager, EventRetentionManager, SystemEventProvider},
    DatabaseConfig,
    Storage,
    StorageNode,
};

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
        Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>,
        anyhow::Error,
    > {
        self.event_provider.events(cursor).await
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
    pub rest_api: Arc<UserServer<StorageNode>>,
    /// Cancellation token for the REST API.
    pub cancel: CancellationToken,
    /// Client that can be used to communicate with the node.
    pub client: Client,
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

/// Builds a new [`StorageNodeHandle`] with custom configuration values.
///
/// Can be created with the methods [`StorageNodeHandle::builder()`] or with
/// [`StorageNodeHandleBuilder::new()`].
///
/// Methods can be chained in order to set the configuration values, with the `StorageNode` being
/// constructed by calling [`build`][Self::build`].
///
/// See function level documentation for details on the various configuration settings.
///
/// # Examples
///
/// The following would create a storage node, and start its REST API and event loop:
///
/// ```
/// use walrus_core::encoding::EncodingConfig;
/// use walrus_service::test_utils::StorageNodeHandleBuilder;
/// use std::num::NonZeroU16;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let handle = StorageNodeHandleBuilder::default()
///     .with_rest_api_started(true)
///     .with_node_started(true)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Whereas the following will create a storage node with no blobs stored and responsible
/// for shards 0 and 4.
///
/// ```
/// use walrus_core::{encoding::EncodingConfig, ShardIndex};
/// use walrus_service::test_utils::{self, StorageNodeHandleBuilder};
/// use std::num::NonZeroU16;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let handle = StorageNodeHandleBuilder::default()
///     .with_storage(test_utils::empty_storage_with_shards(&[ShardIndex(0), ShardIndex(4)]))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct StorageNodeHandleBuilder {
    storage: Option<WithTempDir<Storage>>,
    event_provider: Box<dyn SystemEventProvider>,
    committee_service: Option<Box<dyn CommitteeService>>,
    contract_service: Option<Box<dyn SystemContractService>>,
    run_rest_api: bool,
    run_node: bool,
    test_config: Option<StorageNodeTestConfig>,
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
    pub fn with_committee_service(mut self, service: Box<dyn CommitteeService>) -> Self {
        self.committee_service = Some(service);
        self
    }

    /// Sets the [`SystemContractService`] to be used with the node.
    ///
    /// If not provided, defaults to a [`StubContractService`].
    pub fn with_system_contract_service(
        mut self,
        contract_service: Box<dyn SystemContractService>,
    ) -> Self {
        self.contract_service = Some(contract_service);
        self
    }

    /// Enable or disable the node's event loop being started on build.
    pub fn with_node_started(mut self, run_node: bool) -> Self {
        self.run_node = run_node;
        self
    }

    /// Enable or disable the REST API being started on build.
    pub fn with_rest_api_started(mut self, run_rest_api: bool) -> Self {
        self.run_rest_api = run_rest_api;
        self
    }

    /// Specify the shard assignment for this node.
    ///
    /// If specified, it will determine the the shard assignment for the node in the committee. If
    /// not, the shard assignment will be inferred from the shards present in the storage.
    ///
    /// Resets any prior calls to [`Self::with_test_config`].
    pub fn with_shard_assignment(mut self, shards: &[ShardIndex]) -> Self {
        self.test_config = Some(StorageNodeTestConfig::new(shards.into()));
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

    /// Creates the configured [`StorageNodeHandle`].
    pub async fn build(self) -> anyhow::Result<StorageNodeHandle> {
        // Identify the storage being used, as it allows us to extract the shards
        // that should be assigned to this storage node.
        let WithTempDir {
            inner: storage,
            temp_dir,
        } = self
            .storage
            .unwrap_or_else(|| empty_storage_with_shards(&[]));

        let node_info = self
            .test_config
            .unwrap_or_else(|| StorageNodeTestConfig::new(storage.shards_present()));
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
                None,
            );
            Box::new(StubCommitteeService {
                encoding: EncodingConfig::new(committee.n_shards()).into(),
                committee: committee.into(),
            })
        });

        let contract_service = self
            .contract_service
            .unwrap_or_else(|| Box::new(StubContractService {}));

        // Create the node's config using the previously generated keypair and address.
        let config = StorageNodeConfig {
            storage_path: temp_dir.path().to_path_buf(),
            protocol_key_pair: node_info.key_pair.into(),
            network_key_pair: node_info.network_key_pair.into(),
            rest_api_address: node_info.rest_api_address,
            ..storage_node_config().inner
        };

        let metrics_registry = Registry::default();
        let node = StorageNode::builder()
            .with_storage(storage)
            .with_system_event_manager(Box::new(DefaultSystemEventManager::new(
                self.event_provider,
            )))
            .with_committee_service(committee_service)
            .with_system_contract_service(contract_service)
            .build(&config, metrics_registry.clone())
            .await?;
        let node = Arc::new(node);

        let cancel_token = CancellationToken::new();
        let rest_api = Arc::new(UserServer::new(
            node.clone(),
            cancel_token.clone(),
            UserServerConfig::from(&config),
            &metrics_registry,
        ));

        if self.run_rest_api {
            let rest_api_clone = rest_api.clone();

            tokio::task::spawn(async move { rest_api_clone.run().await }.instrument(
                tracing::info_span!("cluster-node", address = %config.rest_api_address),
            ));
        }

        if self.run_node {
            let node = node.clone();
            let cancel_token = cancel_token.clone();

            tokio::task::spawn(async move { node.run(cancel_token).await }.instrument(
                tracing::info_span!("cluster-node", address = %config.rest_api_address),
            ));
        }

        let client = Client::builder()
            .authenticate_with_public_key(network_public_key.clone())
            // Disable proxy and root certs from the OS for tests.
            .no_proxy()
            .tls_built_in_root_certs(false)
            .build_for_remote_ip(config.rest_api_address)?;

        if self.run_rest_api {
            wait_for_node_ready(&client).await?;
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
        })
    }
}

impl Default for StorageNodeHandleBuilder {
    fn default() -> Self {
        Self {
            event_provider: Box::<Vec<ContractEvent>>::default(),
            committee_service: None,
            storage: Default::default(),
            run_rest_api: Default::default(),
            run_node: Default::default(),
            contract_service: None,
            test_config: None,
        }
    }
}

/// Waits until the node is ready by querying the node's health info endpoint using the node
/// client.
async fn wait_for_node_ready(client: &Client) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Err(err) = client.get_server_health_info().await {
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
/// It is at least 3 and is defined as `n = max(max(shard_ids) + 1, 3)`. If the shards `0..n` are
/// assigned to the existing node, then this function returns `None`. Otherwise, there must be a
/// second node in the committee with the shards not managed by the provided node.
fn committee_partner(node_config: &StorageNodeTestConfig) -> Option<StorageNodeTestConfig> {
    const MIN_SHARDS: u16 = 3;
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
        Some(StorageNodeTestConfig::new(other_shards))
    } else {
        None
    }
}

/// A stub `CommitteeLookupService`.
///
/// Does not perform any network operations.
#[derive(Debug)]
pub struct StubLookupService {
    committee: Committee,
}

#[async_trait]
impl CommitteeLookupService for StubLookupService {
    async fn get_active_committees(&self) -> Result<ActiveCommittees, anyhow::Error> {
        let prior_committee = if self.committee.epoch != 0 {
            let committee = Committee::new(
                self.committee.members().to_vec(),
                self.committee.epoch - 1,
                self.committee.n_shards(),
            )?;
            Some(committee)
        } else {
            None
        };

        Ok(ActiveCommittees::new(
            self.committee.clone(),
            prior_committee,
        ))
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
        _metadata: &VerifiedBlobMetadataWithId,
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

    fn committee(&self) -> Arc<Committee> {
        self.committee.clone()
    }

    fn is_walrus_storage_node(&self, public_key: &PublicKey) -> bool {
        self.committee
            .members()
            .iter()
            .any(|node| node.public_key == *public_key)
    }
}

/// A stub [`SystemContractService`].
///
/// Performs a no-op when calling [`invalidate_blob_id()`][Self::invalidate_blob_id]
#[derive(Debug)]
pub struct StubContractService {}

#[async_trait]
impl SystemContractService for StubContractService {
    async fn invalidate_blob_id(&self, _certificate: &InvalidBlobCertificate) {}
}

/// Returns a socket address that is not currently in use on the system.
pub fn unused_socket_address() -> SocketAddr {
    try_unused_socket_address().expect("unused socket address to be available")
}

fn try_unused_socket_address() -> anyhow::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;

    // Create and accept a connection to force the port into the TIME_WAIT state
    let _client_stream = TcpStream::connect(address)?;
    let _server_stream = listener.accept()?;
    Ok(address)
}

#[async_trait::async_trait]
impl SystemEventProvider for Vec<ContractEvent> {
    async fn events(
        &self,
        _cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>, anyhow::Error>
    {
        Ok(Box::new(
            tokio_stream::iter(
                self.clone()
                    .into_iter()
                    .map(|c| IndexedStreamElement::new(c, EventSequenceNumber::new(0, 0))),
            )
            .chain(tokio_stream::pending()),
        ))
    }
}

#[async_trait::async_trait]
impl SystemEventProvider for tokio::sync::broadcast::Sender<ContractEvent> {
    async fn events(
        &self,
        _cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>, anyhow::Error>
    {
        Ok(Box::new(BroadcastStream::new(self.subscribe()).map(
            |value| {
                IndexedStreamElement::new(
                    value.expect("should not return errors in test"),
                    EventSequenceNumber::new(0, 0),
                )
            },
        )))
    }
}

/// A cluster of [`StorageNodeHandle`]s corresponding to several running storage nodes.
#[derive(Debug)]
pub struct TestCluster {
    /// The running storage nodes.
    pub nodes: Vec<StorageNodeHandle>,
}

impl TestCluster {
    /// Returns a new builder to create the [`TestCluster`].
    pub fn builder() -> TestClusterBuilder {
        TestClusterBuilder::default()
    }

    /// Returns an encoding config valid for use with the storage nodes.
    pub fn encoding_config(&self) -> EncodingConfig {
        let n_shards = self
            .nodes
            .iter()
            .map(|node| node.storage_node.shards().len())
            .sum::<usize>();
        let n_shards: u16 = n_shards.try_into().expect("valid number of shards");

        EncodingConfig::new(NonZeroU16::new(n_shards).expect("more than 1 shard"))
    }

    /// Stops the storage node with index `idx` by cancelling its task.
    pub fn cancel_node(&mut self, idx: usize) {
        assert!(
            idx < self.nodes.len(),
            "the index of the node to be dropped must be within the node vector"
        );
        self.nodes[idx].cancel.cancel();
    }

    /// Returns the client for the node at the specified index.
    pub fn client(&self, index: usize) -> &Client {
        &self.nodes[index].client
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
    // INV: Reset if shard_assignment is changed.
    event_providers: Vec<Option<Box<dyn SystemEventProvider>>>,
    committee_services: Vec<Option<Box<dyn CommitteeService>>>,
    contract_services: Vec<Option<Box<dyn SystemContractService>>>,
    initial_epoch: Option<Epoch>,
}

impl TestClusterBuilder {
    /// Returns a new default builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a reference to the storage node test configs of the builder.
    pub fn storage_node_test_configs(&self) -> &Vec<StorageNodeTestConfig> {
        &self.storage_node_configs
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
        let configs = storage_node_test_configs_from_shard_assignment(assignment);

        self.event_providers = configs.iter().map(|_| None).collect();
        self.committee_services = configs.iter().map(|_| None).collect();
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
    pub async fn with_committee_services<F, Fut, T>(mut self, make_service: F) -> Self
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = T>,
        T: CommitteeService + 'static,
    {
        self.committee_services.clear();
        for _ in self.storage_node_configs.iter() {
            self.committee_services
                .push(Some(Box::new(make_service().await) as _));
        }
        self
    }

    /// Sets the [`SystemContractService`] used for each storage node.
    ///
    /// Should be called after the storage nodes have been specified.
    pub fn with_system_contract_services<T>(mut self, contract_service: T) -> Self
    where
        T: SystemContractService + Clone + 'static,
    {
        self.contract_services = self
            .storage_node_configs
            .iter()
            .map(|_| Some(Box::new(contract_service.clone()) as _))
            .collect();
        self
    }

    /// Sets the initial epoch for the cluster.
    pub fn with_initial_epoch(mut self, epoch: Epoch) -> Self {
        self.initial_epoch = Some(epoch);
        self
    }

    /// Creates the configured `TestCluster`.
    pub async fn build(self) -> anyhow::Result<TestCluster> {
        let mut nodes = vec![];

        let committee_members: Vec<_> = self
            .storage_node_configs
            .iter()
            .enumerate()
            .map(|(i, info)| info.to_storage_node_info(&format!("node-{i}")))
            .collect();
        let committee = committee_from_members(committee_members.clone(), self.initial_epoch);

        for (((config, event_provider), service), contract_service) in self
            .storage_node_configs
            .into_iter()
            .zip(self.event_providers.into_iter())
            .zip(self.committee_services.into_iter())
            .zip(self.contract_services.into_iter())
        {
            let local_identity = config.key_pair.public().clone();
            let mut builder = StorageNodeHandle::builder()
                .with_storage(empty_storage_with_shards(&config.shards))
                .with_test_config(config)
                .with_rest_api_started(true)
                .with_node_started(true);

            if let Some(provider) = event_provider {
                builder = builder.with_boxed_system_event_provider(provider);
            }

            builder = if let Some(service) = service {
                builder.with_committee_service(service)
            } else {
                let service = NodeCommitteeService::new(
                    StubLookupService {
                        committee: committee.clone(),
                    },
                    local_identity,
                    Default::default(),
                )
                .await?;
                builder.with_committee_service(Box::new(service))
            };

            if let Some(service) = contract_service {
                builder = builder.with_system_contract_service(service);
            }

            nodes.push(builder.build().await?);
        }

        Ok(TestCluster { nodes })
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
    fn new(shards: Vec<ShardIndex>) -> Self {
        Self {
            key_pair: ProtocolKeyPair::generate(),
            network_key_pair: NetworkKeyPair::generate(),
            rest_api_address: unused_socket_address(),
            shards,
        }
    }

    /// Creates a `SuiStorageNode` from `self`.
    pub fn to_storage_node_info(&self, name: &str) -> SuiStorageNode {
        SuiStorageNode {
            node_id: ObjectID::random(),
            name: name.into(),
            network_address: NetworkAddress {
                host: self.rest_api_address.ip().to_string(),
                port: self.rest_api_address.port(),
            },
            public_key: self.key_pair.public().clone(),
            network_public_key: self.network_key_pair.public().clone(),
            shard_ids: self.shards.clone(),
        }
    }

    /// Creates `NodeRegistrationParams` from `self`.
    pub fn to_node_registration_params(&self, name: &str) -> NodeRegistrationParams {
        NodeRegistrationParams {
            name: name.into(),
            network_address: NetworkAddress {
                host: self.rest_api_address.ip().to_string(),
                port: self.rest_api_address.port(),
            },
            public_key: self.key_pair.public().clone(),
            network_public_key: self.network_key_pair.public().clone(),
            commission_rate: 0,
            storage_price: 5,
            write_price: 1,
            node_capacity: 1_000_000_000,
        }
    }
}

fn test_config_from_node_shard_assignment<I>(shards: &[I]) -> StorageNodeTestConfig
where
    for<'a> &'a I: Into<ShardIndex>,
{
    let shards: Vec<ShardIndex> = shards.iter().map(|i| i.into()).collect();
    assert!(
        !shards.is_empty(),
        "shard assignments to nodes must be non-empty"
    );
    StorageNodeTestConfig::new(shards)
}

/// Returns storage node test configs for the given `shard_assignment`.
pub fn storage_node_test_configs_from_shard_assignment<S, I>(
    shard_assignment: &[S],
) -> Vec<StorageNodeTestConfig>
where
    S: Borrow<[I]>,
    for<'a> &'a I: Into<ShardIndex>,
{
    let storage_node_configs: Vec<_> = shard_assignment
        .iter()
        .map(|node_shard_assignment| {
            test_config_from_node_shard_assignment(node_shard_assignment.borrow())
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
            event_providers: shard_assignment.iter().map(|_| None).collect(),
            committee_services: shard_assignment.iter().map(|_| None).collect(),
            contract_services: shard_assignment.iter().map(|_| None).collect(),
            storage_node_configs: shard_assignment
                .into_iter()
                .map(StorageNodeTestConfig::new)
                .collect(),
            initial_epoch: None,
        }
    }
}

#[async_trait]
impl<T> SystemContractService for Arc<WithTempDir<T>>
where
    T: SystemContractService,
{
    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate) {
        self.as_ref().inner.invalidate_blob_id(certificate).await
    }
}

/// Returns a test-committee with members with the specified number of shards each.
#[cfg(test)]
#[allow(unused)]
pub(crate) fn test_committee(weights: &[u16]) -> Committee {
    test_committee_with_epoch(weights, 0)
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
            network_public_key: NetworkKeyPair::generate().public().clone(),
            name: String::new(),
            network_address: NetworkAddress {
                host: String::new(),
                port: 0,
            },
        })
        .collect();

    Committee::new(members, epoch, NonZeroU16::new(n_shards).unwrap()).unwrap()
}

/// A module for creating a Walrus test cluster.
#[cfg(all(feature = "client", feature = "node"))]
pub mod test_cluster {
    use std::sync::OnceLock;

    use tokio::sync::Mutex;
    use walrus_sui::{
        client::{SuiContractClient, SuiReadClient},
        test_utils::{
            self,
            system_setup::{create_and_init_system, end_epoch_zero, register_committee_and_stake},
            TestClusterHandle,
            DEFAULT_GAS_BUDGET,
        },
    };

    use super::*;
    use crate::{
        client::{self, ClientCommunicationConfig, Config},
        node::{contract_service::SuiSystemContractService, system_events::SuiSystemEventProvider},
    };

    /// Performs the default setup for the test cluster.
    pub async fn default_setup() -> anyhow::Result<(
        Arc<TestClusterHandle>,
        TestCluster,
        WithTempDir<client::Client<SuiContractClient>>,
    )> {
        #[cfg(not(msim))]
        let sui_cluster = test_utils::using_tokio::global_sui_test_cluster();
        #[cfg(msim)]
        let sui_cluster = test_utils::using_msim::global_sui_test_cluster().await;

        // Get a wallet on the global sui test cluster
        let mut wallet = test_utils::new_wallet_on_sui_test_cluster(sui_cluster.clone()).await?;

        let cluster_builder = TestCluster::builder();

        // Get the default committee from the test cluster builder
        let members = cluster_builder
            .storage_node_test_configs()
            .iter()
            .enumerate()
            .map(|(i, info)| info.to_node_registration_params(&format!("node-{i}")))
            .collect::<Vec<_>>();

        let node_weights = cluster_builder
            .storage_node_test_configs()
            .iter()
            .map(|info| info.shards.len())
            .collect::<Vec<_>>();
        let n_shards = node_weights.iter().sum::<usize>() as u16;

        // TODO(#814): make epoch duration in test configurable. Currently hardcoded to 1 hour.
        let system_ctx = create_and_init_system(&mut wallet.inner, n_shards, 0, 3600000).await?;

        let mut contract_clients = vec![];
        for _ in members.iter() {
            let client = test_utils::new_wallet_on_sui_test_cluster(sui_cluster.clone())
                .await?
                .and_then_async(|wallet| {
                    SuiContractClient::new(
                        wallet,
                        system_ctx.system_obj_id,
                        system_ctx.staking_obj_id,
                        DEFAULT_GAS_BUDGET,
                    )
                })
                .await?;
            contract_clients.push(client);
        }
        let contract_clients_refs = contract_clients
            .iter()
            .map(|client| &client.inner)
            .collect::<Vec<_>>();

        let amounts_to_stake = node_weights
            .iter()
            .map(|&weight| 1_000_000 * weight as u64)
            .collect::<Vec<_>>();
        register_committee_and_stake(
            &mut wallet.inner,
            &system_ctx,
            &members,
            &contract_clients_refs,
            &amounts_to_stake,
        )
        .await?;

        end_epoch_zero(contract_clients_refs.first().unwrap()).await?;

        // Build the walrus cluster
        let sui_read_client = SuiReadClient::new(
            wallet.as_ref().get_client().await?,
            system_ctx.system_obj_id,
            system_ctx.staking_obj_id,
        )
        .await?;

        // Create a contract service for the storage nodes using a wallet in a temp dir
        // The sui test cluster handler can be dropped since we already have one
        // TODO(#786): change cluster builder to take a list of `SuiSystemContractService`s and
        // provide the contract clients used for staking to make sure that each node has the
        // corresponding `StorageNodeCap`.
        let sui_contract_service = test_utils::new_wallet_on_sui_test_cluster(sui_cluster.clone())
            .await?
            .and_then(|wallet| {
                SuiContractClient::new_with_read_client(
                    wallet,
                    DEFAULT_GAS_BUDGET,
                    sui_read_client.clone(),
                )
            })?
            .map(SuiSystemContractService::new);

        // Set up the cluster
        let cluster_builder = cluster_builder
            .with_committee_services(|| async {
                NodeCommitteeService::builder()
                    .build(sui_read_client.clone())
                    .await
                    .expect("service construction must succeed in tests")
            })
            .await
            .with_system_event_providers(SuiSystemEventProvider::new(
                sui_read_client.clone(),
                Duration::from_millis(100),
            ))
            .with_system_contract_services(Arc::new(sui_contract_service));

        let cluster = {
            // Lock to avoid race conditions.
            let _lock = global_test_lock().lock().await;
            cluster_builder.build().await?
        };

        // Create the client with the admin wallet to ensure that we have some WAL.
        let sui_contract_client = wallet.and_then(|wallet| {
            SuiContractClient::new_with_read_client(wallet, DEFAULT_GAS_BUDGET, sui_read_client)
        })?;
        let config = Config {
            system_object: system_ctx.system_obj_id,
            staking_object: system_ctx.staking_obj_id,
            wallet_config: None,
            communication_config: ClientCommunicationConfig::default_for_test(),
        };

        let client = sui_contract_client
            .and_then_async(|contract_client| client::Client::new(config, contract_client))
            .await?;
        Ok((sui_cluster, cluster, client))
    }

    // Prevent tests running simultaneously to avoid interferences or race conditions.
    fn global_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(Mutex::default)
    }
}

/// Creates a new [`StorageNodeConfig`] object for testing.
pub fn storage_node_config() -> WithTempDir<StorageNodeConfig> {
    let temp_dir = TempDir::new().expect("able to create a temporary directory");
    WithTempDir {
        inner: StorageNodeConfig {
            protocol_key_pair: walrus_core::test_utils::protocol_key_pair().into(),
            network_key_pair: walrus_core::test_utils::network_key_pair().into(),
            rest_api_address: unused_socket_address(),
            metrics_address: unused_socket_address(),
            storage_path: temp_dir.path().to_path_buf(),
            db_config: None,
            sui: None,
            blob_recovery: Default::default(),
            tls: Default::default(),
            rest_graceful_shutdown_period_secs: Some(Some(0)),
            shard_sync_config: Default::default(),
            event_processor_config: None,
        },
        temp_dir,
    }
}

/// Returns an empty storage, with the column families for the specified shards already created.
pub fn empty_storage_with_shards(shards: &[ShardIndex]) -> WithTempDir<Storage> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let db_config = DatabaseConfig::default();
    let mut storage = Storage::open(temp_dir.path(), db_config, MetricConf::default())
        .expect("storage creation must succeed");

    for shard in shards {
        storage
            .create_storage_for_shard(*shard)
            .expect("shard should be successfully created");
    }

    WithTempDir {
        inner: storage,
        temp_dir,
    }
}

fn committee_from_members(members: Vec<SuiStorageNode>, initial_epoch: Option<Epoch>) -> Committee {
    let n_shards =
        NonZeroU16::new(members.iter().map(|node| node.shard_ids.len() as u16).sum()).unwrap();
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
                .map(|_| unused_socket_address())
                .collect::<HashSet<_>>()
                .len(),
            n
        )
    }
}
