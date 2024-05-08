// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Test utilities for using storage nodes in tests.
//!
//! For creating an instance of a single storage node in a test, see [`StorageNodeHandleBuilder`] .
//!
//! For creating a cluster of test storage nodes, see [`TestClusterBuilder`].
use std::{
    borrow::Borrow,
    marker::PhantomData,
    net::{SocketAddr, TcpStream},
    num::NonZeroU16,
    sync::Arc,
};

use async_trait::async_trait;
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::KeyPair};
use futures::StreamExt;
use mysten_metrics::RegistryService;
use prometheus::Registry;
use sui_types::event::EventID;
use tempfile::TempDir;
use tokio_stream::{wrappers::BroadcastStream, Stream};
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{
    encoding::EncodingConfig,
    metadata::VerifiedBlobMetadataWithId,
    test_utils,
    BlobId,
    Epoch,
    ProtocolKeyPair,
    PublicKey,
    ShardIndex,
};
use walrus_sui::types::{
    BlobEvent,
    Committee,
    InvalidCommittee,
    NetworkAddress,
    StorageNode as SuiStorageNode,
};
use walrus_test_utils::WithTempDir;

use crate::{
    committee::{CommitteeService, CommitteeServiceFactory, NodeCommitteeService},
    config::{PathOrInPlace, StorageNodeConfig},
    server::UserServer,
    storage::Storage,
    system_events::SystemEventProvider,
    StorageNode,
};

/// Creates a new [`StorageNodeConfig`] object for testing.
pub fn storage_node_config() -> WithTempDir<StorageNodeConfig> {
    let rest_api_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rest_api_address = rest_api_listener.local_addr().unwrap();

    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_address = metrics_listener.local_addr().unwrap();

    let temp_dir = TempDir::new().expect("able to create a temporary directory");
    WithTempDir {
        inner: StorageNodeConfig {
            protocol_key_pair: PathOrInPlace::InPlace(test_utils::keypair()),
            rest_api_address,
            metrics_address,
            storage_path: temp_dir.path().to_path_buf(),
            sui: None,
        },
        temp_dir,
    }
}

/// Returns an empty storage, with the column families for the specified shards already created.
pub fn empty_storage_with_shards(shards: &[ShardIndex]) -> WithTempDir<Storage> {
    let temp_dir = tempfile::tempdir().expect("temporary directory creation must succeed");
    let mut storage = Storage::open(temp_dir.path(), MetricConf::default())
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

/// A storage node and associated data for testing.
#[derive(Debug)]
pub struct StorageNodeHandle {
    /// The wrapped storage node.
    pub storage_node: Arc<StorageNode>,
    /// The temporary directory containing the node's storage.
    pub storage_directory: TempDir,
    /// The node's protocol public key.
    pub public_key: BLS12381PublicKey,
    /// The address of the REST API.
    pub rest_api_address: SocketAddr,
    /// The address of the metric service.
    pub metrics_address: SocketAddr,
    /// Handle the REST API
    pub rest_api: Arc<UserServer<StorageNode>>,
    /// Cancellation token for the REST API
    pub cancel: CancellationToken,
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
    committee_service_factory: Option<Box<dyn CommitteeServiceFactory>>,
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
    /// If a committee service factory is *not* provided with
    /// [`Self::with_committee_service_factory`], then the storage also dictates the shard
    /// assignment to this storage node in the created committee.
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

    /// Sets the [`CommitteeServiceFactory`] used with the node.
    ///
    /// If not provided, defaults to [`StubCommitteeServiceFactory`] created with a valid committee
    /// constructed over at most 1 other node. Note that if the node has no shards assigned to it
    /// (as inferred from the storage), it will not be in the committee.
    pub fn with_committee_service_factory(
        mut self,
        factory: Box<dyn CommitteeServiceFactory>,
    ) -> Self {
        self.committee_service_factory = Some(factory);
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
        let registry_service = RegistryService::new(Registry::default());

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

        let public_key = node_info.key_pair.as_ref().public().clone();

        let committee_service_factory = self.committee_service_factory.unwrap_or_else(|| {
            // Create a list of the committee members, that contains one or two nodes.
            let committee_members = [
                is_in_committee.then(|| node_info.to_storage_node_info("node-under-test")),
                committee_partner(&node_info).map(|info| info.to_storage_node_info("other-node")),
            ];
            debug_assert!(committee_members[0].is_some() || committee_members[1].is_some());

            Box::new(
                StubCommitteeServiceFactory::<StubCommitteeService>::from_members(
                    // Remove the possible None in the members list
                    committee_members.into_iter().flatten().collect(),
                ),
            )
        });

        // Create the node's config using the previously generated keypair and address.
        let config = StorageNodeConfig {
            storage_path: temp_dir.as_ref().to_path_buf(),
            protocol_key_pair: node_info.key_pair.into(),
            rest_api_address: node_info.rest_api_address,
            metrics_address: unused_socket_address(),
            sui: None,
        };

        let node = StorageNode::builder()
            .with_storage(storage)
            .with_system_event_provider(self.event_provider)
            .with_committee_service_factory(committee_service_factory)
            .build(&config, registry_service)
            .await?;
        let node = Arc::new(node);

        let cancel_token = CancellationToken::new();
        let rest_api = Arc::new(UserServer::new(node.clone(), cancel_token.clone()));

        if self.run_rest_api {
            let rest_api_address = config.rest_api_address;
            let rest_api_clone = rest_api.clone();

            tokio::task::spawn(async move { rest_api_clone.run(&rest_api_address).await });
        }

        if self.run_node {
            let node = node.clone();
            let cancel_token = cancel_token.clone();

            tokio::task::spawn(async move { node.run(cancel_token).await });
        }

        Ok(StorageNodeHandle {
            storage_node: node,
            storage_directory: temp_dir,
            public_key,
            rest_api_address: config.rest_api_address,
            metrics_address: config.metrics_address,
            rest_api,
            cancel: cancel_token,
        })
    }
}

impl Default for StorageNodeHandleBuilder {
    fn default() -> Self {
        Self {
            event_provider: Box::<Vec<BlobEvent>>::default(),
            committee_service_factory: None,
            storage: Default::default(),
            run_rest_api: Default::default(),
            run_node: Default::default(),
            test_config: None,
        }
    }
}

/// Returns with a a test config for a storage node that would make a valid committee when paired
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

/// A [`CommitteeServiceFactory`] implementation that constructs committee service
/// instances using the supplied committee.
#[derive(Debug, Clone)]
pub struct StubCommitteeServiceFactory<T> {
    committee: Committee,
    _service_type: PhantomData<T>,
}

impl<T> StubCommitteeServiceFactory<T> {
    fn from_members(members: Vec<SuiStorageNode>) -> Self {
        Self {
            committee: Committee::new(members, 0).expect("valid members to be provided for tests"),
            _service_type: PhantomData,
        }
    }
}

#[async_trait]
impl CommitteeServiceFactory for StubCommitteeServiceFactory<StubCommitteeService> {
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        Ok(Box::new(StubCommitteeService(self.committee.clone())))
    }
}

#[async_trait]
impl CommitteeServiceFactory for StubCommitteeServiceFactory<NodeCommitteeService> {
    async fn new_for_epoch(&self) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        Ok(Box::new(
            NodeCommitteeService::new(self.committee.clone()).await?,
        ))
    }
}

/// A stub [`CommitteeService`].
///
/// Does not perform any network operations.
#[derive(Debug)]
pub struct StubCommitteeService(pub Committee);

#[async_trait]
impl CommitteeService for StubCommitteeService {
    fn get_epoch(&self) -> Epoch {
        0
    }

    fn get_shard_count(&self) -> NonZeroU16 {
        self.0.n_shards()
    }

    fn exclude_member(&mut self, _identity: &PublicKey) {}

    async fn get_and_verify_metadata(
        &self,
        _blob_id: &BlobId,
        _encoding_config: &EncodingConfig,
    ) -> VerifiedBlobMetadataWithId {
        std::future::pending().await
    }

    fn committee(&self) -> &Committee {
        &self.0
    }
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
impl SystemEventProvider for Vec<BlobEvent> {
    async fn events(
        &self,
        _cursor: Option<EventID>,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error> {
        Ok(Box::new(
            tokio_stream::iter(self.clone()).chain(tokio_stream::pending()),
        ))
    }
}

#[async_trait::async_trait]
impl SystemEventProvider for tokio::sync::broadcast::Sender<BlobEvent> {
    async fn events(
        &self,
        _cursor: Option<EventID>,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error> {
        Ok(Box::new(BroadcastStream::new(self.subscribe()).map(
            |value| value.expect("should not return errors in test"),
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

    /// Returns the [`Committee`] configuration for the current cluster.
    pub fn committee(&self) -> Result<Committee, InvalidCommittee> {
        let members = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| SuiStorageNode {
                name: format!("node-{i}"),
                network_address: node.rest_api_address.into(),
                public_key: node.public_key.clone(),
                shard_ids: node.storage_node.shards(),
            })
            .collect();
        Committee::new(members, 0)
    }

    /// Stops the storage node with index `idx` by cancelling its task.
    pub fn cancel_node(&mut self, idx: usize) {
        assert!(
            idx < self.nodes.len(),
            "the index of the node to be dropped must be within the node vector"
        );
        self.nodes[idx].cancel.cancel();
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
    committee_factories: Vec<Option<Box<dyn CommitteeServiceFactory>>>,
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
    /// assignmened to each storage.
    ///
    /// Resets any prior calls to [`Self::with_test_configs`],
    /// [`Self::with_system_event_providers`], and [`Self::with_committee_service_factories`].
    pub fn with_shard_assignment<S, I>(mut self, assignment: &[S]) -> Self
    where
        S: Borrow<[I]>,
        for<'a> &'a I: Into<ShardIndex>,
    {
        let configs = storage_node_test_configs_from_shard_assignment(assignment);

        self.event_providers = configs.iter().map(|_| None).collect();
        self.committee_factories = configs.iter().map(|_| None).collect();
        self.storage_node_configs = configs;
        self
    }

    /// Sets the configurations for each storage node based on `configs`.
    ///
    /// Resets any prior calls to [`Self::with_shard_assignment`],
    /// [`Self::with_system_event_providers`], and [`Self::with_committee_service_factories`].
    pub fn with_test_configs(mut self, configs: Vec<StorageNodeTestConfig>) -> Self {
        self.event_providers = configs.iter().map(|_| None).collect();
        self.committee_factories = configs.iter().map(|_| None).collect();
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

    /// Sets the [`CommitteeServiceFactory`] used for each storage node.
    ///
    /// Should be called after the storage nodes have been specified.
    pub fn with_committee_service_factories<T>(mut self, factory: T) -> Self
    where
        T: CommitteeServiceFactory + Clone + 'static,
    {
        self.committee_factories = self
            .storage_node_configs
            .iter()
            .map(|_| Some(Box::new(factory.clone()) as _))
            .collect();
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

        for ((config, event_provider), factory) in self
            .storage_node_configs
            .into_iter()
            .zip(self.event_providers.into_iter())
            .zip(self.committee_factories.into_iter())
        {
            let mut builder = StorageNodeHandle::builder()
                .with_storage(empty_storage_with_shards(&config.shards))
                .with_test_config(config)
                .with_rest_api_started(true)
                .with_node_started(true);

            if let Some(provider) = event_provider {
                builder = builder.with_boxed_system_event_provider(provider);
            }

            if let Some(factory) = factory {
                builder = builder.with_committee_service_factory(factory);
            } else {
                builder = builder.with_committee_service_factory(Box::new(
                    StubCommitteeServiceFactory::<NodeCommitteeService>::from_members(
                        committee_members.clone(),
                    ),
                ));
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
    shards: Vec<ShardIndex>,
    rest_api_address: SocketAddr,
}

impl StorageNodeTestConfig {
    fn new(shards: Vec<ShardIndex>) -> Self {
        Self {
            key_pair: ProtocolKeyPair::generate(),
            rest_api_address: unused_socket_address(),
            shards,
        }
    }

    /// Creates a `SuiStorageNode` from `self`.
    pub fn to_storage_node_info(&self, name: &str) -> SuiStorageNode {
        SuiStorageNode {
            name: name.into(),
            network_address: NetworkAddress {
                host: self.rest_api_address.ip().to_string(),
                port: self.rest_api_address.port(),
            },
            public_key: self.key_pair.as_ref().public().clone(),
            shard_ids: self.shards.clone(),
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
            committee_factories: shard_assignment.iter().map(|_| None).collect(),
            storage_node_configs: shard_assignment
                .into_iter()
                .map(StorageNodeTestConfig::new)
                .collect(),
        }
    }
}

/// Returns a test-committee with members with the specified number of shards each.
#[cfg(test)]
pub(crate) fn test_committee(weights: &[u16]) -> Committee {
    let n_shards: u16 = weights.iter().sum();
    let mut shards = 0..n_shards;

    let members = weights
        .iter()
        .map(|&node_shard_count| SuiStorageNode {
            shard_ids: (&mut shards)
                .take(node_shard_count.into())
                .map(ShardIndex)
                .collect(),
            public_key: ProtocolKeyPair::generate().as_ref().public().clone(),
            name: String::new(),
            network_address: NetworkAddress {
                host: String::new(),
                port: 0,
            },
        })
        .collect();

    Committee::new(members, 0).unwrap()
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
