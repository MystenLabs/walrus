// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Test utilities for using storage nodes in tests.
//!
//! For creating an instance of a single storage node in a test, see [`StorageNodeHandleBuilder`] .
//!
//! For creating a cluster of test storage nodes, see [`TestClusterBuilder`].
use std::{borrow::Borrow, net::SocketAddr, num::NonZeroU16, sync::Arc};

use anyhow::anyhow;
use async_trait::async_trait;
use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::KeyPair};
use futures::StreamExt;
use mysten_metrics::RegistryService;
use prometheus::Registry;
use sui_types::event::EventID;
use tempfile::TempDir;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{test_utils, Epoch, ProtocolKeyPair, PublicKey, ShardIndex};
use walrus_sui::types::{BlobEvent, Committee, NetworkAddress, StorageNode as SuiStorageNode};
use walrus_test_utils::WithTempDir;

use crate::{
    committee::{CommitteeService, CommitteeServiceFactory},
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

        // Get the shards assigned to this storage node, since we now manage the committee, if the
        // node will be present in the committee, it must have at least one shard assigned to it.
        let shard_assignment = storage.shards_present();
        let is_in_committee = shard_assignment.is_empty();

        // Generate key and address parameters for the node.
        let node_info = StorageNodeTestConfig::new(shard_assignment);
        let public_key = node_info.key_pair.as_ref().public().clone();

        // Create a list of the committee members, that contains one or two nodes.
        let committee_members = [
            if is_in_committee {
                Some(node_info.to_storage_node_info("node-under-test"))
            } else {
                None
            },
            committee_partner(&node_info).map(|info| info.to_storage_node_info("other-node")),
        ];
        debug_assert!(committee_members[0].is_some() || committee_members[1].is_some());

        let committee_service_factory = self.committee_service_factory.unwrap_or_else(|| {
            Box::new(StubCommitteeServiceFactory::from_members(
                // Remove the possible None in the members list
                committee_members.into_iter().flatten().collect(),
            ))
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

/// A [`CommitteeServiceFactory`] implementation that constructs [`StubCommitteeService`] instances.
///
/// This wraps a [`Committee`] and answers queries based on the contained data.
#[derive(Debug, Clone)]
pub struct StubCommitteeServiceFactory(Committee);

impl StubCommitteeServiceFactory {
    fn from_members(members: Vec<SuiStorageNode>) -> Self {
        Self(Committee::new(members, 0).expect("valid members to be provided for tests"))
    }
}

#[async_trait]
impl CommitteeServiceFactory for StubCommitteeServiceFactory {
    async fn new_for_epoch(
        &self,
        epoch: Option<Epoch>,
    ) -> Result<Box<dyn CommitteeService>, anyhow::Error> {
        if let Some(0) | None = epoch {
            Ok(Box::new(StubCommitteeService(self.0.clone())))
        } else {
            Err(anyhow!("stub factory only returns for epoch 0"))
        }
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

    fn exclude_member(&mut self, identity: &PublicKey) -> bool {
        // Nothing to exclude, but return true if the member is present in the committee.
        self.0
            .members()
            .iter()
            .any(|info| info.public_key == *identity)
    }
}

/// Returns a socket address that is not currently in use on the system.
pub fn unused_socket_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
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
    shard_assignment: Vec<Vec<ShardIndex>>,
    // INV: Reset if shard_assignment is changed.
    event_providers: Vec<Option<Box<dyn SystemEventProvider>>>,
}

impl TestClusterBuilder {
    /// Returns a new default builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of storage nodes and their shard assignments from a sequence of the shards
    /// assignmened to each storage.
    ///
    /// Resets any prior calls to [`Self::with_system_event_providers`].
    pub fn with_shard_assignment<S, I>(mut self, assignment: &[S]) -> Self
    where
        S: Borrow<[I]>,
        for<'a> &'a I: Into<ShardIndex>,
    {
        let mut shard_assignment = vec![];

        for node_shard_assignment in assignment {
            let shards: Vec<ShardIndex> = node_shard_assignment
                .borrow()
                .iter()
                .map(|i| i.into())
                .collect();

            assert!(
                !shards.is_empty(),
                "shard assignments to nodes must be non-empty"
            );
            shard_assignment.push(shards);
        }
        assert!(
            !shard_assignment.is_empty(),
            "assignments for at least 1 node must be specified"
        );

        self.event_providers = shard_assignment.iter().map(|_| None).collect();
        self.shard_assignment = shard_assignment;
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
            .shard_assignment
            .iter()
            .map(|_| Some(Box::new(event_provider.clone()) as _))
            .collect();
        self
    }

    /// Creates the configured `TestCluster`.
    pub async fn build(self) -> anyhow::Result<TestCluster> {
        let mut nodes = vec![];

        let node_test_configs: Vec<_> = self
            .shard_assignment
            .into_iter()
            .map(StorageNodeTestConfig::new)
            .collect();

        let committee_members = node_test_configs
            .iter()
            .enumerate()
            .map(|(i, info)| info.to_storage_node_info(&format!("node-{i}")))
            .collect();
        let factory = StubCommitteeServiceFactory::from_members(committee_members);

        for (config, event_provider) in node_test_configs
            .into_iter()
            .zip(self.event_providers.into_iter())
        {
            let mut builder = StorageNodeHandle::builder()
                .with_storage(empty_storage_with_shards(&config.shards))
                .with_committee_service_factory(Box::new(factory.clone()))
                .with_rest_api_started(true)
                .with_node_started(true);

            if let Some(provider) = event_provider {
                builder = builder.with_boxed_system_event_provider(provider);
            }

            nodes.push(builder.build().await?);
        }

        Ok(TestCluster { nodes })
    }
}

struct StorageNodeTestConfig {
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

    fn to_storage_node_info(&self, name: &str) -> SuiStorageNode {
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
            shard_assignment,
        }
    }
}
