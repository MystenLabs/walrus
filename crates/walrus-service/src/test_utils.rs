// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Test utilities for using storage nodes in tests.
//!
//! For creating an instance of a single storage node in a test, see [`StorageNodeHandleBuilder`] .
//!
//! For creating a cluster of test storage nodes, see [`TestClusterBuilder`].
use std::{borrow::Borrow, net::SocketAddr, sync::Arc};

use fastcrypto::{bls12381::min_pk::BLS12381PublicKey, traits::KeyPair};
use futures::StreamExt;
use mysten_metrics::RegistryService;
use prometheus::Registry;
use sui_types::event::EventID;
use tempfile::TempDir;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{encoding::EncodingConfig, test_utils, ProtocolKeyPair, ShardIndex};
use walrus_sui::types::BlobEvent;
use walrus_test_utils::WithTempDir;

use crate::{
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
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let encoding_config = EncodingConfig::new(2, 5, 10);
/// let handle = StorageNodeHandleBuilder::default()
///     .with_rest_api_started(true)
///     .with_node_started(true)
///     .build(encoding_config)
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
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let encoding_config = EncodingConfig::new(2, 5, 10);
/// let handle = StorageNodeHandleBuilder::default()
///     .with_storage(test_utils::empty_storage_with_shards(&[ShardIndex(0), ShardIndex(4)]))
///     .build(encoding_config)
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct StorageNodeHandleBuilder {
    storage: Option<WithTempDir<Storage>>,
    event_provider: Box<dyn SystemEventProvider>,
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
    pub async fn build(self, encoding_config: EncodingConfig) -> anyhow::Result<StorageNodeHandle> {
        let registry_service = RegistryService::new(Registry::default());

        let WithTempDir {
            inner: storage,
            temp_dir,
        } = self
            .storage
            .unwrap_or_else(|| empty_storage_with_shards(&[]));

        let config = StorageNodeConfig {
            storage_path: temp_dir.as_ref().to_path_buf(),
            protocol_key_pair: ProtocolKeyPair::generate().into(),
            metrics_address: unused_socket_address(),
            rest_api_address: unused_socket_address(),
            sui: None,
        };

        let node = StorageNode::builder()
            .with_storage(storage)
            .with_system_event_provider(self.event_provider)
            .build(&config, registry_service, encoding_config)
            .await?;
        let node = Arc::new(node);

        let public_key = config
            .protocol_key_pair
            .get()
            .unwrap()
            .as_ref()
            .public()
            .clone();

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
            storage: Default::default(),
            run_rest_api: Default::default(),
            run_node: Default::default(),
        }
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
    pub async fn build(self, encoding_config: EncodingConfig) -> anyhow::Result<TestCluster> {
        let mut nodes = vec![];

        for (shards, event_provider) in self
            .shard_assignment
            .into_iter()
            .zip(self.event_providers.into_iter())
        {
            let mut builder = StorageNodeHandle::builder()
                .with_storage(empty_storage_with_shards(&shards))
                .with_rest_api_started(true)
                .with_node_started(true);

            if let Some(provider) = event_provider {
                builder = builder.with_boxed_system_event_provider(provider);
            }

            nodes.push(builder.build(encoding_config.clone()).await?);
        }

        Ok(TestCluster { nodes })
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
