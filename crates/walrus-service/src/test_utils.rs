// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc, time::Duration};

use fastcrypto::traits::KeyPair;
use sui_sdk::types::{digests::TransactionDigest, event::EventID};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{
    encoding::{EncodingConfig, Primary, Secondary},
    test_utils,
    ProtocolKeyPair,
    PublicKey,
    ShardIndex,
};
use walrus_sui::{
    client::ReadClient,
    types::{Committee, NetworkAddress, StorageNode as SuiStorageNode},
};
use walrus_test_utils::WithTempDir;

use crate::{
    client::Config,
    config::{PathOrInPlace, StorageNodeConfig},
    server::UserServer,
    storage::Storage,
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

/// Creates a new [`StorageNode`] with a [`ReadClient`].
pub fn new_test_node_with_read_client<T>(
    shards: &[ShardIndex],
    encoding_config: EncodingConfig,

    key_pair: ProtocolKeyPair,
    sui_read_client: T,
) -> WithTempDir<StorageNode<T>>
where
    T: ReadClient,
{
    empty_storage_with_shards(shards).map(|storage| {
        StorageNode::new_with_storage(
            storage,
            encoding_config,
            key_pair,
            sui_read_client,
            Duration::from_micros(1),
        )
    })
}

/// Creates a new [`StorageNode`] with parameters.
pub fn new_test_node_with_address<T>(
    shards: &[ShardIndex],
    encoding_config: EncodingConfig,
    sui_read_client: T,
) -> (WithTempDir<StorageNode<T>>, SocketAddr, PublicKey)
where
    T: ReadClient + Send + Sync + 'static,
{
    let config = storage_node_config();
    let rest_api_address = config.inner.rest_api_address;
    let protocol_key_pair = config.inner.protocol_key_pair.get().unwrap();
    (
        new_test_node_with_read_client(
            shards,
            encoding_config,
            protocol_key_pair.clone(),
            sui_read_client,
        ),
        rest_api_address,
        protocol_key_pair.as_ref().public().clone(),
    )
}

/// Creates and runs a new [`UserServer`] with parameters.
pub async fn spawn_test_server<T>(
    shards: &[ShardIndex],
    encoding_config: EncodingConfig,
    sui_read_client: T,
) -> (SocketAddr, PublicKey)
where
    T: ReadClient + Send + Sync + 'static,
{
    let (storage_node, addr, pk) =
        new_test_node_with_address(shards, encoding_config, sui_read_client);
    let storage_node = storage_node.map(Arc::new);
    let walrus_node_clone = storage_node.inner.clone();
    let _walrus_node_handle =
        tokio::spawn(async move { walrus_node_clone.run(CancellationToken::new()).await });
    let _handle = tokio::spawn(async move {
        UserServer::new(storage_node.inner, CancellationToken::new())
            .run(&addr)
            .await
    });
    tokio::task::yield_now().await;
    (addr, pk)
}

/// Creates and runs a new committee of [`UserServer`s][UserServer].
///
/// The total count of shards in `nodes_shards` must be equal to the number of shards in the
/// `encoding_config`.
pub async fn spawn_test_committee<T>(
    encoding_config: EncodingConfig,
    nodes_shards: &[&[u16]],
    sui_read_client: T,
) -> Config
where
    T: ReadClient + Clone + Send + Sync + 'static,
{
    let n_shards = encoding_config.n_shards() as usize;
    assert_eq!(
        nodes_shards.iter().map(|s| s.len()).sum::<usize>(),
        n_shards
    );
    let mut addrs_pks = vec![];
    // Create the walrus storage nodes.
    for shards in nodes_shards.iter() {
        addrs_pks.push(
            spawn_test_server(
                &to_shards(shards),
                encoding_config.clone(),
                sui_read_client.clone(),
            )
            .await,
        );
    }
    // Create the config.
    let members = nodes_shards
        .iter()
        .zip(addrs_pks.into_iter())
        .map(|(shards, (addr, pk))| to_storage_node_config(addr.into(), pk, &to_shards(shards)))
        .collect::<Vec<_>>();

    Config {
        committee: Committee {
            members,
            epoch: 0,
            total_weight: n_shards,
        },
        source_symbols_primary: encoding_config.n_source_symbols::<Primary>(),
        source_symbols_secondary: encoding_config.n_source_symbols::<Secondary>(),
        concurrent_requests: n_shards,
        connection_timeout: Duration::from_secs(10),
    }
}

fn to_shards(ids: &[u16]) -> Vec<ShardIndex> {
    ids.iter().map(|&i| ShardIndex(i)).collect()
}

fn to_storage_node_config(
    network_address: NetworkAddress,
    public_key: PublicKey,
    shard_ids: &[ShardIndex],
) -> SuiStorageNode {
    SuiStorageNode {
        name: network_address.to_string(),
        network_address,
        public_key,
        shard_ids: shard_ids.to_vec(),
    }
}

pub(crate) fn event_id_for_testing() -> EventID {
    EventID {
        tx_digest: TransactionDigest::random(),
        event_seq: 0,
    }
}
