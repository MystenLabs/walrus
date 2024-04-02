// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use fastcrypto::traits::KeyPair as FCKeyPair;
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{test_utils, KeyPair, PublicKey, ShardIndex};
use walrus_sui::types::{Committee, NetworkAddress, StorageNode as SuiStorageNode};
use walrus_test_utils::WithTempDir;

use crate::{
    client::Config,
    config::StorageNodePrivateParameters,
    server::UserServer,
    storage::Storage,
    StorageNode,
};

/// Creates a new [`StorageNodePrivateParameters`] object for testing.
pub fn storage_node_private_parameters() -> StorageNodePrivateParameters {
    let network_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let network_address = network_listener.local_addr().unwrap();

    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_address = metrics_listener.local_addr().unwrap();

    StorageNodePrivateParameters {
        keypair: test_utils::keypair(),
        network_address,
        metrics_address,
        shards: HashMap::new(),
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

/// Creates a new [`StorageNode`].
pub fn new_test_storage_node(
    shards: &[ShardIndex],
    n_shards: usize,
    key_pair: KeyPair,
) -> StorageNode {
    let storage = empty_storage_with_shards(shards);
    StorageNode::new_with_storage(storage.inner, n_shards, Arc::new(key_pair))
}

/// Creates a new [`UserServer`].
pub fn new_test_server(
    shards: &[ShardIndex],
    n_shards: usize,
    key_pair: KeyPair,
) -> UserServer<StorageNode> {
    let storage_node = new_test_storage_node(shards, n_shards, key_pair);
    UserServer::new(Arc::new(storage_node), CancellationToken::new())
}

/// Creates a new [`UserServer`] with parameters.
pub fn new_test_server_with_address(
    shards: &[ShardIndex],
    n_shards: usize,
) -> (UserServer<StorageNode>, SocketAddr, PublicKey) {
    let StorageNodePrivateParameters {
        keypair,
        network_address,
        metrics_address: _,
        shards: _,
    } = storage_node_private_parameters();
    let pk = keypair.public().clone();
    (
        new_test_server(shards, n_shards, keypair),
        network_address,
        pk,
    )
}

/// Creates and runs a new [`UserServer`] with parameters.
pub async fn spawn_test_server(shards: &[ShardIndex], n_shards: usize) -> (SocketAddr, PublicKey) {
    let (server, addr, pk) = new_test_server_with_address(shards, n_shards);
    let _handle = tokio::spawn(async move { server.run(&addr).await });
    tokio::task::yield_now().await;
    (addr, pk)
}

/// Creates and runs a new committee of [`UserServer`s][UserServer].
pub async fn spawn_test_committee(
    n_symbols_primary: u16,
    n_symbols_secondary: u16,
    n_shards: usize,
    nodes_shards: &[&[u16]],
) -> Config {
    let mut addrs_pks = vec![];
    // Create the walrus storage nodes.
    for shards in nodes_shards.iter() {
        addrs_pks.push(spawn_test_server(&to_shards(shards), n_shards).await);
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
        source_symbols_primary: n_symbols_primary,
        source_symbols_secondary: n_symbols_secondary,
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
