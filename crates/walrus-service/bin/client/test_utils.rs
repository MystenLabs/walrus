// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use fastcrypto::traits::{KeyPair as FcKeyPair, ToFromBytes};
use tokio_util::sync::CancellationToken;
use typed_store::rocks::MetricConf;
use walrus_core::{test_utils, KeyPair, PublicKey, ShardIndex};
use walrus_service::{
    config::StorageNodePrivateParameters,
    server::UserServer,
    Storage,
    StorageNode,
};
use walrus_sui::types::{Committee, StorageNode as SuiStorageNode};
use walrus_test_utils::WithTempDir;

use super::cli::Config;

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
