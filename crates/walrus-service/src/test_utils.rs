// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use walrus_core::test_utils::keypair;

use crate::config::StorageNodePrivateParameters;

/// Creates a new [`StorageNodePrivateParameters`] object for testing.
pub fn storage_node_private_parameters() -> StorageNodePrivateParameters {
    let network_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let network_address = network_listener.local_addr().unwrap();

    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_address = metrics_listener.local_addr().unwrap();

    StorageNodePrivateParameters {
        keypair: keypair(),
        network_address,
        metrics_address,
        shards: HashMap::new(),
    }
}
