// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::test_node;

use std::string::String;
use sui::address;
use walrus::{
    messages,
    node_metadata::{Self, NodeMetadata},
    storage_node::StorageNodeCap,
    test_utils
};

public struct TestStorageNode {
    sui_address: address,
    bls_sk: vector<u8>,
    storage_node_cap: Option<StorageNodeCap>,
}

public fun name(self: &TestStorageNode): String {
    self.sui_address.to_string()
}

public fun network_address(_self: &TestStorageNode): String {
    b"127.0.0.1".to_string()
}

public fun network_key(_self: &TestStorageNode): vector<u8> {
    x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab"
}

public fun metadata(_self: &TestStorageNode): NodeMetadata {
    node_metadata::default()
}

public fun bls_pk(self: &TestStorageNode): vector<u8> {
    test_utils::bls_min_pk_from_sk(&self.bls_sk)
}

public fun create_proof_of_possession(self: &TestStorageNode, epoch: u32): vector<u8> {
    test_utils::bls_min_pk_sign(
        &messages::new_proof_of_possession_msg(epoch, self.sui_address, self.bls_pk()).to_bcs(),
        &self.bls_sk,
    )
}

/// Returns a reference to the storage node cap. Aborts if not set.
public fun cap(self: &TestStorageNode): &StorageNodeCap {
    self.storage_node_cap.borrow()
}

/// Returns a mutable reference to the storage node cap. Aborts if not set.
public fun cap_mut(self: &mut TestStorageNode): &mut StorageNodeCap {
    self.storage_node_cap.borrow_mut()
}

/// Returns the node ID. Aborts if the storage node cap is not set.
public fun node_id(self: &TestStorageNode): ID {
    self.storage_node_cap.borrow().node_id()
}

public fun sui_address(self: &TestStorageNode): address {
    self.sui_address
}

/// Sets the storage node cap, aborts if cap is already set.
public fun set_storage_node_cap(self: &mut TestStorageNode, cap: StorageNodeCap) {
    self.storage_node_cap.fill(cap);
}

public fun destroy(self: TestStorageNode) {
    let TestStorageNode {
        storage_node_cap,
        ..,
    } = self;
    storage_node_cap.destroy!(|cap| cap.destroy_cap_for_testing());
}

/// Returns a vector of 10 test storage nodes, with the secret keys from
/// `test_utils::bls_secret_keys_for_testing`.
public fun test_nodes(): vector<TestStorageNode> {
    let mut sui_address: u256 = 0x0;
    test_utils::bls_secret_keys_for_testing().map!(|bls_sk| {
        sui_address = sui_address + 1;
        TestStorageNode {
            sui_address: address::from_u256(sui_address),
            bls_sk,
            storage_node_cap: option::none(),
        }
    })
}
