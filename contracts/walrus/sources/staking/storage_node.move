// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Copies `blob_store::storage_node` with only addition of `StorageNodeCap` struct.
#[allow(unused_field, unused_function, unused_variable, unused_use)]
module walrus::storage_node;

use std::string::String;
use sui::{bls12381::{G1, g1_from_bytes}, group_ops::Element};

/// Represents a storage node in the system.
public struct StorageNodeInfo has store, drop {
    name: String,
    // node_id: ID,
    network_address: String,
    public_key: Element<G1>,
    shard_ids: vector<u16>,
}

/// A Capability which represents a storage node and authorizes the holder to
/// perform operations on the storage node.
public struct StorageNodeCap has key, store {
    id: UID,
    // node_id: ID,
    pool_id: ID,
}

/// A public constructor for the StorageNodeInfo.
public(package) fun new(
    name: String,
    // node_id: ID,
    network_address: String,
    public_key: vector<u8>,
    shard_ids: vector<u16>,
): StorageNodeInfo {
    StorageNodeInfo {
        name,
        network_address,
        public_key: g1_from_bytes(&public_key),
        shard_ids,
    }
}

/// Return the public key of the storage node.
public(package) fun public_key(self: &StorageNodeInfo): &Element<G1> { &self.public_key }

/// Return the shard IDs of the storage node.
public(package) fun shard_ids(self: &StorageNodeInfo): &vector<u16> { &self.shard_ids }

/// Return the weight of the storage node.
public(package) fun weight(self: &StorageNodeInfo): u16 { self.shard_ids.length() as u16 }

/// Return the pool ID of the storage node.
public fun pool_id(cap: &StorageNodeCap): ID { cap.pool_id }

/// Return the node ID of the storage node.
// public fun node_id(cap: &StorageNodeCap): ID { cap.node_id }

#[test_only]
/// Create a storage node with dummy name & address
public fun new_for_testing(public_key: vector<u8>, weight: u16): StorageNodeInfo {
    assert!(weight <= 0xFFFF);
    let mut shard_ids = vector[];
    weight.do!(|i| shard_ids.push_back(i));
    StorageNodeInfo {
        name: b"node".to_string(),
        network_address: b"127.0.0.1".to_string(),
        public_key: g1_from_bytes(&public_key),
        shard_ids,
    }
}

#[test_only]
/// Create a storage node capability for testing.
public fun new_cap_for_testing(pool_id: ID, ctx: &mut TxContext): StorageNodeCap {
    StorageNodeCap { id: object::new(ctx), pool_id }
}
