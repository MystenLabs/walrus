// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Copies `blob_store::storage_node` with only addition of `StorageNodeCap` struct.
#[allow(unused_field, unused_function, unused_variable, unused_use)]
module walrus::storage_node;

use std::string::String;
use sui::{bls12381::{G1, g1_from_bytes}, group_ops::Element};

// Error codes
const EInvalidNetworkPublicKey: u64 = 1;

/// Represents a storage node in the system.
public struct StorageNodeInfo has store, copy, drop {
    name: String,
    node_id: ID,
    network_address: String,
    public_key: Element<G1>,
    network_public_key: vector<u8>,
    shard_ids: vector<u16>,
}

/// A Capability which represents a storage node and authorizes the holder to
/// perform operations on the storage node.
public struct StorageNodeCap has key, store {
    id: UID,
    node_id: ID,
}

/// A public constructor for the StorageNodeInfo.
public(package) fun new(
    name: String,
    node_id: ID,
    network_address: String,
    public_key: vector<u8>,
    network_public_key: vector<u8>,
): StorageNodeInfo {
    assert!(network_public_key.length() == 33, EInvalidNetworkPublicKey);
    StorageNodeInfo {
        node_id,
        name,
        network_address,
        public_key: g1_from_bytes(&public_key),
        network_public_key,
        shard_ids: vector[],
    }
}

/// Create a new storage node capability.
public(package) fun new_cap(node_id: ID, ctx: &mut TxContext): StorageNodeCap {
    StorageNodeCap {
        id: object::new(ctx),
        node_id,
    }
}

/// Assign the shards to the storage node.
public(package) fun assign_shards(self: &mut StorageNodeInfo, shard_ids: vector<u16>) {
    self.shard_ids = shard_ids;
}

/// Add the extra shards to the storage node.
public(package) fun add_shards(self: &mut StorageNodeInfo, shard_ids: vector<u16>) {
    self.shard_ids.append(shard_ids);
}

/// Return the public key of the storage node.
public(package) fun public_key(self: &StorageNodeInfo): &Element<G1> { &self.public_key }

/// Return the shard IDs of the storage node.
public(package) fun shard_ids(self: &StorageNodeInfo): &vector<u16> { &self.shard_ids }

/// Return the weight of the storage node.
public(package) fun weight(self: &StorageNodeInfo): u16 { self.shard_ids.length() as u16 }

/// Return the node ID of the storage node.
public fun id(cap: &StorageNodeInfo): ID { cap.node_id }

/// Return the pool ID of the storage node.
public fun node_id(cap: &StorageNodeCap): ID { cap.node_id }

#[test_only]
/// Create a storage node with dummy name & address
public fun new_for_testing(public_key: vector<u8>, weight: u16): StorageNodeInfo {
    assert!(weight <= 0xFFFF);
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut shard_ids = vector[];
    weight.do!(|i| shard_ids.push_back(i));
    StorageNodeInfo {
        node_id,
        name: b"node".to_string(),
        network_address: b"127.0.0.1".to_string(),
        public_key: g1_from_bytes(&public_key),
        network_public_key: x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab",
        shard_ids,
    }
}

#[test_only]
/// Create a storage node capability for testing.
public fun new_cap_for_testing(node_id: ID, ctx: &mut TxContext): StorageNodeCap {
    StorageNodeCap { id: object::new(ctx), node_id }
}

#[test_only]
public fun destroy_cap_for_testing(cap: StorageNodeCap) {
    let StorageNodeCap { id, .. } = cap;
    id.delete();
}
