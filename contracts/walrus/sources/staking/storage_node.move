// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_field, unused_function, unused_variable, unused_use)]
module walrus::storage_node;

use std::string::String;
use sui::bls12381::{G1, g1_from_bytes};
use sui::group_ops::Element;
use walrus::event_blob::EventBlobAttestation;

// Error codes
const EInvalidNetworkPublicKey: u64 = 1;

/// Represents a storage node in the system.
public struct StorageNodeInfo has store, copy, drop {
    name: String,
    node_id: ID,
    network_address: String,
    public_key: Element<G1>,
    network_public_key: vector<u8>,
}

/// A Capability which represents a storage node and authorizes the holder to
/// perform operations on the storage node.
public struct StorageNodeCap has key, store {
    id: UID,
    node_id: ID,
    last_epoch_sync_done: u32,
    last_event_blob_attestation: Option<EventBlobAttestation>,
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
    }
}

/// Create a new storage node capability.
public(package) fun new_cap(node_id: ID, ctx: &mut TxContext): StorageNodeCap {
    StorageNodeCap {
        id: object::new(ctx),
        node_id,
        last_epoch_sync_done: 0,
        last_event_blob_attestation: option::none(),
    }
}

/// Return the public key of the storage node.
public(package) fun public_key(self: &StorageNodeInfo): &Element<G1> {
    &self.public_key
}

/// Return the node ID of the storage node.
public fun id(cap: &StorageNodeInfo): ID { cap.node_id }

/// Return the pool ID of the storage node.
public fun node_id(cap: &StorageNodeCap): ID { cap.node_id }

/// Return the last epoch in which the storage node attested that it has
/// finished syncing.
public fun last_epoch_sync_done(cap: &StorageNodeCap): u32 {
    cap.last_epoch_sync_done
}

/// Set the last epoch in which the storage node attested that it has finished
/// syncing.
public(package) fun set_last_epoch_sync_done(self: &mut StorageNodeCap, epoch: u32) {
    self.last_epoch_sync_done = epoch;
}

/// Return the latest event blob attestion.
public fun last_event_blob_attestation(cap: &mut StorageNodeCap): Option<EventBlobAttestation> {
    cap.last_event_blob_attestation
}

/// Set the last epoch in which the storage node attested that it has finished
/// syncing.
public(package) fun set_last_event_blob_attestation(
    self: &mut StorageNodeCap,
    attestation: EventBlobAttestation,
) {
    self.last_event_blob_attestation = option::some(attestation);
}

#[test_only]
/// Create a storage node with dummy name & address
public fun new_for_testing(public_key: vector<u8>): StorageNodeInfo {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    StorageNodeInfo {
        node_id,
        name: b"node".to_string(),
        network_address: b"127.0.0.1".to_string(),
        public_key: g1_from_bytes(&public_key),
        network_public_key: x"820e2b273530a00de66c9727c40f48be985da684286983f398ef7695b8a44677ab",
    }
}

#[test_only]
public fun destroy_cap_for_testing(cap: StorageNodeCap) {
    let StorageNodeCap { id, .. } = cap;
    id.delete();
}
