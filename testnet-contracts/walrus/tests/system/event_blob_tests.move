// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::event_blob_tests;

use sui::test_utils::destroy;
use walrus::blob::{Self};
use walrus::storage_node;
use walrus::system::{Self, System};
use walrus::system_state_inner;
use walrus::test_node::{test_nodes, TestStorageNode};

const RED_STUFF: u8 = 0;

const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;

#[test]
public fun test_event_blob_certify_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(
        ctx,
    );
    // Total of 10 nodes all with equal weights
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RED_STUFF, SIZE);
    let mut index = 0;
    while (index < 10) {
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RED_STUFF,
            100,
            0,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        if (index < 6) {
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
        } else {
            // 7th node signing the blob triggers blob certification
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_some());
        };
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}

#[test, expected_failure(abort_code = system_state_inner::ERepeatedAttestation)]
public fun test_event_blob_certify_repeated_attestation() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(
        ctx,
    );
    // Total of 10 nodes
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RED_STUFF, SIZE);
    let mut index = 0;
    while (index < 10) {
        // Every node is going to attest the blob twice but their
        // votes are only going to be counted once
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RED_STUFF,
            100,
            0,
            ctx,
        );
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RED_STUFF,
            100,
            0,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        if (index < 6) {
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
        } else {
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_some());
        };
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}

#[test, expected_failure(abort_code = system_state_inner::EIncorrectAttestation)]
public fun test_multiple_event_blobs_in_flight() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(
        ctx,
    );
    // Total of 10 nodes
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);
    let blob1 = blob::derive_blob_id(0xabc, RED_STUFF, SIZE);
    let blob2 = blob::derive_blob_id(0xdef, RED_STUFF, SIZE);

    let mut index = 0;
    while (index < 6) {
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob1,
            0xabc,
            SIZE,
            RED_STUFF,
            100,
            0,
            ctx,
        );
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob2,
            0xdef,
            SIZE,
            RED_STUFF,
            200,
            0,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}

#[test]
public fun test_event_blob_certify_change_epoch() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(
        ctx,
    );
    // Total of 10 nodes
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RED_STUFF, SIZE);
    let mut index = 0;
    while (index < 6) {
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RED_STUFF,
            100,
            0,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
        index = index + 1
    };
    // increment epoch
    let mut new_committee = *system.committee();
    new_committee.increment_epoch_for_testing();
    let balance = system.advance_epoch(
        new_committee,
        walrus::epoch_parameters::epoch_params_for_testing(),
    );
    balance.destroy_for_testing();
    // 7th node attesting is not going to certify the blob as all other nodes
    // attested
    // the blob in previous epoch
    system.certify_event_blob(
        nodes.borrow_mut(index).cap_mut(),
        blob_id,
        ROOT_HASH,
        SIZE,
        RED_STUFF,
        100,
        1,
        ctx,
    );
    let state = system.inner().get_event_blob_certification_state();
    assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
    index = 0;
    // All nodes sign the blob in current epoch
    while (index < 10) {
        // 7th node already attested
        if (index == 6) {
            index = index + 1;
            continue
        };
        system.certify_event_blob(
            nodes.borrow_mut(index).cap_mut(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RED_STUFF,
            100,
            1,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        if (index < 5) {
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_none());
        } else {
            assert!(state.get_latest_certified_checkpoint_sequence_number().is_some());
        };
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}

// === Helper functions ===

fun set_storage_node_caps(
    system: &System,
    ctx: &mut TxContext,
    nodes: &mut vector<TestStorageNode>,
) {
    let (node_ids, _values) = system.committee().to_vec_map().into_keys_values();
    let mut index = 0;
    node_ids.do!(|node_id| {
        let storage_cap = storage_node::new_cap(node_id, ctx);
        nodes.borrow_mut(index).set_storage_node_cap(storage_cap);
        index = index + 1;
    });
}
