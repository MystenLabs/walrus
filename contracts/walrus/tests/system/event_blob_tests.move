// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::event_blob_tests;

use std::debug;
use sui::test_utils::destroy;
use walrus::{
    blob,
    storage_node,
    system::{Self, System},
    system_state_inner,
    test_node::{test_nodes, TestStorageNode}
};

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


// Invalid certified blob id attack
// run it and see that when it fails the latest certified event blob is the bad one which was only voted by node 9
// the bug is that the same node can vote again and again for the same bad blob id everytime a new valid event blob is certified
#[test]
public fun certify_invalid_blob_id() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(ctx); // Total of 10 nodes
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);

    let bad_blob_id = blob::derive_blob_id(0xbeef, RED_STUFF, SIZE);
    let mut i: u256 = 0;
    while (i < 30) {
        // certify new good blob id and sq=i*100
        let good_blob_id = blob::derive_blob_id(i as u256, RED_STUFF, SIZE);    
        let good_cp = 100* (i as u64);
        let mut index = 0;
        while (index < 9) {
            system.certify_event_blob(
                nodes.borrow_mut(index).cap_mut(),
                good_blob_id,
                i as u256,
                SIZE,
                RED_STUFF,
                good_cp,
                0,
                ctx,
            );
            index = index + 1
        };

        let state = system.inner().get_event_blob_certification_state();
        assert!(state.get_latest_certified_checkpoint_sequence_number() == option::some(good_cp));

        // vote for bad_blob_id, always node 9 =        
        let bad_cp = 100*(i as u64) + 1;
        // next should never be certified...
        system.certify_event_blob(
            nodes.borrow_mut(9).cap_mut(),
            bad_blob_id,
            0xbeef,
            SIZE,
            RED_STUFF,
            bad_cp,
            0,
            ctx,
        );
        let state = system.inner().get_event_blob_certification_state();
        if (state.get_latest_certified_checkpoint_sequence_number() != option::some(good_cp)) {      
            // check the output -> the bad blob id was certified!
            debug::print(state);
            debug::print(&bad_blob_id);
            assert!(false);
        };
        i = i + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}


// DoS attack:
// run next test with -i 1000000000000
// it fails on MEMORY_LIMIT_EXCEEDED but from the debug logs it can be seen that aggregate_weight_per_blob is growing
// and thus it can be used to completely fill aggregate_weight_per_blob and block future valid blob events
#[test]
public fun block_blob_events() {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing_with_multiple_members(ctx); // Total of 10 nodes
    assert!(system.committee().to_vec_map().size() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, ctx, &mut nodes);

    let mut i: u256 = 0;
    while (i < 1002) {
        // certify sq=i*100
        let good_blob_id = blob::derive_blob_id(i as u256, RED_STUFF, SIZE);    
        let good_cp = 100* (i as u64);
        let mut index = 0;
        while (index < 9) {
            system.certify_event_blob(
                nodes.borrow_mut(index).cap_mut(),
                good_blob_id,
                i as u256,
                SIZE,
                RED_STUFF,
                good_cp,
                0,
                ctx,
            );
            index = index + 1
        };

        let state = system.inner().get_event_blob_certification_state();
        assert!(state.get_latest_certified_checkpoint_sequence_number() == option::some(good_cp));

        // vote for bad_blob_id, always node 9
        let hash = 2000*(i as u256);
        let bad_blob_id = blob::derive_blob_id(hash, RED_STUFF, SIZE);    
        
        let bad_cp = 100*(i as u64) + 1;
        // unique blob id per call -> fill up aggregate_weight_per_blob        
        system.certify_event_blob(
            nodes.borrow_mut(9).cap_mut(),
            bad_blob_id,
            hash,
            SIZE,
            RED_STUFF,
            bad_cp,
            0,
            ctx,
        );
        i = i + 1
    };
    nodes.destroy!(|node| node.destroy());
    destroy(system);
}
