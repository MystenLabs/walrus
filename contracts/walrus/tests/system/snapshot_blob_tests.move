// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::snapshot_blob_tests;

use sui::event;
use walrus::{
    blob,
    epoch_parameters::epoch_params_for_testing,
    events::{Self, BlobCertified},
    snapshot_blob,
    storage_node,
    system::{Self, System},
    system_state_inner,
    test_node::{test_nodes, TestStorageNode},
    upgrade
};

const RS2: u8 = 1;

const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;

#[test]
public fun test_snapshot_blob_certify_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    // Total of 10 nodes all with equal weights
    assert!(system.committee().to_vec_map().length() == 10);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut index = 0;
    while (index < 10) {
        system.certify_snapshot_blob(
            nodes.borrow(index).cap(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RS2,
            0,
            ctx,
        );
        let state = system.snapshot_blob_certification_state();
        if (index < 6) {
            assert!(state.get_latest_certified_epoch().is_none());
        } else {
            // 7th node attesting the blob triggers certification; further attestations for
            // the same epoch are no-ops.
            assert!(state.get_latest_certified_epoch() == option::some(0));
            assert!(state.get_latest_certified_blob_id() == option::some(blob_id));
            assert!(state.get_num_tracked_blobs() == 0);
        };
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing()
}

#[test, expected_failure(abort_code = system_state_inner::ERepeatedAttestation)]
public fun test_snapshot_blob_certify_repeated_attestation() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let divergent_blob_id = blob::derive_blob_id(0xDEF, RS2, SIZE);

    system.certify_snapshot_blob(
        nodes.borrow(0).cap(),
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        0,
        ctx,
    );

    // A second attestation by the same node in the same epoch fails, even for a different
    // blob id.
    system.certify_snapshot_blob(
        nodes.borrow(0).cap(),
        divergent_blob_id,
        0xDEF,
        SIZE,
        RS2,
        0,
        ctx,
    );

    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing();
}

#[test]
public fun test_snapshot_blob_divergent_attestations_and_epoch_rollover() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let divergent_blob_id = blob::derive_blob_id(0xDEF, RS2, SIZE);

    // 6 nodes attest the correct blob (one short of quorum), 4 divergent nodes attest a
    // different blob: no certification, both blob ids are tracked.
    let mut index = 0;
    while (index < 10) {
        let attested_blob_id = if (index < 6) { blob_id } else { divergent_blob_id };
        let attested_root_hash = if (index < 6) { ROOT_HASH } else { 0xDEF };
        system.certify_snapshot_blob(
            nodes.borrow(index).cap(),
            attested_blob_id,
            attested_root_hash,
            SIZE,
            RS2,
            0,
            ctx,
        );
        index = index + 1
    };
    let state = system.snapshot_blob_certification_state();
    assert!(state.get_latest_certified_epoch().is_none());
    assert!(state.get_num_tracked_blobs() == 2);

    // Increment epoch: the uncertified epoch-0 attestations become irrelevant.
    let mut new_committee = *system.committee();
    new_committee.increment_epoch_for_testing();
    let (_, balances) = system
        .advance_epoch(new_committee, &epoch_params_for_testing())
        .into_keys_values();
    balances.do!(|b| { b.destroy_for_testing(); });

    // All nodes (including the previously divergent ones) attest the same blob for epoch 1;
    // the stale epoch-0 attestations are lazily cleared, so re-attesting succeeds and the
    // 7th node triggers certification.
    index = 0;
    while (index < 10) {
        system.certify_snapshot_blob(
            nodes.borrow(index).cap(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RS2,
            1,
            ctx,
        );
        let state = system.snapshot_blob_certification_state();
        if (index < 6) {
            assert!(state.get_latest_certified_epoch().is_none());
        } else {
            assert!(state.get_latest_certified_epoch() == option::some(1));
            assert!(state.get_latest_certified_blob_id() == option::some(blob_id));
        };
        index = index + 1
    };
    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidIdEpoch)]
public fun test_snapshot_blob_certify_wrong_epoch() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);

    // The system is at epoch 0, so attesting a snapshot for epoch 1 fails.
    system.certify_snapshot_blob(
        nodes.borrow(0).cap(),
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        1,
        ctx,
    );

    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::ENotCommitteeMember)]
public fun test_snapshot_blob_certify_non_committee_member() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);

    // A capability whose node is not part of the committee cannot attest.
    let foreign_cap = storage_node::new_cap(
        object::id_from_address(ctx.fresh_object_address()),
        ctx,
    );
    system.certify_snapshot_blob(
        &foreign_cap,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        0,
        ctx,
    );

    transfer::public_transfer(foreign_cap, ctx.sender());
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = snapshot_blob::EInvalidEpoch)]
public fun test_snapshot_blob_certified_epoch_must_increase() {
    // Unit test against the state module directly: the certified epoch is strictly
    // monotonic. This cannot be reached through `certify_snapshot_blob`, which
    // short-circuits on an already-certified epoch; the assertion is defense in depth.
    let mut state = snapshot_blob::create_with_empty_state(53);
    state.certify(1, 0xABC, 3);
    state.certify(1, 0xDEF, 3);
    abort 0
}

// === Helper functions ===

#[test]
public fun test_snapshot_epochs_ahead_defaults_to_max_epochs_ahead() {
    let ctx = &mut tx_context::dummy();
    let system = system::new_for_testing_with_multiple_members(ctx);
    let max_epochs_ahead = system.future_accounting().max_epochs_ahead();
    assert!(system.snapshot_blob_certification_state().epochs_ahead() == max_epochs_ahead);
    system.destroy_for_testing()
}

#[test]
public fun test_one_epoch_system_gets_a_one_epoch_snapshot_lifetime() {
    // A system whose `max_epochs_ahead` is below the usual minimum lifetime must still be
    // creatable and migratable; its lifetime is bounded by its maximum instead.
    let mut state = snapshot_blob::create_with_empty_state(1);
    assert!(state.epochs_ahead() == 1);
    state.set_epochs_ahead(1, 1);
    assert!(state.epochs_ahead() == 1);
    state.destroy_for_testing();
}

#[test]
public fun test_set_snapshot_epochs_ahead() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let cap = upgrade::new_emergency_upgrade_cap_for_testing(ctx);
    let max_epochs_ahead = system.future_accounting().max_epochs_ahead();
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, 2);
    assert!(system.snapshot_blob_certification_state().epochs_ahead() == 2);
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, max_epochs_ahead);
    assert!(system.snapshot_blob_certification_state().epochs_ahead() == max_epochs_ahead);
    upgrade::burn_emergency_upgrade_cap(cap);
    system.destroy_for_testing()
}

#[test, expected_failure(abort_code = snapshot_blob::EInvalidEpochsAhead)]
public fun test_set_snapshot_epochs_ahead_rejects_below_min() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let cap = upgrade::new_emergency_upgrade_cap_for_testing(ctx);
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, 1);
    upgrade::burn_emergency_upgrade_cap(cap);
    system.destroy_for_testing()
}

#[test, expected_failure(abort_code = snapshot_blob::EInvalidEpochsAhead)]
public fun test_set_snapshot_epochs_ahead_rejects_above_max() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let cap = upgrade::new_emergency_upgrade_cap_for_testing(ctx);
    let max_epochs_ahead = system.future_accounting().max_epochs_ahead();
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, max_epochs_ahead + 1);
    upgrade::burn_emergency_upgrade_cap(cap);
    system.destroy_for_testing()
}

#[test]
public fun test_certified_snapshot_is_stored_for_the_configured_lifetime() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    let cap = upgrade::new_emergency_upgrade_cap_for_testing(ctx);
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, 5);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut index = 0;
    while (index < 7) {
        system.certify_snapshot_blob(
            nodes.borrow(index).cap(),
            blob_id,
            ROOT_HASH,
            SIZE,
            RS2,
            0,
            ctx,
        );
        index = index + 1;
    };
    let state = system.snapshot_blob_certification_state();
    assert!(state.get_latest_certified_epoch() == option::some(0));
    // The certified snapshot blob is stored for exactly the configured lifetime.
    let certified_events = event::events_by_type<BlobCertified>();
    assert!(certified_events.length() == 1);
    assert!(events::blob_certified_end_epoch(&certified_events[0]) == 5);
    // The recorded storage end matches the certified blob's.
    assert!(state.latest_certified().map!(|s| s.end_epoch()) == option::some(5));
    upgrade::burn_emergency_upgrade_cap(cap);
    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing()
}

#[test]
public fun test_snapshot_certification_history_keeps_live_and_latest_snapshots() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);
    let cap = upgrade::new_emergency_upgrade_cap_for_testing(ctx);
    // With a lifetime of two epochs, the snapshot certified in epoch E is stored until epoch
    // E + 2 (exclusive), so it expires by the certification of epoch E + 2.
    upgrade::set_snapshot_epochs_ahead(&cap, &mut system, 2);
    let mut nodes = test_nodes();
    set_storage_node_caps(&system, &mut nodes, ctx);
    // Certify one distinct snapshot per epoch for epochs 0, 1, and 3; no snapshot reaches a
    // quorum in epoch 2.
    let mut epoch: u32 = 0;
    while (epoch < 4) {
        if (epoch > 0) {
            let mut new_committee = *system.committee();
            new_committee.increment_epoch_for_testing();
            let (_, balances) = system
                .advance_epoch(new_committee, &epoch_params_for_testing())
                .into_keys_values();
            balances.do!(|b| { b.destroy_for_testing(); });
        };
        if (epoch == 2) {
            epoch = epoch + 1;
            continue
        };
        let root_hash = ROOT_HASH + (epoch as u256);
        let blob_id = blob::derive_blob_id(root_hash, RS2, SIZE);
        let mut index = 0;
        while (index < 7) {
            system.certify_snapshot_blob(
                nodes.borrow(index).cap(),
                blob_id,
                root_hash,
                SIZE,
                RS2,
                epoch,
                ctx,
            );
            index = index + 1;
        };
        assert!(
            system.snapshot_blob_certification_state().get_latest_certified_epoch()
                == option::some(epoch),
        );
        epoch = epoch + 1;
    };
    let state = system.snapshot_blob_certification_state();
    // By the certification of epoch 3, the snapshots of epochs 0 and 1 have both expired. The
    // snapshot of epoch 0 was dropped; the snapshot of epoch 1 is kept because it was the latest
    // certified one; epoch 3 is live.
    let history = state.certified_history();
    assert!(history.length() == 2);
    assert!(history[0].epoch() == 1);
    assert!(history[0].end_epoch() == 3);
    assert!(history[1].epoch() == 3);
    assert!(history[1].end_epoch() == 5);
    assert!(state.certified_for_epoch(0).is_none());
    assert!(state.certified_for_epoch(2).is_none());
    let expected_epoch_1 = blob::derive_blob_id(ROOT_HASH + 1, RS2, SIZE);
    assert!(state.certified_for_epoch(1).map!(|s| s.blob_id()) == option::some(expected_epoch_1));
    let latest_blob_id = state.certified_for_epoch(3).map!(|s| s.blob_id());
    assert!(state.get_latest_certified_blob_id() == latest_blob_id);
    upgrade::burn_emergency_upgrade_cap(cap);
    nodes.destroy!(|node| node.destroy());
    system.destroy_for_testing()
}

fun set_storage_node_caps(
    system: &System,
    nodes: &mut vector<TestStorageNode>,
    ctx: &mut TxContext,
) {
    let (node_ids, _values) = system.committee().to_vec_map().into_keys_values();
    let mut index = 0;
    node_ids.do!(|node_id| {
        let storage_cap = storage_node::new_cap(node_id, ctx);
        nodes[index].set_storage_node_cap(storage_cap);
        index = index + 1;
    });
}
