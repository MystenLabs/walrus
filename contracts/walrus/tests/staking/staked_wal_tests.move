// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staked_wal_tests;

use sui::test_utils::destroy;
use walrus::{staked_wal, test_utils::mint_balance};

#[test]
// Scenario: Test the staked WAL flow
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    // assert that the staked WAL is created correctly
    assert!(staked_wal.value() == 100);
    assert!(staked_wal.node_id() == node_id);
    assert!(staked_wal.activation_epoch() == 1);

    // test that splitting works correctly and copies the parameters
    let other = staked_wal.split(50, ctx);
    assert!(other.value() == 50);
    assert!(other.node_id() == node_id);
    assert!(other.activation_epoch() == 1);
    assert!(staked_wal.value() == 50);

    // test that joining works correctly
    staked_wal.join(other);
    assert!(staked_wal.value() == 100);
    assert!(staked_wal.node_id() == node_id);
    assert!(staked_wal.activation_epoch() == 1);

    // test that zero can be destroyed
    let zero = staked_wal.split(0, ctx);
    zero.destroy_zero();

    // test that the staked WAL can be burned
    let principal = staked_wal.into_balance();
    assert!(principal.value() == 100);

    destroy(principal);
}

#[test, expected_failure(abort_code = staked_wal::ECantSplitWithdrawing)]
// Scenario: Try splitting a staked WAL in the withdrawing state
// TODO: consider enabling this behavior in the future
fun test_unable_to_split_withdrawing() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    staked_wal.set_withdrawing(2, 100);
    let _other = staked_wal.split(50, ctx);

    abort 1337
}

#[test, expected_failure(abort_code = staked_wal::ECantJoinWithdrawing)]
// Scenario: Try splitting a staked WAL in the withdrawing state
// TODO: consider enabling this behavior in the future
fun test_unable_to_join_withdrawing() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal_a = staked_wal::mint(node_id, mint_balance(100), 1, ctx);
    let mut staked_wal_b = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    staked_wal_a.set_withdrawing(2, 100);
    staked_wal_b.set_withdrawing(2, 100);
    staked_wal_a.join(staked_wal_b);

    abort 1337
}

#[test, expected_failure(abort_code = staked_wal::EInvalidAmount)]
// Scenario: Split a staked WAL with a larger amount
fun test_unable_to_split_larger_amount() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    let _other = staked_wal.split(101, ctx);

    abort 1337
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
// Scenario: Join a staked WAL with a different activation epoch
fun test_unable_to_join_activation_epoch() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(node_id, mint_balance(100), 2, ctx);

    sw1.join(sw2);

    abort 1337
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
// Scenario: Join a staked WAL with a different pool ID
fun test_unable_to_join_different_pool() {
    let ctx = &mut tx_context::dummy();
    let node_id1 = ctx.fresh_object_address().to_id();
    let node_id2 = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id1, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(node_id2, mint_balance(100), 1, ctx);

    sw1.join(sw2);

    abort 1337
}

#[test, expected_failure(abort_code = staked_wal::ENonZeroPrincipal)]
// Scenario: Destroy a staked WAL with non-zero principal
fun test_unable_to_destroy_non_zero() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let sw = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    sw.destroy_zero();

    abort 1337
}
