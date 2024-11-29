// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staked_wal_tests;

use std::unit_test::assert_eq;
use sui::test_utils::destroy;
use walrus::{staked_wal, test_utils::mint_balance};

#[test]
// Scenario: Test the staked WAL flow
fun staked_wal_lifecycle() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    // assert that the staked WAL is created correctly
    assert_eq!(staked_wal.value(), 100);
    assert_eq!(staked_wal.node_id(), node_id);
    assert_eq!(staked_wal.activation_epoch(), 1);

    // test that splitting works correctly and copies the parameters
    let other = staked_wal.split(50, ctx);
    assert_eq!(other.value(), 50);
    assert_eq!(other.node_id(), node_id);
    assert_eq!(other.activation_epoch(), 1);
    assert_eq!(staked_wal.value(), 50);

    // test that joining works correctly
    staked_wal.join(other);
    assert_eq!(staked_wal.value(), 100);
    assert_eq!(staked_wal.node_id(), node_id);
    assert_eq!(staked_wal.activation_epoch(), 1);

    // test that the staked WAL can be burned
    let principal = staked_wal.into_balance();
    assert_eq!(principal.destroy_for_testing(), 100);
}

#[test]
fun destroy_zero() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let zero = staked_wal::mint(node_id, mint_balance(0), 1, ctx);
    zero.destroy_zero();
}

#[test]
fun split_join_early_withdraw() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(1000), 1, ctx);

    sw1.set_withdrawing(2, option::none());

    let sw2 = sw1.split(500, ctx);

    assert_eq!(sw1.value(), 500);
    assert_eq!(sw2.value(), 500);
    assert!(sw1.pool_token_amount().is_none());
    assert!(sw2.pool_token_amount().is_none());

    sw1.join(sw2);

    assert_eq!(sw1.value(), 1000);
    assert!(sw1.pool_token_amount().is_none());

    destroy(sw1);
}

#[test, expected_failure(abort_code = staked_wal::EInvalidAmount)]
fun try_splitting_full_amount() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw = staked_wal::mint(node_id, mint_balance(1000), 1, ctx);
    let _v = sw.split(1000, ctx);

    abort
}

#[test, expected_failure(abort_code = staked_wal::EInvalidAmount)]
fun try_split_zero() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw = staked_wal::mint(node_id, mint_balance(1000), 1, ctx);
    sw.split(0, ctx).destroy_zero();

    abort
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
fun try_join_early_withdraw_with_non_early() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(1000), 0, ctx);
    let mut sw2 = staked_wal::mint(node_id, mint_balance(1000), 1, ctx);

    sw1.set_withdrawing(2, option::some(1000));
    sw2.set_withdrawing(2, option::none());
    sw1.join(sw2);

    abort
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
// reverse version of the test above to ensure the error is symmetric
fun try_join_non_early_with_early_withdraw() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(1000), 0, ctx);
    let mut sw2 = staked_wal::mint(node_id, mint_balance(1000), 1, ctx);

    sw1.set_withdrawing(2, option::some(1000));
    sw2.set_withdrawing(2, option::none());
    sw2.join(sw1);

    abort
}

#[test]
fun split_join_withdrawing() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(2000), 1, ctx);

    // set the staked WAL to withdrawing, 1000 pool tokens
    sw1.set_withdrawing(2, option::some(1000));

    let sw2 = sw1.split(500, ctx);

    assert!(sw1.is_withdrawing());
    assert_eq!(sw1.value(), 1500);
    sw1.pool_token_amount().do!(|amt| assert_eq!(amt, 750)); // 3/4 of the pool tokens

    assert!(sw2.is_withdrawing());
    assert_eq!(sw2.value(), 500);
    sw2.pool_token_amount().do!(|amt| assert_eq!(amt, 250)); // 1/4 of the pool tokens

    sw1.join(sw2);

    assert!(sw1.is_withdrawing());
    assert_eq!(sw1.value(), 2000);
    sw1.pool_token_amount().do!(|amt| assert_eq!(amt, 1000)); // all the pool tokens

    destroy(sw1);
}

#[test, expected_failure(abort_code = staked_wal::EInvalidAmount)]
// Scenario: Split a staked WAL with a larger amount
fun unable_to_split_larger_amount() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    let _other = staked_wal.split(101, ctx);

    abort
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
// Scenario: Join a staked WAL with a different activation epoch
fun unable_to_join_activation_epoch() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(node_id, mint_balance(100), 2, ctx);

    sw1.join(sw2);

    abort
}

#[test, expected_failure(abort_code = staked_wal::EMetadataMismatch)]
// Scenario: Join a staked WAL with a different pool ID
fun unable_to_join_different_pool() {
    let ctx = &mut tx_context::dummy();
    let node_id1 = ctx.fresh_object_address().to_id();
    let node_id2 = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(node_id1, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(node_id2, mint_balance(100), 1, ctx);

    sw1.join(sw2);

    abort
}

#[test, expected_failure(abort_code = staked_wal::ENonZeroPrincipal)]
// Scenario: Destroy a staked WAL with non-zero principal
fun unable_to_destroy_non_zero() {
    let ctx = &mut tx_context::dummy();
    let node_id = ctx.fresh_object_address().to_id();
    let sw = staked_wal::mint(node_id, mint_balance(100), 1, ctx);

    sw.destroy_zero();

    abort 1337 // keeping this abort due to a bug in tree-sitter
}
