// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staked_wal_tests;

use sui::test_utils::destroy;
use walrus::{staked_wal, test_utils::mint_balance};

#[test]
// Scenario: Test the staked WAL flow
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();
    let pool_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(pool_id, mint_balance(100), 1, ctx);

    // assert that the staked WAL is created correctly
    assert!(staked_wal.value() == 100);
    assert!(staked_wal.pool_id() == pool_id);
    assert!(staked_wal.activation_epoch() == 1);

    // test that splitting works correctly and copies the parameters
    let other = staked_wal.split(50, ctx);
    assert!(other.value() == 50);
    assert!(other.pool_id() == pool_id);
    assert!(other.activation_epoch() == 1);
    assert!(staked_wal.value() == 50);

    // test that joining works correctly
    staked_wal.join(other);
    assert!(staked_wal.value() == 100);
    assert!(staked_wal.pool_id() == pool_id);
    assert!(staked_wal.activation_epoch() == 1);

    // test that zero can be destroyed
    let zero = staked_wal.split(0, ctx);
    zero.destroy_zero();

    // test that the staked WAL can be burned
    let principal = staked_wal.unwrap();
    assert!(principal.value() == 100);

    destroy(principal);
}

#[test, expected_failure]
// Scenario: Split a staked WAL with a larger amount
fun test_unable_to_split_larger_amount() {
    let ctx = &mut tx_context::dummy();
    let pool_id = ctx.fresh_object_address().to_id();
    let mut staked_wal = staked_wal::mint(pool_id, mint_balance(100), 1, ctx);

    let _other = staked_wal.split(101, ctx);

    abort 1337
}

#[test, expected_failure]
// Scenario: Join a staked WAL with a different activation epoch
fun test_unable_to_join_activation_epoch() {
    let ctx = &mut tx_context::dummy();
    let pool_id = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(pool_id, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(pool_id, mint_balance(100), 2, ctx);

    sw1.join(sw2);

    abort 1337
}

#[test, expected_failure]
// Scenario: Join a staked WAL with a different pool ID
fun test_unable_to_join_different_pool() {
    let ctx = &mut tx_context::dummy();
    let pool_id1 = ctx.fresh_object_address().to_id();
    let pool_id2 = ctx.fresh_object_address().to_id();
    let mut sw1 = staked_wal::mint(pool_id1, mint_balance(100), 1, ctx);
    let sw2 = staked_wal::mint(pool_id2, mint_balance(100), 1, ctx);

    sw1.join(sw2);

    abort 1337
}

#[test, expected_failure]
// Scenario: Destroy a staked WAL with non-zero principal
fun test_unable_to_destroy_non_zero() {
    let ctx = &mut tx_context::dummy();
    let pool_id = ctx.fresh_object_address().to_id();
    let sw = staked_wal::mint(pool_id, mint_balance(100), 1, ctx);

    sw.destroy_zero();

    abort 1337
}
