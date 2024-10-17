// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_use, unused_const)]
module walrus::pool_early_withdraw_tests;

use sui::test_utils::destroy;
use walrus::test_utils::{mint_balance, pool, context_runner, assert_eq, dbg};

const E0: u32 = 0;
const E1: u32 = 1;
const E2: u32 = 2;
const E3: u32 = 3;

#[test]
// Scenario:
// 1. Alice stakes in E0,
// 2. Alice withdraws in E0 before committee selection
fun withdraw_before_activation_before_committee_selection() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);

    // Alice stakes before committee selection, stake applied E+1
    // And she performs the withdrawal right away
    let sw1 = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(sw1.activation_epoch(), E1);
    assert_eq!(pool.wal_balance_at_epoch(E1), 1000);

    let balance = pool.withdraw_stake(sw1, &wctx);
    assert_eq!(balance.destroy_for_testing(), 1000);
    assert_eq!(pool.wal_balance_at_epoch(E1), 0);

    destroy(pool);
}

#[test, expected_failure]
// Scenario:
// 1. Alice stakes in E0,
// 2. Committee selected
// 3. Alice tries to withdraw and fails
fun request_withdraw_after_committee_selection() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);

    // Alice stakes in E0, tries to withdraw after committee selection
    let mut sw1 = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(sw1.activation_epoch(), E1);
    assert_eq!(pool.wal_balance_at_epoch(E1), 1000);

    let (wctx, _ctx) = test.select_committee();

    pool.request_withdraw_stake(&mut sw1, &wctx);

    destroy(sw1);
    destroy(pool);
}

#[test]
// Scenario:
// 1. Alice stakes in E0 after committee selection
// 2. Alice withdraws in the next epoch before committee selection
// 3. Success
fun request_withdraw_after_committee_selection_next_epoch() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);

    // Alice stakes in E0 after committee selection, withdraws in the next epoch
    let (wctx, ctx) = test.select_committee();
    let sw1 = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(sw1.activation_epoch(), E2);
    assert_eq!(pool.wal_balance_at_epoch(E2), 1000);

    let (wctx, _ctx) = test.next_epoch();
    let amount = pool.withdraw_stake(sw1, &wctx).destroy_for_testing();

    assert_eq!(amount, 1000);
    destroy(pool);
}
