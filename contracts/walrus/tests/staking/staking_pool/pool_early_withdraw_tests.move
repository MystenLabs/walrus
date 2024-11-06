// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// Early withdrawal mechanics (for E0, E0', E1, E1', E2, E2'):
// ```
// - stake(E0,  AE=E1) -> immediate withdrawal(E0)              // no rewards
// - stake(E0,  AE=E1) -> request_withdraw(E0') -> withdraw(E2) // rewards for E1
// - stake(E0', AE=E2) -> immediate withdrawal(E0', E1)         // no rewards
// - stake(E0', AE=E2) -> request_withdraw(E1') -> withdraw(E3) // rewards for E2

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

#[test]
// Scenario:
// 1. Alice stakes in E0 and imediately withdraws in E0
// 2. Bob stakes in E0 (and then requests after committee selection)
// 3. Charlie stakes in E0' (after committee selection) and withdraws in E1
// 4. Dave stakes in E0' (after committee selection) and requests in E1' and withdraws in E2
fun withdraw_processing_at_different_epochs() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);

    // Alice stakes before committee selection, stake applied E+1
    // And she performs the withdrawal right away
    let alice = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(alice.activation_epoch(), E1);
    assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
    let balance = pool.withdraw_stake(alice, &wctx);
    assert_eq!(balance.destroy_for_testing(), 1000);

    // Bob stakes before committee selection, stake applied E+1
    let mut bob = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(bob.activation_epoch(), E1);
    assert_eq!(pool.wal_balance_at_epoch(E1), 1000);

    let (wctx, ctx) = test.select_committee();

    // Bob requests withdrawal after committee selection
    pool.request_withdraw_stake(&mut bob, &wctx);
    assert!(bob.activation_epoch() > wctx.epoch());
    assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
    assert_eq!(bob.pool_token_amount(), 0);
    assert_eq!(bob.withdraw_epoch(), E2);

    // Charlie stakes after committee selection, stake applied E+2
    let charlie = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(charlie.activation_epoch(), E2);

    // Dave stakes after committee selection, stake applied E+2
    let mut dave = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(dave.activation_epoch(), E2);

    // E1: Charlie withdraws his stake directly, without requesting
    let (wctx, _ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance = pool.withdraw_stake(charlie, &wctx);
    assert_eq!(balance.destroy_for_testing(), 1000);

    // E1': Dave requests withdrawal
    let (wctx, _ctx) = test.select_committee();
    pool.request_withdraw_stake(&mut dave, &wctx);
    assert_eq!(dave.activation_epoch(), E2);
    assert_eq!(dave.withdraw_epoch(), E3);
    assert_eq!(dave.pool_token_amount(), 0);

    // E2: Bob withdraws his stake
    let (wctx, _ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance = pool.withdraw_stake(bob, &wctx);
    assert_eq!(balance.destroy_for_testing(), 1000);

    // E3: Dave withdraws his stake
    let (wctx, _ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance = pool.withdraw_stake(dave, &wctx);
    assert_eq!(balance.destroy_for_testing(), 1000);

    // empty wal balance but not empty pool tokens
    // because we haven't registered the pool token withdrawal
    assert_eq!(pool.wal_balance(), 0);

    pool.destroy_empty();
}


// #[test, expected_failure]
// // Scenario:
// // 1. Alice stakes in E0,
// // 2. Committee selected
// // 3. Alice tries to withdraw and fails
// fun request_withdraw_after_committee_selection() {
//     let mut test = context_runner();
//     let (wctx, ctx) = test.current();
//     let mut pool = pool().build(&wctx, ctx);

//     // Alice stakes in E0, tries to withdraw after committee selection
//     let mut sw1 = pool.stake(mint_balance(1000), &wctx, ctx);
//     assert_eq!(sw1.activation_epoch(), E1);
//     assert_eq!(pool.wal_balance_at_epoch(E1), 1000);

//     let (wctx, _ctx) = test.select_committee();

//     pool.request_withdraw_stake(&mut sw1, &wctx);

//     destroy(sw1);
//     destroy(pool);
// }

// #[test]
// // Scenario:
// // 1. Alice stakes in E0 after committee selection
// // 2. Alice withdraws in the next epoch before committee selection
// // 3. Success
// fun request_withdraw_after_committee_selection_next_epoch() {
//     let mut test = context_runner();
//     let (wctx, ctx) = test.current();
//     let mut pool = pool().build(&wctx, ctx);

//     // Alice stakes in E0 after committee selection, withdraws in the next epoch
//     let (wctx, ctx) = test.select_committee();
//     let sw1 = pool.stake(mint_balance(1000), &wctx, ctx);
//     assert_eq!(sw1.activation_epoch(), E2);
//     assert_eq!(pool.wal_balance_at_epoch(E2), 1000);

//     let (wctx, _ctx) = test.next_epoch();
//     let amount = pool.withdraw_stake(sw1, &wctx).destroy_for_testing();

//     assert_eq!(amount, 1000);
//     destroy(pool);
// }
