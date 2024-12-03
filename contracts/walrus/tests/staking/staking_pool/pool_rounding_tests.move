// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::pool_rounding_tests;

use walrus::test_utils::{mint_balance, pool, context_runner, assert_eq, dbg};

#[test]
// Scenario: Alice stakes, pool receives rewards, Alice withdraws everything
fun stake_and_receive_rewards() {
    let mut test = context_runner();

    // E0: Alice stakes 199 WAL; Bob stakes 100 WAL
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(100), &wctx, ctx);
    let mut staked_b = pool.stake(mint_balance(100), &wctx, ctx);
    assert_eq!(pool.wal_balance(), 0);

    // E1: No rewards received yet, pool is expected to join the committee
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    assert_eq!(pool.wal_balance(), 200);

    // E2: Rewards received, Alice withdraws everything, Bob too
    // 200 WAL rewards:
    // Alice gets 201 / 2 = 100
    // Bob   gets 201 / 2 = 100
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(201), &wctx);
    assert_eq!(pool.wal_balance(), 401);

    pool.request_withdraw_stake(&mut staked_a, &wctx);
    pool.request_withdraw_stake(&mut staked_b, &wctx);

    staked_a.pool_token_amount().do!(|amt| assert!(amt == 100));
    staked_b.pool_token_amount().do!(|amt| assert!(amt == 100));

    // dbg!(b"staked_a", .destroy_some());
    // dbg!(b"staked_b", staked_b.pool_token_amount().destroy_some());

    let (wctx, _) = test.next_epoch();

    let balance_a = pool.withdraw_stake(staked_a, &wctx);
    let balance_b = pool.withdraw_stake(staked_b, &wctx);

    assert_eq!(balance_a.destroy_for_testing(), 200);
    assert_eq!(balance_b.destroy_for_testing(), 200);

    pool.destroy_empty();

    abort 1337
}
