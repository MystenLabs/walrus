// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::pool_commission_tests;

use walrus::test_utils::{mint_balance, pool, context_runner, assert_eq};

#[test]
// Scenario:
// 0. Pool has initial commission rate of 10%
// 1. E0: Alice stakes
// 2. E1: Alice requests withdrawal
// 2. E2: Pool receives 10_000 rewards, Alice withdraws her stake
fun collect_commission_with_rewards() {
    let mut test = context_runner();
    let (wctx, ctx) = test.current();
    let mut pool = pool().commission_rate(10_00).build(&wctx, ctx);

    // Alice stakes before committee selection, stake applied E+1
    // And she performs the withdrawal right away
    let mut sw1 = pool.stake(mint_balance(1000), &wctx, ctx);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    pool.request_withdraw_stake(&mut sw1, &wctx);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(10_000), &wctx);

    // Alice's stake: 1000 + 9000 (90%) rewards
    assert_eq!(pool.withdraw_stake(sw1, &wctx).destroy_for_testing(), 10_000);
    assert_eq!(pool.commission_amount(), 1000);

    // Commission is 10% -> 1000
    let commission = pool.withdraw_commission(option::none());
    assert_eq!(commission.destroy_for_testing(), 1000);

    pool.destroy_empty();
}
