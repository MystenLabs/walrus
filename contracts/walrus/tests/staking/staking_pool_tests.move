// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_pool_tests;

use std::unit_test::assert_eq;
use sui::test_utils::destroy;
use walrus::test_utils::{mint, pool, context_runner};

#[test]
// Tests correct data combination in the pool for both: pending stake and
// pending withdraw operations.
fun stake_and_withdrawals_at_epochs() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.wal_balance(), 0);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 1000);

    // E1: stake applied in E+2
    let (wctx, ctx) = test.select_committee();
    let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx, ctx);

    // check stake at epochs
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 2), 2000);

    // check pool_token at epochs
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 2), 2000);

    // E2: previous stake active; new stake applied in E+3
    let (wctx, ctx) = test.next_epoch();
    let mut staked_wal_c = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 1000);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 3000);

    // E3: all stakes active
    let (wctx, ctx) = test.next_epoch();

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 3000);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 3000);

    // E4: withdraw 2 stakes before committee selection
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx, ctx);
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx, ctx);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 1000);

    // E5: withdraw 1 stake after committee selection
    let (wctx, ctx) = test.select_committee();

    pool.request_withdraw_stake(&mut staked_wal_c, &wctx, ctx);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 2), 0);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 3), 0);

    destroy(pool);
    destroy(vector[staked_wal_a, staked_wal_b, staked_wal_c]);
}

#[test]
// Alice stakes 1000 in E0, Bob stakes 1000 in E1, Alice withdraws in E2, Bob withdraws in E3
// We expect Alice to withdraw 1000 in E3 + rewards, Bob to withdraw 1000 in E4 without rewards
fun pool_token_with_rewards_at_epochs() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1000);

    // E1: node is in the committee, rewards are distributed, 1000 WAL received
    let (wctx, ctx) = test.next_epoch();

    pool.advance_epoch(mint(1000, ctx).into_balance(), &wctx);

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 2000); // 1000 + 1000 rewards
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000); // 1000 + 1000 rewards

    // bob stakes in E+1, stake applied in E+2
    let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1500);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 3000);

    // E+1, request withdraw stake A (to withdraw in E+2)
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx, ctx);

    assert!(staked_wal_a.is_withdrawing());
    assert_eq!(staked_wal_a.withdraw_epoch(), wctx.epoch() + 1);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 2000);

    // E+2, withdraw stake A
    let (wctx, ctx) = test.next_epoch();

    pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);

    // E+2, withdraw stake A, request withdraw stake B
    let coin = pool.withdraw_stake(staked_wal_a, &wctx, ctx);
    assert_eq!(coin.burn_for_testing(), 2000); // 1000 + 1000 rewards

    pool.request_withdraw_stake(&mut staked_wal_b, &wctx, ctx);

    // E+3, withdraw stake B
    let (wctx, ctx) = test.next_epoch();

    pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);

    let coin = pool.withdraw_stake(staked_wal_b, &wctx, ctx);
    assert_eq!(coin.burn_for_testing(), 1000);

    destroy(pool);
}

#[test]
// Alice stakes 1000 in E0, Bob stakes 1000 in E0, Pool receives 1000 rewards in E1
// Alice withdraws in E2, Bob withdraws in E2, rewards are split between Alice and Bob
fun pool_token_split_rewards() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx, ctx);
    let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 2000);

    // E1: node is in the committee, rewards are distributed, 1000 WAL received
    let (wctx, ctx) = test.next_epoch();

    pool.advance_epoch(mint(1000, ctx).into_balance(), &wctx);

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 3000); // 1000 + 1000 rewards
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 2000); // 2000 rewards

    // E+1, request withdraw stake A and B (to withdraw in E+2)
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx, ctx);
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx, ctx);

    let (wctx, ctx) = test.next_epoch();

    pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);

    // E+2, withdraw stake A and B
    let coin_a = pool.withdraw_stake(staked_wal_a, &wctx, ctx);
    let coin_b = pool.withdraw_stake(staked_wal_b, &wctx, ctx);

    // due to rounding on low values, we cannot check the exact value, but
    // we check that the difference is less than 1%
    assert!(coin_a.burn_for_testing().diff(1500) < 10); // 1000 + 500 rewards
    assert!(coin_b.burn_for_testing().diff(1500) < 10); // 1000 + 500 rewards

    destroy(pool);
}

#[test]
fun test_advance_pool_epoch() {
    let mut test = context_runner();

    // create pool with commission rate 1000.
    let (wctx, ctx) = test.current();
    let mut pool = pool()
        .commission_rate(1000)
        .write_price(1)
        .storage_price(1)
        .node_capacity(1)
        .build(&wctx, ctx);

    assert_eq!(pool.wal_balance(), 0);
    assert_eq!(pool.commission_rate(), 1000);
    assert_eq!(pool.write_price(), 1);
    assert_eq!(pool.storage_price(), 1);
    assert_eq!(pool.node_capacity(), 1);

    // pool changes commission rate to 100 in epoch E+1
    pool.set_next_commission(100, &wctx);
    pool.set_next_node_capacity(1000, &wctx);
    pool.set_next_storage_price(100, &wctx);
    pool.set_next_write_price(100, &wctx);

    // TODO: commission rate should be applied in E+2
    //_eq assert!(pool.commission_rate(), 1000);
    // other voting parameters are applied instantly,
    // given that they are only counted in the committee selection.
    assert_eq!(pool.node_capacity(), 1000);
    assert_eq!(pool.write_price(), 100);
    assert_eq!(pool.storage_price(), 100);

    // Alice stakes before committee selection, stake applied E+1
    // Bob stakes after committee selection, stake applied in E+2
    let sw1 = pool.stake(mint(1000, ctx), &wctx, ctx);

    let (wctx, ctx) = test.select_committee();
    let sw2 = pool.stake(mint(1000, ctx), &wctx, ctx);
    assert_eq!(pool.wal_balance(), 0);

    // advance epoch to 2
    // we expect Alice's stake to be applied already, Bob's not yet
    // and parameters to be updated
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);

    assert_eq!(pool.wal_balance(), 1000);
    assert_eq!(pool.commission_rate(), 100);
    assert_eq!(pool.node_capacity(), 1000);
    assert_eq!(pool.write_price(), 100);
    assert_eq!(pool.storage_price(), 100);

    // update just one parameter
    pool.set_next_write_price(1000, &wctx);
    assert_eq!(pool.write_price(), 1000);

    // advance epoch to 3
    // we expect Bob's stake to be applied
    // and commission rate to be updated
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);
    assert_eq!(pool.wal_balance(), 2000);
    assert_eq!(pool.write_price(), 1000);

    destroy(pool);
    destroy(sw1);
    destroy(sw2);
}
