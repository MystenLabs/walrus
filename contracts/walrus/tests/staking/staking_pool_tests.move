// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_pool_tests;

use std::unit_test::assert_eq;
use sui::test_utils::destroy;
use walrus::test_utils::{mint, pool, context_runner};

#[test]
// Tests correct data combination in the pool for both: pending stake and
// pending withdraw operations.
fun test_stake_and_withdrawals_at_epochs() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert!(pool.active_stake() == 0);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 1000);

    // E1: stake applied in E+2
    let (wctx, ctx) = test.select_committee();
    let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.stake_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 2), 2000);

    // E2: previous stake active; new stake applied in E+3
    let (wctx, ctx) = test.next_epoch();
    let mut staked_wal_c = pool.stake(mint(1000, ctx), &wctx, ctx);

    assert_eq!(pool.stake_at_epoch(wctx.epoch()), 1000);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 3000);

    // E3: all stakes active
    let (wctx, ctx) = test.next_epoch();

    assert_eq!(pool.stake_at_epoch(wctx.epoch()), 3000);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 3000);

    // E4: withdraw 2 stakes before committee selection
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx, ctx);
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx, ctx);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 1000);

    // E5: withdraw 1 stake after committee selection
    let (wctx, ctx) = test.select_committee();

    pool.request_withdraw_stake(&mut staked_wal_c, &wctx, ctx);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 1), 1000);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 2), 0);
    assert_eq!(pool.stake_at_epoch(wctx.epoch() + 3), 0);

    destroy(pool);
    destroy(vector[staked_wal_a, staked_wal_b, staked_wal_c]);
}

#[test]
// This test implements the following scenario:
// E0: create a pool, stake 1000 WALs twice
// E1: stake is applied, node is in the committee
// E2: rewards received, request withdraw stake
// E3: stake is withdrawn (should be E4 really, but not implemented)
fun test_staked_wal_flow() {
    // let mut test = context_runner();

    // let (wctx, ctx) = test.current();

    // // E0:
    // // - create a new pool in epoch 0.
    // // - stake for this pool (twice, different stakes)
    // let mut pool = pool().build(&wctx, ctx);
    // let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx, ctx);
    // let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx, ctx);

    // // E0+:
    // // - select committee
    // // - stake for this pool once more (stake should be applied in E+2)
    // let (wctx, ctx) = test.select_committee();
    // let mut staked_wal_c = pool.stake(mint(1000, ctx), &wctx, ctx);

    // // E1:
    // // - advance epoch, check that all stakes are applied
    // // - node is "in the committee"
    // let (wctx, ctx) = test.next_epoch();

    // pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);

    // assert_eq!(pool.active_stake(), 2000);
    // assert_eq!(pool.rewards_amount(), 0);
    // assert!(pool.is_active());

    // // E2:
    // // - node was operating, rewards are received
    // // - add rewards to the pool
    // // - check pool state and request withdraw for all stakes
    // let (wctx, ctx) = test.next_epoch();

    // // add rewards to the pool & advance epoch
    // pool.advance_epoch(mint(1000, ctx).into_balance(), &wctx);

    // assert_eq!(pool.active_stake(), 3000);
    // assert_eq!(pool.rewards_amount(), 1000);
    // assert!(pool.is_active());

    // // request withdraw for all stakes
    // pool.request_withdraw_stake(&mut staked_wal_a, &wctx, ctx);
    // pool.request_withdraw_stake(&mut staked_wal_b, &wctx, ctx);
    // // pool.request_withdraw_stake(&mut staked_wal_c, &wctx, ctx);

    // assert_eq!(pool.active_stake(), 3000);
    // assert!(staked_wal_a.is_withdrawing());
    // assert!(staked_wal_b.is_withdrawing());
    // // assert!(staked_wal_c.is_withdrawing());

    // assert_eq!(staked_wal_a.withdraw_epoch(), test.epoch() + 1);
    // assert_eq!(staked_wal_b.withdraw_epoch(), test.epoch() + 1);
    // // assert_eq!(staked_wal_c.withdraw_epoch(), test.epoch() + 1);

    // // step5:
    // // - advance epoch, perform actual withdrawal, check pool state and rewards
    // let (wctx, ctx) = test.next_epoch();

    // pool.advance_epoch(mint(0, ctx).into_balance(), &wctx);
    // assert_eq!(pool.active_stake(), 1000);
    // assert_eq!(pool.rewards_amount(), 1000);

    // let coin_a = pool.withdraw_stake(staked_wal_a, &wctx, ctx);
    // let coin_b = pool.withdraw_stake(staked_wal_b, &wctx, ctx);
    // // let coin_c = pool.withdraw_stake(staked_wal_c, &wctx, ctx);

    // std::debug::print(&vector[
    //     coin_a.value(),
    //     coin_b.value(),
    //     // coin_c.value(),
    // ]);

    // destroy(vector[
    //     coin_a,
    //     coin_b,
    //     // coin_c,
    // ]);

    // destroy(vector[
    //     // pool,
    //     // staked_wal_a,
    //     // staked_wal_b,
    //     staked_wal_c,
    // ]);



    // // step3 - stake 1000 WALs in the pool

    // let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx(1, true), ctx);
    // assert!(pool.active_stake() == 0);

    // // step4 - advance the epoch to 2, expecting that the stake A is applied
    // pool.advance_epoch(&wctx(2, false));
    // assert!(pool.active_stake() == 1000);

    // // step5 - advance the epoch to 3, expecting that the stake B is applied
    // pool.advance_epoch(&wctx(3, false));
    // assert!(pool.active_stake() == 2000);

    // // step6 - withdraw the stake A and B
    // pool.request_withdraw_stake(&mut staked_wal_a, &wctx(3, false), ctx);
    // pool.request_withdraw_stake(&mut staked_wal_b, &wctx(3, false), ctx);

    // // step7 - advance the epoch to 4, expecting that the stake A is withdrawn
    // pool.advance_epoch(&wctx(4, false));
    // assert!(pool.active_stake() == 0);

    // let coin_a = pool.withdraw_stake(staked_wal_a, &wctx(4, false), ctx);
    // let coin_b = pool.withdraw_stake(staked_wal_b, &wctx(4, false), ctx);

    // assert!(coin_a.value() == 1000);
    // assert!(coin_b.value() == 1000);

    // destroy(coin_a);
    // destroy(coin_b);
    // destroy(pool);

    // destroy(pool);
    // destroy(vector[staked_wal_a, staked_wal_b, staked_wal_c]);
}

#[test]
fun test_advance_pool_epoch() {
    // let ctx = &mut tx_context::dummy();

    // // create pool with commission rate 1000.
    // let mut pool = pool()
    //     .commission_rate(1000)
    //     .write_price(1)
    //     .storage_price(1)
    //     .node_capacity(1)
    //     .build(&wctx(1, true), ctx);

    // assert!(pool.active_stake() == 0);
    // assert!(pool.commission_rate() == 1000);
    // assert!(pool.write_price() == 1);
    // assert!(pool.storage_price() == 1);
    // assert!(pool.node_capacity() == 1);

    // // pool changes commission rate to 100 in epoch E+1
    // let wctx = &wctx(1, true);
    // pool.set_next_commission(100, wctx);
    // pool.set_next_node_capacity(1000, wctx);
    // pool.set_next_storage_price(100, wctx);
    // pool.set_next_write_price(100, wctx);

    // // TODO: commission rate should be applied in E+2
    // // assert!(pool.commission_rate() == 1000);
    // // other voting parameters are applied instantly,
    // // given that they are only counted in the committee selection.
    // assert!(pool.node_capacity() == 1000);
    // assert!(pool.write_price() == 100);
    // assert!(pool.storage_price() == 100);

    // // Alice stakes before committee selection, stake applied E+1
    // // Bob stakes after committee selection, stake applied in E+2
    // let sw1 = pool.stake(mint(1000, ctx), &wctx(1, false), ctx);
    // let sw2 = pool.stake(mint(1000, ctx), &wctx(1, true), ctx);
    // assert!(pool.active_stake() == 0);

    // // advance epoch to 2
    // // we expect Alice's stake to be applied already, Bob's not yet
    // // and parameters to be updated
    // let wctx = &wctx(2, false);
    // pool.advance_epoch(mint(0, ctx).into_balance(), wctx);

    // assert!(pool.active_stake() == 1000);
    // assert!(pool.commission_rate() == 100);
    // assert!(pool.node_capacity() == 1000);
    // assert!(pool.write_price() == 100);
    // assert!(pool.storage_price() == 100);

    // // update just one parameter
    // pool.set_next_write_price(1000, wctx);
    // assert!(pool.write_price() == 1000);

    // // advance epoch to 3
    // // we expect Bob's stake to be applied
    // // and commission rate to be updated
    // pool.advance_epoch(mint(0, ctx).into_balance(), wctx);
    // assert!(pool.active_stake() == 2000);
    // assert!(pool.write_price() == 1000);

    // destroy(pool);
    // destroy(sw1);
    // destroy(sw2);
}
