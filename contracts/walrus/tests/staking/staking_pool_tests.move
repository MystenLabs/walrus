// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_pool_tests;

use sui::test_utils::destroy;
use walrus::test_utils::{mint, wctx, pool};

#[test]
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();

    // step1 - create a new pool which will get activated in epoch 1
    let mut pool = pool().build(&wctx(1, false), ctx);

    // step3 - stake 1000 WALs in the pool
    let mut staked_wal_a = pool.stake(mint(1000, ctx), &wctx(1, false), ctx);
    let mut staked_wal_b = pool.stake(mint(1000, ctx), &wctx(1, true), ctx);
    assert!(pool.active_stake() == 0);

    // step4 - advance the epoch to 2, expecting that the stake A is applied
    pool.advance_epoch(&wctx(2, false));
    assert!(pool.active_stake() == 1000);

    // step5 - advance the epoch to 3, expecting that the stake B is applied
    pool.advance_epoch(&wctx(3, false));
    assert!(pool.active_stake() == 2000);

    // step6 - withdraw the stake A and B
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx(3, false), ctx);
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx(3, false), ctx);

    // step7 - advance the epoch to 4, expecting that the stake A is withdrawn
    pool.advance_epoch(&wctx(4, false));
    assert!(pool.active_stake() == 0);

    let coin_a = pool.withdraw_stake(staked_wal_a, &wctx(4, false), ctx);
    let coin_b = pool.withdraw_stake(staked_wal_b, &wctx(4, false), ctx);

    assert!(coin_a.value() == 1000);
    assert!(coin_b.value() == 1000);

    destroy(coin_a);
    destroy(coin_b);
    destroy(pool);
}

#[test]
fun test_advance_pool_epoch() {
    let ctx = &mut tx_context::dummy();

    // create pool with commission rate 1000.
    let mut pool = pool()
        .commission_rate(1000)
        .write_price(1)
        .storage_price(1)
        .node_capacity(1)
        .build(&wctx(1, true), ctx);

    assert!(pool.active_stake() == 0);
    assert!(pool.commission_rate() == 1000);
    assert!(pool.write_price() == 1);
    assert!(pool.storage_price() == 1);
    assert!(pool.node_capacity() == 1);

    // pool changes commission rate to 100 in epoch E+1
    let wctx = &wctx(1, true);
    pool.set_next_commission(100, wctx);
    pool.set_next_node_capacity(1000, wctx);
    pool.set_next_storage_price(100, wctx);
    pool.set_next_write_price(100, wctx);

    assert!(pool.commission_rate() == 1000);
    assert!(pool.node_capacity() == 1);
    assert!(pool.write_price() == 1);
    assert!(pool.storage_price() == 1);

    // Alice stakes before committee selection, stake applied E+1
    // Bob stakes after committee selection, stake applied in E+2
    let sw1 = pool.stake(mint(1000, ctx), &wctx(1, false), ctx);
    let sw2 = pool.stake(mint(1000, ctx), &wctx(1, true), ctx);
    assert!(pool.active_stake() == 0);

    // advance epoch to 2
    // we expect Alice's stake to be applied already, Bob's not yet
    // and parameters to be updated
    let wctx = &wctx(2, false);
    pool.advance_epoch(wctx);

    assert!(pool.active_stake() == 1000);
    assert!(pool.commission_rate() == 100);
    assert!(pool.node_capacity() == 1000);
    assert!(pool.write_price() == 100);
    assert!(pool.storage_price() == 100);

    // update just one parameter
    pool.set_next_commission(1000, wctx);
    assert!(pool.commission_rate() == 100);

    // advance epoch to 3
    // we expect Bob's stake to be applied
    // and commission rate to be updated
    pool.advance_epoch(&wctx(3, false));
    assert!(pool.active_stake() == 2000);
    assert!(pool.commission_rate() == 1000);

    destroy(pool);
    destroy(sw1);
    destroy(sw2);
}
