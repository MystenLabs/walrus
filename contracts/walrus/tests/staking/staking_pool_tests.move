// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_pool_tests;

use sui::{coin::{Self, Coin}, sui::SUI, test_utils::destroy};
use walrus::{staking_pool, walrus_context::{Self, WalrusContext}};

#[test]
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();
    let wctx = &wctx(1, true);

    // step1 - create a new pool which will get activated in epoch 1
    let mut pool = staking_pool::new(1000, wctx, ctx);
    assert!(pool.is_new());

    // step2 - set the pool to active, expecting that epoch is now 1
    pool.set_is_active();
    assert!(pool.is_active());

    // step3 - stake 1000 WALs in the pool
    let staked_wal = pool.stake(mint(1000, ctx), wctx, ctx);
    assert!(pool.active_stake_amount() == 0);

    // step4 - advance the epoch to 2
    destroy(staked_wal);
    destroy(pool);
}

#[test]
fun test_advance_pool_epoch() {
    let ctx = &mut tx_context::dummy();

    // create pool with commission rate 1000.
    let mut pool = staking_pool::new(1000, &wctx(1, false), ctx);
    assert!(pool.active_stake_amount() == 0);
    assert!(pool.commission_rate() == 1000);

    // pool changes commission rate to 100 in epoch E+1
    pool.set_next_commission(100, &wctx(1, true));
    assert!(pool.commission_rate() == 1000);

    // Alice stakes before committe selection, stake applied E+1
    // Bob stakes after committee selection, stake applied in E+2
    let sw1 = pool.stake(mint(1000, ctx), &wctx(1, false), ctx);
    let sw2 = pool.stake(mint(1000, ctx), &wctx(1, true), ctx);
    assert!(pool.active_stake_amount() == 0);

    // advance epoch to 2
    // we expect Alice's stake to be applied already, Bob's not yet
    // and commission rate to be updated
    let wctx = &wctx(2, false);
    pool.advance_epoch(wctx);
    assert!(pool.active_stake_amount() == 1000);
    assert!(pool.commission_rate() == 100);

    pool.set_next_commission(1000, wctx);
    assert!(pool.commission_rate() == 100);

    // advance epoch to 3
    // we expect Bob's stake to be applied
    // and commission rate to be updated
    pool.advance_epoch(&wctx(3, false));
    assert!(pool.active_stake_amount() == 2000);
    assert!(pool.commission_rate() == 1000);

    destroy(pool);
    destroy(sw1);
    destroy(sw2);
}

fun wctx(epoch: u64, committee_selected: bool): WalrusContext {
    walrus_context::new(epoch, committee_selected)
}

fun mint(amount: u64, ctx: &mut TxContext): Coin<SUI> {
    coin::mint_for_testing(amount, ctx)
}
