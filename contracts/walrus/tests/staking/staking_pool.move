// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_pool_tests;
use sui::{coin::{Self, Coin}, sui::SUI, test_utils::destroy};
use walrus::staking_pool;

#[test]
fun test_staked_wal_flow() {
    let ctx = &mut tx_context::dummy();

    // step1 - create a new pool which will get activated in epoch 1
    let mut pool = staking_pool::new(1000, 1, ctx);
    assert!(pool.is_new());

    // step2 - set the pool to active, expecting that epoch is now 1
    pool.set_is_active();
    assert!(pool.is_active());

    // step3 - stake 1000 WALs in the pool
    let staked_wal = pool.stake(mint(1000, ctx), 1, ctx);
    assert!(pool.pending_stake_amount() == 1000);
    assert!(pool.active_stake_amount() == 0);

    // step4 - advance the epoch to 2
    destroy(staked_wal);
    destroy(pool);
}

fun mint(amount: u64, ctx: &mut TxContext): Coin<SUI> {
    coin::mint_for_testing(amount, ctx)
}
