// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_use)]
module walrus::staking_pool_tests;

use sui::test_utils::destroy;
use walrus::test_utils::{mint_balance, pool, context_runner, assert_eq, dbg};

#[test]
// Scenario: Alice stakes, pool receives rewards, Alice withdraws everything
fun stake_and_receive_rewards() {
    let mut test = context_runner();

    // E0: Alice stakes 1000 WAL
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(1000), &wctx, ctx);

    assert_eq!(pool.wal_balance(), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);

    // E1: No rewards received, stake is active an rewards will be claimable in
    //  the future
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    assert_eq!(pool.wal_balance(), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000);

    // E2: Rewards received, Alice withdraws everything
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_a, &wctx);

    assert_eq!(pool.wal_balance(), 2000);
    assert_eq!(staked_a.pool_token_amount(), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000);

    // E3: Alice withdraws everything
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance_a = pool.withdraw_stake(staked_a, &wctx);

    assert_eq!(balance_a.destroy_for_testing(), 2000);
    assert_eq!(pool.wal_balance(), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);

    pool.destroy_empty()
}

#[test]
// Scenario:
// Epoch 0: Alice stakes 1000
// Epoch 1: Bob stakes 1000
// Epoch 2: No rewards; Alice requests withdrawal before committee selection
// Epoch 3: No rewards, Bob requests withdrawal before committee selection, Alice withdraws
// Epoch 4: Bob withdraws, pool is empty
fun stake_no_rewards_different_epochs() {
    let mut test = context_runner();

    // E0: Alice stakes 1000 WAL
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(1000), &wctx, ctx);

    assert_eq!(pool.wal_balance(), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);

    // E1: Bob stakes 2000 WAL
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let mut staked_b = pool.stake(mint_balance(1000), &wctx, ctx);

    assert_eq!(pool.wal_balance(), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000);

    // E2: Alice requests withdrawal, expecting to withdraw 1000 WAL + 100 rewards.
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    pool.request_withdraw_stake(&mut staked_a, &wctx);

    assert_eq!(pool.wal_balance(), 2000);
    assert_eq!(staked_a.pool_token_amount(), 1000);

    // E3: Bob requests withdrawal
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    pool.request_withdraw_stake(&mut staked_b, &wctx);
    let balance_a = pool.withdraw_stake(staked_a, &wctx);

    assert_eq!(pool.wal_balance(), 1000);
    assert_eq!(balance_a.destroy_for_testing(), 1000);
    assert_eq!(staked_b.pool_token_amount(), 1000);

    // E5: Bob withdraws
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance_b = pool.withdraw_stake(staked_b, &wctx);
    assert_eq!(balance_b.destroy_for_testing(), 1000);

    pool.destroy_empty()
}

#[test]
// Scenario: Alice stakes, Bob stakes, pool receives rewards, Alice withdraws, Bob withdraws
fun stake_and_receive_partial_rewards() {
    let mut test = context_runner();

    // E0: Alice stakes 1000 WAL, Bob stakes 1000 WAL
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(1000), &wctx, ctx);
    let mut staked_b = pool.stake(mint_balance(1000), &wctx, ctx);

    // E1: No rewards received, stake is active an rewards will be claimable in
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    // E1: Rewards received, Alice requests withdrawal of 1000 WAL + rewards
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_a, &wctx);
    pool.request_withdraw_stake(&mut staked_b, &wctx);

    // E2: Alice withdraws 500 WAL + rewards
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance_a = pool.withdraw_stake(staked_a, &wctx);
    assert!(balance_a.destroy_for_testing().diff(1500) < 10);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    // E3: Bob a little late to withdraw
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance_b = pool.withdraw_stake(staked_b, &wctx);
    assert!(balance_b.destroy_for_testing().diff(1500) < 10);

    pool.destroy_empty()
}

#[test]
// Scenario:
// E0: Alice stakes 1000
// E1: No rewards, Alice splits stake
// E2: 1000 rewards received, Alice requests half of the stake (500)
// E3: 1000 rewards received, Alice requests the rest of the stake, withdraws first half (500 + 500)
// E4: Alice withdraws (500 + 1000), pool is empty
fun stake_split_stake() {
    let mut test = context_runner();

    // E0
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(1000), &wctx, ctx);

    // E1
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let mut staked_b = staked_a.split(500, ctx);

    // E2
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_a, &wctx);

    // E3
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_b, &wctx);
    let balance_a = pool.withdraw_stake(staked_a, &wctx);

    assert_eq!(balance_a.destroy_for_testing(), 1500);

    // E4
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let balance_b = pool.withdraw_stake(staked_b, &wctx);

    assert_eq!(balance_b.destroy_for_testing(), 1500);

    pool.destroy_empty()
}

#[test]
// Scenario:
// E0: Alice stakes: 1000 WAL;
// E1: Bob stakes: 2000 WAL;
// E2: +1000 Rewards; Alice requests withdrawal;
// E3: +1000 Rewards; Alice withdraws (1000 + 1500);  Bob requests withdrawal;
// E4: Bob withdraws (2000 + 1000); pool is empty
fun stake_maintain_ratio() {
    let mut test = context_runner();

    // E0: Alice stakes 1000 WAL
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_a = pool.stake(mint_balance(1000), &wctx, ctx);

    // E1: Bob stakes 1000 WAL
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    let mut staked_b = pool.stake(mint_balance(2000), &wctx, ctx);

    // E2: +1000 Rewards; Alice requests withdrawal
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_a, &wctx);
    assert_eq!(staked_a.pool_token_amount(), 1000);

    // E3: +1000 Rewards; Alice withdraws (1000 + 1000); Bob requests withdrawal
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);
    pool.request_withdraw_stake(&mut staked_b, &wctx);
    assert_eq!(staked_b.pool_token_amount(), 1000);

    let balance_a = pool.withdraw_stake(staked_a, &wctx);
    assert_eq!(balance_a.destroy_for_testing(), 2500);

    // E4: Bob withdraws (1000 + 1000); pool is empty
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    let balance_b = pool.withdraw_stake(staked_b, &wctx);
    assert_eq!(balance_b.destroy_for_testing(), 2500);

    pool.destroy_empty()
}

const E0: u32 = 0;
const E1: u32 = 1;
const E2: u32 = 2;
const E3: u32 = 3;
const E4: u32 = 4;
const E5: u32 = 5;
const E6: u32 = 5;


#[test]
// This test focuses on maintaining correct staked_wal state throughout the
// staking process. Alice and Bob add stake,
fun wal_balance_at_epoch() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);

    assert_eq!(pool.wal_balance(), 0);

    let mut staked_wal_a = pool.stake(mint_balance(1000), &wctx, ctx);

    {
        assert_eq!(pool.wal_balance(), 0);
        assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
        assert_eq!(pool.wal_balance_at_epoch(E2), 1000);
    };

    // E0+: committee has been selected, another stake applied in E+2
    let (wctx, ctx) = test.select_committee();
    let mut staked_wal_b = pool.stake(mint_balance(1000), &wctx, ctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E0), 0);
        assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
        assert_eq!(pool.wal_balance_at_epoch(E2), 2000);
    };

    // === E1 ===

    // E1: previous stake active; new stake applied in E2
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
        assert_eq!(pool.wal_balance_at_epoch(E2), 2000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 2000);
    };

    // add stake
    let mut staked_wal_c = pool.stake(mint_balance(2000), &wctx, ctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
        assert_eq!(pool.wal_balance_at_epoch(E2), 4000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 4000);
    };

    // E1+: committee selected, another stake applied in E+3
    let (wctx, ctx) = test.select_committee();
    let mut staked_wal_d = pool.stake(mint_balance(2000), &wctx, ctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E1), 1000);
        assert_eq!(pool.wal_balance_at_epoch(E2), 4000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 6000);
    };

    // === E2 ===

    // E2: previous stake active; add rewards to the pool: +1000
    // Active stakes: A: 1000 + 1000, B: 1000 (just activated), C: 2000 (just activated)
    // Inactive stakes: D: 2000
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E2), 5000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 7000); // D stake applied
        assert_eq!(pool.wal_balance_at_epoch(E4), 7000); // D stake applied
    };

    pool.request_withdraw_stake(&mut staked_wal_a, &wctx); // -2000 E+1

    {
        assert_eq!(pool.wal_balance_at_epoch(E2), 5000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 5000);
        assert_eq!(pool.wal_balance_at_epoch(E4), 5000);
    };

    // E2+: committee selected, another stake applied in E+2
    let (wctx, _) = test.select_committee();
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx); // -1000 E+2

    {
        assert_eq!(pool.wal_balance_at_epoch(E2), 5000);
        assert_eq!(pool.wal_balance_at_epoch(E3), 5000);
        assert_eq!(pool.wal_balance_at_epoch(E4), 4000);
    };

    // === E3 ===

    // E3: all stake active, add rewards to the pool: +5000
    // Active stakes: A: 2000 + 2000, B: 1000 + 1000, C: 2000 + 2000, D: 2000
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(5000), &wctx);

    // Stake A will withdraw an epoch later to test the behavior

    {
        assert_eq!(pool.wal_balance_at_epoch(E3), 8000); // A (-4000) D (+2000)
        assert_eq!(pool.wal_balance_at_epoch(E4), 6000); // B (-2000)
        assert_eq!(pool.wal_balance_at_epoch(E5), 6000);
    };

    pool.request_withdraw_stake(&mut staked_wal_c, &wctx); // C (-4000)

    {
        assert_eq!(pool.wal_balance_at_epoch(E3), 8000);
        assert_eq!(pool.wal_balance_at_epoch(E4), 2000);
        assert_eq!(pool.wal_balance_at_epoch(E5), 2000);
    };

    // E3+: committee selected, D stake requests withdrawal
    let (wctx, _) = test.select_committee();
    pool.request_withdraw_stake(&mut staked_wal_d, &wctx); // D (-2000)

    {
        assert_eq!(pool.wal_balance_at_epoch(E3), 8000);
        assert_eq!(pool.wal_balance_at_epoch(E4), 2000);
        assert_eq!(pool.wal_balance_at_epoch(E5), 0);
    };

    // === E4 ===

    // E3: B, C and D receive rewards (4000)
    // A is excluded
    // B (2000 + 1000) can withdraw
    // C (4000 + 2000) can withdraw
    // D (2000 + 1000) can't withdraw until E5
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(4000), &wctx);

    {
        assert_eq!(pool.wal_balance_at_epoch(E4), 3000); // only D active
        assert_eq!(pool.wal_balance_at_epoch(E5), 0); // D withdrawn
        assert_eq!(pool.wal_balance_at_epoch(E6), 0);
    };

    let (wctx, _) = test.next_epoch();

    {
        assert_eq!(pool.wal_balance_at_epoch(E5), 0);
    };


    // TODO: finish this test

    destroy(pool);
    destroy(vector[
        staked_wal_a,
        staked_wal_b,
        staked_wal_c,
        staked_wal_d,
    ]);
}

#[test]
// Alice stakes 1000 in E0, Bob stakes 1000 in E1, Alice withdraws in E2, Bob withdraws in E3
// We expect Alice to withdraw 1000 in E3 + rewards, Bob to withdraw 1000 in E4 without rewards
fun pool_token_with_rewards_at_epochs() {
    let mut test = context_runner();

    // E0: stake applied in E+1
    let (wctx, ctx) = test.current();
    let mut pool = pool().build(&wctx, ctx);
    let mut staked_wal_a = pool.stake(mint_balance(1000), &wctx, ctx);

    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 0);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1000);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    // E1: node is in the committee, rewards are distributed, 1000 WAL received
    let (wctx, ctx) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 2000); // 1000 + 1000 rewards
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000); // 1000 + 1000 rewards

    // bob stakes in E+1, stake applied in E+2
    let mut staked_wal_b = pool.stake(mint_balance(1000), &wctx, ctx);

    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 1000);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1500);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 3000);

    // E+1, request withdraw stake A (to withdraw in E+2)
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx);

    assert!(staked_wal_a.is_withdrawing());
    assert_eq!(staked_wal_a.withdraw_epoch(), wctx.epoch() + 1);
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch() + 1), 1500);
    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch() + 1), 1000);

    // E+2, withdraw stake A
    let (wctx, _) = test.next_epoch();

    pool.advance_epoch(mint_balance(0), &wctx);

    // E+2, withdraw stake A, request withdraw stake B
    let balance = pool.withdraw_stake(staked_wal_a, &wctx);
    assert_eq!(balance.destroy_for_testing(), 2000); // 1000 + 1000 rewards

    pool.request_withdraw_stake(&mut staked_wal_b, &wctx);

    // E+3, withdraw stake B
    let (wctx, _) = test.next_epoch();

    pool.advance_epoch(mint_balance(0), &wctx);

    let coin = pool.withdraw_stake(staked_wal_b, &wctx);
    assert_eq!(coin.destroy_for_testing(), 1000);

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
    let mut staked_wal_a = pool.stake(mint_balance(1000), &wctx, ctx);
    let mut staked_wal_b = pool.stake(mint_balance(1000), &wctx, ctx);

    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    // E1: node is in the committee, rewards are distributed, 1000 WAL received
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(1000), &wctx);

    assert_eq!(pool.wal_balance_at_epoch(wctx.epoch()), 3000); // 1000 + 1000 rewards
    assert_eq!(pool.pool_token_at_epoch(wctx.epoch()), 2000); // 2000 rewards

    // E+1, request withdraw stake A and B (to withdraw in E+2)
    pool.request_withdraw_stake(&mut staked_wal_a, &wctx);
    pool.request_withdraw_stake(&mut staked_wal_b, &wctx);

    let (wctx, _) = test.next_epoch();

    pool.advance_epoch(mint_balance(0), &wctx);

    // E+2, withdraw stake A and B
    let balance_a = pool.withdraw_stake(staked_wal_a, &wctx);
    let balance_b = pool.withdraw_stake(staked_wal_b, &wctx);

    // due to rounding on low values, we cannot check the exact value, but
    // we check that the difference is less than 1%
    assert!(balance_a.destroy_for_testing().diff(1500) < 10); // 1000 + 500 rewards
    assert!(balance_b.destroy_for_testing().diff(1500) < 10); // 1000 + 500 rewards

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
    pool.set_next_commission(100);
    pool.set_next_node_capacity(1000);
    pool.set_next_storage_price(100);
    pool.set_next_write_price(100);

    // TODO: commission rate should be applied in E+2
    //_eq assert!(pool.commission_rate(), 1000);
    // other voting parameters are applied instantly,
    // given that they are only counted in the committee selection.
    assert_eq!(pool.node_capacity(), 1000);
    assert_eq!(pool.write_price(), 100);
    assert_eq!(pool.storage_price(), 100);

    // Alice stakes before committee selection, stake applied E+1
    // Bob stakes after committee selection, stake applied in E+2
    let sw1 = pool.stake(mint_balance(1000), &wctx, ctx);

    let (wctx, ctx) = test.select_committee();
    let sw2 = pool.stake(mint_balance(1000), &wctx, ctx);
    assert_eq!(pool.wal_balance(), 0);

    // advance epoch to 2
    // we expect Alice's stake to be applied already, Bob's not yet
    // and parameters to be updated
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);

    assert_eq!(pool.wal_balance(), 1000);
    assert_eq!(pool.commission_rate(), 100);
    assert_eq!(pool.node_capacity(), 1000);
    assert_eq!(pool.write_price(), 100);
    assert_eq!(pool.storage_price(), 100);

    // update just one parameter
    pool.set_next_write_price(1000);
    assert_eq!(pool.write_price(), 1000);

    // advance epoch to 3
    // we expect Bob's stake to be applied
    // and commission rate to be updated
    let (wctx, _) = test.next_epoch();
    pool.advance_epoch(mint_balance(0), &wctx);
    assert_eq!(pool.wal_balance(), 2000);
    assert_eq!(pool.write_price(), 1000);

    destroy(pool);
    destroy(sw1);
    destroy(sw2);
}
