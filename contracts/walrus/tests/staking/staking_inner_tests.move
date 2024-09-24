// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_inner_tests;

use sui::{balance, clock, test_utils::destroy};
use walrus::{staking_inner, storage_node, test_utils as test};
use std::unit_test::assert_eq;

const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;

#[test]
fun test_registration() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);

    // check the initial state: no active stake, no committee selected
    assert!(staking.epoch() == 0);
    assert!(staking.has_pool(pool_one));
    assert!(staking.has_pool(pool_two));
    assert!(staking.committee().size() == 0);
    assert!(staking.previous_committee().size() == 0);

    // destroy empty pools
    staking.destroy_empty_pool(pool_one, ctx);
    staking.destroy_empty_pool(pool_two, ctx);

    // make sure the pools are no longer there
    assert!(!staking.has_pool(pool_one));
    assert!(!staking.has_pool(pool_two));

    destroy(staking);
    clock.destroy_for_testing();
}

#[test]
fun test_staking_active_set() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);
    let pool_three = test::pool().name(b"pool_3".to_string()).register(&mut staking, ctx);

    // now Alice, Bob, and Carl stake in the pools
    let mut wal_alice = staking.stake_with_pool(test::mint(100000, ctx), pool_one, ctx);
    let wal_alice_2 = staking.stake_with_pool(test::mint(100000, ctx), pool_one, ctx);

    wal_alice.join(wal_alice_2);

    let wal_bob = staking.stake_with_pool(test::mint(200000, ctx), pool_two, ctx);
    let wal_carl = staking.stake_with_pool(test::mint(600000, ctx), pool_three, ctx);

    // expect the active set to be modified
    assert!(staking.active_set().total_stake() == 1000000);
    assert!(staking.active_set().active_ids().length() == 3);
    assert!(staking.active_set().cur_min_stake() == 0);

    // trigger `advance_epoch` to update the committee
    staking.select_committee();
    staking.advance_epoch(balance::create_for_testing(1000));

    // we expect:
    // - all 3 pools have been advanced
    // - all 3 pools have been added to the committee
    // - shards have been assigned to the pools evenly

    destroy(wal_alice);
    destroy(staking);
    destroy(wal_bob);
    destroy(wal_carl);
    clock.destroy_for_testing();
}

#[test]
fun test_parameter_changes() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_id = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let cap = storage_node::new_cap(pool_id, ctx);

    staking.set_next_commission(&cap, 10000);
    staking.set_next_storage_price(&cap, 100000000);
    staking.set_next_write_price(&cap, 100000000);
    staking.set_next_node_capacity(&cap, 10000000000000);

    // manually trigger advance epoch to apply the changes
    // TODO: this should be triggered via a system api
    staking[pool_id].advance_epoch(&test::wctx(1, false));

    assert!(staking[pool_id].storage_price() == 100000000);
    assert!(staking[pool_id].write_price() == 100000000);
    assert!(staking[pool_id].node_capacity() == 10000000000000);
    assert!(staking[pool_id].commission_rate() == 10000);

    destroy(staking);
    destroy(cap);
    clock.destroy_for_testing();
}

#[test]
fun test_epoch_sync_done() {
    let ctx = &mut tx_context::dummy();
    let mut clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);

    // now Alice, Bob, and Carl stake in the pools
    let wal_alice = staking.stake_with_pool(test::mint(300000, ctx), pool_one, ctx);
    let wal_bob = staking.stake_with_pool(test::mint(700000, ctx), pool_two, ctx);

    // trigger `advance_epoch` to update the committee and set the epoch state to sync
    staking.select_committee();
    staking.advance_epoch(balance::create_for_testing(1000));

    clock.increment_for_testing(EPOCH_DURATION);

    // send epoch sync done message from pool_one, which does not have a quorum
    let mut cap1 = storage_node::new_cap(pool_one, ctx);
    staking.epoch_sync_done(&mut cap1, &clock);

    assert!(!staking.is_epoch_sync_done());

    // send epoch sync done message from pool_two, which creates a quorum
    let mut cap2 = storage_node::new_cap(pool_two, ctx);
    staking.epoch_sync_done(&mut cap2, &clock);

    assert!(staking.is_epoch_sync_done());

    destroy(wal_alice);
    destroy(staking);
    destroy(wal_bob);
    cap1.destroy_cap_for_testing();
    cap2.destroy_cap_for_testing();
    clock.destroy_for_testing();
}

#[test, expected_failure(abort_code = staking_inner::EDuplicateSyncDone)]
fun test_epoch_sync_done_duplicate() {
    let ctx = &mut tx_context::dummy();
    let mut clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(0, EPOCH_DURATION, 300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);

    // now Alice, Bob, and Carl stake in the pools
    let wal_alice = staking.stake_with_pool(test::mint(300000, ctx), pool_one, ctx);
    let wal_bob = staking.stake_with_pool(test::mint(700000, ctx), pool_two, ctx);

    // trigger `advance_epoch` to update the committee and set the epoch state to sync
    staking.select_committee();
    staking.advance_epoch(balance::create_for_testing(1000));

    clock.increment_for_testing(7 * 24 * 60 * 60 * 1000);

    // send epoch sync done message from pool_one, which does not have a quorum
    let mut cap = storage_node::new_cap(pool_one, ctx);
    staking.epoch_sync_done(&mut cap, &clock);

    assert!(!staking.is_epoch_sync_done());

    // try to send duplicate, test fails here
    staking.epoch_sync_done(&mut cap, &clock);

    destroy(wal_alice);
    destroy(staking);
    destroy(wal_bob);
    cap.destroy_cap_for_testing();
    clock.destroy_for_testing();
}

fun dhondt_case(
    shards: u16,
    stake: vector<u64>,
    expected: vector<u16>,
) {
    use walrus::staking_inner::pub_dhondt as dhondt;
    let (_price, allocation) = dhondt(shards, stake);
    assert_eq!(allocation, expected);
    assert_eq!(allocation.sum!(), shards);
}

#[test]
fun test_dhondt_basic() {
    // even
    let stake = vector[25000, 25000, 25000, 25000];
    dhondt_case(4, stake, vector[1, 1, 1, 1]);
    dhondt_case(778, stake, vector[195, 195, 194, 194]);
    dhondt_case(1000, stake, vector[250, 250, 250, 250]);
    // uneven
    let stake = vector[50000, 30000, 15000, 5000];
    dhondt_case(4, stake, vector[3, 1, 0, 0]);
    dhondt_case(777, stake, vector[390, 233, 116, 38]);
    dhondt_case(1000, stake, vector[500, 300, 150, 50]);
    // uneven+even
    let stake = vector[50000, 50000, 30000, 15000, 15000, 5000];
    dhondt_case(4, stake, vector[2, 1, 1, 0, 0, 0]);
    dhondt_case(777, stake, vector[236, 236, 142, 70, 70, 23]);
    dhondt_case(1000, stake, vector[303, 303, 182, 91, 91, 30]);
}

#[test]
fun test_dhondt_ties() {
    // even
    let stake = vector[25000, 25000, 25000, 25000];
    dhondt_case(7, stake, vector[2, 2, 2, 1]);
    dhondt_case(6, stake, vector[2, 2, 1, 1]);
    // small uneven stake
    let stake = vector[200, 200, 200, 100];
    dhondt_case(7, stake, vector[2, 2, 2, 1]);
    let stake = vector[200, 200, 200, 100, 100, 100];
    dhondt_case(9, stake, vector[2, 2, 2, 1, 1, 1]);
    // tie with many solutions
    let stake = vector[780_000, 650_000, 520_000, 390_000, 260_000];
    dhondt_case(18, stake, vector[6, 5, 4, 2, 1]);
}

#[test]
fun test_dhondt_edge_case() {
    // no shards
    let stake = vector[100, 90, 80];
    dhondt_case(0, stake, vector[0, 0, 0]);
    // low stake
    let stake = vector[1, 0, 0];
    dhondt_case(5, stake, vector[5, 0, 0]);
    // nearly identical stake
    let s = 1_000_000;
    let stake = vector[s, s - 1];
    dhondt_case(3, stake, vector[2, 1]);
    // large stake
    let stake = vector[1_000_000_000_000, 900_000_000_000, 100_000_000_000];
    dhondt_case(500, stake, vector[250, 225, 25]);
}

#[test, expected_failure(abort_code = walrus::staking_inner::ENoStake)]
fun test_dhondt_no_stake() {
    let stake = vector[0, 0, 0];
    dhondt_case(0, stake, vector[0, 0, 0]);
}

use fun sum as vector.sum;
macro fun sum<$T>($v: vector<$T>): $T {
    let v = $v;
    let mut acc = (0: $T);
    v.do!(|e| acc = acc + e);
    acc
}
