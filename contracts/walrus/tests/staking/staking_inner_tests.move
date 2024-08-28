// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::staking_inner_tests;

use sui::{balance, clock, test_utils::destroy};
use walrus::{staking_inner, storage_node, test_utils as test};

#[test]
fun test_registration() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    let mut staking = staking_inner::new(300, &clock, ctx);

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
    let mut staking = staking_inner::new(300, &clock, ctx);

    // register the pool in the `StakingInnerV1`.
    let pool_one = test::pool().name(b"pool_1".to_string()).register(&mut staking, ctx);
    let pool_two = test::pool().name(b"pool_2".to_string()).register(&mut staking, ctx);
    let pool_three = test::pool().name(b"pool_3".to_string()).register(&mut staking, ctx);

    // now Alice, Bob, and Carl stake in the pools
    let wal_alice = staking.stake_with_pool(test::mint(100000, ctx), pool_one, ctx);
    let wal_bob = staking.stake_with_pool(test::mint(200000, ctx), pool_two, ctx);
    let wal_carl = staking.stake_with_pool(test::mint(700000, ctx), pool_three, ctx);

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
    let mut staking = staking_inner::new(300, &clock, ctx);

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
