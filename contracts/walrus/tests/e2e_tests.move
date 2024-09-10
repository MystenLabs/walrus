// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_mut_ref)]
module walrus::e2e_tests;

use walrus::{e2e_runner, test_node, test_utils};

const COMMISSION: u64 = 1;
const STORAGE_PRICE: u64 = 5;
const WRITE_PRICE: u64 = 1;
const NODE_CAPACITY: u64 = 1_000_000_000;
const N_SHARDS: u16 = 100;

/// Taken from `walrus::staking`. Must be the same values as there.
const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;
const PARAM_SELECTION_DELTA: u64 = 7 * 24 * 60 * 60 * 1000 / 2;
const EPOCH_ZERO_DURATION: u64 = 100000000;

#[test]
fun test_init_and_first_epoch_change() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidates ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.bls_pk(),
                node.network_key(),
                COMMISSION,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                ctx,
            );
            node.set_storage_node_cap(cap);
        });
    });

    // === stake with each node ===

    nodes.do_ref!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let coin = test_utils::mint(1000, ctx);
            let staked_wal = staking.stake_with_pool(coin, node.node_id(), ctx);
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // === advance clock and end voting ===
    // === check if epoch state is changed correctly ==

    runner.clock().increment_for_testing(EPOCH_ZERO_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());

        assert!(system.epoch() == 1);
        assert!(system.committee().n_shards() == N_SHARDS);

        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
    });

    // === send epoch sync done messages from all nodes ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), runner.clock());
        });
    });

    // === perform another epoch change ===
    // === check if epoch state is changed correctly ==

    runner.clock().increment_for_testing(PARAM_SELECTION_DELTA);
    runner.tx!(admin, |staking, _, _| {
        assert!(staking.is_epoch_sync_done());
        staking.voting_end(runner.clock());
    });

    // === advance clock and change epoch ===
    // === check if epoch was changed as expected ===

    runner.clock().increment_for_testing(EPOCH_DURATION - PARAM_SELECTION_DELTA);
    runner.tx!(admin, |staking, system, _| {
        staking.initiate_epoch_change(system, runner.clock());

        assert!(system.epoch() == 2);
        assert!(system.committee().n_shards() == N_SHARDS);

        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
    });

    // === send epoch sync done messages from all nodes ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), runner.clock());
        });
    });

    // === check if epoch state is changed correctly ==

    runner.tx!(admin, |staking, _, _| assert!(staking.is_epoch_sync_done()));

    // === cleanup ===

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test, expected_failure(abort_code = walrus::staking_inner::EWrongEpochState)]
fun test_first_epoch_too_soon_fail() {
    let mut nodes = test_node::test_nodes();
    let admin = @0xA11CE;
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register nodes as storage node + stake for each ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let stake = test_utils::mint(1000, ctx);
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.bls_pk(),
                node.network_key(),
                COMMISSION,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                ctx,
            );
            node.set_storage_node_cap(cap);

            let staked_wal = staking.stake_with_pool(stake, node.node_id(), ctx);
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // === advance clock, end voting and initialize epoch change ===

    runner.clock().increment_for_testing(EPOCH_ZERO_DURATION - 1);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());
    });

    abort 0
}
