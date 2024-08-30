// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::e2e_tests;

use sui::{clock, test_scenario};
use walrus::{init, staking, system, test_node, test_utils};

const COMMISSION: u64 = 1;
const STORAGE_PRICE: u64 = 5;
const WRITE_PRICE: u64 = 1;
const NODE_CAPACITY: u64 = 1_000_000_000;
const N_COINS: u64 = 1_000_000_000;
const N_SHARDS: u16 = 100;

/// Taken from `walrus::staking`. Must be the same values as there.
const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;
const PARAM_SELECTION_DELTA: u64 = 7 * 24 * 60 * 60 * 1000 / 2;
const EPOCH_ZERO_DURATION: u64 = 100000000;

#[test]
fun test_init_and_first_epoch_change(): (staking::Staking, system::System) {
    let mut nodes = test_node::test_nodes();
    let mut scenario = test_scenario::begin(nodes[0].sui_address());
    let mut clock = clock::create_for_testing(scenario.ctx());

    // === init ===
    {
        init::init_for_testing(scenario.ctx());
    };

    scenario.next_tx(nodes[0].sui_address());

    // === deploy walrus ===
    {
        let cap = scenario.take_from_sender<init::InitCap>();
        init::initialize_walrus(cap, EPOCH_ZERO_DURATION, N_SHARDS, &clock, scenario.ctx());
    };

    scenario.next_tx(nodes[0].sui_address());
    let mut staking = scenario.take_shared<staking::Staking>();
    let mut system = scenario.take_shared<system::System>();

    // === register nodes as storage node ===

    nodes.do_mut!(
        |node| {
            scenario.next_tx(node.sui_address());
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.bls_pk(),
                node.network_key(),
                COMMISSION,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                scenario.ctx(),
            );
            node.set_storage_node_cap(cap);
        },
    );

    // === stake with each node ===

    let mut coin = test_utils::mint(N_COINS, scenario.ctx());

    scenario.next_tx(nodes[0].sui_address());
    {
        nodes.do_ref!(
            |node| {
                let staked_coin = coin.split(1000, scenario.ctx());
                let staked_wal = staking.stake_with_pool(
                    staked_coin,
                    node.node_id(),
                    scenario.ctx(),
                );
                transfer::public_transfer(staked_wal, scenario.ctx().sender());
            },
        );
    };

    // === advance clock and end voting ===

    clock.increment_for_testing(EPOCH_ZERO_DURATION);
    scenario.next_tx(nodes[0].sui_address());
    {
        staking.voting_end(&clock);
        staking.initiate_epoch_change(&mut system, &clock);
    };

    // === check if epoch was changed as expected ===
    assert!(system.epoch() == 1);
    assert!(system.committee().n_shards() == N_SHARDS);
    let committee_members = system.committee().to_vec_map();
    nodes.do_ref!(|node| assert!(committee_members.contains(&node.node_id())));

    // === send epoch sync done messages from all nodes ===

    nodes.do_mut!(|node| {
        scenario.next_tx(node.sui_address());
        staking.epoch_sync_done(node.cap_mut(), &clock);
    });

    // === check if epoch state is changed correctly ==
    assert!(staking.is_epoch_sync_done());

    // === perform another epoch change ===

    clock.increment_for_testing(PARAM_SELECTION_DELTA);
    scenario.next_tx(nodes[0].sui_address());
    {
        staking.voting_end(&clock);
    };

    // === advance clock and change epoch ===

    clock.increment_for_testing(EPOCH_DURATION - PARAM_SELECTION_DELTA);
    scenario.next_tx(nodes[0].sui_address());
    {
        staking.initiate_epoch_change(&mut system, &clock);
    };

    // === check if epoch was changed as expected ===

    assert!(system.epoch() == 2);
    assert!(system.committee().n_shards() == N_SHARDS);
    let committee_members = system.committee().to_vec_map();
    nodes.do_ref!(|node| assert!(committee_members.contains(&node.node_id())));

    // === send epoch sync done messages from all nodes ===

    nodes.do_mut!(|node| {
        scenario.next_tx(node.sui_address());
        staking.epoch_sync_done(node.cap_mut(), &clock);
    });

    // === check if epoch state is changed correctly ==

    assert!(staking.is_epoch_sync_done());

    // === Clean up ===

    clock.destroy_for_testing();
    coin.burn_for_testing();
    nodes.destroy!(|node| node.destroy());
    scenario.end();
    (staking, system)
}

#[test, expected_failure(abort_code = walrus::staking_inner::EWrongEpochState)]
fun test_first_epoch_too_soon_fail() {
    let mut nodes = test_node::test_nodes();
    let mut scenario = test_scenario::begin(nodes[0].sui_address());
    let mut clock = clock::create_for_testing(scenario.ctx());

    // === init ===
    {
        init::init_for_testing(scenario.ctx());
    };

    scenario.next_tx(nodes[0].sui_address());

    // === deploy walrus ===
    {
        let cap = scenario.take_from_sender<init::InitCap>();
        init::initialize_walrus(cap, EPOCH_ZERO_DURATION, N_SHARDS, &clock, scenario.ctx());
    };

    scenario.next_tx(nodes[0].sui_address());
    let mut staking = scenario.take_shared<staking::Staking>();
    let mut system = scenario.take_shared<system::System>();

    // === register nodes as storage node ===

    nodes.do_mut!(
        |node| {
            scenario.next_tx(node.sui_address());
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.bls_pk(),
                node.network_key(),
                COMMISSION,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                scenario.ctx(),
            );
            node.set_storage_node_cap(cap);
        },
    );

    // === stake with each node ===

    let mut coin = test_utils::mint(N_COINS, scenario.ctx());

    scenario.next_tx(nodes[0].sui_address());
    {
        nodes.do_ref!(
            |node| {
                let staked_coin = coin.split(1000, scenario.ctx());
                let staked_wal = staking.stake_with_pool(
                    staked_coin,
                    node.node_id(),
                    scenario.ctx(),
                );
                transfer::public_transfer(staked_wal, scenario.ctx().sender());
            },
        );
    };

    // === advance clock, end voting and initialize epoch change ===

    clock.increment_for_testing(EPOCH_ZERO_DURATION - 1);
    scenario.next_tx(nodes[0].sui_address());
    {
        staking.voting_end(&clock);
        staking.initiate_epoch_change(&mut system, &clock);
    };

    abort 0
}
