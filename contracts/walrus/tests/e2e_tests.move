// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_mut_ref)]
module walrus::e2e_tests;

use std::unit_test::assert_eq;
use walrus::{auth, e2e_runner, staking_pool, test_node::{Self, TestStorageNode}, test_utils};

const COMMISSION_RATE: u16 = 0;
const STORAGE_PRICE: u64 = 5;
const WRITE_PRICE: u64 = 1;
const NODE_CAPACITY: u64 = 1_000_000_000;
const N_SHARDS: u16 = 100;

/// Taken from `walrus::staking`. Must be the same values as there.
const EPOCH_DURATION: u64 = 7 * 24 * 60 * 60 * 1000;
const PARAM_SELECTION_DELTA: u64 = 7 * 24 * 60 * 60 * 1000 / 2;
const EPOCH_ZERO_DURATION: u64 = 100000000;

#[test]
fun init_and_first_epoch_change() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidates ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                COMMISSION_RATE,
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
            let coin = test_utils::mint_wal(1000, ctx);
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
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
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
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === check if epoch state is changed correctly ==

    runner.tx!(admin, |staking, _, _| assert!(staking.is_epoch_sync_done()));

    // === cleanup ===

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test]
fun stake_after_committee_selection() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidates ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                COMMISSION_RATE,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                ctx,
            );
            node.set_storage_node_cap(cap);
        });
    });

    // === stake with each node except one ===

    let excluded_node = nodes.pop_back();

    nodes.do_ref!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let coin = test_utils::mint_wal(1000, ctx);
            let staked_wal = staking.stake_with_pool(coin, node.node_id(), ctx);
            assert!(staking.can_withdraw_staked_wal_early(&staked_wal));
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // === advance clock and end voting ===

    runner.clock().increment_for_testing(EPOCH_ZERO_DURATION);
    runner.tx!(admin, |staking, _, _| {
        staking.voting_end(runner.clock());
    });

    // === add stake to excluded node ===

    runner.tx!(excluded_node.sui_address(), |staking, _, ctx| {
        let coin = test_utils::mint_wal(1000, ctx);
        let staked_wal = staking.stake_with_pool(coin, excluded_node.node_id(), ctx);
        transfer::public_transfer(staked_wal, ctx.sender());
    });

    // === initiate epoch change ===
    // === check if epoch state is changed correctly ==

    runner.tx!(admin, |staking, system, _| {
        staking.initiate_epoch_change(system, runner.clock());

        assert!(system.epoch() == 1);
        assert!(system.committee().n_shards() == N_SHARDS);

        // all nodes initially staked with are in the committee
        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
        // excluded node is not in the committee
        assert!(!system.committee().contains(&excluded_node.node_id()));
    });

    // === send epoch sync done messages from all nodes in the committee ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === advance clock and change epoch ===
    // === check if previously excluded node is now also in the committee ===

    runner.clock().increment_for_testing(EPOCH_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());

        assert!(system.epoch() == 2);
        assert!(system.committee().n_shards() == N_SHARDS);

        // all nodes initially staked with are in the committee
        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
        // previously excluded node is now also in the committee
        assert!(system.committee().contains(&excluded_node.node_id()));
    });

    // === cleanup ===

    nodes.destroy!(|node| node.destroy());
    excluded_node.destroy();
    runner.destroy();
}

#[test]
fun node_voting_parameters() {
    let mut nodes = test_node::test_nodes();
    let admin = @0xA11CE;
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // 10 storage nodes, we'll set storage price, write_capacity and node_capacity
    // to 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 and equal stake.
    let mut i = 1;
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                COMMISSION_RATE,
                i * 1000,
                i * 1000,
                i * 1000,
                ctx,
            );
            node.set_storage_node_cap(cap);

            i = i + 1;

            // stake in the same tx
            let staked_wal = staking.stake_with_pool(
                test_utils::mint_wal(1000, ctx),
                node.node_id(),
                ctx,
            );
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    runner.clock().increment_for_testing(EPOCH_ZERO_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());

        assert!(system.epoch() == 1);
        assert!(system.committee().n_shards() == N_SHARDS);

        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
    });

    // After initiate_epoch_change, prices are immediately applied to the system
    // from the new committee's quorum calculation.
    // values are: 1000, 2000, 3000, 4000, 5000, 6000, 7000 (picked), 8000, 9000, 10000
    runner.tx!(admin, |staking, system, _| {
        let inner = staking.inner_for_testing();
        let params = inner.next_epoch_params();

        // Prices are no longer set in next_epoch_params (they take effect immediately).
        assert_eq!(params.storage_price(), std::u64::max_value!());
        assert_eq!(params.write_price(), std::u64::max_value!());

        // node capacities are: 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000
        // votes:  10000, 20000, 30000, 40000 (picked), 50000, 60000, 70000, 80000, 90000, 100000
        assert_eq!(params.capacity(), 40000);
    });

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test, expected_failure(abort_code = walrus::staking_inner::EWrongEpochState)]
fun first_epoch_too_soon_fail() {
    let mut nodes = test_node::test_nodes();
    let admin = @0xA11CE;
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register nodes as storage node + stake for each ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let stake = test_utils::mint_wal(1000, ctx);
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                COMMISSION_RATE,
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

    abort
}

#[test]
fun epoch_change_with_rewards_and_commission() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidates ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                100_00, // 100.00% commission
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
            let coin = test_utils::mint_wal(1, ctx);
            let staked_wal = staking.stake_with_pool(coin, node.node_id(), ctx);
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // === advance clock, end voting, and change epoch ===
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
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === buy some storage to add rewards ===

    runner.tx!(admin, |_, system, ctx| {
        let mut coin = test_utils::mint_wal(1_000, ctx);
        let storage = system.reserve_space(1_000_000_000, 10, &mut coin, ctx);
        transfer::public_transfer(storage, ctx.sender());
        transfer::public_transfer(coin, ctx.sender());
    });

    // === register deny list update with each node (for tests) ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |_, system, _| {
            system.register_deny_list_update(node.cap_mut(), @1.to_u256(), 1);
        });

        // make sure that the event was emitted
        assert_eq!(runner.last_tx_effects().num_user_events(), 1);
    });

    // === register once more, now with incremented sequence number ===

    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |_, system, _| {
            system.register_deny_list_update(node.cap_mut(), @2.to_u256(), 2);
        });

        // make sure that the event was emitted
        assert_eq!(runner.last_tx_effects().num_user_events(), 1);
    });

    // === sign a message for the first node to update deny list size ===

    let node = &nodes[0];
    let deny_list_node = nodes[0].node_id();
    let certified_message = node.update_deny_list_message(runner.epoch(), 2u256, 10_000_000, 1);
    let (signature, members_bitmap) = nodes.sign(certified_message);

    // === send the message from the first node
    let node = &mut nodes[0];
    runner.tx!(node.sui_address(), |_, system, _| {
        system.update_deny_list(node.cap_mut(), signature, members_bitmap, certified_message);

        // check the VecMap with sizes
        let deny_list_sizes = system.inner().deny_list_sizes();

        assert_eq!(deny_list_sizes.size(), 1);
        assert!(deny_list_sizes.contains(&node.node_id()));
        assert_eq!(*deny_list_sizes.get(&node.node_id()), 10_000_000);
    });

    assert_eq!(runner.last_tx_effects().num_user_events(), 1); // update deny list event emitted
    assert_eq!(node.cap().deny_list_root(), 2u256); // deny list root updated
    assert_eq!(node.cap().deny_list_sequence(), 1); // sequence number incremented

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

        assert_eq!(system.epoch(), 2);
        assert_eq!(system.committee().n_shards(), N_SHARDS);

        nodes.do_ref!(|node| assert!(system.committee().contains(&node.node_id())));
    });

    // === send epoch sync done messages from all nodes ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === check if epoch state is changed correctly ==

    runner.tx!(admin, |staking, _, _| assert!(staking.is_epoch_sync_done()));

    // === check rewards for each node ===

    // each node is getting 470 in rewards, 10% of that is - 47 - commission
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let auth = auth::authenticate_with_object(node.cap());
            let commission = staking.collect_commission(node.node_id(), auth, ctx);

            // deny_list_node has 10% less rewards
            // all nodes claim 100% of their rewards
            if (node.node_id() == deny_list_node) {
                assert_eq!(commission.burn_for_testing(), 472);
            } else {
                assert_eq!(commission.burn_for_testing(), 477);
            };
        });
    });

    // === cleanup ===

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test]
fun node_update_metadata() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    let epoch = runner.epoch();
    let node = &mut nodes[0];

    runner.tx!(node.sui_address(), |staking, _, ctx| {
        let cap = staking.register_candidate(
            node.name(),
            node.network_address(),
            node.metadata(),
            node.bls_pk(),
            node.network_key(),
            node.create_proof_of_possession(epoch),
            COMMISSION_RATE,
            STORAGE_PRICE,
            WRITE_PRICE,
            NODE_CAPACITY,
            ctx,
        );
        node.set_storage_node_cap(cap);
    });

    runner.tx!(node.sui_address(), |staking, _, _| {
        let mut metadata = staking.node_metadata(node.node_id());
        metadata.set_description(b"Tusk Crew".to_string());
        metadata.set_project_url(b"https://crew.walrus.site/".to_string());
        staking.set_node_metadata(node.cap(), metadata);
    });

    runner.tx!(node.sui_address(), |staking, _, _| {
        let metadata = staking.node_metadata(node.node_id());
        assert_eq!(metadata.description(), b"Tusk Crew".to_string());
        assert_eq!(metadata.project_url(), b"https://crew.walrus.site/".to_string());
    });

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test, expected_failure(abort_code = staking_pool::EInvalidProofOfPossession)]
fun register_invalid_pop_epoch() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidate with proof of possession for wrong epoch ===
    let epoch = runner.epoch() + 1;
    let node = &mut nodes[0];
    // Test fails here
    runner.tx!(node.sui_address(), |staking, _, ctx| {
        let cap = staking.register_candidate(
            node.name(),
            node.network_address(),
            node.metadata(),
            node.bls_pk(),
            node.network_key(),
            node.create_proof_of_possession(epoch),
            COMMISSION_RATE,
            STORAGE_PRICE,
            WRITE_PRICE,
            NODE_CAPACITY,
            ctx,
        );
        node.set_storage_node_cap(cap);
    });

    abort
}

#[test, expected_failure(abort_code = staking_pool::EInvalidProofOfPossession)]
fun register_invalid_pop_signer() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidate with proof of possession for wrong epoch ===
    let epoch = runner.epoch() + 1;
    // wrong signer
    let pop = nodes[1].create_proof_of_possession(epoch);
    let node = &mut nodes[0];
    // Test fails here
    runner.tx!(node.sui_address(), |staking, _, ctx| {
        let cap = staking.register_candidate(
            node.name(),
            node.network_address(),
            node.metadata(),
            node.bls_pk(),
            node.network_key(),
            pop,
            COMMISSION_RATE,
            STORAGE_PRICE,
            WRITE_PRICE,
            NODE_CAPACITY,
            ctx,
        );
        node.set_storage_node_cap(cap);
    });

    abort
}

#[test]
fun protocol_version_updated_event() {
    let (mut runner, nodes) = e2e_runner::setup_committee_for_epoch_one();

    // === update protocol version ===

    let certified_message = nodes[0].protocol_version_updated_message(
        runner.epoch(),
        runner.epoch() + 1,
        1,
    );
    let (signature, members_bitmap) = nodes.sign(certified_message);
    let node = &nodes[0];

    runner.tx!(node.sui_address(), |_, system, _| {
        system.update_protocol_version(node.cap(), signature, members_bitmap, certified_message);
    });

    assert_eq!(runner.last_tx_effects().num_user_events(), 1); // event emitted
    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

#[test]
fun withdraw_rewards_before_joining_committee() {
    let admin = @0xA11CE;
    let mut nodes = test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin)
        .epoch_zero_duration(EPOCH_ZERO_DURATION)
        .epoch_duration(EPOCH_DURATION)
        .n_shards(N_SHARDS)
        .build();

    // === register candidates ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                node.metadata(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                COMMISSION_RATE,
                STORAGE_PRICE,
                WRITE_PRICE,
                NODE_CAPACITY,
                ctx,
            );
            node.set_storage_node_cap(cap);
        });
    });

    // === stake with each node except one ===

    let excluded_node = nodes.pop_back();

    nodes.do_ref!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let coin = test_utils::mint_wal(1000, ctx);
            let staked_wal = staking.stake_with_pool(coin, node.node_id(), ctx);
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // === initiate epoch change ===

    runner.clock().increment_for_testing(EPOCH_ZERO_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());
    });

    // === send epoch sync done messages from all nodes in the committee ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === add small amount of stake to excluded node ===

    runner.tx!(excluded_node.sui_address(), |staking, _, ctx| {
        let coin = test_utils::mint_wal(1, ctx);
        let staked_wal = staking.stake_with_pool(coin, excluded_node.node_id(), ctx);
        transfer::public_transfer(staked_wal, ctx.sender());
    });

    // === initiate epoch change ===

    runner.clock().increment_for_testing(EPOCH_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());
    });

    // === send epoch sync done messages from all nodes in the committee ===
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    // === withdraw stake from excluded node ===

    let staked_wal = runner.scenario().take_from_address(excluded_node.sui_address());
    runner.tx!(excluded_node.sui_address(), |staking, _, ctx| {
        let coin = staking.withdraw_stake(staked_wal, ctx);
        coin.burn_for_testing();
    });

    // === add stake to excluded node again ===

    runner.tx!(excluded_node.sui_address(), |staking, _, ctx| {
        let coin = test_utils::mint_wal(1000, ctx);
        let staked_wal = staking.stake_with_pool(coin, excluded_node.node_id(), ctx);
        transfer::public_transfer(staked_wal, ctx.sender());
    });

    // === advance clock and change epoch ===
    // === check if previously excluded node is now also in the committee ===

    runner.clock().increment_for_testing(EPOCH_DURATION);
    runner.tx!(admin, |staking, system, _| {
        staking.voting_end(runner.clock());
        staking.initiate_epoch_change(system, runner.clock());

        // previously excluded node is now also in the committee
        assert!(system.committee().contains(&excluded_node.node_id()));
    });

    // === cleanup ===

    nodes.destroy!(|node| node.destroy());
    excluded_node.destroy();
    runner.destroy();
}

// local alias for simplicity
use fun sign as vector.sign;

fun sign(nodes: &vector<TestStorageNode>, message: vector<u8>): (vector<u8>, vector<u8>) {
    let signatures = nodes.map_ref!(|node| node.sign_message(message));
    let members_bitmap = test_utils::signers_to_bitmap(
        &vector::tabulate!(nodes.length(), |i| i as u16),
    );
    let signature = test_utils::bls_aggregate_sigs(&signatures);

    (signature, members_bitmap)
}

#[test]
/// Tests that price votes take effect immediately on the system without
/// waiting for `voting_end` or `initiate_epoch_change`.
fun immediate_price_change_on_vote() {
    let (mut runner, mut nodes) = e2e_runner::setup_committee_for_epoch_one();
    let admin = @0xA11CE;

    // After epoch 1 setup, all 10 nodes have storage_price=10_000 and write_price=20_000.
    // The system should reflect these quorum prices after initiate_epoch_change.
    runner.tx!(admin, |_, system, _| {
        assert_eq!(system.storage_price_per_unit_size(), 10_000);
        assert_eq!(system.write_price_per_unit_size(), 20_000);
    });

    // === Test 1: Change storage price only ===
    // Have a quorum of nodes (7 out of 10, each with equal shards) vote for a new
    // storage price of 50_000. With quorum_below, the price picked is the one at
    // which a quorum (2/3) of shard weight votes at or below that value.
    // 7 nodes vote 50_000, 3 nodes still vote 10_000.
    // Sorted: 10000, 10000, 10000, 50000, 50000, 50000, 50000, 50000, 50000, 50000
    // quorum_below picks the value at the 2/3 threshold from the top = 50_000.
    let mut i = 0;
    nodes.do_mut!(|node| {
        if (i < 7) {
            runner.tx!(node.sui_address(), |staking, system, _| {
                staking.set_storage_price_vote(node.cap(), 50_000);
                staking.update_prices(system);
            });
        };
        i = i + 1;
    });

    // Verify: storage price changed immediately, write price unchanged.
    runner.tx!(admin, |_, system, _| {
        assert_eq!(system.storage_price_per_unit_size(), 50_000);
        assert_eq!(system.write_price_per_unit_size(), 20_000);
    });

    // === Test 2: Change write price only ===
    // Have all 10 nodes vote for a new write price of 100_000.
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, system, _| {
            staking.set_write_price_vote(node.cap(), 100_000);
            staking.update_prices(system);
        });
    });

    // Verify: write price changed immediately, storage price still 50_000.
    runner.tx!(admin, |_, system, _| {
        assert_eq!(system.storage_price_per_unit_size(), 50_000);
        assert_eq!(system.write_price_per_unit_size(), 100_000);
    });

    // === Test 3: Incremental vote changes ===
    // Only 1 node changes its storage price to 1. This should NOT change the
    // quorum price because 1 node out of 10 is not enough to shift quorum_below.
    // Current votes: 7 nodes at 50_000, 3 nodes at 10_000.
    // After: 6 nodes at 50_000, 3 nodes at 10_000, 1 node at 1.
    // Sorted: 1, 10000, 10000, 10000, 50000, 50000, 50000, 50000, 50000, 50000
    // quorum_below still picks 50_000.
    runner.tx!(nodes[0].sui_address(), |staking, system, _| {
        staking.set_storage_price_vote(nodes[0].cap(), 1);
        staking.update_prices(system);
    });

    runner.tx!(admin, |_, system, _| {
        assert_eq!(system.storage_price_per_unit_size(), 50_000);
    });

    // === Test 4: Enough votes shift the quorum ===
    // Now have 4 more nodes (total 5 including the one above) vote for storage price 1.
    // After: 5 nodes at 1, 2 nodes at 10_000, 3 remaining at 50_000 (wait, let me recount).
    // Original: nodes 0-6 voted 50_000, nodes 7-9 still at 10_000.
    // Node 0 just changed to 1. So: node 0 at 1, nodes 1-6 at 50_000, nodes 7-9 at 10_000.
    // Now change nodes 1-4 to 1 as well.
    // After: nodes 0-4 at 1, nodes 5-6 at 50_000, nodes 7-9 at 10_000.
    // Sorted: 1, 1, 1, 1, 1, 10000, 10000, 10000, 50000, 50000
    // With equal shards (100 each), quorum_below picks the value where cumulative
    // weight from the top first reaches > n_shards/3 = 334 shards.
    // From top: 50000(100), 50000(200), 10000(300), 10000(400) -> 10_000 at 400 > 334.
    // So quorum_below should return 10_000.
    let mut j = 1;
    while (j <= 4) {
        runner.tx!(nodes[j].sui_address(), |staking, system, _| {
            staking.set_storage_price_vote(nodes[j].cap(), 1);
            staking.update_prices(system);
        });
        j = j + 1;
    };

    runner.tx!(admin, |_, system, _| {
        assert_eq!(system.storage_price_per_unit_size(), 10_000);
    });

    // === Test 5: Prices persist across epoch change ===
    // Advance to epoch 2 and verify prices are maintained from the new committee's votes.
    runner.next_epoch();

    runner.tx!(admin, |_, system, _| {
        // After epoch change, prices are recalculated from the new committee.
        // The votes haven't changed, so prices should be the same.
        assert_eq!(system.storage_price_per_unit_size(), 10_000);
        assert_eq!(system.write_price_per_unit_size(), 100_000);
    });

    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}
