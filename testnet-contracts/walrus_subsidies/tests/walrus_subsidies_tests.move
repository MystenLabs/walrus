// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus_subsidies::walrus_subsidies_tests;

use std::unit_test::assert_eq;
use sui::{test_scenario, test_utils::destroy};
use walrus::{e2e_runner::{Self, TestRunner}, system::System, test_utils::{mint_wal, wal_to_frost}};
use walrus_subsidies::walrus_subsidies::{Self, WalrusSubsidies};

// === Constants ===

const DEFAULT_SYSTEM_SUBSIDY_RATE: u32 = 15_000; // 150%
const DEFAULT_BASE_SUBSIDY: u64 = 1_000_000;
const DEFAULT_PER_SHARD_SUBSIDY: u64 = 100_000;

#[test]
fun test_add_funds_to_subsidies() {
    let admin = @0xA11CE;
    let user = @0xB0B;
    let mut runner = e2e_runner::prepare(admin).build();

    let admin_cap;

    // Create subsidies with admin
    runner.tx!(admin, |staking, system, ctx| {
        admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_PER_SHARD_SUBSIDY,
                ctx,
            );
    });

    // Anyone can add funds
    runner.scenario().next_tx(user);
    let mut subsidies: WalrusSubsidies = runner.scenario().take_shared();
    let funds = mint_wal(5000, runner.scenario().ctx());
    subsidies.add_coin(funds);
    assert_eq!(subsidies.subsidy_pool_balance(), wal_to_frost(5000));

    // Add balance directly
    runner.scenario().next_tx(user);
    let balance = mint_wal(3000, runner.scenario().ctx()).into_balance();
    subsidies.add_balance(balance);
    assert_eq!(subsidies.subsidy_pool_balance(), wal_to_frost(8000));
    test_scenario::return_shared(subsidies);

    destroy(admin_cap);
    runner.destroy();
}

#[test]
fun test_admin_operations() {
    let admin = @0xA11CE;
    let mut runner = e2e_runner::prepare(admin).build();

    let admin_cap;

    // Create subsidies
    runner.tx!(admin, |staking, system, ctx| {
        admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_PER_SHARD_SUBSIDY,
                ctx,
            );
    });

    // Advance to the next transaction to make the subsidies object available in the shared
    // inventory.
    runner.scenario().next_tx(admin);

    let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();

    // Test setting system subsidy rate
    subsidies.set_system_subsidy_rate(&admin_cap, 2*DEFAULT_SYSTEM_SUBSIDY_RATE);
    assert_eq!(subsidies.system_subsidy_rate(), 2*DEFAULT_SYSTEM_SUBSIDY_RATE);

    // Test setting base subsidy
    subsidies.set_base_subsidy(&admin_cap, 2*DEFAULT_BASE_SUBSIDY);
    assert_eq!(subsidies.base_subsidy(), 2*DEFAULT_BASE_SUBSIDY);

    // Test setting per-shard subsidy
    subsidies.set_per_shard_subsidy(&admin_cap, 2*DEFAULT_PER_SHARD_SUBSIDY);
    assert_eq!(subsidies.per_shard_subsidy(), 2*DEFAULT_PER_SHARD_SUBSIDY);

    // Clean up
    test_scenario::return_shared(subsidies);
    destroy(admin_cap);
    runner.destroy();
}

#[test, expected_failure(abort_code = walrus_subsidies::EUnauthorizedAdminCap)]
fun test_unauthorized_admin_cap_fails() {
    let admin = @0xA11CE;
    let mut runner = e2e_runner::prepare(admin).build();

    let _admin_cap;
    let fake_admin_cap;

    // Create a first Subsidies object to get an AdminCap.
    runner.tx!(admin, |staking, system, ctx| {
        fake_admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_PER_SHARD_SUBSIDY,
                ctx,
            );
    });

    // Create second subsidies object to try to act on using the first AdminCap.
    runner.tx!(admin, |staking, system, ctx| {
        _admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_PER_SHARD_SUBSIDY,
                ctx,
            );
    });

    runner.scenario().next_tx(admin);

    // Take the subsidies object. This is the second subsidies object, since `take_shared` is
    // LIFO.
    let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();

    // Test fails here because the AdminCap is from a different subsidies object.
    subsidies.set_system_subsidy_rate(&fake_admin_cap, 2*DEFAULT_SYSTEM_SUBSIDY_RATE);

    // No cleanup, should abort above.
    abort 0
}

// === End-to-End Tests with Multiple Epochs ===

#[test]
fun test_process_subsidies_with_usage_across_epochs() {
    let admin = @0xA11CE;
    let (mut runner, mut nodes) = e2e_runner::setup_committee_for_epoch_one();

    let admin_cap;

    // Create subsidies object.
    runner.tx!(admin, |staking, system, ctx| {
        admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                DEFAULT_SYSTEM_SUBSIDY_RATE,
                DEFAULT_BASE_SUBSIDY,
                DEFAULT_PER_SHARD_SUBSIDY,
                ctx,
            );
    });

    // Add initial funds to the subsidy pool
    runner.scenario().next_tx(admin);
    let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();
    let initial_funds = mint_wal(10_000_000, runner.scenario().ctx());
    subsidies.add_coin(initial_funds);
    test_scenario::return_shared(subsidies);

    // Add some usage and check that the usage-independent subsidy has been applied correctly.
    add_usage_and_check_usage_subsidies(&mut runner, DEFAULT_SYSTEM_SUBSIDY_RATE);

    // Check that the usage-independent subsidy has been applied correctly for each node.
    runner.tx!(admin, |staking, _, _| {
        nodes.do_ref!(|node| {
            let node_weight = staking.committee().shards(&node.node_id()).length() as u64;
            assert_eq!(
                staking.pool_commission(node.node_id()),
                DEFAULT_BASE_SUBSIDY + DEFAULT_PER_SHARD_SUBSIDY * node_weight,
            );
        });
    });

    // Add more usage and check that the usage-dependent subsidies are applied correctly.
    add_usage_and_check_usage_subsidies(&mut runner, DEFAULT_SYSTEM_SUBSIDY_RATE);

    // Check that the usage-independent subsidy has not been applied again.
    runner.tx!(admin, |staking, _, _| {
        nodes.do_ref!(|node| {
            let node_weight = staking.committee().shards(&node.node_id()).length() as u64;
            assert_eq!(
                staking.pool_commission(node.node_id()),
                DEFAULT_BASE_SUBSIDY + DEFAULT_PER_SHARD_SUBSIDY * node_weight,
            );
        });
    });

    // Advance to the next epoch.
    runner.next_epoch();
    runner.send_epoch_sync_done_messages(&mut nodes);

    // Add usage in epoch 2 and check that the usage-dependent subsidies are applied correctly.
    add_usage_and_check_usage_subsidies(&mut runner, DEFAULT_SYSTEM_SUBSIDY_RATE);

    // Check that the usage-independent subsidy has not been applied again.
    runner.tx!(admin, |staking, _, _| {
        nodes.do_ref!(|node| {
            let node_weight = staking.committee().shards(&node.node_id()).length() as u64;
            assert_eq!(
                // This check works because the commission rate is 0 and we don't change the stake
                // distribution.
                staking.pool_commission(node.node_id()),
                2*(DEFAULT_BASE_SUBSIDY + DEFAULT_PER_SHARD_SUBSIDY * node_weight),
            );
        });
    });

    // Clean up
    destroy(admin_cap);
    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}

/// Helper function to add some usage to the system and check that the usage-based subsidies are
/// applied correctly.
fun add_usage_and_check_usage_subsidies(runner: &mut TestRunner, system_subsidy_rate: u32) {
    let mut large_coin = mint_wal(1_000_000, runner.scenario().ctx());
    let admin = runner.admin();

    // Advance to the next transaction to make the system object available in the shared inventory.
    runner.scenario().next_tx(admin);

    // Save the current rewards for the system.
    let pre_usage_rewards;
    runner.tx!(admin, |_, system, _| {
        pre_usage_rewards = system.future_accounting().per_epoch_rewards();
    });

    // Add more usage to the system, buying storage.
    runner.tx!(admin, |_, system, ctx| {
        1u32.range_do!(5, |i| {
            let storage = system.reserve_space(1_000_000, i + 1, &mut large_coin, ctx);
            destroy(storage);
        });
    });

    // Get the per-epoch rewards for the system before processing subsidies again.
    let pre_subsidy_per_epoch_rewards;
    runner.tx!(admin, |_, system, _| {
        pre_subsidy_per_epoch_rewards = system.future_accounting().per_epoch_rewards();
    });

    // Process subsidies, should always add more rewards for the usage-dependent subsidies, but only
    // to the usage-independent subsidy the first time it's called in an epoch.
    runner.tx!(admin, |staking, system, _| {
        let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();
        subsidies.process_subsidies(staking, system, runner.clock());
        test_scenario::return_shared(subsidies);
    });

    // Advance to the next transaction to make the system object available in the shared inventory.
    runner.scenario().next_tx(admin);

    // Get the per-epoch rewards for the system after processing subsidies again.
    let system = runner.scenario().take_shared<System>();
    let post_subsidy_per_epoch_rewards = system.future_accounting().per_epoch_rewards();
    test_scenario::return_shared(system);

    // Check that the usage-dependent subsidies have been applied correctly for the usage since the
    // last invocation.
    post_subsidy_per_epoch_rewards.length().do!(|i| {
        let pre_usage = pre_usage_rewards[i];
        let pre_subsidy = pre_subsidy_per_epoch_rewards[i];
        let post_subsidy = post_subsidy_per_epoch_rewards[i];
        assert_eq!(
            post_subsidy,
            pre_subsidy + ((system_subsidy_rate as u64) * (pre_subsidy - pre_usage) / 10_000),
        );
    });
    destroy(large_coin);
}

#[test]
fun test_variable_subsidy_rates_across_epochs() {
    let admin = @0xA11CE;
    let (mut runner, mut nodes) = e2e_runner::setup_committee_for_epoch_one();
    let admin_cap;

    // Create subsidies with initial rates set to zero.
    runner.tx!(admin, |staking, system, ctx| {
        admin_cap =
            walrus_subsidies::new(
                system,
                staking,
                0,
                0,
                0,
                ctx,
            );
    });

    // Fund the subsidy pool.
    runner.scenario().next_tx(admin);
    let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();
    let funds = mint_wal(10_000_000, runner.scenario().ctx());
    walrus_subsidies::add_coin(&mut subsidies, funds);
    test_scenario::return_shared(subsidies);

    let mut cumulative_base = 0;
    let mut cumulative_per_shard = 0;
    let mut system_subsidy_rate;

    // Advance through multiple epochs with changing subsidy rates
    while (runner.epoch() <= 4) {
        // Update subsidy rates each epoch
        runner.tx!(admin, |_, system, _| {
            let current_epoch = system.epoch();
            let mut subsidies = runner.scenario().take_shared<WalrusSubsidies>();

            // Increase rates each epoch
            system_subsidy_rate = current_epoch * 1_000; // 10%, 20%, 30%, 40%
            let new_base = (current_epoch as u64) * 1000; // 1k, 2k, 3k, 4k
            let new_per_shard = (current_epoch as u64) * 100; // 10, 20, 30, 40

            cumulative_base = cumulative_base + new_base;
            cumulative_per_shard = cumulative_per_shard + new_per_shard;

            subsidies.set_system_subsidy_rate(&admin_cap, system_subsidy_rate);
            subsidies.set_base_subsidy(&admin_cap, new_base);
            subsidies.set_per_shard_subsidy(&admin_cap, new_per_shard);

            test_scenario::return_shared(subsidies);
        });

        // Add usage to the system, apply subsidies and check that the subsidies are applied
        // correctly.
        add_usage_and_check_usage_subsidies(&mut runner, system_subsidy_rate);

        // Check that the usage-independent subsidy has been applied correctly.
        runner.tx!(admin, |staking, _, _| {
            nodes.do_ref!(|node| {
                let node_weight = staking.committee().shards(&node.node_id()).length() as u64;
                assert_eq!(
                    staking.pool_commission(node.node_id()),
                    cumulative_base + cumulative_per_shard * node_weight,
                );
            });
        });

        // Advance to the next epoch.
        runner.next_epoch();
        runner.send_epoch_sync_done_messages(&mut nodes);
    };

    // Clean up
    destroy(admin_cap);
    nodes.destroy!(|node| node.destroy());
    runner.destroy();
}
