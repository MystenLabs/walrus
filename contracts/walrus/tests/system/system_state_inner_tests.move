// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::system_state_inner_tests;

use sui::{
    clock,
    test_utils::destroy,
};
use walrus::{
    system_state_inner,
    test_utils::mint,
    storage_accounting,
};

#[test]
fun test_add_rewards_zero_rewards() {
    let ctx = &mut tx_context::dummy();
    let mut system = system_state_inner::new_for_testing();
    let epochs_ahead = 2;
    let rewards = 0;
    let reward_per_epoch = rewards / (epochs_ahead as u64);

    // Test adding rewards 0 WAL for 2 epochs ahead.
    let subsidy = mint(0, ctx);
    system.add_rewards(subsidy, epochs_ahead);

    // Check rewards for the epochs ahead
    0u32.range_do!(epochs_ahead, |i| {
        assert!(
          storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), i)).value() == reward_per_epoch
      )
    });
      
    destroy(system);
}

#[test]
fun test_add_rewards_one_epoch_ahead() {
    let ctx = &mut tx_context::dummy();
    let mut system = system_state_inner::new_for_testing();
    let epochs_ahead = 1;
    let rewards = 1000u64;
    let reward_per_epoch = rewards / (epochs_ahead as u64);

    // Test adding rewards 1,000 WAL for 1 epoch ahead.
    let subsidy = mint(rewards, ctx);
    system.add_rewards(subsidy, epochs_ahead);

    // Check rewards for the epochs ahead
    0u32.range_do!(epochs_ahead, |i| {
        assert!(
          storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), i)).value() == reward_per_epoch
      )
    });
      
    destroy(system);
}

#[test]
fun test_add_rewards_multiple_epochs_ahead() {
    let ctx = &mut tx_context::dummy();
    let mut system = system_state_inner::new_for_testing();
    let rewards = 1000u64;
    let epochs_ahead = 4;
    let reward_per_epoch = rewards / (epochs_ahead as u64);

    // Test adding rewards 1,000 WAL for 4 epochs ahead.
    let subsidy = mint(rewards, ctx);
    system.add_rewards(subsidy, epochs_ahead);

    // Check rewards for the epochs ahead
    0u32.range_do!(epochs_ahead, |i| {
        assert!(
          storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), i)).value() == reward_per_epoch
      )
    });
      
    destroy(system);
}

#[test]
fun test_add_rewards_uneven_distribution() {
    let ctx = &mut tx_context::dummy();
    let mut system = system_state_inner::new_for_testing();
    let rewards = 1001u64;
    let epochs_ahead = 3;
    let reward_per_epoch = rewards / (epochs_ahead as u64);

    // Test adding rewards 1,001 WAL for 3 epochs ahead.
    let subsidy = mint(rewards, ctx);
    system.add_rewards(subsidy, epochs_ahead);

    // Check rewards for the epochs ahead
    // The first epoch should get 2 more rewards than the others. They are the leftover_rewards.
    let first_epoch_rewards = reward_per_epoch + 2;
    assert!(
        storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), 0)).value() == first_epoch_rewards
    );
    assert!(
        storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), 1)).value() == reward_per_epoch
    );
    assert!(
        storage_accounting::rewards_balance(storage_accounting::ring_lookup_mut(system.get_future_accounting(), 2)).value() == reward_per_epoch
    );
    destroy(system);
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun test_add_rewards_zero_epochs_ahead_fail() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    let mut system = system_state_inner::new_for_testing();

    let subsidy = mint(1000, ctx);

    // Test adding rewards for 0 epochs ahead (should fail)
    system.add_rewards(subsidy, 0);

    destroy(system);
    clock.destroy_for_testing();
}
