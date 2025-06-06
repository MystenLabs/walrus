// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Module to subsidize the Walrus system and its storage nodes.
module walrus_subsidies::walrus_subsidies_inner;

use sui::{bag::{Self, Bag}, balance::{Self, Balance}};
use wal::wal::WAL;
use walrus::{staking::Staking, system::System};
use walrus_subsidies::epoch_balance::{Self, EpochBalanceRingBuffer};

// Stored as `u128` to make it convenient to use in calculations that may overflow with `u64`s.
/// 100% in basis points.
const HUNDRED_PERCENT: u128 = 10_000;

/// Subsidy rate is in basis points (1/100 of a percent).
const MAX_SUBSIDY_RATE: u32 = 1_000_000; // 10_000%

// Stored as `u128` to make it convenient to use in calculations that may overflow with `u64`s.
/// The number of shards in the system, used to check that no overflow can happen
/// with the subsidy rates for the usage-independent subsidies.
const N_SHARDS: u128 = 1000;

// === Errors ===
/// The provided subsidy rate is invalid.
const EInvalidSubsidyRate: u64 = 0;
/// The subsidy pool does not have sufficient funds.
const EInsufficientFundsInPool: u64 = 1;

// === Structs ===

public struct WalrusSubsidiesInnerV1 has store {
    /// The subsidy rate applied to the price paid to the system and added to the per-epoch rewards
    /// pool of the system. Subsidy rates are expressed in basis points (1/100 of a percent). A
    /// subsidy rate of 100 basis points means a 1% subsidy.
    system_subsidy_rate: u32,
    /// The balance of funds available in the subsidy pool.
    subsidy_pool: Balance<WAL>,
    /// The base subsidy (in FROST) paid directly per storage node per epoch.
    base_subsidy: u64,
    /// The additional subsidy (in FROST) paid to each storage node directly per shard.
    subsidy_per_shard: u64,
    /// The last epoch for which the usage-independent subsidies were paid.
    latest_epoch: u32,
    /// Ring buffer to track how much of the per-epoch balance of the walrus system object has
    /// already had subsidies added.
    already_subsidized_balances: EpochBalanceRingBuffer,
    /// Reserved for future use and migrations.
    extra_fields: Bag,
}

// === Constructor ===

public(package) fun new(
    system: &System,
    staking: &Staking,
    subsidy_rate: u32,
    base_subsidy: u64,
    subsidy_per_shard: u64,
    ctx: &mut TxContext,
): WalrusSubsidiesInnerV1 {
    let future_accounting = system.future_accounting();
    let balances = vector::tabulate!(future_accounting.max_epochs_ahead() as u64, |i| {
        let idx = i as u32;
        future_accounting[idx].rewards()
    });
    let already_subsidized_balances = epoch_balance::ring_new_from_balances(
        system.epoch(),
        balances,
    );
    // We start paying subsidies in the current epoch if we are not in epoch 0, so set the latest
    // epoch to the previous epoch.
    let latest_epoch = if (staking.epoch() == 0) {
        0
    } else {
        staking.epoch() - 1
    };
    WalrusSubsidiesInnerV1 {
        system_subsidy_rate: subsidy_rate,
        subsidy_pool: balance::zero(),
        base_subsidy,
        subsidy_per_shard,
        latest_epoch,
        already_subsidized_balances,
        extra_fields: bag::new(ctx),
    }
}

// === Modifiers ===

/// Add a balance as additional funds to the subsidy pool.
public(package) fun add_balance(self: &mut WalrusSubsidiesInnerV1, funds: Balance<WAL>) {
    self.subsidy_pool.join(funds);
}

/// Set the subsidy rate for the system, in basis points.
///
/// Aborts if new_rate is greater than the max value.
public(package) fun set_system_subsidy_rate(self: &mut WalrusSubsidiesInnerV1, new_rate: u32) {
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.system_subsidy_rate = new_rate;
}

/// Set the base subsidy rate for usage-independent subsidies.
public(package) fun set_base_subsidy(self: &mut WalrusSubsidiesInnerV1, base_subsidy: u64) {
    let max_potential_subsidy =
        (base_subsidy as u128) + (self.subsidy_per_shard as u128) * N_SHARDS;
    assert!(max_potential_subsidy <= std::u64::max_value!() as u128, EInvalidSubsidyRate);
    self.base_subsidy = base_subsidy;
}

/// Set the per-shard subsidy for usage-independent subsidies.
public(package) fun set_per_shard_subsidy(
    self: &mut WalrusSubsidiesInnerV1,
    subsidy_per_shard: u64,
) {
    let max_potential_subsidy =
        (self.base_subsidy as u128) + (subsidy_per_shard as u128) * N_SHARDS;
    assert!(max_potential_subsidy <= std::u64::max_value!() as u128, EInvalidSubsidyRate);
    self.subsidy_per_shard = subsidy_per_shard;
}

// === Functions for processing subsidies ===

/// Processes the subsidies.
/// This will pay the usage-independent subsidies if they have not been paid yet for the current
/// epoch, as well as the usage-based subsidy since the last invocation.
public(package) fun process_subsidies(
    self: &mut WalrusSubsidiesInnerV1,
    staking: &mut Staking,
    system: &mut System,
) {
    self.process_fixed_rate_subsidies(staking);
    self.process_usage_subsidies(system);
}

/// Processes the usage-independent subsidies if they have not been processed in the current epoch
/// yet. Returns `true` if subsidies are paid, `false` if not.
public(package) fun process_fixed_rate_subsidies(
    self: &mut WalrusSubsidiesInnerV1,
    staking: &mut Staking,
): bool {
    // Check if we have paid subsidies for the current epoch already.
    if (self.latest_epoch >= staking.epoch()) {
        return false
    };

    let committee = staking.committee();
    let (node_ids, shards) = committee.to_inner().into_keys_values();
    let subsidies = shards.map!(|shards_per_node| {
        let subsidy_value = self.base_subsidy + shards_per_node.length() * self.subsidy_per_shard;
        assert!(self.subsidy_pool.value() >= subsidy_value, EInsufficientFundsInPool);
        self.subsidy_pool.split(subsidy_value)
    });
    staking.add_commission_to_pools(node_ids, subsidies);
    self.latest_epoch = staking.epoch();
    true
}

/// Processes the usage dependent subsidies for all funds that were added since the last invocation.
/// Adds the subsidies to all future epochs.
public(package) fun process_usage_subsidies(
    self: &mut WalrusSubsidiesInnerV1,
    system: &mut System,
) {
    let already_subsidized = &mut self.already_subsidized_balances;
    let future_accounting = system.future_accounting();
    // Get rid of old epochs that are already over.
    while (already_subsidized[0].epoch() < future_accounting[0].epoch()) {
        already_subsidized.ring_pop_expand();
    };
    // Calculate and collect subsidies.
    let subsidies = vector::tabulate!(future_accounting.max_epochs_ahead() as u64, |i| {
        let idx = i as u32;
        let epoch_rewards = future_accounting[idx].rewards();
        let to_subsidize = (epoch_rewards - already_subsidized[idx].balance()) as u128;
        let subsidy_value = ((to_subsidize * (self.system_subsidy_rate as u128))/HUNDRED_PERCENT);
        assert!(subsidy_value <= (self.subsidy_pool.value() as u128), EInsufficientFundsInPool);
        // Safe to cast down to u64, since it is less than the pool value which is a u64.
        let subsidy = self.subsidy_pool.split(subsidy_value as u64);
        // Update the already subsidized balances.
        already_subsidized[idx].set_balance(epoch_rewards + subsidy.value());
        subsidy
    });

    system.add_per_epoch_subsidies(subsidies);
}

// === Accessors ===

/// Returns the current value of the subsidy pool.
public(package) fun subsidy_pool_balance(self: &WalrusSubsidiesInnerV1): u64 {
    self.subsidy_pool.value()
}

/// Returns the current rate for storage node subsidies.
public(package) fun system_subsidy_rate(self: &WalrusSubsidiesInnerV1): u32 {
    self.system_subsidy_rate
}

/// Returns the current base subsidy.
public(package) fun base_subsidy(self: &WalrusSubsidiesInnerV1): u64 {
    self.base_subsidy
}

/// Returns the current per-shard subsidy.
public(package) fun per_shard_subsidy(self: &WalrusSubsidiesInnerV1): u64 {
    self.subsidy_per_shard
}
