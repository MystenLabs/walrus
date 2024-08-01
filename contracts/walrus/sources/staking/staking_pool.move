// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staking_pool
module walrus::staking_pool;

use sui::{balance::{Self, Balance}, coin::Coin, sui::SUI};
use walrus::staked_wal::{Self, StakedWal};

/// Represents the state of the staking pool.
public enum PoolState has store, drop {
    Active,
    Withdrawing,
    New,
}

/// The parameters for the staking pool. Stored for the next epoch.
public struct PoolParams has store, copy, drop {
    /// Commission rate for the pool.
    commission_rate: u64,
}

/// Represents a single staking pool for a token. Even though it is never
/// transferred or shared, the `key` ability is added for discoverability
/// in the `ObjectTable`.
public struct StakingPool has key, store {
    id: UID,
    state: PoolState,
    /// Current epoch's pool parameters.
    params: PoolParams,
    /// The pool parameters for the next epoch. If `Some`, the pool will be
    /// updated in the next epoch.
    params_next_epoch: Option<PoolParams>,
    /// The epoch when the pool is / will be activated.
    /// Serves information purposes only, the checks are performed in the `state`
    /// property.
    activation_epoch: u64,
    /// Currently
    active_stake: Balance<SUI>,
    /// The amount of stake that will be added to the `active_stake` in the next
    /// epoch.
    pending_stake: Balance<SUI>,
    /// The amount of stake that will be withdrawn in the next epoch.
    stake_to_withdraw: Balance<SUI>,
}

/// Create a new `StakingPool` object.
public(package) fun new(
    commission_rate: u64,
    activation_epoch: u64,
    ctx: &mut TxContext,
): StakingPool {
    StakingPool {
        id: object::new(ctx),
        state: PoolState::New,
        params: PoolParams { commission_rate },
        params_next_epoch: option::none(),
        activation_epoch,
        pending_stake: balance::zero(),
        active_stake: balance::zero(),
        stake_to_withdraw: balance::zero(),
    }
}

/// Set the state of the pool to `Withdrawing`.
public(package) fun set_withdrawing(pool: &mut StakingPool) {
    assert!(!pool.is_withdrawing());
    pool.state = PoolState::Withdrawing;
}

/// Stake the given amount of WAL in the pool.
public(package) fun stake(
    pool: &mut StakingPool,
    to_stake: Coin<SUI>,
    current_epoch: u64,
    ctx: &mut TxContext,
): StakedWal {
    assert!(pool.is_active());
    assert!(to_stake.value() > 0);

    let amount = to_stake.value();
    let staked_wal = staked_wal::mint(
        pool.id.to_inner(),
        amount,
        current_epoch + 1, // always the next epoch
        ctx,
    );

    pool.pending_stake.join(to_stake.into_balance());
    staked_wal
}

/// Withdraw the given amount of WAL from the pool + the rewards.
/// TODO: rewards calculation.
public(package) fun withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: StakedWal,
    current_epoch: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    assert!(!pool.is_new());
    assert!(staked_wal.pool_id() == pool.id.to_inner());
    assert!(staked_wal.activation_epoch() <= current_epoch);

    let principal = staked_wal.burn();
    let to_withdraw = pool.active_stake.split(principal);

    // TODO: if pool is out and is withdrawing, we can perform the withdrawal
    //     immediately

    // TODO: mark stake for withdrawing for the current ctx.sender()
    // abort ENotImplemented

    to_withdraw.into_coin(ctx)
}

/// Sets the next commission rate for the pool.
public(package) fun set_next_commission(
    pool: &mut StakingPool,
    commission_rate: u64,
) {
    pool.params_next_epoch.fill(PoolParams { commission_rate });
}

/// Destroy the pool if it is empty.
public(package) fun destroy_empty(pool: StakingPool) {
    assert!(pool.is_empty());
    let StakingPool {
        id,
        pending_stake,
        active_stake,
        stake_to_withdraw,
        ..,
    } = pool;

    id.delete();
    active_stake.destroy_zero();
    pending_stake.destroy_zero();
    stake_to_withdraw.destroy_zero();
}

// === Accessors ===

/// Set the state of the pool to `Active`.
public(package) fun set_is_active(pool: &mut StakingPool) {
    assert!(pool.is_new());
    pool.state = PoolState::Active;
}

/// Returns the amount stored in the `active_stake`.
public(package) fun active_stake_amount(pool: &StakingPool): u64 {
    pool.active_stake.value()
}

/// Returns the pending stake amount.
public(package) fun pending_stake_amount(pool: &StakingPool): u64 {
    pool.pending_stake.value()
}

/// Returns the amount stored in the `stake_to_withdraw`.
public(package) fun stake_to_withdraw_amount(pool: &StakingPool): u64 {
    pool.stake_to_withdraw.value()
}

/// Returns `true` if the pool is active.
public(package) fun is_active(pool: &StakingPool): bool {
    matches!(&pool.state, PoolState::Active)
}

/// Returns `true` if the pool is withdrawing.
public(package) fun is_withdrawing(pool: &StakingPool): bool {
    matches!(&pool.state, PoolState::Withdrawing)
}

/// Returns `true` if the pool is empty.
public(package) fun is_new(pool: &StakingPool): bool {
    matches!(&pool.state, PoolState::New)
}

//// Returns `true` if the pool is empty.
public(package) fun is_empty(pool: &StakingPool): bool {
    pool.active_stake.value() == 0 && pool.pending_stake.value() == 0 &&
    pool.stake_to_withdraw.value() == 0
}

#[allow(unused_variable)]
/// Small helper to match the value. Rust says hi!
macro fun matches<$T>($x: &$T, $p: $T): bool {
    let p = $p;
    match ($x) {
        p => true,
        _ => false,
    }
}
