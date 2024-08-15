// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `staked_wal`
///
/// Implements the `StakedWal` functionality - a staked WAL is an object that
/// represents a staked amount of WALs in a staking pool. It is created in the
/// `staking_pool` on staking and can be split, joined, and burned. The burning
/// is performed via the `withdraw_stake` method in the `staking_pool`.
module walrus::staked_wal;

use sui::{balance::Balance, sui::SUI};

/// The state of the staked WAL.
public enum StakedWalState has store, copy, drop {
    /// Default state of the staked WAL - it is staked in the staking pool.
    Staked,
    /// The staked WAL is in the process of withdrawing. The value inside the
    /// variant is the epoch when the staked WAL can be withdrawn.
    Withdrawing(u64),
}

/// Represents a staked WAL, does not store the `Balance` inside, but uses
/// `u64` to represent the staked amount. Behaves similarly to `Balance` and
/// `Coin` providing methods to `split` and `join`.
///
/// TODO: consider adding a `version` field to the struct to allow public API
///       upgrades in the future.
public struct StakedWal has key, store {
    id: UID,
    /// Whether the staked WAL is active or withdrawing.
    state: StakedWalState,
    /// ID of the staking pool.
    pool_id: ID,
    /// The staked amount.
    principal: Balance<SUI>,
    /// The Walrus epoch when the staked WAL was activated.
    activation_epoch: u64,
}

/// Protected method to create a new staked WAL.
public(package) fun mint(
    pool_id: ID,
    principal: Balance<SUI>,
    activation_epoch: u64,
    ctx: &mut TxContext,
): StakedWal {
    StakedWal {
        id: object::new(ctx),
        state: StakedWalState::Staked,
        pool_id,
        principal,
        activation_epoch,
    }
}

/// Burns the staked WAL and returns the `principal`.
public(package) fun unwrap(sw: StakedWal): Balance<SUI> {
    let StakedWal { id, principal, .. } = sw;
    id.delete();
    principal
}

/// Sets the staked WAL state to `Withdrawing`
public(package) fun set_withdrawing(sw: &mut StakedWal, withdraw_epoch: u64) {
    sw.state = StakedWalState::Withdrawing(withdraw_epoch);
}

// === Accessors ===

/// Returns the `pool_id` of the staked WAL.
public fun pool_id(sw: &StakedWal): ID { sw.pool_id }

/// Returns the `principal` of the staked WAL. Called `value` to be consistent
/// with `Coin`.
public fun value(sw: &StakedWal): u64 { sw.principal.value() }

/// Returns the `activation_epoch` of the staked WAL.
public fun activation_epoch(sw: &StakedWal): u64 { sw.activation_epoch }

/// Returns true if the staked WAL is in the `Staked` state.
public fun is_staked(sw: &StakedWal): bool { sw.state == StakedWalState::Staked }

/// Checks whether the staked WAL is in the `Withdrawing` state.
public fun is_withdrawing(sw: &StakedWal): bool {
    match (sw.state) {
        StakedWalState::Withdrawing(_) => true,
        _ => false,
    }
}

// === Public APIs ===

// TODO: do we want to version them? And should we take precaution measures such
//      as adding a `ctx` parameter for future extensions? What about versioning
//      the staked WAL itself by adding a `version` field into the struct?

/// Joins the staked WAL with another staked WAL, adding the `principal` of the
/// `other` staked WAL to the current staked WAL.
///
/// Aborts if the `pool_id` or `activation_epoch` of the staked WALs do not match.
public fun join(sw: &mut StakedWal, other: StakedWal) {
    let StakedWal { id, state, pool_id, activation_epoch, principal } = other;
    assert!(sw.state == state);
    assert!(sw.pool_id == pool_id);
    assert!(sw.activation_epoch == activation_epoch);
    id.delete();

    sw.principal.join(principal);
}

/// Splits the staked WAL into two parts, one with the `amount` and the other
/// with the remaining `principal`. The `pool_id`, `activation_epoch` are the
/// same for both the staked WALs.
///
/// Aborts if the `amount` is greater than the `principal` of the staked WAL.
public fun split(sw: &mut StakedWal, amount: u64, ctx: &mut TxContext): StakedWal {
    assert!(sw.principal.value() >= amount);

    StakedWal {
        id: object::new(ctx),
        state: sw.state, // state is preserved
        pool_id: sw.pool_id,
        principal: sw.principal.split(amount),
        activation_epoch: sw.activation_epoch,
    }
}

/// Destroys the staked WAL if the `principal` is zero. Ignores the `pool_id`
/// and `activation_epoch` of the staked WAL given that it is zero.
public fun destroy_zero(sw: StakedWal) {
    assert!(sw.principal.value() == 0);
    let StakedWal { id, principal, .. } = sw;
    principal.destroy_zero();
    id.delete();
}
