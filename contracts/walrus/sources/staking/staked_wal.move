// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `staked_wal`
///
/// Implements the `StakedWal` functionality - a staked WAL is an object that
/// represents a staked amount of WALs in a staking pool. It is created in the
/// `staking_pool` on staking and can be split, joined, and burned. The burning
/// is performed via the `withdraw_stake` method in the `staking_pool`.
module walrus::staked_wal;

/// Represents a staked WAL, does not store the `Balance` inside, but uses
/// `u64` to represent the staked amount. Behaves similarly to `Balance` and
/// `Coin` providing methods to `split` and `join`.
///
/// TODO: consider adding a `version` field to the struct to allow public API
///       upgrades in the future.
public struct StakedWal has key, store {
    id: UID,
    /// ID of the staking pool.
    pool_id: ID,
    /// The staked amount.
    principal: u64,
    /// The Walrus epoch when the staked WAL was activated.
    activation_epoch: u64,
}

/// Protected method to create a new staked WAL.
public(package) fun mint(
    pool_id: ID,
    principal: u64,
    activation_epoch: u64,
    ctx: &mut TxContext,
): StakedWal {
    StakedWal {
        id: object::new(ctx),
        pool_id,
        principal,
        activation_epoch,
    }
}

/// Burns the staked WAL and returns the `principal`.
public(package) fun burn(sw: StakedWal): u64 {
    let StakedWal { id, principal, .. } = sw;
    id.delete();
    principal
}

// === Accessors ===

/// Returns the `pool_id` of the staked WAL.
public fun pool_id(sw: &StakedWal): ID { sw.pool_id }

/// Returns the `principal` of the staked WAL. Called `value` to be consistent
/// with `Coin`.
public fun value(sw: &StakedWal): u64 { sw.principal }

/// Returns the `activation_epoch` of the staked WAL.
public fun activation_epoch(sw: &StakedWal): u64 { sw.activation_epoch }

// === Public APIs ===

// TODO: do we want to version them? And should we take precaution measures such
//      as adding a `ctx` parameter for future extensions? What about versioning
//      the staked WAL itself by adding a `version` field into the struct?

/// Joins the staked WAL with another staked WAL, adding the `principal` of the
/// `other` staked WAL to the current staked WAL.
///
/// Aborts if the `pool_id` or `activation_epoch` of the staked WALs do not match.
public fun join(sw: &mut StakedWal, other: StakedWal) {
    let StakedWal { id, pool_id, activation_epoch, principal } = other;
    assert!(sw.pool_id == pool_id);
    assert!(sw.activation_epoch == activation_epoch);
    id.delete();

    sw.principal = sw.principal + principal;
}

/// Splits the staked WAL into two parts, one with the `amount` and the other
/// with the remaining `principal`. The `pool_id`, `activation_epoch` are the
/// same for both the staked WALs.
///
/// Aborts if the `amount` is greater than the `principal` of the staked WAL.
public fun split(sw: &mut StakedWal, amount: u64, ctx: &mut TxContext): StakedWal {
    assert!(sw.principal >= amount);
    sw.principal = sw.principal - amount;

    StakedWal {
        id: object::new(ctx),
        pool_id: sw.pool_id,
        principal: amount,
        activation_epoch: sw.activation_epoch,
    }
}

/// Destroys the staked WAL if the `principal` is zero. Ignores the `pool_id`
/// and `activation_epoch` of the staked WAL given that it is zero.
public fun destroy_zero(sw: StakedWal) {
    assert!(sw.principal == 0);
    let StakedWal { id, .. } = sw;
    id.delete();
}
