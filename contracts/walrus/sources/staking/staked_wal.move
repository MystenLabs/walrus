// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staked_wal
module walrus::staked_wal;
/// Represents a staked WAL, does not store the `Balance` inside, but uses
/// `u64` to represent the staked amount. Behaves similarly to `Balance` and
/// `Coin` providing methods to `split` and `join`.
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

/// Burns the staked WAL and returns the `pool_id` and the `principal`.
public(package) fun burn(staked_wal: StakedWal): u64 {
    let StakedWal { id, principal, .. } = staked_wal;
    id.delete();
    principal
}

/// Returns the `pool_id` of the staked WAL.
public fun pool_id(staked_wal: &StakedWal): ID { staked_wal.pool_id }

/// Returns the `principal` of the staked WAL. Called `value` to be consistent
/// with `Coin`.
public fun value(staked_wal: &StakedWal): u64 { staked_wal.principal }

/// Returns the `activation_epoch` of the staked WAL.
public fun activation_epoch(staked_wal: &StakedWal): u64 {
    staked_wal.activation_epoch
}
