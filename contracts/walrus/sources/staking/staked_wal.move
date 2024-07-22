// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable)]
/// Module: staked_wal
module walrus::staked_wal {
    /// Represents a staked WAL, does not store the `Balance` inside, but uses
    /// `u64` to represent the staked amount. Behaves similarly to `Balance` and
    /// `Coin` providing methods to `split` and `join`.
    public struct StakedWal has key, store {
        id: UID,
        /// ID of the staking pool.
        pool_id: ID,
        /// The staked amount.
        principal: u64,
        // TODO: epoch / activation_epoch
    }

    /// Protected method to create a new staked WAL.
    public(package) fun mint(pool_id: ID, principal: u64, ctx: &mut TxContext): StakedWal {
        StakedWal {
            id: object::new(ctx),
            pool_id,
            principal,
        }
    }

    /// Burns the staked WAL and returns the `pool_id` and the `principal`.
    public(package) fun burn(staked_wal: StakedWal): (ID, u64) {
        let StakedWal { id, pool_id, principal } = staked_wal;
        id.delete();
        (pool_id, principal)
    }

    /// TODO: cycle back on this
    public fun split(_self: &mut StakedWal): StakedWal { abort 0 }

    /// Ditto.
    public fun join(_self: &mut StakedWal, _other: StakedWal) { abort 0 }

    /// Returns the `pool_id` of the staked WAL.
    public fun pool_id(staked_wal: &StakedWal): ID { staked_wal.pool_id }

    /// Returns the `principal` of the staked WAL. Called `value` to be consistent with `Coin`.
    public fun value(staked_wal: &StakedWal): u64 { staked_wal.principal }
}
