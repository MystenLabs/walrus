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

    /// Splits the staked WAL into two parts.
    /// The returned staked WAL will have the same `pool_id` as the original staked WAL.
    public fun split(staked_wal: &mut StakedWal, amount: u64, ctx: &mut TxContext): StakedWal {
        assert!(staked_wal.principal >= amount);
        staked_wal.principal = staked_wal.principal - amount;

        StakedWal {
            id: object::new(ctx),
            pool_id: staked_wal.pool_id,
            principal: amount,
        }
    }

    /// Joins the staked WAL with another staked WAL.
    /// Both staked WALs must belong to the same staking pool.
    public fun join(staked_wal: &mut StakedWal, other: StakedWal, ctx: &mut TxContext) {
        let StakedWal { id, pool_id, principal } = other;

        assert!(staked_wal.pool_id == pool_id);
        id.delete();

        staked_wal.principal = staked_wal.principal + principal;
    }

    /// Returns the `pool_id` of the staked WAL.
    public fun pool_id(staked_wal: &StakedWal): ID { staked_wal.pool_id }

    /// Returns the `principal` of the staked WAL. Called `value` to be consistent with `Coin`.
    public fun value(staked_wal: &StakedWal): u64 { staked_wal.principal }
}
