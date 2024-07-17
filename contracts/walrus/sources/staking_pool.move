// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_field)]
/// Module: staking_pool
module walrus::staking_pool {
    use sui::balance::{Self, Balance};
    use sui::coin::Coin;
    use walrus::staked_wal::StakedWal;

    /// TODO: remove this once the module is implemented.
    const ENotImplemented: u64 = 0;

    /// Represents a single staking pool for a token.
    public struct StakingPool<phantom T> has key {
        id: UID,
        total_staked: Balance<T>,
    }

    /// Create a new `StakingPool` object.
    public(package) fun new<T>(ctx: &mut TxContext): StakingPool<T> {
        StakingPool {
            id: object::new(ctx),
            total_staked: balance::zero(),
        }
    }

    #[allow(lint(share_owned))]
    /// Share the `StakingPool` object.
    public(package) fun share<T>(pool: StakingPool<T>) {
        transfer::share_object(pool)
    }

    /// Stake the given amount of WAL in the pool.
    public fun stake<T>(
        pool: &mut StakingPool<T>,
        to_stake: Coin<T>,
        ctx: &mut TxContext,
    ): StakedWal {
        abort ENotImplemented
    }

    /// Withdraw the given amount of WAL from the pool, returning the `Coin<T>`.
    public fun withdraw_stake<T>(
        pool: &mut StakingPool<T>,
        staked_wal: StakedWal,
        ctx: &mut TxContext,
    ): Coin<T> {
        abort ENotImplemented
    }
}
