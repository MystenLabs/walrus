// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_field, unused_mut_parameter)]
/// Module: staking_pool
module walrus::staking_pool {
    use sui::balance::{Self, Balance};
    use sui::coin::Coin;
    use walrus::staked_wal::StakedWal;

    /// TODO: remove this once the module is implemented.
    const ENotImplemented: u64 = 0;

    /// Represents a single staking pool for a token.
    public struct StakingPool<phantom T> has store {
        id: ID,
        active_stake: Balance<T>,
        stake_to_withdraw: Balance<T>,
    }

    /// Create a new `StakingPool` object.
    public(package) fun new<T>(ctx: &mut TxContext): StakingPool<T> {
        StakingPool {
            id: ctx.fresh_object_address().to_id(),
            active_stake: balance::zero(),
            stake_to_withdraw: balance::zero(),
        }
    }

    /// Stake the given amount of WAL in the pool.
    public(package) fun stake<T>(
        pool: &mut StakingPool<T>,
        to_stake: Coin<T>,
        ctx: &mut TxContext,
    ): StakedWal<T> {
        abort ENotImplemented
    }

    /// Withdraw the given amount of WAL from the pool, returning the `Coin<T>`.
    public(package) fun withdraw_stake<T>(
        pool: &mut StakingPool<T>,
        staked_wal: StakedWal<T>,
        ctx: &mut TxContext,
    ): Coin<T> {
        abort ENotImplemented
    }

    public fun id<T>(self: &StakingPool<T>): ID { self.id }
}
