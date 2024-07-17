// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staking_pool
module walrus::staking_pool {
    use sui::coinCoin;
    use walrus::staked_walStakedWal;

    /// TODO: remove this once the module is implemented.
    const ENotImplemented: u64 = 0;

    /// Represents a single staking pool for a token.
    public struct StakingPool<T> has key {
        id: UID,
        total_staked: Coin<T>,
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
