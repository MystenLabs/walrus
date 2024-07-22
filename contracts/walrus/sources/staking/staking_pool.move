// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_field, unused_mut_parameter)]
/// Module: staking_pool
module walrus::staking_pool {
    use sui::balance::{Self, Balance};
    use sui::coin::Coin;
    use sui::sui::SUI;
    use walrus::staked_wal::StakedWal;

    /// TODO: remove this once the module is implemented.
    const ENotImplemented: u64 = 0;

    /// Represents the state of the staking pool.
    public enum PoolState has store, drop {
        Active,
        Withdrawing,
        Empty,
    }

    /// Represents a single staking pool for a token. Even though it is never
    /// transferred or shared, the `key` ability is added for discoverability
    /// in the `ObjectTable`.
    public struct StakingPool has key, store {
        id: UID,
        state: PoolState,
        commission_rate: u64,
        active_stake: Balance<SUI>,
        stake_to_withdraw: Balance<SUI>,
    }

    /// Create a new `StakingPool` object.
    public(package) fun new(commission_rate: u64, ctx: &mut TxContext): StakingPool {
        StakingPool {
            id: object::new(ctx),
            state: PoolState::Empty,
            commission_rate,
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
        ctx: &mut TxContext,
    ): StakedWal {
        assert!(pool.is_active());
        abort ENotImplemented
    }

    /// Withdraw the given amount of WAL from the pool, returning the `Coin`.
    public(package) fun withdraw_stake(
        pool: &mut StakingPool,
        staked_wal: StakedWal,
        ctx: &mut TxContext,
    ): Coin<SUI> {
        assert!(!pool.is_empty());
        abort ENotImplemented
    }

    // === Accessors ===

    /// Returns the amount stored in the `active_stake`.
    public(package) fun active_stake_amount(pool: &StakingPool): u64 {
        pool.active_stake.value()
    }

    /// Returns the amount stored in the `stake_to_withdraw`.
    public(package) fun stake_to_withdraw_amount(pool: &StakingPool): u64 {
        pool.stake_to_withdraw.value()
    }

    /// Returns `true` if the pool is active.
    public(package) fun is_active(pool: &StakingPool): bool {
        match (&pool.state) {
            PoolState::Active => true,
            _ => false,
        }
    }

    /// Returns `true` if the pool is withdrawing.
    public(package) fun is_withdrawing(pool: &StakingPool): bool {
        match (&pool.state) {
            PoolState::Withdrawing => true,
            _ => false,
        }
    }

    /// Returns `true` if the pool is empty.
    public(package) fun is_empty(pool: &StakingPool): bool {
        match (&pool.state) {
            PoolState::Empty => true,
            _ => false,
        }
    }

    #[test]
    fun test_staking_pool() {
        use sui::test_utils::{assert_eq, destroy};

        let ctx = &mut tx_context::dummy();
        let mut pool = new(0, ctx);
        assert_eq(pool.is_empty(), true);
        assert_eq(pool.is_active(), false);
        assert_eq(pool.is_withdrawing(), false);
        assert_eq(pool.active_stake_amount(), 0);
        assert_eq(pool.stake_to_withdraw_amount(), 0);

        pool.set_withdrawing();
        assert_eq(pool.is_empty(), false);
        assert_eq(pool.is_active(), false);
        assert_eq(pool.is_withdrawing(), true);

        destroy(pool);
    }
}
