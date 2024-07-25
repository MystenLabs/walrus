// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_use, unused_mut_parameter)]
module walrus::staking_inner {
    use sui::coin::Coin;
    use sui::object_table::{Self, ObjectTable};
    use sui::sui::SUI;
    use sui::clock::Clock;
    use walrus::staking_pool::{Self, StakingPool};
    use walrus::staked_wal::{Self, StakedWal};
    use walrus::storage_node::StorageNodeCap;

    /// TODO: remove this once the module is implemented.
    const ENotImplemented: u64 = 0;

    /// The inner object for the staking part of the system.
    public struct StakingInnerV1 has store {
        /// Stored staking pools, each identified by a unique `ID` and contains
        /// the `StakingPool` object.
        ///
        /// TODO: consider using Object Table
        pools: ObjectTable<ID, StakingPool>,
    }

    public(package) fun new(ctx: &mut TxContext): StakingInnerV1 {
        StakingInnerV1 { pools: object_table::new(ctx) }
    }

    // === Storage Node ===

    /// Creates a new staking pool with the given `commission_rate`.
    public(package) fun create_pool(
        self: &mut StakingInnerV1,
        commission_rate: u64,
        ctx: &mut TxContext,
    ): ID {
        let pool = staking_pool::new(commission_rate, ctx);
        let pool_id = object::id(&pool);
        self.pools.add(pool_id, pool);
        pool_id
    }

    public(package) fun register_candidate(
        self: &mut StakingInnerV1,
        pool_id: ID,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        abort ENotImplemented
    }

    public(package) fun withdraw_node(self: &mut StakingInnerV1, cap: StorageNodeCap) {
        abort ENotImplemented
    }

    public(package) fun set_next_commission(
        self: &mut StakingInnerV1,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun collect_commission(
        self: &mut StakingInnerV1,
        cap: &StorageNodeCap,
    ): Coin<SUI> {
        abort ENotImplemented
    }

    public(package) fun vote_for_next_epoch(
        self: &mut StakingInnerV1,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun voting_end(self: &mut StakingInnerV1, clock: &Clock) {
        abort ENotImplemented
    }

    // === Staking ===

    /// Blocks staking for the pool, marks it as "withdrawing".
    public(package) fun set_withdrawing(self: &mut StakingInnerV1, pool_id: ID) {
        self.pools[pool_id].set_withdrawing();
    }

    /// Destroys the pool if it is empty, after the last stake has been withdrawn.
    public(package) fun destroy_empty_pool(
        self: &mut StakingInnerV1,
        pool_id: ID,
        _ctx: &mut TxContext,
    ) {
        abort ENotImplemented
    }

    /// Stakes the given amount of `T` with the pool.
    public(package) fun stake_with_pool(
        self: &mut StakingInnerV1,
        to_stake: Coin<SUI>,
        pool_id: ID,
        _ctx: &mut TxContext,
    ): StakedWal {
        abort ENotImplemented
    }

    /// Requests withdrawal of the given amount from the staked `T`, the withdraw is
    /// not immediate and will be processed in the next epoch.
    public(package) fun request_withdrawal(
        self: &mut StakingInnerV1,
        staked_wal: &mut StakedWal,
        amount: u64,
    ) {
        abort ENotImplemented
    }

    /// Similar to the `request_withdrawal` but takes the full value of the `StakedWal`.
    public(package) fun withdraw_stake(
        self: &mut StakingInnerV1,
        staked_wal: StakedWal,
        _ctx: &mut TxContext,
    ) {
        abort ENotImplemented
    }
}
