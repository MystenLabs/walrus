// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_use, unused_mut_parameter)]
module walrus::staking_inner;

use sui::{
    clock::Clock,
    coin::Coin,
    object_table::{Self, ObjectTable},
    sui::SUI,
    vec_map::{Self, VecMap}
};
use walrus::{
    staked_wal::{Self, StakedWal},
    staking_pool::{Self, StakingPool},
    storage_node::StorageNodeCap,
    walrus_context::{Self, WalrusContext}
};

// TODO: remove this once the module is implemented.
#[error]
const ENotImplemented: vector<u8> = b"Function is not implemented";

/// The inner object for the staking part of the system.
public struct StakingInnerV1 has store {
    /// Stored staking pools, each identified by a unique `ID` and contains
    /// the `StakingPool` object. Uses `ObjectTable` to make the pool discovery
    /// easier by avoiding wrapping.
    ///
    /// The key is the ID of the staking pool.
    pools: ObjectTable<ID, StakingPool>,
    /// The current epoch of the Walrus system. The epochs are not the same as
    /// the Sui epochs, not to be mistaken with `ctx.epoch()`.
    current_epoch: u64,
    /// The vote for the next epoch. Used to update the `storage_price`,
    /// `write_price` and `storage_capacity`. If the vote is not changed, the
    /// current values are used as a vote for the next epoch.
    ///
    /// TODO: Revisit and consider a Table approach to prevent hitting
    /// the object size limit. Current solution is a temporary workaround.
    voting: VecMap<ID, VotingParams>,
}

#[allow(unused_field)]
public struct VotingParams has store, copy, drop {
    storage_price: u64,
    write_price: u64,
    storage_capacity: u64,
}

public(package) fun new(ctx: &mut TxContext): StakingInnerV1 {
    StakingInnerV1 {
        pools: object_table::new(ctx),
        current_epoch: 0,
        voting: vec_map::empty(),
    }
}

// === Storage Node ===

/// Creates a new staking pool with the given `commission_rate`.
public(package) fun create_pool(
    self: &mut StakingInnerV1,
    commission_rate: u64,
    ctx: &mut TxContext,
): ID {
    let pool = staking_pool::new(
        commission_rate,
        &self.new_walrus_context(),
        ctx,
    );

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

public(package) fun collect_commission(self: &mut StakingInnerV1, cap: &StorageNodeCap): Coin<SUI> {
    abort ENotImplemented
}


public(package) fun set_next_commission(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    commission_rate: u64,
) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.pool_id()].set_next_commission(commission_rate, wctx);
}

/// Sets the parameters for the next epoch.
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
/// TODO: Is this action instant or should it be processed in the next epoch?
public(package) fun set_withdrawing(self: &mut StakingInnerV1, pool_id: ID) {
    let wctx = &self.new_walrus_context();
    self.pools[pool_id].set_withdrawing(wctx);
}

/// Destroys the pool if it is empty, after the last stake has been withdrawn.
public(package) fun destroy_empty_pool(
    self: &mut StakingInnerV1,
    pool_id: ID,
    _ctx: &mut TxContext,
) {
    self.pools.remove(pool_id).destroy_empty()
}

/// Stakes the given amount of `T` with the pool.
public(package) fun stake_with_pool(
    self: &mut StakingInnerV1,
    to_stake: Coin<SUI>,
    pool_id: ID,
    ctx: &mut TxContext,
): StakedWal {
    let wctx = &self.new_walrus_context();
    self.pools[pool_id].stake(to_stake, wctx, ctx)
}

/// Requests withdrawal of the given amount from the staked `T`, the withdraw is
/// not immediate and will be processed in the next epoch.
public(package) fun request_withdrawal(
    self: &mut StakingInnerV1,
    staked_wal: &mut StakedWal,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    let to_withdraw = staked_wal.split(amount, ctx);
    withdraw_stake(self, to_withdraw, ctx)
}

/// Similar to the `request_withdrawal` but takes the full value of the `StakedWal`.
public(package) fun withdraw_stake(
    self: &mut StakingInnerV1,
    staked_wal: StakedWal,
    ctx: &mut TxContext,
): Coin<SUI> {
    let wctx = &self.new_walrus_context();
    self.pools[staked_wal.pool_id()].withdraw_stake(staked_wal, wctx, ctx)
}

/// Get the current epoch.
public(package) fun epoch(self: &StakingInnerV1): u64 {
    self.current_epoch
}

// === System ===

/// Sets the next epoch of the system.
public(package) fun advance_epoch(self: &mut StakingInnerV1, new_epoch: u64, ctx: &mut TxContext) {
    self.current_epoch = new_epoch;
}

fun new_walrus_context(self: &StakingInnerV1): WalrusContext {
    walrus_context::new(self.current_epoch, true)
}
