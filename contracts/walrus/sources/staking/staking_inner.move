// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_use, unused_mut_parameter)]
module walrus::staking_inner;

use std::string::String;
use sui::{
    clock::Clock,
    coin::Coin,
    object_table::{Self, ObjectTable},
    sui::SUI,
    vec_map::{Self, VecMap}
};
use walrus::{
    active_set::{Self, ActiveSet},
    bls_aggregate::{Self, BlsCommittee},
    staked_wal::{Self, StakedWal},
    staking_pool::{Self, StakingPool},
    storage_node::{Self, StorageNodeCap, StorageNodeInfo},
    walrus_context::{Self, WalrusContext}
};

/// The number of shards in the system.
const SHARDS: u16 = 1000;
/// The minimum amount of staked WAL required to be included in the active set.
const MIN_STAKE: u64 = 0;

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
    /// Stores the active set of storage nodes. Provides automatic sorting and
    /// tracks the total amount of staked WAL.
    active_set: ActiveSet,
    /// The current committee in the system.
    committee: BlsCommittee,
}

public(package) fun new(ctx: &mut TxContext): StakingInnerV1 {
    StakingInnerV1 {
        pools: object_table::new(ctx),
        current_epoch: 0,
        active_set: active_set::new(SHARDS, MIN_STAKE),
        committee: bls_aggregate::new_bls_committee(0, &vector[]),
    }
}

// === Storage Node ===

/// Creates a new staking pool with the given `commission_rate`.
public(package) fun create_pool(
    self: &mut StakingInnerV1,
    name: String,
    network_address: String,
    public_key: vector<u8>,
    network_public_key: vector<u8>,
    commission_rate: u64,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
    ctx: &mut TxContext,
): ID {
    let pool = staking_pool::new(
        name,
        network_address,
        public_key,
        network_public_key,
        commission_rate,
        storage_price,
        write_price,
        node_capacity,
        &self.new_walrus_context(),
        ctx,
    );

    let node_id = object::id(&pool);
    self.pools.add(node_id, pool);
    node_id
}

public(package) fun register_candidate(
    self: &mut StakingInnerV1,
    node_id: ID,
    ctx: &mut TxContext,
): StorageNodeCap {
    abort ENotImplemented
}

public(package) fun withdraw_node(self: &mut StakingInnerV1, cap: &mut StorageNodeCap) {
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
    self.pools[cap.node_id()].set_next_commission(commission_rate, wctx);
}

/// Sets the parameters for the next epoch.
public(package) fun vote_for_next_epoch(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
) {
    abort ENotImplemented
}

public(package) fun voting_end(self: &mut StakingInnerV1, clock: &Clock) {
    abort ENotImplemented
}

// === Staking ===

/// Blocks staking for the pool, marks it as "withdrawing".
/// TODO: Is this action instant or should it be processed in the next epoch?
public(package) fun set_withdrawing(self: &mut StakingInnerV1, node_id: ID) {
    let wctx = &self.new_walrus_context();
    self.pools[node_id].set_withdrawing(wctx);
}

/// Destroys the pool if it is empty, after the last stake has been withdrawn.
public(package) fun destroy_empty_pool(
    self: &mut StakingInnerV1,
    node_id: ID,
    _ctx: &mut TxContext,
) {
    self.pools.remove(node_id).destroy_empty()
}

/// Stakes the given amount of `T` with the pool, returning the `StakedWal`.
public(package) fun stake_with_pool(
    self: &mut StakingInnerV1,
    to_stake: Coin<SUI>,
    node_id: ID,
    ctx: &mut TxContext,
): StakedWal {
    let wctx = &self.new_walrus_context();
    self.pools[node_id].stake(to_stake, wctx, ctx)
}

/// Requests withdrawal of the given amount from the `StakedWAL`, marking it as
/// `Withdrawing`. Once the epoch is greater than the `withdraw_epoch`, the
/// withdrawal can be performed.
public(package) fun request_withdraw_stake(
    self: &mut StakingInnerV1,
    staked_wal: &mut StakedWal,
    ctx: &mut TxContext,
) {
    let wctx = &self.new_walrus_context();
    self.pools[staked_wal.node_id()].request_withdraw_stake(staked_wal, wctx, ctx)
}

/// Perform the withdrawal of the staked WAL, returning the amount to the caller.
/// The `StakedWal` must be in the `Withdrawing` state, and the epoch must be
/// greater than the `withdraw_epoch`.
public(package) fun withdraw_stake(
    self: &mut StakingInnerV1,
    staked_wal: StakedWal,
    ctx: &mut TxContext,
): Coin<SUI> {
    let wctx = &self.new_walrus_context();
    self.pools[staked_wal.node_id()].withdraw_stake(staked_wal, wctx, ctx)
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
