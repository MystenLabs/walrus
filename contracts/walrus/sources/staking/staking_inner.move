// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO:
// 1. registering the node
// 2. adding staked wal to the pool
// 3. selecting the committee
// 4. withdrawing staked wal from the pool
//
// NOTES:
// - advance_epoch - initiates the epoch change
// - initiate epoch change - bumped in `advance_epoch`
// - get "epoch_sync_done" event
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
    committee::{Self, Committee},
    staked_wal::{Self, StakedWal},
    staking_pool::{Self, StakingPool},
    storage_node::{Self, StorageNodeCap, StorageNodeInfo},
    walrus_context::{Self, WalrusContext}
};

/// The minimum amount of staked WAL required to be included in the active set.
const MIN_STAKE: u64 = 0;

// TODO: remove this once the module is implemented.
#[error]
const ENotImplemented: vector<u8> = b"Function is not implemented";

/// The inner object for the staking part of the system.
public struct StakingInnerV1 has store {
    /// The number of shards in the system.
    shards: u16,
    /// Stored staking pools, each identified by a unique `ID` and contains
    /// the `StakingPool` object. Uses `ObjectTable` to make the pool discovery
    /// easier by avoiding wrapping.
    ///
    /// The key is the ID of the staking pool.
    pools: ObjectTable<ID, StakingPool>,
    /// The current epoch of the Walrus system. The epochs are not the same as
    /// the Sui epochs, not to be mistaken with `ctx.epoch()`.
    epoch: u32,
    /// Stores the active set of storage nodes. Provides automatic sorting and
    /// tracks the total amount of staked WAL.
    active_set: ActiveSet,
    /// The next committee in the system.
    next_committee: Option<Committee>,
    /// The current committee in the system.
    committee: Committee,
    /// The previous committee in the system.
    previous_committee: Committee,
}

/// Creates a new `StakingInnerV1` object with default values.
public(package) fun new(shards: u16, ctx: &mut TxContext): StakingInnerV1 {
    StakingInnerV1 {
        shards,
        pools: object_table::new(ctx),
        epoch: 0,
        active_set: active_set::new(shards, MIN_STAKE),
        next_committee: option::none(),
        committee: committee::empty(),
        previous_committee: committee::empty(),
    }
}

// === Staking Pool / Storage Node ===

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

/// Blocks staking for the pool, marks it as "withdrawing".
public(package) fun withdraw_node(self: &mut StakingInnerV1, cap: &mut StorageNodeCap) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_withdrawing(wctx);
}

public(package) fun collect_commission(self: &mut StakingInnerV1, cap: &StorageNodeCap): Coin<SUI> {
    abort ENotImplemented
}

public(package) fun voting_end(self: &mut StakingInnerV1, clock: &Clock) {
    abort ENotImplemented
}

// === Voting ===

/// Sets the next commission rate for the pool.
public(package) fun set_next_commission(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    commission_rate: u64,
) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_next_commission(commission_rate, wctx);
}

/// Sets the next storage price for the pool.
public(package) fun set_next_storage_price(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    storage_price: u64,
) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_next_storage_price(storage_price, wctx);
}

/// Sets the next write price for the pool.
public(package) fun set_next_write_price(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    write_price: u64,
) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_next_write_price(write_price, wctx);
}

/// Sets the next node capacity for the pool.
public(package) fun set_next_node_capacity(
    self: &mut StakingInnerV1,
    cap: &StorageNodeCap,
    node_capacity: u64,
) {
    let wctx = &self.new_walrus_context();
    self.pools[cap.node_id()].set_next_node_capacity(node_capacity, wctx);
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
    let pool = &mut self.pools[node_id];
    let staked_wal = pool.stake(to_stake, wctx, ctx);

    // active set only tracks the stake for the next epoch, pool already knows
    // whether the stake was applied to E+1 or E+2.
    self.active_set.insert(node_id, pool.stake_at_epoch(wctx.epoch() + 1));

    staked_wal
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
public(package) fun epoch(self: &StakingInnerV1): u32 {
    self.epoch
}

/// Get the current committee.
public(package) fun committee(self: &StakingInnerV1): &Committee {
    &self.committee
}

/// Get the previous committee.
public(package) fun previous_committee(self: &StakingInnerV1): &Committee {
    &self.previous_committee
}

/// Check if a node with the given `ID` exists in the staking pools.
public(package) fun has_pool(self: &StakingInnerV1, node_id: ID): bool {
    self.pools.contains(node_id)
}

// === System ===

/// Selects the committee for the next epoch.
public(package) fun select_committee(self: &mut StakingInnerV1, ctx: &mut TxContext) {
    assert!(self.next_committee.is_none());

    let shard_threshold = self.active_set.total_stake() / (self.shards as u64);
    let mut shards_assigned: u16 = 0;

    let mut distribution = vec_map::from_keys_values(
        self.active_set.active_ids(),
        self
            .active_set
            .active_ids()
            .map_ref!(
                |node_id| {
                    let value = (self.active_set[node_id] / shard_threshold) as u16;
                    shards_assigned = shards_assigned + value;
                    value
                },
            ),
    );

    if (shards_assigned < self.shards) {
        let (_first_node, shards) = distribution.get_entry_by_idx_mut(0);
        *shards = *shards + (self.shards - shards_assigned);
    };

    // if we're dealing with the first epoch, we need to assign the shards to the
    // nodes in a sequential manner. Assuming there's at least 1 node in the set.
    let committee = if (self.committee.size() == 0) {
        committee::initialize(distribution)
    } else {
        self.committee.transition(distribution)
    };

    self.next_committee = option::some(committee);
}

/// Sets the next epoch of the system.
///
/// TODO: add rewards argument and perform the reward distribution.
/// TODO: `advance_epoch` needs to be either pre or post handled by each staking pool as well.
/// TODO: current solution is silly, we need to have a proper algorithm for shard assignment.
public(package) fun advance_epoch(self: &mut StakingInnerV1, ctx: &mut TxContext) {
    assert!(self.next_committee.is_some());

    self.epoch = self.epoch + 1;
    self.previous_committee = self.committee;
    self.committee = self.next_committee.extract(); // overwrites the current committee

    let wctx = &self.new_walrus_context();

    self.committee.inner().keys().do_ref!(|node| {
        self.pools[*node].advance_epoch(wctx);
        self.active_set.update(*node, self.pools[*node].stake_at_epoch(wctx.epoch() + 1));
    });
}

// === Internal ===

fun new_walrus_context(self: &StakingInnerV1): WalrusContext {
    walrus_context::new(
        self.epoch,
        self.next_committee.is_some(),
        self.committee.to_inner(),
    )
}

// ==== Tests ===

#[test_only]
public(package) fun active_set(self: &mut StakingInnerV1): &mut ActiveSet {
    &mut self.active_set
}

#[test_only]
#[syntax(index)]
/// Get the pool with the given `ID`.
public(package) fun borrow(self: &StakingInnerV1, node_id: ID): &StakingPool {
    &self.pools[node_id]
}

#[test_only]
#[syntax(index)]
/// Get mutable reference to the pool with the given `ID`.
public(package) fun borrow_mut(self: &mut StakingInnerV1, node_id: ID): &mut StakingPool {
    &mut self.pools[node_id]
}
