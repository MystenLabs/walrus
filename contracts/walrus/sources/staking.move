// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: staking
module walrus::staking;

use sui::{clock::Clock, coin::Coin, dynamic_field as df, sui::SUI};
use walrus::{
    staked_wal::StakedWal,
    staking_inner::StakingInnerV1,
    storage_node::StorageNodeCap,
    system::System
};

#[error]
const ENotImplemented: vector<u8> = b"Function is not implemented";

/// Flag to indicate the version of the Walrus system.
const VERSION: u64 = 0;

/// The one and only staking object.
public struct Staking has key {
    id: UID,
    version: u64,
}

// === Public API: Storage Node ===

/// Creates a staking pool for the candidate, registers the candidate as a storage node.
public fun register_candidate(
    staking: &mut Staking,
    commission_rate: u64,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
    ctx: &mut TxContext,
): StorageNodeCap {
    // use the Pool ID as the identifier of the storage node (?)
    // TODO: circle back on this
    let pool_id = staking
        .inner_mut()
        .create_pool(
            commission_rate,
            storage_price,
            write_price,
            node_capacity,
            ctx,
        );

    let node_cap = staking.inner_mut().register_candidate(pool_id, ctx);
    node_cap
}

/// Blocks staking for the nodes staking pool
/// Marks node as "withdrawing",
/// - excludes it from the next committee selection
/// - still has to remain active while it is part of the committee and until all shards have
///     been transferred to its successor
/// - The staking pool is deleted once the last funds have been withdrawn from it by its stakers
public fun withdraw_node(staking: &mut Staking, cap: &mut StorageNodeCap) {
    staking.inner_mut().set_withdrawing(cap.pool_id());
    staking.inner_mut().withdraw_node(cap);
}

/// Sets next_commission in the staking pool, which will then take effect as commission rate
/// one epoch after setting the value (to allow stakers to react to setting this).
public fun set_next_commission(staking: &mut Staking, cap: &StorageNodeCap, commission_rate: u64) {
    staking.inner_mut().set_next_commission(cap, commission_rate);
}

/// Returns the accumulated commission for the storage node.
public fun collect_commission(staking: &mut Staking, cap: &StorageNodeCap): Coin<SUI> {
    staking.inner_mut().collect_commission(cap)
}

/// TODO: split these into separate functions.
/// Changes the votes for the storage node. Can be called arbitrarily often, if not called, the
/// votes remain the same as in the previous epoch.
public fun vote_for_price_next_epoch(
    staking: &mut Staking,
    cap: &StorageNodeCap,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
) {
    staking.inner_mut().vote_for_next_epoch(cap, storage_price, write_price, node_capacity)
}

/// Ends the voting period and runs the apportionment if the current time allows.
/// Permissionless, can be called by anyone.
/// Emits: `VotingEnd` event.
public fun voting_end(staking: &mut Staking, clock: &Clock) {
    staking.inner_mut().voting_end(clock)
}

/// Initiates the epoch change if the current time allows.
/// Emits: `EpochChangeSync` event.
public fun initiate_epoch_change(staking: &mut Staking, system: &mut System, clock: &Clock) {
    abort ENotImplemented
}

/// Checks if the node should either have received the specified shards from the specified node
/// or vice-versa.
///
/// - also checks that for the provided shards, this function has not been called before
/// - if so, slashes both nodes and emits an event that allows the receiving node to start
///     shard recovery
public fun shard_transfer_failed(
    staking: &mut Staking,
    cap: &StorageNodeCap,
    node_identity: vector<u8>,
    shard_ids: vector<u16>,
) {
    abort ENotImplemented
}

// === Public API: Staking ===

/// Stake `Coin` with the staking pool.
public fun stake_with_pool(
    staking: &mut Staking,
    to_stake: Coin<SUI>,
    pool_id: ID,
    ctx: &mut TxContext,
): StakedWal {
    staking.inner_mut().stake_with_pool(to_stake, pool_id, ctx)
}

/// Marks the amount as a withdrawal to be processed and removes it from the stake weight of the
/// node. Allows the user to call withdraw_stake after the epoch change to the next epoch and
/// shard transfer is done.
public fun request_withdraw_stake(
    staking: &mut Staking,
    staked_wal: &mut StakedWal,
    ctx: &mut TxContext,
) {
    staking.inner_mut().request_withdraw_stake(staked_wal, ctx);
}

#[allow(lint(self_transfer))]
/// Withdraws the staked amount from the staking pool.
public fun withdraw_stake(
    staking: &mut Staking,
    staked_wal: StakedWal,
    ctx: &mut TxContext,
): Coin<SUI> {
    staking.inner_mut().withdraw_stake(staked_wal, ctx)
}

// === Internals ===

/// Get a mutable reference to `StakingInner` from the `Staking`.
fun inner_mut(staking: &mut Staking): &mut StakingInnerV1 {
    assert!(staking.version == VERSION);
    df::borrow_mut(&mut staking.id, VERSION)
}

// === Tests ===

#[test_only]
use walrus::{storage_node, staking_inner};

#[test_only]
use sui::{clock, coin};

#[test_only]
fun new(ctx: &mut TxContext): Staking {
    let mut staking = Staking { id: object::new(ctx), version: VERSION };
    df::add(&mut staking.id, VERSION, staking_inner::new(ctx));
    staking
}

#[test, expected_failure]
fun test_register_candidate() {
    let ctx = &mut tx_context::dummy();
    let cap = new(ctx).register_candidate(0, 0, 0, 0, ctx);
    abort 1337
}

#[test, expected_failure]
fun test_withdraw_node() {
    let ctx = &mut tx_context::dummy();
    let mut staking = new(ctx);
    let mut cap = staking.register_candidate(0, 0, 0, 0, ctx);
    staking.withdraw_node(&mut cap);
    abort 1337
}

#[test, expected_failure]
fun test_set_next_commission() {
    let ctx = &mut tx_context::dummy();
    let mut staking = new(ctx);
    let cap = staking.register_candidate(0, 0, 0, 0, ctx);
    staking.set_next_commission(&cap, 0);
    abort 1337
}

#[test, expected_failure]
fun test_collect_commission() {
    let ctx = &mut tx_context::dummy();
    let cap = storage_node::new_cap_for_testing(new_id(ctx), ctx);
    let coin = new(ctx).collect_commission(&cap);
    abort 1337
}

#[test, expected_failure]
fun test_vote_for_price_next_epoch() {
    let ctx = &mut tx_context::dummy();
    let cap = storage_node::new_cap_for_testing(new_id(ctx), ctx);
    new(ctx).vote_for_price_next_epoch(&cap, 0, 0, 0);
    abort 1337
}

#[test, expected_failure]
fun test_voting_end() {
    let ctx = &mut tx_context::dummy();
    let clock = clock::create_for_testing(ctx);
    new(ctx).voting_end(&clock);
    abort 1337
}

#[test, expected_failure]
fun test_shard_transfer_failed() {
    let ctx = &mut tx_context::dummy();
    let cap = storage_node::new_cap_for_testing(new_id(ctx), ctx);
    new(ctx).shard_transfer_failed(&cap, vector[], vector[]);
    abort 1337
}

#[test, expected_failure]
fun test_stake_with_pool() {
    let ctx = &mut tx_context::dummy();
    let coin = coin::mint_for_testing<SUI>(100, ctx);
    let cap = storage_node::new_cap_for_testing(new_id(ctx), ctx);
    let staked_wal = new(ctx).stake_with_pool(coin, cap.pool_id(), ctx);
    abort 1337
}

#[test_only]
fun new_id(ctx: &mut TxContext): ID {
    ctx.fresh_object_address().to_id()
}
