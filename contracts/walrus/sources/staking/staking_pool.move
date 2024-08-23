// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staking_pool
module walrus::staking_pool;

use std::string::String;
use sui::{balance::{Self, Balance}, coin::Coin, sui::SUI, vec_map::{Self, VecMap}};
use walrus::{
    staked_wal::{Self, StakedWal},
    storage_node::{Self, StorageNodeInfo},
    walrus_context::WalrusContext
};

/// Represents the state of the staking pool.
///
/// TODO: revisit the state machine.
public enum PoolState has store, copy, drop {
    // The pool is new and awaits the stake to be added.
    New,
    // The pool is active and can accept stakes.
    Active,
    // The pool awaits the stake to be withdrawn. The value inside the
    // variant is the epoch in which the pool will be withdrawn.
    Withdrawing(u32),
    // The pool is empty and can be destroyed.
    Withdrawn,
}

/// The parameters for the staking pool. Stored for the next epoch.
public struct PoolParams has store, copy, drop {
    /// Commission rate for the pool.
    commission_rate: u64,
    /// Voting: storage price for the next epoch.
    storage_price: u64,
    /// Voting: write price for the next epoch.
    write_price: u64,
    /// Voting: node capacity for the next epoch.
    node_capacity: u64,
}

/// Represents a single staking pool for a token. Even though it is never
/// transferred or shared, the `key` ability is added for discoverability
/// in the `ObjectTable`.
public struct StakingPool has key, store {
    id: UID,
    /// The current state of the pool.
    state: PoolState,
    /// Current epoch's pool parameters.
    params: PoolParams,
    /// The storage node info for the pool.
    node_info: StorageNodeInfo,
    /// The pool parameters for the next epoch. If `Some`, the pool will be
    /// updated in the next epoch.
    params_next_epoch: Option<PoolParams>,
    /// The epoch when the pool is / will be activated.
    /// Serves information purposes only, the checks are performed in the `state`
    /// property.
    activation_epoch: u32,
    /// Currently
    active_stake: u64,
    /// The amount of stake that will be added to the `active_stake`. Can hold
    /// up to two keys: E+1 and E+2, due to the differences in the activation
    /// epoch.
    ///
    /// ```
    /// E+1 -> Balance
    /// E+2 -> Balance
    /// ```
    ///
    /// Single key is cleared in the `advance_epoch` function, leaving only the
    /// next epoch's stake.
    pending_stake: VecMap<u32, u64>,
    /// The amount of stake that will be withdrawn in the next epoch.
    pending_withdrawal_amount: u64,
    /// The amount of stake that will be withdrawn in the next epoch.
    stake_to_withdraw: Balance<SUI>,
}

/// Create a new `StakingPool` object.
/// If committee is selected, the pool will be activated in the next epoch.
/// Otherwise, it will be activated in the current epoch.
public(package) fun new(
    name: String,
    network_address: String,
    public_key: vector<u8>,
    network_public_key: vector<u8>,
    commission_rate: u64,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): StakingPool {
    let id = object::new(ctx);
    let node_id = id.to_inner();
    let (activation_epoch, state) = if (wctx.committee_selected()) {
        (wctx.epoch() + 1, PoolState::New)
    } else {
        (wctx.epoch(), PoolState::Active)
    };

    StakingPool {
        id,
        state,
        params: PoolParams { commission_rate, storage_price, write_price, node_capacity },
        node_info: storage_node::new(
            name,
            node_id,
            network_address,
            public_key,
            network_public_key,
        ),
        params_next_epoch: option::none(),
        activation_epoch,
        pending_stake: vec_map::empty(),
        active_stake: 0,
        pending_withdrawal_amount: 0,
        stake_to_withdraw: balance::zero(),
    }
}

/// Set the state of the pool to `Withdrawing`.
/// TODO: improve, once committee selection is implemented.
public(package) fun set_withdrawing(pool: &mut StakingPool, wctx: &WalrusContext) {
    assert!(!pool.is_withdrawing());
    pool.state = PoolState::Withdrawing(wctx.epoch() + 1);
}

/// Stake the given amount of WAL in the pool.
public(package) fun stake(
    pool: &mut StakingPool,
    to_stake: Coin<SUI>,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): StakedWal {
    assert!(pool.is_active() || pool.is_new());
    assert!(to_stake.value() > 0);

    let current_epoch = wctx.epoch();
    let activation_epoch = if (wctx.committee_selected()) {
        current_epoch + 2
    } else {
        current_epoch + 1
    };

    let staked_amount = to_stake.value();
    let staked_wal = staked_wal::mint(
        pool.id.to_inner(),
        to_stake.into_balance(),
        activation_epoch,
        ctx,
    );

    // Add the stake to the pending stake either for E+1 or E+2.
    if (!pool.pending_stake.contains(&activation_epoch)) {
        pool.pending_stake.insert(activation_epoch, staked_amount);
    } else {
        let curr = pool.pending_stake[&activation_epoch];
        *&mut pool.pending_stake[&activation_epoch] = curr + staked_amount;
    };

    staked_wal
}

/// Request withdrawal of the given amount from the staked WAL.
/// Marks the `StakedWal` as withdrawing and updates the activation epoch.
///
/// TODO: rewards calculation.
/// TODO: if pool is out and is withdrawing, we can perform the withdrawal immediately
/// TODO: mark stake for withdrawing for the current ctx.sender()
/// TODO: Only if the pool is already withdrawn.
public(package) fun request_withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: &mut StakedWal,
    wctx: &WalrusContext,
    _ctx: &mut TxContext,
) {
    assert!(!pool.is_new());
    assert!(staked_wal.value() > 0);
    assert!(staked_wal.node_id() == pool.id.to_inner());
    assert!(staked_wal.activation_epoch() <= wctx.epoch());

    // depend on the committee selection + whether a node is active / has been active
    staked_wal.set_withdrawing(wctx.epoch() + 1);

    let principal = staked_wal.value();
    pool.pending_withdrawal_amount = pool.pending_withdrawal_amount + principal;
}

/// Perform the withdrawal of the staked WAL, returning the amount to the caller.
public(package) fun withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: StakedWal,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): Coin<SUI> {
    assert!(!pool.is_new());
    assert!(staked_wal.value() > 0);
    assert!(staked_wal.node_id() == pool.id.to_inner());
    assert!(staked_wal.activation_epoch() <= wctx.epoch());
    assert!(staked_wal.is_withdrawing());

    let principal = staked_wal.unwrap();
    pool.pending_withdrawal_amount = pool.pending_withdrawal_amount - principal.value();
    principal.into_coin(ctx)
}

// === Pool parameters ===

/// Sets the next commission rate for the pool.
public(package) fun set_next_commission(
    pool: &mut StakingPool,
    commission_rate: u64,
    _wctx: &WalrusContext,
) {
    if (pool.params_next_epoch.is_none()) {
        pool.params_next_epoch.fill(pool.params);
    };

    pool.params_next_epoch.do_mut!(|params| params.commission_rate = commission_rate);
}

/// Sets the next storage price for the pool.
public(package) fun set_next_storage_price(
    pool: &mut StakingPool,
    storage_price: u64,
    _wctx: &WalrusContext,
) {
    if (pool.params_next_epoch.is_none()) {
        pool.params_next_epoch.fill(pool.params);
    };

    pool.params_next_epoch.do_mut!(|params| params.storage_price = storage_price);
}

/// Sets the next write price for the pool.
public(package) fun set_next_write_price(
    pool: &mut StakingPool,
    write_price: u64,
    _wctx: &WalrusContext,
) {
    if (pool.params_next_epoch.is_none()) {
        pool.params_next_epoch.fill(pool.params);
    };

    pool.params_next_epoch.do_mut!(|params| params.write_price = write_price);
}

/// Sets the next node capacity for the pool.
public(package) fun set_next_node_capacity(
    pool: &mut StakingPool,
    node_capacity: u64,
    _wctx: &WalrusContext,
) {
    if (pool.params_next_epoch.is_none()) {
        pool.params_next_epoch.fill(pool.params);
    };

    pool.params_next_epoch.do_mut!(|params| params.node_capacity = node_capacity);
}

/// Destroy the pool if it is empty.
public(package) fun destroy_empty(pool: StakingPool) {
    assert!(pool.is_empty());

    let StakingPool {
        id,
        pending_stake,
        stake_to_withdraw,
        ..,
    } = pool;

    id.delete();
    stake_to_withdraw.destroy_zero();

    let (_epochs, pending_stakes) = pending_stake.into_keys_values();
    pending_stakes.do!(|stake| assert!(stake == 0));
}

// === Accessors ===

/// Advance epoch for the `StakingPool`.
public(package) fun advance_epoch(pool: &mut StakingPool, wctx: &WalrusContext) {
    // move pending stake to active stake if it is present.
    let current_epoch = wctx.epoch();
    if (pool.pending_stake.contains(&current_epoch)) {
        let (_, stake) = pool.pending_stake.remove(&current_epoch);
        pool.active_stake = pool.active_stake + stake;
    };

    pool.active_stake = pool.active_stake - pool.pending_withdrawal_amount;

    // Update the pool parameters if the activation epoch is the current epoch.
    if (pool.params_next_epoch.is_some()) {
        pool.params = pool.params_next_epoch.extract()
    }
}

/// Assign shards.
public(package) fun assign_shards(pool: &mut StakingPool, shards: vector<u16>) {
    pool.node_info.assign_shards(shards);
}

/// Add shards.
public(package) fun add_shards(pool: &mut StakingPool, shards: vector<u16>) {
    pool.node_info.add_shards(shards);
}

/// Set the state of the pool to `Active`.
public(package) fun set_is_active(pool: &mut StakingPool) {
    assert!(pool.is_new());
    pool.state = PoolState::Active;
}

/// Returns the amount stored in the `active_stake`.
public(package) fun active_stake(pool: &StakingPool): u64 {
    pool.active_stake
}

/// Returns the expected active stake for epoch `E`.
public(package) fun stake_at_epoch(pool: &StakingPool, epoch: u32): u64 {
    let mut expected = pool.active_stake;
    let pending_stake_epochs = pool.pending_stake.keys();
    pending_stake_epochs.do!(
        |e| if (e <= epoch) {
            expected = expected + pool.pending_stake[&e]
        },
    );

    expected
}
// TODO: return pending stake for E+1 and E+2.

/// Returns the amount stored in the `stake_to_withdraw`.
public(package) fun stake_to_withdraw_amount(pool: &StakingPool): u64 {
    pool.stake_to_withdraw.value()
}

/// Returns the commission rate for the pool.
public(package) fun commission_rate(pool: &StakingPool): u64 { pool.params.commission_rate }

/// Returns the storage price for the pool.
public(package) fun storage_price(pool: &StakingPool): u64 { pool.params.storage_price }

/// Returns the write price for the pool.
public(package) fun write_price(pool: &StakingPool): u64 { pool.params.write_price }

/// Returns the node capacity for the pool.
public(package) fun node_capacity(pool: &StakingPool): u64 { pool.params.node_capacity }

/// Returns the activation epoch for the pool.
public(package) fun activation_epoch(pool: &StakingPool): u32 { pool.activation_epoch }

/// Returns the node info for the pool.
public(package) fun node_info(pool: &StakingPool): &StorageNodeInfo { &pool.node_info }

/// Returns `true` if the pool is active.
public(package) fun is_active(pool: &StakingPool): bool {
    pool.state == PoolState::Active
}

/// Returns `true` if the pool is withdrawing.
public(package) fun is_withdrawing(pool: &StakingPool): bool {
    match (pool.state) {
        PoolState::Withdrawing(_) => true,
        _ => false,
    }
}

/// Returns `true` if the pool is empty.
public(package) fun is_new(pool: &StakingPool): bool {
    pool.state == PoolState::New
}

//// Returns `true` if the pool is empty.
public(package) fun is_empty(pool: &StakingPool): bool {
    let pending_stake_epochs = pool.pending_stake.keys();
    let non_empty = pending_stake_epochs.count!(|epoch| pool.pending_stake[epoch] != 0);

    pool.active_stake == 0 && non_empty == 0 && pool.stake_to_withdraw.value() == 0
}
