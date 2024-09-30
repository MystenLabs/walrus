// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staking_pool
module walrus::staking_pool;

use std::string::String;
use sui::{balance::{Self, Balance}, coin::Coin, table::{Self, Table}};
use wal::wal::WAL;
use walrus::{
    pending_values::{Self, PendingValues},
    pool_exchange_rate::{Self, PoolExchangeRate},
    staked_wal::{Self, StakedWal},
    storage_node::{Self, StorageNodeInfo},
    walrus_context::WalrusContext
};

#[error]
const EPoolAlreadyUpdated: vector<u8> = b"Pool already updated for the current epoch";

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
public struct VotingParams has store, copy, drop {
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
    voting_params: VotingParams,
    /// The storage node info for the pool.
    node_info: StorageNodeInfo,
    /// The epoch when the pool is / will be activated.
    /// Serves information purposes only, the checks are performed in the `state`
    /// property.
    activation_epoch: u32,
    /// Epoch when the pool was last updated.
    latest_epoch: u32,
    /// Currently staked WAL in the pool + rewards pool.
    wal_balance: u64,
    /// Balance of the pool token in the pool in the current epoch.
    pool_token_balance: u64,
    /// Balance of the pool token in the pool in the current epoch, E+1 (and E+2).
    pending_pool_token: PendingValues,
    /// The amount of the pool token that will be withdrawn in E+1 or E+2.
    pending_pool_token_withdraw: PendingValues,
    /// The commission rate for the pool.
    /// TODO: allow changing the commission rate in E+2.
    commission_rate: u64,
    /// Historical exchange rates for the pool. The key is the epoch when the
    /// exchange rate was set, and the value is the exchange rate (the ratio of
    /// the amount of WAL tokens for the pool token).
    exchange_rates: Table<u32, PoolExchangeRate>,
    /// The amount of stake that will be added to the `wal_balance`. Can hold
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
    pending_stake: PendingValues,
    /// The amount of stake that will be withdrawn in the next epoch.
    /// Depending on the committee state, the withdrawal can be in E+1 or E+2.
    /// Hence, we need to track withdrawal amounts for both epochs separately.
    pending_withdrawal: PendingValues,
    /// The rewards that the pool has received from being in the committee.
    rewards_pool: Balance<WAL>,
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

    let mut exchange_rates = table::new(ctx);
    exchange_rates.add(activation_epoch, pool_exchange_rate::empty());

    StakingPool {
        id,
        state,
        exchange_rates,
        voting_params: VotingParams {
            storage_price,
            write_price,
            node_capacity,
        },
        node_info: storage_node::new(
            name,
            node_id,
            network_address,
            public_key,
            network_public_key,
        ),
        commission_rate,
        activation_epoch,
        latest_epoch: wctx.epoch(),
        pending_stake: pending_values::empty(),
        pending_withdrawal: pending_values::empty(),
        pending_pool_token: pending_values::empty(),
        pending_pool_token_withdraw: pending_values::empty(),
        wal_balance: 0,
        pool_token_balance: 0,
        rewards_pool: balance::zero(),
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
    to_stake: Coin<WAL>,
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
    let pool_token_amount = pool
        .exchange_rate_at_epoch(current_epoch)
        .get_token_amount(staked_amount);

    let staked_wal = staked_wal::mint(
        pool.id.to_inner(),
        to_stake.into_balance(),
        activation_epoch,
        ctx,
    );

    // Add the stake to the pending stake either for E+1 or E+2.
    pool.pending_stake.insert_or_add(activation_epoch, staked_amount);
    pool.pending_pool_token.insert_or_add(activation_epoch, pool_token_amount);

    staked_wal
}

/// Request withdrawal of the given amount from the staked WAL.
/// Marks the `StakedWal` as withdrawing and updates the activation epoch.
///
/// TODO: rewards calculation.
/// TODO: if pool is out and is withdrawing, we can perform the withdrawal
/// immediately
/// TODO: Only if the pool is already withdrawn.
/// TODO: consider the case of early withdrawal if stake hasn't been activated
/// and committee not selected.
///
/// TODO: remove the return value (currently returns total amount expected)
public(package) fun request_withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: &mut StakedWal,
    wctx: &WalrusContext,
    _ctx: &mut TxContext,
): u64 {
    assert!(!pool.is_new());
    assert!(staked_wal.value() > 0);
    assert!(staked_wal.node_id() == pool.id.to_inner());
    assert!(staked_wal.activation_epoch() <= wctx.epoch());

    // If the node is in the committee, the stake will be withdrawn in E+2,
    // otherwise in E+1.
    // TODO: add a check that the node is in the committee: `node_in_committee &&`
    // let node_in_committee = wctx.committee().contains(pool.id.as_inner());
    let withdraw_epoch = if (wctx.committee_selected()) {
        wctx.epoch() + 2
    } else {
        wctx.epoch() + 1
    };

    let principal_amount = staked_wal.value();
    let token_amount = pool
        .exchange_rate_at_epoch(staked_wal.activation_epoch())
        .get_token_amount(principal_amount);

    dbg!(b"Request Withdraw token amount", token_amount);
    dbg!(b"RW exchange rate", pool.exchange_rate_at_epoch(wctx.epoch()));

    let total_amount = pool.exchange_rate_at_epoch(wctx.epoch()).get_wal_amount(principal_amount);

    // Add the withdrawal to the pending withdrawal either for E+1 or E+2.
    pool.pending_withdrawal.insert_or_add(withdraw_epoch, total_amount);
    pool.pending_pool_token_withdraw.insert_or_add(withdraw_epoch, token_amount);

    staked_wal.set_withdrawing(withdraw_epoch, total_amount);

    total_amount
}

/// Perform the withdrawal of the staked WAL, returning the amount to the caller.
public(package) fun withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: StakedWal,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): Coin<WAL> {
    assert!(!pool.is_new());
    assert!(staked_wal.value() > 0);
    assert!(staked_wal.node_id() == pool.id.to_inner());
    assert!(staked_wal.withdraw_epoch() <= wctx.epoch());
    assert!(staked_wal.activation_epoch() <= wctx.epoch());
    assert!(staked_wal.is_withdrawing());

    let principal_amount = staked_wal.value();
    let token_amount = pool
        .exchange_rate_at_epoch(staked_wal.activation_epoch())
        .get_token_amount(principal_amount);

    dbg!(b"Withdraw token amount", token_amount);
    dbg!(b"Withdraw exchange rate", pool.exchange_rate_at_epoch(wctx.epoch()));

    let withdraw_amount = staked_wal.withdraw_amount();
    let principal = staked_wal.into_balance();
    let rewards_amount = withdraw_amount - principal.value();

    // edge case of empty `StakedWal`.
    if (principal.value() == 0) {
        return principal.into_coin(ctx)
    };

    // TODO: current epoch or withdraw epoch?
    // let total_amount = pool.exchange_rate_at_epoch(wctx.epoch()).get_wal_amount(token_amount);

    // let rewards_amount = if (total_amount >= principal_amount) {
    //     total_amount - principal_amount
    // } else 0;

    // withdraw rewards. due to rounding errors, there's a chance that the
    // rewards amount is higher than the rewards pool, in this case, we
    // withdraw the maximum amount possible.
    // TODO: add an assert that rounding is within the acceptable range.
    let rewards_amount = rewards_amount.min(pool.rewards_pool.value());
    let mut to_withdraw = pool.rewards_pool.split(rewards_amount);
    to_withdraw.join(principal);
    to_withdraw.into_coin(ctx)
}

// === Pool parameters ===

/// Sets the next commission rate for the pool.
/// TODO: implement changing commission rate in E+2, the change should not be
/// immediate.
public(package) fun set_next_commission(
    pool: &mut StakingPool,
    commission_rate: u64,
    _wctx: &WalrusContext,
) {
    pool.commission_rate = commission_rate;
}

/// Sets the next storage price for the pool.
public(package) fun set_next_storage_price(
    pool: &mut StakingPool,
    storage_price: u64,
    _wctx: &WalrusContext,
) {
    pool.voting_params.storage_price = storage_price;
}

/// Sets the next write price for the pool.
public(package) fun set_next_write_price(
    pool: &mut StakingPool,
    write_price: u64,
    _wctx: &WalrusContext,
) {
    pool.voting_params.write_price = write_price;
}

/// Sets the next node capacity for the pool.
public(package) fun set_next_node_capacity(
    pool: &mut StakingPool,
    node_capacity: u64,
    _wctx: &WalrusContext,
) {
    pool.voting_params.node_capacity = node_capacity;
}

/// Destroy the pool if it is empty.
public(package) fun destroy_empty(pool: StakingPool) {
    assert!(pool.is_empty());

    let StakingPool {
        id,
        pending_stake,
        exchange_rates,
        rewards_pool,
        ..,
    } = pool;

    id.delete();
    exchange_rates.drop();
    rewards_pool.destroy_zero();

    let (_epochs, pending_stakes) = pending_stake.unwrap().into_keys_values();
    pending_stakes.do!(|stake| assert!(stake == 0));
}

macro fun dbg<$T: drop>($note: vector<u8>, $value: $T) {
    use std::debug::print;
    let note = $note;
    let value = $value;
    print(&note.to_string());
    print(&value)
}

/// Advance epoch for the `StakingPool`.
public(package) fun advance_epoch(
    pool: &mut StakingPool,
    rewards: Balance<WAL>,
    wctx: &WalrusContext,
) {
    // process the pending and withdrawal amounts
    let current_epoch = wctx.epoch();

    assert!(current_epoch > pool.latest_epoch, EPoolAlreadyUpdated);

    let rewards_amount = rewards.value();
    pool.rewards_pool.join(rewards);
    pool.wal_balance = pool.wal_balance + rewards_amount;
    pool.latest_epoch = current_epoch;

    // === Process the pending stake and withdrawal requests ===

    // dbg!(b"epoch", current_epoch);
    // dbg!(b"wal_balance", pool.wal_balance);
    // dbg!(b"wal_pending +", pool.pending_stake.value_at(current_epoch));
    // dbg!(b"wal_pending -", pool.pending_withdrawal.value_at(current_epoch));

    // do the withdrawals reduction for both
    let pending_withdrawal = pool.pending_withdrawal.flush(current_epoch);
    // rounding
    if (pool.wal_balance >= pending_withdrawal) {
        pool.wal_balance = pool.wal_balance - pending_withdrawal
    } else dbg!(b"Rounding error", pending_withdrawal - pool.wal_balance);

    pool.pool_token_balance =
        pool.pool_token_balance - pool.pending_pool_token_withdraw.flush(current_epoch);

    // do the stake addition, and then

    // really don't like this, but we have a rounding err

    // let exchange_rate = pool_exchange_rate::new(pool.wal_balance, pool.pool_token_balance);
    // pool.wal_balance = pool.wal_balance + pool.pending_stake.flush(current_epoch);
    // pool.pool_token_balance = exchange_rate.get_token_amount(pool.wal_balance);

    // // pool.pool_toekn_balance = poo
    pool.wal_balance = pool.wal_balance + pool.pending_stake.flush(current_epoch);
    pool.pool_token_balance =
        pool.pool_token_balance + pool.pending_pool_token.flush(current_epoch);

    // pool.pool_token_balance =
    //     pool_exchange_rate::new(
    //         pool.wal_balance,
    //         pool.pool_token_balance,
    //     ).get_token_amount(pool.wal_balance);

    // update the pool token balance
    // let wal_amount = pool.wal_balance;
    // let exchange_rate = pool_exchange_rate::new(wal_amount, pool.pool_token_balance);
    // let pool_token_amount = exchange_rate.get_token_amount(wal_amount);

    // pool.wal_balance = pool.wal_balance + rewards_amount;

    pool
        .exchange_rates
        .add(current_epoch, pool_exchange_rate::new(pool.wal_balance, pool.pool_token_balance));
}

/// Set the state of the pool to `Active`.
public(package) fun set_is_active(pool: &mut StakingPool) {
    assert!(pool.is_new());
    pool.state = PoolState::Active;
}

public(package) fun pool_token_at_epoch(pool: &StakingPool, epoch: u32): u64 {
    let mut expected = pool.pool_token_balance;
    expected = expected + pool.pending_pool_token.value_at(epoch);
    expected = expected - pool.pending_pool_token_withdraw.value_at(epoch);
    expected
}

/// Returns the exchange rate for the given epoch. If there isn't a value for
/// the specified epoch, it will iterate over the previous epochs until it finds
/// one with the exchange rate set.
public(package) fun exchange_rate_at_epoch(pool: &StakingPool, mut epoch: u32): PoolExchangeRate {
    let activation_epoch = pool.activation_epoch;
    while (epoch >= activation_epoch) {
        if (pool.exchange_rates.contains(epoch)) {
            return pool.exchange_rates[epoch]
        };
        epoch = epoch - 1;
    };

    pool_exchange_rate::empty()
}

/// Returns the expected active stake for epoch `E` for the pool. It processes
/// the pending stake and withdrawal requests from the current epoch to `E`.
///
/// Should be the main function to calculate the active stake for the pool at
/// the given epoch, due to the complexity of the pending stake and withdrawal
/// requests, and lack of immediate updates.
public(package) fun wal_balance_at_epoch(pool: &StakingPool, epoch: u32): u64 {
    let mut expected = pool.wal_balance;
    expected = expected + pool.pending_stake.value_at(epoch);
    expected = expected - pool.pending_withdrawal.value_at(epoch);
    expected
}

// === Accessors ===

/// Returns the commission rate for the pool.
public(package) fun commission_rate(pool: &StakingPool): u64 { pool.commission_rate }

/// Returns the rewards amount for the pool.
public(package) fun rewards_amount(pool: &StakingPool): u64 { pool.rewards_pool.value() }

/// Returns the rewards for the pool.
public(package) fun wal_balance(pool: &StakingPool): u64 { pool.wal_balance }

/// Returns the storage price for the pool.
public(package) fun storage_price(pool: &StakingPool): u64 { pool.voting_params.storage_price }

/// Returns the write price for the pool.
public(package) fun write_price(pool: &StakingPool): u64 { pool.voting_params.write_price }

/// Returns the node capacity for the pool.
public(package) fun node_capacity(pool: &StakingPool): u64 { pool.voting_params.node_capacity }

/// Returns the activation epoch for the pool.
public(package) fun activation_epoch(pool: &StakingPool): u32 { pool.activation_epoch }

/// Returns the node info for the pool.
public(package) fun node_info(pool: &StakingPool): &StorageNodeInfo { &pool.node_info }

/// Returns `true` if the pool is empty.
public(package) fun is_new(pool: &StakingPool): bool { pool.state == PoolState::New }

/// Returns `true` if the pool is active.
public(package) fun is_active(pool: &StakingPool): bool { pool.state == PoolState::Active }

/// Returns `true` if the pool is withdrawing.
public(package) fun is_withdrawing(pool: &StakingPool): bool {
    match (pool.state) {
        PoolState::Withdrawing(_) => true,
        _ => false,
    }
}

//// Returns `true` if the pool is empty.
public(package) fun is_empty(pool: &StakingPool): bool {
    let pending_stake = pool.pending_stake.unwrap();
    let non_empty = pending_stake.keys().count!(|epoch| pending_stake[epoch] != 0);

    pool.wal_balance == 0 && non_empty == 0
}
