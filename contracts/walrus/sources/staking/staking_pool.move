// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: staking_pool
module walrus::staking_pool;

use std::string::String;
use sui::{balance::{Self, Balance}, table::{Self, Table}};
use wal::wal::WAL;
use walrus::{
    messages,
    pending_values::{Self, PendingValues},
    pool_exchange_rate::{Self, PoolExchangeRate},
    staked_wal::{Self, StakedWal},
    storage_node::{Self, StorageNodeInfo},
    walrus_context::WalrusContext
};

// Keep errors in `walrus-sui/types/move_errors.rs` up to date with changes here.
const EPoolAlreadyUpdated: u64 = 0;
const ECalculationError: u64 = 1;
const EIncorrectEpochAdvance: u64 = 2;
/// Trying to destroy a non-empty pool.
const EPoolNotEmpty: u64 = 3;
/// Invalid proof of possession during the pool creation.
const EInvalidProofOfPossession: u64 = 4;
/// Trying to set the pool to withdrawing state when it is already withdrawing.
const EPoolAlreadyWithdrawing: u64 = 5;
/// Pool is not in `New` or `Active` state.
const EPoolIsNotActive: u64 = 6;
/// Trying to stake zero amount.
const EZeroStake: u64 = 7;
// abort_code=8 is available
/// Trying to withdraw stake from the incorrect pool.
const EIncorrectPoolId: u64 = 9;
/// Trying to withdraw active stake.
const ENotWithdrawing: u64 = 10;
/// Attempt to withdraw before `withdraw_epoch`.
const EWithdrawEpochNotReached: u64 = 11;
/// Attempt to withdraw before `activation_epoch`.
const EActivationEpochNotReached: u64 = 12;
/// Requesting withdrawal for the stake that can be withdrawn directly.
const EWithdrawDirectly: u64 = 13;

/// Represents the state of the staking pool.
///
/// TODO: revisit the state machine.
public enum PoolState has store, copy, drop {
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
    /// The amount of the pool token that will be withdrawn in E+1 or E+2.
    /// We use this amount to calculate the WAL withdrawal in the
    /// `process_pending_stake`.
    pending_pool_token_withdraw: PendingValues,
    /// The amount of the stake requested for early withdrawal. Differs from the
    /// `pending_pool_token_withdraw` as it stored principals of not yet active
    /// stakes. Token amount for these principals is calculated via the exchange
    /// rate at the activation epoch.
    pending_early_withdrawals: PendingValues,
    /// The commission rate for the pool.
    /// TODO: allow changing the commission rate in E+2.
    commission_rate: u16,
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
    proof_of_possession: vector<u8>,
    commission_rate: u16,
    storage_price: u64,
    write_price: u64,
    node_capacity: u64,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): StakingPool {
    let id = object::new(ctx);
    let node_id = id.to_inner();

    // Verify proof of possession
    assert!(
        messages::new_proof_of_possession_msg(
            wctx.epoch(),
            ctx.sender(),
            public_key,
        ).verify_proof_of_possession(proof_of_possession),
        // Invalid proof of possession in the `new` function.
        EInvalidProofOfPossession,
    );

    let activation_epoch = if (wctx.committee_selected()) wctx.epoch() + 1 else wctx.epoch();
    let mut exchange_rates = table::new(ctx);
    exchange_rates.add(activation_epoch, pool_exchange_rate::empty());

    StakingPool {
        id,
        state: PoolState::Active,
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
        pending_pool_token_withdraw: pending_values::empty(),
        pending_early_withdrawals: pending_values::empty(),
        wal_balance: 0,
        pool_token_balance: 0,
        rewards_pool: balance::zero(),
    }
}

/// Set the state of the pool to `Withdrawing`.
/// TODO: improve, once committee selection is implemented.
public(package) fun set_withdrawing(pool: &mut StakingPool, wctx: &WalrusContext) {
    assert!(!pool.is_withdrawing(), EPoolAlreadyWithdrawing);
    pool.state = PoolState::Withdrawing(wctx.epoch() + 1);
}

/// Stake the given amount of WAL in the pool.
public(package) fun stake(
    pool: &mut StakingPool,
    to_stake: Balance<WAL>,
    wctx: &WalrusContext,
    ctx: &mut TxContext,
): StakedWal {
    assert!(pool.is_active(), EPoolIsNotActive);
    assert!(to_stake.value() > 0, EZeroStake);

    let current_epoch = wctx.epoch();
    let activation_epoch = if (wctx.committee_selected()) {
        current_epoch + 2
    } else {
        current_epoch + 1
    };

    let staked_amount = to_stake.value();
    let staked_wal = staked_wal::mint(
        pool.id.to_inner(),
        to_stake,
        activation_epoch,
        ctx,
    );

    // Add the stake to the pending stake either for E+1 or E+2.
    pool.pending_stake.insert_or_add(activation_epoch, staked_amount);
    staked_wal
}

/// Request withdrawal of the given amount from the staked WAL.
/// Marks the `StakedWal` as withdrawing and updates the activation epoch.
///
/// TODO: if pool is out and is withdrawing, we can perform the withdrawal
/// immediately
/// TODO: consider the case of early withdrawal if stake hasn't been activated
/// and committee not selected.
///
/// TODO: remove the return value (currently returns total amount expected)
public(package) fun request_withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: &mut StakedWal,
    wctx: &WalrusContext,
) {
    assert!(staked_wal.value() > 0);
    assert!(staked_wal.node_id() == pool.id.to_inner());
    assert!(staked_wal.is_staked());

    // early withdrawal request: only possible if activation epoch has not been
    // reached, and the stake is already counted for the next committee selection
    if (staked_wal.activation_epoch() > wctx.epoch()) {
        // only allow requesting if the stake cannot be withdrawn directly
        assert!(!staked_wal.can_withdraw_early(wctx), EWithdrawDirectly);

        let withdraw_epoch = staked_wal.activation_epoch() + 1;
        // register principal in the early withdrawals, the value will get converted to
        // the token amount in the `process_pending_stake` function
        pool.pending_early_withdrawals.insert_or_add(withdraw_epoch, staked_wal.value());
        staked_wal.set_withdrawing(withdraw_epoch, option::none());
        return
    };

    assert!(staked_wal.activation_epoch() <= wctx.epoch(), EActivationEpochNotReached);

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

    pool.pending_pool_token_withdraw.insert_or_add(withdraw_epoch, token_amount);
    staked_wal.set_withdrawing(withdraw_epoch, option::some(token_amount));
}

/// Perform the withdrawal of the staked WAL, returning the amount to the caller.
public(package) fun withdraw_stake(
    pool: &mut StakingPool,
    staked_wal: StakedWal,
    wctx: &WalrusContext,
): Balance<WAL> {
    assert!(staked_wal.value() > 0, EZeroStake);
    assert!(staked_wal.node_id() == pool.id.to_inner(), EIncorrectPoolId);

    let activation_epoch = staked_wal.activation_epoch();

    // early withdrawal in the case when committee before activation epoch hasn't
    // been selected. covers both E+1 and E+2 cases.
    if (staked_wal.can_withdraw_early(wctx)) {
        pool.pending_stake.reduce(activation_epoch, staked_wal.value());
        return staked_wal.into_balance()
    };

    assert!(staked_wal.is_withdrawing(), ENotWithdrawing);
    assert!(staked_wal.withdraw_epoch() <= wctx.epoch(), EWithdrawEpochNotReached);
    assert!(staked_wal.activation_epoch() <= wctx.epoch(), EActivationEpochNotReached);

    // token amount is either set in the `StakedWal` or, in case of the early
    // withdrawal, is calculated from the principal amount and the exchange rate
    // at the activation epoch.
    //
    // note: macro `destroy_or!` is not evaluated if the value is `Some`
    let token_amount = staked_wal
        .pool_token_amount()
        .destroy_or!(
            pool.exchange_rate_at_epoch(activation_epoch).get_token_amount(staked_wal.value()),
        );

    let withdraw_epoch = staked_wal.withdraw_epoch();

    // calculate the total amount to withdraw by converting token amount via the exchange rate
    let total_amount = pool.exchange_rate_at_epoch(withdraw_epoch).get_wal_amount(token_amount);
    let principal = staked_wal.into_balance();
    let rewards_amount = if (total_amount >= principal.value()) {
        total_amount - principal.value()
    } else 0;

    // withdraw rewards. due to rounding errors, there's a chance that the
    // rewards amount is higher than the rewards pool, in this case, we
    // withdraw the maximum amount possible
    let rewards_amount = rewards_amount.min(pool.rewards_pool.value());
    let mut to_withdraw = pool.rewards_pool.split(rewards_amount);
    to_withdraw.join(principal);
    to_withdraw
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
    assert!(rewards.value() == 0 || pool.wal_balance > 0, EIncorrectEpochAdvance);

    // if rewards are calculated only for full epochs,
    // rewards addition should happen prior to pool token calculation
    let rewards_amount = rewards.value();
    pool.rewards_pool.join(rewards);
    pool.wal_balance = pool.wal_balance + rewards_amount;
    pool.latest_epoch = current_epoch;
    pool.node_info.rotate_public_key();

    // perform stake deduction / addition for the current epoch - wctx.epoch()
    pool.process_pending_stake(wctx)
}

/// Process the pending stake and withdrawal requests for the pool. Called in the
/// `advance_epoch` function in case the pool is in the committee and receives the
/// rewards. And may be called in user-facing functions to update the pool state,
/// if the pool is not in the committee.
public(package) fun process_pending_stake(pool: &mut StakingPool, wctx: &WalrusContext) {
    let current_epoch = wctx.epoch();

    // Get the exchange rate to use for all conversions and store it for future use.
    // each value in pending withdrawals contains the principal which became
    // active in the previous epoch. so unlike other pending values, we need to
    // flush it one by one, recalculating the exchange rate and pool token amount
    // for each early withdrawal epoch.
    let mut early_token_withdraw = 0;
    let mut pending_early_withdrawals = pool.pending_early_withdrawals.unwrap();
    pending_early_withdrawals.keys().do!(|epoch| if (epoch <= current_epoch) {
        let (_, epoch_value) = pending_early_withdrawals.remove(&epoch);
        let token_value_for_epoch = pool
            .exchange_rate_at_epoch(epoch - 1)
            .get_token_amount(epoch_value);

        early_token_withdraw = early_token_withdraw + token_value_for_epoch;
    });

    // don't forget to flush the early withdrawals since we worked on a copy
    let _ = pool.pending_early_withdrawals.flush(current_epoch);

    // do the withdrawals reduction for both
    let exchange_rate = pool_exchange_rate::new(
        pool.wal_balance,
        pool.pool_token_balance,
    );
    pool.exchange_rates.add(current_epoch, exchange_rate);

    // Process additions.
    pool.wal_balance = pool.wal_balance + pool.pending_stake.flush(current_epoch);

    // Process withdrawals.
    let token_withdraw = pool.pending_pool_token_withdraw.flush(wctx.epoch());
    let pending_withdrawal = exchange_rate.get_wal_amount(token_withdraw + early_token_withdraw);

    // Check that the amount is not higher than the pool balance
    assert!(pool.wal_balance >= pending_withdrawal, ECalculationError);
    pool.wal_balance = pool.wal_balance - pending_withdrawal;

    // Recalculate the pool token balance.
    pool.pool_token_balance = exchange_rate.get_token_amount(pool.wal_balance);
}

// === Pool parameters ===

/// Sets the next commission rate for the pool.
/// TODO: implement changing commission rate in E+2, the change should not be
/// immediate.
public(package) fun set_next_commission(pool: &mut StakingPool, commission_rate: u16) {
    pool.commission_rate = commission_rate;
}

/// Sets the next storage price for the pool.
public(package) fun set_next_storage_price(pool: &mut StakingPool, storage_price: u64) {
    pool.voting_params.storage_price = storage_price;
}

/// Sets the next write price for the pool.
public(package) fun set_next_write_price(pool: &mut StakingPool, write_price: u64) {
    pool.voting_params.write_price = write_price;
}

/// Sets the next node capacity for the pool.
public(package) fun set_next_node_capacity(pool: &mut StakingPool, node_capacity: u64) {
    pool.voting_params.node_capacity = node_capacity;
}

/// Sets the public key to be used starting from the next epoch for which the node is selected.
public(package) fun set_next_public_key(
    self: &mut StakingPool,
    public_key: vector<u8>,
    proof_of_possession: vector<u8>,
    wctx: &WalrusContext,
    ctx: &TxContext,
) {
    // Verify proof of possession
    assert!(
        messages::new_proof_of_possession_msg(
            wctx.epoch(),
            ctx.sender(),
            public_key,
        ).verify_proof_of_possession(proof_of_possession),
        EInvalidProofOfPossession,
    );
    self.node_info.set_next_public_key(public_key);
}

/// Sets the name of the storage node.
public(package) fun set_name(self: &mut StakingPool, name: String) {
    self.node_info.set_name(name);
}

/// Sets the network address or host of the storage node.
public(package) fun set_network_address(self: &mut StakingPool, network_address: String) {
    self.node_info.set_network_address(network_address);
}

/// Sets the public key used for TLS communication.
public(package) fun set_network_public_key(self: &mut StakingPool, network_public_key: vector<u8>) {
    self.node_info.set_network_public_key(network_public_key);
}

/// Destroy the pool if it is empty.
public(package) fun destroy_empty(pool: StakingPool) {
    assert!(pool.is_empty(), EPoolNotEmpty);

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

/// Returns the exchange rate for the given current or future epoch. If there
/// isn't a value for the specified epoch, it will look for the most recent
/// value down to the pool activation epoch.
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

/// Returns the expected active stake for current or future epoch `E` for the pool.
/// It processes the pending stake and withdrawal requests from the current epoch
/// to `E`.
///
/// Should be the main function to calculate the active stake for the pool at
/// the given epoch, due to the complexity of the pending stake and withdrawal
/// requests, and lack of immediate updates.
public(package) fun wal_balance_at_epoch(pool: &StakingPool, epoch: u32): u64 {
    let mut expected = pool.wal_balance;
    let exchange_rate = pool_exchange_rate::new(pool.wal_balance, pool.pool_token_balance);
    let token_withdraw = pool.pending_pool_token_withdraw.value_at(epoch);
    let pending_withdrawal = exchange_rate.get_wal_amount(token_withdraw);

    expected = expected + pool.pending_stake.value_at(epoch);
    expected = expected - pending_withdrawal;
    expected
}

// === Accessors ===

/// Returns the commission rate for the pool.
public(package) fun commission_rate(pool: &StakingPool): u16 { pool.commission_rate }

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

/// Returns `true` if the pool is active.
public(package) fun is_active(pool: &StakingPool): bool { pool.state == PoolState::Active }

/// Returns `true` if the pool is withdrawing.
public(package) fun is_withdrawing(pool: &StakingPool): bool {
    match (pool.state) {
        PoolState::Withdrawing(_) => true,
        _ => false,
    }
}

/// Returns `true` if the pool is empty.
public(package) fun is_empty(pool: &StakingPool): bool {
    let pending_stake = pool.pending_stake.unwrap();
    let non_empty = pending_stake.keys().count!(|epoch| pending_stake[epoch] != 0);

    pool.wal_balance == 0 && non_empty == 0 && pool.pool_token_balance == 0
}
