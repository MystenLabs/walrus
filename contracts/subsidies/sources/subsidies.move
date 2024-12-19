// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `subsidies`
///
/// This module allows adding funds to a subsidy pool, setting the rate of subsidies, and reserving storage space
// while providing a discounted cost based on the current subsidy rate.
module walrus::subsidies;

use sui::{balance::{Self, Balance}, coin::Coin};
use wal::wal::WAL;
use walrus::{blob::Blob, storage_resource::Storage, system::System};

/// Subsidy rate is in basis points (1/100 of a percent).
const MAX_SUBSIDY_RATE: u16 = 10_000; // 100%

// === Errors ===
const EInvalidSubsidyRate: u64 = 0;
const EInsufficientSubsidyFunds: u64 = 1;

// === Structs ===

/// Capability to perform admin operations.
public struct AdminCap has key {
    id: UID,
}

/// Subsidy rates are expressed in basis points (1/100 of a percent).
/// A subsidy rate of 100 basis points means a 1% subsidy.
/// The maximum subsidy rate is 10,000 basis points (100%).
public struct Subsidies has key, store {
    id: UID,
    buyer_subsidy_rate: u16,
    storage_node_subsidy_rate: u16,
    subsidy_pool: Balance<WAL>,
}

/// Creates a new `Subsidies` object and an `AdminCap`.
public fun new(ctx: &mut TxContext): AdminCap {
    transfer::share_object(Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: 0,
        storage_node_subsidy_rate: 0,
        subsidy_pool: balance::zero(),
    });
    AdminCap { id: object::new(ctx) }
}

/// Creates a new `Subsidies` object with initial rates and funds and an `AdminCap`.
public fun new_with_initial_rates_and_funds(
    initial_buyer_subsidy_rate: u16,
    initial_storage_node_subsidy_rate: u16,
    initial_funds: Coin<WAL>,
    ctx: &mut TxContext,
): AdminCap {
    assert!(initial_buyer_subsidy_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    assert!(initial_storage_node_subsidy_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    transfer::share_object(Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: initial_buyer_subsidy_rate,
        storage_node_subsidy_rate: initial_storage_node_subsidy_rate,
        subsidy_pool: initial_funds.into_balance(),
    });
    AdminCap { id: object::new(ctx) }
}

/// Add additional funds to the subsidy pool.
public fun add_funds(self: &mut Subsidies, funds: Coin<WAL>) {
    self.subsidy_pool.join(funds.into_balance());
}

/// Set the subsidy rate for buyers.
public fun set_buyer_subsidy_rate(self: &mut Subsidies, _: &AdminCap, new_rate: u16) {
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.buyer_subsidy_rate = new_rate;
}

/// Set the subsidy rate for storage nodes.
public fun set_storage_node_subsidy_rate(self: &mut Subsidies, _: &AdminCap, new_rate: u16) {
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.storage_node_subsidy_rate = new_rate;
}

/// Extends a blob's lifetime and applies the buyer and storage node subsidies.
public fun extend_blob(
    self: &mut Subsidies,
    system: &mut System,
    blob: &mut Blob,
    epochs_ahead: u32,
    mut payment: Coin<WAL>,
    ctx: &mut TxContext,
) {
    let initial_payment_value = payment.value();
    system.extend_blob(blob, epochs_ahead, &mut payment);

    let cost = initial_payment_value - payment.value();
    let buyer_subsidy_amount = cost * (self.buyer_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);
    let storage_node_subsidy_amount =
        cost * (self.storage_node_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);
    let total_subsidy = buyer_subsidy_amount + storage_node_subsidy_amount;

    // Ensure sufficient funds in subsidy pool.
    assert!(self.subsidy_pool.value() >= total_subsidy, EInsufficientSubsidyFunds);

    let subsidy_balance = self.subsidy_pool.split(total_subsidy);
    payment.join(subsidy_balance.into_coin(ctx));

    // add the whole payment amount to the system because the system object does not know that part of it is coming from subsidization
    // system handles distribution of subsidy to the nodes.
    system.add_subsidy(payment, epochs_ahead);
}

/// Reserves storage space and applies the buyer and storage node subsidies.
public fun reserve_space(
    self: &mut Subsidies,
    system: &mut System,
    storage_amount: u64,
    epochs_ahead: u32,
    mut payment: Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    let initial_payment_value = payment.value();
    let storage = system.reserve_space(storage_amount, epochs_ahead, &mut payment, ctx);

    let cost = initial_payment_value - payment.value();
    let buyer_subsidy_amount = cost * (self.buyer_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);
    let storage_node_subsidy_amount =
        cost * (self.storage_node_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);

    let total_subsidy = buyer_subsidy_amount + storage_node_subsidy_amount;

    // Ensure sufficient funds in subsidy pool.
    assert!(self.subsidy_pool.value() >= total_subsidy, EInsufficientSubsidyFunds);

    let subsidy_balance = self.subsidy_pool.split(total_subsidy);
    payment.join(subsidy_balance.into_coin(ctx));

    // add the whole payment amount to the system because the system object does not know that part of it is coming from subsidization
    // system handles distribution of subsidy to the nodes.
    system.add_subsidy(payment, epochs_ahead);

    storage
}

// === Accessors ===

public fun subsidy_pool_value(self: &Subsidies): u64 {
    self.subsidy_pool.value()
}

public fun buyer_subsidy_rate(self: &Subsidies): u16 {
    self.buyer_subsidy_rate
}

public fun storage_node_subsidy_rate(self: &Subsidies): u16 {
    self.storage_node_subsidy_rate
}

#[test_only]
public fun get_subsidy_pool(self: &Subsidies): &Balance<WAL> {
    &self.subsidy_pool
}
