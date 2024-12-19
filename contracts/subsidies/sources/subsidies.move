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
//TODO: Maybe it has to be u64 to avoid castings?
const MAX_SUBSIDY_RATE: u16 = 10_000; // 100%

// === Errors ===
const EInvalidSubsidyRate: u64 = 0;
const EInsufficientSubsidyFunds: u64 = 1;

// === Structs ===

/// Capability to perform admin operations.
public struct AdminCap has key {
    id: UID,
}

/// Subsidy rate is expressed in basis points (1/100 of a percent).
/// A subsidy rate of 100 basis points means a 1% subsidy.
/// The maximum subsidy rate is 10,000 basis points (100%).
public struct Subsidies has key, store {
    id: UID,
    subsidy_rate: u16, //TODO: Maybe it has to be u64 to avoid castings?
    subsidy_pool: Balance<WAL>,
}

/// Creates a new `Subsidies` object and an `AdminCap`.
public fun create(ctx: &mut TxContext): (Subsidies, AdminCap) {
    let subsidies = Subsidies {
        id: object::new(ctx),
        subsidy_rate: 0,
        subsidy_pool: balance::zero(),
    };
    let admin_cap = AdminCap { id: object::new(ctx) };
    (subsidies, admin_cap)
}

/// Create a new `Subsidies` object with an initial rate and funds and an `AdminCap`.
public fun create_with_initial_rate_and_funds(
    initial_subsidy_rate: u16,
    initial_funds: Coin<WAL>,
    ctx: &mut TxContext,
): (Subsidies, AdminCap) {
    assert!(initial_subsidy_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    let subsidies = Subsidies {
        id: object::new(ctx),
        subsidy_rate: initial_subsidy_rate,
        subsidy_pool: initial_funds.into_balance(),
    };
    let admin_cap = AdminCap { id: object::new(ctx) };
    (subsidies, admin_cap)
}

/// Add additional funds to the subsidy pool.
public fun add_funds(self: &mut Subsidies, funds: Coin<WAL>) {
    self.subsidy_pool.join(funds.into_balance());
}

/// Set the subsidy rate.
public fun set_subsidy_rate(self: &mut Subsidies, _: &AdminCap, new_rate: u16) {
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.subsidy_rate = new_rate;
}

/// Extends a blob's lifetime and applies the subsidy.
// TODO: The signature, of  public fun extend_blob(self: &mut System, blob: &mut Blob, epochs_ahead: u32, payment: &mut Coin<WAL>)
// in module walrus::system is different from the signature of this function.
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
    let subsidy_amount = cost * (self.subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);

    // Ensure sufficient funds in subsidy pool.
    assert!(self.subsidy_pool.value() >= subsidy_amount as u64, EInsufficientSubsidyFunds);

    let subsidy_balance = self.subsidy_pool.split(subsidy_amount);
    payment.join(subsidy_balance.into_coin(ctx));

    system.add_subsidy(payment, epochs_ahead);
}

// Reserves storage space and applies the subsidy.
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
    let subsidy_amount = cost * (self.subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);

    // Ensure sufficient funds in subsidy pool.
    assert!(self.subsidy_pool.value() >= subsidy_amount as u64, EInsufficientSubsidyFunds);

    let subsidy_balance = self.subsidy_pool.split(subsidy_amount);
    payment.join(subsidy_balance.into_coin(ctx));

    system.add_subsidy(payment, epochs_ahead);

    storage
}

// === Accessors ===

public fun subsidy_pool_value(self: &Subsidies): u64 {
    self.subsidy_pool.value()
}

public fun subsidy_rate(self: &Subsidies): u16 {
    self.subsidy_rate
}

#[test_only]
public fun get_subsidy_pool(self: &Subsidies): &Balance<WAL> {
    &self.subsidy_pool
}
