// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: `subsidies`
///
/// This module allows adding funds to a subsidy pool, setting the rate of subsidies, and reserving storage space
// while providing a discounted cost based on the current subsidy rate.
module subsidies::subsidies;

use sui::{balance::{Self, Balance}, coin::{Self, Coin}};
use wal::wal::WAL;
use walrus::{blob::Blob, storage_resource::Storage, system::System};

/// Subsidy rate is in basis points (1/100 of a percent).
const MAX_SUBSIDY_RATE: u16 = 10_000; // 100%

// === Errors ===
const EInvalidSubsidyRate: u64 = 0;
const EUnauthorizedAdminCap: u64 = 1;

// === Structs ===

/// Capability to perform admin operations, tied to a specific Subsidies object.
public struct AdminCap has key {
    id: UID,
    subsidies_id: ID,
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
    let subsidies = Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: 0,
        storage_node_subsidy_rate: 0,
        subsidy_pool: balance::zero(),
    };
    let admin_cap = AdminCap { id: object::new(ctx), subsidies_id: object::id(&subsidies) };
    transfer::share_object(subsidies);
    admin_cap
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
    let subsidies = Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: initial_buyer_subsidy_rate,
        storage_node_subsidy_rate: initial_storage_node_subsidy_rate,
        subsidy_pool: initial_funds.into_balance(),
    };
    let admin_cap = AdminCap { id: object::new(ctx), subsidies_id: object::id(&subsidies) };
    transfer::share_object(subsidies);
    admin_cap
}

/// Add additional funds to the subsidy pool.
public fun add_funds(self: &mut Subsidies, funds: Coin<WAL>) {
    self.subsidy_pool.join(funds.into_balance());
}

fun check_admin(self: &Subsidies, admin_cap: &AdminCap) {
    assert!(object::id(self) == admin_cap.subsidies_id, EUnauthorizedAdminCap);
}

/// Set the subsidy rate for buyers.
public fun set_buyer_subsidy_rate(self: &mut Subsidies, cap: &AdminCap, new_rate: u16) {
    check_admin(self, cap);
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.buyer_subsidy_rate = new_rate;
}

/// Set the subsidy rate for storage nodes.
public fun set_storage_node_subsidy_rate(self: &mut Subsidies, cap: &AdminCap, new_rate: u16) {
    check_admin(self, cap);
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.storage_node_subsidy_rate = new_rate;
}

/// Applies subsidies and sends rewards to the system.
fun apply_subsidies(
    self: &mut Subsidies,
    cost: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    system: &mut System,
    ctx: &mut TxContext,
) {
    let buyer_subsidy_amount = cost * (self.buyer_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);
    let storage_node_subsidy_amount =
        cost * (self.storage_node_subsidy_rate as u64) / (MAX_SUBSIDY_RATE as u64);
    let total_subsidy = buyer_subsidy_amount + storage_node_subsidy_amount;

    if (self.subsidy_pool.value() >= total_subsidy) {
        let subsidy_balance = self.subsidy_pool.split(total_subsidy);
        payment.join(subsidy_balance.into_coin(ctx));
    };
    let payment_value = payment.value();
    add_subsidy_to_system(payment, payment_value, system, epochs_ahead, ctx);
}

/// Adds a subsidy to the system by taking a `Coin` and epochs ahead.
fun add_subsidy_to_system(
    payment: &mut Coin<WAL>,
    payment_value: u64,
    system: &mut System,
    epochs_ahead: u32,
    ctx: &mut TxContext,
) {
    system.add_subsidy(coin::take(payment.balance_mut(), payment_value, ctx), epochs_ahead);
}

/// Extends a blob's lifetime and applies the buyer and storage node subsidies.
public fun extend_blob(
    self: &mut Subsidies,
    system: &mut System,
    blob: &mut Blob,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
) {
    let payment_value = payment.value();
    system.extend_blob(blob, epochs_ahead, payment);
    self.apply_subsidies(payment_value - payment.value(), epochs_ahead, payment, system, ctx);
}

/// Reserves storage space and applies the buyer and storage node subsidies.
public fun reserve_space(
    self: &mut Subsidies,
    system: &mut System,
    storage_amount: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    let initial_payment_value = payment.value();
    let storage = system.reserve_space(storage_amount, epochs_ahead, payment, ctx);
    self.apply_subsidies(
        initial_payment_value - payment.value(),
        epochs_ahead,
        payment,
        system,
        ctx,
    );
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

// === Tests ===

#[test_only]
use sui::test_utils::destroy;

#[test_only]
public fun get_subsidy_pool(self: &Subsidies): &Balance<WAL> {
    &self.subsidy_pool
}

#[test_only]
public fun new_for_testing(ctx: &mut TxContext): (Subsidies, AdminCap) {
    let subsidies = Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: 0,
        storage_node_subsidy_rate: 0,
        subsidy_pool: balance::zero(),
    };
    let admin_cap = AdminCap {
        id: object::new(ctx),
        subsidies_id: object::id(&subsidies),
    };
    (subsidies, admin_cap)
}

#[test_only]
public fun new_with_initial_rates_and_funds_for_testing(
    initial_buyer_subsidy_rate: u16,
    initial_storage_node_subsidy_rate: u16,
    initial_funds: Coin<WAL>,
    ctx: &mut TxContext,
): (Subsidies, AdminCap) {
    assert!(initial_buyer_subsidy_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    assert!(initial_storage_node_subsidy_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    let subsidies = Subsidies {
        id: object::new(ctx),
        buyer_subsidy_rate: initial_buyer_subsidy_rate,
        storage_node_subsidy_rate: initial_storage_node_subsidy_rate,
        subsidy_pool: initial_funds.into_balance(),
    };
    let admin_cap = AdminCap {
        id: object::new(ctx),
        subsidies_id: object::id(&subsidies),
    };
    (subsidies, admin_cap)
}

#[test_only]
public fun destroy_admin_cap(admin_cap: AdminCap) {
    destroy(admin_cap);
}

#[test_only]
public fun destroy_subsidies(subsidies: Subsidies) {
    destroy(subsidies);
}
