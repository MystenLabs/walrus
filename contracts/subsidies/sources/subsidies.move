// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Module: subsidies
module walrus::subsidies;

use sui::{balance::{Self, Balance}, coin::{Self, Coin}};
use wal::wal::WAL;
use walrus::{blob::Blob, storage_resource::Storage, system::{Self, System}, system_state_inner};

/// Subsidy rate is in basis points (1/100 of a percent).
const MAX_SUBSIDY_RATE: u16 = 10_000; // 100%

// === Errors ===
const EUnauthorized: u64 = 0;
const EInvalidSubsidyRate: u64 = 1;
const EInsufficientSubsidyFunds: u64 = 2;

// === Structs ===

/// Capability to perform admin operations in this module.
public struct AdminCap has key {
    id: UID,
}

/// Subsidy rate is expressed in basis points (1/100 of a percent).
/// A subsidy rate of 100 basis points means a 1% subsidy.
/// The maximum subsidy rate is 10000 basis points (100%).
public struct Subsidies has key, store {
    id: UID,
    subsidy_rate: u16,
    subsidy_pool: Balance<WAL>,
}

// TODO: Not sure if `AdminCap` is needed.
/// Creates a new `Subsidies` object and an `AdminCap`.
public(package) fun create(ctx: &mut TxContext): (Subsidies, AdminCap) {
    let subsidies = Subsidies {
        id: object::new(ctx),
        subsidy_rate: 0,
        subsidy_pool: balance::zero(),
    };
    let admin_cap = AdminCap { id: object::new(ctx) };
    (subsidies, admin_cap)
}

/// Add additional funds to the subsidy pool.
public fun add_funds(self: &mut Subsidies, funds: Coin<WAL>) {
    self.subsidy_pool.join(funds.into_balance());
}

/// Set the subsidy rate.
public fun set_subsidy_rate(self: &mut Subsidies, admin_cap: &AdminCap, new_rate: u16) {
    assert!(new_rate <= MAX_SUBSIDY_RATE, EInvalidSubsidyRate);
    self.subsidy_rate = new_rate;
}
