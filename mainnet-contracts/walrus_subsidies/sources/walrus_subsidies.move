// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Module to subsidize the Walrus system and its storage nodes.
module walrus_subsidies::walrus_subsidies;

use std::type_name;
use sui::{balance::Balance, clock::Clock, coin::Coin, dynamic_field as df, hex};
use wal::wal::WAL;
use walrus::{staking::Staking, system::System};
use walrus_subsidies::walrus_subsidies_inner::{Self, WalrusSubsidiesInnerV1};

// === Errors ===

/// The admin cap is not authorized for this WalrusSubsidies object.
const EUnauthorizedAdminCap: u64 = 0;
/// The package version is not compatible with the WalrusSubsidies object.
const EWrongVersion: u64 = 1;

// === Versioning ===

// Whenever the package is upgraded, we create a new type here that will have the ID of the new
// package in its type name. We can then use this to migrate the object to the new package ID
// without requiring the AdminCap.

/// The current version of this contract.
const VERSION: u64 = 1;

/// Helper struct to get the package ID for the version 1 of this contract.
public struct V1()

/// Returns the package ID for the current version of this contract.
/// Needs to be updated whenever the package is upgraded.
fun package_id_for_current_version(): ID {
    package_id_for_type<V1>()
}

/// Returns the package ID for the given type.
fun package_id_for_type<T>(): ID {
    let address_str = type_name::get<T>().get_address().to_lowercase();
    let address_bytes = hex::decode(address_str.into_bytes());
    object::id_from_bytes(address_bytes)
}

// === Dynamic Field Keys ===

/// Key for storing the inner subsidies struct as a dynamic field.
public struct SubsidiesInnerKey() has copy, drop, store;

// === Structs ===

/// Capability to perform admin operations on a specific `WalrusSubsidies` object.
///
/// Only the holder of this capability can modify subsidy rates.
public struct AdminCap has key, store {
    id: UID,
    /// The ID of the `WalrusSubsidies` object this cap can manage.
    subsidies_id: ID,
}

/// The main WalrusSubsidies object that wraps the inner subsidies logic.
/// This object is shared and uses dynamic fields to store the actual subsidies data.
public struct WalrusSubsidies has key {
    id: UID,
    version: u64,
    package_id: ID,
}

// === Constructor ===

/// Creates a new WalrusSubsidies object and returns the AdminCap for managing it.
public fun new(
    system: &System,
    staking: &Staking,
    system_subsidy_rate: u32,
    base_subsidy: u64,
    subsidy_per_shard: u64,
    ctx: &mut TxContext,
): AdminCap {
    let package_id = package_id_for_current_version();
    let mut subsidies = WalrusSubsidies {
        id: object::new(ctx),
        package_id,
        version: VERSION,
    };

    // Create the inner subsidies struct and store it as a dynamic field
    let inner = walrus_subsidies_inner::new(
        system,
        staking,
        system_subsidy_rate,
        base_subsidy,
        subsidy_per_shard,
        ctx,
    );
    df::add(&mut subsidies.id, SubsidiesInnerKey(), inner);

    let admin_cap = AdminCap {
        id: object::new(ctx),
        subsidies_id: subsidies.id.to_inner(),
    };

    transfer::share_object(subsidies);
    admin_cap
}

// === Public Functions ===

/// Add funds to the subsidy pool.
/// Anyone can add funds to increase the pool.
public fun add_coin(self: &mut WalrusSubsidies, funds: Coin<WAL>) {
    check_version(self);
    self.inner_mut().add_balance(funds.into_balance());
}

/// Add a balance to the subsidy pool.
/// Anyone can add funds to increase the pool.
public fun add_balance(self: &mut WalrusSubsidies, balance: Balance<WAL>) {
    check_version(self);
    self.inner_mut().add_balance(balance);
}

// === Admin Functions ===

/// Set the system subsidy rate (requires AdminCap).
public fun set_system_subsidy_rate(
    self: &mut WalrusSubsidies,
    admin_cap: &AdminCap,
    new_rate: u32,
) {
    check_admin(self, admin_cap);
    check_version(self);
    self.inner_mut().set_system_subsidy_rate(new_rate);
}

/// Set the base subsidy (requires AdminCap).
public fun set_base_subsidy(self: &mut WalrusSubsidies, admin_cap: &AdminCap, base_subsidy: u64) {
    check_admin(self, admin_cap);
    check_version(self);
    self.inner_mut().set_base_subsidy(base_subsidy);
}

/// Set the per-shard subsidy (requires AdminCap).
public fun set_per_shard_subsidy(
    self: &mut WalrusSubsidies,
    admin_cap: &AdminCap,
    subsidy_per_shard: u64,
) {
    check_admin(self, admin_cap);
    check_version(self);
    self.inner_mut().set_per_shard_subsidy(subsidy_per_shard);
}

// === Subsidy Processing Functions ===

/// Process all subsidies (usage-independent and usage-based).
/// This function can be called by anyone to trigger subsidy processing.
public fun process_subsidies(
    self: &mut WalrusSubsidies,
    staking: &mut Staking,
    system: &mut System,
    clock: &Clock,
) {
    check_version(self);
    self.inner_mut().process_subsidies(staking, system, clock);
}

// === Internal Functions ===

/// Check if the admin cap is valid for this subsidies object.
/// Aborts if the cap does not match.
fun check_admin(self: &WalrusSubsidies, admin_cap: &AdminCap) {
    assert!(self.id.to_inner() == admin_cap.subsidies_id, EUnauthorizedAdminCap);
}

/// Check if the version is compatible.
fun check_version(self: &WalrusSubsidies) {
    assert!(self.version == VERSION, EWrongVersion);
}

fun inner_mut(self: &mut WalrusSubsidies): &mut WalrusSubsidiesInnerV1 {
    df::borrow_mut(&mut self.id, SubsidiesInnerKey())
}

// === Test-only Functions ===

#[test_only]
/// Accessor for the inner subsidies struct.
public fun inner(self: &WalrusSubsidies): &WalrusSubsidiesInnerV1 {
    df::borrow(&self.id, SubsidiesInnerKey())
}

#[test_only]
/// Returns the current balance of the subsidy pool.
public fun subsidy_pool_balance(self: &WalrusSubsidies): u64 {
    check_version(self);
    self.inner().subsidy_pool_balance()
}

#[test_only]
/// Returns the current system subsidy rate.
public fun system_subsidy_rate(self: &WalrusSubsidies): u32 {
    check_version(self);
    self.inner().system_subsidy_rate()
}

#[test_only]
/// Returns the current base subsidy.
public fun base_subsidy(self: &WalrusSubsidies): u64 {
    check_version(self);
    self.inner().base_subsidy()
}

#[test_only]
/// Returns the current per-shard subsidy.
public fun per_shard_subsidy(self: &WalrusSubsidies): u64 {
    check_version(self);
    self.inner().per_shard_subsidy()
}

// === Tests ===

#[test_only]
use std::unit_test::assert_eq;

#[test]
fun test_package_id_for_current_version() {
    let package_id = package_id_for_current_version();
    assert_eq!(type_name::get<V1>().get_address(), package_id.to_address().to_ascii_string());
}
