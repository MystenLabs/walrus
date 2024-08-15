// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system;

use sui::{coin::Coin, dynamic_field, sui::SUI};
use walrus::{
    committee::Committee,
    storage_node::StorageNodeCap,
    storage_resource::Storage,
    system_state_inner::SystemStateInnerV1
};

/// Flag to indicate the version of the system.
const VERSION: u64 = 0;

/// The one and only system object.
public struct System has key {
    id: UID,
    version: u64,
}

/// Marks the epoch sync as done for the specified node.
public fun epoch_sync_done(system: &mut System, cap: &StorageNodeCap, epoch_number: u64) {
    system.inner_mut().epoch_sync_done(cap, epoch_number)
}

/// Marks blob as invalid given an invalid blob certificate.
public fun invalidate_blob_id(
    system: &mut System,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
) {
    system.inner_mut().invalidate_blob_id(signature, members, message)
}

/// Certifies a blob containing Walrus events.
public fun certify_event_blob(system: &mut System, cap: &StorageNodeCap, blob_id: u256, size: u64) {
    system.inner_mut().certify_event_blob(cap, blob_id, size)
}

/// Allows buying a storage reservation for a given period of epochs.
public fun reserve_space(
    self: &mut System,
    storage_amount: u64,
    periods_ahead: u64,
    payment: &mut Coin<SUI>,
    ctx: &mut TxContext,
): Storage {
    self.inner_mut().reserve_space(storage_amount, periods_ahead, payment, ctx)
}

// === Public Accessors ===

/// Get epoch. Uses the committee to get the epoch.
public fun epoch(self: &System): u64 {
    self.inner().epoch()
}

/// Accessor for total capacity size.
public fun total_capacity_size(self: &System): u64 {
    self.inner().total_capacity_size()
}

/// Accessor for used capacity size.
public fun used_capacity_size(self: &System): u64 {
    self.inner().used_capacity_size()
}

/// Accessor for the number of shards.
public fun n_shards(self: &System): u16 {
    self.inner().n_shards()
}

// === Restricted to Package ===

/// Accessor for the current committee.
public(package) fun current_committee(self: &System): &Committee {
    self.inner().current_committee()
}

// === Internals ===

/// Get a mutable reference to `SystemStateInner` from the `System`.
fun inner_mut(system: &mut System): &mut SystemStateInnerV1 {
    assert!(system.version == VERSION);
    dynamic_field::borrow_mut(&mut system.id, VERSION)
}

/// Get an immutable reference to `SystemStateInner` from the `System`.
fun inner(system: &System): &SystemStateInnerV1 {
    assert!(system.version == VERSION);
    dynamic_field::borrow(&system.id, VERSION)
}

// === Testing ===

#[test_only]
use walrus::system_state_inner;

#[test_only]
public(package) fun new_for_testing(ctx: &mut TxContext): System {
    let mut system = System { id: object::new(ctx), version: VERSION };
    let system_state_inner = system_state_inner::new_for_testing(ctx);
    dynamic_field::add(&mut system.id, VERSION, system_state_inner);
    system
}
