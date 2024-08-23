// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system;

use sui::{balance::Balance, coin::Coin, dynamic_field, sui::SUI};
use walrus::{
    blob::Blob,
    bls_aggregate::BlsCommittee,
    storage_node::StorageNodeCap,
    storage_resource::Storage,
    system_state_inner::SystemStateInnerV1,
};

/// Flag to indicate the version of the system.
const VERSION: u64 = 0;

/// The one and only system object.
public struct System has key {
    id: UID,
    version: u64,
}

/// Marks blob as invalid given an invalid blob certificate.
public fun invalidate_blob_id(
    system: &System,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
): u256 {
    system.inner().invalidate_blob_id(signature, members, message)
}

/// Certifies a blob containing Walrus events.
public fun certify_event_blob(system: &mut System, cap: &StorageNodeCap, blob_id: u256, size: u64) {
    system.inner_mut().certify_event_blob(cap, blob_id, size)
}

/// Allows buying a storage reservation for a given period of epochs.
public fun reserve_space(
    self: &mut System,
    storage_amount: u64,
    epochs_ahead: u32,
    payment: &mut Coin<SUI>,
    ctx: &mut TxContext,
): Storage {
    self.inner_mut().reserve_space(storage_amount, epochs_ahead, payment, ctx)
}

/// Registers a new blob in the system.
/// `size` is the size of the unencoded blob. The reserved space in `storage` must be at
/// least the size of the encoded blob.
public fun register_blob(
    self: &mut System,
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    erasure_code_type: u8,
    deletable: bool,
    write_payment: &mut Coin<SUI>,
    ctx: &mut TxContext,
): Blob {
    self
        .inner_mut()
        .register_blob(
            storage,
            blob_id,
            root_hash,
            size,
            erasure_code_type,
            deletable,
            write_payment,
            ctx,
        )
}

/// Certify that a blob will be available in the storage system until the end epoch of the
/// storage associated with it.
public fun certify_blob(
    self: &System,
    blob: &mut Blob,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
) {
    self.inner().certify_blob(blob, signature, members, message);
}

/// Deletes a deletable blob and returns the contained storage resource.
public fun delete_blob(self: &System, blob: Blob): Storage {
    self.inner().delete_blob(blob)
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public fun extend_blob_with_resource(self: &System, blob: &mut Blob, extension: Storage) {
    self.inner().extend_blob_with_resource(blob, extension);
}

/// Extend the period of validity of a blob by extending its contained storage resource.
public fun extend_blob(
    self: &mut System,
    blob: &mut Blob,
    epochs_ahead: u32,
    payment: &mut Coin<SUI>,
) {
    self.inner_mut().extend_blob(blob, epochs_ahead, payment);
}

// === Public Accessors ===

/// Get epoch. Uses the committee to get the epoch.
public fun epoch(self: &System): u32 {
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
public(package) fun current_committee(self: &System): &BlsCommittee {
    self.inner().current_committee()
}

/// Update epoch to next epoch, and update the committee, price and capacity.
///
/// Called by the epoch change function that connects `Staking` and `System`. Returns
/// the balance of the rewards from the previous epoch.
public(package) fun advance_epoch(
    self: &mut System,
    new_committee: BlsCommittee,
    new_capacity: u64,
    new_storage_price: u64,
    new_write_price: u64,
): Balance<SUI> {
    self.inner_mut().advance_epoch(new_committee, new_capacity, new_storage_price, new_write_price)
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
public(package) fun new_for_testing(): System {
    let ctx = &mut tx_context::dummy();
    let mut system = System { id: object::new(ctx), version: VERSION };
    let system_state_inner = system_state_inner::new_for_testing(ctx);
    dynamic_field::add(&mut system.id, VERSION, system_state_inner);
    system
}
