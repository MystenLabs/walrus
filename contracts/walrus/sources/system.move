// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system;

use sui::{balance::Balance, coin::Coin, dynamic_field, vec_map::VecMap};
use wal::wal::{WAL, ProtectedTreasury};
use walrus::{
    blob::Blob,
    bls_aggregate::BlsCommittee,
    epoch_parameters::EpochParams,
    storage_accounting::FutureAccountingRingBuffer,
    storage_node::StorageNodeCap,
    storage_resource::Storage,
    system_state_inner::{Self, SystemStateInnerV1, SystemStateInnerV2}
};

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// Error during the migration of the system object to the new package version.
const EInvalidMigration: u64 = 0;
/// The package version is not compatible with the system object.
const EWrongVersion: u64 = 1;
const EDeprecatedFunction: u64 = 2;

/// Flag to indicate the version of the system.
const VERSION: u64 = 3;

// Flag to indicate the first version of the system object that uses SystemStateInnerV2.
const V2_VERSION_START: u64 = 3;

/// The one and only system object.
public struct System has key {
    id: UID,
    version: u64,
    package_id: ID,
    new_package_id: Option<ID>,
}

/// Creates and shares an empty system object.
/// Must only be called by the initialization function.
public(package) fun create_empty(max_epochs_ahead: u32, package_id: ID, ctx: &mut TxContext) {
    let mut system = System {
        id: object::new(ctx),
        version: VERSION,
        package_id,
        new_package_id: option::none(),
    };
    let system_state_inner = system_state_inner::create_empty(max_epochs_ahead, ctx);
    dynamic_field::add(&mut system.id, VERSION, system_state_inner);
    transfer::share_object(system);
}

/// Sets the storage price per unit size. Called when a price vote is cast and the quorum
/// price is recalculated from the current committee.
public(package) fun set_storage_price(self: &mut System, price: u64) {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().set_storage_price(price);
    } else {
        self.inner_mut_v2().set_storage_price_v2(price);
    }
}

/// Sets the write price per unit size. Called when a price vote is cast and the quorum
/// price is recalculated from the current committee.
public(package) fun set_write_price(self: &mut System, price: u64) {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().set_write_price(price);
    } else {
        self.inner_mut_v2().set_write_price_v2(price);
    }
}

/// Update epoch to next epoch, and update the committee, price and capacity.
///
/// Called by the epoch change function that connects `Staking` and `System`. Returns
/// the balance of the rewards from the previous epoch.
public(package) fun advance_epoch(
    self: &mut System,
    new_committee: BlsCommittee,
    new_epoch_params: &EpochParams,
): VecMap<ID, Balance<WAL>> {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().advance_epoch(new_committee, new_epoch_params)
    } else {
        self.inner_mut_v2().advance_epoch_v2(new_committee, new_epoch_params)
    }
}

/// Extracts the balance that will be burned for the current epoch. This function is used when
/// executing the epoch change.
public(package) fun extract_burn_balance(self: &mut System): Balance<WAL> {
    // Burn only supported in V2.
    assert!(self.version >= V2_VERSION_START, EWrongVersion);
    self.inner_mut_v2().extract_burn_balance_v2()
}

/// === Public Functions ===

/// Marks blob as invalid given an invalid blob certificate.
public fun invalidate_blob_id(
    system: &System,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
): u256 {
    if (system.version < V2_VERSION_START) {
        system.inner_v1().invalidate_blob_id(signature, members_bitmap, message)
    } else {
        system.inner_v2().invalidate_blob_id_v2(signature, members_bitmap, message)
    }
}

/// Certifies a blob containing Walrus events.
public fun certify_event_blob(
    system: &mut System,
    cap: &mut StorageNodeCap,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    ending_checkpoint_sequence_num: u64,
    epoch: u32,
    ctx: &mut TxContext,
) {
    if (system.version < V2_VERSION_START) {
        system
            .inner_mut_v1()
            .certify_event_blob(
                cap,
                blob_id,
                root_hash,
                size,
                encoding_type,
                ending_checkpoint_sequence_num,
                epoch,
                ctx,
            )
    } else {
        system
            .inner_mut_v2()
            .certify_event_blob_v2(
                cap,
                blob_id,
                root_hash,
                size,
                encoding_type,
                ending_checkpoint_sequence_num,
                epoch,
                ctx,
            )
    }
}

/// Allows buying a storage reservation for a given period of epochs.
public fun reserve_space(
    self: &mut System,
    storage_amount: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().reserve_space(storage_amount, epochs_ahead, payment, ctx)
    } else {
        self.inner_mut_v2().reserve_space_v2(storage_amount, epochs_ahead, payment, ctx)
    }
}

/// Allows buying a storage reservation for a given period of epochs.
///
/// Returns a storage resource for the period between `start_epoch` (inclusive) and
/// `end_epoch` (exclusive). If `start_epoch` has already passed, reserves space starting
/// from the current epoch.
public fun reserve_space_for_epochs(
    self: &mut System,
    storage_amount: u64,
    start_epoch: u32,
    end_epoch: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Storage {
    if (self.version < V2_VERSION_START) {
        self
            .inner_mut_v1()
            .reserve_space_for_epochs(storage_amount, start_epoch, end_epoch, payment, ctx)
    } else {
        self
            .inner_mut_v2()
            .reserve_space_for_epochs_v2(storage_amount, start_epoch, end_epoch, payment, ctx)
    }
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
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Blob {
    if (self.version < V2_VERSION_START) {
        self
            .inner_mut_v1()
            .register_blob(
                storage,
                blob_id,
                root_hash,
                size,
                encoding_type,
                deletable,
                write_payment,
                ctx,
            )
    } else {
        self
            .inner_mut_v2()
            .register_blob_v2(
                storage,
                blob_id,
                root_hash,
                size,
                encoding_type,
                deletable,
                write_payment,
                ctx,
            )
    }
}

/// Certify that a blob will be available in the storage system until the end epoch of the
/// storage associated with it.
public fun certify_blob(
    self: &System,
    blob: &mut Blob,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().certify_blob(blob, signature, signers_bitmap, message)
    } else {
        self.inner_v2().certify_blob_v2(blob, signature, signers_bitmap, message)
    }
}

/// Deletes a deletable blob and returns the contained storage resource.
public fun delete_blob(self: &System, blob: Blob): Storage {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().delete_blob(blob)
    } else {
        self.inner_v2().delete_blob_v2(blob)
    }
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public fun extend_blob_with_resource(self: &System, blob: &mut Blob, extension: Storage) {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().extend_blob_with_resource(blob, extension)
    } else {
        self.inner_v2().extend_blob_with_resource_v2(blob, extension)
    }
}

/// Extend the period of validity of a blob by extending its contained storage resource
/// by `extended_epochs` epochs.
public fun extend_blob(
    self: &mut System,
    blob: &mut Blob,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().extend_blob(blob, extended_epochs, payment)
    } else {
        self.inner_mut_v2().extend_blob_v2(blob, extended_epochs, payment)
    }
}

/// Adds rewards to the system for the specified number of epochs ahead.
/// The rewards are split equally across the future accounting ring buffer up to the
/// specified epoch.
public fun add_subsidy(system: &mut System, subsidy: Coin<WAL>, epochs_ahead: u32) {
    if (system.version < V2_VERSION_START) {
        system.inner_mut_v1().add_subsidy(subsidy, epochs_ahead)
    } else {
        system.inner_mut_v2().add_subsidy_v2(subsidy, epochs_ahead)
    }
}

/// Adds rewards to the system for future epochs, where `subsidies[i]` is added to the rewards
/// of epoch `system.epoch() + i`.
public fun add_per_epoch_subsidies(system: &mut System, subsidies: vector<Balance<WAL>>) {
    if (system.version < V2_VERSION_START) {
        system.inner_mut_v1().add_per_epoch_subsidies(subsidies)
    } else {
        system.inner_mut_v2().add_per_epoch_subsidies_v2(subsidies)
    }
}

// === Protocol Version ===

/// Node collects signatures on the protocol version event and emits it.
public fun update_protocol_version(
    self: &mut System,
    cap: &StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().update_protocol_version(cap, signature, members_bitmap, message)
    } else {
        self.inner_v2().update_protocol_version_v2(cap, signature, members_bitmap, message)
    }
}

// === Deny List Features ===

/// Register a deny list update.
public fun register_deny_list_update(
    self: &mut System,
    cap: &StorageNodeCap,
    deny_list_root: u256,
    deny_list_sequence: u64,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().register_deny_list_update(cap, deny_list_root, deny_list_sequence)
    } else {
        self.inner_v2().register_deny_list_update_v2(cap, deny_list_root, deny_list_sequence)
    }
}

/// Perform the update of the deny list.
public fun update_deny_list(
    self: &mut System,
    cap: &mut StorageNodeCap,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().update_deny_list(cap, signature, members_bitmap, message)
    } else {
        self.inner_mut_v2().update_deny_list_v2(cap, signature, members_bitmap, message)
    }
}

/// Delete a blob that is deny listed by f+1 members.
public fun delete_deny_listed_blob(
    self: &System,
    signature: vector<u8>,
    members_bitmap: vector<u8>,
    message: vector<u8>,
) {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().delete_deny_listed_blob(signature, members_bitmap, message)
    } else {
        self.inner_v2().delete_deny_listed_blob_v2(signature, members_bitmap, message)
    }
}

// === Slashing ===

/// Votes to slash a target node's reward. The voter must be a current committee member.
/// The target must also be a current committee member. If the accumulated weight of votes
/// against a target exceeds 2f+1 shards, the target will not receive rewards in the
/// upcoming epoch change.
/// Note: Slashing is only supported in V2.
public fun vote_to_slash(self: &mut System, cap: &StorageNodeCap, target_node_id: ID) {
    // Slashing only supported in V2.
    assert!(self.version >= V2_VERSION_START, EWrongVersion);
    self.inner_mut_v2().vote_to_slash(cap, target_node_id)
}

/// Returns the current slashing votes weight for a target node.
/// Note: Returns 0 for V1 systems as slashing is not supported.
public fun get_slashing_votes(self: &System, target_node_id: ID): u16 {
    // Slashing only supported in V2.
    assert!(self.version >= V2_VERSION_START, EWrongVersion);
    self.inner_v2().get_slashing_votes(target_node_id)
}

/// Returns the set of node IDs that have been slashed (received 2f+1 votes).
/// Note: Returns empty vector for V1 systems as slashing is not supported.
public fun get_slashed_nodes(self: &System): vector<ID> {
    // Slashing only supported in V2.
    assert!(self.version >= V2_VERSION_START, EWrongVersion);
    self.inner_v2().get_slashed_nodes()
}

/// Applies slashing to the rewards map. Slashed nodes have their rewards burned.
/// Returns the modified rewards map with slashed nodes' rewards set to zero.
/// Must be called before distributing rewards in epoch change.
/// Note: For V1 systems, returns rewards unchanged as slashing is not supported.
public(package) fun apply_slashing(
    self: &mut System,
    rewards: VecMap<ID, Balance<WAL>>,
    treasury: &mut ProtectedTreasury,
    ctx: &mut TxContext,
): VecMap<ID, Balance<WAL>> {
    // Slashing only supported in V2.
    assert!(self.version >= V2_VERSION_START, EWrongVersion);
    self.inner_mut_v2().apply_slashing(rewards, treasury, ctx)
}

// === Public Accessors ===

/// Get epoch. Uses the committee to get the epoch.
public fun epoch(self: &System): u32 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().epoch()
    } else {
        self.inner_v2().epoch_v2()
    }
}

/// Accessor for total capacity size.
public fun total_capacity_size(self: &System): u64 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().total_capacity_size()
    } else {
        self.inner_v2().total_capacity_size_v2()
    }
}

/// Accessor for used capacity size.
public fun used_capacity_size(self: &System): u64 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().used_capacity_size()
    } else {
        self.inner_v2().used_capacity_size_v2()
    }
}

/// Accessor for the number of shards.
public fun n_shards(self: &System): u16 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().n_shards()
    } else {
        self.inner_v2().n_shards_v2()
    }
}

/// Read-only access to the accounting ring buffer.
public fun future_accounting(self: &System): &FutureAccountingRingBuffer {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().future_accounting()
    } else {
        self.inner_v2().future_accounting_v2()
    }
}

// === Accessors ===

public(package) fun package_id(system: &System): ID {
    system.package_id
}

public fun version(system: &System): u64 {
    system.version
}

// === Upgrade ===

public(package) fun set_new_package_id(system: &mut System, new_package_id: ID) {
    system.new_package_id = option::some(new_package_id);
}

/// Migrate the system object to the new package id.
///
/// This function sets the new package id and version and can be modified in future versions
/// to migrate changes in the `system_state_inner` object if needed.
public(package) fun migrate(system: &mut System) {
    abort EDeprecatedFunction
}

public(package) fun migrate_v2(system: &mut System, ctx: &mut TxContext) {
    assert!(system.version < VERSION, EInvalidMigration);

    // Migrate the system state inner based on the old version.
    // Versions 1-2 use SystemStateInnerV1 (without slashing_votes).
    // Version 3+ uses SystemStateInnerV2 (with slashing_votes).
    let system_state_inner_v2 = if (system.version <= 2) {
        // Migrate from V1 to V2: add slashing_votes field
        let v1: SystemStateInnerV1 = dynamic_field::remove(&mut system.id, system.version);
        system_state_inner::migrate_v1_to_v2(v1, ctx)
    } else {
        // Already V2, just move to new version key
        dynamic_field::remove<u64, SystemStateInnerV2>(&mut system.id, system.version)
    };

    dynamic_field::add(&mut system.id, VERSION, system_state_inner_v2);
    system.version = VERSION;

    // Set the new package id.
    assert!(system.new_package_id.is_some(), EInvalidMigration);
    system.package_id = system.new_package_id.extract();
}

// === Internals ===

/// Get a mutable reference to `SystemStateInner` from the `System`.
fun inner_mut_v1(system: &mut System): &mut SystemStateInnerV1 {
    assert!(system.version < 3, EWrongVersion);
    dynamic_field::borrow_mut(&mut system.id, VERSION)
}

/// Get an immutable reference to `SystemStateInner` from the `System`.
public(package) fun inner_v1(system: &System): &SystemStateInnerV1 {
    assert!(system.version < 3, EWrongVersion);
    dynamic_field::borrow(&system.id, VERSION)
}

/// Get a mutable reference to `SystemStateInner` from the `System`.
fun inner_mut_v2(system: &mut System): &mut SystemStateInnerV2 {
    assert!(system.version == VERSION, EWrongVersion);
    dynamic_field::borrow_mut(&mut system.id, VERSION)
}

/// Get an immutable reference to `SystemStateInner` from the `System`.
public(package) fun inner_v2(system: &System): &SystemStateInnerV2 {
    assert!(system.version == VERSION, EWrongVersion);
    dynamic_field::borrow(&system.id, VERSION)
}

// === Testing ===

#[test_only]
/// Accessor for the current committee.
public(package) fun committee(self: &System): &BlsCommittee {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().committee()
    } else {
        self.inner_v2().committee_v2()
    }
}

#[test_only]
public(package) fun committee_mut(self: &mut System): &mut BlsCommittee {
    if (self.version < V2_VERSION_START) {
        self.inner_mut_v1().committee_mut()
    } else {
        self.inner_mut_v2().committee_mut_v2()
    }
}

#[test_only]
public fun new_for_testing(ctx: &mut TxContext): System {
    let mut system = System {
        id: object::new(ctx),
        version: VERSION,
        package_id: new_id(ctx),
        new_package_id: option::none(),
    };
    let system_state_inner = system_state_inner::new_for_testing();
    dynamic_field::add(&mut system.id, VERSION, system_state_inner);
    system
}

#[test_only]
public(package) fun new_for_testing_with_multiple_members(ctx: &mut TxContext): System {
    let mut system = System {
        id: object::new(ctx),
        version: VERSION,
        package_id: new_id(ctx),
        new_package_id: option::none(),
    };
    let system_state_inner = system_state_inner::new_for_testing_with_multiple_members(ctx);
    dynamic_field::add(&mut system.id, VERSION, system_state_inner);
    system
}

#[test_only]
fun new_id(ctx: &mut TxContext): ID {
    ctx.fresh_object_address().to_id()
}

#[test_only]
public(package) fun new_package_id(system: &System): Option<ID> {
    system.new_package_id
}

#[test_only]
/// Returns the raw storage price per unit size.
public fun storage_price_per_unit_size(self: &System): u64 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().storage_price_per_unit_size()
    } else {
        self.inner_v2().storage_price_per_unit_size_v2()
    }
}

#[test_only]
/// Returns the raw write price per unit size.
public fun write_price_per_unit_size(self: &System): u64 {
    if (self.version < V2_VERSION_START) {
        self.inner_v1().write_price_per_unit_size()
    } else {
        self.inner_v2().write_price_per_unit_size_v2()
    }
}

#[test_only]
public(package) fun destroy_for_testing(self: System) {
    std::unit_test::destroy(self);
}

#[test_only]
public fun get_system_rewards_balance(self: &mut System, epoch_in_future: u32): &mut Balance<WAL> {
    if (self.version < V2_VERSION_START) {
        self
            .inner_mut_v1()
            .future_accounting_mut()
            .ring_lookup_mut(epoch_in_future)
            .rewards_balance()
    } else {
        self
            .inner_mut_v2()
            .future_accounting_mut_v2()
            .ring_lookup_mut(epoch_in_future)
            .rewards_balance()
    }
}
