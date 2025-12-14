// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobManager interface module.
/// This module provides a stable interface for blob management operations.
/// The actual implementation is stored in BlobManagerInnerV1 as a dynamic field,
/// allowing for future upgrades without breaking external contracts.
module walrus::blobmanager;

use std::string::String;
use sui::{coin::Coin, dynamic_field, sui::SUI};
use wal::wal::WAL;
use walrus::{
    blob_manager_inner_v1::{Self, BlobManagerInnerV1},
    events,
    storage_resource::Storage,
    system::{Self, System}
};

// === Version Constants ===

/// Current version of the BlobManager implementation.
const VERSION: u64 = 1;

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// Operation requires Admin capability.
const ERequiresAdminCap: u64 = 5;
/// Operation requires fund_manager permission.
const ERequiresFundManager: u64 = 6;
/// The package version is not compatible with the BlobManager object.
const EWrongVersion: u64 = 13;

// === Main Structures ===

/// The BlobManager interface object.
/// Contains only versioning information; all business logic is in the inner dynamic field.
public struct BlobManager has key, store {
    id: UID,
    version: u64,
}

/// A capability which represents the authority to manage blobs in the BlobManager.
/// Admin can create new capabilities and perform all operations.
/// Fund manager can withdraw funds from the coin stash.
public struct BlobManagerCap has key, store {
    id: UID,
    /// The BlobManager this capability is for.
    manager_id: ID,
    /// Whether this capability has admin permissions (can create new caps).
    is_admin: bool,
    /// Whether this capability has fund manager permissions (can withdraw funds).
    fund_manager: bool,
}

// === Internal Accessors ===

/// Get a mutable reference to `BlobManagerInnerV1` from the `BlobManager`.
fun inner_mut(self: &mut BlobManager): &mut BlobManagerInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    dynamic_field::borrow_mut(&mut self.id, VERSION)
}

/// Get an immutable reference to `BlobManagerInnerV1` from the `BlobManager`.
fun inner(self: &BlobManager): &BlobManagerInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    dynamic_field::borrow(&self.id, VERSION)
}

// === Constructors ===

/// Creates a new shared BlobManager and returns its admin capability.
/// The BlobManager is automatically shared in the same transaction.
/// Requires minimum initial capacity of 500MB.
/// Note: This function requires access to System to emit the creation event with proper epoch.
public fun new_with_unified_storage(
    initial_storage: Storage,
    initial_wal: Coin<WAL>,
    system: &System,
    ctx: &mut TxContext,
): BlobManagerCap {
    // Create the inner implementation.
    let (inner, end_epoch) = blob_manager_inner_v1::new(
        initial_storage,
        initial_wal,
        system,
        ctx,
    );

    // Create the manager interface object.
    let mut manager = BlobManager {
        id: object::new(ctx),
        version: VERSION,
    };

    // Store inner as dynamic field keyed by version.
    dynamic_field::add(&mut manager.id, VERSION, inner);

    // Get the ObjectID from the constructed manager object.
    let manager_object_id = object::id(&manager);

    // Create an Admin capability with fund_manager permissions.
    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_object_id,
        is_admin: true,
        fund_manager: true,
    };

    // Register the initial admin cap in the inner's caps_info.
    manager.inner_mut().register_cap(object::id(&cap));

    // Emit creation event.
    let current_epoch = system::epoch(system);
    events::emit_blob_manager_created(
        current_epoch,
        manager_object_id,
        end_epoch,
    );

    // BlobManager is designed to be a shared object.
    transfer::share_object(manager);

    cap
}

// === Capability Operations ===

/// Creates a new capability for the BlobManager.
/// Only Admin capability can create new capabilities.
/// If the creating cap has fund_manager = true, they can create new caps with fund_manager = true.
/// Returns the newly created capability (caller/PTB handles transfer).
public fun create_cap(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    is_admin: bool,
    fund_manager: bool,
    ctx: &mut TxContext,
): BlobManagerCap {
    // Verify the capability matches this BlobManager.
    check_cap(self, cap);

    // Ensure the caller has Admin capability.
    ensure_admin(cap);

    // Only caps with fund_manager = true can create new fund_manager caps.
    assert!(!fund_manager || cap.fund_manager, ERequiresFundManager);

    let new_cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: object::id(self),
        is_admin,
        fund_manager,
    };

    // Register the new cap in inner's caps_info.
    self.inner_mut().register_cap(object::id(&new_cap));

    new_cap
}

/// Revokes a capability, preventing it from being used for any future operations.
/// Only an admin can revoke a capability.
public fun revoke_cap(self: &mut BlobManager, admin_cap: &BlobManagerCap, cap_to_revoke_id: ID) {
    check_cap(self, admin_cap);
    ensure_admin(admin_cap);

    self.inner_mut().revoke_cap(cap_to_revoke_id);
}

/// Checks if the capability is an Admin capability.
public fun is_admin_cap(cap: &BlobManagerCap): bool {
    cap.is_admin
}

/// Checks that the given BlobManagerCap matches the BlobManager and is not revoked.
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(object::id(self) == cap.manager_id, EInvalidBlobManagerCap);
    self.inner().check_cap_valid(object::id(cap));
}

/// Ensures the capability is an Admin capability.
fun ensure_admin(cap: &BlobManagerCap) {
    assert!(cap.is_admin_cap(), ERequiresAdminCap);
}

// === Core Operations ===

/// Registers a new blob in the BlobManager.
/// BlobManager owns the blob immediately upon registration.
/// Requires a valid BlobManagerCap to prove write access.
/// Note: the sender of the register transaction pays for the write fee, although the storage space
/// is accounted from the blob manager's storage.
/// Returns ok status (no abort) when:
///   - A matching blob already exists (certified or uncertified) - reuses existing blob
///   - A new blob is successfully created
/// Only aborts on errors:
///   - Insufficient funds (payment too small)
///   - Insufficient storage capacity
///   - Inconsistency detected in blob storage
public fun register_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    blob_type: u8,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);

    let manager_id = object::id(self);
    self
        .inner_mut()
        .register_blob(
            manager_id,
            system,
            blob_id,
            root_hash,
            size,
            encoding_type,
            deletable,
            blob_type,
            ctx,
        );
}

/// Certifies a managed blob.
/// Requires a valid BlobManagerCap to prove write access.
public fun certify_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    blob_id: u256,
    deletable: bool,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    check_cap(self, cap);

    self
        .inner_mut()
        .certify_blob(
            system,
            blob_id,
            deletable,
            signature,
            signers_bitmap,
            message,
        );
}

/// Deletes a managed blob from the BlobManager.
/// This removes the blob from storage tracking and emits a deletion event.
/// The blob must be deletable and registered with this BlobManager.
/// Requires a valid BlobManagerCap to prove write access.
public fun delete_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    blob_id: u256,
) {
    check_cap(self, cap);

    let manager_id = object::id(self);
    self.inner_mut().delete_blob(manager_id, system, blob_id);
}

/// Moves a regular blob (which owns Storage directly) into a BlobManager.
/// The sender must own the blob. The blob must be certified and not expired.
/// Storage epochs are aligned: if blob's end_epoch < manager's, purchase extension using coin
/// stash.
/// If blob's end_epoch > manager's, throw an error.
public fun move_blob_into_manager(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    blob: walrus::blob::Blob,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);

    let manager_id = object::id(self);
    self.inner_mut().move_blob_into_manager(manager_id, system, blob, ctx);
}

// === Coin Stash Operations ===

/// Deposits WAL coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support storage operations.
public fun deposit_wal_to_coin_stash(self: &mut BlobManager, payment: Coin<WAL>) {
    self.inner_mut().deposit_wal_to_coin_stash(payment);
}

/// Deposits SUI coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support gas operations.
public fun deposit_sui_to_coin_stash(self: &mut BlobManager, payment: Coin<SUI>) {
    self.inner_mut().deposit_sui_to_coin_stash(payment);
}

/// Buys additional storage capacity using funds from the coin stash.
/// The new storage uses the same epoch range as the existing storage.
/// This only increases capacity, not the end_epoch.
/// Requires a valid BlobManagerCap.
public fun buy_storage_from_stash(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    storage_amount: u64,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);

    self.inner_mut().buy_storage_from_stash(system, storage_amount, ctx);
}

/// Extends the storage period using funds from the coin stash.
/// Must follow the extension policy constraints.
/// Returns tip coin as reward to the caller.
public fun extend_storage_from_stash(
    self: &mut BlobManager,
    system: &mut System,
    extension_epochs: u32,
    ctx: &mut TxContext,
): Coin<SUI> {
    let manager_id = object::id(self);
    self.inner_mut().extend_storage_from_stash(manager_id, system, extension_epochs, ctx)
}

// === Fund Manager Functions ===

/// Withdraws a specific amount of WAL funds from the coin stash.
/// Requires fund_manager permission.
public fun withdraw_wal(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().withdraw_wal(amount, ctx)
}

/// Withdraws a specific amount of SUI funds from the coin stash.
/// Requires fund_manager permission.
public fun withdraw_sui(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().withdraw_sui(amount, ctx)
}

/// Sets the extension policy to disabled (no one can extend).
/// Requires fund_manager permission.
public fun set_extension_policy_disabled(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().set_extension_policy_disabled();
}

/// Sets the extension policy to constrained with the given parameters.
/// Requires fund_manager permission.
public fun set_extension_policy_constrained(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self
        .inner_mut()
        .set_extension_policy_constrained(expiry_threshold_epochs, max_extension_epochs);
}

// === Tip Policy Management ===

/// Sets the tip policy to a fixed amount.
/// Requires fund_manager permission.
public fun set_tip_policy_fixed_amount(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    tip_amount: u64,
) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().set_tip_policy_fixed_amount(tip_amount);
}

// === Storage Purchase Policy Configuration ===

/// Sets the storage purchase policy to unlimited (no restrictions).
/// Requires fund_manager permission.
public fun set_storage_purchase_policy_unlimited(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().set_storage_purchase_policy_unlimited();
}

/// Sets the storage purchase policy to a fixed cap.
/// Requires fund_manager permission.
public fun set_storage_purchase_policy_fixed_cap(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    max_storage_bytes: u64,
) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().set_storage_purchase_policy_fixed_cap(max_storage_bytes);
}

/// Sets the storage purchase policy to conditional purchase.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at threshold_bytes.
/// Requires fund_manager permission.
public fun set_storage_purchase_policy_conditional(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    threshold_bytes: u64,
) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    self.inner_mut().set_storage_purchase_policy_conditional(threshold_bytes);
}

// === Blob Attribute Operations ===

/// Sets an attribute on a managed blob.
///
/// If the key already exists, the value is updated.
/// Aborts if the blob is not found or if attribute limits are exceeded.
/// Requires a valid BlobManagerCap to prove write access.
public fun set_blob_attribute(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    key: String,
    value: String,
) {
    check_cap(self, cap);

    self.inner_mut().set_blob_attribute(blob_id, key, value);
}

/// Removes an attribute from a managed blob.
///
/// Aborts if the blob is not found or if the attribute key doesn't exist.
/// Requires a valid BlobManagerCap to prove write access.
public fun remove_blob_attribute(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    blob_id: u256,
    key: String,
) {
    check_cap(self, cap);

    self.inner_mut().remove_blob_attribute(blob_id, key);
}

/// Clears all attributes from a managed blob.
///
/// Aborts if the blob is not found.
/// Requires a valid BlobManagerCap to prove write access.
public fun clear_blob_attributes(self: &mut BlobManager, cap: &BlobManagerCap, blob_id: u256) {
    check_cap(self, cap);

    self.inner_mut().clear_blob_attributes(blob_id);
}

// === Accessors ===

/// Returns the version of this BlobManager.
public fun version(self: &BlobManager): u64 {
    self.version
}
