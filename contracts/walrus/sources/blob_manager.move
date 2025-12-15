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
/// Operation requires delegate capability.
const ERequiresDelegateCap: u64 = 5;
/// Operation requires can_withdraw_funds permission.
const ERequiresWithdrawFunds: u64 = 6;
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
/// Delegate capability can create new capabilities and perform all operations.
/// Withdraw funds capability can withdraw funds from the coin stash.
public struct BlobManagerCap has key, store {
    id: UID,
    /// The BlobManager this capability is for.
    manager_id: ID,
    /// Whether this capability can delegate (create new caps).
    can_delegate: bool,
    /// Whether this capability can withdraw funds from the coin stash.
    can_withdraw_funds: bool,
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

    // Create a capability with full permissions.
    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_object_id,
        can_delegate: true,
        can_withdraw_funds: true,
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
/// Only delegate capability can create new capabilities.
/// If the creating cap has can_withdraw_funds = true, they can create new caps with
/// can_withdraw_funds = true.
/// Returns the newly created capability (caller/PTB handles transfer).
public fun create_cap(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    can_delegate: bool,
    can_withdraw_funds: bool,
    ctx: &mut TxContext,
): BlobManagerCap {
    // Verify the capability matches this BlobManager.
    check_cap(self, cap);

    // Ensure the caller has delegate capability.
    ensure_can_delegate(cap);

    // Only caps with can_withdraw_funds = true can create new can_withdraw_funds caps.
    assert!(!can_withdraw_funds || cap.can_withdraw_funds, ERequiresWithdrawFunds);

    let new_cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: object::id(self),
        can_delegate,
        can_withdraw_funds,
    };

    // Register the new cap in inner's caps_info.
    self.inner_mut().register_cap(object::id(&new_cap));

    new_cap
}

/// Revokes a capability, preventing it from being used for any future operations.
/// Only a delegate cap can revoke a capability.
public fun revoke_cap(self: &mut BlobManager, delegate_cap: &BlobManagerCap, cap_to_revoke_id: ID) {
    check_cap(self, delegate_cap);
    ensure_can_delegate(delegate_cap);

    self.inner_mut().revoke_cap(cap_to_revoke_id);
}

/// Checks if the capability can delegate (create new caps).
public fun can_delegate(cap: &BlobManagerCap): bool {
    cap.can_delegate
}

/// Checks if the capability can withdraw funds from the coin stash.
public fun can_withdraw_funds(cap: &BlobManagerCap): bool {
    cap.can_withdraw_funds
}

/// Checks that the given BlobManagerCap matches the BlobManager and is not revoked.
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(object::id(self) == cap.manager_id, EInvalidBlobManagerCap);
    self.inner().check_cap_valid(object::id(cap));
}

/// Ensures the capability can delegate.
fun ensure_can_delegate(cap: &BlobManagerCap) {
    assert!(cap.can_delegate(), ERequiresDelegateCap);
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
/// Anyone can certify a registered blob - no capability required.
/// The blob must already be registered, and the certification message is
/// cryptographically verified by the system.
public fun certify_blob(
    self: &mut BlobManager,
    system: &System,
    blob_id: u256,
    deletable: bool,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
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

/// Converts a deletable managed blob to permanent.
/// This is a one-way operation - permanent blobs cannot be made deletable again.
/// Requires a valid BlobManagerCap to prove write access.
/// Requires the blob to be deletable.
public fun make_blob_permanent(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    blob_id: u256,
) {
    check_cap(self, cap);

    self.inner_mut().make_blob_permanent(system, blob_id);
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
/// Requires can_withdraw_funds permission.
public fun withdraw_wal(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    self.inner_mut().withdraw_wal(amount, ctx)
}

/// Withdraws a specific amount of SUI funds from the coin stash.
/// Requires can_withdraw_funds permission.
public fun withdraw_sui(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    self.inner_mut().withdraw_sui(amount, ctx)
}

/// Sets the extension policy with the given parameters.
/// To disable extensions, set max_extension_epochs to 0.
/// Requires can_withdraw_funds permission.
public fun set_extension_policy(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount: u64,
) {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    self
        .inner_mut()
        .set_extension_policy(
            expiry_threshold_epochs,
            max_extension_epochs,
            tip_amount,
        );
}

/// Adjusts storage capacity and/or end_epoch.
/// Requires can_withdraw_funds permission.
/// Can only increase values, not decrease them.
/// Uses funds from the coin stash - bypasses storage_purchase_policy.
public fun adjust_storage(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    new_capacity: u64,
    new_end_epoch: u32,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    let manager_id = object::id(self);
    self.inner_mut().adjust_storage(manager_id, system, new_capacity, new_end_epoch, ctx);
}

// === Storage Purchase Policy Configuration ===

/// Sets the storage purchase policy to unlimited (no restrictions).
/// Requires can_withdraw_funds permission.
public fun set_storage_purchase_policy_unlimited(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    self.inner_mut().set_storage_purchase_policy_unlimited();
}

/// Sets the storage purchase policy to constrained.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at max_purchase_bytes.
/// Requires can_withdraw_funds permission.
public fun set_storage_purchase_policy_constrained(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    threshold_bytes: u64,
    max_purchase_bytes: u64,
) {
    check_cap(self, cap);
    assert!(cap.can_withdraw_funds, ERequiresWithdrawFunds);

    self.inner_mut().set_storage_purchase_policy_constrained(threshold_bytes, max_purchase_bytes);
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
