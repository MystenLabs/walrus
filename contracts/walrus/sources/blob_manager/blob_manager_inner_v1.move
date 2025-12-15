// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Inner implementation for BlobManager (V1).
/// This module contains the actual business logic, stored as a dynamic field of BlobManager.
/// The outer BlobManager interface handles versioning and capability checking.
module walrus::blob_manager_inner_v1;

use std::string::String;
use sui::{coin::{Self, Coin}, sui::SUI, table::{Self, Table}};
use wal::wal::WAL;
use walrus::{
    blob_storage::{Self, BlobStorage, CapacityInfo},
    coin_stash::{Self, BlobManagerCoinStash},
    events,
    extension_policy::{Self, ExtensionPolicy},
    managed_blob,
    storage_purchase_policy::{Self, StoragePurchasePolicy},
    storage_resource::Storage,
    system::{Self, System}
};

// === Constants ===

/// Minimum initial capacity for BlobManager (500MB in bytes).
const MIN_INITIAL_CAPACITY: u64 = 500_000_000; // 500 MB.

// === Error Codes ===

/// The provided storage has expired.
const EStorageExpired: u64 = 1;
/// Not enough blob manager storage capacity.
const EInsufficientBlobManagerCapacity: u64 = 2;
/// Initial storage capacity is below minimum requirement.
const EInitialBlobManagerCapacityTooSmall: u64 = 3;
/// The blob already expired.
const EBlobAlreadyExpired: u64 = 5;
/// Operation requires fund_manager permission.
const ERequiresFundManager: u64 = 6;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 7;
/// Store expired.
const EBlobManagerStorageExpired: u64 = 8;
/// The moving-in regular blob's end_epoch too large.
const ERegularBlobEndEpochTooLarge: u64 = 9;
/// Insufficient funds in coin stash for the operation.
const EInsufficientFunds: u64 = 10;
/// The capability has been revoked.
const ECapRevoked: u64 = 11;
/// The capability was not found in the manager's registry.
const ECapNotFound: u64 = 12;
/// Cannot decrease storage capacity via adjust_storage.
const ECannotDecreaseCapacity: u64 = 13;
/// Cannot decrease storage end_epoch via adjust_storage.
const ECannotDecreaseEndEpoch: u64 = 14;

// === Main Structures ===

/// The inner implementation of BlobManager containing all business logic fields.
/// Stored as a dynamic field of BlobManager, keyed by version number.
public struct BlobManagerInnerV1 has store {
    /// Unified storage that handles both capacity management and blob storage.
    storage: BlobStorage,
    /// Coin stash for community funding - embedded directly.
    coin_stash: BlobManagerCoinStash,
    /// Extension policy controlling who can extend storage and under what conditions.
    /// Also includes tip configuration for community extenders.
    extension_policy: ExtensionPolicy,
    /// Storage purchase policy controlling how much storage can be purchased.
    storage_purchase_policy: StoragePurchasePolicy,
    /// Maps capability IDs to their info, including revocation status.
    caps_info: Table<ID, CapInfo>,
}

/// Information about a capability, including its revocation status.
public struct CapInfo has drop, store {
    is_revoked: bool,
}

// === Constructor ===

/// Creates a new BlobManagerInnerV1 with the given initial storage.
/// Called by the BlobManager interface during creation.
/// Returns the inner and the end_epoch for event emission.
public(package) fun new(
    initial_storage: Storage,
    initial_wal: Coin<WAL>,
    system: &System,
    ctx: &mut TxContext,
): (BlobManagerInnerV1, u32) {
    // Check minimum capacity before consuming the storage.
    let capacity = initial_storage.size();
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialBlobManagerCapacityTooSmall);

    let current_epoch = system::epoch(system);
    let end_epoch = initial_storage.end_epoch();
    assert!(end_epoch > current_epoch, EStorageExpired);

    let mut inner = BlobManagerInnerV1 {
        storage: blob_storage::new_unified_blob_storage(initial_storage, current_epoch, ctx),
        coin_stash: coin_stash::new(),
        // Default policy: extend within 2 epochs of expiry, max 5 epochs per extension,
        // with 1000 MIST tip for community extenders.
        extension_policy: extension_policy::default_constrained(),
        // Default storage purchase policy: constrained with 15GB threshold and 15GB max.
        // Only allow purchases when available storage < 15GB, max purchase is 15GB.
        storage_purchase_policy: storage_purchase_policy::default_constrained(),
        caps_info: table::new(ctx),
    };

    // Deposit initial WAL to the coin stash.
    inner.coin_stash.deposit_wal(initial_wal);

    (inner, end_epoch)
}

// === Capability Registry Operations ===

/// Registers a new capability in the caps_info table.
public(package) fun register_cap(self: &mut BlobManagerInnerV1, cap_id: ID) {
    self.caps_info.add(cap_id, CapInfo { is_revoked: false });
}

/// Checks if a capability is registered and not revoked.
/// Returns true if valid, aborts if not found or revoked.
public(package) fun check_cap_valid(self: &BlobManagerInnerV1, cap_id: ID) {
    assert!(self.caps_info.contains(cap_id), ECapNotFound);
    let cap_info = table::borrow(&self.caps_info, cap_id);
    assert!(!cap_info.is_revoked, ECapRevoked);
}

/// Revokes a capability by ID.
public(package) fun revoke_cap(self: &mut BlobManagerInnerV1, cap_id: ID) {
    assert!(table::contains(&self.caps_info, cap_id), ECapNotFound);
    let cap_info = table::borrow_mut(&mut self.caps_info, cap_id);
    cap_info.is_revoked = true;
}

// === Core Operations ===

/// Registers a new blob in the BlobManager.
/// BlobManager owns the blob immediately upon registration.
/// Note: the sender of the register transaction pays for the write fee, although the storage space
/// is accounted from the blob manager's storage.
/// Returns ok status (no abort) when:
///   - A matching blob already exists (certified or uncertified) - reuses existing blob
///   - A new blob is successfully created
/// Only aborts on errors:
///   - Insufficient funds (payment too small)
///   - Insufficient storage capacity
///   - Inconsistency detected in blob storage
public(package) fun register_blob(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    blob_type: u8,
    ctx: &mut TxContext,
) {
    self.verify_storage(system);

    // Check for existing managed blob with same blob_id and deletable flag.
    // If there is a blob with the blob_id but different deletable flag, it throws an error.
    if (self.storage.check_blob_existence(blob_id, deletable)) {
        return
    };

    let capacity_info = self.storage.capacity_info();
    let end_epoch_at_registration = capacity_info.capacity_end_epoch();

    // Calculate write price and withdraw from coin stash.
    let n_shards = system::n_shards(system);
    let encoded_size = walrus::encoding::encoded_blob_length(size, encoding_type, n_shards);
    let write_price = system::write_price(system, encoded_size);
    let mut payment = self.coin_stash.withdraw_wal_for_storage(write_price, ctx);

    // Register managed blob with the system.
    let managed_blob = system.register_managed_blob(
        manager_id,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        blob_type,
        end_epoch_at_registration,
        &mut payment,
        ctx,
    );

    self.coin_stash.deposit_wal(payment);

    assert!(capacity_info.available() >= encoded_size, EInsufficientBlobManagerCapacity);

    // Add blob to storage with atomic allocation (BlobManager now owns it).
    self.storage.add_blob(managed_blob);
}

/// Certifies a managed blob.
public(package) fun certify_blob(
    self: &mut BlobManagerInnerV1,
    system: &System,
    blob_id: u256,
    deletable: bool,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    self.verify_storage(system);

    // Get the current end_epoch from storage for the certification event.
    let end_epoch_at_certify = self.storage.end_epoch();

    let managed_blob = self.storage.get_mut_blob(blob_id);

    // Verify permanency matches.
    assert!(managed_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    // Ignore replay of certify.
    if (managed_blob.certified_epoch().is_some()) {
        return
    };

    system.certify_managed_blob(
        managed_blob,
        end_epoch_at_certify,
        signature,
        signers_bitmap,
        message,
    );
}

/// Converts a deletable managed blob to permanent.
/// This is a one-way operation - permanent blobs cannot be made deletable again.
/// Requires the blob to be deletable.
public(package) fun make_blob_permanent(
    self: &mut BlobManagerInnerV1,
    system: &System,
    blob_id: u256,
) {
    self.verify_storage(system);

    let current_epoch = system::epoch(system);
    let end_epoch = self.storage.end_epoch();
    let managed_blob = self.storage.get_mut_blob(blob_id);

    // Call the managed_blob function to make it permanent and emit event.
    managed_blob.make_permanent(current_epoch, end_epoch);
}

/// Deletes a managed blob from the BlobManager.
/// This removes the blob from storage tracking and emits a deletion event.
/// The blob must be deletable and registered with this BlobManager.
public(package) fun delete_blob(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &System,
    blob_id: u256,
) {
    self.verify_storage(system);

    // Remove the blob from storage and atomically release storage.
    let managed_blob = self.storage.remove_blob(blob_id);

    // Emit the deletion event directly.
    let epoch = system::epoch(system);
    let deleted_blob_id = managed_blob::blob_id(&managed_blob);
    let object_id = managed_blob::object_id(&managed_blob);
    let was_certified = managed_blob::certified_epoch(&managed_blob).is_some();

    // The blob must be destroyed.
    managed_blob::delete_internal(managed_blob);

    events::emit_managed_blob_deleted(
        epoch,
        manager_id,
        deleted_blob_id,
        object_id,
        was_certified,
    );
}

/// Moves a regular blob (which owns Storage directly) into a BlobManager.
/// The sender must own the blob. The blob must be certified and not expired.
/// Storage epochs are aligned: if blob's end_epoch < manager's, purchase extension using coin
/// stash.
/// If blob's end_epoch > manager's, throw an error.
public(package) fun move_blob_into_manager(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    blob: walrus::blob::Blob,
    ctx: &mut TxContext,
) {
    self.verify_storage(system);

    // Get current epoch from system.
    let current_epoch = system::epoch(system);

    assert!(blob.storage().end_epoch() > current_epoch, EBlobAlreadyExpired);

    // Store metadata before conversion.
    let original_object_id = blob.object_id();
    let blob_id = blob.blob_id();
    let size = blob.size();
    let encoding_type = blob.encoding_type();
    let deletable = blob.is_deletable();
    let blob_end_epoch = blob.end_epoch();

    // Get storage epochs.
    let manager_end_epoch = self.storage.end_epoch();

    // Validate that blob's end epoch doesn't exceed manager's end epoch.
    assert!(blob_end_epoch <= manager_end_epoch, ERegularBlobEndEpochTooLarge);

    // Convert the regular blob to a managed blob and extract storage.
    let (managed_blob, mut storage) = system.convert_blob_to_managed(
        blob,
        manager_id,
        ctx,
    );

    // Align storage epochs (only need to handle extension or already aligned cases).
    let aligned_storage = if (blob_end_epoch < manager_end_epoch) {
        // Blob expires before manager - need to extend blob's storage.
        let extension_amount = storage.size();

        // Purchase extension storage using the helper function.
        let extension = purchase_storage_from_stash(
            &mut self.coin_stash,
            &self.storage_purchase_policy,
            &self.storage,
            system,
            extension_amount,
            blob_end_epoch,
            manager_end_epoch,
            ctx,
        );

        // Fuse extension into the storage.
        storage.fuse_periods(extension);

        storage
    } else {
        // Already aligned (blob_end_epoch == manager_end_epoch).
        storage
    };

    // Get the new object ID from managed blob.
    let new_object_id = managed_blob.object_id();

    // Add storage to BlobManager's unified pool.
    self.storage.add_storage(aligned_storage);

    // Add managed blob to BlobManager.
    self.storage.add_blob(managed_blob);

    // Emit event.
    events::emit_blob_moved_into_blob_manager(
        current_epoch,
        blob_id,
        manager_id,
        original_object_id,
        size,
        encoding_type,
        new_object_id,
        deletable,
    );
}

// === Helper Functions ===

/// Make sure the storage does not expire.
fun verify_storage(self: &BlobManagerInnerV1, system: &System) {
    let current_epoch = system::epoch(system);
    assert!(self.storage.end_epoch() > current_epoch, EBlobManagerStorageExpired);
}

/// Helper function to purchase storage using funds from the coin stash.
/// Withdraws funds, purchases storage for the specified epoch range, and returns unused funds.
/// Returns the purchased Storage object.
fun purchase_storage_from_stash(
    coin_stash: &mut BlobManagerCoinStash,
    storage_purchase_policy: &StoragePurchasePolicy,
    storage: &BlobStorage,
    system: &mut System,
    amount: u64,
    start_epoch: u32,
    end_epoch: u32,
    ctx: &mut TxContext,
): Storage {
    // Get current available storage for policy validation.
    let capacity_info = storage.capacity_info();
    let available_storage = capacity_info.available();

    // Validate and cap the purchase amount using the unified policy method.
    let capped_amount = storage_purchase_policy.validate_purchase(amount, available_storage);

    // Get available WAL balance from stash.
    let available_wal = coin_stash.wal_balance();
    assert!(available_wal > 0, ERequiresFundManager);

    // Withdraw all available funds from stash for payment.
    let mut payment = coin_stash.withdraw_wal_for_storage(available_wal, ctx);

    // Purchase storage from system using the capped amount.
    let storage = system::reserve_space_for_epochs(
        system,
        capped_amount,
        start_epoch,
        end_epoch,
        &mut payment,
        ctx,
    );

    coin_stash.deposit_wal(payment);

    storage
}

// === Coin Stash Operations ===

/// Deposits WAL coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support storage operations.
public(package) fun deposit_wal_to_coin_stash(self: &mut BlobManagerInnerV1, payment: Coin<WAL>) {
    // Simply deposit to coin stash.
    self.coin_stash.deposit_wal(payment);
}

/// Deposits SUI coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support gas operations.
public(package) fun deposit_sui_to_coin_stash(self: &mut BlobManagerInnerV1, payment: Coin<SUI>) {
    // Simply deposit to coin stash.
    self.coin_stash.deposit_sui(payment);
}

/// Buys additional storage capacity using funds from the coin stash.
/// The new storage uses the same epoch range as the existing storage.
/// This only increases capacity, not the end_epoch.
public(package) fun buy_storage_from_stash(
    self: &mut BlobManagerInnerV1,
    system: &mut System,
    storage_amount: u64,
    ctx: &mut TxContext,
) {
    self.verify_storage(system);

    let start_epoch = system::epoch(system);
    let end_epoch = self.storage.end_epoch();

    // Purchase storage with the same epoch range.
    let new_storage = purchase_storage_from_stash(
        &mut self.coin_stash,
        &self.storage_purchase_policy,
        &self.storage,
        system,
        storage_amount,
        start_epoch,
        end_epoch,
        ctx,
    );

    // Add the new storage to the BlobManager's pool.
    self.storage.add_storage(new_storage);
}

/// Extends the storage period using funds from the coin stash.
/// Must follow the extension policy constraints.
/// Returns tip coin as reward to the caller.
public(package) fun extend_storage_from_stash(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    extension_epochs: u32,
    ctx: &mut TxContext,
): Coin<SUI> {
    self.verify_storage(system);

    // Get current epoch and storage end epoch.
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();
    let system_max_epochs_ahead = system.future_accounting().max_epochs_ahead();

    // Validate and compute the new end epoch based on policy.
    let new_end_epoch = self
        .extension_policy
        .validate_and_compute_end_epoch(
            current_epoch,
            storage_end_epoch,
            extension_epochs,
            system_max_epochs_ahead,
        );

    // Execute the extension to the new end epoch.
    self.execute_extension(manager_id, system, new_end_epoch, ctx)
}

/// Internal helper to execute the storage extension.
/// Extends storage to the specified new_end_epoch.
/// Returns tip_coin as reward for the caller.
fun execute_extension(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    new_end_epoch: u32,
    ctx: &mut TxContext,
): Coin<SUI> {
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();

    // If new_end_epoch is not greater than current end epoch, no extension needed.
    if (new_end_epoch <= storage_end_epoch) {
        return coin::zero<SUI>(ctx)
    };

    // Get available WAL balance from stash.
    let available_wal = self.coin_stash.wal_balance();
    assert!(available_wal > 0, EInsufficientFunds);

    // Get current storage info.
    let capacity_info = self.storage.capacity_info();
    let total_capacity = capacity_info.total();

    // Purchase the extension storage from current end_epoch to new_end_epoch.
    let extension_storage = purchase_storage_from_stash(
        &mut self.coin_stash,
        &self.storage_purchase_policy,
        &self.storage,
        system,
        total_capacity,
        storage_end_epoch,
        new_end_epoch,
        ctx,
    );

    // Apply the extension to the BlobManager's storage.
    self.storage.extend_storage(extension_storage);

    // Get tip coin for the caller from extension policy.
    let tip_amount = self.extension_policy.get_tip_amount();
    let tip_coin = if (tip_amount > 0) {
        self.coin_stash.withdraw_sui_for_tip(tip_amount, ctx)
    } else {
        coin::zero<SUI>(ctx)
    };

    // Emit BlobManagerUpdated event.
    events::emit_blob_manager_updated(
        current_epoch,
        manager_id,
        new_end_epoch,
    );

    tip_coin
}

/// Adjusts storage capacity and/or end_epoch. Requires can_withdraw_funds permission.
/// Can only increase values, not decrease them.
/// Uses funds from the coin stash. Bypasses storage_purchase_policy constraints.
public(package) fun adjust_storage(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    new_capacity: u64,
    new_end_epoch: u32,
    ctx: &mut TxContext,
) {
    self.verify_storage(system);

    let current_epoch = system::epoch(system);
    let capacity_info = self.storage.capacity_info();
    let current_capacity = capacity_info.total();
    let current_end_epoch = self.storage.end_epoch();

    // Validate: can only increase, not decrease.
    assert!(new_capacity >= current_capacity, ECannotDecreaseCapacity);
    assert!(new_end_epoch >= current_end_epoch, ECannotDecreaseEndEpoch);

    // Calculate what we need to purchase.
    let capacity_increase = new_capacity - current_capacity;
    let epoch_extension = (new_end_epoch as u64) - (current_end_epoch as u64);

    // Skip if no change needed.
    if (capacity_increase == 0 && epoch_extension == 0) {
        return
    };

    // Get available WAL balance from stash.
    let available_wal = self.coin_stash.wal_balance();
    assert!(available_wal > 0, EInsufficientFunds);

    // Withdraw all available funds from stash for payment.
    let mut payment = self.coin_stash.withdraw_wal_for_storage(available_wal, ctx);

    // Handle the different cases:
    // 1. Only capacity increase (same end_epoch)
    // 2. Only epoch extension (same capacity)
    // 3. Both capacity increase and epoch extension

    if (capacity_increase > 0 && epoch_extension == 0) {
        // Case 1: Only capacity increase.
        // Purchase storage from current_epoch to current_end_epoch.
        let new_storage = system::reserve_space_for_epochs(
            system,
            capacity_increase,
            current_epoch,
            current_end_epoch,
            &mut payment,
            ctx,
        );
        self.storage.add_storage(new_storage);
    } else if (capacity_increase == 0 && epoch_extension > 0) {
        // Case 2: Only epoch extension.
        // Purchase extension for existing capacity from current_end_epoch to new_end_epoch.
        let extension_storage = system::reserve_space_for_epochs(
            system,
            current_capacity,
            current_end_epoch,
            new_end_epoch,
            &mut payment,
            ctx,
        );
        self.storage.extend_storage(extension_storage);
    } else {
        // Case 3: Both capacity increase and epoch extension.
        // First, purchase extension for existing capacity.
        let extension_storage = system::reserve_space_for_epochs(
            system,
            current_capacity,
            current_end_epoch,
            new_end_epoch,
            &mut payment,
            ctx,
        );
        self.storage.extend_storage(extension_storage);

        // Then, purchase the capacity increase for the full period.
        let capacity_storage = system::reserve_space_for_epochs(
            system,
            capacity_increase,
            current_epoch,
            new_end_epoch,
            &mut payment,
            ctx,
        );
        self.storage.add_storage(capacity_storage);
    };

    // Return unused funds to stash.
    self.coin_stash.deposit_wal(payment);

    // Emit BlobManagerUpdated event.
    events::emit_blob_manager_updated(
        current_epoch,
        manager_id,
        new_end_epoch,
    );
}

// === Fund Manager Functions ===

/// Withdraws a specific amount of WAL funds from the coin stash.
public(package) fun withdraw_wal(
    self: &mut BlobManagerInnerV1,
    amount: u64,
    ctx: &mut TxContext,
): Coin<WAL> {
    // Withdraw the specified amount of WAL.
    self.coin_stash.withdraw_wal(amount, ctx)
}

/// Withdraws a specific amount of SUI funds from the coin stash.
public(package) fun withdraw_sui(
    self: &mut BlobManagerInnerV1,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    // Withdraw the specified amount of SUI.
    self.coin_stash.withdraw_sui(amount, ctx)
}

/// Sets the extension policy to disabled (no one can extend).
public(package) fun set_extension_policy_disabled(self: &mut BlobManagerInnerV1) {
    self.extension_policy = extension_policy::disabled();
}

/// Sets the extension policy to constrained with the given parameters.
public(package) fun set_extension_policy_constrained(
    self: &mut BlobManagerInnerV1,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount: u64,
) {
    self.extension_policy =
        extension_policy::constrained(
            expiry_threshold_epochs,
            max_extension_epochs,
            tip_amount,
        );
}

// === Storage Purchase Policy Configuration ===

/// Sets the storage purchase policy to unlimited (no restrictions).
public(package) fun set_storage_purchase_policy_unlimited(self: &mut BlobManagerInnerV1) {
    self.storage_purchase_policy = storage_purchase_policy::unlimited();
}

/// Sets the storage purchase policy to constrained.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at max_purchase_bytes.
public(package) fun set_storage_purchase_policy_constrained(
    self: &mut BlobManagerInnerV1,
    threshold_bytes: u64,
    max_purchase_bytes: u64,
) {
    self.storage_purchase_policy =
        storage_purchase_policy::constrained(threshold_bytes, max_purchase_bytes);
}

// === Blob Attribute Operations ===

/// Sets an attribute on a managed blob.
///
/// If the key already exists, the value is updated.
/// Aborts if the blob is not found or if attribute limits are exceeded.
public(package) fun set_blob_attribute(
    self: &mut BlobManagerInnerV1,
    blob_id: u256,
    key: String,
    value: String,
) {
    // Get the managed blob and set the attribute.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.set_attribute(key, value);
}

/// Removes an attribute from a managed blob.
///
/// Aborts if the blob is not found or if the attribute key doesn't exist.
public(package) fun remove_blob_attribute(
    self: &mut BlobManagerInnerV1,
    blob_id: u256,
    key: String,
) {
    // Get the managed blob and remove the attribute.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.remove_attribute(&key);
}

/// Clears all attributes from a managed blob.
///
/// Aborts if the blob is not found.
public(package) fun clear_blob_attributes(self: &mut BlobManagerInnerV1, blob_id: u256) {
    // Get the managed blob and clear all attributes.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.clear_attributes();
}

// === Accessors ===

/// Returns the end epoch of the storage.
public(package) fun end_epoch(self: &BlobManagerInnerV1): u32 {
    self.storage.end_epoch()
}

/// Returns the capacity info of the storage.
public(package) fun capacity_info(self: &BlobManagerInnerV1): CapacityInfo {
    self.storage.capacity_info()
}

/// Returns the WAL balance in the coin stash.
public(package) fun wal_balance(self: &BlobManagerInnerV1): u64 {
    self.coin_stash.wal_balance()
}

/// Returns the SUI balance in the coin stash.
public(package) fun sui_balance(self: &BlobManagerInnerV1): u64 {
    self.coin_stash.sui_balance()
}
