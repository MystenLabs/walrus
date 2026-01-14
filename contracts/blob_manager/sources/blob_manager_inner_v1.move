// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Inner implementation for BlobManager (V1).
/// This module contains the actual business logic, stored as a dynamic field of BlobManager.
/// The outer BlobManager interface handles versioning and capability checking.
module blob_manager::blob_manager_inner_v1;

use std::string::String;
use sui::{coin::{Self, Coin}, sui::SUI, table::{Self, Table}};
use wal::wal::WAL;
use blob_manager::{
    coin_stash::{Self, BlobManagerCoinStash},
    storage_purchase_policy::{Self, StoragePurchasePolicy}
};
use walrus::{
    storage_resource::Storage,
    system::{Self, System},
    unified_storage::{Self, UnifiedStorage, CapacityInfo}
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
/// The blob manager ID does not match the managed blob's blob manager ID.
const EInvalidBlobManagerId: u64 = 15;

// === Main Structures ===

/// The inner implementation of BlobManager containing all business logic fields.
/// Stored as a dynamic field of BlobManager, keyed by version number.
public struct BlobManagerInnerV1 has store {
    /// The UnifiedStorage that holds the actual blobs and capacity.
    storage: UnifiedStorage,
    /// Coin stash for community funding - embedded directly.
    coin_stash: BlobManagerCoinStash,
    /// Storage purchase policy for this BlobManager.
    storage_purchase_policy: StoragePurchasePolicy,
    /// Maps capability IDs to their info, including revocation status.
    caps_info: Table<ID, CapInfo>,
}

/// Information about a capability, including its revocation status.
public struct CapInfo has drop, store {
    is_revoked: bool,
}

// === Constructor ===

/// Creates a new BlobManagerInnerV1 that owns a UnifiedStorage.
/// Called by the BlobManager interface during creation.
/// Returns the inner.
public(package) fun new(
    storage: UnifiedStorage,
    initial_wal: Coin<WAL>,
    ctx: &mut TxContext,
): BlobManagerInnerV1 {
    let mut inner = BlobManagerInnerV1 {
        storage,
        coin_stash: coin_stash::new(),
        storage_purchase_policy: storage_purchase_policy::default(),
        caps_info: table::new(ctx),
    };

    // Deposit initial WAL to the coin stash.
    inner.coin_stash.deposit_wal(initial_wal);

    inner
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
/// Returns ok status (no abort) when:
///   - A matching blob already exists (certified or uncertified) - reuses existing blob
///   - A new blob is successfully created
/// Only aborts on errors:
///   - Insufficient funds
///   - Insufficient storage capacity
///   - Inconsistency detected in blob storage
public(package) fun register_blob(
    self: &mut BlobManagerInnerV1,
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

    // Calculate write price and withdraw from coin stash.
    let n_shards = system::n_shards(system);
    let encoded_size = walrus::encoding::encoded_blob_length(size, encoding_type, n_shards);
    let write_price = system::write_price(system, encoded_size);
    let mut payment = self.coin_stash.withdraw_wal(write_price, ctx);

    // Register managed blob with the system.
    // This atomically: creates the blob, does capacity check, and adds to storage.
    system.register_managed_blob(
        &mut self.storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        blob_type,
        &mut payment,
        ctx,
    );

    // Deposit the remaining WAL back to the coin stash, if any.
    self.coin_stash.deposit_wal(payment);
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

    // Use the UnifiedStorage facade to make the blob permanent.
    self.storage.make_blob_permanent(blob_id, current_epoch, end_epoch);
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

    // Use the UnifiedStorage facade to delete the blob.
    // This will remove it from storage, emit the deletion event, and destroy it.
    let epoch = system::epoch(system);
    self.storage.delete_blob(blob_id, epoch);
}

/// Moves a regular blob (which owns Storage directly) into a BlobManager.
/// The sender must own the blob. The blob must be certified and not expired.
/// Storage epochs are aligned: if blob's end_epoch < manager's, purchase extension using coin
/// stash.
/// If blob's end_epoch > manager's, throw an error.
/// Note: This will increase the storage of the BlobManager, absorbing the storage of the regular
/// blob, and extend the storage of the regular blob if it's not aligned.
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
    let blob_end_epoch = blob.end_epoch();

    // Get storage epochs.
    let manager_end_epoch = self.storage.end_epoch();

    // Validate that blob's end epoch doesn't exceed manager's end epoch.
    assert!(blob_end_epoch <= manager_end_epoch, ERegularBlobEndEpochTooLarge);

    // Convert the regular blob to a managed blob and extract storage.
    let (managed_blob, mut storage) = system.convert_blob_to_managed(
        blob,
        &self.storage,
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

    // Add storage to BlobManager's unified pool.
    self.storage.add_storage(aligned_storage);

    // Add managed blob to BlobManager.
    self.storage.add_blob(managed_blob);
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
    storage: &UnifiedStorage,
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
    let mut payment = coin_stash.withdraw_wal(available_wal, ctx);

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
/// Returns tip coin (in WAL) as reward to the caller.
public(package) fun extend_storage_from_stash(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    extension_epochs: u32,
    ctx: &mut TxContext,
): Coin<WAL> {
    self.verify_storage(system);

    // Get current epoch and storage end epoch.
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();
    let system_max_epochs_ahead = system.future_accounting().max_epochs_ahead();

    // Validate and compute the new end epoch based on policy.
    let new_end_epoch = storage_purchase_policy::validate_and_compute_end_epoch(
        &self.storage_purchase_policy,
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
/// Returns tip_coin (in WAL) as reward for the caller.
fun execute_extension(
    self: &mut BlobManagerInnerV1,
    manager_id: ID,
    system: &mut System,
    new_end_epoch: u32,
    ctx: &mut TxContext,
): Coin<WAL> {
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();

    // If new_end_epoch is not greater than current end epoch, no extension needed.
    if (new_end_epoch <= storage_end_epoch) {
        return coin::zero<WAL>(ctx)
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

    // Calculate and withdraw tip in WAL for the caller.
    // Note: We pass parameters to avoid time multiplier since we don't have Clock access here.
    // Time multiplier only applies when current_epoch >= storage_end_epoch, so we ensure
    // current_epoch < storage_end_epoch by using current_epoch and storage_end_epoch + 1.
    let used_bytes = self.storage.capacity_info().in_use();
    let tip_amount = storage_purchase_policy::calculate_tip(
        &self.storage_purchase_policy,
        used_bytes,
        0, // current_timestamp_ms (unused)
        current_epoch,
        0, // current_epoch_start_ms (unused)
        1000, // epoch_duration_ms (unused)
        storage_end_epoch,
    );
    let tip_coin = self.coin_stash.withdraw_wal_for_tip(tip_amount, ctx);

    // Emit BlobManagerUpdated event using UnifiedStorage facade.
    unified_storage::emit_blob_manager_updated(
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
    let mut payment = self.coin_stash.withdraw_wal(available_wal, ctx);

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

    // Emit BlobManagerUpdated event using UnifiedStorage facade.
    unified_storage::emit_blob_manager_updated(
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

/// Sets extension parameters.
/// To disable extensions, set max_extension_epochs to 0.
/// `tip_amount_frost`: Tip amount in FROST (e.g., 10_000_000_000 = 10 WAL).
/// `last_epoch_multiplier`: Multiplier for last epoch (e.g., 2 = 2x, 1 = no multiplier).
public(package) fun set_extension_params(
    self: &mut BlobManagerInnerV1,
    expiry_threshold_epochs: u32,
    max_extension_epochs: u32,
    tip_amount_frost: u64,
    last_epoch_multiplier: u64,
) {
    storage_purchase_policy::set_extension_params(
        &mut self.storage_purchase_policy,
        expiry_threshold_epochs,
        max_extension_epochs,
        tip_amount_frost,
        last_epoch_multiplier,
    );
}

// === Storage Purchase Policy Configuration ===

/// Sets the capacity policy to unlimited (no restrictions).
public(package) fun set_capacity_unlimited(self: &mut BlobManagerInnerV1) {
    storage_purchase_policy::set_capacity_unlimited(&mut self.storage_purchase_policy);
}

/// Sets the capacity policy to constrained.
/// Only allows purchases when available storage < threshold_bytes.
/// Maximum purchase is capped at max_purchase_bytes.
public(package) fun set_capacity_constrained(
    self: &mut BlobManagerInnerV1,
    threshold_bytes: u64,
    max_purchase_bytes: u64,
) {
    storage_purchase_policy::set_capacity_constrained(
        &mut self.storage_purchase_policy,
        threshold_bytes,
        max_purchase_bytes,
    );
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
