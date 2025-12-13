// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Minimal blob-management interface using enum-based storage strategies.
/// This version provides only essential functionality: new, register, and certify.
module walrus::blobmanager;

use std::string::String;
use sui::{coin::{Self, Coin}, sui::SUI};
use wal::wal::WAL;
use walrus::{
    blob_storage::{Self, BlobStorage, CapacityInfo},
    coin_stash::{Self, BlobManagerCoinStash},
    events,
    extension_policy::{Self, ExtensionPolicy},
    managed_blob,
    storage_purchase_policy::{Self, StoragePurchasePolicy},
    storage_resource::{Self, Storage},
    system::{Self, System},
    tip_policy::{Self, TipPolicy}
};

// === Constants ===

/// Minimum initial capacity for BlobManager (500MB in bytes).
const MIN_INITIAL_CAPACITY: u64 = 500_000_000; // 500 MB.

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// The provided storage has expired.
const EStorageExpired: u64 = 1;
/// Not enough blob manager storage capacity.
const EInsufficientBlobManagerCapacity: u64 = 2;
/// Initial storage capacity is below minimum requirement.
const EInitialBlobManagerCapacityTooSmall: u64 = 3;
/// The blob already expired.
const EBlobAlreadyExpired: u64 = 4;
/// Operation requires Admin capability.
const ERequiresAdminCap: u64 = 5;
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

// === Main Structures ===

/// The minimal blob-management interface.
public struct BlobManager has key, store {
    id: UID,
    /// Unified storage that handles both capacity management and blob storage.
    storage: BlobStorage,
    /// Coin stash for community funding - embedded directly.
    coin_stash: BlobManagerCoinStash,
    /// Extension policy controlling who can extend storage and under what conditions.
    extension_policy: ExtensionPolicy,
    /// Tip policy controlling how much to tip transaction senders for helping with operations.
    tip_policy: TipPolicy,
    /// Storage purchase policy controlling how much storage can be purchased.
    storage_purchase_policy: StoragePurchasePolicy,
}

/// A capability which represents the authority to manage blobs in the BlobManager.
/// Admin can create new capabilities and perform all operations.
/// Fund manager can withdraw funds from the coin stash.
/// TODO(heliu): revocation for caps.
public struct BlobManagerCap has key, store {
    id: UID,
    /// The BlobManager this capability is for.
    manager_id: ID,
    /// Whether this capability has admin permissions (can create new caps).
    is_admin: bool,
    /// Whether this capability has fund manager permissions (can withdraw funds).
    fund_manager: bool,
}

// === Constructors ===

/// Creates a new shared BlobManager and returns its admin capability.
/// The BlobManager is automatically shared in the same transaction.
/// Requires minimum initial capacity of 500MB.
/// Note: This function requires access to System to emit the creation event with proper epoch.
public fun new_with_unified_storage(
    initial_storage: Storage,
    system: &System,
    ctx: &mut TxContext,
): BlobManagerCap {
    // Check minimum capacity before consuming the storage.
    let capacity = initial_storage.size();
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialBlobManagerCapacityTooSmall);

    let current_epoch = system::epoch(system);
    let end_epoch = initial_storage.end_epoch();
    assert!(end_epoch > current_epoch, EStorageExpired);

    // Create the manager object.
    let manager_uid = object::new(ctx);

    let manager = BlobManager {
        id: manager_uid,
        storage: blob_storage::new_unified_blob_storage(initial_storage, current_epoch, ctx),
        coin_stash: coin_stash::new(),
        // Default policy: extend within 1 epoch of expiry, max 10 epochs per extension.
        extension_policy: extension_policy::default_constrained(),
        // Default tip policy: 1000 MIST (0.001 SUI) fixed amount.
        tip_policy: tip_policy::default_fixed_amount(),
        // Default storage purchase policy: conditional purchase with 15GB threshold.
        // Only allow purchases when available storage < 15GB, max purchase is 15GB.
        storage_purchase_policy: storage_purchase_policy::default_conditional_purchase(),
    };

    // Get the ObjectID from the constructed manager object.
    let manager_object_id = object::id(&manager);

    // Create an Admin capability with fund_manager permissions.
    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_object_id,
        is_admin: true,
        fund_manager: true,
    };

    // Emit creation event.
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
    self: &BlobManager,
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

    BlobManagerCap {
        id: object::new(ctx),
        manager_id: object::id(self),
        is_admin,
        fund_manager,
    }
}

/// Returns the manager ID from a capability.
public fun cap_manager_id(cap: &BlobManagerCap): ID {
    cap.manager_id
}

/// Checks if the capability is an Admin capability.
public fun is_admin_cap(cap: &BlobManagerCap): bool {
    cap.is_admin
}

/// Checks if the capability has fund manager permissions.
public fun is_fund_manager_cap(cap: &BlobManagerCap): bool {
    cap.fund_manager
}

/// Checks that the given BlobManagerCap matches the BlobManager.
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    let manager_id = cap_manager_id(cap);
    assert!(object::id(self) == manager_id, EInvalidBlobManagerCap);
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
/// Returns the end_epoch of the storage when successful, or 0 if blob already exists.
/// Returns ok status (no abort) when:
///   - A matching blob already exists (certified or uncertified) - reuses existing blob
///   - A new blob is successfully created
/// Only aborts on errors:
///   - Insufficient funds (payment too small)
///   - Insufficient storage capacity
///   - Inconsistency detected in blob storage
/// TODO(heliu): Decide whether we should also use coin stash for write fee.
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
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
) {
    // Verify the capability.
    check_cap(self, cap);

    self.verify_storage(system);

    // Check for existing managed blob with same blob_id and deletable flag.
    // If there is a blob with the blob_id but different deletable flag, it throws an error.
    if (self.storage.check_blob_existence(blob_id, deletable)) {
        return
    };

    let capacity_info = self.storage.capacity_info();
    let end_epoch_at_registration = capacity_info.capacity_end_epoch();

    // Register managed blob with the system.
    let managed_blob = system.register_managed_blob(
        object::id(self),
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        blob_type,
        end_epoch_at_registration,
        payment,
        ctx,
    );

    // Get the cached encoded size.
    let encoded_size = managed_blob.encoded_size();

    assert!(capacity_info.available() >= encoded_size, EInsufficientBlobManagerCapacity);

    // Add blob to storage with atomic allocation (BlobManager now owns it).
    self.storage.add_blob(managed_blob);
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
    // Verify the capability.
    check_cap(self, cap);

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
    // Verify the capability.
    check_cap(self, cap);

    self.verify_storage(system);

    // Remove the blob from storage and atomically release storage.
    let managed_blob = self.storage.remove_blob(blob_id);

    // Emit the deletion event directly.
    let epoch = system::epoch(system);
    let blob_manager_id = object::id(self);
    let deleted_blob_id = managed_blob::blob_id(&managed_blob);
    let object_id = managed_blob::object_id(&managed_blob);
    let was_certified = managed_blob::certified_epoch(&managed_blob).is_some();

    // The blob must be destroyed.
    managed_blob::delete_internal(managed_blob);

    events::emit_managed_blob_deleted(
        epoch,
        blob_manager_id,
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
public fun move_blob_into_manager(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    blob: walrus::blob::Blob,
    ctx: &mut TxContext,
) {
    // Verify the capability.
    check_cap(self, cap);

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
        object::id(self),
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

    // Emit event with deletable field.
    events::emit_blob_moved_into_blob_manager(
        current_epoch,
        blob_id,
        object::id(self),
        original_object_id,
        size,
        encoding_type,
        new_object_id,
        deletable,
    );
}

// === Helper Functions ===

/// Make sure the storage does not expire.
fun verify_storage(self: &BlobManager, system: &System) {
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
    // Get current available storage for conditional purchase validation.
    let capacity_info = storage.capacity_info();
    let available_storage = capacity_info.available();

    // Validate conditional purchase if applicable.
    storage_purchase_policy::validate_conditional_purchase(
        storage_purchase_policy,
        available_storage,
    );

    // Apply storage purchase policy to cap the amount.
    let capped_amount = storage_purchase_policy::validate_and_cap_purchase(
        storage_purchase_policy,
        amount,
    );

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

    // Return unused funds to the stash.
    if (payment.value() > 0) {
        coin_stash.deposit_wal(payment);
    } else {
        coin::destroy_zero(payment);
    };

    storage
}

// === Coin Stash Operations ===

/// Deposits WAL coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support storage operations.
/// TODO(heliu): disallow deposit when storage expires.
public fun deposit_wal_to_coin_stash(self: &mut BlobManager, payment: Coin<WAL>) {
    // Simply deposit to coin stash.
    self.coin_stash.deposit_wal(payment);
}

/// Deposits SUI coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support gas operations.
public fun deposit_sui_to_coin_stash(self: &mut BlobManager, payment: Coin<SUI>) {
    // Simply deposit to coin stash.
    self.coin_stash.deposit_sui(payment);
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
    // Verify the capability.
    check_cap(self, cap);

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
public fun extend_storage_from_stash(
    self: &mut BlobManager,
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
    self.execute_extension(system, new_end_epoch, ctx)
}

/// Internal helper to execute the storage extension.
/// Extends storage to the specified new_end_epoch.
/// Returns tip_coin as reward for the caller.
fun execute_extension(
    self: &mut BlobManager,
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

    // Get tip coin for the caller.
    let tip_amount = self.tip_policy.get_tip_amount();
    let tip_coin = if (tip_amount > 0) {
        self.coin_stash.withdraw_sui_for_tip(tip_amount, ctx)
    } else {
        coin::zero<SUI>(ctx)
    };

    // Emit BlobManagerUpdated event.
    events::emit_blob_manager_updated(
        current_epoch,
        object::id(self),
        new_end_epoch,
    );

    tip_coin
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
    // Verify the capability.
    check_cap(self, cap);
    // Ensure the caller has fund_manager permission.
    assert!(cap.fund_manager, ERequiresFundManager);

    // Withdraw the specified amount of WAL.
    self.coin_stash.withdraw_wal(amount, ctx)
}

/// Withdraws a specific amount of SUI funds from the coin stash.
/// Requires fund_manager permission.
public fun withdraw_sui(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    amount: u64,
    ctx: &mut TxContext,
): Coin<SUI> {
    // Verify the capability.
    check_cap(self, cap);
    // Ensure the caller has fund_manager permission.
    assert!(cap.fund_manager, ERequiresFundManager);

    // Withdraw the specified amount of SUI.
    self.coin_stash.withdraw_sui(amount, ctx)
}

/// Sets the extension policy to disabled (no one can extend).
/// Requires fund_manager permission.
public fun set_extension_policy_disabled(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);
    self.extension_policy = extension_policy::disabled();
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
    self.extension_policy =
        extension_policy::constrained(
            expiry_threshold_epochs,
            max_extension_epochs,
        );
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
    self.tip_policy = tip_policy::new_fixed_amount(tip_amount);
}

// === Storage Purchase Policy Configuration ===

/// Sets the storage purchase policy to unlimited (no restrictions).
/// Requires fund_manager permission.
public fun set_storage_purchase_policy_unlimited(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);
    self.storage_purchase_policy = storage_purchase_policy::unlimited();
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
    self.storage_purchase_policy = storage_purchase_policy::fixed_cap(max_storage_bytes);
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
    self.storage_purchase_policy = storage_purchase_policy::conditional_purchase(threshold_bytes);
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
    // Verify the capability.
    check_cap(self, cap);

    // Get the managed blob and set the attribute.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.set_attribute(key, value);
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
    // Verify the capability.
    check_cap(self, cap);

    // Get the managed blob and remove the attribute.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.remove_attribute(&key);
}

/// Clears all attributes from a managed blob.
///
/// Aborts if the blob is not found.
/// Requires a valid BlobManagerCap to prove write access.
public fun clear_blob_attributes(self: &mut BlobManager, cap: &BlobManagerCap, blob_id: u256) {
    // Verify the capability.
    check_cap(self, cap);

    // Get the managed blob and clear all attributes.
    let managed_blob = self.storage.get_mut_blob(blob_id);
    managed_blob.clear_attributes();
}
