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
    encoding,
    events,
    extension_policy::{Self, ExtensionPolicy},
    storage_resource::{Self, Storage},
    system::{Self, System},
    tip_policy::{Self, TipPolicy}
};

// === Constants ===

/// Minimum initial capacity for BlobManager (500MB in bytes).
const MIN_INITIAL_CAPACITY: u64 = 500_000_000; // 500 MB.

// Grace period calculation thresholds (based on storage duration in epochs)
const GRACE_PERIOD_DURATION_THRESHOLD_5: u32 = 5;
const GRACE_PERIOD_DURATION_THRESHOLD_10: u32 = 10;
const GRACE_PERIOD_DURATION_THRESHOLD_20: u32 = 20;
const GRACE_PERIOD_DURATION_THRESHOLD_35: u32 = 35;
const GRACE_PERIOD_DURATION_THRESHOLD_60: u32 = 60;

// Grace period growth parameters
const MAX_GRACE_PERIOD_EPOCHS: u32 = 7;
const GRACE_PERIOD_GROWTH_INTERVAL: u32 = 20;

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// Initial storage capacity is below minimum requirement.
const EInitialBlobManagerCapacityTooSmall: u64 = 3;
/// The blob is already certified (cannot register or certify again).
const EBlobAlreadyCertifiedInBlobManager: u64 = 4;
/// The requested blob was not found in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 2;
/// Operation requires Admin capability.
const ERequiresAdminCap: u64 = 5;
/// Operation requires fund_manager permission.
const ERequiresFundManager: u64 = 6;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 7;
/// Extension attempted beyond grace period.
const EBeyondGracePeriod: u64 = 8;
/// Grace period exceeds maximum allowed for BlobManager age.
const EGracePeriodExceedsMaximum: u64 = 9;

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
    /// Grace period in epochs after storage expiry before blobs become eligible for GC.
    grace_period_epochs: u32,
}

/// A capability which represents the authority to manage blobs in the BlobManager.
/// Admin can create new capabilities and perform all operations.
/// Fund manager can withdraw funds from the coin stash.
/// revocation for caps.
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

/// Creates a new shared BlobManager with deterministic grace period and returns its capability.
/// The BlobManager is automatically shared in the same transaction.
/// Requires minimum initial capacity of 100MB.
/// Note: This function requires access to System to emit the creation event with proper epoch.
public fun new_with_unified_storage(
    initial_storage: Storage,
    system: &System,
    ctx: &mut TxContext,
): BlobManagerCap {
    // Check minimum capacity before consuming the storage.
    let capacity = initial_storage.size();
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialBlobManagerCapacityTooSmall);

    // Calculate grace period based on storage duration.
    let current_epoch = system::epoch(system);
    let end_epoch = storage_resource::end_epoch(&initial_storage);
    let storage_duration = end_epoch - current_epoch;
    let grace_period_epochs = calculate_grace_period(storage_duration);

    // Create the manager object.
    let manager_uid = object::new(ctx);

    let manager = BlobManager {
        id: manager_uid,
        storage: blob_storage::new_unified_blob_storage(initial_storage, ctx),
        coin_stash: coin_stash::new(),
        // Default policy: extend within 1 epoch of expiry, max 10 epochs per extension.
        extension_policy: extension_policy::constrained(1, 10),
        // Default tip policy: 1000 MIST (0.001 SUI) fixed amount.
        tip_policy: tip_policy::new_fixed_amount(1000),
        grace_period_epochs,
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
        grace_period_epochs,
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
    assert!(is_admin_cap(cap), ERequiresAdminCap);
}

/// Calculates grace period based on duration.
/// Uses thresholds: <5: 0, <10: 1, <20: 2, <35: 3, <60: 4, >=60: 4 + floor((delta-60)/20).
/// Maximum grace period is capped at 7 epochs.
fun calculate_grace_period(duration: u32): u32 {
    let grace = if (duration < GRACE_PERIOD_DURATION_THRESHOLD_5) {
        0
    } else if (duration < GRACE_PERIOD_DURATION_THRESHOLD_10) {
        1
    } else if (duration < GRACE_PERIOD_DURATION_THRESHOLD_20) {
        2
    } else if (duration < GRACE_PERIOD_DURATION_THRESHOLD_35) {
        3
    } else if (duration < GRACE_PERIOD_DURATION_THRESHOLD_60) {
        4
    } else {
        // For durations >= 60, add 1 for every 20 epochs beyond 60
        4 + ((duration - GRACE_PERIOD_DURATION_THRESHOLD_60) / GRACE_PERIOD_GROWTH_INTERVAL)
    };

    // Cap at maximum grace period
    if (grace > MAX_GRACE_PERIOD_EPOCHS) {
        MAX_GRACE_PERIOD_EPOCHS
    } else {
        grace
    }
}

// === Core Operations ===

/// Registers a new blob in the BlobManager.
/// BlobManager owns the blob immediately upon registration.
/// Requires a valid BlobManagerCap to prove write access.
/// Returns the end_epoch of the storage when successful, or 0 if blob already exists.
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
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): u32 {
    // Verify the capability.
    check_cap(self, cap);

    // Step 1: Check for existing managed blob with same blob_id and deletable flag.
    let existing_blob = self
        .storage
        .find_blob(
            blob_id,
            deletable,
        );
    if (existing_blob.is_some()) {
        // Blob already exists (certified or uncertified) - reuse it, skip registration.
        // Return 0 to indicate blob already exists.
        return 0
    };

    // Step 2: Register managed blob and allocate storage atomically.
    // Calculate encoded size for storage.
    let n_shards = system::n_shards(system);
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Get the end_epoch from the storage.
    let (_start_epoch, end_epoch_at_registration) = self.storage.storage_epochs();

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

    // Add blob to storage with atomic allocation (BlobManager now owns it).
    self.storage.add_blob(managed_blob, encoded_size);

    // Return the end_epoch for client use.
    end_epoch_at_registration
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

    // Get the current end_epoch from storage for the certification event.
    let (_start_epoch, end_epoch_at_certify) = self.storage.storage_epochs();

    let managed_blob = self.storage.get_mut_blob(blob_id);
    // Verify permanency matches
    assert!(managed_blob.is_deletable() == deletable, EBlobPermanencyConflict);
    assert!(!managed_blob.certified_epoch().is_some(), EBlobAlreadyCertifiedInBlobManager);

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

    // Get the current epoch from the system.
    let epoch = system::epoch(system);

    // Calculate encoded size before removal (needed for atomic storage release).
    let n_shards = system::n_shards(system);
    // We need to get the blob first to get its size and encoding type.
    let existing_blob = self.storage.get_mut_blob(blob_id);
    let blob_size = existing_blob.size();
    let encoding_type = existing_blob.encoding_type();
    let encoded_size = encoding::encoded_blob_length(blob_size, encoding_type, n_shards);

    // Remove the blob from storage and atomically release storage.
    let managed_blob = self.storage.remove_blob(blob_id, encoded_size);

    // Delete the managed blob (this emits the ManagedBlobDeleted event).
    managed_blob.delete(epoch);
}

// === Coin Stash Operations ===

/// Deposits WAL coins to the BlobManager's coin stash.
/// Anyone can deposit funds to support storage operations.
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

/// Buys additional storage using funds from the coin stash.
/// Anyone can call this to extend the BlobManager's capacity.
/// Returns true if successful, false if insufficient funds in stash.
public fun buy_storage_from_stash(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    storage_amount: u64,
    epochs_ahead: u32,
    ctx: &mut TxContext,
): bool {
    // Verify the capability, anyone with a cap can buy storage.
    check_cap(self, cap);

    // Get available WAL balance from stash.
    let available_wal = self.coin_stash.wal_balance();
    if (available_wal == 0) {
        return false
    };

    // Withdraw all available funds from stash for payment.
    // The system will return any unused funds as change.
    let mut payment = self.coin_stash.withdraw_wal_for_storage(available_wal, ctx);

    // Purchase storage from system.
    let new_storage = system::reserve_space(
        system,
        storage_amount,
        epochs_ahead,
        &mut payment,
        ctx,
    );

    // Return any unused funds to the stash.
    if (payment.value() > 0) {
        self.coin_stash.deposit_wal(payment);
    } else {
        coin::destroy_zero(payment);
    };

    // Add the new storage to the BlobManager.
    self.storage.add_storage(new_storage);

    true
}

/// TODO(heliu): optional permanent deletion.

/// Extends the storage period using funds from the coin stash.
/// Public extension - must follow the extension policy constraints.
/// Returns the actual extension epochs applied (may be less than requested due to caps).
/// Returns 0 if extension is not possible (insufficient funds or no extension allowed).
public fun extend_storage_from_stash(
    self: &mut BlobManager,
    system: &mut System,
    extension_epochs: u32,
    ctx: &mut TxContext,
): u32 {
    // Get current epoch and storage end epoch.
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();
    let system_max_epochs_ahead = system.future_accounting().max_epochs_ahead();

    // Validate and cap extension based on policy (public extension).
    let effective_extension = self
        .extension_policy
        .validate_and_cap_extension(
            current_epoch,
            storage_end_epoch,
            extension_epochs,
            system_max_epochs_ahead,
        );

    // Execute the extension with the effective epochs.
    self.execute_extension(system, effective_extension, ctx)
}

/// Extends the storage period using funds from the coin stash.
/// Fund manager extension - bypasses policy time/amount constraints.
/// Returns the actual extension epochs applied (may be less than requested due to system cap).
/// Returns 0 if extension is not possible (insufficient funds or policy disabled).
public fun extend_storage_from_stash_fund_manager(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    extension_epochs: u32,
    ctx: &mut TxContext,
): u32 {
    // Verify the capability and fund_manager permission.
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    // Get current epoch and storage end epoch.
    let current_epoch = system::epoch(system);
    let storage_end_epoch = self.storage.end_epoch();
    let system_max_epochs_ahead = system.future_accounting().max_epochs_ahead();

    // Validate and cap extension based on policy (fund_manager extension).
    let effective_extension = extension_policy::validate_and_cap_extension_fund_manager(
        current_epoch,
        storage_end_epoch,
        extension_epochs,
        system_max_epochs_ahead,
    );

    // Execute the extension with the effective epochs.
    self.execute_extension(system, effective_extension, ctx)
}

/// Internal helper to execute the storage extension.
/// Returns the effective extension epochs, or 0 if no extension possible.
fun execute_extension(
    self: &mut BlobManager,
    system: &mut System,
    effective_extension: u32,
    ctx: &mut TxContext,
): u32 {
    // If no extension possible after capping, return 0.
    if (effective_extension == 0) {
        return 0
    };

    // Get available WAL balance from stash.
    let available_wal = self.coin_stash.wal_balance();
    if (available_wal == 0) {
        return 0
    };

    // Get current storage info.
    let capacity_info = self.storage.capacity_info();
    let total_capacity = capacity_info.total();
    let storage_end_epoch = capacity_info.capacity_end_epoch();
    let current_epoch = system::epoch(system);

    // Withdraw all available funds from stash for payment.
    let mut payment = self.coin_stash.withdraw_wal_for_storage(available_wal, ctx);

    // Phase 1: Handle dormant mode compensation if needed.
    let compensation_epochs = if (current_epoch > storage_end_epoch) {
        // Dormant mode: BlobManager has expired, need to compensate for past epochs.
        // Check if still within grace period.
        let grace_end_epoch = storage_end_epoch + self.grace_period_epochs;
        assert!(current_epoch <= grace_end_epoch, EBeyondGracePeriod);

        // Calculate compensation epochs needed (how many epochs we're past expiry).
        let past_epochs = current_epoch - storage_end_epoch;

        // Buy compensation storage for the past epochs (dormant period).
        // We purchase this to pay for the dormant period but don't apply it.
        let compensation_storage = system::reserve_space_for_epochs(
            system,
            total_capacity,
            storage_end_epoch,
            current_epoch,
            &mut payment,
            ctx,
        );

        // Destroy the compensation storage - we only needed to pay for it.
        storage_resource::destroy(compensation_storage);

        past_epochs
    } else {
        0
    };

    // Phase 2: Buy the actual extension storage.
    // If we paid compensation (dormant mode), extend from current_epoch forward.
    // Otherwise extend from the existing storage_end_epoch.
    let extension_start_epoch = if (compensation_epochs > 0) {
        // In dormant mode, start extension from current epoch.
        current_epoch
    } else {
        // Not dormant, extend from current storage end.
        storage_end_epoch
    };
    let new_end_epoch = extension_start_epoch + effective_extension;

    // Purchase the extension storage.
    let extension_storage = system::reserve_space_for_epochs(
        system,
        total_capacity,
        extension_start_epoch,
        new_end_epoch,
        &mut payment,
        ctx,
    );

    // Return any unused funds to the stash.
    if (payment.value() > 0) {
        self.coin_stash.deposit_wal(payment);
    } else {
        coin::destroy_zero(payment);
    };

    // Apply the extension to the BlobManager's storage.
    self.storage.extend_storage(extension_storage);

    // Tip the transaction sender for helping execute the extension.
    // Get the tip amount from the policy.
    let tip_amount = self.tip_policy.get_tip_amount();
    if (tip_amount > 0) {
        // Try to tip the sender from the coin stash.
        let tip_coin = self.coin_stash.tip_sender(tip_amount, ctx);
        // Transfer the tip to the sender.
        transfer::public_transfer(tip_coin, ctx.sender());
    };

    // Emit BlobManagerUpdated event for storage nodes to update gc_eligible_epoch.
    events::emit_blob_manager_updated(
        system::epoch(system),
        object::id(self),
        new_end_epoch,
        self.grace_period_epochs,
    );

    effective_extension
}

// === Query Functions ===

/// Returns the ID of the BlobManager.
public fun manager_id(self: &BlobManager): ID {
    object::uid_to_inner(&self.id)
}

/// Returns capacity information.
public fun capacity_info(self: &BlobManager): CapacityInfo {
    self.storage.capacity_info()
}

/// Returns storage epoch information: (start, end).
public fun storage_epochs(self: &BlobManager): (u32, u32) {
    self.storage.storage_epochs()
}

/// Gets the ObjectID of a blob by blob_id and deletable flag.
/// Returns the blob's ObjectID if found, aborts otherwise.
public fun get_blob_object_id_by_blob_id_and_deletable(
    self: &BlobManager,
    blob_id: u256,
    deletable: bool,
): ID {
    let mut blob_info_opt = self.storage.find_blob(blob_id, deletable);
    assert!(blob_info_opt.is_some(), EBlobNotRegisteredInBlobManager);
    let blob_info = blob_info_opt.extract();
    blob_info.object_id()
}

/// Returns the coin stash balances: (WAL, SUI).
public fun coin_stash_balances(self: &BlobManager): (u64, u64) {
    self.coin_stash.balances()
}

/// Returns the current extension policy.
public fun extension_policy(self: &BlobManager): &ExtensionPolicy {
    &self.extension_policy
}

/// Returns the grace period in epochs after storage expiry before blobs become eligible for GC.
public fun grace_period_epochs(self: &BlobManager): u32 {
    self.grace_period_epochs
}

// === Fund Manager Functions ===

/// Withdraws all WAL funds from the coin stash.
/// Requires fund_manager permission.
public fun withdraw_all_wal(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    ctx: &mut TxContext,
): Coin<WAL> {
    // Verify the capability.
    check_cap(self, cap);
    // Ensure the caller has fund_manager permission.
    assert!(cap.fund_manager, ERequiresFundManager);

    // Withdraw all WAL.
    self.coin_stash.withdraw_all_wal(ctx)
}

/// Withdraws all SUI funds from the coin stash.
/// Requires fund_manager permission.
public fun withdraw_all_sui(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    ctx: &mut TxContext,
): Coin<SUI> {
    // Verify the capability.
    check_cap(self, cap);
    // Ensure the caller has fund_manager permission.
    assert!(cap.fund_manager, ERequiresFundManager);

    // Withdraw all SUI.
    self.coin_stash.withdraw_all_sui(ctx)
}

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

/// Sets the extension policy to fund_manager only.
/// Requires fund_manager permission.
public fun set_extension_policy_fund_manager_only(self: &mut BlobManager, cap: &BlobManagerCap) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);
    self.extension_policy = extension_policy::fund_manager_only();
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

/// Sets the grace period in epochs after storage expiry before blobs become eligible for GC.
/// The input will be capped between 0 and the maximum allowed based on BlobManager age.
/// Requires fund_manager permission.
public fun set_grace_period_epochs(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    grace_period_epochs: u32,
) {
    check_cap(self, cap);
    assert!(cap.fund_manager, ERequiresFundManager);

    // Calculate maximum allowed grace period based on BlobManager age.
    let current_epoch = system::epoch(system);
    let (start_epoch, _end_epoch) = blob_storage::storage_epochs(&self.storage);
    let age = current_epoch - start_epoch;
    let max_allowed = calculate_grace_period(age);

    // Cap the user input between 0 and maximum.
    let capped_grace_period = if (grace_period_epochs > max_allowed) {
        max_allowed
    } else {
        grace_period_epochs
    };

    self.grace_period_epochs = capped_grace_period;
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
