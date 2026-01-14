// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Unified storage module that combines storage capacity management with blob storage.
/// This module manages storage accounting, blob storage, and access capabilities.
/// Replaces the previous BlobStorage with a key-ability structure that can be referenced.
module walrus::unified_storage;

use std::{option::{Self, Option}, string::String};
use sui::{table::{Self as table, Table}, vec_map::{Self, VecMap}};
use walrus::{
    blob_v2::{Self, BlobV2},
    encoding,
    events,
    messages::CertifiedBlobMessage,
    storage_resource::{Self, Storage}
};

// === Error Codes ===

/// Insufficient storage capacity for the operation.
const EInsufficientStorageCapacity: u64 = 1;
/// The storage end epoch doesn't match.
const EStorageEndEpochMismatch: u64 = 2;
/// The storage has already expired.
const EStorageExpired: u64 = 3;
/// The blob already exists in storage.
const EBlobAlreadyExists: u64 = 4;
/// The blob is not registered in UnifiedStorage.
const EBlobNotRegistered: u64 = 5;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 6;
/// The blob is not deletable.
const EBlobNotDeletable: u64 = 7;
/// Storage size must equal total capacity for extension.
const EStorageSizeMismatch: u64 = 8;
/// Invalid capability for this operation.
const EInvalidCapability: u64 = 9;
/// Capability has been revoked.
const ECapabilityRevoked: u64 = 10;
/// Capability has expired.
const ECapabilityExpired: u64 = 11;
/// Operation requires different permissions.
const EInsufficientPermissions: u64 = 12;
/// The blob's storage_id doesn't match this UnifiedStorage.
const EInvalidStorageId: u64 = 13;

// === Capability System ===

/// Different roles with varying permissions.
public enum Role has copy, drop, store {
    /// Full administrative control.
    Admin,
    /// Can manage storage operations (buy, extend).
    StorageManager,
    /// Can perform blob operations (register, certify, delete).
    BlobManager,
    /// Can only read data.
    Reader,
    /// Custom role with specific permissions.
    Custom {
        can_register: bool,
        can_certify: bool,
        can_delete: bool,
        can_manage_storage: bool,
        can_manage_capabilities: bool,
        can_withdraw_funds: bool,
    },
}

/// Information about a capability's permissions and status.
public struct CapabilityInfo has store {
    /// Whether this capability has been revoked.
    is_revoked: bool,
    /// The role and permissions of this capability.
    role: Role,
    /// Optional expiration epoch for time-limited capabilities.
    expires_at: Option<u32>,
}

/// The capability object that grants access to UnifiedStorage.
public struct UnifiedStorageCap has key, store {
    id: UID,
    /// The UnifiedStorage this capability is for.
    storage_id: ID,
    /// Initial role (cached for quick access, source of truth is in UnifiedStorage).
    initial_role: Role,
}

// === Main Structures ===

/// Unified storage that combines capacity management with blob storage.
/// Has key and store abilities - can be shared globally or owned by other objects.
public struct UnifiedStorage has key, store {
    id: UID,
    // === Storage Capacity Fields ===
    /// Available storage capacity in bytes.
    available_storage: u64,
    /// Used storage capacity in bytes.
    used_storage: u64,
    /// Start epoch for this storage.
    start_epoch: u32,
    /// End epoch for this storage.
    end_epoch: u32,
    // === Blob Storage Fields ===
    /// Maps blob_id directly to the BlobV2 object (one blob per blob_id).
    blobs: Table<u256, BlobV2>,
    /// Total unencoded size of all blobs.
    total_unencoded_size: u64,
    /// Total number of permanent blobs.
    permanent_blob_count: u64,
    /// Total number of deletable blobs.
    deletable_blob_count: u64,
    // === Capability Registry ===
    /// Registry of all capabilities with their permissions and status.
    capabilities: Table<ID, CapabilityInfo>,
}

/// Capacity information for the storage.
public struct CapacityInfo has copy, drop {
    /// Available storage capacity in bytes.
    available: u64,
    /// Used storage capacity in bytes.
    in_use: u64,
    /// End epoch for this storage.
    end_epoch: u32,
}

// === Constructor ===

/// Creates a new UnifiedStorage instance with initial storage.
/// Returns the UnifiedStorage object that can be owned or shared by caller.
/// Takes `current_epoch` as a parameter to avoid dependency on System.
public fun new(
    initial_storage: Storage,
    current_epoch: u32,
    ctx: &mut TxContext,
): UnifiedStorage {
    let capacity = initial_storage.size();
    let end_epoch = initial_storage.end_epoch();

    // Destroy the storage object - we only need accounting.
    initial_storage.destroy();

    // Create and return the unified storage.
    UnifiedStorage {
        id: object::new(ctx),
        available_storage: capacity,
        used_storage: 0,
        start_epoch: current_epoch,
        end_epoch: end_epoch,
        blobs: table::new(ctx),
        total_unencoded_size: 0,
        permanent_blob_count: 0,
        deletable_blob_count: 0,
        capabilities: table::new(ctx),
    }
}

// === Capability Management ===

/// Creates a new capability with specified role.
public fun create_capability(
    storage: &mut UnifiedStorage,
    cap: &UnifiedStorageCap,
    role: Role,
    expires_at: Option<u32>,
    ctx: &mut TxContext,
): UnifiedStorageCap {
    // Verify the capability and check permissions.
    check_capability(storage, cap, false, false, false, false, true, false);

    // Create new capability.
    let new_cap = UnifiedStorageCap {
        id: object::new(ctx),
        storage_id: object::id(storage),
        initial_role: role,
    };

    // Register the new capability.
    storage.capabilities.add(
        object::id(&new_cap),
        CapabilityInfo {
            is_revoked: false,
            role: role,
            expires_at: expires_at,
        }
    );

    new_cap
}

/// Revokes a capability by ID.
public fun revoke_capability(
    storage: &mut UnifiedStorage,
    cap: &UnifiedStorageCap,
    cap_to_revoke: ID,
) {
    // Verify the capability and check permissions.
    check_capability(storage, cap, false, false, false, false, true, false);

    // Get and revoke the capability.
    if (storage.capabilities.contains(cap_to_revoke)) {
        let cap_info = storage.capabilities.borrow_mut(cap_to_revoke);
        cap_info.is_revoked = true;
    };
}

// === Storage Capacity Management Functions ===

/// Returns capacity information.
public fun capacity_info(self: &UnifiedStorage): CapacityInfo {
    CapacityInfo {
        available: self.available_storage,
        in_use: self.used_storage,
        end_epoch: self.end_epoch,
    }
}

/// Returns storage epoch information: (start, end).
public fun storage_epochs(self: &UnifiedStorage): (u32, u32) {
    (self.start_epoch, self.end_epoch)
}

/// Returns the end epoch of the storage.
public fun end_epoch(self: &UnifiedStorage): u32 {
    self.end_epoch
}

/// Adds more storage to the pool by consuming a Storage object.
/// The Storage must have the same end_epoch as existing storage.
public fun add_storage(
    self: &mut UnifiedStorage,
    storage: Storage
) {
    // Verify epochs match.
    assert!(storage.end_epoch() == self.end_epoch, EStorageEndEpochMismatch);

    // Add capacity to available storage.
    let storage_size = storage.size();
    self.available_storage = self.available_storage + storage_size;

    // Destroy the storage object (we only need accounting).
    storage.destroy();
}

/// Extends the storage end_epoch by consuming a Storage object.
/// Adds the epoch duration from the extension storage to the UnifiedStorage's end_epoch.
public fun extend_storage(
    self: &mut UnifiedStorage,
    extension_storage: Storage
) {
    let epochs = extension_storage.end_epoch() - extension_storage.start_epoch();
    let extension_size = extension_storage.size();
    let total_capacity = self.available_storage + self.used_storage;

    // Verify the storage size matches total capacity.
    assert!(extension_size == total_capacity, EStorageSizeMismatch);

    // Update end epoch.
    self.end_epoch = self.end_epoch + epochs;

    // Destroy the storage object (we only use it for the end_epoch).
    extension_storage.destroy();
}

// === Blob Management Functions ===

/// Returns true if a matching blob is found.
public fun check_blob_existence(self: &UnifiedStorage, blob_id: u256, deletable: bool): bool {
    if (!self.blobs.contains(blob_id)) {
        return false
    };

    let existing_blob = self.blobs.borrow(blob_id);

    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    true
}

/// Gets a mutable reference to a blob by blob_id.
public fun get_mut_blob(
    self: &mut UnifiedStorage,
    blob_id: u256
): &mut BlobV2 {
    assert!(self.blobs.contains(blob_id), EBlobNotRegistered);
    self.blobs.borrow_mut(blob_id)
}

/// Adds a new blob to storage with atomic storage allocation.
/// Checks capacity before adding the blob.
/// Security: Validates that the blob's storage_id matches this UnifiedStorage,
/// ensuring blobs can only be added to their designated storage.
public fun add_blob(
    self: &mut UnifiedStorage,
    blob: BlobV2
) {
    // Security check: ensure the blob's storage_id matches this UnifiedStorage.
    assert!(blob.storage_id() == object::id(self), EInvalidStorageId);

    let encoded_size = blob.encoded_size();
    // Check storage capacity first before modifying state.
    assert!(self.available_storage >= encoded_size, EInsufficientStorageCapacity);

    let blob_id = blob.blob_id();
    let size = blob.size();
    let is_deletable = blob.is_deletable();

    // Check if blob_id already exists - only one blob per blob_id allowed.
    assert!(!self.blobs.contains(blob_id), EBlobAlreadyExists);

    // Atomically allocate storage for the blob.
    self.available_storage = self.available_storage - encoded_size;
    self.used_storage = self.used_storage + encoded_size;

    // Store blob directly keyed by blob_id.
    self.blobs.add(blob_id, blob);

    // Update statistics.
    self.total_unencoded_size = self.total_unencoded_size + size;
    if (is_deletable) {
        self.deletable_blob_count = self.deletable_blob_count + 1;
    } else {
        self.permanent_blob_count = self.permanent_blob_count + 1;
    };
}

/// Removes a blob from storage and returns it with atomic storage release.
/// Only deletable blobs can be removed.
public fun remove_blob(
    self: &mut UnifiedStorage,
    blob_id: u256
): BlobV2 {
    assert!(self.blobs.contains(blob_id), EBlobNotRegistered);

    // Verify blob is deletable before removal.
    let existing_blob = self.blobs.borrow(blob_id);
    assert!(existing_blob.is_deletable(), EBlobNotDeletable);

    // Remove and return the blob.
    let blob = self.blobs.remove(blob_id);
    let size = blob.size();
    let encoded_size = blob.encoded_size();

    // Update statistics.
    self.total_unencoded_size = self.total_unencoded_size - size;
    self.deletable_blob_count = self.deletable_blob_count - 1;

    // Atomically release storage back to the pool.
    assert!(self.used_storage >= encoded_size, 0);
    self.available_storage = self.available_storage + encoded_size;
    self.used_storage = self.used_storage - encoded_size;

    blob
}

// === Statistics ===

/// Returns the total number of blobs.
public fun total_blob_count(self: &UnifiedStorage): u64 {
    self.permanent_blob_count + self.deletable_blob_count
}

/// Returns the number of permanent blobs.
public fun permanent_blob_count(self: &UnifiedStorage): u64 {
    self.permanent_blob_count
}

/// Returns the number of deletable blobs.
public fun deletable_blob_count(self: &UnifiedStorage): u64 {
    self.deletable_blob_count
}

/// Returns the total unencoded size of all blobs.
public fun total_unencoded_size(self: &UnifiedStorage): u64 {
    self.total_unencoded_size
}

// === CapacityInfo Accessors ===

/// Gets available storage from CapacityInfo.
public fun available(self: &CapacityInfo): u64 {
    self.available
}

/// Gets used storage from CapacityInfo.
public fun in_use(self: &CapacityInfo): u64 {
    self.in_use
}

/// Gets total capacity from CapacityInfo.
public fun total(self: &CapacityInfo): u64 {
    self.available + self.in_use
}

/// Gets end epoch from CapacityInfo.
public fun capacity_end_epoch(self: &CapacityInfo): u32 {
    self.end_epoch
}

// === Facade Functions for BlobManager ===

/// Creates a new BlobV2 and adds it to storage.
/// This is a facade that allows BlobManager to create blobs without calling package-level functions.
public fun create_and_add_blob(
    self: &mut UnifiedStorage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    blob_type: u8,
    registered_epoch: u32,
    end_epoch_at_registration: u32,
    n_shards: u16,
    ctx: &mut TxContext,
) {
    // Create the blob using the package-level function
    let blob = blob_v2::new(
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        blob_type,
        end_epoch_at_registration,
        registered_epoch,
        object::id(self),
        n_shards,
        ctx,
    );

    // Add it to storage
    self.add_blob(blob);
}

/// Makes a blob permanent.
/// This is a facade that allows BlobManager to make blobs permanent without calling package-level functions.
public fun make_blob_permanent(
    self: &mut UnifiedStorage,
    blob_id: u256,
    current_epoch: u32,
    end_epoch: u32,
) {
    let blob = self.get_mut_blob(blob_id);
    blob.make_permanent(current_epoch, end_epoch);
}

/// Certifies a blob.
/// This is a facade that allows BlobManager to certify blobs without calling package-level functions.
public fun certify_blob(
    self: &mut UnifiedStorage,
    blob_id: u256,
    current_epoch: u32,
    end_epoch_at_certify: u32,
    message: CertifiedBlobMessage,
) {
    let blob = self.get_mut_blob(blob_id);
    blob.certify_with_certified_msg(current_epoch, end_epoch_at_certify, message);
}

/// Deletes a blob and emits the deletion event.
/// This is a facade that allows BlobManager to delete blobs with proper event emission.
public fun delete_blob(
    self: &mut UnifiedStorage,
    blob_id: u256,
    epoch: u32,
) {
    let blob = self.remove_blob(blob_id);
    blob.delete_internal(epoch);
}

/// Emits a blob manager created event.
/// This is a facade that allows BlobManager to emit events without calling package-level functions.
public fun emit_blob_manager_created(
    epoch: u32,
    blob_manager_id: ID,
    storage_id: ID,
    end_epoch: u32,
) {
    events::emit_blob_manager_created(epoch, blob_manager_id, storage_id, end_epoch);
}

/// Emits a blob manager updated event.
/// This is a facade that allows BlobManager to emit events without calling package-level functions.
public fun emit_blob_manager_updated(epoch: u32, blob_manager_id: ID, new_end_epoch: u32) {
    events::emit_blob_manager_updated(epoch, blob_manager_id, new_end_epoch);
}

// === Internal Functions ===

/// Checks if a capability is valid and has required permissions.
fun check_capability(
    storage: &UnifiedStorage,
    cap: &UnifiedStorageCap,
    require_register: bool,
    require_certify: bool,
    require_delete: bool,
    require_manage_storage: bool,
    require_manage_capabilities: bool,
    require_withdraw_funds: bool,
) {
    // Verify the capability matches this storage.
    assert!(cap.storage_id == object::id(storage), EInvalidCapability);

    // Get capability info.
    assert!(storage.capabilities.contains(object::id(cap)), EInvalidCapability);
    let cap_info = storage.capabilities.borrow(object::id(cap));

    // Check if revoked.
    assert!(!cap_info.is_revoked, ECapabilityRevoked);

    // Check expiration if set.
    if (cap_info.expires_at.is_some()) {
        let expires_at = *cap_info.expires_at.borrow();
        let current_epoch = storage.start_epoch; // TODO: Get current epoch from System
        assert!(current_epoch < expires_at, ECapabilityExpired);
    };

    // Check permissions based on role.
    let has_permission = match (&cap_info.role) {
        Role::Admin => true,
        Role::StorageManager => require_manage_storage && !require_register && !require_certify && !require_delete && !require_manage_capabilities && !require_withdraw_funds,
        Role::BlobManager => (require_register || require_certify || require_delete) && !require_manage_storage && !require_manage_capabilities && !require_withdraw_funds,
        Role::Reader => !require_register && !require_certify && !require_delete && !require_manage_storage && !require_manage_capabilities && !require_withdraw_funds,
        Role::Custom { can_register, can_certify, can_delete, can_manage_storage, can_manage_capabilities, can_withdraw_funds } => {
            (!require_register || *can_register) &&
            (!require_certify || *can_certify) &&
            (!require_delete || *can_delete) &&
            (!require_manage_storage || *can_manage_storage) &&
            (!require_manage_capabilities || *can_manage_capabilities) &&
            (!require_withdraw_funds || *can_withdraw_funds)
        },
    };

    assert!(has_permission, EInsufficientPermissions);
}
