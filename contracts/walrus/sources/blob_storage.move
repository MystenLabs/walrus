// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Unified storage module that combines storage capacity management with blob storage.
/// This module manages both the storage accounting (capacity, usage) and the actual
/// ManagedBlob objects in a single unified structure.
module walrus::blob_storage;

use std::option::{Self, Option};
use sui::table::{Self as table, Table};
use walrus::{managed_blob::ManagedBlob, storage_resource::{Self, Storage}, system::{Self, System}};

// === Error Codes ===

/// Insufficient storage capacity for the operation.
const EInsufficientBlobManagerCapacity: u64 = 1;
/// The storage end epoch doesn't match.
const EStorageEndEpochMismatch: u64 = 2;
/// The storage has already expired.
const EStorageExpired: u64 = 3;
/// The blob already exists in storage.
const EBlobAlreadyExists: u64 = 4;
/// The blob is not registered in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 5;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 6;

// === Structs ===

/// Unified storage that combines capacity management with blob storage.
/// This single structure handles both storage accounting and blob management.
public struct BlobStorage has store {
    // === Storage Capacity Fields ===
    /// Available storage capacity in bytes.
    available_storage: u64,
    /// Used storage capacity in bytes.
    used_storage: u64,
    /// End epoch for this storage.
    end_epoch: u32,
    // === Blob Storage Fields ===
    /// Maps blob_id directly to the ManagedBlob object (one blob per blob_id).
    blobs: Table<u256, ManagedBlob>,
    /// Total unencoded size of all blobs.
    total_unencoded_size: u64,
}

/// Result of finding a managed blob with its certification status.
public struct ManagedBlobInfo has drop {
    /// The object ID of the managed blob.
    object_id: ID,
    /// Whether the blob is certified.
    is_certified: bool,
}

// === Constructor ===

/// Creates a new BlobStorage instance by consuming a Storage object.
/// The Storage is destroyed and only its capacity and end_epoch are tracked.
public fun new_unified_blob_storage(initial_storage: Storage, ctx: &mut TxContext): BlobStorage {
    let capacity = initial_storage.size();
    let end_epoch = initial_storage.end_epoch();

    // Destroy the storage object - we only need accounting.
    initial_storage.destroy();

    BlobStorage {
        available_storage: capacity,
        used_storage: 0,
        end_epoch,
        blobs: table::new(ctx),
        total_unencoded_size: 0,
    }
}

// === Storage Capacity Management Functions ===

/// Creates a Storage object from the pool for individual blobs.
/// This decrements the available storage and creates a real Storage object.
public fun prepare_storage_for_blob(
    self: &mut BlobStorage,
    encoded_size: u64,
    system: &System,
    ctx: &mut TxContext,
): Storage {
    assert!(self.available_storage >= encoded_size, EInsufficientBlobManagerCapacity);
    self.available_storage = self.available_storage - encoded_size;
    self.used_storage = self.used_storage + encoded_size;

    // Create a new Storage object from the pool
    let current_epoch = system::epoch(system);
    storage_resource::create_storage(current_epoch, self.end_epoch, encoded_size, ctx)
}

/// Returns capacity information: (total, used, available).
public fun capacity_info(self: &BlobStorage): (u64, u64, u64) {
    let total_capacity = self.available_storage + self.used_storage;
    (total_capacity, self.used_storage, self.available_storage)
}

/// Returns storage epoch information: (start=0, end).
/// Note: start_epoch is always 0 for accounting-based storage.
public fun storage_epochs(self: &BlobStorage): (u32, u32) {
    (0, self.end_epoch)
}

/// Returns the end epoch of the storage.
public fun end_epoch(self: &BlobStorage): u32 {
    self.end_epoch
}

/// Adds more storage to the pool by consuming a Storage object.
/// The Storage must have the same end_epoch as existing storage.
public fun add_storage(self: &mut BlobStorage, storage: Storage) {
    // Verify epochs match
    assert!(storage.end_epoch() == self.end_epoch, EStorageEndEpochMismatch);

    // Add capacity to available storage
    let storage_size = storage.size();
    self.available_storage = self.available_storage + storage_size;

    // Destroy the storage object (we only need accounting)
    storage.destroy();
}

/// Extends the storage validity period by consuming a Storage object.
/// The new Storage must have a later end_epoch.
public fun extend_storage(self: &mut BlobStorage, extension_storage: Storage) {
    let new_end_epoch = extension_storage.end_epoch();

    // Verify the extension is valid
    assert!(new_end_epoch > self.end_epoch, EStorageExpired);

    // Update end epoch
    self.end_epoch = new_end_epoch;

    // Destroy the storage object (we only use it for the end_epoch)
    extension_storage.destroy();
}

/// Extends the storage period by consuming a Storage object.
/// Similar to extend_storage but specifically for coin stash operations.
public fun extend_managed_storage(self: &mut BlobStorage, extension_storage: Storage) {
    // For now, just delegate to extend_storage.
    // In the future, we might have different logic for coin stash extensions.
    self.extend_storage(extension_storage);
}

// === Blob Management Functions ===

/// Finds a matching blob that is already stored in the managed table and checks if it's certified.
/// Returns ManagedBlobInfo if found, otherwise Option::none().
/// This function combines finding and certification check to avoid multiple table lookups.
public fun find_blob(self: &BlobStorage, blob_id: u256, deletable: bool): Option<ManagedBlobInfo> {
    if (!self.blobs.contains(blob_id)) {
        return option::none()
    };

    let existing_blob = self.blobs.borrow(blob_id);

    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    option::some(ManagedBlobInfo {
        object_id: existing_blob.object_id(),
        is_certified: existing_blob.certified_epoch().is_some(),
    })
}

/// Gets a mutable reference to a blob by blob_id.
public fun get_mut_blob(self: &mut BlobStorage, blob_id: u256, deletable: bool): &mut ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);

    let existing_blob = self.blobs.borrow(blob_id);
    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    self.blobs.borrow_mut(blob_id)
}

/// Gets a mutable reference to a blob by blob_id without checking deletable flag.
/// Use this when the permanency type doesn't matter for the operation.
public fun get_mut_blob_unchecked(self: &mut BlobStorage, blob_id: u256): &mut ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);
    self.blobs.borrow_mut(blob_id)
}

/// Gets the object ID for a given blob_id with deletable flag verification.
public(package) fun get_blob_object_id(
    self: &BlobStorage,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    if (!self.blobs.contains(blob_id)) {
        return option::none()
    };

    let existing_blob = self.blobs.borrow(blob_id);

    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    option::some(existing_blob.object_id())
}

/// Adds a new managed blob to storage with atomic storage allocation.
public fun add_blob(self: &mut BlobStorage, managed_blob: ManagedBlob, encoded_size: u64) {
    let blob_id = managed_blob.blob_id();
    let size = managed_blob.size();

    // Check if blob_id already exists - only one blob per blob_id allowed.
    assert!(!self.blobs.contains(blob_id), EBlobAlreadyExists);

    // Atomically allocate storage for the blob.
    assert!(self.available_storage >= encoded_size, EInsufficientBlobManagerCapacity);
    self.available_storage = self.available_storage - encoded_size;
    self.used_storage = self.used_storage + encoded_size;

    // Store managed blob directly keyed by blob_id.
    self.blobs.add(blob_id, managed_blob);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size + size;
}

/// Removes a blob from storage and returns it with atomic storage release.
public fun remove_blob(
    self: &mut BlobStorage,
    blob_id: u256,
    deletable: bool,
    encoded_size: u64,
): ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);

    // Verify deletable flag before removal.
    let existing_blob = self.blobs.borrow(blob_id);
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    // Remove and return the blob.
    let managed_blob = self.blobs.remove(blob_id);
    let size = managed_blob.size();

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size - size;

    // Atomically release storage back to the pool.
    assert!(self.used_storage >= encoded_size, 0);
    self.available_storage = self.available_storage + encoded_size;
    self.used_storage = self.used_storage - encoded_size;

    managed_blob
}

/// Returns the number of blobs.
public fun blob_count(self: &BlobStorage): u64 {
    self.blobs.length()
}

/// Returns the total unencoded size of all blobs.
public fun total_blob_size(self: &BlobStorage): u64 {
    self.total_unencoded_size
}

/// Checks if a blob_id exists.
public fun has_blob(self: &BlobStorage, blob_id: u256): bool {
    self.blobs.contains(blob_id)
}

/// Gets the object ID for a given blob_id without checking deletable flag.
public fun get_blob_object_id_unchecked(self: &BlobStorage, blob_id: u256): Option<ID> {
    if (self.blobs.contains(blob_id)) {
        option::some(self.blobs.borrow(blob_id).object_id())
    } else {
        option::none()
    }
}

// === ManagedBlobInfo Accessors ===

/// Gets the object ID from ManagedBlobInfo.
public fun object_id(self: &ManagedBlobInfo): ID {
    self.object_id
}

/// Gets the certification status from ManagedBlobInfo.
public fun is_certified(self: &ManagedBlobInfo): bool {
    self.is_certified
}
