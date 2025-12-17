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
/// The blob is not deletable.
const EBlobNotDeletable: u64 = 7;
/// Storage size must equal total capacity for extension.
const EStorageSizeMismatch: u64 = 8;

// === Structs ===

/// Unified storage that combines capacity management with blob storage.
/// This single structure handles both storage accounting and blob management.
public struct BlobStorage has store {
    // === Storage Capacity Fields ===
    /// Available storage capacity in bytes.
    available_storage: u64,
    /// Used storage capacity in bytes.
    used_storage: u64,
    /// Start epoch for this storage (when BlobManager was created).
    start_epoch: u32,
    /// End epoch for this storage.
    end_epoch: u32,
    // === Blob Storage Fields ===
    /// Maps blob_id directly to the ManagedBlob object (one blob per blob_id).
    blobs: Table<u256, ManagedBlob>,
    /// Total unencoded size of all blobs.
    total_unencoded_size: u64,
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

/// Creates a new BlobStorage instance by consuming a Storage object.
/// The Storage is destroyed and only its capacity and epochs are tracked.
public(package) fun new_unified_blob_storage(
    initial_storage: Storage,
    start_epoch: u32,
    ctx: &mut TxContext,
): BlobStorage {
    let capacity = initial_storage.size();
    let end_epoch = initial_storage.end_epoch();

    // Destroy the storage object - we only need accounting.
    initial_storage.destroy();

    BlobStorage {
        available_storage: capacity,
        used_storage: 0,
        start_epoch: start_epoch,
        end_epoch: end_epoch,
        blobs: table::new(ctx),
        total_unencoded_size: 0,
    }
}

// === Storage Capacity Management Functions ===

/// Returns capacity information.
public fun capacity_info(self: &BlobStorage): CapacityInfo {
    CapacityInfo {
        available: self.available_storage,
        in_use: self.used_storage,
        end_epoch: self.end_epoch,
    }
}

/// Returns storage epoch information: (start, end).
public fun storage_epochs(self: &BlobStorage): (u32, u32) {
    (self.start_epoch, self.end_epoch)
}

/// Returns the end epoch of the storage.
public fun end_epoch(self: &BlobStorage): u32 {
    self.end_epoch
}

/// Adds more storage to the pool by consuming a Storage object.
/// The Storage must have the same end_epoch as existing storage.
public(package) fun add_storage(self: &mut BlobStorage, storage: Storage) {
    // Verify epochs match
    assert!(storage.end_epoch() == self.end_epoch, EStorageEndEpochMismatch);

    // Add capacity to available storage
    let storage_size = storage.size();
    self.available_storage = self.available_storage + storage_size;

    // Destroy the storage object (we only need accounting)
    storage.destroy();
}

/// Extends the storage end_epoch by consuming a Storage object.
/// Adds the epoch duration from the extension storage to the BlobStorage's end_epoch.
public(package) fun extend_storage(self: &mut BlobStorage, extension_storage: Storage) {
    let epochs = extension_storage.end_epoch() - extension_storage.start_epoch();
    let extension_size = extension_storage.size();
    let total_capacity = self.available_storage + self.used_storage;

    // Verify the storage size matches total capacity
    assert!(extension_size == total_capacity, EStorageSizeMismatch);

    // Update end epoch
    self.end_epoch = self.end_epoch + epochs;

    // Destroy the storage object (we only use it for the end_epoch)
    extension_storage.destroy();
}

// === Blob Management Functions ===

/// Returns true if a matching blob if found.
public(package) fun check_blob_existence(self: &BlobStorage, blob_id: u256, deletable: bool): bool {
    if (!self.blobs.contains(blob_id)) {
        return false;
    };

    let existing_blob = self.blobs.borrow(blob_id);

    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    true
}

/// Gets a mutable reference to a blob by blob_id.
/// Caller should check deletable flag if necessary.
public(package) fun get_mut_blob(self: &mut BlobStorage, blob_id: u256): &mut ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);
    self.blobs.borrow_mut(blob_id)
}

/// Adds a new managed blob to storage with atomic storage allocation.
/// Checks capacity before adding the blob.
public(package) fun add_blob(self: &mut BlobStorage, managed_blob: ManagedBlob) {
    let encoded_size = managed_blob.encoded_size();
    // Check storage capacity first before modifying state.
    assert!(self.available_storage >= encoded_size, EInsufficientBlobManagerCapacity);

    let blob_id = managed_blob.blob_id();
    let size = managed_blob.size();

    // Check if blob_id already exists - only one blob per blob_id allowed.
    assert!(!self.blobs.contains(blob_id), EBlobAlreadyExists);

    // Atomically allocate storage for the blob.
    self.available_storage = self.available_storage - encoded_size;
    self.used_storage = self.used_storage + encoded_size;

    // Store managed blob directly keyed by blob_id.
    self.blobs.add(blob_id, managed_blob);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size + size;
}

/// Removes a blob from storage and returns it with atomic storage release.
/// Only deletable blobs can be removed.
public(package) fun remove_blob(self: &mut BlobStorage, blob_id: u256): ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);

    // Verify blob is deletable before removal.
    let existing_blob = self.blobs.borrow(blob_id);
    assert!(existing_blob.is_deletable(), EBlobNotDeletable);

    // Remove and return the blob.
    let managed_blob = self.blobs.remove(blob_id);
    let size = managed_blob.size();
    let encoded_size = managed_blob.encoded_size();

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size - size;

    // Atomically release storage back to the pool.
    assert!(self.used_storage >= encoded_size, 0);
    self.available_storage = self.available_storage + encoded_size;
    self.used_storage = self.used_storage - encoded_size;

    managed_blob
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
