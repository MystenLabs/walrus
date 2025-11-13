// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Storage management strategy for BlobManager.
/// Currently only implements Unified, but can be extended with new variants.
module walrus::blob_storage;

use walrus::storage_resource::{Self, Storage};

// === Error Codes ===

/// Insufficient storage capacity for the operation.
const EInsufficientBlobManagerCapacity: u64 = 1;
/// The storage end epoch doesn't match.
const EStorageEndEpochMismatch: u64 = 2;
/// The storage has already expired.
const EStorageExpired: u64 = 3;

/// Unified storage accounting structure.
/// Manages storage capacity through accounting rather than holding Storage objects.
public struct UnifiedStorage has store {
    available_storage: u64,
    total_capacity: u64,
    end_epoch: u32,
}

/// Storage management strategy.
/// Currently only implements Unified, but can be extended with new variants.
public enum BlobStorage has store {
    /// Unified storage - manages storage through accounting.
    Unified(UnifiedStorage),
}

// === BlobStorage Methods ===

/// Allocates storage from the pool for a blob.
/// This is accounting-only - no actual Storage object is created.
public fun allocate_storage(self: &mut BlobStorage, encoded_size: u64) {
    match (self) {
        BlobStorage::Unified(unified) => {
            assert!(unified.available_storage >= encoded_size, EInsufficientBlobManagerCapacity);
            unified.available_storage = unified.available_storage - encoded_size;
        },
    }
}

/// Creates a Storage object from the pool for individual blobs.
/// This decrements the available storage and creates a real Storage object.
public fun prepare_storage_for_blob(
    self: &mut BlobStorage,
    encoded_size: u64,
    ctx: &mut TxContext,
): Storage {
    match (self) {
        BlobStorage::Unified(unified) => {
            assert!(unified.available_storage >= encoded_size, EInsufficientBlobManagerCapacity);
            unified.available_storage = unified.available_storage - encoded_size;

            // Create a new Storage object from the pool
            // start_epoch=0 since we're managing validity period at BlobManager level
            storage_resource::create_storage(0, unified.end_epoch, encoded_size, ctx)
        },
    }
}

/// Returns capacity information: (total, used, available).
public fun capacity_info(self: &BlobStorage): (u64, u64, u64) {
    match (self) {
        BlobStorage::Unified(unified) => {
            let used = unified.total_capacity - unified.available_storage;
            (unified.total_capacity, used, unified.available_storage)
        },
    }
}

/// Returns storage epoch information: (start=0, end).
/// Note: start_epoch is always 0 for accounting-based storage.
public fun storage_epochs(self: &BlobStorage): (u32, u32) {
    match (self) {
        BlobStorage::Unified(unified) => {
            (0, unified.end_epoch)
        },
    }
}

/// Returns the end epoch of the storage.
public fun end_epoch(self: &BlobStorage): u32 {
    match (self) {
        BlobStorage::Unified(unified) => unified.end_epoch,
    }
}

/// Adds more storage to the pool by consuming a Storage object.
/// The Storage must have the same end_epoch as existing storage.
public fun add_storage(self: &mut BlobStorage, storage: Storage) {
    match (self) {
        BlobStorage::Unified(unified) => {
            // Verify epochs match
            assert!(storage.end_epoch() == unified.end_epoch, EStorageEndEpochMismatch);

            // Add capacity
            let storage_size = storage.size();
            unified.available_storage = unified.available_storage + storage_size;
            unified.total_capacity = unified.total_capacity + storage_size;

            // Destroy the storage object (we only need accounting)
            storage.destroy();
        },
    }
}

/// Extends the storage validity period by consuming a Storage object.
/// The new Storage must have a later end_epoch.
public fun extend_storage(self: &mut BlobStorage, extension_storage: Storage) {
    match (self) {
        BlobStorage::Unified(unified) => {
            let new_end_epoch = extension_storage.end_epoch();

            // Verify the extension is valid
            assert!(new_end_epoch > unified.end_epoch, EStorageExpired);

            // Update end epoch
            unified.end_epoch = new_end_epoch;

            // Destroy the storage object (we only use it for the end_epoch)
            extension_storage.destroy();
        },
    }
}

/// Creates a new Unified BlobStorage instance by consuming a Storage object.
/// The Storage is destroyed and only its capacity and end_epoch are tracked.
public fun new_unified_blob_storage(initial_storage: Storage): BlobStorage {
    let capacity = initial_storage.size();
    let end_epoch = initial_storage.end_epoch();

    // Destroy the storage object - we only need accounting.
    initial_storage.destroy();

    BlobStorage::Unified(UnifiedStorage {
        available_storage: capacity,
        total_capacity: capacity,
        end_epoch,
    })
}
