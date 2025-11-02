// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Storage management strategy for BlobManager
/// Currently only implements Unified, but can be extended with new variants
module walrus::blob_storage;

use walrus::storage_resource::Storage;

// === Error Codes ===

/// Insufficient storage capacity for the operation.
const EInsufficientBlobManagerCapacity: u64 = 1;

/// Storage management strategy
/// Currently only implements Unified, but can be extended with new variants
public enum BlobStorage has store {
    /// Unified storage - holds the available storage that can be split for blobs
    Unified {
        available_storage: Storage,
        total_capacity: u64,
    },
}

// === BlobStorage Methods ===

/// Prepares storage for blob registration from the BlobStorage's available storage
/// Checks the storage strategy and creates storage accordingly
public fun prepare_storage_for_blob(
    self: &mut BlobStorage,
    encoded_size: u64,
    ctx: &mut TxContext,
): Storage {
    match (self) {
        BlobStorage::Unified { available_storage, total_capacity: _ } => {
            let available = available_storage.size();
            assert!(available >= encoded_size, EInsufficientBlobManagerCapacity);
            available_storage.split_by_size(encoded_size, ctx)
        },
    }
}

/// Returns capacity information: (total, used, available)
public fun capacity_info(self: &BlobStorage): (u64, u64, u64) {
    match (self) {
        BlobStorage::Unified { available_storage, total_capacity } => {
            let available = available_storage.size();
            let used = *total_capacity - available;
            (*total_capacity, used, available)
        },
    }
}

/// Returns storage epoch information: (start, end)
public fun storage_epochs(self: &BlobStorage): (u32, u32) {
    match (self) {
        BlobStorage::Unified { available_storage, total_capacity: _ } => {
            (available_storage.start_epoch(), available_storage.end_epoch())
        },
    }
}

/// Creates a new Unified BlobStorage instance
public fun new_unified_blob_storage(available_storage: Storage, total_capacity: u64): BlobStorage {
    BlobStorage::Unified {
        available_storage,
        total_capacity,
    }
}
