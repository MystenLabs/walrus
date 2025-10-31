// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Minimal blob-management interface using enum-based storage strategies.
/// This version provides only essential functionality: new, register, and certify.
module walrus::blobmanager;

use sui::{coin::Coin, table::{Self, Table}};
use wal::wal::WAL;
use walrus::{blob::{Self, Blob}, encoding, storage_resource::Storage, system::{Self, System}};

// === Constants ===

/// Minimum initial capacity for BlobManager (100MB in bytes)
const MIN_INITIAL_CAPACITY: u64 = 100_000_000; // 100 MB

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// Insufficient storage capacity for the operation.
const EInsufficientCapacity: u64 = 1;
/// The requested blob was not found.
const EBlobNotFound: u64 = 3;
/// Initial storage capacity is below minimum requirement.
const EInitialCapacityTooSmall: u64 = 4;

// === BlobStorage Enum ===

/// Storage management strategy
/// Currently only implements Unified, but can be extended with new variants
public enum BlobStorage has store {
    /// Unified storage - holds the available storage that can be split for blobs
    Unified {
        available_storage: Storage,
        total_capacity: u64,
    },
}

// === BlobStash Enum ===

/// Blob storage implementation
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent)
public enum BlobStash has store {
    /// Blobs by their blob_id
    ObjectBased {
        /// Maps blob_id to its object ID(s)
        blob_id_to_objects: Table<u256, vector<ID>>,
        /// All blobs indexed by their object ID
        blobs_by_object_id: Table<ID, Blob>,
        /// Total unencoded size of all blobs
        total_unencoded_size: u64,
    },
}

// === Main Structures ===

/// The minimal blob-management interface
public struct BlobManager has key, store {
    id: UID,
    /// Storage management strategy
    storage: BlobStorage,
    /// Blob storage strategy
    blob_stash: BlobStash,
}

/// A capability which represents the authority to manage blobs
/// This is the key to write access for the shared BlobManager
public struct BlobManagerCap has key, store {
    id: UID,
    /// The ID of the BlobManager this cap controls
    manager_id: ID,
}

// === Constructors ===

/// Creates a new shared BlobManager and returns its capability
/// The BlobManager is automatically shared in the same transaction
/// Requires minimum initial capacity of 100MB
public fun new_with_unified_storage(initial_storage: Storage, ctx: &mut TxContext): BlobManagerCap {
    let manager_id = object::new(ctx);
    let manager_uid = object::uid_to_inner(&manager_id);

    let capacity = initial_storage.size();

    // Enforce minimum capacity requirement
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialCapacityTooSmall);

    let manager = BlobManager {
        id: manager_id,
        storage: BlobStorage::Unified {
            available_storage: initial_storage,
            total_capacity: capacity,
        },
        blob_stash: BlobStash::ObjectBased {
            blob_id_to_objects: table::new<u256, vector<ID>>(ctx),
            blobs_by_object_id: table::new<ID, Blob>(ctx),
            total_unencoded_size: 0,
        },
    };

    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_uid,
    };

    // Share the BlobManager in the same transaction
    transfer::share_object(manager);

    cap
}

// === Storage Management ===

/// Prepares storage for blob registration by intelligently managing storage resources
/// - If provided storage > encoded_size: splits and returns excess to manager
/// - If provided storage == encoded_size: uses it directly
/// - If provided storage < encoded_size: combines with manager's storage
fun prepare_storage_for_blob(
    available_storage: &mut Storage,
    mut provided_storage: Option<Storage>,
    encoded_size: u64,
    ctx: &mut TxContext,
): Storage {
    if (option::is_some(&provided_storage)) {
        let mut provided = option::extract(&mut provided_storage);
        option::destroy_none(provided_storage);
        let provided_size = provided.size();

        if (provided_size == encoded_size) {
            // Case 2: Exact match - use provided storage
            provided
        } else if (provided_size > encoded_size) {
            // Case 1: Excess storage - split and return remainder to manager
            let excess = provided.split_by_size(encoded_size, ctx);
            available_storage.fuse_amount(excess);
            provided
        } else {
            // Case 3: Insufficient - need to combine with manager's storage
            let available = available_storage.size();
            let needed_from_manager = encoded_size - provided_size;
            assert!(available >= needed_from_manager, EInsufficientCapacity);

            let from_manager = available_storage.split_by_size(needed_from_manager, ctx);
            provided.fuse_amount(from_manager);
            provided
        }
    } else {
        // No provided storage - use manager's storage
        option::destroy_none(provided_storage);
        let available = available_storage.size();
        assert!(available >= encoded_size, EInsufficientCapacity);
        available_storage.split_by_size(encoded_size, ctx)
    }
}

// === Capability Operations ===

/// Duplicates the given BlobManagerCap
/// Allows delegation of write access to other parties
public fun duplicate_cap(cap: &BlobManagerCap, ctx: &mut TxContext): BlobManagerCap {
    BlobManagerCap {
        id: object::new(ctx),
        manager_id: cap.manager_id,
    }
}

/// Returns the manager ID from a capability
public fun cap_manager_id(cap: &BlobManagerCap): ID {
    cap.manager_id
}

/// Checks that the given BlobManagerCap matches the BlobManager
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(object::uid_to_inner(&self.id) == cap.manager_id, EInvalidBlobManagerCap);
}

// === Core Operations ===

/// Registers a new blob with the system and returns it
/// Requires a valid BlobManagerCap to prove write access
/// If storage_for_blob is None, uses manager's storage
/// If storage_for_blob is provided but insufficient, combines with manager's storage
/// The returned Blob can be transferred to the user or stored elsewhere
public fun register_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    storage_for_blob: Option<Storage>,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): Blob {
    // Verify the capability
    check_cap(self, cap);

    // Calculate encoded size for storage
    let n_shards = system::n_shards(system);
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Prepare storage using the helper function
    let final_storage = match (&mut self.storage) {
        BlobStorage::Unified { available_storage, total_capacity: _ } => {
            prepare_storage_for_blob(available_storage, storage_for_blob, encoded_size, ctx)
        },
    };

    // Check for existing blob with same blob_id and deletable flag
    match (&mut self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects,
            blobs_by_object_id,
            total_unencoded_size,
        } => {
            // Look up existing blobs with this blob_id
            if (table::contains(blob_id_to_objects, blob_id)) {
                let object_ids = table::borrow(blob_id_to_objects, blob_id);
                let len = object_ids.length();
                let mut i = 0;

                // Search for matching variant (same deletable flag)
                while (i < len) {
                    let obj_id = object_ids[i];
                    let existing_blob = table::borrow(blobs_by_object_id, obj_id);

                    // Check if this variant matches (same blob_id and deletable)
                    if (blob::is_deletable(existing_blob) == deletable) {
                        // Found matching blob - return excess storage to manager
                        match (&mut self.storage) {
                            BlobStorage::Unified { available_storage, total_capacity: _ } => {
                                available_storage.fuse_amount(final_storage);
                            },
                        };
                        // Return the existing blob (transfer it out of the table)
                        return table::remove(blobs_by_object_id, obj_id)
                    };
                    i = i + 1;
                };
            };

            // No matching blob found - register new one
            let blob = system.register_blob(
                final_storage,
                blob_id,
                root_hash,
                size,
                encoding_type,
                deletable,
                payment,
                ctx,
            );

            let object_id = object::id(&blob);

            // Add to blob_id_to_objects mapping
            if (table::contains(blob_id_to_objects, blob_id)) {
                let object_ids = table::borrow_mut(blob_id_to_objects, blob_id);
                object_ids.push_back(object_id);
            } else {
                let mut new_vec = vector::empty<ID>();
                new_vec.push_back(object_id);
                table::add(blob_id_to_objects, blob_id, new_vec);
            };

            // Store blob in blobs_by_object_id
            table::add(blobs_by_object_id, object_id, blob);

            // Update total unencoded size
            *total_unencoded_size = *total_unencoded_size + size;

            // Return the newly stored blob (remove it from table to transfer ownership)
            table::remove(blobs_by_object_id, object_id)
        },
    }
}

/// Certifies a blob and stores it in the certified table
/// Requires a valid BlobManagerCap to prove write access
/// The blob must have been previously registered (blob_id tracked in blob_id_to_object)
public fun certify_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    mut blob: Blob,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
    _ctx: &mut TxContext,
) {
    // Verify the capability
    check_cap(self, cap);

    let object_id = object::id(&blob);
    let blob_id = blob::blob_id(&blob);

    // Verify this blob was registered with this manager and certify it
    match (&mut self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects,
            blobs_by_object_id,
            total_unencoded_size: _,
        } => {
            // Verify the blob_id is tracked
            assert!(table::contains(blob_id_to_objects, blob_id), EBlobNotFound);

            // Verify this specific object_id is in the list for this blob_id
            let object_ids = table::borrow(blob_id_to_objects, blob_id);
            assert!(object_ids.contains(&object_id), EBlobNotFound);

            // Certify the blob through the system with signature data
            system::certify_blob(system, &mut blob, signature, signers_bitmap, message);

            // Store the certified blob back
            table::add(blobs_by_object_id, object_id, blob);
        },
    };
}

// === Query Functions ===

/// Returns the ID of the BlobManager
public fun manager_id(self: &BlobManager): ID {
    object::uid_to_inner(&self.id)
}

/// Returns capacity information: (total, used, available)
public fun capacity_info(self: &BlobManager): (u64, u64, u64) {
    match (&self.storage) {
        BlobStorage::Unified { available_storage, total_capacity } => {
            let available = available_storage.size();
            let used = *total_capacity - available;
            (*total_capacity, used, available)
        },
    }
}

/// Returns storage epoch information: (start, end)
public fun storage_epochs(self: &BlobManager): (u32, u32) {
    match (&self.storage) {
        BlobStorage::Unified { available_storage, total_capacity: _ } => {
            (available_storage.start_epoch(), available_storage.end_epoch())
        },
    }
}

/// Returns the number of blobs (all variants)
public fun blob_count(self: &BlobManager): u64 {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects: _,
            blobs_by_object_id,
            total_unencoded_size: _,
        } => table::length(blobs_by_object_id),
    }
}

/// Returns the total unencoded size of all blobs
public fun total_blob_size(self: &BlobManager): u64 {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects: _,
            blobs_by_object_id: _,
            total_unencoded_size,
        } => *total_unencoded_size,
    }
}

/// Checks if a blob_id exists (any variant)
public fun has_blob(self: &BlobManager, blob_id: u256): bool {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects,
            blobs_by_object_id: _,
            total_unencoded_size: _,
        } => table::contains(blob_id_to_objects, blob_id),
    }
}

/// Gets all object IDs for a given blob_id (may include multiple variants)
public fun get_blob_object_ids(self: &BlobManager, blob_id: u256): vector<ID> {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            blob_id_to_objects,
            blobs_by_object_id: _,
            total_unencoded_size: _,
        } => {
            if (table::contains(blob_id_to_objects, blob_id)) {
                *table::borrow(blob_id_to_objects, blob_id)
            } else {
                vector::empty()
            }
        },
    }
}
