// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Minimal blob-management interface using enum-based storage strategies.
/// This version provides only essential functionality: new, register, and certify.
module walrus::blobmanager;

use sui::{coin::Coin, table::{Self, Table}};
use wal::wal::WAL;
use walrus::{blob::Blob, encoding, storage_resource::{Self, Storage}, system::{Self, System}};

// === Constants ===

/// Minimum initial capacity for BlobManager (100MB in bytes)
const MIN_INITIAL_CAPACITY: u64 = 100_000_000; // 100 MB

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// Insufficient storage capacity for the operation.
const EInsufficientCapacity: u64 = 1;
/// The blob already exists in the manager.
const EBlobAlreadyExists: u64 = 2;
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
/// Currently implements ObjectID-based storage with separate tables for
/// registered and certified blobs
public enum BlobStash has store {
    /// Blobs keyed by their Object ID for efficient access
    ObjectBased {
        /// Table mapping Blob Object ID to Blob (registered but not yet certified)
        registered_blobs: Table<ID, Blob>,
        /// Table mapping Blob Object ID to Blob (certified)
        certified_blobs: Table<ID, Blob>,
        /// Total number of blobs (registered + certified)
        blob_count: u64,
        /// Total size of all blobs
        total_size: u64,
        /// Reverse lookup: blob_id to Object ID
        blob_id_to_object: Table<u256, ID>,
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
public fun new(initial_storage: Storage, ctx: &mut TxContext): BlobManagerCap {
    let manager_id = object::new(ctx);
    let manager_uid = object::uid_to_inner(&manager_id);

    let capacity = storage_resource::size(&initial_storage);

    // Enforce minimum capacity requirement
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialCapacityTooSmall);

    let manager = BlobManager {
        id: manager_id,
        storage: BlobStorage::Unified {
            available_storage: initial_storage,
            total_capacity: capacity,
        },
        blob_stash: BlobStash::ObjectBased {
            registered_blobs: table::new(ctx),
            certified_blobs: table::new(ctx),
            blob_count: 0,
            total_size: 0,
            blob_id_to_object: table::new(ctx),
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
        let provided_size = storage_resource::size(&provided);

        if (provided_size == encoded_size) {
            // Case 2: Exact match - use provided storage
            provided
        } else if (provided_size > encoded_size) {
            // Case 1: Excess storage - split and return remainder to manager
            let excess = storage_resource::split_by_size(&mut provided, encoded_size, ctx);
            storage_resource::fuse_amount(available_storage, excess);
            provided
        } else {
            // Case 3: Insufficient - need to combine with manager's storage
            let available = storage_resource::size(available_storage);
            let needed_from_manager = encoded_size - provided_size;
            assert!(available >= needed_from_manager, EInsufficientCapacity);

            let from_manager = storage_resource::split_by_size(
                available_storage,
                needed_from_manager,
                ctx,
            );
            storage_resource::fuse_amount(&mut provided, from_manager);
            provided
        }
    } else {
        // No provided storage - use manager's storage
        option::destroy_none(provided_storage);
        let available = storage_resource::size(available_storage);
        assert!(available >= encoded_size, EInsufficientCapacity);
        storage_resource::split_by_size(available_storage, encoded_size, ctx)
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

/// Registers a new blob with the system and adds it to the manager's registered table
/// Requires a valid BlobManagerCap to prove write access
/// storage_for_blob is required - use prepare_storage_for_blob helper if needed
public fun register(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
    storage_for_blob: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): ID {
    // Verify the capability
    check_cap(self, cap);

    // Register blob with the system using the provided storage
    let blob = system.register_blob(
        storage_for_blob,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        payment,
        ctx,
    );

    // Get the object ID before moving the blob
    let object_id = object::id(&blob);

    // Add to registered blob stash
    match (&mut self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs,
            certified_blobs: _,
            blob_count,
            total_size,
            blob_id_to_object,
        } => {
            // Check if blob already exists
            assert!(!table::contains(blob_id_to_object, blob_id), EBlobAlreadyExists);

            // Add blob to registered tables
            table::add(registered_blobs, object_id, blob);
            table::add(blob_id_to_object, blob_id, object_id);

            // Update counters
            *blob_count = *blob_count + 1;
            *total_size = *total_size + size;
        },
    };

    object_id
}

/// Certifies a blob that has been registered and moves it to the certified table
/// Requires a valid BlobManagerCap to prove write access
public fun certify(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    blob_id: u256,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
    _ctx: &mut TxContext,
) {
    // Verify the capability
    check_cap(self, cap);

    // Find the blob object ID and move it from registered to certified
    match (&mut self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs,
            certified_blobs,
            blob_count: _,
            total_size: _,
            blob_id_to_object,
        } => {
            // Find the object ID
            assert!(table::contains(blob_id_to_object, blob_id), EBlobNotFound);
            let object_id = *table::borrow(blob_id_to_object, blob_id);

            // Remove blob from registered table
            let mut blob = table::remove(registered_blobs, object_id);

            // Certify the blob through the system with signature data
            system::certify_blob(system, &mut blob, signature, signers_bitmap, message);

            // Move blob to certified table
            table::add(certified_blobs, object_id, blob);
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
            let available = storage_resource::size(available_storage);
            let used = *total_capacity - available;
            (*total_capacity, used, available)
        },
    }
}

/// Returns storage epoch information: (start, end)
public fun storage_epochs(self: &BlobManager): (u32, u32) {
    match (&self.storage) {
        BlobStorage::Unified { available_storage, total_capacity: _ } => {
            (
                storage_resource::start_epoch(available_storage),
                storage_resource::end_epoch(available_storage),
            )
        },
    }
}

/// Returns the number of blobs (registered + certified)
public fun blob_count(self: &BlobManager): u64 {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs: _,
            certified_blobs: _,
            blob_count,
            total_size: _,
            blob_id_to_object: _,
        } => *blob_count,
    }
}

/// Returns the total size of all blobs
public fun total_blob_size(self: &BlobManager): u64 {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs: _,
            certified_blobs: _,
            blob_count: _,
            total_size,
            blob_id_to_object: _,
        } => *total_size,
    }
}

/// Checks if a blob exists by blob_id (in either registered or certified)
public fun has_blob(self: &BlobManager, blob_id: u256): bool {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs: _,
            certified_blobs: _,
            blob_count: _,
            total_size: _,
            blob_id_to_object,
        } => table::contains(blob_id_to_object, blob_id),
    }
}

/// Gets the object ID for a blob_id
public fun get_blob_object_id(self: &BlobManager, blob_id: u256): Option<ID> {
    match (&self.blob_stash) {
        BlobStash::ObjectBased {
            registered_blobs: _,
            certified_blobs: _,
            blob_count: _,
            total_size: _,
            blob_id_to_object,
        } => {
            if (table::contains(blob_id_to_object, blob_id)) {
                option::some(*table::borrow(blob_id_to_object, blob_id))
            } else {
                option::none()
            }
        },
    }
}
