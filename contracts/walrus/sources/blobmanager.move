// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Minimal blob-management interface using enum-based storage strategies.
/// This version provides only essential functionality: new, register, and certify.
module walrus::blobmanager;

use sui::coin::Coin;
use wal::wal::WAL;
use walrus::{
    blob::{Self, Blob},
    blob_stash::{Self, BlobStash},
    blob_storage::{Self, BlobStorage},
    encoding,
    storage_resource::Storage,
    system::{Self, System}
};

// === Constants ===

/// Minimum initial capacity for BlobManager (100MB in bytes)
const MIN_INITIAL_CAPACITY: u64 = 100_000_000; // 100 MB

// === Error Codes ===

/// The provided BlobManagerCap does not match the BlobManager.
const EInvalidBlobManagerCap: u64 = 0;
/// Initial storage capacity is below minimum requirement.
const EInitialBlobManagerCapacityTooSmall: u64 = 3;
/// The blob is already certified (cannot register or certify again).
const EBlobAlreadyCertifiedInBlobManager: u64 = 4;
/// The requested blob was not found in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 2;

// === Main Structures ===

/// The minimal blob-management interface
public struct BlobManager has key, store {
    id: UID,
    /// Storage management strategy
    storage: BlobStorage,
    /// Blob stash strategy
    blob_stash: BlobStash,
}

/// A capability which represents the authority to manage blobs in the BlobManager.
public struct BlobManagerCap has key, store {
    id: UID,
    /// The ID of the BlobManager this cap controls.
    manager_id: ID,
}

// === Constructors ===

/// Creates a new shared BlobManager and returns its capability
/// The BlobManager is automatically shared in the same transaction
/// Requires minimum initial capacity of 100MB
public fun new_with_unified_storage(initial_storage: Storage, ctx: &mut TxContext): BlobManagerCap {
    let manager_uid = object::new(ctx);

    let capacity = initial_storage.size();

    // Enforce minimum capacity requirement
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialBlobManagerCapacityTooSmall);

    let manager = BlobManager {
        id: manager_uid,
        storage: blob_storage::new_unified_blob_storage(initial_storage, capacity),
        blob_stash: blob_stash::new_object_based_stash(ctx),
    };

    // Get the ObjectID from the constructed manager object
    let manager_object_id = object::id(&manager);

    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_object_id,
    };

    // BlobManager is designed to be a shared object.
    transfer::share_object(manager);

    cap
}

// === Storage Management ===

/// Prepares storage for blob registration from the BlobManager's available storage
/// Checks the storage strategy and creates storage accordingly
fun prepare_storage_for_blob(
    self: &mut BlobManager,
    encoded_size: u64,
    ctx: &mut TxContext,
): Storage {
    blob_storage::prepare_storage_for_blob(&mut self.storage, encoded_size, ctx)
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
    assert!(object::id(self) == cap.manager_id, EInvalidBlobManagerCap);
}

// === Core Operations ===

/// Registers a new blob with the system and returns it to the caller.
/// The caller owns the blob until certification, when ownership is transferred to BlobManager.
/// Requires a valid BlobManagerCap to prove write access
/// Aborts with EBlobAlreadyCertifiedInBlobManager if blob already exists and is certified
public fun register_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &mut System,
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

    // Step 1: Check for existing blob with same blob_id and deletable flag
    // Only check blobs that are already stored in the table (managed by BlobManager)
    // We allow multiple registrations if the blob is not yet in the table
    let existing_obj_id_opt = blob_stash::find_matching_blob_id_for_stash(
        &self.blob_stash,
        blob_id,
        deletable,
    );
    if (option::is_some(&existing_obj_id_opt)) {
        let mut existing_obj_id_opt = existing_obj_id_opt;
        let existing_obj_id = option::extract(&mut existing_obj_id_opt);
        // Check if this blob is already stored in the table (already managed)
        if (blob_stash::is_blob_in_table(&self.blob_stash, existing_obj_id)) {
            // Blob is already in table - check if certified
            let existing_blob_ref = blob_stash::borrow_blob_for_verification(
                &self.blob_stash,
                existing_obj_id,
            );
            assert!(
                blob::certified_epoch(existing_blob_ref).is_none(),
                EBlobAlreadyCertifiedInBlobManager,
            );
            // Blob exists in table but not certified - abort (caller should use existing blob)
            abort EBlobAlreadyCertifiedInBlobManager
        };
        // Blob is tracked but not in table - allow registration (caller will own new blob)
    };

    // Step 2: Create storage and register new blob
    // Calculate encoded size for storage
    let n_shards = system::n_shards(system);
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Prepare storage for the new blob
    let blob_storage = prepare_storage_for_blob(self, encoded_size, ctx);

    // Register new blob (caller will own this)
    let blob = system.register_blob(
        blob_storage,
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        payment,
        ctx,
    );

    let object_id = object::id(&blob);

    // Add object_id to tracking table (blob remains owned by caller)
    // During certification, the blob will be transferred to BlobManager's table
    blob_stash::add_blob_object_id_only_to_stash(&mut self.blob_stash, blob_id, object_id, size);

    // Return blob to caller (caller owns it)
    blob
}

/// Certifies a blob and transfers ownership to BlobManager
/// Requires a valid BlobManagerCap to prove write access
/// Takes the Blob object from the caller and stores it in BlobManager's table
public fun certify_blob(
    self: &mut BlobManager,
    cap: &BlobManagerCap,
    system: &System,
    mut blob: Blob, // Blob object transferred from caller
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
    _ctx: &mut TxContext,
) {
    // Verify the capability
    check_cap(self, cap);

    // Extract blob_id from the blob
    let blob_id = blob::blob_id(&blob);
    let object_id = object::id(&blob);
    let size = blob::size(&blob);

    // Verify the blob's object_id is tracked (registered earlier)
    assert!(
        blob_stash::has_blob_in_stash(&self.blob_stash, blob_id),
        EBlobNotRegisteredInBlobManager,
    );

    // Verify the blob is NOT already stored in table (not yet managed)
    blob_stash::verify_blob_not_in_table(&self.blob_stash, object_id);

    // Verify the blob is not already certified (safety check)
    assert!(blob::certified_epoch(&blob).is_none(), EBlobAlreadyCertifiedInBlobManager);

    // Certify the blob first (while caller still owns it)
    system::certify_blob(system, &mut blob, signature, signers_bitmap, message);

    // Transfer ownership: store blob in BlobManager's table
    blob_stash::add_blob_to_stash(&mut self.blob_stash, blob_id, object_id, blob, size);
}

// === Query Functions ===

/// Returns the ID of the BlobManager
public fun manager_id(self: &BlobManager): ID {
    object::uid_to_inner(&self.id)
}

/// Returns capacity information: (total, used, available)
public fun capacity_info(self: &BlobManager): (u64, u64, u64) {
    blob_storage::capacity_info(&self.storage)
}

/// Returns storage epoch information: (start, end)
public fun storage_epochs(self: &BlobManager): (u32, u32) {
    blob_storage::storage_epochs(&self.storage)
}

/// Returns the number of blobs (all variants)
public fun blob_count(self: &BlobManager): u64 {
    blob_stash::blob_count_for_stash(&self.blob_stash)
}

/// Returns the total unencoded size of all blobs
public fun total_blob_size(self: &BlobManager): u64 {
    blob_stash::total_blob_size_for_stash(&self.blob_stash)
}

/// Checks if a blob_id exists (any variant)
public fun has_blob(self: &BlobManager, blob_id: u256): bool {
    blob_stash::has_blob_in_stash(&self.blob_stash, blob_id)
}

/// Gets all object IDs for a given blob_id (may include multiple variants)
public fun get_blob_object_ids(self: &BlobManager, blob_id: u256): vector<ID> {
    blob_stash::get_blob_object_ids_from_stash(&self.blob_stash, blob_id)
}

/// Gets the ObjectID of a blob by blob_id and deletable flag
/// Returns the blob's ObjectID if found, aborts otherwise
public fun get_blob_object_id_by_blob_id_and_deletable(
    self: &BlobManager,
    blob_id: u256,
    deletable: bool,
): ID {
    let mut blob_object_id_opt = blob_stash::find_blob_object_id_by_blob_id_and_deletable(
        &self.blob_stash,
        blob_id,
        deletable,
    );
    assert!(option::is_some(&blob_object_id_opt), EBlobNotRegisteredInBlobManager);
    option::extract(&mut blob_object_id_opt)
}
