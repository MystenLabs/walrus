// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Minimal blob-management interface using enum-based storage strategies.
/// This version provides only essential functionality: new, register, and certify.
module walrus::blobmanager;

use sui::coin::Coin;
use wal::wal::WAL;
use walrus::{
    blob_stash::{Self, BlobStash},
    blob_storage::{Self, BlobStorage},
    encoding,
    storage_resource::Storage,
    system::{Self, System}
};

// === Constants ===

/// Minimum initial capacity for BlobManager (500MB in bytes).
const MIN_INITIAL_CAPACITY: u64 = 500_000_000; // 500 MB.

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

/// The minimal blob-management interface.
public struct BlobManager has key, store {
    id: UID,
    /// Storage management strategy.
    storage: BlobStorage,
    /// Blob stash strategy - handles both individual blobs and managed blobs.
    blob_stash: BlobStash,
}

/// A capability which represents the authority to manage blobs in the BlobManager.
public struct BlobManagerCap has key, store {
    id: UID,
    /// The ID of the BlobManager this cap controls.
    manager_id: ID,
}

// === Constructors ===

/// Creates a new shared BlobManager and returns its capability.
/// The BlobManager is automatically shared in the same transaction.
/// Requires minimum initial capacity of 100MB.
public fun new_with_unified_storage(initial_storage: Storage, ctx: &mut TxContext): BlobManagerCap {
    let manager_uid = object::new(ctx);

    // Check minimum capacity before consuming the storage.
    let capacity = initial_storage.size();
    assert!(capacity >= MIN_INITIAL_CAPACITY, EInitialBlobManagerCapacityTooSmall);

    let manager = BlobManager {
        id: manager_uid,
        storage: blob_storage::new_unified_blob_storage(initial_storage),
        blob_stash: blob_stash::new_object_based_stash(ctx),
    };

    // Get the ObjectID from the constructed manager object.
    let manager_object_id = object::id(&manager);

    let cap = BlobManagerCap {
        id: object::new(ctx),
        manager_id: manager_object_id,
    };

    // BlobManager is designed to be a shared object.
    transfer::share_object(manager);

    cap
}

// === Capability Operations ===

/// Duplicates the given BlobManagerCap.
/// Allows delegation of write access to other parties.
public fun duplicate_cap(cap: &BlobManagerCap, ctx: &mut TxContext): BlobManagerCap {
    BlobManagerCap {
        id: object::new(ctx),
        manager_id: cap.manager_id,
    }
}

/// Returns the manager ID from a capability.
public fun cap_manager_id(cap: &BlobManagerCap): ID {
    cap.manager_id
}

/// Checks that the given BlobManagerCap matches the BlobManager.
fun check_cap(self: &BlobManager, cap: &BlobManagerCap) {
    assert!(object::id(self) == cap.manager_id, EInvalidBlobManagerCap);
}

// === Core Operations ===

/// Registers a new blob in the BlobManager.
/// BlobManager owns the blob immediately upon registration.
/// Requires a valid BlobManagerCap to prove write access.
/// Returns ok status (no abort) when:
///   - A matching blob already exists (certified or uncertified) - reuses existing blob
///   - A new blob is successfully created
/// Only aborts on errors:
///   - Insufficient funds (payment too small)
///   - Insufficient storage capacity
///   - Inconsistency detected in blob stash
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

    // Step 1: Check for existing managed blob with same blob_id and deletable flag.
    let existing_blob = self
        .blob_stash
        .find_blob_in_stash(
            blob_id,
            deletable,
        );
    if (existing_blob.is_some()) {
        // Blob already exists (certified or uncertified) - reuse it, skip registration.
        // Return ok status - no abort.
        return
    };

    // Step 2: Allocate storage and register managed blob.
    // Calculate encoded size for storage.
    let n_shards = system::n_shards(system);
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Verify we have enough storage capacity.
    self.storage.allocate_storage(encoded_size);

    // Get the end_epoch from the storage.
    let (_start_epoch, end_epoch) = self.storage.storage_epochs();

    // Register managed blob with the system.
    let managed_blob = system.register_managed_blob(
        object::id(self),
        blob_id,
        root_hash,
        size,
        encoding_type,
        deletable,
        blob_type,
        end_epoch,
        payment,
        ctx,
    );

    // Add blob to stash (BlobManager now owns it).
    self.blob_stash.add_blob_to_stash(managed_blob);
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
    let managed_blob = self.blob_stash.get_mut_blob_in_stash(blob_id, deletable);
    assert!(!managed_blob.certified_epoch().is_some(), EBlobAlreadyCertifiedInBlobManager);

    system.certify_managed_blob(
        managed_blob,
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
    deletable: bool,
) {
    // Verify the capability.
    check_cap(self, cap);

    // Get the current epoch from the system.
    let epoch = system::epoch(system);

    // Remove the blob from the stash and get the managed blob.
    let managed_blob = self.blob_stash.remove_blob_from_stash(blob_id, deletable);

    // Get blob info before deleting.
    let blob_size = managed_blob.size();
    let encoding_type = managed_blob.encoding_type();

    // Delete the managed blob (this emits the ManagedBlobDeleted event).
    managed_blob.delete(epoch);

    // Release the storage back to the BlobManager.
    let n_shards = system::n_shards(system);
    let encoded_size = encoding::encoded_blob_length(blob_size, encoding_type, n_shards);
    self.storage.release_storage(encoded_size);
}

// === Query Functions ===

/// Returns the ID of the BlobManager.
public fun manager_id(self: &BlobManager): ID {
    object::uid_to_inner(&self.id)
}

/// Returns capacity information: (total, used, available).
public fun capacity_info(self: &BlobManager): (u64, u64, u64) {
    self.storage.capacity_info()
}

/// Returns storage epoch information: (start, end).
public fun storage_epochs(self: &BlobManager): (u32, u32) {
    self.storage.storage_epochs()
}

/// Returns the number of blobs (all variants).
public fun blob_count(self: &BlobManager): u64 {
    self.blob_stash.blob_count_in_stash()
}

/// Returns the total unencoded size of all blobs.
public fun total_blob_size(self: &BlobManager): u64 {
    self.blob_stash.total_blob_size_in_stash()
}

/// Checks if a blob_id exists (any variant).
public fun has_blob(self: &BlobManager, blob_id: u256): bool {
    self.blob_stash.has_blob_in_stash(blob_id)
}

/// Gets the object ID for a given blob_id (one blob per blob_id).
public fun get_blob_object_id(self: &BlobManager, blob_id: u256): Option<ID> {
    self.blob_stash.get_blob_object_id_from_stash(blob_id)
}

/// Gets the ObjectID of a blob by blob_id and deletable flag.
/// Returns the blob's ObjectID if found, aborts otherwise.
public fun get_blob_object_id_by_blob_id_and_deletable(
    self: &BlobManager,
    blob_id: u256,
    deletable: bool,
): ID {
    let mut blob_info_opt = self.blob_stash.find_blob_in_stash(blob_id, deletable);
    assert!(blob_info_opt.is_some(), EBlobNotRegisteredInBlobManager);
    let blob_info = blob_info_opt.extract();
    blob_info.object_id()
}
