// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Blob storage implementation for BlobManager.
/// Only one blob per blob_id is allowed (either permanent or deletable, not both).
/// Simplified design: single table keyed by blob_id for efficient lookups.
module walrus::blob_stash;

use sui::table::{Self as table, Table};
use walrus::managed_blob::ManagedBlob;

// === Error Codes ===

/// The blob already exists in the stash.
const EBlobAlreadyInStash: u64 = 2;
/// The blob is not registered in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 3;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 7;

// === BlobStashByBlobId Struct ===

/// Each blob is uniquely referenced by its blob_id.
/// Only one blob per blob_id is allowed (either permanent or deletable, not both).
/// Simplified design: single table keyed by blob_id for efficient lookups.
public struct BlobStashByBlobId has store {
    /// Maps blob_id directly to the ManagedBlob object (one blob per blob_id).
    blobs: Table<u256, ManagedBlob>,
    /// Total unencoded size of all blobs.
    total_unencoded_size: u64,
}

/// TODO(heliu): Keep this in the walrus core, but with versioning, like the system
/// and staking objects.
/// For shared objects.
/// Blob storage implementation.
/// Uses an enum to allow future storage variants while maintaining backward compatibility.
public enum BlobStash has store {
    /// Blob-id-based storage variant (simplified, single table).
    BlobIdBased(BlobStashByBlobId),
}

/// Result of finding a managed blob with its certification status.
public struct ManagedBlobInfo has drop {
    object_id: ID,
    is_certified: bool,
}

// === BlobStashByBlobId Methods ===

/// Finds a matching blob that is already stored in the managed table and checks if it's certified.
/// Returns ManagedBlobInfo if found, otherwise Option::none().
/// This function combines finding and certification check to avoid multiple table lookups.
public fun find_blob_by_blob_id(
    self: &BlobStashByBlobId,
    blob_id: u256,
    deletable: bool,
): Option<ManagedBlobInfo> {
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
public fun get_mut_blob_by_blob_id(
    self: &mut BlobStashByBlobId,
    blob_id: u256,
    deletable: bool,
): &mut ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);

    let existing_blob = self.blobs.borrow(blob_id);
    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    self.blobs.borrow_mut(blob_id)
}

/// Gets a mutable reference to a blob by blob_id without checking deletable flag.
/// Use this when the permanency type doesn't matter for the operation.
fun get_mut_blob_by_blob_id_unchecked(
    self: &mut BlobStashByBlobId,
    blob_id: u256,
): &mut ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);
    self.blobs.borrow_mut(blob_id)
}

/// Gets the object ID for a given blob_id with deletable flag verification.
public(package) fun get_blob_object_id(
    self: &BlobStashByBlobId,
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

/// Adds a new blob to the stash.
public fun add_blob(self: &mut BlobStashByBlobId, managed_blob: ManagedBlob) {
    let blob_id = managed_blob.blob_id();
    let size = managed_blob.size();

    // Check if blob_id already exists - only one blob per blob_id allowed.
    assert!(!self.blobs.contains(blob_id), EBlobAlreadyInStash);

    // Store managed blob directly keyed by blob_id.
    self.blobs.add(blob_id, managed_blob);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size + size;
}

/// Removes a blob from the stash and returns it.
public fun remove_blob(self: &mut BlobStashByBlobId, blob_id: u256, deletable: bool): ManagedBlob {
    assert!(self.blobs.contains(blob_id), EBlobNotRegisteredInBlobManager);

    // Verify deletable flag before removal.
    let existing_blob = self.blobs.borrow(blob_id);
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    // Remove and return the blob.
    let managed_blob = self.blobs.remove(blob_id);
    let size = managed_blob.size();

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size - size;

    managed_blob
}

/// Returns the number of blobs.
public fun blob_count(self: &BlobStashByBlobId): u64 {
    self.blobs.length()
}

/// Returns the total unencoded size of all blobs.
public fun total_blob_size(self: &BlobStashByBlobId): u64 {
    self.total_unencoded_size
}

/// Checks if a blob_id exists.
public fun has_blob(self: &BlobStashByBlobId, blob_id: u256): bool {
    self.blobs.contains(blob_id)
}

/// Gets the object ID for a given blob_id (one blob per blob_id).
public fun get_blob_object_id_unchecked(self: &BlobStashByBlobId, blob_id: u256): Option<ID> {
    if (self.blobs.contains(blob_id)) {
        option::some(self.blobs.borrow(blob_id).object_id())
    } else {
        option::none()
    }
}

// === BlobStash Dispatch Functions ===

/// Finds a matching blob that is already stored in the managed table and checks if it's certified.
/// Returns ManagedBlobInfo if found, otherwise Option::none().
/// This function combines finding and certification check to avoid multiple table lookups.
public fun find_blob_in_stash(
    self: &BlobStash,
    blob_id: u256,
    deletable: bool,
): Option<ManagedBlobInfo> {
    match (self) {
        BlobStash::BlobIdBased(s) => find_blob_by_blob_id(s, blob_id, deletable),
    }
}

/// Gets a mutable reference to a blob in the stash.
public fun get_mut_blob_in_stash(
    self: &mut BlobStash,
    blob_id: u256,
    deletable: bool,
): &mut ManagedBlob {
    match (self) {
        BlobStash::BlobIdBased(s) => get_mut_blob_by_blob_id(s, blob_id, deletable),
    }
}

/// Gets a mutable reference to a blob in the stash without checking deletable flag.
/// Use this when the permanency type doesn't matter for the operation (e.g., attributes).
public fun get_mut_blob_in_stash_unchecked(self: &mut BlobStash, blob_id: u256): &mut ManagedBlob {
    match (self) {
        BlobStash::BlobIdBased(s) => get_mut_blob_by_blob_id_unchecked(s, blob_id),
    }
}

/// Gets the object ID from ManagedBlobInfo.
public fun object_id(self: &ManagedBlobInfo): ID {
    self.object_id
}

/// Gets the certification status from ManagedBlobInfo.
public fun is_certified(self: &ManagedBlobInfo): bool {
    self.is_certified
}

/// Adds a new managed blob to storage (dispatches to variant).
/// Transfers ownership from caller to BlobManager (used during certification).
public fun add_blob_to_stash(self: &mut BlobStash, managed_blob: ManagedBlob) {
    match (self) {
        BlobStash::BlobIdBased(s) => add_blob(s, managed_blob),
    }
}

/// Removes a blob from the stash and returns it (dispatches to variant).
public fun remove_blob_from_stash(
    self: &mut BlobStash,
    blob_id: u256,
    deletable: bool,
): ManagedBlob {
    match (self) {
        BlobStash::BlobIdBased(s) => remove_blob(s, blob_id, deletable),
    }
}

/// Returns the number of blobs (dispatches to variant).
public fun blob_count_in_stash(self: &BlobStash): u64 {
    match (self) {
        BlobStash::BlobIdBased(s) => blob_count(s),
    }
}

/// Returns the total unencoded size of all blobs (dispatches to variant).
public fun total_blob_size_in_stash(self: &BlobStash): u64 {
    match (self) {
        BlobStash::BlobIdBased(s) => total_blob_size(s),
    }
}

/// Checks if a blob_id exists (dispatches to variant).
public fun has_blob_in_stash(self: &BlobStash, blob_id: u256): bool {
    match (self) {
        BlobStash::BlobIdBased(s) => has_blob(s, blob_id),
    }
}

/// Gets the object ID for a given blob_id (dispatches to variant).
public fun get_blob_object_id_from_stash(self: &BlobStash, blob_id: u256): Option<ID> {
    match (self) {
        BlobStash::BlobIdBased(s) => get_blob_object_id_unchecked(s, blob_id),
    }
}

/// Creates a new BlobStash with BlobIdBased variant (simplified, single table).
public fun new_blob_id_based_stash(ctx: &mut TxContext): BlobStash {
    let blob_stash = BlobStashByBlobId {
        blobs: table::new(ctx),
        total_unencoded_size: 0,
    };
    BlobStash::BlobIdBased(blob_stash)
}
