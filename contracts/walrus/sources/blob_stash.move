// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Blob storage implementation for BlobManager.
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent).
module walrus::blob_stash;

use sui::table::{Self as table, Table};
use walrus::managed_blob::ManagedBlob;

// === Error Codes ===

/// The requested blob was not found.
const EBlobAlreadyInStash: u64 = 2;
/// The blob is not registered in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 3;
/// The object_id was not found in the blob stash.
const EObjectIdNotFound: u64 = 6;
/// Inconsistent state: object_id found in blob_id_to_objects but not in blobs_by_object_id.
const EInconsistentManagedBlobEntries: u64 = 5;
/// Conflict: Attempting to register a blob with different deletable flag than existing blob.
const EBlobPermanencyConflict: u64 = 7;

// === BlobStashByObject Struct ===

/// Each blob is uniquely referenced by its ManagedBlob object.
/// Only one blob per blob_id is allowed (either permanent or deletable, not both).
/// TODO(heliu): do we need two table.
public struct BlobStashByObject has store {
    /// Maps blob_id to ObjectID (one blob per blob_id).
    blob_id_to_objects: Table<u256, ID>,
    /// Maps ObjectID to the actual ManagedBlob object.
    blobs_by_object_id: Table<ID, ManagedBlob>,
    /// Total unencoded size of all blobs.
    total_unencoded_size: u64,
}

/// Blob storage implementation.
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent).
public enum BlobStash has store {
    /// Object-based storage variant.
    ObjectBased(BlobStashByObject),
}

/// Result of finding a managed blob with its certification status.
public struct ManagedBlobInfo has drop {
    object_id: ID,
    is_certified: bool,
}

// === BlobStashByObject Methods ===

/// Finds a matching blob that is already stored in the managed table and checks if it's certified.
/// Returns ManagedBlobInfo if found, otherwise Option::none().
/// Only searches blobs that are already managed (stored in blobs_by_object_id).
/// This function combines finding and certification check to avoid multiple table lookups.
public fun find_blob_by_blob_id(
    self: &BlobStashByObject,
    blob_id: u256,
    deletable: bool,
): Option<ManagedBlobInfo> {
    let object_id_opt = self.get_blob_object_id(blob_id, deletable);
    if (object_id_opt.is_none()) {
        return option::none()
    };

    let object_id = *option::borrow(&object_id_opt);
    let existing_blob = self.blobs_by_object_id.borrow(object_id);
    return option::some(ManagedBlobInfo {
            object_id,
            is_certified: existing_blob.certified_epoch().is_some(),
        })
}

public fun get_mut_blob_by_blob_id(
    self: &mut BlobStashByObject,
    blob_id: u256,
    deletable: bool,
): &mut ManagedBlob {
    let mut object_id_opt = self.get_blob_object_id(blob_id, deletable);
    assert!(object_id_opt.is_some(), EBlobNotRegisteredInBlobManager);
    let object_id = object_id_opt.extract();
    self.blobs_by_object_id.borrow_mut(object_id)
}

public(package) fun get_blob_object_id(
    self: &BlobStashByObject,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    if (!self.blob_id_to_objects.contains(blob_id)) {
        return option::none()
    };

    let obj_id = *self.blob_id_to_objects.borrow(blob_id);

    // ManagedBlob must exist if object_id is in blob_id_to_objects.
    assert!(self.blobs_by_object_id.contains(obj_id), EInconsistentManagedBlobEntries);
    let existing_blob = self.blobs_by_object_id.borrow(obj_id);

    // Check if deletable flag matches. If not, it's a permanency conflict.
    assert!(existing_blob.is_deletable() == deletable, EBlobPermanencyConflict);

    option::some(obj_id)
}

/// Finds a managed blob by object_id and returns a mutable reference.
public fun find_blob_by_object_id_mut(
    self: &mut BlobStashByObject,
    object_id: ID,
): &mut ManagedBlob {
    assert!(self.blobs_by_object_id.contains(object_id), EObjectIdNotFound);

    // Get the blob_id from the ManagedBlob.
    let blob_id = {
        let managed_blob = self.blobs_by_object_id.borrow(object_id);
        managed_blob.blob_id()
    };

    // Check that the blob_id exists in blob_id_to_objects and matches the object_id.
    assert!(self.blob_id_to_objects.contains(blob_id), EInconsistentManagedBlobEntries);
    let stored_object_id = *self.blob_id_to_objects.borrow(blob_id);
    assert!(stored_object_id == object_id, EInconsistentManagedBlobEntries);

    // Return mutable reference to the ManagedBlob.
    self.blobs_by_object_id.borrow_mut(object_id)
}

/// Adds a new blob to the stash.
public fun add_blob(self: &mut BlobStashByObject, managed_blob: ManagedBlob) {
    let blob_id = managed_blob.blob_id();
    let object_id = managed_blob.object_id();
    let size = managed_blob.size();

    assert!(
        self.find_blob_by_blob_id(blob_id, managed_blob.is_deletable()).is_none(),
        EBlobAlreadyInStash,
    );

    // Add object_id to the table keyed by blob_id.
    // Check if blob_id already exists - only one blob per blob_id allowed.
    assert!(!self.blob_id_to_objects.contains(blob_id), EBlobAlreadyInStash);

    // Store the blob_id -> object_id mapping.
    self.blob_id_to_objects.add(blob_id, object_id);

    // Store managed blob in blobs_by_object_id (blob ownership transferred to BlobManager).
    self.blobs_by_object_id.add(object_id, managed_blob);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size + size;
}

/// Removes a blob from the stash and returns it.
public fun remove_blob(self: &mut BlobStashByObject, blob_id: u256, deletable: bool): ManagedBlob {
    // Get the object_id for the blob.
    let mut object_id_opt = self.get_blob_object_id(blob_id, deletable);
    assert!(object_id_opt.is_some(), EBlobNotRegisteredInBlobManager);
    let object_id = object_id_opt.extract();

    // Remove the blob from blobs_by_object_id.
    let managed_blob = self.blobs_by_object_id.remove(object_id);
    let size = managed_blob.size();

    // Remove the blob_id -> object_id mapping.
    self.blob_id_to_objects.remove(blob_id);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size - size;

    managed_blob
}

/// Returns the number of blobs.
public fun blob_count(self: &BlobStashByObject): u64 {
    self.blobs_by_object_id.length()
}

/// Returns the total unencoded size of all blobs.
public fun total_blob_size(self: &BlobStashByObject): u64 {
    self.total_unencoded_size
}

/// Checks if a blob_id exists.
public fun has_blob(self: &BlobStashByObject, blob_id: u256): bool {
    self.blob_id_to_objects.contains(blob_id)
}

/// Gets the object ID for a given blob_id (one blob per blob_id).
public fun get_blob_object_id_unchecked(self: &BlobStashByObject, blob_id: u256): Option<ID> {
    if (self.blob_id_to_objects.contains(blob_id)) {
        option::some(*self.blob_id_to_objects.borrow(blob_id))
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
        BlobStash::ObjectBased(s) => find_blob_by_blob_id(s, blob_id, deletable),
    }
}

public fun get_mut_blob_in_stash(
    self: &mut BlobStash,
    blob_id: u256,
    deletable: bool,
): &mut ManagedBlob {
    match (self) {
        BlobStash::ObjectBased(s) => get_mut_blob_by_blob_id(s, blob_id, deletable),
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
        BlobStash::ObjectBased(s) => add_blob(s, managed_blob),
    }
}

/// Removes a blob from the stash and returns it (dispatches to variant).
public fun remove_blob_from_stash(
    self: &mut BlobStash,
    blob_id: u256,
    deletable: bool,
): ManagedBlob {
    match (self) {
        BlobStash::ObjectBased(s) => remove_blob(s, blob_id, deletable),
    }
}

/// Returns the number of blobs (dispatches to variant).
public fun blob_count_in_stash(self: &BlobStash): u64 {
    match (self) {
        BlobStash::ObjectBased(s) => blob_count(s),
    }
}

/// Returns the total unencoded size of all blobs (dispatches to variant).
public fun total_blob_size_in_stash(self: &BlobStash): u64 {
    match (self) {
        BlobStash::ObjectBased(s) => total_blob_size(s),
    }
}

/// Checks if a blob_id exists (dispatches to variant).
public fun has_blob_in_stash(self: &BlobStash, blob_id: u256): bool {
    match (self) {
        BlobStash::ObjectBased(s) => has_blob(s, blob_id),
    }
}

/// Gets the object ID for a given blob_id (dispatches to variant).
public fun get_blob_object_id_from_stash(self: &BlobStash, blob_id: u256): Option<ID> {
    match (self) {
        BlobStash::ObjectBased(s) => get_blob_object_id_unchecked(s, blob_id),
    }
}

/// Gets mutable reference to managed blob from stash for certification (dispatches to variant).
public fun find_blob_mut_by_object_id(self: &mut BlobStash, blob_object_id: ID): &mut ManagedBlob {
    match (self) {
        BlobStash::ObjectBased(s) => s.blobs_by_object_id.borrow_mut(blob_object_id),
    }
}

/// Gets the object ID for a blob_id for verification (dispatches to variant).
public fun get_object_id_for_blob_id(self: &BlobStash, blob_id: u256): &ID {
    match (self) {
        BlobStash::ObjectBased(s) => s.blob_id_to_objects.borrow(blob_id),
    }
}

/// Creates a new BlobStash with ObjectBased variant.
public fun new_object_based_stash(ctx: &mut TxContext): BlobStash {
    let blob_stash = BlobStashByObject {
        blob_id_to_objects: table::new(ctx),
        blobs_by_object_id: table::new(ctx),
        total_unencoded_size: 0,
    };
    BlobStash::ObjectBased(blob_stash)
}
