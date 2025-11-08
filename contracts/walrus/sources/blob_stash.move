// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Blob storage implementation for BlobManager.
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent).
module walrus::blob_stash;

use sui::table::{Self as table, Table};
use walrus::managed_blob::{Self, ManagedBlob};

// === Error Codes ===

/// The blob is already certified (deduplication error).
const EBlobAlreadyCertifiedInBlobManager: u64 = 4;
/// The requested blob was not found.
const EBlobAlreadyInStash: u64 = 2;
/// The blob is not registered in BlobManager.
const EBlobNotRegisteredInBlobManager: u64 = 3;
/// The object_id was not found in the blob stash.
const EObjectIdNotFound: u64 = 6;
/// Inconsistent state: object_id found in blob_id_to_objects but not in blobs_by_object_id.
const EInconsistentManagedBlobEntries: u64 = 5;

// === BlobStashByObject Struct ===

/// Each blob is uniquely referenced by its ManagedBlob object.
/// For blobs of the same blob_id, there could be two variants (deletable vs permanent).
public struct BlobStashByObject has store {
    /// Maps blob_id to vector of ObjectIDs.
    blob_id_to_objects: Table<u256, vector<ID>>,
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

    let object_ids = self.blob_id_to_objects.borrow(blob_id);
    let len = object_ids.length();
    let mut i = 0;

    // Search for matching variant (same deletable flag) that is in the table.
    while (i < len) {
        let obj_id = object_ids[i];

        // ManagedBlob must exist if object_id is in blob_id_to_objects.
        assert!(self.blobs_by_object_id.contains(obj_id), EInconsistentManagedBlobEntries);
        let existing_blob = self.blobs_by_object_id.borrow(obj_id);

        // Check if this variant matches (same blob_id and deletable).
        if (managed_blob::is_deletable(existing_blob) == deletable) {
            return option::some(obj_id)
        };
        i = i + 1;
    };

    option::none()
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
        managed_blob::blob_id(managed_blob)
    };

    // Check that the blob_id exists in blob_id_to_objects and that
    // the object_id exists in the vector.
    let mut found = false;
    if (self.blob_id_to_objects.contains(blob_id)) {
        let object_ids = self.blob_id_to_objects.borrow(blob_id);
        let len = object_ids.length();
        let mut i = 0;
        while (i < len) {
            if (object_ids[i] == object_id) {
                found = true;
                break
            };
            i = i + 1;
        };
    };
    assert!(found, EInconsistentManagedBlobEntries);

    // Return mutable reference to the ManagedBlob.
    self.blobs_by_object_id.borrow_mut(object_id)
}

/// Adds a new blob to the stash.
public fun add_blob(self: &mut BlobStashByObject, managed_blob: ManagedBlob) {
    let blob_id = managed_blob::blob_id(&managed_blob);
    let object_id = managed_blob::object_id(&managed_blob);
    let size = managed_blob::size(&managed_blob);

    assert!(
        self.find_blob_by_blob_id(blob_id, managed_blob.is_deletable()).is_none(),
        EBlobAlreadyInStash,
    );

    // Add object_id to the table keyed by blob_id.
    // If entry exists, push to the end of the vector; otherwise create new entry.
    if (self.blob_id_to_objects.contains(blob_id)) {
        self.blob_id_to_objects.borrow_mut(blob_id).push_back(object_id);
    } else {
        self.blob_id_to_objects.add(blob_id, vector[object_id]);
    };

    // Store managed blob in blobs_by_object_id (blob ownership transferred to BlobManager).
    self.blobs_by_object_id.add(object_id, managed_blob);

    // Update total unencoded size.
    self.total_unencoded_size = self.total_unencoded_size + size;
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

/// Gets all object IDs for a given blob_id (may include multiple variants).
public fun get_blob_object_ids(self: &BlobStashByObject, blob_id: u256): vector<ID> {
    if (self.blob_id_to_objects.contains(blob_id)) {
        *self.blob_id_to_objects.borrow(blob_id)
    } else {
        vector[]
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

/// Gets all object IDs for a given blob_id (dispatches to variant).
public fun get_blob_object_ids_from_stash(self: &BlobStash, blob_id: u256): vector<ID> {
    match (self) {
        BlobStash::ObjectBased(s) => get_blob_object_ids(s, blob_id),
    }
}

/// Gets mutable reference to managed blob from stash for certification (dispatches to variant).
public fun find_blob_mut_by_object_id(self: &mut BlobStash, blob_object_id: ID): &mut ManagedBlob {
    match (self) {
        BlobStash::ObjectBased(s) => s.blobs_by_object_id.borrow_mut(blob_object_id),
    }
}

/// Gets object IDs for a blob_id for verification (dispatches to variant).
public fun get_object_ids_for_blob_id(self: &BlobStash, blob_id: u256): &vector<ID> {
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
