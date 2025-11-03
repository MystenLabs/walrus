// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Blob storage implementation for BlobManager
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent)
module walrus::blob_stash;

use sui::table::{Self as table, Table};
use walrus::blob::{Self, Blob};

// === Error Codes ===

/// The blob is already certified (deduplication error).
const EBlobAlreadyCertifiedInBlobManager: u64 = 4;
/// The requested blob was not found.
const EBlobNotRegisteredInBlobManager: u64 = 2;

// === TwoPhaseOwnershipBlobStash Struct ===

/// Two-phase ownership blob storage implementation
/// Tracks blobs with two-phase ownership: initially caller-owned, then BlobManager-owned
/// Stores blobs in tables indexed by blob_id and object_id
/// Blob deduplication behavior:
/// - If a certified blob with same blob_id and deletable exists: Returns
///   EBlobAlreadyCertified error
/// - If an uncertified blob with same blob_id and deletable exists: Returns its object ID
/// - Otherwise: Creates new blob, stores it, and returns its object ID
public struct TwoPhaseOwnershipBlobStash has store {
    /// Maps blob_id to its object ID(s)
    blob_id_to_objects: Table<u256, vector<ID>>,
    /// All blobs indexed by their object ID
    blobs_by_object_id: Table<ID, Blob>,
    /// Total unencoded size of all blobs
    total_unencoded_size: u64,
}

/// Blob storage implementation
/// Supports multiple variants of the same blob_id (e.g., deletable vs permanent)
public enum BlobStash has store {
    /// Object-based storage variant
    ObjectBased(TwoPhaseOwnershipBlobStash),
}

// === TwoPhaseOwnershipBlobStash Methods ===

/// Finds a matching blob with the given blob_id and deletable flag
/// Returns Option::some(object_id) if found and not certified, otherwise Option::none()
/// Aborts with EBlobAlreadyCertifiedInBlobManager if found and already certified
public fun find_matching_blob_id(
    self: &TwoPhaseOwnershipBlobStash,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    // Look up existing blobs with this blob_id
    if (self.blob_id_to_objects.contains(blob_id)) {
        let object_ids = self.blob_id_to_objects.borrow(blob_id);
        let len = object_ids.length();
        let mut i = 0;

        // Search for matching variant (same deletable flag)
        while (i < len) {
            let obj_id = object_ids[i];
            let existing_blob = self.blobs_by_object_id.borrow(obj_id);

            // Check if this variant matches (same blob_id and deletable)
            if (blob::is_deletable(existing_blob) == deletable) {
                // Found matching blob - check if it's already certified
                assert!(
                    blob::certified_epoch(existing_blob).is_none(),
                    EBlobAlreadyCertifiedInBlobManager,
                );

                // Return existing blob's object ID
                return option::some(obj_id)
            };
            i = i + 1;
        };
    };
    option::none()
}

/// Adds only the object_id to tracking (blob is owned by caller, not stored yet)
public fun add_blob_object_id_only(
    self: &mut TwoPhaseOwnershipBlobStash,
    blob_id: u256,
    object_id: ID,
    size: u64,
) {
    // Add to blob_id_to_objects mapping (tracking only)
    if (self.blob_id_to_objects.contains(blob_id)) {
        let object_ids = self.blob_id_to_objects.borrow_mut(blob_id);
        object_ids.push_back(object_id);
    } else {
        let new_vec = vector[object_id];
        self.blob_id_to_objects.add(blob_id, new_vec);
    };

    // Update total unencoded size
    self.total_unencoded_size = self.total_unencoded_size + size;
}

/// Adds a new blob to the object-based storage (transfers ownership to BlobManager)
/// Used during certification when blob is transferred from caller to BlobManager
public fun add_blob(
    self: &mut TwoPhaseOwnershipBlobStash,
    blob_id: u256,
    object_id: ID,
    blob: Blob,
    size: u64,
) {
    // Verify object_id is already tracked (from registration)
    assert!(self.blob_id_to_objects.contains(blob_id), EBlobNotRegisteredInBlobManager);
    let object_ids = self.blob_id_to_objects.borrow(blob_id);
    assert!(object_ids.contains(&object_id), EBlobNotRegisteredInBlobManager);

    // Verify blob is NOT already stored in table (shouldn't happen, but safety check)
    assert!(!self.blobs_by_object_id.contains(object_id), EBlobAlreadyCertifiedInBlobManager);

    // Store blob in blobs_by_object_id (blob ownership transferred to BlobManager)
    self.blobs_by_object_id.add(object_id, blob);
}

/// Returns the number of blobs
public fun blob_count(self: &TwoPhaseOwnershipBlobStash): u64 {
    self.blobs_by_object_id.length()
}

/// Returns the total unencoded size of all blobs
public fun total_blob_size(self: &TwoPhaseOwnershipBlobStash): u64 {
    self.total_unencoded_size
}

/// Checks if a blob_id exists
public fun has_blob(self: &TwoPhaseOwnershipBlobStash, blob_id: u256): bool {
    self.blob_id_to_objects.contains(blob_id)
}

/// Gets all object IDs for a given blob_id (may include multiple variants)
public fun get_blob_object_ids(self: &TwoPhaseOwnershipBlobStash, blob_id: u256): vector<ID> {
    if (self.blob_id_to_objects.contains(blob_id)) {
        *self.blob_id_to_objects.borrow(blob_id)
    } else {
        vector[]
    }
}

// === BlobStash Dispatch Functions ===

/// Finds a matching blob with the given blob_id and deletable flag (dispatches to variant)
public fun find_matching_blob_id_for_stash(
    stash: &BlobStash,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    match (stash) {
        BlobStash::ObjectBased(s) => find_matching_blob_id(s, blob_id, deletable),
    }
}

/// Adds only the object_id to tracking (dispatches to variant)
/// Blob remains owned by caller, not stored in BlobManager yet
public fun add_blob_object_id_only_to_stash(
    stash: &mut BlobStash,
    blob_id: u256,
    object_id: ID,
    size: u64,
) {
    match (stash) {
        BlobStash::ObjectBased(s) => add_blob_object_id_only(s, blob_id, object_id, size),
    }
}

/// Adds a new blob to storage (dispatches to variant)
/// Transfers ownership from caller to BlobManager (used during certification)
public fun add_blob_to_stash(
    stash: &mut BlobStash,
    blob_id: u256,
    object_id: ID,
    blob: Blob,
    size: u64,
) {
    match (stash) {
        BlobStash::ObjectBased(s) => add_blob(s, blob_id, object_id, blob, size),
    }
}

/// Checks if a blob with the given object_id is stored in the table
public fun is_blob_in_table(stash: &BlobStash, object_id: ID): bool {
    match (stash) {
        BlobStash::ObjectBased(s) => s.blobs_by_object_id.contains(object_id),
    }
}

/// Verifies that a blob with the given object_id is NOT already stored in the table
/// Aborts if the blob is already in the table
public fun verify_blob_not_in_table(stash: &BlobStash, object_id: ID) {
    assert!(!is_blob_in_table(stash, object_id), EBlobAlreadyCertifiedInBlobManager);
}

/// Returns the number of blobs (dispatches to variant)
public fun blob_count_for_stash(stash: &BlobStash): u64 {
    match (stash) {
        BlobStash::ObjectBased(s) => blob_count(s),
    }
}

/// Returns the total unencoded size of all blobs (dispatches to variant)
public fun total_blob_size_for_stash(stash: &BlobStash): u64 {
    match (stash) {
        BlobStash::ObjectBased(s) => total_blob_size(s),
    }
}

/// Checks if a blob_id exists (dispatches to variant)
public fun has_blob_in_stash(stash: &BlobStash, blob_id: u256): bool {
    match (stash) {
        BlobStash::ObjectBased(s) => has_blob(s, blob_id),
    }
}

/// Gets all object IDs for a given blob_id (dispatches to variant)
public fun get_blob_object_ids_from_stash(stash: &BlobStash, blob_id: u256): vector<ID> {
    match (stash) {
        BlobStash::ObjectBased(s) => get_blob_object_ids(s, blob_id),
    }
}

/// Gets mutable reference to blob from stash for certification (dispatches to variant)
public fun borrow_blob_mut_for_certification(stash: &mut BlobStash, blob_object_id: ID): &mut Blob {
    match (stash) {
        BlobStash::ObjectBased(s) => s.blobs_by_object_id.borrow_mut(blob_object_id),
    }
}

/// Gets immutable reference to blob from stash for verification (dispatches to variant)
public fun borrow_blob_for_verification(stash: &BlobStash, blob_object_id: ID): &Blob {
    match (stash) {
        BlobStash::ObjectBased(s) => s.blobs_by_object_id.borrow(blob_object_id),
    }
}

/// Verifies blob_id is tracked in stash (dispatches to variant)
public fun verify_blob_id_tracked(stash: &BlobStash, blob_id: u256): bool {
    match (stash) {
        BlobStash::ObjectBased(s) => s.blob_id_to_objects.contains(blob_id),
    }
}

/// Finds the ObjectID of a blob by blob_id and deletable flag
/// Returns Option::some(object_id) if found, Option::none() otherwise
/// Does not check certification status (used for certification operations)
public fun find_blob_object_id_by_blob_id_and_deletable(
    stash: &BlobStash,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    match (stash) {
        BlobStash::ObjectBased(s) => find_blob_object_id_for_certification(s, blob_id, deletable),
    }
}

/// Finds blob ObjectID by blob_id and deletable for certification
/// Does not check if blob is already certified (that check happens in certify_blob)
fun find_blob_object_id_for_certification(
    self: &TwoPhaseOwnershipBlobStash,
    blob_id: u256,
    deletable: bool,
): Option<ID> {
    // Look up existing blobs with this blob_id
    if (self.blob_id_to_objects.contains(blob_id)) {
        let object_ids = self.blob_id_to_objects.borrow(blob_id);
        let len = object_ids.length();
        let mut i = 0;

        // Search for matching variant (same deletable flag)
        while (i < len) {
            let obj_id = object_ids[i];
            let existing_blob = self.blobs_by_object_id.borrow(obj_id);

            // Check if this variant matches (same blob_id and deletable)
            if (blob::is_deletable(existing_blob) == deletable) {
                // Found matching blob - return its object ID
                return option::some(obj_id)
            };
            i = i + 1;
        };
    };
    option::none()
}

/// Gets object IDs for a blob_id for verification (dispatches to variant)
public fun get_object_ids_for_blob_id(stash: &BlobStash, blob_id: u256): &vector<ID> {
    match (stash) {
        BlobStash::ObjectBased(s) => s.blob_id_to_objects.borrow(blob_id),
    }
}

/// Verifies blob exists and returns its blob_id for certification (dispatches to variant)
/// Performs all necessary checks before certification
public fun verify_and_get_blob_id(stash: &BlobStash, blob_object_id: ID): u256 {
    match (stash) {
        BlobStash::ObjectBased(s) => {
            let existing_blob = s.blobs_by_object_id.borrow(blob_object_id);
            let blob_id = blob::blob_id(existing_blob);

            // Verify the blob_id is tracked in our manager
            assert!(s.blob_id_to_objects.contains(blob_id), EBlobNotRegisteredInBlobManager);

            // Verify this specific object_id is in the list for this blob_id
            let object_ids = s.blob_id_to_objects.borrow(blob_id);
            assert!(object_ids.contains(&blob_object_id), EBlobNotRegisteredInBlobManager);

            blob_id
        },
    }
}

/// Creates a new TwoPhaseOwnershipBlobStash instance
public fun new_object_based_blob_stash(ctx: &mut TxContext): TwoPhaseOwnershipBlobStash {
    TwoPhaseOwnershipBlobStash {
        blob_id_to_objects: table::new(ctx),
        blobs_by_object_id: table::new(ctx),
        total_unencoded_size: 0,
    }
}

/// Creates a new BlobStash with ObjectBased variant
public fun new_object_based_stash(ctx: &mut TxContext): BlobStash {
    BlobStash::ObjectBased(new_object_based_blob_stash(ctx))
}
