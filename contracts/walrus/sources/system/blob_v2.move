// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::blob_v2;

use std::string::String;
use sui::vec_map::{Self, VecMap};
use walrus::{
    blob::{Self, Blob},
    encoding,
    events::{
        emit_blob_v2_registered,
        emit_blob_v2_certified,
        emit_blob_v2_deleted,
        emit_blob_v2_made_permanent
    },
    messages::CertifiedBlobMessage,
    storage_resource::Storage
};

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// The blob is not certified.
const ENotCertified: u64 = 0;
/// The blob is not deletable.
const EBlobNotDeletable: u64 = 1;
/// The bounds of the storage resource are exceeded.
const EResourceBounds: u64 = 2;
/// The storage resource size is insufficient.
const EResourceSize: u64 = 3;
// Error code 4 is available
/// The blob was already certified.
const EAlreadyCertified: u64 = 5;
/// The blob ID is incorrect.
const EInvalidBlobId: u64 = 6;
/// Invalid blob type value.
const EInvalidBlobType: u64 = 7;
// Error code 8 is available.
/// The blob persistence type of the blob does not match the certificate.
const EInvalidBlobPersistenceType: u64 = 9;
/// The blob object ID of a deletable blob does not match the ID in the certificate.
const EInvalidBlobObject: u64 = 10;
/// Too many attributes (max 100).
const ETooManyAttributes: u64 = 11;
/// Attribute key too long (max 1024 bytes).
const EAttributeKeyTooLong: u64 = 12;
/// Attribute value too long (max 1024 bytes).
const EAttributeValueTooLong: u64 = 13;
/// The blob is already permanent (not deletable).
const EBlobAlreadyPermanent: u64 = 14;

// === Attribute Limits ===
/// Maximum number of attributes per blob.
const MAX_ATTRIBUTES: u64 = 100;
/// Maximum length of an attribute key in bytes.
const MAX_ATTRIBUTE_KEY_LENGTH: u64 = 1024;
/// Maximum length of an attribute value in bytes.
const MAX_ATTRIBUTE_VALUE_LENGTH: u64 = 1024;

// === Blob Type ===

/// Type of blob: Regular or Quilt.
/// Note the blob type is reported by the client, and is not verified by the contract or the Walrus
/// system.
public enum BlobType has copy, drop, store {
    Regular,
    Quilt,
}

/// Returns a Regular BlobType.
public fun blob_type_regular(): BlobType {
    BlobType::Regular
}

/// Returns a Quilt BlobType.
public fun blob_type_quilt(): BlobType {
    BlobType::Quilt
}

// === Object definitions ===

/// BlobV2 represents a blob backed by a shared storage resource.
public struct BlobV2 has store {
    /// Unique identifier for this blob object.
    object_id: ID,
    /// The content-derived blob ID.
    blob_id: u256,
    /// Encoding type used for this blob.
    encoding_type: u8,
    /// Unencoded size of the blob.
    size: u64,
    /// Encoded size.
    encoded_size: u64,
    /// Epoch when this blob was registered.
    registered_epoch: u32,
    /// Whether this blob can be deleted.
    deletable: bool,
    /// Type of blob: Regular or Quilt.
    blob_type: BlobType,
    /// Epoch when certified (None if uncertified).
    certified_epoch: option::Option<u32>,
    /// Internal attributes map for efficient single-read access.
    /// Limits: max 100 entries, max 1KB per key, max 1KB per value.
    attributes: VecMap<String, String>,
    /// ID of the UnifiedStorage that contains this blob.
    storage_id: ID,
}

// === Accessors ===

public fun object_id(self: &BlobV2): ID {
    self.object_id
}

public fun registered_epoch(self: &BlobV2): u32 {
    self.registered_epoch
}

public fun blob_id(self: &BlobV2): u256 {
    self.blob_id
}

public fun size(self: &BlobV2): u64 {
    self.size
}

public fun encoding_type(self: &BlobV2): u8 {
    self.encoding_type
}

public fun certified_epoch(self: &BlobV2): &Option<u32> {
    &self.certified_epoch
}

public fun storage_id(self: &BlobV2): ID {
    self.storage_id
}

public fun is_deletable(self: &BlobV2): bool {
    self.deletable
}

public fun blob_type(self: &BlobV2): BlobType {
    self.blob_type
}

/// Returns a reference to the internal attributes map.
public fun attributes(self: &BlobV2): &VecMap<String, String> {
    &self.attributes
}

/// Returns the number of attributes.
public fun attributes_count(self: &BlobV2): u64 {
    self.attributes.length()
}

/// Checks if an attribute key exists.
public fun has_attribute(self: &BlobV2, key: &String): bool {
    self.attributes.contains(key)
}

/// Gets the value for an attribute key, if it exists.
public fun get_attribute(self: &BlobV2, key: &String): Option<String> {
    if (self.attributes.contains(key)) {
        let idx = self.attributes.get_idx(key);
        let (_, value) = self.attributes.get_entry_by_idx(idx);
        option::some(*value)
    } else {
        option::none()
    }
}

public fun encoded_size(self: &BlobV2): u64 {
    // Direct getter for cached encoded_size without n_shards parameter.
    self.encoded_size
}

/// Creates a new BlobV2 in `registered_epoch`.
/// `size` is the size of the unencoded blob.
/// The blob is associated with a UnifiedStorage identified by storage_id.
public(package) fun new(
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    blob_type: u8,
    end_epoch_at_registration: u32,
    registered_epoch: u32,
    storage_id: ID,
    n_shards: u16,
    ctx: &mut TxContext,
): BlobV2 {
    let object_id = ctx.fresh_object_address().to_id();

    // Cryptographically verify that the Blob ID authenticates
    // both the size and encoding_type (sanity check).
    assert!(blob::derive_blob_id(root_hash, encoding_type, size) == blob_id, EInvalidBlobId);

    // Calculate encoded size once during creation.
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Convert u8 to BlobType enum (0 = Regular, 1 = Quilt)
    let blob_type_enum = if (blob_type == 0) {
        BlobType::Regular
    } else if (blob_type == 1) {
        BlobType::Quilt
    } else {
        abort EInvalidBlobType
    };

    // Emit register event (event uses u8 for blob_type)
    // Use the storage_id as the owner_id in the event
    emit_blob_v2_registered(
        registered_epoch,
        storage_id,
        blob_id,
        object_id,
        encoding_type,
        blob_type,
        deletable,
        size,
        end_epoch_at_registration,
    );

    BlobV2 {
        object_id,
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        encoded_size,
        certified_epoch: option::none(),
        deletable,
        blob_type: blob_type_enum,
        attributes: vec_map::empty(),
        storage_id,
    }
}

/// Creates a BlobV2 from a regular Blob that is being moved into UnifiedStorage.
/// This preserves the certification status and registration epoch from the original blob.
/// Returns the new BlobV2 and the extracted Storage from the original blob.
public(package) fun new_from_regular(
    blob: Blob,
    storage_id: ID,
    n_shards: u16,
    ctx: &mut TxContext,
): (BlobV2, Storage) {
    let object_id = ctx.fresh_object_address().to_id();

    // Extract metadata from the blob.
    let blob_id = blob.blob_id();
    let size = blob.size();
    let encoding_type = blob.encoding_type();
    let certified_epoch = *blob.certified_epoch();
    let registered_epoch = blob.registered_epoch();
    let deletable = blob.is_deletable();

    // Calculate encoded size once during creation.
    let encoded_size = encoding::encoded_blob_length(size, encoding_type, n_shards);

    // Extract storage from the blob (this destroys the blob).
    let storage = blob.burn_and_extract_storage();

    let blob_v2 = BlobV2 {
        object_id,
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        encoded_size,
        certified_epoch,
        deletable,
        blob_type: BlobType::Regular,
        attributes: vec_map::empty(),
        storage_id,
    };

    (blob_v2, storage)
}

/// Certifies that a blob will be available in the storage system.
/// Note: Storage validity is checked at the UnifiedStorage level.
public(package) fun certify_with_certified_msg(
    self: &mut BlobV2,
    current_epoch: u32,
    end_epoch_at_certify: u32,
    message: CertifiedBlobMessage,
) {
    // Check that the blob is registered in the system.
    assert!(self.blob_id == message.certified_blob_id(), EInvalidBlobId);

    // Check that the blob is not already certified.
    assert!(!self.certified_epoch.is_some(), EAlreadyCertified);

    // Check the blob persistence type.
    assert!(
        self.deletable == message.blob_persistence_type().is_deletable(),
        EInvalidBlobPersistenceType,
    );

    // Only check object_id for deletable blobs (permanent blobs don't have object_id in
    // BlobPersistenceType).
    if (self.deletable) {
        assert!(
            message.blob_persistence_type().object_id() == self.object_id,
            EInvalidBlobObject,
        );
    };

    // Mark the blob as certified.
    self.certified_epoch.fill(current_epoch);

    self.emit_certified(end_epoch_at_certify);
}

/// Internal function to delete a deletable blob.
/// Used by UnifiedStorage when removing blobs.
/// Aborts if the BlobV2 is not deletable.
public(package) fun delete_internal(self: BlobV2, epoch: u32) {
    let BlobV2 {
        object_id,
        blob_id: deleted_blob_id,
        certified_epoch,
        deletable,
        storage_id,
        ..
    } = self;

    assert!(deletable, EBlobNotDeletable);

    let was_certified = certified_epoch.is_some();

    // Emit the deletion event before destroying the object.
    emit_blob_v2_deleted(
        epoch,
        storage_id,  // Use storage_id as the owner_id in the event
        deleted_blob_id,
        object_id,
        was_certified,
    );
}

/// Emits a `BlobV2Certified` event for the given blob.
public(package) fun emit_certified(self: &BlobV2, end_epoch_at_certify: u32) {
    // Emit certified event.
    // Convert BlobType enum to u8 for event (event uses u8 for blob_type).
    let blob_type_u8 = match (self.blob_type) {
        BlobType::Quilt => 1,
        BlobType::Regular => 0,
    };

    // Use storage_id as the owner_id in the event.
    emit_blob_v2_certified(
        *self.certified_epoch.borrow(),
        self.storage_id,
        self.blob_id,
        self.object_id,
        blob_type_u8,
        self.deletable,
        end_epoch_at_certify,
    );
}

/// Converts a deletable blob to permanent.
/// This is a one-way operation - permanent blobs cannot be made deletable again.
/// Aborts if the blob is already permanent (not deletable).
public(package) fun make_permanent(self: &mut BlobV2, current_epoch: u32, end_epoch: u32) {
    // Check that the blob is currently deletable.
    assert!(self.deletable, EBlobAlreadyPermanent);

    // Convert to permanent.
    self.deletable = false;

    // Use storage_id as the owner_id in the event.
    emit_blob_v2_made_permanent(
        current_epoch,
        self.storage_id,
        self.blob_id,
        self.object_id,
        end_epoch,
    );
}

// === Internal Attributes ===
// These are stored directly in the BlobV2 for efficient single-read access.
// Limits: max 100 entries, max 1KB per key, max 1KB per value.

/// Validates attribute key and value lengths.
fun validate_attribute_lengths(key: &String, value: &String) {
    assert!(key.length() <= MAX_ATTRIBUTE_KEY_LENGTH, EAttributeKeyTooLong);
    assert!(value.length() <= MAX_ATTRIBUTE_VALUE_LENGTH, EAttributeValueTooLong);
}

/// Sets an attribute key-value pair.
///
/// If the key already exists, the value is updated.
/// If the key doesn't exist and we're at the limit, aborts with ETooManyAttributes.
/// Aborts if key or value exceeds size limits.
public fun set_attribute(self: &mut BlobV2, key: String, value: String) {
    validate_attribute_lengths(&key, &value);

    if (self.attributes.contains(&key)) {
        // Update existing value.
        *self.attributes.get_mut(&key) = value;
    } else {
        // Adding new - check count limit.
        assert!(self.attributes.length() < MAX_ATTRIBUTES, ETooManyAttributes);
        self.attributes.insert(key, value);
    }
}

/// Removes an attribute by key.
///
/// Returns the removed key-value pair.
/// Aborts if the key doesn't exist.
public fun remove_attribute(self: &mut BlobV2, key: &String): (String, String) {
    self.attributes.remove(key)
}

/// Removes an attribute by key if it exists.
///
/// Returns the removed value if the key existed, None otherwise.
public fun remove_attribute_if_exists(self: &mut BlobV2, key: &String): Option<String> {
    if (self.attributes.contains(key)) {
        let (_, value) = self.attributes.remove(key);
        option::some(value)
    } else {
        option::none()
    }
}

/// Clears all attributes.
public fun clear_attributes(self: &mut BlobV2) {
    while (!self.attributes.is_empty()) {
        self.attributes.pop();
    }
}

#[test_only]
public fun certify_with_certified_msg_for_testing(
    blob: &mut BlobV2,
    current_epoch: u32,
    end_epoch_at_certify: u32,
    message: CertifiedBlobMessage,
) {
    certify_with_certified_msg(blob, current_epoch, end_epoch_at_certify, message)
}
