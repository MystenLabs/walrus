// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::managed_blob;

use std::string::String;
use sui::vec_map::{Self, VecMap};
use walrus::{
    blob,
    encoding,
    events::{emit_managed_blob_registered, emit_managed_blob_certified, emit_managed_blob_deleted},
    messages::CertifiedBlobMessage
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
// Error codes 7-8 are available.
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

// === Attribute Limits ===
/// Maximum number of attributes per blob.
const MAX_ATTRIBUTES: u64 = 100;
/// Maximum length of an attribute key in bytes.
const MAX_ATTRIBUTE_KEY_LENGTH: u64 = 1024;
/// Maximum length of an attribute value in bytes.
const MAX_ATTRIBUTE_VALUE_LENGTH: u64 = 1024;

// === Blob Type ===

/// Type of blob: Regular or Quilt (composite blob).
public enum BlobType has copy, drop, store {
    Regular,
    Quilt,
}

// === Object definitions ===

/// The managed blob structure represents a blob that has been registered with
/// some storage managed by a BlobManager, and then may eventually be certified
/// as being available in the system.
public struct ManagedBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    // Stores the epoch first certified.
    certified_epoch: option::Option<u32>,
    // TODO(heliu): Generic ownership.
    // The ID of the BlobManager that manages this blob's storage.
    blob_manager_id: ID,
    // Marks if this blob can be deleted.
    deletable: bool,
    // Type of blob: Regular or Quilt (composite blob).
    blob_type: BlobType,
    // Internal attributes map for efficient single-read access.
    // Limits: max 100 entries, max 1KB per key, max 1KB per value.
    attributes: VecMap<String, String>,
}

// === Accessors ===

public fun object_id(self: &ManagedBlob): ID {
    object::id(self)
}

public fun registered_epoch(self: &ManagedBlob): u32 {
    self.registered_epoch
}

public fun blob_id(self: &ManagedBlob): u256 {
    self.blob_id
}

public fun size(self: &ManagedBlob): u64 {
    self.size
}

public fun encoding_type(self: &ManagedBlob): u8 {
    self.encoding_type
}

public fun certified_epoch(self: &ManagedBlob): &Option<u32> {
    &self.certified_epoch
}

public fun blob_manager_id(self: &ManagedBlob): ID {
    self.blob_manager_id
}

public fun is_deletable(self: &ManagedBlob): bool {
    self.deletable
}

public fun blob_type(self: &ManagedBlob): BlobType {
    self.blob_type
}

// Removed is_quilt() - use blob_type() and match on BlobType enum directly if needed

/// Returns a reference to the internal attributes map.
public fun attributes(self: &ManagedBlob): &VecMap<String, String> {
    &self.attributes
}

/// Returns the number of attributes.
public fun attributes_count(self: &ManagedBlob): u64 {
    self.attributes.length()
}

/// Checks if an attribute key exists.
public fun has_attribute(self: &ManagedBlob, key: &String): bool {
    self.attributes.contains(key)
}

/// Gets the value for an attribute key, if it exists.
public fun get_attribute(self: &ManagedBlob, key: &String): Option<String> {
    if (self.attributes.contains(key)) {
        let idx = self.attributes.get_idx(key);
        let (_, value) = self.attributes.get_entry_by_idx(idx);
        option::some(*value)
    } else {
        option::none()
    }
}

public fun encoded_size(self: &ManagedBlob, n_shards: u16): u64 {
    encoding::encoded_blob_length(
        self.size,
        self.encoding_type,
        n_shards,
    )
}

/// Aborts if the blob is not certified.
/// Note: Expiration is checked at the BlobManager level, not here.
public(package) fun assert_certified(self: &ManagedBlob) {
    // Assert this is a certified blob
    assert!(self.certified_epoch.is_some(), ENotCertified);
}

/// Derives the blob_id for a blob given the root_hash, encoding_type and size.
public fun derive_blob_id(root_hash: u256, encoding_type: u8, size: u64): u256 {
    blob::derive_blob_id(root_hash, encoding_type, size)
}

/// Creates a new managed blob in `registered_epoch`.
/// `size` is the size of the unencoded blob.
/// The blob's storage is managed by the BlobManager identified by `blob_manager_id`.
public(package) fun new(
    blob_manager_id: ID,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    blob_type: u8,
    end_epoch_at_registration: u32,
    registered_epoch: u32,
    ctx: &mut TxContext,
): ManagedBlob {
    let id = object::new(ctx);

    // Cryptographically verify that the Blob ID authenticates
    // both the size and encoding_type (sanity check).
    assert!(derive_blob_id(root_hash, encoding_type, size) == blob_id, EInvalidBlobId);

    // Convert u8 to BlobType enum (0 = Regular, 1 = Quilt)
    let blob_type_enum = if (blob_type == 1) {
        BlobType::Quilt
    } else {
        BlobType::Regular
    };

    // Emit register event (event uses u8 for blob_type)
    emit_managed_blob_registered(
        registered_epoch,
        blob_manager_id,
        blob_id,
        size,
        encoding_type,
        deletable,
        blob_type,
        end_epoch_at_registration,
        id.to_inner(),
    );

    ManagedBlob {
        id,
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        certified_epoch: option::none(),
        blob_manager_id,
        deletable,
        blob_type: blob_type_enum,
        attributes: vec_map::empty(),
    }
}

/// Certifies that a blob will be available in the storage system.
/// Note: Storage validity is checked at the BlobManager level.
public fun certify_with_certified_msg(
    self: &mut ManagedBlob,
    current_epoch: u32,
    end_epoch_at_certify: u32,
    message: CertifiedBlobMessage,
) {
    // Check that the blob is registered in the system
    assert!(self.blob_id == message.certified_blob_id(), EInvalidBlobId);

    // Check that the blob is not already certified
    assert!(!self.certified_epoch.is_some(), EAlreadyCertified);

    // Check the blob persistence type
    assert!(
        self.deletable == message.blob_persistence_type().is_deletable(),
        EInvalidBlobPersistenceType,
    );

    // Only check object_id for deletable blobs (permanent blobs don't have object_id in
    // BlobPersistenceType).
    if (self.deletable) {
        assert!(
            message.blob_persistence_type().object_id() == object::id(self),
            EInvalidBlobObject,
        );
    };

    // Mark the blob as certified
    self.certified_epoch.fill(current_epoch);

    self.emit_certified(end_epoch_at_certify);
}

/// Deletes a deletable blob.
///
/// Emits a `ManagedBlobDeleted` event for the given epoch.
/// Aborts if the ManagedBlob is not deletable.
public(package) fun delete(self: ManagedBlob, epoch: u32) {
    let ManagedBlob {
        id,
        blob_manager_id,
        deletable,
        blob_id,
        certified_epoch,
        ..,
    } = self;
    assert!(deletable, EBlobNotDeletable);
    let object_id = id.to_inner();
    id.delete();
    emit_managed_blob_deleted(
        epoch,
        blob_manager_id,
        blob_id,
        object_id,
        certified_epoch.is_some(),
    );
}

/// Allow the owner of a managed blob object to destroy it.
///
/// Note: This does not destroy the storage since it's managed by another object.
public fun burn(self: ManagedBlob) {
    let ManagedBlob { id, .. } = self;
    id.delete();
}

/// Emits a `ManagedBlobCertified` event for the given blob.
public(package) fun emit_certified(self: &ManagedBlob, end_epoch_at_certify: u32) {
    // Emit certified event
    // Convert BlobType enum to u8 for event (event uses u8 for blob_type)
    let blob_type_u8 = match (self.blob_type) {
        BlobType::Quilt => 1,
        BlobType::Regular => 0,
    };
    emit_managed_blob_certified(
        *self.certified_epoch.borrow(),
        self.blob_manager_id,
        self.blob_id,
        self.deletable,
        blob_type_u8,
        end_epoch_at_certify,
        self.id.to_inner(),
    );
}

// === Internal Attributes ===
// These are stored directly in the ManagedBlob for efficient single-read access.
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
public fun set_attribute(self: &mut ManagedBlob, key: String, value: String) {
    validate_attribute_lengths(&key, &value);

    if (self.attributes.contains(&key)) {
        // Update existing - remove and re-insert.
        self.attributes.remove(&key);
        self.attributes.insert(key, value);
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
public fun remove_attribute(self: &mut ManagedBlob, key: &String): (String, String) {
    self.attributes.remove(key)
}

/// Removes an attribute by key if it exists.
///
/// Returns the removed value if the key existed, None otherwise.
public fun remove_attribute_if_exists(self: &mut ManagedBlob, key: &String): Option<String> {
    if (self.attributes.contains(key)) {
        let (_, value) = self.attributes.remove(key);
        option::some(value)
    } else {
        option::none()
    }
}

/// Clears all attributes.
public fun clear_attributes(self: &mut ManagedBlob) {
    while (!self.attributes.is_empty()) {
        self.attributes.pop();
    }
}

#[test_only]
public fun certify_with_certified_msg_for_testing(
    blob: &mut ManagedBlob,
    current_epoch: u32,
    end_epoch_at_certify: u32,
    message: CertifiedBlobMessage,
) {
    certify_with_certified_msg(blob, current_epoch, end_epoch_at_certify, message)
}
