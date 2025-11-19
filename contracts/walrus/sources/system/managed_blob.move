// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::managed_blob;

use std::string::String;
use sui::{bcs, dynamic_field, hash, object};
use walrus::{
    blob,
    encoding,
    events::{emit_managed_blob_registered, emit_managed_blob_certified, emit_managed_blob_deleted},
    messages::CertifiedBlobMessage,
    metadata::{Self, Metadata}
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
/// The metadata field already exists.
const EDuplicateMetadata: u64 = 7;
/// The blob does not have any metadata.
const EMissingMetadata: u64 = 8;
/// The blob persistence type of the blob does not match the certificate.
const EInvalidBlobPersistenceType: u64 = 9;
/// The blob object ID of a deletable blob does not match the ID in the certificate.
const EInvalidBlobObject: u64 = 10;

// The fixed dynamic field name for metadata
const METADATA_DF: vector<u8> = b"metadata";

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
    // The ID of the BlobManager that manages this blob's storage.
    blob_manager_id: ID,
    // Marks if this blob can be deleted.
    deletable: bool,
    // Type of blob: Regular or Quilt (composite blob).
    blob_type: BlobType,
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
    }
}

/// Certifies that a blob will be available in the storage system.
/// Note: Storage validity is checked at the BlobManager level.
public fun certify_with_certified_msg(
    self: &mut ManagedBlob,
    current_epoch: u32,
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

    // Check that the object id matches the message
    if (self.deletable) {
        assert!(
            message.blob_persistence_type().object_id() == object::id(self),
            EInvalidBlobObject,
        );
    };

    // Mark the blob as certified
    self.certified_epoch.fill(current_epoch);

    self.emit_certified();
}

/// Deletes a deletable blob.
///
/// Emits a `ManagedBlobDeleted` event for the given epoch.
/// Aborts if the ManagedBlob is not deletable.
/// Also removes any metadata associated with the blob.
public(package) fun delete(mut self: ManagedBlob, epoch: u32) {
    dynamic_field::remove_if_exists<_, Metadata>(&mut self.id, METADATA_DF);
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
/// This function also burns any [`Metadata`] associated with the blob, if present.
/// Note: This does not destroy the storage since it's managed by another object.
public fun burn(mut self: ManagedBlob) {
    dynamic_field::remove_if_exists<_, Metadata>(&mut self.id, METADATA_DF);
    let ManagedBlob { id, .. } = self;

    id.delete();
}

/// Emits a `ManagedBlobCertified` event for the given blob.
public(package) fun emit_certified(self: &ManagedBlob) {
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
        self.id.to_inner(),
    );
}

// === Metadata ===

/// Adds the metadata dynamic field to the ManagedBlob.
///
/// Aborts if the metadata is already present.
public fun add_metadata(self: &mut ManagedBlob, metadata: Metadata) {
    assert!(!dynamic_field::exists_(&self.id, METADATA_DF), EDuplicateMetadata);
    dynamic_field::add(&mut self.id, METADATA_DF, metadata)
}

/// Adds the metadata dynamic field to the ManagedBlob, replacing the existing
/// metadata if present.
///
/// Returns the replaced metadata if present.
public fun add_or_replace_metadata(
    self: &mut ManagedBlob,
    metadata: Metadata,
): option::Option<Metadata> {
    let old_metadata = if (dynamic_field::exists_(&self.id, METADATA_DF)) {
        option::some(self.take_metadata())
    } else {
        option::none()
    };
    self.add_metadata(metadata);
    old_metadata
}

/// Removes the metadata dynamic field from the ManagedBlob, returning the
/// contained `Metadata`.
///
/// Aborts if the metadata does not exist.
public fun take_metadata(self: &mut ManagedBlob): Metadata {
    assert!(dynamic_field::exists_(&self.id, METADATA_DF), EMissingMetadata);
    dynamic_field::remove(&mut self.id, METADATA_DF)
}

/// Returns the metadata associated with the ManagedBlob.
///
/// Aborts if the metadata does not exist.
fun metadata(self: &mut ManagedBlob): &mut Metadata {
    assert!(dynamic_field::exists_(&self.id, METADATA_DF), EMissingMetadata);
    dynamic_field::borrow_mut(&mut self.id, METADATA_DF)
}

/// Returns the metadata associated with the ManagedBlob, if it exists.
///
/// Creates new metadata if it does not exist.
fun metadata_or_create(self: &mut ManagedBlob): &mut Metadata {
    if (!dynamic_field::exists_(&self.id, METADATA_DF)) {
        self.add_metadata(metadata::new());
    };
    dynamic_field::borrow_mut(&mut self.id, METADATA_DF)
}

/// Inserts a key-value pair into the metadata.
///
/// If the key is already present, the value is updated. Creates new metadata on
/// the ManagedBlob object if it does not exist already.
public fun insert_or_update_metadata_pair(self: &mut ManagedBlob, key: String, value: String) {
    self.metadata_or_create().insert_or_update(key, value)
}

/// Removes the metadata associated with the given key.
///
/// Aborts if the metadata does not exist.
public fun remove_metadata_pair(self: &mut ManagedBlob, key: &String): (String, String) {
    self.metadata().remove(key)
}

/// Removes and returns the metadata associated with the given key, if it exists.
public fun remove_metadata_pair_if_exists(
    self: &mut ManagedBlob,
    key: &String,
): option::Option<String> {
    if (!dynamic_field::exists_(&self.id, METADATA_DF)) {
        option::none()
    } else {
        self.metadata().remove_if_exists(key)
    }
}

#[test_only]
public fun certify_with_certified_msg_for_testing(
    blob: &mut ManagedBlob,
    current_epoch: u32,
    message: CertifiedBlobMessage,
) {
    certify_with_certified_msg(blob, current_epoch, message)
}
