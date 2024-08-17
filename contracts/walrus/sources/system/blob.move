// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::blob;

use sui::{bcs, hash};
use walrus::{
    blob_events::{emit_blob_registered, emit_blob_certified},
    encoding,
    messages::CertifiedBlobMessage,
    storage_resource::Storage
};

// Error codes
const EResourceBounds: u64 = 2;
const EResourceSize: u64 = 3;
const EWrongEpoch: u64 = 4;
const EAlreadyCertified: u64 = 5;
const EInvalidBlobId: u64 = 6;
const ENotCertified: u64 = 7;

// == Object definitions ==

/// The blob structure represents a blob that has been registered to with some storage,
/// and then may eventually be certified as being available in the system.
public struct Blob has key, store {
    id: UID,
    stored_epoch: u64,
    blob_id: u256,
    size: u64,
    erasure_code_type: u8,
    certified_epoch: option::Option<u64>, // Store the epoch first certified
    storage: Storage,
}

// === Accessors ===

public fun stored_epoch(self: &Blob): u64 {
    self.stored_epoch
}

public fun blob_id(self: &Blob): u256 {
    self.blob_id
}

public fun size(self: &Blob): u64 {
    self.size
}

public fun erasure_code_type(self: &Blob): u8 {
    self.erasure_code_type
}

public fun certified_epoch(self: &Blob): &Option<u64> {
    &self.certified_epoch
}

public fun storage(self: &Blob): &Storage {
    &self.storage
}

public fun encoded_size(self: &Blob, n_shards: u16): u64 {
    encoding::encoded_blob_length(
        self.size,
        self.erasure_code_type,
        n_shards,
    )
}

public struct BlobIdDerivation has drop {
    erasure_code_type: u8,
    size: u64,
    root_hash: u256,
}

/// Derives the blob_id for a blob given the root_hash, erasure_code_type and size.
public(package) fun derive_blob_id(root_hash: u256, erasure_code_type: u8, size: u64): u256 {
    let blob_id_struct = BlobIdDerivation {
        erasure_code_type,
        size,
        root_hash,
    };

    let serialized = bcs::to_bytes(&blob_id_struct);
    let encoded = hash::blake2b256(&serialized);
    let mut decoder = bcs::new(encoded);
    let blob_id = decoder.peel_u256();
    blob_id
}

/// Creates a new blob in `stored_epoch`.
/// `size` is the size of the unencoded blob. The reserved space in `storage` must be at
/// least the size of the encoded blob.
public(package) fun new(
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    erasure_code_type: u8,
    // TODO: replace with Walrus context
    stored_epoch: u64,
    n_shards: u16,
    ctx: &mut TxContext,
): Blob {
    let id = object::new(ctx);

    // Check resource bounds.
    assert!(stored_epoch >= storage.start_epoch(), EResourceBounds);
    assert!(stored_epoch < storage.end_epoch(), EResourceBounds);

    // check that the encoded size is less than the storage size
    let encoded_size = encoding::encoded_blob_length(
        size,
        erasure_code_type,
        n_shards,
    );
    assert!(encoded_size <= storage.storage_size(), EResourceSize);

    // Cryptographically verify that the Blob ID authenticates
    // both the size and fe_type.
    assert!(
        derive_blob_id(root_hash, erasure_code_type, size) == blob_id,
        EInvalidBlobId,
    );

    // Emit register event
    emit_blob_registered(
        stored_epoch,
        blob_id,
        size,
        erasure_code_type,
        storage.end_epoch(),
    );

    Blob {
        id,
        stored_epoch,
        blob_id,
        size,
        //
        erasure_code_type,
        certified_epoch: option::none(),
        storage,
    }
}

/// Certifies that a blob will be available in the storage system until the end epoch of the
/// storage associated with it, given a [`CertifiedBlobMessage`].
public(package) fun certify_with_certified_msg(
    blob: &mut Blob,
    current_epoch: u64,
    message: CertifiedBlobMessage,
) {
    // Check that the blob is registered in the system
    assert!(blob_id(blob) == message.certified_blob_id(), EInvalidBlobId);

    // Check that the blob is not already certified
    assert!(!blob.certified_epoch.is_some(), EAlreadyCertified);

    // Check that the message is from the current epoch
    assert!(message.certified_epoch() == current_epoch, EWrongEpoch);

    // Check that the storage in the blob is still valid
    assert!(message.certified_epoch() < blob.storage.end_epoch(), EResourceBounds);

    // Mark the blob as certified
    blob.certified_epoch.fill(message.certified_epoch());

    // Emit certified event
    emit_blob_certified(
        message.certified_epoch(),
        message.certified_blob_id(),
        blob.storage.end_epoch(),
    );
}

/// Allow the owner of a blob object to destroy it.
public fun burn(blob: Blob) {
    let Blob {
        id,
        storage,
        ..,
    } = blob;

    id.delete();
    storage.destroy();
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public(package) fun extend_with_resource(blob: &mut Blob, extension: Storage, current_epoch: u64) {
    // We only extend certified blobs within their period of validity
    // with storage that extends this period. First we check for these
    // conditions.

    // Assert this is a certified blob
    assert!(blob.certified_epoch.is_some(), ENotCertified);

    // Check the blob is within its availability period
    assert!(current_epoch < blob.storage.end_epoch(), EResourceBounds);

    // Check that the extension is valid, and the end
    // period of the extension is after the current period.
    assert!(extension.end_epoch() > blob.storage.end_epoch(), EResourceBounds);

    // Note: if the amounts do not match there will be an abort here.
    blob.storage.fuse_periods(extension);

    // Emit certified event
    //
    // Note: We use the original certified period since for the purposes of
    // reconfiguration this is the committee that has a quorum that hold the
    // resource.
    emit_blob_certified(
        *option::borrow(&blob.certified_epoch),
        blob.blob_id,
        blob.storage.end_epoch(),
    );
}
