// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::blob;
use sui::bcs;
use sui::hash;
use walrus::blob_events::{emit_blob_registered, emit_blob_certified};
use walrus::committee::{Self, CertifiedMessage};
use walrus::encoding;
use walrus::storage_resource::Storage;
use walrus::system::System;

// A certify blob message structure
const BLOB_CERT_MSG_TYPE: u8 = 1;

// Error codes
const EInvalidMsgType: u64 = 1;
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

public struct CertifiedBlobMessage has drop {
    epoch: u64,
    blob_id: u256,
}

// === Accessors ===

public fun stored_epoch(b: &Blob): u64 {
    b.stored_epoch
}

public fun blob_id(b: &Blob): u256 {
    b.blob_id
}

public fun size(b: &Blob): u64 {
    b.size
}

public fun erasure_code_type(b: &Blob): u8 {
    b.erasure_code_type
}

public fun certified_epoch(b: &Blob): &Option<u64> {
    &b.certified_epoch
}

public fun storage(b: &Blob): &Storage {
    &b.storage
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

/// Registers a new blob in the system.
/// `size` is the size of the unencoded blob. The reserved space in `storage` must be at
/// least the size of the encoded blob.
public(package) fun register(
    sys: &System,
    storage: Storage,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    erasure_code_type: u8,
    ctx: &mut TxContext,
): Blob {
    let id = object::new(ctx);
    let stored_epoch = sys.epoch();

    // Check resource bounds.
    assert!(stored_epoch >= storage.start_epoch(), EResourceBounds);
    assert!(stored_epoch < storage.end_epoch(), EResourceBounds);

    // check that the encoded size is less than the storage size
    let encoded_size = encoding::encoded_blob_length(
        size,
        erasure_code_type,
        sys.n_shards(),
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

/// Constructs the certified blob message, note that constructing
/// implies a certified message, that is already checked.
public(package) fun certify_blob_message(message: CertifiedMessage): CertifiedBlobMessage {
    // Assert type is correct
    assert!(message.intent_type() == BLOB_CERT_MSG_TYPE, EInvalidMsgType);

    // The certified blob message contain a blob_id : u256
    let epoch = message.cert_epoch();
    let message_body = message.into_message();

    let mut bcs_body = bcs::new(message_body);
    let blob_id = bcs_body.peel_u256();

    // On purpose we do not check that nothing is left in the message
    // to allow in the future for extensibility.

    CertifiedBlobMessage { epoch, blob_id }
}

/// Certifies that a blob will be available in the storage system until the end epoch of the
/// storage associated with it, given a [`CertifiedBlobMessage`].
public(package) fun certify_with_certified_msg(
    sys: &System,
    message: CertifiedBlobMessage,
    blob: &mut Blob,
) {
    // Check that the blob is registered in the system
    assert!(blob_id(blob) == message.blob_id, EInvalidBlobId);

    // Check that the blob is not already certified
    assert!(!blob.certified_epoch.is_some(), EAlreadyCertified);

    // Check that the message is from the current epoch
    assert!(message.epoch == sys.epoch(), EWrongEpoch);

    // Check that the storage in the blob is still valid
    assert!(message.epoch < blob.storage.end_epoch(), EResourceBounds);

    // Mark the blob as certified
    blob.certified_epoch = option::some(message.epoch);

    // Emit certified event
    emit_blob_certified(
        message.epoch,
        message.blob_id,
        blob.storage.end_epoch(),
    );
}

/// Certify that a blob will be available in the storage system until the end epoch of the
/// storage associated with it.
public fun certify(
    sys: &System,
    blob: &mut Blob,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
) {
    let certified_msg = committee::verify_quorum_in_epoch(
        sys.current_committee(),
        signature,
        members,
        message,
    );
    let certified_blob_msg = certify_blob_message(certified_msg);
    certify_with_certified_msg(sys, certified_blob_msg, blob);
}

/// After the period of validity expires for the blob we can destroy the blob resource.
public fun destroy_blob_object(sys: &System, blob: Blob) {
    let current_epoch = sys.epoch();
    assert!(current_epoch >= blob.storage.end_epoch(), EResourceBounds);

    // Destroy the blob
    let Blob {
        id,
        storage,
        ..
    } = blob;

    id.delete();
    storage.destroy();
}

/// Extend the period of validity of a blob with a new storage resource.
/// The new storage resource must be the same size as the storage resource
/// used in the blob, and have a longer period of validity.
public fun extend(sys: &System, blob: &mut Blob, extension: Storage) {
    // We only extend certified blobs within their period of validity
    // with storage that extends this period. First we check for these
    // conditions.

    // Assert this is a certified blob
    assert!(blob.certified_epoch.is_some(), ENotCertified);

    // Check the blob is within its availability period
    assert!(sys.epoch() < blob.storage.end_epoch(), EResourceBounds);

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

// Testing Functions

#[test_only]
public fun drop_for_testing(b: Blob) {
    // deconstruct
    let Blob {
        id,
        storage,
        ..
    } = b;

    id.delete();
    storage.destroy();
}

#[test_only]
// Accessor for blob
public fun message_blob_id(m: &CertifiedBlobMessage): u256 {
    m.blob_id
}

#[test_only]
public fun certified_blob_message_for_testing(epoch: u64, blob_id: u256): CertifiedBlobMessage {
    CertifiedBlobMessage { epoch, blob_id }
}
