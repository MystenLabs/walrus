// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Pooled storage model: one `UnifiedStorage` object reserves capacity for a given epoch range,
/// and multiple blobs can be registered against it. When a blob is deleted, its capacity is freed
/// back into the pool for reuse.
module walrus::unified_storage;

use sui::object_table::{Self, ObjectTable};
use walrus::{
    blob,
    encoding,
    events::{
        emit_blob_in_unified_storage_registered,
        emit_blob_in_unified_storage_certified,
        emit_blob_in_unified_storage_deleted,
    },
    messages::CertifiedBlobMessage,
};

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
/// The blob is not deletable.
const EBlobNotDeletable: u64 = 0;
/// The bounds of the storage resource are exceeded.
const EResourceBounds: u64 = 1;
/// The blob was already certified.
const EAlreadyCertified: u64 = 2;
/// The blob ID is incorrect.
const EInvalidBlobId: u64 = 3;
/// The blob persistence type does not match the certificate.
const EInvalidBlobPersistenceType: u64 = 4;
/// The blob object ID of a deletable blob does not match the ID in the certificate.
const EInvalidBlobObject: u64 = 5;
/// The unified storage pool has insufficient available capacity.
const EInsufficientCapacity: u64 = 6;
/// The unified storage pool still contains blobs and cannot be destroyed.
const EPoolNotEmpty: u64 = 7;

// === Object definitions ===

/// A pooled storage resource. Reserves `storage_size` bytes for epoch range
/// `[start_epoch, end_epoch)`. Multiple blobs can be registered against it.
public struct UnifiedStorage has key, store {
    id: UID,
    start_epoch: u32,
    end_epoch: u32,
    /// Total reserved capacity in encoded bytes (never changes).
    storage_size: u64,
    /// Sum of all active blobs' encoded sizes.
    used_size: u64,
    /// Number of blobs in the table.
    blob_count: u64,
    blobs: ObjectTable<ID, BlobInUnifiedStorage>,
}

/// A blob registered against a `UnifiedStorage` pool. Unlike `Blob`, this has no embedded
/// `Storage` field — the lifetime is determined by the parent `UnifiedStorage.end_epoch`.
public struct BlobInUnifiedStorage has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
    encoding_type: u8,
    certified_epoch: Option<u32>,
    /// Reference back to the owning pool.
    unified_storage_id: ID,
    deletable: bool,
}

// === UnifiedStorage accessors ===

public fun start_epoch(self: &UnifiedStorage): u32 {
    self.start_epoch
}

public fun end_epoch(self: &UnifiedStorage): u32 {
    self.end_epoch
}

public fun storage_size(self: &UnifiedStorage): u64 {
    self.storage_size
}

public fun used_size(self: &UnifiedStorage): u64 {
    self.used_size
}

public fun available_size(self: &UnifiedStorage): u64 {
    self.storage_size - self.used_size
}

public fun blob_count(self: &UnifiedStorage): u64 {
    self.blob_count
}

// === UnifiedStorage operations ===

/// Creates a new `UnifiedStorage` pool.
public(package) fun create(
    start_epoch: u32,
    end_epoch: u32,
    storage_size: u64,
    ctx: &mut TxContext,
): UnifiedStorage {
    UnifiedStorage {
        id: object::new(ctx),
        start_epoch,
        end_epoch,
        storage_size,
        used_size: 0,
        blob_count: 0,
        blobs: object_table::new(ctx),
    }
}

/// Returns the object ID of this unified storage pool.
public fun object_id(self: &UnifiedStorage): ID {
    object::id(self)
}

/// Extends the end epoch by `extension_epochs`.
public(package) fun extend_end_epoch(self: &mut UnifiedStorage, extension_epochs: u32) {
    self.end_epoch = self.end_epoch + extension_epochs;
}

/// Increases `used_size` by `amount`. Asserts capacity is not exceeded.
public(package) fun increase_used_size(self: &mut UnifiedStorage, amount: u64) {
    self.used_size = self.used_size + amount;
    assert!(self.used_size <= self.storage_size, EInsufficientCapacity);
}

/// Decreases `used_size` by `amount`.
public(package) fun decrease_used_size(self: &mut UnifiedStorage, amount: u64) {
    self.used_size = self.used_size - amount;
}

/// Increments the blob count.
public(package) fun inc_blob_count(self: &mut UnifiedStorage) {
    self.blob_count = self.blob_count + 1;
}

/// Decrements the blob count.
public(package) fun dec_blob_count(self: &mut UnifiedStorage) {
    self.blob_count = self.blob_count - 1;
}

/// Adds a blob to the pool's object table.
public(package) fun add_blob(self: &mut UnifiedStorage, blob: BlobInUnifiedStorage) {
    self.blobs.add(object::id(&blob), blob);
}

/// Removes and returns a blob from the pool's object table by its object ID.
public(package) fun remove_blob(self: &mut UnifiedStorage, blob_obj_id: ID): BlobInUnifiedStorage {
    self.blobs.remove(blob_obj_id)
}

/// Borrows a blob mutably from the pool's object table.
public(package) fun borrow_blob_mut(
    self: &mut UnifiedStorage,
    blob_obj_id: ID,
): &mut BlobInUnifiedStorage {
    self.blobs.borrow_mut(blob_obj_id)
}

/// Destroys the pool. Asserts the blobs table is empty and `blob_count == 0`.
public fun destroy(self: UnifiedStorage) {
    let UnifiedStorage { id, blobs, blob_count, .. } = self;
    assert!(blob_count == 0, EPoolNotEmpty);
    blobs.destroy_empty();
    id.delete();
}

// === BlobInUnifiedStorage accessors ===

public fun blob_object_id(self: &BlobInUnifiedStorage): ID {
    object::id(self)
}

public fun registered_epoch(self: &BlobInUnifiedStorage): u32 {
    self.registered_epoch
}

public fun blob_id(self: &BlobInUnifiedStorage): u256 {
    self.blob_id
}

public fun blob_size(self: &BlobInUnifiedStorage): u64 {
    self.size
}

public fun blob_encoding_type(self: &BlobInUnifiedStorage): u8 {
    self.encoding_type
}

public fun certified_epoch(self: &BlobInUnifiedStorage): &Option<u32> {
    &self.certified_epoch
}

public fun unified_storage_id(self: &BlobInUnifiedStorage): ID {
    self.unified_storage_id
}

public fun is_deletable(self: &BlobInUnifiedStorage): bool {
    self.deletable
}

/// Computes the encoded size of this blob.
public fun encoded_size(self: &BlobInUnifiedStorage, n_shards: u16): u64 {
    encoding::encoded_blob_length(self.size, self.encoding_type, n_shards)
}

// === BlobInUnifiedStorage operations ===

/// Creates a new blob for a unified storage pool.
public(package) fun new_blob(
    unified_storage_id: ID,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    registered_epoch: u32,
    end_epoch: u32,
    ctx: &mut TxContext,
): BlobInUnifiedStorage {
    // Cryptographically verify that the blob ID authenticates the size and encoding_type.
    assert!(blob::derive_blob_id(root_hash, encoding_type, size) == blob_id, EInvalidBlobId);

    let id = object::new(ctx);

    emit_blob_in_unified_storage_registered(
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        end_epoch,
        deletable,
        id.to_inner(),
        unified_storage_id,
    );

    BlobInUnifiedStorage {
        id,
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        certified_epoch: option::none(),
        unified_storage_id,
        deletable,
    }
}

/// Certifies a blob in a unified storage pool.
public(package) fun certify(
    blob: &mut BlobInUnifiedStorage,
    current_epoch: u32,
    end_epoch: u32,
    message: CertifiedBlobMessage,
) {
    assert!(blob.blob_id == message.certified_blob_id(), EInvalidBlobId);
    assert!(!blob.certified_epoch.is_some(), EAlreadyCertified);
    assert!(current_epoch < end_epoch, EResourceBounds);

    // Check the blob persistence type
    assert!(
        blob.deletable == message.blob_persistence_type().is_deletable(),
        EInvalidBlobPersistenceType,
    );

    // Check that the object id matches the message for deletable blobs
    if (blob.deletable) {
        assert!(
            message.blob_persistence_type().object_id() == object::id(blob),
            EInvalidBlobObject,
        );
    };

    blob.certified_epoch.fill(current_epoch);

    emit_blob_in_unified_storage_certified(
        current_epoch,
        blob.blob_id,
        end_epoch,
        blob.deletable,
        blob.id.to_inner(),
        blob.unified_storage_id,
        false,
    );
}

/// Deletes a deletable blob from a unified storage pool and destroys it.
public(package) fun delete_blob_from_pool(
    blob: BlobInUnifiedStorage,
    epoch: u32,
    end_epoch: u32,
) {
    let BlobInUnifiedStorage {
        id,
        deletable,
        blob_id,
        certified_epoch,
        unified_storage_id,
        ..
    } = blob;
    assert!(deletable, EBlobNotDeletable);
    assert!(end_epoch > epoch, EResourceBounds);
    let object_id = id.to_inner();
    id.delete();
    emit_blob_in_unified_storage_deleted(
        epoch,
        blob_id,
        end_epoch,
        object_id,
        certified_epoch.is_some(),
        unified_storage_id,
    );
}

// === Testing ===

#[test_only]
public fun destroy_for_testing(self: UnifiedStorage) {
    std::unit_test::destroy(self);
}

#[test_only]
public fun destroy_blob_for_testing(self: BlobInUnifiedStorage) {
    std::unit_test::destroy(self);
}
