// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Pooled storage model: one `StoragePool` object reserves capacity for a given epoch range,
/// and multiple blobs can be registered against it. When a blob is deleted, its capacity is freed
/// back into the pool for reuse.
module walrus::storage_pool;

use sui::object_table::{Self, ObjectTable};
use walrus::{
    blob,
    encoding,
    events::{emit_pooled_blob_certified, emit_pooled_blob_deleted, emit_pooled_blob_registered},
    messages::CertifiedBlobMessage
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
/// The storage pool has insufficient available capacity.
const EInsufficientCapacity: u64 = 6;
/// The storage pool still contains blobs and cannot be destroyed.
const EPoolNotEmpty: u64 = 7;
/// The blob size is invalid.
const EInvalidBlobSize: u64 = 8;
/// The blob count is invalid.
const EInvalidBlobCount: u64 = 9;

// === Object definitions ===

/// A pooled storage resource. Reserves `reserved_encoded_capacity_bytes` bytes for epoch range
/// `[start_epoch, end_epoch)`. Multiple blobs can be registered against it.
public struct StoragePool has key, store {
    id: UID,
    start_epoch: u32,
    end_epoch: u32,
    // TODO(WAL-1159): allow extending the storage capacity of the pool.
    /// Total reserved capacity in encoded bytes.
    reserved_encoded_capacity_bytes: u64,
    /// Sum of all active blobs' encoded sizes.
    used_encoded_bytes: u64,
    /// Number of blobs in the table.
    blob_count: u64,
    blobs: ObjectTable<u256, PooledBlob>,
}

/// A blob registered against a `StoragePool` pool. Unlike `Blob`, this has no embedded
/// `Storage` field — the lifetime is determined by the parent `StoragePool.end_epoch`.
public struct PooledBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    unencoded_size: u64,
    encoding_type: u8,
    certified_epoch: Option<u32>,
    /// Reference back to the owning pool.
    storage_pool_id: ID,
    deletable: bool,
}

// === StoragePool accessors ===

public fun start_epoch(self: &StoragePool): u32 {
    self.start_epoch
}

public fun end_epoch(self: &StoragePool): u32 {
    self.end_epoch
}

public fun reserved_encoded_capacity_bytes(self: &StoragePool): u64 {
    self.reserved_encoded_capacity_bytes
}

public fun used_encoded_bytes(self: &StoragePool): u64 {
    self.used_encoded_bytes
}

public fun available_encoded_bytes(self: &StoragePool): u64 {
    self.reserved_encoded_capacity_bytes - self.used_encoded_bytes
}

public fun blob_count(self: &StoragePool): u64 {
    self.blob_count
}

public(package) fun contains_blob(self: &StoragePool, blob_id: u256): bool {
    self.blobs.contains(blob_id)
}

public(package) fun borrow_blob(self: &StoragePool, blob_id: u256): &PooledBlob {
    self.blobs.borrow(blob_id)
}

public(package) fun blob_object_id(self: &StoragePool, blob_id: u256): ID {
    object::id(self.blobs.borrow(blob_id))
}

// === StoragePool operations ===

/// Creates a new `StoragePool`.
public(package) fun create(
    start_epoch: u32,
    end_epoch: u32,
    reserved_encoded_capacity_bytes: u64,
    ctx: &mut TxContext,
): StoragePool {
    StoragePool {
        id: object::new(ctx),
        start_epoch,
        end_epoch,
        reserved_encoded_capacity_bytes,
        used_encoded_bytes: 0,
        blob_count: 0,
        blobs: object_table::new(ctx),
    }
}

/// Returns the object ID of this storage pool.
public fun object_id(self: &StoragePool): ID {
    object::id(self)
}

/// Extends the end epoch by `extension_epochs`.
public(package) fun extend_end_epoch(self: &mut StoragePool, extension_epochs: u32) {
    self.end_epoch = self.end_epoch + extension_epochs;
}

/// Adds a blob to the pool's object table, and accounts for the space it occupies.
public(package) fun add_blob(self: &mut StoragePool, blob: PooledBlob, encoded_size: u64) {
    self.blob_count = self.blob_count + 1;
    self.used_encoded_bytes = self.used_encoded_bytes + encoded_size;
    assert!(self.used_encoded_bytes <= self.reserved_encoded_capacity_bytes, EInsufficientCapacity);
    self.blobs.add(blob.blob_id, blob);
}

/// Removes and returns a blob from the pool's object table by its blob ID.
public(package) fun remove_blob(self: &mut StoragePool, blob_id: u256, n_shards: u16): PooledBlob {
    let blob = self.blobs.borrow(blob_id);
    let encoded_size = encoding::encoded_blob_length(
        blob.unencoded_size,
        blob.encoding_type,
        n_shards,
    );
    assert!(self.used_encoded_bytes >= encoded_size, EInvalidBlobSize);
    self.used_encoded_bytes = self.used_encoded_bytes - encoded_size;
    assert!(self.blob_count >= 1, EInvalidBlobCount);
    self.blob_count = self.blob_count - 1;
    self.blobs.remove(blob_id)
}

/// Borrows a blob mutably from the pool's object table.
public(package) fun borrow_blob_mut(self: &mut StoragePool, blob_id: u256): &mut PooledBlob {
    self.blobs.borrow_mut(blob_id)
}

// TODO(WAL-1160): decide whether we want to expose this destructor.
/// Destroys the pool. Asserts the blobs table is empty and `blob_count == 0`.
public fun destroy(self: StoragePool) {
    let StoragePool { id, blobs, blob_count, .. } = self;
    assert!(blob_count == 0, EPoolNotEmpty);
    blobs.destroy_empty();
    id.delete();
}

// === PooledBlob operations ===

/// Creates a new blob for a storage pool.
public(package) fun new_pooled_blob(
    storage_pool_id: ID,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    registered_epoch: u32,
    ctx: &mut TxContext,
): PooledBlob {
    // Cryptographically verify that the blob ID authenticates the size and encoding_type.
    assert!(
        blob::derive_blob_id(root_hash, encoding_type, unencoded_size) == blob_id,
        EInvalidBlobId,
    );

    let id = object::new(ctx);

    emit_pooled_blob_registered(
        registered_epoch,
        blob_id,
        unencoded_size,
        encoding_type,
        deletable,
        id.to_inner(),
        storage_pool_id,
    );

    PooledBlob {
        id,
        registered_epoch,
        blob_id,
        unencoded_size,
        encoding_type,
        certified_epoch: option::none(),
        storage_pool_id,
        deletable,
    }
}

/// Certifies a blob in a storage pool.
public(package) fun certify(
    pooled_blob: &mut PooledBlob,
    current_epoch: u32,
    end_epoch: u32,
    message: CertifiedBlobMessage,
) {
    assert!(pooled_blob.blob_id == message.certified_blob_id(), EInvalidBlobId);
    assert!(current_epoch < end_epoch, EResourceBounds);
    assert!(!pooled_blob.certified_epoch.is_some(), EAlreadyCertified);
    pooled_blob.certified_epoch = option::some(current_epoch);

    // Check the blob persistence type
    assert!(
        pooled_blob.deletable == message.blob_persistence_type().is_deletable(),
        EInvalidBlobPersistenceType,
    );

    // Check that the object id matches the message for deletable blobs
    if (pooled_blob.deletable) {
        assert!(
            message.blob_persistence_type().object_id() == object::id(pooled_blob),
            EInvalidBlobObject,
        );
    };

    emit_pooled_blob_certified(
        current_epoch,
        pooled_blob.blob_id,
        pooled_blob.deletable,
        pooled_blob.id.to_inner(),
        pooled_blob.storage_pool_id,
    );
}

/// Deletes a deletable blob from a storage pool and destroys it.
/// Emit `PooledBlobDeleted` event for the current epoch.
public(package) fun delete_blob_object(pooled_blob: PooledBlob, epoch: u32) {
    let PooledBlob {
        id,
        deletable,
        blob_id,
        certified_epoch,
        storage_pool_id,
        ..,
    } = pooled_blob;
    assert!(deletable, EBlobNotDeletable);
    let object_id = id.to_inner();
    id.delete();
    emit_pooled_blob_deleted(
        epoch,
        blob_id,
        object_id,
        certified_epoch.is_some(),
        storage_pool_id,
    );
}

public(package) fun is_deletable(self: &PooledBlob): bool {
    self.deletable
}

public(package) fun is_certified(self: &PooledBlob): bool {
    self.certified_epoch.is_some()
}

// === Testing ===

#[test_only]
public fun destroy_for_testing(self: StoragePool) {
    std::unit_test::destroy(self);
}

#[test_only]
public fun destroy_blob_for_testing(self: PooledBlob) {
    std::unit_test::destroy(self);
}
