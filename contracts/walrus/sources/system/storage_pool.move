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
    events::{emit_pool_blob_registered, emit_pool_blob_certified, emit_pool_blob_deleted},
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

// === Object definitions ===

/// A pooled storage resource. Reserves `storage_size` bytes for epoch range
/// `[start_epoch, end_epoch)`. Multiple blobs can be registered against it.
public struct StoragePool has key, store {
    id: UID,
    start_epoch: u32,
    end_epoch: u32,
    /// Total reserved capacity in encoded bytes (never changes).
    storage_size: u64,
    /// Sum of all active blobs' encoded sizes.
    used_size: u64,
    /// Number of blobs in the table.
    blob_count: u64,
    blobs: ObjectTable<u256, PoolBlob>,
}

/// A blob registered against a `StoragePool` pool. Unlike `Blob`, this has no embedded
/// `Storage` field — the lifetime is determined by the parent `StoragePool.end_epoch`.
public struct PoolBlob has key, store {
    id: UID,
    registered_epoch: u32,
    blob_id: u256,
    size: u64,
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

public fun storage_size(self: &StoragePool): u64 {
    self.storage_size
}

public fun used_size(self: &StoragePool): u64 {
    self.used_size
}

public fun available_size(self: &StoragePool): u64 {
    self.storage_size - self.used_size
}

public fun blob_count(self: &StoragePool): u64 {
    self.blob_count
}

// === StoragePool operations ===

/// Creates a new `StoragePool` pool.
public(package) fun create(
    start_epoch: u32,
    end_epoch: u32,
    storage_size: u64,
    ctx: &mut TxContext,
): StoragePool {
    StoragePool {
        id: object::new(ctx),
        start_epoch,
        end_epoch,
        storage_size,
        used_size: 0,
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

/// Increases `used_size` by `amount`. Asserts capacity is not exceeded.
public(package) fun increase_used_size(self: &mut StoragePool, amount: u64) {
    self.used_size = self.used_size + amount;
    assert!(self.used_size <= self.storage_size, EInsufficientCapacity);
}

/// Decreases `used_size` by `amount`.
public(package) fun decrease_used_size(self: &mut StoragePool, amount: u64) {
    self.used_size = self.used_size - amount;
}

/// Increments the blob count.
public(package) fun inc_blob_count(self: &mut StoragePool) {
    self.blob_count = self.blob_count + 1;
}

/// Decrements the blob count.
public(package) fun dec_blob_count(self: &mut StoragePool) {
    self.blob_count = self.blob_count - 1;
}

/// Adds a blob to the pool's object table.
public(package) fun add_blob(self: &mut StoragePool, blob: PoolBlob) {
    self.blobs.add(blob.blob_id, blob);
}

/// Removes and returns a blob from the pool's object table by its blob ID.
public(package) fun remove_blob(self: &mut StoragePool, blob_id: u256): PoolBlob {
    self.blobs.remove(blob_id)
}

/// Borrows a blob mutably from the pool's object table.
public(package) fun borrow_blob_mut(self: &mut StoragePool, blob_id: u256): &mut PoolBlob {
    self.blobs.borrow_mut(blob_id)
}

/// Destroys the pool. Asserts the blobs table is empty and `blob_count == 0`.
public fun destroy(self: StoragePool) {
    let StoragePool { id, blobs, blob_count, .. } = self;
    assert!(blob_count == 0, EPoolNotEmpty);
    blobs.destroy_empty();
    id.delete();
}

// === PoolBlob accessors ===

public fun blob_object_id(self: &PoolBlob): ID {
    object::id(self)
}

public fun registered_epoch(self: &PoolBlob): u32 {
    self.registered_epoch
}

public fun blob_id(self: &PoolBlob): u256 {
    self.blob_id
}

public fun blob_size(self: &PoolBlob): u64 {
    self.size
}

public fun blob_encoding_type(self: &PoolBlob): u8 {
    self.encoding_type
}

public fun certified_epoch(self: &PoolBlob): &Option<u32> {
    &self.certified_epoch
}

public fun storage_pool_id(self: &PoolBlob): ID {
    self.storage_pool_id
}

public fun is_deletable(self: &PoolBlob): bool {
    self.deletable
}

/// Computes the encoded size of this blob.
public fun encoded_size(self: &PoolBlob, n_shards: u16): u64 {
    encoding::encoded_blob_length(self.size, self.encoding_type, n_shards)
}

// === PoolBlob operations ===

/// Creates a new blob for a storage pool.
public(package) fun new_blob(
    storage_pool_id: ID,
    blob_id: u256,
    root_hash: u256,
    size: u64,
    encoding_type: u8,
    deletable: bool,
    registered_epoch: u32,
    end_epoch: u32,
    ctx: &mut TxContext,
): PoolBlob {
    // Cryptographically verify that the blob ID authenticates the size and encoding_type.
    assert!(blob::derive_blob_id(root_hash, encoding_type, size) == blob_id, EInvalidBlobId);

    let id = object::new(ctx);

    emit_pool_blob_registered(
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        end_epoch,
        deletable,
        id.to_inner(),
        storage_pool_id,
    );

    PoolBlob {
        id,
        registered_epoch,
        blob_id,
        size,
        encoding_type,
        certified_epoch: option::none(),
        storage_pool_id,
        deletable,
    }
}

/// Certifies a blob in a storage pool.
public(package) fun certify(
    blob: &mut PoolBlob,
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

    emit_pool_blob_certified(
        current_epoch,
        blob.blob_id,
        end_epoch,
        blob.deletable,
        blob.id.to_inner(),
        blob.storage_pool_id,
        false,
    );
}

/// Deletes a deletable blob from a storage pool and destroys it.
public(package) fun delete_blob_from_pool(blob: PoolBlob, epoch: u32, end_epoch: u32) {
    let PoolBlob {
        id,
        deletable,
        blob_id,
        certified_epoch,
        storage_pool_id,
        ..,
    } = blob;
    assert!(deletable, EBlobNotDeletable);
    assert!(end_epoch > epoch, EResourceBounds);
    let object_id = id.to_inner();
    id.delete();
    emit_pool_blob_deleted(
        epoch,
        blob_id,
        end_epoch,
        object_id,
        certified_epoch.is_some(),
        storage_pool_id,
    );
}

// === Testing ===

#[test_only]
public fun destroy_for_testing(self: StoragePool) {
    std::unit_test::destroy(self);
}

#[test_only]
public fun destroy_blob_for_testing(self: PoolBlob) {
    std::unit_test::destroy(self);
}
