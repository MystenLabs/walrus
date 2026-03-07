// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobBucket built on top of `StoragePool`.
module blob_bucket::blob_bucket;

use sui::{coin::Coin, dynamic_object_field as dof};
use wal::wal::WAL;
use walrus::{storage_pool::StoragePool, system::System};

const VERSION: u64 = 1;

/// The provided capability does not belong to this blob bucket.
const EInvalidBlobBucketCap: u64 = 0;
/// The blob bucket object version does not match the package version.
const EWrongVersion: u64 = 1;
/// The storage pool backing this blob bucket has expired.
const EBlobBucketStorageExpired: u64 = 2;

public struct BlobBucket has key, store {
    id: UID,
    version: u64,
}

public struct BlobBucketCap has key, store {
    id: UID,
    bucket_id: ID,
}

public fun new(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): (BlobBucket, BlobBucketCap) {
    let mut bucket = BlobBucket {
        id: object::new(ctx),
        version: VERSION,
    };
    // Keep the pool as a child object so it stays addressable by its object ID.
    let storage_pool = system.create_storage_pool(
        reserved_encoded_capacity_bytes,
        epochs_ahead,
        payment,
        ctx,
    );
    dof::add(&mut bucket.id, VERSION, storage_pool);

    let cap = BlobBucketCap {
        id: object::new(ctx),
        bucket_id: object::id(&bucket),
    };

    (bucket, cap)
}

public fun share(self: BlobBucket) {
    transfer::share_object(self);
}

public fun register_blob(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    verify_pool_active(self, system);
    system.register_pooled_blob(
        storage_pool_mut(self),
        blob_id,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        ctx,
    );
}

public fun certify_blob(
    self: &mut BlobBucket,
    system: &System,
    blob_id: u256,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    verify_pool_active(self, system);
    system.certify_pooled_blob(
        storage_pool_mut(self),
        blob_id,
        signature,
        signers_bitmap,
        message,
    );
}

public fun delete_blob(self: &mut BlobBucket, cap: &BlobBucketCap, system: &System, blob_id: u256) {
    check_cap(self, cap);
    verify_pool_active(self, system);
    system.delete_pooled_blob(storage_pool_mut(self), blob_id);
}

public fun extend_storage_pool(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    check_cap(self, cap);
    verify_pool_active(self, system);
    system.extend_storage_pool(storage_pool_mut(self), extended_epochs, payment);
}

public fun version(self: &BlobBucket): u64 {
    self.version
}

public fun bucket_id(self: &BlobBucket): ID {
    object::id(self)
}

public fun has_blob(self: &BlobBucket, blob_id: u256): bool {
    storage_pool(self).contains_blob(blob_id)
}

public fun get_blob_object_id(self: &BlobBucket, blob_id: u256): ID {
    storage_pool(self).blob_object_id(blob_id)
}

public fun storage_pool_id(self: &BlobBucket): ID {
    object::id(storage_pool(self))
}

public fun end_epoch(self: &BlobBucket): u32 {
    storage_pool(self).end_epoch()
}

public fun reserved_encoded_capacity_bytes(self: &BlobBucket): u64 {
    storage_pool(self).reserved_encoded_capacity_bytes()
}

public fun used_encoded_bytes(self: &BlobBucket): u64 {
    storage_pool(self).used_encoded_bytes()
}

public fun available_encoded_bytes(self: &BlobBucket): u64 {
    storage_pool(self).available_encoded_bytes()
}

public fun blob_count(self: &BlobBucket): u64 {
    storage_pool(self).blob_count()
}

fun check_cap(self: &BlobBucket, cap: &BlobBucketCap) {
    assert!(object::id(self) == cap.bucket_id, EInvalidBlobBucketCap);
}

fun storage_pool(self: &BlobBucket): &StoragePool {
    assert!(self.version == VERSION, EWrongVersion);
    dof::borrow(&self.id, VERSION)
}

fun storage_pool_mut(self: &mut BlobBucket): &mut StoragePool {
    assert!(self.version == VERSION, EWrongVersion);
    dof::borrow_mut(&mut self.id, VERSION)
}

fun verify_pool_active(self: &BlobBucket, system: &System) {
    assert!(storage_pool(self).end_epoch() > system.epoch(), EBlobBucketStorageExpired);
}

#[test_only]
public fun destroy_for_testing(self: BlobBucket): StoragePool {
    let mut bucket = self;
    let storage_pool = dof::remove(&mut bucket.id, VERSION);
    let BlobBucket { id, version: _ } = bucket;
    id.delete();
    storage_pool
}

#[test_only]
public fun destroy_cap_for_testing(self: BlobBucketCap) {
    std::unit_test::destroy(self);
}
