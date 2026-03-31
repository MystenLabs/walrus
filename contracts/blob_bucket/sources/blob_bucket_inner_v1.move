// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::blob_bucket_inner_v1;

use sui::coin::Coin;
use wal::wal::WAL;
use walrus::{storage_pool::StoragePool, system::System};

/// The storage pool backing this blob bucket has expired.
const EBlobBucketStorageExpired: u64 = 0;

public struct BlobBucketInnerV1 has store {
    storage_pool: StoragePool,
}

public(package) fun new(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): BlobBucketInnerV1 {
    BlobBucketInnerV1 {
        // Keep the pool wrapped under the bucket's versioned inner state.
        storage_pool: system.create_storage_pool(
            reserved_encoded_capacity_bytes,
            epochs_ahead,
            payment,
            ctx,
        ),
    }
}

public(package) fun register_blob(
    self: &mut BlobBucketInnerV1,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
) {
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

public(package) fun certify_blob(
    self: &mut BlobBucketInnerV1,
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

public(package) fun delete_blob(self: &mut BlobBucketInnerV1, system: &System, blob_id: u256) {
    verify_pool_active(self, system);
    system.delete_pooled_blob(storage_pool_mut(self), blob_id);
}

public(package) fun extend_storage_pool(
    self: &mut BlobBucketInnerV1,
    system: &mut System,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    verify_pool_active(self, system);
    system.extend_storage_pool(storage_pool_mut(self), extended_epochs, payment);
}

public(package) fun increase_storage_pool_capacity(
    self: &mut BlobBucketInnerV1,
    system: &mut System,
    additional_encoded_capacity_bytes: u64,
    payment: &mut Coin<WAL>,
) {
    verify_pool_active(self, system);
    system.increase_storage_pool_capacity(
        storage_pool_mut(self),
        additional_encoded_capacity_bytes,
        payment,
    );
}

public(package) fun merge_storage_pool(self: &mut BlobBucketInnerV1, other: StoragePool) {
    self.storage_pool.merge_storage_pool(other);
}

public(package) fun storage_pool_id(self: &BlobBucketInnerV1): ID {
    object::id(storage_pool(self))
}

public(package) fun has_blob(self: &BlobBucketInnerV1, blob_id: u256): bool {
    storage_pool(self).contains_blob(blob_id)
}

public(package) fun get_blob_object_id(self: &BlobBucketInnerV1, blob_id: u256): ID {
    storage_pool(self).blob_object_id(blob_id)
}

public(package) fun end_epoch(self: &BlobBucketInnerV1): u32 {
    storage_pool(self).end_epoch()
}

public(package) fun reserved_encoded_capacity_bytes(self: &BlobBucketInnerV1): u64 {
    storage_pool(self).reserved_encoded_capacity_bytes()
}

public(package) fun used_encoded_bytes(self: &BlobBucketInnerV1): u64 {
    storage_pool(self).used_encoded_bytes()
}

public(package) fun available_encoded_bytes(self: &BlobBucketInnerV1): u64 {
    storage_pool(self).available_encoded_bytes()
}

public(package) fun blob_count(self: &BlobBucketInnerV1): u64 {
    storage_pool(self).blob_count()
}

fun storage_pool(self: &BlobBucketInnerV1): &StoragePool {
    &self.storage_pool
}

fun storage_pool_mut(self: &mut BlobBucketInnerV1): &mut StoragePool {
    &mut self.storage_pool
}

fun verify_pool_active(self: &BlobBucketInnerV1, system: &System) {
    assert!(storage_pool(self).end_epoch() > system.epoch(), EBlobBucketStorageExpired);
}

#[test_only]
public fun destroy_for_testing(self: BlobBucketInnerV1): StoragePool {
    let BlobBucketInnerV1 { storage_pool } = self;
    storage_pool
}
