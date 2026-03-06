// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobBucket's inner state, backed by `StoragePool`.
module blob_bucket::blob_bucket_inner_v1;

use sui::coin::Coin;
use wal::wal::WAL;
use walrus::{storage_pool::StoragePool, system::System};

/// The storage pool backing this blob bucket has expired.
const EBlobBucketStorageExpired: u64 = 0;

public struct BlobBucketInnerV1 has store {
    storage_pool: StoragePool,
}

public(package) fun new(storage_pool: StoragePool): BlobBucketInnerV1 {
    BlobBucketInnerV1 { storage_pool }
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
        &mut self.storage_pool,
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
        &mut self.storage_pool,
        blob_id,
        signature,
        signers_bitmap,
        message,
    );
}

public(package) fun delete_blob(self: &mut BlobBucketInnerV1, system: &System, blob_id: u256) {
    verify_pool_active(self, system);
    system.delete_pooled_blob(&mut self.storage_pool, blob_id);
}

public(package) fun has_blob(self: &BlobBucketInnerV1, blob_id: u256): bool {
    self.storage_pool.contains_blob(blob_id)
}

public(package) fun get_blob_object_id(self: &BlobBucketInnerV1, blob_id: u256): ID {
    self.storage_pool.blob_object_id(blob_id)
}

public(package) fun end_epoch(self: &BlobBucketInnerV1): u32 {
    self.storage_pool.end_epoch()
}

public(package) fun reserved_encoded_capacity_bytes(self: &BlobBucketInnerV1): u64 {
    self.storage_pool.reserved_encoded_capacity_bytes()
}

public(package) fun used_encoded_bytes(self: &BlobBucketInnerV1): u64 {
    self.storage_pool.used_encoded_bytes()
}

public(package) fun available_encoded_bytes(self: &BlobBucketInnerV1): u64 {
    self.storage_pool.available_encoded_bytes()
}

public(package) fun blob_count(self: &BlobBucketInnerV1): u64 {
    self.storage_pool.blob_count()
}

public(package) fun storage_pool_id(self: &BlobBucketInnerV1): ID {
    self.storage_pool.object_id()
}

fun verify_pool_active(self: &BlobBucketInnerV1, system: &System) {
    assert!(self.storage_pool.end_epoch() > system.epoch(), EBlobBucketStorageExpired);
}

#[test_only]
public fun destroy_for_testing(self: BlobBucketInnerV1) {
    std::unit_test::destroy(self);
}
