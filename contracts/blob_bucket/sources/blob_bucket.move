// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobBucket built on top of `StoragePool`.
module blob_bucket::blob_bucket;

use blob_bucket::blob_bucket_inner_v1::{Self, BlobBucketInnerV1};
use sui::{coin::Coin, dynamic_field as df};
use wal::wal::WAL;
use walrus::system::System;

const VERSION: u64 = 1;

/// The provided capability does not belong to this blob bucket.
const EInvalidBlobBucketCap: u64 = 0;
/// The blob bucket object version does not match the package version.
const EWrongVersion: u64 = 1;

public struct BlobBucket has key {
    id: UID,
    version: u64,
}

public struct BlobBucketCap has key, store {
    id: UID,
    bucket_id: ID,
}

/// Creates and shares a new blob bucket, returning the capability required for admin actions.
public fun new(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): BlobBucketCap {
    let (bucket, cap) = new_impl(
        system,
        reserved_encoded_capacity_bytes,
        epochs_ahead,
        payment,
        ctx,
    );
    transfer::share_object(bucket);
    cap
}

#[test_only]
public fun new_for_testing(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): (BlobBucket, BlobBucketCap) {
    new_impl(system, reserved_encoded_capacity_bytes, epochs_ahead, payment, ctx)
}

fun new_impl(
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
    let cap = BlobBucketCap {
        id: object::new(ctx),
        bucket_id: object::id(&bucket),
    };
    df::add(
        &mut bucket.id,
        VERSION,
        blob_bucket_inner_v1::new(
            system,
            reserved_encoded_capacity_bytes,
            epochs_ahead,
            payment,
            ctx,
        ),
    );
    (bucket, cap)
}

public fun bucket_id(self: &BlobBucketCap): ID {
    self.bucket_id
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
    self
        .inner_mut()
        .register_blob(
            system,
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
    self.inner_mut().certify_blob(system, blob_id, signature, signers_bitmap, message);
}

public fun delete_blob(self: &mut BlobBucket, cap: &BlobBucketCap, system: &System, blob_id: u256) {
    check_cap(self, cap);
    self.inner_mut().delete_blob(system, blob_id);
}

public fun extend_storage_pool(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    check_cap(self, cap);
    self.inner_mut().extend_storage_pool(system, extended_epochs, payment);
}

public fun storage_pool_id(self: &BlobBucket): ID {
    self.inner().storage_pool_id()
}

public fun increase_storage_pool_capacity(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    additional_encoded_capacity_bytes: u64,
    payment: &mut Coin<WAL>,
) {
    check_cap(self, cap);
    self
        .inner_mut()
        .increase_storage_pool_capacity(
            system,
            additional_encoded_capacity_bytes,
            payment,
        );
}

public(package) fun has_blob(self: &BlobBucket, blob_id: u256): bool {
    self.inner().has_blob(blob_id)
}

public(package) fun get_blob_object_id(self: &BlobBucket, blob_id: u256): ID {
    self.inner().get_blob_object_id(blob_id)
}

public(package) fun end_epoch(self: &BlobBucket): u32 {
    self.inner().end_epoch()
}

public(package) fun reserved_encoded_capacity_bytes(self: &BlobBucket): u64 {
    self.inner().reserved_encoded_capacity_bytes()
}

public(package) fun used_encoded_bytes(self: &BlobBucket): u64 {
    self.inner().used_encoded_bytes()
}

public(package) fun available_encoded_bytes(self: &BlobBucket): u64 {
    self.inner().available_encoded_bytes()
}

public(package) fun blob_count(self: &BlobBucket): u64 {
    self.inner().blob_count()
}

fun check_cap(self: &BlobBucket, cap: &BlobBucketCap) {
    assert!(object::id(self) == cap.bucket_id, EInvalidBlobBucketCap);
}

fun inner(self: &BlobBucket): &BlobBucketInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow(&self.id, self.version)
}

fun inner_mut(self: &mut BlobBucket): &mut BlobBucketInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow_mut(&mut self.id, self.version)
}

#[test_only]
public fun destroy_for_testing(self: BlobBucket): BlobBucketInnerV1 {
    let BlobBucket { mut id, version } = self;
    let inner = df::remove(&mut id, version);
    id.delete();
    inner
}

#[test_only]
public fun destroy_cap_for_testing(self: BlobBucketCap) {
    std::unit_test::destroy(self);
}
