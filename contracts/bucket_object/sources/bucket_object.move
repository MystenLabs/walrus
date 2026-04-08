// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Shared bucket object wrapper on top of a linked BlobBucket.
module bucket_object::bucket_object;

use bucket_object::bucket_object_inner_v1::{Self, BucketObjectInnerV1};
use std::string::String;
use sui::dynamic_field as df;

const VERSION: u64 = 1;

/// The provided capability does not belong to this bucket object.
const EInvalidBucketObjectCap: u64 = 0;
/// The bucket object version does not match the package version.
const EWrongVersion: u64 = 1;

public struct BucketObject has key {
    id: UID,
    version: u64,
}

public struct BucketObjectCap has key, store {
    id: UID,
    bucket_id: ID,
}

/// Creates and shares a new bucket object bound to the provided blob bucket ID.
public fun new(blob_bucket_id: ID, name: String, ctx: &mut TxContext): BucketObjectCap {
    let (bucket_object, cap) = new_impl(blob_bucket_id, name, ctx);
    transfer::share_object(bucket_object);
    cap
}

#[test_only]
public fun new_for_testing(
    blob_bucket_id: ID,
    name: String,
    ctx: &mut TxContext,
): (BucketObject, BucketObjectCap) {
    new_impl(blob_bucket_id, name, ctx)
}

fun new_impl(
    blob_bucket_id: ID,
    name: String,
    ctx: &mut TxContext,
): (BucketObject, BucketObjectCap) {
    let mut bucket = BucketObject {
        id: object::new(ctx),
        version: VERSION,
    };
    let cap = BucketObjectCap {
        id: object::new(ctx),
        bucket_id: object::id(&bucket),
    };
    df::add(
        &mut bucket.id,
        VERSION,
        bucket_object_inner_v1::new(name, blob_bucket_id),
    );
    (bucket, cap)
}

public fun bucket_id(self: &BucketObjectCap): ID {
    self.bucket_id
}

public fun blob_bucket_id(self: &BucketObject): ID {
    self.inner().blob_bucket_id()
}

public fun name(self: &BucketObject): &String {
    self.inner().name()
}

public fun versioning_enabled(self: &BucketObject): bool {
    self.inner().versioning_enabled()
}

public fun set_versioning_enabled(
    self: &mut BucketObject,
    cap: &BucketObjectCap,
    enabled: bool,
) {
    check_cap(self, cap);
    self.inner_mut().set_versioning_enabled(enabled);
}

fun check_cap(self: &BucketObject, cap: &BucketObjectCap) {
    assert!(object::id(self) == cap.bucket_id, EInvalidBucketObjectCap);
}

fun inner(self: &BucketObject): &BucketObjectInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow(&self.id, self.version)
}

fun inner_mut(self: &mut BucketObject): &mut BucketObjectInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow_mut(&mut self.id, self.version)
}

#[test_only]
public fun destroy_for_testing(self: BucketObject): BucketObjectInnerV1 {
    let BucketObject { mut id, version } = self;
    let inner = df::remove(&mut id, version);
    id.delete();
    inner
}

#[test_only]
public fun destroy_cap_for_testing(self: BucketObjectCap) {
    std::unit_test::destroy(self);
}
