// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Shared per-key object wrapper on top of a linked BlobBucket.
module bucket_object::bucket_object;

use bucket_object::bucket_object_inner_v1::{Self, BucketObjectInnerV1};
use std::string::String;
use sui::dynamic_field as df;

const VERSION: u64 = 1;

/// The bucket object version does not match the package version.
const EWrongVersion: u64 = 0;

public struct BucketObject has key {
    id: UID,
    version: u64,
}

/// Creates and shares a new bucket object bound to the provided blob bucket ID and key.
public fun new(blob_bucket_id: ID, key: String, ctx: &mut TxContext): ID {
    let bucket_object = new_impl(blob_bucket_id, key, ctx);
    let bucket_object_id = object::id(&bucket_object);
    transfer::share_object(bucket_object);
    bucket_object_id
}

#[test_only]
public fun new_for_testing(
    blob_bucket_id: ID,
    key: String,
    ctx: &mut TxContext,
): BucketObject {
    new_impl(blob_bucket_id, key, ctx)
}

fun new_impl(
    blob_bucket_id: ID,
    key: String,
    ctx: &mut TxContext,
): BucketObject {
    let mut bucket_object = BucketObject {
        id: object::new(ctx),
        version: VERSION,
    };
    df::add(
        &mut bucket_object.id,
        VERSION,
        bucket_object_inner_v1::new(blob_bucket_id, key),
    );
    bucket_object
}

public fun blob_bucket_id(self: &BucketObject): ID {
    self.inner().blob_bucket_id()
}

public fun key(self: &BucketObject): String {
    self.inner().key()
}

fun inner(self: &BucketObject): &BucketObjectInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow(&self.id, self.version)
}

#[test_only]
public fun destroy_for_testing(self: BucketObject): BucketObjectInnerV1 {
    let BucketObject { mut id, version } = self;
    let inner = df::remove(&mut id, version);
    id.delete();
    inner
}
