// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Shared per-key object wrapper on top of a linked BlobBucket.
module bucket_object::bucket_object;

use bucket_object::bucket_object_inner_v1::{Self, BucketObjectInnerV1};
use bucket_object::object_version::{Self, ObjectVersion};
use std::string::String;
use sui::dynamic_field as df;

const VERSION: u64 = 1;

/// The bucket object version does not match the package version.
const EWrongVersion: u64 = 0;
/// The object version belongs to a different bucket object.
const EVersionBucketObjectMismatch: u64 = 1;
/// A pending version is already staged for this bucket object.
const EPendingVersionAlreadyExists: u64 = 2;
/// No pending version is currently staged.
const EPendingVersionMissing: u64 = 3;
/// A staged version must advance the generation by exactly one.
const EGenerationMustAdvance: u64 = 4;

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

public fun generation(self: &BucketObject): u64 {
    self.inner().generation()
}

public fun has_current_version(self: &BucketObject): bool {
    self.inner().has_current_version()
}

public fun current_version(self: &BucketObject): &ObjectVersion {
    self.inner().current_version()
}

public fun has_pending_version(self: &BucketObject): bool {
    self.inner().has_pending_version()
}

public fun pending_version(self: &BucketObject): &ObjectVersion {
    self.inner().pending_version()
}

public(package) fun stage_pending_version(self: &mut BucketObject, version: ObjectVersion) {
    assert!(!self.inner().has_pending_version(), EPendingVersionAlreadyExists);
    assert!(
        object_version::bucket_object_id(&version) == object::id(self),
        EVersionBucketObjectMismatch,
    );
    assert!(
        object_version::generation(&version) == self.inner().generation() + 1,
        EGenerationMustAdvance,
    );
    self.inner_mut().stage_pending_version(version);
}

public(package) fun promote_pending_version(self: &mut BucketObject) {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner_mut().promote_pending_version();
}

public(package) fun clear_pending_version(self: &mut BucketObject): ObjectVersion {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner_mut().clear_pending_version()
}

#[test_only]
public fun stage_pending_version_for_testing(self: &mut BucketObject, version: ObjectVersion) {
    stage_pending_version(self, version);
}

#[test_only]
public fun promote_pending_version_for_testing(self: &mut BucketObject) {
    promote_pending_version(self);
}

#[test_only]
public fun clear_pending_version_for_testing(self: &mut BucketObject): ObjectVersion {
    clear_pending_version(self)
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
