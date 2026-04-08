// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Shared per-key object wrapper on top of a linked BlobBucket.
module bucket_object::bucket_object;

use blob_bucket::blob_bucket::{Self as blob_bucket, BlobBucket, BlobBucketCap};
use bucket_object::bucket_object_inner_v1::{Self, BucketObjectInnerV1};
use bucket_object::object_headers::ObjectHeaders;
use bucket_object::object_metadata::ObjectMetadata;
use bucket_object::object_version::{Self, ObjectVersion};
use sui::coin::Coin;
use std::string::String;
use sui::dynamic_field as df;
use wal::wal::WAL;
use walrus::{blob, system::System};

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
/// The provided blob bucket does not match the object's linked bucket.
const EBlobBucketMismatch: u64 = 5;
/// The requested blob has not been registered in the linked blob bucket.
const EBlobNotRegistered: u64 = 6;
/// The pending version's blob has not been certified yet.
const EPendingVersionNotCertified: u64 = 7;
/// The bucket object already has a current version.
const EObjectAlreadyExists: u64 = 8;
/// The bucket object does not have a current version yet.
const ECurrentVersionMissing: u64 = 9;
/// The expected object etag does not match the current version.
const EObjectEtagMismatch: u64 = 10;

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

public fun is_deleted(self: &BucketObject): bool {
    self.inner().has_current_version()
        && object_version::delete_marker(self.inner().current_version())
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

public fun stage_registered_blob_version(
    self: &mut BucketObject,
    blob_bucket: &BlobBucket,
    blob_id: u256,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    content_etag: String,
    object_etag: String,
) {
    assert!(object::id(blob_bucket) == self.inner().blob_bucket_id(), EBlobBucketMismatch);
    assert!(blob_bucket.has_blob(blob_id), EBlobNotRegistered);

    let version = object_version::new(
        object::id(self),
        self.inner().generation() + 1,
        blob_id,
        blob_bucket.get_blob_object_id(blob_id),
        size,
        headers,
        metadata,
        content_etag,
        object_etag,
        false,
    );
    stage_pending_version(self, version);
}

public fun put_object_if_absent_and_register(
    self: &mut BucketObject,
    blob_bucket: &mut BlobBucket,
    blob_bucket_cap: &BlobBucketCap,
    system: &mut System,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    content_etag: String,
    object_etag: String,
    ctx: &mut TxContext,
) {
    assert!(
        !self.inner().has_current_version()
            || object_version::delete_marker(self.inner().current_version()),
        EObjectAlreadyExists,
    );
    register_and_stage_new_version(
        self,
        blob_bucket,
        blob_bucket_cap,
        system,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        headers,
        metadata,
        content_etag,
        object_etag,
        ctx,
    );
}

public fun update_object_if_match_and_register(
    self: &mut BucketObject,
    blob_bucket: &mut BlobBucket,
    blob_bucket_cap: &BlobBucketCap,
    system: &mut System,
    expected_object_etag: String,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    content_etag: String,
    object_etag: String,
    ctx: &mut TxContext,
) {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    assert!(
        object_version::object_etag(self.inner().current_version()) == expected_object_etag,
        EObjectEtagMismatch,
    );
    register_and_stage_new_version(
        self,
        blob_bucket,
        blob_bucket_cap,
        system,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        headers,
        metadata,
        content_etag,
        object_etag,
        ctx,
    );
}

public fun delete_object(self: &mut BucketObject, object_etag: String) {
    assert!(!self.inner().has_pending_version(), EPendingVersionAlreadyExists);
    let bucket_object_id = object::id(self);
    let next_generation = self.inner().generation() + 1;
    stage_pending_version(
        self,
        object_version::new_delete_marker(
            bucket_object_id,
            next_generation,
            object_etag,
        ),
    );
    self.inner_mut().promote_pending_version();
}

public fun delete_object_if_match(
    self: &mut BucketObject,
    expected_object_etag: String,
    object_etag: String,
) {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    assert!(
        object_version::object_etag(self.inner().current_version()) == expected_object_etag,
        EObjectEtagMismatch,
    );
    delete_object(self, object_etag);
}

public fun finalize_pending_version_if_certified(self: &mut BucketObject, blob_bucket: &BlobBucket) {
    assert!(object::id(blob_bucket) == self.inner().blob_bucket_id(), EBlobBucketMismatch);
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    let pending_version = self.inner().pending_version();
    if (!object_version::delete_marker(pending_version)) {
        assert!(
            blob_bucket.is_blob_certified(object_version::blob_id(pending_version)),
            EPendingVersionNotCertified,
        );
    };
    self.inner_mut().promote_pending_version();
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
public fun stage_registered_blob_version_for_testing(
    self: &mut BucketObject,
    blob_bucket: &BlobBucket,
    blob_id: u256,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    content_etag: String,
    object_etag: String,
) {
    stage_registered_blob_version(
        self,
        blob_bucket,
        blob_id,
        size,
        headers,
        metadata,
        content_etag,
        object_etag,
    );
}

#[test_only]
public fun promote_pending_version_for_testing(self: &mut BucketObject) {
    promote_pending_version(self);
}

#[test_only]
public fun finalize_pending_version_if_certified_for_testing(
    self: &mut BucketObject,
    blob_bucket: &BlobBucket,
) {
    finalize_pending_version_if_certified(self, blob_bucket);
}

#[test_only]
public fun clear_pending_version_for_testing(self: &mut BucketObject): ObjectVersion {
    clear_pending_version(self)
}

fun register_and_stage_new_version(
    self: &mut BucketObject,
    blob_bucket: &mut BlobBucket,
    blob_bucket_cap: &BlobBucketCap,
    system: &mut System,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    content_etag: String,
    object_etag: String,
    ctx: &mut TxContext,
) {
    assert!(object::id(blob_bucket) == self.inner().blob_bucket_id(), EBlobBucketMismatch);

    let blob_id = blob::derive_blob_id(root_hash, encoding_type, unencoded_size);
    blob_bucket::register_blob(
        blob_bucket,
        blob_bucket_cap,
        system,
        blob_id,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        ctx,
    );
    stage_registered_blob_version(
        self,
        blob_bucket,
        blob_id,
        unencoded_size,
        headers,
        metadata,
        content_etag,
        object_etag,
    );
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
