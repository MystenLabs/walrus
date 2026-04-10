// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Shared per-key object wrapper on top of a linked BlobBucket.
module blob_bucket::bucket_object;

use blob_bucket::bucket_object_events;
use blob_bucket::bucket_object_inner_v1::{Self, BucketObjectInnerV1};
use blob_bucket::object_headers::ObjectHeaders;
use blob_bucket::object_metadata::ObjectMetadata;
use blob_bucket::object_tags::ObjectTags;
use blob_bucket::object_version::{Self, ObjectVersion};
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
/// The provided blob bucket does not match the object's linked bucket.
const EBlobBucketMismatch: u64 = 5;
/// The pending version's blob has not been certified yet.
const EPendingVersionNotCertified: u64 = 6;
/// The bucket object already has a current version.
const EObjectAlreadyExists: u64 = 7;
/// The bucket object does not have a current version yet.
const ECurrentVersionMissing: u64 = 8;
/// The expected object etag does not match the current version.
const EObjectEtagMismatch: u64 = 9;
/// The current version cannot be updated because it does not point to a live blob.
const ECurrentVersionHasNoBlob: u64 = 10;

public struct BucketObject has key {
    id: UID,
    version: u64,
}

/// Creates and shares a new bucket object bound to the provided blob bucket ID and key.
public(package) fun new(blob_bucket_id: ID, key: String, ctx: &mut TxContext): ID {
    let bucket_object = new_unshared(blob_bucket_id, key, ctx);
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
    new_unshared(blob_bucket_id, key, ctx)
}

public(package) fun new_unshared(
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

public(package) fun share(bucket_object: BucketObject): ID {
    let bucket_object_id = object::id(&bucket_object);
    transfer::share_object(bucket_object);
    bucket_object_id
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

public fun current_object_etag(self: &BucketObject): String {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    object_version::object_etag(self.inner().current_version())
}

public fun has_pending_version(self: &BucketObject): bool {
    self.inner().has_pending_version()
}

public fun pending_version(self: &BucketObject): &ObjectVersion {
    self.inner().pending_version()
}

public(package) fun assert_can_put_object_if_absent(self: &BucketObject) {
    assert!(
        !self.inner().has_current_version()
            || object_version::delete_marker(self.inner().current_version()),
        EObjectAlreadyExists,
    );
}

public(package) fun assert_can_update_object_if_match(
    self: &BucketObject,
    expected_object_etag: String,
) {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    assert!(
        object_version::object_etag(self.inner().current_version()) == expected_object_etag,
        EObjectEtagMismatch,
    );
}

public(package) fun stage_registered_blob_version(
    self: &mut BucketObject,
    blob_bucket_id: ID,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    assert!(blob_bucket_id == self.inner().blob_bucket_id(), EBlobBucketMismatch);

    let version = object_version::new(
        object::id(self),
        self.inner().generation() + 1,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
        false,
    );
    stage_pending_version(self, version);
}

public(package) fun put_object_if_absent(
    self: &mut BucketObject,
    blob_bucket_id: ID,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    assert_can_put_object_if_absent(self);
    stage_registered_blob_version(
        self,
        blob_bucket_id,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
}

public(package) fun update_object_if_match(
    self: &mut BucketObject,
    expected_object_etag: String,
    blob_bucket_id: ID,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    assert_can_update_object_if_match(self, expected_object_etag);
    stage_registered_blob_version(
        self,
        blob_bucket_id,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
}

public(package) fun update_object_attributes(
    self: &mut BucketObject,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    let next_version = current_version_successor(
        self,
        headers,
        metadata,
        tags,
        object_etag,
    );
    stage_pending_version(self, next_version);
    promote_pending_version(self);
}

public(package) fun update_object_attributes_if_match(
    self: &mut BucketObject,
    expected_object_etag: String,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    assert!(
        object_version::object_etag(self.inner().current_version()) == expected_object_etag,
        EObjectEtagMismatch,
    );
    update_object_attributes(self, headers, metadata, tags, object_etag);
}

public(package) fun delete_object(self: &mut BucketObject, object_etag: String) {
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
    promote_pending_version(self);
}

public(package) fun delete_object_if_match(
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

public(package) fun finalize_pending_version_if_certified(
    self: &mut BucketObject,
    blob_bucket_id: ID,
    pending_blob_is_certified: bool,
) {
    assert!(blob_bucket_id == self.inner().blob_bucket_id(), EBlobBucketMismatch);
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    let pending_version = self.inner().pending_version();
    if (!object_version::delete_marker(pending_version)) {
        assert!(pending_blob_is_certified, EPendingVersionNotCertified);
    };
    promote_pending_version(self);
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
    bucket_object_events::emit_object_version_staged(
        object::id(self),
        self.inner().blob_bucket_id(),
        self.inner().key(),
        object_version::generation(&version),
        object_version::object_etag(&version),
        object_version::delete_marker(&version),
    );
    self.inner_mut().stage_pending_version(version);
}

public(package) fun promote_pending_version(self: &mut BucketObject) {
    assert!(self.inner().has_pending_version(), EPendingVersionMissing);
    self.inner_mut().promote_pending_version();
    let current_version = self.inner().current_version();
    bucket_object_events::emit_object_version_promoted(
        object::id(self),
        self.inner().blob_bucket_id(),
        self.inner().key(),
        object_version::generation(current_version),
        object_version::object_etag(current_version),
        object_version::delete_marker(current_version),
    );
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
    blob_bucket_id: ID,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    stage_registered_blob_version(
        self,
        blob_bucket_id,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
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
    blob_bucket_id: ID,
    pending_blob_is_certified: bool,
) {
    finalize_pending_version_if_certified(self, blob_bucket_id, pending_blob_is_certified);
}

#[test_only]
public fun clear_pending_version_for_testing(self: &mut BucketObject): ObjectVersion {
    clear_pending_version(self)
}

fun current_version_successor(
    self: &BucketObject,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
): ObjectVersion {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    let current_version = self.inner().current_version();
    assert!(object_version::has_blob(current_version), ECurrentVersionHasNoBlob);
    object_version::new_successor(
        current_version,
        object::id(self),
        self.inner().generation() + 1,
        headers,
        metadata,
        tags,
        object_etag,
    )
}

fun current_live_version(self: &BucketObject): &ObjectVersion {
    assert!(self.inner().has_current_version(), ECurrentVersionMissing);
    let current_version = self.inner().current_version();
    assert!(object_version::has_blob(current_version), ECurrentVersionHasNoBlob);
    current_version
}

public(package) fun copy_current_version_to(
    source: &BucketObject,
    destination: &mut BucketObject,
    object_etag: String,
) {
    assert!(source.inner().blob_bucket_id() == destination.inner().blob_bucket_id(), EBlobBucketMismatch);
    assert!(!destination.inner().has_current_version(), EObjectAlreadyExists);
    assert!(!destination.inner().has_pending_version(), EPendingVersionAlreadyExists);
    let source_version = current_live_version(source);
    let next_version = object_version::new_successor(
        source_version,
        object::id(destination),
        destination.inner().generation() + 1,
        object_version::headers(source_version),
        object_version::metadata(source_version),
        object_version::tags(source_version),
        object_etag,
    );
    stage_pending_version(destination, next_version);
    promote_pending_version(destination);
}

public(package) fun set_key(self: &mut BucketObject, key: String) {
    self.inner_mut().set_key(key);
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
