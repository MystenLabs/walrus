// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Per-key object entry state stored inline in a bucket registry shard.
module blob_bucket::bucket_object;

use blob_bucket::object_headers::ObjectHeaders;
use blob_bucket::object_metadata::ObjectMetadata;
use blob_bucket::object_tags::ObjectTags;
use blob_bucket::object_version::{Self as object_version, ObjectVersion};
use std::string::String;

/// A pending version is already staged for this object entry.
const EPendingVersionAlreadyExists: u64 = 0;
/// No pending version is currently staged.
const EPendingVersionMissing: u64 = 1;
/// A staged version must advance the generation by exactly one.
const EGenerationMustAdvance: u64 = 2;
/// The object entry already has a current version.
const EObjectAlreadyExists: u64 = 3;
/// The object entry does not have a current version yet.
const ECurrentVersionMissing: u64 = 4;
/// The expected object etag does not match the current version.
const EObjectEtagMismatch: u64 = 5;
/// The current version cannot be updated because it does not point to a live blob.
const ECurrentVersionHasNoBlob: u64 = 6;
/// The pending version's blob has not been certified yet.
const EPendingVersionNotCertified: u64 = 7;

public struct ObjectEntryState has copy, store, drop {
    generation: u64,
    current_version: option::Option<ObjectVersion>,
    pending_version: option::Option<ObjectVersion>,
}

public(package) fun empty(): ObjectEntryState {
    ObjectEntryState {
        generation: 0,
        current_version: option::none(),
        pending_version: option::none(),
    }
}

#[test_only]
public fun empty_for_testing(): ObjectEntryState {
    empty()
}

// Write functions.

public(package) fun assert_can_put_object(self: &ObjectEntryState) {
    assert!(
        !self.current_version.is_some()
            || object_version::delete_marker(self.current_version.borrow()),
        EObjectAlreadyExists,
    );
}

public(package) fun assert_can_update_object(
    self: &ObjectEntryState,
    expected_object_etag: String,
) {
    assert!(self.current_version.is_some(), ECurrentVersionMissing);
    assert!(
        object_version::object_etag(self.current_version.borrow()) == expected_object_etag,
        EObjectEtagMismatch,
    );
}

public(package) fun stage_registered_blob_version(
    self: &mut ObjectEntryState,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    let next_generation = self.generation + 1;
    stage_pending_version(
        self,
        object_version::new(
            next_generation,
            blob_id,
            pooled_blob_object_id,
            size,
            headers,
            metadata,
            tags,
            content_etag,
            object_etag,
            false,
        ),
    );
}

public(package) fun put_object(
    self: &mut ObjectEntryState,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    assert_can_put_object(self);
    stage_registered_blob_version(
        self,
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

public(package) fun update_object(
    self: &mut ObjectEntryState,
    expected_object_etag: String,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    assert_can_update_object(self, expected_object_etag);
    stage_registered_blob_version(
        self,
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

public(package) fun update_object_attributes_unchecked(
    self: &mut ObjectEntryState,
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

public(package) fun update_object_attributes(
    self: &mut ObjectEntryState,
    expected_object_etag: String,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    assert_can_update_object(self, expected_object_etag);
    update_object_attributes_unchecked(self, headers, metadata, tags, object_etag);
}

public(package) fun delete_object_unchecked(
    self: &mut ObjectEntryState,
    object_etag: String,
) {
    assert!(!self.pending_version.is_some(), EPendingVersionAlreadyExists);
    let next_generation = self.generation + 1;
    stage_pending_version(
        self,
        object_version::new_delete_marker(next_generation, object_etag),
    );
    promote_pending_version(self);
}

public(package) fun delete_object(
    self: &mut ObjectEntryState,
    expected_object_etag: String,
    object_etag: String,
) {
    assert_can_update_object(self, expected_object_etag);
    delete_object_unchecked(self, object_etag);
}

public(package) fun finalize_pending_version(
    self: &mut ObjectEntryState,
    pending_blob_is_certified: bool,
) {
    assert!(self.pending_version.is_some(), EPendingVersionMissing);
    if (!object_version::delete_marker(self.pending_version.borrow())) {
        assert!(pending_blob_is_certified, EPendingVersionNotCertified);
    };
    promote_pending_version(self);
}

public(package) fun stage_pending_version(self: &mut ObjectEntryState, version: ObjectVersion) {
    assert!(!self.pending_version.is_some(), EPendingVersionAlreadyExists);
    assert!(
        object_version::generation(&version) == self.generation + 1,
        EGenerationMustAdvance,
    );
    self.pending_version = option::some(version);
}

public(package) fun promote_pending_version(self: &mut ObjectEntryState) {
    assert!(self.pending_version.is_some(), EPendingVersionMissing);
    let pending_version = self.pending_version.extract();
    self.generation = object_version::generation(&pending_version);
    self.current_version = option::some(pending_version);
}

public(package) fun clear_pending_version(self: &mut ObjectEntryState): ObjectVersion {
    assert!(self.pending_version.is_some(), EPendingVersionMissing);
    self.pending_version.extract()
}

public(package) fun copy_current_version_to(
    source: &ObjectEntryState,
    destination: &mut ObjectEntryState,
    object_etag: String,
) {
    assert!(source.current_version.is_some(), ECurrentVersionMissing);
    assert_can_put_object(destination);
    assert!(!destination.pending_version.is_some(), EPendingVersionAlreadyExists);
    let source_version = source.current_version.borrow();
    assert!(object_version::has_blob(source_version), ECurrentVersionHasNoBlob);
    let next_version = object_version::new_successor(
        source_version,
        destination.generation + 1,
        object_version::headers(source_version),
        object_version::metadata(source_version),
        object_version::tags(source_version),
        object_etag,
    );
    stage_pending_version(destination, next_version);
    promote_pending_version(destination);
}

fun current_version_successor(
    self: &ObjectEntryState,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
): ObjectVersion {
    assert!(self.current_version.is_some(), ECurrentVersionMissing);
    let current_version = self.current_version.borrow();
    assert!(object_version::has_blob(current_version), ECurrentVersionHasNoBlob);
    object_version::new_successor(
        current_version,
        self.generation + 1,
        headers,
        metadata,
        tags,
        object_etag,
    )
}

// Read functions.

public fun generation(self: &ObjectEntryState): u64 {
    self.generation
}

public fun has_current_version(self: &ObjectEntryState): bool {
    self.current_version.is_some()
}

public fun is_deleted(self: &ObjectEntryState): bool {
    self.current_version.is_some()
        && object_version::delete_marker(self.current_version.borrow())
}

public fun current_version(self: &ObjectEntryState): &ObjectVersion {
    self.current_version.borrow()
}

public fun current_object_etag(self: &ObjectEntryState): String {
    assert!(self.current_version.is_some(), ECurrentVersionMissing);
    object_version::object_etag(self.current_version.borrow())
}

public fun has_pending_version(self: &ObjectEntryState): bool {
    self.pending_version.is_some()
}

public fun pending_version(self: &ObjectEntryState): &ObjectVersion {
    self.pending_version.borrow()
}

#[test_only]
public fun stage_pending_version_for_testing(
    self: &mut ObjectEntryState,
    version: ObjectVersion,
) {
    stage_pending_version(self, version);
}

#[test_only]
public fun stage_registered_blob_version_for_testing(
    self: &mut ObjectEntryState,
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
public fun promote_pending_version_for_testing(self: &mut ObjectEntryState) {
    promote_pending_version(self);
}

#[test_only]
public fun finalize_pending_version_for_testing(
    self: &mut ObjectEntryState,
    pending_blob_is_certified: bool,
) {
    finalize_pending_version(self, pending_blob_is_certified);
}

#[test_only]
public fun clear_pending_version_for_testing(self: &mut ObjectEntryState): ObjectVersion {
    clear_pending_version(self)
}
