// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::object_version;

use blob_bucket::object_headers::{Self as object_headers, ObjectHeaders};
use blob_bucket::object_metadata::{Self as object_metadata, ObjectMetadata};
use blob_bucket::object_tags::{Self as object_tags, ObjectTags};
use std::string::String;

public struct ObjectVersion has copy, store, drop {
    bucket_object_id: ID,
    generation: u64,
    blob_id: option::Option<u256>,
    pooled_blob_object_id: option::Option<ID>,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
    delete_marker: bool,
}

public fun new(
    bucket_object_id: ID,
    generation: u64,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
    delete_marker: bool,
): ObjectVersion {
    ObjectVersion {
        bucket_object_id,
        generation,
        blob_id: option::some(blob_id),
        pooled_blob_object_id: option::some(pooled_blob_object_id),
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
        delete_marker,
    }
}

public fun new_delete_marker(
    bucket_object_id: ID,
    generation: u64,
    object_etag: String,
): ObjectVersion {
    ObjectVersion {
        bucket_object_id,
        generation,
        blob_id: option::none(),
        pooled_blob_object_id: option::none(),
        size: 0,
        headers: object_headers::empty(),
        metadata: object_metadata::empty(),
        tags: object_tags::empty(),
        content_etag: b"".to_string(),
        object_etag,
        delete_marker: true,
    }
}

#[test_only]
public fun new_for_testing(
    bucket_object_id: ID,
    generation: u64,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
    delete_marker: bool,
): ObjectVersion {
    new(
        bucket_object_id,
        generation,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
        delete_marker,
    )
}

public fun new_successor(
    self: &ObjectVersion,
    bucket_object_id: ID,
    generation: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
): ObjectVersion {
    new(
        bucket_object_id,
        generation,
        blob_id(self),
        pooled_blob_object_id(self),
        size(self),
        headers,
        metadata,
        tags,
        content_etag(self),
        object_etag,
        false,
    )
}

public fun bucket_object_id(self: &ObjectVersion): ID {
    self.bucket_object_id
}

public fun generation(self: &ObjectVersion): u64 {
    self.generation
}

public fun has_blob(self: &ObjectVersion): bool {
    self.blob_id.is_some()
}

public fun blob_id(self: &ObjectVersion): u256 {
    *self.blob_id.borrow()
}

public fun pooled_blob_object_id(self: &ObjectVersion): ID {
    *self.pooled_blob_object_id.borrow()
}

public fun size(self: &ObjectVersion): u64 {
    self.size
}

public fun headers(self: &ObjectVersion): ObjectHeaders {
    self.headers
}

public fun metadata(self: &ObjectVersion): ObjectMetadata {
    self.metadata
}

public fun tags(self: &ObjectVersion): ObjectTags {
    self.tags
}

public fun content_etag(self: &ObjectVersion): String {
    self.content_etag
}

public fun object_etag(self: &ObjectVersion): String {
    self.object_etag
}

public fun delete_marker(self: &ObjectVersion): bool {
    self.delete_marker
}

#[test_only]
public fun destroy_for_testing(self: ObjectVersion) {
    let ObjectVersion {
        bucket_object_id: _,
        generation: _,
        blob_id: _,
        pooled_blob_object_id: _,
        size: _,
        headers: _,
        metadata: _,
        tags: _,
        content_etag: _,
        object_etag: _,
        delete_marker: _,
    } = self;
}
