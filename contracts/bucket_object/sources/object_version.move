// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module bucket_object::object_version;

use std::string::String;

public struct ObjectVersion has copy, store, drop {
    bucket_object_id: ID,
    generation: u64,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
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
    content_etag: String,
    object_etag: String,
    delete_marker: bool,
): ObjectVersion {
    ObjectVersion {
        bucket_object_id,
        generation,
        blob_id,
        pooled_blob_object_id,
        size,
        content_etag,
        object_etag,
        delete_marker,
    }
}

#[test_only]
public fun new_for_testing(
    bucket_object_id: ID,
    generation: u64,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
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
        content_etag,
        object_etag,
        delete_marker,
    )
}

public fun bucket_object_id(self: &ObjectVersion): ID {
    self.bucket_object_id
}

public fun generation(self: &ObjectVersion): u64 {
    self.generation
}

public fun blob_id(self: &ObjectVersion): u256 {
    self.blob_id
}

public fun pooled_blob_object_id(self: &ObjectVersion): ID {
    self.pooled_blob_object_id
}

public fun size(self: &ObjectVersion): u64 {
    self.size
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
        content_etag: _,
        object_etag: _,
        delete_marker: _,
    } = self;
}
