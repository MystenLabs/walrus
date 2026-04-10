// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::bucket_object_events;

use std::string::String;
use sui::event;

public struct BucketObjectCreated has copy, drop {
    bucket_object_id: ID,
    blob_bucket_id: ID,
    key: String,
}

public struct ObjectVersionStaged has copy, drop {
    bucket_object_id: ID,
    blob_bucket_id: ID,
    key: String,
    generation: u64,
    object_etag: String,
    delete_marker: bool,
}

public struct ObjectVersionPromoted has copy, drop {
    bucket_object_id: ID,
    blob_bucket_id: ID,
    key: String,
    generation: u64,
    object_etag: String,
    delete_marker: bool,
}

public struct ObjectCopied has copy, drop {
    blob_bucket_id: ID,
    source_bucket_object_id: ID,
    source_key: String,
    destination_bucket_object_id: ID,
    destination_key: String,
    generation: u64,
    object_etag: String,
}

public struct ObjectRenamed has copy, drop {
    bucket_object_id: ID,
    blob_bucket_id: ID,
    old_key: String,
    new_key: String,
}

public fun emit_bucket_object_created(bucket_object_id: ID, blob_bucket_id: ID, key: String) {
    event::emit(BucketObjectCreated {
        bucket_object_id,
        blob_bucket_id,
        key,
    });
}

public fun emit_object_version_staged(
    bucket_object_id: ID,
    blob_bucket_id: ID,
    key: String,
    generation: u64,
    object_etag: String,
    delete_marker: bool,
) {
    event::emit(ObjectVersionStaged {
        bucket_object_id,
        blob_bucket_id,
        key,
        generation,
        object_etag,
        delete_marker,
    });
}

public fun emit_object_version_promoted(
    bucket_object_id: ID,
    blob_bucket_id: ID,
    key: String,
    generation: u64,
    object_etag: String,
    delete_marker: bool,
) {
    event::emit(ObjectVersionPromoted {
        bucket_object_id,
        blob_bucket_id,
        key,
        generation,
        object_etag,
        delete_marker,
    });
}

public fun emit_object_copied(
    blob_bucket_id: ID,
    source_bucket_object_id: ID,
    source_key: String,
    destination_bucket_object_id: ID,
    destination_key: String,
    generation: u64,
    object_etag: String,
) {
    event::emit(ObjectCopied {
        blob_bucket_id,
        source_bucket_object_id,
        source_key,
        destination_bucket_object_id,
        destination_key,
        generation,
        object_etag,
    });
}

public fun emit_object_renamed(
    bucket_object_id: ID,
    blob_bucket_id: ID,
    old_key: String,
    new_key: String,
) {
    event::emit(ObjectRenamed {
        bucket_object_id,
        blob_bucket_id,
        old_key,
        new_key,
    });
}
