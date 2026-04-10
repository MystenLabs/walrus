// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::bucket_object_registry;

use blob_bucket::blob_bucket::{Self as blob_bucket, BlobBucketCap};
use blob_bucket::bucket_object::{Self as bucket_object, BucketObject};
use blob_bucket::bucket_object_events;
use std::string::String;
use sui::vec_map::{Self as vec_map, VecMap};

const EBlobBucketMismatch: u64 = 0;
const EKeyAlreadyExists: u64 = 1;
const ERegistryEntryMismatch: u64 = 2;
const EDestinationObjectExists: u64 = 3;
const ECurrentObjectEtagMismatch: u64 = 4;
const EInvalidBlobBucketCap: u64 = 5;

public struct BucketObjectRegistry has key {
    id: UID,
    blob_bucket_id: ID,
    entries: VecMap<String, ID>,
}

public fun new(blob_bucket_cap: &BlobBucketCap, ctx: &mut TxContext): ID {
    let registry = new_for_testing(blob_bucket::bucket_id(blob_bucket_cap), ctx);
    let registry_id = object::id(&registry);
    transfer::share_object(registry);
    registry_id
}

#[test_only]
public fun new_for_testing(blob_bucket_id: ID, ctx: &mut TxContext): BucketObjectRegistry {
    BucketObjectRegistry {
        id: object::new(ctx),
        blob_bucket_id,
        entries: vec_map::empty(),
    }
}

public fun blob_bucket_id(self: &BucketObjectRegistry): ID {
    self.blob_bucket_id
}

public fun contains(self: &BucketObjectRegistry, key: &String): bool {
    self.entries.contains(key)
}

public fun resolve(self: &BucketObjectRegistry, key: &String): option::Option<ID> {
    self.entries.try_get(key)
}

public fun resolve_or_create_bucket_object(
    self: &mut BucketObjectRegistry,
    blob_bucket_cap: &BlobBucketCap,
    key: String,
    ctx: &mut TxContext,
): ID {
    check_blob_bucket_cap(self, blob_bucket_cap);
    if (self.entries.contains(&key)) {
        *self.entries.get(&key)
    } else {
        create_bucket_object(self, key, ctx)
    }
}

public fun copy_object_if_absent(
    self: &mut BucketObjectRegistry,
    blob_bucket_cap: &BlobBucketCap,
    source: &BucketObject,
    destination_key: String,
    object_etag: String,
    ctx: &mut TxContext,
): ID {
    check_blob_bucket_cap(self, blob_bucket_cap);
    bucket_object::share(copy_object_if_absent_impl(self, source, destination_key, object_etag, ctx))
}

public fun rename_object(
    self: &mut BucketObjectRegistry,
    blob_bucket_cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    new_key: String,
) {
    check_blob_bucket_cap(self, blob_bucket_cap);
    assert!(self.blob_bucket_id == bucket_object::blob_bucket_id(bucket_object_ref), EBlobBucketMismatch);
    let old_key = bucket_object::key(bucket_object_ref);
    let bucket_object_id = object::id(bucket_object_ref);
    assert!(self.entries.try_get(&old_key) == option::some(bucket_object_id), ERegistryEntryMismatch);
    if (old_key == new_key) {
        return
    };
    assert!(!self.entries.contains(&new_key), EKeyAlreadyExists);
    self.entries.remove(&old_key);
    self.entries.insert(new_key, bucket_object_id);
    bucket_object::set_key(bucket_object_ref, new_key);
    bucket_object_events::emit_object_renamed(
        bucket_object_id,
        self.blob_bucket_id,
        old_key,
        bucket_object::key(bucket_object_ref),
    );
}

public fun rename_object_if_match(
    self: &mut BucketObjectRegistry,
    blob_bucket_cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    expected_object_etag: String,
    new_key: String,
) {
    assert!(
        bucket_object::current_object_etag(bucket_object_ref) == expected_object_etag,
        ECurrentObjectEtagMismatch,
    );
    rename_object(self, blob_bucket_cap, bucket_object_ref, new_key);
}

#[test_only]
public fun destroy_for_testing(self: BucketObjectRegistry) {
    let BucketObjectRegistry {
        id,
        blob_bucket_id: _,
        entries: _,
    } = self;
    id.delete();
}

fun create_bucket_object(
    self: &mut BucketObjectRegistry,
    key: String,
    ctx: &mut TxContext,
): ID {
    bucket_object::share(create_bucket_object_impl(self, key, ctx))
}

#[test_only]
public fun create_bucket_object_for_testing(
    self: &mut BucketObjectRegistry,
    key: String,
    ctx: &mut TxContext,
): BucketObject {
    create_bucket_object_impl(self, key, ctx)
}

#[test_only]
public fun copy_object_if_absent_for_testing(
    self: &mut BucketObjectRegistry,
    source: &BucketObject,
    destination_key: String,
    object_etag: String,
    ctx: &mut TxContext,
): BucketObject {
    copy_object_if_absent_impl(self, source, destination_key, object_etag, ctx)
}

fun create_bucket_object_impl(
    self: &mut BucketObjectRegistry,
    key: String,
    ctx: &mut TxContext,
): BucketObject {
    assert!(!self.entries.contains(&key), EKeyAlreadyExists);
    let bucket_object_ref = bucket_object::new_unshared(self.blob_bucket_id, key, ctx);
    let bucket_object_id = object::id(&bucket_object_ref);
    let key = bucket_object::key(&bucket_object_ref);
    self.entries.insert(key, bucket_object_id);
    bucket_object_events::emit_bucket_object_created(
        bucket_object_id,
        self.blob_bucket_id,
        key,
    );
    bucket_object_ref
}

fun copy_object_if_absent_impl(
    self: &mut BucketObjectRegistry,
    source: &BucketObject,
    destination_key: String,
    object_etag: String,
    ctx: &mut TxContext,
): BucketObject {
    assert!(self.blob_bucket_id == bucket_object::blob_bucket_id(source), EBlobBucketMismatch);
    assert!(!self.entries.contains(&destination_key), EDestinationObjectExists);

    let mut destination = create_bucket_object_impl(self, destination_key, ctx);
    let destination_id = object::id(&destination);
    bucket_object::copy_current_version_to(source, &mut destination, object_etag);
    bucket_object_events::emit_object_copied(
        self.blob_bucket_id,
        object::id(source),
        bucket_object::key(source),
        destination_id,
        bucket_object::key(&destination),
        bucket_object::generation(&destination),
        bucket_object::current_object_etag(&destination),
    );
    destination
}

fun check_blob_bucket_cap(
    self: &BucketObjectRegistry,
    blob_bucket_cap: &BlobBucketCap,
) {
    assert!(
        blob_bucket::bucket_id(blob_bucket_cap) == self.blob_bucket_id,
        EInvalidBlobBucketCap,
    );
}
