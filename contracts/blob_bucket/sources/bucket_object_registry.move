// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module blob_bucket::bucket_object_registry;

use blob_bucket::bucket_object::{Self as bucket_object, ObjectEntryState};
use blob_bucket::bucket_object_events;
use blob_bucket::object_headers::ObjectHeaders;
use blob_bucket::object_metadata::ObjectMetadata;
use blob_bucket::object_tags::ObjectTags;
use blob_bucket::object_version::{Self as object_version, ObjectVersion};
use std::string::{Self as string, String};
use sui::vec_map::{Self as vec_map, VecMap};

const EObjectAlreadyExists: u64 = 0;
const EObjectMissing: u64 = 1;
const ECurrentObjectEtagMismatch: u64 = 2;
const EInvalidShardCount: u64 = 3;
const DEFAULT_SHARD_COUNT: u64 = 64;

public struct BucketObjectRegistry has key {
    id: UID,
    blob_bucket_id: ID,
    shards: vector<BucketNamespaceShard>,
}

public struct BucketNamespaceShard has copy, drop, store {
    entries: VecMap<String, ObjectEntryState>,
}

public(package) fun new(blob_bucket_id: ID, ctx: &mut TxContext): ID {
    let registry = new_impl(blob_bucket_id, DEFAULT_SHARD_COUNT, ctx);
    let registry_id = object::id(&registry);
    transfer::share_object(registry);
    registry_id
}

#[test_only]
public fun new_for_testing(blob_bucket_id: ID, ctx: &mut TxContext): BucketObjectRegistry {
    new_impl(blob_bucket_id, DEFAULT_SHARD_COUNT, ctx)
}

#[test_only]
public fun new_with_shard_count_for_testing(
    blob_bucket_id: ID,
    shard_count: u64,
    ctx: &mut TxContext,
): BucketObjectRegistry {
    new_impl(blob_bucket_id, shard_count, ctx)
}

fun new_impl(
    blob_bucket_id: ID,
    shard_count: u64,
    ctx: &mut TxContext,
): BucketObjectRegistry {
    assert!(shard_count > 0, EInvalidShardCount);
    let mut shards = vector[];
    let mut i = 0;
    while (i < shard_count) {
        shards.push_back(BucketNamespaceShard {
            entries: vec_map::empty(),
        });
        i = i + 1;
    };
    BucketObjectRegistry {
        id: object::new(ctx),
        blob_bucket_id,
        shards,
    }
}

public fun blob_bucket_id(self: &BucketObjectRegistry): ID {
    self.blob_bucket_id
}

public fun shard_count(self: &BucketObjectRegistry): u64 {
    self.shards.length()
}

public fun contains(self: &BucketObjectRegistry, key: &String): bool {
    shard(self, key).entries.contains(key)
}

public fun resolve(self: &BucketObjectRegistry, key: &String): option::Option<ObjectEntryState> {
    let shard = shard(self, key);
    if (shard.entries.contains(key)) {
        option::some(*shard.entries.get(key))
    } else {
        option::none()
    }
}

public fun current_version(
    self: &BucketObjectRegistry,
    key: &String,
): option::Option<ObjectVersion> {
    let object_entry = resolve(self, key);
    if (object_entry.is_some()) {
        let object_entry = object_entry.destroy_some();
        if (bucket_object::has_current_version(&object_entry)) {
            option::some(*bucket_object::current_version(&object_entry))
        } else {
            option::none()
        }
    } else {
        object_entry.destroy_none();
        option::none()
    }
}

public fun pending_version(
    self: &BucketObjectRegistry,
    key: &String,
): option::Option<ObjectVersion> {
    let object_entry = resolve(self, key);
    if (object_entry.is_some()) {
        let object_entry = object_entry.destroy_some();
        if (bucket_object::has_pending_version(&object_entry)) {
            option::some(*bucket_object::pending_version(&object_entry))
        } else {
            option::none()
        }
    } else {
        object_entry.destroy_none();
        option::none()
    }
}

public fun current_object_etag(
    self: &BucketObjectRegistry,
    key: &String,
): option::Option<String> {
    let object_entry = resolve(self, key);
    if (object_entry.is_some()) {
        let object_entry = object_entry.destroy_some();
        if (bucket_object::has_current_version(&object_entry)) {
            option::some(bucket_object::current_object_etag(&object_entry))
        } else {
            option::none()
        }
    } else {
        object_entry.destroy_none();
        option::none()
    }
}

public(package) fun resolve_or_create_bucket_object(
    self: &mut BucketObjectRegistry,
    key: String,
): bool {
    ensure_entry(self, key)
}

public(package) fun put_object_if_absent(
    self: &mut BucketObjectRegistry,
    key: String,
    blob_id: u256,
    pooled_blob_object_id: ID,
    size: u64,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let _ = ensure_entry(self, key);
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::put_object_if_absent(
        object_entry,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
    emit_object_version_staged(blob_bucket_id, key, object_entry);
}

public(package) fun update_object_if_match(
    self: &mut BucketObjectRegistry,
    key: String,
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
    let blob_bucket_id = self.blob_bucket_id;
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::update_object_if_match(
        object_entry,
        expected_object_etag,
        blob_id,
        pooled_blob_object_id,
        size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
    emit_object_version_staged(blob_bucket_id, key, object_entry);
}

public(package) fun update_object_attributes(
    self: &mut BucketObjectRegistry,
    key: String,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::update_object_attributes(
        object_entry,
        headers,
        metadata,
        tags,
        object_etag,
    );
    emit_object_version_promoted(blob_bucket_id, key, object_entry);
}

public(package) fun update_object_attributes_if_match(
    self: &mut BucketObjectRegistry,
    key: String,
    expected_object_etag: String,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::update_object_attributes_if_match(
        object_entry,
        expected_object_etag,
        headers,
        metadata,
        tags,
        object_etag,
    );
    emit_object_version_promoted(blob_bucket_id, key, object_entry);
}

public(package) fun delete_object(
    self: &mut BucketObjectRegistry,
    key: String,
    object_etag: String,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let _ = ensure_entry(self, key);
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::delete_object(object_entry, object_etag);
    emit_object_version_promoted(blob_bucket_id, key, object_entry);
}

public(package) fun delete_object_if_match(
    self: &mut BucketObjectRegistry,
    key: String,
    expected_object_etag: String,
    object_etag: String,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::delete_object_if_match(
        object_entry,
        expected_object_etag,
        object_etag,
    );
    emit_object_version_promoted(blob_bucket_id, key, object_entry);
}

public(package) fun finalize_pending_version_if_certified(
    self: &mut BucketObjectRegistry,
    key: String,
    pending_blob_is_certified: bool,
) {
    let blob_bucket_id = self.blob_bucket_id;
    let object_entry = borrow_entry_mut(self, &key);
    bucket_object::finalize_pending_version_if_certified(
        object_entry,
        pending_blob_is_certified,
    );
    emit_object_version_promoted(blob_bucket_id, key, object_entry);
}

public(package) fun copy_object_if_absent(
    self: &mut BucketObjectRegistry,
    source_key: String,
    destination_key: String,
    object_etag: String,
): bool {
    let blob_bucket_id = self.blob_bucket_id;
    let source_entry = *borrow_entry(self, &source_key);
    let created = ensure_entry(self, destination_key);
    let destination = borrow_entry_mut(self, &destination_key);
    bucket_object::copy_current_version_to(&source_entry, destination, object_etag);
    emit_object_version_promoted(blob_bucket_id, destination_key, destination);
    bucket_object_events::emit_object_copied(
        blob_bucket_id,
        source_key,
        destination_key,
        bucket_object::generation(destination),
        bucket_object::current_object_etag(destination),
    );
    created
}

public(package) fun rename_object(
    self: &mut BucketObjectRegistry,
    key: String,
    new_key: String,
) {
    assert!(contains(self, &key), EObjectMissing);
    if (key == new_key) {
        return
    };
    assert!(!contains(self, &new_key), EObjectAlreadyExists);

    let blob_bucket_id = self.blob_bucket_id;
    let (_, object_entry) = shard_mut(self, &key).entries.remove(&key);
    shard_mut(self, &new_key).entries.insert(new_key, object_entry);
    bucket_object_events::emit_object_renamed(blob_bucket_id, key, new_key);
}

public(package) fun rename_object_if_match(
    self: &mut BucketObjectRegistry,
    key: String,
    expected_object_etag: String,
    new_key: String,
) {
    assert!(
        current_object_etag(self, &key) == option::some(expected_object_etag),
        ECurrentObjectEtagMismatch,
    );
    rename_object(self, key, new_key);
}

#[test_only]
public fun destroy_for_testing(self: BucketObjectRegistry) {
    let BucketObjectRegistry {
        id,
        blob_bucket_id: _,
        shards: _,
    } = self;
    id.delete();
}

fun ensure_entry(self: &mut BucketObjectRegistry, key: String): bool {
    let blob_bucket_id = self.blob_bucket_id;
    let shard = shard_mut(self, &key);
    if (shard.entries.contains(&key)) {
        false
    } else {
        shard.entries.insert(key, bucket_object::empty());
        bucket_object_events::emit_bucket_object_created(blob_bucket_id, key);
        true
    }
}

fun borrow_entry(self: &BucketObjectRegistry, key: &String): &ObjectEntryState {
    let shard = shard(self, key);
    assert!(shard.entries.contains(key), EObjectMissing);
    shard.entries.get(key)
}

fun borrow_entry_mut(self: &mut BucketObjectRegistry, key: &String): &mut ObjectEntryState {
    let shard = shard_mut(self, key);
    assert!(shard.entries.contains(key), EObjectMissing);
    shard.entries.get_mut(key)
}

fun emit_object_version_staged(
    blob_bucket_id: ID,
    key: String,
    object_entry: &ObjectEntryState,
) {
    let version = bucket_object::pending_version(object_entry);
    bucket_object_events::emit_object_version_staged(
        blob_bucket_id,
        key,
        object_version::generation(version),
        object_version::object_etag(version),
        object_version::delete_marker(version),
    );
}

fun emit_object_version_promoted(
    blob_bucket_id: ID,
    key: String,
    object_entry: &ObjectEntryState,
) {
    let version = bucket_object::current_version(object_entry);
    bucket_object_events::emit_object_version_promoted(
        blob_bucket_id,
        key,
        object_version::generation(version),
        object_version::object_etag(version),
        object_version::delete_marker(version),
    );
}

fun shard_index(self: &BucketObjectRegistry, key: &String): u64 {
    let bytes = string::as_bytes(key);
    let shard_count = self.shards.length();
    let mut hash = 0;
    let mut i = 0;
    while (i < bytes.length()) {
        hash = (hash + (bytes[i] as u64)) % shard_count;
        i = i + 1;
    };
    hash
}

fun shard(self: &BucketObjectRegistry, key: &String): &BucketNamespaceShard {
    vector::borrow(&self.shards, shard_index(self, key))
}

fun shard_mut(self: &mut BucketObjectRegistry, key: &String): &mut BucketNamespaceShard {
    let index = shard_index(self, key);
    vector::borrow_mut(&mut self.shards, index)
}
