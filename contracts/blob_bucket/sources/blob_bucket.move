// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// BlobBucket built on top of `StoragePool`.
module blob_bucket::blob_bucket;

use blob_bucket::blob_bucket_inner_v1::{Self, BlobBucketInnerV1};
use blob_bucket::bucket_object::{Self as bucket_object, BucketObject};
use blob_bucket::bucket_object_registry::{Self as bucket_object_registry, BucketObjectRegistry};
use blob_bucket::object_headers::ObjectHeaders;
use blob_bucket::object_metadata::ObjectMetadata;
use blob_bucket::object_tags::ObjectTags;
use blob_bucket::object_version;
use std::string::String;
use sui::{coin::Coin, dynamic_field as df};
use wal::wal::WAL;
use walrus::{blob, system::System};

const VERSION: u64 = 1;

/// The provided capability does not belong to this blob bucket.
const EInvalidBlobBucketCap: u64 = 0;
/// The blob bucket object version does not match the package version.
const EWrongVersion: u64 = 1;
/// The provided bucket object does not belong to this blob bucket.
const EBucketObjectMismatch: u64 = 2;
/// The provided bucket object registry does not belong to this blob bucket.
const EBucketObjectRegistryMismatch: u64 = 3;

public struct BlobBucket has key {
    id: UID,
    version: u64,
}

public struct BlobBucketCap has key, store {
    id: UID,
    bucket_id: ID,
}

/// Creates and shares a new blob bucket, returning the capability required for admin actions.
public fun new(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): BlobBucketCap {
    let (bucket, cap) = new_impl(
        system,
        reserved_encoded_capacity_bytes,
        epochs_ahead,
        payment,
        ctx,
    );
    transfer::share_object(bucket);
    cap
}

/// Creates and shares a new blob bucket together with its shared object registry.
public fun new_with_object_registry(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): (BlobBucketCap, ID) {
    let (bucket, cap) = new_impl(
        system,
        reserved_encoded_capacity_bytes,
        epochs_ahead,
        payment,
        ctx,
    );
    let registry_id = bucket_object_registry::new(object::id(&bucket), ctx);
    transfer::share_object(bucket);
    (cap, registry_id)
}

#[test_only]
public fun new_for_testing(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): (BlobBucket, BlobBucketCap) {
    new_impl(system, reserved_encoded_capacity_bytes, epochs_ahead, payment, ctx)
}

fun new_impl(
    system: &mut System,
    reserved_encoded_capacity_bytes: u64,
    epochs_ahead: u32,
    payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
): (BlobBucket, BlobBucketCap) {
    let mut bucket = BlobBucket {
        id: object::new(ctx),
        version: VERSION,
    };
    let cap = BlobBucketCap {
        id: object::new(ctx),
        bucket_id: object::id(&bucket),
    };
    df::add(
        &mut bucket.id,
        VERSION,
        blob_bucket_inner_v1::new(
            system,
            reserved_encoded_capacity_bytes,
            epochs_ahead,
            payment,
            ctx,
        ),
    );
    (bucket, cap)
}

public fun bucket_id(self: &BlobBucketCap): ID {
    self.bucket_id
}

public fun create_object_registry(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    ctx: &mut TxContext,
): ID {
    check_cap(self, cap);
    bucket_object_registry::new(object::id(self), ctx)
}

public fun resolve_or_create_bucket_object(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    registry: &mut BucketObjectRegistry,
    key: String,
    ctx: &mut TxContext,
): ID {
    check_cap(self, cap);
    check_registry(self, registry);
    bucket_object_registry::resolve_or_create_bucket_object(registry, key, ctx)
}

public fun copy_object_if_absent(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    registry: &mut BucketObjectRegistry,
    source: &BucketObject,
    destination_key: String,
    object_etag: String,
    ctx: &mut TxContext,
): ID {
    check_cap(self, cap);
    check_registry(self, registry);
    check_bucket_object(self, source);
    bucket_object_registry::copy_object_if_absent(
        registry,
        source,
        destination_key,
        object_etag,
        ctx,
    )
}

public fun rename_object(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    registry: &mut BucketObjectRegistry,
    bucket_object_ref: &mut BucketObject,
    new_key: String,
) {
    check_cap(self, cap);
    check_registry(self, registry);
    check_bucket_object(self, bucket_object_ref);
    bucket_object_registry::rename_object(registry, bucket_object_ref, new_key);
}

public fun rename_object_if_match(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    registry: &mut BucketObjectRegistry,
    bucket_object_ref: &mut BucketObject,
    expected_object_etag: String,
    new_key: String,
) {
    check_cap(self, cap);
    check_registry(self, registry);
    check_bucket_object(self, bucket_object_ref);
    bucket_object_registry::rename_object_if_match(
        registry,
        bucket_object_ref,
        expected_object_etag,
        new_key,
    );
}

public fun register_blob(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    blob_id: u256,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    self
        .inner_mut()
        .register_blob(
            system,
            blob_id,
            root_hash,
            unencoded_size,
            encoding_type,
            deletable,
            write_payment,
            ctx,
        );
}

public fun put_object_if_absent_and_register(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    system: &mut System,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::assert_can_put_object_if_absent(bucket_object_ref);

    let blob_id = blob::derive_blob_id(root_hash, encoding_type, unencoded_size);
    register_blob(
        self,
        cap,
        system,
        blob_id,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        ctx,
    );
    bucket_object::stage_registered_blob_version(
        bucket_object_ref,
        object::id(self),
        blob_id,
        get_blob_object_id(self, blob_id),
        unencoded_size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
}

public fun update_object_if_match_and_register(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    system: &mut System,
    expected_object_etag: String,
    root_hash: u256,
    unencoded_size: u64,
    encoding_type: u8,
    deletable: bool,
    write_payment: &mut Coin<WAL>,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    content_etag: String,
    object_etag: String,
    ctx: &mut TxContext,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::assert_can_update_object_if_match(
        bucket_object_ref,
        expected_object_etag,
    );

    let blob_id = blob::derive_blob_id(root_hash, encoding_type, unencoded_size);
    register_blob(
        self,
        cap,
        system,
        blob_id,
        root_hash,
        unencoded_size,
        encoding_type,
        deletable,
        write_payment,
        ctx,
    );
    bucket_object::stage_registered_blob_version(
        bucket_object_ref,
        object::id(self),
        blob_id,
        get_blob_object_id(self, blob_id),
        unencoded_size,
        headers,
        metadata,
        tags,
        content_etag,
        object_etag,
    );
}

public fun update_object_attributes(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::update_object_attributes(
        bucket_object_ref,
        headers,
        metadata,
        tags,
        object_etag,
    );
}

public fun update_object_attributes_if_match(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    expected_object_etag: String,
    headers: ObjectHeaders,
    metadata: ObjectMetadata,
    tags: ObjectTags,
    object_etag: String,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::update_object_attributes_if_match(
        bucket_object_ref,
        expected_object_etag,
        headers,
        metadata,
        tags,
        object_etag,
    );
}

public fun delete_object(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    object_etag: String,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::delete_object(bucket_object_ref, object_etag);
}

public fun delete_object_if_match(
    self: &BlobBucket,
    cap: &BlobBucketCap,
    bucket_object_ref: &mut BucketObject,
    expected_object_etag: String,
    object_etag: String,
) {
    check_cap(self, cap);
    check_bucket_object(self, bucket_object_ref);
    bucket_object::delete_object_if_match(
        bucket_object_ref,
        expected_object_etag,
        object_etag,
    );
}

public fun certify_blob(
    self: &mut BlobBucket,
    system: &System,
    blob_id: u256,
    signature: vector<u8>,
    signers_bitmap: vector<u8>,
    message: vector<u8>,
) {
    self.inner_mut().certify_blob(system, blob_id, signature, signers_bitmap, message);
}

public fun finalize_pending_version_if_certified(
    self: &BlobBucket,
    bucket_object_ref: &mut BucketObject,
) {
    check_bucket_object(self, bucket_object_ref);
    let pending_blob_is_certified = {
        let pending_version = bucket_object::pending_version(bucket_object_ref);
        object_version::delete_marker(pending_version)
            || is_blob_certified(self, object_version::blob_id(pending_version))
    };
    bucket_object::finalize_pending_version_if_certified(
        bucket_object_ref,
        object::id(self),
        pending_blob_is_certified,
    );
}

public fun delete_blob(self: &mut BlobBucket, cap: &BlobBucketCap, system: &System, blob_id: u256) {
    check_cap(self, cap);
    self.inner_mut().delete_blob(system, blob_id);
}

public fun extend_storage_pool(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    extended_epochs: u32,
    payment: &mut Coin<WAL>,
) {
    check_cap(self, cap);
    self.inner_mut().extend_storage_pool(system, extended_epochs, payment);
}

public fun storage_pool_id(self: &BlobBucket): ID {
    self.inner().storage_pool_id()
}

public fun increase_storage_pool_capacity(
    self: &mut BlobBucket,
    cap: &BlobBucketCap,
    system: &mut System,
    additional_encoded_capacity_bytes: u64,
    payment: &mut Coin<WAL>,
) {
    check_cap(self, cap);
    self
        .inner_mut()
        .increase_storage_pool_capacity(
            system,
            additional_encoded_capacity_bytes,
            payment,
        );
}

public(package) fun has_blob(self: &BlobBucket, blob_id: u256): bool {
    self.inner().has_blob(blob_id)
}

public(package) fun get_blob_object_id(self: &BlobBucket, blob_id: u256): ID {
    self.inner().get_blob_object_id(blob_id)
}

public(package) fun is_blob_certified(self: &BlobBucket, blob_id: u256): bool {
    self.inner().is_blob_certified(blob_id)
}

public(package) fun end_epoch(self: &BlobBucket): u32 {
    self.inner().end_epoch()
}

public(package) fun reserved_encoded_capacity_bytes(self: &BlobBucket): u64 {
    self.inner().reserved_encoded_capacity_bytes()
}

public(package) fun used_encoded_bytes(self: &BlobBucket): u64 {
    self.inner().used_encoded_bytes()
}

public(package) fun available_encoded_bytes(self: &BlobBucket): u64 {
    self.inner().available_encoded_bytes()
}

public(package) fun blob_count(self: &BlobBucket): u64 {
    self.inner().blob_count()
}

fun check_cap(self: &BlobBucket, cap: &BlobBucketCap) {
    assert!(object::id(self) == cap.bucket_id, EInvalidBlobBucketCap);
}

fun check_bucket_object(self: &BlobBucket, bucket_object_ref: &BucketObject) {
    assert!(
        object::id(self) == bucket_object::blob_bucket_id(bucket_object_ref),
        EBucketObjectMismatch,
    );
}

fun check_registry(self: &BlobBucket, registry: &BucketObjectRegistry) {
    assert!(
        object::id(self) == bucket_object_registry::blob_bucket_id(registry),
        EBucketObjectRegistryMismatch,
    );
}

fun inner(self: &BlobBucket): &BlobBucketInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow(&self.id, self.version)
}

fun inner_mut(self: &mut BlobBucket): &mut BlobBucketInnerV1 {
    assert!(self.version == VERSION, EWrongVersion);
    df::borrow_mut(&mut self.id, self.version)
}

#[test_only]
public fun destroy_for_testing(self: BlobBucket): BlobBucketInnerV1 {
    let BlobBucket { mut id, version } = self;
    let inner = df::remove(&mut id, version);
    id.delete();
    inner
}

#[test_only]
public fun destroy_cap_for_testing(self: BlobBucketCap) {
    std::unit_test::destroy(self);
}
