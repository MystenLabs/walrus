// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_bucket::bucket_object_registry_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use blob_bucket::{
    bucket_object,
    bucket_object_registry,
    object_headers,
    object_metadata,
    object_tags,
    object_version,
};
use std::string::String;
use std::unit_test::assert_eq;
use wal::wal::WAL;
use sui::coin::Coin;
use sui::vec_map;
use walrus::{
    blob,
    encoding,
    messages,
    storage_pool,
    system,
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap},
};

const RS2: u8 = 1;
const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;
const N_COINS: u64 = 1_000_000_000;
const WRITE_PAYMENT: u64 = 100_000_000_000;

#[test]
fun resolve_or_create_creates_entry_once() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let key = b"index.html".to_string();

    let created = blob_bucket::resolve_or_create_bucket_object(
        &blob_bucket,
        &blob_bucket_cap,
        &mut registry,
        key,
    );
    let created_again = blob_bucket::resolve_or_create_bucket_object(
        &blob_bucket,
        &blob_bucket_cap,
        &mut registry,
        key,
    );

    assert!(created);
    assert!(!created_again);
    let object_entry = blob_bucket::resolve_object(&blob_bucket, &registry, &key).destroy_some();
    assert_eq!(bucket_object::generation(&object_entry), 0);
    assert!(!bucket_object::has_current_version(&object_entry));
    assert!(!bucket_object::has_pending_version(&object_entry));

    destroy_registry_fixture(
        registry,
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

#[test, expected_failure(abort_code = blob_bucket::EInvalidBlobBucketCap)]
fun resolve_or_create_requires_matching_blob_bucket_cap() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut left_pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (left_blob_bucket, _left_blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut left_pool_payment,
        ctx,
    );
    let mut right_pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (_right_blob_bucket, right_blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut right_pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&left_blob_bucket), ctx);

    blob_bucket::resolve_or_create_bucket_object(
        &left_blob_bucket,
        &right_blob_bucket_cap,
        &mut registry,
        b"index.html".to_string(),
    );

    abort
}

#[test]
fun rename_object_if_match_updates_registry_lookup() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let source_key = b"index.html".to_string();

    register_and_finalize_initial_object_version(
        &mut registry,
        source_key,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );

    blob_bucket::rename_object_if_match(
        &blob_bucket,
        &blob_bucket_cap,
        &mut registry,
        source_key,
        b"object-etag-v1".to_string(),
        b"home.html".to_string(),
    );

    assert_eq!(
        blob_bucket::resolve_object(&blob_bucket, &registry, &b"index.html".to_string()),
        option::none(),
    );
    assert_eq!(
        blob_bucket::current_object_etag(&blob_bucket, &registry, &b"home.html".to_string()),
        option::some(b"object-etag-v1".to_string()),
    );

    destroy_registry_fixture(
        registry,
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

#[test]
fun copy_object_if_absent_reuses_live_payload_and_attributes() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut registry = bucket_object_registry::new_for_testing(object::id(&blob_bucket), ctx);
    let source_key = b"index.html".to_string();
    let destination_key = b"app.js".to_string();

    register_and_finalize_initial_object_version(
        &mut registry,
        source_key,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );

    let source_version = blob_bucket::current_version(&blob_bucket, &registry, &source_key)
        .destroy_some();
    let created = blob_bucket::copy_object_if_absent(
        &blob_bucket,
        &blob_bucket_cap,
        &mut registry,
        source_key,
        destination_key,
        b"object-etag-copy".to_string(),
    );
    let copied_version = blob_bucket::current_version(&blob_bucket, &registry, &destination_key)
        .destroy_some();

    assert!(created);
    assert_eq!(object_version::generation(&copied_version), 1);
    assert_eq!(object_version::blob_id(&copied_version), object_version::blob_id(&source_version));
    assert_eq!(
        object_version::pooled_blob_object_id(&copied_version),
        object_version::pooled_blob_object_id(&source_version),
    );
    assert_eq!(
        object_version::content_etag(&copied_version),
        object_version::content_etag(&source_version),
    );
    assert_eq!(
        object_version::object_etag(&copied_version),
        b"object-etag-copy".to_string(),
    );
    assert_eq!(
        object_metadata::try_get(&object_version::metadata(&copied_version), &b"site".to_string()),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(&object_version::tags(&copied_version), &b"env".to_string()),
        option::some(b"prod".to_string()),
    );

    destroy_registry_fixture(
        registry,
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

fun register_and_finalize_initial_object_version(
    registry: &mut bucket_object_registry::BucketObjectRegistry,
    key: String,
    blob_bucket: &mut blob_bucket::BlobBucket,
    blob_bucket_cap: &blob_bucket::BlobBucketCap,
    system: &mut system::System,
    sk: &vector<u8>,
    ctx: &mut TxContext,
) {
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::put_object_if_absent_and_register(
        blob_bucket,
        blob_bucket_cap,
        registry,
        key,
        system,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        ctx,
    );
    write_payment.burn_for_testing();

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    certify_blob_in_bucket(blob_bucket, system, blob_id, sk);
    blob_bucket::finalize_pending_version_if_certified(blob_bucket, registry, key);
}

fun certify_blob_in_bucket(
    blob_bucket: &mut blob_bucket::BlobBucket,
    system: &system::System,
    blob_id: u256,
    sk: &vector<u8>,
) {
    let object_id = blob_bucket::get_blob_object_id(blob_bucket, blob_id);
    let confirmation_message = messages::certified_deletable_message_bytes(
        EPOCH,
        blob_id,
        object_id,
    );
    let signature = bls_min_pk_sign(&confirmation_message, sk);
    blob_bucket::certify_blob(
        blob_bucket,
        system,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );
}

fun destroy_registry_fixture(
    registry: bucket_object_registry::BucketObjectRegistry,
    blob_bucket: blob_bucket::BlobBucket,
    blob_bucket_cap: blob_bucket::BlobBucketCap,
    pool_payment: Coin<WAL>,
    system: system::System,
) {
    bucket_object_registry::destroy_for_testing(registry);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket::destroy_for_testing(blob_bucket));
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

fun html_headers(): object_headers::ObjectHeaders {
    object_headers::new_for_testing(
        option::some(b"text/html".to_string()),
        option::none(),
        option::some(b"en-US".to_string()),
        option::none(),
        option::some(b"public, max-age=60".to_string()),
    )
}

fun html_metadata(): object_metadata::ObjectMetadata {
    let mut entries = vec_map::empty();
    entries.insert(b"site".to_string(), b"marketing".to_string());
    object_metadata::new_for_testing(entries)
}

fun html_tags(): object_tags::ObjectTags {
    let mut entries = vec_map::empty();
    entries.insert(b"env".to_string(), b"prod".to_string());
    object_tags::new_for_testing(entries)
}
