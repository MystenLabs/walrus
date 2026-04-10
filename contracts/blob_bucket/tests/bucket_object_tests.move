// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_bucket::bucket_object_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use blob_bucket::{
    bucket_object,
    bucket_object_inner_v1,
    object_headers,
    object_metadata,
    object_tags,
    object_version,
};
use std::unit_test::assert_eq;
use wal::wal::WAL;
use sui::coin::Coin;
use walrus::{
    encoding,
    blob,
    messages,
    storage_pool,
    system,
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap},
};

const RS2: u8 = 1;
const ROOT_HASH: u256 = 0xABC;
const NEXT_ROOT_HASH: u256 = 0xDEF;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;
const N_COINS: u64 = 1_000_000_000;
const WRITE_PAYMENT: u64 = 100_000_000_000;

#[test]
fun new_records_linked_blob_bucket_and_key() {
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
    let linked_blob_bucket_id = object::id(&blob_bucket);
    let bucket_object = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        b"index.html".to_string(),
        ctx,
    );

    assert_eq!(bucket_object::blob_bucket_id(&bucket_object), linked_blob_bucket_id);
    assert_eq!(bucket_object::key(&bucket_object), b"index.html".to_string());
    assert_eq!(bucket_object::generation(&bucket_object), 0);
    assert!(!bucket_object::has_current_version(&bucket_object));
    assert!(!bucket_object::has_pending_version(&bucket_object));

    let inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(inner);

    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket::destroy_for_testing(blob_bucket));
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

#[test]
fun objects_in_same_blob_bucket_keep_distinct_keys() {
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
    let linked_blob_bucket_id = object::id(&blob_bucket);
    let left_bucket_object = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        b"index.html".to_string(),
        ctx,
    );
    let right_bucket_object = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        b"app.js".to_string(),
        ctx,
    );

    assert_eq!(bucket_object::blob_bucket_id(&left_bucket_object), linked_blob_bucket_id);
    assert_eq!(bucket_object::blob_bucket_id(&right_bucket_object), linked_blob_bucket_id);
    assert_eq!(bucket_object::key(&left_bucket_object), b"index.html".to_string());
    assert_eq!(bucket_object::key(&right_bucket_object), b"app.js".to_string());
    assert_eq!(bucket_object::generation(&left_bucket_object), 0);
    assert_eq!(bucket_object::generation(&right_bucket_object), 0);

    let left_inner = bucket_object::destroy_for_testing(left_bucket_object);
    bucket_object_inner_v1::destroy_for_testing(left_inner);
    let right_inner = bucket_object::destroy_for_testing(right_bucket_object);
    bucket_object_inner_v1::destroy_for_testing(right_inner);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

#[test]
fun stage_and_promote_pending_version() {
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
    let linked_blob_bucket_id = object::id(&blob_bucket);
    let mut bucket_object = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        b"index.html".to_string(),
        ctx,
    );
    let next_pooled_blob_object_id = new_id_for_testing(ctx);
    let pending_version = object_version::new_for_testing(
        object::id(&bucket_object),
        1,
        0x111,
        next_pooled_blob_object_id,
        768,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );

    bucket_object::stage_pending_version_for_testing(&mut bucket_object, pending_version);
    assert!(bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::generation(bucket_object::pending_version(&bucket_object)), 1);
    assert_eq!(
        object_version::object_etag(bucket_object::pending_version(&bucket_object)),
        b"object-etag-v1".to_string(),
    );

    bucket_object::promote_pending_version_for_testing(&mut bucket_object);

    assert_eq!(bucket_object::generation(&bucket_object), 1);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(!bucket_object::has_pending_version(&bucket_object));
    assert_eq!(
        object_version::pooled_blob_object_id(bucket_object::current_version(&bucket_object)),
        next_pooled_blob_object_id,
    );
    assert_eq!(
        object_version::content_etag(bucket_object::current_version(&bucket_object)),
        b"content-etag-v1".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::current_version(&bucket_object))),
        option::some(b"text/html".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::current_version(&bucket_object)),
            &b"site".to_string(),
        ),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::current_version(&bucket_object)),
            &b"env".to_string(),
        ),
        option::some(b"prod".to_string()),
    );

    let inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(inner);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = bucket_object::EGenerationMustAdvance)]
fun pending_version_must_advance_generation() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (blob_bucket, _blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let invalid_pending_version = object_version::new_for_testing(
        object::id(&bucket_object),
        2,
        0x111,
        new_id_for_testing(ctx),
        768,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );

    bucket_object::stage_pending_version_for_testing(&mut bucket_object, invalid_pending_version);

    abort
}

#[test, expected_failure(abort_code = bucket_object::EVersionBucketObjectMismatch)]
fun pending_version_must_match_bucket_object() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (blob_bucket, _blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut left_bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let right_bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"app.js".to_string(),
        ctx,
    );
    let mismatched_pending_version = object_version::new_for_testing(
        object::id(&right_bucket_object),
        1,
        0x111,
        new_id_for_testing(ctx),
        768,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );

    bucket_object::stage_pending_version_for_testing(&mut left_bucket_object, mismatched_pending_version);

    abort
}

#[test]
fun put_object_if_absent_and_register_stages_pending_version() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);

    bucket_object::put_object_if_absent_and_register(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
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

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    assert!(blob_bucket::has_blob(&blob_bucket, blob_id));
    assert!(bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::blob_id(bucket_object::pending_version(&bucket_object)), blob_id);
    assert_eq!(object_version::generation(bucket_object::pending_version(&bucket_object)), 1);
    assert_eq!(
        object_version::object_etag(bucket_object::pending_version(&bucket_object)),
        b"object-etag-v1".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::pending_version(&bucket_object))),
        option::some(b"text/html".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::pending_version(&bucket_object)),
            &b"site".to_string(),
        ),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::pending_version(&bucket_object)),
            &b"env".to_string(),
        ),
        option::some(b"prod".to_string()),
    );

    write_payment.burn_for_testing();
    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test, expected_failure(abort_code = bucket_object::EObjectAlreadyExists)]
fun put_object_if_absent_requires_no_current_version() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);

    bucket_object::put_object_if_absent_and_register(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        NEXT_ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        ctx,
    );

    abort
}

#[test]
fun update_object_if_match_and_register_stages_next_generation() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size * 2,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);

    bucket_object::update_object_if_match_and_register(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        b"object-etag-v1".to_string(),
        NEXT_ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        js_headers(),
        js_metadata(),
        js_tags(),
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        ctx,
    );

    let next_blob_id = blob::derive_blob_id(NEXT_ROOT_HASH, RS2, SIZE);
    assert_eq!(bucket_object::generation(&bucket_object), 1);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::generation(bucket_object::pending_version(&bucket_object)), 2);
    assert_eq!(object_version::blob_id(bucket_object::pending_version(&bucket_object)), next_blob_id);
    assert_eq!(
        object_version::object_etag(bucket_object::pending_version(&bucket_object)),
        b"object-etag-v2".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::pending_version(&bucket_object))),
        option::some(b"application/javascript".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::pending_version(&bucket_object)),
            &b"cache-bust".to_string(),
        ),
        option::some(b"v2".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::pending_version(&bucket_object)),
            &b"bundle".to_string(),
        ),
        option::some(b"main".to_string()),
    );

    write_payment.burn_for_testing();
    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test]
fun delete_object_promotes_delete_marker() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );

    bucket_object::delete_object(&mut bucket_object, b"object-etag-delete".to_string());

    assert_eq!(bucket_object::generation(&bucket_object), 2);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(!bucket_object::has_pending_version(&bucket_object));
    assert!(bucket_object::is_deleted(&bucket_object));
    assert!(object_version::delete_marker(bucket_object::current_version(&bucket_object)));
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&bucket_object)),
        b"object-etag-delete".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::current_version(&bucket_object))),
        option::none(),
    );
    assert!(object_metadata::is_empty(&object_version::metadata(
        bucket_object::current_version(&bucket_object),
    )));
    assert!(object_tags::is_empty(&object_version::tags(
        bucket_object::current_version(&bucket_object),
    )));

    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test, expected_failure(abort_code = bucket_object::EObjectEtagMismatch)]
fun delete_object_if_match_requires_matching_etag() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );

    bucket_object::delete_object_if_match(
        &mut bucket_object,
        b"wrong-etag".to_string(),
        b"object-etag-delete".to_string(),
    );

    abort
}

#[test]
fun put_object_if_absent_allows_current_delete_marker() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut blob_bucket, blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size * 2,
        3,
        &mut pool_payment,
        ctx,
    );
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    bucket_object::delete_object(&mut bucket_object, b"object-etag-delete".to_string());

    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    bucket_object::put_object_if_absent_and_register(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        NEXT_ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        ctx,
    );

    let next_blob_id = blob::derive_blob_id(NEXT_ROOT_HASH, RS2, SIZE);
    assert!(bucket_object::is_deleted(&bucket_object));
    assert!(bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::generation(bucket_object::pending_version(&bucket_object)), 3);
    assert_eq!(object_version::blob_id(bucket_object::pending_version(&bucket_object)), next_blob_id);

    write_payment.burn_for_testing();
    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test, expected_failure(abort_code = bucket_object::EObjectEtagMismatch)]
fun update_object_if_match_requires_matching_etag() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);

    bucket_object::update_object_if_match_and_register(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        b"wrong-etag".to_string(),
        NEXT_ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        ctx,
    );

    abort
}

#[test]
fun update_object_attributes_if_match_promotes_new_generation() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    let current_blob_id = object_version::blob_id(bucket_object::current_version(&bucket_object));
    let current_pooled_blob_object_id = object_version::pooled_blob_object_id(
        bucket_object::current_version(&bucket_object),
    );
    let current_content_etag = object_version::content_etag(
        bucket_object::current_version(&bucket_object),
    );

    bucket_object::update_object_attributes_if_match(
        &mut bucket_object,
        b"object-etag-v1".to_string(),
        js_headers(),
        js_metadata(),
        js_tags(),
        b"object-etag-v2".to_string(),
    );

    assert_eq!(bucket_object::generation(&bucket_object), 2);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(!bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::generation(bucket_object::current_version(&bucket_object)), 2);
    assert_eq!(object_version::blob_id(bucket_object::current_version(&bucket_object)), current_blob_id);
    assert_eq!(
        object_version::pooled_blob_object_id(bucket_object::current_version(&bucket_object)),
        current_pooled_blob_object_id,
    );
    assert_eq!(
        object_version::content_etag(bucket_object::current_version(&bucket_object)),
        current_content_etag,
    );
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&bucket_object)),
        b"object-etag-v2".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::current_version(&bucket_object))),
        option::some(b"application/javascript".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::current_version(&bucket_object)),
            &b"cache-bust".to_string(),
        ),
        option::some(b"v2".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::current_version(&bucket_object)),
            &b"bundle".to_string(),
        ),
        option::some(b"main".to_string()),
    );

    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test, expected_failure(abort_code = bucket_object::EObjectEtagMismatch)]
fun update_object_attributes_if_match_requires_matching_etag() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );

    bucket_object::update_object_attributes_if_match(
        &mut bucket_object,
        b"wrong-etag".to_string(),
        js_headers(),
        js_metadata(),
        js_tags(),
        b"object-etag-v2".to_string(),
    );

    abort
}

#[test, expected_failure(abort_code = bucket_object::ECurrentVersionHasNoBlob)]
fun update_object_attributes_requires_live_current_blob() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    register_and_finalize_initial_object_version(
        &mut bucket_object,
        &mut blob_bucket,
        &blob_bucket_cap,
        &mut system,
        &sk,
        ctx,
    );
    bucket_object::delete_object(&mut bucket_object, b"object-etag-delete".to_string());

    bucket_object::update_object_attributes(
        &mut bucket_object,
        js_headers(),
        js_metadata(),
        js_tags(),
        b"object-etag-v2".to_string(),
    );

    abort
}

#[test]
fun stage_registered_blob_version_uses_registered_blob_state() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let blob_id = register_blob_in_bucket(&mut system, &mut blob_bucket, &blob_bucket_cap, ctx);

    bucket_object::stage_registered_blob_version_for_testing(
        &mut bucket_object,
        &blob_bucket,
        blob_id,
        SIZE,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
    );

    assert!(bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::blob_id(bucket_object::pending_version(&bucket_object)), blob_id);
    assert_eq!(
        object_version::pooled_blob_object_id(bucket_object::pending_version(&bucket_object)),
        blob_bucket::get_blob_object_id(&blob_bucket, blob_id),
    );
    assert_eq!(
        object_version::size(bucket_object::pending_version(&bucket_object)),
        SIZE,
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::pending_version(&bucket_object))),
        option::some(b"text/html".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::pending_version(&bucket_object)),
            &b"site".to_string(),
        ),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::pending_version(&bucket_object)),
            &b"env".to_string(),
        ),
        option::some(b"prod".to_string()),
    );

    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test, expected_failure(abort_code = bucket_object::EPendingVersionNotCertified)]
fun finalize_pending_version_requires_certification() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let blob_id = register_blob_in_bucket(&mut system, &mut blob_bucket, &blob_bucket_cap, ctx);

    bucket_object::stage_registered_blob_version_for_testing(
        &mut bucket_object,
        &blob_bucket,
        blob_id,
        SIZE,
        empty_headers(),
        empty_metadata(),
        empty_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
    );
    bucket_object::finalize_pending_version_if_certified_for_testing(&mut bucket_object, &blob_bucket);

    abort
}

#[test]
fun finalize_pending_version_promotes_after_certification() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let blob_id = register_blob_in_bucket(&mut system, &mut blob_bucket, &blob_bucket_cap, ctx);

    bucket_object::stage_registered_blob_version_for_testing(
        &mut bucket_object,
        &blob_bucket,
        blob_id,
        SIZE,
        html_headers(),
        html_metadata(),
        html_tags(),
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
    );
    certify_blob_in_bucket(&mut blob_bucket, &system, blob_id, &sk);
    bucket_object::finalize_pending_version_if_certified_for_testing(&mut bucket_object, &blob_bucket);

    assert_eq!(bucket_object::generation(&bucket_object), 1);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(!bucket_object::has_pending_version(&bucket_object));
    assert_eq!(object_version::blob_id(bucket_object::current_version(&bucket_object)), blob_id);
    assert_eq!(
        object_version::object_etag(bucket_object::current_version(&bucket_object)),
        b"object-etag-v1".to_string(),
    );
    assert_eq!(
        object_headers::content_type(&object_version::headers(bucket_object::current_version(&bucket_object))),
        option::some(b"text/html".to_string()),
    );
    assert_eq!(
        object_metadata::try_get(
            &object_version::metadata(bucket_object::current_version(&bucket_object)),
            &b"site".to_string(),
        ),
        option::some(b"marketing".to_string()),
    );
    assert_eq!(
        object_tags::try_get(
            &object_version::tags(bucket_object::current_version(&bucket_object)),
            &b"env".to_string(),
        ),
        option::some(b"prod".to_string()),
    );

    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

#[test]
fun finalize_pending_delete_marker_promotes_without_certification() {
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
    let mut bucket_object = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"index.html".to_string(),
        ctx,
    );
    let delete_marker = object_version::new_delete_marker(
        object::id(&bucket_object),
        1,
        b"object-etag-delete".to_string(),
    );

    bucket_object::stage_pending_version_for_testing(&mut bucket_object, delete_marker);
    bucket_object::finalize_pending_version_if_certified_for_testing(&mut bucket_object, &blob_bucket);

    assert_eq!(bucket_object::generation(&bucket_object), 1);
    assert!(bucket_object::has_current_version(&bucket_object));
    assert!(bucket_object::is_deleted(&bucket_object));
    assert!(object_version::delete_marker(bucket_object::current_version(&bucket_object)));

    destroy_bucket_object_fixture(bucket_object, blob_bucket, blob_bucket_cap, pool_payment, system);
}

fun register_blob_in_bucket(
    system: &mut system::System,
    blob_bucket: &mut blob_bucket::BlobBucket,
    blob_bucket_cap: &blob_bucket::BlobBucketCap,
    ctx: &mut TxContext,
): u256 {
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::register_blob(
        blob_bucket,
        blob_bucket_cap,
        system,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        ctx,
    );
    write_payment.burn_for_testing();
    blob_id
}

fun certify_blob_in_bucket(
    blob_bucket: &mut blob_bucket::BlobBucket,
    system: &system::System,
    blob_id: u256,
    sk: &vector<u8>,
) {
    let object_id = blob_bucket::get_blob_object_id(blob_bucket, blob_id);
    let confirmation_message = messages::certified_deletable_message_bytes(EPOCH, blob_id, object_id);
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

fun destroy_bucket_object_fixture(
    bucket_object: bucket_object::BucketObject,
    blob_bucket: blob_bucket::BlobBucket,
    blob_bucket_cap: blob_bucket::BlobBucketCap,
    pool_payment: Coin<WAL>,
    system: system::System,
) {
    let inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(inner);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

fun register_and_finalize_initial_object_version(
    bucket_object: &mut bucket_object::BucketObject,
    blob_bucket: &mut blob_bucket::BlobBucket,
    blob_bucket_cap: &blob_bucket::BlobBucketCap,
    system: &mut system::System,
    sk: &vector<u8>,
    ctx: &mut TxContext,
) {
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    bucket_object::put_object_if_absent_and_register(
        bucket_object,
        blob_bucket,
        blob_bucket_cap,
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
    bucket_object::finalize_pending_version_if_certified_for_testing(bucket_object, blob_bucket);
}

fun new_id_for_testing(ctx: &mut TxContext): ID {
    let id = object::new(ctx);
    let object_id = id.to_inner();
    id.delete();
    object_id
}

fun empty_headers(): object_headers::ObjectHeaders {
    object_headers::empty()
}

fun empty_metadata(): object_metadata::ObjectMetadata {
    object_metadata::empty()
}

fun empty_tags(): object_tags::ObjectTags {
    object_tags::empty()
}

fun html_headers(): object_headers::ObjectHeaders {
    object_headers::new_for_testing(
        option::some(b"text/html".to_string()),
        option::none(),
        option::some(b"en".to_string()),
        option::none(),
        option::some(b"public, max-age=60".to_string()),
    )
}

fun js_headers(): object_headers::ObjectHeaders {
    object_headers::new_for_testing(
        option::some(b"application/javascript".to_string()),
        option::some(b"gzip".to_string()),
        option::none(),
        option::none(),
        option::some(b"public, max-age=31536000".to_string()),
    )
}

fun html_metadata(): object_metadata::ObjectMetadata {
    let mut metadata = object_metadata::empty();
    object_metadata::insert_or_update(&mut metadata, b"site".to_string(), b"marketing".to_string());
    object_metadata::insert_or_update(&mut metadata, b"owner".to_string(), b"walrus".to_string());
    metadata
}

fun html_tags(): object_tags::ObjectTags {
    let mut tags = object_tags::empty();
    object_tags::insert_or_update(&mut tags, b"env".to_string(), b"prod".to_string());
    object_tags::insert_or_update(&mut tags, b"surface".to_string(), b"web".to_string());
    tags
}

fun js_metadata(): object_metadata::ObjectMetadata {
    let mut metadata = object_metadata::empty();
    object_metadata::insert_or_update(&mut metadata, b"site".to_string(), b"marketing".to_string());
    object_metadata::insert_or_update(&mut metadata, b"cache-bust".to_string(), b"v2".to_string());
    metadata
}

fun js_tags(): object_tags::ObjectTags {
    let mut tags = object_tags::empty();
    object_tags::insert_or_update(&mut tags, b"bundle".to_string(), b"main".to_string());
    object_tags::insert_or_update(&mut tags, b"surface".to_string(), b"web".to_string());
    tags
}
