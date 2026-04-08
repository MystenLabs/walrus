// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module bucket_object::bucket_object_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use bucket_object::{bucket_object, bucket_object_inner_v1, object_version};
use std::unit_test::assert_eq;
use walrus::{
    encoding,
    storage_pool,
    system,
    test_utils,
};

const RS2: u8 = 1;
const SIZE: u64 = 5_000_000;
const N_COINS: u64 = 1_000_000_000;

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
        next_pooled_blob_object_id,
        768,
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
        new_id_for_testing(ctx),
        768,
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
        new_id_for_testing(ctx),
        768,
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );

    bucket_object::stage_pending_version_for_testing(&mut left_bucket_object, mismatched_pending_version);

    abort
}

fun new_id_for_testing(ctx: &mut TxContext): ID {
    let id = object::new(ctx);
    let object_id = id.to_inner();
    id.delete();
    object_id
}
