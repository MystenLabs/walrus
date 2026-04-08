// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module bucket_object::object_entry_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use bucket_object::{
    bucket_object,
    bucket_object_inner_v1,
    object_entry,
    object_entry_inner_v1,
    object_version,
};
use sui::coin::Coin;
use std::unit_test::assert_eq;
use wal::wal::WAL;
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
fun new_entry_tracks_current_version() {
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
    let (bucket_object, bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"assets".to_string(),
        ctx,
    );
    let current_version = object_version::new_for_testing(
        object::id(&bucket_object),
        1,
        new_id_for_testing(ctx),
        512,
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );
    let entry = object_entry::new_for_testing(
        object::id(&bucket_object),
        b"index.html".to_string(),
        current_version,
        ctx,
    );

    assert_eq!(object_entry::bucket_object_id(&entry), object::id(&bucket_object));
    assert_eq!(object_entry::key(&entry), b"index.html".to_string());
    assert_eq!(object_entry::generation(&entry), 1);
    assert_eq!(
        object_version::content_etag(object_entry::current_version(&entry)),
        b"content-etag-v1".to_string(),
    );
    assert_eq!(
        object_version::object_etag(object_entry::current_version(&entry)),
        b"object-etag-v1".to_string(),
    );
    assert!(!object_entry::has_pending_version(&entry));

    destroy_test_state(
        entry,
        bucket_object,
        bucket_object_cap,
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
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
    let (bucket_object, bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"assets".to_string(),
        ctx,
    );
    let current_version = object_version::new_for_testing(
        object::id(&bucket_object),
        1,
        new_id_for_testing(ctx),
        512,
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );
    let mut entry = object_entry::new_for_testing(
        object::id(&bucket_object),
        b"index.html".to_string(),
        current_version,
        ctx,
    );
    let next_pooled_blob_object_id = new_id_for_testing(ctx);
    let pending_version = object_version::new_for_testing(
        object::id(&bucket_object),
        2,
        next_pooled_blob_object_id,
        768,
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        false,
    );

    object_entry::stage_pending_version_for_testing(&mut entry, pending_version);
    assert!(object_entry::has_pending_version(&entry));
    assert_eq!(object_version::generation(object_entry::pending_version(&entry)), 2);
    assert_eq!(
        object_version::object_etag(object_entry::pending_version(&entry)),
        b"object-etag-v2".to_string(),
    );

    object_entry::promote_pending_version_for_testing(&mut entry);

    assert_eq!(object_entry::generation(&entry), 2);
    assert!(!object_entry::has_pending_version(&entry));
    assert_eq!(
        object_version::pooled_blob_object_id(object_entry::current_version(&entry)),
        next_pooled_blob_object_id,
    );
    assert_eq!(
        object_version::content_etag(object_entry::current_version(&entry)),
        b"content-etag-v2".to_string(),
    );

    destroy_test_state(
        entry,
        bucket_object,
        bucket_object_cap,
        blob_bucket,
        blob_bucket_cap,
        pool_payment,
        system,
    );
}

#[test, expected_failure(abort_code = object_entry::EGenerationMustAdvance)]
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
    let (bucket_object, _bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&blob_bucket),
        b"assets".to_string(),
        ctx,
    );
    let current_version = object_version::new_for_testing(
        object::id(&bucket_object),
        1,
        new_id_for_testing(ctx),
        512,
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );
    let mut entry = object_entry::new_for_testing(
        object::id(&bucket_object),
        b"index.html".to_string(),
        current_version,
        ctx,
    );
    let invalid_pending_version = object_version::new_for_testing(
        object::id(&bucket_object),
        1,
        new_id_for_testing(ctx),
        768,
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        false,
    );

    object_entry::stage_pending_version_for_testing(&mut entry, invalid_pending_version);

    abort
}

#[test, expected_failure(abort_code = object_entry::EVersionBucketObjectMismatch)]
fun pending_version_must_match_bucket_object() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut left_payment = test_utils::mint_frost(N_COINS, ctx);
    let mut right_payment = test_utils::mint_frost(N_COINS, ctx);
    let (left_blob_bucket, _left_blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut left_payment,
        ctx,
    );
    let (right_blob_bucket, _right_blob_bucket_cap) = blob_bucket::new_for_testing(
        &mut system,
        encoded_size,
        3,
        &mut right_payment,
        ctx,
    );
    let (left_bucket_object, _left_bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&left_blob_bucket),
        b"left-assets".to_string(),
        ctx,
    );
    let (right_bucket_object, _right_bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&right_blob_bucket),
        b"right-assets".to_string(),
        ctx,
    );
    let current_version = object_version::new_for_testing(
        object::id(&left_bucket_object),
        1,
        new_id_for_testing(ctx),
        512,
        b"content-etag-v1".to_string(),
        b"object-etag-v1".to_string(),
        false,
    );
    let mut entry = object_entry::new_for_testing(
        object::id(&left_bucket_object),
        b"index.html".to_string(),
        current_version,
        ctx,
    );
    let mismatched_pending_version = object_version::new_for_testing(
        object::id(&right_bucket_object),
        2,
        new_id_for_testing(ctx),
        768,
        b"content-etag-v2".to_string(),
        b"object-etag-v2".to_string(),
        false,
    );

    object_entry::stage_pending_version_for_testing(&mut entry, mismatched_pending_version);

    abort
}

fun destroy_test_state(
    entry: object_entry::ObjectEntry,
    bucket_object: bucket_object::BucketObject,
    bucket_object_cap: bucket_object::BucketObjectCap,
    blob_bucket: blob_bucket::BlobBucket,
    blob_bucket_cap: blob_bucket::BlobBucketCap,
    pool_payment: Coin<WAL>,
    system: system::System,
) {
    let inner = object_entry::destroy_for_testing(entry);
    object_entry_inner_v1::destroy_for_testing(inner);

    let bucket_inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(bucket_inner);
    bucket_object::destroy_cap_for_testing(bucket_object_cap);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

fun new_id_for_testing(ctx: &mut TxContext): ID {
    let id = object::new(ctx);
    let object_id = id.to_inner();
    id.delete();
    object_id
}
