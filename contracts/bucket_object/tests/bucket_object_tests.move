// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module bucket_object::bucket_object_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use bucket_object::{bucket_object, bucket_object_inner_v1};
use std::{string, unit_test::assert_eq};
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
fun new_records_linked_blob_bucket_and_defaults_versioning_to_disabled() {
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
    let (bucket_object, bucket_object_cap) = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        string::utf8(b"assets"),
        ctx,
    );

    assert_eq!(bucket_object::bucket_id(&bucket_object_cap), object::id(&bucket_object));
    assert_eq!(bucket_object::blob_bucket_id(&bucket_object), linked_blob_bucket_id);
    assert!(!bucket_object::versioning_enabled(&bucket_object));

    let inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(inner);
    bucket_object::destroy_cap_for_testing(bucket_object_cap);

    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket::destroy_for_testing(blob_bucket));
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

#[test]
fun cap_can_toggle_versioning() {
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
    let (mut bucket_object, bucket_object_cap) = bucket_object::new_for_testing(
        linked_blob_bucket_id,
        string::utf8(b"assets"),
        ctx,
    );

    bucket_object::set_versioning_enabled(&mut bucket_object, &bucket_object_cap, true);
    assert!(bucket_object::versioning_enabled(&bucket_object));

    let inner = bucket_object::destroy_for_testing(bucket_object);
    bucket_object_inner_v1::destroy_for_testing(inner);
    bucket_object::destroy_cap_for_testing(bucket_object_cap);

    let blob_bucket_inner = blob_bucket::destroy_for_testing(blob_bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(blob_bucket_inner);
    blob_bucket::destroy_cap_for_testing(blob_bucket_cap);
    storage_pool::destroy_for_testing(pool);
    pool_payment.burn_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = bucket_object::EInvalidBucketObjectCap)]
fun wrong_cap_cannot_toggle_versioning() {
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
    let (mut left_bucket_object, _left_bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&left_blob_bucket),
        string::utf8(b"left-assets"),
        ctx,
    );
    let (_right_bucket_object, right_bucket_object_cap) = bucket_object::new_for_testing(
        object::id(&right_blob_bucket),
        string::utf8(b"right-assets"),
        ctx,
    );

    bucket_object::set_versioning_enabled(&mut left_bucket_object, &right_bucket_object_cap, true);

    abort
}
