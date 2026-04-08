// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module bucket_object::bucket_object_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use bucket_object::{bucket_object, bucket_object_inner_v1};
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
