// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_bucket::blob_bucket_tests;

use blob_bucket::{blob_bucket, blob_bucket_inner_v1};
use std::unit_test::assert_eq;
use walrus::{
    blob,
    encoding,
    messages,
    storage_pool,
    system::{Self, System},
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap}
};

const RS2: u8 = 1;
const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;
const N_COINS: u64 = 1_000_000_000;
const WRITE_PAYMENT: u64 = 100_000_000_000;

#[test]
fun full_blob_bucket_lifecycle() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let (mut bucket, cap) = blob_bucket::new(
        &mut system,
        encoded_size,
        3,
        &mut pool_payment,
        ctx,
    );
    pool_payment.burn_for_testing();

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::register_blob(
        &mut bucket,
        &cap,
        &mut system,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        ctx,
    );
    assert!(blob_bucket::has_blob(&bucket, blob_id));
    assert_eq!(blob_bucket::blob_count(&bucket), 1);
    assert_eq!(blob_bucket::used_encoded_bytes(&bucket), encoded_size);

    let mut extension_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::extend_storage_pool(&mut bucket, &cap, &mut system, 1, &mut extension_payment);
    assert_eq!(blob_bucket::end_epoch(&bucket), 4);

    let mut capacity_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::increase_storage_pool_capacity(
        &mut bucket,
        &cap,
        &mut system,
        encoded_size,
        &mut capacity_payment,
    );
    assert_eq!(blob_bucket::reserved_encoded_capacity_bytes(&bucket), encoded_size * 2);

    let object_id = blob_bucket::get_blob_object_id(&bucket, blob_id);
    let confirmation_message = messages::certified_deletable_message_bytes(
        EPOCH,
        blob_id,
        object_id,
    );
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    blob_bucket::certify_blob(
        &mut bucket,
        &system,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );

    blob_bucket::delete_blob(&mut bucket, &cap, &system, blob_id);
    assert_eq!(blob_bucket::blob_count(&bucket), 0);
    assert_eq!(blob_bucket::used_encoded_bytes(&bucket), 0);
    assert_eq!(
        blob_bucket::available_encoded_bytes(&bucket),
        blob_bucket::reserved_encoded_capacity_bytes(&bucket),
    );

    write_payment.burn_for_testing();
    extension_payment.burn_for_testing();
    capacity_payment.burn_for_testing();
    let inner = blob_bucket::destroy_for_testing(bucket);
    let pool = blob_bucket_inner_v1::destroy_for_testing(inner);
    blob_bucket::destroy_cap_for_testing(cap);
    storage_pool::destroy_for_testing(pool);
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = blob_bucket::EInvalidBlobBucketCap)]
fun wrong_cap_cannot_register_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut left_payment = test_utils::mint_frost(N_COINS, ctx);
    let mut right_payment = test_utils::mint_frost(N_COINS, ctx);

    let (mut left_bucket, _left_cap) = blob_bucket::new(
        &mut system,
        encoded_size,
        3,
        &mut left_payment,
        ctx,
    );
    let (_right_bucket, right_cap) = blob_bucket::new(
        &mut system,
        encoded_size,
        3,
        &mut right_payment,
        ctx,
    );

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut write_payment = test_utils::mint_frost(WRITE_PAYMENT, ctx);
    blob_bucket::register_blob(
        &mut left_bucket,
        &right_cap,
        &mut system,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        ctx,
    );

    abort
}
