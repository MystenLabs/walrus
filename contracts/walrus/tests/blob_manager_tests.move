// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::blob_manager_tests;

use std::unit_test::assert_eq;
use walrus::{
    blob,
    blob_manager,
    blob_manager_inner_v1,
    encoding,
    messages,
    system::{Self, System},
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap}
};

const RS2: u8 = 1;
const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;
const N_COINS: u64 = 1_000_000_000;
const INITIAL_WAL: u64 = 100_000_000_000;

#[test]
fun full_blob_manager_lifecycle() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut pool_payment = test_utils::mint_frost(N_COINS, ctx);
    let storage_pool = system.create_storage_pool(encoded_size, 3, &mut pool_payment, ctx);
    pool_payment.burn_for_testing();

    let initial_wal = test_utils::mint_frost(INITIAL_WAL, ctx);
    let (mut manager, cap) = blob_manager::new(storage_pool, initial_wal, ctx);

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    blob_manager::register_blob(
        &mut manager,
        &cap,
        &mut system,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        ctx,
    );
    assert!(blob_manager::has_blob(&manager, blob_id));
    assert_eq!(blob_manager::blob_count(&manager), 1);
    assert_eq!(blob_manager::used_encoded_bytes(&manager), encoded_size);

    let object_id = blob_manager::get_blob_object_id(&manager, blob_id);
    let confirmation_message = messages::certified_deletable_message_bytes(
        EPOCH,
        blob_id,
        object_id,
    );
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    blob_manager::certify_blob(
        &mut manager,
        &system,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );

    blob_manager::delete_blob(&mut manager, &cap, &system, blob_id);
    assert_eq!(blob_manager::blob_count(&manager), 0);
    assert_eq!(blob_manager::used_encoded_bytes(&manager), 0);
    assert_eq!(
        blob_manager::available_encoded_bytes(&manager),
        blob_manager::reserved_encoded_capacity_bytes(&manager),
    );

    let inner = blob_manager::destroy_for_testing(manager);
    blob_manager::destroy_cap_for_testing(cap);
    blob_manager_inner_v1::destroy_for_testing(inner);
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = blob_manager::EInvalidBlobManagerCap)]
fun wrong_cap_cannot_register_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    let encoded_size = encoding::encoded_blob_length(SIZE, RS2, system.n_shards());
    let mut left_payment = test_utils::mint_frost(N_COINS, ctx);
    let left_storage_pool = system.create_storage_pool(encoded_size, 3, &mut left_payment, ctx);
    let mut right_payment = test_utils::mint_frost(N_COINS, ctx);
    let right_storage_pool = system.create_storage_pool(encoded_size, 3, &mut right_payment, ctx);

    let initial_wal = test_utils::mint_frost(INITIAL_WAL, ctx);
    let (mut left_manager, _left_cap) = blob_manager::new(left_storage_pool, initial_wal, ctx);
    let other_wal = test_utils::mint_frost(INITIAL_WAL, ctx);
    let (_right_manager, right_cap) = blob_manager::new(right_storage_pool, other_wal, ctx);

    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    blob_manager::register_blob(
        &mut left_manager,
        &right_cap,
        &mut system,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        ctx,
    );

    abort
}
