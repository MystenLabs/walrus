// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::storage_pool_tests;

use std::unit_test::assert_eq;
use walrus::{
    blob,
    encoding,
    messages,
    storage_pool::{Self, StoragePool},
    system::{Self, System},
    system_state_inner,
    test_utils::{Self, bls_min_pk_sign, signers_to_bitmap}
};

const RS2: u8 = 1;

const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;

const N_COINS: u64 = 1_000_000_000;

// === StoragePool creation tests ===

#[test, expected_failure(abort_code = system_state_inner::EInvalidResourceSize)]
fun create_storage_pool_zero_size() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);

    // Zero storage amount should fail.
    let _pool = system.create_storage_pool(0, 3, &mut fake_coin, ctx);

    abort
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun create_storage_pool_zero_epochs() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);

    let storage_size = encoded_size(&system, SIZE);
    // Zero epochs should fail.
    let _pool = system.create_storage_pool(storage_size, 0, &mut fake_coin, ctx);

    abort
}

// === StoragePool creation with Storage tests ===

#[test]
fun create_storage_pool_with_storage_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    // Reserve a Storage object, then create a pool from it.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let storage = system.reserve_space(storage_size, 3, &mut fake_coin, ctx);
    let pool = system.create_storage_pool_with_storage(storage, ctx);

    assert_eq!(pool.reserved_encoded_capacity_bytes(), storage_size);
    assert_eq!(pool.start_epoch(), 0);
    assert_eq!(pool.end_epoch(), 3);
    assert_eq!(pool.blob_count(), 0);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun create_storage_pool_with_storage_from_deleted_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    // Reserve storage, register a deletable blob, then delete it to recover Storage.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let storage = system.reserve_space(storage_size, 3, &mut fake_coin, ctx);
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, SIZE);
    let mut write_payment = test_utils::mint_frost(N_COINS, ctx);
    let blob = system.register_blob(
        storage,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        ctx,
    );
    let recovered_storage = system.delete_blob(blob);

    // Create a pool from the recovered Storage.
    let pool = system.create_storage_pool_with_storage(recovered_storage, ctx);
    assert_eq!(pool.reserved_encoded_capacity_bytes(), storage_size);
    assert_eq!(pool.end_epoch(), 3);

    fake_coin.burn_for_testing();
    write_payment.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun create_storage_pool_with_storage_from_destroyed_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    // Create a pool, destroy it to get Storage back, then create a new pool.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let pool = system.create_storage_pool(storage_size, 3, &mut fake_coin, ctx);
    let storage = pool.destroy();
    let new_pool = system.create_storage_pool_with_storage(storage, ctx);

    assert_eq!(new_pool.reserved_encoded_capacity_bytes(), storage_size);
    assert_eq!(new_pool.end_epoch(), 3);

    fake_coin.burn_for_testing();
    new_pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidResourceSize)]
fun create_storage_pool_with_storage_zero_size() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    // Create a zero-size Storage via split_by_size.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let mut storage = system.reserve_space(storage_size, 3, &mut fake_coin, ctx);
    let _remainder = storage.split_by_size(0, ctx);
    // storage now has size 0.
    let _pool = system.create_storage_pool_with_storage(storage, ctx);

    abort
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun create_storage_pool_with_storage_future_start() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    // Create a Storage with future start_epoch via split_by_epoch.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let mut storage = system.reserve_space(storage_size, 3, &mut fake_coin, ctx);
    let future_storage = storage.split_by_epoch(1, ctx);
    // future_storage covers [1, 3), current epoch is 0.
    let _pool = system.create_storage_pool_with_storage(future_storage, ctx);

    abort
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun create_storage_pool_with_storage_expired() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let storage_size = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let storage = system.reserve_space(storage_size, 1, &mut fake_coin, ctx);

    // Advance past end_epoch.
    advance_epoch(&mut system);

    let _pool = system.create_storage_pool_with_storage(storage, ctx);

    abort
}

// === Pooled blob registration tests ===

#[test]
fun register_multiple_pooled_blobs_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    // Create a pool with enough capacity for multiple small blobs.
    let small_size: u64 = 100;
    let pool_capacity = encoded_size(&system, small_size) * 3;
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let mut pool = system.create_storage_pool(pool_capacity, 3, &mut fake_coin, ctx);
    fake_coin.burn_for_testing();

    // Register three blobs with different root hashes.
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x111, small_size, false, ctx);
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x222, small_size, false, ctx);
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x333, small_size, false, ctx);

    assert_eq!(pool.blob_count(), 3);
    assert_eq!(pool.used_encoded_bytes(), pool_capacity);
    assert_eq!(pool.available_encoded_bytes(), 0);

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EInsufficientCapacity)]
fun register_pooled_blob_exceeds_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    // Create a pool with capacity for only one blob.
    let mut pool = create_default_pool(&mut system, ctx);
    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Try to register a second blob that exceeds the pool capacity. Test fails here.
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x222, SIZE, false, ctx);

    abort
}

// === Pooled blob certification tests ===

// TODO(WAL-1157): decide whether we want to support permanent blob.
#[test]
fun certify_pooled_blob_permanent() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Certify the blob via the system interface.
    let confirmation_message = messages::certified_permanent_message_bytes(EPOCH, blob_id);
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    system.certify_pooled_blob(
        &mut pool,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

// TODO(WAL-1157): if we don't need to support permanent blob, we need to update this test as well.
#[test]
fun certify_pooled_blob_deletable() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    certify_deletable_pooled_blob(&mut pool, blob_id, EPOCH);

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EAlreadyCertified)]
fun certify_pooled_blob_already_certified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Certify once.
    certify_permanent_pooled_blob(&mut pool, blob_id, EPOCH);

    // Certify again. Test fails here.
    certify_permanent_pooled_blob(&mut pool, blob_id, EPOCH);

    abort
}

#[test, expected_failure(abort_code = storage_pool::EInvalidBlobId)]
fun certify_pooled_blob_wrong_blob_id() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Create a certify message with a wrong blob ID.
    let wrong_blob_id = blob::derive_blob_id(0x999, RS2, SIZE);
    let certify_message = messages::certified_permanent_blob_message_for_testing(wrong_blob_id);

    // Test fails here.
    let end_epoch = pool.end_epoch();
    storage_pool::certify(pool.borrow_blob_mut(blob_id), EPOCH, end_epoch, certify_message);

    abort
}

// TODO(WAL-1157): if we don't need to support permanent blob, we need to update this test as well.
#[test, expected_failure(abort_code = storage_pool::EInvalidBlobPersistenceType)]
fun certify_pooled_blob_deletable_msg_for_permanent() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Use a deletable message for a permanent blob.
    let object_id = object::id(pool.borrow_blob_mut(blob_id));
    let certify_message = messages::certified_deletable_blob_message_for_testing(
        blob_id,
        object_id,
    );

    // Test fails here.
    let end_epoch = pool.end_epoch();
    storage_pool::certify(pool.borrow_blob_mut(blob_id), EPOCH, end_epoch, certify_message);

    abort
}

#[test, expected_failure(abort_code = storage_pool::EInvalidBlobObject)]
fun certify_pooled_blob_deletable_wrong_object_id() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    // Use a wrong object ID. Test fails here.
    let certify_message = messages::certified_deletable_blob_message_for_testing(
        blob_id,
        object::id_from_address(@1),
    );
    let end_epoch = pool.end_epoch();
    storage_pool::certify(pool.borrow_blob_mut(blob_id), EPOCH, end_epoch, certify_message);

    abort
}

#[test, expected_failure(abort_code = storage_pool::EResourceBounds)]
fun certify_pooled_blob_expired_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    // Create a pool that expires after epoch 1.
    let mut pool = create_pool_with_epochs(&mut system, SIZE, 1, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Advance the epoch so the pool is expired.
    advance_epoch(&mut system);

    // Try to certify after pool expired. Test fails here.
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob_id);
    let current_epoch = system.epoch();
    let end_epoch = pool.end_epoch();
    storage_pool::certify(
        pool.borrow_blob_mut(blob_id),
        current_epoch,
        end_epoch,
        certify_message,
    );

    abort
}

// === Pooled blob deletion tests ===

#[test]
fun delete_pooled_blob_certified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    // Certify the deletable blob.
    certify_deletable_pooled_blob(&mut pool, blob_id, EPOCH);

    let used_before = pool.used_encoded_bytes();

    // Delete the blob.
    system.delete_pooled_blob(&mut pool, blob_id);

    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_encoded_bytes(), 0);
    assert!(pool.used_encoded_bytes() < used_before);

    pool.destroy().destroy();
    system.destroy_for_testing();
}

#[test]
fun delete_pooled_blob_uncertified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    // Delete without certifying.
    system.delete_pooled_blob(&mut pool, blob_id);

    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_encoded_bytes(), 0);

    pool.destroy().destroy();
    system.destroy_for_testing();
}

// TODO(WAL-1157): if we don't need to support permanent blob, we need to update this test as well.
#[test, expected_failure(abort_code = storage_pool::EBlobNotDeletable)]
fun delete_pooled_blob_not_deletable() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Try to delete a non-deletable blob. Test fails here.
    system.delete_pooled_blob(&mut pool, blob_id);

    abort
}

#[test]
fun delete_and_reuse_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    let available_after_register = pool.available_encoded_bytes();

    // Certify the blob.
    certify_deletable_pooled_blob(&mut pool, blob_id, EPOCH);

    // Delete the blob, freeing capacity.
    system.delete_pooled_blob(&mut pool, blob_id);
    assert_eq!(pool.used_encoded_bytes(), 0);

    // Register a new blob reusing the freed capacity.
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x222, SIZE, false, ctx);

    assert_eq!(pool.blob_count(), 1);
    assert_eq!(pool.available_encoded_bytes(), available_after_register);

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

// === Storage pool extension tests ===

#[test]
fun extend_storage_pool_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let original_end = pool.end_epoch();
    let extension = 2;

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.extend_storage_pool(&mut pool, extension, &mut fake_coin);

    assert_eq!(pool.end_epoch(), original_end + extension);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun extend_storage_pool_zero_epochs() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    // Extending by zero epochs should fail.
    system.extend_storage_pool(&mut pool, 0, &mut fake_coin);

    abort
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun extend_storage_pool_expired() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_pool_with_epochs(&mut system, SIZE, 1, ctx);

    // Advance the epoch so the pool is expired.
    advance_epoch(&mut system);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    // Extending an expired pool should fail.
    system.extend_storage_pool(&mut pool, 2, &mut fake_coin);

    abort
}

// === Storage pool capacity increase tests ===

#[test]
fun increase_storage_pool_capacity_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let additional_capacity = encoded_size(&system, SIZE);
    let original_reserved = pool.reserved_encoded_capacity_bytes();
    let original_available = pool.available_encoded_bytes();

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.increase_storage_pool_capacity(&mut pool, additional_capacity, &mut fake_coin);

    assert_eq!(pool.reserved_encoded_capacity_bytes(), original_reserved + additional_capacity);
    assert_eq!(pool.used_encoded_bytes(), 0);
    assert_eq!(pool.available_encoded_bytes(), original_available + additional_capacity);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun increase_storage_pool_capacity_preserves_used_bytes() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_pool_with_epochs(&mut system, SIZE, 3, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);
    let used_before = pool.used_encoded_bytes();
    let available_before = pool.available_encoded_bytes();
    let additional_capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.increase_storage_pool_capacity(&mut pool, additional_capacity, &mut fake_coin);

    assert_eq!(pool.used_encoded_bytes(), used_before);
    assert_eq!(pool.available_encoded_bytes(), available_before + additional_capacity);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun increase_storage_pool_capacity_allows_additional_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);
    assert_eq!(pool.available_encoded_bytes(), 0);

    let additional_capacity = encoded_size(&system, SIZE);
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.increase_storage_pool_capacity(&mut pool, additional_capacity, &mut fake_coin);
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x222, SIZE, false, ctx);

    assert_eq!(pool.blob_count(), 2);
    assert_eq!(pool.used_encoded_bytes(), encoded_size(&system, SIZE) * 2);
    assert_eq!(pool.available_encoded_bytes(), 0);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidResourceSize)]
fun increase_storage_pool_capacity_zero_size() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.increase_storage_pool_capacity(&mut pool, 0, &mut fake_coin);

    abort
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
fun increase_storage_pool_capacity_expired() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_pool_with_epochs(&mut system, SIZE, 1, ctx);
    let additional_capacity = encoded_size(&system, SIZE);

    advance_epoch(&mut system);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.increase_storage_pool_capacity(&mut pool, additional_capacity, &mut fake_coin);

    abort
}

#[test]
fun increase_storage_pool_capacity_remaining_lifetime_accounting() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = 500_000_000;
    let additional_capacity = 250_000_000;
    let epochs_ahead = 3;

    let storage_units = (additional_capacity + 1_048_576 - 1) / 1_048_576;
    let additional_payment_per_epoch = system.inner().storage_price_per_unit_size() * storage_units;

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, epochs_ahead, &mut fake_coin, ctx);

    advance_epoch(&mut system);

    let future_accounting = system.future_accounting();
    let rewards_before_current = walrus::storage_accounting::rewards(future_accounting.ring_lookup(
        0,
    ));
    let next_epoch_accounting = future_accounting.ring_lookup(1);
    let rewards_before_next = walrus::storage_accounting::rewards(next_epoch_accounting);
    let expired_epoch_accounting = future_accounting.ring_lookup(2);
    let rewards_before_after_expiry = walrus::storage_accounting::rewards(
        expired_epoch_accounting,
    );

    system.increase_storage_pool_capacity(&mut pool, additional_capacity, &mut fake_coin);

    assert_eq!(pool.reserved_encoded_capacity_bytes(), capacity + additional_capacity);
    assert_eq!(system.used_capacity_size(), capacity + additional_capacity);
    assert_eq!(
        system.inner().used_capacity_size_at_future_epoch(0),
        capacity + additional_capacity,
    );
    assert_eq!(
        system.inner().used_capacity_size_at_future_epoch(1),
        capacity + additional_capacity,
    );
    assert_eq!(system.inner().used_capacity_size_at_future_epoch(2), 0);

    let future_accounting = system.future_accounting();
    assert_eq!(
        walrus::storage_accounting::rewards(future_accounting.ring_lookup(0)),
        rewards_before_current + additional_payment_per_epoch,
    );
    assert_eq!(
        walrus::storage_accounting::rewards(future_accounting.ring_lookup(1)),
        rewards_before_next + additional_payment_per_epoch,
    );
    assert_eq!(
        walrus::storage_accounting::rewards(future_accounting.ring_lookup(2)),
        rewards_before_after_expiry,
    );

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EStorageExceeded)]
fun increase_storage_pool_capacity_exceeds_system_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(1_000_000_000, 1, &mut fake_coin, ctx);

    system.increase_storage_pool_capacity(&mut pool, 1, &mut fake_coin);

    abort
}

// === Pool destruction tests ===

#[test]
fun destroy_empty_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let pool = create_default_pool(&mut system, ctx);

    // An empty pool can be destroyed.
    pool.destroy().destroy();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EPoolNotEmpty)]
fun destroy_non_empty_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Destroying a pool with blobs should fail.
    pool.destroy().destroy();

    abort
}

// === Storage-based pool operations tests ===

#[test]
fun increase_capacity_with_storage_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Reserve additional Storage with the same end_epoch.
    let extra_storage = system.reserve_space(capacity, 3, &mut fake_coin, ctx);
    system.increase_storage_pool_capacity_with_storage(&mut pool, extra_storage);

    assert_eq!(pool.reserved_encoded_capacity_bytes(), capacity * 2);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun increase_capacity_with_storage_from_destroyed_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool_a = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);
    let pool_b = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Destroy pool_b and use its Storage to increase pool_a's capacity.
    let storage = pool_b.destroy();
    system.increase_storage_pool_capacity_with_storage(&mut pool_a, storage);

    assert_eq!(pool_a.reserved_encoded_capacity_bytes(), capacity * 2);

    fake_coin.burn_for_testing();
    pool_a.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun increase_capacity_with_storage_from_deleted_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Register and delete a blob to recover its Storage.
    let storage = system.reserve_space(capacity, 3, &mut fake_coin, ctx);
    let blob_id = default_blob_id();
    let mut write_payment = test_utils::mint_frost(N_COINS, ctx);
    let blob = system.register_blob(
        storage,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        true,
        &mut write_payment,
        ctx,
    );
    let recovered = system.delete_blob(blob);

    system.increase_storage_pool_capacity_with_storage(&mut pool, recovered);
    assert_eq!(pool.reserved_encoded_capacity_bytes(), capacity * 2);

    fake_coin.burn_for_testing();
    write_payment.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EIncompatibleEndEpoch)]
fun increase_capacity_with_storage_different_end_epoch() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);
    let storage = system.reserve_space(capacity, 5, &mut fake_coin, ctx);

    // Different end_epoch should fail.
    system.increase_storage_pool_capacity_with_storage(&mut pool, storage);

    abort
}

#[test, expected_failure(abort_code = storage_pool::EResourceBounds)]
fun increase_capacity_with_storage_future_start() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Create a Storage with future start_epoch via split.
    let mut storage = system.reserve_space(capacity, 3, &mut fake_coin, ctx);
    let future_storage = storage.split_by_epoch(1, ctx);
    // future_storage covers [1, 3), current epoch is 0.
    system.increase_storage_pool_capacity_with_storage(&mut pool, future_storage);

    abort
}

#[test]
fun extend_with_storage_happy_path() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);
    assert_eq!(pool.end_epoch(), 3);

    // Reserve Storage for the adjacent period [3, 5) with the same capacity.
    let extension = system.reserve_space_for_epochs(capacity, 3, 5, &mut fake_coin, ctx);
    system.extend_storage_pool_with_storage(&mut pool, extension);

    assert_eq!(pool.end_epoch(), 5);
    assert_eq!(pool.reserved_encoded_capacity_bytes(), capacity);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EResourceBounds)]
fun extend_with_storage_not_extending() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Storage that ends before the pool — not an extension.
    let storage = system.reserve_space(capacity, 2, &mut fake_coin, ctx);
    system.extend_storage_pool_with_storage(&mut pool, storage);

    abort
}

#[test, expected_failure(abort_code = walrus::storage_resource::EIncompatibleEpochs)]
fun extend_with_storage_non_adjacent() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Storage starts at 4, not adjacent to pool's end_epoch 3.
    let extension = system.reserve_space_for_epochs(capacity, 4, 6, &mut fake_coin, ctx);
    system.extend_storage_pool_with_storage(&mut pool, extension);

    abort
}

#[test, expected_failure(abort_code = walrus::storage_resource::EIncompatibleAmount)]
fun extend_with_storage_different_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let mut pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Adjacent period but different capacity — fuse_periods requires same size.
    let extension = system.reserve_space_for_epochs(capacity * 2, 3, 5, &mut fake_coin, ctx);
    system.extend_storage_pool_with_storage(&mut pool, extension);

    abort
}

#[test]
fun destroy_pool_reuse_storage_for_blob() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = encoded_size(&system, SIZE);

    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let pool = system.create_storage_pool(capacity, 3, &mut fake_coin, ctx);

    // Destroy pool and use the returned Storage to register a blob.
    let storage = pool.destroy();
    let blob_id = default_blob_id();
    let mut write_payment = test_utils::mint_frost(N_COINS, ctx);
    let blob = system.register_blob(
        storage,
        blob_id,
        ROOT_HASH,
        SIZE,
        RS2,
        false,
        &mut write_payment,
        ctx,
    );

    assert_eq!(blob.end_epoch(), 3);

    fake_coin.burn_for_testing();
    write_payment.burn_for_testing();
    blob.burn();
    system.destroy_for_testing();
}

// === Capacity accounting and reward distribution tests ===

// Test that the capacity and rewards are accounted for the correct epochs when creating and
// extending a storage pool.
#[test]
fun storage_pool_capacity_and_rewards() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = 500_000_000;
    let epochs_ahead: u32 = 3;

    // Compute expected payment per epoch.
    // storage_price_per_unit_size = 5, BYTES_PER_UNIT_SIZE = 1 MiB
    let storage_units = (capacity + 1_048_576 - 1) / 1_048_576;
    let payment_per_epoch = system.inner().storage_price_per_unit_size() * storage_units;

    assert_eq!(system.used_capacity_size(), 0);

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let mut pool = system.create_storage_pool(capacity, epochs_ahead, &mut fake_coin, ctx);

    // After create: capacity and rewards accounted for epochs [0, 3).
    assert_eq!(system.used_capacity_size(), capacity);
    let future_accounting = system.future_accounting();
    epochs_ahead.do!(|i| {
        assert_eq!(system.inner().used_capacity_size_at_future_epoch(i), capacity);
        assert_eq!(
            walrus::storage_accounting::rewards(future_accounting.ring_lookup(i)),
            payment_per_epoch,
        );
    });
    // Epoch 3: no capacity or rewards (end_epoch is exclusive).
    assert_eq!(system.inner().used_capacity_size_at_future_epoch(epochs_ahead), 0);
    assert_eq!(walrus::storage_accounting::rewards(future_accounting.ring_lookup(epochs_ahead)), 0);

    // Extend by 2 → pool now covers [0, 5).
    system.extend_storage_pool(&mut pool, 2, &mut fake_coin);
    let epoch_after_extend = epochs_ahead + 2;
    assert_eq!(pool.end_epoch(), epoch_after_extend);

    let future_accounting = system.future_accounting();
    // All epochs [0, 5) should have capacity accounted and 1x payment each.
    epoch_after_extend.do!(|i| {
        assert_eq!(system.inner().used_capacity_size_at_future_epoch(i), capacity);
        assert_eq!(
            walrus::storage_accounting::rewards(future_accounting.ring_lookup(i)),
            payment_per_epoch,
        );
    });
    // Epoch 5: free.
    assert_eq!(system.inner().used_capacity_size_at_future_epoch(epoch_after_extend), 0);
    assert_eq!(
        walrus::storage_accounting::rewards(future_accounting.ring_lookup(epoch_after_extend)),
        0,
    );

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test]
fun create_storage_pool_capacity_freed_after_epoch_advance() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let capacity = 500_000_000;

    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let pool = system.create_storage_pool(capacity, 2, &mut fake_coin, ctx);

    // Pool covers epochs [0, 2).
    assert_eq!(system.used_capacity_size(), capacity);

    // Advance to epoch 1.
    advance_epoch(&mut system);
    assert_eq!(system.used_capacity_size(), capacity);

    // Advance to epoch 2 — pool has expired, capacity freed.
    advance_epoch(&mut system);
    assert_eq!(system.used_capacity_size(), 0);

    fake_coin.burn_for_testing();
    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EStorageExceeded)]
fun create_storage_pool_exceeds_system_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    // total_capacity_size in test is 1_000_000_000.
    let mut fake_coin = test_utils::mint_frost(N_COINS * 10, ctx);
    let _pool = system.create_storage_pool(1_000_000_001, 1, &mut fake_coin, ctx);

    abort
}

// === Full lifecycle test ===

#[test]
fun full_pooled_blob_lifecycle() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);

    // 1. Create pool.
    let mut pool = create_default_pool(&mut system, ctx);
    assert_eq!(pool.blob_count(), 0);

    // 2. Register a deletable blob.
    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);
    assert_eq!(pool.blob_count(), 1);
    let used_after_register = pool.used_encoded_bytes();
    assert!(used_after_register > 0);

    // 3. Certify the blob via the system interface.
    let object_id = object::id(pool.borrow_blob_mut(blob_id));
    let confirmation_message = messages::certified_deletable_message_bytes(
        EPOCH,
        blob_id,
        object_id,
    );
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    system.certify_pooled_blob(
        &mut pool,
        blob_id,
        signature,
        signers_to_bitmap(&vector[0]),
        confirmation_message,
    );

    // 4. Extend the pool's lifetime.
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let original_end = pool.end_epoch();
    system.extend_storage_pool(&mut pool, 2, &mut fake_coin);
    assert_eq!(pool.end_epoch(), original_end + 2);

    // 5. Delete the blob.
    system.delete_pooled_blob(&mut pool, blob_id);
    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_encoded_bytes(), 0);

    // 6. Destroy the empty pool.
    pool.destroy().destroy();

    fake_coin.burn_for_testing();
    system.destroy_for_testing();
}

// === Helper functions ===

fun encoded_size(system: &System, unencoded_size: u64): u64 {
    encoding::encoded_blob_length(unencoded_size, RS2, system.n_shards())
}

fun create_default_pool(system: &mut System, ctx: &mut TxContext): StoragePool {
    create_pool_with_epochs(system, SIZE, 3, ctx)
}

fun create_pool_with_epochs(
    system: &mut System,
    unencoded_size: u64,
    epochs_ahead: u32,
    ctx: &mut TxContext,
): StoragePool {
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    let storage_size = encoded_size(system, unencoded_size);
    let pool = system.create_storage_pool(storage_size, epochs_ahead, &mut fake_coin, ctx);
    fake_coin.burn_for_testing();
    pool
}

fun register_blob_in_pool(
    system: &mut System,
    storage_pool: &mut StoragePool,
    blob_id: u256,
    unencoded_size: u64,
    deletable: bool,
    ctx: &mut TxContext,
) {
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.register_pooled_blob(
        storage_pool,
        blob_id,
        ROOT_HASH,
        unencoded_size,
        RS2,
        deletable,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
}

fun register_blob_in_pool_with_root(
    system: &mut System,
    storage_pool: &mut StoragePool,
    root_hash: u256,
    unencoded_size: u64,
    deletable: bool,
    ctx: &mut TxContext,
) {
    let blob_id = blob::derive_blob_id(root_hash, RS2, unencoded_size);
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.register_pooled_blob(
        storage_pool,
        blob_id,
        root_hash,
        unencoded_size,
        RS2,
        deletable,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
}

/// Certifies a permanent pooled blob using the test-only message helper.
fun certify_permanent_pooled_blob(storage_pool: &mut StoragePool, blob_id: u256, epoch: u32) {
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob_id);
    let end_epoch = storage_pool.end_epoch();
    storage_pool::certify(storage_pool.borrow_blob_mut(blob_id), epoch, end_epoch, certify_message);
}

/// Certifies a deletable pooled blob using the test-only message helper.
fun certify_deletable_pooled_blob(storage_pool: &mut StoragePool, blob_id: u256, epoch: u32) {
    let object_id = object::id(storage_pool.borrow_blob_mut(blob_id));
    let certify_message = messages::certified_deletable_blob_message_for_testing(
        blob_id,
        object_id,
    );
    let end_epoch = storage_pool.end_epoch();
    storage_pool::certify(storage_pool.borrow_blob_mut(blob_id), epoch, end_epoch, certify_message);
}

fun default_blob_id(): u256 {
    blob::derive_blob_id(ROOT_HASH, RS2, SIZE)
}

fun advance_epoch(system: &mut System) {
    use walrus::epoch_parameters::epoch_params_for_testing;
    let next_epoch = system.epoch() + 1;
    let committee = test_utils::new_bls_committee_for_testing(next_epoch);
    let (_, balances) = system
        .advance_epoch(committee, &epoch_params_for_testing())
        .into_keys_values();
    balances.do!(|b| { b.destroy_for_testing(); });
}
