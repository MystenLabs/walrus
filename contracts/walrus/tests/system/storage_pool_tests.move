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

// === Pool blob registration tests ===

#[test]
fun register_multiple_pool_blobs_happy_path() {
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
    assert_eq!(pool.used_size(), pool_capacity);
    assert_eq!(pool.available_encoded_size(), 0);

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EInsufficientCapacity)]
fun register_pool_blob_exceeds_capacity() {
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

// === Pool blob certification tests ===

// TODO(WAL-1157): decide whether we want to support permanent blob.
#[test]
fun certify_pool_blob_permanent() {
    let sk = test_utils::bls_sk_for_testing();
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Certify the blob via the system interface.
    let confirmation_message = messages::certified_permanent_message_bytes(EPOCH, blob_id);
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    system.certify_pool_blob(
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
fun certify_pool_blob_deletable() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    certify_deletable_pool_blob(&mut pool, blob_id, EPOCH);

    pool.destroy_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = storage_pool::EAlreadyCertified)]
fun certify_pool_blob_already_certified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Certify once.
    certify_permanent_pool_blob(&mut pool, blob_id, EPOCH);

    // Certify again. Test fails here.
    certify_permanent_pool_blob(&mut pool, blob_id, EPOCH);

    abort
}

#[test, expected_failure(abort_code = storage_pool::EInvalidBlobId)]
fun certify_pool_blob_wrong_blob_id() {
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
fun certify_pool_blob_deletable_msg_for_permanent() {
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
fun certify_pool_blob_deletable_wrong_object_id() {
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
fun certify_pool_blob_expired_pool() {
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

// === Pool blob deletion tests ===

#[test]
fun delete_pool_blob_certified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    // Certify the deletable blob.
    certify_deletable_pool_blob(&mut pool, blob_id, EPOCH);

    let used_before = pool.used_size();

    // Delete the blob.
    system.delete_pool_blob(&mut pool, blob_id);

    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_size(), 0);
    assert!(pool.used_size() < used_before);

    pool.destroy();
    system.destroy_for_testing();
}

#[test]
fun delete_pool_blob_uncertified() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    // Delete without certifying.
    system.delete_pool_blob(&mut pool, blob_id);

    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_size(), 0);

    pool.destroy();
    system.destroy_for_testing();
}

// TODO(WAL-1157): if we don't need to support permanent blob, we need to update this test as well.
#[test, expected_failure(abort_code = storage_pool::EBlobNotDeletable)]
fun delete_pool_blob_not_deletable() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, false, ctx);

    // Try to delete a non-deletable blob. Test fails here.
    system.delete_pool_blob(&mut pool, blob_id);

    abort
}

#[test]
fun delete_and_reuse_capacity() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let mut pool = create_default_pool(&mut system, ctx);

    let blob_id = default_blob_id();
    register_blob_in_pool(&mut system, &mut pool, blob_id, SIZE, true, ctx);

    let available_after_register = pool.available_encoded_size();

    // Certify the blob.
    certify_deletable_pool_blob(&mut pool, blob_id, EPOCH);

    // Delete the blob, freeing capacity.
    system.delete_pool_blob(&mut pool, blob_id);
    assert_eq!(pool.used_size(), 0);

    // Register a new blob reusing the freed capacity.
    register_blob_in_pool_with_root(&mut system, &mut pool, 0x222, SIZE, false, ctx);

    assert_eq!(pool.blob_count(), 1);
    assert_eq!(pool.available_encoded_size(), available_after_register);

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

// === Pool destruction tests ===

#[test]
fun destroy_empty_pool() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let pool = create_default_pool(&mut system, ctx);

    // An empty pool can be destroyed.
    pool.destroy();
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
    pool.destroy();

    abort
}

// === Full lifecycle test ===

#[test]
fun full_pool_blob_lifecycle() {
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
    let used_after_register = pool.used_size();
    assert!(used_after_register > 0);

    // 3. Certify the blob via the system interface.
    let object_id = object::id(pool.borrow_blob_mut(blob_id));
    let confirmation_message = messages::certified_deletable_message_bytes(
        EPOCH,
        blob_id,
        object_id,
    );
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    system.certify_pool_blob(
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
    system.delete_pool_blob(&mut pool, blob_id);
    assert_eq!(pool.blob_count(), 0);
    assert_eq!(pool.used_size(), 0);

    // 6. Destroy the empty pool.
    pool.destroy();

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
    pool: &mut StoragePool,
    blob_id: u256,
    size: u64,
    deletable: bool,
    ctx: &mut TxContext,
) {
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.register_pool_blob(
        pool,
        blob_id,
        ROOT_HASH,
        size,
        RS2,
        deletable,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
}

fun register_blob_in_pool_with_root(
    system: &mut System,
    pool: &mut StoragePool,
    root_hash: u256,
    size: u64,
    deletable: bool,
    ctx: &mut TxContext,
) {
    let blob_id = blob::derive_blob_id(root_hash, RS2, size);
    let mut fake_coin = test_utils::mint_frost(N_COINS, ctx);
    system.register_pool_blob(
        pool,
        blob_id,
        root_hash,
        size,
        RS2,
        deletable,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
}

/// Certifies a permanent pool blob using the test-only message helper.
fun certify_permanent_pool_blob(pool: &mut StoragePool, blob_id: u256, epoch: u32) {
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob_id);
    let end_epoch = pool.end_epoch();
    storage_pool::certify(pool.borrow_blob_mut(blob_id), epoch, end_epoch, certify_message);
}

/// Certifies a deletable pool blob using the test-only message helper.
fun certify_deletable_pool_blob(pool: &mut StoragePool, blob_id: u256, epoch: u32) {
    let object_id = object::id(pool.borrow_blob_mut(blob_id));
    let certify_message = messages::certified_deletable_blob_message_for_testing(
        blob_id,
        object_id,
    );
    let end_epoch = pool.end_epoch();
    storage_pool::certify(pool.borrow_blob_mut(blob_id), epoch, end_epoch, certify_message);
}

fun default_blob_id(): u256 {
    blob::derive_blob_id(ROOT_HASH, RS2, SIZE)
}

fun advance_epoch(system: &mut System) {
    use walrus::epoch_parameters::epoch_params_for_testing;
    let committee = test_utils::new_bls_committee_for_testing(1);
    let (_, balances) = system
        .advance_epoch(committee, &epoch_params_for_testing())
        .into_keys_values();
    balances.do!(|b| { b.destroy_for_testing(); });
}
