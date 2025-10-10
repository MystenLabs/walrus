// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(deprecated_usage)]
#[test_only]
module subsidies::subsidies_tests;

use subsidies::subsidies::{Self, Subsidies};
use sui::{coin::Coin, test_scenario as test};
use wal::wal::WAL;
use walrus::{
    blob::{Self, Blob},
    messages,
    storage_resource::Storage,
    system::{Self, System},
    test_utils::{mint_frost, assert_eq}
};

const RS2: u8 = 1;

const ROOT_HASH: u256 = 0xABC;
const ENCODED_SIZE: u64 = 5_000_000;
const UNENCODED_SIZE: u64 = 1;

const N_COINS: u64 = 1_000_000_000;

#[test]
fun test_new() {
    let user = @0xa11ce;
    let mut test = test::begin(user);
    let ctx = test.ctx();

    let package_id = object::new(ctx);
    let package_id_inner = object::uid_to_inner(&package_id);
    let admin_cap = subsidies::new(package_id_inner, ctx);

    test.next_tx(user);
    let subsidies = test.take_shared<Subsidies>();

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), 0);
    assert!(admin_cap.admin_cap_subsidies_id() == object::id(&subsidies));

    admin_cap.destroy_admin_cap();
    subsidies.destroy_subsidies();
    object::delete(package_id);
    test.end();
}

#[test]
fun test_new_with_initial_rates_and_funds_public_fn() {
    let user = @0xa11ce;
    let mut test = test::begin(user);
    let ctx = test.ctx();

    let initial_buyer_subsidy_rate: u16 = 5_00; // 5%
    let initial_storage_node_subsidy_rate: u16 = 10_00; // 10%
    let initial_funds_value = 1_000_000;
    let package_id = object::new(ctx);

    let admin_cap = subsidies::new_with_initial_rates_and_funds(
        package_id.to_inner(),
        initial_buyer_subsidy_rate,
        initial_storage_node_subsidy_rate,
        mint_frost(initial_funds_value, ctx),
        ctx,
    );

    test.next_tx(user);
    let subsidies = test.take_shared<Subsidies>();

    assert_eq!(subsidies.buyer_subsidy_rate(), initial_buyer_subsidy_rate);
    assert_eq!(subsidies.system_subsidy_rate(), initial_storage_node_subsidy_rate);
    assert_eq!(subsidies.subsidy_pool_value(), initial_funds_value);
    assert!(admin_cap.admin_cap_subsidies_id() == object::id(&subsidies));

    admin_cap.destroy_admin_cap();
    subsidies.destroy_subsidies();
    object::delete(package_id);

    test.end();
}

#[test]
fun test_new_subsidy_object(): System {
    let (system, subsidies, admin_cap) = setup_system_and_subsidies_no_funds();

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);

    system
}

#[test]
fun test_new_subsidy_object_with_initial_rates_and_funds() {
    let initial_buyer_subsidy_rate: u16 = 5_00; // 5%
    let initial_storage_node_subsidy_rate: u16 = 10_00; // 10%
    let initial_funds_value = 1_000_000;

    let ctx = &mut tx_context::dummy();
    let (subsidies, admin_cap) = subsidies::new_with_initial_rates_and_funds_for_testing(
        initial_buyer_subsidy_rate,
        initial_storage_node_subsidy_rate,
        mint_frost(initial_funds_value, ctx),
        ctx,
    );

    assert_eq!(subsidies.buyer_subsidy_rate(), initial_buyer_subsidy_rate);
    assert_eq!(subsidies.system_subsidy_rate(), initial_storage_node_subsidy_rate);
    assert_eq!(subsidies.subsidy_pool_value(), initial_funds_value);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
}

#[test]
fun test_add_funds_to_subsidy_pool(): System {
    let (system, mut subsidies, admin_cap) = setup_system_and_subsidies_no_funds();
    let initial_funds_value = 1_000_000;
    let ctx = &mut tx_context::dummy();

    subsidies.add_funds(mint_frost(initial_funds_value, ctx));

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), initial_funds_value);

    subsidies.add_funds(mint_frost(initial_funds_value, ctx));

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), (initial_funds_value + initial_funds_value));

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);

    system
}

#[test]
fun test_set_buyer_subsidy_rate() {
    let ctx = &mut tx_context::dummy();
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    let buyer_subsidy_rate: u16 = 5_00; // 5%
    subsidies.set_buyer_subsidy_rate(&admin_cap, buyer_subsidy_rate);

    assert_eq!(subsidies.buyer_subsidy_rate(), buyer_subsidy_rate);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
}

#[test]
fun test_set_system_subsidy_rate() {
    let ctx = &mut tx_context::dummy();
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), 0);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    let storage_node_subsidy_rate: u16 = 10_00; // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, storage_node_subsidy_rate);

    assert_eq!(subsidies.buyer_subsidy_rate(), 0);
    assert_eq!(subsidies.system_subsidy_rate(), storage_node_subsidy_rate);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
}

#[test, expected_failure(abort_code = subsidies::EInvalidSubsidyRate)]
fun test_set_buyer_subsidy_rate_invalid() {
    let ctx = &mut tx_context::dummy();
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);

    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_001);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
}

#[test, expected_failure(abort_code = subsidies::EInvalidSubsidyRate)]
fun test_set_system_subsidy_rate_invalid() {
    let ctx = &mut tx_context::dummy();
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);

    subsidies.set_system_subsidy_rate(&admin_cap, 10_001);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
}

#[test]
fun test_extend_blob_no_funds_no_subsidies(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let mut payment = mint_frost(1000, ctx);

    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    // No subsidies applied, the pool should remain at 0
    assert_eq!(subsidies.subsidy_pool_value(), 0);
    // Blob storage end should increase by 3 epochs
    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, blob)
}

#[test]
fun test_extend_blob_no_funds_buyer_subsidies(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    let mut payment = mint_frost(1000, ctx);

    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 0);
    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, blob)
}

#[test]
fun test_extend_blob_no_funds_storage_node_subsidies(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%
    let mut payment = mint_frost(1000, ctx);

    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    assert_eq!(payment.value(), 1000);
    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 0);
    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, blob)
}

#[test]
fun test_extend_blob_funds_with_subsidies(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 1_000_000;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(68, ctx);
    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 0);
    assert_eq!(system.get_system_rewards_balance(0).value(), 29);
    assert_eq!(system.get_system_rewards_balance(1).value(), 27);
    assert_eq!(system.get_system_rewards_balance(2).value(), 27);
    assert_eq!(subsidies.subsidy_pool_value(), 999_986);

    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);

    (system, payment, blob)
}

#[test]
fun test_reserve_space_no_funds_no_subsidies(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_no_funds_buyer_subsidies(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_no_funds_storage_node_subsidies(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 0);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_funds_with_subsidies(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 1_000_000;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 932);
    assert_eq!(subsidies.subsidy_pool_value(), 999_986);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_funds_with_subsidies_full_pool_consumption(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 100;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 932);
    assert_eq!(subsidies.subsidy_pool_value(), 86);
    assert_eq!(system.get_system_rewards_balance(0).value(), 28);
    assert_eq!(system.get_system_rewards_balance(1).value(), 27);
    assert_eq!(system.get_system_rewards_balance(2).value(), 27);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_insufficient_funds_with_subsidies(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 10;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 20_00); // 20%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    // split funds porpotionally => buyer subsidy: pool_value * 0.6666 = 6
    // original cost = 75 => remaining: 1000 - 75 + 6 = 931
    assert_eq!(payment.value(), 931);
    // system subsidy: pool_value * 0.3333 = 3
    // pool remaining: 10 - 6 - 3 = 1
    assert_eq!(subsidies.subsidy_pool_value(), 1);
    assert_eq!(system.get_system_rewards_balance(0).value(), 26);
    assert_eq!(system.get_system_rewards_balance(1).value(), 26);
    assert_eq!(system.get_system_rewards_balance(2).value(), 26);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_reserve_space_with_credits(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 100;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 100_00); // 100%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(0, ctx);
    assert_eq!(payment.value(), 0);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    // buyer subsidy: 75, system_subsidy: 7
    // 100 - 75 - 7 = 18
    assert_eq!(subsidies.subsidy_pool_value(), 18);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

// === Helper functions ===

fun setup_system_and_subsidies_no_funds(): (System, subsidies::Subsidies, subsidies::AdminCap) {
    let ctx = &mut tx_context::dummy();
    let system = system::new_for_testing(ctx);
    let (subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    (system, subsidies, admin_cap)
}

fun get_storage_resource(system: &mut System, encoded_size: u64, epochs_ahead: u32): Storage {
    let ctx = &mut tx_context::dummy();
    let mut fake_coin = mint_frost(N_COINS, ctx);
    let storage = system.reserve_space(
        encoded_size,
        epochs_ahead,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
    storage
}

#[test]
fun test_extend_blob_funds_with_subsidies_full_pool_consumption(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 150;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);
    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);
    assert_eq!(subsidies.subsidy_pool_value(), 136);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);

    (system, payment, blob)
}

#[test]
fun test_extend_blob_with_credits(): (System, Coin<WAL>, Blob) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 100;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 100_00); // 100%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(0, ctx);
    let storage = get_storage_resource(&mut system, ENCODED_SIZE, 3);

    let mut blob = register_default_blob(&mut subsidies, &mut system, storage, false);
    let certify_message = messages::certified_permanent_blob_message_for_testing(blob.blob_id());
    // Set certify
    blob.certify_with_certified_msg_for_testing(system.epoch(), certify_message);
    // Assert certified
    assert!(blob.certified_epoch().is_some());
    let initial_blob_storage_end = blob.storage().end_epoch();

    assert_eq!(payment.value(), 0);
    subsidies.extend_blob(&mut system, &mut blob, 3, &mut payment, ctx);

    assert_eq!(blob.storage().end_epoch(), initial_blob_storage_end + 3);
    // buyer subsidy: 75, system subsidy: 7
    // 100 - 75 - 7 = 18
    assert_eq!(subsidies.subsidy_pool_value(), 18);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);

    (system, payment, blob)
}

#[test]
fun test_subsidies_with_zero_buyer_rate(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 1_000_000;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 0); // 0%
    subsidies.set_system_subsidy_rate(&admin_cap, 10_00); // 10%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 925);
    assert_eq!(subsidies.subsidy_pool_value(), 999_993);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

#[test]
fun test_subsidies_with_zero_system_rate(): (System, Coin<WAL>, Storage) {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing(ctx);
    let (mut subsidies, admin_cap) = subsidies::new_for_testing(ctx);
    let initial_funds_value = 1_000_000;
    subsidies.add_funds(mint_frost(initial_funds_value, ctx));
    subsidies.set_buyer_subsidy_rate(&admin_cap, 10_00); // 10%
    subsidies.set_system_subsidy_rate(&admin_cap, 0); // 0%

    let mut payment = mint_frost(1000, ctx);

    let storage = subsidies.reserve_space(&mut system, ENCODED_SIZE, 3, &mut payment, ctx);

    assert_eq!(payment.value(), 932);
    assert_eq!(subsidies.subsidy_pool_value(), 999_993);

    subsidies::destroy_admin_cap(admin_cap);
    subsidies::destroy_subsidies(subsidies);
    (system, payment, storage)
}

fun register_default_blob(
    subsidies: &mut Subsidies,
    system: &mut System,
    storage: Storage,
    deletable: bool,
): Blob {
    let ctx = &mut tx_context::dummy();
    let mut fake_coin = mint_frost(N_COINS, ctx);
    // Register a Blob
    let blob_id = blob::derive_blob_id(ROOT_HASH, RS2, UNENCODED_SIZE);
    let blob = subsidies.register_blob_v2(
        system,
        storage,
        blob_id,
        ROOT_HASH,
        UNENCODED_SIZE,
        RS2,
        0, // chunk_size: 0 for RS2 encoding (non-chunked)
        deletable,
        &mut fake_coin,
        ctx,
    );

    fake_coin.burn_for_testing();
    blob
}
