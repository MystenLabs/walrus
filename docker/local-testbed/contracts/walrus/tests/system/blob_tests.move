// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::blob_tests;

use sui::{bcs, dynamic_field};
use walrus::{
    blob::{Self, Blob},
    encoding,
    epoch_parameters::epoch_params_for_testing,
    messages,
    metadata,
    storage_resource::{Self, split_by_epoch, destroy, Storage},
    system::{Self, System},
    system_state_inner,
    test_utils::{Self, bls_min_pk_sign}
};

const RED_STUFF: u8 = 0;
const MAX_EPOCHS_AHEAD: u32 = 104;

const ROOT_HASH: u256 = 0xABC;
const SIZE: u64 = 5_000_000;
const EPOCH: u32 = 0;

const N_COINS: u64 = 1_000_000_000;

#[test]
public fun test_blob_register_happy_path(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    let blob = register_default_blob(&mut system, storage, false);

    blob.burn();
    system
}

#[test, expected_failure(abort_code = blob::EResourceSize)]
public fun test_blob_insufficient_space(): system::System {
    let mut system: system::System = system::new_for_testing();

    // Get a storage resource that is too small.
    let storage = get_storage_resource(&mut system, SIZE / 2, 3);

    // Test fails here
    let blob = register_default_blob(&mut system, storage, false);

    blob.burn();
    system
}

#[test]
public fun test_blob_certify_happy_path(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    let mut blob = register_default_blob(&mut system, storage, false);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Assert certified
    assert!(blob.certified_epoch().is_some());

    blob.burn();
    system
}

#[test]
public fun test_blob_certify_single_function(): system::System {
    let sk = test_utils::bls_sk_for_testing();

    // Create a new system object
    let mut system: system::System = system::new_for_testing();

    // Get some space for a few epochs
    let storage = get_storage_resource(&mut system, SIZE, 3);

    // Register a Blob
    let mut blob1 = register_default_blob(&mut system, storage, false);

    // BCS confirmation message for epoch 0 and blob id `blob_id` with intents
    let confirmation_message = messages::certified_message_bytes(EPOCH, default_blob_id());
    // Signature from private key scalar(117) on `confirmation`
    let signature = bls_min_pk_sign(&confirmation_message, &sk);
    // Set certify
    system.certify_blob(&mut blob1, signature, vector[0], confirmation_message);

    // Assert certified
    assert!(blob1.certified_epoch().is_some());

    blob1.burn();
    system
}

#[test, expected_failure(abort_code = blob::EWrongEpoch)]
public fun test_blob_certify_bad_epoch(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    let mut blob = register_default_blob(&mut system, storage, false);

    // Certify message for wrong epoch.
    let certify_message = messages::certified_blob_message_for_testing(EPOCH + 1, blob.blob_id());

    // Try to certify. Test fails here.
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    blob.burn();
    system
}

#[test, expected_failure(abort_code = blob::EInvalidBlobId)]
public fun test_blob_certify_bad_blob_id(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    let mut blob = register_default_blob(&mut system, storage, false);

    // Create certify message with wrong blob id.
    let certify_message = messages::certified_blob_message_for_testing(EPOCH, 0x42);

    // Try to certify. Test fails here.
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    blob.burn();
    system
}

#[test]
public fun test_certified_blob_message() {
    let blob_id = default_blob_id();
    let message_bytes = messages::certified_message_bytes(EPOCH, blob_id);
    let msg = messages::new_certified_message(message_bytes, EPOCH, 10);

    let message = msg.certify_blob_message();
    assert!(message.certified_blob_id() == blob_id);
}

#[test, expected_failure(abort_code = bcs::EOutOfRange)]
public fun test_certified_blob_message_too_short() {
    let mut msg_bytes = messages::certified_message_bytes(EPOCH, default_blob_id());
    // Shorten message
    let _ = msg_bytes.pop_back();
    let cert_msg = messages::new_certified_message(msg_bytes, EPOCH, 10);

    // Test fails here
    let _message = cert_msg.certify_blob_message();
}

#[test]
public fun test_blob_extend_happy_path(): system::System {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing();

    let end_epoch_1 = 3;
    let end_epoch_2 = 5;

    // Get some space for a few epochs
    let storage = get_storage_resource(&mut system, SIZE, end_epoch_1);

    // Get a longer storage period
    let mut storage_long = get_storage_resource(&mut system, SIZE, end_epoch_2);

    // Split by period
    let trailing_storage = storage_long.split_by_epoch(end_epoch_1, ctx);

    // Register a Blob
    let mut blob = register_default_blob(&mut system, storage, false);
    let certify_message = messages::certified_blob_message_for_testing(EPOCH, default_blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Check that the blob expires in the initial epoch.
    assert!(blob.storage().end_epoch() == end_epoch_1);

    // Now extend the blob
    system.extend_blob_with_resource(&mut blob, trailing_storage);

    // Check that the blob has been extended.
    assert!(blob.storage().end_epoch() == end_epoch_2);

    storage_long.destroy();
    blob.burn();
    system
}

#[test, expected_failure(abort_code = storage_resource::EIncompatibleEpochs)]
public fun test_blob_extend_bad_period(): system::System {
    let ctx = &mut tx_context::dummy();
    let mut system: system::System = system::new_for_testing();

    let end_epoch_1 = 3;
    let end_epoch_2 = 5;

    // Get some space for a few epochs
    let storage = get_storage_resource(&mut system, SIZE, end_epoch_1);

    // Get a longer storage period
    let mut storage_long = get_storage_resource(&mut system, SIZE, end_epoch_2);

    // Split by period, one epoch too late.
    let trailing_storage = storage_long.split_by_epoch(end_epoch_1 + 1, ctx);

    // Register a Blob
    let mut blob = register_default_blob(&mut system, storage, false);
    let certify_message = messages::certified_blob_message_for_testing(EPOCH, default_blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Check that the blob expires in the initial epoch.
    assert!(blob.storage().end_epoch() == end_epoch_1);

    // Now try to extend the blob. Test fails here.
    system.extend_blob_with_resource(&mut blob, trailing_storage);

    storage_long.destroy();
    blob.burn();
    system
}

#[test]
public fun test_direct_extend_happy(): system::System {
    let mut system: system::System = system::new_for_testing();
    let initial_duration = 3;
    let extension = 3;

    let storage = get_storage_resource(&mut system, SIZE, initial_duration);

    let mut blob = register_default_blob(&mut system, storage, false);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Now extend the blob with another 3 epochs
    let mut fake_coin = test_utils::mint(N_COINS, &mut tx_context::dummy());
    system.extend_blob(&mut blob, extension, &mut fake_coin);

    // Assert end epoch
    assert!(blob.storage().end_epoch() == EPOCH + initial_duration + extension);

    fake_coin.burn_for_testing();
    blob.burn();
    system
}

#[test, expected_failure(abort_code = blob::ENotCertified)]
public fun test_direct_extend_not_certified(): system::System {
    let mut system: system::System = system::new_for_testing();
    let initial_duration = 3;
    let extension = 3;

    let storage = get_storage_resource(&mut system, SIZE, initial_duration);

    let mut blob = register_default_blob(&mut system, storage, false);

    // Don't certify the blob

    // Now try to extend the blob with another 3 epochs
    let mut fake_coin = test_utils::mint(N_COINS, &mut tx_context::dummy());
    system.extend_blob(&mut blob, extension, &mut fake_coin);

    fake_coin.burn_for_testing();
    blob.burn();
    system
}

#[test, expected_failure(abort_code = blob::EResourceBounds)]
public fun test_direct_extend_expired(): system::System {
    let mut system: system::System = system::new_for_testing();
    let initial_duration = 1;
    let extension = 3;

    let storage = get_storage_resource(&mut system, SIZE, initial_duration);

    let mut blob = register_default_blob(&mut system, storage, false);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Advance the epoch
    let committee = test_utils::new_bls_committee_for_testing(1);
    let epoch_balance = system.advance_epoch(committee, epoch_params_for_testing());
    epoch_balance.destroy_for_testing();

    let mut fake_coin = test_utils::mint(N_COINS, &mut tx_context::dummy());
    // Now extend the blob with another 3 epochs. Test fails here.
    system.extend_blob(&mut blob, extension, &mut fake_coin);

    fake_coin.burn_for_testing();
    blob.burn();
    system
}

#[test, expected_failure(abort_code = system_state_inner::EInvalidEpochsAhead)]
public fun test_direct_extend_too_long(): system::System {
    let mut system: system::System = system::new_for_testing();
    let initial_duration = 3;
    let extension = MAX_EPOCHS_AHEAD;

    let storage = get_storage_resource(&mut system, SIZE, initial_duration);

    let mut blob = register_default_blob(&mut system, storage, false);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    let mut fake_coin = test_utils::mint(N_COINS, &mut tx_context::dummy());
    // Try to extend the blob with max epochs. Test fails here.
    system.extend_blob(&mut blob, extension, &mut fake_coin);

    fake_coin.burn_for_testing();
    blob.burn();
    system
}

#[test]
public fun test_delete_blob(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    // Register a deletable blob.
    let mut blob = register_default_blob(&mut system, storage, true);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Assert certified
    assert!(blob.certified_epoch().is_some());

    // Now delete the blob
    let storage = system.delete_blob(blob);

    storage.destroy();
    system
}

#[test, expected_failure(abort_code = blob::EBlobNotDeletable)]
public fun test_delete_undeletable_blob(): system::System {
    let mut system: system::System = system::new_for_testing();

    let storage = get_storage_resource(&mut system, SIZE, 3);

    // Register a non-deletable blob.
    let mut blob = register_default_blob(&mut system, storage, false);

    let certify_message = messages::certified_blob_message_for_testing(EPOCH, blob.blob_id());

    // Set certify
    blob.certify_with_certified_msg(system.epoch(), certify_message);

    // Assert certified
    assert!(blob.certified_epoch().is_some());

    // Now delete the blob. Test fails here.
    let storage = system.delete_blob(blob);

    storage.destroy();
    system
}

// === Metadata ===

#[test]
public fun test_blob_add_metadata(): system::System {
    call_function_with_default_blob!(| blob| {
        let metadata = metadata::new();
        blob.add_metadata(metadata);
        blob.insert_or_update_metadata_pair(b"key1".to_string(), b"value1".to_string());
        blob.insert_or_update_metadata_pair(b"key1".to_string(), b"value3".to_string());

        let (key, value) = blob.remove_metadata_pair(&b"key1".to_string());
        assert!(key == b"key1".to_string());
        assert!(value == b"value3".to_string());
    })
}

#[test, expected_failure(abort_code = dynamic_field::EFieldAlreadyExists)]
public fun test_blob_add_metadata_already_exists(): system::System {
    call_function_with_default_blob!(| blob | {
        let metadata1 = metadata::new();
        blob.add_metadata(metadata1);
        let metadata2 = metadata::new();

        // The metadata field already exists. Test fails here.
        blob.add_metadata(metadata2);
    })
}

#[test, expected_failure(abort_code = dynamic_field::EFieldDoesNotExist)]
public fun test_blob_take_metadata_nonexistent(): system::System {
    call_function_with_default_blob!(| blob | {
        // Try to take the metadata from a blob without metadata. Test fails here.
        blob.take_metadata();
    })
}

#[test, expected_failure(abort_code = dynamic_field::EFieldDoesNotExist)]
public fun test_blob_insert_metadata_pair_nonexistent(): system::System {
    call_function_with_default_blob!(| blob | {
        // Try to insert metadata into a blob without metadata. Test fails here.
        blob.insert_or_update_metadata_pair(b"key1".to_string(), b"value1".to_string());
    })
}

#[test, expected_failure(abort_code = dynamic_field::EFieldDoesNotExist)]
public fun test_blob_remove_metadata_pair_nonexistent(): system::System {
    call_function_with_default_blob!(| blob | {
        // Try to remove metadata from a blob without metadata. Test fails here.
        blob.remove_metadata_pair(&b"key1".to_string());
    })
}

// === Helper functions ===

fun get_storage_resource(system: &mut System, unencoded_size: u64, epochs_ahead: u32): Storage {
    let ctx = &mut tx_context::dummy();
    let mut fake_coin = test_utils::mint(N_COINS, ctx);
    let storage_size = encoding::encoded_blob_length(unencoded_size, RED_STUFF, system.n_shards());
    let storage = system.reserve_space(
        storage_size,
        epochs_ahead,
        &mut fake_coin,
        ctx,
    );
    fake_coin.burn_for_testing();
    storage
}

fun register_default_blob(system: &mut System, storage: Storage, deletable: bool): Blob {
    let ctx = &mut tx_context::dummy();
    let mut fake_coin = test_utils::mint(N_COINS, ctx);
    // Register a Blob
    let blob_id = blob::derive_blob_id(ROOT_HASH, RED_STUFF, SIZE);
    let blob = system.register_blob(
        storage,
        blob_id,
        ROOT_HASH,
        SIZE,
        RED_STUFF,
        deletable,
        &mut fake_coin,
        ctx,
    );

    fake_coin.burn_for_testing();
    blob
}

fun default_blob_id(): u256 {
    blob::derive_blob_id(ROOT_HASH, RED_STUFF, SIZE)
}

/// Utiliy macro that calls the given function on a new Blob.
///
/// Creates the system, registers the default blob, and calls the given function with the blob.
/// Finally, it destroys the blob and returns the system.
macro fun call_function_with_default_blob($f: |&mut Blob|->()): system::System {
    let mut system: system::System = system::new_for_testing();
    let storage = get_storage_resource(&mut system, SIZE, 3);
    let mut blob = register_default_blob(&mut system, storage, false);

    // Call the function with the blob.
    $f(&mut blob);

    // Cleanup.
    blob.burn();
    system
}
