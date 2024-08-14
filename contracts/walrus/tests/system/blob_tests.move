// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::blob_tests;

use sui::{bcs, coin, sui::SUI};
use walrus::{blob, messages, storage_resource::{split_by_epoch, destroy}, system};

const RED_STUFF: u8 = 0;

#[test]
public fun test_blob_register_happy_path(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test, expected_failure(abort_code = blob::EResourceSize)]
public fun test_blob_insufficient_space(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs - TOO LITTLE SPACE
    let storage = system::reserve_space(
        &mut system,
        5000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test]
public fun test_blob_certify_happy_path(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    let certify_message = messages::certified_blob_message_for_testing(0, blob_id);

    // Set certify
    blob1.certify_with_certified_msg(system.epoch(), certify_message);

    // Assert certified
    assert!(option::is_some(blob::certified_epoch(&blob1)), 0);

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test]
public fun test_blob_certify_single_function(): system::System {
    let mut ctx = tx_context::dummy();

    // Derive blob ID and root_hash from bytes
    let root_hash_vec = vector[
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
    ];

    let mut encode = bcs::new(root_hash_vec);
    let root_hash = bcs::peel_u256(&mut encode);

    let blob_id_vec = vector[
        119,
        174,
        25,
        167,
        128,
        57,
        96,
        1,
        163,
        56,
        61,
        132,
        191,
        35,
        44,
        18,
        231,
        224,
        79,
        178,
        85,
        51,
        69,
        53,
        214,
        95,
        198,
        203,
        56,
        221,
        111,
        83,
    ];

    let mut encode = bcs::new(blob_id_vec);
    let blob_id = bcs::peel_u256(&mut encode);

    // Derive and check blob ID
    let blob_id_bis = blob::derive_blob_id(root_hash, RED_STUFF, 10000);
    assert!(blob_id == blob_id_bis, 0);

    // BCS confirmation message for epoch 0 and blob id `blob_id` with intents
    let confirmation = vector[
        1,
        0,
        3,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        119,
        174,
        25,
        167,
        128,
        57,
        96,
        1,
        163,
        56,
        61,
        132,
        191,
        35,
        44,
        18,
        231,
        224,
        79,
        178,
        85,
        51,
        69,
        53,
        214,
        95,
        198,
        203,
        56,
        221,
        111,
        83,
    ];
    // Signature from private key scalar(117) on `confirmation`
    let signature = vector[
        184,
        138,
        78,
        92,
        221,
        170,
        180,
        107,
        75,
        249,
        222,
        177,
        183,
        25,
        107,
        214,
        237,
        214,
        213,
        12,
        239,
        65,
        88,
        112,
        65,
        229,
        225,
        23,
        62,
        158,
        144,
        67,
        206,
        37,
        148,
        1,
        69,
        64,
        190,
        180,
        121,
        153,
        39,
        149,
        41,
        2,
        112,
        69,
        23,
        68,
        69,
        159,
        192,
        116,
        41,
        113,
        21,
        116,
        123,
        169,
        204,
        165,
        232,
        70,
        146,
        1,
        175,
        70,
        126,
        14,
        20,
        206,
        113,
        234,
        141,
        195,
        218,
        52,
        172,
        56,
        78,
        168,
        114,
        213,
        241,
        83,
        188,
        215,
        123,
        191,
        111,
        136,
        26,
        193,
        60,
        246,
    ];

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        root_hash,
        10000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    // Set certify
    system.certify_blob(&mut blob1, signature, vector[0], confirmation);

    // Assert certified
    assert!(option::is_some(blob::certified_epoch(&blob1)), 0);

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test, expected_failure(abort_code = blob::EWrongEpoch)]
public fun test_blob_certify_bad_epoch(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    // Set INCORRECT EPOCH TO 1
    let certify_message = messages::certified_blob_message_for_testing(1, blob_id);

    // Set certify
    blob1.certify_with_certified_msg(system.epoch(), certify_message);

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test, expected_failure(abort_code = blob::EInvalidBlobId)]
public fun test_blob_certify_bad_blob_id(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(1000000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );

    // DIFFERENT blob id
    let certify_message = messages::certified_blob_message_for_testing(0, 0xFFF);

    // Set certify
    blob1.certify_with_certified_msg(system.epoch(), certify_message);

    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test]
public fun test_certified_blob_message() {
    let msg = messages::certified_message_for_testing(
        1,
        0,
        10,
        100,
        vector[
            0xAA,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
    );

    let message = msg.certify_blob_message();
    assert!(message.certified_blob_id() == 0xAA, 0);
}

#[test, expected_failure]
public fun test_certified_blob_message_too_short() {
    let msg = messages::certified_message_for_testing(
        1,
        0,
        10,
        100,
        vector[
            0xAA,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
    );

    let message = msg.certify_blob_message();
    assert!(message.certified_blob_id() == 0xAA, 0);
}

#[test]
public fun test_blob_extend_happy_path(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Get a longer storage period
    let mut storage_long = system::reserve_space(
        &mut system,
        1_000_000,
        5,
        &mut fake_coin,
        &mut ctx,
    );

    // Split by period
    let trailing_storage = split_by_epoch(&mut storage_long, 3, &mut ctx);

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );
    let certify_message = messages::certified_blob_message_for_testing(0, blob_id);

    // Set certify
    blob1.certify_with_certified_msg(system.epoch(), certify_message);

    // Now extend the blob
    system.extend_blob_with_resource(&mut blob1, trailing_storage);

    // Assert certified
    assert!(option::is_some(blob::certified_epoch(&blob1)), 0);

    destroy(storage_long);
    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}

#[test, expected_failure]
public fun test_blob_extend_bad_period(): system::System {
    let mut ctx = tx_context::dummy();

    // A test coin.
    let mut fake_coin = coin::mint_for_testing<SUI>(100000000, &mut ctx);

    // Create a new system object
    let mut system: system::System = system::new_for_testing(&mut ctx);

    // Get some space for a few epochs
    let storage = system::reserve_space(
        &mut system,
        1_000_000,
        3,
        &mut fake_coin,
        &mut ctx,
    );

    // Get a longer storage period
    let mut storage_long = system::reserve_space(
        &mut system,
        1_000_000,
        5,
        &mut fake_coin,
        &mut ctx,
    );

    // Split by period
    let trailing_storage = split_by_epoch(&mut storage_long, 4, &mut ctx);

    // Register a Blob
    let blob_id = blob::derive_blob_id(0xABC, RED_STUFF, 5000);
    let mut blob1 = system.register_blob(
        storage,
        blob_id,
        0xABC,
        5000,
        RED_STUFF,
        &mut fake_coin,
        &mut ctx,
    );
    let certify_message = messages::certified_blob_message_for_testing(0, 0xABC);

    // Set certify
    blob1.certify_with_certified_msg(system.epoch(), certify_message);

    // Now extend the blob // ITS THE WRONG PERIOD
    system.extend_blob_with_resource(&mut blob1, trailing_storage);

    destroy(storage_long);
    coin::burn_for_testing<SUI>(fake_coin);
    blob1.burn();
    system
}
