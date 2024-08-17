// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module walrus::invalid_tests;

use walrus::{committee, messages, system};

const BLOB_ID : u256 = 0xabababababababababababababababababababababababababababababababab;

// BCS confirmation message for epoch 5 and blob id `blob_id` with intents
const INVALID_MESSAGE: vector<u8> = vector[2, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0,
    171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171,
    171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171];

// Signature from private key scalar(117) on `INVALID_MESSAGE`
const MESSAGE_SIGNATURE: vector<u8> = vector[
    143, 92, 248, 128, 87, 79, 148, 183, 217, 204, 80, 23, 165, 20, 177, 244, 195, 58, 211,
    68, 96, 54, 23, 17, 187, 131, 69, 35, 243, 61, 209, 23, 11, 75, 236, 235, 199, 245, 53,
    10, 120, 47, 152, 39, 205, 152, 188, 230, 12, 213, 35, 133, 121, 27, 238, 80, 93, 35,
    241, 26, 55, 151, 38, 190, 131, 149, 149, 89, 134, 115, 85, 8, 133, 11, 220, 82, 100,
    14, 214, 146, 147, 200, 192, 155, 181, 143, 199, 38, 202, 125, 25, 22, 246, 117, 30, 82
    ];

#[test]
public fun test_invalid_blob_ok(): committee::Committee {
    // Create a new committee
    let committee = committee::committee_for_testing(5);

    let certified_message = committee::verify_quorum_in_epoch(
        &committee,
        MESSAGE_SIGNATURE,
        vector[0],
        INVALID_MESSAGE,
    );

    // Now check this is a invalid blob message
    let invalid_blob_msg = certified_message.invalid_blob_id_message();
    assert!(invalid_blob_msg.invalid_blob_id() == BLOB_ID, 0);

    committee
}

#[test]
public fun test_invalidate_happy(): system::System {
    let mut ctx = tx_context::dummy();

    // Create a new system object
    let mut system = system::new_for_testing(&mut ctx);

    let mut epoch = 0;

    loop {
        epoch = epoch + 1;
        let committee = committee::committee_for_testing(epoch);
        let epoch_balance = system.advance_epoch(committee, 1000000000, 5, 1);
        epoch_balance.destroy_for_testing();

        if (epoch == 5) {
            assert!(system.epoch() == 5);
            break
        }
    };

    // Now check this is a invalid blob message
    let blob_id = system::invalidate_blob_id(
        &system,
        MESSAGE_SIGNATURE,
        vector[0],
        INVALID_MESSAGE,
    );

    assert!(blob_id == BLOB_ID, 0);

    system
}

#[test, expected_failure(abort_code = messages::EIncorrectEpoch)]
public fun test_system_invalid_id_wrong_epoch(): system::System {
    let mut ctx = tx_context::dummy();

    // Create a new system object
    let mut system = system::new_for_testing(&mut ctx);

    let mut epoch = 0;

    loop {
        epoch = epoch + 1;
        let committee = committee::committee_for_testing(epoch);
        let epoch_balance = system.advance_epoch(committee, 1000000000, 5, 1);
        epoch_balance.destroy_for_testing();

        // wrong epoch
        if (epoch == 6) {
            break
        }
    };

    // Now check this is a invalid blob message
    // Test should fail here
    let _blob_id = system::invalidate_blob_id(
        &system,
        MESSAGE_SIGNATURE,
        vector[0],
        INVALID_MESSAGE,
    );

    system
}
