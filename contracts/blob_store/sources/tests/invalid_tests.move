// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::invalid_tests {

    use sui::tx_context::{Self};
    use sui::coin;

    use std::option;
    use std::string;

    use blob_store::committee;
    use blob_store::system;
    use blob_store::storage_accounting as sa;
    use blob_store::blob;
    use blob_store::storage_node;

    use blob_store::storage_resource::{
        split_by_epoch,
        destroy};

    #[test]
    public fun test_invalid_blob_ok() : committee::Committee {

        let mut ctx = tx_context::dummy();

        let blob_id : u256 = 0xabababababababababababababababababababababababababababababababab;

        // BCS confirmation message for epoch 0 and blob id `blob_id` with intents
        let invalid_message : vector<u8> = vector[2, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0,
            171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171,
            171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171];

        // Signature from private key scalar(117) on `confirmation`
        let invalid_message_signature : vector<u8> = vector[
            143, 92, 248, 128, 87, 79, 148, 183, 217, 204, 80, 23, 165, 20, 177, 244, 195, 58, 211,
            68, 96, 54, 23, 17, 187, 131, 69, 35, 243, 61, 209, 23, 11, 75, 236, 235, 199, 245, 53,
            10, 120, 47, 152, 39, 205, 152, 188, 230, 12, 213, 35, 133, 121, 27, 238, 80, 93, 35,
            241, 26, 55, 151, 38, 190, 131, 149, 149, 89, 134, 115, 85, 8, 133, 11, 220, 82, 100,
            14, 214, 146, 147, 200, 192, 155, 181, 143, 199, 38, 202, 125, 25, 22, 246, 117, 30, 82
            ];

        // Create storage node
        // Pk corresponding to secret key scalar(117)
        let public_key : vector<u8> = vector[
            149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45,
            224, 104, 191, 76, 245, 208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15,
            155, 235, 17, 44, 138, 126, 156, 47, 12, 114, 4, 51, 112, 92, 240];
        let storage_node = storage_node::create_storage_node_info(
            string::utf8(b"node"),
            string::utf8(b"127.0.0.1"),
            public_key,
            vector[0, 1, 2, 3, 4, 5]
        );

        // Create a new committee
        let cap = committee::create_committee_cap_for_tests();
        let committee = committee::create_committee(&cap, 5, vector[storage_node]);

        let certified_message = committee::verify_quorum_in_epoch(
            &committee,
            invalid_message_signature,
            vector[0],
            invalid_message,);

        // Now check this is a invalid blob message
        let invalid_blob = system::invalid_blob_id_message(certified_message);
        assert(system::invalid_blob_id(&invalid_blob) == blob_id, 0);

        committee
    }

    #[test]
    #[expected_failure]
    public fun test_invalid_blob_bad_epoch() : committee::Committee {

        let mut ctx = tx_context::dummy();

        let blob_id : u256 = 0xabababababababababababababababababababababababababababababababab;

        // BCS confirmation message for epoch 0 and blob id `blob_id` with intents
        let invalid_message : vector<u8> = vector[2, 0, 3, 5, 0, 0, 0, 0, 0, 0, 0,
            171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171,
            171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171];

        // Signature from private key scalar(117) on `confirmation`
        let invalid_message_signature : vector<u8> = vector[
            143, 92, 248, 128, 87, 79, 148, 183, 217, 204, 80, 23, 165, 20, 177, 244, 195, 58, 211,
            68, 96, 54, 23, 17, 187, 131, 69, 35, 243, 61, 209, 23, 11, 75, 236, 235, 199, 245, 53,
            10, 120, 47, 152, 39, 205, 152, 188, 230, 12, 213, 35, 133, 121, 27, 238, 80, 93, 35,
            241, 26, 55, 151, 38, 190, 131, 149, 149, 89, 134, 115, 85, 8, 133, 11, 220, 82, 100,
            14, 214, 146, 147, 200, 192, 155, 181, 143, 199, 38, 202, 125, 25, 22, 246, 117, 30, 82
            ];

        // Create storage node
        // Pk corresponding to secret key scalar(117)
        let public_key : vector<u8> = vector[
            149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45,
            224, 104, 191, 76, 245, 208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15,
            155, 235, 17, 44, 138, 126, 156, 47, 12, 114, 4, 51, 112, 92, 240];
        let storage_node = storage_node::create_storage_node_info(
            string::utf8(b"node"),
            string::utf8(b"127.0.0.1"),
            public_key,
            vector[0, 1, 2, 3, 4, 5]
        );

        // Create a new committee
        let cap = committee::create_committee_cap_for_tests();
        let committee = committee::create_committee(&cap, 999, vector[storage_node]);

        let certified_message = committee::verify_quorum_in_epoch(
            &committee,
            invalid_message_signature,
            vector[0],
            invalid_message,);

        // Now check this is a invalid blob message
        let invalid_blob = system::invalid_blob_id_message(certified_message);
        assert(system::invalid_blob_id(&invalid_blob) == blob_id, 0);

        committee
    }


}