// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::blob_tests {

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

    struct TESTTAG has drop {}
    struct TESTWAL has store, drop {}

    #[test]
    public fun test_blob_register_happy_path() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
            &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test, expected_failure(abort_code=blob::ERROR_RESOURCE_SIZE)]
    public fun test_blob_insufficient_space() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs - TOO LITTLE SPACE
        let (storage, fake_coin) = system::reserve_space(
            &mut system, 100, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test]
    public fun test_blob_certify_happy_path() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xABC);

        // Set certify
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        // Assert certified
        assert!(option::is_some(blob::certified(&blob1)), 0);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test]
    public fun test_blob_certify_single_function() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag1 = TESTTAG{};
        let tag2 = TESTTAG{};

        let blob_id = 0x0807060504030201080706050403020108070605040302010807060504030201;

        // BCS confirmation message for epoch 0 and blob id `blob_id` with intents
        let confirmation = vector[
            1, 0, 3, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 2, 3, 4, 5,
            6, 7, 8, 1, 2, 3, 4, 5,
            6, 7, 8, 1, 2, 3, 4, 5,
            6, 7, 8, 1, 2, 3, 4, 5,
            6, 7, 8
        ];
        // Signature from private key scalar(117) on `confirmation`
        let signature = vector[
            128, 62, 224, 78, 156, 217, 159, 1,
            190, 168, 80, 144, 85, 79, 82, 23,
            4, 146, 248, 165, 127, 252, 137, 171,
            8, 105, 155, 147, 216, 36, 206, 221,
            222, 241, 110, 27, 208, 230, 121, 167,
            185, 195, 150, 184, 164, 123, 203, 200,
            15, 156, 65, 247, 115, 213, 30, 219,
            164, 24, 134, 245, 219, 234, 195, 129,
            150, 162, 237, 124, 51, 243, 151, 186,
            203, 123, 253, 2, 121, 17, 242, 153,
            225, 16, 110, 117, 172, 163, 55, 147,
            192, 128, 223, 68, 206, 144, 103, 11
        ];

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create storage node
        // Pk corresponding to secret key scalar(117)
        let public_key = vector[149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45,
            224, 104, 191, 76, 245, 208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15,
            155, 235, 17, 44, 138, 126, 156, 47, 12, 114, 4, 51, 112, 92, 240];
        let storage_node = storage_node::create_storage_node_info(
            string::utf8(b"node"),
            string::utf8(b"127.0.0.1"),
            public_key,
            vector[0, 1, 2, 3, 4, 5]
        );

        // Create a new committee
        let cap = committee::create_committee_cap(tag1);
        let committee = committee::create_committee(&cap, 0, vector[storage_node]);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
            &mut system,
            10000,
            3,
            fake_coin,
            &mut ctx
        );

        // Register a Blob
        let blob1 = blob::register(&system, storage, blob_id, 10000, 0x1, &mut ctx);

        // Set certify
        blob::certify(&system, &mut blob1, signature, vector[0], confirmation);

        // Assert certified
        assert!(option::is_some(blob::certified(&blob1)), 0);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }


    #[test, expected_failure(abort_code=blob::ERROR_WRONG_EPOCH)]
    public fun test_blob_certify_bad_epoch() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        // Set INCORRECT EPOCH TO 1
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(1, 0xABC);

        // Set certify
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test, expected_failure(abort_code=blob::ERROR_INVALID_BLOB_ID)]
    public fun test_blob_certify_bad_blob_id() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        // DIFFERENT blob id
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xFFF);

        // Set certify
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test, expected_failure(abort_code=blob::ERROR_RESOURCE_BOUNDS)]
    public fun test_blob_certify_past_epoch() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);


        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 2
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(2);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 3
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(3);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);


        // Set certify -- EPOCH BEYOND RESOURCE BOUND
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(3, 0xABC);
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test]
    public fun test_blob_happy_destroy() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        // Set certify
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xABC);
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 2
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(2);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 3
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(3);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Destroy the blob
        blob::destroy_blob(&system, blob1);

        coin::burn_for_testing(fake_coin);
        system
    }

    #[test, expected_failure(abort_code=blob::ERROR_RESOURCE_BOUNDS)]
    public fun test_blob_unhappy_destroy() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        // Destroy the blob
        blob::destroy_blob(&system, blob1);

        coin::burn_for_testing(fake_coin);
        system
    }

    #[test]
    public fun test_certified_blob_message() {
        let msg = committee::certified_message_for_testing<TESTTAG>(
            1, 0, 10, 100, vector[
                0xAA, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
            ]);

            let message = blob::certify_blob_message(msg);
            assert!(blob::message_blob_id(&message) == 0xAA, 0);
    }


    #[test, expected_failure]
    public fun test_certified_blob_message_too_short() {
        let msg = committee::certified_message_for_testing<TESTTAG>(
            1, 0, 10, 100, vector[
                0xAA, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0,
            ]);

            let message = blob::certify_blob_message(msg);
            assert!(blob::message_blob_id(&message) == 0xAA, 0);
    }

    #[test]
    public fun test_blob_extend_happy_path() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Get a longer storage period
        let (storage_long, fake_coin) = system::reserve_space(
                &mut system, 10000, 5, fake_coin, &mut ctx);

        // Split by period
        let trailing_storage =
            split_by_epoch(&mut storage_long, 3, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xABC);

        // Set certify
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        // Now extend the blob
        blob::extend(&system, &mut blob1, trailing_storage);

        // Assert certified
        assert!(option::is_some(blob::certified(&blob1)), 0);

        destroy(storage_long);
        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test, expected_failure]
    public fun test_blob_extend_bad_period() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Get a longer storage period
        let (storage_long, fake_coin) = system::reserve_space(
                &mut system, 10000, 5, fake_coin, &mut ctx);

        // Split by period
        let trailing_storage =
            split_by_epoch(&mut storage_long, 4, &mut ctx);

        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xABC);

        // Set certify
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        // Now extend the blob // ITS THE WRONG PERIOD
        blob::extend(&system, &mut blob1, trailing_storage);

        destroy(storage_long);
        coin::burn_for_testing(fake_coin);
        blob::drop_for_testing(blob1);
        system
    }

    #[test,expected_failure(abort_code=blob::ERROR_RESOURCE_BOUNDS)]
    public fun test_blob_unhappy_extend() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100000000, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000000000, 5, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(
                &mut system, 10000, 3, fake_coin, &mut ctx);

        // Get a longer storage period
        let (storage_long, fake_coin) = system::reserve_space(
                &mut system, 10000, 5, fake_coin, &mut ctx);

        // Split by period
        let trailing_storage =
            split_by_epoch(&mut storage_long, 3, &mut ctx);


        // Register a Blob
        let blob1 = blob::register(&system, storage, 0xABC, 5000, 0x1, &mut ctx);

        // Set certify
        let certify_message = blob::certified_blob_message_for_testing<TESTTAG>(0, 0xABC);
        blob::certify_with_certified_msg(&system, certify_message, &mut blob1);

        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 2
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(2);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Advance epoch -- to epoch 3
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(3);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        sa::burn_for_testing(epoch_accounts);

        // Try to extend after expiry.

        // Now extend the blo
        blob::extend(&system, &mut blob1, trailing_storage);

        // Destroy the blob
        blob::destroy_blob(&system, blob1);

        destroy(storage_long);
        coin::burn_for_testing(fake_coin);
        system
    }

}
