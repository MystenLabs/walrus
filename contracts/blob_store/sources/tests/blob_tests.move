// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::blob_tests {

    use sui::tx_context::{Self};
    use sui::coin;

    use blob_store::committee;
    use blob_store::system;
    use blob_store::storage_accounting as sa;
    use blob_store::blob;

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
        blob::certify(&system, certify_message, &mut blob1);

        // Assert certified
        assert!(blob::certified(&blob1), 0);

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
        blob::certify(&system, certify_message, &mut blob1);

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
        blob::certify(&system, certify_message, &mut blob1);

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
        blob::certify(&system, certify_message, &mut blob1);

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
        blob::certify(&system, certify_message, &mut blob1);

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

}
