// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::epoch_change_tests {

    use sui::tx_context::{Self};
    use sui::coin;
    use sui::balance;

    use blob_store::committee;
    use blob_store::system;
    use blob_store::storage_accounting as sa;
    use blob_store::storage_resource as sr;

    struct TESTTAG has drop {}
    struct TESTWAL has store, drop {}

        // ------------- TESTS --------------------

    #[test]
    public fun test_use_system() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000, 2, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(&mut system, 10, 3, fake_coin, &mut ctx);
        sr::destroy(storage);

        // Check things about the system
        assert!(system::epoch(&system) == 0, 0);

        // The value of the coin should be 100 - 60
        assert!(coin::value(&fake_coin) == 40, 0);

        // Space is reduced by 10
        assert!(system::used_capacity_size(&system) == 10, 0);

        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        assert!(balance::value(sa::rewards_to_distribute(&mut epoch_accounts)) == 20, 0);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(&mut system, 5, 1, fake_coin, &mut ctx);
        sr::destroy(storage);
        // The value of the coin should be 40 - 3 x 5
        assert!(coin::value(&fake_coin) == 25, 0);
        sa::burn_for_testing(epoch_accounts);

        assert!(system::used_capacity_size(&system) == 15, 0);

        // Advance epoch -- to epoch 2
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(2);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        assert!(balance::value(sa::rewards_to_distribute(&mut epoch_accounts)) == 35, 0);
        sa::burn_for_testing(epoch_accounts);

        assert!(system::used_capacity_size(&system) == 10, 0);

        // Advance epoch -- to epoch 3
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(3);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        assert!(balance::value(sa::rewards_to_distribute(&mut epoch_accounts)) == 20, 0);
        sa::burn_for_testing(epoch_accounts);

        // check all space is reclaimed
        assert!(system::used_capacity_size(&system) == 0, 0);

        // Advance epoch -- to epoch 4
        system::set_done_for_testing(&mut system);
        let committee = committee::committee_for_testing(4);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);
        assert!(balance::value(sa::rewards_to_distribute(&mut epoch_accounts)) == 0, 0);
        sa::burn_for_testing(epoch_accounts);

        // check all space is reclaimed
        assert!(system::used_capacity_size(&system) == 0, 0);

        coin::burn_for_testing(fake_coin);

        system
    }

    #[test, expected_failure(abort_code=system::ERROR_SYNC_EPOCH_CHANGE)]
    public fun test_move_sync_err_system() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000, 2, &mut ctx);

        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts1 = system::next_epoch(&mut system, committee, 1000, 3);

        // Advance epoch -- to epoch 2
        let committee = committee::committee_for_testing(2);
        // FAIL HERE BECAUSE WE ARE IN SYNC MODE NOT DONE!
        let epoch_accounts2 = system::next_epoch(&mut system, committee, 1000, 3);


        coin::burn_for_testing(fake_coin);
        sa::burn_for_testing(epoch_accounts1);
        sa::burn_for_testing(epoch_accounts2);

        system
    }


    #[test, expected_failure(abort_code=system::ERROR_STORAGE_EXCEEDED)]
    public fun test_fail_capacity_system() : system::System<TESTTAG, TESTWAL> {

        let ctx = tx_context::dummy();
        let tag2 = TESTTAG{};

        // A test coin.
        let fake_coin = coin::mint_for_testing<TESTWAL>(100, &mut ctx);

        // Create a new committee
        let committee = committee::committee_for_testing(0);

        // Create a new system object
        let system : system::System<TESTTAG,TESTWAL> = system::new(&tag2, committee,
            1000, 2, &mut ctx);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(&mut system, 10, 3, fake_coin, &mut ctx);
        sr::destroy(storage);

        // Advance epoch -- to epoch 1
        let committee = committee::committee_for_testing(1);
        let epoch_accounts = system::next_epoch(&mut system, committee, 1000, 3);

        // Get some space for a few epochs
        let (storage, fake_coin) = system::reserve_space(&mut system, 995, 1, fake_coin, &mut ctx);
        sr::destroy(storage);
        // The value of the coin should be 40 - 3 x 5
        sa::burn_for_testing(epoch_accounts);


        coin::burn_for_testing(fake_coin);

        system
    }

}
