// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[test_only]
module blob_store::storage_resource_tests {
    use sui::tx_context;
    use blob_store::storage_resource::{
        fuse,
        split_by_epoch,
        split_by_size,
        create_for_test,
        destroy_for_test,
        start_epoch,
        end_epoch,
        storage_size,
        EInvalidAmount,
        EInvalidEpoch,
        EIncompatibleAmount,
        EIncompatibleEpochs,
    };

    struct TESTTAG has drop {}

    #[test]
    public fun test_split_epoch(){
        let ctx = &mut tx_context::dummy();
        let storage_amount = 5_000_000;
        let storage = create_for_test<TESTTAG>(0, 10, storage_amount, ctx);
        let new_storage = split_by_epoch(&mut storage, 7, ctx);
        assert!(start_epoch(&storage) == 0 && end_epoch(&storage) == 7
            && start_epoch(&new_storage) == 7 && end_epoch(&new_storage) == 10, 0);
        assert!(
            storage_size(&storage) == storage_amount
            && storage_size(&new_storage) == storage_amount,
            0
        );
        destroy_for_test(storage);
        destroy_for_test(new_storage);
    }

    #[test]
    public fun test_split_size(){
        let ctx = &mut tx_context::dummy();
        let storage = create_for_test<TESTTAG>(0, 10, 5_000_000, ctx);
        let new_storage = split_by_size(&mut storage, 1_000_000, ctx);
        assert!(start_epoch(&storage) == 0 && end_epoch(&storage) == 10
            && start_epoch(&new_storage) == 0 && end_epoch(&new_storage) == 10, 0);
        assert!(storage_size(&storage) == 1_000_000 && storage_size(&new_storage) == 4_000_000, 0);
        destroy_for_test(storage);
        destroy_for_test(new_storage);
    }

    #[test]
    #[expected_failure(abort_code=EInvalidAmount)]
    public fun test_split_size_invalid(){
        let ctx = &mut tx_context::dummy();
        let storage = create_for_test<TESTTAG>(0, 10, 5_000_000, ctx);
        let new_storage = split_by_size(&mut storage, 4_500_000, ctx);
        destroy_for_test(storage);
        destroy_for_test(new_storage);
    }

    #[test]
    #[expected_failure(abort_code=EInvalidEpoch)]
    public fun test_split_epoch_invalid_end(){
        let ctx = &mut tx_context::dummy();
        let storage = create_for_test<TESTTAG>(0, 10, 5_000_000, ctx);
        let new_storage = split_by_epoch(&mut storage, 10, ctx);
        destroy_for_test(storage);
        destroy_for_test(new_storage);
    }

    #[test]
    #[expected_failure(abort_code=EInvalidEpoch)]
    public fun test_split_epoch_invalid_start(){
        let ctx = &mut tx_context::dummy();
        let storage = create_for_test<TESTTAG>(0, 10, 5_000_000, ctx);
        let new_storage = split_by_epoch(&mut storage, 0, ctx);
        destroy_for_test(storage);
        destroy_for_test(new_storage);
    }

    #[test]
    public fun test_fuse_size(){
        let ctx = &mut tx_context::dummy();
        let first = create_for_test<TESTTAG>(0, 10, 1_000_000, ctx);
        let second = create_for_test<TESTTAG>(0, 10, 2_000_000, ctx);
        fuse(&mut first, second);
        assert!(start_epoch(&first) == 0 && end_epoch(&first) == 10, 0);
        assert!(storage_size(&first) == 3_000_000, 0);
        destroy_for_test(first);
    }

    #[test]
    public fun test_fuse_epochs(){
        let ctx = &mut tx_context::dummy();
        let first = create_for_test<TESTTAG>(0, 5, 1_000_000, ctx);
        let second = create_for_test<TESTTAG>(5, 10, 1_000_000, ctx);
        // list the `earlier` resource first
        fuse(&mut first, second);
        assert!(start_epoch(&first) == 0 && end_epoch(&first) == 10, 0);
        assert!(storage_size(&first) == 1_000_000, 0);

        let second = create_for_test<TESTTAG>(10, 15, 1_000_000, ctx);
        // list the `latter` resource first
        fuse(&mut second, first);
        assert!(start_epoch(&second) == 0 && end_epoch(&second) == 15, 0);
        assert!(storage_size(&second) == 1_000_000, 0);
        destroy_for_test(second);
    }

    #[test]
    #[expected_failure(abort_code=EIncompatibleAmount)]
    public fun test_fuse_incompatible_size(){
        let ctx = &mut tx_context::dummy();
        let first = create_for_test<TESTTAG>(0, 5, 1_000_000, ctx);
        let second = create_for_test<TESTTAG>(5, 10, 2_000_000, ctx);
        fuse(&mut first, second);
        destroy_for_test(first);
    }

    #[test]
    #[expected_failure(abort_code=EIncompatibleEpochs)]
    public fun test_fuse_incompatible_epochs(){
        let ctx = &mut tx_context::dummy();
        let first = create_for_test<TESTTAG>(0, 6, 1_000_000, ctx);
        let second = create_for_test<TESTTAG>(5, 10, 1_000_000, ctx);
        fuse(&mut first, second);
        destroy_for_test(first);
    }

}
