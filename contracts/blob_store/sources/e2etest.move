// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::e2e_test  {

    use sui::tx_context::TxContext;
    use blob_store::committee;
    use blob_store::system;
    use std::string;
    use sui::sui::SUI;

    struct TESTTAG has drop {}

    fun init(ctx: &mut TxContext) {

        // Create a committee caps
        let committee_cap = committee::create_committee_cap(TESTTAG {});

        // Pk corresponding to secret key scalar(117)
        let pub_key_bytes = vector[149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45,
            224, 104, 191, 76, 245, 208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15,
            155, 235, 17, 44, 138, 126, 156, 47, 12, 114, 4, 51, 112, 92, 240];

        // The one storage node
        let storage_node = committee::create_storage_node_info(
            string::utf8(b"Test0"),
            string::utf8(b"Address0"),
            pub_key_bytes,
            vector[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        );

        // Create a new committee at zero
        let committee_0 = committee::create_committee(
            &committee_cap,
            0,
            vector[storage_node]
        );

        // Share the new blob store as a shared object
        system::share_new<TESTTAG, SUI>(
            &TESTTAG {},
            committee_0,
            1000000000,
            10,
            ctx,
        )

        // We drop the committee cap

    }

}
