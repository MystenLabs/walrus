// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::e2e_test  {

    use sui::tx_context::{Self, TxContext};
    use sui::object::{Self, UID};
    use blob_store::committee::{Self, CreateCommitteeCap};
    use blob_store::system;
    use blob_store::storage_node;
    use std::string;
    use sui::sui::SUI;
    use sui::transfer;

    struct CommitteeCapHolder has key, store  {
        id: UID,
        cap: CreateCommitteeCap,
    }

    // NOTE: the function below is means to be used as part of a PTB to construct a committee
    //       The PTB contains a number of `create_storage_node_info` invocations, then
    //       a `MakeMoveVec` invocation, and finally a `make_committee` invocation.

    /// Create a committee given a capability and a list of storage nodes
    public fun make_committee(
        cap : &CommitteeCapHolder,
        epoch: u64,
        storage_nodes: vector<storage_node::StorageNodeInfo>)
        : committee::Committee {

        committee::create_committee(
            &cap.cap,
            epoch,
            storage_nodes,
        )
    }

    fun init(ctx: &mut TxContext) {

        // Create a committee caps
        let committee_cap = committee::create_committee_cap();

        // TODO: eventually do not make a n initial committee or system object and fully
        //       rely on a PTB to create it.

        // Pk corresponding to secret key scalar(117)
        let pub_key_bytes = vector[149, 234, 204, 58, 220, 9, 200, 39, 89, 63, 88, 30, 142, 45,
            224, 104, 191, 76, 245, 208, 192, 235, 41, 229, 55, 47, 13, 35, 54, 71, 136, 238, 15,
            155, 235, 17, 44, 138, 126, 156, 47, 12, 114, 4, 51, 112, 92, 240];

        // The one storage node
        let storage_node = storage_node::create_storage_node_info(
            string::utf8(b"Test0"),
            string::utf8(b"127.0.0.1:8080"),
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
        system::share_new<SUI>(
            committee_0,
            1000000000,
            10,
            ctx,
        );

        // We send the wrapped cap to the creator of the package
        transfer::public_transfer(
            CommitteeCapHolder {
                id:  object::new(ctx),
                cap: committee_cap,
            },
            tx_context::sender(ctx),
        );
    }
}
