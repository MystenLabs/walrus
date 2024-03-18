// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::bls_aggregate {

    use sui::group_ops::Self;
    use sui::bls12381::{Self, bls12381_min_pk_verify};
    use std::vector;

    use blob_store::storage_node::{Self, StorageNodeInfo};

    // Error codes
    const ERROR_TOTAL_MEMBER_ORDER: u64 = 0;
    const ERROR_SIG_VERIFICATION: u64 = 1;
    const ERROR_NOT_ENOUGH_STAKE: u64 = 2;
    const ERROR_INCORRECT_COMMITTEE: u64 = 3;

    /// This represents a BLS signing committee.
    /// The total_weight is the sum of the number of shards of each member.
    struct BlsCommittee has store, drop {
        /// A vector of committee members
        members: vector<StorageNodeInfo>,
        /// The total weight of the committee
        total_weight: u16,
    }

    /// Constructor
    public fun new_bls_committee(
        members: vector<StorageNodeInfo>
    ) : BlsCommittee {

        // Compute the total weight
        let total_weight = 0;
        let i = 0;
        while (i < vector::length(&members)) {
            let added_weight = storage_node::weight(vector::borrow(&members, i));
            assert!(added_weight > 0, ERROR_INCORRECT_COMMITTEE);
            total_weight = total_weight + added_weight;
            i = i + 1;
        };

        BlsCommittee {
            members,
            total_weight
        }
    }

    #[test_only]
    /// Test committee
    public fun new_bls_committee_for_testing() : BlsCommittee {
        BlsCommittee {
            members: vector[],
            total_weight: 100,
        }
    }

    /// Verify an aggregate BLS signature is a certificate in the epoch, and return the type of
    /// certificate and the bytes certified. The `signers` vector is an increasing list of indexes
    /// into the `members` vector of the committee. If there is a certificate, the function
    /// returns the total stake. Otherwise, it aborts.
    public fun verify_certificate(
        self: &BlsCommittee,
        signature: &vector<u8>,
        signers: &vector<u16>,
        message: &vector<u8>,
    ) : u16
    {
        // Use the signers flags to construct the key and the weights.

        // Lower bound for the next `member_idx` to ensure they are monotonically increasing
        let min_next_member_idx = 0;
        let i = 0;

        let aggregate_key = bls12381::g1_identity();
        let aggregate_weight = 0;

        while (i < vector::length(signers)) {
            let member_idx = (*vector::borrow(signers, i) as u64);
            assert!(member_idx >= min_next_member_idx, ERROR_TOTAL_MEMBER_ORDER);
            min_next_member_idx = member_idx + 1;

            // Bounds check happens here
            let member = vector::borrow(&self.members, member_idx);
            let key = storage_node::public_key(member);
            let weight = storage_node::weight(member);

            aggregate_key = bls12381::g1_add(&aggregate_key, key);
            aggregate_weight = aggregate_weight + weight;

            i = i + 1;
        };

        // The expression below is the solution to the inequality:
        // total_shards = 3 f + 1
        // stake >= 2f + 1
        assert!(3 * (aggregate_weight as u64) >= 2 * (self.total_weight as u64) + 1,
            ERROR_NOT_ENOUGH_STAKE);

        // Verify the signature
        let pub_key_bytes = group_ops::bytes(&aggregate_key);
        assert!(bls12381_min_pk_verify(
            signature,
            pub_key_bytes,
            message), ERROR_SIG_VERIFICATION);

        (aggregate_weight as u16)

    }

}
