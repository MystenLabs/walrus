// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::bls_aggregate {

    use sui::group_ops::{Self, Element};
    use sui::bls12381::{Self, G1, bls12381_min_pk_verify};
    use std::vector;

    // Error codes
    const ERROR_TOTAL_MEMBER_ORDER: u64 = 0;
    const ERROR_SIG_VERIFICATION: u64 = 1;
    const ERROR_NOT_ENOUGH_STAKE: u64 = 2;
    const ERROR_INCORRECT_COMMITTEE: u64 = 3;

    /// This is a greatly compressed and optimized structure to represent a BLS signing committee.
    /// The length of public_keys and weights is equal, and total_weight is the sum of weights.
    struct BlsCommittee has store, drop {
        /// A vector of public keys of the committee members
        public_keys: vector<Element<G1>>,
        /// The total weight of the committee
        total_weight: u16,
        /// A vector of weights of the committee members
        weights: vector<u16>,
    }

    /// Constructor
    public fun new_bls_committee(
        public_keys: vector<Element<G1>>,
        weights: vector<u16>,
    ) : BlsCommittee {

        // Ensure the lengths are equal
        assert!(vector::length(&public_keys) == vector::length(&weights),
            ERROR_INCORRECT_COMMITTEE);

        // Compute the total weight
        let total_weight = 0;
        let i = 0;
        while (i < vector::length(&weights)) {
            let added_weight = (*vector::borrow(&weights, i) as u16);
            assert!(added_weight > 0, ERROR_INCORRECT_COMMITTEE);
            total_weight = total_weight + added_weight;
            i = i + 1;
        };

        BlsCommittee {
            public_keys: public_keys,
            total_weight: total_weight,
            weights: weights,
        }
    }

    #[test_only]
    /// Test committee
    public fun new_bls_committee_for_testing() : BlsCommittee {
        let public_keys = vector[];
        let weights = vector[];
        let total_weight = 100;
        BlsCommittee {
            public_keys: public_keys,
            total_weight: total_weight,
            weights: weights,
        }
    }

    /// Verify an aggregate BLS signature is a certificate in the epoch, and return the type of
    /// certificate and the bytes certified. The members vector is an increasing list of indexes
    /// into the public_keys and weights vectors of the committee. If there is a certificate, the
    /// function returns the total stake. Otherwise, it aborts.
    public fun verify_certificate(
        committee: &BlsCommittee,
        signature: &vector<u8>,
        members: &vector<u16>,
        message: &vector<u8>,
    ) : u16
    {
        // Use the members flags to construct the key and the weights.
        let initial_member = 0; // store member - 1
        let i = 0;

        let aggregate_key = bls12381::g1_identity();
        let aggregate_weight = 0;

        while (i < vector::length(members)) {
            let member = (*vector::borrow(members, i) as u64);
            assert!(member + 1 > initial_member, ERROR_TOTAL_MEMBER_ORDER);
            initial_member = member + 1;

            // Bounds check happens here
            let key = vector::borrow(&committee.public_keys, member);
            let weight = (*vector::borrow(&committee.weights, member) as u64);

            aggregate_key = bls12381::g1_add(&aggregate_key, key);
            aggregate_weight = aggregate_weight + weight;

            i = i + 1;
        };

        // The expression below is the solution to the inequality:
        // total_shards = 3 f + 1
        // stake >= 2f + 1
        assert!(3 * (aggregate_weight as u64) >= 2 * (committee.total_weight as u64) + 1,
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
