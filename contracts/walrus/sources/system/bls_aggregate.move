// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::bls_aggregate;

use sui::{
    bls12381::{Self, bls12381_min_pk_verify, G1},
    group_ops::{Self, Element},
    vec_map::{Self, VecMap}
};
use walrus::{messages::{Self, CertifiedMessage}, storage_node::StorageNodeInfo};

// Error codes
const ETotalMemberOrder: u64 = 0;
const ESigVerification: u64 = 1;
const ENotEnoughStake: u64 = 2;
const EIncorrectCommittee: u64 = 3;

public struct BlsCommitteeMember has store, copy, drop {
    public_key: Element<G1>,
    weight: u16,
    node_id: ID,
}

/// This represents a BLS signing committee for a given epoch.
public struct BlsCommittee has store, copy, drop {
    /// A vector of committee members
    members: vector<BlsCommitteeMember>,
    /// The total number of shards held by the committee
    n_shards: u16,
    /// The epoch in which the committee is active.
    epoch: u64,
}

/// Constructor for committee
public(package) fun new_bls_committee(epoch: u64, members: &vector<StorageNodeInfo>): BlsCommittee {
    // Compute the total number of shards
    let mut n_shards = 0;
    let bls_members = members.map_ref!(
        |member| {
            let weight = member.weight();
            assert!(weight > 0, EIncorrectCommittee);
            n_shards = n_shards + weight;
            BlsCommitteeMember {
                public_key: *member.public_key(),
                weight,
                node_id: member.id(),
            }
        },
    );

    // TODO: if we keep this check, there has to be a test for it
    // TODO: discuss relaxing this restriction to allow for empty committee in
    //       the staking, and don't require Option<BlsCommittee> there.
    // assert!(n_shards != 0, EIncorrectCommittee);

    BlsCommittee { members: bls_members, n_shards, epoch }
}

// == Accessors for BlsCommittee ==

/// Get the epoch of the committee.
public fun epoch(self: &BlsCommittee): u64 {
    self.epoch
}

/// Returns the number of shards held by the committee.
public(package) fun n_shards(self: &BlsCommittee): u16 {
    self.n_shards
}

/// Returns the members of the committee with their weights.
public(package) fun to_vec_map(self: &BlsCommittee): VecMap<ID, u16> {
    let mut result = vec_map::empty();
    self.members.do_ref!(|member| result.insert(member.node_id, member.weight));
    result
}

/// Verifies that a message is signed by a quorum of the members of a committee.
///
/// The members are listed in increasing order and with no repetitions. And the signatures
/// match the order of the members. The total stake is returned, but if a quorum is not reached
/// the function aborts with an error.
public(package) fun verify_quorum_in_epoch(
    self: &BlsCommittee,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
): CertifiedMessage {
    let stake_support = self.verify_certificate(
        &signature,
        &members,
        &message,
    );

    messages::new_certified_message(message, self.epoch, stake_support)
}

/// Verify an aggregate BLS signature is a certificate in the epoch, and return the type of
/// certificate and the bytes certified. The `signers` vector is an increasing list of indexes
/// into the `members` vector of the committee. If there is a certificate, the function
/// returns the total stake. Otherwise, it aborts.
public(package) fun verify_certificate(
    self: &BlsCommittee,
    signature: &vector<u8>,
    signers: &vector<u16>,
    message: &vector<u8>,
): u16 {
    // Use the signers flags to construct the key and the weights.

    // Lower bound for the next `member_index` to ensure they are monotonically increasing
    let mut min_next_member_index = 0;
    let mut aggregate_key = bls12381::g1_identity();
    let mut aggregate_weight = 0;

    signers.do_ref!(
        |member_index| {
            let member_index = *member_index as u64;
            assert!(member_index >= min_next_member_index, ETotalMemberOrder);
            min_next_member_index = member_index + 1;

            // Bounds check happens here
            let member = &self.members[member_index];
            let key = &member.public_key;
            let weight = member.weight;

            aggregate_key = bls12381::g1_add(&aggregate_key, key);
            aggregate_weight = aggregate_weight + weight;
        },
    );

    // The expression below is the solution to the inequality:
    // n_shards = 3 f + 1
    // stake >= 2f + 1
    assert!(
        3 * (aggregate_weight as u64) >= 2 * (self.n_shards as u64) + 1,
        ENotEnoughStake,
    );

    // Verify the signature
    let pub_key_bytes = group_ops::bytes(&aggregate_key);
    assert!(
        bls12381_min_pk_verify(
            signature,
            pub_key_bytes,
            message,
        ),
        ESigVerification,
    );

    (aggregate_weight as u16)
}

#[test_only]
use sui::bls12381::g1_from_bytes;

#[test_only]
/// Test committee
public fun new_bls_committee_for_testing(epoch: u64): BlsCommittee {
    let ctx = &mut tx_context::dummy();
    let id = object::new(ctx);
    let node_id = id.to_inner();
    id.delete();
    // Pk corresponding to secret key scalar(117)
    let pub_key_bytes = x"95eacc3adc09c827593f581e8e2de068bf4cf5d0c0eb29e5372f0d23364788ee0f9beb112c8a7e9c2f0c720433705cf0"; // editorconfig-checker-disable-line
    let member = BlsCommitteeMember {
        public_key: g1_from_bytes(&pub_key_bytes),
        weight: 100,
        node_id,
    };
    BlsCommittee { members: vector[member], n_shards: 100, epoch }
}
