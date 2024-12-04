// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::bls_aggregate;

use sui::{
    bls12381::{Self, bls12381_min_pk_verify, G1, UncompressedG1},
    group_ops::{Self, Element},
    vec_map::{Self, VecMap}
};
use walrus::messages::{Self, CertifiedMessage};

// Error codes
// Error types in `walrus-sui/types/move_errors.rs` are auto-generated from the Move error codes.
const ETotalMemberOrder: u64 = 0;
const ESigVerification: u64 = 1;
const ENotEnoughStake: u64 = 2;
const EIncorrectCommittee: u64 = 3;

public struct BlsCommitteeMember has copy, drop, store {
    public_key: Element<UncompressedG1>,
    weight: u16,
    node_id: ID,
}

/// This represents a BLS signing committee for a given epoch.
public struct BlsCommittee has copy, drop, store {
    /// A vector of committee members
    members: vector<BlsCommitteeMember>,
    /// The total number of shards held by the committee
    n_shards: u16,
    /// The epoch in which the committee is active.
    epoch: u32,
    /// The aggregation of public keys for all members of the committee
    total_aggregated_key: Element<G1>,
}

/// Constructor for committee.
public(package) fun new_bls_committee(
    epoch: u32,
    members: vector<BlsCommitteeMember>,
): BlsCommittee {
    // Compute the total number of shards
    let mut n_shards = 0;
    members.do_ref!(|member| {
        let weight = member.weight;
        assert!(weight > 0, EIncorrectCommittee);
        n_shards = n_shards + weight;
    });

    // TODO: if we keep this check, there has to be a test for it
    // TODO: discuss relaxing this restriction to allow for empty committee in
    //       the staking, and don't require Option<BlsCommittee> there.
    // assert!(n_shards != 0, EIncorrectCommittee);

    // Compute the total aggregated key, e.g. the sum of all public keys in the committee.
    let total_aggregated_key = bls12381::uncompressed_g1_to_g1(
        &bls12381::uncompressed_g1_sum(
            &members.map!(|member| member.public_key),
        ),
    );

    BlsCommittee { members, n_shards, epoch, total_aggregated_key }
}

/// Constructor for committee member.
public(package) fun new_bls_committee_member(
    public_key: Element<G1>,
    weight: u16,
    node_id: ID,
): BlsCommitteeMember {
    BlsCommitteeMember {
        public_key: bls12381::g1_to_uncompressed_g1(&public_key),
        weight,
        node_id,
    }
}

// === Accessors for BlsCommitteeMember ===

/// Get the node id of the committee member.
public(package) fun node_id(self: &BlsCommitteeMember): sui::object::ID {
    self.node_id
}

// === Accessors for BlsCommittee ===

/// Get the epoch of the committee.
public(package) fun epoch(self: &BlsCommittee): u32 {
    self.epoch
}

/// Returns the number of shards held by the committee.
public(package) fun n_shards(self: &BlsCommittee): u16 {
    self.n_shards
}

/// Returns the member at given index
public(package) fun get_idx(self: &BlsCommittee, idx: u64): &BlsCommitteeMember {
    &self.members[idx]
}

/// Checks if the committee contains a given node.
public(package) fun contains(self: &BlsCommittee, node_id: &ID): bool {
    self.find_index(node_id).is_some()
}

/// Returns the member weight if it is part of the committee or 0 otherwise
public(package) fun get_member_weight(self: &BlsCommittee, node_id: &ID): u16 {
    self.find_index(node_id).map!(|idx| self.members[idx].weight).destroy_or!(0)
}

/// Finds the index of the member by node_id
public(package) fun find_index(self: &BlsCommittee, node_id: &ID): Option<u64> {
    self.members.find_index!(|member| &member.node_id == node_id)
}

/// Returns the members of the committee with their weights.
public(package) fun to_vec_map(self: &BlsCommittee): VecMap<ID, u16> {
    let mut result = vec_map::empty();
    self.members.do_ref!(|member| {
        result.insert(member.node_id, member.weight)
    });
    result
}

/// Verifies that a message is signed by a quorum of the members of a committee.
///
/// The signers are listed as indices into the `members` vector of the committee
/// in increasing
/// order and with no repetitions. The total weight of the signers (i.e. total
/// number of shards)
/// is returned, but if a quorum is not reached the function aborts with an
/// error.
public(package) fun verify_quorum_in_epoch(
    self: &BlsCommittee,
    signature: vector<u8>,
    signers: vector<u16>,
    message: vector<u8>,
): CertifiedMessage {
    let stake_support = self.verify_certificate(
        &signature,
        &signers,
        &message,
    );

    messages::new_certified_message(message, self.epoch, stake_support)
}

/// Returns true if the weight is more than the aggregate weight of quorum members of a committee.
public(package) fun verify_quorum(self: &BlsCommittee, weight: u16): bool {
    3 * (weight as u64) >= 2 * (self.n_shards as u64) + 1
}

/// Verify an aggregate BLS signature is a certificate in the epoch, and return
/// the type of
/// certificate and the bytes certified. The `signers` vector is an increasing
/// list of indexes
/// into the `members` vector of the committee. If there is a certificate, the
/// function
/// returns the total stake. Otherwise, it aborts.
public(package) fun verify_certificate(
    self: &BlsCommittee,
    signature: &vector<u8>,
    signers: &vector<u16>,
    message: &vector<u8>,
): u16 {
    // Use the signers flags to construct the key and the weights.

    // Lower bound for the next `member_index` to ensure they are monotonically
    // increasing
    let mut min_next_member_index = 0;
    let mut aggregate_weight = 0;

    signers.do_ref!(|member_index| {
        let member_index = *member_index as u64;
        assert!(member_index >= min_next_member_index, ETotalMemberOrder);
        min_next_member_index = member_index + 1;

        // Bounds check happens here
        let member = &self.members[member_index];
        let weight = member.weight;

        aggregate_weight = aggregate_weight + weight;
    });

    // The expression below is the solution to the inequality:
    // n_shards = 3 f + 1
    // stake >= 2f + 1
    assert!(self.verify_quorum(aggregate_weight), ENotEnoughStake);

    // Compute the aggregate public key of the signers as the difference between
    // the total aggregated key and the aggregate public key of the non-signers.
    let non_signer_public_keys = complement(signers, self.members.length() as u16).map!(
        |index| self.members[index as u64].public_key,
    );
    let aggregate_key = bls12381::g1_sub(
        &self.total_aggregated_key,
        &bls12381::uncompressed_g1_to_g1(
            &bls12381::uncompressed_g1_sum(&non_signer_public_keys),
        ),
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

/// Returns the complement of the given list with respect to the range [0, n) assuming that the list is in strictly increasing order.
fun complement(list: &vector<u16>, n: u16): vector<u16> {
    assert!(list.length() <= n as u64);
    let mut result: vector<u16> = vector::empty();
    let mut offset = 0;
    list.do_ref!(|index| {
        assert!(*index >= offset);
        (*index - offset as u16).do!(|i| result.push_back(offset + i));
        offset = *index + 1;
    });
    (n - offset).do!(|i| result.push_back(offset + i));
    result
}

#[test_only]
/// Increments the committee epoch by one.
public fun increment_epoch_for_testing(self: &mut BlsCommittee) {
    self.epoch = self.epoch + 1;
}
