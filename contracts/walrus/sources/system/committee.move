// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::committee;

use walrus::{
    bls_aggregate::{Self, BlsCommittee, new_bls_committee, verify_certificate},
    messages::{Self, CertifiedMessage},
    storage_node::StorageNodeInfo
};

/// Represents a committee for a given epoch.
public struct Committee has store, drop {
    epoch: u64,
    bls_committee: BlsCommittee,
}

// == Accessors for Committee ==

/// Get the epoch of the committee.
public fun epoch(self: &Committee): u64 {
    self.epoch
}

/// Returns the number of shards held by the committee.
public fun n_shards(self: &Committee): u16 {
    bls_aggregate::n_shards(&self.bls_committee)
}

/// Creating a committee for a given epoch.
public(package) fun create_committee(epoch: u64, members: vector<StorageNodeInfo>): Committee {
    // Make BlsCommittee
    let bls_committee = new_bls_committee(members);

    Committee { epoch, bls_committee }
}

/// Verifies that a message is signed by a quorum of the members of a committee.
///
/// The members are listed in increasing order and with no repetitions. And the signatures
/// match the order of the members. The total stake is returned, but if a quorum is not reached
/// the function aborts with an error.
public(package) fun verify_quorum_in_epoch(
    committee: &Committee,
    signature: vector<u8>,
    members: vector<u16>,
    message: vector<u8>,
): CertifiedMessage {
    let stake_support = verify_certificate(
        &committee.bls_committee,
        &signature,
        &members,
        &message,
    );

    messages::new_certified_message(message, committee.epoch, stake_support)
}

#[test_only]
use walrus::bls_aggregate::new_bls_committee_for_testing;

#[test_only]
public fun committee_for_testing(epoch: u64): Committee {
    let bls_committee = new_bls_committee_for_testing();
    Committee { epoch, bls_committee }
}

#[test_only]
public fun committee_for_testing_with_bls(epoch: u64, bls_committee: BlsCommittee): Committee {
    Committee { epoch, bls_committee }
}
