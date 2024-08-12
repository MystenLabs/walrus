// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::committee;

use sui::bcs;
use walrus::{
    bls_aggregate::{Self, BlsCommittee, new_bls_committee, verify_certificate},
    storage_node::StorageNodeInfo
};

const APP_ID: u8 = 3;

// Errors
const EIncorrectAppId: u64 = 0;
const EIncorrectEpoch: u64 = 1;

/// Represents a committee for a given epoch.
public struct Committee has store {
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

public struct CertifiedMessage has drop {
    intent_type: u8,
    intent_version: u8,
    cert_epoch: u64,
    stake_support: u16,
    message: vector<u8>,
}

// == Accessors for CertifiedMessage ==

public fun intent_type(self: &CertifiedMessage): u8 {
    self.intent_type
}

public fun intent_version(self: &CertifiedMessage): u8 {
    self.intent_version
}

public fun cert_epoch(self: &CertifiedMessage): u64 {
    self.cert_epoch
}

public fun stake_support(self: &CertifiedMessage): u16 {
    self.stake_support
}

public fun message(self: &CertifiedMessage): &vector<u8> {
    &self.message
}

// Deconstruct into the vector of message bytes
public fun into_message(self: CertifiedMessage): vector<u8> {
    self.message
}

/// Verifies that a message is signed by a quorum of the members of a committee.
///
/// The members are listed in increasing order and with no repetitions. And the signatures
/// match the order of the members. The total stake is returned, but if a quorum is not reached
/// the function aborts with an error.
public fun verify_quorum_in_epoch(
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

    // Here we BCS decode the header of the message to check intents, epochs, etc.

    let mut bcs_message = bcs::new(message);
    let intent_type = bcs_message.peel_u8();
    let intent_version = bcs_message.peel_u8();

    let intent_app = bcs_message.peel_u8();
    assert!(intent_app == APP_ID, EIncorrectAppId);

    let cert_epoch = bcs_message.peel_u64();
    assert!(cert_epoch == epoch(committee), EIncorrectEpoch);

    let message = bcs_message.into_remainder_bytes();

    CertifiedMessage { intent_type, intent_version, cert_epoch, stake_support, message }
}

#[test_only]
use walrus::bls_aggregate::new_bls_committee_for_testing;

#[test_only]
public fun certified_message_for_testing(
    intent_type: u8,
    intent_version: u8,
    cert_epoch: u64,
    stake_support: u16,
    message: vector<u8>,
): CertifiedMessage {
    CertifiedMessage { intent_type, intent_version, cert_epoch, stake_support, message }
}

#[test_only]
public fun committee_for_testing(epoch: u64): Committee {
    let bls_committee = new_bls_committee_for_testing();
    Committee { epoch, bls_committee }
}

#[test_only]
public fun committee_for_testing_with_bls(epoch: u64, bls_committee: BlsCommittee): Committee {
    Committee { epoch, bls_committee }
}
