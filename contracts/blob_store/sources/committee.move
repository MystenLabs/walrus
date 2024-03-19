// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::committee {

    use sui::bcs;

    const APP_ID: u8 = 3;

    // Errors
    const ERROR_INCORRECT_APP_ID: u64 = 0;
    const ERROR_INCORRECT_EPOCH: u64 = 1;

    #[test_only]
    use blob_store::bls_aggregate::new_bls_committee_for_testing;

    use blob_store::bls_aggregate::{BlsCommittee, new_bls_committee, verify_certificate};
    use blob_store::storage_node::StorageNodeInfo;

    /// Represents a committee for a given epoch, for a phantom type TAG.
    ///
    /// The construction of a committee for a type is a controlled operation
    /// and signifies that the committee is valid for the given epoch. It has
    /// no drop since valid committees must be stored for ever. And no copy
    /// since each epoch must only have one committee. Finally, no key since
    /// It must never be stored outside controlled places.
    ///
    /// The above restrictions allow us to implement a separation between committee
    /// formation and the actual System object for a phantom type TAG. One structure
    /// can take care of the epoch management including the committee formation, and
    /// the System object can simply receive a committee of the correct type as a
    /// signal that the new epoch has started.
    struct Committee<phantom TAG> has store {
        epoch: u64,
        bls_committee : BlsCommittee,
    }

    /// Get the epoch of the committee.
    public fun epoch<TAG>(self: &Committee<TAG>) : u64 {
        self.epoch
    }

    /// A capability that allows the creation of committees for a given phantom type TAG.
    struct CreateCommitteeCap<phantom TAG> has copy, store, drop {}

    /// A constructor for the capability to create committees for a given phantom type TAG.
    public fun create_committee_cap<TAG : drop>(_witness : TAG) : CreateCommitteeCap<TAG> {
        CreateCommitteeCap {}
    }

    /// Creating a committee for a given epoch.
    /// Requires a capability for the phantom type TAG.
    public fun create_committee<TAG>(
        _cap: &CreateCommitteeCap<TAG>,
        epoch: u64,
        members: vector<StorageNodeInfo>,
    ) : Committee<TAG> {
        // Make BlsCommittee
        let bls_committee = new_bls_committee(members);

        Committee { epoch, bls_committee }
    }

    #[test_only]
    public fun committee_for_testing<TAG>(
        epoch: u64,
    ) : Committee<TAG> {
        let bls_committee = new_bls_committee_for_testing();
        Committee { epoch, bls_committee }
    }

    #[test_only]
    public fun committee_for_testing_with_bls<TAG>(
        epoch: u64,
        bls_committee: BlsCommittee,
    ) : Committee<TAG> {
        Committee { epoch, bls_committee }
    }

    struct CertifiedMessage<phantom TAG> has drop {
        intent_type: u8,
        intent_version: u8,
        cert_epoch: u64,
        stake_support: u16,
        message: vector<u8>,
    }

    #[test_only]
    public fun certified_message_for_testing<TAG>(
        intent_type: u8,
        intent_version: u8,
        cert_epoch: u64,
        stake_support: u16,
        message: vector<u8>,
    ) : CertifiedMessage<TAG> {
        CertifiedMessage { intent_type, intent_version, cert_epoch, stake_support, message }
    }

    // Make accessors for the CertifiedMessage
    public fun intent_type<TAG>(self: &CertifiedMessage<TAG>) : u8 {
        self.intent_type
    }

    public fun intent_version<TAG>(self: &CertifiedMessage<TAG>) : u8 {
        self.intent_version
    }

    public fun cert_epoch<TAG>(self: &CertifiedMessage<TAG>) : u64 {
        self.cert_epoch
    }

    public fun stake_support<TAG>(self: &CertifiedMessage<TAG>) : u16 {
        self.stake_support
    }

    public fun message<TAG>(self: &CertifiedMessage<TAG>) : &vector<u8> {
        &self.message
    }

    // Deconstruct into the vector of message bytes
    public fun into_message<TAG>(self: CertifiedMessage<TAG>) : vector<u8> {
        self.message
    }

    /// Verifies that a message is signed by a quorum of the members of a committee.
    ///
    /// The members are listed in increasing order and with no repetitions. And the signatures
    /// match the order of the members. The total stake is returned, but if a quorum is not reached
    /// the function aborts with an error.
    public fun verify_quorum_in_epoch<TAG>(
        committee: &Committee<TAG>,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
        ) : CertifiedMessage<TAG> {

        let stake_support =
            verify_certificate(&committee.bls_committee, &signature, &members, &message);

        // Here we BCS decode the header of the message to check intents, epochs, etc.

        let bcs_message = bcs::new(message);
        let intent_type = bcs::peel_u8(&mut bcs_message);
        let intent_version = bcs::peel_u8(&mut bcs_message);

        let intent_app = bcs::peel_u8(&mut bcs_message);
        assert!(intent_app == APP_ID, ERROR_INCORRECT_APP_ID);

        let cert_epoch = bcs::peel_u64(&mut bcs_message);
        assert!(cert_epoch == epoch(committee), ERROR_INCORRECT_EPOCH);

        let message = bcs::into_remainder_bytes(bcs_message);

        CertifiedMessage<TAG> { intent_type, intent_version, cert_epoch, stake_support, message }
    }


}
