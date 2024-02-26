// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module blob_store::committee {

    use std::vector;
    use std::string::String;
    use sui::ed25519;

    /// Represents a storage node and its meta-data.
    ///
    /// Creation and deletion of storage node info is an
    /// uncontrolled operation, but it lacks key so cannot
    /// be stored outside the context of another object.
    struct StorageNodeInfo has store, drop {
        name: String,
        network_address: String,
        public_key: vector<u8>,
        shard_ids: vector<u16>,
    }

    /// A public constructor for the StorageNodeInfo.
    public fun create_storage_node_info(
        name: String,
        network_address: String,
        public_key: vector<u8>,
        shard_ids: vector<u16>,
    ) : StorageNodeInfo {
        StorageNodeInfo { name, network_address, public_key, shard_ids }
    }

    public fun public_key(self: &StorageNodeInfo) : &vector<u8> {
        &self.public_key
    }

    public fun shard_ids(self: &StorageNodeInfo) : &vector<u16> {
        &self.shard_ids
    }

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
        total_shards: u16,
        members: vector<StorageNodeInfo>,
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
        total_shards: u16,
        members: vector<StorageNodeInfo>,
    ) : Committee<TAG> {
        Committee { epoch, total_shards, members }
    }

    // Quorum verification errors
    const ERROR_INCOMPATIBLE_LEN : u64 = 0;
    const ERROR_NON_INCREASING : u64 = 2;
    const ERROR_SIG_VERIFICATION : u64 = 3;
    const ERROR_NOT_ENOUGH_STAKE : u64 = 4;

    // TODO: port to BLS right now!

    /// Verifies that a message is signed by a quorum of the members of a committee.
    ///
    /// The members are listed in increasing order and with no repetitions. And the signatures
    /// match the order of the members. The total stake is returned, but if a quorum is not reached
    /// the function aborts with an error.
    public fun verify_quorum<TAG>(
        committee: &Committee<TAG>,
        message: &vector<u8>,
        members: &vector<u16>,
        signatures: &vector<vector<u8>>) : u16 {

        assert!(vector::length(members) == vector::length(signatures), ERROR_INCOMPATIBLE_LEN);

        // Here we store the index + 1 so that we can safely initialize to zero as an initial
        // minimum value.
        let recall_floor = 0;
        let stake = 0;

        let i = 0;
        while (i < vector::length(members)) {
            // Ensure the sequence is strictly increasing, to avoid duplicates and stake double
            // counting.
            let member_index = *vector::borrow(members, i);
            let member = vector::borrow(&committee.members, (member_index as u64));

            assert!(recall_floor < member_index + 1, ERROR_NON_INCREASING);
            recall_floor = member_index + 1;

            // Ensure that the signature is valid.
            let sig = vector::borrow(signatures, i);
            let pk = public_key(member);
            let verify = ed25519::ed25519_verify(sig, pk, message);
            assert!(verify, ERROR_SIG_VERIFICATION);

            // Add the stake of the member to the total stake.
            // The stake here is the number of shards held by the member.
            stake = stake + vector::length(shard_ids(member));

            i = i + 1;
        };

        // The expression below is the solution to the inequality:
        // total_shards = 3 f + 1
        // stake >= 2f + 1
        assert!(3 * (stake as u64) >= 2 * (committee.total_shards as u64) + 1,
            ERROR_NOT_ENOUGH_STAKE);

        (stake as u16)
    }


}
