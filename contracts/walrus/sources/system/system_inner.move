// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner {
    use sui::clock::Clock;
    use sui::coin::Coin;
    use sui::sui::SUI;
    use walrus::storage_node::StorageNodeCap;

    const ENotImplemented: u64 = 0;

    /// The inner object that is not present in signatures and can be versioned.
    public struct SystemStateInnerV1 has store {}

    public(package) fun register_candidate(
        self: &mut SystemStateInnerV1,
        pool_id: ID,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        abort ENotImplemented
    }

    public(package) fun withdraw_node(self: &mut SystemStateInnerV1, cap: StorageNodeCap) {
        abort ENotImplemented
    }

    public(package) fun set_next_commission(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun collect_commission(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
    ): Coin<SUI> {
        abort ENotImplemented
    }

    public(package) fun vote_for_next_epoch(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun voting_end(self: &mut SystemStateInnerV1, clock: &Clock) {
        abort ENotImplemented
    }
    
    public(package) fun initiate_epoch_change(self: &mut SystemStateInnerV1, clock: &Clock) {
        abort ENotImplemented
    }

    public(package) fun epoch_sync_done(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
        epoch_number: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun shard_transfer_failed(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
        node_identity: vector<u8>,
        shard_ids: vector<u16>,
    ) {
        abort ENotImplemented
    }

    public(package) fun invalidate_blob_id(
        self: &mut SystemStateInnerV1,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
    ) {
        abort ENotImplemented
    }

    public(package) fun certify_event_blob(
        self: &mut SystemStateInnerV1,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        abort ENotImplemented
    }
}
