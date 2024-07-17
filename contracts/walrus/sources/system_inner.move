// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner {
    use sui::coin::Coin;
    use sui::table::Table;
    use walrus::staking_pool::StakingPool;
    use walrus::storage_node::StorageNodeCap;

    const ENotImplemented: u64 = 0;

    /// The inner object that is not present in signatures and can be versioned.
    public struct SystemStateInner<phantom T> has store {
        staking_pools: Table<ID, StakingPool<T>>,
    }

    public(package) fun register_candidate<T>(
        self: &mut SystemStateInner<T>,
        commission_rate: u64,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        abort ENotImplemented
    }

    public(package) fun withdraw_node<T>(
        self: &mut SystemStateInner<T>,
        cap: StorageNodeCap,
    ) {
        abort ENotImplemented
    }

    public(package) fun set_next_commission<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun collect_commission<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
    ): Coin<T> {
        abort ENotImplemented
    }

    public(package) fun vote_for_next_epoch<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun voting_end<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
    ) {
        abort ENotImplemented
    }

    public(package) fun epoch_sync_done<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        epoch_number: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun shard_transfer_failed<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        node_identity: vector<u8>,
        shard_ids: vector<u16>,
    ) {
        abort ENotImplemented
    }

    public(package) fun invalidate_blob_id<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        blob_id: u256,
    ) {
        abort ENotImplemented
    }

    public(package) fun certify_event_blob<T>(
        self: &mut SystemStateInner<T>,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        abort ENotImplemented
    }
}
