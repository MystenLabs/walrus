// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner {
    use sui::coin::Coin;
    use sui::table::Table;
    use sui::clock::Clock;
    use walrus::staked_wal::StakedWal;
    use walrus::staking_pool::StakingPool;
    use walrus::storage_node::StorageNodeCap;

    const ENotImplemented: u64 = 0;

    /// The inner object that is not present in signatures and can be versioned.
    public struct SystemStateInnerV1<phantom T> has store {
        staking_pools: Table<ID, StakingPool<T>>,
    }

    public(package) fun register_candidate<T>(
        self: &mut SystemStateInnerV1<T>,
        commission_rate: u64,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        abort ENotImplemented
    }

    public(package) fun withdraw_node<T>(self: &mut SystemStateInnerV1<T>, cap: StorageNodeCap) {
        abort ENotImplemented
    }

    public(package) fun set_next_commission<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun collect_commission<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
    ): Coin<T> {
        abort ENotImplemented
    }

    public(package) fun vote_for_next_epoch<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun voting_end<T>(self: &mut SystemStateInnerV1<T>, clock: &Clock) {
        abort ENotImplemented
    }

    public(package) fun epoch_sync_done<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
        epoch_number: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun shard_transfer_failed<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
        node_identity: vector<u8>,
        shard_ids: vector<u16>,
    ) {
        abort ENotImplemented
    }

    public(package) fun invalidate_blob_id<T>(
        self: &mut SystemStateInnerV1<T>,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
    ) {
        abort ENotImplemented
    }

    public(package) fun certify_event_blob<T>(
        self: &mut SystemStateInnerV1<T>,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        abort ENotImplemented
    }

    // === Staking ===

    public(package) fun stake_with_pool<T>(
        self: &mut SystemStateInnerV1<T>,
        to_stake: Coin<T>,
        pool_id: ID,
        ctx: &mut TxContext,
    ): StakedWal<T> {
        abort ENotImplemented
    }

    public(package) fun request_withdrawal<T>(
        self: &mut SystemStateInnerV1<T>,
        staked_wal: &mut StakedWal<T>,
        amount: u64,
    ) {
        abort ENotImplemented
    }

    public(package) fun withdraw_stake<T>(
        self: &mut SystemStateInnerV1<T>,
        staked_wal: StakedWal<T>,
        ctx: &mut TxContext,
    ) {
        abort ENotImplemented
    }

    // NOTE: join and split are currently in `staked_wal` module.
}
