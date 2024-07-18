// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system {
    use sui::coin::Coin;
    use sui::clock::Clock;
    use walrus::staked_wal::StakedWal;
    use walrus::storage_node::StorageNodeCap;
    use walrus::system_state_inner::SystemStateInnerV1;

    const ENotImplemented: u64 = 0;

    /// Flag to indicate the version of the system.
    const VERSION: u64 = 0;

    /// The one and only system object.
    public struct System<phantom T> has key {
        id: UID,
        version: u64,
        address: u8,
    }

    // === Public API: Storage Node ===

    /// Creates a staking pool for the candidate.
    public fun register_candidate<T>(
        system: &mut System<T>,
        // TODO: node_parameters
        commission_rate: u64,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        assert!(system.version == VERSION);
        system
            .inner_mut()
            .register_candidate(
                commission_rate,
                // node_parameters,
                // signature ?
                //
                ctx,
            )
    }

    /// Blocks staking for the nodes staking pool
    /// Marks node as "withdrawing",
    /// - excludes it from the next committee selection
    /// - still has to remain active while it is part of the committee and until all shards have
    ///     been transferred to its successor
    /// - The staking pool is deleted once the last funds have been withdrawn from it by its stakers
    public fun withdraw_node<T>(system: &mut System<T>, cap: StorageNodeCap) {
        assert!(system.version == VERSION);
        system.inner_mut().withdraw_node(cap);
    }

    /// Sets next_commission in the staking pool, which will then take effect as commission rate
    /// one epoch after setting the value (to allow stakers to react to setting this).
    public fun set_next_commission<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().set_next_commission(cap, commission_rate);
    }

    /// Returns the accumulated commission for the storage node.
    public fun collect_commission<T>(system: &mut System<T>, cap: &StorageNodeCap): Coin<T> {
        assert!(system.version == VERSION);
        system.inner_mut().collect_commission(cap)
    }

    /// TODO: split these into separate functions.
    /// Changes the votes for the storage node. Can be called arbitrarily often, if not called, the
    /// votes remain the same as in the previous epoch.
    public fun vote_for_price_next_epoch<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().vote_for_next_epoch(cap, storage_price, write_price, storage_capacity)
    }

    /// Ends the voting period and runs the apportionment if the current time allows.
    /// Permissionless, can be called by anyone.
    public fun voting_end<T>(system: &mut System<T>, clock: &Clock) {
        assert!(system.version == VERSION);
        system.inner_mut().voting_end(clock)
    }

    public fun epoch_sync_done<T>(system: &mut System<T>, cap: &StorageNodeCap, epoch_number: u64) {
        assert!(system.version == VERSION);
        system.inner_mut().epoch_sync_done(cap, epoch_number)
    }

    /// Checks if the node should either have received the specified shards from the specified node
    /// or vice-versa.
    ///
    /// - also checks that for the provided shards, this function has not been called before
    /// - if so, slashes both nodes and emits an event that allows the receiving node to start
    ///     shard recovery
    public fun shard_transfer_failed<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        node_identity: vector<u8>,
        shard_ids: vector<u16>,
    ) {
        assert!(system.version == VERSION);
        system
            .inner_mut()
            .shard_transfer_failed(
                cap,
                node_identity,
                shard_ids,
            )
    }

    /// Marks blob as invalid given an invalid blob certificate.
    public fun invalidate_blob_id<T>(
        system: &mut System<T>,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().invalidate_blob_id(signature, members, message)
    }

    public fun certify_event_blob<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().certify_event_blob(cap, blob_id, size)
    }

    // === Public API: Staking ===

    /// Stake `Coin` with the staking pool.
    public fun stake_with_pool<T>(
        system: &mut System<T>,
        to_stake: Coin<T>,
        pool_id: ID,
        ctx: &mut TxContext,
    ): StakedWal<T> {
        assert!(system.version == VERSION);
        system.inner_mut().stake_with_pool(to_stake, pool_id, ctx)
    }

    /// Marks the amount as a withdrawal to be processed and removes it from the stake weight of the
    /// node. Allows the user to call withdraw_stake after the epoch change to the next epoch and
    /// shard transfer is done.
    public fun request_withdrawal<T>(
        system: &mut System<T>,
        staked_wal: &mut StakedWal<T>,
        amount: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().request_withdrawal(staked_wal, amount)
    }

    /// Withdraws the staked amount from the staking pool.
    public fun withdraw_stake<T>(
        system: &mut System<T>,
        staked_wal: StakedWal<T>,
        ctx: &mut TxContext,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().withdraw_stake(staked_wal, ctx)
    }

    // === Internals ===

    /// Get a reference to `SystemStateInner` from the `System`.
    fun inner<T>(system: &System<T>): &SystemStateInnerV1<T> { abort ENotImplemented }

    /// Get a mutable reference to `SystemStateInner` from the `System`.
    fun inner_mut<T>(system: &mut System<T>): &mut SystemStateInnerV1<T> { abort ENotImplemented }
}
