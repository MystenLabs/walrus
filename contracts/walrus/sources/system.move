// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system {
    use sui::coin::Coin;
    use sui::clock::Clock;
    use sui::sui::SUI;
    use walrus::staked_wal::StakedWal;
    use walrus::storage_node::StorageNodeCap;
    use walrus::system_state_inner::SystemStateInnerV1;
    use walrus::staking_inner::StakingInnerV1;

    const ENotImplemented: u64 = 0;

    /// Flag to indicate the version of the system.
    const VERSION: u64 = 0;

    /// The one and only system object.
    public struct System has key {
        id: UID,
        version: u64,
    }

    // === Public API: Storage Node ===

    /// Creates a staking pool for the candidate, registers the candidate as a storage node.
    public fun register_candidate(
        system: &mut System,
        // TODO: node_parameters
        commission_rate: u64,
        ctx: &mut TxContext,
    ): StorageNodeCap {
        // use the Pool ID as the identifier of the storage node (?)
        // TODO: circle back on this
        let pool_id = system.staking_mut().create_pool(commission_rate, ctx);
        let node_cap = system.system_mut().register_candidate(pool_id, ctx);
        node_cap
    }

    /// Blocks staking for the nodes staking pool
    /// Marks node as "withdrawing",
    /// - excludes it from the next committee selection
    /// - still has to remain active while it is part of the committee and until all shards have
    ///     been transferred to its successor
    /// - The staking pool is deleted once the last funds have been withdrawn from it by its stakers
    public fun withdraw_node(system: &mut System, cap: StorageNodeCap) {
        system.staking_mut().set_withdrawing(cap.pool_id());
        system.system_mut().withdraw_node(cap);
    }

    /// Sets next_commission in the staking pool, which will then take effect as commission rate
    /// one epoch after setting the value (to allow stakers to react to setting this).
    public fun set_next_commission(
        system: &mut System,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        system.system_mut().set_next_commission(cap, commission_rate);
    }

    /// Returns the accumulated commission for the storage node.
    public fun collect_commission(system: &mut System, cap: &StorageNodeCap): Coin<SUI> {
        system.system_mut().collect_commission(cap)
    }

    /// TODO: split these into separate functions.
    /// Changes the votes for the storage node. Can be called arbitrarily often, if not called, the
    /// votes remain the same as in the previous epoch.
    public fun vote_for_price_next_epoch(
        system: &mut System,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        system.system_mut().vote_for_next_epoch(cap, storage_price, write_price, storage_capacity)
    }

    /// Ends the voting period and runs the apportionment if the current time allows.
    /// Permissionless, can be called by anyone.
    /// Emits: `VotingEnd` event.
    public fun voting_end(system: &mut System, clock: &Clock) {
        system.system_mut().voting_end(clock)
    }

    /// Initiates the epoch change if the current time allows.
    /// Emits: `EpochChangeSync` event.
    public fun initiate_epoch_change(system: &mut System, clock: &Clock) {
        system.system_mut().initiate_epoch_change(clock)
    }

    /// Marks the epoch sync as done for the specified node.
    public fun epoch_sync_done(system: &mut System, cap: &StorageNodeCap, epoch_number: u64) {
        system.system_mut().epoch_sync_done(cap, epoch_number)
    }

    /// Checks if the node should either have received the specified shards from the specified node
    /// or vice-versa.
    ///
    /// - also checks that for the provided shards, this function has not been called before
    /// - if so, slashes both nodes and emits an event that allows the receiving node to start
    ///     shard recovery
    public fun shard_transfer_failed(
        system: &mut System,
        cap: &StorageNodeCap,
        node_identity: vector<u8>,
        shard_ids: vector<u16>,
    ) {
        system
            .system_mut()
            .shard_transfer_failed(
                cap,
                node_identity,
                shard_ids,
            )
    }

    /// Marks blob as invalid given an invalid blob certificate.
    public fun invalidate_blob_id(
        system: &mut System,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
    ) {
        system.system_mut().invalidate_blob_id(signature, members, message)
    }

    public fun certify_event_blob(
        system: &mut System,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        system.system_mut().certify_event_blob(cap, blob_id, size)
    }

    // === Public API: Staking ===

    /// Stake `Coin` with the staking pool.
    public fun stake_with_pool(
        system: &mut System,
        to_stake: Coin<SUI>,
        pool_id: ID,
        ctx: &mut TxContext,
    ): StakedWal {
        system.staking_mut().stake_with_pool(to_stake, pool_id, ctx)
    }

    /// Marks the amount as a withdrawal to be processed and removes it from the stake weight of the
    /// node. Allows the user to call withdraw_stake after the epoch change to the next epoch and
    /// shard transfer is done.
    public fun request_withdrawal(system: &mut System, staked_wal: &mut StakedWal, amount: u64) {
        system.staking_mut().request_withdrawal(staked_wal, amount)
    }

    /// Withdraws the staked amount from the staking pool.
    public fun withdraw_stake(system: &mut System, staked_wal: StakedWal, ctx: &mut TxContext) {
        system.staking_mut().withdraw_stake(staked_wal, ctx)
    }

    // === Internals ===

    /// Get a mutable reference to `SystemStateInner` from the `System`.
    fun system_mut(system: &mut System): &mut SystemStateInnerV1 {
        assert!(system.version == VERSION);
        abort ENotImplemented
    }

    /// Get a mutable reference to `StakingInner` from the `System`.
    fun staking_mut(system: &mut System): &mut StakingInnerV1 {
        assert!(system.version == VERSION);
        abort ENotImplemented
    }

    // === Tests ===

    #[test_only]
    use walrus::storage_node;

    #[test_only]
    use sui::{clock, coin};

    #[test_only]
    fun new(ctx: &mut TxContext): System { System { id: object::new(ctx), version: VERSION } }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_register_candidate() {
        let ctx = &mut tx_context::dummy();
        let cap = new(ctx).register_candidate(0, ctx);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_withdraw_node() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).withdraw_node(cap);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_set_next_commission() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).set_next_commission(&cap, 0);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_collect_commission() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        let coin = new(ctx).collect_commission(&cap);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_vote_for_price_next_epoch() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).vote_for_price_next_epoch(&cap, 0, 0, 0);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_voting_end() {
        let ctx = &mut tx_context::dummy();
        let clock = clock::create_for_testing(ctx);
        new(ctx).voting_end(&clock);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_epoch_sync_done() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).epoch_sync_done(&cap, 0);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_shard_transfer_failed() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).shard_transfer_failed(&cap, vector[], vector[]);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_invalidate_blob_id() {
        let ctx = &mut tx_context::dummy();
        new(ctx).invalidate_blob_id(vector[], vector[], vector[]);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_certify_event_blob() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).certify_event_blob(&cap, 0, 0);
        abort 1337
    }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_stake_with_pool() {
        let ctx = &mut tx_context::dummy();
        let coin = coin::mint_for_testing<SUI>(100, ctx);
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        let staked_wal = new(ctx).stake_with_pool(coin, cap.pool_id(), ctx);
        abort 1337
    }
}
