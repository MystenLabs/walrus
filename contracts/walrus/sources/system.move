// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field)]
/// Module: system
module walrus::system {
    use sui::coin::Coin;
    use walrus::storage_node::StorageNodeCap;
    use walrus::system_state_inner::SystemStateInner;

    const ENotImplemented: u64 = 0;

    /// Flag to indicate the version of the system.
    const VERSION: u64 = 0;

    /// The one and only system object.
    public struct System<phantom T> has key {
        id: UID,
        version: u64,
    }

    // === Public-facing API ===

    public fun register_candidate<T>(
        system: &mut System<T>,
        // node_parameters: ???
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

    public fun withdraw_node<T>(system: &mut System<T>, cap: StorageNodeCap) {
        assert!(system.version == VERSION);
        system.inner_mut().withdraw_node(cap);
    }

    // public fun update_node_info<T>() {}

    public fun set_next_commission<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        commission_rate: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().set_next_commission(cap, commission_rate);
    }

    public fun collect_commission<T>(system: &mut System<T>, cap: &StorageNodeCap): Coin<T> {
        assert!(system.version == VERSION);
        system.inner_mut().collect_commission(cap)
    }

    public fun vote_for_next_epoch<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().vote_for_next_epoch(cap, storage_price, write_price, storage_capacity)
    }

    public fun voting_end<T>(system: &mut System<T>, cap: &StorageNodeCap) {
        assert!(system.version == VERSION);
        system.inner_mut().voting_end(cap)
    }

    public fun epoch_sync_done<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        epoch_number: u64,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().epoch_sync_done(cap, epoch_number)
    }

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

    public fun invalidate_blob_id<T>(
        system: &mut System<T>,
        cap: &StorageNodeCap,
        blob_id: u256,
    ) {
        assert!(system.version == VERSION);
        system.inner_mut().invalidate_blob_id(cap, blob_id)
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

    // === Internals ===

    /// Get a reference to `SystemStateInner` from the `System`.
    fun inner<T>(system: &System<T>): &SystemStateInner<T> { abort ENotImplemented }

    /// Get a mutable reference to `SystemStateInner` from the `System`.
    fun inner_mut<T>(system: &mut System<T>): &mut SystemStateInner<T> { abort ENotImplemented }
}
