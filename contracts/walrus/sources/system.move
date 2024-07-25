// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_function, unused_field, unused_mut_parameter)]
/// Module: system
module walrus::system {
    use walrus::storage_node::StorageNodeCap;
    use walrus::system_state_inner::SystemStateInnerV1;

    const ENotImplemented: u64 = 0;

    /// Flag to indicate the version of the system.
    const VERSION: u64 = 0;

    /// The one and only system object.
    public struct System has key {
        id: UID,
        version: u64,
    }

    /// Marks the epoch sync as done for the specified node.
    public fun epoch_sync_done(system: &mut System, cap: &StorageNodeCap, epoch_number: u64) {
        system.inner_mut().epoch_sync_done(cap, epoch_number)
    }

    /// Marks blob as invalid given an invalid blob certificate.
    public fun invalidate_blob_id(
        system: &mut System,
        signature: vector<u8>,
        members: vector<u16>,
        message: vector<u8>,
    ) {
        system.inner_mut().invalidate_blob_id(signature, members, message)
    }

    public fun certify_event_blob(
        system: &mut System,
        cap: &StorageNodeCap,
        blob_id: u256,
        size: u64,
    ) {
        system.inner_mut().certify_event_blob(cap, blob_id, size)
    }

    // === Internals ===

    /// Get a mutable reference to `SystemStateInner` from the `System`.
    fun inner_mut(system: &mut System): &mut SystemStateInnerV1 {
        assert!(system.version == VERSION);
        abort ENotImplemented
    }

    // === Tests ===

    #[test_only]
    use walrus::storage_node;

    #[test_only]
    fun new(ctx: &mut TxContext): System { System { id: object::new(ctx), version: VERSION } }

    #[test, expected_failure(abort_code = ENotImplemented)]
    fun test_epoch_sync_done() {
        let ctx = &mut tx_context::dummy();
        let cap = storage_node::new_cap_for_testing(ctx.fresh_object_address().to_id(), ctx);
        new(ctx).epoch_sync_done(&cap, 0);
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
}
