// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_variable, unused_mut_parameter, unused_field)]
module walrus::system_state_inner {
    use walrus::storage_node::StorageNodeCap;

    const ENotImplemented: u64 = 0;

    /// The inner object that is not present in signatures and can be versioned.
    public struct SystemStateInnerV1 has store {}

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
