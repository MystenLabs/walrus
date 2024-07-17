// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_field, unused_function, unused_variable)]
module walrus::storage_node {
    const ENotImplemented: u64 = 0;

    /// Represents a storage node.
    public struct StorageNode has key {
        id: UID,
        params: NodeParams,
    }

    public struct StorageNodeCap has key, store { id: UID }
    public struct NodeParams has copy, store, drop {}

    /// Creates a staking pool for the candidate.
    /// Node parameters, commission rate, proof of private key possession,
    /// votes for parameters for the next epoch
    public fun register_candidate(
        node_params: NodeParams,
        commission_rate: u64,
        proof_of_possession: vector<u8>,
        params_for_next_epoch: vector<NodeParams>,
    ): (StakingPool, StorageNodeCap) {
        abort ENotImplemented
    }

    /// Blocks staking for the node's staking pool.
    /// Marks node as `withdrawing`.
    public fun withdraw_node(cap: StorageNodeCap) {
        abort ENotImplemented
    }

    public fun update_node_info(params: NodeParams) {
        abort ENotImplemented
    }

    public fun set_next_commission(commission: u64) {
        abort ENotImplemented
    }

    public fun collect_commission(cap: &StorageNodeCapability) {
        abort ENotImplemented
    }

    public fun vote_for_next_epoch(
        cap: &StorageNodeCapability,
        storage_price: u64,
        write_price: u64,
        storage_capacity: u64,
    ) {}

    public fun vote_end() {}

    public fun epoch_sync_done() {}

    public fun shart_transfer_failed() {}

    public fun invalidate_blob_id() {}

    public fun certify_event_blob() {}
}
