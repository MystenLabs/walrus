// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_field, unused_function, unused_variable)]
module walrus::storage_node {
    use std::string::String;
    use walrus::staking_pool::{Self, StakingPool};

    const ENotImplemented: u64 = 0;

    public struct StorageNodeInfo has store, drop {}

    public enum NodeState has store, drop {
        Active,
        Withdrawing,
        Custom(String),
    }

    /// Represents a storage node.
    public struct StorageNode has key {
        id: UID,
        info: StorageNodeInfo,
        state: NodeState,
    }

    /// The capability to manage the storage node.
    public struct StorageNodeCap has key, store {
        id: UID,
        node_id: ID,
    }

    /// Creates a staking pool for the candidate.
    /// Node parameters, commission rate, proof of private key possession,
    /// votes for parameters for the next epoch
    public fun register_candidate<T>(
        node_params: StorageNodeInfo,
        commission_rate: u64,
        proof_of_possession: vector<u8>,
        ctx: &mut TxContext,
    ): (StakingPool<T>, StorageNodeCap) {
        let pool = staking_pool::new(ctx);
        let cap = StorageNodeCap {
            id: object::new(ctx),
            node_id: object::id(&pool),
        };

        (pool, cap)
    }

    /// Blocks staking for the node's staking pool.
    /// Marks node as `withdrawing`.
    public fun withdraw_node(node: &mut StorageNode, cap: StorageNodeCap) {
        node.state = NodeState::Withdrawing;

        abort ENotImplemented
    }

    // public fun update_node_info(params: NodeParams) {
    //     abort ENotImplemented
    // }

    public fun set_next_commission(commission: u64) {
        abort ENotImplemented
    }

    public fun collect_commission(cap: &StorageNodeCap) {
        abort ENotImplemented
    }

    public fun vote_for_next_epoch(
        cap: &StorageNodeCap,
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
