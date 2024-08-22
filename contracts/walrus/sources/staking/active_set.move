// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// Contains an active set of storage nodes. The active set is a smart collection
/// that only stores up to a 1000 nodes. The nodes are sorted by the amount of
/// staked WAL. Additionally, the active set tracks the total amount of staked
/// WAL to make the calculation of the rewards and voting power distribution easier.
///
/// TODOs:
/// - consider using a different data structure for the active set (#714)
/// - consider removing `min_stake` field, use threshold from number of
///   shards and total_staked (#715)
module walrus::active_set;

use sui::vec_map::{Self, VecMap};

/// The active set of storage nodes, a smart collection that only stores up
/// to a 1000 nodes. The nodes are sorted by the amount of staked WAL.
/// Additionally, the active set tracks the total amount of staked WAL to make
/// the calculation of the rewards and voting power distribution easier.
///
/// TODO: implement a reserve to track N + K nodes, where N is the active set
/// size and K is the number of nodes that are in the process of being added to
/// the active set. This will allow us to handle removals from the active set
/// without refetching the nodes from the storage.
public struct ActiveSet has store, copy, drop {
    /// The maximum number of storage nodes in the active set.
    /// Potentially remove this field.
    max_size: u16,
    /// The minimum amount of staked WAL in the active set. This is used to
    /// determine if a storage node can be added to the active set.
    min_stake: u64,
    /// The storage nodes in the active set, sorted by the amount of staked WAL.
    nodes: VecMap<ID, u64>,
    /// The total amount of staked WAL in the active set.
    total_staked: u64,
    /// Stores indexes of the nodes in the active set sorted by stake. This
    /// allows us to quickly find the index of a node in the sorted list of
    /// nodes. Uses `u16` to save space, as the active set can only contain up
    /// to 1000 nodes.
    idx_sorted: vector<u16>,
}

/// Creates a new active set with the given `size` and `min_stake`. The
/// latter is used to filter out storage nodes that do not have enough staked
/// WAL to be included in the active set initially.
public(package) fun new(max_size: u16, min_stake: u64): ActiveSet {
    ActiveSet {
        max_size,
        min_stake,
        nodes: vec_map::empty(),
        // sorted_nodes: vector[],
        total_staked: 0,
        idx_sorted: vector[],
    }
}

public(package) fun insert(set: &mut ActiveSet, node_id: ID, staked_amount: u64) {
    assert!(!set.nodes.contains(&node_id));

    // check if the staked amount is enough to be included in the active set
    if (staked_amount < set.min_stake) return;

    // happy path for the first node, no need to sort, just insert
    if (set.nodes.size() == 0) {
        set.total_staked = set.total_staked + staked_amount;
        set.nodes.insert(node_id, staked_amount);
        set.idx_sorted.push_back(0);
        return
    };

    //
    if (set.nodes.size() as u16 < set.max_size) {
        set.total_staked = set.total_staked + staked_amount;
        set.nodes.insert(node_id, staked_amount);

        let map_idx = set.nodes.size() as u16 - 1;
        let insert_idx = set
            .idx_sorted
            .find_index!(
                |idx| {
                    let (_node_id, stake) = set.nodes.get_entry_by_idx(*idx as u64);
                    staked_amount > *stake
                },
            );

        if (insert_idx.is_some()) {
            insert_idx.do!(|idx| set.idx_sorted.insert(idx as u16, map_idx as u64));
        } else {
            set.idx_sorted.push_back(map_idx);
        };

        if (set.nodes.size() as u16 == set.max_size) {
            let (_node_id, stake) = set.nodes.get_entry_by_idx(set.idx_sorted[0] as u64);
            set.min_stake = *stake;
        }
    } else if (staked_amount > set.min_stake) {
        // find the node with the smallest staked WAL
        let (min_node_id, _) = set.nodes.get_entry_by_idx(set.idx_sorted[0] as u64);
        let min_node_id = *min_node_id;
        let (_, min_stake) = set.nodes.remove(&min_node_id);

        // decrease the total staked WAL
        set.total_staked = set.total_staked - min_stake;

        // insert the new node as if the set was not full
        insert(set, node_id, staked_amount);
    }

    // or operation didn't happen
}

/// The maximum size of the active set.
public(package) fun max_size(set: &ActiveSet): u16 { set.max_size }

/// The current size of the active set.
public(package) fun size(set: &ActiveSet): u16 { set.nodes.size() as u16 }

/// The total amount of staked WAL in the active set.
public(package) fun active_ids(set: &ActiveSet): vector<ID> { set.nodes.keys() }

/// The minimum amount of staked WAL in the active set.
public(package) fun min_stake(set: &ActiveSet): u64 { set.min_stake }

/// The total amount of staked WAL in the active set.
public(package) fun total_staked(set: &ActiveSet): u64 { set.total_staked }

#[test]
fun test_insert() {
    use sui::test_utils::assert_eq;

    let mut set = new(3, 100);
    set.insert(@0x1.to_id(), 200);
    set.insert(@0x2.to_id(), 300);
    set.insert(@0x3.to_id(), 400);

    assert_eq(set.size(), 3);
    assert_eq(set.max_size(), 3);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x1.to_id()));
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert_eq(set.min_stake(), 200);

    // now insert a node with even more staked WAL
    set.insert(@0x4.to_id(), 500);

    assert_eq(set.size(), 3);
    assert_eq(set.min_stake(), 300);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert!(active_ids.contains(&@0x4.to_id()));

    // and now insert a node with less staked WAL
    set.insert(@0x5.to_id(), 250);

    assert_eq(set.size(), 3);
    assert_eq(set.min_stake(), 300);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert!(active_ids.contains(&@0x4.to_id()));

    // and now insert 3 more nodes with super high staked WAL
    set.insert(@0x6.to_id(), 1000);
    set.insert(@0x7.to_id(), 1000);
    set.insert(@0x8.to_id(), 1000);

    assert_eq(set.size(), 3);
    assert_eq(set.min_stake(), 1000);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x6.to_id()));
    assert!(active_ids.contains(&@0x7.to_id()));
    assert!(active_ids.contains(&@0x8.to_id()));
}
