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
    /// The minimum amount of staked WAL needed to enter the active set. This is used to
    /// determine if a storage node can be added to the active set.
    threshold_stake: u64,
    /// The amount of staked WAL for each storage node in the active set.
    nodes: VecMap<ID, u64>,
    /// Stores indexes of the nodes in the active set sorted by stake. This
    /// allows us to quickly find the index of a node in the sorted list of
    /// nodes. Uses `u16` to save space, as the active set can only contain up
    /// to 1000 nodes.
    idx_sorted: vector<u16>,
    /// The total amount of staked WAL in the active set.
    total_stake: u64,
}

/// Creates a new active set with the given `size` and `threshold_stake`. The
/// latter is used to filter out storage nodes that do not have enough staked
/// WAL to be included in the active set initially.
public(package) fun new(max_size: u16, threshold_stake: u64): ActiveSet {
    assert!(max_size > 0);
    ActiveSet {
        max_size,
        threshold_stake,
        nodes: vec_map::empty(),
        idx_sorted: vector[],
        total_stake: 0,
    }
}

/// Performs the `insert` if the node is not in the active set, otherwise calls
/// the `update`.
/// Returns true if the node is in the set, false otherwise.
public(package) fun insert_or_update(set: &mut ActiveSet, node_id: ID, staked_amount: u64): bool {
    if (set.nodes.contains(&node_id)) {
        update(set, node_id, staked_amount);
        true
    } else {
        insert(set, node_id, staked_amount)
    }
}

/// Updates the staked amount of the storage node with the given `node_id` in
/// the active set.
public(package) fun update(set: &mut ActiveSet, node_id: ID, staked_amount: u64) {
    let idx = set.nodes.get_idx(&node_id);
    let (_, old_stake) = set.nodes.remove(&node_id);
    set.total_stake = set.total_stake - old_stake;
    set.idx_sorted.remove(idx);

    insert(set, node_id, staked_amount);
}

/// Inserts a storage node with the given `node_id` and `staked_amount` into the
/// active set. The node is only added if it has enough staked WAL to be included
/// in the active set. If the active set is full, the node with the smallest
/// staked WAL is removed to make space for the new node.
/// Returns true if the node was inserted, false otherwise.
public(package) fun insert(set: &mut ActiveSet, node_id: ID, staked_amount: u64): bool {
    assert!(!set.nodes.contains(&node_id));

    // check if the staked amount is enough to be included in the active set
    if (staked_amount < set.threshold_stake) return false;

    // If the nodes are less than the max size, insert the node
    if (set.nodes.size() as u16 < set.max_size) {
        insert_impl(set, node_id, staked_amount);
        true
    } else {
        // find the node with the smallest staked WAL
        let (min_node_id, cur_min_stake) = set.nodes.get_entry_by_idx(set.idx_sorted[0] as u64);
        if (staked_amount > *cur_min_stake) {
            // Remove the smallest node and decrease the total stake
            let (_, min_stake) = set.nodes.remove(&*min_node_id);
            set.total_stake = set.total_stake - min_stake;

            // insert the new node as if the set was not full
            insert_impl(set, node_id, staked_amount);
            true
        } else {
            false
        }
    }
}

fun insert_impl(set: &mut ActiveSet, node_id: ID, staked_amount: u64) {
    // happy path for the first node, no need to sort, just insert
    if (set.nodes.size() == 0) {
        set.total_stake = set.total_stake + staked_amount;
        set.nodes.insert(node_id, staked_amount);
        set.idx_sorted.push_back(0);
        return
    };

    assert!(set.nodes.size() as u16 < set.max_size); // internal invariant
    set.total_stake = set.total_stake + staked_amount;
    set.nodes.insert(node_id, staked_amount);
    let map_idx = set.nodes.size() as u16 - 1;
    let insert_idx = set
        .idx_sorted
        .find_index!(
            |idx| {
                let (_node_id, stake) = set.nodes.get_entry_by_idx(*idx as u64);
                staked_amount > *stake
            },
        )
        .destroy_or!(set.idx_sorted.length());
    set.idx_sorted.insert(insert_idx as u16, map_idx as u64);
}

/// Removes the storage node with the given `node_id` from the active set.
public(package) fun remove(set: &mut ActiveSet, node_id: ID) {
    if (!set.nodes.contains(&node_id)) return;

    let idx = set.nodes.get_idx(&node_id);
    let (_, staked_amount) = set.nodes.remove(&node_id);

    set.idx_sorted.remove(idx);
    set.total_stake = set.total_stake - staked_amount;
}

/// The maximum size of the active set.
public(package) fun max_size(set: &ActiveSet): u16 { set.max_size }

/// The current size of the active set.
public(package) fun size(set: &ActiveSet): u16 { set.nodes.size() as u16 }

/// The total amount of staked WAL in the active set.
public(package) fun active_ids(set: &ActiveSet): vector<ID> { set.nodes.keys() }

/// The minimum amount of staked WAL in the active set.
public(package) fun threshold_stake(set: &ActiveSet): u64 { set.threshold_stake }

/// The total amount of staked WAL in the active set.
public(package) fun total_stake(set: &ActiveSet): u64 { set.total_stake }

/// Current minimum stake needed to be in the active set.
/// If the active set is full, the minimum stake is the stake of the node with the smallest stake.
/// Otherwise, the minimum stake is the threshold stake.
public(package) fun cur_min_stake(set: &ActiveSet): u64 {
    if (set.nodes.size() == set.max_size as u64) {
        let (_, stake) = set.nodes.get_entry_by_idx(set.idx_sorted[0] as u64);
        *stake
    } else {
        set.threshold_stake
    }
}

#[syntax(index)]
/// Get the staked amount of the storage node with the given `node_id`.
public(package) fun borrow(set: &ActiveSet, node_id: &ID): &u64 {
    &set.nodes[node_id]
}

/// Get the `node_id` by the given `idx`. The `idx` is the index of the node in
/// the sorted list of nodes, meaning that `0` is the node with the highest
/// staked WAL, `1` is the second highest, and so on.
public(package) fun get_by_stake_idx(set: &ActiveSet, idx: u64): &ID {
    let idx = set.idx_sorted[idx];
    let (node_id, _) = set.nodes.get_entry_by_idx(idx as u64);
    node_id
}

#[test]
fun test_insert() {
    use std::unit_test::assert_eq;

    let mut set = new(3, 100);
    set.insert(@0x1.to_id(), 200);
    set.insert(@0x2.to_id(), 300);
    set.insert(@0x3.to_id(), 400);

    assert_eq!(set.size(), 3);
    assert_eq!(set.max_size(), 3);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x1.to_id()));
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert_eq!(set.cur_min_stake(), 200);

    // now insert a node with even more staked WAL
    set.insert(@0x4.to_id(), 500);

    assert_eq!(set.size(), 3);
    assert_eq!(set.cur_min_stake(), 300);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert!(active_ids.contains(&@0x4.to_id()));

    // and now insert a node with less staked WAL
    set.insert(@0x5.to_id(), 250);

    assert_eq!(set.size(), 3);
    assert_eq!(set.cur_min_stake(), 300);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x2.to_id()));
    assert!(active_ids.contains(&@0x3.to_id()));
    assert!(active_ids.contains(&@0x4.to_id()));

    // and now insert 3 more nodes with super high staked WAL
    set.insert(@0x6.to_id(), 1000);
    set.insert(@0x7.to_id(), 1000);
    set.insert(@0x8.to_id(), 1000);

    assert_eq!(set.size(), 3);
    assert_eq!(set.cur_min_stake(), 1000);

    let active_ids = set.active_ids();
    assert!(active_ids.contains(&@0x6.to_id()));
    assert!(active_ids.contains(&@0x7.to_id()));
    assert!(active_ids.contains(&@0x8.to_id()));
}

#[test]
fun test_size_1() {
    use std::unit_test::assert_eq;

    let mut set = new(1, 100);
    assert_eq!(set.cur_min_stake(), 100);
    set.insert(@0x1.to_id(), 1000);
    assert_eq!(set.cur_min_stake(), 1000);
    set.insert(@0x2.to_id(), 1001);
    assert_eq!(set.cur_min_stake(), 1001);
}
