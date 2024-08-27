// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// This module defines the `Committee` struct which stores the current
/// committee with shard assignments. Additionally, it manages transitions /
/// transfers of shards between committees with the least amount of changes.
module walrus::committee;

use sui::vec_map::{Self, VecMap};

/// Represents the current committee in the system. Each node in the committee
/// has assigned shard IDs.
public struct Committee(VecMap<ID, vector<u16>>) has store, copy, drop;

/// Creates an empty committee. Only relevant for epoch 0, when no nodes are
/// assigned any shards.
public(package) fun empty(): Committee { Committee(vec_map::empty()) }

/// Initializes the committee with the given `assigned_number` of shards per
/// node. Shards are assigned sequentially to each node.
public(package) fun initialize(assigned_number: VecMap<ID, u16>): Committee {
    let mut shard_idx: u16 = 0;
    let (keys, values) = assigned_number.into_keys_values();
    let cmt = vec_map::from_keys_values(
        keys,
        values.map!(
            |v| vector::tabulate!(
                v as u64,
                |_| {
                    let res = shard_idx;
                    shard_idx = shard_idx + 1;
                    res
                },
            ),
        ),
    );

    Committee(cmt)
}

/// Transitions the current committee to the new committee with the given shard
/// assignments. The function tries to minimize the number of changes by keeping
/// as many shards in place as possible.
///
/// This assumes that the number of shards in the new committee is equal to the
/// number of shards in the current committee. Check for this is not performed.
public(package) fun transition(cmt: &Committee, mut new_assignments: VecMap<ID, u16>): Committee {
    let mut new_cmt = vec_map::empty();
    let mut to_move = vector[];
    let size = cmt.0.size();

    size.do!(
        |idx| {
            let (node_id, prev_shards) = cmt.0.get_entry_by_idx(idx);
            let node_id = *node_id;
            let assigned_len = new_assignments
                .get_idx_opt(&node_id)
                .map!(
                    |idx| {
                        let (_, value) = new_assignments.remove_entry_by_idx(idx);
                        value as u64
                    },
                );

            // if the node is not in the new committee, remove all shards, make
            // them available for reassignment
            if (assigned_len.is_none() || assigned_len.borrow() == &0) {
                let shards = cmt.0.get(&node_id);
                to_move.append(*shards);
                return
            };

            let curr_len = prev_shards.length();
            let assigned_len = assigned_len.destroy_some();

            // node stays the same, we copy the shards over, best scenario
            if (curr_len == assigned_len) {
                new_cmt.insert(node_id, *prev_shards);
            };

            // if the node is in the new committee, check if the number of shards
            // assigned to the node has decreased. If so, remove the extra shards,
            // and move the node to the new committee
            if (curr_len > assigned_len) {
                let mut node_shards = *prev_shards;
                (curr_len - assigned_len).do!(|_| to_move.push_back(node_shards.pop_back()));
                new_cmt.insert(node_id, node_shards);
            };

            // if the node is in the new committee, and we already freed enough
            // shards from other nodes, perform the reassignment. Alternatively,
            // mark the node as needing more shards, so when we free up enough
            // shards, we can assign them to this node
            if (curr_len < assigned_len) {
                let diff = assigned_len - curr_len;
                if (to_move.length() >= diff) {
                    let mut node_shards = *prev_shards;
                    diff.do!(|_| node_shards.push_back(to_move.pop_back()));
                    new_cmt.insert(node_id, node_shards);
                } else {
                    // insert it back, we didn't have enough shards to assign
                    new_assignments.insert(node_id, assigned_len as u16);
                };
            };
        },
    );

    // Now the `new_assignments` only contains nodes for which we didn't have
    // enough shards to assign, and the nodes that were not part of the old
    // committee.
    let (keys, values) = new_assignments.into_keys_values();
    keys.zip_do!(
        values,
        |key, value| {
            if (value == 0) return; // ignore nodes with 0 shards

            let mut current_shards = cmt.0.try_get(&key).destroy_or!(vector[]);
            current_shards
                .length()
                .diff(value as u64)
                .do!(|_| current_shards.push_back(to_move.pop_back()));

            new_cmt.insert(key, current_shards);
        },
    );

    Committee(new_cmt)
}

#[syntax(index)]
/// Get the shards assigned to the given `node_id`.
public(package) fun shards(cmt: &Committee, node_id: &ID): &vector<u16> {
    cmt.0.get(node_id)
}

/// Get the number of nodes in the committee.
public(package) fun size(cmt: &Committee): u64 {
    cmt.0.size()
}

/// Get the inner representation of the committee.
public(package) fun inner(cmt: &Committee): &VecMap<ID, vector<u16>> {
    &cmt.0
}

/// Copy the inner representation of the committee.
public(package) fun to_inner(cmt: &Committee): VecMap<ID, vector<u16>> {
    cmt.0
}

#[test]
fun test_committee() {
    let n1 = @0x1.to_id();
    let n2 = @0x2.to_id();
    let n3 = @0x3.to_id();
    let n4 = @0x4.to_id();
    let n5 = @0x5.to_id();

    // Initialize the committee with 2 shards per node, 5 nodes, 10 shards in total
    let cmt = initialize(
        vec_map::from_keys_values(
            vector[n1, n2, n3, n4, n5],
            vector[2, 2, 2, 2, 2],
        ),
    );

    assert!(cmt.size() == 5);

    assert!(cmt[&n1] == &vector[0, 1]);
    assert!(cmt[&n2] == &vector[2, 3]);
    assert!(cmt[&n3] == &vector[4, 5]);
    assert!(cmt[&n4] == &vector[6, 7]);
    assert!(cmt[&n5] == &vector[8, 9]);

    // Transition the committee to 4/3 shards per node, 3 nodes, same number of shards
    let cmt2 = cmt.transition(
        vec_map::from_keys_values(
            vector[n1, n2, n3],
            vector[4, 3, 3],
        ),
    );

    assert!(cmt2.size() == 3);

    // we make sure that the shards this node had are still in place
    // repeat the checks for all nodes 1-3
    assert!(cmt2[&n1].length() == 4);
    assert!(cmt2[&n1].contains(&0));
    assert!(cmt2[&n1].contains(&1));

    assert!(cmt2[&n2].length() == 3);
    assert!(cmt2[&n2].contains(&2));
    assert!(cmt2[&n2].contains(&3));

    assert!(cmt2[&n3].length() == 3);
    assert!(cmt2[&n3].contains(&4));
    assert!(cmt2[&n3].contains(&5));

    // store shard assignments to check later
    let n2_shards = cmt2[&n2];
    let n3_shards = cmt2[&n3];

    // Transition the committee to 3, 3, 2, 2 for nodes 2-5 (removing node 1)
    let cmt3 = cmt2.transition(
        vec_map::from_keys_values(
            vector[n2, n3, n4, n5],
            vector[3, 3, 2, 2],
        ),
    );

    assert!(cmt3.size() == 4);

    // Make sure that n2 and n3 have the same shards as before
    assert!(cmt3[&n2] == n2_shards);
    assert!(cmt3[&n3] == n3_shards);

    // Make sure that n4 and n5 have correct number of shards
    assert!(cmt3[&n4].length() == 2);
    assert!(cmt3[&n5].length() == 2);

    // Transition the committee to just N1 owning all the shards
    let cmt4 = cmt3.transition(
        vec_map::from_keys_values(
            vector[n1],
            vector[10],
        ),
    );

    assert!(cmt4.size() == 1);
    assert!(cmt4[&n1].length() == 10);
    assert!(cmt4[&n1].contains(&0));
    assert!(cmt4[&n1].contains(&1));
    assert!(cmt4[&n1].contains(&2));
    assert!(cmt4[&n1].contains(&3));
    assert!(cmt4[&n1].contains(&4));
    assert!(cmt4[&n1].contains(&5));
    assert!(cmt4[&n1].contains(&6));
    assert!(cmt4[&n1].contains(&7));
    assert!(cmt4[&n1].contains(&8));
    assert!(cmt4[&n1].contains(&9));
}
