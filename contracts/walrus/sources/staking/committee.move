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

/// Initializes the committee with the given `assigned_number` of shards per
/// node. Shards are assigned sequentially to each node.
public fun initialize(assigned_number: VecMap<ID, u16>): Committee {
    let mut shard_idx: u16 = 0;
    let (keys, values) = assigned_number.into_keys_values();
    let cmt = vec_map::from_keys_values(
        keys,
        values.map!(|v| vector::tabulate!(v as u64, |_| {
            let res = shard_idx;
            shard_idx = shard_idx + 1;
            res
        })),
    );

    Committee(cmt)
}

/// Transitions the current committee to the new committee with the given shard
/// assignments. The function tries to minimize the number of changes by keeping
/// as many shards in place as possible.
public fun transition(cmt: &mut Committee, assigned_number: VecMap<ID, u16>): Committee {
    let mut new_cmt = vec_map::empty();
    let mut to_move = vector[];
    let mut revisit = vector[];
    let size = cmt.0.size();

    // first remove all shards that are not in the new committee
    size.do!(
        |idx| {
            let (node_id, node_shards) = cmt.0.get_entry_by_idx_mut(idx);
            let node_id = *node_id;

            // if the node is not in the new committee, remove all shards, make
            // them available for reassignment
            if (!assigned_number.contains(&node_id)) {
                let shards = cmt.0.get(&node_id);
                to_move.append(*shards);
                return
            };

            let curr_len = node_shards.length();
            let assigned_len = *assigned_number.get(&node_id) as u64;

            // if the node is in the new committee, check if the number of shards
            // assigned to the node has decreased. If so, remove the extra shards,
            // and move the node to the new committee
            if (curr_len > assigned_len) {
                (curr_len - assigned_len).do!(|_| to_move.push_back(node_shards.pop_back()));
                let (_, shards) = cmt.0.remove(&node_id);
                new_cmt.insert(node_id, shards);
                return
            };

            // if the node is in the new committee, and we already freed enough
            // shards from other nodes, perform the reassignment. Alternatively,
            // mark the node as needing more shards, so when we free up enough
            // shards, we can assign them to this node
            if (curr_len < assigned_len) {
                let diff = assigned_len - curr_len;
                if (to_move.length() >= diff) {
                    diff.do!(|_| node_shards.push_back(to_move.pop_back()));
                    new_cmt.insert(node_id, *node_shards);
                } else {
                    revisit.push_back(node_id);
                };

                return
            };
        },
    );

    // revisit nodes that need more shards; we expect all shards to be available
    // at this point
    revisit.do!(|node_id| {
        let node_shards = cmt.0.get_mut(&node_id);
        let assigned_len = *assigned_number.get(&node_id) as u64;
        let diff = assigned_len - node_shards.length();
        diff.do!(|_| node_shards.push_back(to_move.pop_back()));
        new_cmt.insert(node_id, *node_shards);
    });

    Committee(new_cmt)
}

#[syntax(index)]
/// Get the shards assigned to the given `node_id`.
public fun shards(cmt: &Committee, node_id: &ID): &vector<u16> {
    cmt.0.get(node_id)
}

#[test]
fun test_committee() {

    let n1 = @0x1.to_id();
    let n2 = @0x2.to_id();
    let n3 = @0x3.to_id();
    let n4 = @0x4.to_id();
    let n5 = @0x5.to_id();

    // Initialize the committee with 2 shards per node, 5 nodes, 10 shards in total
    let mut cmt = initialize(vec_map::from_keys_values(
        vector[n1, n2, n3, n4, n5],
        vector[2, 2, 2, 2, 2],
    ));

    assert!(cmt[&n1] == &vector[0, 1]);
    assert!(cmt[&n2] == &vector[2, 3]);
    assert!(cmt[&n3] == &vector[4, 5]);
    assert!(cmt[&n4] == &vector[6, 7]);
    assert!(cmt[&n5] == &vector[8, 9]);


    // Transition the committee to 4/3 shards per node, 3 nodes, same number of shards
    let cmt2 = cmt.transition(vec_map::from_keys_values(
        vector[n1, n2, n3],
        vector[4, 3, 3],
    ));

    assert!(cmt2[&n1].length() == 4);
    assert!(cmt2[&n1].contains(&0));
    assert!(cmt2[&n1].contains(&1));

    assert!(cmt2[&n2].length() == 3);
    assert!(cmt2[&n2].contains(&2));
    assert!(cmt2[&n2].contains(&3));

    assert!(cmt2[&n3].length() == 3);
    assert!(cmt2[&n3].contains(&4));
    assert!(cmt2[&n3].contains(&5));
}
