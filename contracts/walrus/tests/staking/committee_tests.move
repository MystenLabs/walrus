// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

module walrus::committee_tests;

use sui::{address, vec_map};
use walrus::committee;

#[test]
fun empty_committee() {
    let cmt = committee::empty();
    assert!(cmt.size() == 0);
}

#[test]
fun default_scenario() {
    let n1 = @0x1.to_id();
    let n2 = @0x2.to_id();
    let n3 = @0x3.to_id();
    let n4 = @0x4.to_id();
    let n5 = @0x5.to_id();

    // Initialize the committee with 2 shards per node, 5 nodes, 10 shards in total
    let cmt = committee::initialize(
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

#[test]
fun ignore_empty_assignments() {
    let (n1, n2, n3, n4, n5) = (
        @0x1.to_id(),
        @0x2.to_id(),
        @0x3.to_id(),
        @0x4.to_id(),
        @0x5.to_id(),
    );

    // expect n4 and n5 to be ignored
    let cmt = committee::initialize(
        vec_map::from_keys_values(
            vector[n1, n2, n3, n4, n5],
            vector[2, 2, 2, 0, 0],
        ),
    );

    // expect n1 and n5 to be ignored
    let cmt2 = cmt.transition(
        vec_map::from_keys_values(
            vector[n1, n2, n3, n4, n5],
            vector[0, 2, 2, 2, 0],
        ),
    );

    assert!(cmt2.size() == 3);
    assert!(cmt.shards(&n2) == cmt2.shards(&n2));
    assert!(cmt.shards(&n3) == cmt2.shards(&n3));
}

// #[test] // requires manual --gas-limit set, ignored for convenience
#[allow(unused_function)]
fun large_set_assignments_1() {
    let nodes = vector::tabulate!(1000, |i| address::from_u256(i as u256).to_id());
    let assignments = vec_map::from_keys_values(nodes, vector::tabulate!(1000, |_| 1));
    let _cmt = committee::initialize(assignments);
}

// #[test] // requires manual --gas-limit set, ignored for convenience
#[allow(unused_function)]
fun large_set_assignments_2() {
    let nodes = vector::tabulate!(1000, |i| address::from_u256(i as u256).to_id());
    let assignments = vec_map::from_keys_values(nodes, vector::tabulate!(1000, |_| 1));

    let cmt = committee::initialize(assignments);
    cmt.transition(
        vec_map::from_keys_values(
            nodes,
            vector::tabulate!(1000, |i| (i % 3) as u16),
        ),
    );
}
