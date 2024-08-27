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
module walrus::active_set_tests;

use sui::test_utils::assert_eq;
use walrus::active_set;


#[test]
fun test_insert() {
    use std::unit_test::assert_eq;

    let mut set = active_set::new(3, 100);
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

    let mut set = active_set::new(1, 100);
    assert_eq!(set.cur_min_stake(), 100);
    set.insert(@0x1.to_id(), 1000);
    assert_eq!(set.cur_min_stake(), 1000);
    set.insert(@0x2.to_id(), 1001);
    assert_eq!(set.cur_min_stake(), 1001);
}
