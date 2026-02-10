// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::slashing_tests;

use std::unit_test::assert_eq;
use walrus::{storage_node, system, system_state_inner};

#[test]
fun test_slashing_vote_basic() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    // Get committee members IDs (each has 100 shards, 10 members total = 1000 shards)
    let node_one = system.committee().get_idx(0).node_id();
    let node_two = system.committee().get_idx(1).node_id();

    // Node one votes to slash node two
    let cap_one = storage_node::new_cap(node_one, ctx);
    system.vote_to_slash(&cap_one, node_two);

    // Check votes are recorded (should be 100 shards)
    let votes = system.get_slashing_votes(node_two);
    assert!(votes == 100);

    // Node two should not be slashed yet (need 667 shards for 1000 total, only have 100)
    let slashed = system.get_slashed_nodes();
    assert!(slashed.length() == 0);

    cap_one.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test]
fun test_slashing_votes_accumulate() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    // Get node IDs first
    let target_node = system.committee().get_idx(0).node_id();
    let voter_1 = system.committee().get_idx(1).node_id();
    let voter_2 = system.committee().get_idx(2).node_id();
    let voter_3 = system.committee().get_idx(3).node_id();
    let voter_4 = system.committee().get_idx(4).node_id();
    let voter_5 = system.committee().get_idx(5).node_id();
    let voter_6 = system.committee().get_idx(6).node_id();
    let voter_7 = system.committee().get_idx(7).node_id();

    // Create caps
    let cap_1 = storage_node::new_cap(voter_1, ctx);
    let cap_2 = storage_node::new_cap(voter_2, ctx);
    let cap_3 = storage_node::new_cap(voter_3, ctx);
    let cap_4 = storage_node::new_cap(voter_4, ctx);
    let cap_5 = storage_node::new_cap(voter_5, ctx);
    let cap_6 = storage_node::new_cap(voter_6, ctx);
    let cap_7 = storage_node::new_cap(voter_7, ctx);

    // Have 3 members vote (300 shards)
    system.vote_to_slash(&cap_1, target_node);
    system.vote_to_slash(&cap_2, target_node);
    system.vote_to_slash(&cap_3, target_node);

    // 300 shards, not enough yet for 667 threshold
    let slashed = system.get_slashed_nodes();
    assert!(slashed.length() == 0);

    // Check accumulated votes
    let votes = system.get_slashing_votes(target_node);
    assert!(votes == 300);

    // Add 4 more votes (400 shards)
    system.vote_to_slash(&cap_4, target_node);
    system.vote_to_slash(&cap_5, target_node);
    system.vote_to_slash(&cap_6, target_node);
    system.vote_to_slash(&cap_7, target_node);

    // Now 700 shards > 667 threshold
    let slashed = system.get_slashed_nodes();
    assert_eq!(slashed.length(), 1);
    assert!(slashed.contains(&target_node));

    // Cleanup
    cap_1.destroy_cap_for_testing();
    cap_2.destroy_cap_for_testing();
    cap_3.destroy_cap_for_testing();
    cap_4.destroy_cap_for_testing();
    cap_5.destroy_cap_for_testing();
    cap_6.destroy_cap_for_testing();
    cap_7.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test]
fun test_slashing_multiple_targets() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    // Get node IDs first
    let target_one = system.committee().get_idx(0).node_id();
    let target_two = system.committee().get_idx(1).node_id();
    let voter_1 = system.committee().get_idx(2).node_id();
    let voter_2 = system.committee().get_idx(3).node_id();
    let voter_3 = system.committee().get_idx(4).node_id();
    let voter_4 = system.committee().get_idx(5).node_id();
    let voter_5 = system.committee().get_idx(6).node_id();
    let voter_6 = system.committee().get_idx(7).node_id();
    let voter_7 = system.committee().get_idx(8).node_id();

    // Create caps
    let cap_1 = storage_node::new_cap(voter_1, ctx);
    let cap_2 = storage_node::new_cap(voter_2, ctx);
    let cap_3 = storage_node::new_cap(voter_3, ctx);
    let cap_4 = storage_node::new_cap(voter_4, ctx);
    let cap_5 = storage_node::new_cap(voter_5, ctx);
    let cap_6 = storage_node::new_cap(voter_6, ctx);
    let cap_7 = storage_node::new_cap(voter_7, ctx);

    // Have 7 members vote to slash target_one
    system.vote_to_slash(&cap_1, target_one);
    system.vote_to_slash(&cap_2, target_one);
    system.vote_to_slash(&cap_3, target_one);
    system.vote_to_slash(&cap_4, target_one);
    system.vote_to_slash(&cap_5, target_one);
    system.vote_to_slash(&cap_6, target_one);
    system.vote_to_slash(&cap_7, target_one);

    // target_one should be slashed
    let slashed = system.get_slashed_nodes();
    assert_eq!(slashed.length(), 1);
    assert!(slashed.contains(&target_one));

    // Have 7 members also vote to slash target_two
    system.vote_to_slash(&cap_1, target_two);
    system.vote_to_slash(&cap_2, target_two);
    system.vote_to_slash(&cap_3, target_two);
    system.vote_to_slash(&cap_4, target_two);
    system.vote_to_slash(&cap_5, target_two);
    system.vote_to_slash(&cap_6, target_two);
    system.vote_to_slash(&cap_7, target_two);

    // Both targets should now be slashed
    let slashed = system.get_slashed_nodes();
    assert_eq!(slashed.length(), 2);
    assert!(slashed.contains(&target_one));
    assert!(slashed.contains(&target_two));

    // Cleanup
    cap_1.destroy_cap_for_testing();
    cap_2.destroy_cap_for_testing();
    cap_3.destroy_cap_for_testing();
    cap_4.destroy_cap_for_testing();
    cap_5.destroy_cap_for_testing();
    cap_6.destroy_cap_for_testing();
    cap_7.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::EDuplicateSlashingVote)]
fun test_slashing_duplicate_vote_fails() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    let voter_id = system.committee().get_idx(0).node_id();
    let target_id = system.committee().get_idx(1).node_id();

    let cap = storage_node::new_cap(voter_id, ctx);

    // First vote should succeed
    system.vote_to_slash(&cap, target_id);

    // Second vote should fail
    system.vote_to_slash(&cap, target_id);

    cap.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::ECannotSlashSelf)]
fun test_slashing_self_fails() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    let node_id = system.committee().get_idx(0).node_id();

    let cap = storage_node::new_cap(node_id, ctx);

    // Trying to slash yourself should fail
    system.vote_to_slash(&cap, node_id);

    cap.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::ENotCommitteeMember)]
fun test_slashing_non_committee_member_fails() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    let target_id = system.committee().get_idx(0).node_id();

    // Create a cap with a fake node ID that's not in the committee
    let fake_node_id = ctx.fresh_object_address().to_id();
    let cap = storage_node::new_cap(fake_node_id, ctx);

    // Non-committee member trying to vote should fail
    system.vote_to_slash(&cap, target_id);

    cap.destroy_cap_for_testing();
    system.destroy_for_testing();
}

#[test, expected_failure(abort_code = system_state_inner::ENotCommitteeMember)]
fun test_slashing_target_not_in_committee_fails() {
    let ctx = &mut tx_context::dummy();
    let mut system = system::new_for_testing_with_multiple_members(ctx);

    let voter_id = system.committee().get_idx(0).node_id();

    let cap = storage_node::new_cap(voter_id, ctx);

    // Create a fake target ID that's not in the committee
    let fake_target_id = ctx.fresh_object_address().to_id();

    // Trying to slash a non-committee member should fail
    system.vote_to_slash(&cap, fake_target_id);

    cap.destroy_cap_for_testing();
    system.destroy_for_testing();
}
