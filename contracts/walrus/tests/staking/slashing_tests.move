// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

module walrus::slashing_tests;

use std::unit_test::assert_eq;
use sui::{test_scenario, vec_map};
use wal::wal::ProtectedTreasury;
use walrus::{storage_node, system, system_state_inner, test_utils};

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

#[test]
fun test_apply_slashing_burns_rewards() {
    let admin = @0xAD;
    let mut scenario = test_scenario::begin(admin);

    // Initialize WAL token to get ProtectedTreasury
    wal::wal::init_for_testing(scenario.ctx());
    scenario.next_tx(admin);

    // Take the treasury
    let mut treasury = scenario.take_shared<ProtectedTreasury>();
    let initial_supply = treasury.total_supply();

    // Create system with test committee
    let mut system = system::new_for_testing_with_multiple_members(scenario.ctx());

    // Get node IDs
    let slashed_node = system.committee().get_idx(0).node_id();
    let normal_node_1 = system.committee().get_idx(1).node_id();
    let normal_node_2 = system.committee().get_idx(2).node_id();
    let voter_1 = system.committee().get_idx(3).node_id();
    let voter_2 = system.committee().get_idx(4).node_id();
    let voter_3 = system.committee().get_idx(5).node_id();
    let voter_4 = system.committee().get_idx(6).node_id();
    let voter_5 = system.committee().get_idx(7).node_id();
    let voter_6 = system.committee().get_idx(8).node_id();
    let voter_7 = system.committee().get_idx(9).node_id();

    // Create caps and vote to slash the target (7 voters = 700 shards > 667 threshold)
    let cap_1 = storage_node::new_cap(voter_1, scenario.ctx());
    let cap_2 = storage_node::new_cap(voter_2, scenario.ctx());
    let cap_3 = storage_node::new_cap(voter_3, scenario.ctx());
    let cap_4 = storage_node::new_cap(voter_4, scenario.ctx());
    let cap_5 = storage_node::new_cap(voter_5, scenario.ctx());
    let cap_6 = storage_node::new_cap(voter_6, scenario.ctx());
    let cap_7 = storage_node::new_cap(voter_7, scenario.ctx());

    system.vote_to_slash(&cap_1, slashed_node);
    system.vote_to_slash(&cap_2, slashed_node);
    system.vote_to_slash(&cap_3, slashed_node);
    system.vote_to_slash(&cap_4, slashed_node);
    system.vote_to_slash(&cap_5, slashed_node);
    system.vote_to_slash(&cap_6, slashed_node);
    system.vote_to_slash(&cap_7, slashed_node);

    // Verify node is slashed
    let slashed = system.get_slashed_nodes();
    assert_eq!(slashed.length(), 1);
    assert!(slashed.contains(&slashed_node));

    // Create rewards map: slashed node gets 1000 WAL, normal nodes get 500 WAL each
    let slashed_reward_amount = 1000;
    let normal_reward_amount = 500;
    let mut rewards = vec_map::empty();
    rewards.insert(slashed_node, test_utils::mint_wal_balance(slashed_reward_amount));
    rewards.insert(normal_node_1, test_utils::mint_wal_balance(normal_reward_amount));
    rewards.insert(normal_node_2, test_utils::mint_wal_balance(normal_reward_amount));

    // Apply slashing - this should burn the slashed node's rewards
    let result = system.apply_slashing(rewards, &mut treasury, scenario.ctx());

    // Verify results:
    // 1. Slashed node should have zero balance
    let slashed_balance = result.get(&slashed_node);
    assert_eq!(slashed_balance.value(), 0);

    // 2. Normal nodes should keep their full rewards
    let normal_1_balance = result.get(&normal_node_1);
    assert_eq!(normal_1_balance.value(), test_utils::frost_per_wal() * normal_reward_amount);

    let normal_2_balance = result.get(&normal_node_2);
    assert_eq!(normal_2_balance.value(), test_utils::frost_per_wal() * normal_reward_amount);

    // 3. Total supply should decrease by the burned amount
    let expected_burned = test_utils::frost_per_wal() * slashed_reward_amount;
    assert_eq!(treasury.total_supply(), initial_supply - expected_burned);

    // 4. Slashing votes should be cleared after apply_slashing
    assert_eq!(system.get_slashing_votes(slashed_node), 0);
    assert_eq!(system.get_slashed_nodes().length(), 0);

    // Cleanup
    let (_, balances) = result.into_keys_values();
    balances.do!(|b| b.destroy_for_testing());

    cap_1.destroy_cap_for_testing();
    cap_2.destroy_cap_for_testing();
    cap_3.destroy_cap_for_testing();
    cap_4.destroy_cap_for_testing();
    cap_5.destroy_cap_for_testing();
    cap_6.destroy_cap_for_testing();
    cap_7.destroy_cap_for_testing();
    system.destroy_for_testing();
    test_scenario::return_shared(treasury);
    scenario.end();
}
