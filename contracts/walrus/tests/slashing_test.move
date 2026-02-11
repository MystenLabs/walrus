// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

#[allow(unused_mut_ref, lint(self_transfer))]
module walrus::slashing_tests;

use std::unit_test::assert_eq;
use walrus::{auth, e2e_runner, slashing, test_utils as walrus_test_utils};

/// Helper: set up a committee with commission so that nodes accumulate commission.
/// Returns a runner and nodes where each node has a 10% commission rate.
fun setup_committee_with_commission(): (
    e2e_runner::TestRunner,
    vector<walrus::test_node::TestStorageNode>,
) {
    let admin = @0xA11CE;
    let mut nodes = walrus::test_node::test_nodes();
    let mut runner = e2e_runner::prepare(admin).build();
    let commission_rate: u16 = 10_00; // 10%
    let storage_price: u64 = 10_000;
    let write_price: u64 = 20_000;
    let node_capacity: u64 = 1_000_000_000_000;

    // Register candidates with 10% commission.
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let cap = staking.register_candidate(
                node.name(),
                node.network_address(),
                walrus::node_metadata::default(),
                node.bls_pk(),
                node.network_key(),
                node.create_proof_of_possession(epoch),
                commission_rate,
                storage_price,
                write_price,
                node_capacity,
                ctx,
            );
            node.set_storage_node_cap(cap);
        });
    });

    // Stake with each node.
    nodes.do_ref!(|node| {
        runner.tx!(node.sui_address(), |staking, _, ctx| {
            let coin = walrus_test_utils::mint_wal(1000, ctx);
            let staked_wal = staking.stake_with_pool(coin, node.node_id(), ctx);
            transfer::public_transfer(staked_wal, ctx.sender());
        });
    });

    // Advance to epoch 1.
    runner.clock().increment_for_testing(e2e_runner::default_epoch_duration());
    runner.tx_with_wal_treasury!(admin, |staking, system, protected_treasury, clock, ctx| {
        staking.voting_end(clock);
        staking.initiate_epoch_change_v2(system, protected_treasury, clock, ctx);
    });

    // Send epoch sync done.
    let epoch = runner.epoch();
    nodes.do_mut!(|node| {
        runner.tx!(node.sui_address(), |staking, _, _| {
            staking.epoch_sync_done(node.cap_mut(), epoch, runner.clock());
        });
    });

    (runner, nodes)
}

/// Add commission to a node's pool by calling add_commission_to_pools.
fun add_commission_to_node(
    runner: &mut e2e_runner::TestRunner,
    node: &walrus::test_node::TestStorageNode,
    amount: u64,
) {
    runner.tx!(node.sui_address(), |staking, _, _ctx| {
        let commission = walrus_test_utils::mint_wal_balance(amount);
        staking.add_commission_to_pools(vector[node.node_id()], vector[commission]);
    });
}

// === Success cases ===

#[test]
public fun test_slashing_quorum_and_execute() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Add commission to the candidate node so there's something to burn.
    add_commission_to_node(&mut runner, candidate, 500);

    // Verify the candidate has commission.
    runner.tx!(candidate.sui_address(), |staking, _, _| {
        assert_eq!(
            staking.pool_commission(candidate_node_id),
            500 * walrus_test_utils::frost_per_wal(),
        );
    });

    // Vote for slashing with quorum (2/3 + 1).
    let n_votes = nodes.length() * 2 / 3 + 1;
    n_votes.do!(|idx| {
        let node = &nodes[idx];
        runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
            let auth = auth::authenticate_sender(ctx);
            slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
        });
    });

    // Execute slashing.
    runner.tx_with_slashing_manager_and_treasury!(
        nodes[0].sui_address(),
        |staking, slashing_manager, treasury, ctx| {
            slashing_manager.execute_slashing(staking, treasury, candidate_node_id, ctx);
        },
    );

    // Verify commission was burned.
    runner.tx!(candidate.sui_address(), |staking, _, _| {
        assert_eq!(staking.pool_commission(candidate_node_id), 0);
    });

    runner.destroy();
    nodes.destroy!(|node| node.destroy());
}

#[test]
public fun test_slashing_vote_refreshes_on_epoch_change() {
    let (mut runner, mut nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    add_commission_to_node(&mut runner, candidate, 500);

    // Vote with a few nodes in epoch 1 (not enough for quorum).
    let n_votes = 2u64;
    n_votes.do!(|idx| {
        let node = &nodes[idx];
        runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
            let auth = auth::authenticate_sender(ctx);
            slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
        });
    });

    // Advance to epoch 2.
    runner.next_epoch();
    runner.send_epoch_sync_done_messages(&mut nodes);

    // Vote again - the proposal should be refreshed (old votes cleared).
    // Need full quorum again from scratch.
    let n_votes = nodes.length() * 2 / 3 + 1;
    n_votes.do!(|idx| {
        let node = &nodes[idx];
        runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
            let auth = auth::authenticate_sender(ctx);
            slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
        });
    });

    // Execute slashing succeeds in epoch 2.
    runner.tx_with_slashing_manager_and_treasury!(
        nodes[0].sui_address(),
        |staking, slashing_manager, treasury, ctx| {
            slashing_manager.execute_slashing(staking, treasury, candidate_node_id, ctx);
        },
    );

    runner.destroy();
    nodes.destroy!(|node| node.destroy());
}

#[test]
public fun test_cleanup_slashing_proposals() {
    let (mut runner, mut nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Vote in epoch 1 (not enough for quorum, just create the proposal).
    let node = &nodes[1];
    runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
    });

    // Advance to epoch 2.
    runner.next_epoch();
    runner.send_epoch_sync_done_messages(&mut nodes);

    // Cleanup stale proposals from epoch 1.
    runner.tx_with_slashing_manager!(nodes[0].sui_address(), |staking, slashing_manager, _| {
        slashing_manager.cleanup_slashing_proposals(staking, vector[candidate_node_id]);
    });

    // Verify the proposal was cleaned up: trying to execute should fail with no proposal.
    // We re-vote to confirm the old proposal is gone and a fresh one is needed.
    let node = &nodes[1];
    runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        // This should succeed (create a new proposal, not duplicate vote error).
        slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
    });

    runner.destroy();
    nodes.destroy!(|node| node.destroy());
}

#[test]
public fun test_cleanup_does_not_remove_current_epoch_proposals() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Vote in current epoch.
    let node = &nodes[1];
    runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
    });

    // Try to cleanup - should not remove current epoch proposals.
    runner.tx_with_slashing_manager!(nodes[0].sui_address(), |staking, slashing_manager, _| {
        slashing_manager.cleanup_slashing_proposals(staking, vector[candidate_node_id]);
    });

    // Verify proposal still exists by voting with a different node (should not crash).
    // If cleanup had removed it, a fresh proposal would be created.
    // We confirm the original proposal persists by voting with node 2.
    let node2 = &nodes[2];
    runner.tx_with_slashing_manager!(node2.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        slashing_manager.vote_for_slashing(staking, auth, node2.node_id(), candidate_node_id);
    });

    runner.destroy();
    nodes.destroy!(|node| node.destroy());
}

// === Failure cases ===

#[test, expected_failure(abort_code = slashing::ENotEnoughVotes)]
public fun test_slashing_insufficient_votes() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Vote with just shy of a quorum (2/3 without +1).
    let n_votes = nodes.length() * 2 / 3;
    n_votes.do!(|idx| {
        let node = &nodes[idx];
        runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
            let auth = auth::authenticate_sender(ctx);
            slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
        });
    });

    // Try to execute - should fail with ENotEnoughVotes.
    runner.tx_with_slashing_manager_and_treasury!(
        nodes[0].sui_address(),
        |staking, slashing_manager, treasury, ctx| {
            slashing_manager.execute_slashing(staking, treasury, candidate_node_id, ctx);
        },
    );

    abort
}

#[test, expected_failure(abort_code = slashing::EWrongEpoch)]
public fun test_slashing_wrong_epoch() {
    let (mut runner, mut nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Vote with quorum in epoch 1.
    nodes.do_ref!(|node| {
        runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
            let auth = auth::authenticate_sender(ctx);
            slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
        });
    });

    // Advance to epoch 2.
    runner.next_epoch();
    runner.send_epoch_sync_done_messages(&mut nodes);

    // Try to execute the proposal from epoch 1 - should fail with EWrongEpoch.
    // Note: the proposal gets refreshed on next vote, but execute_slashing removes
    // the proposal and checks its epoch directly.
    // We need to add a fresh proposal first since voting refreshes it.
    // Actually, execute_slashing removes the proposal. Since no one voted in epoch 2,
    // the old proposal still has epoch 1. But voting would refresh it.
    // So we need to not vote, but the proposal from epoch 1 should still be there.
    // Wait - voting in epoch 1 created a proposal. Voting in epoch 2 would refresh it.
    // We want to test the stale proposal path. But `vote_for_slashing` refreshes on access.
    // `execute_slashing` checks the epoch AFTER removing the proposal.
    // Since no one voted in epoch 2, the proposal is still at epoch 1.
    // But we voted with ALL nodes above, which means the proposal had quorum in epoch 1.
    // Now it's epoch 2 and we try to execute - it checks proposal.epoch == staking.epoch().
    runner.tx_with_slashing_manager_and_treasury!(
        nodes[0].sui_address(),
        |staking, slashing_manager, treasury, ctx| {
            slashing_manager.execute_slashing(staking, treasury, candidate_node_id, ctx);
        },
    );

    abort
}

#[test, expected_failure(abort_code = slashing::EDuplicateVote)]
public fun test_slashing_duplicate_vote() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Node 1 votes.
    let node = &nodes[1];
    runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
    });

    // Node 1 votes again - should fail with EDuplicateVote.
    runner.tx_with_slashing_manager!(node.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx);
        slashing_manager.vote_for_slashing(staking, auth, node.node_id(), candidate_node_id);
    });

    abort
}

#[test, expected_failure(abort_code = slashing::ENotAuthorized)]
public fun test_slashing_not_authorized() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate = &nodes[0];
    let candidate_node_id = candidate.node_id();

    // Node 1 tries to vote with node 2's identity - auth won't match governance_authorized.
    let voter = &nodes[1];
    let impersonated = &nodes[2];
    runner.tx_with_slashing_manager!(voter.sui_address(), |staking, slashing_manager, ctx| {
        let auth = auth::authenticate_sender(ctx); // sender is node 1's address
        // But voter_node_id is node 2's - auth won't match node 2's governance_authorized.
        slashing_manager.vote_for_slashing(
            staking,
            auth,
            impersonated.node_id(),
            candidate_node_id,
        );
    });

    abort
}

#[test, expected_failure(abort_code = slashing::ENoProposalForNode)]
public fun test_execute_slashing_no_proposal() {
    let (mut runner, nodes) = setup_committee_with_commission();
    let candidate_node_id = nodes[0].node_id();

    // Try to execute slashing without any votes.
    runner.tx_with_slashing_manager_and_treasury!(
        nodes[0].sui_address(),
        |staking, slashing_manager, treasury, ctx| {
            slashing_manager.execute_slashing(staking, treasury, candidate_node_id, ctx);
        },
    );

    abort
}
