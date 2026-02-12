// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/// Module to manage slashing of storage nodes.
///
/// This allows committee members to vote for slashing a misbehaving node. When a quorum is reached,
/// the slashing can be executed to burn the node's accumulated commission.
///
/// Proposals are epoch-bound: if the epoch advances, the proposal is refreshed with the new epoch
/// and prior votes are cleared.
module walrus::slashing;

use sui::{table::{Self, Table}, vec_set::{Self, VecSet}};
use wal::wal::ProtectedTreasury;
use walrus::{auth::Authenticated, staking::Staking};

// Error codes
/// Caller is not authorized to vote for slashing for the specified node.
const ENotAuthorized: u64 = 0;
/// The node already voted for this slashing proposal.
const EDuplicateVote: u64 = 1;
/// No slashing proposal exists for the specified node.
const ENoProposalForNode: u64 = 2;
/// The slashing proposal was not created in the current epoch.
const EWrongEpoch: u64 = 3;
/// The slashing proposal has not received enough votes yet.
const ENotEnoughVotes: u64 = 4;

/// A slashing proposal for a candidate node.
public struct SlashingProposal has drop, store {
    /// The epoch in which the proposal was created or last refreshed.
    /// The slashing must be executed in the same epoch.
    epoch: u32,
    /// The node ID of the slashing candidate.
    node_id: ID,
    /// The accumulated voting weight of the proposal.
    voting_weight: u16,
    /// The node IDs that have voted for this proposal.
    /// Note: the number of nodes in the committee is capped, so we can use a VecSet.
    voters: VecSet<ID>,
}

/// The slashing manager object.
public struct SlashingManager has key {
    id: UID,
    slashing_candidates: Table<ID, SlashingProposal>,
}

// TODO(WAL-1147): create a new slashing manager in the migration function during the next contract
// upgrade.
/// Create a new slashing manager and share it.

public(package) fun new(ctx: &mut TxContext) {
    let slashing_manager = SlashingManager {
        id: object::new(ctx),
        slashing_candidates: table::new(ctx),
    };
    transfer::share_object(slashing_manager);
}

// === Slashing Manager Public API ===

/// Vote for slashing a node given its node ID.
///
/// The voter must be authorized via the node's governance_authorization.
/// If a proposal already exists but is from a previous epoch, it is refreshed
/// (votes are cleared and the epoch is updated).
public fun vote_for_slashing(
    self: &mut SlashingManager,
    staking: &Staking,
    auth: Authenticated,
    voter_node_id: ID,
    candidate_node_id: ID,
) {
    assert!(staking.check_governance_authorization(voter_node_id, auth), ENotAuthorized);
    let weight = staking.get_current_node_weight(voter_node_id);
    let epoch = staking.epoch();

    let proposal = if (self.slashing_candidates.contains(candidate_node_id)) {
        let proposal = self.slashing_candidates.borrow_mut(candidate_node_id);
        // Reset if the epoch has advanced.
        if (proposal.epoch != epoch) {
            *proposal = fresh_proposal(epoch, candidate_node_id);
        };
        proposal
    } else {
        let proposal = fresh_proposal(epoch, candidate_node_id);
        self.slashing_candidates.add(candidate_node_id, proposal);
        self.slashing_candidates.borrow_mut(candidate_node_id)
    };
    proposal.add_vote(voter_node_id, weight);
}

/// Execute slashing for a node whose proposal has reached quorum.
///
/// Burns the commission balance of the slashed node's staking pool.
/// The proposal must be from the current epoch and have reached quorum.
public fun execute_slashing(
    self: &mut SlashingManager,
    staking: &mut Staking,
    treasury: &mut ProtectedTreasury,
    candidate_node_id: ID,
    ctx: &mut TxContext,
) {
    assert!(self.slashing_candidates.contains(candidate_node_id), ENoProposalForNode);
    let proposal = self.slashing_candidates.remove(candidate_node_id);

    assert!(proposal.epoch == staking.epoch(), EWrongEpoch);
    assert!(staking.is_quorum(proposal.voting_weight), ENotEnoughVotes);

    // Burn the commission from the slashed node's staking pool.
    staking.burn_commission(candidate_node_id, treasury, ctx);
}

/// Remove any slashing proposals whose epoch is in the past.
///
/// This is a permissionless cleanup function that anyone can call.
public fun cleanup_slashing_proposals(
    self: &mut SlashingManager,
    staking: &Staking,
    node_ids: vector<ID>,
) {
    let current_epoch = staking.epoch();
    node_ids.do!(|node_id| {
        if (self.slashing_candidates.contains(node_id)) {
            let proposal = self.slashing_candidates.borrow(node_id);
            if (proposal.epoch < current_epoch) {
                self.slashing_candidates.remove(node_id);
            }
        }
    });
}

// === Internal functions ===

/// Creates a new slashing proposal.
fun fresh_proposal(epoch: u32, node_id: ID): SlashingProposal {
    SlashingProposal { epoch, node_id, voting_weight: 0, voters: vec_set::empty() }
}

/// Adds a vote to a slashing proposal.
fun add_vote(proposal: &mut SlashingProposal, node_id: ID, weight: u16) {
    assert!(!proposal.voters.contains(&node_id), EDuplicateVote);
    proposal.voters.insert(node_id);
    proposal.voting_weight = proposal.voting_weight + weight;
}
