// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The epoch synchronization goal: the desired state the long-running sync services reconcile
//! toward.
//!
//! The epoch-change executor publishes an [`EpochSyncGoal`] inside the epoch-change critical
//! section whenever the node's desired synchronization state changes: at every epoch change it
//! processes at the head of the event stream, when the node enters recovery mode, and once at
//! startup (derived from persisted state). The goal is published through a
//! [`watch`][tokio::sync::watch] channel, so services always observe the *latest* goal — a
//! superseded goal is unobservable, which is what revokes work derived from it — and
//! long-running tasks can monitor goal changes mid-work via
//! [`changed`][tokio::sync::watch::Receiver::changed].
//!
//! The goal carries only cloneable *descriptions* of the desired state. The consumable halves of
//! the commit protocol — the `epoch_sync_done` attestation token and the completion
//! instructions — cannot ride in a watch channel (they are taken, not borrowed) and stay in the
//! owners' slots, minted by the same critical-section occupancy that publishes the goal.

use tokio::sync::watch;
use walrus_core::{Epoch, ShardIndex};
use walrus_sdk::sui::types::GENESIS_EPOCH;

use super::plan::NewShards;

/// The node's relationship to the committee of the goal's epoch.
// The shared postfix is deliberate: `Membership::NewMember` reads better at use sites than a
// bare `Membership::New`.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Membership {
    /// The node is not a member of the epoch's committee.
    NotMember,
    /// The node is a member of both the previous and the current committee.
    ContinuingMember,
    /// The node is a member of the current committee, but was not a member of the previous one.
    /// It must recover blob metadata before syncing shard contents.
    NewMember,
}

impl Membership {
    /// Derives the membership from the node's presence in the current and previous committees.
    pub(crate) fn from_committee_presence(
        in_current_committee: bool,
        in_previous_committee: bool,
    ) -> Self {
        match (in_current_committee, in_previous_committee) {
            (false, _) => Membership::NotMember,
            (true, true) => Membership::ContinuingMember,
            (true, false) => Membership::NewMember,
        }
    }

    /// Returns `true` if the node is a member of the epoch's committee.
    // Consumed by the sync-service reconcilers introduced in the follow-up commits.
    #[allow(dead_code)]
    pub(crate) fn is_member(&self) -> bool {
        !matches!(self, Membership::NotMember)
    }
}

/// The desired synchronization state of the node for one epoch.
///
/// Published by the epoch-change executor inside the epoch-change critical section; replacing
/// the goal supersedes (and thereby revokes) everything the sync services derived from the
/// previous one.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EpochSyncGoal {
    /// Monotonically increasing publication counter, assigned by
    /// [`publish_epoch_sync_goal`][crate::node::StorageNodeInner::publish_epoch_sync_goal]
    /// (constructors leave it 0). A run of a sync service is bound to the generation it started
    /// from and commits only if the generation is unchanged: any publication in between —
    /// including one that entered and left catch-up — supersedes the run.
    pub generation: u64,
    /// The epoch this goal describes.
    pub epoch: Epoch,
    /// Whether the node is replaying an event backlog. While catching up, the node's view of
    /// its shard assignment is not authoritative, so sync services hold off on new work; the
    /// remaining fields describe the latest known committee state and are only advisory.
    pub catching_up: bool,
    /// The node's relationship to the epoch's committee.
    pub membership: Membership,
    /// All shards assigned to the node in the epoch.
    pub owned_shards: Vec<ShardIndex>,
    /// The shards newly assigned to the node at this epoch change, if any, together with how
    /// they are brought up to date.
    pub shards_to_fill: Option<NewShards>,
}

impl EpochSyncGoal {
    /// The goal a node starts from before anything about the committee is known.
    pub(crate) fn genesis() -> Self {
        Self {
            generation: 0,
            epoch: GENESIS_EPOCH,
            catching_up: false,
            membership: Membership::NotMember,
            owned_shards: Vec::new(),
            shards_to_fill: None,
        }
    }

    /// Creates the goal watch channel, starting from the genesis goal.
    pub(crate) fn channel() -> (watch::Sender<EpochSyncGoal>, watch::Receiver<EpochSyncGoal>) {
        watch::channel(Self::genesis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_derivation() {
        assert_eq!(
            Membership::from_committee_presence(false, false),
            Membership::NotMember
        );
        assert_eq!(
            Membership::from_committee_presence(false, true),
            Membership::NotMember
        );
        assert_eq!(
            Membership::from_committee_presence(true, true),
            Membership::ContinuingMember
        );
        assert_eq!(
            Membership::from_committee_presence(true, false),
            Membership::NewMember
        );
    }

    #[test]
    fn publishing_supersedes_previous_goal() {
        let (sender, mut receiver) = EpochSyncGoal::channel();
        assert_eq!(*receiver.borrow(), EpochSyncGoal::genesis());

        sender.send_replace(EpochSyncGoal {
            generation: 1,
            epoch: 5,
            catching_up: false,
            membership: Membership::ContinuingMember,
            owned_shards: vec![ShardIndex(1)],
            shards_to_fill: None,
        });
        sender.send_replace(EpochSyncGoal {
            generation: 2,
            epoch: 6,
            catching_up: true,
            membership: Membership::NotMember,
            owned_shards: vec![],
            shards_to_fill: None,
        });

        // A receiver only ever observes the latest goal.
        assert!(receiver.has_changed().expect("sender alive"));
        assert_eq!(receiver.borrow_and_update().epoch, 6);
        assert!(!receiver.has_changed().expect("sender alive"));
    }
}
