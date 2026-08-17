// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The sync-and-recovery info: the desired state the long-running sync services reconcile
//! toward.
//!
//! The epoch-change executor publishes an [`EpochChangeSyncAndRecoveryInfo`] inside the
//! epoch-change critical section whenever the node's desired synchronization state changes: at
//! every epoch change it processes at the head of the event stream, when the node enters
//! recovery mode, and once at startup (derived from persisted state). The info is published
//! through a [`watch`] channel, so services always observe the *latest* state — a superseded
//! info is unobservable, which is what revokes work derived from it — and long-running tasks
//! can monitor changes mid-work via [`changed`][tokio::sync::watch::Receiver::changed].
//!
//! The info carries only cloneable *descriptions* of the desired state. The consumable halves
//! of the commit protocol — the `epoch_sync_done` attestation token and the completion
//! instructions — cannot ride in a watch channel (they are taken, not borrowed) and stay in
//! the owners' slots, minted by the same critical-section occupancy that publishes the info.

use tokio::sync::watch;
use walrus_core::{Epoch, ShardIndex};
use walrus_sdk::sui::types::GENESIS_EPOCH;

use super::plan::NewShards;

/// The node's relationship to the committee of the info's epoch.
// The shared postfix is deliberate: `MembershipAtEpochChange::NewMember` reads better at use
// sites than a bare `MembershipAtEpochChange::New`.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MembershipAtEpochChange {
    /// The node is not a member of the epoch's committee.
    NotMember,
    /// The node is a member of both the previous and the current committee.
    ContinuingMember,
    /// The node is a member of the current committee, but was not a member of the previous one.
    /// It must recover blob metadata before syncing shard contents.
    NewMember,
}

impl MembershipAtEpochChange {
    /// Derives the membership from the node's presence in the current and previous committees.
    pub(crate) fn from_committee_presence(
        in_current_committee: bool,
        in_previous_committee: bool,
    ) -> Self {
        match (in_current_committee, in_previous_committee) {
            (false, _) => MembershipAtEpochChange::NotMember,
            (true, true) => MembershipAtEpochChange::ContinuingMember,
            (true, false) => MembershipAtEpochChange::NewMember,
        }
    }

    /// Returns `true` if the node is a member of the epoch's committee.
    // Consumed by the sync-service reconcilers introduced in the follow-up commits.
    #[allow(dead_code)]
    pub(crate) fn is_member(&self) -> bool {
        !matches!(self, MembershipAtEpochChange::NotMember)
    }
}

/// The desired synchronization state of the node for one epoch.
///
/// Published by the epoch-change executor inside the epoch-change critical section; replacing
/// the info supersedes (and thereby revokes) everything the sync services derived from the
/// previous one.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EpochChangeSyncAndRecoveryInfo {
    /// Monotonically increasing counter of published infos, assigned by
    /// [`publish`][crate::node::StorageNodeInner::publish_epoch_change_sync_and_recovery_info]
    /// (constructors leave it 0).
    pub generation: u64,
    /// The [`generation`][Self::generation] of the most recent published info that invalidated
    /// in-flight node-recovery work — one that described the node as catching up or as not a
    /// committee member (see [`Self::invalidates_node_recovery_baseline`]). Assigned by
    /// [`publish`][crate::node::StorageNodeInner::publish_epoch_change_sync_and_recovery_info]
    /// (constructors leave it 0).
    ///
    /// A recovery run is bound to the baseline it started from. An epoch change that merely
    /// advances the recovery target leaves the baseline unchanged and *extends* the run: its
    /// frozen scan bound stays sufficient (blobs certified at later epochs are synced through
    /// live event processing), and its completion instruction is replaced with one carrying
    /// the new epoch's attestation. An epoch change (or recovery-mode entry) that changes the
    /// baseline supersedes the run — even if the node has since returned to a recovering
    /// state, because blob events were skipped in between and the run's scan bound no longer
    /// covers them.
    pub node_recovery_baseline_generation: u64,
    /// The epoch this info describes.
    pub epoch: Epoch,
    /// Whether the node is replaying an event backlog. While catching up, the node's view of
    /// its shard assignment is not authoritative, so sync services hold off on new work; the
    /// remaining fields describe the latest known committee state and are only advisory.
    pub catching_up: bool,
    /// The node's relationship to the epoch's committee.
    pub membership: MembershipAtEpochChange,
    /// All shards assigned to the node in the epoch.
    pub owned_shards: Vec<ShardIndex>,
    /// The shards newly assigned to the node at this epoch change, if any, together with how
    /// they are brought up to date.
    pub shards_to_fill: Option<NewShards>,
}

impl EpochChangeSyncAndRecoveryInfo {
    /// Returns `true` if publishing this info invalidates in-flight node-recovery work: while
    /// catching up the node's view of its shard assignment is not authoritative (and blob
    /// events are skipped), and a non-member has no sync work at all.
    pub(crate) fn invalidates_node_recovery_baseline(&self) -> bool {
        self.catching_up || !self.membership.is_member()
    }

    /// Creates the info describing the given state. The generation fields are assigned by
    /// [`publish_epoch_change_sync_and_recovery_info`][pub_fn] when the info is published.
    ///
    /// [pub_fn]: crate::node::StorageNodeInner::publish_epoch_change_sync_and_recovery_info
    pub(crate) fn new(
        epoch: Epoch,
        catching_up: bool,
        membership: MembershipAtEpochChange,
        owned_shards: Vec<ShardIndex>,
        shards_to_fill: Option<NewShards>,
    ) -> Self {
        Self {
            generation: 0,
            node_recovery_baseline_generation: 0,
            epoch,
            catching_up,
            membership,
            owned_shards,
            shards_to_fill,
        }
    }

    /// The info a node starts from before anything about the committee is known.
    pub(crate) fn genesis() -> Self {
        Self::new(
            GENESIS_EPOCH,
            false,
            MembershipAtEpochChange::NotMember,
            Vec::new(),
            None,
        )
    }

    /// Creates the goal watch channel, starting from the genesis info.
    pub(crate) fn channel() -> (
        watch::Sender<EpochChangeSyncAndRecoveryInfo>,
        watch::Receiver<EpochChangeSyncAndRecoveryInfo>,
    ) {
        watch::channel(Self::genesis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_derivation() {
        assert_eq!(
            MembershipAtEpochChange::from_committee_presence(false, false),
            MembershipAtEpochChange::NotMember
        );
        assert_eq!(
            MembershipAtEpochChange::from_committee_presence(false, true),
            MembershipAtEpochChange::NotMember
        );
        assert_eq!(
            MembershipAtEpochChange::from_committee_presence(true, true),
            MembershipAtEpochChange::ContinuingMember
        );
        assert_eq!(
            MembershipAtEpochChange::from_committee_presence(true, false),
            MembershipAtEpochChange::NewMember
        );
    }

    #[test]
    fn catching_up_and_non_member_info_invalidates_the_recovery_baseline() {
        let mut info = EpochChangeSyncAndRecoveryInfo::genesis();
        info.membership = MembershipAtEpochChange::ContinuingMember;
        assert!(!info.invalidates_node_recovery_baseline());
        info.membership = MembershipAtEpochChange::NewMember;
        assert!(!info.invalidates_node_recovery_baseline());

        info.catching_up = true;
        assert!(info.invalidates_node_recovery_baseline());

        info.catching_up = false;
        info.membership = MembershipAtEpochChange::NotMember;
        assert!(info.invalidates_node_recovery_baseline());
    }

    #[test]
    fn publishing_supersedes_previous_info() {
        let (sender, mut receiver) = EpochChangeSyncAndRecoveryInfo::channel();
        assert_eq!(
            *receiver.borrow(),
            EpochChangeSyncAndRecoveryInfo::genesis()
        );

        sender.send_replace(EpochChangeSyncAndRecoveryInfo {
            generation: 1,
            node_recovery_baseline_generation: 0,
            epoch: 5,
            catching_up: false,
            membership: MembershipAtEpochChange::ContinuingMember,
            owned_shards: vec![ShardIndex(1)],
            shards_to_fill: None,
        });
        sender.send_replace(EpochChangeSyncAndRecoveryInfo {
            generation: 2,
            node_recovery_baseline_generation: 2,
            epoch: 6,
            catching_up: true,
            membership: MembershipAtEpochChange::NotMember,
            owned_shards: vec![],
            shards_to_fill: None,
        });

        // A receiver only ever observes the latest info.
        assert!(receiver.has_changed().expect("sender alive"));
        assert_eq!(receiver.borrow_and_update().epoch, 6);
        assert!(!receiver.has_changed().expect("sender alive"));
    }
}
