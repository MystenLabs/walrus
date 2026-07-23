// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Pure planning of an epoch change.
//!
//! [`plan_epoch_change`] is a side-effect-free function that maps the facts established during
//! committee reconciliation (see `reconcile_committee_for_epoch_change`) to an explicit
//! [`EpochChangePlan`] describing everything the node must do for an `EpochChangeStart` event:
//! the node-status transition, the shard changes, the recovery action, and which component owns
//! the `epoch_sync_done` attestation.
//!
//! Keeping the decision logic pure means the complete decision table lives in one place and is
//! exhaustively unit-testable without any storage, network, or async machinery. The plan is
//! applied by `apply_epoch_change_plan`, which contains no decisions of its own.

use walrus_core::{Epoch, ShardIndex};

use crate::node::NodeStatus;

/// The route the node takes through an epoch change, established by committee reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitteeRoute {
    /// The node is up to date: the committee service transitioned (or had already transitioned)
    /// to the event's epoch.
    InSync,
    /// A committee change to this epoch is already in progress; the event is a duplicate.
    AlreadyInProgress,
    /// The node is (or just started) catching up: the committee service jumped to the latest
    /// committee, which may be ahead of the event's epoch.
    CatchingUp,
}

/// The shard sets affected by this epoch change, as computed by
/// [`ShardDiffCalculator`][crate::common::utils::ShardDiffCalculator].
#[derive(Debug, Clone, Default)]
pub(crate) struct ShardDiff {
    /// Shards assigned to the node in the current epoch that it did not own in the previous
    /// epoch; these are filled via shard sync from their previous owners.
    pub gained: Vec<ShardIndex>,
    /// Shards the node owned in the previous epoch but not in the current one; they are locked
    /// against further writes and kept as a sync source for the new owner.
    pub lost: Vec<ShardIndex>,
    /// Locally stored shards that the node has not owned for at least two epochs; they are
    /// removed in the background.
    pub removed: Vec<ShardIndex>,
    /// All shards assigned to the node in the current committee.
    pub all_owned: Vec<ShardIndex>,
}

/// Everything [`plan_epoch_change`] needs to decide what the node must do.
#[derive(Debug, Clone)]
pub(crate) struct PlanInputs {
    /// The epoch of the `EpochChangeStart` event being processed.
    pub event_epoch: Epoch,
    /// The committee service's epoch after reconciliation. Equal to `event_epoch` on the
    /// [`InSync`][CommitteeRoute::InSync] route; may be ahead of it while catching up.
    pub committee_epoch: Epoch,
    /// The reconciliation route.
    pub route: CommitteeRoute,
    /// The node's persisted status at planning time.
    pub node_status: NodeStatus,
    /// Whether the node is a member of the current (new) committee.
    pub in_current_committee: bool,
    /// Whether the node was a member of the previous committee.
    pub in_previous_committee: bool,
    /// The shard sets affected by this epoch change.
    pub shards: ShardDiff,
}

/// A node-status transition to perform while applying the plan.
///
/// The apply step orders these relative to the shard work: [`Standby`][Self::Standby] and
/// [`RecoveryInProgress`][Self::RecoveryInProgress] are written before shard storage is created,
/// while [`RecoverMetadata`][Self::RecoverMetadata] is written after creation (a restart that
/// observes `RecoverMetadata` assumes all shard storage exists) but before shard syncs start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatusTransition {
    /// The node is not a member of the new committee.
    Standby,
    /// The node newly joined the committee and must first sync blob metadata for its shards.
    RecoverMetadata,
    /// Set (or advance) the node recovery target to the event's epoch.
    RecoveryInProgress,
}

/// The node-recovery action to perform while applying the plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryAction {
    /// No recovery involvement.
    None,
    /// Abort any stale recovery task and start a fresh one targeting the given epoch. Used when
    /// the node has lost track of the previous epoch's shard assignment: all owned shards are
    /// force-set to `Active` and their missing blobs are recovered per blob.
    StartFresh(Epoch),
    /// The node was already recovering: keep the running task (it picks up the advanced target
    /// and the newly started shard syncs) and only start a new one if the running task completed
    /// concurrently with this epoch change.
    EnsureRunning(Epoch),
}

/// The component responsible for attesting `epoch_sync_done` for this epoch.
///
/// Exactly one component owns the attestation per epoch change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttestationOwner {
    /// No shard syncs were started and the node is not recovering: the epoch-change finisher
    /// attests directly (it skips the attestation if the node is not in the committee).
    Finisher,
    /// Shard syncs were started: the last shard-sync task to complete attests.
    ShardSync,
    /// The node is recovering: the recovery task attests once the blob scan is complete and all
    /// owned shards are active.
    RecoveryTask,
}

/// The shard- and recovery-related work to apply for this epoch change.
#[derive(Debug, Clone)]
pub(crate) struct ShardTransition {
    /// The node-status transition to perform, if any.
    pub status: Option<StatusTransition>,
    /// Shard storage to create.
    pub create: Vec<ShardIndex>,
    /// Whether to force-set all created shards to `Active` (full-recovery path: their contents
    /// are recovered per blob, not via shard sync).
    pub force_set_active: bool,
    /// Shards to fill via shard sync from their previous owners.
    pub sync: Vec<ShardIndex>,
    /// Shards to lock against further writes because they moved to another node.
    pub lock: Vec<ShardIndex>,
    /// Shards to remove in the background.
    pub remove: Vec<ShardIndex>,
    /// The recovery action to perform.
    pub recovery: RecoveryAction,
    /// The component that attests `epoch_sync_done` for this epoch.
    pub sync_done_owner: AttestationOwner,
}

/// The reason an epoch change requires no action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// The node is catching up and the event is for an epoch older than the committee's; the
    /// event is part of the backlog being replayed.
    StaleEventWhileCatchingUp,
    /// A committee change to this epoch is already in progress.
    ChangeAlreadyInProgress,
}

/// The complete plan for processing an `EpochChangeStart` event.
#[derive(Debug, Clone)]
pub(crate) enum EpochChangePlan {
    /// Nothing to do; mark the event as complete.
    Skip(SkipReason),
    /// A catching-up node reached the current epoch but is not a member of the new committee:
    /// move to `Standby` and mark the event as complete without touching shards.
    MoveToStandby,
    /// Apply the contained shard and recovery transition.
    Apply(ShardTransition),
}

/// Decides what the node must do for an `EpochChangeStart` event.
///
/// This function is pure: it performs no I/O and has no side effects. All branching on the
/// node's situation (route, committee membership, and node status) happens here; the apply step
/// executes the returned plan without further decisions.
pub(crate) fn plan_epoch_change(inputs: &PlanInputs) -> EpochChangePlan {
    match inputs.route {
        CommitteeRoute::AlreadyInProgress => {
            EpochChangePlan::Skip(SkipReason::ChangeAlreadyInProgress)
        }
        CommitteeRoute::CatchingUp => plan_while_catching_up(inputs),
        CommitteeRoute::InSync => plan_while_in_sync(inputs),
    }
}

/// Plans the epoch change for a node that is catching up with the event backlog.
///
/// Stale events from the backlog are skipped. Once the events reach the committee's (latest)
/// epoch, the node either moves to `Standby` (not a member), processes shard changes like a new
/// committee member (a member that was not in the previous committee), or starts a full node
/// recovery (a continuing member whose local shards may be arbitrarily far behind).
fn plan_while_catching_up(inputs: &PlanInputs) -> EpochChangePlan {
    if inputs.event_epoch < inputs.committee_epoch {
        return EpochChangePlan::Skip(SkipReason::StaleEventWhileCatchingUp);
    }

    if !inputs.in_current_committee {
        return EpochChangePlan::MoveToStandby;
    }

    if !inputs.in_previous_committee {
        // The node just became a new committee member: its gained shards are filled via shard
        // sync, preceded by metadata recovery.
        return EpochChangePlan::Apply(shard_sync_transition(inputs, new_joiner_status(inputs)));
    }

    // The node is a past and current committee member, but has lost track of the shard
    // assignment history while catching up: recover all owned shards per blob.
    EpochChangePlan::Apply(ShardTransition {
        status: Some(StatusTransition::RecoveryInProgress),
        create: inputs.shards.all_owned.clone(),
        force_set_active: true,
        sync: vec![],
        lock: inputs.shards.lost.clone(),
        remove: inputs.shards.removed.clone(),
        recovery: RecoveryAction::StartFresh(inputs.event_epoch),
        sync_done_owner: AttestationOwner::RecoveryTask,
    })
}

/// Plans the epoch change for a node that is up to date with events.
fn plan_while_in_sync(inputs: &PlanInputs) -> EpochChangePlan {
    debug_assert!(inputs.event_epoch <= inputs.committee_epoch);

    if !inputs.in_current_committee {
        // The node dropped out of the committee. It moves to `Standby` (from this epoch on it no
        // longer syncs blob metadata) but still processes the shard changes: its previous shards
        // are locked as sync sources and old shards are removed. If it was recovering, the
        // recovery task is left to finish on its own; its completion attempt observes the
        // `Standby` status and does not attest.
        return EpochChangePlan::Apply(ShardTransition {
            status: Some(StatusTransition::Standby),
            ..shard_sync_transition(inputs, None)
        });
    }

    if let NodeStatus::RecoveryInProgress(_) = inputs.node_status {
        // The node is already recovering. Since it is up to date with events, newly gained
        // shards are synced from their previous owners instead of being filled by blob
        // recovery, and the running recovery task keeps its progress: it waits for these shard
        // syncs, then attests for the advanced target.
        return EpochChangePlan::Apply(ShardTransition {
            status: Some(StatusTransition::RecoveryInProgress),
            recovery: RecoveryAction::EnsureRunning(inputs.event_epoch),
            sync_done_owner: AttestationOwner::RecoveryTask,
            ..shard_sync_transition(inputs, None)
        });
    }

    let status = if inputs.node_status == NodeStatus::Standby {
        // The node just joined the committee.
        new_joiner_status(inputs)
    } else {
        None
    };
    EpochChangePlan::Apply(shard_sync_transition(inputs, status))
}

/// The common transition shape for nodes whose gained shards are filled via shard sync.
fn shard_sync_transition(inputs: &PlanInputs, status: Option<StatusTransition>) -> ShardTransition {
    ShardTransition {
        status,
        create: inputs.shards.gained.clone(),
        force_set_active: false,
        sync: inputs.shards.gained.clone(),
        lock: inputs.shards.lost.clone(),
        remove: inputs.shards.removed.clone(),
        recovery: RecoveryAction::None,
        sync_done_owner: if inputs.shards.gained.is_empty() {
            AttestationOwner::Finisher
        } else {
            AttestationOwner::ShardSync
        },
    }
}

/// The status transition for a node that newly joined the committee: it must recover blob
/// metadata before syncing shard contents. Only set when shards are gained, as the status is
/// flipped back to `Active` by the metadata-recovery task.
fn new_joiner_status(inputs: &PlanInputs) -> Option<StatusTransition> {
    (!inputs.shards.gained.is_empty()).then_some(StatusTransition::RecoverMetadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_ids(ids: &[u16]) -> Vec<ShardIndex> {
        ids.iter().map(|&id| ShardIndex(id)).collect()
    }

    fn base_inputs() -> PlanInputs {
        PlanInputs {
            event_epoch: 5,
            committee_epoch: 5,
            route: CommitteeRoute::InSync,
            node_status: NodeStatus::Active,
            in_current_committee: true,
            in_previous_committee: true,
            shards: ShardDiff {
                gained: shard_ids(&[1, 2]),
                lost: shard_ids(&[3]),
                removed: shard_ids(&[4]),
                all_owned: shard_ids(&[0, 1, 2]),
            },
        }
    }

    fn expect_apply(plan: EpochChangePlan) -> ShardTransition {
        match plan {
            EpochChangePlan::Apply(transition) => transition,
            other => panic!("expected Apply plan, got {other:?}"),
        }
    }

    #[test]
    fn change_already_in_progress_is_skipped() {
        let inputs = PlanInputs {
            route: CommitteeRoute::AlreadyInProgress,
            ..base_inputs()
        };
        assert!(matches!(
            plan_epoch_change(&inputs),
            EpochChangePlan::Skip(SkipReason::ChangeAlreadyInProgress)
        ));
    }

    #[test]
    fn stale_event_while_catching_up_is_skipped() {
        let inputs = PlanInputs {
            route: CommitteeRoute::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            event_epoch: 3,
            committee_epoch: 5,
            ..base_inputs()
        };
        assert!(matches!(
            plan_epoch_change(&inputs),
            EpochChangePlan::Skip(SkipReason::StaleEventWhileCatchingUp)
        ));
    }

    #[test]
    fn caught_up_non_member_moves_to_standby_without_shard_changes() {
        let inputs = PlanInputs {
            route: CommitteeRoute::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            in_current_committee: false,
            ..base_inputs()
        };
        assert!(matches!(
            plan_epoch_change(&inputs),
            EpochChangePlan::MoveToStandby
        ));
    }

    #[test]
    fn caught_up_new_member_syncs_shards_after_metadata_recovery() {
        let inputs = PlanInputs {
            route: CommitteeRoute::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            in_previous_committee: false,
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, Some(StatusTransition::RecoverMetadata));
        assert_eq!(transition.create, shard_ids(&[1, 2]));
        assert_eq!(transition.sync, shard_ids(&[1, 2]));
        assert!(!transition.force_set_active);
        assert_eq!(transition.lock, shard_ids(&[3]));
        assert_eq!(transition.remove, shard_ids(&[4]));
        assert_eq!(transition.recovery, RecoveryAction::None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::ShardSync);
    }

    #[test]
    fn caught_up_continuing_member_starts_full_recovery() {
        let inputs = PlanInputs {
            route: CommitteeRoute::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            transition.status,
            Some(StatusTransition::RecoveryInProgress)
        );
        assert_eq!(transition.create, shard_ids(&[0, 1, 2]));
        assert!(transition.force_set_active);
        assert!(transition.sync.is_empty());
        assert_eq!(transition.lock, shard_ids(&[3]));
        assert_eq!(transition.remove, shard_ids(&[4]));
        assert_eq!(transition.recovery, RecoveryAction::StartFresh(5));
        assert_eq!(transition.sync_done_owner, AttestationOwner::RecoveryTask);
    }

    #[test]
    fn in_sync_member_with_gained_shards_starts_shard_sync() {
        let transition = expect_apply(plan_epoch_change(&base_inputs()));
        assert_eq!(transition.status, None);
        assert_eq!(transition.create, shard_ids(&[1, 2]));
        assert_eq!(transition.sync, shard_ids(&[1, 2]));
        assert_eq!(transition.recovery, RecoveryAction::None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::ShardSync);
    }

    #[test]
    fn in_sync_member_without_gained_shards_lets_finisher_attest() {
        let inputs = PlanInputs {
            shards: ShardDiff {
                gained: vec![],
                lost: shard_ids(&[3]),
                removed: shard_ids(&[4]),
                all_owned: shard_ids(&[0]),
            },
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, None);
        assert!(transition.create.is_empty());
        assert!(transition.sync.is_empty());
        assert_eq!(transition.sync_done_owner, AttestationOwner::Finisher);
    }

    #[test]
    fn in_sync_dropout_moves_to_standby_but_still_processes_shards() {
        let inputs = PlanInputs {
            in_current_committee: false,
            shards: ShardDiff {
                gained: vec![],
                lost: shard_ids(&[0, 1]),
                removed: shard_ids(&[4]),
                all_owned: vec![],
            },
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, Some(StatusTransition::Standby));
        assert_eq!(transition.lock, shard_ids(&[0, 1]));
        assert_eq!(transition.remove, shard_ids(&[4]));
        assert_eq!(transition.recovery, RecoveryAction::None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::Finisher);
    }

    #[test]
    fn in_sync_new_joiner_recovers_metadata_first() {
        let inputs = PlanInputs {
            node_status: NodeStatus::Standby,
            in_previous_committee: false,
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, Some(StatusTransition::RecoverMetadata));
        assert_eq!(transition.sync, shard_ids(&[1, 2]));
        assert_eq!(transition.sync_done_owner, AttestationOwner::ShardSync);
    }

    #[test]
    fn in_sync_new_joiner_without_gained_shards_keeps_status() {
        let inputs = PlanInputs {
            node_status: NodeStatus::Standby,
            in_previous_committee: false,
            shards: ShardDiff::default(),
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::Finisher);
    }

    #[test]
    fn in_sync_while_recovering_advances_target_and_keeps_task() {
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoveryInProgress(3),
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            transition.status,
            Some(StatusTransition::RecoveryInProgress)
        );
        assert_eq!(transition.create, shard_ids(&[1, 2]));
        assert_eq!(transition.sync, shard_ids(&[1, 2]));
        assert!(!transition.force_set_active);
        assert_eq!(transition.recovery, RecoveryAction::EnsureRunning(5));
        assert_eq!(transition.sync_done_owner, AttestationOwner::RecoveryTask);
    }

    #[test]
    fn in_sync_dropout_while_recovering_moves_to_standby() {
        // A recovering node that drops out of the committee takes the standby path; the running
        // recovery task is not advanced and will not attest.
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoveryInProgress(3),
            in_current_committee: false,
            shards: ShardDiff {
                gained: vec![],
                lost: shard_ids(&[0, 1]),
                removed: vec![],
                all_owned: vec![],
            },
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, Some(StatusTransition::Standby));
        assert_eq!(transition.recovery, RecoveryAction::None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::Finisher);
    }

    #[test]
    fn in_sync_metadata_recovery_in_progress_keeps_status() {
        // An epoch change while a previous metadata recovery is still running does not touch the
        // node status; the metadata-recovery task flips it to `Active` when done.
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoverMetadata,
            ..base_inputs()
        };
        let transition = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(transition.status, None);
        assert_eq!(transition.sync_done_owner, AttestationOwner::ShardSync);
    }
}
