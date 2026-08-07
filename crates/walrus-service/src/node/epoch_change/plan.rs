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

use super::goal::MembershipAtEpochChange;
use crate::node::NodeStatus;

/// The node's status with respect to an epoch change, established by committee
/// reconciliation; it determines the route the node takes through the epoch change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeStatusAtBeginningOfEpochChange {
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
    /// [`InSync`][NodeStatusAtBeginningOfEpochChange::InSync] route; may be ahead of it while
    /// catching up.
    pub committee_epoch: Epoch,
    /// The node's status with respect to this epoch change, established by committee
    /// reconciliation.
    pub node_status_at_beginning_of_epoch_change: NodeStatusAtBeginningOfEpochChange,
    /// The node's persisted status at planning time.
    pub node_status: NodeStatus,
    /// Whether the node is a member of the current (new) committee.
    pub in_current_committee: bool,
    /// Whether the node was a member of the previous committee.
    pub in_previous_committee: bool,
    /// Whether shard-sync tasks (from this or earlier epochs) are still running at planning
    /// time. Outstanding syncs mean the node's epoch-sync claim is not yet complete, so the
    /// attestation must wait for them instead of being sent by the finisher.
    pub has_ongoing_shard_syncs: bool,
    /// The shard sets affected by this epoch change.
    pub shards: ShardDiff,
}

/// A node-status transition to perform while applying the plan.
///
/// `Active` is deliberately absent: the epoch-change apply step never sets a node to `Active`
/// directly. That transition is only ever performed by a long-running task consuming its
/// completion instruction — metadata recovery or node recovery finishing — so it cannot appear
/// in a plan.
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

/// The component responsible for attesting `epoch_sync_done` for this epoch.
///
/// Exactly one component owns the attestation per epoch change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochSyncDoneAttestationOwner {
    /// No shard-sync work exists at all — none started for this epoch, none still running
    /// from earlier epochs — and the node is not recovering: the start-epoch-change finisher
    /// attests directly (it skips the attestation if the node is not in the committee).
    StartEpochChangeFinisher,
    /// Shard syncs were started for this epoch or are still running from earlier ones: the
    /// sync completion that finishes the pending shard syncs attests.
    ShardSync,
    /// The node is recovering: the recovery task attests once the blob scan is complete and all
    /// owned shards are active.
    RecoveryTask,
}

/// How created shards are brought up to date. A created shard is filled either by shard sync
/// or by node recovery, never both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardFill {
    /// Sync each shard's contents from its previous owner.
    ShardSync,
    /// Force the shards to `Active` status; node recovery fills their missing blobs per blob
    /// (full-recovery path, where no previous-owner assignment is known to sync from).
    ForceActive,
}

/// The shards newly assigned to the node in this epoch change: their storage is created, and
/// their contents are then brought up to date by the described fill method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewShards {
    /// The shards to create storage for.
    pub shards: Vec<ShardIndex>,
    /// How the created shards are brought up to date.
    pub fill: ShardFill,
}

/// The shard- and recovery-related work to execute for this epoch change.
#[derive(Debug, Clone)]
pub(crate) struct EpochChangeExecutionInfo {
    /// The node-status transition to perform, if any.
    pub status: Option<StatusTransition>,
    /// The node's relationship to the new epoch's committee.
    pub membership: MembershipAtEpochChange,
    /// All shards assigned to the node in the new epoch.
    pub owned_shards: Vec<ShardIndex>,
    /// The shards newly assigned to the node, if any.
    pub new_shards: Option<NewShards>,
    /// Shards to lock against further writes because they moved to another node.
    pub lock: Vec<ShardIndex>,
    /// Shards to remove in the background.
    pub remove: Vec<ShardIndex>,
    /// The component that attests `epoch_sync_done` for this epoch.
    pub sync_done_owner: EpochSyncDoneAttestationOwner,
}

/// The reason an epoch change requires no action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipEpochChangeExecutionReason {
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
    Skip(SkipEpochChangeExecutionReason),
    /// A catching-up node reached the current epoch but is not a member of the new committee:
    /// move to `Standby` and mark the event as complete without touching shards.
    MoveToStandby,
    /// Execute the contained epoch-change work.
    Apply(EpochChangeExecutionInfo),
}

/// Decides what the node must do for an `EpochChangeStart` event.
///
/// This function is pure: it performs no I/O and has no side effects. All branching on the
/// node's situation (route, committee membership, and node status) happens here; the apply step
/// executes the returned plan without further decisions.
pub(crate) fn plan_epoch_change(inputs: &PlanInputs) -> EpochChangePlan {
    match inputs.node_status_at_beginning_of_epoch_change {
        NodeStatusAtBeginningOfEpochChange::AlreadyInProgress => {
            EpochChangePlan::Skip(SkipEpochChangeExecutionReason::ChangeAlreadyInProgress)
        }
        // A catching-up node replays a backlog: events for epochs older than the committee's
        // are skipped.
        NodeStatusAtBeginningOfEpochChange::CatchingUp
            if inputs.event_epoch < inputs.committee_epoch =>
        {
            EpochChangePlan::Skip(SkipEpochChangeExecutionReason::StaleEventWhileCatchingUp)
        }
        status => plan_for_epoch_change_execution_for_caught_up_node(
            inputs,
            matches!(status, NodeStatusAtBeginningOfEpochChange::CatchingUp),
        ),
    }
}

/// Plans an epoch change whose event is current — the two-branch model: a stale event was
/// skipped above, everything else is one path parameterized by the node's membership and prior
/// status. `exited_catch_up` marks the one genuinely special situation: this event is the first
/// current one after replaying a backlog, so the node's local shard map is not trustworthy.
fn plan_for_epoch_change_execution_for_caught_up_node(
    inputs: &PlanInputs,
    exited_catch_up: bool,
) -> EpochChangePlan {
    debug_assert!(inputs.event_epoch <= inputs.committee_epoch);
    let membership = MembershipAtEpochChange::from_committee_presence(
        inputs.in_current_committee,
        inputs.in_previous_committee,
    );

    if !membership.is_member() {
        if exited_catch_up {
            // A catching-up node's local shard map is not authoritative, so no shard changes
            // are derived from it; the node just moves to `Standby`.
            return EpochChangePlan::MoveToStandby;
        }
        // The node dropped out of the committee. It moves to `Standby` (from this epoch on it
        // no longer syncs blob metadata) but still processes the shard changes: its previous
        // shards are locked as sync sources and old shards are removed. If it was recovering,
        // the completion instruction is revoked and the recovery run superseded.
        return EpochChangePlan::Apply(epoch_change_execution_info(
            inputs,
            Some(StatusTransition::Standby),
        ));
    }

    if exited_catch_up && membership == MembershipAtEpochChange::ContinuingMember {
        // A continuing member that just caught up has lost track of its shard assignment
        // history: no previous-owner mapping is trustworthy to sync from, so all owned shards
        // are force-activated and their missing blobs recovered per blob.
        return EpochChangePlan::Apply(EpochChangeExecutionInfo {
            status: Some(StatusTransition::RecoveryInProgress),
            membership,
            owned_shards: inputs.shards.all_owned.clone(),
            new_shards: Some(NewShards {
                shards: inputs.shards.all_owned.clone(),
                fill: ShardFill::ForceActive,
            }),
            lock: inputs.shards.lost.clone(),
            remove: inputs.shards.removed.clone(),
            sync_done_owner: EpochSyncDoneAttestationOwner::RecoveryTask,
        });
    }

    if matches!(inputs.node_status, NodeStatus::RecoveryInProgress(_)) {
        // The node is already recovering (it cannot also be catching up). Newly gained shards
        // are synced from their previous owners; the recovery target advances, superseding the
        // current recovery run.
        return EpochChangePlan::Apply(EpochChangeExecutionInfo {
            sync_done_owner: EpochSyncDoneAttestationOwner::RecoveryTask,
            ..epoch_change_execution_info(inputs, Some(StatusTransition::RecoveryInProgress))
        });
    }

    // A node that newly joined the committee recovers blob metadata before syncing its gained
    // shards. Guarded by the transition's legality so that a replayed event on a node whose
    // status has already advanced does not plan an illegal write.
    let status = (membership == MembershipAtEpochChange::NewMember
        && inputs
            .node_status
            .can_transition_to(&NodeStatus::RecoverMetadata))
    .then(|| new_joiner_status(inputs))
    .flatten();
    EpochChangePlan::Apply(epoch_change_execution_info(inputs, status))
}

/// The common transition shape for nodes whose gained shards are filled via shard sync.
fn epoch_change_execution_info(
    inputs: &PlanInputs,
    status: Option<StatusTransition>,
) -> EpochChangeExecutionInfo {
    // The epoch-sync claim is complete only once every sync has delivered: newly gained
    // shards, syncs still draining from earlier epochs (for shards the node may still own),
    // and an unfinished metadata recovery all defer the attestation to shard sync. The
    // start-epoch-change finisher attests directly only when the node is a committee member
    // with no sync work at all (and skips the attestation entirely for non-members).
    let outstanding_sync_work =
        inputs.has_ongoing_shard_syncs || inputs.node_status == NodeStatus::RecoverMetadata;
    let sync_done_owner = if inputs.in_current_committee
        && (!inputs.shards.gained.is_empty() || outstanding_sync_work)
    {
        EpochSyncDoneAttestationOwner::ShardSync
    } else {
        EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
    };

    EpochChangeExecutionInfo {
        status,
        membership: MembershipAtEpochChange::from_committee_presence(
            inputs.in_current_committee,
            inputs.in_previous_committee,
        ),
        owned_shards: inputs.shards.all_owned.clone(),
        new_shards: (!inputs.shards.gained.is_empty()).then(|| NewShards {
            shards: inputs.shards.gained.clone(),
            fill: ShardFill::ShardSync,
        }),
        lock: inputs.shards.lost.clone(),
        remove: inputs.shards.removed.clone(),
        sync_done_owner,
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
            node_status_at_beginning_of_epoch_change: NodeStatusAtBeginningOfEpochChange::InSync,
            node_status: NodeStatus::Active,
            in_current_committee: true,
            in_previous_committee: true,
            has_ongoing_shard_syncs: false,
            shards: ShardDiff {
                gained: shard_ids(&[1, 2]),
                lost: shard_ids(&[3]),
                removed: shard_ids(&[4]),
                all_owned: shard_ids(&[0, 1, 2]),
            },
        }
    }

    fn new_shards(ids: &[u16], fill: ShardFill) -> Option<NewShards> {
        Some(NewShards {
            shards: shard_ids(ids),
            fill,
        })
    }

    fn expect_apply(plan: EpochChangePlan) -> EpochChangeExecutionInfo {
        match plan {
            EpochChangePlan::Apply(execution_info) => execution_info,
            other => panic!("expected Apply plan, got {other:?}"),
        }
    }

    #[test]
    fn change_already_in_progress_is_skipped() {
        let inputs = PlanInputs {
            node_status_at_beginning_of_epoch_change:
                NodeStatusAtBeginningOfEpochChange::AlreadyInProgress,
            ..base_inputs()
        };
        assert!(matches!(
            plan_epoch_change(&inputs),
            EpochChangePlan::Skip(SkipEpochChangeExecutionReason::ChangeAlreadyInProgress)
        ));
    }

    #[test]
    fn stale_event_while_catching_up_is_skipped() {
        let inputs = PlanInputs {
            node_status_at_beginning_of_epoch_change:
                NodeStatusAtBeginningOfEpochChange::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            event_epoch: 3,
            committee_epoch: 5,
            ..base_inputs()
        };
        assert!(matches!(
            plan_epoch_change(&inputs),
            EpochChangePlan::Skip(SkipEpochChangeExecutionReason::StaleEventWhileCatchingUp)
        ));
    }

    #[test]
    fn caught_up_non_member_moves_to_standby_without_shard_changes() {
        let inputs = PlanInputs {
            node_status_at_beginning_of_epoch_change:
                NodeStatusAtBeginningOfEpochChange::CatchingUp,
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
            node_status_at_beginning_of_epoch_change:
                NodeStatusAtBeginningOfEpochChange::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            in_previous_committee: false,
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.status,
            Some(StatusTransition::RecoverMetadata)
        );
        assert_eq!(
            execution_info.membership,
            MembershipAtEpochChange::NewMember
        );
        assert_eq!(
            execution_info.new_shards,
            new_shards(&[1, 2], ShardFill::ShardSync)
        );
        assert_eq!(execution_info.lock, shard_ids(&[3]));
        assert_eq!(execution_info.remove, shard_ids(&[4]));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
    }

    #[test]
    fn caught_up_continuing_member_starts_full_recovery() {
        let inputs = PlanInputs {
            node_status_at_beginning_of_epoch_change:
                NodeStatusAtBeginningOfEpochChange::CatchingUp,
            node_status: NodeStatus::RecoveryCatchUp,
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.status,
            Some(StatusTransition::RecoveryInProgress)
        );
        assert_eq!(
            execution_info.new_shards,
            new_shards(&[0, 1, 2], ShardFill::ForceActive)
        );
        assert_eq!(execution_info.lock, shard_ids(&[3]));
        assert_eq!(execution_info.remove, shard_ids(&[4]));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::RecoveryTask
        );
    }

    #[test]
    fn in_sync_member_with_gained_shards_starts_shard_sync() {
        let execution_info = expect_apply(plan_epoch_change(&base_inputs()));
        assert_eq!(execution_info.status, None);
        assert_eq!(
            execution_info.new_shards,
            new_shards(&[1, 2], ShardFill::ShardSync)
        );
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
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
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(execution_info.status, None);
        assert!(execution_info.new_shards.is_none());
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
        );
    }

    #[test]
    fn outstanding_syncs_keep_shard_sync_as_attestation_owner() {
        // Gaining no shards does not mean the epoch-sync claim is complete: syncs from earlier
        // epochs may still be draining, and the attestation must wait for them.
        let inputs = PlanInputs {
            has_ongoing_shard_syncs: true,
            shards: ShardDiff {
                gained: vec![],
                lost: vec![],
                removed: vec![],
                all_owned: shard_ids(&[0]),
            },
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert!(execution_info.new_shards.is_none());
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
    }

    #[test]
    fn unfinished_metadata_recovery_keeps_shard_sync_as_attestation_owner() {
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoverMetadata,
            shards: ShardDiff {
                gained: vec![],
                lost: vec![],
                removed: vec![],
                all_owned: shard_ids(&[0]),
            },
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
    }

    #[test]
    fn dropout_with_outstanding_syncs_uses_finisher() {
        // A non-member makes no epoch-sync claim: the finisher (which skips attestation for
        // non-members) owns the token even if old syncs are still draining.
        let inputs = PlanInputs {
            in_current_committee: false,
            has_ongoing_shard_syncs: true,
            shards: ShardDiff {
                gained: vec![],
                lost: shard_ids(&[0, 1]),
                removed: vec![],
                all_owned: vec![],
            },
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
        );
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
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(execution_info.status, Some(StatusTransition::Standby));
        assert_eq!(execution_info.lock, shard_ids(&[0, 1]));
        assert_eq!(execution_info.remove, shard_ids(&[4]));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
        );
    }

    #[test]
    fn in_sync_new_joiner_recovers_metadata_first() {
        let inputs = PlanInputs {
            node_status: NodeStatus::Standby,
            in_previous_committee: false,
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.status,
            Some(StatusTransition::RecoverMetadata)
        );
        assert_eq!(
            execution_info.membership,
            MembershipAtEpochChange::NewMember
        );
        assert_eq!(
            execution_info.new_shards,
            new_shards(&[1, 2], ShardFill::ShardSync)
        );
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
    }

    #[test]
    fn in_sync_new_joiner_without_gained_shards_keeps_status() {
        let inputs = PlanInputs {
            node_status: NodeStatus::Standby,
            in_previous_committee: false,
            shards: ShardDiff::default(),
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(execution_info.status, None);
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
        );
    }

    #[test]
    fn in_sync_while_recovering_advances_target_and_keeps_task() {
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoveryInProgress(3),
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(
            execution_info.status,
            Some(StatusTransition::RecoveryInProgress)
        );
        assert_eq!(
            execution_info.new_shards,
            new_shards(&[1, 2], ShardFill::ShardSync)
        );
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::RecoveryTask
        );
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
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(execution_info.status, Some(StatusTransition::Standby));
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::StartEpochChangeFinisher
        );
    }

    #[test]
    fn in_sync_metadata_recovery_in_progress_keeps_status() {
        // An epoch change while a previous metadata recovery is still running does not touch the
        // node status; the metadata-recovery task flips it to `Active` when done.
        let inputs = PlanInputs {
            node_status: NodeStatus::RecoverMetadata,
            ..base_inputs()
        };
        let execution_info = expect_apply(plan_epoch_change(&inputs));
        assert_eq!(execution_info.status, None);
        assert_eq!(
            execution_info.sync_done_owner,
            EpochSyncDoneAttestationOwner::ShardSync
        );
    }
}
