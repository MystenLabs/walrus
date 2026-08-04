// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Completion instructions for long-running tasks.
//!
//! A long-running task (node recovery, blob-metadata recovery) ends by transitioning the node
//! status — and, for node recovery, attesting `epoch_sync_done`. Which transition is correct is
//! not the task's decision: it is decided by the epoch-change executor when it starts or
//! advances the task's work, and can be *superseded* by a later epoch change (for example, the
//! node dropping out of the committee) while the task is still running.
//!
//! A [`BackgroundSyncTaskCompletionInstruction`] makes this explicit: the executor (or a
//! startup resumption path) mints the instruction inside the epoch-change critical section and
//! places it in the task's [`CompletionSlot`]; a later transition that supersedes it replaces
//! or clears the slot. On
//! success, the task consumes whatever instruction is present — a task whose work was
//! superseded finds none and finishes without touching the node status. A task can only
//! perform a status change that the epoch-change logic has pre-authorized and not revoked.

use typed_store::TypedStoreError;

use super::attestation::{EpochSyncDoneToken, Slot};
use crate::node::{NodeStatus, StorageNodeInner};

/// Instructs a long-running task what to do when its work completes successfully.
#[derive(Debug)]
pub(crate) struct BackgroundSyncTaskCompletionInstruction {
    /// The node status to transition to.
    new_status: NodeStatus,
    /// The `epoch_sync_done` attestation to send, if the task owns the attestation.
    attestation: Option<EpochSyncDoneToken>,
}

impl BackgroundSyncTaskCompletionInstruction {
    /// Creates an instruction. Minting is restricted to the epoch-change apply step and the
    /// startup resumption paths, mirroring [`EpochSyncDoneToken`]'s discipline.
    pub(crate) fn new(new_status: NodeStatus, attestation: Option<EpochSyncDoneToken>) -> Self {
        Self {
            new_status,
            attestation,
        }
    }

    /// Applies the instructed status transition, consuming the instruction, and returns the
    /// attestation to send, if any.
    ///
    /// Taking the instruction from its slot and applying it must both happen inside a single
    /// occupancy of the epoch-change critical section: clearing a slot only revokes an
    /// instruction that has not been taken yet, so a take outside the critical section could
    /// apply a status that a concurrent transition has just superseded. The returned
    /// attestation is a network call and should be sent after leaving the critical section
    /// (exclusivity then rides in the consumed token, not the critical section).
    pub(crate) fn apply_status(
        self,
        node: &StorageNodeInner,
    ) -> Result<Option<EpochSyncDoneToken>, TypedStoreError> {
        node.set_node_status(self.new_status)?;
        Ok(self.attestation)
    }
}

/// The slot holding the pending [`BackgroundSyncTaskCompletionInstruction`] of a long-running task.
pub(crate) type CompletionSlot = Slot<BackgroundSyncTaskCompletionInstruction>;
