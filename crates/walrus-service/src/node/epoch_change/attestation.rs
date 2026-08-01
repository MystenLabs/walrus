// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Single-owner `epoch_sync_done` attestation.
//!
//! [`EpochSyncDoneToken`] makes the "exactly one attester per epoch change" rule structural
//! instead of emergent: the token cannot be cloned, attesting consumes it, and at most one live
//! token exists at a time. A component without the token cannot attest, so no negative guards
//! (such as "skip attestation while the node is recovering") are needed.
//!
//! Tokens are minted at exactly two kinds of places:
//!
//! - The epoch-change apply step mints the token for the new epoch and hands it to the
//!   attestation owner named in the epoch-change plan — the epoch-change finisher, the
//!   shard-sync handler, or the node-recovery handler — while clearing the slots of the other
//!   components.
//! - At startup, when resuming interrupted work whose `EpochChangeStart` event was already
//!   marked complete (resumed shard syncs, or a resumed node recovery), the resuming component
//!   mints the token itself, mirroring the ownership it held before the restart.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use walrus_core::{Epoch, ShardIndex};

use crate::node::StorageNodeInner;

/// The right — and the obligation — to attest `epoch_sync_done` for the contained epoch.
///
/// The token is deliberately not `Clone`: attesting consumes it, so double attestation for one
/// epoch change is unrepresentable.
#[derive(Debug)]
pub(crate) struct EpochSyncDoneToken {
    epoch: Epoch,
}

impl EpochSyncDoneToken {
    /// Mints the token for the given epoch.
    ///
    /// Minting is restricted to the epoch-change apply step and the startup resumption paths
    /// (see the module documentation); no other component may mint a token.
    pub(crate) fn new_for_epoch(epoch: Epoch) -> Self {
        Self { epoch }
    }

    /// The epoch this token attests.
    pub(crate) fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Attests `epoch_sync_done` for the token's epoch, consuming the token.
    ///
    /// The underlying contract call retries internally and drops the attestation if a newer
    /// epoch has already been attested or the epoch is stale on chain.
    pub(crate) async fn attest(self, node: &StorageNodeInner) {
        tracing::info!(walrus.epoch = self.epoch, "attesting epoch sync done");
        node.contract_service
            .epoch_sync_done(self.epoch, node.node_capability())
            .await;
    }
}

/// A shareable slot holding a consumable, single-owner value — an attestation token or a
/// completion instruction.
///
/// Each candidate owner (the shard-sync handler and the node-recovery handler) holds one slot;
/// the epoch-change apply step fills the owner's slot and clears the others. The owner takes the
/// value out when the condition it is waiting for is met. Clones share the same slot, so a value
/// placed through one handle is visible (and can be invalidated) through all of them.
#[derive(Debug)]
pub(crate) struct Slot<T>(Arc<Mutex<Option<T>>>);

// Manual impls: the derives would incorrectly require `T: Clone` / `T: Default`.
impl<T> Clone for Slot<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}

impl<T: std::fmt::Debug> Slot<T> {
    /// Places a value in the slot, replacing (and thereby invalidating) any previous one.
    pub(crate) fn put(&self, value: T) {
        let replaced = self
            .0
            .lock()
            .expect("slot mutex should not be poisoned")
            .replace(value);
        if let Some(replaced) = replaced {
            tracing::debug!(?replaced, "replacing an unconsumed slot value");
        }
    }

    /// Removes and returns the value, if any.
    pub(crate) fn take(&self) -> Option<T> {
        self.0
            .lock()
            .expect("slot mutex should not be poisoned")
            .take()
    }

    /// Clears the slot, invalidating any unconsumed value.
    pub(crate) fn clear(&self) {
        let _ = self.take();
    }
}

/// The shard-sync attestation: the [`EpochSyncDoneToken`] together with the set of shards whose
/// syncs must complete before the token may be consumed.
///
/// The token and its pending shards are registered atomically, *before* the corresponding sync
/// tasks are spawned. This closes a race with sync tasks left over from an earlier epoch: such
/// a task can observe the in-progress task map as empty (the new epoch's tasks may not have
/// been inserted yet), but it cannot consume the new token, because the new shards are already
/// registered as pending. Conversely, a leftover task that finishes a shard the new epoch also
/// gained counts toward the new registration, since the shard's data is there either way.
///
/// Clones share the same state.
#[derive(Debug, Default, Clone)]
pub(crate) struct ShardSyncAttestation(Arc<Mutex<Option<ShardSyncAttestationState>>>);

#[derive(Debug)]
struct ShardSyncAttestationState {
    token: EpochSyncDoneToken,
    pending_shards: HashSet<ShardIndex>,
}

impl ShardSyncAttestation {
    /// Places the token, atomically registering the shards that must complete their syncs
    /// before it may be consumed. Replaces (and thereby invalidates) any previous registration.
    pub(crate) fn set(
        &self,
        token: EpochSyncDoneToken,
        pending_shards: impl IntoIterator<Item = ShardIndex>,
    ) {
        let replaced = self.lock().replace(ShardSyncAttestationState {
            token,
            pending_shards: pending_shards.into_iter().collect(),
        });
        if let Some(replaced) = replaced {
            tracing::debug!(?replaced, "replacing an unconsumed shard sync attestation");
        }
    }

    /// Registers additional shards that must complete their syncs before the token may be
    /// consumed. A no-op if no token is registered.
    ///
    /// Used by the sync-shards task when it derives its full work list (during metadata
    /// recovery, all owned shards instead of only the newly gained ones): the derived shards
    /// are registered before their sync tasks are spawned.
    pub(crate) fn register_pending_shards(&self, shards: impl IntoIterator<Item = ShardIndex>) {
        if let Some(state) = self.lock().as_mut() {
            state.pending_shards.extend(shards);
        }
    }

    /// Records the shard as synced (its data is present locally). Returns the token if this
    /// completes the registration — every registered shard synced — and no other sync task is
    /// running (`no_other_sync_running`; this guards leftover tasks from earlier epochs that
    /// are still draining and whose shards the node may still own).
    pub(crate) fn record_shard_synced(
        &self,
        shard: ShardIndex,
        no_other_sync_running: bool,
    ) -> Option<EpochSyncDoneToken> {
        let mut guard = self.lock();
        let state = guard.as_mut()?;
        state.pending_shards.remove(&shard);
        if state.pending_shards.is_empty() && no_other_sync_running {
            return guard.take().map(|state| state.token);
        }
        None
    }

    /// Clears the registration, invalidating any unconsumed token.
    pub(crate) fn clear(&self) {
        let _ = self.lock().take();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<ShardSyncAttestationState>> {
        self.0
            .lock()
            .expect("shard sync attestation mutex should not be poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_clones_share_state() {
        let slot = Slot::<EpochSyncDoneToken>::default();
        let clone = slot.clone();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        let token = clone.take().expect("clone should see the placed token");
        assert_eq!(token.epoch(), 7);
        assert!(slot.take().is_none(), "the token can only be taken once");
    }

    #[test]
    fn put_replaces_unconsumed_value() {
        let slot = Slot::<EpochSyncDoneToken>::default();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        slot.put(EpochSyncDoneToken::new_for_epoch(8));
        assert_eq!(slot.take().expect("token should be present").epoch(), 8);
        assert!(slot.take().is_none());
    }

    #[test]
    fn clear_invalidates_value() {
        let slot = Slot::<EpochSyncDoneToken>::default();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        slot.clear();
        assert!(slot.take().is_none());
    }

    #[test]
    fn old_epoch_task_cannot_consume_token_with_pending_shards() {
        // Regression test: the token is registered together with the new epoch's shards before
        // their sync tasks exist. A leftover task from the previous epoch that observes no
        // other running syncs must not consume it while the new shards are pending.
        let attestation = ShardSyncAttestation::default();
        attestation.set(
            EpochSyncDoneToken::new_for_epoch(8),
            [ShardIndex(1), ShardIndex(2)],
        );

        // The old-epoch task finishes its shard (not part of the new registration) and sees the
        // task map as empty.
        assert!(
            attestation
                .record_shard_synced(ShardIndex(7), true)
                .is_none()
        );

        // The new shards complete; the last one consumes the token.
        assert!(
            attestation
                .record_shard_synced(ShardIndex(1), false)
                .is_none()
        );
        let token = attestation
            .record_shard_synced(ShardIndex(2), true)
            .expect("last registered shard should consume the token");
        assert_eq!(token.epoch(), 8);
    }

    #[test]
    fn token_is_not_consumed_while_other_syncs_run() {
        // Even with every registered shard synced, draining tasks from earlier epochs (still
        // present in the in-progress map) defer the attestation until they finish.
        let attestation = ShardSyncAttestation::default();
        attestation.set(EpochSyncDoneToken::new_for_epoch(8), [ShardIndex(1)]);

        assert!(
            attestation
                .record_shard_synced(ShardIndex(1), false)
                .is_none()
        );
        // The draining old-epoch task finishes last and consumes the token for the new epoch.
        let token = attestation
            .record_shard_synced(ShardIndex(7), true)
            .expect("final completion should consume the token");
        assert_eq!(token.epoch(), 8);
    }

    #[test]
    fn additionally_registered_shards_defer_consumption() {
        let attestation = ShardSyncAttestation::default();
        attestation.set(EpochSyncDoneToken::new_for_epoch(8), [ShardIndex(1)]);
        attestation.register_pending_shards([ShardIndex(2)]);

        assert!(
            attestation
                .record_shard_synced(ShardIndex(1), true)
                .is_none()
        );
        assert!(
            attestation
                .record_shard_synced(ShardIndex(2), true)
                .is_some()
        );
    }

    #[test]
    fn clear_revokes_shard_sync_attestation() {
        let attestation = ShardSyncAttestation::default();
        attestation.set(EpochSyncDoneToken::new_for_epoch(8), [ShardIndex(1)]);
        attestation.clear();
        assert!(
            attestation
                .record_shard_synced(ShardIndex(1), true)
                .is_none()
        );
    }
}
