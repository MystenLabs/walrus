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

/// A shareable slot holding a consumable, single-owner value. Both long-running sync tasks
/// use one for their completion instruction: the shard-sync handler for the metadata-recovery
/// completion and the node-recovery handler for the recovery completion.
///
/// The epoch-change apply step fills a task's slot (and clears the slots that a transition
/// supersedes); the task takes the value out when its work completes. Clones share the same
/// slot, so a value placed through one handle is visible (and can be invalidated) through all
/// of them.
#[derive(Debug)]
pub(crate) struct Slot<T>(Arc<Mutex<Option<T>>>);

// `Clone` and `Default` are implemented by hand: deriving them would add `T: Clone` and
// `T: Default` bounds, which the slot does not need — clones share the one inner value, and a
// slot always starts empty.
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

/// The shard-sync attestation: the [`EpochSyncDoneToken`] together with the set of shards
/// whose syncs must complete before the token may be consumed.
///
/// The token and its pending shards are set atomically, inside the epoch-change critical
/// section, *after* all sync work has been quiesced and *before* the reconciler rebuilds the
/// syncs: every sync task in existence therefore belongs to the current pending set, and the
/// completion that empties the set consumes the token.
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
    /// Places the token, atomically recording the shards whose syncs must complete before it
    /// may be consumed. Replaces (and thereby invalidates) any previous token and pending set.
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

    /// Records the shard as synced. Returns the token if this empties the pending set, that
    /// is, every pending shard has synced.
    pub(crate) fn record_shard_synced(&self, shard: ShardIndex) -> Option<EpochSyncDoneToken> {
        let mut guard = self.lock();
        let state = guard.as_mut()?;
        state.pending_shards.remove(&shard);
        if state.pending_shards.is_empty() {
            return guard.take().map(|state| state.token);
        }
        None
    }

    /// Takes the token if no shard syncs are pending. Used by the metadata-recovery
    /// completion, which can be the last outstanding work once all pending shards have
    /// synced.
    pub(crate) fn take_if_complete(&self) -> Option<EpochSyncDoneToken> {
        let mut guard = self.lock();
        let state = guard.as_ref()?;
        if state.pending_shards.is_empty() {
            return guard.take().map(|state| state.token);
        }
        None
    }

    /// Clears the token and the pending set, invalidating any unconsumed token.
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
    fn token_is_consumed_by_the_completion_that_empties_the_pending_set() {
        let attestation = ShardSyncAttestation::default();
        attestation.set(
            EpochSyncDoneToken::new_for_epoch(8),
            [ShardIndex(1), ShardIndex(2)],
        );

        assert!(attestation.record_shard_synced(ShardIndex(1)).is_none());
        // Shards outside the pending set do not affect it.
        assert!(attestation.record_shard_synced(ShardIndex(7)).is_none());
        let token = attestation
            .record_shard_synced(ShardIndex(2))
            .expect("last pending shard should consume the token");
        assert_eq!(token.epoch(), 8);
        // The token can only be consumed once.
        assert!(attestation.record_shard_synced(ShardIndex(2)).is_none());
    }

    #[test]
    fn take_if_complete_requires_an_empty_pending_set() {
        let attestation = ShardSyncAttestation::default();
        attestation.set(EpochSyncDoneToken::new_for_epoch(8), [ShardIndex(1)]);
        assert!(attestation.take_if_complete().is_none());

        assert!(attestation.record_shard_synced(ShardIndex(1)).is_some());
        // Consumed by the recording; nothing left to take.
        assert!(attestation.take_if_complete().is_none());

        attestation.set(EpochSyncDoneToken::new_for_epoch(9), []);
        let token = attestation
            .take_if_complete()
            .expect("no pending shards means the claim is complete");
        assert_eq!(token.epoch(), 9);
    }

    #[test]
    fn clear_revokes_shard_sync_attestation() {
        let attestation = ShardSyncAttestation::default();
        attestation.set(EpochSyncDoneToken::new_for_epoch(8), [ShardIndex(1)]);
        attestation.clear();
        assert!(attestation.record_shard_synced(ShardIndex(1)).is_none());
    }
}
