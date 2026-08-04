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

use std::sync::{Arc, Mutex};

use walrus_core::Epoch;

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

/// The slot holding the [`EpochSyncDoneToken`] of the component that currently owns the
/// attestation.
pub(crate) type AttestationSlot = Slot<EpochSyncDoneToken>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_clones_share_state() {
        let slot = AttestationSlot::default();
        let clone = slot.clone();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        let token = clone.take().expect("clone should see the placed token");
        assert_eq!(token.epoch(), 7);
        assert!(slot.take().is_none(), "the token can only be taken once");
    }

    #[test]
    fn put_replaces_unconsumed_value() {
        let slot = AttestationSlot::default();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        slot.put(EpochSyncDoneToken::new_for_epoch(8));
        assert_eq!(slot.take().expect("token should be present").epoch(), 8);
        assert!(slot.take().is_none());
    }

    #[test]
    fn clear_invalidates_value() {
        let slot = AttestationSlot::default();
        slot.put(EpochSyncDoneToken::new_for_epoch(7));
        slot.clear();
        assert!(slot.take().is_none());
    }
}
