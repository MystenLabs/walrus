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

/// A shareable slot holding the [`EpochSyncDoneToken`] of the component that currently owns the
/// attestation.
///
/// Each candidate owner (the shard-sync handler and the node-recovery handler) holds one slot;
/// the epoch-change apply step fills the owner's slot and clears the others. The owner takes the
/// token out when its half of the epoch-sync claim is complete.
#[derive(Debug, Clone, Default)]
pub(crate) struct AttestationSlot(Arc<Mutex<Option<EpochSyncDoneToken>>>);

impl AttestationSlot {
    /// Places a token in the slot, replacing (and thereby invalidating) any previous one.
    pub(crate) fn put(&self, token: EpochSyncDoneToken) {
        let replaced = self
            .0
            .lock()
            .expect("attestation slot mutex should not be poisoned")
            .replace(token);
        if let Some(replaced) = replaced {
            tracing::debug!(
                walrus.epoch = replaced.epoch(),
                "replacing an unconsumed epoch sync done token"
            );
        }
    }

    /// Removes and returns the token, if any.
    pub(crate) fn take(&self) -> Option<EpochSyncDoneToken> {
        self.0
            .lock()
            .expect("attestation slot mutex should not be poisoned")
            .take()
    }

    /// Clears the slot, invalidating any unconsumed token.
    pub(crate) fn clear(&self) {
        let _ = self.take();
    }
}
