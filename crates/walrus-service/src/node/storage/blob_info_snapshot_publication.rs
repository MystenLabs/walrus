// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The record of the blob info snapshot this node is publishing (storing and attesting).
//!
//! A snapshot that is not certified during its epoch is never certified: the contract rejects
//! attestations for any epoch but the current one, and the next epoch produces a different blob
//! ID. Metadata and slivers stored for such a snapshot have no blob-info entry and are therefore
//! invisible to garbage collection, so the node tracks every publication attempt durably, from
//! before the first write, and reconciles it at the next epoch boundary (see
//! `blob_info_snapshot_writer::reconcile_previous_publication`).

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};

use super::{DatabaseTableOptionsFactory, constants::blob_info_snapshot_publication_cf_name};

/// The progress of a snapshot publication attempt.
#[derive(Eq, PartialEq, Debug, Clone, Copy, Deserialize, Serialize)]
pub(crate) enum SnapshotPublicationState {
    /// The record is written; metadata and slivers may be partially stored.
    Pending,
    /// The metadata and this node's slivers are stored.
    Stored,
    /// The snapshot blob is attested on chain.
    Attested,
    /// The snapshot blob is certified on chain.
    Certified,
}

/// A versioned snapshot publication record.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub(crate) enum SnapshotPublication {
    V1(SnapshotPublicationV1),
}

/// Version 1 of the snapshot publication record.
#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SnapshotPublicationV1 {
    epoch: Epoch,
    blob_id: BlobId,
    state: SnapshotPublicationState,
}

impl SnapshotPublication {
    /// Creates a pending publication record for the snapshot of `epoch` with the given blob ID.
    pub fn new(epoch: Epoch, blob_id: BlobId) -> Self {
        Self::V1(SnapshotPublicationV1 {
            epoch,
            blob_id,
            state: SnapshotPublicationState::Pending,
        })
    }

    /// Returns the epoch of the snapshot.
    pub fn epoch(&self) -> Epoch {
        match self {
            Self::V1(v1) => v1.epoch,
        }
    }

    /// Returns the blob ID of the snapshot.
    pub fn blob_id(&self) -> BlobId {
        match self {
            Self::V1(v1) => v1.blob_id,
        }
    }

    /// Returns the state of the publication.
    pub fn state(&self) -> SnapshotPublicationState {
        match self {
            Self::V1(v1) => v1.state,
        }
    }

    /// Returns the record with the given state.
    pub fn with_state(&self, state: SnapshotPublicationState) -> Self {
        match self {
            Self::V1(v1) => Self::V1(SnapshotPublicationV1 {
                state,
                ..v1.clone()
            }),
        }
    }
}

/// The table holding the single current snapshot publication record.
#[derive(Debug, Clone)]
pub(super) struct SnapshotPublicationTable {
    inner: DBMap<(), SnapshotPublication>,
}

impl SnapshotPublicationTable {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let inner = DBMap::reopen(
            database,
            Some(blob_info_snapshot_publication_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;
        Ok(Self { inner })
    }

    pub fn options(db_table_opts_factory: &DatabaseTableOptionsFactory) -> (&'static str, Options) {
        (
            blob_info_snapshot_publication_cf_name(),
            db_table_opts_factory.blob_info_snapshot_publication(),
        )
    }

    /// Returns the current publication record, if any.
    pub fn get(&self) -> Result<Option<SnapshotPublication>, TypedStoreError> {
        self.inner.get(&())
    }

    /// Sets the current publication record, replacing any previous one.
    pub fn set(&self, record: &SnapshotPublication) -> Result<(), TypedStoreError> {
        self.inner.insert(&(), record)
    }

    /// Removes the current publication record.
    pub fn clear(&self) -> Result<(), TypedStoreError> {
        self.inner.remove(&())
    }
}
