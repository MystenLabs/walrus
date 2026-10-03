// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Attach Strata's pending-operation tables to the Walrus control database.
//! Walrus supplies event indexes, filters replays, and completes fan-out before publishing
//! barriers. Reference checks and shared blob coordination remain Walrus responsibilities.
//! Registrations cancel ordinary pending deletes in the same batch as their Walrus metadata.

use std::sync::Arc;

use rocksdb::Options;
use serde::{Deserialize, Serialize};
use strata::queue::{
    BlobCommand,
    BlobEdit,
    BlobOperand,
    BlobOperation,
    PENDING_BLOBS_CF,
    PendingQueue,
};
use strata_index::port::IndexDb;
use sui_types::event::EventID;
use typed_store::{
    Map,
    TypedStoreError,
    rocks::{DBBatch, DBMap, ReadWriteOptions, RocksDB},
};
use walrus_core::{BlobId, Epoch};

use super::DatabaseTableOptionsFactory;

mod database;

pub(crate) type StrataQueue = PendingQueue;

const REGISTRATIONS_CF: &str = "strata_registrations";

pub(super) fn options(factory: &DatabaseTableOptionsFactory) -> Vec<(&'static str, Options)> {
    strata::queue::cf_options(factory.standard())
        .into_iter()
        .chain([(REGISTRATIONS_CF, factory.standard())])
        .collect()
}

pub(super) fn database(database: &Arc<RocksDB>) -> Arc<dyn IndexDb> {
    Arc::new(database::Database(Arc::clone(database)))
}

/// The newest registration and a conservative lifetime across registrations. Deletion will
/// eventually retire this state; a shorter registration must never shorten another live reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StrataRegistration {
    pub event_index: u64,
    pub end_epoch: Epoch,
}

#[derive(Debug, Clone)]
pub(super) struct StrataRegistrations {
    records: DBMap<BlobId, StrataRegistration>,
    // Used only to stage raw merge operands. Queue VALUES use Strata's codec, not DBMap's BCS.
    pending: DBMap<Vec<u8>, Vec<u8>>,
}

impl StrataRegistrations {
    pub(super) fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        Ok(Self {
            records: DBMap::reopen(
                database,
                Some(REGISTRATIONS_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
            pending: DBMap::reopen(
                database,
                Some(PENDING_BLOBS_CF),
                &ReadWriteOptions::default(),
                false,
            )?,
        })
    }

    pub(super) fn register(
        &self,
        batch: &mut DBBatch,
        blob_id: BlobId,
        source: StrataSourceEvent,
        end_epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        let end_epoch = self
            .records
            .get(&blob_id)?
            .map_or(end_epoch, |previous| previous.end_epoch.max(end_epoch));
        let record = StrataRegistration {
            event_index: source.event_index,
            end_epoch,
        };
        let operand = BlobOperand::V1(BlobEdit::Register(BlobCommand {
            event_index: source.event_index,
            source: bcs::to_bytes(&source)
                .map_err(|e| TypedStoreError::SerializationError(e.to_string()))?,
            operation: BlobOperation::SetLifetime {
                end_epoch: u64::from(end_epoch),
            },
        }));
        batch.insert_batch(&self.records, [(blob_id, record)])?;
        batch.partial_merge_batch(
            &self.pending,
            [(blob_id.0.to_vec(), operand.encode().map_err(queue_error)?)],
        )?;
        Ok(())
    }

    pub(super) fn get(
        &self,
        blob_id: &BlobId,
    ) -> Result<Option<StrataRegistration>, TypedStoreError> {
        self.records.get(blob_id)
    }

    pub(super) fn is_empty(&self) -> Result<bool, TypedStoreError> {
        Ok(self.records.safe_iter()?.next().transpose()?.is_none())
    }
}

pub(super) fn queue_error(error: strata::queue::Error) -> TypedStoreError {
    TypedStoreError::TaskError(format!("Strata lifecycle queue: {error}"))
}

/// Declare after the blob guard and arm only once a write may start. Errors, panics, and task
/// cancellation must close admission before the guard is released; recovery resolves the outcome.
pub(super) struct HaltOnIncompleteWrite {
    queue: StrataQueue,
    complete: bool,
}

impl HaltOnIncompleteWrite {
    pub(super) fn new(queue: StrataQueue) -> Self {
        Self {
            queue,
            complete: false,
        }
    }
    pub(super) fn complete(&mut self) {
        self.complete = true;
    }
}

impl Drop for HaltOnIncompleteWrite {
    fn drop(&mut self) {
        if !self.complete {
            self.queue
                .halt("Walrus write did not complete; recovery required".into());
        }
    }
}

/// Application provenance, encoded as opaque bytes in generic queue commands.
/// The event index also identifies the lifetime initialization that a foreground put waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StrataSourceEvent {
    pub event_index: u64,
    pub event_id: EventID,
}

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod tests;
