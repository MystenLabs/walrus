// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex, Weak};

use rocksdb::{
    BoundColumnFamily,
    IteratorMode,
    MergeOperands,
    Options,
    WriteBatch,
    WriteBatchWithTransaction,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sui_types::event::EventID;
use tracing::Level;
use typed_store::{
    rocks::{DBMap, ReadWriteOptions, RocksDB, RocksDBBatch},
    Map,
    TypedStoreError,
};

use super::{event_sequencer::EventSequencer, DatabaseConfig};

const CURSOR_KEY: [u8; 6] = *b"cursor";
const COLUMN_FAMILY_NAME: &str = "event_cursor";

type EventIdWithProgress = (u64, EventID);
type ProgressMergeOperand = (EventID, u64);

#[derive(Debug, Clone)]
pub(super) struct EventCursorTable {
    inner: DBMap<[u8; 6], EventIdWithProgress>,
    event_queue: Arc<Mutex<EventSequencer>>,
}

impl EventCursorTable {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let inner = DBMap::reopen(
            database,
            Some(COLUMN_FAMILY_NAME),
            &ReadWriteOptions::default(),
        )?;

        Ok(Self {
            inner,
            event_queue: Arc::new(Mutex::new(EventSequencer::default())),
        })
    }

    pub fn options(config: &DatabaseConfig) -> (&'static str, Options) {
        let mut options = config.event_cursor.to_options();
        options.set_merge_operator(
            "update_cursor_and_progress",
            update_cursor_and_progress,
            |_, _, _| None,
        );
        (COLUMN_FAMILY_NAME, options)
    }

    pub fn get_sequentially_processed_event_count(&self) -> Result<u64, TypedStoreError> {
        if let Some((count, _)) = self.inner.get(&CURSOR_KEY)? {
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Get the event cursor for `event_type`
    pub fn get_event_cursor(&self) -> Result<Option<EventID>, TypedStoreError> {
        if let Some((_, cursor)) = self.inner.get(&CURSOR_KEY)? {
            Ok(Some(cursor))
        } else {
            Ok(None)
        }
    }

    pub fn maybe_advance_event_cursor(
        &self,
        sequence_number: usize,
        cursor: &EventID,
    ) -> Result<u64, TypedStoreError> {
        let mut event_queue = self.event_queue.lock().unwrap();

        event_queue.add(sequence_number, *cursor);

        let mut count = 0u64;
        let most_recent_cursor = event_queue
            .ready_events()
            .inspect(|_| {
                count += 1;
            })
            .last();

        if let Some(cursor) = most_recent_cursor {
            let merge_operand = bcs::to_bytes(&(cursor, count)).expect("encode to succeed");

            let mut batch = self.inner.batch();
            batch.partial_merge_batch(&self.inner, [(CURSOR_KEY, merge_operand)])?;
            batch.write()?;

            event_queue.advance();
            Ok(count)
        } else {
            Ok(0)
        }
    }
}

#[tracing::instrument(level = Level::DEBUG, skip(operands))]
fn update_cursor_and_progress(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut current_val: Option<EventIdWithProgress> = existing_val
        .map(|data| bcs::from_bytes(data).expect("value stored in database is decodable"));

    for operand_bytes in operands {
        let (cursor, progress_increment) = bcs::from_bytes::<ProgressMergeOperand>(operand_bytes)
            .expect("merge operand to be decodable");
        tracing::debug!("updating {current_val:?} with {cursor:?} (+{progress_increment})");

        let updated_progress = if let Some((progress, _)) = current_val {
            progress + progress_increment
        } else {
            progress_increment
        };

        current_val = Some((updated_progress, cursor));
    }

    current_val.map(|value| bcs::to_bytes(&value).unwrap())
}
