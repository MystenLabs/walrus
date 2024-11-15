// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use rocksdb::{MergeOperands, Options};
use sui_types::event::EventID;
use tracing::Level;
use typed_store::{
    rocks::{DBMap, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};

use super::{event_sequencer::EventSequencer, DatabaseConfig};

const CURSOR_KEY: [u8; 6] = *b"cursor";
const COLUMN_FAMILY_NAME: &str = "event_cursor";

type EventIdWithProgress = (EventID, u64);
type ProgressMergeOperand = (EventID, u64);

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct EventProgress {
    pub persisted: u64,
    pub pending: u64,
}

impl From<EventProgress> for walrus_sdk::api::EventProgress {
    fn from(progress: EventProgress) -> Self {
        Self {
            persisted: progress.persisted,
            pending: progress.pending,
        }
    }
}

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
            false,
        )?;

        let this = Self {
            inner,
            event_queue: Arc::default(),
        };

        let next_index = this.get_sequentially_processed_event_count()?;
        *this.event_queue.lock().unwrap() =
            EventSequencer::continue_from(next_index.try_into().expect("64-bit architecture"));

        Ok(this)
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
        let entry = self.inner.get(&CURSOR_KEY)?;
        Ok(entry.map_or(0, |(_, count)| count))
    }

    /// Returns the current event cursor and the next event index.
    pub fn get_event_cursor_and_next_index(
        &self,
    ) -> Result<Option<(EventID, u64)>, TypedStoreError> {
        self.inner.get(&CURSOR_KEY)
    }

    pub fn maybe_advance_event_cursor(
        &self,
        event_index: usize,
        cursor: &EventID,
    ) -> Result<EventProgress, TypedStoreError> {
        let mut event_queue = self.event_queue.lock().unwrap();
        event_queue.add(event_index, *cursor);
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
        }

        Ok(EventProgress {
            persisted: count,
            pending: event_queue.remaining(),
        })
    }

    /// Returns the current event cursor.
    pub fn get_event_cursor_progress(&self) -> Result<EventProgress, TypedStoreError> {
        let count = self.get_sequentially_processed_event_count()?;
        Ok(EventProgress {
            persisted: count,
            pending: self.event_queue.lock().unwrap().remaining(),
        })
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
        let (cursor, increment) = bcs::from_bytes::<ProgressMergeOperand>(operand_bytes)
            .expect("merge operand to be decodable");
        tracing::debug!("updating {current_val:?} with {cursor:?} (+{increment})");

        let updated_progress = current_val.map_or(0, |(_, progress)| progress) + increment;

        current_val = Some((cursor, updated_progress));
    }

    current_val.map(|value| bcs::to_bytes(&value).expect("this can be BCS-encoded"))
}
