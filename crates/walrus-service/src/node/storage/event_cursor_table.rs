// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
    Mutex,
};

use rocksdb::{MergeOperands, Options};
use serde::{Deserialize, Serialize};
use sui_types::event::EventID;
use tracing::Level;
use typed_store::{
    rocks::{DBMap, ReadWriteOptions, RocksDB},
    Map,
    TypedStoreError,
};

use super::{
    constants::{event_cursor_cf_name, event_cursor_key},
    event_sequencer::EventSequencer,
    DatabaseConfig,
};

type ProgressMergeOperand = (EventID, u64);

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub enum EventIdWithProgress {
    V1(EventIdWithProgressV1),
}

impl EventIdWithProgress {
    pub fn event_id(&self) -> EventID {
        match self {
            EventIdWithProgress::V1(v1) => v1.event_id,
        }
    }

    pub fn next_event_index(&self) -> u64 {
        match self {
            EventIdWithProgress::V1(v1) => v1.next_event_index,
        }
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct EventIdWithProgressV1 {
    event_id: EventID,
    next_event_index: u64,
}

#[derive(Debug, Copy, Clone, Default)]
pub(crate) struct EventProgress {
    /// The number of events that have been persisted.
    pub persisted: u64,
    /// The number of events that are pending to be persisted in the event sequencer.
    pub pending: u64,
    /// The highest event index that has been finished.
    pub highest_finished_event_index: u64,
}

impl From<EventProgress> for walrus_sdk::api::EventProgress {
    fn from(progress: EventProgress) -> Self {
        Self {
            persisted: progress.persisted,
            pending: progress.pending,
            highest_finished_event_index: Some(progress.highest_finished_event_index),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct EventCursorTable {
    inner: DBMap<[u8; 6], EventIdWithProgress>,
    event_queue: Arc<Mutex<EventSequencer>>,
    // Store the number of events that have been persisted and pending separately for fast access.
    persisted_event_count: Arc<AtomicU64>,
    pending_event_count: Arc<AtomicU64>,
    highest_finished_event_index: Arc<AtomicU64>,
}

impl EventCursorTable {
    pub fn reopen(database: &Arc<RocksDB>) -> Result<Self, TypedStoreError> {
        let inner = DBMap::reopen(
            database,
            Some(event_cursor_cf_name()),
            &ReadWriteOptions::default(),
            false,
        )?;

        let this = Self {
            inner,
            event_queue: Arc::default(),
            persisted_event_count: Arc::new(AtomicU64::new(0)),
            pending_event_count: Arc::new(AtomicU64::new(0)),
            highest_finished_event_index: Arc::new(AtomicU64::new(0)),
        };

        let next_index = this.get_sequentially_processed_event_count()?;
        this.persisted_event_count
            .store(next_index, Ordering::SeqCst);
        *this.event_queue.lock().unwrap() = EventSequencer::continue_from(next_index);

        Ok(this)
    }

    pub fn options(config: &DatabaseConfig) -> (&'static str, Options) {
        let mut options = config.event_cursor().to_options();
        options.set_merge_operator(
            "update_cursor_and_progress",
            update_cursor_and_progress,
            |_, _, _| None,
        );
        (event_cursor_cf_name(), options)
    }

    pub fn reposition_event_cursor(
        &self,
        cursor: EventID,
        next_index: u64,
    ) -> Result<(), TypedStoreError> {
        self.inner.insert(
            event_cursor_key(),
            &EventIdWithProgress::V1(EventIdWithProgressV1 {
                event_id: cursor,
                next_event_index: next_index,
            }),
        )?;
        self.persisted_event_count
            .store(next_index, Ordering::SeqCst);
        *self.event_queue.lock().unwrap() = EventSequencer::continue_from(next_index);

        Ok(())
    }

    pub fn get_sequentially_processed_event_count(&self) -> Result<u64, TypedStoreError> {
        let entry = self.inner.get(event_cursor_key())?;
        Ok(entry.map_or(0, |EventIdWithProgress::V1(v1)| v1.next_event_index))
    }

    /// Returns the current event cursor and the next event index.
    pub fn get_event_cursor_and_next_index(
        &self,
    ) -> Result<Option<EventIdWithProgress>, TypedStoreError> {
        self.inner.get(event_cursor_key())
    }

    pub fn maybe_advance_event_cursor(
        &self,
        event_index: u64,
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
            batch.partial_merge_batch(&self.inner, [(event_cursor_key(), merge_operand)])?;
            batch.write()?;

            event_queue.advance();
        }

        let persisted: u64 = event_queue.head_index();
        let pending = event_queue.remaining();

        // In debug mode, assert that the number of events processed matches the number of events.
        debug_assert_eq!(persisted, self.get_sequentially_processed_event_count()?);

        self.persisted_event_count
            .store(persisted, Ordering::SeqCst);
        self.pending_event_count.store(pending, Ordering::SeqCst);

        // Update highest_finished_event_index if the current event_index is higher
        let prev_index = self
            .highest_finished_event_index
            .fetch_max(event_index, Ordering::SeqCst);
        Ok(EventProgress {
            persisted,
            pending,
            highest_finished_event_index: std::cmp::max(prev_index, event_index),
        })
    }

    /// Returns the current event cursor.
    pub fn get_event_cursor_progress(&self) -> Result<EventProgress, TypedStoreError> {
        Ok(EventProgress {
            persisted: self.persisted_event_count.load(Ordering::SeqCst),
            pending: self.pending_event_count.load(Ordering::SeqCst),
            highest_finished_event_index: self.highest_finished_event_index.load(Ordering::SeqCst),
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
            .expect("merge operand should be decodable");
        tracing::trace!(?current_val, ?cursor, increment, "updating event cursor");

        let updated_progress =
            current_val.map_or(0, |EventIdWithProgress::V1(v1)| v1.next_event_index) + increment;

        current_val = Some(EventIdWithProgress::V1(EventIdWithProgressV1 {
            event_id: cursor,
            next_event_index: updated_progress,
        }));
    }

    current_val.map(|value| bcs::to_bytes(&value).expect("this can be BCS-encoded"))
}
