// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::{Ordering, Reverse},
    collections::{hash_map::Entry, BinaryHeap, HashMap},
    sync::{Arc, Mutex},
};

use sui_types::event::EventID;

/// On receiving a sequence of (sequence_number, EventID, BlobEventType) pairs, tracks the highest
/// sequentially observed sequence_number, and allows sequentially accessing the resulting queue.
#[derive(Debug, Default)]
pub(super) struct EventSequencer {
    next_required_sequence_num: usize,
    queue: HashMap<usize, EventID>,
}

impl EventSequencer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the provided (sequence_number, EventID) pair to those observed.
    ///
    /// Added sequence numbers must be unique over those observed up to this point.
    pub fn add(&mut self, sequence_number: usize, event_id: EventID) {
        dbg!(sequence_number);
        if sequence_number < self.next_required_sequence_num {
            // This class provides the invariant that we never advance unless we have seen all
            // prior sequence numbers, therefore anything encountered here is a duplicate.
            debug_assert!(
                false,
                "sequence number repeated: ({sequence_number}, {event_id:?})"
            );
            tracing::warn!(sequence_number, ?event_id, "sequence number repeated");
        }

        if self.queue.insert(sequence_number, event_id).is_some() {
            panic!("queued sequence number repeated")
        }
    }

    /// Returns an iterator over queued events that form a contiguous sequence.
    pub fn ready_events(&self) -> impl Iterator<Item = &EventID> {
        (self.next_required_sequence_num..).scan((), |_, i| self.queue.get(&i))
    }

    /// Advances the queue to the next gap in the sequence, discarding all events before the gap.
    pub fn advance(&mut self) {
        while self
            .queue
            .remove(&self.next_required_sequence_num)
            .is_some()
        {
            self.next_required_sequence_num += 1;
        }
    }
}
