// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use sui_types::event::EventID;

/// A queue of contiguous events.
///
/// Given an arbitrary permutation of the index and [`EventID`] pairs `(0, EventID_0),
/// (1, EventID_1), (2, EventID_2), ...`, this queue sorts the event IDs by the their index and
/// provides access to the `EventID`s sequentially starting from 0.
#[derive(Debug, Default)]
pub(super) struct EventSequencer {
    head_index: u64,
    queue: HashMap<u64, EventID>,
}

impl EventSequencer {
    /// Creates a new `EventSequencer`, that continues sequencing from the provided next index.
    pub fn continue_from(head_index: u64) -> Self {
        Self {
            head_index,
            ..Self::default()
        }
    }

    /// The index of the next event to be observed. This is also the number of events that have been
    /// persisted.
    pub fn head_index(&self) -> u64 {
        self.head_index
    }

    /// Adds the provided (index, EventID) pair to those observed.
    ///
    /// # Panics
    ///
    /// Panics if the provided index has already been observed.
    pub fn add(&mut self, index: u64, event_id: EventID) {
        if index < self.head_index || self.queue.insert(index, event_id).is_some() {
            // This class provides the invariant that we never advance unless we have seen all
            // prior sequence numbers, therefore anything less than the head_index or with a prior
            // inserted value is a repeat.
            panic!("index repeated: ({index}, {event_id:?})");
        }
    }

    /// Returns an iterator the next contiguous observed event IDs.
    pub fn ready_events(&self) -> impl Iterator<Item = &EventID> {
        (self.head_index..).scan((), |_, i| self.queue.get(&i))
    }

    /// Advances the queue to the next gap in the sequence, discarding all events before the gap.
    pub fn advance(&mut self) {
        while self.queue.remove(&self.head_index).is_some() {
            self.head_index += 1;
        }
    }

    pub fn remaining(&self) -> u64 {
        self.queue.len().try_into().expect("usize is at most u64")
    }
}
