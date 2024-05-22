// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    sync::{Arc, Mutex},
};

use sui_types::event::EventID;

/// On receiving a sequence of (sequence_number, EventID) pairs, tracks the highest sequentially
/// observed sequence_number.
#[derive(Debug, Default)]
pub(super) struct EventSequencer {
    // INV: head tracks the highest event ID before the gap in sequence indicated by
    // next_required_sequence_num.
    head: Option<EventID>,
    next_required_sequence_num: usize,
    queue: BinaryHeap<Reverse<Sequenced<EventID>>>,
}

impl EventSequencer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the provided (sequence_number, EventID) pair to those observed.
    ///
    /// Added sequence numbers must be unique over those observed up to this point.
    pub fn add(&mut self, sequence_number: usize, event_id: EventID) {
        match sequence_number.cmp(&self.next_required_sequence_num) {
            Ordering::Equal => {
                self.next_required_sequence_num += 1;
                self.head = Some(event_id);

                // Attempt to advance the head, in the case that this filled a gap.
                while let Some(Reverse(Sequenced(next_sequence_num, _))) = self.queue.peek() {
                    if *next_sequence_num == self.next_required_sequence_num {
                        let Reverse(Sequenced(_, next_event)) = self.queue.pop().unwrap();

                        self.next_required_sequence_num += 1;
                        self.head = Some(next_event);
                    } else {
                        break;
                    }
                }
            }
            Ordering::Greater => {
                debug_assert!(
                    !self
                        .queue
                        .iter()
                        .any(|Reverse(Sequenced(i, _))| *i == sequence_number),
                    "larger sequence number repeated"
                );
                self.queue
                    .push(Reverse(Sequenced(sequence_number, event_id)));
            }
            Ordering::Less => {
                // This class provides the invariant that we never advance unless we have seen all
                // prior sequence numbers, therefore anything encountered here is a duplicate.
                debug_assert!(
                    false,
                    "sequence number repeated: ({sequence_number}, {event_id:?})"
                );
                tracing::warn!(sequence_number, ?event_id, "sequence number repeated");
            }
        }
    }

    /// Returns the furthest EventID that has been observed in a contiguous sequence.
    pub fn peek(&self) -> Option<&EventID> {
        self.head.as_ref()
    }

    /// Returns the furthest EventID that has been observed in a contiguous sequence, and removes
    /// it from the sequence.
    pub fn pop(&mut self) -> Option<EventID> {
        self.head.take()
    }
}

/// A wrapper implementation of Ord that only considers the sequence number for comparison and
/// equality operations.
#[derive(Debug, Clone, Copy)]
struct Sequenced<T>(pub usize, pub T);

impl<T> PartialOrd for Sequenced<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Sequenced<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T> PartialEq for Sequenced<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for Sequenced<T> {}
