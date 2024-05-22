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
    queue: BinaryHeap<Reverse<SequencedEventId>>,
}

impl EventSequencer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the provided (sequence_number, EventID) pair to those observed.
    pub fn add(&mut self, sequence_number: usize, event_id: EventID) {
        match sequence_number.cmp(&self.next_required_sequence_num) {
            Ordering::Equal => {
                self.next_required_sequence_num += 1;
                self.head = Some(event_id);

                // Attempt to advance the head, in the case that this filled a gap.
                while let Some(Reverse(SequencedEventId(next_sequence_num, _))) = self.queue.peek()
                {
                    if *next_sequence_num == self.next_required_sequence_num {
                        let Reverse(SequencedEventId(_, next_event)) = self.queue.pop().unwrap();

                        self.next_required_sequence_num += 1;
                        self.head = Some(next_event);
                    } else {
                        break;
                    }
                }
            }
            Ordering::Greater => {
                self.queue
                    .push(Reverse(SequencedEventId(sequence_number, event_id)));
            }
            Ordering::Less => {
                // Do nothing as we already advanced past this sequence number.
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct SequencedEventId(pub usize, pub EventID);

impl PartialOrd for SequencedEventId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SequencedEventId {
    fn cmp(&self, other: &Self) -> Ordering {
        let this = (self.0, self.1.tx_digest, self.1.event_seq);
        let other = (other.0, other.1.tx_digest, other.1.event_seq);

        this.cmp(&other)
    }
}
