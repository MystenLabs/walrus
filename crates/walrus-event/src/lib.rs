// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use sui_types::{event::EventID, messages_checkpoint::CheckpointSequenceNumber};
use walrus_core::BlobId;
use walrus_sui::types::{BlobEvent, ContractEvent};

pub mod event_processor;

/// Configuration for event processing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventProcessorConfig {
    /// The REST URL of the fullnode.
    pub rest_url: String,
    /// The path to the Sui genesis file.
    pub sui_genesis_path: PathBuf,
    /// Event pruning interval in number of seconds.
    pub pruning_interval: u64,
}

/// The sequence number of an event in the event stream. This is a combination of the sequence
/// number of the Sui checkpoint the event belongs to and the index of the event in the checkpoint.
#[derive(Eq, PartialEq, Default, Clone, Debug, Serialize, Deserialize)]
pub struct EventSequenceNumber {
    /// The sequence number of the Sui checkpoint an event belongs to.
    pub checkpoint_sequence_number: CheckpointSequenceNumber,
    /// Index of the event in the checkpoint.
    pub counter: u64,
}

impl EventSequenceNumber {
    pub fn new(checkpoint_sequence_number: CheckpointSequenceNumber, counter: u64) -> Self {
        Self {
            checkpoint_sequence_number,
            counter,
        }
    }

    /// Writes the event ID to the given buffer.
    #[allow(dead_code)]
    pub fn write(&self, wbuf: &mut BufWriter<File>) -> anyhow::Result<()> {
        wbuf.write_u64::<BigEndian>(self.checkpoint_sequence_number)?;
        wbuf.write_u64::<BigEndian>(self.counter)?;
        Ok(())
    }
    /// Reads an event ID from the given buffer.
    #[allow(dead_code)]
    pub(crate) fn read(rbuf: &mut BufReader<File>) -> anyhow::Result<EventSequenceNumber> {
        let sequence = rbuf.read_u64::<BigEndian>()?;
        let counter = rbuf.read_u64::<BigEndian>()?;
        Ok(EventSequenceNumber::new(sequence, counter))
    }
}

/// This enum represents elements in a stream of events, which can be either actual events or
/// markers
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStreamElement {
    ContractEvent(ContractEvent),
    CheckpointBoundary,
}

impl EventStreamElement {
    pub fn event_id(&self) -> Option<EventID> {
        match self {
            EventStreamElement::ContractEvent(event) => Some(event.event_id()),
            EventStreamElement::CheckpointBoundary => None,
        }
    }

    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            EventStreamElement::ContractEvent(event) => event.blob_id(),
            EventStreamElement::CheckpointBoundary => None,
        }
    }

    pub fn blob_event(&self) -> Option<&BlobEvent> {
        match self {
            EventStreamElement::ContractEvent(ContractEvent::BlobEvent(event)) => Some(event),
            _ => None,
        }
    }
}

/// An indexed element in the event stream.
#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct IndexedStreamElement {
    /// The walrus Blob event or a marker event.
    pub element: EventStreamElement,
    /// Unique identifier for the element within the overall sequence.
    pub global_sequence_number: EventSequenceNumber,
}

impl IndexedStreamElement {
    #[allow(dead_code)]
    pub fn new(contract_event: ContractEvent, event_sequence_number: EventSequenceNumber) -> Self {
        Self {
            element: EventStreamElement::ContractEvent(contract_event),
            global_sequence_number: event_sequence_number,
        }
    }

    /// Creates a new (non-existent) marker event that indicates the end of a checkpoint. This is
    /// used to commit the blob file at the end of every N checkpoints.
    pub fn new_checkpoint_boundary(
        sequence_number: CheckpointSequenceNumber,
        counter: u64,
    ) -> Self {
        Self {
            element: EventStreamElement::CheckpointBoundary,
            global_sequence_number: EventSequenceNumber::new(sequence_number, counter),
        }
    }

    pub fn is_end_of_checkpoint_marker(&self) -> bool {
        matches!(self.element, EventStreamElement::CheckpointBoundary)
    }

    pub fn is_end_of_epoch_event(&self) -> bool {
        // TODO: Update this once we add an epoch change event
        false
    }
}

/// A cursor that points to a specific element in the event stream.
#[derive(Debug, Clone)]
pub struct EventStreamCursor {
    pub event_id: Option<EventID>,
    pub element_index: u64,
}

impl EventStreamCursor {
    pub fn new(event_id: Option<EventID>, element_index: u64) -> Self {
        Self {
            event_id,
            element_index,
        }
    }
}
