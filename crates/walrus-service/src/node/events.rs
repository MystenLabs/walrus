// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Service functionality for downloading and processing events from the full node.

use std::{
    cmp::Ordering,
    fmt::Debug,
    fs::File,
    io::{BufReader, BufWriter},
    time::Duration,
};

use anyhow::bail;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use sui_rpc_api::Client;
use sui_types::{event::EventID, messages_checkpoint::CheckpointSequenceNumber};
use walrus_core::{BlobId, Epoch};
use walrus_sui::types::{BlobEvent, ContractEvent};
use walrus_utils::checkpoint_downloader::AdaptiveDownloaderConfig;

pub mod event_blob;
pub mod event_blob_writer;
pub mod event_processor;

/// Configuration for event processing.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventProcessorConfig {
    /// The REST URL of the full node.
    pub rest_url: String,
    /// Event pruning interval.
    pub pruning_interval: Duration,
    /// Configuration options for the pipelined checkpoint fetcher.
    pub adaptive_downloader_config: Option<AdaptiveDownloaderConfig>,
    /// Minimum checkpoint lag threshold for event blob based catch-up.
    ///
    /// Specifies the minimum number of checkpoints the system must be behind
    /// the latest checkpoint before initiating catch-up using event blobs.
    /// This helps balance between catchup for small lags using checkpoints vs
    /// using event streams for longer checkpoint lags.
    pub event_stream_catchup_min_checkpoint_lag: u64,
}

impl EventProcessorConfig {
    /// Creates a new config with the default pruning interval of 1h.
    pub fn new_with_default_pruning_interval(rest_url: String) -> Self {
        Self {
            rest_url,
            pruning_interval: Duration::from_secs(3600),
            adaptive_downloader_config: Some(AdaptiveDownloaderConfig::default()),
            event_stream_catchup_min_checkpoint_lag: 20_000,
        }
    }

    /// Returns the checkpoint adaptive downloader configuration.
    pub fn adaptive_downloader_config(&self) -> AdaptiveDownloaderConfig {
        self.adaptive_downloader_config.clone().unwrap_or_default()
    }
}

/// The position of an event in the event stream. This is a combination of the sequence
/// number of the Sui checkpoint the event belongs to and the index of the event in the checkpoint.
#[derive(Eq, PartialEq, Default, Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointEventPosition {
    /// The sequence number of the Sui checkpoint an event belongs to.
    pub checkpoint_sequence_number: CheckpointSequenceNumber,
    /// Index of the event in the checkpoint.
    pub counter: u64,
}

impl CheckpointEventPosition {
    /// Creates a new event sequence number.
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
    pub(crate) fn read(rbuf: &mut BufReader<File>) -> anyhow::Result<CheckpointEventPosition> {
        let sequence = rbuf.read_u64::<BigEndian>()?;
        let counter = rbuf.read_u64::<BigEndian>()?;
        Ok(CheckpointEventPosition::new(sequence, counter))
    }
}

/// This enum represents elements in a stream of events, which can be either actual events or
/// markers
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventStreamElement {
    /// A contract event.
    ContractEvent(ContractEvent),
    /// A marker that indicates the end of a checkpoint.
    CheckpointBoundary,
}

impl EventStreamElement {
    /// Returns the event ID of the event, if it is an actual event.
    pub fn event_id(&self) -> Option<EventID> {
        match self {
            EventStreamElement::ContractEvent(event) => Some(event.event_id()),
            EventStreamElement::CheckpointBoundary => None,
        }
    }

    /// Returns the blob ID of the event, if it is an actual event.
    pub fn blob_id(&self) -> Option<BlobId> {
        match self {
            EventStreamElement::ContractEvent(event) => event.blob_id(),
            EventStreamElement::CheckpointBoundary => None,
        }
    }

    /// Returns the blob event, if it is an actual event.
    pub fn blob_event(&self) -> Option<&BlobEvent> {
        match self {
            EventStreamElement::ContractEvent(ContractEvent::BlobEvent(event)) => Some(event),
            _ => None,
        }
    }
}

/// An indexed element in the event stream.
#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct PositionedStreamEvent {
    /// The walrus Blob event or a marker event.
    pub element: EventStreamElement,
    /// Unique identifier for the element within the overall sequence.
    pub checkpoint_event_position: CheckpointEventPosition,
}

impl PositionedStreamEvent {
    /// Creates a new indexed stream element.
    #[allow(dead_code)]
    pub fn new(
        contract_event: ContractEvent,
        checkpoint_event_position: CheckpointEventPosition,
    ) -> Self {
        Self {
            element: EventStreamElement::ContractEvent(contract_event),
            checkpoint_event_position,
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
            checkpoint_event_position: CheckpointEventPosition::new(sequence_number, counter),
        }
    }

    /// Returns true if the element is a marker event that indicates the end of a checkpoint.
    pub fn is_end_of_checkpoint_marker(&self) -> bool {
        matches!(self.element, EventStreamElement::CheckpointBoundary)
    }

    /// Returns true if the element is an event that indicates the end of an epoch.
    pub fn is_end_of_epoch_event(&self) -> bool {
        // TODO: Update this once we add an epoch change event
        false
    }
}

/// An indexed element in the event stream with an index that points to the element.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexedStreamEvent {
    /// The indexed stream element.
    pub element: PositionedStreamEvent,
    /// The index of the element in the event stream.
    pub index: u64,
}

impl IndexedStreamEvent {
    /// Creates a new indexed stream element with a cursor.
    pub fn new(element: PositionedStreamEvent, index: u64) -> Self {
        Self { element, index }
    }
}

/// An indexed element in the event stream with an initialization state.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamEventWithInitState {
    /// The indexed stream element.
    pub element: PositionedStreamEvent,
    /// Init state required to initialize event blob writer and storage node's event tracking.
    pub init_state: Option<InitState>,
}

impl StreamEventWithInitState {
    /// Creates a new indexed stream element with a cursor.
    pub fn new(element: PositionedStreamEvent, init_state: Option<InitState>) -> Self {
        Self {
            element,
            init_state,
        }
    }
}

/// State that is needed to initialize event blob writer and storage node's event tracking.
/// This struct contains essential information required to start processing of events for blob
/// writing and processing:
///
/// - The ID of the previous written event blob
/// - The cursor pointing to the next event to be processed
/// - The current Walrus epoch of the event stream
///
/// InitState is like a snapshot/summary/checkpoint of all previous events at different points in
/// the event stream. Every time we download an event blob during node startup, we store an
/// InitState entry in our database. InitState serves two critical purposes - it helps both the
/// event blob writer and storage node initialize correctly and maintain event continuity given that
/// some initial event blobs may have expired (due to MAX_EPOCHS_AHEAD limitation).
///
/// For example: if there are total 1000 event blobs each containing 5 events that were generated
/// so far, and first 100 blobs have expired - a new node joining the network will bootstrap its
/// event db starting from event blob 101 with event index of the first event in its db being 500.
/// The reason we need this info because:
///
/// For Event Blob Writer:
/// - Needs to know how to start writing new blobs when joining the network
///   Uses InitState to:
/// - Get prev_blob_id to properly chain new blobs to existing ones
/// - Even if starting at event 500, needs to link to the blob that contained events [495, 499]
/// - Know which epoch it's starting in
/// - Ensures blob metadata is correct for that epoch
///
/// For Storage Node:
/// - Needs to know how to handle events when some early events aren't available
///   Uses InitState to:
/// - Recognize legitimate gaps in early events
/// - If starting at event 500, knows events [0, 499] are intentionally missing
/// - Can mark events [0, 499] as "processed" to prevent blocking
/// - Maintain proper event sequencing
/// - Won't wait forever for events that will never arrive (like [0, 499])
/// - Can still process new events correctly
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InitState {
    /// The blob ID of the previous event blob.
    pub prev_blob_id: BlobId,
    /// The cursor that points to the current element in the event stream.
    pub event_cursor: EventStreamCursor,
    /// The epoch of the event stream.
    pub epoch: Epoch,
}

impl InitState {
    /// This creates a new initialization state for event blob writing.
    pub fn new(
        prev_blob_id: BlobId,
        prev_event_id: Option<EventID>,
        event_index: u64,
        epoch: Epoch,
    ) -> Self {
        Self {
            prev_blob_id,
            event_cursor: EventStreamCursor::new(prev_event_id, event_index),
            epoch,
        }
    }
}

/// A cursor that points to a specific element in the event stream.
#[derive(Default, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventStreamCursor {
    /// The event ID of the event the cursor points to.
    pub event_id: Option<EventID>,
    /// The index of the element the cursor points to.
    pub element_index: u64,
}

impl EventStreamCursor {
    /// Creates a new cursor.
    pub fn new(event_id: Option<EventID>, element_index: u64) -> Self {
        Self {
            event_id,
            element_index,
        }
    }
}

impl PartialOrd for EventStreamCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventStreamCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.element_index.cmp(&other.element_index)
    }
}

/// Checks if the full node provides the required REST endpoint for event processing.
async fn check_experimental_rest_endpoint_exists(client: Client) -> anyhow::Result<bool> {
    // TODO: https://github.com/MystenLabs/walrus/issues/1049
    // TODO: Use utils::retry once it is outside walrus-service such that it doesn't trigger
    // cyclic dependency errors
    let latest_checkpoint = client.get_latest_checkpoint().await?;
    let mut total_remaining_attempts = 5;
    while client
        .get_full_checkpoint(latest_checkpoint.sequence_number)
        .await
        .is_err()
    {
        total_remaining_attempts -= 1;
        if total_remaining_attempts == 0 {
            return Ok(false);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(true)
}

/// Ensures that the full node provides the required REST endpoint for event processing.
async fn ensure_experimental_rest_endpoint_exists(client: Client) -> anyhow::Result<()> {
    if !check_experimental_rest_endpoint_exists(client.clone()).await? {
        bail!(
            "the configured full node *does not* provide the required REST endpoint for event \
            processing; make sure to configure a full node in the node's configuration file, which \
            provides the necessary endpoint"
        );
    } else {
        tracing::info!(
            "the configured full node provides the required REST endpoint for event processing"
        );
    }
    Ok(())
}
