// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Service functionality for downloading and processing events from the full node.

use std::{
    fs::File,
    io::{BufReader, BufWriter},
    time::Duration,
};

use anyhow::{anyhow, bail};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use sui_rpc_api::Client;
use sui_sdk::{
    rpc_types::{SuiObjectDataOptions, SuiTransactionBlockResponseOptions},
    SuiClient,
};
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    event::EventID,
    messages_checkpoint::{CheckpointSequenceNumber, VerifiedCheckpoint},
    sui_serde::BigInt,
};
use walrus_core::BlobId;
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
}

impl EventProcessorConfig {
    /// Creates a new config with the default pruning interval of 1h.
    pub fn new_with_default_pruning_interval(rest_url: String) -> Self {
        Self {
            rest_url,
            pruning_interval: Duration::from_secs(3600),
            adaptive_downloader_config: Some(AdaptiveDownloaderConfig::default()),
        }
    }

    /// Returns the checkpoint adaptive downloader configuration.
    pub fn adaptive_downloader_config(&self) -> AdaptiveDownloaderConfig {
        self.adaptive_downloader_config.clone().unwrap_or_default()
    }
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
pub struct IndexedStreamElement {
    /// The walrus Blob event or a marker event.
    pub element: EventStreamElement,
    /// Unique identifier for the element within the overall sequence.
    pub global_sequence_number: EventSequenceNumber,
}

impl IndexedStreamElement {
    /// Creates a new indexed stream element.
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

/// A cursor that points to a specific element in the event stream.
#[derive(Debug, Clone)]
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

/// Returns the next checkpoint sequence number.
pub async fn get_bootstrap_committee_and_checkpoint(
    sui_client: &SuiClient,
    client: Client,
    system_pkg_id: ObjectID,
) -> anyhow::Result<(Committee, VerifiedCheckpoint)> {
    let object = sui_client
        .read_api()
        .get_object_with_options(
            system_pkg_id,
            SuiObjectDataOptions::new()
                .with_bcs()
                .with_type()
                .with_previous_transaction(),
        )
        .await?;
    let txn = sui_client
        .read_api()
        .get_transaction_with_options(
            object
                .data
                .ok_or(anyhow!("No object data"))?
                .previous_transaction
                .ok_or(anyhow!("No transaction data"))?,
            SuiTransactionBlockResponseOptions::new(),
        )
        .await?;
    let checkpoint_data = client
        .get_full_checkpoint(txn.checkpoint.ok_or(anyhow!("No checkpoint data"))?)
        .await?;
    let sui_committee = sui_client
        .governance_api()
        .get_committee_info(Some(BigInt::from(checkpoint_data.checkpoint_summary.epoch)))
        .await?;
    let committee = Committee::new(
        sui_committee.epoch,
        sui_committee.validators.into_iter().collect(),
    );
    let verified_checkpoint = VerifiedCheckpoint::new_unchecked(checkpoint_data.checkpoint_summary);
    Ok((committee, verified_checkpoint))
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
