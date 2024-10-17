// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Event processing functionality.

use std::{
    cmp::Ordering,
    fs::File,
    io::{BufReader, BufWriter, Read},
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use sui_rest_api::Client;
use sui_sdk::{
    rpc_types::{SuiObjectDataOptions, SuiTransactionBlockResponseOptions},
    SuiClient,
};
use sui_types::{
    base_types::ObjectID,
    committee::Committee,
    event::EventID,
    messages_checkpoint::{CheckpointSequenceNumber, TrustedCheckpoint, VerifiedCheckpoint},
    sui_serde::BigInt,
};
use typed_store::{rocks::DBMap, Map};
use walrus_core::{encoding::Primary, BlobId, Epoch};
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::{BlobEvent, ContractEvent},
};
use walrus_utils::checkpoint_downloader::AdaptiveDownloaderConfig;

use crate::{
    client::{Client as WalrusClient, ClientCommunicationConfig, Config},
    events::event_blob::EventBlob,
};

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
    /// The minimum number of checkpoints to lag behind the latest checkpoint before starting to
    /// catch up using event streams.
    pub event_stream_catchup_min_checkpoint_lag: u64,
}

impl EventProcessorConfig {
    /// Creates a new config with the default pruning interval of 1h.
    pub fn new_with_default_pruning_interval(rest_url: String) -> Self {
        Self {
            rest_url,
            pruning_interval: Duration::from_secs(3600),
            adaptive_downloader_config: Some(AdaptiveDownloaderConfig::default()),
            event_stream_catchup_min_checkpoint_lag: 10_000,
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

/// An indexed element in the event stream with a cursor that points to the element.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexedStreamElementWithCursor {
    /// The indexed stream element.
    pub element: IndexedStreamElement,
    /// The index of the element in the event stream.
    pub index: u64,
}

impl IndexedStreamElementWithCursor {
    /// Creates a new indexed stream element with a cursor.
    pub fn new(element: IndexedStreamElement, index: u64) -> Self {
        Self { element, index }
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

/// The initial state for event processing.
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
    /// Creates a new initial state.
    pub fn new(prev_blob_id: BlobId, event_index: u64, epoch: Epoch) -> Self {
        Self {
            prev_blob_id,
            event_cursor: EventStreamCursor::new(None, event_index),
            epoch,
        }
    }
}

/// Struct to group client-related parameters.
pub struct ClientConfig {
    /// Sui client.
    sui_client: SuiClient,
    /// Rest client for the full node.
    client: Client,
}

/// Struct to group object IDs.
pub struct SystemObjects {
    /// System object ID.
    system_object_id: ObjectID,
    /// Staking object ID.
    staking_object_id: ObjectID,
}

/// Struct to group database maps.
pub struct Database {
    /// Checkpoints database.
    checkpoints: DBMap<(), TrustedCheckpoint>,
    /// Events database.
    events: DBMap<u64, IndexedStreamElement>,
    /// Committee database.
    committee: DBMap<(), Committee>,
    /// Initial state database.
    init_state: DBMap<u64, InitState>,
}

/// Function to catch up events using event blobs.
pub async fn catchup_using_event_blobs(
    clients: ClientConfig,
    system_objects: SystemObjects,
    databases: Database,
) -> anyhow::Result<()> {
    let sui_read_client = SuiReadClient::new(
        clients.sui_client.clone(),
        system_objects.system_object_id,
        system_objects.staking_object_id,
    )
    .await?;
    let config = Config {
        system_object: system_objects.system_object_id,
        staking_object: system_objects.staking_object_id,
        exchange_object: None,
        wallet_config: None,
        communication_config: ClientCommunicationConfig::default(),
    };
    let walrus_client = WalrusClient::new(config, &sui_read_client)
        .await?
        .with_client(sui_read_client.clone())
        .await;
    let last_certified_event_blob = sui_read_client.last_certified_event_blob().await?;
    let Some(last_certified_event_blob) = last_certified_event_blob else {
        tracing::info!("No certified event blob exists to facilitate catchup using event blobs");
        return Ok(());
    };
    let zero_blob_id = BlobId([0; 32]);
    let next_checkpoint = databases
        .checkpoints
        .unbounded_iter()
        .skip_to_last()
        .next()
        .map(|(_, checkpoint)| checkpoint.inner().sequence_number + 1);
    let mut prev_event_blob = last_certified_event_blob.blob_id;
    let mut counter = 0;
    let temp_dir = tempfile::tempdir()?;
    while prev_event_blob != zero_blob_id {
        let blob = walrus_client
            .read_blob::<Primary>(&prev_event_blob)
            .await
            .context("should be able to read blob we just stored")?;
        let mut event_blob = EventBlob::new(&blob)?;
        prev_event_blob = event_blob.prev_blob_id();
        let blob_start_checkpoint = event_blob.start_checkpoint_sequence_number();
        let blob_end_checkpoint = event_blob.end_checkpoint_sequence_number();
        if let Some(next_checkpoint) = next_checkpoint {
            if blob_end_checkpoint >= next_checkpoint {
                event_blob.store_as_file(&temp_dir.as_ref().join(counter.to_string()))?;
                counter += 1;
            }
            if blob_start_checkpoint <= next_checkpoint {
                break;
            }
        }
    }
    let next_event_index = databases
        .events
        .unbounded_iter()
        .skip_to_last()
        .next()
        .map(|(i, _)| i + 1)
        .unwrap_or(0);
    for i in (0..counter).rev() {
        let file = File::open(temp_dir.as_ref().join(i.to_string()))?;
        let mut buf_reader = BufReader::new(file);
        let mut buf = Vec::new();
        buf_reader.read_to_end(&mut buf)?;
        let event_blob = EventBlob::new(&buf)?;
        let mut batch = databases.events.batch();
        let epoch = event_blob.epoch();
        let last_checkpoint = event_blob.end_checkpoint_sequence_number();
        for event in event_blob {
            if event.index < next_event_index {
                continue;
            }
            batch.insert_batch(
                &databases.events,
                std::iter::once((event.index, event.element)),
            )?;
        }
        // Download the checkpoint and committee for the next checkpoint
        let checkpoint_data = clients.client.get_full_checkpoint(last_checkpoint).await?;
        let sui_committee = clients
            .sui_client
            .governance_api()
            .get_committee_info(Some(BigInt::from(checkpoint_data.checkpoint_summary.epoch)))
            .await?;
        let next_committee = Committee::new(
            sui_committee.epoch,
            sui_committee.validators.into_iter().collect(),
        );
        let verified_checkpoint =
            VerifiedCheckpoint::new_unchecked(checkpoint_data.checkpoint_summary);
        batch.insert_batch(
            &databases.checkpoints,
            std::iter::once(((), verified_checkpoint.serializable_ref())),
        )?;
        batch.insert_batch(&databases.committee, std::iter::once(((), next_committee)))?;
        let state = InitState::new(prev_event_blob, next_event_index, epoch);
        batch.insert_batch(
            &databases.init_state,
            std::iter::once((next_event_index, state)),
        )?;
        batch.write()?;
    }

    Ok(())
}

/// Function to get the bootstrap committee and checkpoint from the FullNode.
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
