// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Catchup module for catching up the event processor with the network.

use std::{fs, path::PathBuf};

use sui_types::{
    committee::Committee, event::EventID, messages_checkpoint::VerifiedCheckpoint,
    sui_serde::BigInt,
};
use tracing;
use typed_store::{Map, TypedStoreError, rocks::DBBatch};
use walrus_core::{BlobId, Epoch};
use walrus_sui::client::{SuiReadClient, contract_config::ContractConfig};
use walrus_utils::metrics::Registry;

use super::metrics::EventCatchupManagerMetrics;
use crate::event::{
    event_blob::EventBlob,
    event_processor::{
        config::{SuiClientSet, SystemConfig},
        db::EventProcessorStores,
    },
    events::{IndexedStreamEvent, InitState},
};

/// A struct that contains the metadata and events of a downloaded event blob.
#[derive(Clone)]
struct DownloadedBlob {
    blob_id: BlobId,
    /// Older blob.
    prev_blob_id: BlobId,
    /// The last event from the previous blob.
    prev_event_id: Option<EventID>,
    epoch: Epoch,
    first_event: Option<IndexedStreamEvent>,
    events: Vec<IndexedStreamEvent>,
    start_checkpoint: u64,
    end_checkpoint: u64,
}

impl std::fmt::Debug for DownloadedBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadedBlob")
            .field("blob_id", &self.blob_id)
            .field("prev_blob_id", &self.prev_blob_id)
            .field("prev_event_id", &self.prev_event_id)
            .field("epoch", &self.epoch)
            .field("first_event", &self.first_event)
            .field("event count", &self.events.len())
            .field("start_checkpoint", &self.start_checkpoint)
            .field("end_checkpoint", &self.end_checkpoint)
            .finish()
    }
}

/// Manages the catchup process for events in the event processor using event blobs.
///
/// This manager handles the process of catching up the local event store with the network state.
/// It performs the following steps:
/// 1. Checks if the local store is lagging behind the network
/// 2. If lagging, downloads and processes event blobs to catch up
/// 3. Maintains continuity of events and checkpoints during catchup
#[derive(Clone)]
pub struct EventBlobCatchupManager {
    stores: EventProcessorStores,
    clients: SuiClientSet,
    system_config: SystemConfig,
    recovery_path: PathBuf,
    metrics: EventCatchupManagerMetrics,
}

impl std::fmt::Debug for EventBlobCatchupManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBlobCatchupManager")
            .field("stores", &"EventProcessorStores")
            .field("clients", &"SuiClientSet")
            .field("system_config", &"SystemConfig")
            .finish()
    }
}

impl EventBlobCatchupManager {
    /// Creates a new instance of the event catchup manager.
    pub fn new(
        stores: EventProcessorStores,
        clients: SuiClientSet,
        system_config: SystemConfig,
        recovery_path: PathBuf,
        registry: &Registry,
    ) -> Self {
        let metrics = EventCatchupManagerMetrics::new(registry);
        Self {
            stores,
            clients,
            system_config,
            recovery_path,
            metrics,
        }
    }

    /// Checks if the event processor is lagging behind the network and performs catchup if needed.
    pub async fn catchup(&self, lag_threshold: u64) -> anyhow::Result<()> {
        let current_checkpoint = self.get_current_checkpoint()?;
        let latest_checkpoint = self.get_latest_network_checkpoint().await;

        let current_lag = self.calculate_lag(current_checkpoint, latest_checkpoint)?;

        if current_lag > lag_threshold {
            tracing::info!(
                current_checkpoint,
                latest_checkpoint,
                lag_threshold,
                "performing catchup - lag is above threshold"
            );
            self.perform_catchup().await?;
        } else {
            tracing::info!(
                current_checkpoint,
                latest_checkpoint,
                lag_threshold,
                "skipping catchup - lag is below threshold"
            );
        }

        Ok(())
    }

    /// Gets the current checkpoint from the store
    fn get_current_checkpoint(&self) -> Result<u64, TypedStoreError> {
        Ok(self
            .stores
            .checkpoint_store
            .get(&())?
            .map(|t| *t.inner().sequence_number())
            .unwrap_or(0))
    }

    /// Gets the latest checkpoint number from the network
    async fn get_latest_network_checkpoint(&self) -> Option<u64> {
        match self
            .clients
            .rpc_client
            .get_latest_checkpoint_summary()
            .await
        {
            Ok(summary) => Some(summary.sequence_number),
            Err(e) => {
                tracing::warn!(
                    error = ?e,
                    "Failed to get latest checkpoint summary, proceeding without lag check"
                );
                None
            }
        }
    }

    /// Calculates the lag between current and latest checkpoint
    fn calculate_lag(&self, current: u64, latest: Option<u64>) -> anyhow::Result<u64> {
        let lag = match latest {
            Some(latest) => {
                if current > latest {
                    tracing::error!(
                        current,
                        latest,
                        "Current store has a checkpoint that is greater than latest network \
                        checkpoint! This is especially likely when a node is restarted running
                        against a newer localnet, testnet or devnet network."
                    );
                    return Err(anyhow::anyhow!("Invalid checkpoint state"));
                }
                latest - current
            }
            None => {
                tracing::info!(
                    "Using 0 as fallback for current_lag since latest checkpoint is unavailable"
                );
                0
            }
        };
        Ok(lag)
    }

    /// Performs the catchup operation using event blobs
    pub async fn perform_catchup(&self) -> anyhow::Result<()> {
        match self.catchup_using_event_blobs().await {
            Ok(()) => {
                tracing::info!("successfully caught up using event blobs");
                Ok(())
            }
            Err(error) => {
                tracing::error!(?error, "failed to catch up using event blobs");
                Err(error)
            }
        }
    }

    /// Catch up the local event store using certified event blobs stored on Walrus nodes.
    ///
    /// This function performs the following steps:
    /// 1. Initializes Sui and Walrus clients for network communication.
    /// 2. Retrieves the last certified event blob from the network.
    /// 3. Iteratively fetches event blobs backwards from the latest, storing relevant ones locally:
    ///    - Stops when it reaches a blob containing events earlier than the local store's next
    ///      checkpoint.
    ///    - Temporarily stores relevant blobs in a local directory.
    /// 4. Processes stored blobs in reverse order (oldest to newest):
    ///    - Extracts events and inserts them into the local event database.
    ///    - Skips events that are already present in the local store.
    ///    - Updates checkpoints and committee information.
    ///    - Maintains initialization state for continuity.
    ///
    /// This catch-up mechanism ensures that it never introduces any gaps in stored events (i.e., if
    /// the last stored event index in local store is `N`, the catch-up will only store events
    /// starting from `N+1`). If however, the local store is empty, the catch-up will store all
    /// events from the earliest available event blob (in which case the first stored event index
    /// could be greater than `0`).
    async fn catchup_using_event_blobs(&self) -> anyhow::Result<()> {
        #[cfg(msim)]
        sui_macros::fail_point!("fail_point_catchup_using_event_blobs_start");

        let next_checkpoint = self.get_next_checkpoint()?;
        tracing::info!(next_checkpoint, "starting event catchup using event blobs");
        self.ensure_recovery_directory()?;

        let blobs = self
            .collect_event_blobs_for_catchup(next_checkpoint)
            .await?;
        let next_event_index = self.get_next_event_index()?;

        self.process_event_blobs(blobs, next_event_index).await?;

        Ok(())
    }

    /// Gets the next checkpoint sequence number that is after the latest checkpoint in the
    /// checkpoint store.
    fn get_next_checkpoint(&self) -> Result<Option<u64>, TypedStoreError> {
        Ok(self
            .stores
            .checkpoint_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(_, checkpoint)| checkpoint.inner().sequence_number + 1))
    }

    /// Gets the next event index that is after the latest event index in the event store.
    fn get_next_event_index(&self) -> Result<Option<u64>, TypedStoreError> {
        Ok(self
            .stores
            .event_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(i, _)| i + 1))
    }

    /// Ensures the recovery directory exists
    fn ensure_recovery_directory(&self) -> anyhow::Result<()> {
        if !self.recovery_path.exists() {
            fs::create_dir_all(&self.recovery_path)?;
        }
        Ok(())
    }

    /// Placeholder function for when the client feature is not enabled.
    #[cfg(not(feature = "client"))]
    pub async fn collect_event_blobs_for_catchup(
        starting_checkpoint_to_process: Option<u64>,
        recovery_path: &Path,
    ) -> Result<Vec<BlobId>> {
        Ok(vec![])
    }

    /// Downloads event blobs for catchup purposes.
    ///
    /// This function creates a client to download event blobs up to a specified
    /// checkpoint. The blobs are stored in the provided recovery path.
    #[cfg(feature = "client")]
    async fn collect_event_blobs_for_catchup(
        &self,
        starting_checkpoint_to_process: Option<u64>,
    ) -> anyhow::Result<Vec<BlobId>> {
        use crate::event::event_blob_downloader::EventBlobDownloader;

        let contract_config = ContractConfig::new(
            self.system_config.system_object_id,
            self.system_config.staking_object_id,
        );
        let sui_read_client =
            SuiReadClient::new(self.clients.sui_client.clone(), &contract_config).await?;
        let config = crate::client::ClientConfig::new_from_contract_config(contract_config);
        let walrus_client = walrus_sdk::client::WalrusNodeClient::new_read_client_with_refresher(
            config,
            sui_read_client.clone(),
        )
        .await?;
        let blob_downloader = EventBlobDownloader::new(walrus_client, sui_read_client);
        let blob_ids = blob_downloader
            .download(
                starting_checkpoint_to_process,
                None,
                &self.recovery_path,
                &self.metrics,
            )
            .await?;

        tracing::info!("successfully downloaded {} event blobs", blob_ids.len());
        Ok(blob_ids)
    }

    async fn process_event_blobs(
        &self,
        blobs: Vec<BlobId>,
        next_event_index: Option<u64>,
    ) -> anyhow::Result<()> {
        tracing::info!("starting to process event blobs");

        let mut num_events_recovered = 0;
        let mut next_event_index = next_event_index;

        for blob_id in blobs.iter().rev() {
            let downloaded_blob = self.process_single_blob(blob_id, next_event_index).await?;

            tracing::debug!("processed event blob {:?}", downloaded_blob);

            if downloaded_blob.events.is_empty() {
                // We break (rather than continue) because empty events indicates we've hit our
                // first "gap" in the sequence, and all future blobs will also have gaps. Here's
                // why:
                // We process blobs from oldest to newest (in chronological order)
                // For each blob, we only collect events that maintain a continuous sequence (no
                // gaps) with what's already in our database. If our last stored event in the DB has
                // index N, we only accept events starting at index N+1.
                // If a blob returns empty events, it means none of its events could maintain this
                // continuous sequence - there's a gap between our DB's last event and this blob's
                // first event. Since we're going forward in time, all future blobs will have even
                // larger gaps so there's no point in processing them.
                //
                // For example:
                // If our DB's last event has index 100
                // And we find a blob with events [200,201,202], it will return empty events
                // All future blobs will have indices > 200, making gaps even larger
                // So we can safely break the loop
                tracing::info!(
                    event_blob_id = %blob_id,
                    next_event_index = ?next_event_index,
                    "no relevant events found in event blob; breaking the loop"
                );
                break;
            }

            tracing::info!(
                "processed event blob {} with {} events, last event index: {}, \
                start checkpoint: {}, end checkpoint: {}",
                blob_id,
                downloaded_blob.events.len(),
                downloaded_blob
                    .events
                    .last()
                    .expect("event list is not empty")
                    .index,
                downloaded_blob.start_checkpoint,
                downloaded_blob.end_checkpoint
            );
            num_events_recovered += downloaded_blob.events.len();
            next_event_index = self.store_events_and_update_state(downloaded_blob).await?;
        }

        tracing::info!("recovered {} events from event blobs", num_events_recovered);
        Ok(())
    }

    async fn process_single_blob(
        &self,
        blob_id: &BlobId,
        next_event_index: Option<u64>,
    ) -> anyhow::Result<DownloadedBlob> {
        let blob_path = self.recovery_path.join(blob_id.to_string());
        let buf = std::fs::read(&blob_path)?;
        let event_blob = EventBlob::new(&buf)?;
        let prev_blob_id = event_blob.prev_blob_id();
        let prev_event_id = event_blob.prev_event_id();
        let epoch = event_blob.epoch();
        let start_checkpoint = event_blob.start_checkpoint_sequence_number();
        let end_checkpoint = event_blob.end_checkpoint_sequence_number();

        let (first_event, events) = self.collect_relevant_events(event_blob, next_event_index);

        Ok(DownloadedBlob {
            blob_id: *blob_id,
            prev_blob_id,
            prev_event_id,
            epoch,
            first_event,
            events,
            start_checkpoint,
            end_checkpoint,
        })
    }

    async fn store_events_and_update_state(
        &self,
        downloaded_blob: DownloadedBlob,
    ) -> anyhow::Result<Option<u64>> {
        // Note that this is the first event in the blob, which may be different from the first
        // event stored in `downloaded_blob.events`.
        let first_event_index = downloaded_blob
            .first_event
            .expect("event list is not empty")
            .index;
        let last_event_index = downloaded_blob
            .events
            .last()
            .expect("event list is not empty")
            .index;

        let mut batch = self.stores.event_store.batch();
        batch.insert_batch(
            &self.stores.event_store,
            downloaded_blob
                .events
                .iter()
                .map(|event| (event.index, event.element.clone())),
        )?;

        // Update checkpoint and committee information
        self.update_checkpoint_and_committee(&mut batch, downloaded_blob.end_checkpoint)
            .await?;

        // Update initialization state
        self.update_init_state(
            &mut batch,
            first_event_index,
            &downloaded_blob.prev_blob_id,
            downloaded_blob.prev_event_id,
            downloaded_blob.epoch,
        )
        .await?;

        batch.write()?;

        self.cleanup_blob_file(downloaded_blob.blob_id)?;
        Ok(Some(last_event_index + 1))
    }

    async fn update_checkpoint_and_committee(
        &self,
        batch: &mut DBBatch,
        last_checkpoint: u64,
    ) -> anyhow::Result<()> {
        let checkpoint_summary = self
            .clients
            .rpc_client
            .get_checkpoint_summary(last_checkpoint)
            .await?;
        let verified_checkpoint = VerifiedCheckpoint::new_unchecked(checkpoint_summary.clone());

        batch.insert_batch(
            &self.stores.checkpoint_store,
            [((), verified_checkpoint.serializable_ref())],
        )?;

        let next_committee = self.get_next_committee(&checkpoint_summary).await?;
        batch.insert_batch(
            &self.stores.committee_store,
            std::iter::once(((), next_committee)),
        )?;

        Ok(())
    }

    async fn get_next_committee(
        &self,
        checkpoint_summary: &sui_types::messages_checkpoint::CheckpointSummary,
    ) -> anyhow::Result<Committee> {
        if let Some(end_of_epoch_data) = &checkpoint_summary.end_of_epoch_data {
            Ok(Committee::new(
                checkpoint_summary.epoch + 1,
                end_of_epoch_data
                    .next_epoch_committee
                    .iter()
                    .cloned()
                    .collect(),
            ))
        } else {
            let committee_info = self
                .clients
                .sui_client
                .get_committee_info(Some(BigInt::from(checkpoint_summary.epoch)))
                .await?;
            Ok(Committee::new(
                committee_info.epoch,
                committee_info.validators.into_iter().collect(),
            ))
        }
    }

    async fn update_init_state(
        &self,
        batch: &mut DBBatch,
        first_event_index: u64,
        blob_id: &BlobId,
        prev_event_id: Option<EventID>,
        epoch: Epoch,
    ) -> Result<(), TypedStoreError> {
        tracing::debug!(
            blob_id = %blob_id,
            prev_event_id = ?prev_event_id,
            first_event_index,
            epoch,
            "updating init state"
        );
        let state = InitState::new(*blob_id, prev_event_id, first_event_index, epoch);
        batch.insert_batch(
            &self.stores.init_state,
            std::iter::once((first_event_index, state)),
        )?;
        Ok(())
    }

    /// Processes an event blob and returns relevant events that maintain a continuous sequence with
    /// the local store.
    ///
    /// Returns a tuple containing:
    /// - The first event in the blob (regardless of relevance)
    /// - A vector of relevant events paired with their indices
    ///
    /// Events are considered relevant if they either:
    /// - Start at the next expected index (when next_event_index is Some)
    /// - Or all events in the blob (when next_event_index is None)
    ///
    /// The function stops collecting events as soon as it encounters a gap in the sequence.
    fn collect_relevant_events(
        &self,
        event_blob: EventBlob,
        next_event_index: Option<u64>,
    ) -> (Option<IndexedStreamEvent>, Vec<IndexedStreamEvent>) {
        let mut iterator = event_blob.peekable();
        let first_event = iterator.peek().cloned();
        let relevant_events: Vec<IndexedStreamEvent> = iterator
            .skip_while(|event| next_event_index.is_some_and(|index| event.index < index))
            .scan(next_event_index, |state, event| match state {
                Some(expected_index) if event.index == *expected_index => {
                    *state = Some(*expected_index + 1);
                    Some(event)
                }
                None => {
                    // Ensure sequential event index stored in event blob.
                    *state = Some(event.index + 1);
                    Some(event)
                }
                _ => None,
            })
            .collect();
        (first_event, relevant_events)
    }

    // Clean up the blob file
    fn cleanup_blob_file(&self, blob_id: BlobId) -> anyhow::Result<()> {
        let blob_path = self.recovery_path.join(blob_id.to_string());
        fs::remove_file(blob_path)?;
        Ok(())
    }
}
