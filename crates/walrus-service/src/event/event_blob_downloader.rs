// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Responsible for downloading and managing event blobs

use std::{
    path::Path,
    time::{Duration, Instant},
};

use anyhow::Result;
use walrus_core::{BlobId, Epoch};
use walrus_sdk::{client::Client as WalrusClient, error::ClientErrorKind};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::move_structs::EventBlob,
};

use super::event_processor::metrics::EventCatchupManagerMetrics;
use crate::event::{event_blob::EventBlob as LocalEventBlob, events::EventStreamCursor};

/// A struct that contains the metadata of an event blob.
#[derive(Debug, Clone)]
pub struct EventBlobWithMetadata {
    /// The ID of the event blob.
    pub blob_id: BlobId,
    /// The cursor of the event stream.
    pub event_stream_cursor: EventStreamCursor,
    /// The epoch of the event blob.
    pub epoch: Epoch,
    /// The ending checkpoint sequence number of the event blob.
    pub ending_checkpoint_sequence_number: u64,
}

impl EventBlobWithMetadata {
    /// Returns the ending checkpoint sequence number of the event blob.
    pub fn end(&self) -> u64 {
        self.event_stream_cursor.element_index
    }

    /// Returns a reference to the event blob.
    pub fn blob(&self) -> EventBlob {
        EventBlob {
            blob_id: self.blob_id,
            ending_checkpoint_sequence_number: self.ending_checkpoint_sequence_number,
        }
    }
}

/// This is used to store the last certified event blob in the event blob writer.
#[derive(Debug, Clone)]
pub enum LastCertifiedEventBlob {
    /// A struct that contains the metadata of an event blob.
    EventBlobWithMetadata(EventBlobWithMetadata),
    /// A reference to an event blob.
    EventBlob(EventBlob),
}

/// Responsible for downloading and managing event blobs
#[derive(Debug)]
pub struct EventBlobDownloader {
    walrus_client: WalrusClient<SuiReadClient>,
    sui_read_client: SuiReadClient,
}

impl EventBlobDownloader {
    /// Creates a new instance of the event blob downloader.
    pub fn new(walrus_client: WalrusClient<SuiReadClient>, sui_read_client: SuiReadClient) -> Self {
        Self {
            walrus_client,
            sui_read_client,
        }
    }

    /// Returns the metadata of the last certified event blob.
    pub async fn get_last_certified_event_blob(&self) -> Result<Option<EventBlobWithMetadata>> {
        let Some(blob) = self.sui_read_client.last_certified_event_blob().await? else {
            return Ok(None);
        };
        let blob_id = blob.blob_id;
        let result = self
            .walrus_client
            .read_blob::<walrus_core::encoding::Primary>(&blob_id)
            .await?;
        let blob = LocalEventBlob::new(&result)?;
        let blob_epoch = blob.ending_epoch()?;
        let blob_ending_checkpoint_sequence_number = blob.end_checkpoint_sequence_number();
        let blob_event = blob.into_iter().last().ok_or(anyhow::anyhow!(
            "last certified event blob does not contain any events"
        ))?;
        Ok(Some(EventBlobWithMetadata {
            blob_id,
            event_stream_cursor: EventStreamCursor::new(
                blob_event.element.element.event_id(),
                blob_event.index + 1,
            ),
            epoch: blob_epoch,
            ending_checkpoint_sequence_number: blob_ending_checkpoint_sequence_number,
        }))
    }

    /// Collects event blobs starting from the given blob ID and going backwards until reaching
    /// the specified checkpoint or an expired blob.
    ///
    /// Returns a vector of blob IDs in reverse chronological order (newest to oldest).
    pub async fn download(
        &self,
        starting_checkpoint_to_process: Option<u64>,
        from_blob: Option<BlobId>,
        path: &Path,
        metrics: &EventCatchupManagerMetrics,
    ) -> Result<Vec<BlobId>> {
        let mut blobs = Vec::new();
        let mut event_blob_id = match from_blob {
            Some(blob) => blob,
            None => match self.sui_read_client.last_certified_event_blob().await? {
                Some(blob) => blob.blob_id,
                None => {
                    tracing::info!("no certified event blobs found");
                    return Ok(vec![]);
                }
            },
        };

        tracing::info!(
            starting_checkpoint_to_process = ?starting_checkpoint_to_process,
            from_blob = ?from_blob,
            "starting download of event blobs from latest blob ID {} and going backwards",
            event_blob_id
        );

        // Track if we are reading the first certified event blob. First event blob has special
        // handling in `read_event_blob_status()`. See the comment there for more details.
        let mut reading_first_event_blob = true;

        loop {
            if event_blob_id == BlobId::ZERO {
                tracing::info!("reached the beginning of the event history",);
                break;
            }

            // Timer for waiting for the first certified event blob to be certified.
            let start_time = Instant::now();
            let blob_status = self
                .read_event_blob_status(event_blob_id, reading_first_event_blob, start_time)
                .await?;

            if blob_status == BlobStatus::Nonexistent {
                anyhow::ensure!(!blobs.is_empty(), "no available event blobs found");
                tracing::info!(
                    "stopping downloading event blobs with expired blob {}",
                    event_blob_id
                );
                break;
            }

            let blob_path = path.join(event_blob_id.to_string());
            let (blob, blob_source) = if blob_path.exists() {
                (std::fs::read(blob_path.as_path())?, "local")
            } else {
                let start_time = Instant::now();
                self.read_event_blob(
                    event_blob_id,
                    blob_status,
                    reading_first_event_blob,
                    start_time,
                )
                .await?
            };

            reading_first_event_blob = false;

            metrics
                .event_catchup_manager_event_blob_fetched
                .with_label_values(&[blob_source])
                .inc();

            tracing::info!(blob_id = %event_blob_id, "finished reading event blob");

            let mut event_blob = LocalEventBlob::new(&blob)?;

            let should_store = match starting_checkpoint_to_process {
                Some(starting_checkpoint_to_process) => {
                    event_blob.end_checkpoint_sequence_number() >= starting_checkpoint_to_process
                }
                None => true,
            };

            if should_store {
                blobs.push(event_blob_id);
                let num_checkpoints_stored = event_blob.end_checkpoint_sequence_number()
                    - event_blob.start_checkpoint_sequence_number()
                    + 1;
                tracing::info!(
                    "storing event blob {} with {} checkpoints",
                    event_blob_id,
                    num_checkpoints_stored
                );
            } else {
                tracing::info!(
                    "skipping event blob {} as it contains events only before the next checkpoint",
                    event_blob_id
                );
                break;
            }

            if !blob_path.exists() {
                event_blob.store_as_file(&blob_path)?;
            }

            if let Some(starting_checkpoint_to_process) = starting_checkpoint_to_process
                && event_blob.start_checkpoint_sequence_number() <= starting_checkpoint_to_process
            {
                break;
            }

            event_blob_id = event_blob.prev_blob_id();
        }

        Ok(blobs)
    }

    /// Reads the status of an event blob.
    ///
    /// When reading the first certified event blob, it may be the case that the event blob is
    /// just certified, and the storage nodes may not know it yet. Therefore, upon encountering
    /// a nonexistent blob, we will retry and wait for the blob to be certified.
    /// For all the earlier blobs, they must have been seen by the storage nodes, since
    /// otherwise, no new certified event blobs would be created.
    async fn read_event_blob_status(
        &self,
        event_blob_id: BlobId,
        reading_first_event_blob: bool,
        start_time: Instant,
    ) -> Result<BlobStatus> {
        let blob_status = loop {
            let blob_status = match self
                .walrus_client
                .get_blob_status_with_retries(&event_blob_id, &self.sui_read_client)
                .await
            {
                Ok(blob_status) => blob_status,
                Err(err) if matches!(err.kind(), ClientErrorKind::BlobIdDoesNotExist) => {
                    BlobStatus::Nonexistent
                }
                Err(err) => return Err(err.into()),
            };

            if blob_status == BlobStatus::Nonexistent && reading_first_event_blob {
                tracing::debug!(
                    "reading first certified event blob {} encountered a nonexistent \
                blob, waiting for it to be certified",
                    event_blob_id
                );

                if start_time.elapsed() > Duration::from_secs(30) {
                    tracing::warn!(
                        "waiting for first certified event blob to be certified timed out"
                    );
                    break blob_status;
                }
                // Short sleep since we expect storage nodes to keep up with Sui events in most
                // of the time.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            break blob_status;
        };
        Ok(blob_status)
    }

    /// Reads an event blob.
    ///
    /// When reading the first certified event blob, it may be the case that the event blob is
    /// just certified, and the storage nodes may not know it yet. Therefore, upon encountering
    /// a nonexistent blob, we will retry reading the blob.
    async fn read_event_blob(
        &self,
        event_blob_id: BlobId,
        blob_status: BlobStatus,
        reading_first_event_blob: bool,
        start_time: Instant,
    ) -> Result<(Vec<u8>, &'static str)> {
        let (blob, blob_source) = loop {
            match self
                .walrus_client
                .read_blob_with_status::<walrus_core::encoding::Primary>(
                    &event_blob_id,
                    blob_status,
                )
                .await
            {
                Ok(blob) => break (blob, "network"),
                Err(err)
                    if matches!(err.kind(), ClientErrorKind::BlobIdDoesNotExist)
                        && reading_first_event_blob =>
                {
                    tracing::info!(
                        blob_id = %event_blob_id,
                        "reading first event blob encountered non-existent blob error: {:?}, \
                        retrying",
                        err
                    );

                    if start_time.elapsed() > Duration::from_secs(30) {
                        tracing::warn!(
                            blob_id = %event_blob_id,
                            "waiting for first certified event blob to be certified timed out"
                        );
                        return Err(err.into());
                    }

                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(err) => {
                    tracing::error!(
                        blob_id = %event_blob_id,
                        reading_first_event_blob,
                        "reading event blob encountered error: {:?}",
                        err,
                    );
                    return Err(err.into());
                }
            }
        };
        Ok((blob, blob_source))
    }
}
