// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Responsible for downloading and managing event blobs

use std::path::Path;

use anyhow::Result;
use walrus_core::{BlobId, Epoch};
use walrus_sdk::{client::Client as WalrusClient, error::ClientErrorKind};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::move_structs::EventBlob,
};

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
        let blob_iter = blob.into_iter();
        let blob_event = blob_iter
            .last()
            .ok_or(anyhow::anyhow!("No events in blob"))?;
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
        upto_checkpoint: Option<u64>,
        from_blob: Option<BlobId>,
        path: &Path,
    ) -> Result<Vec<BlobId>> {
        let mut blobs = Vec::new();
        let mut prev_event_blob = match from_blob {
            Some(blob) => blob,
            None => match self.sui_read_client.last_certified_event_blob().await? {
                Some(blob) => blob.blob_id,
                None => return Ok(vec![]),
            },
        };

        tracing::info!(
            "starting download of event blobs from latest blob ID {} and going backwards",
            prev_event_blob
        );

        while prev_event_blob != BlobId::ZERO {
            let blob_status = match self
                .walrus_client
                .get_blob_status_with_retries(&prev_event_blob, &self.sui_read_client)
                .await
            {
                Ok(blob_status) => blob_status,
                Err(err) => {
                    if matches!(err.kind(), ClientErrorKind::BlobIdDoesNotExist) {
                        // We've reached an expired blob, safe to terminate
                        break;
                    } else {
                        return Err(err.into());
                    }
                }
            };

            if blob_status == BlobStatus::Nonexistent {
                // We've reached an expired blob, safe to terminate
                break;
            }

            let blob_path = path.join(prev_event_blob.to_string());
            let blob = if blob_path.exists() {
                std::fs::read(blob_path.as_path())?
            } else {
                match self
                    .walrus_client
                    .read_blob_with_status::<walrus_core::encoding::Primary>(
                        &prev_event_blob,
                        blob_status,
                    )
                    .await
                {
                    Ok(blob) => blob,
                    Err(err) => {
                        return Err(err.into());
                    }
                }
            };

            tracing::info!(blob_id = %prev_event_blob, "finished reading event blob");

            let mut event_blob = LocalEventBlob::new(&blob)?;

            let should_store = match upto_checkpoint {
                Some(next_cp) => event_blob.end_checkpoint_sequence_number() >= next_cp,
                None => true,
            };

            if should_store {
                blobs.push(prev_event_blob);
                let num_checkpoints_stored = event_blob.end_checkpoint_sequence_number()
                    - event_blob.start_checkpoint_sequence_number()
                    + 1;
                tracing::info!(
                    "storing event blob {} with {} checkpoints",
                    prev_event_blob,
                    num_checkpoints_stored
                );
            } else {
                tracing::info!(
                    "skipping event blob {} as it contains events only before the next checkpoint",
                    prev_event_blob
                );
                break;
            }

            if !blob_path.exists() {
                event_blob.store_as_file(&blob_path)?;
            }

            if let Some(max_cp) = upto_checkpoint {
                if event_blob.start_checkpoint_sequence_number() <= max_cp {
                    break;
                }
            }

            prev_event_blob = event_blob.prev_blob_id();
        }

        Ok(blobs)
    }
}
