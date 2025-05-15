// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Responsible for downloading and managing event blobs

use std::path::Path;

use anyhow::Result;
use walrus_core::BlobId;
use walrus_sdk::{client::Client as WalrusClient, error::ClientErrorKind};
use walrus_storage_node_client::api::BlobStatus;
use walrus_sui::client::{ReadClient, SuiReadClient};

use crate::node::events::{
    event_blob::EventBlob as LocalEventBlob,
    event_processor::EventProcessorMetrics,
};

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

    /// Collects event blobs starting from the given blob ID and going backwards until reaching
    /// the specified checkpoint or an expired blob.
    ///
    /// Returns a vector of blob IDs in reverse chronological order (newest to oldest).
    pub async fn download(
        &self,
        upto_checkpoint: Option<u64>,
        from_blob: Option<BlobId>,
        path: &Path,
        metrics: Option<&EventProcessorMetrics>,
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
            let result = self
                .walrus_client
                .get_blob_status_with_retries(&prev_event_blob, &self.sui_read_client)
                .await;

            let Ok(blob_status) = result else {
                let err = result.err().unwrap();
                if matches!(err.kind(), ClientErrorKind::BlobIdDoesNotExist) {
                    // We've reached an expired blob, safe to terminate
                    break;
                } else {
                    return Err(err.into());
                }
            };

            if blob_status == BlobStatus::Nonexistent {
                // We've reached an expired blob, safe to terminate
                break;
            }

            let blob_path = path.join(prev_event_blob.to_string());
            let (blob, blob_source) = if blob_path.exists() {
                (std::fs::read(blob_path.as_path())?, "local")
            } else {
                let result = self
                    .walrus_client
                    .read_blob_with_status::<walrus_core::encoding::Primary>(
                        &prev_event_blob,
                        blob_status,
                    )
                    .await;
                let Ok(blob) = result else {
                    let err = result.err().unwrap();
                    metrics.inspect(|&m| {
                        m.event_processor_event_blob_fetched
                            .with_label_values(&["network"])
                            .inc()
                    });
                    return Err(err.into());
                };
                (blob, "network")
            };

            metrics.inspect(|&m| {
                m.event_processor_event_blob_fetched
                    .with_label_values(&[blob_source])
                    .inc()
            });

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
