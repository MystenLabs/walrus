// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Main processor module for the indexer event processor.

use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use checkpoint_downloader::ParallelCheckpointDownloader;
use sui_types::{
    base_types::ObjectID,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::VerifiedCheckpoint,
};
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;
use typed_store::Map;
use walrus_utils::{metrics::Registry, tracing_sampled};

use super::{
    checkpoint::IndexerCheckpointProcessor,
    client::ClientManager,
    config::{IndexerEventProcessorConfig, IndexerRuntimeConfig},
    db::IndexerEventProcessorStores,
    metrics::IndexerEventProcessorMetrics,
};
use crate::IndexOperation;

/// Index event extracted from Sui events for the indexer
#[derive(Debug, Clone)]
pub struct IndexerEvent {
    /// The checkpoint sequence number this event came from
    pub checkpoint: u64,
    /// The actual index operation
    pub operation: IndexOperation,
}

/// The indexer event processor.
#[derive(Clone)]
pub struct IndexerEventProcessor {
    /// Full node REST client.
    pub client_manager: ClientManager,
    /// Event database.
    pub stores: IndexerEventProcessorStores,
    /// Event polling interval.
    pub event_polling_interval: Duration,
    /// The Walrus package ID to filter events from.
    pub walrus_package_id: ObjectID,
    /// Event processor metrics.
    pub metrics: IndexerEventProcessorMetrics,
    /// Pipelined checkpoint downloader.
    pub checkpoint_downloader: ParallelCheckpointDownloader,
    /// Checkpoint processor.
    pub checkpoint_processor: IndexerCheckpointProcessor,
    /// The interval at which to sample high-frequency tracing logs.
    pub sampled_tracing_interval: Duration,
    /// Channel for sending processed events to the indexer
    event_sender: mpsc::UnboundedSender<IndexerEvent>,
    /// Channel for receiving processed events
    event_receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<IndexerEvent>>>,
}

impl std::fmt::Debug for IndexerEventProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerEventProcessor")
            .field("walrus_package_id", &self.walrus_package_id)
            .field("checkpoint_store", &self.stores.checkpoint_store)
            .field("sampled_tracing_interval", &self.sampled_tracing_interval)
            .finish()
    }
}

impl IndexerEventProcessor {
    /// Creates a new indexer event processor.
    pub async fn new(
        config: &IndexerEventProcessorConfig,
        runtime_config: IndexerRuntimeConfig,
        metrics_registry: &Registry,
    ) -> Result<Self> {
        let client_manager = ClientManager::new(
            &runtime_config.rpc_addresses,
            config.processor_config.checkpoint_request_timeout,
            runtime_config.rpc_fallback_config.as_ref(),
            metrics_registry,
            config.processor_config.sampled_tracing_interval,
        )
        .await?;

        let stores = IndexerEventProcessorStores::new(&runtime_config.db_path)?;

        let checkpoint_downloader = ParallelCheckpointDownloader::new(
            client_manager.get_client().clone(),
            stores.checkpoint_store.clone(),
            config.downloader_config.clone(),
            metrics_registry,
        )?;

        let metrics = IndexerEventProcessorMetrics::new(metrics_registry);
        let checkpoint_processor =
            IndexerCheckpointProcessor::new(stores.clone(), config.walrus_package_id);

        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        let event_processor = IndexerEventProcessor {
            client_manager,
            stores,
            walrus_package_id: config.walrus_package_id,
            event_polling_interval: runtime_config.event_polling_interval,
            metrics,
            checkpoint_downloader,
            checkpoint_processor,
            sampled_tracing_interval: config.processor_config.sampled_tracing_interval,
            event_sender,
            event_receiver: Arc::new(tokio::sync::Mutex::new(event_receiver)),
        };

        // Initialize checkpoint store if empty
        if event_processor.stores.checkpoint_store.is_empty() {
            event_processor.stores.clear_stores()?;
        }

        let current_checkpoint = event_processor
            .stores
            .checkpoint_store
            .get(&())?
            .map(|t| *t.inner().sequence_number())
            .unwrap_or(0);

        event_processor
            .checkpoint_processor
            .update_cached_latest_checkpoint_seq_number(current_checkpoint);

        Ok(event_processor)
    }

    /// Gets the latest checkpoint sequence number, preferring the cache.
    pub fn get_latest_checkpoint_sequence_number(&self) -> Option<u64> {
        self.checkpoint_processor
            .get_latest_checkpoint_sequence_number()
    }

    /// Starts the indexer event processor. This method will run until the cancellation token is cancelled.
    pub async fn start(&self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        tracing::info!("Starting indexer event processor");
        let tailing_task = self.start_tailing_checkpoints(cancellation_token.clone());
        select! {
            tailing_result = tailing_task => {
                cancellation_token.cancel();
                tailing_result
            }
        }
    }

    /// Tails the full node for new checkpoints and processes them.
    pub async fn start_tailing_checkpoints(&self, cancel_token: CancellationToken) -> Result<()> {
        let mut next_event_index = self
            .stores
            .processed_events_store
            .safe_iter_with_bounds(None, None)?
            .map(|result| result.map(|(k, _)| k))
            .last()
            .transpose()?
            .map(|k| k + 1)
            .unwrap_or(0);

        let Some(prev_checkpoint) = self.stores.checkpoint_store.get(&())? else {
            bail!("No checkpoint found in the checkpoint store");
        };

        let mut next_checkpoint = prev_checkpoint.inner().sequence_number().saturating_add(1);
        tracing::info!(
            next_event_index,
            next_checkpoint,
            "Starting to tail checkpoints for indexing"
        );

        let mut prev_verified_checkpoint =
            VerifiedCheckpoint::new_from_verified(prev_checkpoint.into_inner());
        let mut rx = self.checkpoint_downloader.start(
            next_checkpoint,
            cancel_token,
            self.sampled_tracing_interval,
        );

        while let Some(entry) = rx.recv().await {
            let Ok(checkpoint) = entry.result else {
                let error = entry.result.err().unwrap_or(anyhow!("unknown error"));
                tracing::error!(
                    ?error,
                    sequence_number = entry.sequence_number,
                    "Failed to download checkpoint for indexing",
                );
                bail!("Failed to download checkpoint: {}", entry.sequence_number);
            };

            tracing_sampled::info!(
                self.sampled_tracing_interval,
                sequence_number = next_checkpoint,
                next_event_index,
                "Processing checkpoint for indexing",
            );

            self.metrics
                .indexer_event_processor_latest_downloaded_checkpoint
                .set(next_checkpoint.try_into()?);
            self.metrics
                .indexer_event_processor_total_downloaded_checkpoints
                .inc();

            (next_event_index, prev_verified_checkpoint) = self
                .process_checkpoint(checkpoint, prev_verified_checkpoint, next_event_index)
                .await?;
            next_checkpoint += 1;
        }
        Ok(())
    }

    /// Processes a single checkpoint and extracts indexing events.
    pub async fn process_checkpoint(
        &self,
        checkpoint: CheckpointData,
        _prev_checkpoint: VerifiedCheckpoint,
        next_event_index: u64,
    ) -> Result<(u64, VerifiedCheckpoint)> {
        // For now, we'll create a simple verified checkpoint
        // In a real implementation, this would verify the checkpoint properly
        let verified_checkpoint =
            VerifiedCheckpoint::new_from_verified(checkpoint.checkpoint_summary.clone());

        let next_event_index = self
            .checkpoint_processor
            .process_checkpoint_data(checkpoint, verified_checkpoint.clone(), next_event_index)
            .await?;

        // Send any processed events to the indexer
        // This is a placeholder - in reality we'd extract and send the actual events

        Ok((next_event_index, verified_checkpoint))
    }

    /// Receive the next indexer event
    pub async fn receive_event(&self) -> Option<IndexerEvent> {
        let mut receiver = self.event_receiver.lock().await;
        receiver.recv().await
    }

    /// Send an indexer event (used internally by the processor)
    fn send_event(&self, event: IndexerEvent) -> Result<()> {
        self.event_sender
            .send(event)
            .map_err(|_| anyhow!("Failed to send indexer event"))?;
        self.metrics
            .indexer_event_processor_index_events_extracted
            .inc();
        Ok(())
    }
}
