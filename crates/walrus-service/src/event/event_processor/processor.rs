// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Processor module for the event processor.
use std::{fmt, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use bincode::Options;
use checkpoint_downloader::ParallelCheckpointDownloader;
use sui_types::{
    base_types::ObjectID,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::VerifiedCheckpoint,
};
use tokio::{select, sync::Mutex, time::sleep};
use tokio_util::sync::CancellationToken;
use typed_store::{Map, TypedStoreError};
use walrus_core::ensure;
use walrus_utils::{metrics::Registry, tracing_sampled};

use super::{metrics::EventProcessorMetrics, package_store::LocalDBPackageStore};
use crate::event::{
    event_processor::{
        bootstrap::get_bootstrap_committee_and_checkpoint,
        catchup::EventBlobCatchupManager,
        checkpoint::CheckpointProcessor,
        client::ClientManager,
        config::{EventProcessorConfig, EventProcessorRuntimeConfig, SystemConfig},
        db::EventProcessorStores,
    },
    events::{IndexedStreamEvent, InitState, StreamEventWithInitState},
};

/// The maximum number of events to poll per poll.
const MAX_EVENTS_PER_POLL: usize = 1000;

/// The event processor.
#[derive(Clone)]
pub struct EventProcessor {
    /// Full node REST client.
    pub client_manager: ClientManager,
    /// Event database.
    pub stores: EventProcessorStores,
    /// Event polling interval.
    pub event_polling_interval: Duration,
    /// The original address of the Walrus system package.
    pub original_system_pkg_id: ObjectID,
    /// Event index before which events are pruned.
    pub event_store_commit_index: Arc<Mutex<u64>>,
    /// Event store pruning interval.
    pub pruning_interval: Duration,
    /// Event processor metrics.
    pub metrics: EventProcessorMetrics,
    /// Pipelined checkpoint downloader.
    pub checkpoint_downloader: ParallelCheckpointDownloader,
    /// Package store.
    pub package_store: LocalDBPackageStore,
    /// Checkpoint processor.
    pub checkpoint_processor: CheckpointProcessor,
    /// The interval at which to sample high-frequency tracing logs.
    pub sampled_tracing_interval: Duration,
}

impl fmt::Debug for EventProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventProcessor")
            .field("original_system_pkg_id", &self.original_system_pkg_id)
            .field("checkpoint_store", &self.stores.checkpoint_store)
            .field("walrus_package_store", &self.stores.walrus_package_store)
            .field("committee_store", &self.stores.committee_store)
            .field("event_store", &self.stores.event_store)
            .field("sampled_tracing_interval", &self.sampled_tracing_interval)
            .finish()
    }
}

impl EventProcessor {
    /// Creates a new event processor.
    pub async fn new(
        config: &EventProcessorConfig,
        runtime_config: EventProcessorRuntimeConfig,
        system_config: SystemConfig,
        metrics_registry: &Registry,
    ) -> Result<Self> {
        let client_manager = ClientManager::new(
            &runtime_config.rpc_addresses,
            config.checkpoint_request_timeout,
            runtime_config.rpc_fallback_config.as_ref(),
            metrics_registry,
            config.sampled_tracing_interval,
        )
        .await?;

        let stores = EventProcessorStores::new(&runtime_config.db_config, &runtime_config.db_path)?;

        let package_store = LocalDBPackageStore::new(
            stores.walrus_package_store.clone(),
            client_manager.get_client().clone(),
        );

        let original_system_package_id = package_store
            .get_original_package_id(system_config.system_pkg_id.into())
            .await?;

        let checkpoint_downloader = ParallelCheckpointDownloader::new(
            client_manager.get_client().clone(),
            stores.checkpoint_store.clone(),
            config.adaptive_downloader_config.clone(),
            metrics_registry,
        )?;

        let metrics = EventProcessorMetrics::new(metrics_registry);
        let checkpoint_processor = CheckpointProcessor::new(
            stores.clone(),
            package_store.clone(),
            original_system_package_id,
        );

        let event_processor = EventProcessor {
            client_manager: client_manager.clone(),
            stores,
            original_system_pkg_id: original_system_package_id,
            event_polling_interval: runtime_config.event_polling_interval,
            event_store_commit_index: Arc::new(Mutex::new(0)),
            pruning_interval: config.pruning_interval,
            metrics,
            checkpoint_downloader,
            package_store,
            checkpoint_processor,
            sampled_tracing_interval: config.sampled_tracing_interval,
        };

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

        let clients = client_manager.into_client_set();

        let catchup_manager = EventBlobCatchupManager::new(
            event_processor.stores.clone(),
            clients,
            system_config,
            runtime_config.db_path.join("recovery"),
            metrics_registry,
        );
        if let Err(e) = catchup_manager
            .catchup(config.event_stream_catchup_min_checkpoint_lag)
            .await
        {
            tracing::error!("failed to catchup using event blobs: {e}");
        }

        if event_processor.stores.checkpoint_store.is_empty() {
            let (committee, verified_checkpoint) = get_bootstrap_committee_and_checkpoint(
                client_manager.get_sui_client().clone(),
                client_manager.get_client().clone(),
                event_processor.original_system_pkg_id,
            )
            .await?;
            event_processor
                .stores
                .committee_store
                .insert(&(), &committee)?;
            event_processor
                .stores
                .checkpoint_store
                .insert(&(), verified_checkpoint.serializable_ref())?;

            // Also update the cache with the bootstrap checkpoint sequence number.
            event_processor
                .checkpoint_processor
                .update_cached_latest_checkpoint_seq_number(*verified_checkpoint.sequence_number());
        }

        Ok(event_processor)
    }

    /// Processes a single checkpoint.
    pub async fn process_checkpoint(
        &self,
        checkpoint: CheckpointData,
        prev_checkpoint: VerifiedCheckpoint,
        next_event_index: u64,
    ) -> Result<(u64, VerifiedCheckpoint)> {
        let verified_checkpoint = self
            .checkpoint_processor
            .verify_checkpoint(&checkpoint, prev_checkpoint)?;
        let next_event_index = self
            .checkpoint_processor
            .process_checkpoint_data(checkpoint, verified_checkpoint.clone(), next_event_index)
            .await?;
        Ok((next_event_index, verified_checkpoint))
    }

    /// Gets the latest checkpoint sequence number, preferring the cache.
    pub fn get_latest_checkpoint_sequence_number(&self) -> Option<u64> {
        self.checkpoint_processor
            .get_latest_checkpoint_sequence_number()
    }

    /// Returns the initialization state for the given event index. If the event index is not found,
    /// it will return `None`. This method is used to recover the state of the event blob writer.
    pub fn get_init_state(&self, from: u64) -> Result<Option<InitState>> {
        let res = self.stores.init_state.get(&from)?;
        Ok(res)
    }

    /// Polls the event store for new events starting from the given sequence number.
    pub fn poll(&self, from: u64) -> Result<Vec<IndexedStreamEvent>, TypedStoreError> {
        self.stores
            .event_store
            .safe_iter_with_bounds(Some(from), None)?
            .take(MAX_EVENTS_PER_POLL)
            .map(|result| result.map(IndexedStreamEvent::from_index_and_element))
            .collect()
    }

    /// Polls the event store for the next event starting from the given sequence number,
    /// and returns the event along with any InitState that exists at that index.
    pub fn poll_next(&self, from: u64) -> Result<Option<StreamEventWithInitState>> {
        let mut iter = self
            .stores
            .event_store
            .safe_iter_with_bounds(Some(from), None)?;
        let Some(result) = iter.next() else {
            return Ok(None);
        };
        let (index, event) = result?;
        let init_state = self.get_init_state(index)?;
        let event_with_cursor = StreamEventWithInitState::new(event, init_state);
        Ok(Some(event_with_cursor))
    }

    /// Starts the event processor. This method will run until the cancellation token is cancelled.
    pub async fn start(&self, cancellation_token: CancellationToken) -> Result<(), anyhow::Error> {
        tracing::info!("starting event processor");
        let pruning_task = self.start_pruning_events(cancellation_token.clone());
        let tailing_task = self.start_tailing_checkpoints(cancellation_token.clone());
        select! {
            pruning_result = pruning_task => {
                cancellation_token.cancel();
                pruning_result
            }
            tailing_result = tailing_task => {
                cancellation_token.cancel();
                tailing_result
            }
        }
    }

    /// Tails the full node for new checkpoints and processes them. This method will run until the
    /// cancellation token is cancelled. If the checkpoint processor falls behind the full node, it
    /// will read events from the event blobs so it can catch up.
    pub async fn start_tailing_checkpoints(&self, cancel_token: CancellationToken) -> Result<()> {
        let mut next_event_index = self
            .stores
            .event_store
            .reversed_safe_iter_with_bounds(None, None)?
            .next()
            .transpose()?
            .map(|(k, _)| k + 1)
            .unwrap_or(0);
        let Some(prev_checkpoint) = self.stores.checkpoint_store.get(&())? else {
            bail!("No checkpoint found in the checkpoint store");
        };

        let mut next_checkpoint = prev_checkpoint.inner().sequence_number().saturating_add(1);
        tracing::info!(
            next_event_index,
            next_checkpoint,
            "starting to tail checkpoints"
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
                    "failed to download checkpoint",
                );
                bail!("failed to download checkpoint: {}", entry.sequence_number);
            };
            ensure!(
                *checkpoint.checkpoint_summary.sequence_number() == next_checkpoint,
                "received out-of-order checkpoint: expected {}, got {}",
                next_checkpoint,
                checkpoint.checkpoint_summary.sequence_number()
            );
            tracing_sampled::info!(
                self.sampled_tracing_interval,
                sequence_number = next_checkpoint,
                next_event_index,
                "processing checkpoint",
            );
            self.metrics
                .event_processor_latest_downloaded_checkpoint
                .set(next_checkpoint.try_into()?);
            self.metrics
                .event_processor_total_downloaded_checkpoints
                .inc();

            (next_event_index, prev_verified_checkpoint) = self
                .process_checkpoint(checkpoint, prev_verified_checkpoint, next_event_index)
                .await?;
            next_checkpoint += 1;
        }
        Ok(())
    }

    /// Starts a periodic pruning process for events in the event store. This method will run until
    /// the cancellation token is cancelled.
    pub async fn start_pruning_events(&self, cancel_token: CancellationToken) -> Result<()> {
        loop {
            select! {
                _ = sleep(self.pruning_interval) => {
                    let commit_index = *self.event_store_commit_index.lock().await;
                    if commit_index == 0 {
                        continue;
                    }
                    let mut write_batch = self.stores.event_store.batch();
                    let range_delete_handle = self.stores.event_store.rocksdb.as_range_delete()
                        .expect("range delete handle required");
                    write_batch.schedule_delete_range(
                        &self.stores.event_store, &0, &commit_index, &range_delete_handle)?;
                    write_batch.schedule_delete_range(
                        &self.stores.init_state, &0, &commit_index, &range_delete_handle)?;
                    write_batch.write()?;

                    // This will prune the event store by deleting all the sst files relevant to the
                    // events before the commit index
                    let start = bincode::DefaultOptions::new()
                        .with_big_endian()
                        .with_fixint_encoding()
                        .serialize(&0)?;
                    let end = bincode::DefaultOptions::new()
                        .with_big_endian()
                        .with_fixint_encoding()
                        .serialize(&commit_index)?;
                    self.stores.event_store.rocksdb.delete_file_in_range(
                        &self.stores.event_store.cf()?,
                        &start,
                        &end,
                    )?;
                }
                _ = cancel_token.cancelled() => {
                    return Ok(());
                },
            }
        }
    }
}
