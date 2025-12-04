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
use tokio::{
    select,
    sync::{Mutex, mpsc},
    task::{JoinHandle, JoinSet},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use typed_store::{Map, TypedStoreError};
use walrus_core::ensure;
use walrus_utils::{
    metrics::{Registry, monitored_scope},
    tracing_sampled,
};

use super::{
    catchup,
    coordination::CatchupCoordinationState,
    metrics::EventProcessorMetrics,
    package_store::LocalDBPackageStore,
};
use crate::event::{
    event_processor::{
        bootstrap::get_bootstrap_committee_and_checkpoint,
        catchup::EventBlobCatchupManager,
        checkpoint::CheckpointProcessor,
        client::ClientManager,
        config::{EventProcessorConfig, EventProcessorRuntimeConfig, SystemConfig},
        coordination::CoordinationMessage,
        db::EventProcessorStores,
    },
    events::{IndexedStreamEvent, InitState, StreamEventWithInitState},
};

/// The maximum number of events to poll per poll.
const MAX_EVENTS_PER_POLL: usize = 1000;
/// Startup catchup timeout (seconds) used during initial catchup in EventProcessor::new
const STARTUP_CATCHUP_TIMEOUT_SECS: u64 = 5 * 60;
/// Runtime catchup processing timeout (seconds) used during runtime catchup in
/// EventProcessor::start_runtime_catchup_monitoring
const RUNTIME_CATCHUP_PROCESSING_TIMEOUT_SECS: u64 = 5 * 60 * 60;

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
    /// Configuration for event processing.
    pub config: EventProcessorConfig,
    /// System configuration.
    pub system_config: SystemConfig,
    /// Recovery path for catchup operations.
    pub recovery_path: std::path::PathBuf,
    /// Metrics registry for creating new metric instances.
    pub metrics_registry: Registry,
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
            .field(
                "runtime_catchup_enabled",
                &self.config.enable_runtime_catchup,
            )
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
        )?;

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
            config: config.clone(),
            system_config: system_config.clone(),
            recovery_path: runtime_config.db_path.join("recovery"),
            metrics_registry: metrics_registry.clone(),
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

        let _clients = client_manager.into_client_set();

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
        if self.config.enable_runtime_catchup {
            tracing::info!("starting event processor with runtime catchup enabled");
        } else {
            tracing::info!("starting event processor with runtime catchup disabled");
        }

        let (coordination_state, coordination_rx) = CatchupCoordinationState::new();
        let coordination_state = Arc::new(coordination_state);

        let catchup_manager = EventBlobCatchupManager::new(
            self.stores.clone(),
            self.client_manager.clone().into_client_set(),
            self.system_config.clone(),
            self.recovery_path.clone(),
            &self.metrics_registry,
            catchup::CatchupRuntimeConfig {
                coordination_state: coordination_state.clone(),
                coordination_timeout: Duration::from_secs(STARTUP_CATCHUP_TIMEOUT_SECS),
                processing_timeout: Duration::from_secs(RUNTIME_CATCHUP_PROCESSING_TIMEOUT_SECS),
            },
        );
        // Notify tailing stopped to unblock the catchup task
        coordination_state.notify_tailing_stopped();

        if let Err(e) = catchup_manager
            .catchup(self.config.event_stream_catchup_min_checkpoint_lag)
            .await
        {
            tracing::error!("failed to catchup using event blobs: {e}");
        }

        if self.stores.checkpoint_store.is_empty() {
            let (committee, verified_checkpoint) = get_bootstrap_committee_and_checkpoint(
                self.client_manager.get_sui_client().clone(),
                self.client_manager.get_client().clone(),
                self.original_system_pkg_id,
            )
            .await?;

            self.stores.committee_store.insert(&(), &committee)?;
            self.stores
                .checkpoint_store
                .insert(&(), verified_checkpoint.serializable_ref())?;

            self.checkpoint_processor
                .update_cached_latest_checkpoint_seq_number(*verified_checkpoint.sequence_number());
        }

        let mut join_set = JoinSet::new();
        join_set.spawn({
            let self_clone = self.clone();
            let cancellation_token = cancellation_token.clone();
            async move { self_clone.start_pruning_events(cancellation_token).await }
        });
        join_set.spawn({
            let self_clone = self.clone();
            let cancellation_token = cancellation_token.clone();
            let coordination_state = coordination_state.clone();
            async move {
                self_clone
                    .start_tailing_checkpoints_with_restart_support(
                        cancellation_token,
                        coordination_state,
                        coordination_rx,
                    )
                    .await
            }
        });
        if self.config.enable_runtime_catchup {
            join_set.spawn({
                let self_clone = self.clone();
                let cancellation_token = cancellation_token.clone();
                let coordination_state = coordination_state.clone();
                async move {
                    self_clone
                        .start_runtime_catchup_monitoring(cancellation_token, coordination_state)
                        .await
                }
            });
        }

        let _cancel_guard = cancellation_token.clone().drop_guard();
        join_set
            .join_next()
            .await
            .ok_or(anyhow!("error unwrapping task result"))??
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

        #[cfg(msim)]
        sui_macros::fail_point_async!("pause_checkpoint_tailing_entry");

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

    fn start_tailing_task(
        &self,
        cancel_token: CancellationToken,
        coordination_state: Arc<CatchupCoordinationState>,
    ) -> JoinHandle<Result<()>> {
        let processor = self.clone();
        coordination_state.mark_tailing_started();
        let coordination_state = coordination_state.clone();
        tokio::spawn(async move {
            tracing::info!("Starting tailing task");
            let result = processor.start_tailing_checkpoints(cancel_token).await;
            tracing::info!("Tailing task exited");
            coordination_state.notify_tailing_stopped();
            result
        })
    }

    /// Checkpoint tailing with message-based coordination for catchup
    async fn start_tailing_checkpoints_with_restart_support(
        &self,
        cancel_token: CancellationToken,
        coordination_state: Arc<CatchupCoordinationState>,
        mut coordination_rx: mpsc::UnboundedReceiver<CoordinationMessage>,
    ) -> Result<()> {
        let mut child_cancel_token = cancel_token.child_token();

        let mut tailing_task: Option<JoinHandle<Result<()>>> =
            Some(self.start_tailing_task(child_cancel_token.clone(), coordination_state.clone()));
        loop {
            tokio::select! {
                msg = coordination_rx.recv() => {
                    match msg {
                        Some(CoordinationMessage::StopCheckpointTailing) => {
                            child_cancel_token.cancel();
                            if let Some(handle) = tailing_task.take() {
                                match handle.await {
                                    Ok(Ok(_)) => tracing::info!("tailing task exited"),
                                    Ok(Err(error)) => tracing::error!(?error, "tailing task error"),
                                    Err(error) => tracing::error!(?error, "tailing task panicked"),
                                }
                            } else {
                                tracing::error!("stop requested but tailing task was not running");
                                coordination_state.notify_tailing_stopped();
                            }
                            continue;
                        }
                        Some(CoordinationMessage::RestartCheckpointTailing |
                            CoordinationMessage::CatchupFailed) => {
                            tracing::info!("restarting checkpoint tailing");
                            child_cancel_token = cancel_token.child_token();
                            tailing_task = Some(self.start_tailing_task(
                                child_cancel_token.clone(), coordination_state.clone()));
                            continue;
                        }
                        None => {
                            tracing::info!("tailing task exited");
                            break;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("tailing task cancelled");
                    break;
                }
            }
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

    /// Runtime catchup monitoring task
    async fn start_runtime_catchup_monitoring(
        &self,
        cancel_token: CancellationToken,
        coordination_state: Arc<CatchupCoordinationState>,
    ) -> Result<()> {
        let mut lag_check_interval = tokio::time::interval(self.config.runtime_lag_check_interval);
        lag_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!(
            check_interval_secs = self.config.runtime_lag_check_interval.as_secs(),
            lag_threshold = self.config.runtime_catchup_lag_threshold,
            "starting runtime catchup monitoring"
        );

        let coordination_state_clone = coordination_state.clone();
        let catchup_manager = EventBlobCatchupManager::new(
            self.stores.clone(),
            self.client_manager.clone().into_client_set(),
            self.system_config.clone(),
            self.recovery_path.clone(),
            &self.metrics_registry,
            catchup::CatchupRuntimeConfig {
                coordination_state: coordination_state_clone,
                coordination_timeout: self.config.catchup_coordination_timeout,
                processing_timeout: self.config.catchup_processing_timeout,
            },
        );

        loop {
            let _scope = monitored_scope::monitored_scope("RuntimeCatchupMonitoring");
            select! {
                _ = lag_check_interval.tick() => {
                    if coordination_state.is_catchup_active() {
                        continue;
                    }

                    match catchup_manager.get_current_lag().await {
                        Ok(lag) => {
                            self.metrics
                                .runtime_lag_current
                                .set(i64::try_from(lag).unwrap_or(i64::MAX));

                            if lag > self.config.runtime_catchup_lag_threshold {
                                tracing::info!(
                                    lag = lag,
                                    threshold = self.config.runtime_catchup_lag_threshold,
                                    "triggering runtime catchup due to high lag"
                                );
                                match catchup_manager.perform_catchup().await {
                                    Ok(()) => {}
                                    Err(catchup::CatchupError::Recoverable(error)) => {
                                        tracing::warn!(?error, "recoverable error in catchup");
                                    }
                                    Err(catchup::CatchupError::NonRecoverable(error)) => {
                                        return Err(error);
                                    }
                                }
                            } else {
                                tracing::debug!(
                                    lag = lag,
                                    threshold = self.config.runtime_catchup_lag_threshold,
                                    "lag below threshold, no catchup needed"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::warn!(
                                error = ?error,
                                "failed to calculate current lag for runtime catchup monitoring"
                            );
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    tracing::info!("runtime catchup monitoring shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}
