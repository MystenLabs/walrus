// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint downloader.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

use anyhow::{Result, anyhow};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use sui_rpc_api::client::ResponseExt;
use sui_types::messages_checkpoint::{CheckpointSequenceNumber, TrustedCheckpoint};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;
use typed_store::{Map, rocks::DBMap};
use walrus_sui::client::retry_client::{RetriableClientError, RetriableRpcClient};
use walrus_utils::{
    backoff::ExponentialBackoff,
    metrics::{Registry, monitored_scope},
    tracing_sampled,
};

use crate::{
    ParallelDownloaderConfig,
    config::{AdaptiveDownloaderConfig, PoolMonitorConfig},
    metrics::AdaptiveDownloaderMetrics,
    types::{CheckpointEntry, PoolMonitorChannels, WorkerMessage},
};

/// Parallel checkpoint downloader that fetches checkpoints in parallel.
#[derive(Clone, Debug)]
pub struct ParallelCheckpointDownloader {
    full_node_client: RetriableRpcClient,
    checkpoint_store: DBMap<(), TrustedCheckpoint>,
    config: AdaptiveDownloaderConfig,
    metrics: AdaptiveDownloaderMetrics,
}

impl ParallelCheckpointDownloader {
    /// Creates a new parallel checkpoint downloader.
    pub fn new(
        full_node_client: RetriableRpcClient,
        checkpoint_store: DBMap<(), TrustedCheckpoint>,
        config: AdaptiveDownloaderConfig,
        registry: &Registry,
    ) -> Result<Self> {
        let metrics = AdaptiveDownloaderMetrics::new(registry);
        Ok(Self {
            full_node_client,
            checkpoint_store,
            config,
            metrics,
        })
    }

    /// Starts fetching checkpoints from the given sequence number.
    pub fn start(
        &self,
        sequence_number: CheckpointSequenceNumber,
        cancellation_token: CancellationToken,
        sampled_tracing_interval: Duration,
    ) -> mpsc::Receiver<CheckpointEntry> {
        let inner = ParallelCheckpointDownloaderInner::new(
            self.full_node_client.clone(),
            self.checkpoint_store.clone(),
            self.config.clone(),
            cancellation_token,
            self.metrics.clone(),
            sampled_tracing_interval,
        );
        inner.start(sequence_number)
    }
}

/// Parallel checkpoint downloader that fetches checkpoints in parallel.
struct ParallelCheckpointDownloaderInner {
    message_sender: async_channel::Sender<WorkerMessage>,
    checkpoint_receiver: mpsc::Receiver<CheckpointEntry>,
    config: AdaptiveDownloaderConfig,
    worker_count: Arc<AtomicUsize>,
    cancellation_token: CancellationToken,
    metrics: AdaptiveDownloaderMetrics,
    sampled_tracing_interval: Duration,
}

impl ParallelCheckpointDownloaderInner {
    /// Creates a new parallel checkpoint downloader.
    fn new(
        full_node_client: RetriableRpcClient,
        checkpoint_store: DBMap<(), TrustedCheckpoint>,
        config: AdaptiveDownloaderConfig,
        cancellation_token: CancellationToken,
        metrics: AdaptiveDownloaderMetrics,
        sampled_tracing_interval: Duration,
    ) -> Self {
        let (message_sender, message_receiver) =
            async_channel::bounded(config.message_queue_size());
        let (checkpoint_sender, checkpoint_receiver) =
            mpsc::channel(config.checkpoint_queue_size());

        for worker_id in 0..config.initial_workers {
            let full_node_client = full_node_client.clone();
            let cloned_message_receiver = message_receiver.clone();
            let config = config.clone();
            let cloned_checkpoint_sender = checkpoint_sender.clone();
            Self::spawn_new_worker(
                worker_id,
                full_node_client,
                cloned_message_receiver,
                cloned_checkpoint_sender,
                config.base_config,
                sampled_tracing_interval,
            );
        }

        let worker_count = Arc::new(AtomicUsize::new(config.initial_workers));
        let cloned_checkpoint_store = checkpoint_store.clone();
        let cloned_cancel_token = cancellation_token.clone();
        let cloned_client = full_node_client.clone();
        let cloned_message_sender = message_sender.clone();
        let cloned_config = config.clone();
        let cloned_worker_count = worker_count.clone();
        let cloned_metrics = metrics.clone();
        tokio::spawn(async move {
            let config = PoolMonitorConfig {
                downloader_config: cloned_config.clone(),
                checkpoint_store: cloned_checkpoint_store,
                client: cloned_client,
                metrics: cloned_metrics,
            };
            let channels = PoolMonitorChannels {
                message_sender: cloned_message_sender,
                message_receiver,
                checkpoint_sender,
            };
            Self::pool_monitor(
                config,
                channels,
                cloned_worker_count,
                cloned_cancel_token,
                sampled_tracing_interval,
            )
            .await?;
            anyhow::Ok(())
        });

        Self {
            message_sender,
            checkpoint_receiver,
            config,
            worker_count,
            cancellation_token,
            metrics,
            sampled_tracing_interval,
        }
    }

    fn spawn_new_worker(
        worker_id: usize,
        full_node_client: RetriableRpcClient,
        message_receiver: async_channel::Receiver<WorkerMessage>,
        checkpoint_sender: mpsc::Sender<CheckpointEntry>,
        config: ParallelDownloaderConfig,
        sampled_tracing_interval: Duration,
    ) {
        tokio::spawn(async move {
            Self::worker_loop(
                worker_id,
                full_node_client,
                message_receiver,
                checkpoint_sender,
                config,
                sampled_tracing_interval,
            )
            .await?;
            anyhow::Ok(())
        });
    }

    /// Returns the current checkpoint lag between the local store and the full node
    /// in terms of sequence numbers.
    ///
    /// This works by downloading the latest checkpoint
    /// summary from the full node and comparing it with the current checkpoint in the store.
    async fn current_checkpoint_lag(
        checkpoint_store: &DBMap<(), TrustedCheckpoint>,
        client: &RetriableRpcClient,
    ) -> Result<u64> {
        let Ok(Some(current_checkpoint)) = checkpoint_store.get(&()) else {
            return Err(anyhow!("failed to get current checkpoint"));
        };

        #[cfg(msim)]
        {
            let mut checkpoint_lag_error = false;
            sui_macros::fail_point_if!("fail_point_current_checkpoint_lag_error", || {
                checkpoint_lag_error = true;
            });

            if checkpoint_lag_error {
                return Err(anyhow!("fail point triggered checkpoint lag error"));
            }
        }

        let latest_checkpoint = client.get_latest_checkpoint_summary().await?;
        let current_lag = latest_checkpoint
            .sequence_number
            .saturating_sub(current_checkpoint.inner().sequence_number);
        Ok(current_lag)
    }

    /// Worker loop that fetches checkpoints.
    async fn worker_loop(
        worker_id: usize,
        client: RetriableRpcClient,
        message_receiver: async_channel::Receiver<WorkerMessage>,
        checkpoint_sender: mpsc::Sender<CheckpointEntry>,
        config: ParallelDownloaderConfig,
        sampled_tracing_interval: Duration,
    ) -> Result<()> {
        let _scope = monitored_scope::monitored_scope("WorkerLoop");

        tracing::info!(worker_id, "starting checkpoint download worker");
        let mut rng = StdRng::from_entropy();
        while let Ok(WorkerMessage::Download(sequence_number)) = message_receiver.recv().await {
            let entry =
                Self::download_with_retry(&client, sequence_number, &config, &mut rng).await;
            tracing_sampled::info!(
                sampled_tracing_interval,
                sequence_number,
                "downloaded checkpoint",
            );
            checkpoint_sender.send(entry).await?;
        }
        tracing::info!(worker_id, "checkpoint download worker shutting down");
        Ok(())
    }

    async fn adjust_workers(
        next_worker_id: &mut usize,
        current_workers: usize,
        target_workers: usize,
        channels: &PoolMonitorChannels,
        config: &PoolMonitorConfig,
        worker_count: &Arc<AtomicUsize>,
        sampled_tracing_interval: Duration,
    ) -> Result<()> {
        match current_workers.cmp(&target_workers) {
            std::cmp::Ordering::Greater => {
                let to_remove = current_workers - target_workers;
                for _ in 0..to_remove {
                    channels
                        .message_sender
                        .send(WorkerMessage::Shutdown)
                        .await?;
                    let new_count = worker_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    config.metrics.num_workers.set(
                        new_count
                            .try_into()
                            .expect("new_count should always fit into a i64"),
                    );
                }
            }
            std::cmp::Ordering::Less => {
                let to_add = target_workers - current_workers;
                for _ in 0..to_add {
                    let cloned_client = config.client.clone();
                    let cloned_receiver = channels.message_receiver.clone();
                    let cloned_checkpoint_sender = channels.checkpoint_sender.clone();
                    let cloned_config = config.downloader_config.base_config.clone();
                    Self::spawn_new_worker(
                        *next_worker_id,
                        cloned_client,
                        cloned_receiver,
                        cloned_checkpoint_sender,
                        cloned_config,
                        sampled_tracing_interval,
                    );
                    *next_worker_id += 1;
                    let new_count = worker_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    config.metrics.num_workers.set(
                        new_count
                            .try_into()
                            .expect("new_count should always fit into a i64"),
                    );
                }
            }
            std::cmp::Ordering::Equal => {}
        }
        Ok(())
    }

    /// Pool monitor that scales the worker pool based on checkpoint lag.
    async fn pool_monitor(
        config: PoolMonitorConfig,
        channels: PoolMonitorChannels,
        worker_count: Arc<AtomicUsize>,
        cancellation_token: CancellationToken,
        sampled_tracing_interval: Duration,
    ) -> Result<()> {
        let downloader_config = config.downloader_config.clone();
        let mut next_worker_id = downloader_config.initial_workers;
        let mut last_scale = Instant::now();
        let mut consecutive_failures = 0;
        let average_workers = (downloader_config.min_workers + downloader_config.max_workers) / 2;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    tracing::info!("pool monitor shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let now = Instant::now();
                    if now.duration_since(last_scale) < downloader_config.scale_cooldown {
                        continue;
                    }

                    let result = Self::current_checkpoint_lag(
                        &config.checkpoint_store, &config.client).await;
                    let Ok(lag) = result else {
                        let err = result.err();
                        consecutive_failures += 1;
                        tracing::warn!(
                            error = ?err,
                            consecutive_failures,
                            max_failures = downloader_config.max_consecutive_pool_monitor_failures,
                            "failed to fetch checkpoint lag from full node"
                        );
                        if consecutive_failures >=
                            downloader_config.max_consecutive_pool_monitor_failures {
                            tracing::error!(
                                error = ?err,
                                consecutive_failures,
                                target_workers = average_workers,
                                "checkpoint lag monitoring has failed repeatedly \
                                - adjusting to average workers"
                            );
                            let current_workers =
                                worker_count.load(std::sync::atomic::Ordering::SeqCst);
                            tracing::info!("adjusting to average workers {:?}", average_workers);
                            Self::adjust_workers(
                                &mut next_worker_id,
                                current_workers,
                                average_workers,
                                &channels,
                                &config,
                                &worker_count,
                                sampled_tracing_interval,
                            ).await?;
                        }
                        continue;
                    };

                    consecutive_failures = 0;
                    config.metrics.checkpoint_lag.set(
                        lag.try_into()
                            .map_err(|_| tracing::warn!(
                                "encountered checkpoint lag larger than `i64::MAX`"
                            ))
                            .unwrap_or(i64::MAX),
                    );

                    let current = worker_count.load(std::sync::atomic::Ordering::SeqCst);
                    if lag > downloader_config.scale_up_lag_threshold &&
                        current < downloader_config.max_workers {
                        tracing::info!(
                            "scaling up checkpoint workers from {} to {} due to high lag ({})",
                            current,
                            current + 1,
                            lag
                        );
                        Self::adjust_workers(
                            &mut next_worker_id,
                            current,
                            current + 1,
                            &channels,
                            &config,
                            &worker_count,
                            sampled_tracing_interval,
                        ).await?;
                        last_scale = now;
                    } else if lag < downloader_config.scale_down_lag_threshold &&
                        current > downloader_config.min_workers {
                        tracing::info!(
                            "scaling down checkpoint workers from {} to {} due to low lag ({})",
                            current,
                            current - 1,
                            lag
                        );
                        Self::adjust_workers(
                            &mut next_worker_id,
                            current,
                            current - 1,
                            &channels,
                            &config,
                            &worker_count,
                            sampled_tracing_interval,
                        ).await?;
                        last_scale = now;
                    }
                }
            }
        }
    }

    /// Starts downloading checkpoints from the given sequence number.
    pub fn start(
        self,
        sequence_number: CheckpointSequenceNumber,
    ) -> mpsc::Receiver<CheckpointEntry> {
        let (result_tx, result_rx) = mpsc::channel(self.config.result_queue_size());
        let message_sender = self.message_sender.clone();

        tokio::task::spawn(async move {
            self.run_download_loop(sequence_number, result_tx, message_sender)
                .await;
        });

        result_rx
    }

    /// Main download loop that coordinates checkpoint fetching and result delivery.
    async fn run_download_loop(
        mut self,
        sequence_number: CheckpointSequenceNumber,
        result_tx: mpsc::Sender<CheckpointEntry>,
        message_sender: async_channel::Sender<WorkerMessage>,
    ) {
        let mut sequence_number = sequence_number;
        let mut in_flight = BTreeSet::new();
        let mut pending = BTreeMap::new();
        let mut next_expected = sequence_number;

        while !self.cancellation_token.is_cancelled() {
            let num_workers = self.worker_count.load(std::sync::atomic::Ordering::SeqCst);
            let max_in_flight_requests =
                num_workers * self.config.channel_config.work_queue_buffer_factor;
            while !message_sender.is_full() && in_flight.len() < max_in_flight_requests {
                if message_sender
                    .send(WorkerMessage::Download(sequence_number))
                    .await
                    .is_err()
                {
                    tracing::error!("failed to send job to workers; channel closed");
                    return;
                }
                tracing_sampled::info!(
                    self.sampled_tracing_interval,
                    sequence_number,
                    "adding checkpoint to worker queue",
                );
                in_flight.insert(sequence_number);
                self.metrics
                    .num_inflight_downloading
                    .set(i64::try_from(in_flight.len()).unwrap_or(i64::MAX));
                sequence_number += 1;
            }

            let Some(entry) = self.checkpoint_receiver.recv().await else {
                break;
            };

            in_flight.remove(&entry.sequence_number);
            self.metrics
                .num_inflight_downloading
                .set(i64::try_from(in_flight.len()).unwrap_or(i64::MAX));

            if entry.sequence_number != next_expected {
                // Store out-of-order response
                pending.insert(entry.sequence_number, entry);
                self.metrics
                    .num_pending_processing_checkpoints
                    .set(i64::try_from(pending.len()).unwrap_or(i64::MAX));
                continue;
            }

            if result_tx.send(entry).await.is_err() {
                tracing::info!("result receiver dropped, stopping checkpoint fetcher");
                break;
            }

            next_expected += 1;

            while let Some(checkpoint) = pending.remove(&next_expected) {
                if result_tx.send(checkpoint).await.is_err() {
                    tracing::debug!("result receiver dropped, stopping checkpoint fetcher");
                    return;
                }
                next_expected += 1;
            }
            self.metrics
                .num_pending_processing_checkpoints
                .set(i64::try_from(pending.len()).unwrap_or(i64::MAX));
        }
    }

    /// Downloads a checkpoint with retries.
    async fn download_with_retry(
        client: &RetriableRpcClient,
        sequence_number: CheckpointSequenceNumber,
        config: &ParallelDownloaderConfig,
        rng: &mut StdRng,
    ) -> CheckpointEntry {
        let mut backoff = if cfg!(test) {
            // Note that we only return error in test mode.
            ExponentialBackoff::new_with_seed(
                config.initial_delay,
                config.max_delay,
                config.retries,
                rng.next_u64(),
            )
        } else {
            // In production, we should never stop trying to fetch the checkpoint. This is critical
            // for the node to make progress.
            ExponentialBackoff::new_with_seed(
                config.initial_delay,
                config.max_delay,
                None,
                rng.next_u64(),
            )
        };

        loop {
            match client.get_full_checkpoint(sequence_number).await {
                Ok(checkpoint) => return CheckpointEntry::new(sequence_number, Ok(checkpoint)),
                Err(err) => {
                    handle_checkpoint_error(&err, sequence_number);
                    if let Some(delay) = backoff.next() {
                        tokio::time::sleep(delay).await;
                        continue;
                    }

                    // Note that we only return error in test mode.
                    assert!(cfg!(test));
                    return CheckpointEntry::new(sequence_number, Err(err.into()));
                }
            }
        }
    }
}

/// Handles an error that occurred while reading the next checkpoint.
/// If the error is due to a checkpoint that is already present on the server,
/// it is logged as an error.
/// Otherwise, it is logged as a debug.
fn handle_checkpoint_error(err: &RetriableClientError, next_checkpoint: u64) {
    if let RetriableClientError::RpcError(rpc_error) = err
        && let Some(checkpoint_height) = rpc_error.status.checkpoint_height()
        && next_checkpoint > checkpoint_height
    {
        return tracing::trace!(
            next_checkpoint,
            checkpoint_height,
            message = rpc_error.status.message(),
            "failed to read next checkpoint, probably not produced yet",
        );
    }
    tracing::warn!(next_checkpoint, ?err, "failed to read next checkpoint");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rocksdb::Options;
    use typed_store::{
        rocks,
        rocks::{MetricConf, ReadWriteOptions, errors::typed_store_err_from_rocks_err},
    };
    use walrus_sui::client::retry_client::retriable_rpc_client::LazyFallibleRpcClientBuilder;
    use walrus_utils::{backoff::ExponentialBackoffConfig, tests::global_test_lock};

    use super::*;
    use crate::ChannelConfig;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_fetcher() -> Result<()> {
        telemetry_subscribers::init_for_testing();
        let rest_url = "http://localhost:9000";
        let retriable_client = RetriableRpcClient::new(
            vec![LazyFallibleRpcClientBuilder::Url {
                rpc_url: rest_url.to_string(),
                ensure_experimental_rest_endpoint: false,
            }],
            Duration::from_millis(100),
            ExponentialBackoffConfig {
                min_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
                max_retries: Some(0),
            },
            None,
            None,
            Duration::from_secs(30),
        )
        .await?;
        let parallel_config = ParallelDownloaderConfig {
            retries: Some(0),
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(2),
        };
        let channel_config = ChannelConfig {
            work_queue_buffer_factor: 3,
            result_queue_buffer_factor: 3,
        };
        let config = AdaptiveDownloaderConfig {
            min_workers: 2,
            max_workers: 10,
            initial_workers: 5,
            scale_up_lag_threshold: 100,
            scale_down_lag_threshold: 50,
            scale_cooldown: Duration::from_secs(30),
            base_config: parallel_config,
            channel_config,
            max_consecutive_pool_monitor_failures: 10,
        };
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let root_dir_path = tempfile::tempdir()
            .expect("failed to open temporary directory")
            .keep();
        let database = {
            let _lock = global_test_lock().lock().await;
            rocks::open_cf_opts(
                root_dir_path.as_path(),
                Some(db_opts),
                metric_conf,
                &[("checkpoint_store", Options::default())],
            )?
        };
        if database.cf_handle("checkpoint_store").is_none() {
            database
                .create_cf("checkpoint_store", &Options::default())
                .map_err(typed_store_err_from_rocks_err)?;
        }
        let checkpoint_store = DBMap::reopen(
            &database,
            Some("checkpoint_store"),
            &ReadWriteOptions::default(),
            false,
        )?;
        let cancellation_token = CancellationToken::new();
        let cloned_cancel_token = cancellation_token.clone();
        let downloader = ParallelCheckpointDownloader::new(
            retriable_client,
            checkpoint_store,
            config,
            &Registry::default(),
        )?;
        let mut rx = downloader.start(0, cloned_cancel_token, Duration::from_secs(30));

        for i in 0..10 {
            match rx.recv().await {
                Some(entry) => {
                    assert_eq!(entry.sequence_number, i);
                }
                None => panic!("Channel closed unexpectedly"),
            }
        }

        cancellation_token.cancel();
        Ok(())
    }
}
