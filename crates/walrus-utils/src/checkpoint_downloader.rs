// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Checkpoint downloader util.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use prometheus::{register_int_gauge_with_registry, IntGauge, Registry};
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use sui_rpc_api::{client::sdk, Client};
use sui_types::{
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::{CheckpointSequenceNumber, TrustedCheckpoint},
};
use tokio::{
    sync::{mpsc, RwLock},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use typed_store::{rocks::DBMap, Map};

/// Fetcher configuration options for the parallel checkpoint fetcher.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ParallelDownloaderConfig {
    /// Number of retries per checkpoint before giving up.
    pub min_retries: u32,
    /// Initial delay before the first retry.
    pub initial_delay: Duration,
    /// Maximum delay between retries. Once this delay is reached, the
    /// fetcher will keep retrying with this duration.
    pub max_delay: Duration,
}

impl Default for ParallelDownloaderConfig {
    fn default() -> Self {
        // Checkpoint downloader configuration tuned for:
        // - Checkpoint generation rate: 1 per 200ms (5 per second)
        // - Network RTT: ~100-300ms
        //
        // Retry timing example with 100ms RTT:
        // T+0ms:   Client requests checkpoint N
        // T+100ms: Client learns checkpoint N unavailable
        // T+200ms: Server generates checkpoint N
        // T+250ms: Client retries after retry_delay
        // T+350ms: Client gets response which should contain checkpoint N.

        // Retry timing example with 300ms RTT:
        // T+0ms:   Client requests checkpoint N
        // T+200ms: Server generates checkpoint N
        // T+300ms: Client learns checkpoint N unavailable
        // T+450ms: Client retries after retry_delay
        // T+750ms: Client gets response which should contain checkpoint N.
        // The retry_delay of 150ms is chosen to balance in steady state with
        // fully caught up client:
        // - Not retrying too quickly (which could waste network resources)
        // - Not waiting too long (which would cause us to fall behind)
        // TODO: Use adaptive retry delay based on RTT and error type (issue-1123)
        Self {
            min_retries: 10,
            initial_delay: Duration::from_millis(150),
            max_delay: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Additional buffer factor for work queue
    /// Helps keep the worker busy in case of delays
    /// in draining the checkpoint queue.
    pub work_queue_buffer_factor: usize,
    /// Buffer factor for result queue
    /// Helps handle delays in processing the results.
    pub result_queue_buffer_factor: usize,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            work_queue_buffer_factor: 3,
            result_queue_buffer_factor: 3,
        }
    }
}

/// Configuration for the adaptive worker pool.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AdaptiveDownloaderConfig {
    /// Minimum number of workers.
    pub min_workers: usize,
    /// Maximum number of workers.
    pub max_workers: usize,
    /// Initial number of workers.
    pub initial_workers: usize,
    /// Checkpoint lag threshold for scaling up.
    pub scale_up_lag_threshold: u64,
    /// Checkpoint lag threshold for scaling down.
    pub scale_down_lag_threshold: u64,
    /// Minimum time between scaling operations.
    pub scale_cooldown: Duration,
    /// Base configuration.
    pub base_config: ParallelDownloaderConfig,
    /// Channel configuration.
    pub channel_config: ChannelConfig,
}

impl Default for AdaptiveDownloaderConfig {
    fn default() -> Self {
        Self {
            min_workers: 1,
            max_workers: 10,
            initial_workers: 5,
            scale_up_lag_threshold: 100,
            scale_down_lag_threshold: 10,
            scale_cooldown: Duration::from_secs(10),
            base_config: ParallelDownloaderConfig::default(),
            channel_config: ChannelConfig::default(),
        }
    }
}

impl AdaptiveDownloaderConfig {
    fn message_queue_size(&self) -> usize {
        self.max_workers * self.channel_config.work_queue_buffer_factor
    }

    fn checkpoint_queue_size(&self) -> usize {
        self.max_workers * self.channel_config.work_queue_buffer_factor
    }

    fn result_queue_size(&self) -> usize {
        self.max_workers
            * self.channel_config.work_queue_buffer_factor
            * self.channel_config.result_queue_buffer_factor
    }
}

/// Metrics for the event processor.
#[derive(Clone, Debug)]
struct AdaptiveDownloaderMetrics {
    /// The number of checkpoint downloader workers.
    pub num_workers: IntGauge,
    /// The current checkpoint lag between the local store and the full node.
    pub checkpoint_lag: IntGauge,
}

impl AdaptiveDownloaderMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            num_workers: register_int_gauge_with_registry!(
                "checkpoint_downloader_num_workers",
                "Number of workers active in the worker pool",
                registry,
            )
            .expect("this is a valid metrics registration"),
            checkpoint_lag: register_int_gauge_with_registry!(
                "checkpoint_downloader_checkpoint_lag",
                "Current checkpoint lag between local store and full node",
                registry,
            )
            .expect("this is a valid metrics registration"),
        }
    }
}

/// Worker message types.
enum WorkerMessage {
    /// Download a checkpoint with the given sequence number.
    Download(CheckpointSequenceNumber),
    /// Shutdown the worker.
    Shutdown,
}

/// Entry in the checkpoint fetcher queue.
#[derive(Debug)]
pub struct CheckpointEntry {
    pub sequence_number: u64,
    pub result: Result<CheckpointData>,
}

impl CheckpointEntry {
    pub fn new(sequence_number: u64, result: Result<CheckpointData>) -> Self {
        Self {
            sequence_number,
            result,
        }
    }
}

/// Configuration for the pool monitor.
#[derive(Clone)]
struct PoolMonitorConfig {
    downloader_config: AdaptiveDownloaderConfig,
    checkpoint_store: DBMap<(), TrustedCheckpoint>,
    client: Client,
    metrics: AdaptiveDownloaderMetrics,
}

/// Channels for the pool monitor.
#[derive(Clone)]
struct PoolMonitorChannels {
    message_sender: async_channel::Sender<WorkerMessage>,
    message_receiver: async_channel::Receiver<WorkerMessage>,
    checkpoint_sender: mpsc::Sender<CheckpointEntry>,
}

/// Parallel checkpoint downloader that fetches checkpoints in parallel.
#[derive(Clone)]
pub struct ParallelCheckpointDownloader {
    full_node_client: Client,
    checkpoint_store: DBMap<(), TrustedCheckpoint>,
    config: AdaptiveDownloaderConfig,
    metrics: AdaptiveDownloaderMetrics,
}

impl ParallelCheckpointDownloader {
    /// Creates a new parallel checkpoint downloader.
    pub fn new(
        full_node_client: Client,
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
    ) -> mpsc::Receiver<CheckpointEntry> {
        let inner = ParallelCheckpointDownloaderInner::new(
            self.full_node_client.clone(),
            self.checkpoint_store.clone(),
            self.config.clone(),
            cancellation_token,
            self.metrics.clone(),
        );
        inner.start(sequence_number)
    }
}

/// Parallel checkpoint downloader that fetches checkpoints in parallel.
struct ParallelCheckpointDownloaderInner {
    message_sender: async_channel::Sender<WorkerMessage>,
    checkpoint_receiver: mpsc::Receiver<CheckpointEntry>,
    config: AdaptiveDownloaderConfig,
    worker_count: Arc<RwLock<usize>>,
    cancellation_token: CancellationToken,
}

impl ParallelCheckpointDownloaderInner {
    /// Creates a new parallel checkpoint downloader.
    fn new(
        full_node_client: Client,
        checkpoint_store: DBMap<(), TrustedCheckpoint>,
        config: AdaptiveDownloaderConfig,
        cancellation_token: CancellationToken,
        metrics: AdaptiveDownloaderMetrics,
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
            );
        }

        let worker_count = Arc::new(RwLock::new(config.initial_workers));
        let cloned_checkpoint_store = checkpoint_store.clone();
        let cloned_cancel_token = cancellation_token.clone();
        let cloned_client = full_node_client.clone();
        let cloned_message_sender = message_sender.clone();
        let cloned_config = config.clone();
        let cloned_worker_count = worker_count.clone();
        tokio::spawn(async move {
            let config = PoolMonitorConfig {
                downloader_config: cloned_config.clone(),
                checkpoint_store: cloned_checkpoint_store,
                client: cloned_client,
                metrics,
            };
            let channels = PoolMonitorChannels {
                message_sender: cloned_message_sender,
                message_receiver,
                checkpoint_sender,
            };
            Self::pool_monitor(config, channels, cloned_worker_count, cloned_cancel_token).await?;
            anyhow::Ok(())
        });

        Self {
            message_sender,
            checkpoint_receiver,
            config,
            worker_count,
            cancellation_token,
        }
    }

    fn spawn_new_worker(
        worker_id: usize,
        full_node_client: Client,
        message_receiver: async_channel::Receiver<WorkerMessage>,
        checkpoint_sender: mpsc::Sender<CheckpointEntry>,
        config: ParallelDownloaderConfig,
    ) {
        tokio::spawn(async move {
            Self::worker_loop(
                worker_id,
                full_node_client,
                message_receiver,
                checkpoint_sender,
                config,
            )
            .await?;
            anyhow::Ok(())
        });
    }

    /// Returns the current checkpoint lag between the local store and the full node
    /// in terms of sequence numbers. This works by downloading the latest checkpoint
    /// summary from the full node and comparing it with the current checkpoint in the store.
    async fn current_checkpoint_lag(
        checkpoint_store: &DBMap<(), TrustedCheckpoint>,
        client: &Client,
    ) -> Result<u64> {
        let Ok(Some(current_checkpoint)) = checkpoint_store.get(&()) else {
            return Err(anyhow!("Failed to fetch current checkpoint"));
        };
        let latest_checkpoint = client.get_latest_checkpoint().await?;
        let current_lag =
            latest_checkpoint.sequence_number - current_checkpoint.inner().sequence_number;
        Ok(current_lag)
    }

    /// Worker loop that fetches checkpoints.
    async fn worker_loop(
        worker_id: usize,
        client: Client,
        message_receiver: async_channel::Receiver<WorkerMessage>,
        checkpoint_sender: mpsc::Sender<CheckpointEntry>,
        config: ParallelDownloaderConfig,
    ) -> Result<()> {
        tracing::info!(worker_id, "starting checkpoint download worker");
        let mut rng = StdRng::from_entropy();
        while let Ok(WorkerMessage::Download(sequence_number)) = message_receiver.recv().await {
            let entry =
                Self::download_with_retry(&client, sequence_number, &config, &mut rng).await;
            checkpoint_sender.send(entry).await?;
        }
        tracing::info!(worker_id, "checkpoint download worker shutting down");
        Ok(())
    }

    /// Pool monitor that scales the worker pool based on checkpoint lag.
    async fn pool_monitor(
        config: PoolMonitorConfig,
        channels: PoolMonitorChannels,
        worker_count: Arc<RwLock<usize>>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let downloader_config = config.downloader_config.clone();
        let mut next_worker_id = downloader_config.initial_workers;
        let mut last_scale = Instant::now();

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

                    let Ok(lag) = Self::current_checkpoint_lag(
                        &config.checkpoint_store, &config.client).await else {
                        tracing::warn!("failed to fetch current checkpoint lag");
                        continue;
                    };

                    config.metrics.checkpoint_lag.set(lag as i64);
                    let num_workers_before = *worker_count.read().await;
                    let mut num_workers_after = num_workers_before;
                    if lag > downloader_config.scale_up_lag_threshold &&
                        num_workers_before < downloader_config.max_workers {
                        num_workers_after = num_workers_before + 1;
                        tracing::info!(
                            "scaling up checkpoint workers from {} to {} due to high lag ({})",
                            num_workers_before,
                            num_workers_after,
                            lag
                        );
                        let cloned_client = config.client.clone();
                        let cloned_receiver = channels.message_receiver.clone();
                        let cloned_checkpoint_sender = channels.checkpoint_sender.clone();
                        let cloned_config = downloader_config.base_config.clone();
                        Self::spawn_new_worker(next_worker_id, cloned_client, cloned_receiver,
                            cloned_checkpoint_sender, cloned_config);
                        *worker_count.write().await = num_workers_after;
                        last_scale = now;
                        next_worker_id += 1;
                    } else if lag < downloader_config.scale_down_lag_threshold &&
                            num_workers_before > downloader_config.min_workers {
                        num_workers_after = num_workers_before - 1;
                        tracing::info!(
                            "scaling down checkpoint workers from {} to {} due to low lag ({})",
                            num_workers_before,
                            num_workers_after,
                            lag
                        );
                        channels.message_sender.send(WorkerMessage::Shutdown).await?;
                        *worker_count.write().await = num_workers_after;
                        last_scale = now;
                    }

                    config.metrics.num_workers.set(num_workers_after as i64);
                }
            }
        }
    }

    /// Starts downloading checkpoints from the given sequence number.
    pub fn start(
        mut self,
        sequence_number: CheckpointSequenceNumber,
    ) -> mpsc::Receiver<CheckpointEntry> {
        let (result_tx, result_rx) = mpsc::channel(self.config.result_queue_size());
        let message_sender = self.message_sender.clone();

        tokio::task::spawn(async move {
            let mut sequence_number = sequence_number;
            let mut in_flight = BTreeSet::new();
            let mut pending = BTreeMap::new();
            let mut next_expected = sequence_number;

            while !self.cancellation_token.is_cancelled() {
                let num_workers = *self.worker_count.read().await;
                let max_in_flight_requests =
                    num_workers * self.config.channel_config.work_queue_buffer_factor;
                while in_flight.len() < max_in_flight_requests {
                    if message_sender
                        .send(WorkerMessage::Download(sequence_number))
                        .await
                        .is_err()
                    {
                        tracing::error!("failed to send job to workers; channel closed");
                        return;
                    }
                    in_flight.insert(sequence_number);
                    sequence_number += 1;
                }

                let Some(entry) = self.checkpoint_receiver.recv().await else {
                    break;
                };

                in_flight.remove(&entry.sequence_number);

                if entry.sequence_number != next_expected {
                    // Store out-of-order response
                    pending.insert(entry.sequence_number, entry);
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
            }
        });

        result_rx
    }

    /// Downloads a checkpoint with retries.
    #[cfg(not(test))]
    async fn download_with_retry(
        client: &Client,
        sequence_number: CheckpointSequenceNumber,
        config: &ParallelDownloaderConfig,
        rng: &mut StdRng,
    ) -> CheckpointEntry {
        let mut backoff = create_backoff(rng, config);
        loop {
            let result = client.get_full_checkpoint(sequence_number).await;
            let Ok(checkpoint) = result else {
                let err = result.err();
                handle_checkpoint_error(err.as_ref(), sequence_number);

                let delay = backoff.next().expect("backoff should not be exhausted");

                tokio::time::sleep(delay).await;
                continue;
            };

            return CheckpointEntry::new(sequence_number, Ok(checkpoint));
        }
    }

    #[cfg(test)]
    async fn download_with_retry(
        client: &Client,
        sequence_number: CheckpointSequenceNumber,
        _config: &ParallelDownloaderConfig,
        _rng: &mut StdRng,
    ) -> CheckpointEntry {
        let res = client.get_full_checkpoint(sequence_number).await;
        let Ok(checkpoint) = res else {
            let err = res.err();
            handle_checkpoint_error(err.as_ref(), sequence_number);
            let err = err
                .map(|e| e.into())
                .unwrap_or_else(|| anyhow!("Failed to download checkpoint"));
            return CheckpointEntry::new(sequence_number, Err(err));
        };
        CheckpointEntry {
            sequence_number,
            result: Ok(checkpoint),
        }
    }
}

/// Helper function to create backoff with consistent settings
#[cfg(not(test))]
fn create_backoff(
    rng: &mut StdRng,
    config: &ParallelDownloaderConfig,
) -> crate::backoff::ExponentialBackoff<StdRng> {
    use rand::RngCore;
    crate::backoff::ExponentialBackoff::new_with_seed(
        config.initial_delay,
        config.max_delay,
        None,
        rng.next_u64(),
    )
}

/// Handles an error that occurred while reading the next checkpoint.
/// If the error is due to a checkpoint that is already present on the server, it is logged as an
/// error. Otherwise, it is logged as a debug.
fn handle_checkpoint_error(err: Option<&sdk::Error>, next_checkpoint: u64) {
    let error = err.as_ref().map(|e| e.to_string()).unwrap_or_default();
    if let Some(checkpoint_height) = err
        .as_ref()
        .and_then(|e| e.parts())
        .and_then(|p| p.checkpoint_height)
    {
        if next_checkpoint > checkpoint_height {
            tracing::trace!(
                next_checkpoint,
                checkpoint_height,
                %error,
                "failed to read next checkpoint, probably not produced yet",
            );
            return;
        }
    }
    tracing::warn!(next_checkpoint, ?error, "failed to read next checkpoint",);
}

#[cfg(test)]
mod tests {
    use rocksdb::Options;
    use typed_store::{
        rocks,
        rocks::{errors::typed_store_err_from_rocks_err, MetricConf, ReadWriteOptions},
    };

    use super::*;
    use crate::tests::global_test_lock;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_fetcher() -> Result<()> {
        let client = Client::new("http://localhost:9000")?;
        let parallel_config = ParallelDownloaderConfig {
            min_retries: 10,
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
        };
        let metric_conf = MetricConf::default();
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let root_dir_path = tempfile::tempdir()
            .expect("Failed to open temporary directory")
            .into_path();
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
            client,
            checkpoint_store,
            config,
            &Registry::default(),
        )?;
        let mut rx = downloader.start(0, cloned_cancel_token);

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
