// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Generate writes and reads for stress tests.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use blob::WriteBlobConfig;
use futures::future::try_join_all;
use rand::{Rng, thread_rng};
use sui_sdk::types::base_types::SuiAddress;
use tokio::{
    sync::mpsc::{self, Receiver, Sender, error::TryRecvError},
    time::{Interval, MissedTickBehavior},
};
use walrus_core::{BlobId, EpochCount, encoding::Primary};
use walrus_sdk::{
    client::{
        WalrusNodeClient,
        metrics::{self, ClientMetrics},
    },
    config::ClientConfig,
    error::ClientResult,
};
use walrus_service::client::{RefillHandles, Refiller};
use walrus_sui::{
    client::{SuiReadClient, retry_client::RetriableSuiClient},
    utils::SuiNetwork,
};

/// Minimum burst duration in milliseconds.
const MIN_BURST_DURATION_MS: u64 = 100;
/// Minimum burst duration.
const MIN_BURST_DURATION: Duration = Duration::from_millis(MIN_BURST_DURATION_MS);
/// Number of seconds per load period.
const SECS_PER_LOAD_PERIOD: u64 = 60;

pub(crate) mod blob;

mod write_client;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoffConfig};
use write_client::WriteClient;

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    write_client_pool: Receiver<WriteClient>,
    write_client_pool_tx: Sender<WriteClient>,
    read_client_pool: Receiver<WalrusNodeClient<SuiReadClient>>,
    read_client_pool_tx: Sender<WalrusNodeClient<SuiReadClient>>,
    metrics: Arc<ClientMetrics>,
    _refill_handles: RefillHandles,
}

impl LoadGenerator {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        n_clients: usize,
        blob_config: WriteBlobConfig,
        client_config: ClientConfig,
        network: SuiNetwork,
        gas_refill_period: Duration,
        metrics: Arc<ClientMetrics>,
        refiller: Refiller,
    ) -> anyhow::Result<Self> {
        tracing::info!("initializing clients...");

        // Set up read clients
        let (read_client_pool_tx, read_client_pool) = mpsc::channel(n_clients);
        let sui_client = RetriableSuiClient::new_for_rpc_urls(
            &[network.env().rpc.clone()],
            ExponentialBackoffConfig::default(),
            client_config
                .communication_config
                .sui_client_request_timeout,
        )?;

        let sui_read_client = client_config.new_read_client(sui_client.clone()).await?;

        let refresher_handle = client_config
            .refresh_config
            .build_refresher_and_run(sui_read_client.clone())
            .await?;
        for read_client in try_join_all((0..n_clients).map(|_| {
            WalrusNodeClient::new_read_client(
                client_config.clone(),
                refresher_handle.clone(),
                sui_read_client.clone(),
            )
        }))
        .await?
        {
            read_client_pool_tx.send(read_client).await?;
        }

        // Set up write clients
        let (write_client_pool_tx, write_client_pool) = mpsc::channel(n_clients);
        let mut write_clients = Vec::with_capacity(n_clients);

        for _ in 0..n_clients {
            write_clients.push(
                WriteClient::new(
                    &client_config,
                    &network,
                    None,
                    blob_config.clone(),
                    refresher_handle.clone(),
                    refiller.clone(),
                    metrics.clone(),
                )
                .await?,
            )
        }

        let addresses: Vec<SuiAddress> = write_clients
            .iter_mut()
            .map(|client| client.address())
            .collect();
        for write_client in write_clients {
            write_client_pool_tx.send(write_client).await?;
        }

        tracing::info!("finished initializing clients and blobs");

        tracing::info!("spawning gas refill task...");

        let _refill_handles = refiller.refill_gas_and_wal(
            addresses.clone(),
            gas_refill_period,
            metrics.clone(),
            sui_client,
        );

        Ok(Self {
            write_client_pool,
            write_client_pool_tx,
            read_client_pool,
            read_client_pool_tx,
            metrics,
            _refill_handles,
        })
    }

    fn submit_write(&mut self, inconsistent_blob_rate: f64) -> bool {
        let mut client = match self.write_client_pool.try_recv() {
            Ok(client) => client,
            Err(TryRecvError::Empty) => {
                tracing::warn!("no client available to submit write");
                return false;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("write client channel was disconnected");
            }
        };
        let sender = self.write_client_pool_tx.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let result = if thread_rng().gen_bool(inconsistent_blob_rate) {
                client.write_fresh_inconsistent_blob().await
            } else {
                client.write_fresh_blob().await
            };
            match result {
                Ok((_blob_id, elapsed)) => {
                    tracing::info!("write finished");
                    metrics.observe_latency(metrics::WRITE_WORKLOAD, elapsed);
                }
                Err(error) => {
                    // This may happen if the client runs out of gas.
                    tracing::warn!("failed to write blob");
                    metrics.observe_error("failed to write blob");
                    if error.is_out_of_coin_error() {
                        tracing::warn!("consider decreasing the gas refill period");
                    }
                }
            }
            sender
                .send(client)
                .await
                .expect("write client channel should not be closed");
        });

        tracing::info!("submitted write operation");
        self.metrics.observe_submitted(metrics::WRITE_WORKLOAD);
        true
    }

    fn submit_read(&mut self, blob_id: BlobId) -> bool {
        let client = match self.read_client_pool.try_recv() {
            Ok(client) => client,
            Err(TryRecvError::Empty) => {
                tracing::warn!("no client available to submit read");
                return false;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("read client channel was disconnected");
            }
        };
        let sender = self.read_client_pool_tx.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let now = Instant::now();
            let result = client.read_blob::<Primary>(&blob_id).await;
            let elapsed = now.elapsed();

            match result {
                Ok(_) => {
                    tracing::info!("read finished");
                    metrics.observe_latency(metrics::READ_WORKLOAD, elapsed);
                }
                Err(error) => {
                    tracing::error!(?error, ?blob_id, "failed to read blob");
                    metrics.observe_error("failed to read blob");
                }
            }
            sender
                .send(client)
                .await
                .expect("read client channel should not be closed");
        });

        tracing::info!("submitted read operation");
        self.metrics.observe_submitted(metrics::READ_WORKLOAD);
        true
    }

    fn write_burst(
        &mut self,
        writes_per_burst: u64,
        burst_duration: Duration,
        inconsistent_blob_rate: f64,
    ) {
        let burst_start = Instant::now();

        for _ in 0..writes_per_burst {
            if !self.submit_write(inconsistent_blob_rate) {
                break;
            }
        }

        // Check if the submission rate is too high.
        if burst_start.elapsed() > burst_duration {
            self.metrics.observe_error("write rate too high");
            tracing::warn!("write rate too high for this client");
        }
    }

    fn read_burst(&mut self, reads_per_burst: u64, burst_duration: Duration, blob_id: BlobId) {
        let burst_start = Instant::now();
        for _ in 0..reads_per_burst {
            if !self.submit_read(blob_id) {
                break;
            }
        }

        // Check if the submission rate is too high.
        if burst_start.elapsed() > burst_duration {
            self.metrics.observe_error("read rate too high");
            tracing::warn!("read rate too high for this client");
        }
    }

    /// Run the load generator. The `write_load` and `read_load` are the number of respective
    /// operations per minute.
    pub async fn start(
        &mut self,
        write_load: u64,
        read_load: u64,
        inconsistent_blob_rate: f64,
    ) -> anyhow::Result<()> {
        tracing::info!("starting load generator...");

        let (reads_per_burst, read_interval) = burst_load(read_load);
        let read_blob_id = if reads_per_burst != 0 {
            tracing::info!("submitting initial write...");
            // Sets a long enough number of epochs to store to avoid blob not found errors.
            let epochs_to_store = 50;
            let read_blob_id = self
                .initial_write_to_serve_read_workload(epochs_to_store)
                .await
                .inspect_err(|error| tracing::error!(?error, "initial write failed"))?;
            tracing::info!(
                "initial write finished, created blob id: {read_blob_id} \
                with {epochs_to_store} epochs ahead"
            );
            tracing::info!(
                "submitting {reads_per_burst} reads every {} ms",
                read_interval.period().as_millis()
            );
            read_blob_id
        } else {
            BlobId([0; 32])
        };
        tokio::pin!(read_interval);

        let (writes_per_burst, write_interval) = burst_load(write_load);

        tokio::pin!(write_interval);
        if writes_per_burst != 0 {
            tracing::info!(
                "submitting {writes_per_burst} writes every {} ms",
                write_interval.period().as_millis()
            );
        }

        // Submit operations.
        let start = Instant::now();

        loop {
            tokio::select! {
                _ = write_interval.tick() => {
                    self.metrics.observe_execution_duration(Instant::now().duration_since(start));
                    self.write_burst(
                        writes_per_burst,
                        write_interval.period(),
                        inconsistent_blob_rate,
                    );
                }
                _ = read_interval.tick() => {
                    self.metrics.observe_execution_duration(Instant::now().duration_since(start));
                    self.read_burst(reads_per_burst, read_interval.period(), read_blob_id);
                }
                else => break
            }
        }
        Ok(())
    }

    /// Write a blob to serve the read workload.
    ///
    /// This function does extensive retries since the initial write is critical to the read
    /// workload.
    async fn initial_write_to_serve_read_workload(
        &mut self,
        epochs_to_store: EpochCount,
    ) -> ClientResult<BlobId> {
        let mut retry_strategy =
            ExponentialBackoffConfig::default().get_strategy(thread_rng().r#gen());
        let mut attempt = 0;

        loop {
            attempt += 1;
            let mut client = self
                .write_client_pool
                .recv()
                .await
                .expect("write client should be available");
            let result = client
                .write_fresh_blob_with_epochs(Some(epochs_to_store))
                .await
                .map(|(blob_id, _)| blob_id);
            self.write_client_pool_tx
                .send(client)
                .await
                .expect("channel should not be closed");

            match result {
                Ok(blob_id) => return Ok(blob_id),
                Err(error) => {
                    tracing::error!(?error, "writing single blob failed, attempt: {}", attempt);
                    // TODO(zhewu): return earlier if not a retriable error.
                    match retry_strategy.next_delay() {
                        Some(delay) => {
                            tracing::info!("retrying write in {} ms", delay.as_millis());
                            tokio::time::sleep(delay).await;
                        }
                        None => {
                            tracing::error!("write retry strategy exhausted");
                            return Err(error);
                        }
                    }
                }
            }
        }
    }
}

fn burst_load(load: u64) -> (u64, Interval) {
    if load == 0 {
        // Set the interval to ~100 years. `Duration::MAX` causes an overflow in tokio.
        return (
            0,
            tokio::time::interval(Duration::from_hours(100 * 365 * 24)),
        );
    }
    let duration_per_op = Duration::from_secs_f64(SECS_PER_LOAD_PERIOD as f64 / (load as f64));
    let (load_per_burst, burst_duration) = if duration_per_op < MIN_BURST_DURATION {
        (
            load / (SECS_PER_LOAD_PERIOD * 1_000 / MIN_BURST_DURATION_MS),
            MIN_BURST_DURATION,
        )
    } else {
        (1, duration_per_op)
    };

    let mut interval = tokio::time::interval(burst_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    (load_per_burst, interval)
}
