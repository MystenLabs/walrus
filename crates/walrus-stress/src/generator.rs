// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generate writes and reads for stress tests.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use futures::{
    future::{self, try_join_all},
    StreamExt,
};
use rand::{thread_rng, Rng};
use sui_sdk::{types::base_types::SuiAddress, SuiClient, SuiClientBuilder};
use tokio::{
    sync::mpsc::{self, error::TryRecvError, Receiver, Sender},
    task::JoinHandle,
    time::{Interval, MissedTickBehavior},
};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{Client, ClientError, Config};
use walrus_sui::{
    client::SuiReadClient,
    utils::{send_faucet_request, SuiNetwork},
};

use crate::{metrics, metrics::ClientMetrics};

const DEFAULT_GAS_BUDGET: u64 = 100_000_000;

// If a node has less than `MIN_NUM_COINS` without at least `MIN_COIN_VALUE`,
// we need to request additional coins from the faucet.
const MIN_COIN_VALUE: u64 = 500_000_000;
// We need at least a payment coin and a gas coin.
const MIN_NUM_COINS: usize = 2;

/// Minimum burst duration.
const MIN_BURST_DURATION: Duration = Duration::from_millis(100);
/// Number of seconds per load period.
const SECS_PER_LOAD_PERIOD: u64 = 60;

mod blob;

mod write_client;
use write_client::WriteClient;

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    write_client_pool: Receiver<WriteClient>,
    write_client_pool_tx: Sender<WriteClient>,
    read_client_pool: Receiver<Client<()>>,
    read_client_pool_tx: Sender<Client<()>>,
    metrics: Arc<ClientMetrics>,
    _gas_refill_handle: JoinHandle<anyhow::Result<()>>,
}

impl LoadGenerator {
    pub async fn new(
        n_clients: usize,
        blob_size: usize,
        client_config: Config,
        network: SuiNetwork,
        gas_refill_period: Duration,
        metrics: Arc<ClientMetrics>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing clients...");

        // Set up read clients
        let (read_client_pool_tx, read_client_pool) = mpsc::channel(n_clients);
        let sui_client = SuiClientBuilder::default()
            .build(&network.env().rpc)
            .await
            .context(format!(
                "cannot connect to Sui RPC node at {}",
                &network.env().rpc
            ))?;
        let sui_read_client =
            SuiReadClient::new(sui_client.clone(), client_config.system_object).await?;
        for read_client in try_join_all(
            (0..n_clients)
                .map(|_| Client::new_read_client(client_config.clone(), &sui_read_client)),
        )
        .await?
        {
            read_client_pool_tx.send(read_client).await?;
        }

        // Set up write clients
        let (write_client_pool_tx, write_client_pool) = mpsc::channel(n_clients);
        let mut write_clients = try_join_all(
            (0..n_clients)
                .map(|_| WriteClient::new(&client_config, network, DEFAULT_GAS_BUDGET, blob_size)),
        )
        .await?;

        let addresses = write_clients
            .iter_mut()
            .map(|client| client.address())
            .collect();
        for write_client in write_clients {
            write_client_pool_tx.send(write_client).await?;
        }

        tracing::info!("Finished initializing clients and blobs.");

        tracing::info!("Spawning gas refill task...");
        let gas_refill_handle = refill_gas(
            addresses,
            network,
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
            _gas_refill_handle: gas_refill_handle,
        })
    }

    fn submit_write(&mut self, inconsistent_blob_rate: f64) -> bool {
        let mut client = match self.write_client_pool.try_recv() {
            Ok(client) => client,
            Err(TryRecvError::Empty) => {
                tracing::warn!("No client available to submit write");
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
                    tracing::info!("Write finished");
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

        tracing::info!("Submitted write operation");
        self.metrics.observe_submitted(metrics::WRITE_WORKLOAD);
        true
    }

    fn submit_read(&mut self, blob_id: BlobId) -> bool {
        let client = match self.read_client_pool.try_recv() {
            Ok(client) => client,
            Err(TryRecvError::Empty) => {
                tracing::warn!("No client available to submit write");
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
                Err(e) => {
                    tracing::error!(error=?e, "failed to read blob");
                    metrics.observe_error("failed to read blob");
                }
            }
            sender
                .send(client)
                .await
                .expect("read client channel should not be closed");
        });

        tracing::info!("Submitted read operation");
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
        tracing::info!("Starting load generator...");

        let (reads_per_burst, read_interval) = burst_load(read_load);
        let read_blob_id = if reads_per_burst != 0 {
            tracing::info!("Submitting initial write...");
            let read_blob_id = self
                .single_write()
                .await
                .inspect_err(|e| tracing::error!(error = ?e, "initial write failed"))?;
            tracing::info!(
                "Submitting {reads_per_burst} reads every {} ms",
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
                "Submitting {writes_per_burst} writes every {} ms",
                write_interval.period().as_millis()
            );
        }

        // Submit operations.
        let start = Instant::now();

        loop {
            tokio::select! {
                _ = write_interval.tick() => {
                    self.metrics.observe_benchmark_duration(Instant::now().duration_since(start));
                    self.write_burst(writes_per_burst, write_interval.period(), inconsistent_blob_rate);
                }
                _ = read_interval.tick() => {
                    self.metrics.observe_benchmark_duration(Instant::now().duration_since(start));
                    self.read_burst(reads_per_burst, write_interval.period(), read_blob_id);
                }
                else => break
            }
        }
        Ok(())
    }

    async fn single_write(&mut self) -> Result<BlobId, ClientError> {
        let mut client = self
            .write_client_pool
            .recv()
            .await
            .expect("write client should be available");
        let result = client.write_fresh_blob().await.map(|(blob_id, _)| blob_id);
        self.write_client_pool_tx
            .send(client)
            .await
            .expect("channel should not be closed");
        result
    }
}

fn burst_load(load: u64) -> (u64, Interval) {
    if load == 0 {
        return (0, tokio::time::interval(Duration::MAX));
    }
    let duration_per_op = Duration::from_secs_f64(SECS_PER_LOAD_PERIOD as f64 / (load as f64));
    let (load_per_burst, burst_duration) = if duration_per_op < MIN_BURST_DURATION {
        (
            load / (SECS_PER_LOAD_PERIOD * 1_000 / MIN_BURST_DURATION.as_millis() as u64),
            MIN_BURST_DURATION,
        )
    } else {
        (1, duration_per_op)
    };

    let mut interval = tokio::time::interval(burst_duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    (load_per_burst, interval)
}

pub fn refill_gas(
    addresses: Vec<SuiAddress>,
    network: SuiNetwork,
    period: Duration,
    metrics: Arc<ClientMetrics>,
    sui_client: SuiClient,
) -> JoinHandle<anyhow::Result<()>> {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    tokio::spawn(async move {
        loop {
            interval.tick().await;
            let sui_client = &sui_client;
            let metrics = &metrics;
            let _ = try_join_all(addresses.iter().cloned().map(|address| async move {
                if sui_client
                    .coin_read_api()
                    .get_coins_stream(address, None)
                    .filter(|coin| future::ready(coin.balance >= MIN_COIN_VALUE))
                    .take(MIN_NUM_COINS)
                    .collect::<Vec<_>>()
                    .await
                    .len()
                    < MIN_NUM_COINS
                {
                    let result = send_faucet_request(address, network).await;
                    tracing::debug!("Clients gas coins refilled");
                    metrics.observe_gas_refill();
                    result
                } else {
                    Ok(())
                }
            }))
            .await
            .inspect_err(|e| tracing::error!("error while refilling gas: {e}"));
        }
    })
}
