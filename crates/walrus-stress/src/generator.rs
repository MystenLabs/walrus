// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generate writes and reads for stress tests.

use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use futures::{
    future::{self, try_join_all},
    stream::FuturesUnordered,
    StreamExt,
};
use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, SeedableRng};
use sui_sdk::{types::base_types::SuiAddress, SuiClient, SuiClientBuilder};
use tokio::{
    task::JoinHandle,
    time::{Interval, MissedTickBehavior},
};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{Client, ClientError, Config};
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient},
    test_utils::wallet_for_testing_from_faucet,
    utils::{send_faucet_request, SuiNetwork},
};
use walrus_test_utils::WithTempDir;

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
use blob::BlobData;

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    write_client_pool: VecDeque<(WithTempDir<Client<SuiContractClient>>, BlobData)>,
    read_client_pool: VecDeque<Client<()>>,
    metrics: Arc<ClientMetrics>,
    _gas_refill_handle: JoinHandle<anyhow::Result<()>>,
}

impl LoadGenerator {
    pub async fn new(
        n_write_clients: usize,
        blob_size: usize,
        client_config: Config,
        network: SuiNetwork,
        gas_refill_period: Duration,
        metrics: Arc<ClientMetrics>,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing clients...");

        // Set up read clients
        let sui_client = SuiClientBuilder::default()
            .build(&network.env().rpc)
            .await
            .context(format!(
                "cannot connect to Sui RPC node at {}",
                &network.env().rpc
            ))?;
        let sui_read_client =
            SuiReadClient::new(sui_client.clone(), client_config.system_object).await?;
        let read_clients = VecDeque::from(
            try_join_all(
                (0..n_write_clients)
                    .map(|_| Client::new_read_client(client_config.clone(), &sui_read_client)),
            )
            .await?,
        );

        // Set up write clients
        let mut clients = try_join_all(
            (0..n_write_clients).map(|_| new_client(&client_config, network, DEFAULT_GAS_BUDGET)),
        )
        .await?;

        tracing::info!("Spawning gas refill task...");
        let addresses = clients
            .iter_mut()
            .map(|client| {
                client
                    .as_mut()
                    .sui_client()
                    .wallet()
                    .active_address()
                    .expect("clients are created with an have an active address")
            })
            .collect();
        let gas_refill_handle = refill_gas(
            addresses,
            network,
            gas_refill_period,
            metrics.clone(),
            sui_client,
        );

        tracing::info!("Initializing blobs...");
        let blobs = (0..n_write_clients).map(|_| {
            BlobData::random(
                StdRng::from_rng(thread_rng()).expect("rng should be seedable from thread_rng"),
                blob_size,
            )
        });
        let write_client_pool = clients.into_iter().zip(blobs).collect();
        tracing::info!("Finished initializing clients and blobs.");
        Ok(Self {
            write_client_pool,
            read_client_pool: read_clients,
            metrics,
            _gas_refill_handle: gas_refill_handle,
        })
    }

    /// Run the load generator. The `write_load` and `read_load` are the number of respective
    /// operations per minute.
    pub async fn start(&mut self, write_load: u64, read_load: u64) -> anyhow::Result<()> {
        tracing::info!("Starting load generator...");

        // Pool of blob IDs to read.
        let mut blob_ids = Vec::with_capacity(read_load as usize);

        // Structures holding futures waiting for clients to finish their requests.
        let mut write_finished = FuturesUnordered::new();
        let mut read_finished = FuturesUnordered::new();
        let (writes_per_burst, write_interval) = burst_load(write_load);

        tokio::pin!(write_interval);
        if writes_per_burst != 0 {
            tracing::info!(
                "Submitting {writes_per_burst} writes every {} ms",
                write_interval.period().as_millis()
            );
        } else {
            self.single_write().await?;
        }

        let (reads_per_burst, read_interval) = burst_load(read_load);
        if reads_per_burst != 0 {
            tracing::info!(
                "Submitting {reads_per_burst} reads every {} ms",
                read_interval.period().as_millis()
            );
        }
        tokio::pin!(read_interval);

        // Submit operations.
        let start = Instant::now();

        loop {
            tokio::select! {
                _ = write_interval.tick() => {
                    let burst_start = Instant::now();

                    let duration_since_start = burst_start.duration_since(start);
                    self.metrics.observe_benchmark_duration(duration_since_start);

                    for _ in 0..writes_per_burst {
                        let Some((client, mut blob)) = self.write_client_pool.pop_front() else {
                            tracing::warn!("No client available to submit write");
                            break;
                        };
                        write_finished.push(tokio::spawn(async move {
                            // Update the blob data to get a fresh value for the write.
                            blob.refresh();
                            let now = Instant::now();
                            let result = client.as_ref()
                                .reserve_and_store_blob(blob.as_ref(), 1, true).await;
                            let elapsed = now.elapsed();
                            (elapsed, result, client, blob)
                        }));

                        tracing::info!("Submitted write operation");
                        self.metrics.observe_submitted(metrics::WRITE_WORKLOAD);
                    }

                    // Check if the submission rate is too high.
                    if burst_start.elapsed() > write_interval.period() {
                        self.metrics.observe_error("write rate too high");
                        tracing::warn!("write rate too high for this client");
                    }
                }
                _ = read_interval.tick() => {
                    let burst_start = Instant::now();

                    let duration_since_start = burst_start.duration_since(start);
                    self.metrics.observe_benchmark_duration(duration_since_start);

                    for _ in 0..reads_per_burst {
                        let Some(&blob_id) = blob_ids.choose(&mut thread_rng()) else {
                            tracing::info!("no blobs have been written yet, skipping reads");
                            break;
                        };
                        let Some(client) = self.read_client_pool.pop_front() else {
                            tracing::warn!("No client available to submit read");
                            break;
                        };
                        read_finished.push(tokio::spawn(async move {
                            let now = Instant::now();
                            let result = client.read_blob::<Primary>(&blob_id).await;
                            let elapsed = now.elapsed();
                            (elapsed, result, client)
                        }));

                        tracing::info!("Submitted read operation");
                        self.metrics.observe_submitted(metrics::READ_WORKLOAD);
                    }

                    // Check if the submission rate is too high.
                    if burst_start.elapsed() > read_interval.period() {
                        self.metrics.observe_error("read rate too high");
                        tracing::warn!("read rate too high for this client");
                    }
                }
                Some(Ok((elapsed, result, client, blob))) = write_finished.next() => {
                    match result {
                        Ok(blob_result) => {
                            tracing::info!("Write finished");
                            self.metrics.observe_latency(metrics::WRITE_WORKLOAD, elapsed);
                            if (blob_ids.len() as u64) < read_load {
                                blob_ids.push(blob_result.blob_id().to_owned());
                            } else if let Some(entry) = blob_ids.choose_mut(&mut thread_rng()) {
                                blob_result.blob_id().clone_into(entry);
                            }
                        }
                        Err(error) => {
                            // This may happen if the client runs out of gas.
                            tracing::warn!("failed to obtain storage certificate");
                           self.metrics.observe_error("failed to obtain storage certificate");
                            if error.is_out_of_coin_error() {
                                tracing::warn!("consider increasing the gas refill period");
                            }
                        }
                    }
                    self.write_client_pool.push_back((client, blob));
                },
                Some(Ok((elapsed, result, client))) = read_finished.next() => {
                    match result {
                        Ok(_) => {
                            tracing::info!("read finished");
                            self.metrics.observe_latency(metrics::READ_WORKLOAD, elapsed);
                        }
                        Err(e) => {
                            tracing::error!(error=?e, "failed to read blob");
                            self.metrics.observe_error("failed to read blob");
                        }
                    }
                    self.read_client_pool.push_back(client);
                },
                else => break
            }
        }
        Ok(())
    }

    async fn single_write(&self) -> Result<BlobId, ClientError> {
        let (client, blob) = self
            .write_client_pool
            .front()
            .expect("write client should be available");
        client
            .as_ref()
            .reserve_and_store_blob(blob.as_ref(), 1, true)
            .await
            .map(|blob_result| blob_result.blob_id().to_owned())
    }
}

async fn new_client(
    config: &Config,
    network: SuiNetwork,
    gas_budget: u64,
) -> anyhow::Result<WithTempDir<Client<SuiContractClient>>> {
    // Create the client with a separate wallet
    let wallet = wallet_for_testing_from_faucet(network).await?;
    let sui_read_client =
        SuiReadClient::new(wallet.as_ref().get_client().await?, config.system_object).await?;
    let sui_contract_client = wallet.and_then(|wallet| {
        SuiContractClient::new_with_read_client(wallet, gas_budget, sui_read_client)
    })?;

    let client = sui_contract_client
        .and_then_async(|contract_client| Client::new(config.clone(), contract_client))
        .await?;
    Ok(client)
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
