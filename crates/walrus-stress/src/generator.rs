// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generate transactions for stress tests.

use std::time::{Duration, Instant};

use anyhow::Context;
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use tokio::{
    sync::mpsc::{self, error::TryRecvError, Receiver, Sender},
    time::MissedTickBehavior,
};
use walrus_service::client::{Client, Config};
use walrus_sui::{
    client::{SuiContractClient, SuiReadClient},
    test_utils::wallet_for_testing_from_faucet,
    utils::SuiNetwork,
};
use walrus_test_utils::WithTempDir;

use crate::{metrics, metrics::ClientMetrics};

const DEFAULT_GAS_BUDGET: u64 = 100_000_000;
/// Timing burst precision.
const PRECISION: u64 = 10;

/// Create a random blob of a given size.
pub fn create_random_blob(rng: &mut StdRng, blob_size: usize) -> Vec<u8> {
    (0..blob_size).map(|_| rng.gen::<u8>()).collect()
}

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    client_pool_tx: Sender<WithTempDir<Client<SuiContractClient>>>,
    client_pool_rx: Receiver<WithTempDir<Client<SuiContractClient>>>,
}

impl LoadGenerator {
    pub async fn new(
        n_clients: usize,
        client_config: Config,
        network: SuiNetwork,
    ) -> anyhow::Result<Self> {
        let (client_pool_tx, client_pool_rx) = mpsc::channel(n_clients);
        let futures =
            (0..n_clients).map(|_| new_client(&client_config, network, DEFAULT_GAS_BUDGET));
        for client in try_join_all(futures).await?.into_iter() {
            client_pool_tx
                .send(client)
                .await
                .expect("channel should not be closed");
        }
        Ok(Self {
            client_pool_tx,
            client_pool_rx,
        })
    }

    /// Run the load generator.
    pub async fn start(
        &mut self,
        load: u64,
        blob_size: usize,
        metrics: &ClientMetrics,
    ) -> anyhow::Result<()> {
        let (tx_per_burst, burst_duration) = if load > PRECISION {
            (load / PRECISION, Duration::from_millis(1000 / PRECISION))
        } else {
            (load, Duration::from_secs(1))
        };
        tracing::info!(
            "Submitting {tx_per_burst} transactions every {} ms",
            burst_duration.as_millis()
        );

        let mut rng =
            StdRng::from_rng(thread_rng()).expect("should be able to seed rng from thread_rng");

        // Structures holding futures waiting for clients to finish their requests.
        let mut write_finished = FuturesUnordered::new();

        // Submit transactions.
        let start = Instant::now();
        let mut interval = tokio::time::interval(burst_duration);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let burst_start = Instant::now();

                    let duration_since_start = burst_start.duration_since(start);
                    metrics.observe_benchmark_duration(duration_since_start);

                    for _ in 0..tx_per_burst {
                        let blob = create_random_blob(&mut rng, blob_size);
                        let client = match self
                            .client_pool_rx
                            .try_recv() {
                                Ok(client) => client,
                                Err(TryRecvError::Empty) => {
                                    tracing::warn!("No client available to submit write");
                                    break;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    panic!("The channel should remain open")
                                }
                            };
                        write_finished.push(tokio::spawn(async move {
                            let now = Instant::now();
                            let result = client.as_ref().reserve_and_store_blob(&blob, 1, true).await;
                            (now, result, client)
                        }));

                        tracing::info!("Submitted write transaction");
                        metrics.observe_submitted(metrics::WRITE_WORKLOAD);
                    }

                    // Check if the submission rate is too high.
                    if burst_start.elapsed() > burst_duration {
                        metrics.observe_error("rate too high");
                        tracing::warn!("rate too high for this client");
                    }
                }
                Some(Ok((instant, result, client))) = write_finished.next() => {
                    tracing::info!("Write finished");
                    let _certificate = result.context("Failed to obtain storage certificate")?;
                    let elapsed = instant.elapsed();
                    metrics.observe_latency(metrics::WRITE_WORKLOAD, elapsed);
                    self.client_pool_tx.send(client).await.expect("channel should not be closed");
                },
                else => break
            }
        }

        Ok(())
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
