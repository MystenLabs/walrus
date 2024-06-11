// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generate transactions for stress tests.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use anyhow::Context;
use futures::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use tokio::time::MissedTickBehavior;
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

mod blob;
use blob::BlobData;

/// A load generator for Walrus writes.
#[derive(Debug)]
pub struct LoadGenerator {
    client_pool: VecDeque<(WithTempDir<Client<SuiContractClient>>, BlobData)>,
}

impl LoadGenerator {
    pub async fn new(
        n_clients: usize,
        blob_size: usize,
        client_config: Config,
        network: SuiNetwork,
    ) -> anyhow::Result<Self> {
        tracing::info!("Initializing clients...");
        let clients = try_join_all(
            (0..n_clients).map(|_| new_client(&client_config, network, DEFAULT_GAS_BUDGET)),
        )
        .await?;

        tracing::info!("Initializing blobs...");
        let blobs = (0..n_clients).map(|_| {
            BlobData::random(
                StdRng::from_rng(thread_rng()).expect("rng should be seedable from thread_rng"),
                blob_size,
            )
        });
        let client_pool = clients.into_iter().zip(blobs).collect();
        tracing::info!("Finished initializing clients and blobs.");
        Ok(Self { client_pool })
    }

    /// Run the load generator.
    pub async fn start(&mut self, load: u64, metrics: &ClientMetrics) -> anyhow::Result<()> {
        tracing::info!("Starting load generator...");
        let (tx_per_burst, burst_duration) = if load > PRECISION {
            (load / PRECISION, Duration::from_millis(1000 / PRECISION))
        } else {
            (load, Duration::from_secs(1))
        };
        tracing::info!(
            "Submitting {tx_per_burst} transactions every {} ms",
            burst_duration.as_millis()
        );

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
                        let Some((client, mut blob)) = self.client_pool.pop_front() else {
                            tracing::warn!("No client available to submit write");
                            break;
                        };
                        write_finished.push(tokio::spawn(async move {
                            blob.refresh();
                            let now = Instant::now();
                            let result = client.as_ref().reserve_and_store_blob(blob.as_ref(), 1, true).await;
                            let elapsed = now.elapsed();
                            (elapsed, result, client, blob)
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
                Some(Ok((elapsed, result, client, blob))) = write_finished.next() => {
                    let _certificate = result.context("Failed to obtain storage certificate")?;
                    tracing::info!("Write finished");
                    metrics.observe_latency(metrics::WRITE_WORKLOAD, elapsed);
                    self.client_pool.push_back((client, blob));
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
