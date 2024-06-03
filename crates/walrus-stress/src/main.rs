// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Load generators for stress testing the Walrus nodes.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use futures::{stream::FuturesUnordered, StreamExt};
use generator::WriteTransactionGenerator;
use rand::{rngs::StdRng, SeedableRng};
use tokio::time::{interval, Instant};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::{client::Config, config::LoadConfig};
use walrus_stress::StressParameters;

use crate::{generator::ReadTransactionGenerator, metrics::ClientMetrics};

mod generator;
mod metrics;

/// Timing burst precision.
const PRECISION: u64 = 10;
/// Duration of each burst of transaction.
const BURST_DURATION: Duration = Duration::from_millis(1000 / PRECISION);

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus load generator", long_about = None)]
struct Args {
    /// Turn debugging information on.
    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// The load to submit to the system (tx/s).
    #[clap(long)]
    load: u64,
    /// The path to the wallet configuration file.
    #[clap(long)]
    config_path: PathBuf,
    /// Path to the load parameters file.
    #[clap(long)]
    stress_parameters_path: PathBuf,
    /// The duration of the benchmark in seconds (used to estimate the number of
    /// transactions to pre-generate)
    #[clap(long, value_parser = parse_duration)]
    duration: Duration,
}

fn parse_duration(arg: &str) -> Result<Duration, std::num::ParseIntError> {
    let seconds = arg.parse()?;
    Ok(Duration::from_secs(seconds))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    set_tracing_subscriber(args.verbose);

    let config = Config::load(args.config_path).context("Failed to load client config")?;
    let stress_parameters = StressParameters::load(&args.stress_parameters_path)
        .context("Failed to load stress parameters")?;
    let percentage_writes = stress_parameters.load_type.min(100);
    let duration = args.duration.as_secs();

    // Start the metrics server.
    let metrics_address = SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        stress_parameters.metrics_port,
    );
    let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
    let prometheus_registry = registry_service.default_registry();
    let metrics = ClientMetrics::new(&prometheus_registry);

    // Start the write transaction generator.
    tracing::info!("Initializing write transactions generators...");
    let write_pre_compute = (args.load * duration * percentage_writes) / 100;
    let write_tx_generator = WriteTransactionGenerator::start(
        config.clone(),
        stress_parameters.clone(),
        write_pre_compute as usize,
    )
    .await
    .context("Failed to start write transaction generator")?;
    write_tx_generator.initialize().await;

    // Make one write transaction (which will be used as a template for the read transactions).
    tracing::info!("Submitting one write transaction...");
    let mut rng = StdRng::from_entropy();
    let blob_size = stress_parameters.blob_size;
    let blob = generator::create_random_blob(&mut rng, blob_size);
    let epochs_ahead = 2;
    let client = generator::create_walrus_client(config.clone(), &stress_parameters)
        .await
        .context("Failed to create Walrus client")?;
    let blob = client
        .reserve_and_store_blob(&blob, epochs_ahead)
        .await
        .context("Failed to reserve and store blob")?;

    // Start the read transaction generator.
    tracing::info!("Initializing read transactions generators...");
    let read_pre_compute = (args.load * duration * (100 - percentage_writes)) / 100;
    let read_tx_generator = ReadTransactionGenerator::start(
        config,
        stress_parameters.clone(),
        read_pre_compute as usize,
    )
    .await
    .context("Failed to start read transaction generator")?;
    read_tx_generator.initialize().await;

    // Start the benchmark.
    tracing::info!("Start sending transactions");
    benchmark(
        args.load,
        &stress_parameters,
        write_tx_generator,
        read_tx_generator,
        &blob.blob_id,
        &metrics,
    )
    .await
    .context("Failed to run benchmark")?;

    Ok(())
}

fn set_tracing_subscriber(verbosity: u8) {
    let log_level = match verbosity {
        0 => LevelFilter::ERROR,
        1 => LevelFilter::WARN,
        2 => LevelFilter::INFO,
        3 => LevelFilter::DEBUG,
        _ => LevelFilter::TRACE,
    };

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(log_level.into())
                .from_env_lossy(),
        )
        .with_ansi(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();
}

async fn benchmark(
    load: u64,
    stress_parameters: &StressParameters,
    mut write_tx_generator: WriteTransactionGenerator,
    mut read_tx_generator: ReadTransactionGenerator,
    blob_id: &BlobId,
    metrics: &ClientMetrics,
) -> anyhow::Result<()> {
    let burst = load / PRECISION;
    let writes_per_burst = (burst * stress_parameters.load_type) / 100;
    let reads_per_burst = burst - writes_per_burst;

    // Structures holding futures waiting for clients to finish their requests.
    let mut write_finished = FuturesUnordered::new();
    let mut read_finished = FuturesUnordered::new();

    // Submit transactions.
    let start = Instant::now();
    let interval = interval(BURST_DURATION);
    tokio::pin!(interval);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Generate the transactions for this burst.
                let mut write_load = Vec::new();
                for _ in 1..=writes_per_burst {
                    write_load.push(write_tx_generator.make_tx().await);
                }
                let mut read_load = Vec::new();
                for _ in 1..=reads_per_burst {
                    read_load.push(read_tx_generator.make_tx().await);
                }

                // Submit those transactions.
                let now = Instant::now();
                let duration_since_start = now.duration_since(start);
                metrics.observe_benchmark_duration(duration_since_start);

                for (client, pairs, metadata) in write_load {
                    metrics.observe_submitted(metrics::WRITE_WORKLOAD);

                    write_finished.push(async move {
                        let certificate = client.store_metadata_and_pairs(&metadata, &pairs).await;
                        (now, certificate)
                    });
                }
                for client in read_load {
                    metrics.observe_submitted(metrics::READ_WORKLOAD);

                    read_finished.push(async move {
                        let blob = client.read_blob::<Primary>(blob_id).await;
                        (now, blob)
                    });
                }

                // Check if the submission rate is too high.
                if now.elapsed() > BURST_DURATION {
                    metrics.observe_error("rate too high");
                    tracing::warn!("Transaction rate too high for this client");
                }
            },
            Some((instant, result)) = write_finished.next() => {
                let _certificate = result.context("Failed to obtain storage certificate")?;
                let elapsed = instant.elapsed();
                metrics.observe_latency(metrics::WRITE_WORKLOAD, elapsed);
            },
            Some((instant, result)) = read_finished.next() => {
                let _blob = result.context("Failed to obtain blob")?;
                let elapsed = instant.elapsed();
                metrics.observe_latency(metrics::READ_WORKLOAD, elapsed);
            },
            else => break
        }
    }

    Ok(())
}
