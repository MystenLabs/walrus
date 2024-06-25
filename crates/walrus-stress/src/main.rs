// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Load generators for stress testing the Walrus nodes.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use walrus_service::{client::Config, config::LoadConfig};
use walrus_sui::utils::SuiNetwork;

use crate::{generator::LoadGenerator, metrics::ClientMetrics};

mod generator;
mod metrics;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus load generator", long_about = None)]
struct Args {
    /// The target write load to submit to the system (writes/minute).
    /// The actual load may be limited by the number of clients.
    #[clap(long, default_value = "60")]
    write_load: NonZeroU64,
    /// The target read load to submit to the system (reads/minute).
    /// The actual load may be limited by the number of clients.
    #[clap(long, default_value = "60")]
    read_load: NonZeroU64,
    /// The path to the client configuration file containing the system object address.
    #[clap(long, default_value = "./working_dir/client_config.yaml")]
    config_path: PathBuf,
    /// The number of write clients to use for the load generation.
    #[clap(long, default_value = "10")]
    n_clients: NonZeroUsize,
    /// The port on which metrics are exposed.
    #[clap(long, default_value = "9584")]
    metrics_port: u16,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The blob size to use for the load generation.
    #[clap(long, default_value = "10000")]
    blob_size: NonZeroUsize,
    /// The period in seconds for the gas refill. This option is useful for continuous load testing
    /// where the gas budget need to be refilled periodically.
    #[clap(long, default_value = "600")]
    gas_refill_period: NonZeroU64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();

    let config = Config::load(args.config_path).context("Failed to load client config")?;
    let n_clients = args.n_clients.get();
    let blob_size = args.blob_size.get();

    // Start the metrics server.
    let metrics_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.metrics_port);
    let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
    let prometheus_registry = registry_service.default_registry();
    let metrics = Arc::new(ClientMetrics::new(&prometheus_registry));
    tracing::info!("Starting metrics server on {metrics_address}");

    // Start the write transaction generator.
    let gas_refill_period = Duration::from_secs(args.gas_refill_period.get());
    let mut load_generator = LoadGenerator::new(
        n_clients,
        blob_size,
        config,
        args.sui_network,
        gas_refill_period,
        metrics,
    )
    .await?;

    load_generator
        .start(args.write_load.get(), args.read_load.get())
        .await?;
    Ok(())
}
