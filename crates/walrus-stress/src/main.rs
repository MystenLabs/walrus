// Copyright (c) Walrus Foundation
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
use walrus_service::{
    client::{metrics::ClientMetrics, Config, Refiller},
    utils::load_from_yaml,
};
use walrus_sui::{config::load_wallet_context_from_path, utils::SuiNetwork};

use crate::generator::LoadGenerator;

mod generator;

/// The amount of gas or MIST to refil each time.
const COIN_REFILL_AMOUNT: u64 = 500_000_000;
/// The minimum balance to keep in the wallet.
const MIN_BALANCE: u64 = 1_000_000_000;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus load generator", long_about = None)]
struct Args {
    /// The target write load to submit to the system (writes/minute).
    /// The actual load may be limited by the number of clients.
    /// If the write load is 0, a single write will be performed to enable reads.
    #[clap(long, default_value_t = 60)]
    write_load: u64,
    /// The target read load to submit to the system (reads/minute).
    /// The actual load may be limited by the number of clients.
    #[clap(long, default_value_t = 60)]
    read_load: u64,
    /// The path to the client configuration file containing the system object address.
    #[clap(long, default_value = "./working_dir/client_config.yaml")]
    config_path: PathBuf,
    /// The number of clients to use for the load generation for reads and writes.
    #[clap(long, default_value = "10")]
    n_clients: NonZeroUsize,
    /// The port on which metrics are exposed.
    #[clap(long, default_value = "9584")]
    metrics_port: u16,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The binary logarithm of the minimum blob size to use for the load generation.
    ///
    /// Blobs sizes are uniformly distributed across the powers of two between
    /// this and the maximum blob size.
    #[clap(long, default_value = "10")]
    min_size_log2: u8,
    /// The binary logarithm of the maximum blob size to use for the load generation.
    #[clap(long, default_value = "20")]
    max_size_log2: u8,
    /// The period in milliseconds to check if gas needs to be refilled.
    ///
    /// This is useful for continuous load testing where the gas budget need to be refilled
    /// periodically.
    #[clap(long, default_value = "1000")]
    gas_refill_period_millis: NonZeroU64,
    /// The fraction of writes that write inconsistent blobs.
    #[clap(long, default_value_t = 0.0)]
    inconsistent_blob_rate: f64,
    /// The path to the Sui Wallet to be used for funding the gas.
    ///
    /// If specified, the funds to run the stress client will be taken from this wallet. Otherwise,
    /// the stress client will try to use the faucet.
    #[clap(long)]
    wallet_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt::try_init();

    let config: Config =
        load_from_yaml(args.config_path).context("Failed to load client config")?;
    let n_clients = args.n_clients.get();

    // Start the metrics server.
    let metrics_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.metrics_port);
    let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
    let prometheus_registry = registry_service.default_registry();
    let metrics = Arc::new(ClientMetrics::new(&prometheus_registry));
    tracing::info!("starting metrics server on {metrics_address}");

    // Start the write transaction generator.
    let gas_refill_period = Duration::from_millis(args.gas_refill_period_millis.get());

    let wallet = load_wallet_context_from_path(args.wallet_path)?;
    let contract_client = config.new_contract_client(wallet, None).await?;

    let refiller = Refiller::new(
        contract_client,
        COIN_REFILL_AMOUNT,
        COIN_REFILL_AMOUNT,
        MIN_BALANCE,
    );
    let mut load_generator = LoadGenerator::new(
        n_clients,
        args.min_size_log2,
        args.max_size_log2,
        config,
        args.sui_network,
        gas_refill_period,
        metrics,
        refiller,
    )
    .await?;

    load_generator
        .start(args.write_load, args.read_load, args.inconsistent_blob_rate)
        .await?;
    Ok(())
}
