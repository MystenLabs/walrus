// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Fan-out Proxy entry point.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
#[cfg(feature = "test-client")]
use reqwest::Url;
#[cfg(feature = "test-client")]
use walrus_sdk::core::EpochCount;
use walrus_sdk::core_utils::metrics::{Registry, monitored_scope};

#[cfg(any(test, feature = "test-client"))]
mod client;

mod controller;
mod error;
mod metrics;
mod params;
mod tip;
mod utils;

const VERSION: &str = self::utils::version!();

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    about = "The Walrus fan-out proxy",
    long_about = None,
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
)]
struct Args {
    #[arg(
        long,
        short,
        help = "Override the metrics address to use",
        default_value = "127.0.0.1:9184"
    )]
    metrics_address: std::net::SocketAddr,
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Subcommand)]
#[command(rename_all = "kebab-case")]
enum Command {
    /// Run the Walrus Fan-out Proxy.
    Proxy {
        /// The configuration context to use for the client, if omitted the default_context is used.
        #[arg(long)]
        context: Option<String>,
        /// The file path to the Walrus read client configuration.
        #[arg(long)]
        walrus_config: PathBuf,
        /// The address to listen on. Defaults to 0.0.0.0:57391.
        #[arg(long)]
        server_address: Option<SocketAddr>,
        /// The file path to the configuration of the fan-out proxy.
        #[arg(long)]
        fan_out_config: PathBuf,
    },
    #[cfg(feature = "test-client")]
    /// A client to test the fanout proxy. Not intended for production use.
    ///
    /// For the moment, this client will always register and store a permanent blob, encoded with
    /// RS2.
    TestClient {
        /// The file to be stored.
        file: PathBuf,
        /// The configuration context to use for the client, if omitted the default_context is used.
        #[arg(long)]
        context: Option<String>,
        /// The file path to the Walrus read client configuration.
        #[arg(long)]
        walrus_config: Option<PathBuf>,
        /// The url of the proxy server the client will use.
        #[arg(long)]
        server_url: Url,
        /// The number of epochs for which to store the blob.
        #[arg(long)]
        epochs: EpochCount,
        /// The path to the Sui wallet configuration file.
        ///
        /// The wallet configuration is taken from the following locations:
        ///
        /// 1. From this configuration parameter, if set.
        /// 2. From the path specified in the Walrus configuration, if set.
        /// 3. From `./sui_config.yaml`.
        /// 4. From `~/.sui/sui_config/client.yaml`.
        ///
        /// If an invalid path is specified through this option or in the configuration file, an
        /// error is returned.
        // NB: Keep this in sync with `crate::cli`.
        #[arg(long, verbatim_doc_comment)]
        wallet: Option<PathBuf>,
        /// The gas budget for the client.
        ///
        /// If not specified, the client will estimate the gas budget by dry-running the
        /// transaction.
        #[arg(long)]
        gas_budget: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let registry_service = mysten_metrics::start_prometheus_server(args.metrics_address);
    let walrus_registry = registry_service.default_registry();

    monitored_scope::init_monitored_scope_metrics(&walrus_registry);

    // Initialize logging subscriber
    let (_telemetry_guards, _tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .with_prom_registry(&walrus_registry)
        .with_json()
        .init();
    let registry = Registry::new(walrus_registry);

    match args.command {
        Command::Proxy {
            context,
            walrus_config,
            server_address,
            fan_out_config,
        } => {
            controller::run_proxy(
                context,
                walrus_config,
                server_address,
                fan_out_config,
                registry,
            )
            .await
        }
        #[cfg(feature = "test-client")]
        Command::TestClient {
            file,
            context,
            walrus_config,
            server_url,
            epochs,
            wallet,
            gas_budget,
        } => {
            client::run_client(
                file,
                context,
                walrus_config,
                server_url,
                epochs,
                wallet,
                gas_budget,
            )
            .await
        }
    }
}
