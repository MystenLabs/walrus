// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A client to test the Walrus Upload Relay. Not intended for production use.
//!
//! For the moment, this client will always register and store a permanent blob, encoded with
//! RS2.
#![cfg(feature = "test-client")]
#![allow(dead_code)]
use std::{env, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use reqwest::Url;
use walrus_sdk::{core::EpochCount, core_utils::metrics::monitored_scope};

mod test_client;

mod error;
mod metrics;
mod params;
mod shared;
mod tip;
mod utils;

const VERSION: &str = self::utils::version!();

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    about = "The Walrus Upload Relay",
    long_about = None,
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
rename_all = "kebab-case",
)]
struct Args {
    #[arg(
        long,
        short,
        help = "Override the metrics address to use",
        default_value = "127.0.0.1:9184"
    )]
    metrics_address: std::net::SocketAddr,
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

    test_client::run_client(
        args.file,
        args.context,
        args.walrus_config,
        args.server_url,
        args.epochs,
        args.wallet,
        args.gas_budget,
    )
    .await
}
