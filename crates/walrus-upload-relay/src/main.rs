// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Upload Relay entry point.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use walrus_sdk::core_utils::{
    bin_version,
    metrics::{Registry, monitored_scope},
};

mod controller;
mod error;
mod metrics;
mod params;
mod shared;
mod tip;
mod utils;

bin_version!();

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
    /// The configuration context to use for the client, if omitted the default_context is used.
    #[arg(long)]
    context: Option<String>,
    /// The file path to the Walrus read client configuration.
    #[arg(long)]
    walrus_config: PathBuf,
    /// The address to listen on. Defaults to 0.0.0.0:57391.
    #[arg(long)]
    server_address: Option<SocketAddr>,
    /// The file path to the configuration of the Walrus Upload Relay.
    #[arg(long)]
    relay_config: PathBuf,
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

    controller::run_proxy(
        args.context,
        args.walrus_config,
        args.server_address,
        args.relay_config,
        registry,
    )
    .await
}
