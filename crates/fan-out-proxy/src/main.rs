// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Fan-out Proxy entry point.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
use reqwest::Url;
use tip::TipConfig;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};
use walrus_core::EpochCount;

#[cfg(feature = "test-client")]
mod client;

mod controller;
mod error;
mod params;
mod tip;

#[derive(Parser)]
#[command(author, version, about)]
struct Args {
    /// Subcommand to run.
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
#[command(rename_all = "kebab-case")]
enum Command {
    /// Run the Walrus Fan-out Proxy.
    Proxy {
        /// The configuration context to use for the client, if omitted the default_context is used.
        #[arg(long, global = true)]
        context: Option<String>,
        /// The file path to the Walrus read client configuration.
        #[arg(long, global = true)]
        walrus_config: PathBuf,
        /// The address to listen on. Defaults to 0.0.0.0:57391.
        #[arg(long, global = true)]
        server_address: Option<SocketAddr>,
        /// The file path to the Tip configuration.
        #[arg(long, global = true)]
        tip_config: PathBuf,
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
        #[arg(long, global = true)]
        context: Option<String>,
        /// The file path to the Walrus read client configuration.
        #[arg(long, global = true)]
        walrus_config: Option<PathBuf>,
        /// The url of the proxy server the client will use.
        #[arg(long, global = true)]
        server_url: Url,
        /// The number of epochs for which to store the blob.
        #[arg(long, global = true)]
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
        #[arg(long, verbatim_doc_comment, global = true)]
        wallet: Option<PathBuf>,
        /// The gas budget for the client.
        ///
        /// If not specified, the client will estimate the gas budget by dry-running the
        /// transaction.
        #[arg(long, global = true)]
        gas_budget: Option<u64>,
    },
}

fn init_logging() {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .boxed()
                .with_filter(EnvFilter::new(directive.clone())),
        )
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging();
    match args.command {
        Command::Proxy {
            context,
            walrus_config,
            server_address,
            tip_config,
        } => controller::run_proxy(context, walrus_config, server_address, tip_config).await,
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
