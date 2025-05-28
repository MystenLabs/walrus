// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Fan-out Proxy entry point.

use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tip::TipConfig;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt as _, util::SubscriberInitExt};

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
    }
}
