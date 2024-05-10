// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The aggregator binary.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use sui_sdk::SuiClientBuilder;
use walrus_service::{
    aggregator::AggregatorServer,
    cli_utils::{error, load_configuration},
    client::{Client, Config},
};
use walrus_sui::client::SuiReadClient;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus client", long_about = None)]
struct Args {
    /// The path to the wallet configuration file.
    ///
    /// The Walrus configuration is taken from the following locations:
    ///
    /// 1. From this configuration parameter, if set.
    /// 1. From `./config.yaml`.
    /// 1. From `~/.walrus/config.yaml`.
    ///
    /// If an invalid path is specified through this option, an error is returned.
    #[clap(short, long)]
    config: Option<PathBuf>,
    /// The URL of the sui RPC endpoint.
    #[clap(short, long)]
    rpc_url: String,
    /// The address where to bind the aggregator.
    #[clap(short, long)]
    network_address: SocketAddr,
}

async fn aggregator() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = load_configuration(&args.config)?;
    tracing::debug!(?args, ?config, "running the Walrus aggregator");
    let sui_client = SuiClientBuilder::default()
        .build(&args.rpc_url)
        .await
        .context("could not connect to the Sui RPC node at {args.rpc_url}")?;
    let read_client =
        SuiReadClient::new(sui_client, config.system_pkg, config.system_object).await?;
    let aggregator = AggregatorServer::new(Arc::new(
        Client::new_read_client(config, &read_client).await?,
    ));
    Ok(aggregator.run(&args.network_address).await?)
}

#[tokio::main]
async fn main() {
    if let Err(e) = aggregator().await {
        eprintln!("{} {:#}", error(), e)
    }
}
