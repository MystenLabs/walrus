// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{io::Write, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::{Parser, Subcommand};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::{
    aggregator::AggregatorServer,
    cli_utils::{error, get_read_client, load_configuration, load_wallet_context, success},
    client::{Client, Config},
};
use walrus_sui::client::SuiContractClient;

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
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long)]
    config: Option<PathBuf>,
    /// The path to the Sui wallet configuration file.
    ///
    /// The wallet configuration is taken from the following locations:
    ///
    /// 1. From this configuration parameter, if set.
    /// 1. From the path specified in the Walrus configuration, if set.
    /// 1. From `./client.yaml`.
    /// 1. From `./sui_config.yaml`.
    /// 1. From `~/.sui/sui_config/client.yaml`.
    ///
    /// If an invalid path is specified through this option or in the configuration file, an error
    /// is returned.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long, default_value = None)]
    wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    #[clap(short, long, default_value_t = 500_000_000)]
    gas_budget: u64,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum Commands {
    /// Store a new blob into Walrus.
    Store {
        /// The file containing the blob to be published to Walrus.
        file: PathBuf,
        /// The number of epochs ahead for which to store the blob.
        #[clap(short, long, default_value_t = 1)]
        epochs: u64,
    },
    /// Read a blob from Walrus, given the blob ID.
    Read {
        /// The blob ID to be read.
        blob_id: BlobId,
        /// The file path where to write the blob.
        ///
        /// If unset, prints the blob to stdout.
        #[clap(short, long)]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
        ///
        /// If unset, the wallet configuration is applied (if set), or the fullnode at
        /// `fullnode.testnet.sui.io:443` is used.
        // NB: Keep this in sync with `walrus_service::cli_utils`.
        #[clap(short, long)]
        rpc_url: Option<String>,
    },
    /// Run an aggregator service at the provided network address.
    Aggregator {
        /// The URL of the Sui RPC node to use.
        ///
        /// If unset, the wallet configuration is applied (if set), or the fullnode at
        /// `fullnode.testnet.sui.io:443` is used.
        // NB: Keep this in sync with `walrus_service::cli_utils`.
        #[clap(short, long)]
        rpc_url: Option<String>,
        /// The address to which to bind the aggregator.
        #[clap(short, long)]
        bind_address: SocketAddr,
    },
}

async fn client() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = load_configuration(&args.config)?;
    tracing::debug!(?args, ?config, "initializing the client");
    let wallet_path = args.wallet.clone().or(config.wallet_config.clone());
    let wallet = load_wallet_context(&wallet_path);

    match args.command {
        Commands::Store { file, epochs } => {
            let sui_client = SuiContractClient::new(
                wallet?,
                config.system_pkg,
                config.system_object,
                args.gas_budget,
            )
            .await?;
            let client = Client::new(config, sui_client).await?;

            tracing::info!(
                file = %file.display(),
                "Storing blob read from the filesystem"
            );
            let blob = client
                .reserve_and_store_blob(&std::fs::read(file)?, epochs)
                .await?;
            println!(
                "{} Blob stored successfully.\nBlob ID: {}",
                success(),
                blob.blob_id
            );
        }
        Commands::Read {
            blob_id,
            out,
            rpc_url,
        } => {
            let client = get_read_client(config, rpc_url, wallet, wallet_path.is_none()).await?;

            let blob = client.read_blob::<Primary>(&blob_id).await?;
            match out {
                Some(path) => {
                    std::fs::write(&path, blob)?;
                    println!(
                        "{} Blob {} reconstructed from Walrus and written to {}.",
                        success(),
                        blob_id,
                        path.display()
                    )
                }
                None => std::io::stdout().write_all(&blob)?,
            }
        }
        Commands::Aggregator {
            rpc_url,
            bind_address,
        } => {
            tracing::debug!(?rpc_url, "attempting to run the Walrus aggregator");
            let client = get_read_client(config, rpc_url, wallet, wallet_path.is_none()).await?;
            let aggregator = AggregatorServer::new(Arc::new(client));
            aggregator.run(&bind_address).await?;
        }
    }
    Ok(())
}

/// The CLI entrypoint.
#[tokio::main]
pub async fn main() {
    if let Err(e) = client().await {
        // Print any error in a (relatively) user-friendly way.
        eprintln!("{} {:#}", error(), e)
    }
}
