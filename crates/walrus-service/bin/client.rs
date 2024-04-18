// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{env, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use colored::{ColoredString, Colorize};
use sui_sdk::{wallet_context::WalletContext, SuiClientBuilder};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{Client, Config};
use walrus_sui::client::{SuiContractClient, SuiReadClient};

/// Default URL of the devnet RPC node.
pub const DEVNET_RPC: &str = "https://fullnode.devnet.sui.io:443";

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus client", long_about = None)]
struct Args {
    /// The path to the configuration file with the system information ([`Config`]).
    #[clap(short, long, default_value = "config.yml")]
    config: PathBuf,
    /// The path to the wallet configuration file.
    #[clap(short, long, default_value = None)]
    wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    #[clap(short, long, default_value_t = 1_000_000_000)]
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
        epochs_ahead: u64,
    },
    /// Read a blob from Walrus, given the blob ID.
    Read {
        /// The blob ID to be read.
        blob_id: BlobId,
        /// The file path where to write the blob.
        ///
        /// If unset, prints the blob to stdout.
        #[clap(long)]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
        ///
        /// If unset, the wallet configuration is applied (if set), or the default [`DEVNET_RPC`]
        /// is used.
        #[clap(short, long, default_value = None)]
        rpc_url: Option<String>,
    },
}

/// Loads the wallet context from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the home folder.
fn load_wallet_context(path: &Option<PathBuf>) -> Result<WalletContext> {
    let path = path
        .as_ref()
        .cloned()
        .or_else(local_client_config)
        .or_else(home_client_config)
        .ok_or(anyhow!("Could not find a valid wallet config file."))?;
    tracing::info!("Using wallet configuration from {}", path.display());
    WalletContext::new(&path, None, None)
}

fn local_client_config() -> Option<PathBuf> {
    let path: PathBuf = "./client.yaml".into();
    path.exists().then_some(path)
}

fn home_client_config() -> Option<PathBuf> {
    env::var("HOME")
        .map(|home| {
            PathBuf::from(home)
                .join(".sui")
                .join("sui_config")
                .join("client.yaml")
        })
        .ok()
        .filter(|path| path.exists())
}

async fn client() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config =
        serde_yaml::from_str(&std::fs::read_to_string(&args.config).context(format!(
            "Unable to read client configuration from {}",
            args.config.display()
        ))?)?;
    tracing::debug!(?args, ?config);

    match args.command {
        Commands::Store { file, epochs_ahead } => {
            let wallet = load_wallet_context(&args.wallet)?;
            let sui_client = SuiContractClient::new(
                wallet,
                config.system_pkg,
                config.system_object,
                args.gas_budget,
            )
            .await?;
            let client = Client::new(config, sui_client).await?;

            tracing::info!(?file, "Storing blob read from the filesystem");
            let blob = client
                .reserve_and_store_blob(&std::fs::read(file)?, epochs_ahead)
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
            let sui_client = match rpc_url {
                Some(url) => SuiClientBuilder::default()
                    .build(&url)
                    .await
                    .context(format!("cannot connect to Sui RPC node at {url}")),
                None => match load_wallet_context(&args.wallet) {
                    Ok(wallet) => wallet.get_client().await.context(
                        "cannot connect to Sui RPC node specified in the wallet configuration",
                    ),
                    Err(e) => match args.wallet {
                        Some(_) => {
                            // A wallet config was explicitly set, but couldn't be read.
                            return Err(e);
                        }
                        None => SuiClientBuilder::default()
                            .build(DEVNET_RPC)
                            .await
                            .context(format!("cannot connect to Sui RPC node at {DEVNET_RPC}")),
                    },
                },
            }?;
            let read_client =
                SuiReadClient::new(sui_client, config.system_pkg, config.system_object).await?;
            let client = Client::new_read_client(config, &read_client).await?;
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
                None => println!("{}", std::str::from_utf8(&blob)?),
            }
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

fn success() -> ColoredString {
    "Success:".bold().green()
}

fn error() -> ColoredString {
    "Error:".bold().red()
}
