// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{io::Write, path::PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use colored::{ColoredString, Colorize};
use sui_sdk::{wallet_context::WalletContext, SuiClientBuilder};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{default_configuration_paths, Client, Config};
use walrus_sui::client::{SuiContractClient, SuiReadClient};

/// Default URL of the devnet RPC node.
pub const DEVNET_RPC: &str = "https://fullnode.devnet.sui.io:443";
/// Default RPC URL to connect to if none is specified explicitly or in the wallet config.
pub const DEFAULT_RPC_URL: &str = DEVNET_RPC;

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
        #[clap(long)]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
        ///
        /// If unset, the wallet configuration is applied (if set), or the fullnode at
        /// `fullnode.devnet.sui.io:443` is used.
        #[clap(short, long, default_value = None)]
        rpc_url: Option<String>,
    },
}

/// Returns the path if it is `Some` or any of the default paths if they exist (attempt in order).
fn path_or_defaults_if_exist(path: &Option<PathBuf>, defaults: &[PathBuf]) -> Option<PathBuf> {
    let mut path = path.clone();
    for default in defaults {
        if path.is_some() {
            break;
        }
        path = default.exists().then_some(default.clone());
    }
    path
}

/// Loads the wallet context from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Sui configuration directory.
fn load_wallet_context(path: &Option<PathBuf>) -> Result<WalletContext> {
    let mut default_paths = vec!["./client.yaml".into(), "./sui_config.yaml".into()];
    if let Some(home_dir) = home::home_dir() {
        default_paths.push(home_dir.join(".sui").join("sui_config").join("client.yaml"))
    }
    let path = path_or_defaults_if_exist(path, &default_paths)
        .ok_or(anyhow!("Could not find a valid wallet config file."))?;
    tracing::info!("Using wallet configuration from {}", path.display());
    WalletContext::new(&path, None, None)
}

/// Loads the Walrus configuration from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Walrus configuration directory.
fn load_configuration(path: &Option<PathBuf>) -> Result<Config> {
    let path = path_or_defaults_if_exist(path, &default_configuration_paths())
        .ok_or(anyhow!("Could not find a valid Walrus configuration file."))?;
    tracing::info!("Using Walrus configuration from {}", path.display());

    serde_yaml::from_str(&std::fs::read_to_string(&path).context(format!(
        "Unable to read Walrus configuration from {}",
        path.display()
    ))?)
    .context(format!(
        "Parsing Walrus configuration from {} failed",
        path.display()
    ))
}

async fn client() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = load_configuration(&args.config)?;
    tracing::debug!(?args, ?config);
    let wallet = load_wallet_context(&args.wallet.clone().or(config.wallet_config.clone()));

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

            tracing::info!(?file, "Storing blob read from the filesystem");
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
            let sui_client = match rpc_url {
                Some(url) => {
                    tracing::info!("Using explicitly set RPC URL {url}");
                    SuiClientBuilder::default()
                        .build(&url)
                        .await
                        .context(format!("cannot connect to Sui RPC node at {url}"))
                }
                None => {
                    match wallet {
                        Ok(wallet) => {
                            tracing::info!("Using RPC URL set in wallet configuration");
                            wallet.get_client().await.context(
                            "cannot connect to Sui RPC node specified in the wallet configuration",
                        )
                        }
                        Err(e) => {
                            match args.wallet {
                                Some(_) => {
                                    // A wallet config was explicitly set, but couldn't be read.
                                    return Err(e);
                                }
                                None => {
                                    tracing::info!("Using default RPC URL {DEFAULT_RPC_URL}");
                                    SuiClientBuilder::default()
                                        .build(DEFAULT_RPC_URL)
                                        .await
                                        .context(format!(
                                            "cannot connect to Sui RPC node at {DEFAULT_RPC_URL}"
                                        ))
                                }
                            }
                        }
                    }
                }
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
                None => std::io::stdout().write_all(&blob)?,
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
