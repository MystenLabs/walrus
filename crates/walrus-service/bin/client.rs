// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{env, path::PathBuf};

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use sui_sdk::wallet_context::WalletContext;
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{Client, Config};
use walrus_sui::client::SuiContractClient;

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus client", long_about = None)]
struct Args {
    // TODO(giac): this will eventually be pulled from the chain.
    /// The configuration file with the committee information.
    #[clap(short, long, default_value = "config.yml")]
    config: PathBuf,
    /// The path to the wallet config file.
    #[clap(short, long, default_value = None)]
    wallet: Option<PathBuf>,
    /// The gas budget for the transactions.
    #[clap(short, long, default_value_t = 1_000_000_000)]
    gas_budget: u64,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum Commands {
    /// Store a new blob into Walrus.
    // TODO(giac): At the moment, the client does not interact with the chain;
    // therefore, this command only works with storage nodes that blindly accept blobs.
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
        #[clap(long)]
        out: PathBuf,
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
        .ok_or(anyhow!("could not find a valid Wallet config file"))?;
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

/// The client.
#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string(args.config)?)?;
    // NOTE(giac): at the moment the wallet is needed for both reading and storing, because is also
    // configures the RPC. In the future, it could be nice to have a "read only" client.
    let sui_client = SuiContractClient::new(
        load_wallet_context(&args.wallet)?,
        config.system_pkg,
        config.system_object,
        args.gas_budget,
    )
    .await?;
    let client = Client::new(config, sui_client).await?;

    match args.command {
        Commands::Store { file, epochs_ahead } => {
            tracing::info!(?file, "Storing blob read from the filesystem");
            client
                .reserve_and_store_blob(&std::fs::read(file)?, epochs_ahead)
                .await?;
            Ok(())
        }
        Commands::Read { blob_id, out } => {
            let blob = client.read_blob::<Primary>(&blob_id).await?;
            Ok(std::fs::write(out, blob)?)
        }
    }
}
