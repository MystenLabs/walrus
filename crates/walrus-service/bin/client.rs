// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{env, path::PathBuf};

use anyhow::Result;
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
    #[clap(short, long, default_value = default_client_config().into_os_string())]
    wallet: PathBuf,
    /// The gas budget for the transactions.
    #[clap(short, long, default_value = "1_000_000_000")]
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

fn default_client_config() -> PathBuf {
    PathBuf::from(env::var("HOME").expect("environment variable HOME should be set"))
        .join(".sui")
        .join("sui_config")
        .join("client.yaml")
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
        WalletContext::new(&args.wallet, None, None)?,
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
