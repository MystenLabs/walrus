// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::client::{Client, Config};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus client", long_about = None)]
struct Args {
    // TODO(giac): this will eventually be pulled from the chain.
    /// The configuration file with the committee information.
    #[clap(short, long, default_value = "config.yml")]
    config: PathBuf,
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

/// The client.
#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config: Config = serde_yaml::from_str(&std::fs::read_to_string(args.config)?)?;
    let client = Client::new(config);

    match args.command {
        Commands::Store { file } => {
            tracing::info!(?file, "Storing blob read from the filesystem");
            client?.store_blob(&std::fs::read(file)?).await?;
            Ok(())
        }
        Commands::Read { blob_id, out } => {
            let blob = client?.read_blob::<Primary>(&blob_id).await?;
            Ok(std::fs::write(out, blob)?)
        }
    }
}
