// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{io::Write, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::{
    cli_utils::{
        error,
        get_contract_client,
        get_read_client,
        load_configuration,
        load_wallet_context,
        success,
        HumanReadableBytes,
    },
    client::Config,
    daemon::ClientDaemon,
};

#[derive(Parser, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
#[command(author, version, about = "Walrus client", long_about = None)]
struct App {
    /// The path to the wallet configuration file.
    ///
    /// The Walrus configuration is taken from the following locations:
    ///
    /// 1. From this configuration parameter, if set.
    /// 2. From `./config.yaml`.
    /// 3. From `~/.walrus/config.yaml`.
    ///
    /// If an invalid path is specified through this option, an error is returned.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long, verbatim_doc_comment)]
    config: Option<PathBuf>,
    /// The path to the Sui wallet configuration file.
    ///
    /// The wallet configuration is taken from the following locations:
    ///
    /// 1. From this configuration parameter, if set.
    /// 2. From the path specified in the Walrus configuration, if set.
    /// 3. From `./client.yaml`.
    /// 4. From `./sui_config.yaml`.
    /// 5. From `~/.sui/sui_config/client.yaml`.
    ///
    /// If an invalid path is specified through this option or in the configuration file, an error
    /// is returned.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long, default_value = None, verbatim_doc_comment)]
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
    #[clap(alias("write"))]
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
    /// Run a publisher service at the provided network address.
    ///
    /// This does not perform any type of access control and is thus not suited for a public
    /// deployment when real money is involved.
    Publisher {
        #[clap(flatten)]
        args: PublisherArgs,
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
    /// Run a client daemon at the provided network address, combining the functionality of an
    /// aggregator and a publisher.
    Daemon {
        #[clap(flatten)]
        args: PublisherArgs,
    },
}

#[derive(Debug, Clone, Args)]
struct PublisherArgs {
    /// The address to which to bind the service.
    #[clap(short, long)]
    pub bind_address: SocketAddr,
    /// The maximum body size of PUT requests in KiB.
    #[clap(short, long = "max-body-size", default_value_t = 10_240)]
    pub max_body_size_kib: usize,
}

impl PublisherArgs {
    fn max_body_size(&self) -> usize {
        self.max_body_size_kib << 10
    }

    fn format_max_body_size(&self) -> String {
        format!(
            "{}",
            HumanReadableBytes(
                self.max_body_size()
                    .try_into()
                    .expect("should fit into a `u64`")
            )
        )
    }

    fn print_debug_message(&self, message: &str) {
        tracing::debug!(
            bind_address = %self.bind_address,
            max_body_size = self.format_max_body_size(),
            message
        );
    }
}

async fn client() -> Result<()> {
    tracing_subscriber::fmt::init();
    let app = App::parse();
    let config: Config = load_configuration(&app.config)?;
    tracing::debug!(?app, ?config, "initializing the client");
    let wallet_path = app.wallet.clone().or(config.wallet_config.clone());
    let wallet = load_wallet_context(&wallet_path);

    match app.command {
        Commands::Store { file, epochs } => {
            let client = get_contract_client(config, wallet, app.gas_budget).await?;

            tracing::info!(
                file = %file.display(),
                "Storing blob read from the filesystem"
            );
            let blob = client
                .reserve_and_store_blob(&std::fs::read(file)?, epochs)
                .await?;
            println!(
                "{} Blob stored successfully.\n\
                Unencoded size: {}\nBlob ID: {}\nSui object ID: {}",
                success(),
                HumanReadableBytes(blob.size),
                blob.blob_id,
                blob.id,
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
        Commands::Publisher { args } => {
            args.print_debug_message("attempting to run the Walrus publisher");
            let client = get_contract_client(config, wallet, app.gas_budget).await?;
            let publisher =
                ClientDaemon::new(client, args.bind_address).with_publisher(args.max_body_size());
            publisher.run().await?;
        }
        Commands::Aggregator {
            rpc_url,
            bind_address,
        } => {
            tracing::debug!(?rpc_url, "attempting to run the Walrus aggregator");
            let client = get_read_client(config, rpc_url, wallet, wallet_path.is_none()).await?;
            let aggregator = ClientDaemon::new(client, bind_address).with_aggregator();
            aggregator.run().await?;
        }
        Commands::Daemon { args } => {
            args.print_debug_message("attempting to run the Walrus daemon");
            let client = get_contract_client(config, wallet, app.gas_budget).await?;
            let publisher = ClientDaemon::new(client, args.bind_address)
                .with_aggregator()
                .with_publisher(args.max_body_size());
            publisher.run().await?;
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
