// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{io::Write, net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use walrus_core::{encoding::Primary, BlobId};
use walrus_service::{
    cli_utils::{
        error,
        get_contract_client,
        get_read_client,
        get_sui_client_from_rpc_node_or_wallet,
        load_configuration,
        load_wallet_context,
        print_walrus_info,
        success,
        HumanReadableBytes,
    },
    client::Config,
    daemon::ClientDaemon,
};
use walrus_sui::client::{ReadClient, SuiReadClient};

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Walrus client", long_about = None)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
enum App {
    /// Run the Walrus client by providing the commands as a json-encoded string.
    Json {
        /// The json-encoded commands.
        content: String,
    },
    /// Run the Walrus client as a normal cli tool with flags.
    Cli {
        #[clap(flatten)]
        commands: AppInterface,
    },
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
struct AppInterface {
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

#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
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
        #[clap(flatten)]
        rpc_arg: RpcArg,
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
        #[clap(flatten)]
        rpc_arg: RpcArg,
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
    /// Print information about the Walrus storage system this client is connected to.
    Info {
        #[clap(flatten)]
        rpc_arg: RpcArg,
        /// Print extended information for developers.
        #[clap(long, action)]
        dev: bool,
    },
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PublisherArgs {
    /// The address to which to bind the service.
    #[clap(short, long)]
    pub bind_address: SocketAddr,
    /// The maximum body size of PUT requests in KiB.
    #[clap(short, long = "max-body-size", default_value_t = 10_240)]
    pub max_body_size_kib: usize,
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RpcArg {
    /// The URL of the Sui RPC node to use.
    ///
    /// If unset, the wallet configuration is applied (if set), or the fullnode at
    /// `fullnode.testnet.sui.io:443` is used.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long)]
    rpc_url: Option<String>,
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
    let app: AppInterface = match App::parse() {
        App::Json { content } => serde_json::from_str(&content)?,
        App::Cli { commands } => commands,
    };

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
            rpc_arg: RpcArg { rpc_url },
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
            rpc_arg: RpcArg { rpc_url },
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
        Commands::Info {
            rpc_arg: RpcArg { rpc_url },
            dev,
        } => {
            let sui_client =
                get_sui_client_from_rpc_node_or_wallet(rpc_url, wallet, wallet_path.is_none())
                    .await?;
            let sui_read_client =
                SuiReadClient::new(sui_client, config.system_pkg, config.system_object).await?;
            let price = sui_read_client.price_per_unit_size().await?;
            print_walrus_info(&sui_read_client.current_committee().await?, price, dev);
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
