// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::{
    fmt::{self, Display},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as, DisplayFromStr};
use sui_types::base_types::ObjectID;
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
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::Blob,
};

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Walrus client", long_about = None)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "lowercase")]
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
    #[serde(default)]
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
    #[clap(short, long, verbatim_doc_comment)]
    #[serde(default)]
    wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    #[clap(short, long, default_value_t = default::gas_budget())]
    #[serde(default = "default::gas_budget")]
    gas_budget: u64,
    #[command(subcommand)]
    command: Commands,
}

#[serde_as]
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "lowercase")]
enum Commands {
    /// Store a new blob into Walrus.
    #[clap(alias("write"))]
    Store {
        /// The file containing the blob to be published to Walrus.
        file: PathBuf,
        /// The number of epochs ahead for which to store the blob.
        #[clap(short, long, default_value_t = default::epochs())]
        #[serde(default = "default::epochs")]
        epochs: u64,
    },
    /// Read a blob from Walrus, given the blob ID.
    Read {
        /// The blob ID to be read.
        #[serde_as(as = "DisplayFromStr")]
        blob_id: BlobId,
        /// The file path where to write the blob.
        ///
        /// If unset, prints the blob to stdout.
        #[clap(short, long)]
        #[serde(default)]
        out: Option<PathBuf>,
        #[clap(flatten)]
        #[serde(default)]
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
        #[serde(default)]
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
        #[serde(default)]
        rpc_arg: RpcArg,
        /// Print extended information for developers.
        #[clap(long, action)]
        #[serde(default)]
        dev: bool,
    },
    /// Run the client by specifying the arguments in a json string; cli options are ignored.
    Json {
        /// The json-encoded command for the Walrus cli. The commands "store", "read", "publisher",
        /// "aggregator", and "daemon", are available; "info" and "json" are not available. The json
        /// command follows the same structure of the respective cli command. All flags are
        /// *ignored*.
        command_string: String,
    },
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct PublisherArgs {
    /// The address to which to bind the service.
    #[clap(short, long)]
    pub bind_address: SocketAddr,
    /// The maximum body size of PUT requests in KiB.
    #[clap(short, long = "max-body-size", default_value_t = default::max_body_size_kib())]
    #[serde(default = "default::max_body_size_kib")]
    pub max_body_size_kib: usize,
}

#[derive(Default, Debug, Clone, Args, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct RpcArg {
    /// The URL of the Sui RPC node to use.
    ///
    /// If unset, the wallet configuration is applied (if set), or the fullnode at
    /// `fullnode.testnet.sui.io:443` is used.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long)]
    rpc_url: Option<String>,
}

mod default {
    pub(crate) fn gas_budget() -> u64 {
        500_000_000
    }

    pub(crate) fn epochs() -> u64 {
        1
    }

    pub(crate) fn max_body_size_kib() -> usize {
        10_240
    }
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

/// Helper trait to get the correct string depending on the output mode.
trait PrintOutput: Display + Serialize {
    fn output_string(&self, json: bool) -> Result<String> {
        if json {
            Ok(serde_json::to_string(&self)?)
        } else {
            Ok(self.to_string())
        }
    }
}

/// The output of the store action.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct StoreOutput {
    #[serde_as(as = "DisplayFromStr")]
    blob_id: BlobId,
    sui_object_id: ObjectID,
    blob_size: u64,
}

impl From<Blob> for StoreOutput {
    fn from(blob: Blob) -> Self {
        Self {
            blob_id: blob.blob_id,
            sui_object_id: blob.id,
            blob_size: blob.size,
        }
    }
}

impl Display for StoreOutput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} Blob stored successfully.\n\
                Unencoded size: {}\nSui object ID: {}\nBlob ID: {}",
            success(),
            HumanReadableBytes(self.blob_size),
            self.sui_object_id,
            self.blob_id,
        )
    }
}

impl PrintOutput for StoreOutput {}

/// The output of the read action.
#[serde_as]
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
struct ReadOutput {
    out: Option<PathBuf>,
    #[serde_as(as = "DisplayFromStr")]
    blob_id: BlobId,
    // When serializing to json, the blob is encoded as Base64 string.
    #[serde_as(as = "Base64")]
    blob: Vec<u8>,
}

impl Display for ReadOutput {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.out {
            Some(path) => {
                write!(
                    f,
                    "{} Blob {} reconstructed from Walrus and written to {}.",
                    success(),
                    self.blob_id,
                    path.display()
                )
            }
            // The full blob has been written sto stdout.
            None => write!(f, ""),
        }
    }
}

impl PrintOutput for ReadOutput {}

async fn client() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut app = App::parse();
    let mut json = false;

    if let Commands::Json {
        command_string: command,
    } = app.command
    {
        app = serde_json::from_str(&command)?;
        json = true;
    }
    run_app(app, json).await
}

async fn run_app(app: App, json: bool) -> Result<()> {
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
            println!("{}", StoreOutput::from(blob).output_string(json)?);
        }
        Commands::Read {
            blob_id,
            out,
            rpc_arg: RpcArg { rpc_url },
        } => {
            let client = get_read_client(config, rpc_url, wallet, wallet_path.is_none()).await?;
            let blob = client.read_blob::<Primary>(&blob_id).await?;
            match out.as_ref() {
                Some(path) => std::fs::write(path, &blob)?,
                None => {
                    if !json {
                        std::io::stdout().write_all(&blob)?
                    }
                }
            }
            println!("{}", ReadOutput { out, blob_id, blob }.output_string(json)?);
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
            if json {
                // TODO: Implement the info command for json as well?
                return Err(anyhow!("the info command is only available in cli mode"));
            }
            let sui_client =
                get_sui_client_from_rpc_node_or_wallet(rpc_url, wallet, wallet_path.is_none())
                    .await?;
            let sui_read_client =
                SuiReadClient::new(sui_client, config.system_pkg, config.system_object).await?;
            let price = sui_read_client.price_per_unit_size().await?;
            print_walrus_info(&sui_read_client.current_committee().await?, price, dev);
        }
        Commands::Json { .. } => {
            // If we reach this point, it means that the json command had a json command inside.
            return Err(anyhow!("recursive json commands are not permitted"));
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
