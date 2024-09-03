// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The arguments to the Walrus client binary.

use std::{env, net::SocketAddr, num::NonZeroU16, path::PathBuf, time::Duration};

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use walrus_core::{encoding::EncodingConfig, BlobId};
use walrus_service::client::cli_utils::{
    parse_blob_id,
    read_blob_from_file,
    BlobIdDecimal,
    HumanReadableBytes,
};

pub(crate) const VERSION: &str = walrus_service::utils::version!();

#[derive(Parser, Debug, Clone, Deserialize)]
#[command(author, version, about = "Walrus client", long_about = None)]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase")]
pub(crate) struct App {
    /// The path to the Walrus configuration file.
    ///
    /// If a path is specified through this option, the CLI attempts to read the specified file and
    /// returns an error if the path is invalid.
    ///
    /// If no path is specified explicitly, the CLI looks for `client_config.yaml` or
    /// `client_config.yml` in the following locations (in order):
    ///
    /// 1. The current working directory (`./`).
    /// 2. If the environment variable `XDG_CONFIG_HOME` is set, in `$XDG_CONFIG_HOME/walrus/`.
    /// 3. In `~/.config/walrus/`.
    /// 4. In `~/.walrus/`.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long, verbatim_doc_comment)]
    #[serde(
        default,
        deserialize_with = "walrus_service::utils::resolve_home_dir_option"
    )]
    pub(crate) config: Option<PathBuf>,
    /// The path to the Sui wallet configuration file.
    ///
    /// The wallet configuration is taken from the following locations:
    ///
    /// 1. From this configuration parameter, if set.
    /// 2. From the path specified in the Walrus configuration, if set.
    /// 3. From `./sui_config.yaml`.
    /// 4. From `~/.sui/sui_config/client.yaml`.
    ///
    /// If an invalid path is specified through this option or in the configuration file, an error
    /// is returned.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long, verbatim_doc_comment)]
    #[serde(
        default,
        deserialize_with = "walrus_service::utils::resolve_home_dir_option"
    )]
    pub(crate) wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    #[clap(short, long, default_value_t = default::gas_budget())]
    #[serde(default = "default::gas_budget")]
    pub(crate) gas_budget: u64,
    /// Write output as JSON.
    ///
    /// This is always done in JSON mode.
    #[clap(long, action)]
    #[serde(default)]
    pub(crate) json: bool,
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[serde_as]
#[derive(Subcommand, Debug, Clone, Deserialize)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum Commands {
    /// Store a new blob into Walrus.
    #[clap(alias("write"))]
    Store {
        /// The file containing the blob to be published to Walrus.
        #[serde(deserialize_with = "walrus_service::utils::resolve_home_dir")]
        file: PathBuf,
        /// The number of epochs ahead for which to store the blob.
        #[clap(short, long, default_value_t = default::epochs())]
        #[serde(default = "default::epochs")]
        epochs: u64,
        /// Perform a dry-run of the store without performing any actions on chain.
        ///
        /// This assumes `--force`; i.e., it does not check the current status of the blob.
        #[clap(long, action)]
        #[serde(default)]
        dry_run: bool,
        /// Do not check for the blob status before storing it.
        ///
        /// This will create a new blob even if the blob is already certified for a sufficient
        /// duration.
        #[clap(long, action)]
        #[serde(default)]
        force: bool,
    },
    /// Read a blob from Walrus, given the blob ID.
    Read {
        /// The blob ID to be read.
        #[serde_as(as = "DisplayFromStr")]
        #[clap(allow_hyphen_values = true, value_parser = parse_blob_id)]
        blob_id: BlobId,
        /// The file path where to write the blob.
        ///
        /// If unset, prints the blob to stdout.
        #[clap(short, long)]
        #[serde(
            default,
            deserialize_with = "walrus_service::utils::resolve_home_dir_option"
        )]
        out: Option<PathBuf>,
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// Get the status of a blob.
    ///
    /// This queries multiple storage nodes representing more than a third of the shards for the
    /// blob status and return the "latest" status (in the life-cycle of a blob) that can be
    /// verified with an on-chain event.
    ///
    /// This does not take into account any transient states. For example, for invalid blobs, there
    /// is a short period in which some of the storage nodes are aware of the inconsistency before
    /// this is posted on chain. During this time period, this command would still return a
    /// "verified" status.
    BlobStatus {
        #[clap(flatten)]
        #[serde(flatten)]
        file_or_blob_id: FileOrBlobId,
        /// Timeout for status requests to storage nodes.
        #[clap(short, long, value_parser = humantime::parse_duration, default_value = "1s")]
        #[serde(default = "default::status_timeout")]
        timeout: Duration,
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// Run a publisher service at the provided network address.
    ///
    /// This does not perform any type of access control and is thus not suited for a public
    /// deployment when real money is involved.
    Publisher {
        #[clap(flatten)]
        #[serde(flatten)]
        args: PublisherArgs,
    },
    /// Run an aggregator service at the provided network address.
    Aggregator {
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        #[clap(flatten)]
        #[serde(flatten)]
        daemon_args: DaemonArgs,
    },
    /// Run a client daemon at the provided network address, combining the functionality of an
    /// aggregator and a publisher.
    Daemon {
        #[clap(flatten)]
        #[serde(flatten)]
        args: PublisherArgs,
    },
    /// Print information about the Walrus storage system this client is connected to.
    Info {
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// Print extended information for developers.
        #[clap(long, action)]
        #[serde(default)]
        dev: bool,
    },
    /// Run the client by specifying the arguments in a JSON string; CLI options are ignored.
    Json {
        /// The JSON-encoded args for the Walrus CLI; if not present, the args are read from stdin.
        ///
        /// The JSON structure follows the CLI arguments, containing global options and a "command"
        /// object at the root level. The "command" object itself contains the command (e.g.,k
        /// "store", "read", "publisher", "blobStatus", ...) with an object containing the command
        /// options.
        ///
        /// Note that where CLI options are in "kebab-case", the respective JSON strings are in
        /// "camelCase".
        ///
        /// For example, to read a blob and write it to "some_output_file" using a specific
        /// configuration file, you can use the following JSON input:
        ///
        ///     {
        ///       "config": "path/to/client_config.yaml",
        ///       "command": {
        ///         "read": {
        ///           "blobId": "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo",
        ///           "out": "some_output_file"
        ///         }
        ///       }
        ///     }
        ///
        /// Important: If the "read" command does not have an "out" file specified, the output JSON
        /// string will contain the full bytes of the blob, encoded as a Base64 string.
        #[clap(verbatim_doc_comment)]
        command_string: Option<String>,
    },
    /// Encode the specified file to obtain its blob ID.
    BlobId {
        /// The file containing the blob for which to compute the blob ID.
        #[serde(deserialize_with = "walrus_service::utils::resolve_home_dir")]
        file: PathBuf,
        /// The number of shards for which to compute the blob ID.
        ///
        /// If not specified, the number of shards is read from chain.
        #[clap(short, long)]
        #[serde(default)]
        n_shards: Option<NonZeroU16>,
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// Convert a decimal value to the Walrus blob ID (using URL-safe base64 encoding).
    ConvertBlobId {
        /// The decimal value to be converted to the Walrus blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id_decimal: BlobIdDecimal,
    },
    /// List all registered blobs for the current wallet.
    ListBlobs {
        #[clap(long, action)]
        #[serde(default)]
        /// The output list of blobs will include expired blobs.
        include_expired: bool,
    },
}

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublisherArgs {
    #[clap(flatten)]
    #[serde(flatten)]
    pub(crate) daemon_args: DaemonArgs,
    /// The maximum body size of PUT requests in KiB.
    #[clap(short, long = "max-body-size", default_value_t = default::max_body_size_kib())]
    #[serde(default = "default::max_body_size_kib")]
    pub(crate) max_body_size_kib: usize,
}

impl PublisherArgs {
    pub(crate) fn max_body_size(&self) -> usize {
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

    pub(crate) fn print_debug_message(&self, message: &str) {
        tracing::debug!(
            bind_address = %self.daemon_args.bind_address,
            max_body_size = self.format_max_body_size(),
            message
        );
    }
}

#[derive(Default, Debug, Clone, Args, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcArg {
    /// The URL of the Sui RPC node to use.
    ///
    /// If unset, the wallet configuration is applied (if set), or the fullnode at
    /// `fullnode.testnet.sui.io:443` is used.
    // NB: Keep this in sync with `walrus_service::cli_utils`.
    #[clap(short, long)]
    #[serde(default)]
    pub(crate) rpc_url: Option<String>,
}

#[derive(Debug, Clone, Args, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DaemonArgs {
    /// The address to which to bind the service.
    #[clap(short, long, default_value_t = default::bind_address())]
    #[serde(default = "default::bind_address")]
    pub(crate) bind_address: SocketAddr,
    /// Socket address on which the Prometheus server should export its metrics.
    #[clap(short = 'a', long, default_value_t = default::metrics_address())]
    #[serde(default = "default::metrics_address")]
    pub(crate) metrics_address: SocketAddr,
    /// Path to a blocklist file containing a list (in YAML syntax) of blocked blob IDs.
    #[clap(long)]
    #[serde(
        default,
        deserialize_with = "walrus_service::utils::resolve_home_dir_option"
    )]
    pub(crate) blocklist: Option<PathBuf>,
}

#[serde_as]
#[derive(Debug, Clone, Args, Deserialize)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub(crate) struct FileOrBlobId {
    /// The file containing the blob to be checked.
    #[clap(short, long)]
    #[serde(default)]
    pub(crate) file: Option<PathBuf>,
    /// The blob ID to be checked.
    #[clap(short, long, allow_hyphen_values = true, value_parser = parse_blob_id)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) blob_id: Option<BlobId>,
}

impl FileOrBlobId {
    pub(crate) fn get_or_compute_blob_id(self, encoding_config: &EncodingConfig) -> Result<BlobId> {
        match self {
            FileOrBlobId {
                blob_id: Some(blob_id),
                ..
            } => Ok(blob_id),
            FileOrBlobId {
                file: Some(file), ..
            } => {
                tracing::debug!(
                    file = %file.display(),
                    "checking status of blob read from the filesystem"
                );
                Ok(*encoding_config
                    .get_blob_encoder(&read_blob_from_file(&file)?)?
                    .compute_metadata()
                    .blob_id())
            }
            // This case is required for JSON mode where we don't have the clap checking.
            _ => Err(anyhow!("either the file or blob ID must be defined")),
        }
    }
}

mod default {
    use std::{net::SocketAddr, time::Duration};

    pub(crate) fn gas_budget() -> u64 {
        500_000_000
    }

    pub(crate) fn epochs() -> u64 {
        1
    }

    pub(crate) fn max_body_size_kib() -> usize {
        10_240
    }

    pub(crate) fn status_timeout() -> Duration {
        Duration::from_secs(10)
    }

    pub(crate) fn bind_address() -> SocketAddr {
        "127.0.0.1:31415"
            .parse()
            .expect("this is a correct socket address")
    }

    pub(crate) fn metrics_address() -> SocketAddr {
        "127.0.0.1:27182"
            .parse()
            .expect("this is a correct socket address")
    }
}
