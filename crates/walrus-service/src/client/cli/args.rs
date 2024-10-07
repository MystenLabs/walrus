// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The arguments to the Walrus client binary.

use std::{net::SocketAddr, num::NonZeroU16, path::PathBuf, time::Duration};

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use sui_types::base_types::ObjectID;
use walrus_core::{encoding::EncodingConfig, BlobId, EpochCount};

use super::{parse_blob_id, read_blob_from_file, BlobIdDecimal, HumanReadableBytes};

/// The command-line arguments for the Walrus client.
#[derive(Parser, Debug, Clone, Deserialize)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase")]
pub struct App {
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
    // NB: Keep this in sync with `crate::cli`.
    #[clap(short, long, verbatim_doc_comment)]
    #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
    pub config: Option<PathBuf>,
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
    // NB: Keep this in sync with `crate::cli`.
    #[clap(short, long, verbatim_doc_comment)]
    #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
    pub wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    #[clap(short, long, default_value_t = default::gas_budget())]
    #[serde(default = "default::gas_budget")]
    pub gas_budget: u64,
    /// Write output as JSON.
    ///
    /// This is always done in JSON mode.
    #[clap(long, action)]
    #[serde(default)]
    pub json: bool,
    /// The command to run.
    #[command(subcommand)]
    pub command: Commands,
}

impl App {
    /// Checks if the command has been supplied in JSON mode, and extracts the underlying command.
    pub fn extract_json_command(&mut self) -> Result<()> {
        while let Commands::Json { command_string } = &self.command {
            tracing::info!("running in JSON mode");
            let command_string = match command_string {
                Some(s) => s,
                None => {
                    tracing::debug!("reading JSON input from stdin");
                    &std::io::read_to_string(std::io::stdin())?
                }
            };
            tracing::debug!(
                command = command_string.replace('\n', ""),
                "running JSON command"
            );
            let new_self = serde_json::from_str(command_string)?;
            let _ = std::mem::replace(self, new_self);
            self.json = true;
        }
        Ok(())
    }
}

/// Top level enum to separate the daemon and CLI commands.
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Commands {
    /// Run the client by specifying the arguments in a JSON string; CLI options are ignored.
    Json {
        /// The JSON-encoded args for the Walrus CLI; if not present, the args are read from stdin.
        ///
        /// The JSON structure follows the CLI arguments, containing global options and a "command"
        /// object at the root level. The "command" object itself contains the command (e.g.,
        /// "store", "read", "publisher", "blobStatus", ...) with an object containing the command
        /// options.
        ///
        /// Note that where CLI options are in "kebab-case", the respective JSON strings are in
        /// "camelCase".
        ///
        /// For example, to read a blob and write it to "some_output_file" using a specific
        /// configuration file, you can use the following JSON input:
        ///
        /// {
        ///   "config": "path/to/client_config.yaml",
        ///   "command": {
        ///     "read": {
        ///       "blobId": "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo",
        ///       "out": "some_output_file"
        ///     }
        ///   }
        /// }
        ///
        /// Important: If the "read" command does not have an "out" file specified, the output JSON
        /// string will contain the full bytes of the blob, encoded as a Base64 string.
        #[clap(verbatim_doc_comment)]
        command_string: Option<String>,
    },
    /// Commands to run the binary in CLI mode.
    #[clap(flatten)]
    #[serde(untagged)]
    Cli(CliCommands),
    /// Commands to run the binary in daemon mode.
    #[clap(flatten)]
    #[serde(untagged)]
    Daemon(DaemonCommands),
}

/// The CLI commands for the Walrus client.
#[serde_as]
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum CliCommands {
    /// Store a new blob into Walrus.
    #[clap(alias("write"))]
    Store {
        /// The file containing the blob to be published to Walrus.
        #[serde(deserialize_with = "crate::utils::resolve_home_dir")]
        file: PathBuf,
        /// The number of epochs ahead for which to store the blob.
        #[clap(short, long, default_value_t = default::epochs())]
        #[serde(default = "default::epochs")]
        epochs: EpochCount,
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
        /// Mark the blob as deletable.
        ///
        /// Deletable blobs can be removed from Walrus before their expiration time.
        #[clap(long, action)]
        #[serde(default)]
        deletable: bool,
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
        #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
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
        /// The filename or the blob ID of the blob to check the status of.
        #[clap(flatten)]
        #[serde(flatten)]
        file_or_blob_id: FileOrBlobId,
        /// Timeout for status requests to storage nodes.
        #[clap(short, long, value_parser = humantime::parse_duration, default_value = "1s")]
        #[serde(default = "default::status_timeout")]
        timeout: Duration,
        /// The URL of the Sui RPC node to use.
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// Print information about the Walrus storage system this client is connected to.
    Info {
        /// The URL of the Sui RPC node to use.
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// Print extended information for developers.
        #[clap(long, action)]
        #[serde(default)]
        dev: bool,
    },
    /// Encode the specified file to obtain its blob ID.
    BlobId {
        /// The file containing the blob for which to compute the blob ID.
        #[serde(deserialize_with = "crate::utils::resolve_home_dir")]
        file: PathBuf,
        /// The number of shards for which to compute the blob ID.
        ///
        /// If not specified, the number of shards is read from chain.
        #[clap(short, long)]
        #[serde(default)]
        n_shards: Option<NonZeroU16>,
        /// The URL of the Sui RPC node to use.
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
    /// Delete a blob from Walrus.
    ///
    /// This command is only available for blobs that are deletable.
    Delete {
        /// The filename, or the blob ID, or the object ID of the blob to delete.
        #[clap(flatten)]
        #[serde(flatten)]
        target: FileOrBlobIdOrObjectId,
    },
    /// Stake with storage node.
    Stake {
        #[clap(long)]
        /// The object ID of the storage node to stake with.
        node_id: ObjectID,
        #[clap(short, long, default_value_t = default::staking_amount_frost())]
        #[serde(default = "default::staking_amount_frost")]
        /// The amount of FROST (smallest unit of WAL token) to stake with the storage node.
        amount: u64,
    },
    /// Exchange SUI for WAL through the configured exchange.
    GetWal {
        #[clap(long)]
        /// The object ID of the exchange to use.
        ///
        /// This takes precedence over the value in the config file.
        exchange_id: Option<ObjectID>,
        #[clap(short, long, default_value_t = default::exchange_amount_mist())]
        #[serde(default = "default::exchange_amount_mist")]
        /// The amount of MIST to exchange for WAL/FROST.
        amount: u64,
    },
}

/// The daemon commands for the Walrus client.
#[serde_as]
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DaemonCommands {
    /// Run a publisher service at the provided network address.
    ///
    /// This does not perform any type of access control and is thus not suited for a public
    /// deployment when real money is involved.
    Publisher {
        #[clap(flatten)]
        #[serde(flatten)]
        /// The publisher args.
        args: PublisherArgs,
    },
    /// Run an aggregator service at the provided network address.
    Aggregator {
        #[clap(flatten)]
        #[serde(flatten)]
        /// The URL of the Sui RPC node to use.
        rpc_arg: RpcArg,
        #[clap(flatten)]
        #[serde(flatten)]
        /// The daemon args.
        daemon_args: DaemonArgs,
    },
    /// Run a client daemon at the provided network address, combining the functionality of an
    /// aggregator and a publisher.
    Daemon {
        #[clap(flatten)]
        #[serde(flatten)]
        /// The publisher args.
        args: PublisherArgs,
    },
}

impl DaemonCommands {
    /// Gets the metrics address from the commands that support it.
    pub fn get_metrics_address(&self) -> SocketAddr {
        match &self {
            DaemonCommands::Publisher { args } => args.daemon_args.metrics_address,
            DaemonCommands::Aggregator { daemon_args, .. } => daemon_args.metrics_address,
            DaemonCommands::Daemon { args } => args.daemon_args.metrics_address,
        }
    }
}

#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PublisherArgs {
    #[clap(flatten)]
    #[serde(flatten)]
    pub daemon_args: DaemonArgs,
    /// The maximum body size of PUT requests in KiB.
    #[clap(short, long = "max-body-size", default_value_t = default::max_body_size_kib())]
    #[serde(default = "default::max_body_size_kib")]
    pub max_body_size_kib: usize,
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

/// The URL of the Sui RPC node to use.
#[derive(Default, Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcArg {
    /// The URL of the Sui RPC node to use.
    ///
    /// If unset, the wallet configuration is applied (if set), or the fullnode at
    /// `fullnode.testnet.sui.io:443` is used.
    // NB: Keep this in sync with `crate::cli`.
    #[clap(short, long)]
    #[serde(default)]
    pub(crate) rpc_url: Option<String>,
}

#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonArgs {
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
    #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
    pub(crate) blocklist: Option<PathBuf>,
}

#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct FileOrBlobId {
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

#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct FileOrBlobIdOrObjectId {
    /// The file containing the blob to be deleted.
    ///
    /// This is equivalent to calling `blob-id` on the file, and then deleting with `--blob-id`.
    #[clap(short, long)]
    #[serde(default)]
    pub(crate) file: Option<PathBuf>,
    /// The blob ID to be deleted.
    ///
    /// This command deletes _all_ owned blob objects matching the provided blob ID.
    #[clap(short, long, allow_hyphen_values = true, value_parser = parse_blob_id)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) blob_id: Option<BlobId>,
    /// The object ID of the blob object to be deleted.
    ///
    /// This command deletes only the blob object with the given object ID.
    #[clap(short, long)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) object_id: Option<ObjectID>,
}

impl FileOrBlobIdOrObjectId {
    pub(crate) fn get_or_compute_blob_id(
        &self,
        encoding_config: &EncodingConfig,
    ) -> Result<Option<BlobId>> {
        match self {
            FileOrBlobIdOrObjectId {
                blob_id: Some(blob_id),
                ..
            } => Ok(Some(*blob_id)),
            FileOrBlobIdOrObjectId {
                file: Some(file), ..
            } => {
                tracing::debug!(
                    file = %file.display(),
                    "checking status of blob read from the filesystem"
                );
                Ok(Some(
                    *encoding_config
                        .get_blob_encoder(&read_blob_from_file(file)?)?
                        .compute_metadata()
                        .blob_id(),
                ))
            }
            // This case is required for JSON mode where we don't have the clap checking, or when
            // an object ID is provided directly.
            _ => Ok(None),
        }
    }

    // Checks that the file, blob ID, and object ID are mutually exclusive.
    pub(crate) fn exactly_one_is_some(&self) -> Result<()> {
        match (
            self.file.is_some(),
            self.blob_id.is_some(),
            self.object_id.is_some(),
        ) {
            (true, false, false) | (false, true, false) | (false, false, true) => Ok(()),
            _ => Err(anyhow!(
                "exactly one of `file`, `blob-id`, or `object-id` must be specified"
            )),
        }
    }
}

mod default {
    use std::{net::SocketAddr, time::Duration};

    use walrus_core::EpochCount;

    pub(crate) fn gas_budget() -> u64 {
        500_000_000
    }

    pub(crate) fn epochs() -> EpochCount {
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

    pub(crate) fn staking_amount_frost() -> u64 {
        1_000_000_000 // 1 WAL
    }

    pub(crate) fn exchange_amount_mist() -> u64 {
        500_000_000 // 0.5 SUI
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use walrus_test_utils::{param_test, Result as TestResult};

    use super::*;

    const STORE_STR: &str = r#"{"store": {"file": "README.md"}}"#;
    const READ_STR: &str = r#"{"read": {"blobId": "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo"}}"#;
    const DAEMON_STR: &str = r#"{"daemon": {"bindAddress": "127.0.0.1:12345"}}"#;

    // Creates the fixture for the JSON command string.
    fn make_cmd_str(command: &str) -> String {
        format!(
            r#"{{
                "config": "path/to/client_config.yaml",
                "command": {}
            }}"#,
            command
        )
    }

    // Fixture for the store command.
    fn store_command() -> Commands {
        Commands::Cli(CliCommands::Store {
            file: PathBuf::from("README.md"),
            epochs: 1,
            dry_run: false,
            force: false,
            deletable: false,
        })
    }

    // Fixture for the read command.
    fn read_command() -> Commands {
        Commands::Cli(CliCommands::Read {
            blob_id: BlobId::from_str("4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo").unwrap(),
            out: None,
            rpc_arg: RpcArg { rpc_url: None },
        })
    }

    // Fixture for the daemon command.
    fn daemon_command() -> Commands {
        Commands::Daemon(DaemonCommands::Daemon {
            args: PublisherArgs {
                daemon_args: DaemonArgs {
                    bind_address: SocketAddr::from_str("127.0.0.1:12345").unwrap(),
                    metrics_address: default::metrics_address(),
                    blocklist: None,
                },
                max_body_size_kib: default::max_body_size_kib(),
            },
        })
    }

    param_test! {
        test_json_string_extraction -> TestResult: [
            store: (&make_cmd_str(STORE_STR), store_command()),
            read: (&make_cmd_str(READ_STR), read_command()),
            daemon: (&make_cmd_str(DAEMON_STR), daemon_command())
        ]
    }
    /// Test that the command string in JSON mode is extracted correctly.
    fn test_json_string_extraction(json: &str, command: Commands) -> TestResult {
        let mut app = App {
            config: None,
            wallet: None,
            gas_budget: 100,
            json: false,
            command: Commands::Json {
                command_string: Some(json.to_string()),
            },
        };
        app.extract_json_command()?;
        assert_eq!(app.command, command);
        Ok(())
    }
}
