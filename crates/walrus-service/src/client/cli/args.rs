// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The arguments to the Walrus client binary.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroU32},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result, anyhow};
use clap::{Args, Parser, Subcommand};
use jsonwebtoken::Algorithm;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use sui_types::base_types::{ObjectID, SuiAddress};
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
    QuiltPatchId,
    encoding::{EncodingConfig, EncodingFactory},
    ensure,
};
use walrus_sdk::config::UploadMode;
use walrus_sui::{
    client::{ExpirySelectionPolicy, ReadClient, SuiContractClient},
    types::{StorageNode, move_structs::Authorized},
    utils::SuiNetwork,
};
use walrus_utils::read_blob_from_file;

use super::{BlobIdDecimal, HumanReadableBytes, parse_blob_id, parse_quilt_patch_id};
use crate::client::{config::AuthConfig, daemon::CacheConfig};

/// The command-line arguments for the Walrus client.
#[derive(Parser, Debug, Clone, Deserialize)]
#[command(rename_all = "kebab-case")]
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
    #[arg(long, verbatim_doc_comment, global = true)]
    #[serde(
        default,
        deserialize_with = "walrus_utils::config::resolve_home_dir_option"
    )]
    pub config: Option<PathBuf>,
    /// The configuration context to use for the client, if omitted the default_context is used.
    #[arg(long, global = true)]
    #[serde(default)]
    pub context: Option<String>,
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
    #[arg(long, verbatim_doc_comment, global = true)]
    #[serde(
        default,
        deserialize_with = "walrus_utils::config::resolve_home_dir_option"
    )]
    pub wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    ///
    /// If not specified, the gas budget is estimated automatically.
    #[arg(long, global = true)]
    pub gas_budget: Option<u64>,
    /// Write output as JSON.
    ///
    /// This is always done in JSON mode.
    #[arg(long, global = true)]
    #[serde(default)]
    pub json: bool,

    /// Enable tracing output for the CLI, possible options are 'otlp' and 'file=path'.
    ///
    /// The value 'otlp' sends the traces to an OTLP trace collector. The destination can be
    /// configured using standard OTLP environment variables.
    ///
    /// The value 'file=path' writes JSON-encoded gzipped traces to the file at the specified
    /// path. These traces can later be replayed to an OTLP/HTTP collector.
    ///
    /// See <https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter> for more
    /// information on OTLP environment variables.
    #[arg(long, global = true)]
    #[serde(default)]
    pub trace_cli: Option<TraceExporter>,

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
#[command(rename_all = "kebab-case")]
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
        #[arg(verbatim_doc_comment)]
        command_string: Option<String>,
    },
    /// Generate autocompletion script
    Completion {
        /// Shell type to generate completion script for the specified shell.
        #[arg(long)]
        shell: Option<String>,
    },
    /// Commands to run the binary in CLI mode.
    #[command(flatten)]
    #[serde(untagged)]
    Cli(CliCommands),
    /// Commands to run the binary in daemon mode.
    #[command(flatten)]
    #[serde(untagged)]
    Daemon(DaemonCommands),
}

/// The CLI commands for the Walrus client.
#[serde_as]
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[command(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum CliCommands {
    /// Store a new blob into Walrus.
    ///
    /// The store operation considers the Storage and Blob objects already owned by the active
    /// wallet, trying to reuse them whenever possible to optimize the cost of storing the blob.
    ///
    /// First, the store operation checks the status of the blob ID on Walrus. If the blob is
    /// already certified for the requested number of epochs, and the command is not run with the
    /// `--force` flag, the store operation stops. Otherwise, the operation proceeds as follows:
    ///
    /// (i) check if the blob is registered (but not certified) in the current wallet for a
    /// sufficient duration, and if so reuse the registration to store the blob; (ii) otherwise,
    /// check if there is an appropriate storage resource (with sufficient space and for a
    /// sufficient duration) that can be used to register the blob; or (iii) if the above fails, it
    /// purchase a new storage resource and register the blob.
    ///
    /// If the `--force` flag is used, this operation always creates a new certification for the
    /// blob (possibly reusing storage resources or uncertified but registered blobs).
    ///
    /// The `--ignore-resources` flag can be used to ignore the storage resources owned by the
    /// wallet and always purchase new storage resources, bypassing the need to check the owned
    /// resources on chain. Using this flag could speed up the store operation in case there are
    /// thousands of resources or blobs owned by the wallet.
    #[command(alias("write"))]
    Store {
        /// The files containing the blobs to be published to Walrus.
        #[arg(required = true, value_name = "FILES")]
        #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir_vec")]
        files: Vec<PathBuf>,
        /// Common options shared between store and store-quilt commands.
        #[command(flatten)]
        #[serde(flatten)]
        common_options: CommonStoreOptions,
    },
    /// Store files as a quilt.
    #[command(alias("write-quilt"))]
    StoreQuilt {
        /// Paths to files to include in the quilt.
        ///
        /// If a path is a directory, all the files in the directory will be included
        /// in the quilt, recursively.
        /// If a path is a file, the file will be included in the quilt.
        /// The filenames are used as the identifiers of the quilt patches.
        /// Note duplicate filenames are not allowed.
        /// Custom identifiers and tags are NOT supported for quilt patches.
        /// Use `--blobs` to specify custom identifiers and tags.
        #[arg(long, num_args = 0..)]
        #[serde(
            default,
            deserialize_with = "walrus_utils::config::resolve_home_dir_vec"
        )]
        paths: Vec<PathBuf>,
        /// Blobs to include in the quilt, each blob is specified as a JSON string.
        ///
        /// Example:
        ///   walrus store-quilt --epochs 10
        ///     --blobs '{"path":"/path/to/food-locations.pdf","identifier":"paper-v2",\
        ///     "tags":{"author":"Walrus","project":"food","status":"final-review"}}' \
        ///     '{"path":"/path/to/water-locations.pdf","identifier":"water-v3",\
        ///     "tags":{"author":"Walrus","project":"water","status":"draft"}}'
        /// Note if identifier is not specified, the filename will be used as the identifier,
        /// and duplicate identifiers are not allowed.
        #[arg(long, num_args = 0.., conflicts_with = "paths")]
        #[serde(default)]
        blobs: Vec<QuiltBlobInput>,
        /// Common options shared between store and store-quilt commands.
        #[command(flatten)]
        #[serde(flatten)]
        common_options: CommonStoreOptions,
    },
    /// Read a blob from Walrus, given the blob ID.
    Read {
        /// The blob ID to be read.
        #[serde_as(as = "DisplayFromStr")]
        #[arg(allow_hyphen_values = true, value_parser = parse_blob_id)]
        blob_id: BlobId,
        /// The file path where to write the blob.
        ///
        /// If unset, prints the blob to stdout.
        #[arg(long)]
        #[serde(
            default,
            deserialize_with = "walrus_utils::config::resolve_home_dir_option"
        )]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// Whether to perform a strict consistency check.
        ///
        /// This was the default before `v1.37`. In `v1.37`, the default consistency was changed to
        /// a more performant one, which is sufficient for the majority of cases. This flag can be
        /// used to enable the previous strict consistency check. See
        /// <https://docs.wal.app/design/encoding.html#data-integrity-and-consistency> for more
        /// details.
        #[arg(long)]
        #[serde(default)]
        strict_consistency_check: bool,
        /// Whether to skip consistency checks entirely.
        ///
        /// When enabled, this flag bypasses all consistency verification during blob reading. This
        /// should be used only when the writer of the blob is trusted. Does *not* affect any
        /// authentication checks for data received from storage nodes, which are always performed.
        #[arg(long, conflicts_with = "strict_consistency_check")]
        #[serde(default)]
        skip_consistency_check: bool,
    },
    /// Read quilt patches (blobs) from Walrus.
    #[command(override_usage = "walrus read-quilt ARGUMENTS [OPTIONS]")]
    ReadQuilt {
        /// The quilt patch query.
        #[command(flatten)]
        #[serde(flatten)]
        quilt_patch_query: QuiltPatchQuery,
        /// The directory path where to write the quilt patches.
        /// The blobs are written to the directory with the same name as the identifier.
        /// The user-defined metadata of the quilt patches, including identifiers and tags are
        /// printed to the stdout.
        ///
        /// If unset, prints the quilt patches raw data to stdout, while the metadata of the quilt
        /// patches are ignored.
        //
        // TODO(WAL-900): Provide more flexible options to specify the file names.
        #[arg(long)]
        #[serde(
            default,
            deserialize_with = "walrus_utils::config::resolve_home_dir_option"
        )]
        out: Option<PathBuf>,
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// List the blobs in a quilt.
    #[command(alias("resolve-quilt"))]
    ListPatchesInQuilt {
        /// The quilt ID to be inspected.
        #[serde_as(as = "DisplayFromStr")]
        #[arg(allow_hyphen_values = true, value_parser = parse_blob_id)]
        quilt_id: BlobId,
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
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
        #[command(flatten)]
        #[serde(flatten)]
        file_or_blob_id: FileOrBlobId,
        /// Timeout for status requests to storage nodes.
        #[arg(long, value_parser = humantime::parse_duration, default_value = "1s")]
        #[serde(default = "default::status_timeout")]
        timeout: Duration,
        /// The encoding type to use for encoding the file.
        #[arg(long, hide = true)]
        #[serde(default)]
        encoding_type: Option<EncodingType>,
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
    },
    /// Print information about the Walrus storage system this client is connected to.
    /// Several subcommands are available to print different information.
    ///
    /// The `all` subcommand prints all information.
    /// The `epoch` subcommand prints information about the current epoch.
    /// The `storage` subcommand prints information about the storage nodes.
    /// The `size` subcommand prints information about the size of the storage system.
    /// The `price` subcommand prints information about the price of the storage system.
    /// The `bft` subcommand prints information about the byzantine fault tolerance (BFT) system.
    /// The `committee` subcommand prints information about the committee.
    /// When no subcommand is provided, epoch, storage, size, and price information is printed.
    Info {
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// The specific info command to run.
        #[command(subcommand)]
        command: Option<InfoCommands>,
    },
    /// Print health information for one or multiple storage nodes.
    ///
    /// Only one of `--node_ids`, `--node_urls`, `--committee`, and `--active_set` can be specified.
    Health {
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// The node selector for the storage node to print health information for.
        #[command(flatten)]
        #[serde(flatten)]
        node_selection: NodeSelection,
        /// Print detailed health information.
        #[arg(long)]
        #[serde(default)]
        detail: bool,
        /// Sort configuration
        #[command(flatten)]
        #[serde(flatten)]
        sort: SortBy<HealthSortBy>,
        /// Number of concurrent requests to send to the storage nodes.
        #[arg(long, default_value_t = default::concurrent_requests_for_health())]
        #[serde(default = "default::concurrent_requests_for_health")]
        concurrent_requests: usize,
    },
    /// Encode the specified file to obtain its blob ID.
    BlobId {
        /// The file containing the blob for which to compute the blob ID.
        #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir")]
        file: PathBuf,
        /// The number of shards for which to compute the blob ID.
        ///
        /// If not specified, the number of shards is read from chain.
        #[arg(long)]
        #[serde(default)]
        n_shards: Option<NonZeroU16>,
        /// The URL of the Sui RPC node to use.
        #[command(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// The encoding type to use for computing the blob ID.
        #[arg(long, hide = true)]
        #[serde(default)]
        encoding_type: Option<EncodingType>,
    },
    /// Convert a decimal value to the Walrus blob ID (using URL-safe base64 encoding).
    ConvertBlobId {
        /// The decimal value to be converted to the Walrus blob ID.
        #[serde_as(as = "DisplayFromStr")]
        blob_id_decimal: BlobIdDecimal,
    },
    /// List all registered blobs for the current wallet.
    ListBlobs {
        #[arg(long)]
        #[serde(default)]
        /// The output list of blobs will include expired blobs.
        include_expired: bool,
    },
    /// Delete a blob from Walrus.
    ///
    /// This command is only available for blobs that are deletable.
    Delete {
        /// The filename(s), or the blob ID(s), or the object ID(s) of the blob(s) to delete.
        #[command(flatten)]
        #[serde(flatten)]
        target: BlobIdentifiers,
        /// Proceed to delete the blob without confirmation.
        #[arg(long)]
        #[serde(default)]
        yes: bool,
        /// Disable checking the status of the blob after deletion.
        ///
        /// Checking the status adds delay and requires additional requests.
        #[arg(long)]
        #[serde(default)]
        no_status_check: bool,
        /// The encoding type to use for computing the blob ID.
        ///
        /// This is only used when running the command with the `--file` target.
        #[arg(long, hide = true)]
        #[serde(default)]
        encoding_type: Option<EncodingType>,
    },
    /// Stake with storage node.
    Stake {
        /// The object ID of the storage node to stake with.
        #[arg(long, required=true, num_args=1.., alias("node-id"))]
        node_ids: Vec<ObjectID>,
        /// The amount of FROST (smallest unit of WAL token) to stake with the storage node.
        ///
        /// If this is a single value, this amount is staked at all nodes. Otherwise, the number of
        /// values must be equal to the number of node IDs, and each amount is staked at the node
        /// with the same index.
        #[arg(long, alias("amount"), num_args=1.., default_value = "1000000000")]
        #[serde(default = "default::staking_amounts_frost")]
        amounts: Vec<u64>,
    },
    /// Generates a new Sui wallet.
    GenerateSuiWallet {
        /// The path where the wallet configuration will be stored.
        ///
        /// If not specified, the command will try to create the wallet configuration at the default
        /// location `$HOME/.sui/sui_config/`. If the directory already exists, an error will be
        /// returned specifying to use the Sui CLI to manage the existing wallets.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Sui network for which the wallet is generated.
        ///
        /// Available options are `devnet`, `testnet`, `mainnet`, and `localnet`.
        #[arg(long, default_value_t = default::sui_network())]
        #[serde(default = "default::sui_network")]
        sui_network: SuiNetwork,
        /// Whether to attempt to get SUI tokens from the faucet.
        #[arg(long)]
        #[serde(default)]
        use_faucet: bool,
        /// Timeout for the faucet call.
        #[arg(
            long,
            value_parser = humantime::parse_duration,
            default_value = "1min",
            requires = "use_faucet")
        ]
        #[serde(default = "default::faucet_timeout")]
        faucet_timeout: Duration,
    },
    /// Exchange SUI for WAL through the configured exchange. This command is only available on
    /// Testnet.
    GetWal {
        #[arg(long)]
        /// The object ID of the exchange to use.
        ///
        /// This takes precedence over any values set in the configuration file.
        exchange_id: Option<ObjectID>,
        #[arg(long, default_value_t = default::exchange_amount_mist())]
        #[serde(default = "default::exchange_amount_mist")]
        /// The amount of MIST to exchange for WAL/FROST.
        amount: u64,
    },
    /// Burns one or more owned Blob object on Sui.
    ///
    /// This command burns the Blob objects with the given object IDs. The Blob objects must be
    /// owned by the wallet currently in use. Using the flag `--all` will burn all the Blob objects
    /// owned by the wallet. Using the flag `--all-expired` burns all expired Blob objects owned by
    /// the wallet.
    ///
    /// This operation simply removes the Blob objects from Sui, _without_ deleting the data from
    /// Walrus and _without_ refunding the storage. Running this command will result in the loss of
    /// control over the Blob objects and the data they represent.
    ///
    /// Importantly, after burning: (i) Permanent blobs cannot be extended; (ii) deletable blobs
    /// cannot be extended nor deleted.
    BurnBlobs {
        /// The object IDs of the Blob objects to burn.
        #[command(flatten)]
        #[serde(flatten)]
        burn_selection: BurnSelection,
        /// Proceed to burn the blobs without confirmation.
        #[arg(long)]
        #[serde(default)]
        yes: bool,
    },
    /// Fund a shared blob.
    FundSharedBlob {
        /// The object ID of the shared blob to fund.
        #[arg(long)]
        shared_blob_obj_id: ObjectID,
        /// The amount of FROST (smallest unit of WAL token) to fund the shared blob with.
        #[arg(long)]
        amount: u64,
    },
    /// Extend an owned or shared blob.
    Extend {
        /// The object ID of the blob to extend.
        #[arg(long)]
        blob_obj_id: ObjectID,
        /// If the blob_obj_id refers to a shared blob object, this flag must be present.
        #[arg(long)]
        shared: bool,
        /// The number of epochs to extend the blob for.
        // TODO (WAL-614): Offer multiple options similar to the `store` command:
        // `--extended-epochs`, `--epochs-ahead`, `--max`, `--end-epoch`,
        // `--earliest-expiration-time`.
        #[arg(long)]
        epochs_extended: EpochCount,
    },
    /// Share a blob.
    Share {
        /// The object ID of the (owned) blob to share.
        #[arg(long)]
        blob_obj_id: ObjectID,
        /// If specified, share and directly fund the blob.
        #[arg(long)]
        amount: Option<u64>,
    },
    /// Get the attribute of a blob.
    ///
    /// This command will return all the attribute fields of a blob, but not the blob data.
    GetBlobAttribute {
        /// The object ID of the blob to get the attribute of.
        #[arg(index = 1)]
        blob_obj_id: ObjectID,
    },
    /// Set the attribute of a blob.
    SetBlobAttribute {
        /// The object ID of the blob to set the attribute of.
        #[arg(index = 1)]
        blob_obj_id: ObjectID,
        /// The key-value pairs to set as attributes.
        /// Multiple pairs can be specified by repeating the flag.
        /// Example:
        ///   --attr "key1" "value1" --attr "key2" "value2"
        #[arg(
            long = "attr",
            value_names = &["KEY", "VALUE"],
            num_args = 2,
            action = clap::ArgAction::Append,
        )]
        attributes: Vec<String>,
    },
    /// Remove a key-value pair from a blob's attribute.
    RemoveBlobAttributeFields {
        /// The object ID of the blob.
        #[arg(index = 1)]
        blob_obj_id: ObjectID,
        /// The keys to remove from the blob's attribute.
        /// Multiple keys should be provided as separate arguments.
        /// Examples:
        ///   --keys "key1" "key2,with,commas" "key3 with spaces"
        #[arg(long, num_args = 1..,)]
        keys: Vec<String>,
    },
    /// Remove the attribute dynamic field from a blob.
    RemoveBlobAttribute {
        /// The object ID of the blob.
        #[arg(index = 1)]
        blob_obj_id: ObjectID,
    },
    /// Administration subcommands for storage node operators.
    NodeAdmin {
        /// The specific node admin command to run.
        #[command(subcommand)]
        command: NodeAdminCommands,
    },
    /// Pull all blobs (filtered by optional prefix specifier) from Google Cloud Storage down into
    /// the specified backfill_dir.
    PullArchiveBlobs {
        /// The Google Cloud Storage bucket to pull from.
        #[arg(long)]
        gcs_bucket: String,
        /// Optional object name prefix filter.
        #[arg(long)]
        prefix: Option<String>,
        /// The directory to pull into.
        #[arg(long)]
        backfill_dir: String,
        /// Durable list of objects already pulled.
        ///
        /// This is loaded at script initialization and used to avoid pulling redundant objects.
        /// Note that looking in the backfill_dir is not a complete solution because the disk space
        /// needs to be reclaimed after the archival blobs have been pushed up to the network. So,
        /// this `pulled_state` file is ensuring we don't have to start from the beginning in the
        /// event that we need to stop and restart the backfill.
        #[arg(long)]
        pulled_state: PathBuf,
    },
    /// Upload blob slivers and metadata from a specified directory to the listed storage nodes.
    BlobBackfill {
        /// The subdirectory when blob-backfill can find blobs.
        ///
        /// Blobs in this directory must be named with their blob id. Any files that exist in this
        /// directory that do not have a conforming blob id name will be skipped.
        #[arg(long)]
        backfill_dir: PathBuf,
        /// The file where successfully pushed blob IDs will be stored.
        #[arg(long)]
        pushed_state: PathBuf,
        /// The nodes to backfill with slivers and blob metadata.
        node_ids: Vec<ObjectID>,
    },
}

impl CliCommands {
    /// Returns a string representation of the command's name.
    pub fn as_str(&self) -> &'static str {
        match self {
            CliCommands::Store { .. } => "store",
            CliCommands::StoreQuilt { .. } => "store-quilt",
            CliCommands::Read { .. } => "read",
            CliCommands::ReadQuilt { .. } => "read-quilt",
            CliCommands::ListPatchesInQuilt { .. } => "list-patches-in-quilt",
            CliCommands::BlobStatus { .. } => "blob-status",
            CliCommands::Info { .. } => "info",
            CliCommands::Health { .. } => "health",
            CliCommands::BlobId { .. } => "blob-id",
            CliCommands::ConvertBlobId { .. } => "convert-blob-id",
            CliCommands::ListBlobs { .. } => "list-blobs",
            CliCommands::Delete { .. } => "delete",
            CliCommands::Stake { .. } => "stake",
            CliCommands::GenerateSuiWallet { .. } => "generate-sui-wallet",
            CliCommands::GetWal { .. } => "get-wal",
            CliCommands::BurnBlobs { .. } => "burn-blobs",
            CliCommands::FundSharedBlob { .. } => "fund-shared-blob",
            CliCommands::Extend { .. } => "extend",
            CliCommands::Share { .. } => "share",
            CliCommands::GetBlobAttribute { .. } => "get-blob-attribute",
            CliCommands::SetBlobAttribute { .. } => "set-blob-attribute",
            CliCommands::RemoveBlobAttributeFields { .. } => "remove-blob-attribute-fields",
            CliCommands::RemoveBlobAttribute { .. } => "remove-blob-attribute",
            CliCommands::NodeAdmin { .. } => "node-admin",
            CliCommands::PullArchiveBlobs { .. } => "pull-archive-blobs",
            CliCommands::BlobBackfill { .. } => "blob-backfill",
        }
    }
}

/// Subcommands for the `info` command.
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[command(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum InfoCommands {
    /// Print all information listed below.
    All {
        /// Sort configuration for committee information
        #[command(flatten)]
        #[serde(flatten)]
        sort: SortBy<NodeSortBy>,
    },
    /// Print epoch information.
    Epoch,
    /// Print storage information.
    Storage,
    /// Print size information.
    Size,
    /// Print price information.
    Price,
    /// Print byzantine fault tolerance (BFT) information.
    Bft,
    /// Print committee information.
    Committee {
        /// Sort configuration
        #[command(flatten)]
        #[serde(flatten)]
        sort: SortBy<NodeSortBy>,
    },
}

/// Subcommands for the `node-admin` command.
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[command(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum NodeAdminCommands {
    /// Collect the commission.
    CollectCommission {
        #[arg(long)]
        /// The ID of the node for which the operation should be performed.
        node_id: ObjectID,
    },
    /// Vote for a contract upgrade.
    VoteForUpgrade {
        #[arg(long)]
        /// The ID of the node for which the operation should be performed.
        node_id: ObjectID,
        /// The upgrade manager object ID.
        #[arg(long)]
        upgrade_manager_object_id: ObjectID,
        /// The path to the walrus package directory.
        #[arg(long)]
        package_path: PathBuf,
    },
    /// Set the authorized entity for governance operations.
    SetGovernanceAuthorized {
        #[arg(long)]
        /// The ID of the node for which the operation should be performed.
        node_id: ObjectID,
        #[command(flatten)]
        /// The object or address to set as authorized entity.
        object_or_address: ObjectOrAddress,
    },
    /// Set the authorized entity for commission operations.
    SetCommissionAuthorized {
        #[arg(long)]
        /// The ID of the node for which the operation should be performed.
        node_id: ObjectID,
        #[command(flatten)]
        /// The object or address to set as authorized entity.
        object_or_address: ObjectOrAddress,
    },
    /// Outputs the package digest of a sui package.
    PackageDigest {
        /// The path to the package directory.
        #[arg(long)]
        package_path: PathBuf,
    },
}

#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[group(required = true, multiple = false)]
pub struct ObjectOrAddress {
    #[arg(long, global = true)]
    /// Set an address as authorized entity.
    address: Option<SuiAddress>,
    #[arg(long, global = true)]
    /// Set an object as capability to authorize operations.
    object: Option<ObjectID>,
}

impl TryFrom<ObjectOrAddress> for Authorized {
    type Error = anyhow::Error;

    fn try_from(value: ObjectOrAddress) -> std::result::Result<Self, Self::Error> {
        match value {
            ObjectOrAddress {
                address: Some(address),
                object: None,
            } => Ok(Authorized::Address(address)),
            ObjectOrAddress {
                address: None,
                object: Some(object),
            } => Ok(Authorized::Object(object)),
            _ => Err(anyhow!("exactly one of `address` or `object` must be set")),
        }
    }
}

/// The daemon commands for the Walrus client.
#[serde_as]
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[command(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DaemonCommands {
    /// Run a publisher service at the provided network address.
    ///
    /// This does not perform any type of access control and is thus not suited for a public
    /// deployment when real money is involved.
    Publisher {
        #[command(flatten)]
        #[serde(flatten)]
        /// The publisher args.
        args: PublisherArgs,
    },
    /// Run an aggregator service at the provided network address.
    Aggregator {
        #[command(flatten)]
        #[serde(flatten)]
        /// The URL of the Sui RPC node to use.
        rpc_arg: RpcArg,
        #[command(flatten)]
        #[serde(flatten)]
        /// The daemon args.
        daemon_args: DaemonArgs,
        #[command(flatten)]
        #[serde(flatten, default)]
        /// The aggregator args.
        aggregator_args: AggregatorArgs,
    },
    /// Run a client daemon at the provided network address, combining the functionality of an
    /// aggregator and a publisher.
    Daemon {
        #[command(flatten)]
        #[serde(flatten)]
        /// The publisher args.
        args: PublisherArgs,
        #[command(flatten)]
        #[serde(flatten, default)]
        /// The aggregator args.
        aggregator_args: AggregatorArgs,
    },
}

impl DaemonCommands {
    /// Gets the metrics address from the commands that support it.
    pub fn get_metrics_address(&self) -> SocketAddr {
        match &self {
            DaemonCommands::Publisher { args } => args.daemon_args.metrics_address,
            DaemonCommands::Aggregator { daemon_args, .. } => daemon_args.metrics_address,
            DaemonCommands::Daemon { args, .. } => args.daemon_args.metrics_address,
        }
    }
}

/// The arguments for the aggregator service.
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Default, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AggregatorArgs {
    /// Allowed headers for the daemon.
    ///
    /// This defines the allow-list of headers. It is currently used for the
    /// /v1/blobs/by-object-id/{blob_object_id} aggregator endpoint. The response will include the
    /// allowed headers if the specified header names are present in the BlobAttribute associated
    /// with the requested blob.
    #[arg(long, num_args = 1.., default_values_t = default::allowed_headers())]
    #[serde(default = "default::allowed_headers")]
    pub(crate) allowed_headers: Vec<String>,
    /// Whether to allow quilt patch tags to be returned in the response headers.
    /// If true, the tags will be returned in the response headers, regardless of the allowed
    /// headers.
    #[arg(long, default_value_t = false)]
    #[serde(default)]
    pub allow_quilt_patch_tags_in_response: bool,
    /// The maximum blob size in bytes.
    #[arg(long)]
    #[serde(default)]
    pub max_blob_size: Option<u64>,
    /// The maximum number of requests that can be buffered before the server starts rejecting new
    /// ones. Note that this includes the number of requests that are being processed currently.
    #[arg(
        long = "aggregator-max-buffer-size",
        default_value_t = default::max_aggregator_buffer_size()
    )]
    #[serde(default = "default::max_aggregator_buffer_size")]
    pub max_request_buffer_size: usize,
    /// The maximum number of requests the aggregator can process concurrently.
    ///
    /// If more requests than this maximum are received, the excess requests are buffered up to
    /// `--max-buffer-size`. Any outstanding request will result in a response with a
    /// 429 HTTP status code.
    #[arg(
        long = "aggregator-max-concurrent-requests",
        default_value_t = default::max_aggregator_concurrent_requests()
    )]
    #[serde(default = "default::max_aggregator_concurrent_requests")]
    pub max_concurrent_requests: usize,
}

/// The arguments for the publisher service.
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PublisherArgs {
    /// The configuration for the daemon.
    #[command(flatten)]
    #[serde(flatten)]
    pub daemon_args: DaemonArgs,
    /// The maximum body size of PUT requests in KiB.
    #[arg(long = "max-body-size", default_value_t = default::max_body_size_kib())]
    #[serde(default = "default::max_body_size_kib")]
    pub max_body_size_kib: usize,
    /// The maximum body size of quilt PUT requests in KiB.
    #[arg(long = "max-quilt-body-size", default_value_t = default::max_quilt_body_size_kib())]
    #[serde(default = "default::max_quilt_body_size_kib")]
    pub max_quilt_body_size_kib: usize,
    /// The maximum number of requests that can be buffered before the server starts rejecting new
    /// ones.
    #[arg(long = "max-buffer-size", default_value_t = default::max_request_buffer_size())]
    #[serde(default = "default::max_request_buffer_size")]
    pub max_request_buffer_size: usize,
    /// The maximum number of requests the publisher can process concurrently.
    ///
    /// If more requests than this maximum are received, the excess requests are buffered up to
    /// `--max-buffer-size`. Any outstanding request will result in a response with a 429 HTTP
    /// status code.
    #[arg(long, default_value_t = default::max_concurrent_requests())]
    #[serde(default = "default::max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// The number of clients to use for the publisher.
    ///
    /// The publisher uses this number of clients to publish blobs concurrently.
    #[arg(long, default_value_t = default::n_publisher_clients())]
    #[serde(default = "default::n_publisher_clients")]
    pub n_clients: usize,
    /// The interval of time between refilling the publisher's sub-clients' wallets.
    #[arg(long, value_parser = humantime::parse_duration, default_value="1s")]
    #[serde(default = "default::refill_interval")]
    pub refill_interval: Duration,
    /// The directory where the publisher will store the sub-wallets used for client multiplexing.
    #[arg(long)]
    #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir")]
    pub sub_wallets_dir: PathBuf,
    /// The amount of MIST transferred at every refill.
    #[arg(long, default_value_t = default::gas_refill_amount())]
    #[serde(default = "default::gas_refill_amount")]
    pub gas_refill_amount: u64,
    /// The amount of FROST transferred at every refill.
    #[arg(long, default_value_t = default::wal_refill_amount())]
    #[serde(default = "default::wal_refill_amount")]
    pub wal_refill_amount: u64,
    /// The minimum balance the sub-wallets should have.
    ///
    /// Below this threshold, the sub-wallets are refilled.
    #[arg(long, default_value_t = default::sub_wallets_min_balance())]
    #[serde(default = "default::sub_wallets_min_balance")]
    pub sub_wallets_min_balance: u64,
    /// Deprecated flag for backwards compatibility.
    ///
    /// By default, the publisher already keeps created Blob objects in its main wallet. This flag
    /// is only available for backwards-compatibility reasons. Therefore _specifying this flag has
    /// no effect_.
    ///
    /// To burn the created Blob objects immediately after storing, use the `--burn-after-store`
    /// flag.
    #[arg(long)]
    #[serde(default)]
    pub keep: bool,
    /// If set, the publisher will burn the created Blob objects immediately.
    ///
    /// If unset, the publisher will keep all created blob objects in its _main wallet_ by default.
    /// However, note that this flag _does not affect_ the use of the `send_object_to` query
    /// parameter: Regardless of this flag's status, the publisher will send created objects to the
    /// address in the `send_object_to` query parameter, if it is specified in the PUT request.
    #[arg(long)]
    #[serde(default)]
    pub burn_after_store: bool,
    /// If set, the publisher will verify the JWT token.
    ///
    /// If not specified, the verification is disabled.
    /// This is useful, e.g., in case the API Gateway has already checked the token.
    /// The secret can be hex string, starting with `0x`.
    ///
    /// JWT tokens are expected to have the `jti` (JWT ID) set in the claim to a unique value.
    /// The JWT creator must ensure that this value is unique among all requests to the publisher.
    /// We recommend using large nonces to avoid collisions.
    #[arg(long)]
    #[serde(default)]
    pub jwt_decode_secret: Option<String>,
    /// If unset, the JWT authentication algorithm will be HMAC (HS256).
    ///
    /// The following algorithms are supported: "HS256", "HS384", "HS512", "ES256", "ES384",
    /// "RS256", "RS384", "PS256", "PS384", "PS512", "RS512", "EdDSA".
    #[arg(long)]
    #[serde(default)]
    pub jwt_algorithm: Option<Algorithm>,
    /// If set and greater than 0, the publisher will check if the JWT token is expired based on
    /// the "issued at" (`iat`) value.
    #[arg(long, default_value_t = 0)]
    #[serde(default)]
    pub jwt_expiring_sec: i64,
    /// If set, the publisher will verify that the requested upload matches the claims in the JWT.
    ///
    /// Specifically, the publisher will:
    /// - Verify that the number of `epochs` in query is the the same as `epochs` in the JWT, if
    ///   present;
    /// - Verify that the `send_object_to` field in the query is the same as the `send_object_to`
    ///   in the JWT, if present;
    /// - Verify the size of uploaded file;
    /// - Verify the uniqueness of the `jti` claim.
    #[arg(long)]
    #[serde(default)]
    pub jwt_verify_upload: bool,
    #[command(flatten)]
    #[serde(flatten)]
    /// The configuration for the JWT duplicate suppression cache.
    pub replay_suppression_config: CacheConfig,
}

impl PublisherArgs {
    pub(crate) fn max_body_size(&self) -> usize {
        self.max_body_size_kib << 10
    }

    pub(crate) fn max_quilt_body_size(&self) -> usize {
        self.max_quilt_body_size_kib << 10
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

    pub(crate) fn generate_auth_config(&self) -> Result<Option<AuthConfig>> {
        if self.jwt_decode_secret.is_some() || self.jwt_expiring_sec > 0 || self.jwt_verify_upload {
            let mut auth_config = AuthConfig {
                expiring_sec: self.jwt_expiring_sec,
                verify_upload: self.jwt_verify_upload,
                algorithm: self.jwt_algorithm,
                replay_suppression_config: self.replay_suppression_config.clone(),
                ..Default::default()
            };

            if let Some(secret) = self.jwt_decode_secret.as_ref() {
                auth_config.with_key_from_str(secret)?;
            }

            tracing::info!(config=?auth_config, "authentication config applied");
            Ok(Some(auth_config))
        } else {
            tracing::info!("auth disabled");
            Ok(None)
        }
    }
}

/// The URL of the Sui RPC node to use.
#[derive(Default, Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcArg {
    /// The URL of the Sui RPC node to use.
    ///
    /// If not provided, the RPC URL is set based on the `rpc_url` field in the client configuration
    /// (if set), or the wallet configuration.
    // NB: Keep this in sync with `crate::cli`.
    #[arg(long)]
    #[serde(default)]
    pub(crate) rpc_url: Option<String>,
}

#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonArgs {
    /// The address to which to bind the service.
    #[arg(long, default_value_t = default::bind_address())]
    #[serde(default = "default::bind_address")]
    pub(crate) bind_address: SocketAddr,
    /// Socket address on which the Prometheus server should export its metrics.
    #[arg(short = 'a', long, default_value_t = default::metrics_address())]
    #[serde(default = "default::metrics_address")]
    pub(crate) metrics_address: SocketAddr,
    /// Path to a blocklist file containing a list (in YAML syntax) of blocked blob IDs.
    #[arg(long)]
    #[serde(
        default,
        deserialize_with = "walrus_utils::config::resolve_home_dir_option"
    )]
    pub(crate) blocklist: Option<PathBuf>,
}

/// Common options shared between store and store-quilt commands.
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CommonStoreOptions {
    /// The epoch argument to specify either the number of epochs to store the blob, or the
    /// end epoch, or the earliest expiry time in rfc3339 format.
    #[command(flatten)]
    #[serde(flatten)]
    pub epoch_arg: EpochArg,
    /// Perform a dry-run of the store without performing any actions on chain.
    ///
    /// This assumes `--force`; i.e., it does not check the current status of the blob/quilt.
    #[arg(long)]
    #[serde(default)]
    pub dry_run: bool,
    /// Do not check for the blob/quilt status before storing it.
    ///
    /// This will create a new blob/quilt even if it is already certified for a sufficient
    /// duration.
    #[arg(long)]
    #[serde(default)]
    pub force: bool,
    /// Ignore the storage resources owned by the wallet.
    ///
    /// The client will not check if it can reuse existing resources, and just check the blob/quilt
    /// status on chain.
    #[arg(long)]
    #[serde(default)]
    pub ignore_resources: bool,
    /// Mark the blob/quilt as deletable. Conflicts with `--permanent`.
    ///
    /// Deletable blobs/quilts can be removed from Walrus before their expiration time.
    ///
    /// New blobs are created as deletable by default since v1.33, and this flag is no longer
    /// required.
    #[arg(long, conflicts_with = "permanent", hide = true)]
    #[serde(default)]
    pub deletable: bool,
    /// Mark the blob/quilt as permanent.
    ///
    /// Permanent blobs/quilts *cannot* be removed from Walrus before their expiration time. This is
    /// beneficial if strong availability guarantees are required.
    ///
    /// This was the default behavior before v1.33, but *blobs are now deletable by default*.
    #[arg(long)]
    #[serde(default)]
    pub permanent: bool,
    /// Whether to put the blob/quilt into a shared object.
    #[arg(long)]
    #[serde(default)]
    pub share: bool,
    /// The encoding type to use for encoding the files.
    #[arg(long, hide = true)]
    #[serde(default)]
    pub encoding_type: Option<EncodingType>,
    /// Walrus Upload Relay URL to use for storing the blob/quilt.
    #[arg(long)]
    #[serde(default)]
    pub upload_relay: Option<Url>,
    /// Skip the tip confirmation prompt when using the upload relay.
    ///
    /// If specified, the client will not ask for a tip confirmation and proceed with the upload
    /// relay store operation.
    #[arg(long, requires = "upload_relay")]
    #[serde(default)]
    pub skip_tip_confirmation: bool,
    /// Preset upload mode to tune network concurrency and bytes-in-flight.
    #[arg(long, value_enum)]
    #[serde(default)]
    pub upload_mode: Option<UploadModeCli>,
    /// Spawn a helper process that continues detached tail uploads after quorum is reached.
    /// This is only effective when tail handling is configured as `detached`.
    #[arg(long)]
    #[serde(default)]
    pub child_process_uploads: bool,
    /// Internal flag to signal the process is running as a child for background uploads.
    #[arg(long, hide = true)]
    #[serde(default)]
    pub internal_run: bool,
}

#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct FileOrBlobId {
    /// The file containing the blob to be checked.
    #[arg(long)]
    #[serde(default)]
    pub(crate) file: Option<PathBuf>,
    /// The blob ID to be checked.
    #[arg(long, allow_hyphen_values = true, value_parser = parse_blob_id)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) blob_id: Option<BlobId>,
}

/// CLI enum for selecting upload presets. Converted to SDK UploadMode at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum UploadModeCli {
    Conservative,
    Balanced,
    Aggressive,
}

impl Default for UploadModeCli {
    fn default() -> Self {
        Self::Balanced
    }
}

impl From<UploadModeCli> for UploadMode {
    fn from(value: UploadModeCli) -> Self {
        match value {
            UploadModeCli::Conservative => UploadMode::Conservative,
            UploadModeCli::Balanced => UploadMode::Balanced,
            UploadModeCli::Aggressive => UploadMode::Aggressive,
        }
    }
}

impl FileOrBlobId {
    pub(crate) fn get_or_compute_blob_id(
        self,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> Result<BlobId> {
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
                    .get_for_type(encoding_type)
                    .compute_metadata(&read_blob_from_file(&file)?)?
                    .blob_id())
            }
            // This case is required for JSON mode where we don't have the clap checking.
            _ => Err(anyhow!("either the file or blob ID must be defined")),
        }
    }
}

/// Represents a blob to be stored in a quilt, together with its identifier and tags.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QuiltBlobInput {
    /// The path to the blob.
    #[serde(deserialize_with = "walrus_utils::config::resolve_home_dir")]
    pub(crate) path: PathBuf,
    /// The identifier of the blob.
    ///
    /// If not provided, the file name will be used as the identifier.
    #[serde(default)]
    pub(crate) identifier: Option<String>,
    /// The tags of the blob.
    #[serde(default)]
    pub(crate) tags: BTreeMap<String, String>,
}

impl FromStr for QuiltBlobInput {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

/// Query to read quilt patches.
#[serde_as]
#[derive(Debug, Clone, Args, Default, Serialize, Deserialize, PartialEq, Eq)]
#[group(required = true)]
#[serde(rename_all = "camelCase")]
pub struct QuiltPatchQuery {
    /// The quilt ID, which is the BlobID of the quilt.
    ///
    /// It is required unless `--quilt-patch-id` is used.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[arg(
        long,
        allow_hyphen_values = true,
        value_parser = parse_blob_id,
        conflicts_with = "quilt_patch_ids",
        required_unless_present = "quilt_patch_ids",
        help_heading = "Arguments",
    )]
    quilt_id: Option<BlobId>,

    /// The identifiers to read from the quilt.
    ///
    /// It is required to be used with `--quilt-id`.
    #[arg(
        long = "identifiers",
        alias = "identifier",
        conflicts_with = "tag",
        conflicts_with = "quilt_patch_ids",
        num_args = 1..,
        help_heading = "Arguments"
    )]
    #[serde(default)]
    identifiers: Vec<String>,

    /// The tag key and value.
    ///
    /// It is required to be used with `--quilt-id`.
    #[arg(
        long,
        conflicts_with = "quilt_patch_ids",
        value_names = &["KEY", "VALUE"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help_heading = "Arguments"
    )]
    #[serde(default)]
    tag: Vec<String>,

    /// The quilt patch IDs.
    ///
    /// It should be used alone without other arguments.
    #[serde_as(as = "Vec<DisplayFromStr>")]
    #[arg(
        long = "quilt-patch-ids",
        aliases = ["quilt-patch-id", "patch-ids"],
        num_args = 1..,
        value_terminator = "--",
        allow_hyphen_values = true,
        value_parser = parse_quilt_patch_id,
        help_heading = "Arguments",
    )]
    #[serde(default)]
    quilt_patch_ids: Vec<QuiltPatchId>,
}

impl QuiltPatchQuery {
    /// Returns a QuiltPatchSelector from the command line arguments.
    pub fn into_selector(mut self) -> Result<QuiltPatchSelector> {
        match (
            !self.identifiers.is_empty(),
            !self.tag.is_empty(),
            !self.quilt_patch_ids.is_empty(),
            self.quilt_id,
        ) {
            // quilt_id and identifiers provided.
            (true, false, false, Some(quilt_id)) => {
                Ok(QuiltPatchSelector::ByIdentifier(QuiltPatchByIdentifier {
                    quilt_id,
                    identifiers: self.identifiers,
                }))
            }

            // quilt_id and tags provided.
            (false, true, false, Some(quilt_id)) => {
                if self.tag.len() != 2 {
                    return Err(anyhow!("Only one tag is supported for now."));
                }
                let value = self.tag.pop().expect("value should be present");
                let tag = self.tag.pop().expect("tag should be present");
                Ok(QuiltPatchSelector::ByTag(QuiltPatchByTag {
                    quilt_id,
                    tag,
                    value,
                }))
            }

            // quilt_patch_ids provided.
            (false, false, true, None) => Ok(QuiltPatchSelector::ByPatchId(QuiltPatchByPatchId {
                quilt_patch_ids: self.quilt_patch_ids,
            })),

            // Only quilt_id provided.
            (false, false, false, Some(quilt_id)) => Ok(QuiltPatchSelector::All(quilt_id)),

            // All other combinations are invalid
            _ => Err(Self::invalid_query_error()),
        }
    }

    /// Returns an error message for an invalid query.
    fn invalid_query_error() -> anyhow::Error {
        anyhow!(
            "Exactly one query pattern must be specified. Valid query patterns are:\n\
            - quiltId + identifiers: {{\"quiltId\": \"<ID>\", \
            \"identifiers\": [\"<IDENTIFIER>\", ...]}}\n\
            - quiltId + tag: {{\"quiltId\": \"<ID>\", \"tag\": [\"<KEY>\", \"<VALUE>\"]}}\n\
            - quiltPatchIds: {{\"quiltPatchIds\": [\"<PATCH_ID>\", ...]}}\n\
            - quiltId only: {{\"quiltId\": \"<ID>\"}}"
        )
    }
}

/// Represents a blob.
#[serde_as]
#[derive(Debug, Clone, Args, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobIdentity {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub(crate) blob_id: Option<BlobId>,
    pub(crate) file: Option<PathBuf>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub(crate) object_id: Option<ObjectID>,
}

impl std::fmt::Display for BlobIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut identity = String::new();
        if let Some(blob_id) = &self.blob_id {
            identity = format!("blob ID: {blob_id}");
        }
        if let Some(file) = &self.file {
            if !identity.is_empty() {
                identity.push(' ');
            }
            identity.push_str(&format!("file: {}", file.display()));
        }
        if let Some(object_id) = &self.object_id {
            if !identity.is_empty() {
                identity.push(' ');
            }
            identity.push_str(&format!("object ID: {object_id}"));
        }
        write!(f, "{identity}")
    }
}

/// Query for quilt patches by identifier.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QuiltPatchByIdentifier {
    /// The quilt ID, which is the BlobID of the quilt.
    pub quilt_id: BlobId,
    /// The identifiers to read from the quilt.
    pub identifiers: Vec<String>,
}

/// Query for quilt patches by tag.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QuiltPatchByTag {
    /// The quilt ID, which is the BlobID of the quilt.
    pub quilt_id: BlobId,
    /// The tag key.
    pub tag: String,
    /// The tag value.
    pub value: String,
}

/// Query for quilt patches by patch ID.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QuiltPatchByPatchId {
    /// The quilt patch IDs.
    pub quilt_patch_ids: Vec<QuiltPatchId>,
}

/// Selector for quilt patches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuiltPatchSelector {
    /// Patches by quilt_id and identifier.
    ByIdentifier(QuiltPatchByIdentifier),
    /// Patches by quilt_id and tag.
    ByTag(QuiltPatchByTag),
    /// Patches by quilt_patch_id.
    ByPatchId(QuiltPatchByPatchId),
    /// Read 'em all.
    All(BlobId),
}

#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobIdentifiers {
    /// The file containing the blob to be deleted.
    ///
    /// This is equivalent to calling `blob-id` on the file, and then deleting with `--blob-id`.
    #[arg(long, num_args = 0.., alias = "file")]
    #[serde(default)]
    pub(crate) files: Vec<PathBuf>,
    /// The blob ID to be deleted.
    ///
    /// This command deletes _all_ owned blob objects matching the provided blob ID.
    #[arg(
        long,
        allow_hyphen_values = true,
        value_parser = parse_blob_id,
        alias = "blob-id",
        action = clap::ArgAction::Append
    )]
    #[serde_as(as = "Vec<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) blob_ids: Vec<BlobId>,
    /// The object ID of the blob object to be deleted.
    ///
    /// This command deletes only the blob object with the given object ID.
    #[arg(long, num_args = 0.., alias = "object-id")]
    #[serde_as(as = "Vec<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) object_ids: Vec<ObjectID>,
}

impl BlobIdentifiers {
    pub(crate) fn get_blob_identities(
        &self,
        encoding_config: &EncodingConfig,
        encoding_type: EncodingType,
    ) -> Result<Vec<BlobIdentity>> {
        let mut result = Vec::new();

        for file in &self.files {
            tracing::debug!(
                file = %file.display(),
                "computing blob ID for file from the filesystem"
            );
            let blob_id = *encoding_config
                .get_for_type(encoding_type)
                .compute_metadata(&read_blob_from_file(file)?)?
                .blob_id();

            result.push(BlobIdentity {
                blob_id: Some(blob_id),
                file: Some(file.clone()),
                object_id: None,
            });
        }

        for blob_id in &self.blob_ids {
            result.push(BlobIdentity {
                blob_id: Some(*blob_id),
                file: None,
                object_id: None,
            });
        }

        for object_id in &self.object_ids {
            result.push(BlobIdentity {
                blob_id: None,
                file: None,
                object_id: Some(*object_id),
            });
        }

        if result.is_empty() {
            return Err(anyhow!("no files, blob IDs, or object IDs specified"));
        }

        Ok(result)
    }
}

/// Selector for the blob object IDs to burn.
#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct BurnSelection {
    /// The object IDs of the Blob objects to burn.
    #[arg(long, num_args=1.., required=true)]
    #[serde_as(as = "Vec<DisplayFromStr>")]
    object_ids: Vec<ObjectID>,
    /// Burn all the blob objects owned by the wallet.
    #[arg(long)]
    #[serde(default)]
    all: bool,
    /// Burn all the expired blob objects owned by the wallet.
    #[arg(long)]
    #[serde(default)]
    all_expired: bool,
}

impl BurnSelection {
    pub(crate) async fn get_object_ids(
        &self,
        client: &SuiContractClient,
    ) -> anyhow::Result<Vec<ObjectID>> {
        match (self.object_ids.is_empty(), self.all, self.all_expired) {
            (false, false, false) => Ok(self.object_ids.clone()),
            (true, true, false) => Ok(client
                .owned_blobs(None, ExpirySelectionPolicy::All)
                .await?
                .into_iter()
                .map(|blob| blob.id)
                .collect::<Vec<_>>()),
            (true, false, true) => Ok(client
                .owned_blobs(None, ExpirySelectionPolicy::Expired)
                .await?
                .into_iter()
                .map(|blob| blob.id)
                .collect::<Vec<_>>()),
            _ => Err(anyhow!(
                "exactly one of `objectIds`, `all`, or `allExpired` must be specified"
            )),
        }
    }

    pub(super) fn is_all_expired(&self) -> bool {
        self.all_expired
    }
}

/// Selector for the storage nodes.
#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct NodeSelection {
    /// The IDs of the storage nodes to be selected.
    #[arg(long, alias="node-id", num_args=1..)]
    #[serde(default)]
    pub node_ids: Vec<ObjectID>,
    /// The URLs of the storage nodes to be selected.
    #[arg(long, alias="node-url", num_args=1..)]
    #[serde(default)]
    pub node_urls: Vec<String>,
    /// Select all storage nodes in the current committee.
    #[arg(long)]
    #[serde(default)]
    pub committee: bool,
    /// Select all storage nodes in the active set.
    #[arg(long)]
    #[serde(default)]
    pub active_set: bool,
}

impl NodeSelection {
    /// Checks that exactly one of the node selection options is set and returns an error otherwise.
    pub(crate) fn exactly_one_is_set(&self) -> Result<()> {
        match (
            !self.node_ids.is_empty(),
            !self.node_urls.is_empty(),
            self.committee,
            self.active_set,
        ) {
            (true, false, false, false)
            | (false, true, false, false)
            | (false, false, true, false)
            | (false, false, false, true) => Ok(()),
            _ => Err(anyhow!(
                "exactly one of `nodeId`, `nodeUrl`, `committee`, or `activeSet` must be specified"
            )),
        }
    }

    /// Returns the list of storage nodes to be queried.
    pub(crate) async fn get_nodes(
        &self,
        sui_read_client: &impl ReadClient,
    ) -> Result<Vec<StorageNode>> {
        match self {
            Self { node_ids, .. } if !node_ids.is_empty() => {
                sui_read_client.get_storage_nodes_by_ids(node_ids).await
            }
            Self { node_urls, .. } if !node_urls.is_empty() => {
                let active_set = sui_read_client.get_storage_nodes_from_active_set().await?;
                node_urls
                    .iter()
                    .map(|node_url| {
                        active_set
                            .iter()
                            .find(|node| &node.network_address.0 == node_url)
                            .cloned()
                            .context(format!(
                                "node URL {node_url} not found in active set; \
                                try to query it by node ID"
                            ))
                    })
                    .collect::<Result<Vec<_>>>()
            }
            Self {
                committee: true, ..
            } => Ok(sui_read_client.get_storage_nodes_from_committee().await?),
            Self {
                active_set: true, ..
            } => sui_read_client.get_storage_nodes_from_active_set().await,
            _ => unreachable!("we checked that exactly one of the node selection options is set"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum TraceExporter {
    #[serde(rename = "otlp")]
    Otlp,
    #[serde(rename = "file")]
    File(PathBuf),
}

impl FromStr for TraceExporter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s == "otlp" {
            Ok(TraceExporter::Otlp)
        } else if s.starts_with("file=") {
            let (_, path) = s.split_once('=').expect("checked for equal sign above");
            Ok(TraceExporter::File(Path::new(path).into()))
        } else {
            Err(anyhow::anyhow!("unrecognised trace exporter '{}'", s))
        }
    }
}

/// The number of epochs to store the blob for.
///
/// Can be either a non-zero number of epochs or the special value `max`, which will store the blob
/// for the maximum number of epochs allowed by the system object on chain.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub enum EpochCountOrMax {
    /// Store the blob for the maximum number of epochs allowed.
    #[serde(rename = "max")]
    Max,
    /// The number of epochs to store the blob for.
    #[serde(untagged)]
    Epochs(NonZeroU32),
}

impl EpochCountOrMax {
    fn parse_epoch_count(input: &str) -> Result<Self> {
        if input == "max" {
            Ok(Self::Max)
        } else {
            let epochs = input.parse::<u32>()?;
            Ok(Self::Epochs(NonZeroU32::new(epochs).ok_or_else(|| {
                anyhow!("invalid epoch count; please a number >0 or `max`")
            })?))
        }
    }

    /// Tries to convert the `EpochCountOrMax` into an `EpochCount` value.
    ///
    /// If the `EpochCountOrMax` is `Max`, the `max_epochs_ahead` is used as the maximum number of
    /// epochs that can be stored ahead.
    pub fn try_into_epoch_count(&self, max_epochs_ahead: EpochCount) -> anyhow::Result<EpochCount> {
        match self {
            EpochCountOrMax::Max => Ok(max_epochs_ahead),
            EpochCountOrMax::Epochs(epochs) => {
                let epochs = epochs.get();
                ensure!(
                    epochs <= max_epochs_ahead,
                    "blobs can only be stored for up to {} epochs ahead; {} epochs were requested",
                    max_epochs_ahead,
                    epochs
                );
                Ok(epochs)
            }
        }
    }
}

/// The number of epochs to store the blob for.
#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct EpochArg {
    /// The number of epochs the blob is stored for.
    ///
    /// If set to `max`, the blob is stored for the maximum number of epochs allowed by the
    /// system object on chain. Otherwise, the blob is stored for the specified number of
    /// epochs. The number of epochs must be greater than 0.
    #[arg(long, value_parser = EpochCountOrMax::parse_epoch_count)]
    pub(crate) epochs: Option<EpochCountOrMax>,

    /// The earliest time when the blob can expire, in RFC3339 format (e.g., "2024-03-20T15:00:00Z")
    /// or a more relaxed format (e.g., "2024-03-20 15:00:00").
    #[arg(long, value_parser = humantime::parse_rfc3339_weak)]
    pub(crate) earliest_expiry_time: Option<SystemTime>,

    /// The end epoch for the blob.
    ///
    /// The end epoch must be greater than the current epoch.
    #[arg(long)]
    pub(crate) end_epoch: Option<Epoch>,
}

impl EpochArg {
    pub(crate) fn exactly_one_is_some(&self) -> Result<()> {
        match (
            self.epochs.is_some(),
            self.earliest_expiry_time.is_some(),
            self.end_epoch.is_some(),
        ) {
            (true, false, false) | (false, true, false) | (false, false, true) => Ok(()),
            _ => Err(anyhow!(
                "exactly one of `epochs`, `earliest-expiry-time`, or `end-epoch` must be specified"
            )),
        }
    }
}

pub(crate) mod default {
    use std::{net::SocketAddr, time::Duration};

    use walrus_sui::utils::SuiNetwork;

    pub(crate) fn max_body_size_kib() -> usize {
        10_240
    }

    pub(crate) fn max_quilt_body_size_kib() -> usize {
        102_400 // 100MB default for quilts.
    }

    pub(crate) fn max_concurrent_requests() -> usize {
        8
    }

    pub(crate) fn max_request_buffer_size() -> usize {
        // 1x the number of concurrent requests by default means that we start rejecting requests
        // rather soon to avoid overloading the publisher.
        //
        // In total there can be 1x processing and 1x in the buffer, for 2x number of concurrent
        // requests allowed before we start rejecting.
        max_concurrent_requests()
    }

    pub(crate) fn n_publisher_clients() -> usize {
        // Use the same number of clients as the number of concurrent requests. This way, the
        // publisher will have the lowest possible latency for every request.
        max_concurrent_requests()
    }

    pub(crate) fn sub_wallets_min_balance() -> u64 {
        500_000_000 // 0.5 SUI or WAL
    }

    pub(crate) fn gas_refill_amount() -> u64 {
        500_000_000 // 0.5 SUI
    }

    pub(crate) fn wal_refill_amount() -> u64 {
        500_000_000 // 0.5 WAL
    }

    pub(crate) fn refill_interval() -> Duration {
        Duration::from_secs(1)
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

    pub(crate) fn staking_amounts_frost() -> Vec<u64> {
        vec![1_000_000_000] // 1 WAL
    }

    pub(crate) fn exchange_amount_mist() -> u64 {
        500_000_000 // 0.5 SUI
    }

    pub(crate) fn sui_network() -> SuiNetwork {
        SuiNetwork::Testnet
    }

    pub(crate) fn faucet_timeout() -> Duration {
        Duration::from_secs(60)
    }

    pub(crate) fn allowed_headers() -> Vec<String> {
        vec![
            "content-type".to_string(),
            "authorization".to_string(),
            "content-disposition".to_string(),
            "content-encoding".to_string(),
            "content-language".to_string(),
            "content-location".to_string(),
            "link".to_string(),
        ]
    }

    pub(crate) fn concurrent_requests_for_health() -> usize {
        60
    }

    pub(crate) fn max_aggregator_concurrent_requests() -> usize {
        256
    }

    pub(crate) fn max_aggregator_buffer_size() -> usize {
        320 // 256 + 64
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use walrus_test_utils::{Result as TestResult, param_test};

    use super::*;

    const STORE_STR_1: &str = r#"{"store": {"files": ["README.md"], "epochs": 1}}"#;
    const STORE_STR_MAX: &str = r#"{"store": {"files": ["README.md"], "epochs": "max"}}"#;
    const READ_STR: &str = r#"{"read": {"blobId": "4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo"}}"#;
    const DAEMON_STR: &str =
        r#"{"daemon": {"bindAddress": "127.0.0.1:12345", "subWalletsDir": "/some/path"}}"#;

    // Creates the fixture for the JSON command string.
    fn make_cmd_str(command: &str) -> String {
        format!(
            r#"{{
                "config": "path/to/client_config.yaml",
                "command": {command}
            }}"#
        )
    }

    // Fixture for the store command.
    fn store_command(epochs: EpochCountOrMax) -> Commands {
        Commands::Cli(CliCommands::Store {
            files: vec![PathBuf::from("README.md")],
            common_options: CommonStoreOptions {
                epoch_arg: EpochArg {
                    epochs: Some(epochs),
                    earliest_expiry_time: None,
                    end_epoch: None,
                },
                dry_run: false,
                force: false,
                ignore_resources: false,
                deletable: false,
                permanent: false,
                share: false,
                encoding_type: Default::default(),
                upload_relay: None,
                skip_tip_confirmation: false,
                upload_mode: None,
                child_process_uploads: false,
                internal_run: false,
            },
        })
    }

    // Fixture for the read command.
    fn read_command() -> Commands {
        Commands::Cli(CliCommands::Read {
            blob_id: BlobId::from_str("4BKcDC0Ih5RJ8R0tFMz3MZVNZV8b2goT6_JiEEwNHQo").unwrap(),
            out: None,
            rpc_arg: RpcArg { rpc_url: None },
            strict_consistency_check: false,
            skip_consistency_check: false,
        })
    }

    // Fixture for the daemon command.
    fn daemon_command() -> Commands {
        Commands::Daemon(DaemonCommands::Daemon {
            args: PublisherArgs {
                n_clients: default::n_publisher_clients(),
                daemon_args: DaemonArgs {
                    bind_address: SocketAddr::from_str("127.0.0.1:12345").unwrap(),
                    metrics_address: default::metrics_address(),
                    blocklist: None,
                },
                max_body_size_kib: default::max_body_size_kib(),
                max_quilt_body_size_kib: default::max_quilt_body_size_kib(),
                max_request_buffer_size: default::max_request_buffer_size(),
                max_concurrent_requests: default::max_concurrent_requests(),
                refill_interval: default::refill_interval(),
                sub_wallets_dir: "/some/path".into(),
                gas_refill_amount: default::gas_refill_amount(),
                wal_refill_amount: default::wal_refill_amount(),
                sub_wallets_min_balance: default::sub_wallets_min_balance(),
                keep: false,
                burn_after_store: false,
                jwt_decode_secret: None,
                jwt_algorithm: None,
                jwt_expiring_sec: 0,
                jwt_verify_upload: false,
                replay_suppression_config: Default::default(),
            },
            aggregator_args: AggregatorArgs {
                allowed_headers: default::allowed_headers(),
                allow_quilt_patch_tags_in_response: false,
                max_blob_size: None,
                max_request_buffer_size: default::max_aggregator_buffer_size(),
                max_concurrent_requests: default::max_aggregator_concurrent_requests(),
            },
        })
    }

    param_test! {
        test_json_string_extraction -> TestResult: [
            store_max: (&make_cmd_str(STORE_STR_MAX), store_command(EpochCountOrMax::Max)),
            store_1: (
                &make_cmd_str(STORE_STR_1),
                store_command(EpochCountOrMax::Epochs(NonZeroU32::new(1).expect("1 > 0")))
            ),
            read: (&make_cmd_str(READ_STR), read_command()),
            daemon: (&make_cmd_str(DAEMON_STR), daemon_command())
        ]
    }
    /// Test that the command string in JSON mode is extracted correctly.
    fn test_json_string_extraction(json: &str, command: Commands) -> TestResult {
        let mut app = App {
            config: None,
            context: None,
            wallet: None,
            gas_budget: None,
            json: false,
            trace_cli: None,
            command: Commands::Json {
                command_string: Some(json.to_string()),
            },
        };
        app.extract_json_command()?;
        assert_eq!(app.command, command);
        Ok(())
    }
}

/// Specifies whether the user has granted the confirmation for the action, or if it is required.
#[derive(Debug, Clone, Default, Eq, PartialEq, Deserialize)]
pub enum UserConfirmation {
    // The user needs to confirm the action.
    #[default]
    Required,
    // The action can proceed without confirmation from the user.
    Granted,
}

impl From<&bool> for UserConfirmation {
    fn from(yes: &bool) -> Self {
        Self::from(*yes)
    }
}

impl From<bool> for UserConfirmation {
    fn from(yes: bool) -> Self {
        if yes {
            UserConfirmation::Granted
        } else {
            UserConfirmation::Required
        }
    }
}

impl UserConfirmation {
    /// Checks if the user confirmation is required.
    pub fn is_required(&self) -> bool {
        matches!(self, UserConfirmation::Required)
    }
}

/// Sort options for node information display
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum, Default)]
#[serde(rename_all = "kebab-case")]
pub enum NodeSortBy {
    /// Sort by node ID
    Id,
    /// Sort by node name
    #[default]
    Name,
    /// Sort by node URL
    Url,
}

/// Sort options for health information display
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum, Default)]
#[serde(rename_all = "kebab-case")]
pub enum HealthSortBy {
    /// Sort by node status
    #[default]
    Status,
    /// Sort by node ID
    Id,
    /// Sort by node name
    Name,
    /// Sort by node URL
    Url,
}

/// Generic sort configuration that can be used with any ValueEnum type
#[derive(Debug, Clone, Deserialize, PartialEq, Eq, clap::Args)]
#[serde(rename_all = "camelCase")]
pub struct SortBy<T: clap::ValueEnum + Send + Sync + Default + 'static> {
    /// Field to sort by
    #[arg(long, value_enum)]
    #[serde(default)]
    pub sort_by: Option<T>,

    /// Sort in descending order
    #[arg(long)]
    #[serde(default)]
    pub desc: bool,
}

impl<T: clap::ValueEnum + Send + Sync + Default + 'static> Default for SortBy<T> {
    fn default() -> Self {
        Self {
            sort_by: Some(T::default()),
            desc: false,
        }
    }
}
