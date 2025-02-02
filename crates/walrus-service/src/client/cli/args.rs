// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The arguments to the Walrus client binary.

use std::{
    net::SocketAddr,
    num::{NonZeroU16, NonZeroU32},
    path::PathBuf,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context as _, Result};
use clap::{Args, Parser, Subcommand};
use jsonwebtoken::Algorithm;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use sui_types::base_types::ObjectID;
use walrus_core::{encoding::EncodingConfig, ensure, BlobId, Epoch, EpochCount};
use walrus_sui::{
    client::{ExpirySelectionPolicy, ReadClient, SuiContractClient},
    types::StorageNode,
    utils::SuiNetwork,
};

use super::{parse_blob_id, read_blob_from_file, BlobIdDecimal, HumanReadableBytes};
use crate::client::config::AuthConfig;

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
    #[clap(long, verbatim_doc_comment, global = true)]
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
    #[clap(long, verbatim_doc_comment, global = true)]
    #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
    pub wallet: Option<PathBuf>,
    /// The gas budget for transactions.
    ///
    /// If not specified, the gas budget is estimated automatically.
    #[clap(long, global = true)]
    pub gas_budget: Option<u64>,
    /// Write output as JSON.
    ///
    /// This is always done in JSON mode.
    #[clap(long, action, global = true)]
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
    #[clap(alias("write"))]
    Store {
        /// The files containing the blob to be published to Walrus.
        #[clap(required = true, value_name = "FILES")]
        #[serde(deserialize_with = "crate::utils::resolve_home_dir_vec")]
        files: Vec<PathBuf>,
        /// The epoch argument to specify either the number of epochs to store the blob, or the
        /// end epoch, or the earliest expiry time in rfc3339 format.
        ///
        #[clap(flatten)]
        #[serde(flatten)]
        epoch_arg: EpochArg,
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

        /// Whether to put the blob into a shared blob object.
        #[clap(long, action)]
        #[serde(default)]
        share: bool,
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
        #[clap(long)]
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
        #[clap(long, value_parser = humantime::parse_duration, default_value = "1s")]
        #[serde(default = "default::status_timeout")]
        timeout: Duration,
        /// The URL of the Sui RPC node to use.
        #[clap(flatten)]
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
        #[clap(flatten)]
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
        #[clap(flatten)]
        #[serde(flatten)]
        rpc_arg: RpcArg,
        /// The node selector for the storage node to print health information for.
        #[clap(flatten)]
        #[serde(flatten)]
        node_selection: NodeSelection,
        /// Print detailed health information.
        #[clap(long, action)]
        #[serde(default)]
        detail: bool,
    },
    /// Encode the specified file to obtain its blob ID.
    BlobId {
        /// The file containing the blob for which to compute the blob ID.
        #[serde(deserialize_with = "crate::utils::resolve_home_dir")]
        file: PathBuf,
        /// The number of shards for which to compute the blob ID.
        ///
        /// If not specified, the number of shards is read from chain.
        #[clap(long)]
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
        /// Proceed to delete the blob without confirmation.
        #[clap(long, action)]
        #[serde(default)]
        yes: bool,
        /// Disable checking the status of the blob after deletion.
        ///
        /// Checking the status adds delay and requires additional requests.
        #[clap(long, action)]
        #[serde(default)]
        no_status_check: bool,
    },
    /// Stake with storage node.
    Stake {
        /// The object ID of the storage node to stake with.
        #[clap(long, required=true, num_args=1.., alias("node-id"))]
        node_ids: Vec<ObjectID>,
        /// The amount of FROST (smallest unit of WAL token) to stake with the storage node.
        ///
        /// If this is a single value, this amount is staked at all nodes. Otherwise, the number of
        /// values must be equal to the number of node IDs, and each amount is staked at the node
        /// with the same index.
        #[clap(long, alias("amount"), default_value = "1000000000")]
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
        #[clap(long)]
        path: Option<PathBuf>,
        /// Sui network for which the wallet is generated.
        ///
        /// Available options are `devnet`, `testnet`, and `localnet`.
        #[clap(long, default_value_t = default::sui_network())]
        #[serde(default = "default::sui_network")]
        sui_network: SuiNetwork,
        /// Whether to attempt to get SUI tokens from the faucet.
        #[clap(long, action)]
        #[serde(default)]
        use_faucet: bool,
        /// Timeout for the faucet call.
        #[clap(
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
        #[clap(long)]
        /// The object ID of the exchange to use.
        ///
        /// This takes precedence over any values set in the configuration file.
        exchange_id: Option<ObjectID>,
        #[clap(long, default_value_t = default::exchange_amount_mist())]
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
        #[clap(flatten)]
        #[serde(flatten)]
        burn_selection: BurnSelection,
        /// Proceed to burn the blobs without confirmation.
        #[clap(long, action)]
        #[serde(default)]
        yes: bool,
    },

    /// Fund a shared blob.
    FundSharedBlob {
        /// The object ID of the shared blob to fund.
        #[clap(long)]
        shared_blob_obj_id: ObjectID,
        /// The amount of FROST (smallest unit of WAL token) to fund the shared blob with.
        #[clap(long)]
        amount: u64,
    },
    /// Extend an owned or shared blob.
    Extend {
        /// The object ID of the blob to extend.
        #[clap(long)]
        blob_obj_id: ObjectID,

        /// If the blob_obj_id refers to a shared blob object, this flag must be present.
        #[clap(long)]
        shared: bool,

        /// The number of epochs to extend the blob for.
        ///
        /// If set to `max`, the blob is stored for the maximum number of epochs allowed by the
        /// system object on chain. Otherwise, the blob is stored for the specified number of
        /// epochs. The number of epochs must be greater than 0.
        #[clap(long, value_parser = EpochCountOrMax::parse_epoch_count)]
        epochs_ahead: EpochCountOrMax,
    },
    /// Share a blob.
    Share {
        /// The object ID of the (owned) blob to share.
        #[clap(long)]
        blob_obj_id: ObjectID,
        /// If specified, share and directly fund the blob.
        #[clap(long)]
        amount: Option<u64>,
    },
    /// Get the metadata of a blob.
    GetBlobMetadata {
        /// The object ID of the blob to get the metadata of.
        #[clap(index = 1)]
        blob_obj_id: ObjectID,
        /// If unset, prints the blob to stdout.
        #[clap(long)]
        #[serde(default, deserialize_with = "crate::utils::resolve_home_dir_option")]
        out: Option<PathBuf>,
    },
    /// Set the metadata of a blob.
    SetBlobMetadata {
        /// The object ID of the blob to set the metadata of.
        #[clap(index = 1)]
        blob_obj_id: ObjectID,
        /// The key to set the metadata of.
        #[clap(long)]
        key: String,
        /// The value to set the metadata of.
        #[clap(long)]
        value: String,
    },
    /// Remove the metadata from a blob.
    RemoveBlobMetadata {
        /// The object ID of the blob to remove the metadata from.
        #[clap(index = 1)]
        blob_obj_id: ObjectID,
    },

    /// Remove a key-value pair from a blob's metadata.
    RemoveBlobMetadataPair {
        /// The object ID of the blob.
        #[clap(index = 1)]
        blob_obj_id: ObjectID,
        /// The key to remove from the blob's metadata.
        #[clap(long)]
        key: String,
    },
}

/// Subcommands for the `info` command.
#[derive(Subcommand, Debug, Clone, Deserialize, PartialEq, Eq)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum InfoCommands {
    /// Print all information listed below.
    All,
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
    Committee,
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

/// The arguments for the publisher service.
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PublisherArgs {
    /// The configuration for the daemon.
    #[clap(flatten)]
    #[serde(flatten)]
    pub daemon_args: DaemonArgs,
    /// The maximum body size of PUT requests in KiB.
    #[clap(long = "max-body-size", default_value_t = default::max_body_size_kib())]
    #[serde(default = "default::max_body_size_kib")]
    pub max_body_size_kib: usize,
    /// The maximum number of requests that can be buffered before the server starts rejecting new
    /// ones.
    #[clap(long = "max-buffer-size", default_value_t = default::max_request_buffer_size())]
    #[serde(default = "default::max_request_buffer_size")]
    pub max_request_buffer_size: usize,
    /// The maximum number of requests the publisher can process concurrently.
    ///
    /// If more requests than this maximum are received, the excess requests are buffered up to
    /// `--max-buffer-size`. Any outstanding request will result in a response with a 429 HTTP
    /// status code.
    #[clap(long, default_value_t = default::max_concurrent_requests())]
    #[serde(default = "default::max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// The number of clients to use for the publisher.
    ///
    /// The publisher uses this number of clients to publish blobs concurrently.
    #[clap(long, default_value_t = default::n_publisher_clients())]
    #[serde(default = "default::n_publisher_clients")]
    pub n_clients: usize,
    /// The interval of time between refilling the publisher's sub-clients' wallets.
    #[clap(long, value_parser = humantime::parse_duration, default_value="1s")]
    #[serde(default = "default::refill_interval")]
    pub refill_interval: Duration,
    /// The directory where the publisher will store the sub-wallets used for client multiplexing.
    #[clap(long)]
    #[serde(deserialize_with = "crate::utils::resolve_home_dir")]
    pub sub_wallets_dir: PathBuf,
    /// The amount of MIST transferred at every refill.
    #[clap(long, default_value_t = default::gas_refill_amount())]
    #[serde(default = "default::gas_refill_amount")]
    pub gas_refill_amount: u64,
    /// The amount of FROST transferred at every refill.
    #[clap(long, default_value_t = default::wal_refill_amount())]
    #[serde(default = "default::wal_refill_amount")]
    pub wal_refill_amount: u64,
    /// The minimum balance the sub-wallets should have.
    ///
    /// Below this threshold, the sub-wallets are refilled.
    #[clap(long, default_value_t = default::sub_wallets_min_balance())]
    #[serde(default = "default::sub_wallets_min_balance")]
    pub sub_wallets_min_balance: u64,
    /// If set, the publisher will keep the created Blob objects in its _main_ wallet.
    ///
    /// If unset, the publisher will immediately burn all created blob objects by default. However,
    /// note that this flag _does not affect_ the use of the `send_object_to` query parameter:
    /// Regardless of this flag's status, the publisher will send created objects to the address in
    /// the `send_object_to` query parameter, if it is specified in the PUT request.
    #[clap(long, action)]
    #[serde(default)]
    pub keep: bool,
    /// If set, the publisher will verify the JWT token.
    ///
    /// If not specified, the verification is disabled.
    /// This is useful, e.g., in case the API Gateway has already checked the token.
    /// The secret can be hex string, starting with `0x`.
    #[clap(long)]
    #[serde(default)]
    pub jwt_decode_secret: Option<String>,
    /// If unset, the JWT authentication algorithm will be HMAC.
    ///
    /// The following algorithms are supported: "HS256", "HS384", "HS512", "ES256", "ES384",
    /// "RS256", "RS384", "PS256", "PS384", "PS512", "RS512", "EdDSA".
    #[clap(long)]
    #[serde(default)]
    pub jwt_algorithm: Option<Algorithm>,
    /// If set and greater than 0, the publisher will check if the JWT token is expired based on
    /// the "issued at" (`iat`) value.
    #[clap(long, default_value_t = 0)]
    #[serde(default)]
    pub jwt_expiring_sec: u64,
    /// If set, the publisher will verify that the requested upload matches the claims in the JWT.
    ///
    /// Specifically, the publisher will:
    /// - Verify that the number of `epochs` in query is the the same as `epochs` in the JWT, if
    ///   present;
    /// - Verify that the `send_object_to` field in the query is the same as the `send_object_to`
    ///   in the JWT, if present;
    // TODO: /// - Verify the size/hash of uploaded file
    #[clap(long, action)]
    #[serde(default)]
    pub jwt_verify_upload: bool,
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

    pub(crate) fn generate_auth_config(&mut self) -> Result<Option<AuthConfig>> {
        if self.jwt_decode_secret.is_some() || self.jwt_expiring_sec > 0 || self.jwt_verify_upload {
            let mut config = self
                .jwt_decode_secret
                .take()
                .map(AuthConfig::new)
                .unwrap_or(Ok(AuthConfig::default()))?;
            config.expiring_sec = self.jwt_expiring_sec;
            config.verify_upload = self.jwt_verify_upload;
            config.algorithm = self.jwt_algorithm;
            tracing::info!("Auth config applied: {config:?}");
            Ok(Some(config))
        } else {
            tracing::info!("Auth disabled");
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
    /// If unset, the wallet configuration is applied (if set), or the fullnode at
    /// `fullnode.testnet.sui.io:443` is used.
    // NB: Keep this in sync with `crate::cli`.
    #[clap(long)]
    #[serde(default)]
    pub(crate) rpc_url: Option<String>,
}

#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DaemonArgs {
    /// The address to which to bind the service.
    #[clap(long, default_value_t = default::bind_address())]
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
    #[clap(long)]
    #[serde(default)]
    pub(crate) file: Option<PathBuf>,
    /// The blob ID to be checked.
    #[clap(long, allow_hyphen_values = true, value_parser = parse_blob_id)]
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
    #[clap(long)]
    #[serde(default)]
    pub(crate) file: Option<PathBuf>,
    /// The blob ID to be deleted.
    ///
    /// This command deletes _all_ owned blob objects matching the provided blob ID.
    #[clap(long, allow_hyphen_values = true, value_parser = parse_blob_id)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(default)]
    pub(crate) blob_id: Option<BlobId>,
    /// The object ID of the blob object to be deleted.
    ///
    /// This command deletes only the blob object with the given object ID.
    #[clap(long)]
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

/// Selector for the blob object IDs to burn.
#[serde_as]
#[derive(Debug, Clone, Args, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[group(required = true, multiple = false)]
pub struct BurnSelection {
    /// The object IDs of the Blob objects to burn.
    #[clap(long, num_args=1.., required=true)]
    #[serde_as(as = "Vec<DisplayFromStr>")]
    object_ids: Vec<ObjectID>,
    /// Burn all the blob objects owned by the wallet.
    #[clap(long, action)]
    #[serde(default)]
    all: bool,
    /// Burn all the expired blob objects owned by the wallet.
    #[clap(long, action)]
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
    #[clap(long, alias="node-id", num_args=1..)]
    #[serde(default)]
    pub node_ids: Vec<ObjectID>,
    /// The URLs of the storage nodes to be selected.
    #[clap(long, alias="node-url", num_args=1..)]
    #[serde(default)]
    pub node_urls: Vec<String>,
    /// Select all storage nodes in the current committee.
    #[clap(long, action)]
    #[serde(default)]
    pub committee: bool,
    /// Select all storage nodes in the active set.
    #[clap(long, action)]
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
    #[clap(long, value_parser = EpochCountOrMax::parse_epoch_count)]
    pub(crate) epochs: Option<EpochCountOrMax>,

    /// The earliest time when the blob can expire, in RFC3339 format (e.g. "2024-03-20T15:00:00Z")
    /// or a more relaxed format (e.g. "2024-03-20 15:00:00").
    #[clap(long, value_parser = humantime::parse_rfc3339_weak)]
    pub(crate) earliest_expiry_time: Option<SystemTime>,

    /// The end epoch for the blob.
    #[clap(long)]
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
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use walrus_test_utils::{param_test, Result as TestResult};

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
                "command": {}
            }}"#,
            command
        )
    }

    // Fixture for the store command.
    fn store_command(epochs: EpochCountOrMax) -> Commands {
        Commands::Cli(CliCommands::Store {
            files: vec![PathBuf::from("README.md")],
            epoch_arg: EpochArg {
                epochs: Some(epochs),
                earliest_expiry_time: None,
                end_epoch: None,
            },
            dry_run: false,
            force: false,
            deletable: false,
            share: false,
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
                n_clients: default::n_publisher_clients(),
                daemon_args: DaemonArgs {
                    bind_address: SocketAddr::from_str("127.0.0.1:12345").unwrap(),
                    metrics_address: default::metrics_address(),
                    blocklist: None,
                },
                max_body_size_kib: default::max_body_size_kib(),
                max_request_buffer_size: default::max_request_buffer_size(),
                max_concurrent_requests: default::max_concurrent_requests(),
                refill_interval: default::refill_interval(),
                sub_wallets_dir: "/some/path".into(),
                gas_refill_amount: default::gas_refill_amount(),
                wal_refill_amount: default::wal_refill_amount(),
                sub_wallets_min_balance: default::sub_wallets_min_balance(),
                keep: false,
                jwt_decode_secret: None,
                jwt_algorithm: None,
                jwt_expiring_sec: 0,
                jwt_verify_upload: false,
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
            wallet: None,
            gas_budget: None,
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
