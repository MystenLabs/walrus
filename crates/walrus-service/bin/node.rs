// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Storage Node entry point.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fmt::Display,
    fs,
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum as _};
use commands::generate_or_convert_key;
use config::PathOrInPlace;
use fs::File;
use serde::{Deserialize, Serialize};
use sui_types::base_types::{ObjectID, SuiAddress};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::{
    Epoch,
    keys::{NetworkKeyPair, ProtocolKeyPair},
};
use walrus_service::{
    DbCheckpointManager,
    SyncNodeConfigError,
    common::{config::SuiConfig, telemetry::WalrusTracingHandle},
    event::event_processor::runtime::EventProcessorRuntime,
    node::{
        ConfigLoader,
        StorageNode,
        StorageNodeConfigLoader,
        config::{self, StorageNodeConfig, VotingParamsConfig, defaults::REST_API_PORT},
        dbtool::DbToolCommands,
        server::{RestApiConfig, RestApiServer},
        system_events::EventManager,
    },
    utils::{
        self,
        ByteCount,
        EnableMetricsPush,
        MAX_NODE_NAME_LENGTH,
        MetricPushRuntime,
        MetricsAndLoggingRuntime,
        wait_until_terminated,
    },
};
use walrus_sui::{
    client::{SuiContractClient, rpc_config::RpcFallbackConfigArgs},
    utils::SuiNetwork,
};
// Define the `GIT_REVISION` and `VERSION` consts
walrus_utils::bin_version!();

/// Manage and run a Walrus storage node.
#[derive(Debug, Parser)]
#[command(rename_all = "kebab-case", name = env!("CARGO_BIN_NAME"), version = VERSION)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

/// A wrapper around the necessary components required by the admin commands.
#[derive(Debug, Clone)]
struct AdminArgs {
    /// Checkpoint manager.
    checkpoint_manager: Option<Arc<DbCheckpointManager>>,
    /// Admin socket path.
    admin_socket_path: Option<PathBuf>,
    /// Tracing handle.
    tracing_handle: WalrusTracingHandle,
}

#[derive(Subcommand, Debug, Clone)]
#[command(rename_all = "kebab-case")]
enum Commands {
    /// Generate Sui wallet, keys, and configuration for a Walrus node and optionally generates a
    /// YAML file that can be used to register the node by a third party.
    ///
    /// Attempts to create the specified directory. Fails if the directory is not empty (unless the
    /// `--force` option is provided).
    Setup(SetupArgs),

    /// Register a new node with the Walrus storage network.
    Register {
        /// The path to the node's configuration file.
        #[arg(long)]
        config_path: PathBuf,
        /// Overwrite existing storage node capability object if the input config already has one.
        #[arg(long)]
        force: bool,
    },

    /// Run a storage node with the provided configuration.
    Run {
        /// Path to the Walrus node configuration file.
        #[arg(long)]
        config_path: PathBuf,
        /// Whether to cleanup the storage directory before starting the node.
        #[arg(long, default_value_t = false)]
        cleanup_storage: bool,
        /// Whether to ignore the failures from node parameter synchronization with on-chain values.
        #[deprecated(note = "This flag is being removed and will have no effect")]
        #[arg(long, default_value_t = false)]
        ignore_sync_failures: bool,
    },

    /// Generate a new key for use with the Walrus protocol, and writes it to a file.
    KeyGen {
        /// Path to the file at which the key will be created [default: ./<KEY_TYPE>.key].
        ///
        /// If the file already exists, it is not overwritten and the operation will fail unless
        /// the `--force` option is provided.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Which type of key to generate.
        #[arg(long, value_enum)]
        key_type: KeyType,
        /// Output the key in the specified format.
        #[arg(long, value_enum, default_value_t = KeyFormat::Tagged)]
        format: KeyFormat,
        /// Overwrite existing files.
        #[arg(long)]
        force: bool,
        /// Convert an existing key instead of generating a new key.
        ///
        /// Provide a path to an existing key in a supported format. The key is converted to the
        /// format specified by `--format` before being written.
        #[arg(long, value_name = "INPUT_KEY_PATH")]
        convert: Option<PathBuf>,
    },

    /// Generate a new node configuration.
    GenerateConfig {
        #[command(flatten)]
        path_args: PathArgs,
        #[command(flatten)]
        config_args: ConfigArgs,
        /// Overwrite existing files.
        #[arg(long)]
        force: bool,
    },

    /// Database inspection and maintenance tools.
    /// Hidden command for emergency use only.
    #[command(hide = true)]
    DbTool {
        #[command(subcommand)]
        command: DbToolCommands,
    },

    /// Catchup events using event blobs.
    /// Hidden command for emergency use only.
    #[command(hide = true)]
    Catchup(CatchupArgs),

    /// Restore the database from a checkpoint.
    Restore(RestoreArgs),

    /// List available database checkpoints without connecting to a running node.
    ListDbCheckpoint(ListDbCheckpointArgs),

    /// Local admin commands for managing a running node.
    LocalAdmin {
        /// Admin subcommand to execute.
        #[command(subcommand)]
        command: AdminCommands,
        /// Path to the admin socket.
        #[arg(long)]
        socket_path: PathBuf,
    },
}

/// Admin subcommands for remote node management.
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
enum AdminCommands {
    /// Checkpoint management.
    Checkpoint {
        /// Subcommand to execute.
        #[command(subcommand)]
        command: CheckpointCommands,
    },
    /// Log level management.
    /// It also supports log directive like `walrus-service=debug`
    /// to set the log level for a specific component. Use it like this:
    /// `walrus-node local-admin --socket-path admin log-level --level "walrus_service=debug"`
    LogLevel {
        /// The log level to set.
        #[arg(long)]
        level: String,
    },
}

/// Standard response format for admin commands.
#[derive(Serialize, Deserialize)]
struct AdminCommandResponse {
    /// Whether the command was successful.
    success: bool,
    /// A message describing the command.
    message: String,
}

/// Commands for checkpoint management.
///
/// Note the checkpoint command works only on the Walrus main DB.
#[allow(deprecated)]
#[derive(Subcommand, Debug, Clone, Serialize, Deserialize)]
#[command(rename_all = "kebab-case")]
enum CheckpointCommands {
    /// Create a new checkpoint.
    // TODO(WAL-953): Add a flag to make this non-blocking.
    Create {
        /// The path where the checkpoint will be created. If not specified, the checkpoint will be
        /// created in the `checkpoint_dir` specified in [`StorageNodeConfig::checkpoint_config`].
        #[arg(long)]
        #[serde(default)]
        path: Option<PathBuf>,
        /// The delay before creating the checkpoint.
        #[arg(long)]
        delay_secs: Option<u64>,
    },

    /// Cancel an ongoing checkpoint creation.
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum KeyType {
    /// A protocol key used to sign Walrus protocol messages.
    Protocol,
    /// A network key used to authenticate nodes in network communication.
    Network,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

impl KeyType {
    fn default_filename(&self) -> &'static str {
        match self {
            KeyType::Protocol => "protocol.key",
            KeyType::Network => "network.key",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum KeyFormat {
    /// Format the key as a base64 value comprised of (tag || private-key-bytes).
    Tagged,
    /// Format the key as a PKCS#8 PEM-encoded private-key (only supported for the network key
    /// type).
    Pkcs8,
}

impl Display for KeyFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

#[derive(Debug, Clone, clap::Args)]
struct SetupArgs {
    /// The path to the directory in which to set up wallet and node configuration.
    #[arg(long)]
    config_directory: PathBuf,
    /// The path where the Walrus database will be stored.
    #[arg(long)]
    storage_path: PathBuf,
    /// Sui network for which the config is generated.
    ///
    /// Available options are `devnet`, `testnet`, `mainnet`, and `localnet`, or a custom Sui
    /// network. To specify a custom Sui network, pass a string of the format
    /// `<RPC_URL>(;<FAUCET_URL>)?`.
    #[arg(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// Whether to attempt to get SUI tokens from the faucet.
    #[arg(long)]
    use_faucet: bool,
    /// Timeout for the faucet call.
    #[arg(
        long,
        value_parser = humantime::parse_duration,
        default_value = "1min",
        requires = "use_faucet",
    )]
    faucet_timeout: Duration,
    /// Additional arguments for the generated configuration.
    #[command(flatten)]
    config_args: ConfigArgs,
    /// Path to an existing network key. If not specified, a new key will be generated.
    #[arg(long)]
    network_key_path: Option<PathBuf>,
    /// Overwrite existing files.
    #[arg(long)]
    force: bool,
    /// The wallet address of the third party that will register the node.
    ///
    /// If this is set, a YAML file is generated that can be used to register the node by a
    /// third party.
    #[arg(long)]
    registering_third_party: Option<SuiAddress>,
    /// The epoch at which the node will be registered.
    #[arg(long, requires = "registering_third_party", default_value_t = 0)]
    registration_epoch: Epoch,
}

/// Currency unit for storage and write prices.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
enum PriceCurrency {
    /// Prices in FROST (1e9 FROST = 1 WAL)
    #[default]
    Frost,
    /// Prices in NanoUSD (1e9 NanoUSD = 1 USD)
    NanoUsd,
}

#[derive(Debug, Clone, clap::Args)]
struct ConfigArgs {
    /// Object ID of the Walrus system object. If not provided, a dummy value is used and the
    /// system object needs to be manually added to the configuration file at a later time.
    #[arg(long)]
    system_object: Option<ObjectID>,
    /// Object ID of the Walrus staking object. If not provided, a dummy value is used and the
    /// staking object needs to be manually added to the configuration file at a later time.
    #[arg(long)]
    staking_object: Option<ObjectID>,
    /// Initial storage capacity of this node in bytes.
    ///
    /// The value can either by unitless; have suffixes for powers of 1000, such as (B),
    /// kilobytes (K), etc.; or have suffixes for the IEC units such as kibibytes (Ki),
    /// mebibytes (Mi), etc.
    #[arg(long)]
    node_capacity: ByteCount,
    /// The host name or public IP address of the node.
    #[arg(long)]
    public_host: String,
    /// The name of the storage node used in the registration.
    #[arg(long)]
    name: String,

    // ***************************
    //   Optional fields below
    // ***************************
    /// HTTP URL of the Sui full-node RPC endpoint (including scheme and port) to use for event
    /// processing.
    ///
    /// If not provided, the RPC node from the wallet's active environment will be used.
    #[arg(long)]
    sui_rpc: Option<String>,
    /// The port on which the storage node will serve requests.
    #[arg(long, default_value_t = REST_API_PORT)]
    public_port: u16,
    /// Socket address on which the REST API listens.
    #[arg(long, default_value_t = config::defaults::rest_api_address())]
    rest_api_address: SocketAddr,
    /// Socket address on which the Prometheus server should export its metrics.
    #[arg(long, default_value_t = config::defaults::metrics_address())]
    metrics_address: SocketAddr,
    /// URL of the Walrus proxy to push metrics to.
    #[arg(long)]
    metrics_push_url: Option<String>,
    /// Path to an existing TLS certificate. If not specified, the node will automatically generate
    /// self-signed certificates.
    #[arg(long)]
    certificate_path: Option<PathBuf>,
    /// Gas budget for transactions.
    ///
    /// If not specified, the gas budget is estimated automatically.
    #[arg(long)]
    gas_budget: Option<u64>,
    /// Initial vote for the storage price per MiB per epoch.
    ///
    /// The unit is determined by `--price-currency` (FROST or NanoUSD).
    #[arg(long, default_value_t = config::defaults::storage_price())]
    storage_price: u64,
    /// Initial vote for the write price per MiB.
    ///
    /// The unit is determined by `--price-currency` (FROST or NanoUSD).
    #[arg(long, default_value_t = config::defaults::write_price())]
    write_price: u64,
    /// Currency unit for storage and write prices.
    ///
    /// Use "frost" for FROST (1e9 FROST = 1 WAL) or "nano-usd" for NanoUSD (1e9 NanoUSD = 1 USD).
    #[arg(long, value_enum, default_value_t = PriceCurrency::Frost)]
    price_currency: PriceCurrency,
    /// The commission rate of the storage node, in basis points (1% = 100 basis points).
    #[arg(long, default_value_t = config::defaults::commission_rate())]
    commission_rate: u16,
    /// The image URL of the storage node.
    #[arg(long, default_value = "")]
    image_url: String,
    /// The project URL of the storage node.
    #[arg(long, default_value = "")]
    project_url: String,
    /// The description of the storage node.
    #[arg(long, default_value = "")]
    description: String,
    /// The config for rpc fallback.
    #[command(flatten)]
    rpc_fallback_config_args: Option<RpcFallbackConfigArgs>,
    /// Additional Sui full-node RPC endpoints.
    #[arg(long, default_values_t = Vec::<String>::new())]
    additional_rpc_endpoints: Vec<String>,
}

#[derive(Debug, Clone, clap::Args)]
struct PathArgs {
    /// The output path for the generated configuration file. If the file already exists, it is
    /// not overwritten and the operation will fail unless the `--force` option is provided.
    #[arg(long)]
    config_path: PathBuf,
    /// The path where the Walrus database will be stored.
    #[arg(long)]
    storage_path: PathBuf,
    /// The path to the key pair used in Walrus protocol messages.
    #[arg(long)]
    protocol_key_path: PathBuf,
    /// The path to the key pair used to authenticate nodes in network communication.
    #[arg(long)]
    network_key_path: PathBuf,
    /// Location of the node's wallet config.
    #[arg(long)]
    wallet_config: PathBuf,
}

#[derive(Debug, Clone, clap::Args)]
struct CatchupArgs {
    /// Path to the RocksDB database directory.
    #[arg(long)]
    db_path: PathBuf,
    /// Object ID of the Walrus system object.
    #[arg(long)]
    system_object_id: ObjectID,
    /// Object ID of the Walrus staking object.
    #[arg(long)]
    staking_object_id: ObjectID,
    /// The Sui RPC URL to use for catchup.
    #[arg(long, default_value = "http://localhost:9000")]
    sui_rpc_url: String,
    /// The timeout for each request to the Sui RPC node.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10s")]
    checkpoint_request_timeout: Duration,
    /// The duration to run the event processor for.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1min")]
    runtime_duration: Duration,
    /// The minimum checkpoint lag to use for event stream catchup.
    #[arg(long)]
    event_stream_catchup_min_checkpoint_lag: u64,
    /// The config for RPC fallback.
    #[command(flatten)]
    rpc_fallback_config_args: Option<RpcFallbackConfigArgs>,
    /// The interval at which to sample high-frequency tracing logs.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30s")]
    sampled_tracing_interval: Duration,
}

#[derive(Debug, Clone, clap::Args)]
struct RestoreArgs {
    /// The path where the checkpoint is stored. Note, it will not be defaulted to the
    /// checkpoint dir in the config file.
    #[arg(long)]
    db_checkpoint_path: PathBuf,
    /// The path where the database will be restored.
    #[arg(long)]
    db_path: PathBuf,
    /// The path where the WAL will be restored. If not specified, the WAL will be restored in
    /// the same directory as the database.
    #[arg(long)]
    wal_path: Option<PathBuf>,
    /// The ID of the checkpoint to restore. If not specified, the latest checkpoint will be
    /// restored.
    #[arg(long)]
    checkpoint_id: Option<u32>,
}

#[derive(Debug, Clone, clap::Args)]
struct ListDbCheckpointArgs {
    /// The path to the checkpoint directory.
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !matches!(args.command, Commands::Run { .. }) {
        utils::init_tracing_subscriber()?;
    }

    match args.command {
        Commands::Setup(setup_args) => commands::setup(setup_args)?,

        Commands::Register { config_path, force } => commands::register_node(config_path, force)?,

        #[allow(deprecated)]
        Commands::Run {
            config_path,
            cleanup_storage,
            ignore_sync_failures: _,
        } => loop {
            let config = StorageNodeConfig::load_config(&config_path)?;
            let result = commands::run(
                config,
                cleanup_storage,
                Arc::new(StorageNodeConfigLoader::new(config_path.clone())),
            );

            match result {
                Err(e)
                    if matches!(
                        e.downcast_ref::<SyncNodeConfigError>(),
                        Some(SyncNodeConfigError::ProtocolKeyPairRotationRequired)
                    ) =>
                {
                    tracing::info!("protocol key pair rotation required, rotating key pair...");
                    StorageNodeConfig::rotate_protocol_key_pair_persist(&config_path)?;
                    continue;
                }
                Err(e)
                    if matches!(
                        e.downcast_ref::<SyncNodeConfigError>(),
                        Some(SyncNodeConfigError::NodeNeedsReboot)
                    ) =>
                {
                    tracing::info!("node needs reboot, restarting...");
                    continue;
                }
                Err(e) => return Err(e),
                Ok(()) => return Ok(()),
            }
        },

        Commands::KeyGen {
            out,
            key_type,
            force,
            format,
            convert,
        } => generate_or_convert_key(
            out.as_deref()
                .unwrap_or_else(|| Path::new(key_type.default_filename())),
            key_type,
            force,
            format,
            convert.as_deref(),
        )?,

        Commands::GenerateConfig {
            path_args,
            config_args,
            force,
        } => {
            commands::generate_config(path_args, config_args, force)?;
        }

        Commands::DbTool { command } => command.execute()?,

        Commands::Catchup(catchup_args) => commands::catchup(catchup_args)?,

        Commands::Restore(restore_args) => commands::restore(restore_args)?,

        Commands::ListDbCheckpoint(args) => commands::list_db_checkpoints(args)?,

        Commands::LocalAdmin {
            command,
            socket_path,
        } => commands::handle_admin_command(command, socket_path)?,
    }
    Ok(())
}

mod commands {
    use config::{
        LoadsFromPath,
        MetricsPushConfig,
        NodeRegistrationParamsForThirdPartyRegistration,
        ServiceRole,
    };
    #[cfg(not(msim))]
    use tokio::task::JoinSet;
    use walrus_core::{
        ensure,
        keys::{SupportedKeyPair, TaggedKeyPair},
    };
    use walrus_service::{
        event::event_processor::{
            config::{EventProcessorConfig, EventProcessorRuntimeConfig, SystemConfig},
            processor::EventProcessor,
        },
        node::{
            DatabaseConfig,
            config::{LoadedConfig, TlsConfig, VotingPrices, VotingPricesConfig},
        },
        utils,
    };
    use walrus_sui::{
        client::{
            ReadClient as _,
            SuiReadClient,
            contract_config::ContractConfig,
            retry_client::{RetriableSuiClient, retriable_sui_client::LazySuiClientBuilder},
        },
        config::{WalletConfig, load_wallet_context_from_path},
        types::move_structs::NodeMetadata,
    };
    use walrus_utils::{backoff::ExponentialBackoffConfig, metrics::Registry};

    use super::*;

    pub(super) fn run(
        loaded_config: LoadedConfig,
        cleanup_storage: bool,
        config_loader: Arc<dyn ConfigLoader>,
    ) -> anyhow::Result<()> {
        let mut config = loaded_config.config;
        if cleanup_storage {
            let storage_path = &config.storage_path;

            match fs::remove_dir_all(storage_path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => {
                    return Err(e).context(format!(
                        "Failed to remove directory '{}'",
                        storage_path.display()
                    ));
                }
                _ => (),
            }
        }

        let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;
        let registry_clone = metrics_runtime.registry.clone();
        metrics_runtime
            .runtime
            .as_ref()
            .expect("storage node requires metrics to have their own runtime")
            .spawn(async move {
                registry_clone
                    .register(mysten_metrics::uptime_metric(
                        "walrus_node",
                        VERSION,
                        "walrus",
                    ))
                    .expect("metrics defined at compile time must be valid");
            });

        // Log the basic information about the node. Note that anything logged before the logging
        // runtime starts will not be seen by the runtime.
        tracing::info!(
            version = VERSION,
            network_kind = ?loaded_config.network_kind,
            config_path = ?loaded_config.config_path,
            "Walrus node configuration",
        );
        config.load_keys()?;
        tracing::info!(
            walrus.node.public_key = %config.protocol_key_pair().public(),
            "Walrus protocol public key",
        );
        let network_key_pair = config.network_key_pair().clone();
        tracing::info!(
            walrus.node.network_key = %network_key_pair.public(),
            "Walrus network public key",
        );
        tracing::info!(
            metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
        );

        utils::export_build_info(&metrics_runtime.registry, VERSION);
        if let Some(config) = config.sui.as_ref() {
            utils::export_contract_info(
                &metrics_runtime.registry,
                &config.contract_config.system_object,
                &config.contract_config.staking_object,
                WalletConfig::load_wallet(Some(&config.wallet_config), config.request_timeout)
                    .map(|wallet| wallet.active_address())
                    .ok(),
            );
        }

        let cancel_token = CancellationToken::new();
        let (exit_notifier, exit_listener) = oneshot::channel::<()>();

        let metrics_push_registry_clone = metrics_runtime.registry.clone();
        let metrics_push_runtime = match config.metrics_push.take() {
            Some(mut mc) => {
                mc.set_name_and_host_label(&config.name);
                mc.set_role_label(ServiceRole::StorageNode);
                let network_key_pair = network_key_pair.0.clone();
                let mp_config = EnableMetricsPush {
                    cancel: cancel_token.child_token(),
                    network_key_pair,
                    config: mc,
                };
                Some(MetricPushRuntime::start(
                    metrics_push_registry_clone,
                    mp_config,
                )?)
            }
            None => None,
        };

        let (event_manager, event_processor_runtime) = EventProcessorRuntime::start(
            config
                .sui
                .as_ref()
                .map(|config| config.into())
                .expect("Sui configuration must be present"),
            config.event_processor_config.clone(),
            &config.storage_path,
            &metrics_runtime.registry,
            cancel_token.child_token(),
            &config.db_config,
        )?;

        let node_runtime = StorageNodeRuntime::start(
            &config,
            metrics_runtime,
            exit_notifier,
            event_manager,
            cancel_token.child_token(),
            Some(config_loader),
        )?;

        monitor_runtimes(
            node_runtime,
            event_processor_runtime,
            metrics_push_runtime,
            exit_listener,
            cancel_token,
        )?;

        Ok(())
    }

    #[cfg(not(msim))]
    fn monitor_runtimes(
        mut node_runtime: StorageNodeRuntime,
        mut event_processor_runtime: EventProcessorRuntime,
        metrics_push_runtime: Option<MetricPushRuntime>,
        exit_listener: oneshot::Receiver<()>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let monitor_runtime = Runtime::new()?;
        monitor_runtime.block_on(async move {
            tokio::spawn(async move {
                let mut set = JoinSet::new();
                set.spawn_blocking(move || node_runtime.join());
                set.spawn_blocking(move || event_processor_runtime.join());
                if let Some(mut metrics_push_runtime) = metrics_push_runtime {
                    set.spawn_blocking(move || metrics_push_runtime.join());
                }
                tokio::select! {
                    _ = wait_until_terminated(exit_listener) => {
                        tracing::info!("received termination signal, shutting down...");
                    }
                    _ = set.join_next() => {
                        tracing::info!("runtime stopped successfully");
                    }
                }
                cancel_token.cancel();
                tracing::info!("cancellation token triggered, waiting for tasks to shut down...");

                // Drain remaining runtimes
                while set.join_next().await.is_some() {}
                tracing::info!("all runtimes have shut down");
            })
            .await
        })?;
        Ok(())
    }

    #[cfg(msim)]
    fn monitor_runtimes(
        mut node_runtime: StorageNodeRuntime,
        mut event_processor_runtime: EventProcessorRuntime,
        metrics_push_runtime: Option<MetricPushRuntime>,
        exit_listener: oneshot::Receiver<()>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let monitor_runtime = Runtime::new()?;
        monitor_runtime.block_on(async {
            tokio::spawn(async move { wait_until_terminated(exit_listener).await }).await
        })?;
        // Cancel the node runtime, if it is still executing.
        cancel_token.cancel();
        event_processor_runtime.join()?;
        // Wait for the node runtime to complete, may take a moment as
        // the REST-API waits for open connections to close before exiting.
        node_runtime.join()?;
        if let Some(mut metrics_push_runtime) = metrics_push_runtime {
            // Wait for metrics to flush
            metrics_push_runtime.join()?;
        }
        Ok(())
    }

    pub(super) fn generate_or_convert_key(
        output_path: &Path,
        key_type: KeyType,
        force: bool,
        format: KeyFormat,
        key_source: Option<&Path>,
    ) -> anyhow::Result<()> {
        walrus_core::ensure!(
            format != KeyFormat::Pkcs8 || key_type == KeyType::Network,
            "`--format=pkcs8` is only supported with `--key-type=network`"
        );

        if let Some(path) = key_source {
            print!("Converting {key_type} key pair from '{}'", path.display());
        } else {
            print!("Generating {key_type} key pair")
        }
        println!(" and writing it to '{}'", output_path.display());

        let key_string = match (key_type, format) {
            (KeyType::Network, KeyFormat::Pkcs8) => {
                NetworkKeyPair::to_pem(&load_or_generate_key(key_source, key_type)?)
            }

            (KeyType::Network, KeyFormat::Tagged) => {
                NetworkKeyPair::to_base64(&load_or_generate_key(key_source, key_type)?).into()
            }
            (KeyType::Protocol, _) => {
                ProtocolKeyPair::to_base64(&load_or_generate_key(key_source, key_type)?).into()
            }
        };

        write_key_to_file(output_path, force, &key_string)
    }

    fn load_or_generate_key<T>(
        key_source: Option<&Path>,
        key_type: KeyType,
    ) -> Result<TaggedKeyPair<T>, anyhow::Error>
    where
        TaggedKeyPair<T>: LoadsFromPath,
        T: SupportedKeyPair,
    {
        if let Some(path) = key_source {
            TaggedKeyPair::<T>::load(path).with_context(|| {
                format!(
                    "unable to load the input keyfile at '{}' as type '{}'",
                    path.display(),
                    key_type
                )
            })
        } else {
            Ok(TaggedKeyPair::<T>::generate())
        }
    }

    pub(super) fn keygen(
        path: &Path,
        key_type: KeyType,
        force: bool,
        format: KeyFormat,
    ) -> anyhow::Result<()> {
        generate_or_convert_key(path, key_type, force, format, None)
    }

    fn write_key_to_file(output_file: &Path, force: bool, contents: &str) -> anyhow::Result<()> {
        let mut file = create_file(output_file, force)
            .with_context(|| format!("Cannot create the keyfile '{}'", output_file.display()))?;

        file.write_all(contents.as_bytes())?;

        Ok(())
    }

    /// Register the node to the Sui contract.
    ///
    /// This function will update the config file with the new storage node capability object ID.
    /// Note that if the config file contains any configuration that matches the default values,
    /// the new config file may not contain it after adding the storage node capability object ID.
    #[tokio::main]
    pub(crate) async fn register_node(config_path: PathBuf, force: bool) -> anyhow::Result<()> {
        let mut config = StorageNodeConfig::load_config(&config_path)?.config;
        let contract_client = get_contract_client_from_node_config(&config).await?;

        if !force
            && (config.storage_node_cap.is_some()
                || !matches!(
                    contract_client
                        .read_client()
                        .get_address_capability_object(contract_client.address())
                        .await,
                    Ok(None)
                ))
        {
            bail!(
                "storage node capability object already exists, \
                use the '--force' option to overwrite it"
            );
        }

        config.load_keys()?;

        // If we have an IP address, use a SocketAddr to get the string representation
        // as IPv6 addresses are enclosed in square brackets.
        ensure!(
            !config.public_host.contains(':'),
            "DNS names must not contain ':'; the public port can be specified in the config file \
                with the `public_port` parameter."
        );
        let registration_params = config.to_registration_params();

        // Uses the Sui wallet configuration in the storage node config to register the node.
        let proof_of_possession = walrus_sui::utils::generate_proof_of_possession(
            config.protocol_key_pair(),
            &contract_client,
            contract_client.current_epoch().await?,
        );

        let node_capability = contract_client
            .register_candidate(&registration_params, proof_of_possession)
            .await?;

        println!("Successfully registered storage node:",);
        println!("      Capability object ID: {}", node_capability.id);
        println!("      Node ID: {}", node_capability.node_id);

        // Update the config in `config_path` with the new storage node capability object ID.
        config.storage_node_cap = Some(node_capability.id);
        write_config_to_file(&config, &config_path, true)?;

        Ok(())
    }

    pub(crate) fn generate_config(
        PathArgs {
            config_path,
            storage_path,
            protocol_key_path,
            network_key_path,
            wallet_config,
        }: PathArgs,
        ConfigArgs {
            system_object,
            staking_object,
            node_capacity,
            public_host,
            sui_rpc,
            public_port,
            rest_api_address,
            metrics_address,
            metrics_push_url,
            certificate_path,
            gas_budget,
            storage_price,
            write_price,
            price_currency,
            commission_rate,
            name,
            image_url,
            project_url,
            description,
            rpc_fallback_config_args,
            additional_rpc_endpoints,
        }: ConfigArgs,
        force: bool,
    ) -> anyhow::Result<StorageNodeConfig> {
        let sui_rpc = if let Some(rpc) = sui_rpc {
            rpc
        } else {
            tracing::debug!(
                "getting Sui RPC URL from wallet at '{}'",
                wallet_config.display()
            );
            let wallet = load_wallet_context_from_path(Some(&wallet_config), None)
                .context("Reading Sui wallet failed")?;
            wallet.get_rpc_url().to_string()
        };

        // Do a minor sanity check that the user has not included a port in the hostname.
        ensure!(
            !public_host.contains(':'),
            "DNS names must not contain ':'; to specify a port different from the default, use the \
                '--public-port' option."
        );

        // Check that the name does not exceed the maximum length.
        ensure!(
            name.len() <= MAX_NODE_NAME_LENGTH,
            "name must not exceed {} characters",
            MAX_NODE_NAME_LENGTH
        );

        let system_object = system_object.unwrap_or_else(|| {
            tracing::warn!(
                "no system object provided; \
                please replace the dummy value in the config file manually"
            );
            ObjectID::ZERO
        });
        let staking_object = staking_object.unwrap_or_else(|| {
            tracing::warn!(
                "no staking object provided; \
                please replace the dummy value in the config file manually"
            );
            ObjectID::ZERO
        });
        let contract_config = ContractConfig::new(system_object, staking_object);
        let metadata = NodeMetadata::new(image_url, project_url, description);
        let metrics_push = metrics_push_url.map(MetricsPushConfig::new_for_url);

        let config = StorageNodeConfig {
            storage_path,
            protocol_key_pair: PathOrInPlace::from_path(protocol_key_path),
            network_key_pair: PathOrInPlace::from_path(network_key_path),
            public_host,
            public_port,
            rest_api_address,
            metrics_address,
            sui: Some(SuiConfig {
                rpc: sui_rpc,
                contract_config,
                wallet_config: WalletConfig::from_path(&wallet_config),
                event_polling_interval: config::defaults::polling_interval(),
                backoff_config: ExponentialBackoffConfig::default(),
                gas_budget,
                rpc_fallback_config: rpc_fallback_config_args
                    .clone()
                    .and_then(|args| args.to_config()),
                additional_rpc_endpoints,
                request_timeout: None,
            }),
            tls: TlsConfig {
                certificate_path,
                ..Default::default()
            },
            voting_params: VotingParamsConfig {
                voting_prices: {
                    let prices = VotingPricesConfig {
                        storage_price,
                        write_price,
                    };
                    match price_currency {
                        PriceCurrency::Frost => VotingPrices::FROST(prices),
                        PriceCurrency::NanoUsd => VotingPrices::NanoUsd(prices),
                    }
                },
                node_capacity: node_capacity.as_u64(),
            },
            commission_rate,
            name,
            metadata,
            metrics_push,
            ..Default::default()
        };

        write_config_to_file(&config, &config_path, force)?;

        Ok(config)
    }

    #[tokio::main]
    pub async fn catchup(
        CatchupArgs {
            db_path,
            system_object_id,
            staking_object_id,
            sui_rpc_url,
            checkpoint_request_timeout,
            runtime_duration,
            event_stream_catchup_min_checkpoint_lag,
            rpc_fallback_config_args,
            sampled_tracing_interval,
        }: CatchupArgs,
    ) -> anyhow::Result<()> {
        let event_processor_config = EventProcessorConfig {
            checkpoint_request_timeout,
            event_stream_catchup_min_checkpoint_lag,
            sampled_tracing_interval,
            ..Default::default()
        };

        // Since this is a manual catchup, we use a single RPC address.
        let runtime_config = EventProcessorRuntimeConfig {
            rpc_addresses: vec![sui_rpc_url.clone()],
            event_polling_interval: Duration::from_secs(1),
            db_path: db_path.clone(),
            rpc_fallback_config: rpc_fallback_config_args.and_then(|args| args.to_config()),
            db_config: DatabaseConfig::default(),
        };

        let retriable_sui_client = RetriableSuiClient::new(
            vec![LazySuiClientBuilder::new(sui_rpc_url, None)],
            ExponentialBackoffConfig::default(),
        )?;
        let contract_config = ContractConfig::new(system_object_id, staking_object_id);
        let sui_read_client =
            SuiReadClient::new(retriable_sui_client.clone(), &contract_config).await?;
        let system_pkg_id = sui_read_client.walrus_package_id();

        let system_config = SystemConfig::new(system_pkg_id, system_object_id, staking_object_id);
        let event_processor = EventProcessor::new(
            &event_processor_config,
            runtime_config,
            system_config,
            &Registry::default(),
        )
        .await?;
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        tokio::spawn(async move {
            event_processor
                .start(cancel_token_clone)
                .await
                .expect("event processor should not fail");
        });

        tokio::time::sleep(runtime_duration).await;

        cancel_token.cancel();
        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn restore(
        RestoreArgs {
            db_checkpoint_path,
            db_path,
            wal_path,
            checkpoint_id,
        }: RestoreArgs,
    ) -> anyhow::Result<()> {
        DbCheckpointManager::restore_from_backup(
            &db_checkpoint_path,
            &db_path,
            wal_path.as_deref(),
            checkpoint_id,
        )?;

        let target_checkpoint = checkpoint_id.map_or("latest".to_string(), |id| id.to_string());
        println!(
            "Restored from {target_checkpoint} successfully. The node must be restarted for \
            changes to take effect."
        );

        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn list_db_checkpoints(
        ListDbCheckpointArgs { path }: ListDbCheckpointArgs,
    ) -> anyhow::Result<()> {
        use rocksdb::{
            Env,
            backup::{BackupEngine, BackupEngineOptions},
        };
        use walrus_service::DisplayableDbCheckpointInfo;

        // Ensure the directory exists
        if !path.exists() {
            return Err(anyhow::anyhow!(
                "Checkpoint directory does not exist: {}",
                path.display()
            ));
        }

        // Create BackupEngine directly
        let env = Env::new()
            .map_err(|e| anyhow::anyhow!("Failed to create RocksDB environment: {}", e))?;
        let backup_opts = BackupEngineOptions::new(&path)
            .map_err(|e| anyhow::anyhow!("Failed to create backup options: {}", e))?;
        let engine = BackupEngine::open(&backup_opts, &env)
            .map_err(|e| anyhow::anyhow!("Failed to open backup engine: {}", e))?;

        let backup_info = engine.get_backup_info();

        if backup_info.is_empty() {
            println!("No checkpoints found in {}", path.display());
        } else {
            let checkpoints: Vec<DisplayableDbCheckpointInfo> =
                backup_info.into_iter().map(|info| info.into()).collect();

            println!(
                "Available checkpoints in {}:\n{}",
                path.display(),
                checkpoints
                    .iter()
                    .map(|b| format!("  {b}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn setup(
        SetupArgs {
            config_directory,
            storage_path,
            sui_network,
            use_faucet,
            faucet_timeout,
            config_args,
            force,
            network_key_path,
            registering_third_party,
            registration_epoch,
        }: SetupArgs,
    ) -> anyhow::Result<()> {
        fs::create_dir_all(&config_directory).context(format!(
            "failed to create the config directory '{}'",
            config_directory.display()
        ))?;
        if !force && config_directory.read_dir()?.next().is_some() {
            bail!(
                "the specified configuration directory '{}' is not empty; \
                use the '--force' option to overwrite existing files",
                config_directory.display()
            );
        }
        let config_path = config_directory.join("walrus-node.yaml");
        let protocol_key_path = config_directory.join("protocol.key");
        let wallet_config = config_directory.join("sui_config.yaml");
        ensure!(
            config_directory.is_dir(),
            "The directory '{}' does not exist.",
            config_directory.display()
        );

        keygen(
            &protocol_key_path,
            KeyType::Protocol,
            true,
            KeyFormat::Tagged,
        )?;
        let network_key_path = if let Some(network_key_path) = network_key_path {
            network_key_path
        } else {
            let network_key_path = config_directory.join("network.key");
            keygen(&network_key_path, KeyType::Network, true, KeyFormat::Pkcs8)?;
            network_key_path
        };

        let wallet_address =
            utils::generate_sui_wallet(sui_network, &wallet_config, use_faucet, faucet_timeout)
                .await?;
        println!("Successfully generated a new Sui wallet with address {wallet_address}");

        let mut config = generate_config(
            PathArgs {
                config_path,
                storage_path,
                protocol_key_path,
                network_key_path,
                wallet_config,
            },
            config_args,
            force,
        )?;

        if let Some(registering_third_party) = registering_third_party {
            let registration_params_path = config_directory.join("registration-params.yaml");
            config.load_keys()?;
            let proof_of_possession = walrus_sui::utils::generate_proof_of_possession_for_address(
                config.protocol_key_pair(),
                registering_third_party,
                registration_epoch,
            );
            let registration_params = NodeRegistrationParamsForThirdPartyRegistration {
                node_registration_params: config.to_registration_params(),
                proof_of_possession,
                wallet_address,
            };
            let yaml_config = serde_yaml::to_string(&registration_params)
                .context("failed to serialize registration parameters to YAML")?;
            let mut file = create_file(&registration_params_path, force).with_context(|| {
                format!(
                    "failed to create the registration parameters file '{}'",
                    registration_params_path.display()
                )
            })?;
            file.write_all(yaml_config.as_bytes()).context(format!(
                "failed to write the generated registration parameters to '{}'",
                registration_params_path.display()
            ))?;
        }

        Ok(())
    }

    /// Creates a new file at the given path. If force is true, overwrites any existing file.
    /// Otherwise, fails if the file already exists.
    fn create_file(path: &Path, force: bool) -> Result<File, std::io::Error> {
        if force {
            File::create(path)
        } else {
            File::create_new(path)
        }
    }

    /// Writes the given storage node config to the specified file.
    fn write_config_to_file(
        config: &StorageNodeConfig,
        config_path: &Path,
        force: bool,
    ) -> anyhow::Result<()> {
        let yaml_config =
            serde_yaml::to_string(&config).context("failed to serialize configuration to YAML")?;
        let mut file = create_file(config_path, force).with_context(|| {
            format!(
                "failed to create the config file '{}'",
                config_path.display()
            )
        })?;
        file.write_all(yaml_config.as_bytes()).context(format!(
            "failed to write the generated configuration to '{}'",
            config_path.display()
        ))?;
        Ok(())
    }

    /// Handle local admin commands.
    #[tokio::main]
    #[cfg(unix)]
    pub(crate) async fn handle_admin_command(
        command: AdminCommands,
        socket_path: PathBuf,
    ) -> anyhow::Result<()> {
        // Connect to the socket.
        let socket = UnixStream::connect(&socket_path).await.context(format!(
            "failed to connect to local admin socket at '{}'",
            socket_path.display()
        ))?;
        let (reader, mut writer) = tokio::io::split(socket);

        // Serialize and send the AdminCommands.
        let cmd_json = serde_json::to_string(&command)?;
        writer.write_all(cmd_json.as_bytes()).await?;
        writer.write_all(b"\n").await?;

        // Wait for response.
        let mut buf_reader = BufReader::new(reader);
        let mut response = String::new();
        buf_reader.read_line(&mut response).await?;

        // Parse and print response.
        match serde_json::from_str::<AdminCommandResponse>(&response) {
            Ok(resp) => {
                if resp.success {
                    println!("{}", resp.message);
                } else {
                    eprintln!("Error: {}", resp.message);
                    std::process::exit(1);
                }
            }
            Err(_) => {
                eprintln!("Error: Invalid response format");
                std::process::exit(1);
            }
        }

        Ok(())
    }

    #[cfg(windows)]
    pub(crate) fn handle_admin_command(
        _command: AdminCommands,
        _socket_path: PathBuf,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Admin commands via Unix domain sockets are not supported on Windows")
    }
}

/// Creates a [`SuiContractClient`] from the Sui config in the provided storage node config.
async fn get_contract_client_from_node_config(
    storage_config: &StorageNodeConfig,
) -> anyhow::Result<SuiContractClient> {
    let Some(ref node_wallet_config) = storage_config.sui else {
        bail!("storage config does not contain Sui wallet configuration");
    };
    Ok(node_wallet_config.new_contract_client(None).await?)
}

struct StorageNodeRuntime {
    walrus_node_handle: JoinHandle<anyhow::Result<()>>,
    rest_api_handle: JoinHandle<Result<(), anyhow::Error>>,
    // Preserve the metrics runtime to keep the runtime alive
    metrics_runtime: MetricsAndLoggingRuntime,
    // INV: Runtime must be dropped last
    runtime: Runtime,
    /// Path to the local admin socket.
    local_admin_socket_handle: Option<JoinHandle<()>>,
}

impl StorageNodeRuntime {
    fn start(
        node_config: &StorageNodeConfig,
        metrics_runtime: MetricsAndLoggingRuntime,
        exit_notifier: oneshot::Sender<()>,
        event_manager: Box<dyn EventManager>,
        cancel_token: CancellationToken,
        config_loader: Option<Arc<dyn ConfigLoader>>,
    ) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("walrus-node-runtime")
            .enable_all()
            .max_blocking_threads(node_config.thread_pool.max_blocking_io_threads)
            .build()
            .expect("walrus-node runtime creation must succeed");
        let _guard = runtime.enter();
        let tracing_handle = WalrusTracingHandle(metrics_runtime.tracing_handle.clone());
        let walrus_node = Arc::new(
            runtime.block_on(
                StorageNode::builder()
                    .with_system_event_manager(event_manager)
                    .with_config_loader(config_loader)
                    .build(node_config, metrics_runtime.registry.clone()),
            )?,
        );

        let walrus_node_clone = walrus_node.clone();
        let walrus_node_cancel_token = cancel_token.child_token();
        let walrus_node_handle = tokio::spawn(async move {
            let cancel_token = walrus_node_cancel_token.clone();
            let result = walrus_node_clone.run(walrus_node_cancel_token).await;

            if exit_notifier.send(()).is_err() && !cancel_token.is_cancelled() {
                tracing::warn!(
                    "unable to notify that the node has exited, but shutdown is not in progress?"
                )
            }
            if let Err(ref error) = result
                && error.downcast_ref::<SyncNodeConfigError>().is_none()
            {
                // Only log an error if it is not due to to the config sync (as those are handled
                // separately).
                tracing::error!(?error, "storage node exited with an error");
            }

            result
        });

        let checkpoint_manager = walrus_node.checkpoint_manager();
        let admin_cancel_token = cancel_token.child_token();
        let rest_api = RestApiServer::new(
            walrus_node,
            cancel_token.child_token(),
            RestApiConfig::from(node_config),
            &metrics_runtime.registry,
        );
        let rest_api_handle = tokio::spawn(async move {
            let result = rest_api
                .run()
                .await
                .inspect_err(|error| tracing::error!(?error, "REST API exited with an error"));

            if !cancel_token.is_cancelled() {
                tracing::info!("signalling the storage node to shutdown");
                cancel_token.cancel();
            }

            result
        });
        tracing::info!("started REST API on {}", node_config.rest_api_address);

        let local_admin_socket_handle = Self::start_admin_socket(
            AdminArgs {
                checkpoint_manager,
                admin_socket_path: node_config.admin_socket_path.clone(),
                tracing_handle,
            },
            admin_cancel_token,
        )?;

        Ok(Self {
            walrus_node_handle,
            rest_api_handle,
            local_admin_socket_handle,
            metrics_runtime,
            runtime,
        })
    }

    fn join(&mut self) -> Result<(), anyhow::Error> {
        tracing::debug!("waiting for the REST API to shutdown...");
        let _ = self.runtime.block_on(&mut self.rest_api_handle)?;
        tracing::debug!("waiting for the storage node to shutdown...");
        let _ = self.runtime.block_on(&mut self.walrus_node_handle)?;
        if let Some(handle) = self.local_admin_socket_handle.take() {
            handle.abort();
        }

        // Shutdown the metrics runtime.
        if let Some(runtime) = self.metrics_runtime.runtime.take() {
            runtime.shutdown_background();
        }

        Ok(())
    }

    #[cfg(unix)]
    fn start_admin_socket(
        admin_args: AdminArgs,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Option<JoinHandle<()>>> {
        if admin_args.checkpoint_manager.is_none() {
            tracing::warn!("checkpoint manager is not initialized, skipping local admin socket");
            return Ok(None);
        }
        let Some(socket_path) = admin_args.admin_socket_path.clone() else {
            tracing::warn!("local admin socket path is not specified, skipping local admin socket");
            return Ok(None);
        };

        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let _ = std::fs::remove_file(&socket_path);

        let listener = UnixListener::bind(&socket_path)?;

        // Set the permissions to 600 to ensure only the owner can access the socket.
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;

        let handle = tokio::spawn(async move {
            tracing::info!("local admin socket listening on {}", socket_path.display());

            loop {
                tokio::select! {
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            let args = admin_args.clone();
                            tokio::spawn(async move {
                                handle_connection(stream, args).await;
                            });
                        }
                    }
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                }
            }

            let _ = std::fs::remove_file(socket_path);
            tracing::info!("local admin socket stopped");
        });

        Ok(Some(handle))
    }

    #[cfg(windows)]
    fn start_admin_socket(
        _admin_args: AdminArgs,
        _cancel_token: CancellationToken,
    ) -> anyhow::Result<Option<JoinHandle<()>>> {
        tracing::warn!("Unix domain sockets are not supported on Windows");
        Ok(None)
    }
}

/// Handle log level commands from admin socket.
fn handle_log_level_command(level: String, args: &AdminArgs) -> AdminCommandResponse {
    match args.tracing_handle.update_log(level.as_str()) {
        Ok(_) => AdminCommandResponse {
            success: true,
            message: format!("Log level updated successfully to: {level}"),
        },
        Err(e) => AdminCommandResponse {
            success: false,
            message: format!("Failed to update log level: {e}"),
        },
    }
}

/// Handle checkpoint commands from admin socket.
async fn handle_checkpoint_command(
    command: CheckpointCommands,
    args: &AdminArgs,
) -> AdminCommandResponse {
    let Some(manager) = args.checkpoint_manager.as_ref() else {
        return AdminCommandResponse {
            success: false,
            message: "Checkpoint manager is not initialized".to_string(),
        };
    };
    match command {
        CheckpointCommands::Create { path, delay_secs } => {
            match manager
                .schedule_and_wait_for_db_checkpoint_creation(
                    path.as_deref(),
                    delay_secs.map(std::time::Duration::from_secs),
                )
                .await
            {
                Ok(_) => AdminCommandResponse {
                    success: true,
                    message: "Checkpoint created successfully".to_string(),
                },
                Err(e) => AdminCommandResponse {
                    success: false,
                    message: format!("Failed to create checkpoint: {e:?}"),
                },
            }
        }
        CheckpointCommands::Cancel => {
            let result = manager.cancel_db_checkpoint_creation().await;
            match result {
                Ok(true) => AdminCommandResponse {
                    success: true,
                    message: "Checkpoint creation cancelled".to_string(),
                },
                Ok(false) => AdminCommandResponse {
                    success: true,
                    message: "No backup was in progress".to_string(),
                },
                Err(e) => AdminCommandResponse {
                    success: false,
                    message: format!("Failed to cancel checkpoint creation: {e}"),
                },
            }
        }
    }
}

/// Handles a connection to the admin socket.
#[cfg(unix)]
async fn handle_connection(stream: UnixStream, args: AdminArgs) {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
        let response = match serde_json::from_str::<AdminCommands>(&line) {
            Ok(AdminCommands::Checkpoint { command }) => {
                handle_checkpoint_command(command, &args).await
            }
            Ok(AdminCommands::LogLevel { level }) => handle_log_level_command(level, &args),
            Err(e) => AdminCommandResponse {
                success: false,
                message: format!("Failed to parse command: {e}"),
            },
        };

        // Serialize and send response.
        if let Ok(json) = serde_json::to_string(&response) {
            let _ = writer.write_all((json + "\n").as_bytes()).await;
        }

        line.clear();
    }
}

#[cfg(test)]
mod tests {
    use config::LoadsFromPath;
    use tempfile::TempDir;
    use walrus_test_utils::{Result, param_test};

    use super::*;

    #[test]
    fn generate_key_pair_saves_base64_key_to_file() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        commands::keygen(&filename, KeyType::Protocol, false, KeyFormat::Tagged)?;

        let file_content = std::fs::read_to_string(filename)
            .expect("a file should have been created with the key");

        assert_eq!(
            file_content.len(),
            44,
            "33-byte key should be 44 characters in base64"
        );

        let _: ProtocolKeyPair = file_content
            .parse()
            .expect("a protocol keypair must be parseable from the the file's contents");

        Ok(())
    }

    #[test]
    fn generate_key_pair_does_not_overwrite_files() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        std::fs::write(filename.as_path(), "original-file-contents".as_bytes())?;

        commands::keygen(&filename, KeyType::Protocol, false, KeyFormat::Tagged)
            .expect_err("must fail as the file already exists");

        let file_content = std::fs::read_to_string(filename).expect("the file should still exist");
        assert_eq!(file_content, "original-file-contents");

        Ok(())
    }

    #[test]
    fn generate_key_pair_with_force_overwrites_files() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        std::fs::write(filename.as_path(), "original-file-contents".as_bytes())?;

        commands::keygen(&filename, KeyType::Protocol, true, KeyFormat::Tagged)?;

        let file_content = std::fs::read_to_string(filename).expect("the file should still exist");

        let _: ProtocolKeyPair = file_content
            .parse()
            .expect("a protocol keypair must be parseable from the the file's contents");

        Ok(())
    }

    #[test]
    fn generate_key_pair_errs_for_unsupported_format() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        commands::keygen(&filename, KeyType::Protocol, false, KeyFormat::Pkcs8)
            .expect_err("pkcs8 should be unsupported for protocol keys");

        Ok(())
    }

    #[test]
    fn generate_key_pair_saves_pkcs8_to_file() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.pem");

        commands::keygen(&filename, KeyType::Network, false, KeyFormat::Pkcs8)?;

        let file_content = std::fs::read_to_string(&filename)
            .expect("a file should have been created with the key");

        assert!(file_content.starts_with("-----BEGIN PRIVATE KEY-----"));

        NetworkKeyPair::load(&filename)
            .expect("network keypair must be parseable from the the file's contents");

        Ok(())
    }

    param_test! {
        converts_key_type -> Result<()>: [
            network_tagged_to_tagged: (KeyType::Network, KeyFormat::Tagged, KeyFormat::Tagged),
            network_tagged_to_pkcs8: (KeyType::Network, KeyFormat::Tagged, KeyFormat::Pkcs8),
            network_pkcs8_to_tagged: (KeyType::Network, KeyFormat::Pkcs8, KeyFormat::Tagged),
            protocol_tagged_to_tagged: (KeyType::Protocol, KeyFormat::Tagged, KeyFormat::Tagged)
        ]
    }
    fn converts_key_type(
        key_type: KeyType,
        input_format: KeyFormat,
        output_format: KeyFormat,
    ) -> Result<()> {
        let dir = TempDir::new()?;
        let input_file = dir.path().join("input.key");
        let output_file = dir.path().join("output.key");

        // Create the input keyfile.
        commands::keygen(&input_file, key_type, false, input_format)?;

        // Convert the file to the new format.
        commands::generate_or_convert_key(
            &output_file,
            key_type,
            false,
            output_format,
            Some(&input_file),
        )?;

        assert_key_format(&output_file, output_format);
        assert_valid_key_of_type(&output_file, key_type);

        Ok(())
    }

    fn assert_key_format(path: &Path, format: KeyFormat) {
        let pkcs8_header = "-----BEGIN PRIVATE KEY-----";
        let file_content =
            std::fs::read_to_string(path).expect("a file should have been created with the key");

        match format {
            KeyFormat::Tagged => assert!(!file_content.starts_with(pkcs8_header)),
            KeyFormat::Pkcs8 => assert!(file_content.starts_with(pkcs8_header)),
        }
    }

    fn assert_valid_key_of_type(path: &Path, key_type: KeyType) {
        match key_type {
            KeyType::Protocol => {
                ProtocolKeyPair::load(path).expect("file contents should be a valid protoocl key");
            }
            KeyType::Network => {
                NetworkKeyPair::load(path).expect("file contents should be a valid netwrk key");
            }
        }
    }
}
