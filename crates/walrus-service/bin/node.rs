// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Storage Node entry point.

use std::{
    fmt::Display,
    fs,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use config::PathOrInPlace;
use fs::File;
use humantime::Duration;
use prometheus::Registry;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::{ObjectID, SuiAddress};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::{
    keys::{NetworkKeyPair, ProtocolKeyPair},
    Epoch,
};
use walrus_service::{
    common::config::SuiConfig,
    node::{
        config::{self, defaults::REST_API_PORT, StorageNodeConfig},
        events::event_processor_runtime::EventProcessorRuntime,
        server::{RestApiConfig, RestApiServer},
        system_events::EventManager,
        StorageNode,
    },
    utils::{
        self,
        version,
        wait_until_terminated,
        ByteCount,
        EnableMetricsPush,
        LoadConfig as _,
        MetricPushRuntime,
        MetricsAndLoggingRuntime,
    },
};
use walrus_sui::{client::SuiContractClient, types::move_structs::VotingParams, utils::SuiNetwork};

const VERSION: &str = version!();

/// Manage and run a Walrus storage node.
#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[derive(Debug)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
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
        #[clap(long)]
        config_path: PathBuf,
    },

    /// Run a storage node with the provided configuration.
    Run {
        /// Path to the Walrus node configuration file.
        #[clap(long)]
        config_path: PathBuf,
        /// Whether to cleanup the storage directory before starting the node.
        #[clap(long, action, default_value_t = false)]
        cleanup_storage: bool,
    },

    /// Generate a new key for use with the Walrus protocol, and writes it to a file.
    KeyGen {
        /// Path to the file at which the key will be created. If the file already exists, it is
        /// not overwritten and the operation will fail unless the `--force` option is provided.
        /// [default: ./<KEY_TYPE>.key]
        #[clap(long)]
        out: Option<PathBuf>,
        /// Which type of key to generate. Valid options are 'protocol' and 'network'.
        ///
        /// The protocol key is used to sign Walrus protocol messages.
        /// The network key is used to authenticate nodes in network communication.
        #[clap(long)]
        key_type: KeyType,
        /// Overwrite existing files.
        #[clap(long)]
        force: bool,
    },

    /// Generate a new node configuration.
    GenerateConfig {
        #[clap(flatten)]
        path_args: PathArgs,
        #[clap(flatten)]
        config_args: ConfigArgs,
        /// Overwrite existing files.
        #[clap(long)]
        force: bool,
    },

    /// Repair a corrupted RocksDB database due to non-clean shutdowns.
    /// Hidden command for emergency use only.
    #[clap(hide = true)]
    RepairDb {
        #[clap(long)]
        /// Path to the RocksDB database directory.
        db_path: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
enum KeyType {
    Protocol,
    Network,
}

impl FromStr for KeyType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_ascii_lowercase().as_str() {
            "protocol" => Self::Protocol,
            "network" => Self::Network,
            _ => bail!("invalid key type provided"),
        })
    }
}

impl Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                KeyType::Protocol => "protocol",
                KeyType::Network => "network",
            }
        )
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

#[derive(Debug, Clone, clap::Args)]
struct SetupArgs {
    #[clap(long)]
    /// The path to the directory in which to set up wallet and node configuration.
    config_directory: PathBuf,
    #[clap(long)]
    /// The path where the Walrus database will be stored.
    storage_path: PathBuf,
    /// Sui network for which the config is generated.
    ///
    /// Available options are `devnet`, `testnet`, and `localnet`.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// Whether to attempt to get SUI tokens from the faucet.
    #[clap(long, action)]
    use_faucet: bool,
    /// Timeout for the faucet call.
    #[clap(long, default_value = "1min", requires = "use_faucet")]
    faucet_timeout: Duration,
    #[clap(flatten)]
    config_args: ConfigArgs,
    /// Overwrite existing files.
    #[clap(long)]
    force: bool,
    /// The wallet address of the third party that will register the node.
    ///
    /// If this is set, a YAML file is generated that can be used to register the node by a
    /// third party.
    #[clap(long)]
    registering_third_party: Option<SuiAddress>,
    /// The epoch at which the node will be registered.
    #[clap(long, requires = "registering_third_party", default_value_t = 0)]
    registration_epoch: Epoch,
}

#[derive(Debug, Clone, clap::Args)]
struct ConfigArgs {
    #[clap(long)]
    /// Object ID of the Walrus system object. If not provided, a dummy value is used and the
    /// system object needs to be manually added to the configuration file at a later time.
    system_object: Option<ObjectID>,
    #[clap(long)]
    /// Object ID of the Walrus staking object. If not provided, a dummy value is used and the
    /// staking object needs to be manually added to the configuration file at a later time.
    staking_object: Option<ObjectID>,
    #[clap(long)]
    /// Initial storage capacity of this node in bytes.
    ///
    /// The value can either by unitless; have suffixes for powers of 1000, such as (B),
    /// kilobytes (K), etc.; or have suffixes for the IEC units such as kibibytes (Ki),
    /// mebibytes (Mi), etc.
    node_capacity: ByteCount,
    #[clap(long)]
    /// The host name or public IP address of the node.
    public_host: String,
    /// The name of the storage node used in the registration.
    #[clap(long)]
    name: String,
    // ***************************
    //   Optional fields below
    // ***************************
    #[clap(long)]
    /// HTTP URL of the Sui full-node RPC endpoint (including scheme and port) to use for event
    /// processing.
    ///
    /// If not provided, the RPC node from the wallet's active environment will be used.
    sui_rpc: Option<String>,
    #[clap(long, action)]
    /// Use the legacy event provider instead of the standard checkpoint-based event processor.
    use_legacy_event_provider: bool,
    #[clap(long, action)]
    /// Disable event blob writer
    disable_event_blob_writer: bool,
    #[clap(long, default_value_t = REST_API_PORT)]
    /// The port on which the storage node will serve requests.
    public_port: u16,
    #[clap(long, default_value_t = config::defaults::rest_api_address())]
    /// Socket address on which the REST API listens.
    rest_api_address: SocketAddr,
    #[clap(long, default_value_t = config::defaults::metrics_address())]
    /// Socket address on which the Prometheus server should export its metrics.
    metrics_address: SocketAddr,
    #[clap(long, default_value_t = config::defaults::gas_budget())]
    /// Gas budget for transactions.
    gas_budget: u64,
    #[clap(long, default_value_t = config::defaults::storage_price())]
    /// Initial vote for the storage price in FROST per MiB per epoch.
    storage_price: u64,
    #[clap(long, default_value_t = config::defaults::write_price())]
    /// Initial vote for the write price in FROST per MiB.
    write_price: u64,
    #[clap(long, default_value_t = 0)]
    /// The commission rate of the storage node, in basis points.
    commission_rate: u16,
    /// The image URL of the storage node.
    #[clap(long, default_value = "")]
    image_url: String,
    /// The project URL of the storage node.
    #[clap(long, default_value = "")]
    project_url: String,
    /// The description of the storage node.
    #[clap(long, default_value = "")]
    description: String,
}

#[derive(Debug, Clone, clap::Args)]
struct PathArgs {
    #[clap(long)]
    /// The output path for the generated configuration file. If the file already exists, it is
    /// not overwritten and the operation will fail unless the `--force` option is provided.
    config_path: PathBuf,
    #[clap(long)]
    /// The path where the Walrus database will be stored.
    storage_path: PathBuf,
    #[clap(long)]
    /// The path to the key pair used in Walrus protocol messages.
    protocol_key_path: PathBuf,
    #[clap(long)]
    /// The path to the key pair used to authenticate nodes in network communication.
    network_key_path: PathBuf,
    #[clap(long)]
    /// Location of the node's wallet config.
    wallet_config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !matches!(args.command, Commands::Run { .. }) {
        utils::init_tracing_subscriber()?;
    }

    match args.command {
        Commands::Setup(setup_args) => commands::setup(setup_args)?,

        Commands::Register { config_path } => commands::register_node(config_path)?,

        Commands::Run {
            config_path,
            cleanup_storage,
        } => commands::run(StorageNodeConfig::load(config_path)?, cleanup_storage)?,

        Commands::KeyGen {
            out,
            key_type,
            force,
        } => commands::keygen(
            out.as_deref()
                .unwrap_or_else(|| Path::new(key_type.default_filename())),
            key_type,
            force,
        )?,

        Commands::GenerateConfig {
            path_args,
            config_args,
            force,
        } => {
            commands::generate_config(path_args, config_args, force)?;
        }

        Commands::RepairDb { db_path } => commands::repair_db(db_path)?,
    }
    Ok(())
}

mod commands {
    use config::NodeRegistrationParamsForThirdPartyRegistration;
    use rocksdb::{Options, DB};
    #[cfg(not(msim))]
    use tokio::task::JoinSet;
    use walrus_core::ensure;
    use walrus_service::utils;
    use walrus_sui::{
        client::{contract_config::ContractConfig, ReadClient as _},
        types::NodeMetadata,
    };
    use walrus_utils::backoff::ExponentialBackoffConfig;

    use super::*;

    pub(super) fn run(mut config: StorageNodeConfig, cleanup_storage: bool) -> anyhow::Result<()> {
        if cleanup_storage {
            let storage_path = &config.storage_path;

            match fs::remove_dir_all(storage_path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => {
                    return Err(e).context(format!(
                        "Failed to remove directory '{}'",
                        storage_path.display()
                    ))
                }
                _ => (),
            }
        }

        let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;
        let registry_clone = metrics_runtime.registry.clone();
        metrics_runtime.runtime.spawn(async move {
            registry_clone
                .register(mysten_metrics::uptime_metric(
                    "walrus_node",
                    VERSION,
                    "walrus",
                ))
                .unwrap();
        });

        tracing::info!(version = VERSION, "Walrus binary version");
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
                utils::load_wallet_context(&Some(config.wallet_config.clone()))
                    .and_then(|mut wallet| wallet.active_address())
                    .ok(),
            );
        }

        let cancel_token = CancellationToken::new();
        let (exit_notifier, exit_listener) = oneshot::channel::<()>();

        let (event_manager, event_processor_runtime) = EventProcessorRuntime::start(
            config
                .sui
                .as_ref()
                .map(|config| config.into())
                .expect("SUI configuration must be present"),
            config.event_processor_config.clone(),
            config.use_legacy_event_provider,
            &config.storage_path,
            &metrics_runtime.registry,
            cancel_token.child_token(),
        )?;

        let metrics_push_registry_clone = metrics_runtime.registry.clone();
        let metrics_push_runtime = match config.metrics_push.take() {
            Some(mut mc) => {
                mc.set_name(&config.name);
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

        let node_runtime = StorageNodeRuntime::start(
            &config,
            metrics_runtime.registry.clone(),
            exit_notifier,
            event_manager,
            cancel_token.child_token(),
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
        monitor_runtime.block_on(async {
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

    pub(super) fn keygen(path: &Path, key_type: KeyType, force: bool) -> anyhow::Result<()> {
        println!(
            "Generating {key_type} key pair and writing it to '{}'",
            path.display()
        );
        let mut file = create_file(path, force)
            .with_context(|| format!("Cannot create the keyfile '{}'", path.display()))?;

        file.write_all(
            match key_type {
                KeyType::Protocol => ProtocolKeyPair::generate().to_base64(),
                KeyType::Network => NetworkKeyPair::generate().to_base64(),
            }
            .as_bytes(),
        )?;

        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn register_node(config_path: PathBuf) -> anyhow::Result<()> {
        let mut config = StorageNodeConfig::load(&config_path)?;

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
        let contract_client = get_contract_client_from_node_config(&config).await?;
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
            use_legacy_event_provider,
            disable_event_blob_writer,
            public_port,
            rest_api_address,
            metrics_address,
            gas_budget,
            storage_price,
            write_price,
            commission_rate,
            name,
            image_url,
            project_url,
            description,
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
            let wallet_context = WalletContext::new(&wallet_config, None, None)
                .context("Reading Sui wallet failed")?;
            wallet_context
                .config
                .get_active_env()
                .context("Unable to get the wallet's active environment")?
                .rpc
                .clone()
        };

        // Do a minor sanity check that the user has not included a port in the hostname.
        ensure!(
            !public_host.contains(':'),
            "DNS names must not contain ':'; to specify a port different from the default, use the \
                '--public-port' option."
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
                wallet_config,
                event_polling_interval: config::defaults::polling_interval(),
                backoff_config: ExponentialBackoffConfig::default(),
                gas_budget,
            }),
            voting_params: VotingParams {
                storage_price,
                write_price,
                node_capacity: node_capacity.as_u64(),
            },
            commission_rate,
            use_legacy_event_provider,
            disable_event_blob_writer,
            name,
            metadata,
            ..Default::default()
        };

        // Generate and write config file.
        let yaml_config =
            serde_yaml::to_string(&config).context("failed to serialize configuration to YAML")?;
        let mut file = create_file(&config_path, force).with_context(|| {
            format!(
                "failed to create the config file '{}'",
                config_path.display()
            )
        })?;
        file.write_all(yaml_config.as_bytes()).context(format!(
            "failed to write the generated configuration to '{}'",
            config_path.display()
        ))?;
        println!(
            "storage node configuration written to '{}'",
            config_path.display()
        );

        Ok(config)
    }

    pub fn repair_db(db_path: PathBuf) -> anyhow::Result<()> {
        let mut opts = Options::default();

        // In case the integrity of the entire database is compromised.
        opts.create_if_missing(true);
        opts.set_max_open_files(512_000);
        DB::repair(&opts, db_path).map_err(|err| err.into())
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
        let network_key_path = config_directory.join("network.key");
        let wallet_config = config_directory.join("sui_config.yaml");
        ensure!(
            config_directory.is_dir(),
            "The directory '{}' does not exist.",
            config_directory.display()
        );

        keygen(&protocol_key_path, KeyType::Protocol, true)?;
        keygen(&network_key_path, KeyType::Network, true)?;

        let wallet_address = utils::generate_sui_wallet(
            sui_network,
            &wallet_config,
            use_faucet,
            faucet_timeout.into(),
        )
        .await?;
        println!(
            "Successfully generated a new Sui wallet with address {}",
            wallet_address
        );

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
}

/// Creates a [`SuiContractClient`] from the Sui config in the provided storage node config.
async fn get_contract_client_from_node_config(
    storage_config: &StorageNodeConfig,
) -> anyhow::Result<SuiContractClient> {
    let Some(ref node_wallet_config) = storage_config.sui else {
        bail!("storage config does not contain Sui wallet configuration");
    };
    Ok(node_wallet_config.new_contract_client().await?)
}

struct StorageNodeRuntime {
    walrus_node_handle: JoinHandle<anyhow::Result<()>>,
    rest_api_handle: JoinHandle<Result<(), io::Error>>,
    // INV: Runtime must be dropped last
    runtime: Runtime,
}

impl StorageNodeRuntime {
    fn start(
        node_config: &StorageNodeConfig,
        metrics_registry: Registry,
        exit_notifier: oneshot::Sender<()>,
        event_manager: Box<dyn EventManager>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("walrus-node-runtime")
            .enable_all()
            .build()
            .expect("walrus-node runtime creation must succeed");
        let _guard = runtime.enter();

        let walrus_node = Arc::new(
            runtime.block_on(
                StorageNode::builder()
                    .with_system_event_manager(event_manager)
                    .build(node_config, metrics_registry.clone()),
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
            if let Err(ref error) = result {
                tracing::error!(?error, "storage node exited with an error");
            }

            result
        });

        let rest_api = RestApiServer::new(
            walrus_node,
            cancel_token.child_token(),
            RestApiConfig::from(node_config),
            &metrics_registry,
        );
        let mut rest_api_address = node_config.rest_api_address;
        rest_api_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
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

        Ok(Self {
            runtime,
            walrus_node_handle,
            rest_api_handle,
        })
    }

    fn join(&mut self) -> Result<(), anyhow::Error> {
        tracing::debug!("waiting for the REST API to shutdown...");
        let _ = self.runtime.block_on(&mut self.rest_api_handle)?;
        tracing::debug!("waiting for the storage node to shutdown...");
        self.runtime.block_on(&mut self.walrus_node_handle)?
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use walrus_test_utils::Result;

    use super::*;

    #[test]
    fn generate_key_pair_saves_base64_key_to_file() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        commands::keygen(&filename, KeyType::Protocol, false)?;

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

        commands::keygen(&filename, KeyType::Protocol, false)
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

        commands::keygen(&filename, KeyType::Protocol, true)?;

        let file_content = std::fs::read_to_string(filename).expect("the file should still exist");

        let _: ProtocolKeyPair = file_content
            .parse()
            .expect("a protocol keypair must be parseable from the the file's contents");

        Ok(())
    }
}
