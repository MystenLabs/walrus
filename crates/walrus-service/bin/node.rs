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

use anyhow::{anyhow, bail, Context};
use clap::{Parser, Subcommand};
use config::{PathOrInPlace, TlsConfig};
use fastcrypto::traits::KeyPair;
use fs::File;
use humantime::Duration;
use prometheus::Registry;
use sui_sdk::wallet_context::WalletContext;
use sui_types::base_types::ObjectID;
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::keys::{NetworkKeyPair, ProtocolKeyPair};
use walrus_event::{event_processor::EventProcessor, EventProcessorConfig};
use walrus_service::{
    node::{
        config::{
            self,
            defaults::REST_API_PORT,
            EventProviderConfig,
            StorageNodeConfig,
            SuiConfig,
        },
        server::{UserServer, UserServerConfig},
        system_events::{EventManager, SuiSystemEventProvider},
        StorageNode,
    },
    utils::{self, version, ByteCount, LoadConfig as _, MetricsAndLoggingRuntime},
};
use walrus_sui::{
    client::{ContractClient, SuiContractClient},
    types::move_structs::VotingParams,
    utils::SuiNetwork,
};

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
    /// Generate Sui wallet, keys, and configuration for a Walrus node.
    ///
    /// This overwrites existing files in the configuration directory.
    /// Fails if the specified directory does not exist yet.
    Setup {
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
        /// Timeout for the faucet call.
        #[clap(long, default_value = "1min")]
        faucet_timeout: Duration,
        #[clap(flatten)]
        config_args: ConfigArgs,
    },

    /// Register a new node with the Walrus storage network.
    Register {
        #[clap(long)]
        /// The path to the node's configuration file.
        config_path: PathBuf,
        #[clap(long)]
        /// The host name or public IP address of the node.
        public_host: String,
        /// The port on which the storage node will serve requests.
        #[clap(long, default_value_t = REST_API_PORT)]
        port: u16,
        #[clap(long)]
        /// The name of the node.
        name: Option<String>,
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
struct ConfigArgs {
    #[clap(long)]
    /// Object ID of the Walrus system object.
    system_object: ObjectID,
    #[clap(long)]
    /// Object ID of the Walrus staking object.
    staking_object: ObjectID,
    #[clap(long)]
    /// Initial storage capacity of this node in bytes.
    ///
    /// The value can either by unitless; have suffixes for powers of 1000, such as (B),
    /// kilobytes (K), etc, or have suffixes for the IEC units such as kibibytes (Ki),
    /// mebibytes (Mi), etc.
    node_capacity: ByteCount,
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
    #[clap(long, default_value_t = config::defaults::storage_price())]
    /// Initial vote for the storage price in FROST per MiB per epoch.
    storage_price: u64,
    #[clap(long, default_value_t = config::defaults::write_price())]
    /// Initial vote for the write price in FROST per MiB.
    write_price: u64,
    #[clap(long, default_value_t = config::defaults::gas_budget())]
    /// Gas budget for transactions.
    gas_budget: u64,
    #[clap(long)]
    /// Public IP address or hostname of the storage node.
    server_name: Option<String>,
    #[clap(long, default_value_t = config::defaults::rest_api_address())]
    /// Socket address on which the REST API listens.
    rest_api_address: SocketAddr,
    #[clap(long, default_value_t = config::defaults::metrics_address())]
    /// Socket address on which the Prometheus server should export its metrics.
    metrics_address: SocketAddr,
    #[clap(long, default_value_t = 0)]
    /// The commission rate of the storage node, in basis points.
    commission_rate: u64,
    #[clap(long)]
    /// The name of the storage node used in the registration.
    name: Option<String>,
}

#[derive(Debug, Clone, clap::Args)]
struct PathArgs {
    #[clap(long)]
    /// The output path for the generated configuration file.
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
        Commands::Setup {
            config_directory,
            storage_path,
            sui_network,
            faucet_timeout,
            config_args,
        } => commands::setup(
            config_directory,
            storage_path,
            sui_network,
            faucet_timeout.into(),
            config_args,
        )?,

        Commands::Register {
            config_path,
            public_host,
            port,
            name,
        } => commands::register_node(config_path, public_host, port, name)?,

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
        } => commands::generate_config(path_args, config_args)?,
    }
    Ok(())
}

mod commands {

    use std::time::Duration;

    use config::EventProviderConfig;
    use tokio::task::JoinSet;
    use walrus_core::ensure;
    use walrus_service::utils;
    use walrus_sui::types::NetworkAddress;

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

        tracing::info!(version = VERSION, "walrus binary version");
        tracing::info!(
            walrus.node.public_key = %config.protocol_key_pair.load()?.as_ref().public(),
            "walrus protocol public key",
        );
        tracing::info!(
            walrus.node.network_key = %config.network_key_pair.load()?.as_ref().public(),
            "walrus network key",
        );
        tracing::info!(
            metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
        );

        utils::export_build_info(&metrics_runtime.registry, VERSION);
        if let Some(config) = config.sui.as_ref() {
            utils::export_contract_info(
                &metrics_runtime.registry,
                &config.system_object,
                &config.staking_object,
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
                .clone()
                .expect("SUI configuration must be present"),
            config.event_provider_config.clone(),
            &config.storage_path,
            &metrics_runtime.registry,
            cancel_token.child_token(),
        )?;

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
            exit_listener,
            cancel_token,
        )?;

        Ok(())
    }

    #[cfg(not(msim))]
    fn monitor_runtimes(
        mut node_runtime: StorageNodeRuntime,
        mut event_processor_runtime: EventProcessorRuntime,
        exit_listener: oneshot::Receiver<()>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let monitor_runtime = Runtime::new()?;
        monitor_runtime.block_on(async {
            tokio::spawn(async move {
                let mut set = JoinSet::new();
                set.spawn_blocking(move || node_runtime.join());
                set.spawn_blocking(move || event_processor_runtime.join());
                tokio::select! {
                    _ = wait_until_terminated(exit_listener) => {
                        tracing::info!("Received termination signal, shutting down...");
                    }
                    _ = set.join_next() => {
                        tracing::info!("Runtime stopped successfully");
                    }
                }
                cancel_token.cancel();
                tracing::info!("Cancellation token triggered, waiting for tasks to shut down...");

                // Drain remaining runtimes
                while set.join_next().await.is_some() {}
                tracing::info!("All runtimes have shut down");
            })
            .await
        })?;
        Ok(())
    }

    #[cfg(msim)]
    fn monitor_runtimes(
        mut node_runtime: StorageNodeRuntime,
        mut event_processor_runtime: EventProcessorRuntime,
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
        Ok(())
    }

    pub(super) fn keygen(path: &Path, key_type: KeyType, force: bool) -> anyhow::Result<()> {
        println!(
            "Generating {key_type} key pair and writing it to '{}'",
            path.display()
        );
        let mut file = if force {
            File::create(path)
        } else {
            File::create_new(path)
        }
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
    pub(crate) async fn register_node(
        config_path: PathBuf,
        public_host: String,
        port: u16,
        name: Option<String>,
    ) -> anyhow::Result<()> {
        let mut storage_config = StorageNodeConfig::load(&config_path)?;
        let node_name = name.or(storage_config.name.clone()).ok_or(anyhow!(
            "Name is required to register a node. Set it in the config file or provide it as a \
                command-line argument."
        ))?;

        storage_config.protocol_key_pair.load()?;
        storage_config.network_key_pair.load()?;

        // If we have an IP address, use a SocketAddr to get the string representation
        // as IPv6 addresses are enclosed in square brackets.
        let public_address = if let Ok(ip_addr) = IpAddr::from_str(&public_host) {
            NetworkAddress(SocketAddr::new(ip_addr, port).to_string())
        } else {
            // Do a minor sanity check that the user has not included a port in the hostname
            ensure!(
                !public_host.contains(':'),
                "DNS names must not contain ':', to specify a port, use the --port option."
            );
            NetworkAddress(format!("{public_host}:{port}"))
        };
        let registration_params = storage_config.to_registration_params(public_address, node_name);

        // Uses the Sui wallet configuration in the storage node config to register the node.
        let contract_client = get_contract_client_from_node_config(&storage_config).await?;
        let proof_of_possession = walrus_sui::utils::generate_proof_of_possession(
            storage_config.protocol_key_pair(),
            &contract_client,
            &registration_params,
        )
        .await?;

        let node_capability = contract_client
            .register_candidate(&registration_params, &proof_of_possession)
            .await?;

        println!("Successfully registered storage node with capability:",);
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
            sui_rpc,
            use_legacy_event_provider,
            system_object,
            staking_object,
            storage_price,
            write_price,
            node_capacity,
            gas_budget,
            server_name,
            rest_api_address,
            metrics_address,
            commission_rate,
            name,
        }: ConfigArgs,
    ) -> anyhow::Result<()> {
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

        let event_provider_config = if use_legacy_event_provider {
            EventProviderConfig::LegacyEventProvider
        } else {
            EventProviderConfig::CheckpointBasedEventProcessor(None)
        };

        let config = StorageNodeConfig {
            storage_path,
            protocol_key_pair: PathOrInPlace::from_path(protocol_key_path),
            network_key_pair: PathOrInPlace::from_path(network_key_path),
            sui: Some(SuiConfig {
                rpc: sui_rpc,
                system_object,
                staking_object,
                wallet_config,
                event_polling_interval: config::defaults::polling_interval(),
                gas_budget,
            }),
            voting_params: VotingParams {
                storage_price,
                write_price,
                node_capacity: node_capacity.as_u64(),
            },
            rest_api_address,
            metrics_address,
            name,
            commission_rate,
            tls: TlsConfig {
                disable_tls: false,
                pem_files: None,
                server_name,
            },
            event_provider_config,
            ..Default::default()
        };

        // Generate and write config file.
        let yaml_config =
            serde_yaml::to_string(&config).context("Failed to serialize configuration to YAML")?;
        fs::write(&config_path, yaml_config)
            .context("Failed to write the generated configuration to a file")?;
        println!(
            "Storage node configuration written to '{}'",
            config_path.display()
        );
        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn setup(
        config_directory: PathBuf,
        storage_path: PathBuf,
        sui_network: SuiNetwork,
        faucet_timeout: Duration,
        config_args: ConfigArgs,
    ) -> anyhow::Result<()> {
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

        let wallet_address =
            utils::generate_sui_wallet(sui_network, &wallet_config, faucet_timeout).await?;
        println!(
            "Successfully generated a new Sui wallet with address {}",
            wallet_address
        );

        generate_config(
            PathArgs {
                config_path,
                storage_path,
                protocol_key_path,
                network_key_path,
                wallet_config,
            },
            config_args,
        )
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

struct EventProcessorRuntime {
    event_processor_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
}

impl EventProcessorRuntime {
    async fn build_event_processor(
        sui_config: SuiConfig,
        event_processor_config: &EventProcessorConfig,
        db_path: &Path,
        metrics_registry: &Registry,
    ) -> anyhow::Result<Arc<EventProcessor>> {
        Ok(Arc::new(
            EventProcessor::new(
                event_processor_config,
                sui_config.rpc.clone(),
                sui_config.new_read_client().await?.get_system_package_id(),
                sui_config.event_polling_interval,
                &db_path.join("events"),
                metrics_registry,
            )
            .await?,
        ))
    }

    fn start(
        sui_config: SuiConfig,
        event_provider_config: EventProviderConfig,
        db_path: &Path,
        metrics_registry: &Registry,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<(Box<dyn EventManager>, Self)> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("event-manager-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("event manager runtime creation failed")?;
        let _guard = runtime.enter();

        let (event_manager, event_processor_handle): (Box<dyn EventManager>, _) =
            match event_provider_config {
                EventProviderConfig::CheckpointBasedEventProcessor(event_processor_config) => {
                    let event_processor_config = event_processor_config.unwrap_or_else(|| {
                        EventProcessorConfig::new_with_default_pruning_interval(
                            sui_config.rpc.clone(),
                        )
                    });
                    let event_processor = runtime.block_on(async {
                        Self::build_event_processor(
                            sui_config.clone(),
                            &event_processor_config,
                            db_path,
                            metrics_registry,
                        )
                        .await
                    })?;
                    let cloned_event_processor = event_processor.clone();
                    let event_processor_handle = tokio::spawn(async move {
                        let result = cloned_event_processor.start(cancel_token).await;
                        if let Err(ref error) = result {
                            tracing::error!(?error, "event manager exited with an error");
                        }
                        result
                    });
                    (Box::new(event_processor), event_processor_handle)
                }
                EventProviderConfig::LegacyEventProvider => {
                    let read_client =
                        runtime.block_on(async { sui_config.new_read_client().await })?;
                    (
                        Box::new(SuiSystemEventProvider::new(
                            read_client,
                            sui_config.event_polling_interval,
                        )),
                        tokio::spawn(async { Ok(()) }),
                    )
                }
            };

        Ok((
            event_manager,
            Self {
                runtime,
                event_processor_handle,
            },
        ))
    }

    fn join(&mut self) -> Result<(), anyhow::Error> {
        tracing::debug!("waiting for the event processor to shutdown...");
        self.runtime.block_on(&mut self.event_processor_handle)?
    }
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

        let rest_api = UserServer::new(
            walrus_node,
            cancel_token.child_token(),
            UserServerConfig::from(node_config),
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
        tracing::info!("Started REST API on {}", node_config.rest_api_address);

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

/// Wait for SIGINT and SIGTERM (unix only).
#[tracing::instrument(skip_all)]
async fn wait_until_terminated(mut exit_listener: oneshot::Receiver<()>) {
    #[cfg(not(unix))]
    async fn wait_for_other_signals() {
        // Disables this branch in the select statement.
        std::future::pending().await
    }

    #[cfg(unix)]
    async fn wait_for_other_signals() {
        use tokio::signal::unix;

        unix::signal(unix::SignalKind::terminate())
            .expect("unable to register for SIGTERM signals")
            .recv()
            .await;
        tracing::info!("received SIGTERM")
    }

    tokio::select! {
        biased;
        _ = wait_for_other_signals() => (),
        _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT"),
        exit_or_dropped = &mut exit_listener => match exit_or_dropped {
            Err(_) => tracing::info!("exit notification sender was dropped"),
            Ok(_) => tracing::info!("exit notification received"),
        }
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
