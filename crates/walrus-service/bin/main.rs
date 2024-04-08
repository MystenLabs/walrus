// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Walrus Storage Node entry point.

use std::{fs, io, net::SocketAddr, num::NonZeroU16, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use fastcrypto::traits::KeyPair;
use mysten_metrics::RegistryService;
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::{encoding::EncodingConfig, ShardIndex};
use walrus_service::{
    client,
    config::{LoadConfig, StorageNodeConfig},
    server::UserServer,
    testbed::{node_config_name_prefix, testbed_configs},
    StorageNode,
};

const GIT_REVISION: &str = {
    if let Some(revision) = option_env!("GIT_REVISION") {
        revision
    } else {
        let version = git_version::git_version!(
            args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
            fallback = ""
        );
        if version.is_empty() {
            panic!("unable to query git revision");
        }
        version
    }
};
const VERSION: &str = walrus_core::concat_const_str!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION);

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
    /// Run a storage node with the provided configuration.
    Run {
        /// Path to the Walrus node configuration file.
        #[clap(long)]
        config_path: PathBuf,
        /// The committee configuration.
        #[command(subcommand)]
        committee_config: CommitteeConfig,
        /// Whether to cleanup the storage directory before starting the node.
        #[clap(long, action, default_value_t = false)]
        cleanup_storage: bool,
    },
    /// Generate the configuration files to run a testbed of storage nodes.
    GenerateDryRunConfigs {
        /// The directory where the storage nodes will be deployed.
        #[clap(long, default_value = "./working_dir")]
        working_dir: PathBuf,
        /// The number of storage nodes in the committee.
        #[clap(long, default_value = "4")]
        committee_size: NonZeroU16,
        /// The total number of shards.
        #[clap(long, default_value = "10")]
        n_shards: NonZeroU16,
        /// Number of primary symbols to use (used to generate client config).
        #[clap(long, default_value = "2")]
        n_symbols_primary: NonZeroU16,
        /// Number of secondary symbols to use (used to generate client config).
        #[clap(long, default_value = "4")]
        n_symbols_secondary: NonZeroU16,
    },
    /// Generate a new key pair.
    KeyGen {
        /// Path to the directory where the key pair will be saved.
        out: PathBuf,
    },
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum CommitteeConfig {
    OnChain,
    FromClientConfig {
        /// The path to the client configuration file.
        #[clap(long, default_value = "./working_dir/client_config.yaml")]
        client_config_path: PathBuf,
        /// The index of the storage node to run.
        #[clap(long)]
        storage_node_index: usize,
    },
    Manual {
        /// The total number of shards.
        #[clap(long, default_value = "100")]
        n_shards: NonZeroU16,
        /// The number of source symbols for the primary encoding.
        #[clap(long, default_value = "30")]
        source_symbols_primary: NonZeroU16,
        /// The number of source symbols for the secondary encoding.
        #[clap(long, default_value = "62")]
        source_symbols_secondary: NonZeroU16,
        /// The shards to be handled by this node.
        #[clap(long)]
        handled_shards: Vec<u16>,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Commands::Run {
            config_path,
            committee_config,
            cleanup_storage,
        } => {
            let config = StorageNodeConfig::load(config_path)?;
            let (encoding_config, shards) = match committee_config {
                CommitteeConfig::OnChain => {
                    // TODO(alberto): Get the committee from the chain. (#212)
                    todo!()
                }
                CommitteeConfig::FromClientConfig {
                    client_config_path,
                    storage_node_index,
                } => {
                    let client_config = client::Config::load(client_config_path)?;
                    let encoding_config = client_config.encoding_config();
                    let handled_shards = client_config.shards_for_node(storage_node_index);
                    (encoding_config, handled_shards)
                }
                CommitteeConfig::Manual {
                    n_shards,
                    source_symbols_primary,
                    source_symbols_secondary,
                    handled_shards,
                } => (
                    EncodingConfig::new(
                        source_symbols_primary.get(),
                        source_symbols_secondary.get(),
                        n_shards.get(),
                    ),
                    handled_shards
                        .into_iter()
                        .map(ShardIndex)
                        .collect::<Vec<_>>(),
                ),
            };
            run_storage_node(config, encoding_config, cleanup_storage, &shards)?;
        }
        Commands::GenerateDryRunConfigs {
            working_dir,
            committee_size,
            n_shards,
            n_symbols_primary,
            n_symbols_secondary,
        } => {
            generate_dry_run_configs(
                working_dir,
                committee_size,
                n_shards,
                n_symbols_primary,
                n_symbols_secondary,
            )?;
        }
        Commands::KeyGen { out: _out } => {
            // TODO(jsmith): Add a CLI endpoint to generate a new private key file (#148)
            todo!();
        }
    }
    Ok(())
}

fn run_storage_node(
    mut node_config: StorageNodeConfig,
    encoding_config: EncodingConfig,
    cleanup_storage: bool,
    handled_shards: &[ShardIndex],
) -> anyhow::Result<()> {
    if cleanup_storage {
        let storage_path = &node_config.storage_path;
        match fs::remove_dir_all(storage_path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).context(format!(
                    "Failed to remove directory '{}'",
                    storage_path.display()
                ))
            }
        }
    }

    let metrics_runtime = MetricsAndLoggingRuntime::start(node_config.metrics_address)?;

    tracing::info!("Walrus Node version: {VERSION}");
    tracing::info!(
        "Walrus public key: {}",
        node_config.protocol_key_pair.load()?.as_ref().public()
    );
    tracing::info!(
        "Started Prometheus HTTP endpoint at {}",
        node_config.metrics_address
    );

    let cancel_token = CancellationToken::new();
    let (exit_notifier, exit_listener) = oneshot::channel::<()>();

    let mut node_runtime = StorageNodeRuntime::start(
        &node_config,
        handled_shards,
        metrics_runtime.registry_service.clone(),
        encoding_config,
        exit_notifier,
        cancel_token.child_token(),
    )?;

    wait_until_terminated(exit_listener);

    // Cancel the node runtime, if it is still executing.
    cancel_token.cancel();

    // Wait for the node runtime to complete, may take a moment as
    // the REST-API waits for open connections to close before exiting.
    node_runtime.join()
}

fn generate_dry_run_configs(
    working_dir: PathBuf,
    committee_size: NonZeroU16,
    shards: NonZeroU16,
    n_symbols_primary: NonZeroU16,
    n_symbols_secondary: NonZeroU16,
) -> anyhow::Result<()> {
    if let Err(e) = fs::create_dir_all(&working_dir) {
        return Err(e).context(format!(
            "Failed to create directory '{}'",
            working_dir.display()
        ));
    }

    // Generate testbed configs.
    let (storage_node_configs, client_config) = testbed_configs(
        &working_dir,
        committee_size,
        shards,
        n_symbols_primary,
        n_symbols_secondary,
    );

    // Write client config to file.
    let serialized_client_config =
        serde_yaml::to_string(&client_config).context("Failed to serialize client configs")?;
    let client_config_path = working_dir.join("client_config.yaml");
    fs::write(client_config_path, serialized_client_config)
        .context("Failed to write client configs")?;

    // Write the storage nodes config files.
    for storage_node_index in 0..committee_size.get() {
        let storage_node_config = storage_node_configs[storage_node_index as usize].clone();
        let serialized_storage_node_config = serde_yaml::to_string(&storage_node_config)
            .context("Failed to serialize storage node configs")?;
        let node_config_name = format!(
            "{}.yaml",
            node_config_name_prefix(storage_node_index, committee_size)
        );
        let node_config_path = working_dir.join(node_config_name);
        fs::write(node_config_path, serialized_storage_node_config)
            .context("Failed to write storage node configs")?;
    }

    Ok(())
}

struct MetricsAndLoggingRuntime {
    registry_service: RegistryService,
    _telemetry_guards: TelemetryGuards,
    _tracing_handle: TracingHandle,
    // INV: Runtime must be dropped last.
    _runtime: Runtime,
}

impl MetricsAndLoggingRuntime {
    fn start(metrics_address: SocketAddr) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metrics-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics runtime creation failed")?;
        let _guard = runtime.enter();

        let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
        let prometheus_registry = registry_service.default_registry();

        // Initialize logging subscriber
        let (telemetry_guards, tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
            .with_env()
            .with_prom_registry(&prometheus_registry)
            .with_log_level("debug")
            .init();

        Ok(Self {
            _runtime: runtime,
            registry_service,
            _telemetry_guards: telemetry_guards,
            _tracing_handle: tracing_handle,
        })
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
        handled_shards: &[ShardIndex],
        registry_service: RegistryService,
        encoding_config: EncodingConfig,
        exit_notifier: oneshot::Sender<()>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("walrus-node-runtime")
            .enable_all()
            .build()
            .expect("walrus-node runtime creation must succeed");
        let _guard = runtime.enter();

        let walrus_node = Arc::new(
            runtime
                .block_on(StorageNode::new(
                    node_config,
                    registry_service,
                    encoding_config,
                ))?
                .with_storage_shards(handled_shards),
        );

        let walrus_node_clone = walrus_node.clone();
        let walrus_node_cancel_token = cancel_token.child_token();
        let walrus_node_handle = tokio::spawn(async move {
            let cancel_token = walrus_node_cancel_token.clone();
            let result = walrus_node_clone.run(walrus_node_cancel_token).await;

            if exit_notifier.send(()).is_err() && !cancel_token.is_cancelled() {
                tracing::warn!(
                    "unable to notify that the node has exited, but shutdown s not in progress?"
                )
            }
            if let Err(ref err) = result {
                tracing::error!("storage node exited with an error: {err:?}");
            }

            result
        });

        let rest_api = UserServer::new(walrus_node, cancel_token.child_token());
        let rest_api_address = node_config.rest_api_address;
        let rest_api_handle = tokio::spawn(async move {
            let result = rest_api.run(&rest_api_address).await;
            if let Err(ref err) = result {
                tracing::error!("rest API exited with an error: {err:?}");
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
#[tokio::main(flavor = "current_thread")]
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
