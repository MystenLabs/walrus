// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Walrus Storage Node entry point.

use std::{fs, io, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::{Parser, Subcommand};
use fastcrypto::traits::KeyPair;
use futures::future;
use mysten_metrics::RegistryService;
use rand::thread_rng;
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::{ProtocolKeyPair, ShardIndex};
use walrus_service::{
    client,
    config::{self, StorageNodeConfig},
    server::UserServer,
    StorageNode,
};
use walrus_sui::types::StorageNode as SuiStorageNode;

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
    Run {
        /// Path to the Walrus node configuration file.
        config_path: PathBuf,
    },
    /// Deploy a testbed of storage nodes.
    Testbed {
        /// Path to the directory where the testbed will be deployed.
        #[clap(long, default_value = "./")]
        working_dir: PathBuf,
        /// Number of storage nodes to deploy.
        #[clap(long, default_value_t = 10)]
        number_of_nodes: u16,
        /// Number of primary symbols to use.
        #[clap(long, default_value_t = 2)]
        n_symbols_primary: u16,
        /// Number of secondary symbols to use.
        #[clap(long, default_value_t = 4)]
        n_symbols_secondary: u16,
    },
    KeyGen {
        /// Path to the directory where the key pair will be saved.
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Commands::Run { config_path } => {
            let config = StorageNodeConfig::load(config_path)?;
            run_storage_node(config)?;
        }

        Commands::Testbed {
            working_dir,
            number_of_nodes,
            n_symbols_primary,
            n_symbols_secondary,
        } => {
            deploy_testbed(
                working_dir,
                number_of_nodes,
                n_symbols_primary,
                n_symbols_secondary,
            )
            .await?
        }
        Commands::KeyGen { out: _out } => {
            // TODO(jsmith): Add a CLI endpoint to generate a new private key file (#148)
            todo!();
        }
    }
    Ok(())
}

fn run_storage_node(mut config: StorageNodeConfig) -> anyhow::Result<()> {
    let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;

    tracing::info!("Walrus Node version: {VERSION}");
    tracing::info!(
        "Walrus public key: {}",
        config.protocol_key_pair.load()?.as_ref().public()
    );
    tracing::info!(
        "Started Prometheus HTTP endpoint at {}",
        config.metrics_address
    );

    let cancel_token = CancellationToken::new();
    let (exit_notifier, exit_listener) = oneshot::channel::<()>();

    let mut node_runtime = StorageNodeRuntime::start(
        &config,
        metrics_runtime.registry_service.clone(),
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

async fn deploy_testbed(
    working_dir: PathBuf,
    number_of_nodes: u16,
    n_symbols_primary: u16,
    n_symbols_secondary: u16,
) -> anyhow::Result<()> {
    // Generate storage node configs.
    let mut storage_node_configs = Vec::new();
    let mut sui_storage_node_configs = Vec::new();
    for i in 0..number_of_nodes {
        let name = format!("node-{i}");

        let mut storage_path = working_dir.clone();
        storage_path.push("storage");
        storage_path.push(&name);

        let protocol_key_pair = ProtocolKeyPair::random(&mut thread_rng());
        let public_key = protocol_key_pair.as_ref().public().clone();

        let mut metrics_address = config::defaults::metrics_address();
        metrics_address.set_port(metrics_address.port() + i as u16);

        let mut rest_api_address = config::defaults::rest_api_address();
        rest_api_address.set_port(rest_api_address.port() + i as u16);

        storage_node_configs.push(StorageNodeConfig {
            storage_path,
            protocol_key_pair: config::PathOrInPlace::InPlace(protocol_key_pair),
            metrics_address,
            rest_api_address,
        });

        sui_storage_node_configs.push(SuiStorageNode {
            name,
            network_address: rest_api_address.into(),
            public_key,
            shard_ids: vec![ShardIndex(i)],
        });
    }

    // Print client configs.
    let client_config = client::Config {
        committee: walrus_sui::types::Committee {
            members: sui_storage_node_configs,
            epoch: 0,
            total_weight: number_of_nodes as usize,
        },
        source_symbols_primary: n_symbols_primary,
        source_symbols_secondary: n_symbols_secondary,
        concurrent_requests: number_of_nodes as usize,
        connection_timeout: Duration::from_secs(10),
    };
    let client_config_path = working_dir.join("client_config.yaml");
    let serialized_client_config =
        serde_yaml::to_string(&client_config).context("Failed to serialize client configs")?;
    fs::write(&client_config_path, serialized_client_config)
        .context("Failed to write client configs")?;

    // Start storage nodes.
    let storage_node_handles = storage_node_configs.into_iter().map(|config| {
        tokio::spawn(async move { run_storage_node(config).expect("unable to start storage node") })
    });
    future::try_join_all(storage_node_handles).await?;
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
        config: &StorageNodeConfig,
        registry_service: RegistryService,
        exit_notifier: oneshot::Sender<()>,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("walrus-node-runtime")
            .enable_all()
            .build()
            .expect("walrus-node runtime creation must succeed");
        let _guard = runtime.enter();

        let walrus_node = Arc::new(StorageNode::new(config, registry_service)?);

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
        let rest_api_address = config.rest_api_address;
        let rest_api_handle = tokio::spawn(async move {
            let result = rest_api.run(&rest_api_address).await;
            if let Err(ref err) = result {
                tracing::error!("rest API exited with an error: {err:?}");
            }
            result
        });
        tracing::info!("Started REST API on {}", config.rest_api_address);

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
