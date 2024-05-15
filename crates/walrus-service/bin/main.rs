// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Walrus Storage Node entry point.

use std::{
    fs,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

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
use walrus_core::keys::ProtocolKeyPair;
use walrus_service::{
    config::{
        defaults::{METRICS_PORT, POLLING_INTERVAL_MS, REST_API_PORT},
        LoadConfig,
        StorageNodeConfig,
        SuiConfig,
    },
    server::UserServer,
    testbed::node_config_name_prefix,
    StorageNode,
};
use walrus_sui::utils::SuiNetwork;

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
        /// Whether to cleanup the storage directory before starting the node.
        #[clap(long, action, default_value_t = false)]
        cleanup_storage: bool,
    },

    /// Deploy the Walrus system contract on the Sui network.
    DeploySystemContract(DeploySystemContractArgs),

    /// Generate the configuration files to run a testbed of storage nodes.
    GenerateDryRunConfigs(GenerateDryRunConfigsArgs),

    /// Generates a new key for use with the Walrus protocol, and writes it to a file.
    KeyGen {
        /// Path to the file at which the key will be created. If the file already exists, it is
        /// not overwritten and the operation will fail.
        #[clap(default_value = "protocol.key")]
        out: PathBuf,
    },
}

#[derive(Debug, Clone, clap::Args)]
struct DeploySystemContractArgs {
    /// The directory where the storage nodes will be deployed.
    #[clap(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "testnet")]
    sui_network: SuiNetwork,
    /// The directory in which the contracts are located.
    #[clap(long, default_value = "./contracts/blob_store")]
    contract_path: PathBuf,
    /// Gas budget for sui transactions to publish the contracts and set up the system.
    #[arg(long, default_value_t = 500_000_000)]
    gas_budget: u64,
    /// The total number of shards. The shards are distributed evenly among the storage nodes.
    // Todo: accept non-even shard distributions #377
    #[arg(long, default_value_t = 1000)]
    n_shards: u16,
    /// The list of ip addresses of the storage nodes.
    #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
    ips: Vec<Ipv4Addr>,
    /// The port on which the REST API of the storage nodes will listen.
    #[clap(long, default_value_t = REST_API_PORT)]
    rest_api_port: u16,
    /// Whether to deploy a local testbed with all nodes on the same machine.
    #[clap(long, action, default_value_t = false)]
    local_testbed: bool,
    /// The interval with which events are polled, in milliseconds.
    #[clap(long,  default_value_t = POLLING_INTERVAL_MS)]
    event_polling_interval: u64,
}

#[derive(Debug, Clone, clap::Args)]
struct GenerateDryRunConfigsArgs {
    /// The directory where the storage nodes will be deployed.
    #[clap(long, default_value = "./working_dir")]
    working_dir: PathBuf,
    /// Sui network for which the config is generated.
    #[clap(long, default_value = "devnet")]
    sui_network: SuiNetwork,
    /// The path to the configuration file of the Walrus smart contract deployed on Sui.
    #[clap(long)]
    sui_config_path: PathBuf,
    /// The list of ip addresses of the storage nodes.
    #[clap(long, value_name = "ADDR", value_delimiter = ' ', num_args(4..))]
    ips: Vec<Ipv4Addr>,
    /// The port on which the REST API of the storage nodes will listen.
    #[clap(long, default_value_t = REST_API_PORT)]
    rest_api_port: u16,
    /// The port on which the metrics server of the storage nodes will listen.
    #[clap(long, default_value_t = METRICS_PORT)]
    metrics_port: u16,
    /// Whether to deploy a local testbed with all nodes on the same machine.
    #[clap(long, action, default_value_t = false)]
    local_testbed: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Run {
            config_path,
            cleanup_storage,
        } => commands::run(StorageNodeConfig::load(config_path)?, cleanup_storage)?,

        Commands::DeploySystemContract(args) => commands::deploy_system_contract(args)?,

        Commands::GenerateDryRunConfigs(args) => commands::generate_dry_run_configs(args)?,

        Commands::KeyGen { out } => commands::keygen(&out)?,
    }
    Ok(())
}

mod commands {
    use std::io;

    use walrus_service::testbed::{
        create_client_config,
        create_storage_node_configs,
        deploy_walrus_contract,
        even_shards_allocation,
    };

    use super::*;

    #[tokio::main]
    pub(super) async fn deploy_system_contract(
        DeploySystemContractArgs {
            working_dir,
            sui_network,
            contract_path,
            gas_budget,
            n_shards,
            ips,
            rest_api_port,
            local_testbed,
            event_polling_interval,
        }: DeploySystemContractArgs,
    ) -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;

        // Deploy the system contract.
        let number_of_shards = NonZeroU16::new(n_shards).context("number of shards must be > 0")?;
        let committee_size = NonZeroU16::new(ips.len() as u16).unwrap();
        let shards_information = even_shards_allocation(number_of_shards, committee_size);
        let sui_config = deploy_walrus_contract(
            &working_dir,
            sui_network,
            contract_path,
            gas_budget,
            shards_information,
            ips,
            rest_api_port,
            local_testbed,
            Duration::from_millis(event_polling_interval),
        )
        .await
        .context("Failed to deploy system contract")?;

        // Write the Sui config to file.
        let serialized_sui_config =
            serde_yaml::to_string(&sui_config).context("Failed to serialize Sui config")?;
        let sui_config_path = working_dir.join("sui_config.yaml");
        fs::write(sui_config_path, serialized_sui_config).context("Failed to write Sui config")?;
        Ok(())
    }

    #[tokio::main]
    pub(super) async fn generate_dry_run_configs(
        GenerateDryRunConfigsArgs {
            working_dir,
            sui_network,
            sui_config_path,
            ips,
            rest_api_port,
            metrics_port,
            local_testbed,
        }: GenerateDryRunConfigsArgs,
    ) -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        fs::create_dir_all(&working_dir)
            .with_context(|| format!("Failed to create directory '{}'", working_dir.display()))?;
        let sui_config = SuiConfig::load(sui_config_path)?;

        let client_config = create_client_config(
            sui_config.pkg_id,
            sui_config.system_object,
            working_dir.as_path(),
            sui_network,
        )
        .await?;
        let serialized_client_config =
            serde_yaml::to_string(&client_config).context("Failed to serialize client configs")?;
        let client_config_path = working_dir.join("client_config.yaml");
        fs::write(client_config_path, serialized_client_config)
            .context("Failed to write client configs")?;

        let committee_size = NonZeroU16::new(ips.len() as u16).expect("committee size must be > 0");
        let storage_node_configs = create_storage_node_configs(
            working_dir.as_path(),
            sui_config,
            ips,
            rest_api_port,
            metrics_port,
            local_testbed,
        );
        for (i, storage_node_config) in storage_node_configs.into_iter().enumerate() {
            let serialized_storage_node_config = serde_yaml::to_string(&storage_node_config)
                .context("Failed to serialize storage node configs")?;
            let node_config_name =
                format!("{}.yaml", node_config_name_prefix(i as u16, committee_size));
            let node_config_path = working_dir.join(node_config_name);
            fs::write(node_config_path, serialized_storage_node_config)
                .context("Failed to write storage node configs")?;
        }

        Ok(())
    }

    pub(super) fn keygen(path: &Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create_new(path)
            .with_context(|| format!("Cannot create a the keyfile '{}'", path.display()))?;

        file.write_all(ProtocolKeyPair::generate().to_base64().as_bytes())?;

        Ok(())
    }

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
}

struct MetricsAndLoggingRuntime {
    registry_service: RegistryService,
    _telemetry_guards: TelemetryGuards,
    _tracing_handle: TracingHandle,
    // INV: Runtime must be dropped last.
    _runtime: Runtime,
}

impl MetricsAndLoggingRuntime {
    fn start(mut metrics_address: SocketAddr) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metrics-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics runtime creation failed")?;
        let _guard = runtime.enter();

        metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
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

        let walrus_node = Arc::new(
            runtime.block_on(StorageNode::builder().build(node_config, registry_service))?,
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

        let rest_api = UserServer::new(walrus_node, cancel_token.child_token());
        let mut rest_api_address = node_config.rest_api_address;
        rest_api_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let rest_api_handle = tokio::spawn(async move {
            let result = rest_api.run(&rest_api_address).await;
            if let Err(ref error) = result {
                tracing::error!(?error, "rest API exited with an error");
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use walrus_test_utils::Result;

    use super::*;

    #[test]
    fn generate_key_pair_saves_base64_key_to_file() -> Result<()> {
        let dir = TempDir::new()?;
        let filename = dir.path().join("keyfile.key");

        commands::keygen(&filename)?;

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

        commands::keygen(&filename).expect_err("must fail as the file already exists");

        let file_content = std::fs::read_to_string(filename).expect("the file should still exist");
        assert_eq!(file_content, "original-file-contents");

        Ok(())
    }
}
