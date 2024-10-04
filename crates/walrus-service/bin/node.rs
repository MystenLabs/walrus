// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Storage Node entry point.

use std::{
    fs,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use fastcrypto::traits::KeyPair;
use prometheus::Registry;
use serde::Deserialize;
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::keys::ProtocolKeyPair;
use walrus_event::{event_processor::EventProcessor, EventProcessorConfig};
use walrus_service::{
    node::{
        config::{StorageNodeConfig, SuiConfig},
        server::{UserServer, UserServerConfig},
        system_events::{EventManager, SuiSystemEventProvider},
        StorageNode,
    },
    utils::{self, version, LoadConfig as _, MetricsAndLoggingRuntime},
};
use walrus_sui::client::{ContractClient, SuiContractClient, SuiReadClient};

const VERSION: &str = version!();

#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[derive(Debug)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone, Deserialize)]
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

    /// Generates a new key for use with the Walrus protocol, and writes it to a file.
    KeyGen {
        /// Path to the file at which the key will be created. If the file already exists, it is
        /// not overwritten and the operation will fail.
        #[clap(default_value = "protocol.key")]
        out: PathBuf,
    },

    /// Register a new node with the Walrus storage network.
    Register {
        #[clap(short, long)]
        #[serde(deserialize_with = "crate::utils::resolve_home_dir")]
        /// The path to the node's configuration file.
        config_path: PathBuf,
        #[clap(short, long)]
        #[serde(default)]
        /// The name of the node.
        name: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Run {
            config_path,
            cleanup_storage,
        } => commands::run(StorageNodeConfig::load(config_path)?, cleanup_storage)?,

        Commands::KeyGen { out } => commands::keygen(&out)?,

        Commands::Register { config_path, name } => commands::register_node(config_path, name)?,
    }
    Ok(())
}

mod commands {
    use std::io;

    use anyhow::anyhow;

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

        let (event_manager, mut event_processor_runtime) = EventProcessorRuntime::start(
            config
                .sui
                .clone()
                .expect("SUI configuration must be present"),
            config.event_processor_config.clone(),
            &config.storage_path,
            cancel_token.child_token(),
        )?;

        let mut node_runtime = StorageNodeRuntime::start(
            &config,
            metrics_runtime.registry.clone(),
            exit_notifier,
            event_manager,
            cancel_token.child_token(),
        )?;

        wait_until_terminated(exit_listener);

        // Cancel the node runtime, if it is still executing.
        cancel_token.cancel();

        event_processor_runtime.join()?;

        // Wait for the node runtime to complete, may take a moment as
        // the REST-API waits for open connections to close before exiting.
        node_runtime.join()
    }

    pub(super) fn keygen(path: &Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create_new(path)
            .with_context(|| format!("Cannot create a the keyfile '{}'", path.display()))?;

        file.write_all(ProtocolKeyPair::generate().to_base64().as_bytes())?;

        Ok(())
    }

    #[tokio::main]
    pub(crate) async fn register_node(
        config_path: PathBuf,
        name: Option<String>,
    ) -> anyhow::Result<()> {
        let storage_config = StorageNodeConfig::load(&config_path)?;
        let node_name = name.or(storage_config.name.clone()).ok_or(anyhow!(
            "Name is required to register node. Set it in the config file or provided in the \
                commandline argument."
        ))?;
        let registration_params = storage_config.to_registration_params(node_name);

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
}

/// Creates a [`SuiContractClient`] from the Sui config in the provided storage node config.
async fn get_contract_client_from_node_config(
    storage_config: &StorageNodeConfig,
) -> anyhow::Result<SuiContractClient> {
    let Some(ref node_wallet_config) = storage_config.sui else {
        bail!("storage config does not contain Sui wallet configuration");
    };

    let node_wallet = utils::load_wallet_context(&Some(node_wallet_config.wallet_config.clone()))?;
    let contract_client = SuiContractClient::new(
        node_wallet,
        node_wallet_config.system_object,
        node_wallet_config.staking_object,
        node_wallet_config.gas_budget,
    )
    .await?;

    Ok(contract_client)
}

struct EventProcessorRuntime {
    event_processor_handle: JoinHandle<anyhow::Result<()>>,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
}

impl EventProcessorRuntime {
    async fn build_event_processor(
        sui_config: SuiConfig,
        event_processor_config: Option<EventProcessorConfig>,
        db_path: &Path,
    ) -> anyhow::Result<Option<Arc<EventProcessor>>> {
        let SuiConfig {
            rpc,
            system_object,
            staking_object,
            ..
        } = sui_config;

        let read_client = SuiReadClient::new_for_rpc(&rpc, system_object, staking_object).await?;
        match &event_processor_config {
            Some(event_processor_config) => Ok(Some(Arc::new(
                EventProcessor::new(
                    event_processor_config,
                    read_client.get_system_package_id(),
                    sui_config.event_polling_interval,
                    db_path,
                )
                .await?,
            ))),
            None => Ok(None),
        }
    }
    fn start(
        sui_config: SuiConfig,
        event_processor_config: Option<EventProcessorConfig>,
        db_path: &Path,
        cancel_token: CancellationToken,
    ) -> anyhow::Result<(Box<dyn EventManager>, Self)> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("event-manager-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("event manager runtime creation failed")?;
        let _guard = runtime.enter();
        let (read_client, event_processor) = runtime.block_on(async {
            let read_client = SuiReadClient::new_for_rpc(
                &sui_config.rpc,
                sui_config.system_object,
                sui_config.staking_object,
            )
            .await?;
            let event_processor =
                Self::build_event_processor(sui_config.clone(), event_processor_config, db_path)
                    .await?;
            anyhow::Ok((read_client, event_processor))
        })?;
        let cloned_event_processor = event_processor.clone();
        let event_manager: Box<dyn EventManager> = match cloned_event_processor {
            Some(event_processor) => Box::new(event_processor.clone()),
            None => Box::new(SuiSystemEventProvider::new(
                read_client,
                sui_config.event_polling_interval,
            )),
        };
        let event_processor_handle = tokio::spawn(async move {
            let result = if let Some(event_processor) = event_processor {
                event_processor.start(cancel_token).await
            } else {
                Ok(())
            };
            if let Err(ref error) = result {
                tracing::error!(?error, "event manager exited with an error");
            }
            result
        });

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
            rest_api
                .run()
                .await
                .inspect_err(|error| tracing::error!(?error, "REST API exited with an error"))
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
