// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Storage Node entry point.

use std::{
    fs,
    io::{self, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use fastcrypto::traits::KeyPair;
use prometheus::Registry;
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_core::keys::ProtocolKeyPair;
use walrus_service::{
    node::{
        config::StorageNodeConfig,
        server::{UserServer, UserServerConfig},
        StorageNode,
    },
    utils::{version, LoadConfig as _},
};

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

    /// Generates a new key for use with the Walrus protocol, and writes it to a file.
    KeyGen {
        /// Path to the file at which the key will be created. If the file already exists, it is
        /// not overwritten and the operation will fail.
        #[clap(default_value = "protocol.key")]
        out: PathBuf,
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
    }
    Ok(())
}

mod commands {
    use std::io;

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

        let mut node_runtime = StorageNodeRuntime::start(
            &config,
            metrics_runtime.registry.clone(),
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

    pub(super) fn keygen(path: &Path) -> anyhow::Result<()> {
        let mut file = std::fs::File::create_new(path)
            .with_context(|| format!("Cannot create a the keyfile '{}'", path.display()))?;

        file.write_all(ProtocolKeyPair::generate().to_base64().as_bytes())?;

        Ok(())
    }
}

struct MetricsAndLoggingRuntime {
    registry: Registry,
    _telemetry_guards: TelemetryGuards,
    _tracing_handle: TracingHandle,
    // INV: Runtime must be dropped last.
    runtime: Runtime,
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
        let walrus_registry = registry_service.default_registry();

        // Initialize logging subscriber
        let (telemetry_guards, tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
            .with_env()
            .with_prom_registry(&walrus_registry)
            .with_log_level("debug")
            .init();

        Ok(Self {
            runtime,
            registry: walrus_registry,
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
        metrics_registry: Registry,
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
                .block_on(StorageNode::builder().build(node_config, metrics_registry.clone()))?,
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
