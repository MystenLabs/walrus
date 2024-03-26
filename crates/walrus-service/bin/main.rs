// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Walrus Storage Node entry point.

use std::{io, net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use fastcrypto::traits::KeyPair;
use mysten_metrics::RegistryService;
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use walrus_service::{config::StorageNodeConfig, server::UserServer, StorageNode};

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
    /// Path to the Walrus node configuration file.
    config_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let config = StorageNodeConfig::load(args.config_path)?;

    let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;

    tracing::info!("Walrus Node version: {VERSION}");
    tracing::info!("Walrus public key: {}", config.protocol_key_pair.public());
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
