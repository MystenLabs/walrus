// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup Node entry point.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio::{runtime::Runtime, sync::oneshot};
use tokio_util::sync::CancellationToken;
use walrus_service::{
    backup::{BackupNodeConfig, BackupNodeRuntime},
    node::events::event_processor_runtime::EventProcessorRuntime,
    utils::{self, version, wait_until_terminated, LoadConfig as _, MetricsAndLoggingRuntime},
};

const VERSION: &str = version!();

/// Manage and run a Walrus backup node.
#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[derive(Debug)]
struct Args {
    #[command(subcommand)]
    command: BackupCommands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum BackupCommands {
    /// Run a backup node with the provided configuration.
    Run { config_path: PathBuf },
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !matches!(args.command, BackupCommands::Run { .. }) {
        utils::init_tracing_subscriber()?;
    }

    match args.command {
        BackupCommands::Run { config_path } => {
            let config = BackupNodeConfig::load(config_path)?;
            commands::run_backup_node(config)?
        }
    };
    Ok(())
}

mod commands {
    #[cfg(not(msim))]
    use tokio::task::JoinSet;
    use walrus_service::utils;

    use super::*;

    pub(super) fn run_backup_node(config: BackupNodeConfig) -> anyhow::Result<()> {
        let metrics_runtime = MetricsAndLoggingRuntime::start(config.metrics_address)?;
        let registry_clone = metrics_runtime.registry.clone();
        metrics_runtime.runtime.spawn(async move {
            registry_clone
                .register(mysten_metrics::uptime_metric(
                    "walrus_backup_node",
                    VERSION,
                    "walrus",
                ))
                .unwrap();
        });

        tracing::info!(version = VERSION, "Walrus backup binary version");
        tracing::info!(
            metrics_address = %config.metrics_address, "started Prometheus HTTP endpoint",
        );

        utils::export_build_info(&metrics_runtime.registry, VERSION);

        let cancel_token = CancellationToken::new();
        let (exit_notifier, exit_listener) = oneshot::channel::<()>();

        let (event_manager, event_processor_runtime) = EventProcessorRuntime::start(
            config.sui,
            config.event_processor_config.clone(),
            config.use_legacy_event_provider,
            &config.backup_storage_path,
            &metrics_runtime.registry,
            cancel_token.child_token(),
        )?;

        let node_runtime = BackupNodeRuntime::start(
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
        mut node_runtime: BackupNodeRuntime,
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
                        tracing::info!("received termination signal, shutting down...");
                    }
                    _ = set.join_next() => {
                        tracing::info!("backup runtime stopped successfully");
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
        mut node_runtime: BackupNodeRuntime,
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
}
