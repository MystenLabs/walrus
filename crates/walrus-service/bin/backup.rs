// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup Node entry point.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use walrus_service::{
    backup::{start_backup_node, BackupNodeConfig},
    node::events::event_processor_runtime::EventProcessorRuntime,
    utils::{self, load_from_yaml, version, MetricsAndLoggingRuntime},
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if !matches!(args.command, BackupCommands::Run { .. }) {
        utils::init_tracing_subscriber()?;
    }

    match args.command {
        BackupCommands::Run { config_path } => {
            commands::run_backup_node(load_from_yaml(config_path)?).await?
        }
    };
    Ok(())
}

mod commands {
    use walrus_service::utils;

    use super::*;

    pub(super) async fn run_backup_node(config: BackupNodeConfig) -> anyhow::Result<()> {
        let metrics_runtime = MetricsAndLoggingRuntime::new(config.metrics_address, None)?;
        let registry_clone = metrics_runtime.registry.clone();
        tokio::spawn(async move {
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

        let event_manager = EventProcessorRuntime::start_async(
            config.sui.clone(),
            config.event_processor_config.clone(),
            &config.backup_storage_path,
            &metrics_runtime.registry,
            cancel_token.child_token(),
        )
        .await?;

        let ret = start_backup_node(metrics_runtime.registry.clone(), event_manager, config).await;
        if let Err(err) = ret.as_ref() {
            tracing::error!("backup node exited with an error [{:?}]", err);
        }
        ret
    }
}
