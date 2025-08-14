// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup Nodes entry points.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use walrus_service::{
    backup::{
        BackupConfig, run_backup_database_migrations, start_backup_fetcher,
        start_backup_garbage_collector, start_backup_orchestrator,
    },
    common::utils::MetricsAndLoggingRuntime,
};
use walrus_utils::load_from_yaml;

// Define the `GIT_REVISION` and `VERSION` consts
walrus_utils::bin_version!();

/// Manage and run Walrus backup nodes
#[derive(Parser)]
#[command(
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
    rename_all = "kebab-case",
)]
#[derive(Debug)]
struct Args {
    #[arg(long, short, help = "Specify the config file path to use")]
    config: PathBuf,
    #[arg(
        long,
        short,
        help = "Override the metrics address to use (ie: 127.0.0.1:9184)"
    )]
    metrics_address: Option<std::net::SocketAddr>,
    #[command(subcommand)]
    command: BackupCommands,
}

#[derive(Subcommand, Debug, Clone)]
#[command(rename_all = "kebab-case")]
enum BackupCommands {
    /// Run a backup orchestrator with the provided configuration.
    Orchestrator,
    /// Run a backup fetcher with the provided configuration.
    Fetcher,
    /// Run a backup garbage collector that cleans out old backups from Cloud Storage.
    GarbageCollector,
}

fn exit_process_on_return(result: anyhow::Result<()>, context: &str) {
    if let Err(error) = result {
        tracing::error!(?error, context, "encountered error");
    }
    tracing::error!(context, "exited prematurely");
    std::process::exit(1);
}

fn main() {
    let args = Args::parse();
    let mut config: BackupConfig = load_from_yaml(&args.config).expect("loading config from yaml");
    if let Some(metrics_address) = args.metrics_address {
        config.metrics_address = metrics_address;
    }

    let rt = tokio::runtime::Runtime::new().expect("creating tokio runtime");
    let _guard = rt.enter();

    let metrics_runtime = MetricsAndLoggingRuntime::new(config.metrics_address, None)
        .expect("starting metrics runtime");

    // Run migrations before starting the backup node.
    run_backup_database_migrations(&config);

    rt.block_on(async move {
        exit_process_on_return(
            match args.command {
                BackupCommands::Orchestrator => {
                    start_backup_orchestrator(VERSION, config, &metrics_runtime).await
                }
                BackupCommands::Fetcher => {
                    start_backup_fetcher(VERSION, config, &metrics_runtime).await
                }
                BackupCommands::GarbageCollector => {
                    start_backup_garbage_collector(VERSION, config, &metrics_runtime).await
                }
            },
            "backup node",
        )
    });
}
