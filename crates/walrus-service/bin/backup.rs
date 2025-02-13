// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup Nodes entry points.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use walrus_service::{
    backup::{
        run_backup_database_migrations,
        start_backup_fetcher,
        start_backup_orchestrator,
        BackupConfig,
        VERSION,
    },
    common::utils::MetricsAndLoggingRuntime,
    utils::load_from_yaml,
};

/// Manage and run Walrus backup nodes
#[derive(Parser)]
#[clap(rename_all = "kebab-case")]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[derive(Debug)]
struct Args {
    #[clap(long, short, help = "Specify the config file path to use")]
    config: PathBuf,
    #[clap(
        long,
        short,
        help = "Override the metrics address to use (ie: 127.0.0.1:9184)"
    )]
    metrics_address: Option<std::net::SocketAddr>,
    #[command(subcommand)]
    command: BackupCommands,
}

#[derive(Subcommand, Debug, Clone)]
#[clap(rename_all = "kebab-case")]
enum BackupCommands {
    /// Run a backup orchestrator with the provided configuration.
    RunOrchestrator,
    /// Run a backup fetcher with the provided configuration.
    RunFetcher,
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
                BackupCommands::RunOrchestrator => {
                    start_backup_orchestrator(config, &metrics_runtime).await
                }
                BackupCommands::RunFetcher => start_backup_fetcher(config, &metrics_runtime).await,
            },
            "backup node",
        )
    });
}
