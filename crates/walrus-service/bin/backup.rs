// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Walrus Backup Nodes entry points.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use walrus_service::{
    backup::{start_backup_fetcher, start_backup_orchestrator, VERSION},
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

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let config = load_from_yaml(args.config).expect("failed to load config");
    let _ = match args.command {
        BackupCommands::RunOrchestrator => start_backup_orchestrator(config).await,
        BackupCommands::RunFetcher => start_backup_fetcher(config).await,
    }
    .inspect(|_| {
        tracing::error!("backup node exited prematurely without an explicit error");
    })
    .inspect_err(|error| {
        tracing::error!(?error, "backup node exited prematurely");
    });
    // Exit immediately with an error code if either backup node exits prematurely.
    std::process::exit(1);
}
