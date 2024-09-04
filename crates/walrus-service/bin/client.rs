// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use serde::Deserialize;
use walrus_service::{
    client::cli::{self, error, init_tracing_subscriber, App, ClientCommandRunner, Commands},
    utils::MetricsAndLoggingRuntime,
};

/// The version of the Walrus client.
pub const VERSION: &str = walrus_service::utils::version!();

/// The command-line arguments for the Walrus client.
#[derive(Parser, Debug, Clone, Deserialize)]
#[command(author, version, about = "Walrus client", long_about = None)]
#[clap(name = env!("CARGO_BIN_NAME"))]
#[clap(version = VERSION)]
#[clap(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase")]
pub struct ClientArgs {
    #[clap(flatten)]
    inner: App,
}

fn client() -> Result<()> {
    let mut app = ClientArgs::parse().inner;
    app.extract_json_command()?;

    match app.command {
        Commands::Cli(command) => {
            init_tracing_subscriber()?;
            tracing::info!("client version: {}", VERSION);
            let runner =
                ClientCommandRunner::new(&app.config, &app.wallet, app.gas_budget, app.json);
            cli::run_cli_app(runner, command)
        }
        Commands::Daemon(command) => {
            let metrics_address = command.get_metrics_address();
            let runtime = MetricsAndLoggingRuntime::start(metrics_address)?;
            // NOTE: We duplicate the client version info and the runner creation to ensure it is
            // logged, since we start logging separately.
            tracing::info!("client version: {}", VERSION);
            tracing::debug!(%metrics_address, "started metrics and logging on separate runtime");
            let runner =
                ClientCommandRunner::new(&app.config, &app.wallet, app.gas_budget, app.json);
            cli::run_daemon_app(runner, command, runtime)
        }
        Commands::Json { .. } => unreachable!("we have extracted the json command above"),
    }
}

/// The CLI entrypoint.
pub fn main() -> ExitCode {
    if let Err(e) = client() {
        // Print any error in a (relatively) user-friendly way.
        eprintln!("{} {:#}", error(), e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
