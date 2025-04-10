// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use serde::Deserialize;
use walrus_service::{
    client::cli::{error, App, ClientCommandRunner, Commands},
    utils::{self, MetricsAndLoggingRuntime},
};
use walrus_sui::client::retry_client::RetriableRpcError;

/// The version of the Walrus client.
pub const VERSION: &str = walrus_service::utils::version!();

/// The command-line arguments for the Walrus client.
#[derive(Parser, Debug, Clone, Deserialize)]
#[command(
    author,
    about = "Walrus client",
    long_about = None,
    name = env!("CARGO_BIN_NAME"),
    version = VERSION,
    rename_all = "kebab-case"
)]
#[serde(rename_all = "camelCase")]
pub struct ClientArgs {
    #[command(flatten)]
    inner: App,
}

fn client() -> Result<()> {
    let subscriber_guard = utils::init_scoped_tracing_subscriber()?;
    let mut app = ClientArgs::parse().inner;
    app.extract_json_command()?;

    tracing::info!("client version: {VERSION}");
    let runner = ClientCommandRunner::new(
        &app.config,
        app.context.as_deref(),
        &app.wallet,
        app.gas_budget,
        app.json,
    );

    // Drop the temporary tracing subscriber, as the global ones are about to be initialized.
    drop(subscriber_guard);

    match app.command {
        Commands::Cli(command) => {
            utils::init_tracing_subscriber()?;
            runner.run_cli_app(command)
        }
        Commands::Daemon(command) => {
            let metrics_address = command.get_metrics_address();

            let runtime = MetricsAndLoggingRuntime::start(metrics_address)?;
            utils::export_build_info(&runtime.registry, VERSION);

            tracing::debug!(%metrics_address, "started metrics and logging on separate runtime");

            runner.run_daemon_app(command, runtime)
        }
        Commands::Json { .. } => unreachable!("we have extracted the json command above"),
    }
}

/// The CLI entrypoint.
pub fn main() -> ExitCode {
    if let Err(err) = client() {
        // Print any error in a (relatively) user-friendly way.
        let error_str = if err.is_retriable_rpc_error() {
            "The Sui full node RPC seems to be overwhelmed by too many requests. \
            Please try with another full node, or try again later.\nError: "
        } else {
            ""
        };
        eprintln!("{} {}{:#}", error(), error_str, err);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
