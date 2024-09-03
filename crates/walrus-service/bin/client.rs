// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use serde::Deserialize;
use walrus_service::{
    client::cli_utils::{
        error,
        init_tracing_subscriber,
        run_cli_app,
        run_daemon_app,
        App,
        VERSION,
    },
    utils::MetricsAndLoggingRuntime,
};

// NOTE: this wrapper is required to make the `env!` macro work.
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

    let metrics_runtime = if let Some(metrics_address) = app.get_metrics_address() {
        let runtime = MetricsAndLoggingRuntime::start(metrics_address)?;
        tracing::debug!(%metrics_address, "started metrics and logging on separate runtime");
        Some(runtime)
    } else {
        init_tracing_subscriber()?;
        None
    };

    tracing::info!("client version: {}", VERSION);
    run_app(app, metrics_runtime)
}

#[tokio::main]
async fn run_app(app: App, metrics_runtime: Option<MetricsAndLoggingRuntime>) -> Result<()> {
    match (app.is_daemon_command(), metrics_runtime) {
        (true, Some(runtime)) => run_daemon_app(app, runtime).await,
        (false, None) => run_cli_app(app).await,
        _ => unreachable!("if the command is a daemon command, then metrics_runtime must be Some"),
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
