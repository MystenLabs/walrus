// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
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

fn client() -> Result<()> {
    let mut app = App::parse();
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
