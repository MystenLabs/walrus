// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A client for the Walrus blob store.

mod client_bin;

use std::{net::SocketAddr, process::ExitCode};

use anyhow::Result;
use clap::Parser;
use client_bin::{
    args::{App, Commands, RpcArg, VERSION},
    runner::ClientCommandRunner,
    utils::init_tracing_subscriber,
};
use walrus_service::{client::cli_utils::error, utils::MetricsAndLoggingRuntime};

/// Gets the metrics address from the commands that support it.
fn get_metrics_address(command: &Commands) -> Option<SocketAddr> {
    match command {
        Commands::Publisher { args } => Some(args.daemon_args.metrics_address),
        Commands::Aggregator { daemon_args, .. } => Some(daemon_args.metrics_address),
        Commands::Daemon { args } => Some(args.daemon_args.metrics_address),
        _ => None,
    }
}

fn client() -> Result<()> {
    let mut app = App::parse();

    let metrics_runtime = if let Some(metrics_address) = get_metrics_address(&app.command) {
        let runtime = MetricsAndLoggingRuntime::start(metrics_address)?;
        tracing::debug!(%metrics_address, "started metrics and logging on separate runtime");
        Some(runtime)
    } else {
        init_tracing_subscriber()?;
        None
    };

    tracing::info!("client version: {}", VERSION);

    while let Commands::Json { command_string } = app.command {
        tracing::info!("running in JSON mode");
        let command_string = match command_string {
            Some(s) => s,
            None => {
                tracing::debug!("reading JSON input from stdin");
                std::io::read_to_string(std::io::stdin())?
            }
        };
        tracing::debug!(
            command = command_string.replace('\n', ""),
            "running JSON command"
        );
        app = serde_json::from_str(&command_string)?;
        app.json = true;
    }
    run_app(app, metrics_runtime)
}

fn run_app(app: App, metrics_runtime: Option<MetricsAndLoggingRuntime>) -> Result<()> {
    let runner = ClientCommandRunner::new(&app.config, &app.wallet, app.gas_budget, app.json);
    match app.command {
        Commands::Read {
            blob_id,
            out,
            rpc_arg: RpcArg { rpc_url },
        } => runner.read(blob_id, out, rpc_url),

        Commands::Store {
            file,
            epochs,
            dry_run,
            force,
        } => runner.store(file, epochs, dry_run, force),

        Commands::BlobStatus {
            file_or_blob_id,
            timeout,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_status(file_or_blob_id, timeout, rpc_url),

        Commands::Info {
            rpc_arg: RpcArg { rpc_url },
            dev,
        } => runner.info(rpc_url, dev),

        Commands::Json { .. } => {
            unreachable!("we unpack JSON commands until we obtain a different command")
        }

        Commands::BlobId {
            file,
            n_shards,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_id(file, n_shards, rpc_url),

        Commands::ConvertBlobId { blob_id_decimal } => runner.convert_blob_id(blob_id_decimal),

        Commands::ListBlobs { include_expired } => runner.list_blobs(include_expired),

        Commands::Publisher { args } => runner
            .with_metrics_runtime(
                metrics_runtime.expect("the command has the metrics address with this command"),
            )
            .publisher(args),

        Commands::Aggregator {
            rpc_arg: RpcArg { rpc_url },
            daemon_args,
        } => runner
            .with_metrics_runtime(
                metrics_runtime.expect("the command has the metrics address with this command"),
            )
            .aggregator(rpc_url, daemon_args),

        Commands::Daemon { args } => runner
            .with_metrics_runtime(
                metrics_runtime.expect("the command has the metrics address with this command"),
            )
            .daemon(args),
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
