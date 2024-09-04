// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the Walrus client binary.

use std::env;

use anyhow::{anyhow, Result};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter, Layer};

use super::args::{CliCommands, DaemonCommands, RpcArg};
use crate::{client::cli::runner::ClientCommandRunner, utils::MetricsAndLoggingRuntime};

/// Initializes the logger and tracing subscriber.
pub fn init_tracing_subscriber() -> Result<()> {
    // Use INFO level by default.
    let directive = format!(
        "info,{}",
        env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default()
    );
    let layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // Control output format based on `LOG_FORMAT` env variable.
    let format = env::var("LOG_FORMAT").ok();
    let layer = if let Some(format) = &format {
        match format.to_lowercase().as_str() {
            "default" => layer.boxed(),
            "compact" => layer.compact().boxed(),
            "pretty" => layer.pretty().boxed(),
            "json" => layer.json().boxed(),
            s => Err(anyhow!("LOG_FORMAT '{}' is not supported", s))?,
        }
    } else {
        layer.boxed()
    };

    tracing_subscriber::registry()
        .with(layer.with_filter(EnvFilter::new(directive.clone())))
        .init();
    tracing::debug!(%directive, ?format, "initialized tracing subscriber");

    Ok(())
}

/// Runs the binary commands in "cli" mode (i.e., without running a server).
#[tokio::main]
pub async fn run_cli_app(runner: ClientCommandRunner, command: CliCommands) -> Result<()> {
    match command {
        CliCommands::Read {
            blob_id,
            out,
            rpc_arg: RpcArg { rpc_url },
        } => runner.read(blob_id, out, rpc_url).await,

        CliCommands::Store {
            file,
            epochs,
            dry_run,
            force,
        } => runner.store(file, epochs, dry_run, force).await,

        CliCommands::BlobStatus {
            file_or_blob_id,
            timeout,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_status(file_or_blob_id, timeout, rpc_url).await,

        CliCommands::Info {
            rpc_arg: RpcArg { rpc_url },
            dev,
        } => runner.info(rpc_url, dev).await,

        CliCommands::BlobId {
            file,
            n_shards,
            rpc_arg: RpcArg { rpc_url },
        } => runner.blob_id(file, n_shards, rpc_url).await,

        CliCommands::ConvertBlobId { blob_id_decimal } => runner.convert_blob_id(blob_id_decimal),

        CliCommands::ListBlobs { include_expired } => runner.list_blobs(include_expired).await,
    }
}

/// Runs the binary commands in "daemon" mode (i.e., running a server).
#[tokio::main]
pub async fn run_daemon_app(
    runner: ClientCommandRunner,
    command: DaemonCommands,
    metrics_runtime: MetricsAndLoggingRuntime,
) -> Result<()> {
    match command {
        DaemonCommands::Publisher { args } => {
            runner.publisher(&metrics_runtime.registry, args).await
        }

        DaemonCommands::Aggregator {
            rpc_arg: RpcArg { rpc_url },
            daemon_args,
        } => {
            runner
                .aggregator(&metrics_runtime.registry, rpc_url, daemon_args)
                .await
        }

        DaemonCommands::Daemon { args } => runner.daemon(&metrics_runtime.registry, args).await,
    }
}
