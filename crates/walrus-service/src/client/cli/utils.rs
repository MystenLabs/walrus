// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the Walrus client binary.

use std::env;

use anyhow::{anyhow, Result};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{
    filter::Filtered,
    layer::{Layered, SubscriberExt as _},
    util::SubscriberInitExt,
    EnvFilter,
    Layer,
    Registry,
};

type PrepareResult = Result<
    Layered<Filtered<Box<dyn Layer<Registry> + Send + Sync>, EnvFilter, Registry>, Registry>,
>;

/// Prepare the tracing subscriber based on the environment variables.
fn prepare_subscriber() -> PrepareResult {
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

    Ok(tracing_subscriber::registry().with(layer.with_filter(EnvFilter::new(directive.clone()))))
}

/// Initializes the logger and tracing subscriber as the global subscriber.
pub fn init_tracing_subscriber() -> Result<()> {
    prepare_subscriber()?.init();
    tracing::debug!("initialized global tracing subscriber");
    Ok(())
}

/// Initializes the logger and tracing subscriber as the subscriber for the current scope.
pub fn init_scoped_tracing_subscriber() -> Result<DefaultGuard> {
    let guard = prepare_subscriber()?.set_default();
    tracing::debug!("initialized scoped tracing subscriber");
    Ok(guard)
}
