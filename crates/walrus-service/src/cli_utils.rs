// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the walrus cli tools.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use colored::{ColoredString, Colorize};
use sui_sdk::wallet_context::WalletContext;

use crate::client::{default_configuration_paths, Config};

/// Returns the path if it is `Some` or any of the default paths if they exist (attempt in order).
pub fn path_or_defaults_if_exist(path: &Option<PathBuf>, defaults: &[PathBuf]) -> Option<PathBuf> {
    let mut path = path.clone();
    for default in defaults {
        if path.is_some() {
            break;
        }
        path = default.exists().then_some(default.clone());
    }
    path
}

/// Loads the wallet context from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Sui configuration directory.
#[allow(dead_code)]
pub fn load_wallet_context(path: &Option<PathBuf>) -> Result<WalletContext> {
    let mut default_paths = vec!["./client.yaml".into(), "./sui_config.yaml".into()];
    if let Some(home_dir) = home::home_dir() {
        default_paths.push(home_dir.join(".sui").join("sui_config").join("client.yaml"))
    }
    let path = path_or_defaults_if_exist(path, &default_paths)
        .ok_or(anyhow!("Could not find a valid wallet config file."))?;
    tracing::info!("Using wallet configuration from {}", path.display());
    WalletContext::new(&path, None, None)
}

/// Loads the Walrus configuration from the given path.
///
/// If no path is provided, tries to load the configuration first from the local folder, and then
/// from the standard Walrus configuration directory.
pub fn load_configuration(path: &Option<PathBuf>) -> Result<Config> {
    let path = path_or_defaults_if_exist(path, &default_configuration_paths())
        .ok_or(anyhow!("Could not find a valid Walrus configuration file."))?;
    tracing::info!("Using Walrus configuration from {}", path.display());

    serde_yaml::from_str(&std::fs::read_to_string(&path).context(format!(
        "Unable to read Walrus configuration from {}",
        path.display()
    ))?)
    .context(format!(
        "Parsing Walrus configuration from {} failed",
        path.display()
    ))
}

/// Returns the string `Success:` colored in green for terminal output.
pub fn success() -> ColoredString {
    "Success:".bold().green()
}

/// Returns the string `Error:` colored in red for terminal output.
pub fn error() -> ColoredString {
    "Error:".bold().red()
}
