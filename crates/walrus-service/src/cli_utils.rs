// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the walrus cli tools.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use colored::{ColoredString, Colorize};
use sui_sdk::{wallet_context::WalletContext, SuiClient, SuiClientBuilder};
use walrus_sui::client::SuiReadClient;

use crate::client::{default_configuration_paths, Client, Config};

/// Default URL of the testnet RPC node.
pub const TESTNET_RPC: &str = "https://fullnode.testnet.sui.io:443";
/// Default RPC URL to connect to if none is specified explicitly or in the wallet config.
pub const DEFAULT_RPC_URL: &str = TESTNET_RPC;

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
// NB: When making changes to the logic, make sure to update the argument docs in
// `crates/walrus-service/bin/client.rs`.
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
// NB: When making changes to the logic, make sure to update the argument docs in
// `crates/walrus-service/bin/client.rs`.
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

/// Creates a [`Client`] based on the provided [`Config`] with read-only access to Sui.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `wallet` (if `Ok`) or the
/// default [`DEFAULT_RPC_URL`] if `allow_fallback_to_default` is true.
pub async fn get_read_client(
    config: Config,
    rpc_url: Option<String>,
    wallet: Result<WalletContext>,
    allow_fallback_to_default: bool,
) -> Result<Client<()>> {
    let sui_client =
        get_sui_client_from_rpc_node_or_wallet(rpc_url, wallet, allow_fallback_to_default).await?;
    let sui_read_client =
        SuiReadClient::new(sui_client, config.system_pkg, config.system_object).await?;
    Ok(Client::new_read_client(config, &sui_read_client).await?)
}

// NB: When making changes to the logic, make sure to update the docstring of `get_read_client` and
// the argument docs in `crates/walrus-service/bin/client.rs`.
async fn get_sui_client_from_rpc_node_or_wallet(
    rpc_url: Option<String>,
    wallet: Result<WalletContext>,
    allow_fallback_to_default: bool,
) -> Result<SuiClient> {
    tracing::debug!(
        ?rpc_url,
        %allow_fallback_to_default,
        "attempting to create a read client from explicitly set RPC URL, wallet config, or default"
    );
    match rpc_url {
        Some(url) => {
            tracing::info!("Using explicitly set RPC URL {url}");
            SuiClientBuilder::default()
                .build(&url)
                .await
                .context(format!("cannot connect to Sui RPC node at {url}"))
        }
        None => match wallet {
            Ok(wallet) => {
                tracing::info!("Using RPC URL set in wallet configuration");
                wallet
                    .get_client()
                    .await
                    .context("cannot connect to Sui RPC node specified in the wallet configuration")
            }
            Err(e) => {
                if allow_fallback_to_default {
                    tracing::info!("Using default RPC URL {DEFAULT_RPC_URL}");
                    SuiClientBuilder::default()
                        .build(DEFAULT_RPC_URL)
                        .await
                        .context(format!(
                            "cannot connect to Sui RPC node at {DEFAULT_RPC_URL}"
                        ))
                } else {
                    Err(e)
                }
            }
        },
    }
}

/// Returns the string `Success:` colored in green for terminal output.
pub fn success() -> ColoredString {
    "Success:".bold().green()
}

/// Returns the string `Error:` colored in red for terminal output.
pub fn error() -> ColoredString {
    "Error:".bold().red()
}
