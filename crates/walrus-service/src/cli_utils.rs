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

/// Type to help with formatting bytes as human-readable strings.
///
/// Formatting of `HumanReadableBytes` works as follows:
///
/// 1. If the value is smaller than 1024, print the value with a ` B` suffix (as we always have
///    an integer number of bytes). Otherwise, follow the next steps.
/// 1. Divide the value by 1024 until we get a *normalized value* in the interval `0..1024`.
/// 1. Round the value (see precision below).
/// 1. Print the normalized value and the unit `B` with an appropriate binary prefix.
///
/// The precision specified in format strings is interpreted differently compared to standard
/// floating-point uses:
///
/// - If the number of digits of the integer part of the normalized value is greater than or
///   equal to the precision, print the integer value.
/// - Else, print the value with the number of significant digits set by the precision.
///
/// A specified precision of `0` is replaced by `1`. The default precision is `3`.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanReadableBytes(pub u64);

impl std::fmt::Display for HumanReadableBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const BASE: u64 = 1024;
        const UNITS: [&str; 6] = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
        let value = self.0;

        if value < BASE {
            return write!(f, "{value} B");
        }

        // We know that `value >= 1024`, so `exponent >= 1`.
        let exponent = value.ilog(BASE);
        let normalized_value = value as f64 / BASE.pow(exponent) as f64;
        let unit =
            UNITS[usize::try_from(exponent - 1).expect("we assume at least a 32-bit architecture")];

        // Get correct number of significant digits (not rounding integer part).
        let normalized_integer_digits = normalized_value.log10() as usize + 1;
        let set_precision = f.precision().unwrap_or(3).max(1);
        let precision = if set_precision > normalized_integer_digits {
            set_precision - normalized_integer_digits
        } else {
            0
        };

        write!(f, "{normalized_value:.*} {unit}", precision)
    }
}

#[cfg(test)]
mod tests {
    use walrus_test_utils::param_test;

    use super::*;

    param_test! {
        test_display_without_precision: [
            b_0: (0, "0 B"),
            b_1: (1, "1 B"),
            b_1023: (1023, "1023 B"),
            kib_1: (1024, "1.00 KiB"),
            kib_99: (1024 * 99, "99.0 KiB"),
            kib_100: (1024 * 100, "100 KiB"),
            kib_1023: (1024 * 1023, "1023 KiB"),
            eib_1: (1024_u64.pow(6), "1.00 EiB"),
            u64_max: (u64::MAX, "16.0 EiB"),
        ]
    }
    fn test_display_without_precision(bytes: u64, expected_result: &str) {
        assert_eq!(
            format!("{}", HumanReadableBytes(bytes)),
            expected_result.to_string()
        );
    }

    param_test! {
        test_display_with_explicit_precision: [
            b_0_p0: (0, 0, "0 B"),
            b_1_p0: (1, 0, "1 B"),
            b_1023_p0: (1023, 0, "1023 B"),
            kib_1_p0: (1024, 0, "1 KiB"),
            kib_99_p0: (1024 * 99, 0, "99 KiB"),
            kib_100_p0: (1024 * 100, 0, "100 KiB"),
            kib_1023_p0: (1024 * 1023, 0, "1023 KiB"),
            eib_1_p0: (1024_u64.pow(6), 0, "1 EiB"),
            u64_max_p0: (u64::MAX, 0, "16 EiB"),
            b_1_p1: (1, 1, "1 B"),
            b_1023_p1: (1023, 1, "1023 B"),
            kib_1_p1: (1024, 1, "1 KiB"),
            b_1_p5: (1, 5, "1 B"),
            b_1023_p5: (1023, 5, "1023 B"),
            kib_1_p5: (1024, 5, "1.0000 KiB"),
            b1025_p5: (1025, 5, "1.0010 KiB"),
        ]
    }
    fn test_display_with_explicit_precision(bytes: u64, precision: usize, expected_result: &str) {
        assert_eq!(
            format!("{:.*}", precision, HumanReadableBytes(bytes)),
            expected_result.to_string()
        );
    }
}
