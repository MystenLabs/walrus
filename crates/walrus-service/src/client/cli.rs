// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the walrus cli tools.

use std::{
    fmt::{self, Display},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{anyhow, Context, Result};
use colored::{ColoredString, Colorize};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use sui_sdk::{wallet_context::WalletContext, SuiClientBuilder};
use sui_types::event::EventID;
use walrus_core::BlobId;
use walrus_sui::client::{SuiContractClient, SuiReadClient};

use super::{default_configuration_paths, Blocklist, Client, Config};

mod args;
mod cli_output;
mod runner;
mod utils;
pub use args::{App, CliCommands, Commands, DaemonCommands};
pub use cli_output::CliOutput;
pub use runner::ClientCommandRunner;
pub use utils::{init_scoped_tracing_subscriber, init_tracing_subscriber};

/// Default URL of the testnet RPC node.
pub const TESTNET_RPC: &str = "https://fullnode.testnet.sui.io:443";
/// Default RPC URL to connect to if none is specified explicitly or in the wallet config.
pub const DEFAULT_RPC_URL: &str = TESTNET_RPC;

/// Returns the path if it is `Some` or any of the default paths if they exist (attempt in order).
pub fn path_or_defaults_if_exist(path: &Option<PathBuf>, defaults: &[PathBuf]) -> Option<PathBuf> {
    tracing::debug!(?path, ?defaults, "looking for configuration file");
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
pub fn load_wallet_context(path: &Option<PathBuf>) -> Result<WalletContext> {
    let mut default_paths = vec!["./sui_config.yaml".into()];
    if let Some(home_dir) = home::home_dir() {
        default_paths.push(home_dir.join(".sui").join("sui_config").join("client.yaml"))
    }
    let path = path_or_defaults_if_exist(path, &default_paths)
        .ok_or(anyhow!("could not find a valid wallet config file"))?;
    tracing::info!("using Sui wallet configuration from '{}'", path.display());
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
        .ok_or(anyhow!("could not find a valid Walrus configuration file"))?;
    tracing::info!("using Walrus configuration from '{}'", path.display());

    serde_yaml::from_str(&std::fs::read_to_string(&path).context(format!(
        "unable to read Walrus configuration from '{}'",
        path.display()
    ))?)
    .context(format!(
        "parsing Walrus configuration from '{}' failed",
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
    blocklist_path: &Option<PathBuf>,
) -> Result<Client<()>> {
    let sui_read_client = get_sui_read_client_from_rpc_node_or_wallet(
        &config,
        rpc_url,
        wallet,
        allow_fallback_to_default,
    )
    .await?;
    let client = Client::new_read_client(config, &sui_read_client).await?;

    if blocklist_path.is_some() {
        Ok(client.with_blocklist(Blocklist::new(blocklist_path)?))
    } else {
        Ok(client)
    }
}

/// Creates a [`Client<ContractClient>`] based on the provided [`Config`] with write access to Sui.
pub async fn get_contract_client(
    config: Config,
    wallet: Result<WalletContext>,
    gas_budget: u64,
    blocklist_path: &Option<PathBuf>,
) -> Result<Client<SuiContractClient>> {
    let sui_client = SuiContractClient::new(wallet?, config.system_object, gas_budget).await?;
    let client = Client::new(config, sui_client).await?;

    if blocklist_path.is_some() {
        Ok(client.with_blocklist(Blocklist::new(blocklist_path)?))
    } else {
        Ok(client)
    }
}

/// Creates a [`SuiReadClient`] from the provided RPC URL or wallet.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `wallet` (if `Ok`) or the
/// default [`DEFAULT_RPC_URL`] if `allow_fallback_to_default` is true.
// NB: When making changes to the logic, make sure to update the docstring of `get_read_client` and
// the argument docs in `crates/walrus-service/bin/client.rs`.
pub async fn get_sui_read_client_from_rpc_node_or_wallet(
    config: &Config,
    rpc_url: Option<String>,
    wallet: Result<WalletContext>,
    allow_fallback_to_default: bool,
) -> Result<SuiReadClient> {
    tracing::debug!(
        ?rpc_url,
        %allow_fallback_to_default,
        "attempting to create a read client from explicitly set RPC URL, wallet config, or default"
    );
    let sui_client = match rpc_url {
        Some(url) => {
            tracing::info!("using explicitly set RPC URL {url}");
            SuiClientBuilder::default()
                .build(&url)
                .await
                .context(format!("cannot connect to Sui RPC node at {url}"))
        }
        None => match wallet {
            Ok(wallet) => {
                tracing::info!("using RPC URL set in wallet configuration");
                wallet
                    .get_client()
                    .await
                    .context("cannot connect to Sui RPC node specified in the wallet configuration")
            }
            Err(e) => {
                if allow_fallback_to_default {
                    tracing::info!("using default RPC URL {DEFAULT_RPC_URL}");
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
    }?;

    Ok(SuiReadClient::new(sui_client, config.system_object).await?)
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

/// A human readable representation of a price in MIST.
///
/// [`HumanReadableMist`] is a helper type to format prices in MIST. The formatting works as
/// follows:
///
/// 1. If the price is below 1_000_000 MIST, it is printed fully, with thousands separators.
/// 2. Else, it is printed in SUI with 3 decimal places.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanReadableMist(pub u64);

impl Display for HumanReadableMist {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = self.0;
        if value < 1_000_000 {
            let with_separator = thousands_separator(value);
            return write!(f, "{with_separator} MIST");
        }
        let digits = if value < 10_000_000 { 4 } else { 3 };
        let sui = mist_to_sui(value);
        write!(f, "{sui:.digits$} SUI",)
    }
}

fn mist_to_sui(mist: u64) -> f64 {
    mist as f64 / 1e9
}

/// Returns a string representation of the input `num`, with digits grouped in threes by a
/// separator.
fn thousands_separator(num: u64) -> String {
    num.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .expect("going from utf8 to bytes and back always works")
        .join(",")
}

/// Reads a blob from the filesystem or returns a helpful error message.
pub fn read_blob_from_file(path: impl AsRef<Path>) -> anyhow::Result<Vec<u8>> {
    fs::read(&path).context(format!(
        "unable to read blob from '{}'",
        path.as_ref().display()
    ))
}

/// Format the event ID as the transaction digest and the sequence number.
pub fn format_event_id(event_id: &EventID) -> String {
    format!("(tx: {}, seq: {})", event_id.tx_digest, event_id.event_seq)
}

/// Error type distinguishing between a decimal value that corresponds to a valid blob ID and any
/// other parse error.
#[derive(Debug, thiserror::Error)]
pub enum BlobIdParseError {
    /// Returned when attempting to parse a decimal value for the blob ID.
    #[error(
        "you seem to be using a numeric value in decimal format corresponding to a Walrus blob ID \
        (maybe copied from a Sui explorer) whereas Walrus uses URL-safe base64 encoding;\n\
        the Walrus blob ID corresponding to the provided value is {0}"
    )]
    BlobIdInDecimalFormat(BlobId),
    /// Returned when attempting to parse any other invalid string as a blob ID.
    #[error("the provided blob ID is invalid")]
    InvalidBlobId,
}

/// Attempts to parse the blob ID and provides a detailed error when the blob ID was provided in
/// decimal format.
pub fn parse_blob_id(input: &str) -> Result<BlobId, BlobIdParseError> {
    if let Ok(blob_id) = BlobId::from_str(input) {
        return Ok(blob_id);
    }
    Err(match BlobIdDecimal::from_str(input) {
        Err(_) => BlobIdParseError::InvalidBlobId,
        Ok(blob_id) => BlobIdParseError::BlobIdInDecimalFormat(blob_id.into()),
    })
}

/// Helper struct to parse and format blob IDs as decimal numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[repr(transparent)]
pub struct BlobIdDecimal(BlobId);

/// Error returned when unable to parse a decimal value corresponding to a Walrus blob ID.
#[derive(Debug, thiserror::Error)]
pub enum BlobIdDecimalParseError {
    /// Returned when attempting to parse an actual Walrus blob ID.
    #[error(
        "the provided value is already a valid Walrus blob ID;\n\
        the value represented as a decimal number is {0}"
    )]
    BlobIdInBase64Format(BlobIdDecimal),
    /// Returned when attempting to parse any other invalid string as a decimal blob ID.
    #[error("the provided value cannot be converted to a Walrus blob ID")]
    InvalidBlobId,
}

impl FromStr for BlobIdDecimal {
    type Err = BlobIdDecimalParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some(number) = BigUint::parse_bytes(s.as_bytes(), 10) {
            let bytes = number.to_bytes_le();

            if bytes.len() <= BlobId::LENGTH {
                let mut blob_id = [0; BlobId::LENGTH];
                blob_id[..bytes.len()].copy_from_slice(&bytes);
                return Ok(Self(BlobId(blob_id)));
            }
        }

        Err(if let Ok(blob_id) = BlobId::from_str(s) {
            BlobIdDecimalParseError::BlobIdInBase64Format(blob_id.into())
        } else {
            BlobIdDecimalParseError::InvalidBlobId
        })
    }
}

impl From<BlobIdDecimal> for BlobId {
    fn from(value: BlobIdDecimal) -> Self {
        value.0
    }
}

impl From<BlobId> for BlobIdDecimal {
    fn from(value: BlobId) -> Self {
        Self(value)
    }
}

impl Display for BlobIdDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", BigUint::from_bytes_le(self.0.as_ref()))
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

    param_test! {
        test_thousands_separator: [
            thousand: (1_000, "1,000"),
            million: (2_000_000, "2,000,000"),
            hundred_million: (123_456_789, "123,456,789"),
        ]
    }
    fn test_thousands_separator(num: u64, expected: &str) {
        assert_eq!(thousands_separator(num), expected);
    }

    param_test! {
        test_human_readable_mist: [
            ten: (10, "10 MIST"),
            ten_thousand: (10_000, "10,000 MIST"),
            million: (1_000_000, "0.0010 SUI"),
            nine_million: (9_123_456, "0.0091 SUI"),
            ten_million: (10_123_456, "0.010 SUI"),
        ]
    }
    fn test_human_readable_mist(mist: u64, expected: &str) {
        assert_eq!(&format!("{}", HumanReadableMist(mist)), expected,)
    }

    param_test! {
        test_parse_blob_id_matches_expected_slice_prefix: [
            valid_base64: ("M5YQinGO3RoRLaW_KbCfvXStVlWEqO5dGDe5cSiN07I", &[]),
            // A 43-digit decimal value is parsed as a base-64 string.
            valid_base64_and_decimal: (
                "1000000000000000000000000000000000000000000",
                &[215, 77, 52, 211, 77, 52, 211, 77, 52, 211, 77, 52, 211, 77, 52]),
    ]}
    fn test_parse_blob_id_matches_expected_slice_prefix(
        input_string: &str,
        expected_result: &[u8],
    ) {
        let blob_id = parse_blob_id(input_string).unwrap();
        assert_eq!(blob_id.0[..expected_result.len()], expected_result[..]);
    }

    param_test! {
        test_parse_blob_id_provides_correct_decimal_error: [
            zero: ("0", &[0; 32]),
            ten: ("256", &[0, 1]),
            valid_decimal: (
                "80885466015098902458382552429473803233277035186046880821304527730792838764083",
                &[]
            ),
    ]}
    fn test_parse_blob_id_provides_correct_decimal_error(
        input_string: &str,
        expected_result: &[u8],
    ) {
        let Err(BlobIdParseError::BlobIdInDecimalFormat(blob_id)) = parse_blob_id(input_string)
        else {
            panic!()
        };
        assert_eq!(blob_id.0[..expected_result.len()], expected_result[..]);
    }

    param_test! {
        test_parse_blob_id_failure: [
            empty: (""),
            too_short_base64: ("aaaa"),
            two_pow_256: (
                "115792089237316195423570985008687907853269984665640564039457584007913129639936"
            )
        ]
    }
    fn test_parse_blob_id_failure(input_string: &str) {
        assert!(matches!(
            parse_blob_id(input_string),
            Err(BlobIdParseError::InvalidBlobId)
        ));
    }
}
