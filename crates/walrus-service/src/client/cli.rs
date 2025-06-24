// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Utilities for running the walrus cli tools.

use std::{
    fmt::{self, Display},
    path::PathBuf,
    str::FromStr,
};

use anyhow::{Context, Result};
use colored::{Color, ColoredString, Colorize};
use num_bigint::BigUint;
use serde::{Deserialize, Serialize};
use walrus_core::{BlobId, QuiltPatchId, encoding::QuiltError};
use walrus_sdk::{
    blocklist::Blocklist,
    client::Client,
    config::ClientConfig,
    sui::client::{SuiContractClient, SuiReadClient, retry_client::RetriableSuiClient},
};
use walrus_sui::wallet::Wallet;

mod args;
mod cli_output;
mod runner;
pub use args::{
    AggregatorArgs,
    App,
    BlobIdentity,
    CliCommands,
    Commands,
    DaemonCommands,
    HealthSortBy,
    NodeSelection,
    NodeSortBy,
    PublisherArgs,
    QuiltBlobInput,
    QuiltPatchByIdentifier,
    QuiltPatchByPatchId,
    QuiltPatchByTag,
    QuiltPatchSelector,
    SortBy,
};
pub use cli_output::CliOutput;
pub use runner::ClientCommandRunner;

/// Creates a [`Client`] based on the provided [`ClientConfig`] with read-only access to Sui.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `rpc_url` field in the
/// `config` (if `Some`), or the `wallet` (if `Ok`). An error is returned if it cannot be set
/// successfully.
pub async fn get_read_client(
    config: ClientConfig,
    rpc_url: Option<String>,
    wallet: Result<Wallet>,
    blocklist_path: &Option<PathBuf>,
) -> Result<Client<SuiReadClient>> {
    let sui_read_client =
        get_sui_read_client_from_rpc_node_or_wallet(&config, rpc_url, wallet).await?;

    let refresh_handle = config
        .refresh_config
        .build_refresher_and_run(sui_read_client.clone())
        .await?;
    let client = Client::new_read_client(config, refresh_handle, sui_read_client).await?;

    if blocklist_path.is_some() {
        Ok(client.with_blocklist(Blocklist::new(blocklist_path)?))
    } else {
        Ok(client)
    }
}

/// Creates a [`Client<SuiContractClient>`] based on the provided [`ClientConfig`] with
/// write access to Sui.
pub async fn get_contract_client(
    config: ClientConfig,
    wallet: Result<Wallet>,
    gas_budget: Option<u64>,
    blocklist_path: &Option<PathBuf>,
) -> Result<Client<SuiContractClient>> {
    let sui_client = config.new_contract_client(wallet?, gas_budget).await?;

    let refresh_handle = config
        .refresh_config
        .build_refresher_and_run(sui_client.read_client().clone())
        .await?;
    let client = Client::new_contract_client(config, refresh_handle, sui_client).await?;

    if blocklist_path.is_some() {
        Ok(client.with_blocklist(Blocklist::new(blocklist_path)?))
    } else {
        Ok(client)
    }
}

/// Creates a [`SuiReadClient`] from the provided RPC URL or wallet.
///
/// The RPC URL is set based on the `rpc_url` parameter (if `Some`), the `rpc_url` field in the
/// `config` (if `Some`), or the `wallet` (if `Ok`). An error is returned if it cannot be set
/// successfully.
// NB: When making changes to the logic, make sure to update the docstring of `get_read_client` and
// the argument docs in `crates/walrus-service/client/cli/args.rs`.
pub async fn get_sui_read_client_from_rpc_node_or_wallet(
    config: &ClientConfig,
    rpc_url: Option<String>,
    wallet: Result<Wallet>,
) -> Result<SuiReadClient> {
    tracing::debug!(
        ?rpc_url,
        ?config.rpc_urls,
        "attempting to create a read client from explicitly set RPC URL, RPC URLs in client \
        config, or wallet config"
    );
    let backoff_config = config.backoff_config().clone();
    let rpc_urls = match (rpc_url, &config.rpc_urls, wallet) {
        (Some(url), _, _) => {
            tracing::info!("using explicitly set RPC URL: {url}");
            vec![url]
        }
        (_, urls, _) if !urls.is_empty() => {
            tracing::info!(
                "using RPC URLs set in client configuration: {}",
                urls.join(", ")
            );
            urls.clone()
        }
        (_, _, Ok(wallet)) => {
            let url = wallet
                .get_active_env()
                .context("unable to get the wallet's active environment")?
                .rpc
                .clone();
            tracing::info!("using RPC URL set in wallet configuration: {url}");
            vec![url]
        }
        (_, _, Err(e)) => {
            anyhow::bail!(
                "Sui RPC url is not specified as a CLI argument or in the client configuration, \
                and no valid Sui wallet was provided ({e})"
            );
        }
    };

    let sui_client = RetriableSuiClient::new_for_rpc_urls(
        &rpc_urls,
        backoff_config,
        config.communication_config.sui_client_request_timeout,
    )
    .await
    .context(format!(
        "cannot connect to Sui RPC nodes at {}",
        rpc_urls.join(", ")
    ))?;

    Ok(config.new_read_client(sui_client).await?)
}

/// Returns the string `Success:` colored in green for terminal output.
pub fn success() -> ColoredString {
    "Success:".bold().walrus_teal()
}

/// Returns the string `Error:` colored in red for terminal output.
pub fn error() -> ColoredString {
    "Error:".bold().red()
}

/// Returns the string `Warning:` colored in yellow for terminal output.
pub fn warning() -> ColoredString {
    "Warning:".bold().yellow()
}

// Custom colors to match the Walrus brand identity.

/// The Walrus teal color.
///
/// The original walrus teal is `#99efe4`. However, to remain compatible with 256-color
/// terminals, we use the closest color available in the 256-color palette, which is `122
/// (#87ffd7)`.
pub const WALRUS_TEAL: Color = Color::TrueColor {
    r: 135,
    g: 255,
    b: 215,
};
/// The Walrus purple color.
///
/// The original walrus purple is `#c484f6 `. However, to remain compatible with 256-color
/// terminals, we use the closest color available in the 256-color palette, which is `177
/// (#d787ff)`.
pub const WALRUS_PURPLE: Color = Color::TrueColor {
    r: 215,
    g: 135,
    b: 255,
};

/// Trait to color strings with the Walrus brand colors.
pub trait WalrusColors {
    /// Colors the string in the Walrus teal color.
    fn walrus_teal(self) -> ColoredString;

    /// Colors the string in the Walrus purple color.
    fn walrus_purple(self) -> ColoredString;
}

impl WalrusColors for ColoredString {
    fn walrus_teal(self) -> ColoredString {
        self.color(WALRUS_TEAL)
    }

    fn walrus_purple(self) -> ColoredString {
        self.color(WALRUS_PURPLE)
    }
}

impl WalrusColors for &str {
    fn walrus_teal(self) -> ColoredString {
        self.color(WALRUS_TEAL)
    }

    fn walrus_purple(self) -> ColoredString {
        self.color(WALRUS_PURPLE)
    }
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
        #[allow(clippy::cast_possible_truncation)] // the truncation is intentional here
        let normalized_integer_digits = normalized_value.log10().floor() as usize + 1;
        let set_precision = f.precision().unwrap_or(3).max(1);
        let precision = set_precision.saturating_sub(normalized_integer_digits);

        write!(f, "{normalized_value:.*} {unit}", precision)
    }
}

trait CurrencyForDisplay {
    /// The name of the main unit of currency
    const SUPERUNIT_NAME: &'static str;
    /// The name of the subunit, that divides the main currency.
    const UNIT_NAME: &'static str;
    /// Number of decimal places the coin uses: 1 superunit is equal to 10^decimal units.
    const DECIMALS: u8;

    /// Converts the value in base units to the value in the superunit.
    fn unit_to_superunit(value: u64) -> f64 {
        value as f64 / Self::units_in_superunit() as f64
    }

    fn units_in_superunit() -> u64 {
        10u64.pow(u32::from(Self::DECIMALS))
    }

    /// Gets the value of the current coin.
    fn value(&self) -> u64;

    // Creates a new instance of the currency.
    fn new(value: u64) -> Self;
}

/// The representation of a currency in a human readable format.
#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HumanReadableCoin<C>(C);

impl<C: CurrencyForDisplay> From<u64> for HumanReadableCoin<C> {
    fn from(value: u64) -> Self {
        Self(C::new(value))
    }
}

impl<C: CurrencyForDisplay> Display for HumanReadableCoin<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0.value() == 0 {
            return write!(f, "0 {}", C::UNIT_NAME);
        }
        // We ensure that the values displayed in C::UNIT_NAME always require less or equal to 4
        // decimal digits to be readable.
        let ratio = C::units_in_superunit() / self.0.value();
        if ratio > 10_000 {
            let with_separator = thousands_separator(self.0.value());
            return write!(f, "{with_separator} {}", C::UNIT_NAME);
        }
        let digits = if ratio <= 100 { 3 } else { 4 };
        let sui = C::unit_to_superunit(self.0.value());

        write!(
            f,
            "{} {}",
            thousands_separator_float(sui, digits),
            C::SUPERUNIT_NAME
        )
    }
}

/// The SUI coin for simple display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SuiCoin(u64);

/// The human readable representation of the MIST and SUI coins.
pub type HumanReadableMist = HumanReadableCoin<SuiCoin>;

impl CurrencyForDisplay for SuiCoin {
    const SUPERUNIT_NAME: &'static str = "SUI";
    const UNIT_NAME: &'static str = "MIST";
    const DECIMALS: u8 = 9;

    fn value(&self) -> u64 {
        self.0
    }

    fn new(value: u64) -> Self {
        Self(value)
    }
}

/// The WAL coin for simple display.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalCoin(u64);

/// The human readable representation of the FROST and WAL coins.
pub type HumanReadableFrost = HumanReadableCoin<WalCoin>;

impl CurrencyForDisplay for WalCoin {
    const SUPERUNIT_NAME: &'static str = "WAL";
    const UNIT_NAME: &'static str = "FROST";
    const DECIMALS: u8 = 9;

    fn value(&self) -> u64 {
        self.0
    }

    fn new(value: u64) -> Self {
        Self(value)
    }
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

#[allow(clippy::cast_possible_truncation)] // the truncation is intentional here
fn thousands_separator_float(num: f64, digits: u32) -> String {
    let integer = num.floor() as u64;
    let decimal = (num.fract() * 10u64.pow(digits) as f64).round() as u64;
    let digits = digits as usize;
    format!("{}.{:0digits$}", thousands_separator(integer), decimal)
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

/// Parses a [`QuiltPatchId`] from a string.
pub fn parse_quilt_patch_id(input: &str) -> Result<QuiltPatchId, QuiltError> {
    if let Ok(quilt_id) = QuiltPatchId::from_str(input) {
        return Ok(quilt_id);
    }
    Err(QuiltError::QuiltPatchIdParseError(input.to_string()))
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
        test_thousands_separator_float: [
            thousand: (1_000.0, 3, "1,000.000"),
            million: (2_000_000.009, 3, "2,000,000.009"),
            hundred_million: (123_456_789.123_49, 4, "123,456,789.1235"),
        ]
    }
    fn test_thousands_separator_float(num: f64, digits: u32, expected: &str) {
        assert_eq!(thousands_separator_float(num, digits), expected);
    }

    param_test! {
        test_human_readable_mist: [
            zero: (0, "0 MIST"),
            ten: (10, "10 MIST"),
            ten_thousand: (10_000, "10,000 MIST"),
            hundred_thousand: (100_000, "0.0001 SUI"),
            hundred_thousand_and_one: (100_001, "0.0001 SUI"),
            million: (1_000_000, "0.0010 SUI"),
            nine_million: (9_123_456, "0.0091 SUI"),
            ten_million_exact: (10_000_000, "0.010 SUI"),
            ten_million: (10_123_456, "0.010 SUI"),
            hundred_million: (123_456_789, "0.123 SUI"),
        ]
    }
    fn test_human_readable_mist(mist: u64, expected: &str) {
        assert_eq!(&format!("{}", HumanReadableMist::from(mist)), expected,)
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
