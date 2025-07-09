// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy side of the registration and tipping system.
//!
//! It is responsible for validating that a given transaction contains appropriate balance changes
//! to the associated address.

use std::{
    num::NonZeroU16,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sui_sdk::{SUI_COIN_TYPE, rpc_types::SuiTransactionBlockResponse};
use sui_types::TypeTag;
use tracing::Level;
use walrus_sdk::core::EncodingType;

use crate::tip::{config::TipConfig, error::TipError};

/// Checks the freshness of the transaction.
///
/// Returns an error if the transaction was sent too far in the past.
#[tracing::instrument(level = Level::DEBUG, skip_all, fields(tx_id=%response.digest))]
pub(crate) fn check_tx_freshness(
    response: &SuiTransactionBlockResponse,
    freshness_threshold: Duration,
    max_future_threshold: Duration,
) -> Result<(), TipError> {
    let Some(tx_timestamp_ms) = response.timestamp_ms else {
        return Err(TipError::UnexpectedResponse(
            "the transaction does not have a timestamp; it has not been executed",
        ));
    };
    let tx_timestamp = Duration::from_millis(tx_timestamp_ms);
    let cur_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clocks are set correctly");

    if let Some(diff) = tx_timestamp.checked_sub(cur_timestamp) {
        // The timestamp is in the future.
        tracing::warn!(
            ?tx_timestamp,
            ?cur_timestamp,
            "a transaction with a timestamp in the future was received"
        );
        if diff > max_future_threshold {
            tracing::error!(
                ?diff,
                "the transaction is in the future by more than the allowed threshold"
            );
            return Err(TipError::UnexpectedResponse(
                "the transaction is in the future by more than the allowed threshold",
            ));
        }
    };

    if let Some(diff) = cur_timestamp.checked_sub(tx_timestamp) {
        if diff > freshness_threshold {
            tracing::debug!(?diff, "the transaction is too old");
            return Err(TipError::TxTooOld);
        }
    }

    Ok(())
}

/// Checks if the execution results are sufficient to cover the tip.
pub(crate) fn check_response_tip(
    tip_config: &TipConfig,
    response: &SuiTransactionBlockResponse,
    unencoded_size: u64,
    n_shards: NonZeroU16,
    encoding_type: EncodingType,
) -> Result<(), TipError> {
    match tip_config {
        TipConfig::NoTip => {
            // `TipConfig::Notip` means no tip is required, and we are always ok.
            Ok(())
        }
        TipConfig::SendTip { address, kind } => {
            let expected_tip = i128::from(
                kind.compute_tip(n_shards, unencoded_size, encoding_type)
                    .ok_or(TipError::EncodedBlobLengthFailed)?,
            );
            tracing::debug!(%expected_tip, "checking tip");

            let Some(balance_changes) = response.balance_changes.as_ref() else {
                tracing::debug!("no balance changes found");
                return Err(TipError::UnexpectedResponse(
                    "no balance changes in the provided transaction; it has not been executed",
                ));
            };

            // Go across all balance changes, looking for one that says the sui for the current
            // address have increased of at least the expected tip.
            for change in balance_changes.iter() {
                let owner_address = change.owner.get_address_owner_address().map_err(|_| {
                    TipError::UnexpectedResponse("could not extract the owner address")
                })?;

                tracing::debug!(%owner_address, "checking balance changes for address");

                if owner_address == *address
                    && change.coin_type
                        == TypeTag::from_str(SUI_COIN_TYPE).expect("SUI is always valid")
                {
                    if change.amount >= expected_tip {
                        return Ok(());
                    } else {
                        tracing::debug!("insufficient tip");
                        return Err(TipError::InsufficientTip(change.amount, expected_tip));
                    };
                } else {
                    continue;
                }
            }
            Err(TipError::NoTipSent)
        }
    }
}
