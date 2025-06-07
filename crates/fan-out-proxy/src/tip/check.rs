// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy side of the registration and tipping system.
//!
//! It is responsible for validating that a given transaction contains appropriate balance changes
//! to the associated address.

use std::{num::NonZeroU16, str::FromStr};

use sui_sdk::{SUI_COIN_TYPE, rpc_types::SuiTransactionBlockResponse};
use sui_types::TypeTag;
use walrus_sdk::core::EncodingType;

use crate::tip::{config::TipConfig, error::TipError};

/// Checks if the execution results are sufficient to cover the tip.
pub fn check_response_tip(
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
            let expected_tip = kind
                .compute_tip(n_shards, unencoded_size, encoding_type)
                .ok_or(TipError::EncodedBlobLengthFailed)? as i128;
            tracing::debug!(%expected_tip, "checking tip");

            let Some(balance_changes) = response.balance_changes.as_ref() else {
                tracing::debug!("no balance changes found");
                return Err(TipError::UnexpectedResponse(
                    "no balance changes in the provided transaction".to_owned(),
                ));
            };

            // Go across all balance changes, looking for one that says the sui for the current
            // address have increased of at least the expected tip.
            for change in balance_changes.iter() {
                let owner_address = change.owner.get_address_owner_address().map_err(|_| {
                    TipError::UnexpectedResponse("could not extract the owner address".to_owned())
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
