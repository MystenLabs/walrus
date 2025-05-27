// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy side of the registration and tipping system.
//!
//! It is responsible of parsing the information from the received transactions and submitting the
//! transaction to the full nodes for execution, finally verifying that the tip amount was
//! correctly provided.

use std::str::FromStr;

use fastcrypto::encoding::Base64;
use sui_sdk::{
    SUI_COIN_TYPE,
    rpc_types::{SuiTransactionBlockResponse, SuiTransactionBlockResponseOptions},
};
use sui_types::{TypeTag, quorum_driver_types::ExecuteTransactionRequestType};
use walrus_core::BlobId;
use walrus_sui::{
    client::{SuiClientResult, retry_client::RetriableSuiClient},
    types::{BlobEvent, BlobRegistered},
};

use crate::tip::{config::TipConfig, error::TipError};

/// Checks that the submitted transactions are valid and contain a sufficient tip.
pub(crate) struct TipChecker {
    config: TipConfig,
    sui_client: RetriableSuiClient,
}

impl TipChecker {
    /// Creates a new checker.
    pub(crate) fn new(config: TipConfig, sui_client: RetriableSuiClient) -> Self {
        Self { config, sui_client }
    }

    /// Submits the transaction received from the client for execution to the full nodes.
    pub(crate) async fn execute_transaction_from_bytes(
        &self,
        tx_bytes: Base64,
        signatures: Vec<Base64>,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        self.sui_client
            .execute_transaction_from_bytes(
                tx_bytes,
                signatures,
                SuiTransactionBlockResponseOptions::new()
                    // We only need events and balance changes.
                    .with_events()
                    .with_balance_changes(),
                Some(ExecuteTransactionRequestType::WaitForEffectsCert),
                // TODO: is this the right way of using `method`?
                "execute_transaction",
            )
            .await
    }

    /// Returns the blob registration for the given blob ID.
    /// //
    /// If the expected registration is found, the function will return the registration
    /// information. Otherwise, it will return an error.
    pub(crate) fn blob_registration_from_response(
        &self,
        response: SuiTransactionBlockResponse,
        expected_blob_id: BlobId,
    ) -> Result<BlobRegistered, TipError> {
        let registrations = blob_registrations_from_response(response);

        registrations
            .into_iter()
            .find(|reg| reg.blob_id == expected_blob_id)
            .ok_or(TipError::BlobIdNotRegistered(expected_blob_id))
    }

    /// Checks if the execution results are sufficient to cover the tip.
    pub(crate) fn check_response_tip(
        &self,
        response: &SuiTransactionBlockResponse,
        encoded_size: u64,
    ) -> Result<(), TipError> {
        let Some(balance_changes) = response.balance_changes.as_ref() else {
            return Err(TipError::UnexpectedResponse(
                "no balance changes in the provided transaction".to_owned(),
            ));
        };

        let Some(expected_tip) = self.config.tip.compute_tip(encoded_size) else {
            // `None` means no tip is required, and we are always ok.
            return Ok(());
        };
        let expected_tip = expected_tip as i128;

        for change in balance_changes.iter() {
            let owner_address = change.owner.get_address_owner_address().map_err(|_| {
                TipError::UnexpectedResponse("could not extract the owner address".to_owned())
            })?;

            if owner_address == self.config.address
                && change.coin_type
                    == TypeTag::from_str(SUI_COIN_TYPE).expect("SUI is always valid")
            {
                if change.amount >= expected_tip {
                    return Ok(());
                } else {
                    return Err(TipError::InsufficientTip(change.amount, expected_tip));
                };
            } else {
                continue;
            }
        }
        Err(TipError::NoTipSent)
    }
}

/// Returns all the blob events contained in the response.
pub(crate) fn blob_registrations_from_response(
    response: SuiTransactionBlockResponse,
) -> Vec<BlobRegistered> {
    let Some(events) = response.events else {
        return vec![];
    };

    events
        .data
        .into_iter()
        .filter_map(|sui_event| {
            sui_event.try_into().ok().and_then(|blob_event| {
                if let BlobEvent::Registered(registration) = blob_event {
                    Some(registration)
                } else {
                    None
                }
            })
        })
        .collect::<Vec<_>>()
}
