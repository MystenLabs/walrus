// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! The proxy side of the registration and tipping system.
//!
//! It is responsible of parsing the information from the received transactions and submitting the
//! transaction to the full nodes for execution, finally verifying that the tip amount was
//! correctly provided.

use std::{fmt::Debug, num::NonZeroU16, str::FromStr};

use fastcrypto::encoding::Base64;
use sui_sdk::{
    SUI_COIN_TYPE,
    rpc_types::{SuiTransactionBlockResponse, SuiTransactionBlockResponseOptions},
};
use sui_types::{TypeTag, quorum_driver_types::ExecuteTransactionRequestType};
use walrus_core::{BlobId, encoding};
use walrus_sui::{
    client::{SuiClientResult, retry_client::RetriableSuiClient},
    types::{BlobEvent, BlobRegistered},
};

use crate::tip::{config::TipConfig, error::TipError};

/// Checks that the submitted transactions are valid and contain a sufficient tip.
#[derive(Clone)]
pub(crate) struct TipChecker {
    config: TipConfig,
    sui_client: RetriableSuiClient,
    n_shards: NonZeroU16,
}

impl Debug for TipChecker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TipChecker")
            .field("config", &self.config)
            .field("n_shards", &self.n_shards)
            .finish()
    }
}

impl TipChecker {
    /// Creates a new checker.
    pub(crate) fn new(
        config: TipConfig,
        sui_client: RetriableSuiClient,
        n_shards: NonZeroU16,
    ) -> Self {
        Self {
            config,
            sui_client,
            n_shards,
        }
    }

    /// Returns the configuration for the checker.
    pub(crate) fn config(&self) -> &TipConfig {
        &self.config
    }

    /// Checks the transaction correctly registers the blob of given ID and tips the proxy.
    pub(crate) async fn execute_and_check_transaction(
        &self,
        tx_bytes: Base64,
        signatures: Vec<Base64>,
        expected_blob_id: BlobId,
    ) -> Result<BlobRegistered, TipError> {
        let response = self
            .execute_transaction_from_bytes(tx_bytes, signatures)
            .await
            .map_err(Box::new)?;

        // Check that the transaction has registered the blob ID.
        let registration =
            self.blob_registration_from_response(response.clone(), expected_blob_id)?;

        // Check that the transaction contains an appropriate tip.
        let encoded_size = encoding::encoded_blob_length_for_n_shards(
            self.n_shards,
            registration.size,
            registration.encoding_type,
        )
        .ok_or(TipError::EncodedBlobLengthFailed)?;
        self.check_response_tip(&response, encoded_size)?;

        Ok(registration)
    }

    /// Submits the transaction received from the client for execution to the full nodes.
    pub(crate) async fn execute_transaction_from_bytes(
        &self,
        tx_bytes: Base64,
        signatures: Vec<Base64>,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        // NOTE: From Hayes, weird edge cases exist when re-submitting already executed
        // transactions. We should either validate that running a tx on another node returns all
        // required data, or we should just wait for the tx on the proxy, and get events from that.
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
    ///
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

        match &self.config {
            TipConfig::NoTip => {
                // `TipConfig::Notip` means no tip is required, and we are always ok.
                Ok(())
            }
            TipConfig::SendTip { address, kind } => {
                let expected_tip = kind.compute_tip(encoded_size) as i128;

                // Go across all balance changes, looking for one that says the sui for the current
                // address have increased of at least the expected tip.
                for change in balance_changes.iter() {
                    let owner_address = change.owner.get_address_owner_address().map_err(|_| {
                        TipError::UnexpectedResponse(
                            "could not extract the owner address".to_owned(),
                        )
                    })?;

                    if owner_address == *address
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
