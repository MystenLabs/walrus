// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
#![allow(unused)]
//! A client for the walrus upload relay.
use std::{
    fmt::Debug,
    fs,
    num::NonZeroU16,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use rand::{Rng, RngCore, rngs::ThreadRng};
use reqwest::{Response, Url};
use serde::Serialize;
use sui_sdk::rpc_types::SuiTransactionBlockResponse;
use sui_types::{
    base_types::SuiAddress,
    digests::TransactionDigest,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{Argument, Command, Transaction, TransactionData, TransactionKind},
};
use walrus_core::messages::BlobPersistenceType;
use walrus_sui::client::{
    SuiClientError,
    transaction_builder::build_transaction_data_with_min_gas_balance,
};
use walrus_utils::backoff::{self, BackoffStrategy, ExponentialBackoff, ExponentialBackoffConfig};

use crate::{
    ObjectID,
    client::WalrusNodeClient,
    config::{ClientConfig as WalrusConfig, load_configuration},
    core::{
        BlobId,
        EncodingType,
        EpochCount,
        encoding::EncodingFactory,
        messages::ConfirmationCertificate,
        metadata::{BlobMetadataApi, VerifiedBlobMetadataWithId},
    },
    sui::{
        client::{
            BlobPersistence,
            CoinType,
            SuiContractClient,
            transaction_builder::WalrusPtbBuilder,
        },
        config::WalletConfig,
        types::{BlobEvent, BlobRegistered},
        wallet::Wallet,
    },
    upload_relay::{
        ResponseType,
        TIP_CONFIG_ROUTE,
        blob_upload_relay_url,
        params::{AuthPackage, NONCE_LEN, Params},
        tip_config::{TipConfig, TipKind},
    },
};

/// The error type for the upload relay client.
#[derive(Debug, thiserror::Error)]
pub enum UploadRelayClientError {
    /// The tip configuration could not be fetched from the upload relay.
    #[error("failed to fetch tip configuration from upload relay: {0}")]
    TipConfigFetchFailed(reqwest::Error),

    /// The tip computation failed for the given blob size, n_shards, and encoding type.
    #[error(
        "tip computation failed for blob size {unencoded_length} bytes, \
        n_shards: {n_shards}, encoding_type: {encoding_type:?}"
    )]
    TipComputationFailed {
        /// The unencoded length of the blob.
        unencoded_length: u64,
        /// The number of shards for this network.
        n_shards: NonZeroU16,
        /// The encoding type.
        encoding_type: EncodingType,
    },

    /// The tip payment failed.
    #[error("failed to pay tip to relay: {0}")]
    TipPaymentFailed(#[from] Box<SuiClientError>),

    /// The upload relay request failed after a number of attempts.
    #[error("upload relay request failed: {0}")]
    UploadRequestFailed(#[from] reqwest::Error),

    /// The blob ID mismatch in the response.
    #[error("blob ID mismatch in update relay response: expected {expected}, got {actual}")]
    BlobIdMismatch {
        /// The expected blob ID.
        expected: BlobId,
        /// The actual blob ID.
        actual: BlobId,
    },

    /// The URL encoding failed.
    #[error("update relay URL encoding failed: {0}")]
    UrlEndocodingFailed(#[from] url::ParseError),
}

/// A client to communicate with the Walrus Upload Relay.
#[derive(Debug, Clone)]
pub struct UploadRelayClient {
    /// The user of the upload relay client. This is the account that will own the blob, and pay for
    /// its registration, etc.
    user_address: SuiAddress,
    /// The number of shards for this network.
    n_shards: NonZeroU16,
    /// The upload relay url.
    upload_relay: Url,
    /// The tip configuration.
    tip_config: TipConfig,
    /// The gas budget for the tip payment.
    gas_budget: Option<u64>,
    /// The HTTP client for making requests to the upload relay.
    http_client: reqwest::Client,
    /// The backoff configuration for retries.
    backoff_config: ExponentialBackoffConfig,
}

impl PartialEq for UploadRelayClient {
    fn eq(&self, other: &Self) -> bool {
        self.user_address == other.user_address
            && self.n_shards == other.n_shards
            && self.upload_relay == other.upload_relay
            && self.tip_config == other.tip_config
            && self.gas_budget == other.gas_budget
            && self.backoff_config == other.backoff_config
    }
}

impl UploadRelayClient {
    /// Fetches the tip configuration from the upload relay and creates a new upload relay tip
    /// client.
    pub async fn new(
        user_address: SuiAddress,
        n_shards: NonZeroU16,
        upload_relay: Url,
        gas_budget: Option<u64>,
        backoff_config: ExponentialBackoffConfig,
    ) -> Result<Self, UploadRelayClientError> {
        tracing::debug!(
            ?upload_relay,
            "fetching the tip confign and creating upload relay tip client"
        );
        let http_client = reqwest::Client::new();
        let tip_config = Self::get_tip_config_with_client(&http_client, &upload_relay).await?;
        Ok(Self {
            user_address,
            n_shards,
            upload_relay,
            tip_config,
            gas_budget,
            http_client,
            backoff_config,
        })
    }

    /// Pays the tip to the upload relay if required.
    ///
    /// Optionally returns the transaction ID of the payment transaction.
    pub async fn pay_tip_if_required(
        &self,
        sui_client: &SuiContractClient,
        auth_package: &AuthPackage,
        unencoded_length: u64,
        encoding_type: EncodingType,
    ) -> Result<Option<TransactionDigest>, UploadRelayClientError> {
        if let TipConfig::SendTip { address, kind } = &self.tip_config {
            tracing::debug!("tip payment required");
            let tip_amount = kind
                .compute_tip(self.n_shards, unencoded_length, encoding_type)
                .ok_or(UploadRelayClientError::TipComputationFailed {
                    unencoded_length,
                    n_shards: self.n_shards,
                    encoding_type,
                })?;

            let tx_id = self
                .pay_tip(sui_client, *address, auth_package, tip_amount)
                .await
                .map_err(|e| UploadRelayClientError::TipPaymentFailed(Box::new(e)))?;
            Ok(Some(tx_id))
        } else {
            Ok(None)
        }
    }

    /// Pays the tip to the upload relay, based on the blob's unencoded length.
    ///
    /// Returns the transaction ID of the payment transaction.
    async fn pay_tip(
        &self,
        sui_client: &SuiContractClient,
        relay_address: SuiAddress,
        auth_package: &AuthPackage,
        tip_amount: u64,
    ) -> Result<TransactionDigest, SuiClientError> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // The first input is the authentication package.
        pt_builder.pure(auth_package.to_hashed_nonce())?;

        // Pay the tip.
        let amount_arg = pt_builder.pure(tip_amount)?;
        let split_coin =
            pt_builder.command(Command::SplitCoins(Argument::GasCoin, vec![amount_arg]));
        pt_builder.transfer_arg(relay_address, split_coin);

        // Sign and execute.
        let gas_price = sui_client.read_client().get_reference_gas_price().await?;
        let transaction_data = build_transaction_data_with_min_gas_balance(
            pt_builder.finish(),
            gas_price,
            sui_client.read_client(),
            self.user_address,
            self.gas_budget,
            0, // No additional gas budget.
            tip_amount,
        )
        .await?;

        let response = sui_client
            .sign_and_send_transaction(transaction_data, "pay_tip")
            .await?;

        Ok(response.digest)
    }

    /// Sends the blob to the upload relay and waits for the certificate.
    ///
    /// Additionally, it pays the tip if required.
    ///
    /// NOTE: This function is somewhat suboptimal at the moment, as it pays the tip just before
    /// sending the data to the relay client. This means that its gas usage is not optimized -- it
    /// could be bundled in the registration PTBs, which would reduce the total gas usage.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub async fn send_blob_data_and_get_certificate_with_relay(
        &self,
        sui_client: &SuiContractClient,
        blob: &[u8],
        blob_id: BlobId,
        encoding_type: EncodingType,
        blob_persistence_type: BlobPersistenceType,
    ) -> Result<ConfirmationCertificate, UploadRelayClientError> {
        tracing::info!("using the upload relay to store the blob and getting the certificate");

        let auth_package = AuthPackage::new(blob);
        let unencoded_length = blob.len().try_into().expect("using a u32 or u64 arch");
        let tx_id = self
            .pay_tip_if_required(sui_client, &auth_package, unencoded_length, encoding_type)
            .await?;

        // Only add the nonce if we paid for the transaction.
        let nonce = tx_id.is_some().then_some(auth_package.nonce);
        let deletable_blob_object: Option<ObjectID> =
            if let BlobPersistenceType::Deletable { object_id } = blob_persistence_type {
                Some(object_id.into())
            } else {
                None
            };

        let params = Params {
            blob_id,
            nonce,
            tx_id,
            deletable_blob_object,
            encoding_type: Some(encoding_type),
        };

        let response = self.send_to_relay(blob, params).await?;
        if response.blob_id != blob_id {
            return Err(UploadRelayClientError::BlobIdMismatch {
                expected: blob_id,
                actual: response.blob_id,
            });
        }

        Ok(response.confirmation_certificate)
    }

    /// Returns a reference to the tip configuration.
    pub fn tip_config(&self) -> &TipConfig {
        &self.tip_config
    }

    /// Returns the number of shards for this network.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }

    /// Send the blob to the upload relay.
    async fn send_to_relay(
        &self,
        blob: &[u8],
        params: Params,
    ) -> Result<ResponseType, UploadRelayClientError> {
        let post_url = blob_upload_relay_url(&self.upload_relay, &params)?;

        tracing::debug!(
            ?post_url,
            ?params,
            "sending request to the walrus upload relay"
        );

        let response = self.send_with_retries(blob, &post_url).await?;
        tracing::debug!(?response, "upload relay response received");

        response
            .json()
            .await
            .map_err(UploadRelayClientError::UploadRequestFailed)
    }

    /// Sends the request repeatedly with retries.
    async fn send_with_retries(
        &self,
        blob: &[u8],
        post_url: &Url,
    ) -> Result<Response, UploadRelayClientError> {
        let mut attempts = 0;
        let mut backoff = self
            .backoff_config
            .get_strategy(ThreadRng::default().next_u64());

        let post_fn = |blob: Vec<u8>| async {
            self.http_client
                .post(post_url.clone())
                .body(blob)
                .send()
                .await
        };

        while let Some(delay) = backoff.next_delay() {
            let response = post_fn(blob.to_vec()).await;

            match response {
                Ok(response) => return Ok(response),
                Err(error) => {
                    tracing::debug!(
                        %error,
                        ?delay,
                        attempts,
                        "upload relay request failed; retrying after a delay"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
            attempts += 1;
        }

        // Try one last time.
        post_fn(blob.to_vec()).await.map_err(|error| {
            tracing::warn!(
                attempts,
                ?error,
                "final attempt to post the blob to the upload relay failed"
            );
            UploadRelayClientError::UploadRequestFailed(error)
        })
    }

    /// Gets the tip configuration from the specified Walrus Upload Relay.
    async fn get_tip_config(&self, server_url: &Url) -> Result<TipConfig, UploadRelayClientError> {
        Self::get_tip_config_with_client(&self.http_client, server_url).await
    }

    /// Gets the tip configuration from the specified Walrus Upload Relay using the provided client.
    async fn get_tip_config_with_client(
        client: &reqwest::Client,
        server_url: &Url,
    ) -> Result<TipConfig, UploadRelayClientError> {
        client
            .get(
                server_url
                    .join(TIP_CONFIG_ROUTE)
                    .map_err(UploadRelayClientError::UrlEndocodingFailed)?,
            )
            .send()
            .await
            .map_err(UploadRelayClientError::TipConfigFetchFailed)?
            .json()
            .await
            .map_err(UploadRelayClientError::TipConfigFetchFailed)
    }
}
