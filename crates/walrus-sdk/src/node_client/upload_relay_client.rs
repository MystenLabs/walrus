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
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use rand::{Rng, RngCore, rngs::ThreadRng};
use reqwest::{Response, Url};
use serde::{Serialize, de::DeserializeOwned};
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
    config::{ClientConfig as WalrusConfig, load_configuration},
    core::{
        BlobId,
        EncodingType,
        EpochCount,
        encoding::{DataTooLargeError, EncodingConfig, EncodingFactory, Primary},
        messages::ConfirmationCertificate,
        metadata::{BlobMetadataApi, VerifiedBlobMetadataWithId},
    },
    node_client::WalrusNodeClient,
    sui::{
        client::{BlobPersistence, SuiContractClient, transaction_builder::WalrusPtbBuilder},
        config::WalletConfig,
        types::{BlobEvent, BlobRegistered},
        wallet::Wallet,
    },
    upload_relay::{
        CreateSliverUploadSessionResponse,
        ResponseType,
        SliverUploadSessionStatus,
        TIP_CONFIG_ROUTE,
        blob_upload_relay_url,
        params::{AuthPackage, NONCE_LEN, Params},
        sliver_upload_relay_complete_url,
        sliver_upload_relay_primary_url,
        sliver_upload_relay_session_url,
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

    /// The blob could not be encoded for the sliver upload relay.
    #[error("failed to encode blob for upload relay: {0}")]
    EncodingFailed(#[from] DataTooLargeError),

    /// The upload relay request body could not be serialized.
    #[error("failed to serialize upload relay request: {0}")]
    RequestSerializationFailed(#[from] bcs::Error),

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
    /// The upload relay endpoint to use for blob data.
    upload_relay_endpoint: UploadRelayEndpoint,
    /// The tip configuration.
    tip_config: TipConfig,
    /// The gas budget for the tip payment.
    gas_budget: Option<u64>,
    /// The HTTP client for making requests to the upload relay.
    http_client: reqwest::Client,
    /// The backoff configuration for retries.
    backoff_config: ExponentialBackoffConfig,
}

/// The upload relay endpoint used to send blob data.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum UploadRelayEndpoint {
    /// Send the full raw blob to the legacy blob upload relay endpoint.
    Blob,
    /// Send systematic primary slivers through the session-based sliver upload relay endpoint.
    #[default]
    Sliver,
}

impl PartialEq for UploadRelayClient {
    fn eq(&self, other: &Self) -> bool {
        self.user_address == other.user_address
            && self.n_shards == other.n_shards
            && self.upload_relay == other.upload_relay
            && self.upload_relay_endpoint == other.upload_relay_endpoint
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
        Self::new_with_endpoint(
            user_address,
            n_shards,
            upload_relay,
            UploadRelayEndpoint::default(),
            gas_budget,
            backoff_config,
        )
        .await
    }

    /// Fetches the tip configuration from the upload relay and creates a new upload relay tip
    /// client using the specified blob-data endpoint.
    pub async fn new_with_endpoint(
        user_address: SuiAddress,
        n_shards: NonZeroU16,
        upload_relay: Url,
        upload_relay_endpoint: UploadRelayEndpoint,
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
            upload_relay_endpoint,
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
        let transaction_data = build_transaction_data_with_min_gas_balance(
            pt_builder.finish(),
            sui_client.read_client(),
            self.user_address,
            self.gas_budget,
            0, // No additional gas budget.
            tip_amount,
            None,
        )
        .await?;

        let response = sui_client
            .sign_and_send_transaction(transaction_data, "pay_tip")
            .await?;

        Ok(response.digest)
    }

    /// Encodes the blob, sends its systematic primary slivers to the relay, and waits for the
    /// certificate.
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

        let response = match self.upload_relay_endpoint {
            UploadRelayEndpoint::Blob => self.send_to_relay(blob, &params).await?,
            UploadRelayEndpoint::Sliver => {
                self.send_slivers_to_relay(blob, blob_id, encoding_type, &params)
                    .await?
            }
        };
        if response.blob_id != blob_id {
            return Err(UploadRelayClientError::BlobIdMismatch {
                expected: blob_id,
                actual: response.blob_id,
            });
        }

        Ok(response.confirmation_certificate)
    }

    async fn send_slivers_to_relay(
        &self,
        blob: &[u8],
        blob_id: BlobId,
        encoding_type: EncodingType,
        params: &Params,
    ) -> Result<ResponseType, UploadRelayClientError> {
        let encoding_config = EncodingConfig::new(self.n_shards).get_for_type(encoding_type);
        let n_systematic_primary = usize::from(encoding_config.n_source_symbols::<Primary>().get());
        let (sliver_pairs, metadata) = encoding_config.encode_with_metadata(blob.to_vec())?;
        if *metadata.blob_id() != blob_id {
            return Err(UploadRelayClientError::BlobIdMismatch {
                expected: blob_id,
                actual: *metadata.blob_id(),
            });
        }

        let upload_session_id = self.create_sliver_upload_session(&metadata, params).await?;
        self.upload_systematic_primary_slivers(
            &upload_session_id,
            &sliver_pairs,
            n_systematic_primary,
        )
        .await?;

        self.complete_sliver_upload_session(&upload_session_id)
            .await
    }

    async fn create_sliver_upload_session(
        &self,
        metadata: &VerifiedBlobMetadataWithId,
        params: &Params,
    ) -> Result<String, UploadRelayClientError> {
        let post_url = sliver_upload_relay_session_url(&self.upload_relay, params)?;
        let body = Bytes::from(bcs::to_bytes(&metadata.clone().into_unverified())?);

        tracing::debug!(?post_url, ?params, "creating sliver upload relay session");

        let response: CreateSliverUploadSessionResponse = self
            .send_json_request(
                self.http_client
                    .post(post_url)
                    .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                    .body(body),
            )
            .await?;

        Ok(response.upload_session_id)
    }

    async fn upload_systematic_primary_slivers(
        &self,
        upload_session_id: &str,
        sliver_pairs: &[walrus_core::encoding::SliverPair],
        n_systematic_primary: usize,
    ) -> Result<(), UploadRelayClientError> {
        let primary_sliver_uploads = sliver_pairs
            .iter()
            .take(n_systematic_primary)
            .map(|pair| {
                bcs::to_bytes(&pair.primary)
                    .map(|body| (pair.primary.index.0, Bytes::from(body)))
                    .map_err(UploadRelayClientError::RequestSerializationFailed)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let concurrency = 50;

        stream::iter(primary_sliver_uploads)
            .map(|(sliver_index, body)| async move {
                self.put_sliver_upload_session_primary(upload_session_id, sliver_index, body)
                    .await?;
                Ok::<(), UploadRelayClientError>(())
            })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    async fn put_sliver_upload_session_primary(
        &self,
        upload_session_id: &str,
        sliver_index: u16,
        body: Bytes,
    ) -> Result<SliverUploadSessionStatus, UploadRelayClientError> {
        let put_url =
            sliver_upload_relay_primary_url(&self.upload_relay, upload_session_id, sliver_index)?;

        tracing::debug!(
            ?put_url,
            upload_session_id,
            sliver_index,
            "uploading systematic primary sliver to relay session"
        );

        self.send_json_request(
            self.http_client
                .put(put_url)
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .body(body),
        )
        .await
    }

    async fn complete_sliver_upload_session(
        &self,
        upload_session_id: &str,
    ) -> Result<ResponseType, UploadRelayClientError> {
        let post_url = sliver_upload_relay_complete_url(&self.upload_relay, upload_session_id)?;

        tracing::debug!(
            ?post_url,
            upload_session_id,
            "completing sliver upload relay session"
        );

        self.send_json_request(self.http_client.post(post_url))
            .await
    }

    async fn send_json_request<T>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, UploadRelayClientError>
    where
        T: DeserializeOwned,
    {
        request
            .send()
            .await
            .map_err(UploadRelayClientError::UploadRequestFailed)?
            .error_for_status()
            .map_err(UploadRelayClientError::UploadRequestFailed)?
            .json()
            .await
            .map_err(UploadRelayClientError::UploadRequestFailed)
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
        params: &Params,
    ) -> Result<ResponseType, UploadRelayClientError> {
        let post_url = blob_upload_relay_url(&self.upload_relay, params)?;

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
        let payload = Bytes::copy_from_slice(blob);
        let client = self.http_client.clone();
        let post_url = post_url.clone();
        let mut attempts = 0;
        let mut backoff = self
            .backoff_config
            .get_strategy(ThreadRng::default().next_u64());

        let post_fn = |body: Bytes| async { client.post(post_url.clone()).body(body).send().await };

        while let Some(delay) = backoff.next_delay() {
            match post_fn(payload.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    attempts += 1;
                    tracing::debug!(
                        %error,
                        ?delay,
                        attempts,
                        "upload relay request failed; retrying after a delay"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // Try one last time.
        post_fn(payload).await.map_err(|error| {
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
