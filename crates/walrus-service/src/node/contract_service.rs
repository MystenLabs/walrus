// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Service to handle write interactions with the system contract for storage nodes.
//! Currently, this is only used for submitting inconsistency proofs to the contract.

use std::{
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::Context as _;
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sui_sdk::wallet_context::WalletContext;
use tokio::sync::Mutex as TokioMutex;
use walrus_core::{messages::InvalidBlobCertificate, Epoch};
use walrus_sui::{
    client::{ContractClient, FixedSystemParameters, SuiClientError, SuiContractClient},
    types::move_structs::EpochState,
};

use super::config::SuiConfig;
use crate::common::utils::{self, ExponentialBackoff};

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// A service for interacting with the system contract.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SystemContractService: std::fmt::Debug + Sync + Send {
    /// Returns the current epoch and the state that the committee's state.
    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error>;

    /// Returns the non-variable system parameters.
    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error>;

    /// Submits a certificate that a blob is invalid to the contract.
    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate);

    /// Submits a notification to the contract that this storage node epoch sync is done.
    async fn epoch_sync_done(&self, epoch: Epoch);

    /// Ends voting for the parameters of the next epoch.
    async fn end_voting(&self) -> Result<(), anyhow::Error>;

    /// Initiates epoch change.
    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error>;
}

/// A [`SystemContractService`] that uses a [`ContractClient`] for chain interactions.
#[derive(Debug)]
pub struct SuiSystemContractService<T> {
    contract_client: Arc<TokioMutex<T>>,
    rng: Arc<StdMutex<StdRng>>,
}

impl<T> Clone for SuiSystemContractService<T> {
    fn clone(&self) -> Self {
        Self {
            contract_client: self.contract_client.clone(),
            rng: self.rng.clone(),
        }
    }
}

impl<T> SuiSystemContractService<T>
where
    T: ContractClient,
{
    /// Creates a new service with the supplied [`ContractClient`].
    pub fn new(contract_client: T) -> Self {
        Self::new_with_seed(contract_client, rand::thread_rng().gen())
    }

    fn new_with_seed(contract_client: T, seed: u64) -> Self {
        Self {
            contract_client: Arc::new(TokioMutex::new(contract_client)),
            rng: Arc::new(StdMutex::new(StdRng::seed_from_u64(seed))),
        }
    }
}

impl SuiSystemContractService<SuiContractClient> {
    /// Creates a new provider with a [`SuiContractClient`] constructed from the config.
    pub async fn from_config(config: &SuiConfig) -> Result<Self, anyhow::Error> {
        let wallet = WalletContext::new(&config.wallet_config, None, None)?;
        let contract_client = SuiContractClient::new(
            wallet,
            config.system_object,
            config.staking_object,
            config.gas_budget,
        )
        .await?;
        Ok(Self::new(contract_client))
    }
}

#[async_trait]
impl<T> SystemContractService for SuiSystemContractService<T>
where
    T: ContractClient + std::fmt::Debug + Sync + Send,
{
    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error> {
        let contract_client = self.contract_client.lock().await;
        contract_client
            .fixed_system_parameters()
            .await
            .context("failed to retrieve system parameters")
    }

    async fn end_voting(&self) -> Result<(), anyhow::Error> {
        let contract_client = self.contract_client.lock().await;
        contract_client
            .voting_end()
            .await
            .context("failed to end voting for the next epoch")
    }

    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate) {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );
        utils::retry(backoff, || async {
            self.contract_client
                .lock()
                .await
                .invalidate_blob_id(certificate)
                .await
                .inspect_err(|error| {
                    tracing::error!(
                        ?error,
                        "submitting invalidity certificate to contract failed"
                    )
                })
                .ok()
        })
        .await;
    }

    async fn epoch_sync_done(&self, epoch: Epoch) {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );

        utils::retry(backoff, || async {
            match self
                .contract_client
                .lock()
                .await
                .epoch_sync_done(epoch)
                .await
            {
                Ok(()) => Some(()),
                Err(SuiClientError::LatestAttestedIsMoreRecent) => {
                    tracing::debug!(walrus.epoch = epoch, "repeatedly submitted epoch_sync_done");
                    Some(())
                }
                Err(error) => {
                    tracing::warn!(?error, "submitting epoch sync done to contract failed");
                    None
                }
            }
        })
        .await;
    }

    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        let client = self.contract_client.lock().await;
        let committees = client.get_committees_and_state().await?;
        Ok((committees.current.epoch, committees.epoch_state))
    }

    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error> {
        let client = self.contract_client.lock().await;
        client.initiate_epoch_change().await?;
        Ok(())
    }
}
