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
use tokio::sync::Mutex as TokioMutex;
use walrus_core::{messages::InvalidBlobCertificate, Epoch};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        FixedSystemParameters,
        ReadClient as _,
        RetriableOperation,
        RetriableOperationResult,
        SuiClientError,
        SuiContractClient,
    },
    types::move_structs::EpochState,
};
use walrus_utils::backoff::ExponentialBackoff;

use super::{committee::CommitteeService, config::SuiConfig};

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// A service for interacting with the system contract.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SystemContractService: std::fmt::Debug + Sync + Send {
    /// Returns the current epoch and the state that the committee's state.
    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error>;

    /// Returns the current epoch.
    fn current_epoch(&self) -> Epoch;

    /// Returns the non-variable system parameters.
    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error>;

    /// Submits a certificate that a blob is invalid to the contract.
    async fn invalidate_blob_id(&self, certificate: Arc<InvalidBlobCertificate>);

    /// Submits a notification to the contract that this storage node epoch sync is done.
    async fn epoch_sync_done(&self, epoch: Epoch);

    /// Ends voting for the parameters of the next epoch.
    async fn end_voting(&self) -> Result<(), anyhow::Error>;

    /// Initiates epoch change.
    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error>;

    /// Certify an event blob to the contract.
    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
    );

    /// Refreshes the contract package that the service is using.
    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error>;
}

/// A [`SystemContractService`] that uses a [`SuiContractClient`] for chain interactions.
#[derive(Debug, Clone)]
pub struct SuiSystemContractService {
    contract_client: Arc<TokioMutex<SuiContractClient>>,
    committee_service: Arc<dyn CommitteeService>,
    rng: Arc<StdMutex<StdRng>>,
}

impl SuiSystemContractService {
    /// Creates a new service with the supplied [`SuiContractClient`].
    pub fn new(
        contract_client: SuiContractClient,
        committee_service: Arc<dyn CommitteeService>,
    ) -> Self {
        Self::new_with_seed(contract_client, committee_service, rand::thread_rng().gen())
    }

    fn new_with_seed(
        contract_client: SuiContractClient,
        committee_service: Arc<dyn CommitteeService>,
        seed: u64,
    ) -> Self {
        Self {
            contract_client: Arc::new(TokioMutex::new(contract_client)),
            committee_service,
            rng: Arc::new(StdMutex::new(StdRng::seed_from_u64(seed))),
        }
    }

    /// Creates a new provider with a [`SuiContractClient`] constructed from the config.
    pub async fn from_config(
        config: &SuiConfig,
        committee_service: Arc<dyn CommitteeService>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self::new(
            config.new_contract_client().await?,
            committee_service,
        ))
    }

    /// Executes a retriable operation with exponential backoff retries.
    /// Retry here means retry the exact same ptb instead of building a new one.
    async fn execute_with_retries<F>(&self, operation: RetriableOperation, should_early_return: F)
    where
        F: Fn(&Result<RetriableOperationResult, SuiClientError>) -> bool,
    {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );
        let mut retry_last_transaction = None;

        for backoff_duration in backoff {
            let result = self
                .contract_client
                .lock()
                .await
                .execute_retriable_contract_operation(
                    operation.clone(),
                    retry_last_transaction.clone(),
                )
                .await;

            if should_early_return(&result) {
                tracing::info!(?result, "early return from execute_with_retries");
                return;
            }

            match result {
                Ok(RetriableOperationResult::Success) => return,
                Ok(RetriableOperationResult::Retry {
                    retry_transaction,
                    error,
                }) => {
                    // Record the Sui transaction to retry with next time.
                    // It's important to retry the same PTB instead of building a new one because
                    // transaction with retryable error may already locked some objects in some
                    // validators. So retry the same PTB to reduce the chance of transaction
                    // equivocation.
                    retry_last_transaction = Some(*retry_transaction);
                    tracing::warn!(?error, "operation failed due to retryable error");
                }
                Err(error) => {
                    tracing::warn!(?error, "operation failed");
                }
            }

            tokio::time::sleep(backoff_duration).await;
        }
    }
}

#[async_trait]
impl SystemContractService for SuiSystemContractService {
    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        let client = self.contract_client.lock().await;
        let committees = client.get_committees_and_state().await?;
        Ok((committees.current.epoch, committees.epoch_state))
    }

    fn current_epoch(&self) -> Epoch {
        self.committee_service.active_committees().epoch()
    }

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

    async fn invalidate_blob_id(&self, certificate: Arc<InvalidBlobCertificate>) {
        self.execute_with_retries(RetriableOperation::InvalidateBlobId(certificate), |_| false)
            .await;
    }

    async fn epoch_sync_done(&self, epoch: Epoch) {
        // Early return if epoch is already outdated
        let current_epoch = self.current_epoch();
        if epoch < current_epoch {
            tracing::info!(
                epoch,
                current_epoch,
                "stop trying to submit epoch sync done for older epoch"
            );
            return;
        }

        self.execute_with_retries(RetriableOperation::EpochSyncDone(epoch), |result| {
            matches!(result, Err(SuiClientError::LatestAttestedIsMoreRecent))
        })
        .await;
    }

    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error> {
        let client = self.contract_client.lock().await;
        client.initiate_epoch_change().await?;
        Ok(())
    }

    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
    ) {
        self.execute_with_retries(
            RetriableOperation::CertifyEventBlob {
                blob_metadata,
                ending_checkpoint_seq_num,
                epoch,
            },
            |result| matches!(result, Err(SuiClientError::TransactionExecutionError(_))),
        )
        .await;
    }

    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error> {
        let client = self.contract_client.lock().await;
        client.refresh_package_id().await?;
        Ok(())
    }
}
