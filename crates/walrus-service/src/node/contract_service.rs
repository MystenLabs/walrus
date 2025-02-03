// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Service to handle write interactions with the system contract for storage nodes.
//! Currently, this is only used for submitting inconsistency proofs to the contract.

use std::{
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context as _, Error};
use async_trait::async_trait;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sui_types::base_types::ObjectID;
use tokio::sync::Mutex as TokioMutex;
use walrus_core::{messages::InvalidBlobCertificate, Epoch};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        FixedSystemParameters,
        ReadClient as _,
        SuiClientError,
        SuiContractClient,
    },
    types::{
        move_errors::{MoveExecutionError, SystemStateInnerError},
        move_structs::EpochState,
        StorageNodeCap,
    },
};
use walrus_utils::backoff::{self, ExponentialBackoff};

use super::committee::CommitteeService;
use crate::common::config::SuiConfig;

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
    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate);

    /// Submits a notification to the contract that this storage node epoch sync is done.
    ///
    /// If `node_capability` is provided, it will be used to set the node capability object ID.
    async fn epoch_sync_done(&self, epoch: Epoch, node_capability_object_id: ObjectID);

    /// Ends voting for the parameters of the next epoch.
    async fn end_voting(&self) -> Result<(), anyhow::Error>;

    /// Initiates epoch change.
    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error>;

    /// Certify an event blob to the contract.
    ///
    /// If `node_capability` is provided, it will be used to set the node capability object ID.
    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> Result<(), Error>;

    /// Refreshes the contract package that the service is using.
    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error>;

    /// Returns the node capability object.
    ///
    /// If `node_capability_object_id` is provided, it will be used to set the node capability
    /// object ID.
    ///
    /// If `node_capability_object_id` is not provided, it will be retrieved from the contract.
    /// Note that if the wallet address owns multiple node capability objects, an error will be
    /// returned. Storage node must use the `storage_node_cap` configuration in the node config
    /// file to specify which node capability object to use.
    async fn get_node_capability_object(
        &self,
        node_capability_object_id: Option<ObjectID>,
    ) -> Result<StorageNodeCap, anyhow::Error>;
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

    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate) {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );
        backoff::retry(backoff, || async {
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

    async fn epoch_sync_done(&self, epoch: Epoch, node_capability_object_id: ObjectID) {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );

        backoff::retry(backoff, || async {
            let current_epoch = self.current_epoch();
            if epoch < current_epoch {
                tracing::info!(
                    epoch,
                    current_epoch,
                    "stop trying to submit epoch sync done for older epoch"
                );
                return Some(());
            }
            match self
                .contract_client
                .lock()
                .await
                .epoch_sync_done(epoch, node_capability_object_id)
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
        node_capability_object_id: ObjectID,
    ) -> Result<(), Error> {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng.lock().unwrap().gen(),
        );
        backoff::retry(backoff, || {
            let blob_metadata = blob_metadata.clone();
            let blob_id = blob_metadata.blob_id;
            async move {
                match self
                    .contract_client
                    .lock()
                    .await
                    .certify_event_blob(
                        blob_metadata,
                        ending_checkpoint_seq_num,
                        epoch,
                        node_capability_object_id,
                    )
                    .await
                {
                    Ok(()) => Some(()),
                    Err(SuiClientError::StorageNodeCapabilityObjectNotSet) => {
                        tracing::debug!(blob_id = ?blob_id,
                            "Storage node capability object not set");
                        Some(())
                    }
                    Err(
                        e @ SuiClientError::TransactionExecutionError(
                            MoveExecutionError::SystemStateInner(
                                SystemStateInnerError::EInvalidIdEpoch(_),
                            ),
                        ),
                    ) => {
                        tracing::debug!(
                            walrus.epoch = epoch,
                            error = ?e,
                            blob_id = ?blob_id,
                            "Non-retriable event blob certification error while \
                            attesting event blob"
                        );
                        Some(())
                    }
                    Err(
                        e @ SuiClientError::TransactionExecutionError(
                            MoveExecutionError::SystemStateInner(
                                SystemStateInnerError::EIncorrectAttestation(_)
                                | SystemStateInnerError::ERepeatedAttestation(_)
                                | SystemStateInnerError::ENotCommitteeMember(_),
                            ),
                        ),
                    ) => {
                        tracing::warn!(
                            walrus.epoch = epoch,
                            error = ?e,
                            blob_id = ?blob_id,
                            "Unexpected non-retriable event blob certification error \
                            while attesting event blob"
                        );
                        Some(())
                    }
                    Err(SuiClientError::TransactionExecutionError(
                        MoveExecutionError::NotParsable(_),
                    )) => {
                        tracing::error!(blob_id = ?blob_id,
                            "Unexpected unknown transaction execution error while \
                            attesting event blob, retrying");
                        None
                    }
                    Err(SuiClientError::TransactionExecutionError(e)) => {
                        tracing::warn!(error = ?e, blob_id = ?blob_id,
                            "Unexpected move execution error while attesting event blob");
                        Some(())
                    }
                    Err(SuiClientError::SharedObjectCongestion(object_ids)) => {
                        tracing::debug!(blob_id = ?blob_id,
                            object_ids = ?object_ids,
                            "Shared object congestion error while attesting event blob, retrying");
                        None
                    }
                    Err(error) => {
                        tracing::error!(?error, blob_id = ?blob_id,
                            "Unexpected unknown sui client error while attesting event blob, \
                            retrying"
                        );
                        None
                    }
                }
            }
        })
        .await;
        Ok(())
    }

    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error> {
        let client = self.contract_client.lock().await;
        client.refresh_package_id().await?;
        Ok(())
    }

    async fn get_node_capability_object(
        &self,
        node_capability_object_id: Option<ObjectID>,
    ) -> Result<StorageNodeCap, anyhow::Error> {
        let node_capability = if let Some(node_cap) = node_capability_object_id {
            self.contract_client
                .lock()
                .await
                .sui_client()
                .get_sui_object(node_cap)
                .await?
        } else {
            let contract_client = self.contract_client.lock().await;
            let address = contract_client.address();
            contract_client
                .read_client
                .get_address_capability_object(address)
                .await?
                .ok_or(SuiClientError::StorageNodeCapabilityObjectNotSet)?
        };

        Ok(node_capability)
    }
}
