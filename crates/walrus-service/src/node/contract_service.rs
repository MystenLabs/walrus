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
use walrus_core::{messages::InvalidBlobCertificate, Epoch, PublicKey, QuiltTaskId};
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
        UpdatePublicKeyParams,
    },
};
use walrus_utils::backoff::{self, ExponentialBackoff};

use super::{committee::CommitteeService, config::StorageNodeConfig, errors::SyncNodeConfigError};
use crate::common::config::SuiConfig;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);

enum ProtocolKeyAction {
    UpdateRemoteNextPublicKey(PublicKey),
    RotateLocalKeyPair,
    DoNothing,
}

/// A service for interacting with the system contract.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SystemContractService: std::fmt::Debug + Sync + Send {
    /// Syncs the node parameters with the on-chain values.
    async fn sync_node_params(
        &self,
        config: &StorageNodeConfig,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SyncNodeConfigError>;

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
    ) -> Result<StorageNodeCap, SuiClientError>;

    async fn add_quilt_task(&self, node_capability_object_id: ObjectID, task_id: ObjectID) -> Result<(), anyhow::Error>;
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
    /// Syncs the node parameters with the on-chain values.
    /// If the node parameters are not in sync, it updates the node parameters on-chain.
    /// Note this could return error if the node needs reboot, e.g., when protocol key pair
    /// rotation is required.
    async fn sync_node_params(
        &self,
        config: &StorageNodeConfig,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SyncNodeConfigError> {
        let node_capability = self
            .get_node_capability_object(Some(node_capability_object_id))
            .await?;
        let contract_client: tokio::sync::MutexGuard<'_, SuiContractClient> =
            self.contract_client.lock().await;
        let pool = contract_client
            .read_client
            .get_staking_pool(node_capability.node_id)
            .await?;

        let node_info = &pool.node_info;

        let mut update_params = config.generate_update_params(
            node_info.name.as_str(),
            node_info.network_address.0.as_str(),
            &node_info.network_public_key,
            &pool.voting_params,
        );
        let action = calculate_protocol_key_action(
            config.protocol_key_pair().public().clone(),
            config
                .next_protocol_key_pair()
                .map(|kp| kp.public().clone()),
            node_info.public_key.clone(),
            node_info.next_epoch_public_key.clone(),
        )?;
        match action {
            ProtocolKeyAction::UpdateRemoteNextPublicKey(next_public_key) => {
                tracing::info!(
                    "going to update remote next public key to {:?}",
                    next_public_key
                );

                update_params.update_public_key = Some(UpdatePublicKeyParams {
                    next_public_key,
                    proof_of_possession:
                        walrus_sui::utils::generate_proof_of_possession_for_address(
                            config.next_protocol_key_pair().unwrap(),
                            contract_client.address(),
                            contract_client.read_client.current_epoch().await?,
                        ),
                });
            }
            ProtocolKeyAction::RotateLocalKeyPair => {
                tracing::info!("going to rotate local key pair");
                return Err(SyncNodeConfigError::ProtocolKeyPairRotationRequired);
            }
            ProtocolKeyAction::DoNothing => {}
        }

        if update_params.needs_update() {
            tracing::info!(
                node_name = config.name,
                node_id = ?node_info.node_id,
                update_params = ?update_params,
                "update node params"
            );
            contract_client
                .update_node_params(update_params.clone(), node_capability_object_id)
                .await?;
            if update_params.needs_reboot() {
                tracing::info!("node needs reboot");
                return Err(SyncNodeConfigError::NodeNeedsReboot);
            }
        } else {
            tracing::info!(
                node_name = config.name,
                node_id = ?node_info.node_id,
                "node parameters are in sync with on-chain values"
            );
        }

        Ok(())
    }

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
    ) -> Result<StorageNodeCap, SuiClientError> {
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

    async fn add_quilt_task(&self, node_capability_object_id: ObjectID, task_id: ObjectID) -> Result<(), anyhow::Error> {
        let client = self.contract_client.lock().await;
        client.add_quilt_task(node_capability_object_id, task_id).await?;
        Ok(())
    }
}

/// Calculates the protocol key action based on the local and remote public keys.
#[tracing::instrument]
fn calculate_protocol_key_action(
    local_public_key: PublicKey,
    local_next_public_key: Option<PublicKey>,
    remote_public_key: PublicKey,
    remote_next_public_key: Option<PublicKey>,
) -> Result<ProtocolKeyAction, SyncNodeConfigError> {
    // Case 1: Local public key matches remote public key
    if local_public_key == remote_public_key {
        match (local_next_public_key, remote_next_public_key) {
            // If local has no next key and remote's next key matches local's current key,
            // do nothing
            (None, Some(remote_next)) if remote_next == local_public_key => {
                Ok(ProtocolKeyAction::DoNothing)
            }

            // If local has no next key but remote does, update remote's next key to
            // local's current key
            (None, Some(_)) => Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(
                local_public_key,
            )),

            // If local has next key and it differs from remote's next key (or remote has none),
            // update remote's next key
            (Some(local_next), remote_next) if Some(local_next.clone()) != remote_next => {
                Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(local_next))
            }

            // Keys match or both have no next key - do nothing
            _ => Ok(ProtocolKeyAction::DoNothing),
        }
    }
    // Case 2: Local public key doesn't match remote public key
    else {
        let error_msg = format!(
            "Local protocol key pair does not match remote protocol key pair, \
            please update the protocol key pair to match the remote protocol public key: \
            local public key: {}, remote public key: {}",
            local_public_key, remote_public_key
        );

        let Some(local_next) = &local_next_public_key else {
            return Err(SyncNodeConfigError::NodeConfigInconsistent(error_msg));
        };

        // Check if local next key matches remote current key, this indicates that
        // the on-chain protocol public key is updated, so we need to rotate the local key pair.
        if local_next == &remote_public_key {
            // Warn if remote has a next key set
            if remote_next_public_key.is_some() {
                tracing::warn!("remote node has next public key set while local node is rotating");
            }
            return Ok(ProtocolKeyAction::RotateLocalKeyPair);
        } else {
            // Update remote's next key only if it differs from local next or is not set
            tracing::error!(
                "local and remote protocol key pairs do not match, updating remote's next \
                public key, it will take effect in the next epoch, consider restoring the \
                local protocol key pair to match the remote protocol public key"
            );
            if local_next_public_key != remote_next_public_key {
                return Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(
                    local_next.clone(),
                ));
            }
        }

        tracing::error!(error = error_msg.as_str(), "protocol key mismatch");

        Err(SyncNodeConfigError::NodeConfigInconsistent(error_msg))
    }
}

#[cfg(test)]
mod tests {
    use walrus_core::keys::ProtocolKeyPair;

    use super::*;
    #[test]
    fn test_handle_protocol_key_pair_update() {
        let key1 = ProtocolKeyPair::generate().public().clone();
        let key2 = ProtocolKeyPair::generate().public().clone();
        let key3 = ProtocolKeyPair::generate().public().clone();

        // Case 1: Local public key matches remote public key
        {
            // Case 1a: Next public keys don't match - should update remote
            let result = calculate_protocol_key_action(
                key1.clone(),
                Some(key2.clone()),
                key1.clone(),
                Some(key3.clone()),
            );
            assert!(matches!(
                result,
                Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(k)) if k == key2
            ));

            // Case 1b: Local has next key, remote doesn't - should update remote
            let result =
                calculate_protocol_key_action(key1.clone(), Some(key2.clone()), key1.clone(), None);
            assert!(matches!(
                result,
                Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(k)) if k == key2
            ));

            // Case 1c: Local doesn't have next key, remote does - should update remote
            let result =
                calculate_protocol_key_action(key1.clone(), None, key1.clone(), Some(key2.clone()));
            assert!(matches!(
                result,
                Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(k)) if k == key1
            ));

            // Case 1d: Next public keys match - should do nothing
            let result = calculate_protocol_key_action(
                key1.clone(),
                Some(key2.clone()),
                key1.clone(),
                Some(key2.clone()),
            );
            assert!(matches!(result, Ok(ProtocolKeyAction::DoNothing)));

            // Case 1e: Neither has next key - should do nothing
            let result = calculate_protocol_key_action(key1.clone(), None, key1.clone(), None);
            assert!(matches!(result, Ok(ProtocolKeyAction::DoNothing)));

            // Case 1f: Local has no next key and remote's next key matches local's current key
            // - should do nothing
            let result =
                calculate_protocol_key_action(key1.clone(), None, key1.clone(), Some(key1.clone()));
            assert!(matches!(result, Ok(ProtocolKeyAction::DoNothing)));
        }

        // Case 2: Local public key doesn't match remote public key
        {
            // Case 2a: Local next key matches remote current key - should rotate local
            let result =
                calculate_protocol_key_action(key1.clone(), Some(key2.clone()), key2.clone(), None);
            assert!(matches!(result, Ok(ProtocolKeyAction::RotateLocalKeyPair)));

            // Case 2b: Local next key matches remote current key, but remote has next key set
            // Should rotate local but emit warning (warning check would require log capture)
            let result = calculate_protocol_key_action(
                key1.clone(),
                Some(key2.clone()),
                key2.clone(),
                Some(key3.clone()),
            );
            assert!(matches!(result, Ok(ProtocolKeyAction::RotateLocalKeyPair)));

            // Case 2c: Keys don't match and local next key doesn't match remote current key
            // Should return error
            let result =
                calculate_protocol_key_action(key1.clone(), Some(key3.clone()), key2.clone(), None);
            assert!(
                matches!(result, Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(k)) if k == key3)
            );

            // Case 2d: Keys don't match and local has no next key
            // Should return error
            let result = calculate_protocol_key_action(key1.clone(), None, key2.clone(), None);
            assert!(result.is_err());

            // Case 2e: Keys don't match and remote's next key differs from local's next key
            // Should update remote's next key
            let result = calculate_protocol_key_action(
                key1.clone(),
                Some(key3.clone()),
                key2.clone(),
                Some(key1.clone()),
            );
            assert!(matches!(
                result,
                Ok(ProtocolKeyAction::UpdateRemoteNextPublicKey(k)) if k == key3
            ));
        }
    }
}
