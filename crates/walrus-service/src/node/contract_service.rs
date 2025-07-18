// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Service to handle write interactions with the system contract for storage nodes.
//! Currently, this is only used for submitting inconsistency proofs to the contract.

use std::{
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::Context as _;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use prometheus::core::{AtomicU64, GenericGaugeVec};
use rand::{Rng, SeedableRng, rngs::StdRng};
use sui_types::base_types::ObjectID;
use tokio::{sync::Mutex as TokioMutex, task::JoinSet, time::MissedTickBehavior};
use tracing::Instrument as _;
use walrus_core::{Epoch, PublicKey, messages::InvalidBlobCertificate};
use walrus_sui::{
    client::{
        BlobObjectMetadata,
        CoinType,
        FixedSystemParameters,
        ReadClient,
        SuiClientError,
        SuiClientMetricSet,
        SuiContractClient,
        SuiReadClient,
    },
    types::{
        StorageNodeCap,
        UpdatePublicKeyParams,
        move_errors::{MoveExecutionError, StakingInnerError},
        move_structs::{EpochState, EventBlob},
    },
};
use walrus_utils::{
    backoff::{self, ExponentialBackoff},
    metrics::Registry,
};

use super::{
    committee::CommitteeService,
    config::{CommissionRateData, StorageNodeConfig, SyncedNodeConfigSet, defaults},
    errors::SyncNodeConfigError,
};
use crate::common::config::SuiConfig;

const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(3600);
type UIntGaugeVec = GenericGaugeVec<AtomicU64>;

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

    /// Initiates subsidy distribution.
    async fn process_subsidies(&self) -> Result<(), anyhow::Error>;

    /// Returns the time at which process_subsidies was last called on the walrus subsidies object.
    async fn last_walrus_subsidies_call(&self) -> Result<DateTime<Utc>, SuiClientError>;

    /// Certify an event blob to the contract.
    ///
    /// If `node_capability` is provided, it will be used to set the node capability object ID.
    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SuiClientError>;

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

    /// Returns the system object version.
    async fn get_system_object_version(&self) -> Result<u64, SuiClientError>;

    /// Checks if subsidies are configured in the contract.
    fn is_subsidies_object_configured(&self) -> bool;

    /// Returns the last certified event blob.
    async fn last_certified_event_blob(&self) -> Result<Option<EventBlob>, SuiClientError>;
}

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus"]
    struct ContractServiceMetrics {
        #[help = "The observed balance (in MIST) of a SUI address"]
        sui_balance_mist: UIntGaugeVec["sui_address"],
    }
}

/// Builder for creating a new [`SuiSystemContractService`].
#[derive(Debug, Clone)]
pub struct SuiSystemContractServiceBuilder {
    seed: u64,
    balance_check_frequency: Duration,
    balance_check_warning_threshold: u64,
    metrics_registry: Option<Registry>,
}

impl Default for SuiSystemContractServiceBuilder {
    fn default() -> Self {
        Self {
            seed: rand::thread_rng().r#gen(),
            balance_check_frequency: defaults::BALANCE_CHECK_FREQUENCY,
            balance_check_warning_threshold: defaults::BALANCE_CHECK_WARNING_THRESHOLD_MIST,
            metrics_registry: None,
        }
    }
}

impl SuiSystemContractServiceBuilder {
    /// Set the random seed with which to seed the PRNG used within the service.
    pub fn random_seed(&mut self, seed: u64) -> &mut Self {
        self.seed = seed;
        self
    }

    /// Perform a SUI balance check with the specified frequency.
    ///
    /// Logs the available balance as an info or warning log message based on the threshold
    /// specified with [`Self::balance_check_warning_threshold`].
    ///
    /// If a metrics registry was provided then the balance is also exported as a metric.
    ///
    /// The default is [`defaults::BALANCE_CHECK_FREQUENCY`].
    pub fn balance_check_frequency(&mut self, frequency: Duration) -> &mut Self {
        self.balance_check_frequency = frequency;
        self
    }

    /// Logs a warning if the SUI balance is below the provided MIST threshold.
    ///
    /// The default is [`defaults::BALANCE_CHECK_WARNING_THRESHOLD_MIST`].
    pub fn balance_check_warning_threshold(&mut self, mist_threshold: u64) -> &mut Self {
        self.balance_check_warning_threshold = mist_threshold;
        self
    }

    /// Sets the prometheus [`Registry`] on which to record metrics.
    ///
    /// Defaults to the global prometheus registry.
    pub fn metrics_registry(&mut self, registry: Registry) -> &mut Self {
        self.metrics_registry = Some(registry);
        self
    }

    /// Creates a new [`SuiSystemContractService`] with a [`SuiContractClient`] constructed from
    /// the config.
    pub async fn build_from_config(
        &mut self,
        config: &SuiConfig,
        committee_service: Arc<dyn CommitteeService>,
    ) -> Result<SuiSystemContractService, anyhow::Error> {
        Ok(self.build(
            config
                .new_contract_client(
                    self.metrics_registry
                        .as_ref()
                        .map(|r| Arc::new(SuiClientMetricSet::new(r))),
                )
                .await?,
            committee_service,
        ))
    }

    /// Creates a new [`SuiSystemContractService`] with the provided [`SuiContractClient`].
    pub fn build(
        &mut self,
        contract_client: SuiContractClient,
        committee_service: Arc<dyn CommitteeService>,
    ) -> SuiSystemContractService {
        let mut service = SuiSystemContractService {
            read_client: contract_client.read_client.clone(),
            contract_tx_client: Arc::new(TokioMutex::new(contract_client)),
            committee_service,
            rng: Arc::new(StdMutex::new(StdRng::seed_from_u64(self.seed))),
            background_tasks: Default::default(),
        };

        service.start_balance_monitor(
            self.balance_check_frequency,
            self.balance_check_warning_threshold,
            &self.metrics_registry.clone().unwrap_or_default(),
        );

        service
    }
}

/// A [`SystemContractService`] that uses a [`SuiContractClient`] for chain interactions.
#[derive(Debug, Clone)]
pub struct SuiSystemContractService {
    // A client for sending transactions to the contract.
    contract_tx_client: Arc<TokioMutex<SuiContractClient>>,
    // A client for reading from the contract. Note that reading from the contract can be done
    // concurrently.
    read_client: Arc<SuiReadClient>,
    committee_service: Arc<dyn CommitteeService>,
    rng: Arc<StdMutex<StdRng>>,

    /// Spawned background tasks that are automatically aborted when all instances
    /// of the service is dropped.
    background_tasks: Arc<JoinSet<()>>,
}

impl SuiSystemContractService {
    /// Returns a new [`SuiSystemContractServiceBuilder`] for constructing an instance of
    /// `SuiSystemContractService`.
    pub fn builder() -> SuiSystemContractServiceBuilder {
        Default::default()
    }

    /// Fetches the synced node config set from the contract.
    pub async fn get_synced_node_config_set(
        &self,
        node_capability_object_id: ObjectID,
    ) -> Result<SyncedNodeConfigSet, anyhow::Error> {
        let node_capability = self
            .get_node_capability_object(Some(node_capability_object_id))
            .await?;
        let pool = self
            .read_client
            .get_staking_pool(node_capability.node_id)
            .await?;

        let node_info = pool.node_info.clone();
        let metadata = self
            .read_client
            .get_node_metadata(node_info.metadata)
            .await?;

        Ok(SyncedNodeConfigSet {
            name: node_info.name,
            network_address: node_info.network_address,
            network_public_key: node_info.network_public_key,
            public_key: node_info.public_key,
            next_public_key: node_info.next_epoch_public_key,
            voting_params: pool.voting_params,
            metadata,
            commission_rate_data: CommissionRateData {
                pending_commission_rate: pool.pending_commission_rate,
                commission_rate: pool.commission_rate,
            },
        })
    }

    /// Starts a background task to monitor SUI balance.
    ///
    /// # Panics
    ///
    /// Called during construction of the service, and panics if called after the service
    /// has been cloned.
    fn start_balance_monitor(
        &mut self,
        frequency: Duration,
        warning_threshold_mist: u64,
        metrics_registry: &Registry,
    ) {
        let metrics = ContractServiceMetrics::new(metrics_registry);

        let background_tasks = Arc::get_mut(&mut self.background_tasks)
            .expect("called from constructor so no clones yet exist");

        background_tasks.spawn(monitor_sui_balance(
            frequency,
            warning_threshold_mist,
            metrics,
            self.contract_tx_client.clone(),
        ));
    }
}

async fn monitor_sui_balance(
    frequency: Duration,
    warning_threshold_mist: u64,
    metrics: ContractServiceMetrics,
    contract_client: Arc<TokioMutex<SuiContractClient>>,
) {
    tracing::info!(?frequency, %warning_threshold_mist, "starting monitor for SUI balance");
    let mut interval = tokio::time::interval(frequency);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let _now = interval.tick().await;
        let mut client = contract_client.lock().await;

        let address = client.wallet_mut().active_address().ok();
        let address = address
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let span = tracing::info_span!("check_sui_balance", sui.address = %address);

        async {
            tracing::trace!("querying wallet SUI balance");

            let balance_mist = match client.balance(CoinType::Sui).await {
                Ok(balance_mist) => balance_mist,
                Err(error) => {
                    tracing::warn!(?error, "failed to get SUI balance, skipping interval");
                    return; // Exit the async closure, does not exit the outer function.
                }
            };
            tracing::info!(sui.balance_mist = %balance_mist, "retrieved sui balance");

            walrus_utils::with_label!(metrics.sui_balance_mist, address).set(balance_mist);

            if balance_mist <= warning_threshold_mist {
                tracing::warn!(
                    sui.balance_mist = %balance_mist,
                    %warning_threshold_mist,
                    "SUI balance is below warning threshold, please add SUI to your account"
                );
            }
        }
        .instrument(span)
        .await
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
        let synced_config = self
            .get_synced_node_config_set(node_capability_object_id)
            .await?;

        tracing::debug!("on-chain synced config: {:?}", synced_config);

        let mut update_params = config.generate_update_params(&synced_config);
        let action = calculate_protocol_key_action(
            config.protocol_key_pair().public().clone(),
            config
                .next_protocol_key_pair()
                .map(|kp| kp.public().clone()),
            synced_config.public_key.clone(),
            synced_config.next_public_key.clone(),
        )?;
        let contract_client: tokio::sync::MutexGuard<'_, SuiContractClient> =
            self.contract_tx_client.lock().await;
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
                            config.next_protocol_key_pair().expect(
                                "next key pair must be set when updating remote next public key",
                            ),
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
            tracing::debug!(
                node_name = config.name,
                "node parameters are in sync with on-chain values"
            );
        }

        Ok(())
    }

    fn current_epoch(&self) -> Epoch {
        self.committee_service.active_committees().epoch()
    }

    async fn end_voting(&self) -> Result<(), anyhow::Error> {
        self.contract_tx_client
            .lock()
            .await
            .voting_end()
            .await
            .context("failed to end voting for the next epoch")
    }

    async fn invalidate_blob_id(&self, certificate: &InvalidBlobCertificate) {
        let backoff = ExponentialBackoff::new_with_seed(
            MIN_BACKOFF,
            MAX_BACKOFF,
            None,
            self.rng
                .lock()
                .expect("mutex should not be poisoned")
                .r#gen(),
        );
        backoff::retry(backoff, || async {
            self.contract_tx_client
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
            self.rng
                .lock()
                .expect("mutex should not be poisoned")
                .r#gen(),
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
                .contract_tx_client
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
                Err(SuiClientError::TransactionExecutionError(
                    MoveExecutionError::StakingInner(StakingInnerError::EInvalidSyncEpoch(error)),
                )) => match self.read_client.current_epoch().await {
                    Ok(latest_epoch_on_chain) => {
                        if latest_epoch_on_chain > epoch {
                            tracing::info!(
                                %error,
                                "walrus epoch has advanced, skipping epoch sync done"
                            );
                            Some(())
                        } else if latest_epoch_on_chain < epoch {
                            panic!("walrus epoch onchain cannot be less than event epoch");
                        } else {
                            None
                        }
                    }
                    Err(error) => {
                        tracing::info!(?error, "reading onchain epoch failed");
                        None
                    }
                },
                Err(error) => {
                    tracing::warn!(?error, "submitting epoch sync done to contract failed");
                    None
                }
            }
        })
        .await;
    }

    async fn initiate_epoch_change(&self) -> Result<(), anyhow::Error> {
        self.contract_tx_client
            .lock()
            .await
            .initiate_epoch_change()
            .await?;
        Ok(())
    }

    async fn last_walrus_subsidies_call(&self) -> Result<DateTime<Utc>, SuiClientError> {
        self.read_client.last_walrus_subsidies_call().await
    }

    async fn process_subsidies(&self) -> Result<(), anyhow::Error> {
        self.contract_tx_client
            .lock()
            .await
            .process_subsidies()
            .await?;
        Ok(())
    }

    async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> Result<(), SuiClientError> {
        let blob_metadata = blob_metadata.clone();
        self.contract_tx_client
            .lock()
            .await
            .certify_event_blob(
                blob_metadata,
                ending_checkpoint_seq_num,
                epoch,
                node_capability_object_id,
            )
            .await
    }

    async fn refresh_contract_package(&self) -> Result<(), anyhow::Error> {
        self.contract_tx_client
            .lock()
            .await
            .refresh_package_id()
            .await?;
        Ok(())
    }

    // Below are the methods that only read state from Sui, which do not require a lock on the
    // contract client.

    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        let committees = self.read_client.get_committees_and_state().await?;
        Ok((committees.current.epoch, committees.epoch_state))
    }

    async fn fixed_system_parameters(&self) -> Result<FixedSystemParameters, anyhow::Error> {
        self.read_client
            .fixed_system_parameters()
            .await
            .context("failed to retrieve system parameters")
    }

    async fn get_node_capability_object(
        &self,
        node_capability_object_id: Option<ObjectID>,
    ) -> Result<StorageNodeCap, SuiClientError> {
        let node_capability = if let Some(node_cap) = node_capability_object_id {
            self.read_client
                .sui_client()
                .get_sui_object(node_cap)
                .await?
        } else {
            let address = self.contract_tx_client.lock().await.address();
            self.read_client
                .get_address_capability_object(address)
                .await?
                .ok_or(SuiClientError::StorageNodeCapabilityObjectNotSet)?
        };

        Ok(node_capability)
    }

    async fn get_system_object_version(&self) -> Result<u64, SuiClientError> {
        self.read_client.system_object_version().await
    }

    fn is_subsidies_object_configured(&self) -> bool {
        self.read_client.get_walrus_subsidies_object_id().is_some()
    }

    async fn last_certified_event_blob(&self) -> Result<Option<EventBlob>, SuiClientError> {
        self.read_client.last_certified_event_blob().await
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
            local public key: {local_public_key}, remote public key: {remote_public_key}"
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
