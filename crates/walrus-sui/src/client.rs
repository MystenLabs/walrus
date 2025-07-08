// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use core::fmt;
use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use contract_config::ContractConfig;
use move_package::BuildConfig as MoveBuildConfig;
use retry_client::{RetriableSuiClient, retriable_sui_client::MAX_GAS_PAYMENT_OBJECTS};
use serde::{Deserialize, Serialize};
use sui_package_management::LockCommand;
use sui_sdk::{
    rpc_types::{
        SuiExecutionStatus,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
        get_new_package_obj_from_response,
    },
    types::base_types::ObjectID,
};
use sui_types::{TypeTag, base_types::SuiAddress, event::EventID, transaction::TransactionData};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tracing::Level;
use transaction_builder::{MAX_BURNS_PER_PTB, WalrusPtbBuilder};
use walrus_core::{
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
    ensure,
    merkle::Node as MerkleNode,
    messages::{ConfirmationCertificate, InvalidBlobCertificate, ProofOfPossession},
    metadata::{BlobMetadataApi as _, BlobMetadataWithId},
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    contracts,
    system_setup::compile_package,
    types::{
        BlobEvent,
        Committee,
        ContractEvent,
        NodeRegistrationParams,
        NodeUpdateParams,
        StakedWal,
        StorageNodeCap,
        StorageResource,
        move_errors::{BlobError, MoveExecutionError, StakingError, SubsidiesError, SystemError},
        move_structs::{
            Authorized,
            Blob,
            BlobAttribute,
            BlobWithAttribute,
            EpochState,
            SharedBlob,
            StorageNode,
        },
    },
    utils::get_created_sui_object_ids_by_type,
    wallet::Wallet,
};

mod read_client;
pub use read_client::{
    CoinType,
    CommitteesAndState,
    Credits,
    FixedSystemParameters,
    ReadClient,
    SuiReadClient,
};
pub mod retry_client;
pub mod rpc_config;

pub mod transaction_builder;
use crate::types::move_structs::EventBlob;

pub mod contract_config;

mod metrics;
pub use metrics::SuiClientMetricSet;

// Keep in sync with the corresponding value in
// `contracts/walrus/sources/staking/staked_wal.move`
/// The minimum threshold for staking.
pub const MIN_STAKING_THRESHOLD: u64 = 1_000_000_000; // 1 WAL

#[derive(Debug, thiserror::Error)]
/// Error returned by the [`SuiContractClient`] and the [`SuiReadClient`].
pub enum SuiClientError {
    /// Credits are not enabled for this client.
    #[error("credits are not enabled for this client")]
    CreditsNotEnabled,
    /// Unexpected internal errors.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    /// Error resulting from a Sui-SDK call.
    #[error(transparent)]
    SuiSdkError(#[from] sui_sdk::error::Error),
    /// Other errors resulting from Sui crates.
    // Wrapped in a `Box` to avoid the large memory overhead of this error variant.
    #[error(transparent)]
    SuiError(#[from] Box<sui_types::error::SuiError>),
    /// Error in a transaction execution.
    #[error("transaction execution failed: {0}")]
    TransactionExecutionError(MoveExecutionError),
    /// No matching WAL coin found for the transaction.
    #[error("could not find WAL coins with sufficient balance")]
    NoCompatibleWalCoins,
    /// No matching gas coin found for the transaction.
    #[error("could not find SUI coins with sufficient balance [requested_amount={0:?}]")]
    NoCompatibleGasCoins(Option<u128>),
    /// The Walrus system object does not exist.
    #[error(
        "the specified Walrus system object {0} does not exist or is incompatible with this \
        binary;\n\
        make sure you have the latest binary and configuration, and the correct Sui network is  \
        activated in your Sui wallet"
    )]
    WalrusSystemObjectDoesNotExist(ObjectID),
    /// The specified Walrus package could not be found.
    #[error(
        "the specified Walrus package {0} could not be found;\n\
        make sure you have the latest binary and configuration, and the correct Sui network is \
        activated in your Sui wallet"
    )]
    WalrusPackageNotFound(ObjectID),
    /// The type of the `WAL` coin could not be found.
    #[error("the type of the WAL coin could not be found in the package {0}")]
    WalTypeNotFound(ObjectID),
    /// The specified event ID is not associated with a Walrus event.
    #[error("no corresponding blob event found for {0:?}")]
    NoCorrespondingBlobEvent(EventID),
    /// Storage capability object is missing when interacting with the contract.
    #[error("no storage capability object set")]
    StorageNodeCapabilityObjectNotSet,
    /// An attestation has already been performed for that or a more recent epoch.
    #[error("the storage node has already attested to that or a later epoch being synced")]
    LatestAttestedIsMoreRecent,
    /// The address has multiple storage node capability objects, which is unexpected.
    #[error(
        "there are multiple storage node capability objects in the address, but the config \
        does not specify which one to use"
    )]
    MultipleStorageNodeCapabilities,
    /// The storage capability object already exists in the account and cannot register another.
    #[error(
        "storage capability object already exists in the account and cannot register another \
        object ID: {0}"
    )]
    CapabilityObjectAlreadyExists(ObjectID),
    /// The sender is not authorized to perform the action on the pool.
    #[error("the sender is not authorized to perform the action on the pool with node ID {0}")]
    NotAuthorizedForPool(ObjectID),
    /// Transaction execution was cancelled due to shared object congestion
    #[error("execution cancelled due to shared object congestion on objects {0:?}")]
    SharedObjectCongestion(Vec<ObjectID>),
    /// The attribute does not exist on the blob.
    #[error("the attribute does not exist on the blob")]
    AttributeDoesNotExist,
    /// The attribute already exists on the blob.
    #[error("the attribute already exists on the blob")]
    AttributeAlreadyExists,
    /// The amount of stake is below the threshold for staking.
    #[error(
        "the stake amount {0} FROST is below the minimum threshold of {MIN_STAKING_THRESHOLD} \
        FROST for staking"
    )]
    StakeBelowThreshold(u64),
    /// The required coin balance cannot be achieved with the maximum number of coins allowed.
    #[error(
        "there is enough balance to cover the requested amount of type {0}, but cannot be achieved \
        with less than the maximum number of coins allowed ({MAX_GAS_PAYMENT_OBJECTS}); consider \
        merging the coins in the wallet and retrying"
    )]
    InsufficientFundsWithMaxCoins(String),
}

impl From<sui_types::error::SuiError> for SuiClientError {
    fn from(error: sui_types::error::SuiError) -> Self {
        Self::SuiError(Box::new(error))
    }
}

impl SuiClientError {
    /// Attempts to parse a shared object congestion error from an error string.
    /// Returns None if the string does not match the expected format.
    pub fn parse_congestion_error(error: &str) -> Result<Self, anyhow::Error> {
        use regex::Regex;
        let re = Regex::new(
            r"(?x)
            ExecutionCancelledDueToSharedObjectCongestion\x20\{\x20congested_objects:
            \x20CongestedObjects\(\[(0x[a-f0-9]+)\]\)\x20\}",
        )
        .expect("this regex is valid");
        re.captures(error)
            .and_then(|caps| caps.get(1))
            .map(|objects_match| {
                objects_match
                    .as_str()
                    .split(", ")
                    .filter_map(|id| ObjectID::from_hex_literal(id).ok())
                    .collect::<Vec<_>>()
            })
            .map(Self::SharedObjectCongestion)
            .ok_or_else(|| anyhow::anyhow!("not a congestion error: {}", error))
    }
}

/// Parameters for certifying and extending a blob.
///
/// When certificate is present, the blob will be certified on Sui.
/// When epochs_ahead is present, the blob will be extended on Sui.
/// These two operations are allowed to be present at the same time.
#[derive(Debug, Clone)]
pub struct CertifyAndExtendBlobParams<'a> {
    /// The ID of the blob.
    pub blob: &'a Blob,
    /// The certificate for the blob.
    pub certificate: Option<ConfirmationCertificate>,
    /// The number of epochs by which to extend the blob.
    pub epochs_extended: Option<EpochCount>,
}

/// Result of certifying and extending a blob.
#[derive(Debug, Clone)]
pub struct CertifyAndExtendBlobResult {
    /// The blob.
    pub blob_object_id: ObjectID,
    /// The result of the post store action.
    pub post_store_action_result: PostStoreActionResult,
}

impl CertifyAndExtendBlobResult {
    /// Returns the shared blob object ID if the post store action is [`PostStoreAction::Share`].
    pub fn shared_blob_object(&self) -> Option<ObjectID> {
        if let PostStoreActionResult::Shared(GetSharedBlobResult::Success(id)) =
            &self.post_store_action_result
        {
            Some(*id)
        } else {
            None
        }
    }
}

/// Metadata for a blob object on Sui.
#[derive(Debug, Clone)]
pub struct BlobObjectMetadata {
    /// The ID of the blob.
    pub blob_id: BlobId,
    /// The root hash of the blob.
    pub root_hash: MerkleNode,
    /// The unencoded size of the blob.
    pub unencoded_size: u64,
    /// The encoded size of the blob.
    pub encoded_size: u64,
    /// The encoding type of the blob.
    pub encoding_type: EncodingType,
}

impl<const V: bool> TryFrom<&BlobMetadataWithId<V>> for BlobObjectMetadata {
    type Error = SuiClientError;

    fn try_from(metadata: &BlobMetadataWithId<V>) -> Result<Self, Self::Error> {
        let encoded_size = metadata
            .metadata()
            .encoded_size()
            .context("cannot compute encoded size")?;
        Ok(Self {
            blob_id: *metadata.blob_id(),
            root_hash: metadata.metadata().compute_root_hash(),
            unencoded_size: metadata.metadata().unencoded_length(),
            encoded_size,
            encoding_type: metadata.metadata().encoding_type(),
        })
    }
}

/// Represents the persistence state of a blob on Walrus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum BlobPersistence {
    /// The blob cannot be deleted.
    Permanent,
    /// The blob is deletable.
    Deletable,
}

/// Represents the selection of blob and storage objects in relation to their expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpirySelectionPolicy {
    /// Select all the objects.
    All,
    /// Select only expired objects.
    Expired,
    /// Select only valid (non-expired) objects.
    Valid,
}

impl ExpirySelectionPolicy {
    /// Returns the policy for a give `include_expired` flag.
    pub fn from_include_expired_flag(include_expired: bool) -> Self {
        if include_expired {
            Self::All
        } else {
            Self::Valid
        }
    }

    /// Return `true` if the expiry epoch matches the policy for the current epoch.
    pub fn matches(&self, expiry_epoch: Epoch, current_epoch: Epoch) -> bool {
        match self {
            Self::All => true,
            Self::Expired => expiry_epoch <= current_epoch,
            Self::Valid => expiry_epoch > current_epoch,
        }
    }
}

impl BlobPersistence {
    /// Returns `true` if the blob is deletable.
    pub fn is_deletable(&self) -> bool {
        matches!(self, Self::Deletable)
    }

    /// Constructs [`Self`] based on the value of a `deletable` flag.
    ///
    /// If `deletable` is true, returns [`Self::Deletable`], force otherwise returns
    /// [`Self::Permanent`].
    pub fn from_deletable_and_permanent(
        deletable: bool,
        permanent: bool,
    ) -> Result<Self, InvalidBlobPersistenceError> {
        match (deletable, permanent) {
            (true, false) => Ok(Self::Deletable),
            (false, true) => Ok(Self::Permanent),
            (true, true) => Err(InvalidBlobPersistenceError),
            (false, false) => {
                // TODO(WAL-911): Change the default behavior to return `Deletable` after a few
                // releases.
                tracing::warn!(
                    "blob is marked as neither deletable nor permanent; blobs are currently \
                    permanent by default, but this behavior will change in the future; use \
                    `--deletable` or `--permanent` to explicitly specify the desired behavior"
                );
                Ok(Self::Permanent)
            }
        }
    }
}

/// Error returned when a blob is defined as both deletable and permanent.
#[derive(Debug, thiserror::Error)]
#[error("the blob cannot be defined as both deletable and permanent")]
pub struct InvalidBlobPersistenceError;

/// The action to be performed for newly-created blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostStoreAction {
    /// Burn the blob object.
    Burn,
    /// Transfer the blob object to the given address.
    TransferTo(SuiAddress),
    /// Keep the blob object in the wallet that created it.
    Keep,
    /// Put the blob into a shared blob object.
    Share,
}

impl PostStoreAction {
    /// Constructs [`Self`] based on the value of a `share` flag.
    ///
    /// If `share` is true, returns [`Self::Share`], otherwise returns [`Self::Keep`].
    pub fn from_share(share: bool) -> Self {
        if share { Self::Share } else { Self::Keep }
    }
}

/// Result of getting the shared blob object ID.
#[derive(Debug, Clone)]
pub enum GetSharedBlobResult {
    /// The blob was found.
    Success(ObjectID),
    /// The blob was not found.
    Failed(String),
}

/// Result of the post store action.
#[derive(Debug, Clone)]
pub enum PostStoreActionResult {
    /// The blob was burned.
    Burned,
    /// The blob was transferred to the given address.
    TransferredTo(SuiAddress),
    /// The blob was kept in the wallet that created it.
    Kept,
    /// The blob was put into a shared blob object.
    Shared(GetSharedBlobResult),
}

impl PostStoreActionResult {
    /// Constructs a new [`PostStoreActionResult`] from the given [`PostStoreAction`] and
    /// optional shared blob object ID.
    pub fn new(
        action: &PostStoreAction,
        shared_blob_object_id: Option<GetSharedBlobResult>,
    ) -> Self {
        match action {
            PostStoreAction::Burn => Self::Burned,
            PostStoreAction::TransferTo(address) => Self::TransferredTo(*address),
            PostStoreAction::Keep => Self::Kept,
            PostStoreAction::Share => {
                Self::Shared(shared_blob_object_id.expect("result should be present"))
            }
        }
    }
}

/// Enum to select between different pool operations that require authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolOperationWithAuthorization {
    /// The operation relates to the commission.
    Commission,
    /// The operation relates to the governance.
    Governance,
}

/// Enum to select between an emergency upgrade authorized with an EmergencyUpgradeCap
/// or a normal quorum-based upgrade that has been voted for by a quorum of nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeType {
    /// Authorize and execute an emergency upgrade.
    Emergency,
    /// Execute a quorum-based upgrade.
    Quorum,
}

impl UpgradeType {
    fn is_emergency_upgrade(&self) -> bool {
        *self == UpgradeType::Emergency
    }
}

/// Result alias for functions returning a `SuiClientError`.
pub type SuiClientResult<T> = Result<T, SuiClientError>;

/// Client implementation for interacting with the Walrus smart contracts.
pub struct SuiContractClient {
    /// SuiContractClientInner executes Sui transactions in a mutually exclusive manner. It makes
    /// sure that only one transaction is executed at a time, which prevents race conditions in
    /// using the same owned object in multiple transactions.
    inner: Mutex<SuiContractClientInner>,
    /// Client to read Walrus on-chain state.
    pub read_client: Arc<SuiReadClient>,
    /// The active address of the client from `wallet`. Store here for fast access without
    /// locking the wallet.
    wallet_address: SuiAddress,
    /// The gas budget used by the client. If not set, the client will use a dry run to estimate
    /// the required gas budget.
    gas_budget: Option<u64>,
}

impl SuiContractClient {
    /// Constructor for [`SuiContractClient`].
    pub async fn new<S: AsRef<str>>(
        wallet: Wallet,
        rpc_urls: &[S],
        contract_config: &ContractConfig,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: Option<u64>,
    ) -> SuiClientResult<Self> {
        let read_client = Arc::new(
            SuiReadClient::new(
                RetriableSuiClient::new_for_rpc_urls(rpc_urls, backoff_config.clone(), None)
                    .await?,
                contract_config,
            )
            .await?,
        );
        Self::new_with_read_client(wallet, gas_budget, read_client)
    }

    /// Constructor for [`SuiContractClient`] with metrics.
    pub async fn new_with_metrics<S: AsRef<str>>(
        wallet: Wallet,
        rpc_urls: &[S],
        contract_config: &ContractConfig,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: Option<u64>,
        metrics: Arc<SuiClientMetricSet>,
    ) -> SuiClientResult<Self> {
        let read_client = Arc::new(
            SuiReadClient::new(
                RetriableSuiClient::new_for_rpc_urls(rpc_urls, backoff_config.clone(), None)
                    .await?
                    .with_metrics(Some(metrics)),
                contract_config,
            )
            .await?,
        );
        Self::new_with_read_client(wallet, gas_budget, read_client)
    }

    /// Constructor for [`SuiContractClient`] with an existing [`SuiReadClient`].
    pub fn new_with_read_client(
        mut wallet: Wallet,
        gas_budget: Option<u64>,
        read_client: Arc<SuiReadClient>,
    ) -> SuiClientResult<Self> {
        let wallet_address = wallet.active_address()?;
        Ok(Self {
            inner: Mutex::new(SuiContractClientInner::new(
                wallet,
                read_client.clone(),
                gas_budget,
            )?),
            read_client,
            wallet_address,
            gas_budget,
        })
    }

    /// Returns the contained [`SuiReadClient`].
    pub fn read_client(&self) -> &SuiReadClient {
        &self.read_client
    }

    /// Gets the [`RetriableSuiClient`] from the associated read client.
    pub fn sui_client(&self) -> &RetriableSuiClient {
        self.read_client.sui_client()
    }

    /// Returns the active address of the client.
    pub fn address(&self) -> SuiAddress {
        self.wallet_address
    }

    /// Returns the balance of the owner for the given coin type.
    pub async fn balance(&self, coin_type: CoinType) -> SuiClientResult<u64> {
        self.read_client
            .balance(self.wallet_address, coin_type)
            .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .reserve_space(encoded_size, epochs_ahead)
                .await
        })
        .await
    }

    /// Registers blobs with the specified [`BlobObjectMetadata`] and [`StorageResource`]s,
    /// and returns the created blob objects.
    pub async fn register_blobs(
        &self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .register_blobs(blob_metadata_and_storage.clone(), persistence)
                .await
        })
        .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blobs`][Self::register_blobs] functions in one atomic transaction.
    pub async fn reserve_and_register_blobs(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .reserve_and_register_blobs(epochs_ahead, blob_metadata_list.clone(), persistence)
                .await
        })
        .await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    ///
    /// If the post store action is `share`, returns a mapping blob ID -> shared_blob_object_id.
    pub async fn certify_blobs(
        &self,
        blobs_with_certificates: &[(&Blob, ConfirmationCertificate)],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_blobs(blobs_with_certificates, post_store)
                .await
        })
        .await
    }

    /// Certifies the specified event blob on Sui, with the given metadata and epoch.
    pub async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_event_blob(
                    blob_metadata.clone(),
                    ending_checkpoint_seq_num,
                    epoch,
                    node_capability_object_id,
                )
                .await
        })
        .await
    }

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .invalidate_blob_id(certificate)
                .await
        })
        .await
    }

    /// Registers a candidate node.
    pub async fn register_candidate(
        &self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<StorageNodeCap> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .register_candidate(node_parameters, proof_of_possession.clone())
                .await
        })
        .await
    }

    /// Registers candidate nodes, sending the resulting capability objects to the specified
    /// addresses.
    pub async fn register_candidates(
        &self,
        registration_params_with_stake_amounts: Vec<(
            NodeRegistrationParams,
            ProofOfPossession,
            SuiAddress,
        )>,
    ) -> SuiClientResult<Vec<StorageNodeCap>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .register_candidates(registration_params_with_stake_amounts.clone())
                .await
        })
        .await
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the second
    /// element of the pair with the node represented by the first element of the pair in a single
    /// PTB.
    pub async fn stake_with_pools(
        &self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> SuiClientResult<Vec<StakedWal>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .stake_with_pools(node_ids_with_amounts)
                .await
        })
        .await
    }

    /// Call to request a withdrawal of staked WAL.
    pub async fn request_withdraw_stake(&self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .request_withdraw_stake(staked_wal_id)
                .await
        })
        .await
    }

    /// Withdraw staked WAL that has already been requested.
    pub async fn withdraw_stake(&self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.withdraw_stake(staked_wal_id).await
        })
        .await
    }

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    pub async fn voting_end(&self) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async { self.inner.lock().await.voting_end().await })
            .await
    }

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    pub async fn initiate_epoch_change(&self) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.initiate_epoch_change().await
        })
        .await
    }

    /// Call to notify the contract that this node is done syncing the specified epoch.
    pub async fn epoch_sync_done(
        &self,
        epoch: Epoch,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .epoch_sync_done(epoch, node_capability_object_id)
                .await
        })
        .await
    }

    /// Sets the commission receiver for the node.
    pub async fn set_commission_receiver(
        &self,
        node_id: ObjectID,
        receiver: Authorized,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .set_authorized_for_pool(
                    node_id,
                    PoolOperationWithAuthorization::Commission,
                    receiver.clone(),
                )
                .await
        })
        .await
    }

    /// Sets the governance authorized entity for the pool.
    pub async fn set_governance_authorized(
        &self,
        node_id: ObjectID,
        authorized: Authorized,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .set_authorized_for_pool(
                    node_id,
                    PoolOperationWithAuthorization::Governance,
                    authorized.clone(),
                )
                .await
        })
        .await
    }

    /// Vote as node `node_id` for upgrading the walrus package to the package at
    /// `package_path`.
    /// Returns the digest of the package.
    pub async fn vote_for_upgrade(
        &self,
        upgrade_manager: ObjectID,
        node_id: ObjectID,
        package_path: PathBuf,
    ) -> SuiClientResult<[u8; 32]> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .vote_for_upgrade(upgrade_manager, node_id, package_path.clone())
                .await
        })
        .await
    }

    /// Performs an upgrade, either executing a quorum-based upgrade or authorizing
    /// and executing an emergency upgrade, depending on the `upgrade_type`.
    ///
    /// Returns the new package ID.
    pub async fn upgrade(
        &self,
        upgrade_manager: ObjectID,
        package_path: PathBuf,
        upgrade_type: UpgradeType,
    ) -> SuiClientResult<ObjectID> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .upgrade(upgrade_manager, package_path.clone(), upgrade_type)
                .await
        })
        .await
    }

    /// Set the migration epoch on the staking object to the following epoch.
    ///
    /// This must be called in the new package after an upgrade is committed in a separate
    /// transaction.
    pub async fn set_migration_epoch(&self, new_package_id: ObjectID) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .set_migration_epoch(new_package_id)
            .await
    }

    /// Migrate the staking and system objects to the new package id.
    ///
    /// This must be called in the new package after the migration epoch is set and has started.
    pub async fn migrate_contracts(&self, new_package_id: ObjectID) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .migrate_contracts(new_package_id)
            .await
    }

    /// Creates a new [`contracts::wal_exchange::Exchange`] with a 1:1 exchange rate, funds it with
    /// `amount` FROST, and returns its object ID.
    pub async fn create_and_fund_exchange(
        &self,
        exchange_package: ObjectID,
        amount: u64,
    ) -> SuiClientResult<ObjectID> {
        self.inner
            .lock()
            .await
            .create_and_fund_exchange(exchange_package, amount)
            .await
    }

    /// Exchanges the given `amount` of SUI (in MIST) for WAL using the shared exchange.
    pub async fn exchange_sui_for_wal(
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .exchange_sui_for_wal(exchange_id, amount)
            .await
    }

    /// Creates a new credits (`subsidies::Subsidies` in Move) object,
    /// funds it with the specified amount,
    /// and returns the object ID and the admin cap ID.
    pub async fn create_and_fund_credits(
        &self,
        package_id: ObjectID,
        initial_buyer_subsidy_rate: u16,
        initial_system_subsidy_rate: u16,
        amount: u64,
    ) -> SuiClientResult<(ObjectID, ObjectID)> {
        self.inner
            .lock()
            .await
            .create_and_fund_credits(
                package_id,
                initial_buyer_subsidy_rate,
                initial_system_subsidy_rate,
                amount,
            )
            .await
    }

    /// Returns the list of [`Blob`] objects owned by the wallet currently in use.
    ///
    /// If `owner` is `None`, the current wallet address is used.
    pub async fn owned_blobs(
        &self,
        owner: Option<SuiAddress>,
        selection_policy: ExpirySelectionPolicy,
    ) -> SuiClientResult<Vec<Blob>> {
        let current_epoch = self.read_client.current_committee().await?.epoch;
        Ok(self
            .read_client
            .get_owned_objects::<Blob>(owner.unwrap_or(self.wallet_address), &[])
            .await?
            .filter(|blob| selection_policy.matches(blob.storage.end_epoch, current_epoch))
            .collect())
    }

    /// Returns the list of [`StorageResource`] objects owned by the wallet currently in use.
    pub async fn owned_storage(
        &self,
        selection_policy: ExpirySelectionPolicy,
    ) -> SuiClientResult<Vec<StorageResource>> {
        let current_epoch = self.read_client.current_committee().await?.epoch;
        Ok(self
            .read_client
            .get_owned_objects::<StorageResource>(self.wallet_address, &[])
            .await?
            .filter(|storage| selection_policy.matches(storage.end_epoch, current_epoch))
            .collect())
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.delete_blob(blob_object_id).await
        })
        .await
    }

    /// Merges the WAL and SUI coins owned by the wallet of the contract client.
    pub async fn merge_coins(&self) -> SuiClientResult<()> {
        self.inner.lock().await.merge_coins().await
    }

    /// Sends the `amount` gas to the provided `address`.
    pub async fn send_sui(&self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        self.inner.lock().await.send_sui(amount, address).await
    }

    /// Sends the `amount` WAL to the provided `address`.
    pub async fn send_wal(&self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        self.inner.lock().await.send_wal(amount, address).await
    }

    /// Burns the blob objects with the given object IDs.
    ///
    /// May use multiple PTBs in sequence to burn all the given object IDs.
    pub async fn burn_blobs(&self, blob_object_ids: &[ObjectID]) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.burn_blobs(blob_object_ids).await
        })
        .await
    }

    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .fund_shared_blob(shared_blob_obj_id, amount)
                .await
        })
        .await
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .extend_shared_blob(shared_blob_obj_id, epochs_extended)
                .await
        })
        .await
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .share_and_maybe_fund_blob(blob_obj_id, amount)
                .await
        })
        .await
    }

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
        &self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .extend_blob(blob_obj_id, epochs_extended)
                .await
        })
        .await
    }

    /// Updates the parameters for a storage node.
    pub async fn update_node_params(
        &self,
        node_parameters: NodeUpdateParams,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .update_node_params(node_parameters.clone(), node_capability_object_id)
                .await
        })
        .await
    }

    /// Collects the commission for the pool with id `node_id` and returns the
    /// withdrawn amount in FROST.
    pub async fn collect_commission(&self, node_id: ObjectID) -> SuiClientResult<u64> {
        self.retry_on_wrong_version(|| async {
            self.inner.lock().await.collect_commission(node_id).await
        })
        .await
    }

    /// Adds attribute to a blob object.
    ///
    /// If attribute does not exist, it is created with the given key-value pairs.
    /// If attribute already exists, an error is returned unless `force` is true.
    /// If `force` is true, the attribute is updated with the given key-value pairs.
    pub async fn add_blob_attribute(
        &mut self,
        blob_obj_id: ObjectID,
        blob_attribute: BlobAttribute,
        force: bool,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            let mut inner = self.inner.lock().await;
            match inner.add_blob_attribute(blob_obj_id, &blob_attribute).await {
                Ok(()) => Ok(()),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                    BlobError::EDuplicateMetadata(_),
                ))) => {
                    if force {
                        inner
                            .insert_or_update_blob_attribute_pairs(
                                blob_obj_id,
                                blob_attribute.iter(),
                            )
                            .await
                    } else {
                        Err(SuiClientError::AttributeAlreadyExists)
                    }
                }
                Err(e) => Err(e),
            }
        })
        .await
    }

    /// Removes the attribute dynamic field from a blob object.
    ///
    /// If attribute does not exist, an error is returned.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            match self
                .inner
                .lock()
                .await
                .remove_blob_attribute(blob_obj_id)
                .await
            {
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                    BlobError::EMissingMetadata(_),
                ))) => Err(SuiClientError::AttributeDoesNotExist),
                result => result,
            }
        })
        .await
    }

    /// Inserts or updates key-value pairs in the blob's attribute.
    ///
    /// If the attribute does not exist and `force` is true, it will be created.
    /// If the attribute does not exist and `force` is false, an error is returned.
    /// If the attribute exists, the key-value pairs will be updated.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        pairs: I,
        force: bool,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let mut inner = self.inner.lock().await;
        let pairs_clone: Vec<(String, String)> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();

        // Check if attribute exists first
        let attribute_exists = inner
            .read_client
            .get_blob_attribute(&blob_obj_id)
            .await?
            .is_some();

        if !attribute_exists {
            if !force {
                return Err(SuiClientError::AttributeDoesNotExist);
            }
            // Create new attribute if it doesn't exist and force is true
            inner
                .add_blob_attribute(blob_obj_id, &BlobAttribute::from(pairs_clone))
                .await
        } else {
            // Update existing attribute
            inner
                .insert_or_update_blob_attribute_pairs(blob_obj_id, pairs_clone)
                .await
        }
    }

    /// Removes key-value pairs from the blob's attribute.
    ///
    /// If any key does not exist, an error is returned.
    pub async fn remove_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        match self
            .inner
            .lock()
            .await
            .remove_blob_attribute_pairs(blob_obj_id, keys)
            .await
        {
            Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                BlobError::EMissingMetadata(_),
            ))) => Err(SuiClientError::AttributeDoesNotExist),
            result => result,
        }
    }

    /// Returns a mutable reference to the wallet.
    ///
    /// This is mainly useful for deployment code where a wallet is used to provide
    /// gas coins to the storage nodes and client, while also being used for staking
    /// operations.
    pub fn wallet_mut(&mut self) -> &mut Wallet {
        &mut self.inner.get_mut().wallet
    }

    /// Sends `n` WAL coins of `amount` to the specified `address`.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn multiple_pay_wal(
        &self,
        address: SuiAddress,
        amount: u64,
        n: u64,
    ) -> SuiClientResult<()> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .multiple_pay_wal(address, amount, n)
                .await
        })
        .await
    }

    /// Certifies and extends the specified blob on Sui in a single transaction.
    ///
    /// Returns the shared blob object ID if the post store action is Share.
    /// See [`CertifyAndExtendBlobParams`] for the details of the parameters.
    pub async fn certify_and_extend_blobs(
        &self,
        blobs_with_certificates: &[CertifyAndExtendBlobParams<'_>],
        post_store: PostStoreAction,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        self.retry_on_wrong_version(|| async {
            self.inner
                .lock()
                .await
                .certify_and_extend_blobs(blobs_with_certificates, post_store)
                .await
        })
        .await
    }

    async fn retry_on_wrong_version<F, Fut, T>(&self, f: F) -> SuiClientResult<T>
    where
        F: Fn() -> Fut + Send,
        Fut: Future<Output = SuiClientResult<T>> + Send,
        T: Send,
    {
        match f().await {
            e @ Err(SuiClientError::TransactionExecutionError(
                MoveExecutionError::Subsidies(SubsidiesError::EWrongVersion(_))
                | MoveExecutionError::Staking(StakingError::EWrongVersion(_))
                | MoveExecutionError::System(SystemError::EWrongVersion(_)),
            )) => {
                // Store old package IDs
                let old_package_id = self.read_client.get_system_package_id();
                let old_credits_package_id = self.read_client.get_credits_package_id();

                self.read_client.refresh_package_id().await?;
                self.read_client.refresh_credits_package_id().await?;

                // Check if either package ID changed
                if self.read_client.get_system_package_id() != old_package_id
                    || self.read_client.get_credits_package_id() != old_credits_package_id
                {
                    f().await
                } else {
                    e
                }
            }
            result => result,
        }
    }
}

struct SuiContractClientInner {
    /// The wallet used by the client.
    wallet: Wallet,
    /// The read client used by the client.
    read_client: Arc<SuiReadClient>,
    /// The gas budget used by the client. If not set, the client will use a dry run to estimate
    /// the required gas budget.
    gas_budget: Option<u64>,
}

impl SuiContractClientInner {
    /// Constructor for [`SuiContractClientInner`] with an existing [`SuiReadClient`].
    pub fn new(
        wallet: Wallet,
        read_client: Arc<SuiReadClient>,
        gas_budget: Option<u64>,
    ) -> SuiClientResult<Self> {
        Ok(Self {
            wallet,
            read_client,
            gas_budget,
        })
    }

    /// Adds attribute to a blob object.
    pub async fn add_blob_attribute(
        &mut self,
        blob_obj_id: ObjectID,
        blob_attribute: &BlobAttribute,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .add_blob_attribute(blob_obj_id.into(), blob_attribute.clone())
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "add_blob_attribute")
            .await?;
        Ok(())
    }

    /// Removes the attribute dynamic field from a blob object.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.remove_blob_attribute(blob_obj_id.into()).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "remove_blob_attribute")
            .await?;
        Ok(())
    }

    /// Inserts or updates a key-value pair in the blob's attribute.
    pub async fn insert_or_update_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        pairs: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = (T, T)>,
        T: Into<String>,
    {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .insert_or_update_blob_attribute_pairs(blob_obj_id.into(), pairs)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "insert_or_update_blob_attribute_pairs")
            .await?;
        Ok(())
    }

    /// Removes key-value pairs from the blob's attribute.
    pub async fn remove_blob_attribute_pairs<I, T>(
        &mut self,
        blob_obj_id: ObjectID,
        keys: I,
    ) -> SuiClientResult<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .remove_blob_attribute_pairs(blob_obj_id.into(), keys)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "remove_blob_attribute_pairs")
            .await?;
        Ok(())
    }

    /// Returns the contained [`SuiReadClient`].
    pub fn read_client(&self) -> &SuiReadClient {
        &self.read_client
    }

    /// Gets the [`RetriableSuiClient`] from the associated read client.
    pub fn sui_client(&self) -> &RetriableSuiClient {
        self.read_client.sui_client()
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        if self.read_client.get_credits_object_id().is_some() {
            match self
                .reserve_space_with_credits(encoded_size, epochs_ahead)
                .await
            {
                Ok(arg) => return Ok(arg),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    // TODO(WAL-908): Change warning to credits.
                    tracing::warn!(
                        "Walrus package version mismatch in subsidies call,
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }
        self.reserve_space_without_credits(encoded_size, epochs_ahead)
            .await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    async fn reserve_space_with_credits(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .reserve_space_with_credits(encoded_size, epochs_ahead)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_space_with_credits")
            .await?;
        let storage_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );

        self.sui_client().get_sui_object(storage_id[0]).await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    async fn reserve_space_without_credits(
        &mut self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.reserve_space(encoded_size, epochs_ahead).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_space_without_credits")
            .await?;
        let storage_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );

        self.sui_client().get_sui_object(storage_id[0]).await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    ///
    /// `blob_size` is the size of the unencoded blob. The encoded size of the blob must be
    /// less than or equal to the size reserved in `storage`.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn register_blobs(
        &mut self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        if blob_metadata_and_storage.is_empty() {
            tracing::debug!("no blobs to register");
            return Ok(vec![]);
        }

        let with_credits = self.read_client.get_credits_object_id().is_some();

        let expected_num_blobs = blob_metadata_and_storage.len();
        tracing::debug!(num_blobs = expected_num_blobs, "starting to register blobs");
        let mut pt_builder = self.transaction_builder()?;
        // Build a ptb to include all register blob commands for all blobs.
        for (blob_metadata, storage) in blob_metadata_and_storage.into_iter() {
            if with_credits {
                pt_builder
                    .register_blob_with_credits(storage.id.into(), blob_metadata, persistence)
                    .await?;
            } else {
                pt_builder
                    .register_blob(storage.id.into(), blob_metadata, persistence)
                    .await?;
            };
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "register_blobs")
            .await?;
        let blob_obj_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            blob_obj_ids.len() == expected_num_blobs,
            "unexpected number of blob objects created: {} expected {} ",
            blob_obj_ids.len(),
            expected_num_blobs
        );

        self.sui_client().get_sui_objects(&blob_obj_ids).await
    }

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blobs`][Self::register_blobs] functions in one atomic transaction.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn reserve_and_register_blobs(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        let with_credits = self.read_client.get_credits_object_id().is_some();
        if with_credits {
            match self
                .reserve_and_register_blobs_inner(
                    epochs_ahead,
                    blob_metadata_list.clone(),
                    persistence,
                    true,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    // TODO(WAL-908): Change warning to credits.
                    tracing::warn!(
                        "Walrus package version mismatch in subsidies call, \
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }
        self.reserve_and_register_blobs_inner(epochs_ahead, blob_metadata_list, persistence, false)
            .await
    }

    /// reserve and register blobs inner
    pub async fn reserve_and_register_blobs_inner(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
        with_credits: bool,
    ) -> SuiClientResult<Vec<Blob>> {
        if blob_metadata_list.is_empty() {
            tracing::debug!("no blobs to register");
            return Ok(vec![]);
        }

        let expected_num_blobs = blob_metadata_list.len();
        tracing::debug!(
            num_blobs = expected_num_blobs,
            "starting to reserve and register blobs"
        );

        let mut pt_builder = self.transaction_builder()?;

        // Reserve enough space for all blobs
        let mut main_storage_arg_size = blob_metadata_list
            .iter()
            .fold(0, |acc, metadata| acc + metadata.encoded_size);

        let main_storage_arg = if with_credits {
            pt_builder
                .reserve_space_with_credits(main_storage_arg_size, epochs_ahead)
                .await?
        } else {
            pt_builder
                .reserve_space(main_storage_arg_size, epochs_ahead)
                .await?
        };

        for blob_metadata in blob_metadata_list.into_iter() {
            // Split off a storage resource, unless the remainder is equal to the required size.
            let storage_arg = if main_storage_arg_size != blob_metadata.encoded_size {
                main_storage_arg_size -= blob_metadata.encoded_size;
                pt_builder
                    .split_storage_by_size(main_storage_arg.into(), main_storage_arg_size)
                    .await?
            } else {
                main_storage_arg
            };

            if with_credits {
                pt_builder
                    .register_blob_with_credits(storage_arg.into(), blob_metadata, persistence)
                    .await?
            } else {
                pt_builder
                    .register_blob(storage_arg.into(), blob_metadata, persistence)
                    .await?
            };
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "reserve_and_register_blobs_inner")
            .await?;
        let blob_obj_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            blob_obj_ids.len() == expected_num_blobs,
            "unexpected number of blob objects created: {} expected {} ",
            blob_obj_ids.len(),
            expected_num_blobs
        );

        self.sui_client().get_sui_objects(&blob_obj_ids).await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    ///
    /// If the post store action is `share`, returns a mapping blob ID -> shared_blob_object_id.
    pub async fn certify_blobs(
        &mut self,
        blobs_with_certificates: &[(&Blob, ConfirmationCertificate)],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        let mut pt_builder = self.transaction_builder()?;
        for (i, (blob, certificate)) in blobs_with_certificates.iter().enumerate() {
            tracing::debug!(
                blob_id = %blob.blob_id,
                count = format!("{}/{}", i + 1, blobs_with_certificates.len()),
                "certifying blob on Sui"
            );
            pt_builder.certify_blob(blob.id.into(), certificate).await?;
            Self::apply_post_store_action(&mut pt_builder, blob.id, post_store).await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "certify_blobs")
            .await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify blobs on Sui");
            return Err(anyhow!("could not certify blob: {:?}", res.errors).into());
        }

        if post_store != PostStoreAction::Share {
            return Ok(HashMap::new());
        }

        // If the blobs are shared, create a mapping blob ID -> shared_blob_object_id.
        self.create_blob_id_to_shared_mapping(
            &res,
            blobs_with_certificates
                .iter()
                .map(|(blob, _)| blob.blob_id)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
    }

    /// Certifies the specified event blob on Sui, with the given metadata and epoch.
    pub async fn certify_event_blob(
        &mut self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        tracing::debug!(
            %node_capability_object_id,
            "calling certify_event_blob"
        );

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .certify_event_blob(
                blob_metadata,
                node_capability_object_id.into(),
                ending_checkpoint_seq_num,
                epoch,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "certify_event_blob")
            .await?;
        Ok(())
    }

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &mut self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.invalidate_blob_id(certificate).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "invalidate_blob_id")
            .await?;
        Ok(())
    }

    /// Registers a candidate node.
    pub async fn register_candidate(
        &mut self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<StorageNodeCap> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .register_candidate(node_parameters, proof_of_possession)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "register_candidate")
            .await?;
        let cap_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_node::StorageNodeCap
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            cap_id.len() == 1,
            "unexpected number of StorageNodeCap created: {}",
            cap_id.len()
        );

        self.sui_client().get_sui_object(cap_id[0]).await
    }

    /// Registers candidate nodes, sending the resulting capability objects to the specified
    /// addresses.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn register_candidates(
        &mut self,
        registration_params_with_stake_amounts: Vec<(
            NodeRegistrationParams,
            ProofOfPossession,
            SuiAddress,
        )>,
    ) -> SuiClientResult<Vec<StorageNodeCap>> {
        let count = registration_params_with_stake_amounts.len();
        if count == 0 {
            tracing::debug!("no candidates to register");
            return Ok(vec![]);
        }

        let mut pt_builder = self.transaction_builder()?;
        for (node_parameters, proof_of_possession, address) in
            registration_params_with_stake_amounts.into_iter()
        {
            let cap = pt_builder
                .register_candidate(&node_parameters, proof_of_possession)
                .await?;
            pt_builder.transfer(Some(address), vec![cap.into()]).await?;
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;

        let res = self
            .sign_and_send_transaction(transaction, "register_candidates")
            .await?;

        let cap_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_node::StorageNodeCap
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            cap_ids.len() == count,
            "unexpected number of StorageNodeCap created: {} (expected {count})",
            cap_ids.len(),
        );

        self.sui_client().get_sui_objects(&cap_ids).await
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the second
    /// element of the pair with the node represented by the first element of the pair in a single
    /// PTB.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn stake_with_pools(
        &mut self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> SuiClientResult<Vec<StakedWal>> {
        let count = node_ids_with_amounts.len();
        if count == 0 {
            tracing::debug!("no nodes to stake with provided");
            return Ok(vec![]);
        }
        let mut pt_builder = self.transaction_builder()?;
        for &(node_id, amount) in node_ids_with_amounts.iter() {
            if amount < MIN_STAKING_THRESHOLD {
                return Err(SuiClientError::StakeBelowThreshold(amount));
            }
            pt_builder.stake_with_pool(amount, node_id).await?;
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "stake_with_pools")
            .await?;

        let staked_wal = get_created_sui_object_ids_by_type(
            &res,
            &contracts::staked_wal::StakedWal
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            staked_wal.len() == count,
            "unexpected number of StakedWal objects created: {} (expected {})",
            staked_wal.len(),
            count
        );

        self.sui_client().get_sui_objects(&staked_wal).await
    }

    /// Call to request withdrawal of stake from StakedWal object.
    ///
    /// StakedWal is available after an epoch has passed.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn request_withdraw_stake(&mut self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.request_withdraw_stake(staked_wal_id).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "request_withdraw_stake")
            .await?;
        Ok(())
    }

    /// Call to request withdrawal of stake from StakedWal object.
    ///
    /// StakedWal is available after an epoch has passed.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn withdraw_stake(&mut self, staked_wal_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.withdraw_stake(staked_wal_id).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "withdraw_stake")
            .await?;
        Ok(())
    }

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    pub async fn voting_end(&mut self) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.voting_end().await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "voting_end")
            .await?;
        Ok(())
    }

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    pub async fn initiate_epoch_change(&mut self) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.initiate_epoch_change().await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "initiate_epoch_change")
            .await?;
        Ok(())
    }

    /// Call to notify the contract that this node is done syncing the specified epoch.
    pub async fn epoch_sync_done(
        &mut self,
        epoch: Epoch,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        let node_capability: StorageNodeCap = self
            .sui_client()
            .get_sui_object(node_capability_object_id)
            .await?;

        if node_capability.last_epoch_sync_done >= epoch {
            return Err(SuiClientError::LatestAttestedIsMoreRecent);
        }

        tracing::debug!(
            %node_capability,
            "calling epoch_sync_done"
        );

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .epoch_sync_done(node_capability.id.into(), epoch)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "epoch_sync_done")
            .await?;
        Ok(())
    }

    /// Vote as node `node_id` for upgrading the walrus package to the package at
    /// `package_path`.
    /// Returns the digest of the package.
    pub async fn vote_for_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        node_id: ObjectID,
        package_path: PathBuf,
    ) -> SuiClientResult<[u8; 32]> {
        let digest = self
            .read_client
            .compute_package_digest(package_path)
            .await?;

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .vote_for_upgrade(upgrade_manager, node_id, &digest)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "vote_for_upgrade")
            .await?;

        Ok(digest)
    }

    /// Performs an upgrade.
    ///
    /// Returns the new package ID.
    pub async fn upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        package_path: PathBuf,
        upgrade_type: UpgradeType,
    ) -> SuiClientResult<ObjectID> {
        // Compile package
        let chain_id = self.sui_client().get_chain_identifier().await.ok();
        let (compiled_package, build_config) =
            compile_package(package_path, Default::default(), chain_id).await?;

        let mut pt_builder = self.transaction_builder()?;

        pt_builder
            .custom_walrus_upgrade(upgrade_manager, compiled_package, upgrade_type)
            .await?;

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let response = self
            .sign_and_send_transaction(transaction, "upgrade")
            .await?;
        self.post_upgrade_lock_file_update(&response, build_config)
            .await
    }

    /// Set the migration epoch on the staking object to the following epoch.
    ///
    /// This must be called in the new package after an upgrade is committed in a separate
    /// transaction.
    pub async fn set_migration_epoch(&mut self, new_package_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.set_migration_epoch(new_package_id).await?;
        let transaction: TransactionData =
            pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "set_migration_epoch")
            .await?;
        Ok(())
    }

    /// Migrate the staking and system objects to the new package id.
    ///
    /// This must be called in the new package after the migration epoch is set and has started.
    pub async fn migrate_contracts(&mut self, new_package_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.migrate_contracts(new_package_id).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "migrate_contracts")
            .await?;
        Ok(())
    }

    async fn set_authorized_for_pool(
        &mut self,
        node_id: ObjectID,
        operation: PoolOperationWithAuthorization,
        authorized: Authorized,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        let authorized_arg = pt_builder.authorized_address_or_object(authorized)?;
        match operation {
            PoolOperationWithAuthorization::Commission => {
                pt_builder
                    .set_commission_receiver(node_id, authorized_arg)
                    .await?;
            }
            PoolOperationWithAuthorization::Governance => {
                pt_builder
                    .set_governance_authorized(node_id, authorized_arg)
                    .await?;
            }
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "set_authorized_for_pool")
            .await?;
        Ok(())
    }

    /// Creates a new [`contracts::wal_exchange::Exchange`] with a 1:1 exchange rate, funds it with
    /// `amount` FROST, and returns its object ID.
    pub async fn create_and_fund_exchange(
        &mut self,
        exchange_package: ObjectID,
        amount: u64,
    ) -> SuiClientResult<ObjectID> {
        tracing::info!("creating a new SUI/WAL exchange");

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .create_and_fund_exchange(exchange_package, amount)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "create_and_fund_exchange")
            .await?;
        let exchange_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::wal_exchange::Exchange
                .to_move_struct_tag_with_package(exchange_package, &[])?,
        )?;
        ensure!(
            exchange_id.len() == 1,
            "unexpected number of `Exchange`s created: {}",
            exchange_id.len()
        );
        Ok(exchange_id[0])
    }

    /// Creates a new credits object (i.e. `subsidies::Subsidies` in Move),
    /// funds it with `amount` FROST, and returns its object ID, as well
    /// as the object ID of its admin cap.
    pub async fn create_and_fund_credits(
        &mut self,
        package_id: ObjectID,
        initial_buyer_subsidy_rate: u16,
        initial_system_subsidy_rate: u16,
        amount: u64,
    ) -> SuiClientResult<(ObjectID, ObjectID)> {
        tracing::info!("creating a new credits object");

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .create_and_fund_credits(
                package_id,
                initial_buyer_subsidy_rate,
                initial_system_subsidy_rate,
                amount,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "create_and_fund_credits")
            .await?;
        let admin_cap = get_created_sui_object_ids_by_type(
            &res,
            &contracts::credits::AdminCap.to_move_struct_tag_with_package(package_id, &[])?,
        )?;
        let credits_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::credits::Subsidies.to_move_struct_tag_with_package(package_id, &[])?,
        )?;
        ensure!(
            credits_id.len() == 1,
            "unexpected number of `Subsidies`s created: {}",
            credits_id.len()
        );
        Ok((credits_id[0], admin_cap[0]))
    }

    /// Exchanges the given `amount` of SUI (in MIST) for WAL using the shared exchange.
    pub async fn exchange_sui_for_wal(
        &mut self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        tracing::debug!(amount, "exchanging SUI/MIST for WAL/FROST");

        let mut pt_builder = self.transaction_builder()?;
        pt_builder.exchange_sui_for_wal(exchange_id, amount).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "exchange_sui_for_wal")
            .await?;
        Ok(())
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&mut self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.delete_blob(blob_object_id.into()).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "delete_blob")
            .await?;
        Ok(())
    }

    /// Returns a new [`WalrusPtbBuilder`] for the client.
    pub fn transaction_builder(&mut self) -> SuiClientResult<WalrusPtbBuilder> {
        Ok(WalrusPtbBuilder::new(
            self.read_client.clone(),
            self.wallet.active_address()?,
        ))
    }

    async fn sign_and_send_transaction(
        &mut self,
        transaction: TransactionData,
        method: &'static str,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        // Sign the transaction with the wallet's keys
        let signed_transaction = self.wallet.sign_transaction(&transaction);

        // Execute the transaction and wait for response
        let response = self
            .sui_client()
            .execute_transaction(signed_transaction, method)
            .await?;

        // Check transaction execution status from effects
        match response
            .effects
            .as_ref()
            .ok_or_else(|| anyhow!("No transaction effects in response"))?
            .status()
        {
            SuiExecutionStatus::Success => Ok(response),
            SuiExecutionStatus::Failure { error } => {
                // Convert execution error into client error
                // Try parsing congestion error first, fallback to general execution error
                Err(
                    SuiClientError::parse_congestion_error(error.as_str()).unwrap_or_else(|_| {
                        SuiClientError::TransactionExecutionError(error.as_str().into())
                    }),
                )
            }
        }
    }

    /// Merges the WAL and SUI coins owned by the wallet of the contract client.
    pub async fn merge_coins(&mut self) -> SuiClientResult<()> {
        let mut tx_builder = self.transaction_builder()?;
        let address = self.wallet.active_address()?;
        let sui_balance = self.sui_client().get_balance(address, None).await?;
        let wal_balance = self
            .sui_client()
            .get_balance(address, Some(self.read_client().wal_coin_type().to_owned()))
            .await?;

        if wal_balance.coin_object_count > 1 {
            tx_builder
                .fill_wal_balance(
                    wal_balance
                        .total_balance
                        .try_into()
                        .expect("this is always smaller than u64::MAX"),
                )
                .await?;
        }

        if sui_balance.coin_object_count > 1 || wal_balance.coin_object_count > 1 {
            self.sign_and_send_transaction(
                tx_builder
                    .build_transaction_data_with_min_gas_balance(
                        self.gas_budget,
                        sui_balance
                            .total_balance
                            .try_into()
                            .expect("this is always smaller than u64::MAX"),
                    )
                    .await?,
                "merge_coins",
            )
            .await?;
        }

        Ok(())
    }

    /// Sends the `amount` gas to the provided `recipient`.
    pub async fn send_sui(&mut self, amount: u64, recipient: SuiAddress) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.pay_sui(recipient, amount).await?;
        self.sign_and_send_transaction(
            pt_builder.build_transaction_data(self.gas_budget).await?,
            "send_sui",
        )
        .await?;
        Ok(())
    }

    /// Sends the `amount` WAL to the provided `address`.
    pub async fn send_wal(&mut self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        tracing::debug!(%address, "sending WAL to address");
        let mut pt_builder = self.transaction_builder()?;

        pt_builder.pay_wal(address, amount).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "send_wal")
            .await?;
        Ok(())
    }

    /// Burns the blob objects with the given object IDs.
    ///
    /// May use multiple PTBs in sequence to burn all the given object IDs.
    pub async fn burn_blobs(&mut self, blob_object_ids: &[ObjectID]) -> SuiClientResult<()> {
        tracing::debug!(n_blobs = blob_object_ids.len(), "burning blobs");

        for id_block in blob_object_ids.chunks(MAX_BURNS_PER_PTB) {
            let mut pt_builder = self.transaction_builder()?;
            for id in id_block {
                pt_builder.burn_blob(id.into()).await?;
            }
            let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
            self.sign_and_send_transaction(transaction, "burn_blobs")
                .await?;
        }

        Ok(())
    }

    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &mut self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "fund_shared_blob")
            .await?;
        Ok(())
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &mut self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .extend_shared_blob(shared_blob_obj_id, epochs_extended)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_shared_blob")
            .await?;
        Ok(())
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &mut self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        let blob: Blob = self
            .read_client
            .sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder()?;

        if let Some(amount) = amount {
            ensure!(amount > 0, "must fund with non-zero amount");
            pt_builder
                .new_funded_shared_blob(blob.id.into(), amount)
                .await?;
        } else {
            pt_builder.new_shared_blob(blob.id.into()).await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "share_and_maybe_fund_blob")
            .await?;
        let shared_blob_obj_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            shared_blob_obj_id.len() == 1,
            "unexpected number of `SharedBlob`s created: {}",
            shared_blob_obj_id.len()
        );
        Ok(shared_blob_obj_id[0])
    }

    /// Extends the owned blob object by `epochs_extended` epochs without credits.
    async fn extend_blob_without_credits(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let blob: Blob = self
            .read_client
            .sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .extend_blob(
                blob_obj_id.into(),
                epochs_extended,
                blob.storage.storage_size,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_blob_without_credits")
            .await?;
        Ok(())
    }

    /// Extends the owned blob object by `epochs_extended` epochs with credits.
    async fn extend_blob_with_credits(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let blob: Blob = self
            .read_client
            .sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .extend_blob_with_credits(
                blob_obj_id.into(),
                epochs_extended,
                blob.storage.storage_size,
            )
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "extend_blob_with_credits")
            .await?;
        Ok(())
    }

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
        &mut self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        let with_credits = self.read_client.get_credits_package_id().is_some();
        if with_credits {
            match self
                .extend_blob_with_credits(blob_obj_id, epochs_extended)
                .await
            {
                Ok(_) => return Ok(()),
                Err(SuiClientError::TransactionExecutionError(MoveExecutionError::System(
                    SystemError::EWrongVersion(_),
                ))) => {
                    // TODO(WAL-908): Change warning to credits.
                    tracing::warn!(
                        "Walrus package version mismatch in subsidies
                        call, falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        self.extend_blob_without_credits(blob_obj_id, epochs_extended)
            .await?;
        Ok(())
    }

    /// Updates the parameters for a storage node.
    pub async fn update_node_params(
        &mut self,
        node_parameters: NodeUpdateParams,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        let wallet_address = self.wallet.active_address()?;

        tracing::debug!(
            ?wallet_address,
            network_address = ?node_parameters.network_address,
            "updating node parameters"
        );

        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .update_node_params(node_capability_object_id.into(), node_parameters)
            .await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "update_node_params")
            .await?;
        Ok(())
    }

    /// Withdraws the commission for the pool with id `node_id` and returns the
    /// withdrawn amount in FROST.
    pub async fn collect_commission(&mut self, node_id: ObjectID) -> SuiClientResult<u64> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.collect_commission(node_id).await?;
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let response = self
            .sign_and_send_transaction(transaction, "collect_commission")
            .await?;
        let wal_type_tag = TypeTag::from_str(self.read_client.wal_coin_type())?;
        let sender_address = self.wallet.active_address()?;
        let Some(balance_change) = response
            .balance_changes
            .ok_or_else(|| anyhow!("transaction response does not contain balance changes"))?
            .into_iter()
            .find(|change| {
                change.coin_type == wal_type_tag
                    && change
                        .owner
                        .get_address_owner_address()
                        .is_ok_and(|address| address == sender_address)
            })
        else {
            return Err(anyhow!("no balance change for sender in transaction response").into());
        };
        let balance_change = u64::try_from(balance_change.amount).map_err(|e| {
            anyhow!(
                "balance change should be positive and fit into a u64: {}",
                e
            )
        })?;
        Ok(balance_change)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn multiple_pay_wal(
        &mut self,
        address: SuiAddress,
        amount: u64,
        n: u64,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        for _ in 0..n {
            pt_builder.pay_wal(address, amount).await?;
        }
        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        self.sign_and_send_transaction(transaction, "multiple_pay_wal")
            .await?;
        Ok(())
    }

    /// Certifies and extends the specified blob on Sui in a single transaction.
    /// Returns the shared blob object ID if the post store action is Share.
    pub async fn certify_and_extend_blobs(
        &mut self,
        blobs_with_certificates: &[CertifyAndExtendBlobParams<'_>],
        post_store: PostStoreAction,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        let with_credits = self.read_client.get_credits_package_id().is_some();
        if with_credits {
            match self
                .certify_and_extend_blobs_inner(blobs_with_certificates, post_store, true)
                .await
            {
                Ok(result) => return Ok(result),
                Err(SuiClientError::TransactionExecutionError(
                    MoveExecutionError::Staking(StakingError::EWrongVersion(_))
                    | MoveExecutionError::System(SystemError::EWrongVersion(_)),
                )) => {
                    // TODO(WAL-908): Change warning to credits.
                    tracing::warn!(
                        "Walrus package version mismatch in subsidies call, \
                            falling back to direct contract call"
                    );
                }
                Err(e) => return Err(e),
            }
        }

        self.certify_and_extend_blobs_inner(blobs_with_certificates, post_store, false)
            .await
    }

    /// Common implementation for certifying and extending blobs with different extension strategies
    async fn certify_and_extend_blobs_inner<'b>(
        &mut self,
        blobs_with_certificates: &[CertifyAndExtendBlobParams<'b>],
        post_store: PostStoreAction,
        with_credits: bool,
    ) -> SuiClientResult<Vec<CertifyAndExtendBlobResult>> {
        let mut pt_builder = self.transaction_builder()?;
        for blob_params in blobs_with_certificates {
            if let Some(certificate) = blob_params.certificate.as_ref() {
                pt_builder
                    .certify_blob(blob_params.blob.id.into(), certificate)
                    .await?;
            }

            if let Some(epochs_extended) = blob_params.epochs_extended {
                // TODO(WAL-835): buy single storage resource to extend multiple blobs
                if with_credits {
                    pt_builder
                        .extend_blob_with_credits(
                            blob_params.blob.id.into(),
                            epochs_extended,
                            blob_params.blob.storage.storage_size,
                        )
                        .await?;
                } else {
                    pt_builder
                        .extend_blob(
                            blob_params.blob.id.into(),
                            epochs_extended,
                            blob_params.blob.storage.storage_size,
                        )
                        .await?;
                }
            }

            SuiContractClientInner::apply_post_store_action(
                &mut pt_builder,
                blob_params.blob.id,
                post_store,
            )
            .await?;
        }

        let transaction = pt_builder.build_transaction_data(self.gas_budget).await?;
        let res = self
            .sign_and_send_transaction(transaction, "certify_and_extend_blobs")
            .await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify/extend blobs on Sui");
            return Err(anyhow!("could not certify/extend blob: {:?}", res.errors).into());
        }

        let post_store_action_results = self
            .get_post_store_action_results(&res, blobs_with_certificates, &post_store)
            .await
            .map_err(|e| {
                tracing::warn!(error = ?e, "failed to get post store action results");
                anyhow!(
                    "Blobs have been stored, but could not get post store action results: {:?}",
                    e
                )
            })?;

        let results = blobs_with_certificates
            .iter()
            .zip(post_store_action_results.into_iter())
            .map(|(blob_params, r)| CertifyAndExtendBlobResult {
                blob_object_id: blob_params.blob.id,
                post_store_action_result: r,
            })
            .collect();

        Ok(results)
    }

    /// Helper function to create a mapping from blob IDs to shared blob object IDs.
    async fn get_post_store_action_results(
        &self,
        res: &SuiTransactionBlockResponse,
        cert_and_extend_params: &[CertifyAndExtendBlobParams<'_>],
        post_store: &PostStoreAction,
    ) -> SuiClientResult<Vec<PostStoreActionResult>> {
        if *post_store == PostStoreAction::Share {
            self.get_share_blob_result(cert_and_extend_params, res)
                .await
        } else {
            Ok(cert_and_extend_params
                .iter()
                .map(|_| PostStoreActionResult::new(post_store, None))
                .collect())
        }
    }

    async fn get_share_blob_result(
        &self,
        cert_and_extend_params: &[CertifyAndExtendBlobParams<'_>],
        res: &SuiTransactionBlockResponse,
    ) -> SuiClientResult<Vec<PostStoreActionResult>> {
        // Try to get the shared blob mapping
        match self
            .get_shared_blob_mapping(cert_and_extend_params, res)
            .await
        {
            Ok(shared_mapping) => {
                // Create results with successful ID mappings
                let results = cert_and_extend_params
                    .iter()
                    .map(|param| {
                        let shared_result = match shared_mapping.get(&param.blob.id) {
                            Some(object_id) => GetSharedBlobResult::Success(*object_id),
                            None => GetSharedBlobResult::Failed(
                                "Object ID not found in mapping".to_string(),
                            ),
                        };
                        PostStoreActionResult::Shared(shared_result)
                    })
                    .collect();
                Ok(results)
            }
            Err(err) => {
                // Create results with error for all params
                let results = cert_and_extend_params
                    .iter()
                    .map(|_| {
                        PostStoreActionResult::Shared(GetSharedBlobResult::Failed(err.to_string()))
                    })
                    .collect();
                Ok(results)
            }
        }
    }

    async fn get_shared_blob_mapping(
        &self,
        params: &[CertifyAndExtendBlobParams<'_>],
        res: &SuiTransactionBlockResponse,
    ) -> SuiClientResult<HashMap<ObjectID, ObjectID>> {
        let object_ids = get_created_sui_object_ids_by_type(
            res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;

        ensure!(
            object_ids.len() == params.len(),
            "unexpected number of shared blob objects created: {} (expected {})",
            object_ids.len(),
            params.len()
        );

        let shared_blobs = self
            .sui_client()
            .get_sui_objects::<SharedBlob>(&object_ids)
            .await?;

        Ok(shared_blobs
            .into_iter()
            .map(|shared_blob| (shared_blob.blob.id, shared_blob.id))
            .collect())
    }

    /// Helper function to create a mapping from blob IDs to shared blob object IDs.
    async fn create_blob_id_to_shared_mapping(
        &self,
        res: &SuiTransactionBlockResponse,
        blobs_ids: &[BlobId],
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        let object_ids = get_created_sui_object_ids_by_type(
            res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            object_ids.len() == blobs_ids.len(),
            "unexpected number of shared blob objects created: {} (expected {})",
            object_ids.len(),
            blobs_ids.len()
        );

        // If there is only one blob, we can directly return the mapping
        if object_ids.len() == 1 {
            Ok(HashMap::from([(blobs_ids[0], object_ids[0])]))
        } else {
            // Fetch all SharedBlob objects and collect them as a mapping from blob object ID
            // to shared blob object ID.
            let shared_blobs = self
                .sui_client()
                .get_sui_objects::<SharedBlob>(&object_ids)
                .await?;
            Ok(shared_blobs
                .into_iter()
                .map(|shared_blob| (shared_blob.blob.blob_id, shared_blob.id))
                .collect())
        }
    }

    /// Applies the post-store action for a single blob ID to the transaction builder.
    async fn apply_post_store_action(
        pt_builder: &mut WalrusPtbBuilder,
        blob_id: ObjectID,
        post_store: PostStoreAction,
    ) -> SuiClientResult<()> {
        match post_store {
            PostStoreAction::TransferTo(address) => {
                pt_builder
                    .transfer(Some(address), vec![blob_id.into()])
                    .await?;
            }
            PostStoreAction::Burn => {
                pt_builder.burn_blob(blob_id.into()).await?;
            }
            PostStoreAction::Keep => (),
            PostStoreAction::Share => {
                pt_builder.new_shared_blob(blob_id.into()).await?;
            }
        }
        Ok(())
    }

    /// Updates the lock file after an upgrade and returns the new package ID.
    async fn post_upgrade_lock_file_update(
        &mut self,
        response: &SuiTransactionBlockResponse,
        build_config: MoveBuildConfig,
    ) -> SuiClientResult<ObjectID> {
        let new_package_id = get_new_package_obj_from_response(response)
            .ok_or_else(|| {
                anyhow!(
                    "no new package ID found in the transaction response: {:?}",
                    response
                )
            })?
            .0;

        // Update the lock file with the upgraded package info.
        self.wallet
            .update_lock_file(
                LockCommand::Upgrade,
                build_config.install_dir,
                build_config.lock_file,
                response,
            )
            .await?;
        Ok(new_package_id)
    }
}

impl ReadClient for SuiContractClient {
    async fn storage_price_per_unit_size(&self) -> SuiClientResult<u64> {
        self.read_client.storage_price_per_unit_size().await
    }

    async fn write_price_per_unit_size(&self) -> SuiClientResult<u64> {
        self.read_client.write_price_per_unit_size().await
    }

    async fn storage_and_write_price_per_unit_size(&self) -> SuiClientResult<(u64, u64)> {
        self.read_client
            .storage_and_write_price_per_unit_size()
            .await
    }

    async fn event_stream(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = ContractEvent>> {
        self.read_client
            .event_stream(polling_interval, cursor)
            .await
    }

    async fn get_blob_event(&self, event_id: EventID) -> SuiClientResult<BlobEvent> {
        self.read_client.get_blob_event(event_id).await
    }

    async fn current_committee(&self) -> SuiClientResult<Committee> {
        self.read_client.current_committee().await
    }

    async fn previous_committee(&self) -> SuiClientResult<Committee> {
        self.read_client.previous_committee().await
    }

    async fn next_committee(&self) -> SuiClientResult<Option<Committee>> {
        self.read_client.next_committee().await
    }

    async fn get_storage_nodes_from_active_set(&self) -> Result<Vec<StorageNode>> {
        self.read_client.get_storage_nodes_from_active_set().await
    }

    async fn get_storage_nodes_from_committee(&self) -> SuiClientResult<Vec<StorageNode>> {
        self.read_client.get_storage_nodes_from_committee().await
    }

    async fn get_storage_nodes_by_ids(&self, node_ids: &[ObjectID]) -> Result<Vec<StorageNode>> {
        self.read_client.get_storage_nodes_by_ids(node_ids).await
    }

    async fn get_blob_attribute(
        &self,
        blob_obj_id: &ObjectID,
    ) -> SuiClientResult<Option<BlobAttribute>> {
        self.read_client.get_blob_attribute(blob_obj_id).await
    }

    async fn get_blob_by_object_id(
        &self,
        blob_obj_id: &ObjectID,
    ) -> SuiClientResult<BlobWithAttribute> {
        self.read_client.get_blob_by_object_id(blob_obj_id).await
    }

    async fn epoch_state(&self) -> SuiClientResult<EpochState> {
        self.read_client.epoch_state().await
    }

    async fn current_epoch(&self) -> SuiClientResult<Epoch> {
        self.read_client.current_epoch().await
    }

    async fn get_committees_and_state(&self) -> SuiClientResult<CommitteesAndState> {
        self.read_client.get_committees_and_state().await
    }

    async fn fixed_system_parameters(&self) -> SuiClientResult<FixedSystemParameters> {
        self.read_client.fixed_system_parameters().await
    }

    async fn stake_assignment(&self) -> SuiClientResult<HashMap<ObjectID, u64>> {
        self.read_client.stake_assignment().await
    }

    async fn last_certified_event_blob(&self) -> SuiClientResult<Option<EventBlob>> {
        self.read_client.last_certified_event_blob().await
    }

    async fn refresh_package_id(&self) -> SuiClientResult<()> {
        self.read_client.refresh_package_id().await
    }

    async fn refresh_credits_package_id(&self) -> SuiClientResult<()> {
        self.read_client.refresh_credits_package_id().await
    }

    async fn system_object_version(&self) -> SuiClientResult<u64> {
        self.read_client.system_object_version().await
    }
}

impl fmt::Debug for SuiContractClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SuiContractClient")
            .field("wallet", &"<redacted>")
            .field("read_client", &self.read_client)
            .field("wallet_address", &self.wallet_address)
            .field("gas_budget", &self.gas_budget)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_congestion_error() {
        let error_string = "ExecutionCancelledDueToSharedObjectCongestion { \
            congested_objects: \
            CongestedObjects([0x98ebc47370603fe81d9e15491b2f1443d619d1dab720d586e429ed233e1255c1]) \
        }";

        let error = SuiClientError::parse_congestion_error(error_string);
        assert!(error.is_ok());
        let congestion_error = error.unwrap();
        assert!(matches!(
            congestion_error,
            SuiClientError::SharedObjectCongestion(obj_ids)
                if obj_ids[0] == ObjectID::from_hex_literal(
                    "0x98ebc47370603fe81d9e15491b2f1443d619d1dab720d586e429ed233e1255c1"
                ).unwrap()
        ));
    }
}
