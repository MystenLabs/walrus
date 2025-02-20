// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use core::fmt;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use contract_config::ContractConfig;
use move_package::BuildConfig as MoveBuildConfig;
use retry_client::RetriableSuiClient;
use sui_package_management::LockCommand;
use sui_sdk::{
    rpc_types::{
        get_new_package_obj_from_response,
        Coin,
        SuiExecutionStatus,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
    },
    types::base_types::{ObjectID, ObjectRef},
    wallet_context::WalletContext,
};
use sui_types::{
    base_types::SuiAddress,
    event::EventID,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{ProgrammableTransaction, TransactionData, TransactionKind},
};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use transaction_builder::{WalrusPtbBuilder, MAX_BURNS_PER_PTB};
use walrus_core::{
    ensure,
    merkle::Node as MerkleNode,
    messages::{ConfirmationCertificate, InvalidBlobCertificate, ProofOfPossession},
    metadata::{BlobMetadataApi as _, BlobMetadataWithId},
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
};
use walrus_utils::backoff::ExponentialBackoffConfig;

use crate::{
    contracts,
    system_setup::compile_package,
    types::{
        move_errors::{BlobError, MoveExecutionError},
        move_structs::{
            Authorized,
            Blob,
            BlobAttribute,
            BlobWithAttribute,
            EmergencyUpgradeCap,
            EpochState,
            SharedBlob,
            StorageNode,
        },
        BlobEvent,
        Committee,
        ContractEvent,
        NodeRegistrationParams,
        NodeUpdateParams,
        StakedWal,
        StorageNodeCap,
        StorageResource,
    },
    utils::get_created_sui_object_ids_by_type,
};

mod read_client;
pub use read_client::{
    CoinType,
    CommitteesAndState,
    FixedSystemParameters,
    ReadClient,
    SuiReadClient,
};
pub mod retry_client;

pub mod transaction_builder;
use crate::types::move_structs::EventBlob;

pub mod contract_config;

#[derive(Debug, thiserror::Error)]
/// Error returned by the [`SuiContractClient`] and the [`SuiReadClient`].
pub enum SuiClientError {
    /// Unexpected internal errors.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    /// Error resulting from a Sui-SDK call.
    #[error(transparent)]
    SuiSdkError(#[from] sui_sdk::error::Error),
    /// Other errors resulting from Sui crates.
    #[error(transparent)]
    SuiError(#[from] sui_types::error::SuiError),
    /// Error in a transaction execution.
    #[error("transaction execution failed: {0}")]
    TransactionExecutionError(MoveExecutionError),
    /// No matching WAL coin found for the transaction.
    #[error("could not find WAL coins with sufficient balance")]
    NoCompatibleWalCoins,
    /// No matching gas coin found for the transaction.
    #[error("could not find gas coins with sufficient balance")]
    NoCompatibleGasCoins,
    /// The Walrus system object does not exist.
    #[error(
        "the specified Walrus system object {0} does not exist
        or is incompatible with this binary;\n\
        make sure you have the latest binary and configuration,
        and the correct Sui network is activated \
        in your Sui wallet"
    )]
    WalrusSystemObjectDoesNotExist(ObjectID),
    /// The specified Walrus package could not be found.
    #[error(
        "the specified Walrus package {0} could not be found\n\
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
        "storage capability object already exists in the account and cannot register another\n\
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
        .unwrap();
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn from_deletable(deletable: bool) -> Self {
        if deletable {
            Self::Deletable
        } else {
            Self::Permanent
        }
    }
}

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
        if share {
            Self::Share
        } else {
            Self::Keep
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
    pub async fn new(
        wallet: WalletContext,
        contract_config: &ContractConfig,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: Option<u64>,
    ) -> SuiClientResult<Self> {
        let read_client = Arc::new(
            SuiReadClient::new(
                RetriableSuiClient::new_from_wallet(&wallet, backoff_config.clone()).await?,
                contract_config,
            )
            .await?,
        );
        Self::new_with_read_client(wallet, gas_budget, read_client)
    }

    /// Constructor for [`SuiContractClient`] with an existing [`SuiReadClient`].
    pub fn new_with_read_client(
        mut wallet: WalletContext,
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
        self.inner
            .lock()
            .await
            .reserve_space(encoded_size, epochs_ahead)
            .await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    ///
    /// `blob_size` is the size of the unencoded blob. The encoded size of the blob must be
    /// less than or equal to the size reserved in `storage`.
    pub async fn register_blobs(
        &self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        self.inner
            .lock()
            .await
            .register_blobs(blob_metadata_and_storage, persistence)
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
        self.inner
            .lock()
            .await
            .reserve_and_register_blobs(epochs_ahead, blob_metadata_list, persistence)
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
        self.inner
            .lock()
            .await
            .certify_blobs(blobs_with_certificates, post_store)
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
        self.inner
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

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .invalidate_blob_id(certificate)
            .await
    }

    /// Registers a candidate node.
    pub async fn register_candidate(
        &self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<StorageNodeCap> {
        self.inner
            .lock()
            .await
            .register_candidate(node_parameters, proof_of_possession)
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
        self.inner
            .lock()
            .await
            .register_candidates(registration_params_with_stake_amounts)
            .await
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the second
    /// element of the pair with the node represented by the first element of the pair in a single
    /// PTB.
    pub async fn stake_with_pools(
        &self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> SuiClientResult<Vec<StakedWal>> {
        self.inner
            .lock()
            .await
            .stake_with_pools(node_ids_with_amounts)
            .await
    }

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    pub async fn voting_end(&self) -> SuiClientResult<()> {
        self.inner.lock().await.voting_end().await
    }

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    pub async fn initiate_epoch_change(&self) -> SuiClientResult<()> {
        self.inner.lock().await.initiate_epoch_change().await
    }

    /// Call to notify the contract that this node is done syncing the specified epoch.
    pub async fn epoch_sync_done(
        &self,
        epoch: Epoch,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .epoch_sync_done(epoch, node_capability_object_id)
            .await
    }

    /// Sets the commission receiver for the node.
    pub async fn set_commission_receiver(
        &self,
        node_id: ObjectID,
        receiver: Authorized,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .set_authorized_for_pool(
                node_id,
                PoolOperationWithAuthorization::Commission,
                receiver,
            )
            .await
    }

    /// Sets the governance authorized entity for the pool.
    pub async fn set_governance_authorized(
        &self,
        node_id: ObjectID,
        authorized: Authorized,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .set_authorized_for_pool(
                node_id,
                PoolOperationWithAuthorization::Governance,
                authorized,
            )
            .await
    }

    /// Performs an emergency upgrade.
    ///
    /// Returns the new package ID.
    pub async fn emergency_upgrade(
        &self,
        upgrade_manager: ObjectID,
        package_path: PathBuf,
    ) -> SuiClientResult<ObjectID> {
        self.inner
            .lock()
            .await
            .emergency_upgrade(upgrade_manager, package_path)
            .await
    }

    /// Migrate the staking and system objects to the new package id.
    ///
    /// This must be called in the new package after an upgrade is committed in a separate
    /// transaction.
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

    /// Returns the closest-matching owned storage resources for given size and number of epochs.
    ///
    /// Among all the owned [`StorageResource`] objects, returns the one that:
    /// - has the closest size to `storage_size`; and
    /// - breaks ties by taking the one with the smallest end epoch that is greater or equal to the
    ///   requested `end_epoch`.
    /// - If object id is in the excluded list, do not select.
    ///
    /// Returns `None` if no matching storage resource is found.
    pub async fn owned_storage_for_size_and_epoch(
        &self,
        storage_size: u64,
        end_epoch: Epoch,
        excluded: &[ObjectID],
    ) -> SuiClientResult<Option<StorageResource>> {
        Ok(self
            .owned_storage(ExpirySelectionPolicy::Valid)
            .await?
            .into_iter()
            .filter(|storage| {
                storage.storage_size >= storage_size && storage.end_epoch >= end_epoch
            })
            .filter(|storage| !excluded.contains(&storage.id))
            // Pick the smallest storage size. Break ties by comparing the end epoch, and take the
            // one that is the closest to `end_epoch`. NOTE: we are already sure that these values
            // are above the minimum.
            .min_by_key(|a| (a.storage_size, a.end_epoch)))
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        self.inner.lock().await.delete_blob(blob_object_id).await
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
        self.inner.lock().await.burn_blobs(blob_object_ids).await
    }

    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: u32,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .extend_shared_blob(shared_blob_obj_id, epochs_extended)
            .await
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        self.inner
            .lock()
            .await
            .share_and_maybe_fund_blob(blob_obj_id, amount)
            .await
    }

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
        &self,
        blob_obj_id: ObjectID,
        epochs_extended: EpochCount,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .extend_blob(blob_obj_id, epochs_extended)
            .await
    }

    /// Updates the parameters for a storage node.
    pub async fn update_node_params(
        &self,
        node_parameters: NodeUpdateParams,
        node_capability_object_id: ObjectID,
    ) -> SuiClientResult<()> {
        self.inner
            .lock()
            .await
            .update_node_params(node_parameters, node_capability_object_id)
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
        let mut inner = self.inner.lock().await;
        match inner.add_blob_attribute(blob_obj_id, &blob_attribute).await {
            Ok(()) => Ok(()),
            Err(SuiClientError::TransactionExecutionError(MoveExecutionError::Blob(
                BlobError::EDuplicateMetadata(_),
            ))) => {
                if force {
                    inner
                        .insert_or_update_blob_attribute_pairs(blob_obj_id, blob_attribute.iter())
                        .await
                } else {
                    Err(SuiClientError::AttributeAlreadyExists)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Removes the attribute dynamic field from a blob object.
    ///
    /// If attribute does not exist, an error is returned.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
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
    pub fn wallet_mut(&mut self) -> &mut WalletContext {
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
        self.inner
            .lock()
            .await
            .multiple_pay_wal(address, amount, n)
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
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        self.inner
            .lock()
            .await
            .certify_and_extend_blobs(blobs_with_certificates, post_store)
            .await
    }
}

struct SuiContractClientInner {
    /// The wallet used by the client.
    wallet: WalletContext,
    /// The read client used by the client.
    read_client: Arc<SuiReadClient>,
    /// The gas budget used by the client. If not set, the client will use a dry run to estimate
    /// the required gas budget.
    gas_budget: Option<u64>,
}

impl SuiContractClientInner {
    /// Constructor for [`SuiContractClientInner`] with an existing [`SuiReadClient`].
    pub fn new(
        wallet: WalletContext,
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Removes the attribute dynamic field from a blob object.
    pub async fn remove_blob_attribute(&mut self, blob_obj_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.remove_blob_attribute(blob_obj_id.into()).await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        tracing::debug!(encoded_size, "starting to reserve storage for blob");
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.reserve_space(encoded_size, epochs_ahead).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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
    pub async fn register_blobs(
        &mut self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        let mut pt_builder = self.transaction_builder()?;
        // Build a ptb to include all register blob commands for all blobs.
        let expected_num_blobs = blob_metadata_and_storage.len();
        for (blob_metadata, storage) in blob_metadata_and_storage.into_iter() {
            pt_builder
                .register_blob(storage.id.into(), blob_metadata, persistence)
                .await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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
    pub async fn reserve_and_register_blobs(
        &mut self,
        epochs_ahead: EpochCount,
        blob_metadata_list: Vec<BlobObjectMetadata>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        tracing::debug!(
            size = blob_metadata_list.len(),
            "starting to reserve and register blobs"
        );

        let mut pt_builder = self.transaction_builder()?;
        // Build a ptb to include all reserve space and register blob commands for all blobs.
        let expected_num_blobs = blob_metadata_list.len();
        for blob_metadata in blob_metadata_list.into_iter() {
            let storage_arg = pt_builder
                .reserve_space(blob_metadata.encoded_size, epochs_ahead)
                .await?;
            // Blob is transferred automatically in the call to `finish`.
            pt_builder
                .register_blob(storage_arg.into(), blob_metadata, persistence)
                .await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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

        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;

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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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
    pub async fn register_candidates(
        &mut self,
        registration_params_with_stake_amounts: Vec<(
            NodeRegistrationParams,
            ProofOfPossession,
            SuiAddress,
        )>,
    ) -> SuiClientResult<Vec<StorageNodeCap>> {
        let count = registration_params_with_stake_amounts.len();

        let mut pt_builder = self.transaction_builder()?;
        for (node_parameters, proof_of_possession, address) in
            registration_params_with_stake_amounts.into_iter()
        {
            let cap = pt_builder
                .register_candidate(&node_parameters, proof_of_possession)
                .await?;
            pt_builder.transfer(Some(address), vec![cap.into()]).await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;

        let res = self.sign_and_send_ptb(ptb).await?;

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
    pub async fn stake_with_pools(
        &mut self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> SuiClientResult<Vec<StakedWal>> {
        let count = node_ids_with_amounts.len();
        let mut pt_builder = self.transaction_builder()?;
        for (node_id, amount) in node_ids_with_amounts.iter() {
            pt_builder.stake_with_pool(*amount, *node_id).await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;

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

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    pub async fn voting_end(&mut self) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.voting_end().await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    pub async fn initiate_epoch_change(&mut self) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.initiate_epoch_change().await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Performs an emergency upgrade.
    ///
    /// Returns the new package ID.
    pub async fn emergency_upgrade(
        &mut self,
        upgrade_manager: ObjectID,
        package_path: PathBuf,
    ) -> SuiClientResult<ObjectID> {
        // Compile package
        let chain_id = self.sui_client().get_chain_identifier().await.ok();
        let (dependencies, compiled_package, build_config) =
            compile_package(package_path, MoveBuildConfig::default(), chain_id).await?;

        let digest = compiled_package.get_package_digest(false);

        let emergency_upgrade_cap: EmergencyUpgradeCap = self
            .read_client
            .get_owned_objects(self.wallet.active_address()?, &[])
            .await?
            .next()
            .ok_or_else(|| anyhow!("no emergency upgrade capability found"))?;
        let mut pt_builder = self.transaction_builder()?;

        // Authorize the upgrade.
        let upgrade_ticket_arg = pt_builder
            .authorize_emergency_upgrade(upgrade_manager, emergency_upgrade_cap.id.into(), &digest)
            .await?;

        // Execute the upgrade.
        let modules = compiled_package.get_package_bytes(false);
        let upgrade_receipt_arg = pt_builder.upgrade(
            self.read_client.get_system_package_id(),
            upgrade_ticket_arg,
            dependencies.published.into_values().collect(),
            modules,
        );

        // Commit the upgrade
        pt_builder
            .commit_upgrade(upgrade_manager, upgrade_receipt_arg)
            .await?;

        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let response = self.sign_and_send_ptb(ptb).await?;
        self.post_upgrade_lock_file_update(&response, build_config)
            .await
    }

    /// Migrate the staking and system objects to the new package id.
    ///
    /// This must be called in the new package after an upgrade is committed in a separate
    /// transaction.
    pub async fn migrate_contracts(&mut self, new_package_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.migrate_contracts(new_package_id).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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

    /// Exchanges the given `amount` of SUI (in MIST) for WAL using the shared exchange.
    pub async fn exchange_sui_for_wal(
        &mut self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        tracing::debug!(amount, "exchanging SUI/MIST for WAL/FROST");

        let mut pt_builder = self.transaction_builder()?;
        pt_builder.exchange_sui_for_wal(exchange_id, amount).await?;
        let (ptb, sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb_with_additional_gas_coin_balance(ptb, sui_cost)
            .await?;
        Ok(())
    }

    /// Deletes the specified blob from the wallet's storage.
    pub async fn delete_blob(&mut self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder.delete_blob(blob_object_id.into()).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Returns a new [`WalrusPtbBuilder`] for the client.
    pub fn transaction_builder(&mut self) -> SuiClientResult<WalrusPtbBuilder> {
        Ok(WalrusPtbBuilder::new(
            self.read_client.clone(),
            self.wallet.active_address()?,
        ))
    }

    /// Signs and sends a programmable transaction with no additional gas coin balance.
    ///
    /// This is the default case for any transactions that only use sui for paying gas. If the
    /// transaction uses sui for other purposes, the function
    /// [`Self::sign_and_send_ptb_with_additional_gas_coin_balance`] should be used instead.
    pub async fn sign_and_send_ptb(
        &mut self,
        programmable_transaction: ProgrammableTransaction,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        self.sign_and_send_ptb_inner(programmable_transaction, 0, 0)
            .await
    }

    /// Signs and sends a programmable transaction with an additional gas coin balance.
    ///
    /// This is useful for transactions that use sui in the transaction which is split off
    /// from the gas coin. The `additional_gas_coin_balance` is the amount of sui that is
    /// used in the transaction for anything except gas.
    pub async fn sign_and_send_ptb_with_additional_gas_coin_balance(
        &mut self,
        programmable_transaction: ProgrammableTransaction,
        additional_gas_coin_balance: u64,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        self.sign_and_send_ptb_inner(programmable_transaction, additional_gas_coin_balance, 0)
            .await
    }

    /// Signs and sends a programmable transaction with a minimum gas coin balance.
    ///
    /// This is useful mainly for merging sui coins, since it allows to set the full balance as
    /// minimum gas coin balance but still have the gas budget be estimated.
    async fn sign_and_send_ptb_with_min_gas_coin_balance(
        &mut self,
        programmable_transaction: ProgrammableTransaction,
        minimum_gas_coin_balance: u64,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        self.sign_and_send_ptb_inner(programmable_transaction, 0, minimum_gas_coin_balance)
            .await
    }

    async fn sign_and_send_ptb_inner(
        &mut self,
        programmable_transaction: ProgrammableTransaction,
        additional_gas_coin_balance: u64,
        minimum_gas_coin_balance: u64,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        // Get the current gas price from the network
        let gas_price = self.wallet.get_reference_gas_price().await?;
        let wallet_address = self.wallet.active_address()?;

        // Estimate the gas budget unless explicitly set.
        let gas_budget = if let Some(budget) = self.gas_budget {
            budget
        } else {
            let tx_kind =
                TransactionKind::ProgrammableTransaction(programmable_transaction.clone());
            self.read_client
                .sui_client()
                .estimate_gas_budget(wallet_address, tx_kind, gas_price)
                .await?
        };

        let min_gas_coin_balance =
            minimum_gas_coin_balance.max(gas_budget + additional_gas_coin_balance);

        // Construct the transaction with gas coins that meet the minimum balance requirement
        let transaction = TransactionData::new_programmable(
            wallet_address,
            self.get_compatible_gas_coins(min_gas_coin_balance).await?,
            programmable_transaction,
            gas_budget,
            gas_price,
        );

        // Sign the transaction with the wallet's keys
        let signed_transaction = self.wallet.sign_transaction(&transaction);

        // Execute the transaction and wait for response
        let response = self
            .sui_client()
            .execute_transaction(signed_transaction)
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

    async fn get_compatible_gas_coins(
        &mut self,
        min_balance: u64,
    ) -> SuiClientResult<Vec<ObjectRef>> {
        Ok(self
            .read_client
            .get_coins_with_total_balance(
                self.wallet.active_address()?,
                CoinType::Sui,
                min_balance,
                vec![],
            )
            .await?
            .iter()
            .map(Coin::object_ref)
            .collect())
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
                .fill_wal_balance(wal_balance.total_balance as u64)
                .await?;
        }

        if sui_balance.coin_object_count > 1 || wal_balance.coin_object_count > 1 {
            self.sign_and_send_ptb_with_min_gas_coin_balance(
                tx_builder.finish().await?.0,
                sui_balance.total_balance as u64,
            )
            .await?;
        }

        Ok(())
    }

    /// Sends the `amount` gas to the provided `address`.
    pub async fn send_sui(&mut self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        pt_builder.pay_sui(vec![address], vec![amount])?;
        self.sign_and_send_ptb_with_additional_gas_coin_balance(pt_builder.finish(), amount)
            .await?;
        Ok(())
    }

    /// Sends the `amount` WAL to the provided `address`.
    pub async fn send_wal(&mut self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        tracing::debug!(%address, "sending WAL to address");
        let mut pt_builder = self.transaction_builder()?;

        pt_builder.pay_wal(address, amount).await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
            let (ptb, _) = pt_builder.finish().await?;
            self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Extends the shared blob's lifetime by `epochs_extended` epochs.
    pub async fn extend_shared_blob(
        &mut self,
        shared_blob_obj_id: ObjectID,
        epochs_extended: u32,
    ) -> SuiClientResult<()> {
        let mut pt_builder = self.transaction_builder()?;
        pt_builder
            .extend_shared_blob(shared_blob_obj_id, epochs_extended)
            .await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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

        let (ptb, _) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;
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

    /// Extends the owned blob object by `epochs_extended` epochs.
    pub async fn extend_blob(
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
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
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
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
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(ptb).await?;
        Ok(())
    }

    /// Certifies and extends the specified blob on Sui in a single transaction.
    /// Returns the shared blob object ID if the post store action is Share.
    pub async fn certify_and_extend_blobs(
        &mut self,
        blobs_with_certificates: &[CertifyAndExtendBlobParams<'_>],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        let mut pt_builder = self.transaction_builder()?;
        for blob_params in blobs_with_certificates {
            if let Some(certificate) = blob_params.certificate.as_ref() {
                pt_builder
                    .certify_blob(blob_params.blob.id.into(), certificate)
                    .await?;
            }

            if let Some(epochs_extended) = blob_params.epochs_extended {
                pt_builder
                    .extend_blob(
                        blob_params.blob.id.into(),
                        epochs_extended,
                        blob_params.blob.storage.storage_size,
                    )
                    .await?;
            }

            Self::apply_post_store_action(&mut pt_builder, blob_params.blob.id, post_store).await?;
        }

        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(ptb).await?;

        if !res.errors.is_empty() {
            tracing::warn!(errors = ?res.errors, "failed to certify/extend blobs on Sui");
            return Err(anyhow!("could not certify/extend blob: {:?}", res.errors).into());
        }

        if post_store != PostStoreAction::Share {
            return Ok(HashMap::new());
        }

        // If the blobs are shared, create a mapping blob ID -> shared_blob_object_id.
        self.create_blob_id_to_shared_mapping(
            &res,
            blobs_with_certificates
                .iter()
                .map(|blob_params| blob_params.blob.blob_id)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
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
            // Fetch all SharedBlob objects and collect them as a mapping blob id
            // to shared blob object id
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
        sui_package_management::update_lock_file(
            &self.wallet,
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
