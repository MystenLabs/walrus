// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use core::fmt;
use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context, Result};
use contract_config::ContractConfig;
use retry_client::RetriableSuiClient;
use sui_sdk::{
    rpc_types::{
        Coin,
        SuiExecutionStatus,
        SuiObjectDataOptions,
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
    transaction::{Argument, ProgrammableTransaction},
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
    types::{
        move_errors::MoveExecutionError,
        move_structs::{Authorized, EpochState, SharedBlob},
        Blob,
        BlobEvent,
        Committee,
        ContractEvent,
        NodeRegistrationParams,
        StakedWal,
        StorageNodeCap,
        StorageResource,
    },
    utils::{get_created_sui_object_ids_by_type, sign_and_send_ptb},
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
        "the specified Walrus system object {0} does not exist or is incompatible with this binary;\
        \nmake sure you have the latest binary and configuration, and the correct Sui network is \
        activated in your Sui wallet"
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
    #[error("there are multiple storage node capability objects in the address")]
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
    wallet: Mutex<WalletContext>,
    /// Client to read Walrus on-chain state.
    pub read_client: SuiReadClient,
    wallet_address: SuiAddress,
    gas_budget: u64,
}

impl SuiContractClient {
    /// Constructor for [`SuiContractClient`].
    pub async fn new(
        wallet: WalletContext,
        contract_config: &ContractConfig,
        backoff_config: ExponentialBackoffConfig,
        gas_budget: u64,
    ) -> SuiClientResult<Self> {
        let read_client = SuiReadClient::new(
            RetriableSuiClient::new_from_wallet(&wallet, backoff_config).await?,
            contract_config,
        )
        .await?;
        Self::new_with_read_client(wallet, gas_budget, read_client)
    }

    /// Constructor for [`SuiContractClient`] with an existing [`SuiReadClient`].
    pub fn new_with_read_client(
        mut wallet: WalletContext,
        gas_budget: u64,
        read_client: SuiReadClient,
    ) -> SuiClientResult<Self> {
        let wallet_address = wallet.active_address()?;
        Ok(Self {
            wallet: Mutex::new(wallet),
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

    /// Returns a reference to the inner wallet context.
    pub async fn wallet(&self) -> tokio::sync::MutexGuard<'_, WalletContext> {
        self.wallet.lock().await
    }

    /// Returns the active address of the client.
    pub fn address(&self) -> SuiAddress {
        self.wallet_address
    }

    /// Returns the gas budget used by the client.
    pub fn gas_budget(&self) -> u64 {
        self.gas_budget
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
        tracing::debug!(encoded_size, "starting to reserve storage for blob");

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder.reserve_space(encoded_size, epochs_ahead).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
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
        &self,
        blob_metadata_and_storage: Vec<(BlobObjectMetadata, StorageResource)>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Vec<Blob>> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        // Build a ptb to include all register blob commands for all blobs.
        let expected_num_blobs = blob_metadata_and_storage.len();
        for (blob_metadata, storage) in blob_metadata_and_storage.into_iter() {
            pt_builder
                .register_blob(storage.id.into(), blob_metadata, persistence)
                .await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
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

        self.sui_client().get_sui_objects(blob_obj_ids).await
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
        tracing::debug!(
            size = blob_metadata_list.len(),
            "starting to reserve and register blobs"
        );

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
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
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
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

        self.sui_client().get_sui_objects(blob_obj_ids).await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    ///
    /// If the post store action is `share`, returns a mapping blob ID -> shared_blob_object_id.
    // NB: This intentionally takes an owned `Blob` object even though it is not required, as the
    // corresponding object on Sui will be changed in the process.
    pub async fn certify_blobs(
        &self,
        blobs_with_certificates: &[(&Blob, ConfirmationCertificate)],
        post_store: PostStoreAction,
    ) -> SuiClientResult<HashMap<BlobId, ObjectID>> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        for (blob, certificate) in blobs_with_certificates.iter() {
            pt_builder.certify_blob(blob.id.into(), certificate).await?;
            match post_store {
                PostStoreAction::TransferTo(address) => {
                    pt_builder
                        .transfer(Some(address), vec![blob.id.into()])
                        .await?;
                }
                PostStoreAction::Burn => {
                    pt_builder.burn_blob(blob.id.into()).await?;
                }
                PostStoreAction::Keep => (),
                PostStoreAction::Share => {
                    pt_builder.new_shared_blob(blob.id.into()).await?;
                }
            }
        }

        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;

        if !res.errors.is_empty() {
            return Err(anyhow!("could not certify blob: {:?}", res.errors).into());
        }

        if post_store != PostStoreAction::Share {
            return Ok(HashMap::new());
        }

        // If the blobs are shared, create a mapping blob ID -> shared_blob_object_id.
        let object_ids = get_created_sui_object_ids_by_type(
            &res,
            &contracts::shared_blob::SharedBlob
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
        )?;
        ensure!(
            object_ids.len() == blobs_with_certificates.len(),
            "unexpected number of shared blob objects created: {} (expected {})",
            object_ids.len(),
            blobs_with_certificates.len()
        );

        // If there is only one blob, we can directly return the mapping.
        if object_ids.len() == 1 {
            Ok(HashMap::from([(
                blobs_with_certificates[0].0.blob_id,
                object_ids[0],
            )]))
        } else {
            // Fetch all SharedBlob objects and collect them as a mapping blob id
            // to shared blob object id.
            let shared_blobs = self
                .sui_client()
                .get_sui_objects::<SharedBlob>(object_ids)
                .await?;
            Ok(shared_blobs
                .into_iter()
                .map(|shared_blob| (shared_blob.blob.blob_id, shared_blob.id))
                .collect())
        }
    }

    /// Certifies the specified event blob on Sui, with the given metadata and epoch.
    pub async fn certify_event_blob(
        &self,
        blob_metadata: BlobObjectMetadata,
        ending_checkpoint_seq_num: u64,
        epoch: u32,
    ) -> SuiClientResult<()> {
        let node_capability = self
            .read_client
            .get_address_capability_object(self.wallet_address)
            .await?
            .ok_or(SuiClientError::StorageNodeCapabilityObjectNotSet)?;

        tracing::debug!(
            storage_node_cap = %node_capability.node_id,
            "calling certify_event_blob"
        );

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .certify_event_blob(
                blob_metadata,
                node_capability.id.into(),
                ending_checkpoint_seq_num,
                epoch,
            )
            .await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    pub async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder.invalidate_blob_id(certificate).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Registers a candidate node.
    pub async fn register_candidate(
        &self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> SuiClientResult<StorageNodeCap> {
        // Ensure that a storage capability object does not already exist for the given address.
        // This is enforced to guarantee that there is only one capability object associated with
        // each address. With this invariant, we don't need to persist the node ID or capability
        // object ID separately in the storage node. If needed, we can simply query the capability
        // object linked to the address.
        //
        // However, the test-and-set operation in this function is susceptible to a race condition.
        // If two instances of this function run concurrently (may not be in the same process), both
        // could potentially pass the capability object check  and attempt to register as a
        // candidate. Ideally, this enforcement should be handled within the contract itself.
        // However, in practice, this race condition is unlikely to occur, as each node registers
        // only once during its lifetime, typically under human supervision by the node operator.
        //
        // TODO(#928): revisit this choice after mainnet to see if this causes inconvenience for
        // node operators.
        let existing_capability_object = self
            .read_client
            .get_address_capability_object(self.wallet_address)
            .await?;

        if let Some(cap) = existing_capability_object {
            return Err(SuiClientError::CapabilityObjectAlreadyExists(cap.id));
        }

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .register_candidate(node_parameters, proof_of_possession)
            .await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
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
        &self,
        registration_params_with_stake_amounts: Vec<(
            NodeRegistrationParams,
            ProofOfPossession,
            SuiAddress,
        )>,
    ) -> SuiClientResult<Vec<StorageNodeCap>> {
        let count = registration_params_with_stake_amounts.len();

        let mut pt_builder = self.transaction_builder();
        for (node_parameters, proof_of_possession, address) in
            registration_params_with_stake_amounts.into_iter()
        {
            let cap = pt_builder
                .register_candidate(&node_parameters, proof_of_possession)
                .await?;
            pt_builder.transfer(Some(address), vec![cap.into()]).await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let wallet = self.wallet().await;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;

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

        self.sui_client().get_sui_objects(cap_ids).await
    }

    /// For each entry in `node_ids_with_amounts`, stakes the amount of WAL specified by the second
    /// element of the pair with the node represented by the first element of the pair in a single
    /// PTB.
    pub async fn stake_with_pools(
        &self,
        node_ids_with_amounts: &[(ObjectID, u64)],
    ) -> SuiClientResult<Vec<StakedWal>> {
        let count = node_ids_with_amounts.len();
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        for (node_id, amount) in node_ids_with_amounts.iter() {
            pt_builder.stake_with_pool(*amount, *node_id).await?;
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;

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

        self.sui_client().get_sui_objects(staked_wal).await
    }

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    pub async fn voting_end(&self) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder.voting_end().await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    pub async fn initiate_epoch_change(&self) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        pt_builder.initiate_epoch_change().await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Call to notify the contract that this node is done syncing the specified epoch.
    pub async fn epoch_sync_done(&self, epoch: Epoch) -> SuiClientResult<()> {
        let node_capability = self
            .read_client
            .get_address_capability_object(self.wallet_address)
            .await?
            .ok_or(SuiClientError::StorageNodeCapabilityObjectNotSet)?;

        if node_capability.last_epoch_sync_done >= epoch {
            return Err(SuiClientError::LatestAttestedIsMoreRecent);
        }

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        tracing::debug!(
            storage_node_cap = %node_capability.node_id,
            "calling epoch_sync_done"
        );

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .epoch_sync_done(node_capability.id.into(), epoch)
            .await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Sets the commission receiver for the node.
    pub async fn set_commission_receiver(
        &self,
        node_id: ObjectID,
        receiver: Authorized,
    ) -> SuiClientResult<()> {
        self.set_authorized_for_pool(
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
        self.set_authorized_for_pool(
            node_id,
            PoolOperationWithAuthorization::Governance,
            authorized,
        )
        .await
    }

    async fn set_authorized_for_pool(
        &self,
        node_id: ObjectID,
        operation: PoolOperationWithAuthorization,
        authorized: Authorized,
    ) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        let authenticated_arg = self
            .get_authenticated_arg_for_pool(&mut pt_builder, node_id, operation)
            .await?;
        let authorized_arg = pt_builder.authorized_address_or_object(authorized)?;
        match operation {
            PoolOperationWithAuthorization::Commission => {
                pt_builder
                    .set_commission_receiver(node_id, authenticated_arg, authorized_arg)
                    .await?;
            }
            PoolOperationWithAuthorization::Governance => {
                pt_builder
                    .set_governance_authorized(node_id, authenticated_arg, authorized_arg)
                    .await?;
            }
        }
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Given the node ID, checks if the sender is authorized to perform the operation (either as
    /// sender or by owning the corresponding object) and returns an `Authenticated` Move type as
    /// result argument.
    async fn get_authenticated_arg_for_pool(
        &self,
        pt_builder: &mut WalrusPtbBuilder,
        node_id: ObjectID,
        operation: PoolOperationWithAuthorization,
    ) -> SuiClientResult<Argument> {
        let pool = self.read_client.get_staking_pool(node_id).await?;
        let authorized = match operation {
            PoolOperationWithAuthorization::Commission => pool.commission_receiver,
            PoolOperationWithAuthorization::Governance => pool.governance_authorized,
        };
        match authorized {
            Authorized::Address(receiver) => {
                ensure!(
                    receiver == self.wallet_address,
                    SuiClientError::NotAuthorizedForPool(node_id)
                );
                pt_builder.authenticate_sender()
            }
            Authorized::Object(receiver) => {
                let object = self
                    .sui_client()
                    .get_object_with_options(receiver, SuiObjectDataOptions::default().with_owner())
                    .await?;
                ensure!(
                    object
                        .owner()
                        .ok_or_else(|| anyhow!("no object owner returned from rpc"))?
                        .get_owner_address()?
                        == self.wallet_address,
                    SuiClientError::NotAuthorizedForPool(node_id)
                );
                pt_builder.authenticate_with_object(receiver).await
            }
        }
    }

    /// Creates a new [`contracts::wal_exchange::Exchange`] with a 1:1 exchange rate, funds it with
    /// `amount` FROST, and returns its object ID.
    pub async fn create_and_fund_exchange(
        &self,
        exchange_package: ObjectID,
        amount: u64,
    ) -> SuiClientResult<ObjectID> {
        tracing::info!("creating a new SUI/WAL exchange");

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let mut pt_builder = self.transaction_builder();
        pt_builder
            .create_and_fund_exchange(exchange_package, amount)
            .await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
        let exchange_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::wal_exchange::Exchange
                .to_move_struct_tag_with_type_map(&self.read_client.type_origin_map(), &[])?,
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
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        tracing::debug!(amount, "exchanging SUI/MIST for WAL/FROST");

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        pt_builder.exchange_sui_for_wal(exchange_id, amount).await?;
        let (ptb, sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, Some(self.gas_budget + sui_cost))
            .await?;
        Ok(())
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
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        pt_builder.delete_blob(blob_object_id.into()).await?;
        let (ptb, _sui_cost) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Returns a new [`WalrusPtbBuilder`] for the client.
    pub fn transaction_builder(&self) -> WalrusPtbBuilder {
        WalrusPtbBuilder::new(self.read_client.clone(), self.wallet_address)
    }

    /// Signs and sends a programmable transaction.
    // TODO(giac): Currently we pass the wallet as an argument to ensure that the caller can lock
    // before taking the object references. This ensures that no race conditions occur. We could
    // consider a more ergonomic approach, where this function takes `&mut self`, and the whole
    // client needs to be locked. (#1023).
    pub async fn sign_and_send_ptb(
        &self,
        wallet: &WalletContext,
        programmable_transaction: ProgrammableTransaction,
        min_gas_coin_balance: Option<u64>,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        let response = sign_and_send_ptb(
            self.wallet_address,
            wallet,
            programmable_transaction,
            self.get_compatible_gas_coins(min_gas_coin_balance).await?,
            self.gas_budget,
        )
        .await?;
        match response
            .effects
            .as_ref()
            .ok_or_else(|| anyhow!("No transaction effects in response"))?
            .status()
        {
            SuiExecutionStatus::Success => Ok(response),
            SuiExecutionStatus::Failure { error } => Err(
                SuiClientError::TransactionExecutionError(error.as_str().into()),
            ),
        }
    }

    async fn get_compatible_gas_coins(
        &self,
        min_balance: Option<u64>,
    ) -> SuiClientResult<Vec<ObjectRef>> {
        Ok(self
            .read_client
            .get_coins_with_total_balance(
                self.wallet_address,
                CoinType::Sui,
                min_balance.unwrap_or(self.gas_budget),
                vec![],
            )
            .await?
            .iter()
            .map(Coin::object_ref)
            .collect())
    }

    /// Merges the WAL and SUI coins owned by the wallet of the contract client.
    pub async fn merge_coins(&self) -> SuiClientResult<()> {
        let wallet = self.wallet().await;
        let mut tx_builder = self.transaction_builder();
        let sui_balance = self.sui_client().get_balance(self.address(), None).await?;
        let wal_balance = self
            .sui_client()
            .get_balance(
                self.address(),
                Some(self.read_client().wal_coin_type().to_owned()),
            )
            .await?;

        if wal_balance.coin_object_count > 1 {
            tx_builder
                .fill_wal_balance(wal_balance.total_balance as u64)
                .await?;
        }

        if sui_balance.coin_object_count > 1 || wal_balance.coin_object_count > 1 {
            self.sign_and_send_ptb(
                &wallet,
                tx_builder.finish().await?.0,
                Some(sui_balance.total_balance as u64),
            )
            .await?;
        }

        Ok(())
    }

    /// Sends the `amount` gas to the provided `address`.
    pub async fn send_sui(&self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        pt_builder.pay_sui(vec![address], vec![amount])?;
        self.sign_and_send_ptb(&wallet, pt_builder.finish(), Some(self.gas_budget + amount))
            .await?;
        Ok(())
    }

    /// Sends the `amount` WAL to the provided `address`.
    pub async fn send_wal(&self, amount: u64, address: SuiAddress) -> SuiClientResult<()> {
        tracing::debug!(%address, "sending WAL to address");
        let mut pt_builder = self.transaction_builder();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;
        pt_builder.pay_wal(address, amount).await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Burns the blob objects with the given object IDs.
    ///
    /// May use multiple PTBs in sequence to burn all the given object IDs.
    pub async fn burn_blobs(&self, blob_object_ids: &[ObjectID]) -> SuiClientResult<()> {
        tracing::debug!(n_blobs = blob_object_ids.len(), "burning blobs");

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        for id_block in blob_object_ids.chunks(MAX_BURNS_PER_PTB) {
            let mut pt_builder = self.transaction_builder();
            for id in id_block {
                pt_builder.burn_blob(id.into()).await?;
            }
            let (ptb, _) = pt_builder.finish().await?;
            self.sign_and_send_ptb(&wallet, ptb, None).await?;
        }

        Ok(())
    }

    /// Funds the shared blob object.
    pub async fn fund_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .fund_shared_blob(shared_blob_obj_id, amount)
            .await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Extends the shared object epoch.
    pub async fn extend_shared_blob(
        &self,
        shared_blob_obj_id: ObjectID,
        epochs_ahead: u32,
    ) -> SuiClientResult<()> {
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();
        pt_builder
            .extend_shared_blob(shared_blob_obj_id, epochs_ahead)
            .await?;
        let (ptb, _) = pt_builder.finish().await?;
        self.sign_and_send_ptb(&wallet, ptb, None).await?;
        Ok(())
    }

    /// Shares the blob object with the given object ID. If amount is specified, also fund the blob.
    pub async fn share_and_maybe_fund_blob(
        &self,
        blob_obj_id: ObjectID,
        amount: Option<u64>,
    ) -> SuiClientResult<ObjectID> {
        let blob: Blob = self
            .read_client
            .sui_client()
            .get_sui_object(blob_obj_id)
            .await?;
        let wallet = self.wallet().await;
        let mut pt_builder = self.transaction_builder();

        if let Some(amount) = amount {
            ensure!(amount > 0, "must fund with non-zero amount");
            pt_builder
                .new_funded_shared_blob(blob.id.into(), amount)
                .await?;
        } else {
            pt_builder.new_shared_blob(blob.id.into()).await?;
        }

        let (ptb, _) = pt_builder.finish().await?;
        let res = self.sign_and_send_ptb(&wallet, ptb, None).await?;
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
