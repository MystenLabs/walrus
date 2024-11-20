// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use core::{fmt, str::FromStr};
use std::{future::Future, time::Duration};

use anyhow::{anyhow, Context, Result};
use fastcrypto::traits::ToFromBytes;
use sui_sdk::{
    rpc_types::{
        Coin,
        SuiExecutionStatus,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
    },
    types::{
        base_types::{ObjectID, ObjectRef},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::CallArg,
    },
    wallet_context::WalletContext,
    SuiClient,
};
use sui_types::{
    base_types::SuiAddress,
    event::EventID,
    transaction::{Argument, Command, ProgrammableTransaction},
    Identifier,
    SUI_CLOCK_OBJECT_ID,
    SUI_CLOCK_OBJECT_SHARED_VERSION,
};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use walrus_core::{
    ensure,
    merkle::DIGEST_LEN,
    messages::{ConfirmationCertificate, InvalidBlobCertificate, ProofOfPossession},
    metadata::BlobMetadataWithId,
    BlobId,
    EncodingType,
    Epoch,
    EpochCount,
};

use crate::{
    contracts::{self, FunctionTag},
    types::{
        move_structs::EpochState,
        Blob,
        BlobEvent,
        Committee,
        ContractEvent,
        NodeRegistrationParams,
        StakedWal,
        StorageNodeCap,
        StorageResource,
    },
    utils::{
        get_address_capability_object,
        get_created_sui_object_ids_by_type,
        get_owned_objects,
        get_sui_object,
        price_for_encoded_length,
        sign_and_send_ptb,
        write_price_for_encoded_length,
    },
};

mod read_client;
pub use read_client::{
    get_system_package_id,
    CommitteesAndState,
    FixedSystemParameters,
    ReadClient,
    SuiReadClient,
};

const CLOCK_CALL_ARG: CallArg = CallArg::Object(sui_types::transaction::ObjectArg::SharedObject {
    id: SUI_CLOCK_OBJECT_ID,
    initial_shared_version: SUI_CLOCK_OBJECT_SHARED_VERSION,
    mutable: false,
});

#[derive(Debug, thiserror::Error)]
/// Error returned by the [`SuiContractClient`] and the [`SuiReadClient`].
pub enum SuiClientError {
    /// Unexpected internal errors.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    /// Error resulting from a Sui-SDK call.
    #[error(transparent)]
    SuiSdkError(#[from] sui_sdk::error::Error),
    /// Error in a transaction execution.
    #[error("transaction execution failed: {0}")]
    TransactionExecutionError(String),
    /// No matching WAL coin found for the transaction.
    #[error("could not find a WAL coin with sufficient balance")]
    NoCompatibleWalCoin,
    /// No matching gas coin found for the transaction.
    #[error("could not find gas coins with sufficient balance: {0}")]
    NoCompatibleGasCoins(anyhow::Error),
    /// The Walrus system object does not exist.
    #[error(
        "the specified Walrus system object {0} does not exist or is incompatible with this binary;\
        \nmake sure you have the latest binary and configuration, and the correct Sui network is \
        activated in your Sui wallet"
    )]
    WalrusSystemObjectDoesNotExist(ObjectID),
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
    CapabilityObjectAlreadyExists(StorageNodeCap),
}

/// Represents the persistence state of a blob on Walrus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobPersistence {
    /// The blob cannot be deleted.
    Permanent,
    /// The blob is deletable.
    Deletable,
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

/// Result alias for functions returning a `SuiClientError`.
pub type SuiClientResult<T> = Result<T, SuiClientError>;

/// Trait for interactions with the Walrus contracts.
pub trait ContractClient: ReadClient + Send + Sync {
    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> impl Future<Output = SuiClientResult<StorageResource>> + Send;

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    ///
    /// `blob_size` is the size of the unencoded blob. The encoded size of the blob must be
    /// less than or equal to the size reserved in `storage`.
    fn register_blob(
        &self,
        storage: &StorageResource,
        blob_id: BlobId,
        root_digest: [u8; DIGEST_LEN],
        blob_size: u64,
        encoding_type: EncodingType,
        persistence: BlobPersistence,
    ) -> impl Future<Output = SuiClientResult<Blob>> + Send;

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blob`][Self::register_blob] functions in one atomic transaction.
    fn reserve_and_register_blob<const V: bool>(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata: &BlobMetadataWithId<V>,
        persistence: BlobPersistence,
    ) -> impl Future<Output = SuiClientResult<Blob>> + Send;

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    // NB: This intentionally takes an owned `Blob` object even though it is not required, as the
    // corresponding object on Sui will be changed in the process.
    fn certify_blob(
        &self,
        blob: Blob,
        certificate: &ConfirmationCertificate,
    ) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Returns a compatible [`ReadClient`].
    fn read_client(&self) -> &impl ReadClient;

    /// Returns the list of [`Blob`] objects owned by the wallet currently in use.
    fn owned_blobs(
        &self,
        include_expired: bool,
    ) -> impl Future<Output = SuiClientResult<Vec<Blob>>> + Send;

    /// Returns the list of [`StorageResource`] objects owned by the wallet currently in use.
    fn owned_storage(
        &self,
        include_expired: bool,
    ) -> impl Future<Output = SuiClientResult<Vec<StorageResource>>> + Send;

    /// Returns the closest-matching owned storage resources for given size and number of epochs.
    ///
    /// Among all the owned [`StorageResource`] objects, returns the one that:
    /// - has the closest size to `storage_size`; and
    /// - breaks ties by taking the one with the smallest end epoch that is greater or equal to the
    ///   requested `end_epoch`.
    ///
    /// Returns `None` if no matching storage resource is found.
    fn owned_storage_for_size_and_epoch(
        &self,
        storage_size: u64,
        end_epoch: Epoch,
    ) -> impl Future<Output = SuiClientResult<Option<StorageResource>>> + Send;

    /// Registers a candidate node.
    fn register_candidate(
        &self,
        node_parameters: &NodeRegistrationParams,
        proof_of_possession: ProofOfPossession,
    ) -> impl Future<Output = SuiClientResult<StorageNodeCap>> + Send;

    /// Stakes the given amount with the pool of node with `node_id`.
    fn stake_with_pool(
        &self,
        amount_to_stake: u64,
        node_id: ObjectID,
    ) -> impl Future<Output = SuiClientResult<StakedWal>> + Send;

    /// Call to end voting and finalize the next epoch parameters.
    ///
    /// Can be called once the voting period is over.
    fn voting_end(&self) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Call to initialize the epoch change.
    ///
    /// Can be called once the epoch duration is over.
    fn initiate_epoch_change(&self) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Call to notify the contract that this node is done syncing the specified epoch.
    fn epoch_sync_done(&self, epoch: Epoch) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Deletes the owned blob with the specified Sui object ID, returning the storage resource.
    fn delete_blob(
        &self,
        blob_object_id: ObjectID,
    ) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Creates a new [`contracts::wal_exchange::Exchange`] with a 1:1 exchange rate, funds it with
    /// `amount` FROST, and returns its object ID.
    fn create_and_fund_exchange(
        &self,
        amount: u64,
    ) -> impl Future<Output = SuiClientResult<ObjectID>>;

    /// Exchanges the given `amount` of SUI (in MIST) for WAL using the shared exchange.
    fn exchange_sui_for_wal(
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> impl Future<Output = SuiClientResult<()>>;
}

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
        system_object: ObjectID,
        staking_object: ObjectID,
        gas_budget: u64,
    ) -> SuiClientResult<Self> {
        let read_client =
            SuiReadClient::new(wallet.get_client().await?, system_object, staking_object).await?;
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

    /// Executes the move call to `function` with `call_args` and transfers all outputs
    /// (if any) to the sender.
    // TODO(giac): Currently we pass the wallet as an argument to ensure that the caller can lock
    // before taking the object references. This ensures that no race conditions occur. We could
    // consider a more ergonomic approach, where this function takes `&mut self`, and the whole
    // client needs to be locked. (#1023).
    #[tracing::instrument(err, skip(self, wallet))]
    async fn move_call_and_transfer<'a>(
        &self,
        wallet: &WalletContext,
        function: FunctionTag<'a>,
        call_args: Vec<CallArg>,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();
        let arguments = call_args
            .iter()
            .map(|arg| pt_builder.input(arg.to_owned()))
            .collect::<Result<Vec<_>>>()?;
        let n_object_outputs = function.n_object_outputs;
        let result_index = self.add_move_call_to_ptb(&mut pt_builder, function, arguments)?;
        for i in 0..n_object_outputs {
            pt_builder.transfer_arg(self.wallet_address, Argument::NestedResult(result_index, i));
        }

        self.sign_and_send_ptb(wallet, pt_builder.finish(), None)
            .await
    }

    /// Returns a reference to the inner wallet context.
    pub async fn wallet(&self) -> tokio::sync::MutexGuard<'_, WalletContext> {
        self.wallet.lock().await
    }

    fn add_move_call_to_ptb(
        &self,
        pt_builder: &mut ProgrammableTransactionBuilder,
        function: FunctionTag<'_>,
        arguments: Vec<Argument>,
    ) -> SuiClientResult<u16> {
        let Argument::Result(result_index) = pt_builder.programmable_move_call(
            self.read_client.system_pkg_id,
            Identifier::from_str(function.module)?,
            Identifier::from_str(function.name)?,
            function.type_params,
            arguments,
        ) else {
            unreachable!("the result of `programmable_move_call` is always an Argument::Result");
        };
        Ok(result_index)
    }

    async fn get_compatible_gas_coins(
        &self,
        min_balance: Option<u64>,
    ) -> SuiClientResult<Vec<ObjectRef>> {
        Ok(self
            .read_client
            .get_coins_with_total_balance(
                self.wallet_address,
                None,
                min_balance.unwrap_or(self.gas_budget),
            )
            .await
            .map_err(SuiClientError::NoCompatibleGasCoins)?
            .iter()
            .map(Coin::object_ref)
            .collect())
    }

    /// Sign and send a programmable transaction.
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
            SuiExecutionStatus::Failure { error } => {
                Err(SuiClientError::TransactionExecutionError(error.into()))
            }
        }
    }

    /// Returns the active address of the client.
    pub fn address(&self) -> SuiAddress {
        self.wallet_address
    }

    async fn storage_price_for_encoded_length(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<u64> {
        Ok(price_for_encoded_length(
            encoded_size,
            self.read_client.storage_price_per_unit_size().await?,
            epochs_ahead,
        ))
    }

    async fn write_price_for_encoded_length(&self, encoded_size: u64) -> SuiClientResult<u64> {
        Ok(write_price_for_encoded_length(
            encoded_size,
            self.read_client.write_price_per_unit_size().await?,
        ))
    }

    /// Returns a WAL coin with a balance of at least `min_balance`.
    ///
    /// # Errors
    ///
    /// Returns a [`SuiClientError::NoCompatibleWalCoin`] if no WAL coin with sufficient balance can
    /// be found.
    pub async fn get_wal_coin(&self, min_balance: u64) -> SuiClientResult<Coin> {
        tracing::debug!(coin_type = ?self.read_client.coin_type());
        self.read_client
            .get_coin_with_balance(
                self.wallet_address,
                Some(self.read_client.coin_type()),
                min_balance,
                Default::default(),
            )
            .await
            .map_err(|_| SuiClientError::NoCompatibleWalCoin)
    }

    /// Adds a WAL coin with a balance of at least `min_balance` as an input to the PTB and returns
    /// the corresponding [`Argument`].
    ///
    /// # Errors
    ///
    /// Returns a [`SuiClientError::NoCompatibleWalCoin`] if no WAL coin with sufficient balance can
    /// be found.
    async fn add_wal_coin_to_ptb(
        &self,
        pt_builder: &mut ProgrammableTransactionBuilder,
        min_balance: u64,
    ) -> anyhow::Result<Argument> {
        pt_builder.input(self.get_wal_coin(min_balance).await?.object_ref().into())
    }

    /// Adds a call to `reserve_space` to the `pt_builder` and returns the result index.
    pub async fn add_reserve_transaction(
        &self,
        pt_builder: &mut ProgrammableTransactionBuilder,
        encoded_size: u64,
        epochs_ahead: EpochCount,
        payment_coin: Option<Argument>,
    ) -> SuiClientResult<u16> {
        let system_object_arg = self.read_client.call_arg_from_system_obj(true).await?;

        let payment_coin_arg = match payment_coin {
            Some(arg) => arg,
            None => {
                let price = self
                    .storage_price_for_encoded_length(encoded_size, epochs_ahead)
                    .await?;
                self.add_wal_coin_to_ptb(pt_builder, price).await?
            }
        };

        let reserve_arguments = vec![
            pt_builder.input(system_object_arg.clone())?,
            pt_builder.input(encoded_size.into())?,
            pt_builder.input(epochs_ahead.into())?,
            payment_coin_arg,
        ];
        let reserve_result_index = self.add_move_call_to_ptb(
            pt_builder,
            contracts::system::reserve_space,
            reserve_arguments,
        )?;

        Ok(reserve_result_index)
    }

    /// Returns the gas budget used by the client.
    pub fn gas_budget(&self) -> u64 {
        self.gas_budget
    }

    /// Gets the [`SuiClient`] from the associated read client.
    pub fn sui_client(&self) -> &SuiClient {
        &self.read_client.sui_client
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
}

impl ContractClient for SuiContractClient {
    async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: EpochCount,
    ) -> SuiClientResult<StorageResource> {
        tracing::debug!(encoded_size, "starting to reserve storage for blob");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let reserve_result_index = self
            .add_reserve_transaction(&mut pt_builder, encoded_size, epochs_ahead, None)
            .await?;
        // Transfer the created storage resource.
        pt_builder.transfer_arg(
            self.wallet_address,
            Argument::NestedResult(reserve_result_index, 0),
        );

        let res = self
            .sign_and_send_ptb(&wallet, pt_builder.finish(), None)
            .await?;
        let storage_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );
        get_sui_object(&self.read_client.sui_client, storage_id[0]).await
    }

    async fn register_blob(
        &self,
        storage: &StorageResource,
        blob_id: BlobId,
        root_digest: [u8; DIGEST_LEN],
        blob_size: u64,
        encoding_type: EncodingType,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Blob> {
        let price = self
            .write_price_for_encoded_length(storage.storage_size)
            .await?;

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let res = self
            .move_call_and_transfer(
                &wallet,
                contracts::system::register_blob,
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.read_client.get_object_ref(storage.id).await?.into(),
                    call_arg_pure!(&blob_id),
                    call_arg_pure!(&root_digest),
                    blob_size.into(),
                    u8::from(encoding_type).into(),
                    persistence.is_deletable().into(),
                    self.get_wal_coin(price).await?.object_ref().into(),
                ],
            )
            .await?;
        let blob_obj_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob.to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;
        ensure!(
            blob_obj_id.len() == 1,
            "unexpected number of blob objects created: {}",
            blob_obj_id.len()
        );

        get_sui_object(&self.read_client.sui_client, blob_obj_id[0]).await
    }

    async fn reserve_and_register_blob<const V: bool>(
        &self,
        epochs_ahead: EpochCount,
        blob_metadata: &BlobMetadataWithId<V>,
        persistence: BlobPersistence,
    ) -> SuiClientResult<Blob> {
        let encoded_size = blob_metadata
            .metadata()
            .encoded_size()
            .context("cannot compute encoded size")?;
        tracing::debug!(encoded_size, "starting to reserve and register blob");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let price = self
            .storage_price_for_encoded_length(encoded_size, epochs_ahead)
            .await?
            + self.write_price_for_encoded_length(encoded_size).await?;
        let payment_coin = self.add_wal_coin_to_ptb(&mut pt_builder, price).await?;

        let reserve_result_index = self
            .add_reserve_transaction(
                &mut pt_builder,
                encoded_size,
                epochs_ahead,
                Some(payment_coin),
            )
            .await?;

        let register_arguments = vec![
            pt_builder.input(self.read_client.call_arg_from_system_obj(true).await?)?,
            Argument::NestedResult(reserve_result_index, 0),
            pt_builder.input(call_arg_pure!(blob_metadata.blob_id()))?,
            pt_builder.input(call_arg_pure!(&blob_metadata
                .metadata()
                .compute_root_hash()
                .bytes()))?,
            pt_builder.input(blob_metadata.metadata().unencoded_length.into())?,
            pt_builder.input(u8::from(blob_metadata.metadata().encoding_type).into())?,
            pt_builder.input(persistence.is_deletable().into())?,
            payment_coin,
        ];
        let register_result_index = self.add_move_call_to_ptb(
            &mut pt_builder,
            contracts::system::register_blob,
            register_arguments,
        )?;
        for i in 0..contracts::system::register_blob.n_object_outputs {
            pt_builder.transfer_arg(
                self.wallet_address,
                Argument::NestedResult(register_result_index, i),
            );
        }

        let res = self
            .sign_and_send_ptb(&wallet, pt_builder.finish(), None)
            .await?;
        let blob_obj_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::blob::Blob.to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;
        ensure!(
            blob_obj_id.len() == 1,
            "unexpected number of blob objects created: {}",
            blob_obj_id.len()
        );

        get_sui_object(&self.read_client.sui_client, blob_obj_id[0]).await
    }

    async fn certify_blob(
        &self,
        blob: Blob,
        certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<()> {
        // Sort the list of signers, since the move contract requires them to be in
        // ascending order (see `walrus::system::bls_aggregate::verify_certificate`)
        let mut signers = certificate.signers.clone();
        signers.sort_unstable();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let res = self
            .move_call_and_transfer(
                &wallet,
                contracts::system::certify_blob,
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.read_client.get_object_ref(blob.id).await?.into(),
                    call_arg_pure!(certificate.signature.as_bytes()),
                    call_arg_pure!(&signers),
                    (&certificate.serialized_message).into(),
                ],
            )
            .await?;
        if res.errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("could not certify blob: {:?}", res.errors).into())
        }
    }

    async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        // Sort the list of signers, since the move contract requires them to be in
        // ascending order (see `walrus::system::bls_aggregate::verify_certificate`)
        let mut signers = certificate.signers.clone();
        signers.sort_unstable();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        self.move_call_and_transfer(
            &wallet,
            contracts::system::invalidate_blob_id,
            vec![
                self.read_client.call_arg_from_system_obj(true).await?,
                call_arg_pure!(certificate.signature.as_bytes()),
                call_arg_pure!(&signers),
                (&certificate.serialized_message).into(),
            ],
        )
        .await?;
        Ok(())
    }

    async fn register_candidate(
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
        let existing_capability_object = get_address_capability_object(
            &self.read_client.sui_client,
            self.wallet_address,
            self.read_client.system_pkg_id,
        )
        .await?;

        if let Some(cap) = existing_capability_object {
            return Err(SuiClientError::CapabilityObjectAlreadyExists(cap));
        }

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let res = self
            .move_call_and_transfer(
                &wallet,
                contracts::staking::register_candidate,
                vec![
                    self.read_client.call_arg_from_staking_obj(true).await?,
                    call_arg_pure!(&node_parameters.name),
                    call_arg_pure!(&node_parameters.network_address.to_string()),
                    call_arg_pure!(node_parameters.public_key.as_bytes()),
                    call_arg_pure!(node_parameters.network_public_key.as_bytes()),
                    call_arg_pure!(proof_of_possession.signature.as_bytes()),
                    node_parameters.commission_rate.into(),
                    node_parameters.storage_price.into(),
                    node_parameters.write_price.into(),
                    node_parameters.node_capacity.into(),
                ],
            )
            .await?;
        let cap_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::storage_node::StorageNodeCap
                .to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;
        ensure!(
            cap_id.len() == 1,
            "unexpected number of StorageNodeCap created: {}",
            cap_id.len()
        );

        get_sui_object(&self.read_client.sui_client, cap_id[0]).await
    }

    async fn stake_with_pool(
        &self,
        amount_to_stake: u64,
        node_id: ObjectID,
    ) -> SuiClientResult<StakedWal> {
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let staking_object_arg = self.read_client.call_arg_from_staking_obj(true).await?;

        let input_coin_arg = self
            .add_wal_coin_to_ptb(&mut pt_builder, amount_to_stake)
            .await?;

        // TODO: split coin, does this work?
        let amount_to_stake_arg = pt_builder.pure(amount_to_stake)?;
        let stake_coin_arg = pt_builder.command(Command::SplitCoins(
            input_coin_arg,
            vec![amount_to_stake_arg],
        ));

        let stake_arguments = vec![
            pt_builder.input(staking_object_arg.clone())?,
            stake_coin_arg,
            pt_builder.pure(node_id)?,
        ];
        let stake_result_index = self.add_move_call_to_ptb(
            &mut pt_builder,
            contracts::staking::stake_with_pool,
            stake_arguments,
        )?;

        // Transfer the created StakedWal object.
        pt_builder.transfer_arg(
            self.wallet_address,
            Argument::NestedResult(stake_result_index, 0),
        );

        let res = self
            .sign_and_send_ptb(&wallet, pt_builder.finish(), None)
            .await?;

        let staked_wal = get_created_sui_object_ids_by_type(
            &res,
            &contracts::staked_wal::StakedWal
                .to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;
        ensure!(
            staked_wal.len() == 1,
            "unexpected number of StakedWal objects created: {}",
            staked_wal.len()
        );

        get_sui_object(&self.read_client.sui_client, staked_wal[0]).await
    }

    async fn voting_end(&self) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        self.move_call_and_transfer(
            &wallet,
            contracts::staking::voting_end,
            vec![
                self.read_client.call_arg_from_staking_obj(true).await?,
                CLOCK_CALL_ARG,
            ],
        )
        .await?;
        Ok(())
    }

    async fn initiate_epoch_change(&self) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        self.move_call_and_transfer(
            &wallet,
            contracts::staking::initiate_epoch_change,
            vec![
                self.read_client.call_arg_from_staking_obj(true).await?,
                self.read_client.call_arg_from_system_obj(true).await?,
                CLOCK_CALL_ARG,
            ],
        )
        .await?;
        Ok(())
    }

    async fn epoch_sync_done(&self, epoch: Epoch) -> SuiClientResult<()> {
        let node_capability = get_address_capability_object(
            &self.read_client.sui_client,
            self.wallet_address,
            self.read_client.system_pkg_id,
        )
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
        let cap_obj_ref = wallet.get_object_ref(node_capability.id).await?;

        self.move_call_and_transfer(
            &wallet,
            contracts::staking::epoch_sync_done,
            vec![
                self.read_client.call_arg_from_staking_obj(true).await?,
                cap_obj_ref.into(),
                epoch.into(),
                CLOCK_CALL_ARG,
            ],
        )
        .await?;
        Ok(())
    }

    async fn create_and_fund_exchange(&self, amount: u64) -> SuiClientResult<ObjectID> {
        tracing::info!("creating a new SUI/WAL exchange");

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let res = self
            .move_call_and_transfer(
                &wallet,
                contracts::wal_exchange::new_funded,
                vec![
                    self.get_wal_coin(amount).await?.object_ref().into(),
                    call_arg_pure!(&amount),
                ],
            )
            .await?;

        let exchange_id = get_created_sui_object_ids_by_type(
            &res,
            &contracts::wal_exchange::Exchange
                .to_move_struct_tag(self.read_client.system_pkg_id, &[])?,
        )?;
        ensure!(
            exchange_id.len() == 1,
            "unexpected number of `Exchange`s created: {}",
            exchange_id.len()
        );
        Ok(exchange_id[0])
    }

    async fn exchange_sui_for_wal(
        &self,
        exchange_id: ObjectID,
        amount: u64,
    ) -> SuiClientResult<()> {
        tracing::debug!("exchanging {amount} MIST for WAL/FROST");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        let amount_argument = pt_builder.input(call_arg_pure!(&amount))?;
        let Argument::Result(split_result_index) = pt_builder.command(Command::SplitCoins(
            Argument::GasCoin,
            vec![amount_argument],
        )) else {
            unreachable!("this always returns an `Argument::Result`")
        };
        let sui_coin_arg = Argument::NestedResult(split_result_index, 0);
        let exchange_arg = pt_builder.input(
            self.read_client
                .call_arg_for_shared_obj(exchange_id, true)
                .await?,
        )?;

        let result_index = self.add_move_call_to_ptb(
            &mut pt_builder,
            contracts::wal_exchange::exchange_all_for_wal,
            vec![exchange_arg, sui_coin_arg],
        )?;
        pt_builder.transfer_arg(self.wallet_address, Argument::NestedResult(result_index, 0));

        self.sign_and_send_ptb(&wallet, pt_builder.finish(), Some(self.gas_budget + amount))
            .await?;

        Ok(())
    }

    fn read_client(&self) -> &impl ReadClient {
        &self.read_client
    }

    async fn owned_blobs(&self, include_expired: bool) -> SuiClientResult<Vec<Blob>> {
        let current_epoch = self.read_client.current_committee().await?.epoch;
        Ok(get_owned_objects::<Blob>(
            &self.read_client.sui_client,
            self.wallet_address,
            self.read_client.system_pkg_id,
            &[],
        )
        .await?
        .filter(|blob| include_expired || blob.storage.end_epoch > current_epoch)
        .collect())
    }

    async fn owned_storage(&self, include_expired: bool) -> SuiClientResult<Vec<StorageResource>> {
        let current_epoch = self.read_client.current_committee().await?.epoch;
        Ok(get_owned_objects::<StorageResource>(
            &self.read_client.sui_client,
            self.wallet_address,
            self.read_client.system_pkg_id,
            &[],
        )
        .await?
        .filter(|storage| include_expired || storage.end_epoch > current_epoch)
        .collect())
    }

    async fn owned_storage_for_size_and_epoch(
        &self,
        storage_size: u64,
        end_epoch: Epoch,
    ) -> SuiClientResult<Option<StorageResource>> {
        Ok(self
            .owned_storage(false)
            .await?
            .into_iter()
            .filter(|storage| {
                storage.storage_size >= storage_size && storage.end_epoch >= end_epoch
            })
            // Pick the smallest storage size. Break ties by comparing the end epoch, and take the
            // one that is the closest to `end_epoch`. NOTE: we are already sure that these values
            // are above the minimum.
            .min_by_key(|a| (a.storage_size, a.end_epoch)))
    }

    async fn delete_blob(&self, blob_object_id: ObjectID) -> SuiClientResult<()> {
        // Lock the wallet here to ensure there are no race conditions with object references.
        let wallet = self.wallet().await;

        self.move_call_and_transfer(
            &wallet,
            contracts::system::delete_blob,
            vec![
                self.read_client.call_arg_from_system_obj(true).await?,
                self.read_client
                    .get_object_ref(blob_object_id)
                    .await?
                    .into(),
            ],
        )
        .await?;
        Ok(())
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
