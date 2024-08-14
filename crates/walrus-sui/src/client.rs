// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use core::{fmt, str::FromStr};
use std::future::Future;

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
};
use sui_types::{
    base_types::SuiAddress,
    event::EventID,
    transaction::{Argument, Command, ProgrammableTransaction},
    Identifier,
};
use tokio::sync::Mutex;
use walrus_core::{
    ensure,
    merkle::DIGEST_LEN,
    messages::{ConfirmationCertificate, InvalidBlobCertificate},
    metadata::BlobMetadataWithId,
    BlobId,
    EncodingType,
};

use crate::{
    contracts::{self, FunctionTag},
    types::{Blob, StorageResource},
    utils::{
        get_created_sui_object_ids_by_type,
        get_sui_object,
        price_for_encoded_length,
        sign_and_send_ptb,
    },
};

mod read_client;
pub use read_client::{ReadClient, SuiReadClient};

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
    /// No matching payment coin found for the transaction.
    #[error("no compatible payment coin found")]
    NoCompatiblePaymentCoin,
    /// No matching gas coin found for the transaction.
    #[error("no compatible gas coins found: {0}")]
    NoCompatibleGasCoins(anyhow::Error),
    /// The Walrus system object does not exist.
    #[error(
        "the specified Walrus system object {0} does not exist; \
        make sure you have the latest configuration and you are using the correct Sui network"
    )]
    WalrusSystemObjectDoesNotExist(ObjectID),
    /// The specified event ID is not associated with a Walrus event.
    #[error("no corresponding blob event found for {0:?}")]
    NoCorrespondingBlobEvent(EventID),
}

/// Result alias for functions returning a `SuiClientError`.
pub type SuiClientResult<T> = Result<T, SuiClientError>;

/// Trait for interactions with the walrus contracts.
pub trait ContractClient: Send + Sync {
    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: u64,
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
        erasure_code_type: EncodingType,
    ) -> impl Future<Output = SuiClientResult<Blob>> + Send;

    /// Purchases blob storage for the next `epochs_ahead` Walrus epochs and uses the resulting
    /// storage resource to register a blob with the provided `blob_metadata`.
    ///
    /// This combines the [`reserve_space`][Self::reserve_space] and
    /// [`register_blob`][Self::register_blob] functions in one atomic transaction.
    fn reserve_and_register_blob<const V: bool>(
        &self,
        epochs_ahead: u64,
        blob_metadata: &BlobMetadataWithId<V>,
    ) -> impl Future<Output = SuiClientResult<Blob>> + Send;

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    fn certify_blob(
        &self,
        blob: Blob,
        certificate: &ConfirmationCertificate,
    ) -> impl Future<Output = SuiClientResult<Blob>> + Send;

    /// Invalidates the specified blob id on Sui, given a certificate that confirms that it is
    /// invalid.
    fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Returns a compatible `ReadClient`.
    fn read_client(&self) -> &impl ReadClient;
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
        gas_budget: u64,
    ) -> SuiClientResult<Self> {
        let read_client = SuiReadClient::new(wallet.get_client().await?, system_object).await?;
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
    #[tracing::instrument(err, skip(self))]
    async fn move_call_and_transfer<'a>(
        &self,
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

        self.sign_and_send_ptb(pt_builder.finish(), None).await
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

    async fn sign_and_send_ptb(
        &self,
        programmable_transaction: ProgrammableTransaction,
        min_gas_coin_balance: Option<u64>,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        let wallet = self.wallet.lock().await;
        let response = sign_and_send_ptb(
            self.wallet_address,
            &wallet,
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

    async fn price_for_encoded_length(
        &self,
        encoded_size: u64,
        epochs_ahead: u64,
    ) -> SuiClientResult<u64> {
        Ok(price_for_encoded_length(
            encoded_size,
            self.read_client.price_per_unit_size().await?,
            epochs_ahead,
        ))
    }

    /// Returns a payment coin with sufficient balance to pay the `price`.
    ///
    /// # Errors
    ///
    /// Returns a [`SuiClientError::NoCompatiblePaymentCoin`] if no payment coin with sufficient
    /// balance can be found or created (by merging coins).
    async fn get_payment_coin(&self, price: u64) -> SuiClientResult<Coin> {
        self.read_client
            .get_coin_with_balance(
                self.wallet_address,
                Some(self.read_client.coin_type.to_canonical_string(true)),
                price,
                Default::default(),
            )
            .await
            .map_err(|_| SuiClientError::NoCompatiblePaymentCoin)
    }

    /// Adds an appropriate `reserve_space` transaction to the `pt_builder` depending on the used
    /// coin type and returns the result index and the minimum balance of compatible gas coins.
    ///
    /// If the payment coin type is SUI, the storage resource is paid for with a coin split off from
    /// the gas coin and adds the necessary transaction to the PTB). Otherwise, a separate coin with
    /// sufficient balance is used.
    async fn add_reserve_transaction(
        &self,
        pt_builder: &mut ProgrammableTransactionBuilder,
        encoded_size: u64,
        epochs_ahead: u64,
    ) -> SuiClientResult<(u16, u64)> {
        let price = self
            .price_for_encoded_length(encoded_size, epochs_ahead)
            .await?;
        let system_object_arg = self.read_client.call_arg_from_system_obj(true).await?;

        let payment_coin_arg = if self.read_client.uses_sui_coin() {
            let price_argument = pt_builder.input(call_arg_pure!(&price))?;
            let Argument::Result(split_result_index) =
                pt_builder.command(Command::SplitCoins(Argument::GasCoin, vec![price_argument]))
            else {
                unreachable!("this always returns an `Argument::Result`")
            };
            Argument::NestedResult(split_result_index, 0)
        } else {
            pt_builder.input(self.get_payment_coin(price).await?.object_ref().into())?
        };

        let mut gas_coin_balance = self.gas_budget;
        if self.read_client.uses_sui_coin() {
            gas_coin_balance += price;
        }

        let reserve_arguments = vec![
            pt_builder.input(system_object_arg.clone())?,
            pt_builder.input(encoded_size.into())?,
            pt_builder.input(epochs_ahead.into())?,
            payment_coin_arg,
        ];
        let reserve_result_index = self.add_move_call_to_ptb(
            pt_builder,
            contracts::system::reserve_space
                .with_type_params(&[self.read_client.coin_type.clone()]),
            reserve_arguments,
        )?;

        let returned_coin = Argument::NestedResult(reserve_result_index, 1);

        if self.read_client.uses_sui_coin() {
            // Join the return coin back into the gas coin. This ensures that there are no
            // zero-balance coins left in the user's wallet.
            pt_builder.command(Command::MergeCoins(Argument::GasCoin, vec![returned_coin]));
        } else {
            // Transfer back payment coin.
            pt_builder.transfer_arg(
                self.wallet_address,
                Argument::NestedResult(reserve_result_index, 1),
            );
        }

        Ok((reserve_result_index, gas_coin_balance))
    }
}

impl ContractClient for SuiContractClient {
    async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: u64,
    ) -> SuiClientResult<StorageResource> {
        tracing::debug!(encoded_size, "starting to reserve storage for blob");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        let (reserve_result_index, min_gas_coin_balance) = self
            .add_reserve_transaction(&mut pt_builder, encoded_size, epochs_ahead)
            .await?;
        // Transfer the created storage resource.
        pt_builder.transfer_arg(
            self.wallet_address,
            Argument::NestedResult(reserve_result_index, 0),
        );

        let res = self
            .sign_and_send_ptb(pt_builder.finish(), Some(min_gas_coin_balance))
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
        erasure_code_type: EncodingType,
    ) -> SuiClientResult<Blob> {
        let res = self
            .move_call_and_transfer(
                contracts::blob::register.with_type_params(&[self.read_client.coin_type.clone()]),
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.read_client.get_object_ref(storage.id).await?.into(),
                    call_arg_pure!(&blob_id),
                    call_arg_pure!(&root_digest),
                    blob_size.into(),
                    u8::from(erasure_code_type).into(),
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
        epochs_ahead: u64,
        blob_metadata: &BlobMetadataWithId<V>,
    ) -> SuiClientResult<Blob> {
        let encoded_size = blob_metadata
            .metadata()
            .encoded_size()
            .context("cannot compute encoded size")?;
        tracing::debug!(encoded_size, "starting to reserve and register blob");
        let mut pt_builder = ProgrammableTransactionBuilder::new();

        let (reserve_result_index, min_gas_coin_balance) = self
            .add_reserve_transaction(&mut pt_builder, encoded_size, epochs_ahead)
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
        ];
        let register_result_index = self.add_move_call_to_ptb(
            &mut pt_builder,
            contracts::blob::register.with_type_params(&[self.read_client.coin_type.clone()]),
            register_arguments,
        )?;
        for i in 0..contracts::blob::register.n_object_outputs {
            pt_builder.transfer_arg(
                self.wallet_address,
                Argument::NestedResult(register_result_index, i),
            );
        }

        let res = self
            .sign_and_send_ptb(pt_builder.finish(), Some(min_gas_coin_balance))
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
    ) -> SuiClientResult<Blob> {
        // Sort the list of signers, since the move contract requires them to be in
        // ascending order (see `blob_store::bls_aggregate::verify_certificate`)
        let mut signers = certificate.signers.clone();
        signers.sort_unstable();
        let res = self
            .move_call_and_transfer(
                contracts::blob::certify.with_type_params(&[self.read_client.coin_type.clone()]),
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.read_client.get_object_ref(blob.id).await?.into(),
                    call_arg_pure!(certificate.signature.as_bytes()),
                    call_arg_pure!(&signers),
                    (&certificate.serialized_message).into(),
                ],
            )
            .await?;
        let blob: Blob = get_sui_object(&self.read_client.sui_client, blob.id).await?;
        ensure!(
            blob.certified_epoch.is_some(),
            "could not certify blob: {:?}",
            res.errors
        );
        Ok(blob)
    }

    async fn invalidate_blob_id(
        &self,
        certificate: &InvalidBlobCertificate,
    ) -> SuiClientResult<()> {
        // Sort the list of signers, since the move contract requires them to be in
        // ascending order (see `blob_store::bls_aggregate::verify_certificate`)
        let mut signers = certificate.signers.clone();
        signers.sort_unstable();
        self.move_call_and_transfer(
            contracts::system::invalidate_blob_id
                .with_type_params(&[self.read_client.coin_type.clone()]),
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

    fn read_client(&self) -> &impl ReadClient {
        &self.read_client
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
