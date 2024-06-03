// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!
use core::{fmt, str::FromStr};
use std::future::Future;

use anyhow::{anyhow, Result};
use fastcrypto::traits::ToFromBytes;
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse},
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
    transaction::{Argument, ProgrammableTransaction},
    Identifier,
};
use walrus_core::{
    ensure,
    merkle::DIGEST_LEN,
    messages::{ConfirmationCertificate, InvalidBlobCertificate},
    BlobId,
    EncodingType,
};

use crate::{
    contracts::{self, FunctionTag},
    types::{Blob, StorageResource},
    utils::{
        call_args_to_object_ids,
        get_created_sui_object_ids_by_type,
        get_sui_object,
        sign_and_send_ptb,
        storage_units_from_size,
    },
};

mod read_client;
pub use read_client::{ReadClient, SuiReadClient};

#[derive(Debug, thiserror::Error)]
/// Error returned by the [`SuiContractClient`] and the [`SuiReadClient`].
pub enum SuiClientError {
    #[error(transparent)]
    /// Unexpected internal errors.
    Internal(#[from] anyhow::Error),
    #[error(transparent)]
    /// Error resulting from a Sui-SDK call.
    SuiSdkError(#[from] sui_sdk::error::Error),
    #[error("transaction execution failed: {0}")]
    /// Error in a transaction execution.
    TransactionExecutionError(String),
    #[error("no compatible payment coin found")]
    /// No matching payment coin found for the transaction.
    NoCompatiblePaymentCoin,
    #[error("no compatible gas coin found: {0}")]
    /// No matching gas coin found for the transaction.
    NoCompatibleGasCoin(anyhow::Error),
    /// The Walrus package does not exist.
    #[error("the specified Walrus package {0} does not exist")]
    WalrusPackageDoesNotExist(ObjectID),
    /// The Walrus system object does not exist.
    #[error("the specified Walrus system object {0} does not exist")]
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

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    fn certify_blob(
        &self,
        blob: &Blob,
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
    wallet: WalletContext,
    /// Client to read Walrus on-chain state.
    pub read_client: SuiReadClient,
    wallet_address: SuiAddress,
    gas_budget: u64,
}

impl SuiContractClient {
    /// Constructor for [`SuiContractClient`].
    pub async fn new(
        mut wallet: WalletContext,
        system_pkg: ObjectID,
        system_object: ObjectID,
        gas_budget: u64,
    ) -> SuiClientResult<Self> {
        let sui_client = wallet.get_client().await?;
        let wallet_address = wallet.active_address()?;
        let read_client = SuiReadClient::new(sui_client, system_pkg, system_object).await?;
        Ok(Self {
            wallet,
            read_client,
            wallet_address,
            gas_budget,
        })
    }

    /// Constructor for [`SuiContractClient`] with an existing [`SuiReadClient`].
    pub fn new_with_read_client(
        mut wallet: WalletContext,
        gas_budget: u64,
        read_client: SuiReadClient,
    ) -> SuiClientResult<Self> {
        let wallet_address = wallet.active_address()?;
        Ok(Self {
            wallet,
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
        let Argument::Result(result_index) = pt_builder.programmable_move_call(
            self.read_client.system_pkg_id,
            Identifier::from_str(function.module)?,
            Identifier::from_str(function.name)?,
            function.type_params,
            arguments,
        ) else {
            return Err(anyhow!("Result should be Argument::Result").into());
        };
        for i in 0..function.n_object_outputs {
            pt_builder.transfer_arg(self.wallet_address, Argument::NestedResult(result_index, i));
        }
        let programmable_transaction = pt_builder.finish();
        self.sign_and_send_ptb(
            programmable_transaction,
            self.wallet
                .gas_for_owner_budget(
                    self.wallet_address,
                    self.gas_budget,
                    call_args_to_object_ids(call_args),
                )
                .await
                .map_err(SuiClientError::NoCompatibleGasCoin)?
                .1
                .object_ref(),
        )
        .await
    }

    async fn sign_and_send_ptb(
        &self,
        programmable_transaction: ProgrammableTransaction,
        gas_coin: ObjectRef,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        let response = sign_and_send_ptb(
            self.wallet_address,
            &self.wallet,
            programmable_transaction,
            gas_coin,
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

    /// Returns the wallet context used by the client.
    pub fn wallet(&self) -> &WalletContext {
        &self.wallet
    }
}

impl ContractClient for SuiContractClient {
    async fn reserve_space(
        &self,
        encoded_size: u64,
        epochs_ahead: u64,
    ) -> SuiClientResult<StorageResource> {
        let price = epochs_ahead
            * storage_units_from_size(encoded_size)
            * self.read_client.price_per_unit_size().await?;
        let payment_coin = self
            .read_client
            .get_payment_coins(self.wallet_address)
            .await
            .map_err(|_| SuiClientError::NoCompatiblePaymentCoin)?
            .find(|coin| coin.balance >= price)
            .ok_or_else(|| SuiClientError::NoCompatiblePaymentCoin)?;
        let res = self
            .move_call_and_transfer(
                contracts::system::reserve_space
                    .with_type_params(&[self.read_client.coin_type.clone()]),
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    encoded_size.into(),
                    epochs_ahead.into(),
                    payment_coin.object_ref().into(),
                ],
            )
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
        let erasure_code_type: u8 = erasure_code_type.into();
        let res = self
            .move_call_and_transfer(
                contracts::blob::register.with_type_params(&[self.read_client.coin_type.clone()]),
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.wallet.get_object_ref(storage.id).await?.into(),
                    call_arg_pure!(&blob_id),
                    call_arg_pure!(&root_digest),
                    blob_size.into(),
                    erasure_code_type.into(),
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

    async fn certify_blob(
        &self,
        blob: &Blob,
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
                    self.wallet.get_object_ref(blob.id).await?.into(),
                    call_arg_pure!(certificate.signature.as_bytes()),
                    call_arg_pure!(&signers),
                    (&certificate.serialized_message).into(),
                ],
            )
            .await?;
        let blob: Blob = get_sui_object(&self.read_client.sui_client, blob.id).await?;
        ensure!(
            blob.certified.is_some(),
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
