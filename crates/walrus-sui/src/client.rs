// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!
use core::{fmt, str::FromStr};

use anyhow::{anyhow, Result};
use fastcrypto::traits::ToFromBytes;
use sui_sdk::{
    rpc_types::{SuiExecutionStatus, SuiTransactionBlockEffectsAPI, SuiTransactionBlockResponse},
    types::{
        base_types::{ObjectID, ObjectRef},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::{CallArg, TransactionData},
    },
    wallet_context::WalletContext,
};
use sui_types::{
    base_types::SuiAddress,
    transaction::{Argument, ProgrammableTransaction},
    Identifier,
};
use walrus_core::{messages::ConfirmationCertificate, BlobId, EncodingType};

use crate::{
    contracts::{self, FunctionTag},
    types::{Blob, StorageResource},
    utils::{call_args_to_object_ids, get_created_object_ids_by_type},
};

mod read_client;
pub use read_client::{MockSuiReadClient, ReadClient, SuiReadClient};

#[derive(Debug, thiserror::Error)]
/// Error returned by the [`SuiContractClient`] and the [`SuiReadClient`]
pub enum SuiClientError {
    #[error(transparent)]
    /// Unexpected internal errors
    Internal(#[from] anyhow::Error),
    #[error(transparent)]
    /// Error resulting from a sui sdk call
    SuiSdkError(#[from] sui_sdk::error::Error),
    #[error("transaction execution failed: {0}")]
    /// Error in a transaction execution
    TransactionExecutionError(String),
    #[error("no compatible payment coin found")]
    /// No matching payment coin found for the transaction
    NoCompatiblePaymentCoin,
    #[error("no compatible gas coin found: {0:?}")]
    /// No matching gas coin found for the transaction
    NoCompatibleGasCoin(anyhow::Error),
}

type SuiClientResult<T> = Result<T, SuiClientError>;

/// Client implementation for interacting with the Walrus smart contracts
pub struct SuiContractClient {
    wallet: WalletContext,
    /// Client to read Walrus on-chain state
    pub read_client: SuiReadClient,
    wallet_address: SuiAddress,
    gas_budget: u64,
}

impl SuiContractClient {
    /// Constructor for [`SuiContractClient`]
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

    /// Executes the move call to `function` with `call_args` and transfers all outputs
    /// (if any) to the sender
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
            self.read_client.system_pkg,
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
        let gas_price = self.wallet.get_reference_gas_price().await?;

        let transaction = TransactionData::new_programmable(
            self.wallet_address,
            vec![gas_coin],
            programmable_transaction,
            self.gas_budget,
            gas_price,
        );

        let transaction = self.wallet.sign_transaction(&transaction);

        let response = self
            .wallet
            .execute_transaction_may_fail(transaction)
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

    /// Purchases blob storage for the next `periods_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &self,
        encoded_size: u64,
        periods_ahead: u64,
    ) -> SuiClientResult<StorageResource> {
        let price = periods_ahead * encoded_size * self.read_client.price_per_unit_size().await?;
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
                    periods_ahead.into(),
                    payment_coin.object_ref().into(),
                ],
            )
            .await?;
        let storage_id = get_created_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag(self.read_client.system_pkg, &[])?,
        )?;

        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );
        self.read_client.get_object(storage_id[0]).await
    }

    /// Registers a blob with the specified [`BlobId`] using the provided [`StorageResource`],
    /// and returns the created blob object.
    ///
    /// `encoded_size` is the size of the encoded blob, must be less than or equal to the size
    ///  reserved in `storage`.
    pub async fn register_blob(
        &self,
        storage: &StorageResource,
        blob_id: BlobId,
        encoded_size: u64,
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
                    encoded_size.into(),
                    erasure_code_type.into(),
                ],
            )
            .await?;
        let blob_obj_id = get_created_object_ids_by_type(
            &res,
            &contracts::blob::Blob.to_move_struct_tag(self.read_client.system_pkg, &[])?,
        )?;
        ensure!(
            blob_obj_id.len() == 1,
            "unexpected number of blob objects created: {}",
            blob_obj_id.len()
        );

        self.read_client.get_object(blob_obj_id[0]).await
    }

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    pub async fn certify_blob(
        &self,
        blob: &Blob,
        certificate: &ConfirmationCertificate,
    ) -> SuiClientResult<Blob> {
        let res = self
            .move_call_and_transfer(
                contracts::blob::certify.with_type_params(&[self.read_client.coin_type.clone()]),
                vec![
                    self.read_client.call_arg_from_system_obj(true).await?,
                    self.wallet.get_object_ref(blob.id).await?.into(),
                    call_arg_pure!(certificate.signature.as_bytes()),
                    call_arg_pure!(&certificate.signers),
                    (&certificate.confirmation).into(),
                ],
            )
            .await?;
        let blob: Blob = self.read_client.get_object(blob.id).await?;
        ensure!(
            blob.certified.is_some(),
            "could not certify blob: {:?}",
            res.errors
        );
        Ok(blob)
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
