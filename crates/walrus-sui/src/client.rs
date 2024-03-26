// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!
use core::str::FromStr;

use anyhow::{anyhow, bail, ensure, Result};
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
pub use read_client::SuiReadClient;
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
    ) -> Result<Self> {
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
    ) -> Result<SuiTransactionBlockResponse> {
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
            bail!("Result should be Argument::Result")
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
                .await?
                .1
                .object_ref(),
        )
        .await
    }

    async fn sign_and_send_ptb(
        &self,
        programmable_transaction: ProgrammableTransaction,
        gas_coin: ObjectRef,
    ) -> Result<SuiTransactionBlockResponse> {
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
                Err(anyhow!("Error in transaction execution: {}", error))
            }
        }
    }

    /// Purchases blob storage for the next `periods_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &self,
        encoded_size: u64,
        periods_ahead: u64,
    ) -> Result<StorageResource> {
        let price = periods_ahead * encoded_size * self.read_client.price_per_unit_size().await?;
        let payment_coin = self
            .read_client
            .get_payment_coins(self.wallet_address)
            .await?
            .find(|coin| coin.balance >= price)
            .ok_or_else(|| anyhow!("no compatible payment coin found"))?;
        let res = self
            .move_call_and_transfer(
                contracts::system::reserve_space.with_type_params(&[
                    self.read_client.system_tag.clone(),
                    self.read_client.coin_type.clone(),
                ]),
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
            &contracts::storage_resource::Storage.to_move_struct_tag(
                self.read_client.system_pkg,
                &[self.read_client.system_tag.clone()],
            )?,
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
    ) -> Result<Blob> {
        let erasure_code_type: u8 = erasure_code_type.into();
        let res = self
            .move_call_and_transfer(
                contracts::blob::register.with_type_params(&[
                    self.read_client.system_tag.clone(),
                    self.read_client.coin_type.clone(),
                ]),
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
            &contracts::blob::Blob.to_move_struct_tag(
                self.read_client.system_pkg,
                &[self.read_client.system_tag.clone()],
            )?,
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
    ) -> Result<Blob> {
        let res = self
            .move_call_and_transfer(
                contracts::blob::certify.with_type_params(&[
                    self.read_client.system_tag.clone(),
                    self.read_client.coin_type.clone(),
                ]),
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
            format!("could not certify blob: {:?}", res.errors)
        );
        Ok(blob)
    }
}
