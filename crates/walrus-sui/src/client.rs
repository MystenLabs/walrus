// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!
use core::str::FromStr;

use anyhow::{anyhow, bail, ensure, Result};
use sui_sdk::{
    rpc_types::{
        SuiExecutionStatus,
        SuiMoveValue,
        SuiObjectDataOptions,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
    },
    types::{
        base_types::{ObjectID, ObjectRef},
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::{CallArg, TransactionData},
    },
    wallet_context::WalletContext,
    SuiClient,
};
use sui_types::{
    base_types::SuiAddress,
    object::Owner,
    transaction::{Argument, ProgrammableTransaction},
    Identifier,
    TypeTag,
};
use walrus_core::{BlobId, EncodingType};

use crate::{
    contracts::{self, AssociatedContractStruct, FunctionTag},
    types::{Blob, StorageResource},
    utils::{
        blob_id_to_call_arg,
        call_args_to_object_ids,
        get_created_object_ids_by_type,
        get_struct_from_object_response,
        get_type_parameters,
        handle_pagination,
    },
};

/// Client implementation for interacting with the Walrus smart contracts
pub struct WalrusSuiClient {
    wallet: WalletContext,
    walrus_pkg: ObjectID,
    sui_client: SuiClient,
    wallet_address: SuiAddress,
    system_object: ObjectID,
    coin_type: TypeTag,
    system_tag: TypeTag,
    gas_budget: u64,
}

impl WalrusSuiClient {
    /// Constructor for [`WalrusSuiClient`]
    pub async fn new(
        mut wallet: WalletContext,
        walrus_pkg: ObjectID,
        system_object: ObjectID,
        gas_budget: u64,
    ) -> Result<Self> {
        let sui_client = wallet.get_client().await?;
        let wallet_address = wallet.active_address()?;
        let type_params = get_type_parameters(&sui_client, system_object).await?;
        ensure!(
            type_params.len() == 2,
            "unexpected number of type parameters in system object"
        );
        Ok(Self {
            wallet,
            walrus_pkg,
            sui_client,
            wallet_address,
            system_object,
            coin_type: type_params[1].clone(),
            system_tag: type_params[0].clone(),
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
            self.walrus_pkg,
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

    async fn call_arg_from_shared_object_id(&self, id: ObjectID, mutable: bool) -> Result<CallArg> {
        let Some(Owner::Shared {
            initial_shared_version,
        }) = self
            .sui_client
            .read_api()
            .get_object_with_options(id, SuiObjectDataOptions::new().with_owner())
            .await?
            .owner()
        else {
            bail!(
                "trying to get the initial version of a non-shared object: {}",
                id
            )
        };
        Ok(CallArg::Object(
            sui_types::transaction::ObjectArg::SharedObject {
                id,
                initial_shared_version,
                mutable,
            },
        ))
    }

    async fn price_per_unit_size(&self) -> Result<u64> {
        let system_struct = get_struct_from_object_response(
            &self
                .sui_client
                .read_api()
                .get_object_with_options(
                    self.system_object,
                    SuiObjectDataOptions::new().with_content(),
                )
                .await?,
        )?;
        Ok(
            get_dynamic_field!(system_struct, "price_per_unit_size", SuiMoveValue::String)
                .parse()?,
        )
    }

    async fn get_object<U>(&self, object_id: ObjectID) -> Result<U>
    where
        U: AssociatedContractStruct,
    {
        let obj_struct = get_struct_from_object_response(
            &self
                .sui_client
                .read_api()
                .get_object_with_options(object_id, SuiObjectDataOptions::new().with_content())
                .await?,
        )?;
        U::try_from(obj_struct).map_err(|_e| {
            anyhow!(
                "could not convert object with id {} to expected type",
                object_id
            )
        })
    }

    /// Purchases blob storage for the next `periods_ahead` Walrus epochs and an encoded
    /// size of `encoded_size` and returns the created storage resource.
    pub async fn reserve_space(
        &self,
        encoded_size: u64,
        periods_ahead: u64,
    ) -> Result<StorageResource> {
        let price = periods_ahead * encoded_size * self.price_per_unit_size().await?;
        let payment_coin = handle_pagination(|cursor| {
            self.sui_client.coin_read_api().get_coins(
                self.wallet_address,
                Some(self.coin_type.to_canonical_string(true)),
                cursor,
                None,
            )
        })
        .await?
        .find(|coin| coin.balance >= price)
        .ok_or_else(|| anyhow!("no compatible payment coin found"))?;
        let res = self
            .move_call_and_transfer(
                contracts::system::reserve_space
                    .with_type_params(&[self.system_tag.clone(), self.coin_type.clone()]),
                vec![
                    self.call_arg_from_shared_object_id(self.system_object, true)
                        .await?,
                    encoded_size.into(),
                    periods_ahead.into(),
                    payment_coin.object_ref().into(),
                ],
            )
            .await?;
        let storage_id = get_created_object_ids_by_type(
            &res,
            &contracts::storage_resource::Storage
                .to_move_struct_tag(self.walrus_pkg, &[self.system_tag.clone()])?,
        )?;
        ensure!(
            storage_id.len() == 1,
            "unexpected number of storage resources created: {}",
            storage_id.len()
        );
        self.get_object(storage_id[0]).await
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
                contracts::blob::register
                    .with_type_params(&[self.system_tag.clone(), self.coin_type.clone()]),
                vec![
                    self.call_arg_from_shared_object_id(self.system_object, true)
                        .await?,
                    self.wallet.get_object_ref(storage.id).await?.into(),
                    blob_id_to_call_arg(blob_id),
                    encoded_size.into(),
                    erasure_code_type.into(),
                ],
            )
            .await?;
        let blob_obj_id = get_created_object_ids_by_type(
            &res,
            &contracts::blob::Blob
                .to_move_struct_tag(self.walrus_pkg, &[self.system_tag.clone()])?,
        )?;
        ensure!(
            blob_obj_id.len() == 1,
            "unexpected number of blob objects created: {}",
            blob_obj_id.len()
        );
        self.get_object(blob_obj_id[0]).await
    }
}
