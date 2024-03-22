// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!
use core::str::FromStr;
use std::{fmt::Debug, time::Duration};

use anyhow::{anyhow, bail, ensure, Result};
use fastcrypto::traits::ToFromBytes;
use move_core_types::language_storage::StructTag as MoveStructTag;
use sui_sdk::{
    apis::EventApi,
    rpc_types::{
        EventFilter,
        SuiEvent,
        SuiExecutionStatus,
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
    event::EventID,
    object::Owner,
    transaction::{Argument, ProgrammableTransaction},
    Identifier,
    TypeTag,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, Instrument};
use walrus_core::{messages::ConfirmationCertificate, BlobId, EncodingType};

use crate::{
    contracts::{self, AssociatedContractStruct, AssociatedSuiEvent, FunctionTag},
    types::{Blob, BlobCertified, BlobRegistered, Committee, StorageResource, SystemObject},
    utils::{
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

const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 1024;

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
        Ok(self.get_system_object().await?.price_per_unit_size)
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

    async fn get_event_stream<U>(
        &self,
        polling_interval: Duration,
        event_type_params: &[TypeTag],
        cursor: Option<EventID>,
    ) -> Result<ReceiverStream<U>>
    where
        U: AssociatedSuiEvent + Debug + Send + Sync + 'static,
    {
        let (tx_event, rx_event) = mpsc::channel::<U>(EVENT_CHANNEL_CAPACITY);
        let event_tag = U::EVENT_STRUCT.to_move_struct_tag(self.walrus_pkg, event_type_params)?;

        let event_api = self.sui_client.event_api().clone();

        tokio::spawn(async move {
            poll_for_events(tx_event, polling_interval, event_api, event_tag, cursor).await
        });
        Ok(ReceiverStream::new(rx_event))
    }

    /// Get a stream of new [`BlobRegistered`] events.
    /// The `polling_interval` defines how often the connected full node is polled for events.
    /// If a `cursor` is provided, the stream will contain only events that are emitted
    /// after the event with the provided [`EventID`]. Otherwise the event stream contains all
    /// events available from the connected full node. Since the full node may prune old
    /// events, the stream is not guaranteed to contain historic events.
    pub async fn blob_registered_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> Result<ReceiverStream<BlobRegistered>> {
        self.get_event_stream(polling_interval, &[self.system_tag.clone()], cursor)
            .await
    }

    /// Get a stream of new [`BlobCertified`] events.
    /// See [`blob_registered_events`][Self::blob_registered_events] for more information.
    pub async fn blob_certified_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> Result<ReceiverStream<BlobCertified>> {
        self.get_event_stream(polling_interval, &[self.system_tag.clone()], cursor)
            .await
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
                    call_arg_pure!(&blob_id),
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

    /// Certifies the specified blob on Sui, given a certificate that confirms its storage and
    /// returns the certified blob.
    pub async fn certify_blob(
        &self,
        blob: &Blob,
        certificate: &ConfirmationCertificate,
    ) -> Result<Blob> {
        let res = self
            .move_call_and_transfer(
                contracts::blob::certify
                    .with_type_params(&[self.system_tag.clone(), self.coin_type.clone()]),
                vec![
                    self.call_arg_from_shared_object_id(self.system_object, true)
                        .await?,
                    self.wallet.get_object_ref(blob.id).await?.into(),
                    call_arg_pure!(certificate.signature.as_bytes()),
                    call_arg_pure!(&certificate.signers),
                    (&certificate.confirmation).into(),
                ],
            )
            .await?;
        let blob: Blob = self.get_object(blob.id).await?;
        ensure!(
            blob.certified.is_some(),
            format!("could not certify blob: {:?}", res.errors)
        );
        Ok(blob)
    }

    /// Get the current Walrus system object
    pub async fn get_system_object(&self) -> Result<SystemObject> {
        self.get_object(self.system_object).await
    }

    /// Get the current committee
    pub async fn current_committee(&self) -> Result<Committee> {
        Ok(self.get_system_object().await?.current_committee)
    }
}

#[instrument(err, skip_all)]
async fn poll_for_events<U>(
    tx_event: mpsc::Sender<U>,
    initial_polling_interval: Duration,
    event_api: EventApi,
    event_tag: MoveStructTag,
    mut last_event: Option<EventID>,
) -> Result<()>
where
    U: TryFrom<SuiEvent> + Send + Sync + Debug + 'static,
{
    // The actual interval with which we poll, increases if there is an RPC error
    let mut polling_interval = initial_polling_interval;
    let event_filter = EventFilter::MoveEventType(event_tag.clone());
    let mut page_available = false;
    while !tx_event.is_closed() {
        // only wait if no event pages were left in the last iteration
        if !page_available {
            tokio::time::sleep(polling_interval).await;
        }
        // Get the next page of events/newly emitted events
        match event_api
            .query_events(event_filter.clone(), last_event, None, false)
            .await
        {
            Ok(events) => {
                page_available = events.has_next_page;
                polling_interval = initial_polling_interval;
                for event in events.data {
                    last_event = Some(event.id);
                    let event_obj = event.try_into().map_err(|_| {
                        anyhow!(
                            "event matching the event type {:?} could not be converted",
                            &event_tag,
                        )
                    })?;
                    // unwrap safe because we wrap it in `Some()` above
                    let span = tracing::debug_span!("sui-event", event_id = ?last_event.unwrap());
                    let _guard = span.enter();
                    match tx_event.send(event_obj).in_current_span().await {
                        Ok(()) => tracing::debug!("received event"),
                        Err(_) => {
                            tracing::debug!("channel was closed by receiver");
                            return Ok(());
                        }
                    }
                }
            }
            Err(sui_sdk::error::Error::RpcError(e)) => {
                // We retry here, since this error generally (only?)
                // occurs if the cursor could not be found, but this is
                // resolved quickly after retrying.
                tracing::error!("RPC error for otherwise valid RPC call: {}", e);
                // Do an exponential backoff until `MAX_POLLING_INTERVAL` is reached
                // unless `initial_polling_interval` is larger
                // TODO(karl): Stop retrying and switch to a different full node.
                // Ideally, we cut off the stream after retrying
                // for a few times and then switch to a different full node.
                // This logic would need to be handled by a consumer of the
                // stream. Until that is in place, retry indefinitely.
                // See https://github.com/MystenLabs/walrus/issues/144
                polling_interval = polling_interval
                    .saturating_mul(2)
                    .min(MAX_POLLING_INTERVAL)
                    .max(initial_polling_interval);
                page_available = false;
                continue;
            }
            Err(e) => {
                bail!("unexpected error from event api: {}", e);
            }
        };
    }
    tracing::debug!("channel was closed by receiver");
    return Ok(());
}
