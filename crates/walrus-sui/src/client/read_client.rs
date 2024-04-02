// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!

use std::{fmt::Debug, time::Duration};

use anyhow::{anyhow, bail, Result};
use move_core_types::language_storage::StructTag as MoveStructTag;
use sui_sdk::{
    apis::EventApi,
    rpc_types::{Coin, EventFilter, SuiEvent, SuiObjectDataOptions},
    types::{base_types::ObjectID, transaction::CallArg},
    SuiClient,
};
use sui_types::{base_types::SuiAddress, event::EventID, object::Owner, TypeTag};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{instrument, Instrument};

use super::SuiClientError;
use crate::{
    contracts::{AssociatedContractStruct, AssociatedSuiEvent},
    types::{BlobCertified, BlobRegistered, Committee, SystemObject},
    utils::{get_struct_from_object_response, get_type_parameters, handle_pagination},
};

/// Client implementation for interacting with the Walrus smart contracts
pub struct SuiReadClient {
    pub(crate) system_pkg: ObjectID,
    pub(crate) sui_client: SuiClient,
    pub(crate) system_object: ObjectID,
    pub(crate) coin_type: TypeTag,
    pub(crate) system_tag: TypeTag,
}

const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 1024;

impl SuiReadClient {
    /// Constructor for [`SuiReadClient`]
    pub async fn new(
        sui_client: SuiClient,
        system_pkg: ObjectID,
        system_object: ObjectID,
    ) -> Result<Self, SuiClientError> {
        let type_params = get_type_parameters(&sui_client, system_object).await?;
        ensure!(
            type_params.len() == 2,
            "unexpected number of type parameters in system object: {}",
            type_params.len()
        );

        Ok(Self {
            system_pkg,
            sui_client,
            system_object,
            coin_type: type_params[1].clone(),
            system_tag: type_params[0].clone(),
        })
    }

    pub(crate) async fn call_arg_from_system_obj(
        &self,
        mutable: bool,
    ) -> Result<CallArg, SuiClientError> {
        self.call_arg_from_shared_object_id(self.system_object, mutable)
            .await
    }

    pub(crate) async fn get_payment_coins(
        &self,
        owner_address: SuiAddress,
    ) -> Result<impl Iterator<Item = Coin> + '_, sui_sdk::error::Error> {
        handle_pagination(move |cursor| {
            self.sui_client.coin_read_api().get_coins(
                owner_address,
                Some(self.coin_type.to_canonical_string(true)),
                cursor,
                None,
            )
        })
        .await
    }

    pub(crate) async fn call_arg_from_shared_object_id(
        &self,
        id: ObjectID,
        mutable: bool,
    ) -> Result<CallArg, SuiClientError> {
        let Some(Owner::Shared {
            initial_shared_version,
        }) = self
            .sui_client
            .read_api()
            .get_object_with_options(id, SuiObjectDataOptions::new().with_owner())
            .await?
            .owner()
        else {
            return Err(anyhow!(
                "trying to get the initial version of a non-shared object: {}",
                id
            )
            .into());
        };
        Ok(CallArg::Object(
            sui_types::transaction::ObjectArg::SharedObject {
                id,
                initial_shared_version,
                mutable,
            },
        ))
    }

    /// Get the price for one unit of storage per epoch
    pub async fn price_per_unit_size(&self) -> Result<u64, SuiClientError> {
        Ok(self.get_system_object().await?.price_per_unit_size)
    }

    pub(crate) async fn get_object<U>(&self, object_id: ObjectID) -> Result<U, SuiClientError>
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
            .into()
        })
    }

    async fn get_event_stream<U>(
        &self,
        polling_interval: Duration,
        event_type_params: &[TypeTag],
        cursor: Option<EventID>,
    ) -> Result<ReceiverStream<U>, SuiClientError>
    where
        U: AssociatedSuiEvent + Debug + Send + Sync + 'static,
    {
        let (tx_event, rx_event) = mpsc::channel::<U>(EVENT_CHANNEL_CAPACITY);
        let event_tag = U::EVENT_STRUCT.to_move_struct_tag(self.system_pkg, event_type_params)?;

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
    ) -> Result<ReceiverStream<BlobRegistered>, SuiClientError> {
        self.get_event_stream(polling_interval, &[self.system_tag.clone()], cursor)
            .await
    }

    /// Get a stream of new [`BlobCertified`] events.
    /// See [`blob_registered_events`][Self::blob_registered_events] for more information.
    pub async fn blob_certified_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> Result<ReceiverStream<BlobCertified>, SuiClientError> {
        self.get_event_stream(polling_interval, &[self.system_tag.clone()], cursor)
            .await
    }

    /// Get the current Walrus system object
    pub async fn get_system_object(&self) -> Result<SystemObject, SuiClientError> {
        self.get_object(self.system_object).await
    }

    /// Get the current committee
    pub async fn current_committee(&self) -> Result<Committee, SuiClientError> {
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
