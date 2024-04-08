// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.
//!

use std::{
    fmt::{self, Debug},
    future::Future,
    time::Duration,
};

use anyhow::{anyhow, bail, Result};
use move_core_types::language_storage::StructTag as MoveStructTag;
use sui_sdk::{
    apis::EventApi,
    rpc_types::{Coin, EventFilter, SuiEvent, SuiObjectDataOptions},
    types::{base_types::ObjectID, transaction::CallArg},
    SuiClient,
};
use sui_types::{
    base_types::{SequenceNumber, SuiAddress},
    event::EventID,
    object::Owner,
    TypeTag,
};
use tokio::sync::{mpsc, OnceCell};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tracing::{instrument, Instrument};

use super::SuiClientResult;
use crate::{
    contracts::{AssociatedContractStruct, AssociatedSuiEvent},
    types::{BlobCertified, BlobRegistered, Committee, SystemObject},
    utils::{get_struct_from_object_response, get_type_parameters, handle_pagination},
};

/// Trait to read system state information and events from chain.
pub trait ReadClient {
    /// Get the price for one unit of storage per epoch.
    fn price_per_unit_size(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;

    /// Get a stream of new [`BlobRegistered`] events.
    /// The `polling_interval` defines how often the connected full node is polled for events.
    /// If a `cursor` is provided, the stream will contain only events that are emitted
    /// after the event with the provided [`EventID`]. Otherwise the event stream contains all
    /// events available from the connected full node. Since the full node may prune old
    /// events, the stream is not guaranteed to contain historic events.
    fn blob_registered_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> impl Future<Output = SuiClientResult<impl Stream<Item = BlobRegistered> + Send>> + Send;

    /// Get a stream of new [`BlobCertified`] events.
    /// See [`blob_registered_events`][Self::blob_registered_events] for more information.
    fn blob_certified_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> impl Future<Output = SuiClientResult<impl Stream<Item = BlobCertified> + Send>> + Send;
    /// Get the current Walrus system object.
    fn get_system_object(&self) -> impl Future<Output = SuiClientResult<SystemObject>> + Send;

    /// Get the current committee.
    fn current_committee(&self) -> impl Future<Output = SuiClientResult<Committee>> + Send;
}

/// Client implementation for interacting with the Walrus smart contracts.
#[derive(Clone)]
pub struct SuiReadClient {
    pub(crate) system_pkg_id: ObjectID,
    pub(crate) sui_client: SuiClient,
    pub(crate) system_object_id: ObjectID,
    sys_obj_initial_version: OnceCell<SequenceNumber>,
    pub(crate) coin_type: TypeTag,
}

const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 1024;

impl SuiReadClient {
    /// Constructor for [`SuiReadClient`].
    pub async fn new(
        sui_client: SuiClient,
        system_pkg: ObjectID,
        system_object: ObjectID,
    ) -> SuiClientResult<Self> {
        let type_params = get_type_parameters(&sui_client, system_object).await?;
        ensure!(
            type_params.len() == 1,
            "unexpected number of type parameters in system object: {}",
            type_params.len()
        );

        Ok(Self {
            system_pkg_id: system_pkg,
            sui_client,
            system_object_id: system_object,
            sys_obj_initial_version: OnceCell::new(),
            coin_type: type_params[0].clone(),
        })
    }

    pub(crate) async fn call_arg_from_system_obj(&self, mutable: bool) -> SuiClientResult<CallArg> {
        let initial_shared_version = self.system_object_initial_version().await?;
        Ok(CallArg::Object(
            sui_types::transaction::ObjectArg::SharedObject {
                id: self.system_object_id,
                initial_shared_version,
                mutable,
            },
        ))
    }

    async fn system_object_initial_version(&self) -> SuiClientResult<SequenceNumber> {
        let initial_shared_version = self
            .sys_obj_initial_version
            .get_or_try_init(|| self.get_shared_object_initial_version(self.system_object_id))
            .await?;
        Ok(*initial_shared_version)
    }

    async fn get_shared_object_initial_version(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<SequenceNumber> {
        let Some(Owner::Shared {
            initial_shared_version,
        }) = self
            .sui_client
            .read_api()
            .get_object_with_options(object_id, SuiObjectDataOptions::new().with_owner())
            .await?
            .owner()
        else {
            return Err(anyhow!("trying to get the initial version of a non-shared object").into());
        };
        Ok(initial_shared_version)
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

    pub(crate) async fn get_object<U>(&self, object_id: ObjectID) -> SuiClientResult<U>
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
    ) -> SuiClientResult<ReceiverStream<U>>
    where
        U: AssociatedSuiEvent + Debug + Send + Sync + 'static,
    {
        let (tx_event, rx_event) = mpsc::channel::<U>(EVENT_CHANNEL_CAPACITY);
        let event_tag =
            U::EVENT_STRUCT.to_move_struct_tag(self.system_pkg_id, event_type_params)?;

        let event_api = self.sui_client.event_api().clone();

        tokio::spawn(async move {
            poll_for_events(tx_event, polling_interval, event_api, event_tag, cursor).await
        });
        Ok(ReceiverStream::new(rx_event))
    }
}

impl ReadClient for SuiReadClient {
    async fn price_per_unit_size(&self) -> SuiClientResult<u64> {
        Ok(self.get_system_object().await?.price_per_unit_size)
    }

    async fn blob_registered_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = BlobRegistered>> {
        self.get_event_stream(polling_interval, &[], cursor).await
    }

    async fn blob_certified_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = BlobCertified>> {
        self.get_event_stream(polling_interval, &[], cursor).await
    }

    async fn get_system_object(&self) -> SuiClientResult<SystemObject> {
        self.get_object(self.system_object_id).await
    }

    async fn current_committee(&self) -> SuiClientResult<Committee> {
        Ok(self.get_system_object().await?.current_committee)
    }
}

impl fmt::Debug for SuiReadClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SuiReadClient")
            .field("system_pkg", &self.system_pkg_id)
            .field("sui_client", &"<redacted>")
            .field("system_object", &self.system_object_id)
            .field("coin_type", &self.coin_type)
            .finish()
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
                // See https://github.com/MystenLabs/walrus/issues/144.
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
