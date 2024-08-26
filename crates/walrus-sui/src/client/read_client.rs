// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use std::{
    collections::BTreeSet,
    fmt::{self, Debug},
    future::Future,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use sui_sdk::{
    apis::EventApi,
    rpc_types::{Coin, EventFilter, SuiEvent, SuiObjectDataOptions},
    types::{base_types::ObjectID, transaction::CallArg},
    SuiClient,
    SuiClientBuilder,
};
use sui_types::{
    base_types::{ObjectRef, SequenceNumber, SuiAddress},
    event::EventID,
    object::Owner,
    Identifier,
};
use tokio::sync::{mpsc, OnceCell};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tracing::{instrument, Instrument};
use walrus_core::ensure;

use super::{SuiClientError, SuiClientResult};
use crate::{
    types::{
        move_structs::{
            StakingObjectForDeserialization,
            StakingPool,
            SystemObjectForDeserialization,
        },
        BlobEvent,
        Committee,
        StakingObject,
        StorageNode,
        SystemObject,
    },
    utils::{
        get_package_id_from_object_response,
        get_sui_object,
        get_sui_object_from_object_response,
        handle_pagination,
    },
};

const EVENT_MODULE: &str = "events";
const MULTI_GET_OBJ_LIMIT: usize = 50;

/// Trait to read system state information and events from chain.
pub trait ReadClient: Send + Sync {
    /// Returns the price for one unit of storage per epoch.
    fn storage_price_per_unit_size(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;

    /// Returns the price to write one unit of storage.
    fn write_price_per_unit_size(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;

    /// Returns a stream of new blob events.
    ///
    /// The `polling_interval` defines how often the connected full node is polled for events.
    /// If a `cursor` is provided, the stream will contain only events that are emitted
    /// after the event with the provided [`EventID`]. Otherwise the event stream contains all
    /// events available from the connected full node. Since the full node may prune old
    /// events, the stream is not guaranteed to contain historic events.
    fn blob_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> impl Future<Output = SuiClientResult<impl Stream<Item = BlobEvent> + Send>> + Send;

    /// Returns the blob event with the given Event ID.
    fn get_blob_event(
        &self,
        event_id: EventID,
    ) -> impl Future<Output = SuiClientResult<BlobEvent>> + Send;

    /// Returns the current committee.
    fn current_committee(&self) -> impl Future<Output = SuiClientResult<Committee>> + Send;
}

/// Client implementation for interacting with the Walrus smart contracts.
#[derive(Clone)]
pub struct SuiReadClient {
    pub(crate) system_pkg_id: ObjectID,
    pub(crate) sui_client: SuiClient,
    pub(crate) system_object_id: ObjectID,
    pub(crate) staking_object_id: ObjectID,
    sys_obj_initial_version: OnceCell<SequenceNumber>,
    staking_obj_initial_version: OnceCell<SequenceNumber>,
}

const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 1024;

impl SuiReadClient {
    /// Constructor for `SuiReadClient`.
    pub async fn new(
        sui_client: SuiClient,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
    ) -> SuiClientResult<Self> {
        let system_pkg_id = get_system_package_id(&sui_client, system_object_id).await?;
        Ok(Self {
            system_pkg_id,
            sui_client,
            system_object_id,
            staking_object_id,
            sys_obj_initial_version: OnceCell::new(),
            staking_obj_initial_version: OnceCell::new(),
        })
    }

    /// Constructs a new `SuiReadClient` around a [`SuiClient`] constructed for the
    /// provided fullnode's RPC address.
    pub async fn new_for_rpc<S: AsRef<str>>(
        rpc_address: S,
        system_object: ObjectID,
        staking_object: ObjectID,
    ) -> SuiClientResult<Self> {
        let client = SuiClientBuilder::default().build(rpc_address).await?;
        Self::new(client, system_object, staking_object).await
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

    pub(crate) async fn call_arg_from_staking_obj(
        &self,
        mutable: bool,
    ) -> SuiClientResult<CallArg> {
        let initial_shared_version = self.staking_object_initial_version().await?;
        Ok(CallArg::Object(
            sui_types::transaction::ObjectArg::SharedObject {
                id: self.staking_object_id,
                initial_shared_version,
                mutable,
            },
        ))
    }

    async fn staking_object_initial_version(&self) -> SuiClientResult<SequenceNumber> {
        let initial_shared_version = self
            .staking_obj_initial_version
            .get_or_try_init(|| self.get_shared_object_initial_version(self.staking_object_id))
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

    /// Returns the system package ID.
    pub fn get_system_package_id(&self) -> ObjectID {
        self.system_pkg_id
    }

    pub(crate) async fn get_coins_of_type(
        &self,
        owner_address: SuiAddress,
        coin_type: Option<String>,
    ) -> Result<impl Iterator<Item = Coin> + '_, sui_sdk::error::Error> {
        handle_pagination(move |cursor| {
            self.sui_client.coin_read_api().get_coins(
                owner_address,
                coin_type.clone(),
                cursor,
                None,
            )
        })
        .await
    }

    /// Returns a vector of coins of provided `coin_type` whose total balance is at least `balance`.
    ///
    /// Returns `None` if no coins of sufficient total balance are found.
    pub(crate) async fn get_coins_with_total_balance(
        &self,
        owner_address: SuiAddress,
        coin_type: Option<String>,
        balance: u64,
    ) -> Result<Vec<Coin>> {
        let mut coins_iter = self.get_coins_of_type(owner_address, coin_type).await?;

        let mut coins = vec![];
        let mut total_balance = 0;
        while total_balance < balance {
            let coin = coins_iter.next().context("insufficient total balance")?;
            total_balance += coin.balance;
            coins.push(coin);
        }
        Ok(coins)
    }

    /// Returns a coin of provided `coin_type` whose balance is at least `balance`.
    ///
    /// Filters out any coin objects included in the `forbidden_objects` set.
    ///
    /// Returns `None` if no coin of sufficient balance is found.
    pub(crate) async fn get_coin_with_balance(
        &self,
        owner_address: SuiAddress,
        coin_type: Option<String>,
        balance: u64,
        forbidden_objects: BTreeSet<ObjectID>,
    ) -> Result<Coin> {
        self.get_coins_of_type(owner_address, coin_type)
            .await?
            .filter(|coin| !forbidden_objects.contains(&coin.object_ref().0))
            .find(|coin| coin.balance >= balance)
            .context("no coin with sufficient balance exists")
    }

    /// Get the latest object reference given an [`ObjectID`].
    pub async fn get_object_ref(&self, object_id: ObjectID) -> Result<ObjectRef, anyhow::Error> {
        Ok(self
            .sui_client
            .read_api()
            .get_object_with_options(object_id, SuiObjectDataOptions::new())
            .await?
            .into_object()?
            .object_ref())
    }

    pub(crate) fn coin_type(&self) -> String {
        format!("{}::wal::WAL", self.system_pkg_id)
    }

    async fn get_system_object(&self) -> SuiClientResult<SystemObject> {
        let SystemObjectForDeserialization { id, version } =
            get_sui_object(&self.sui_client, self.system_object_id).await?;
        let Some(dynamic_field_info) = self
            .sui_client
            .read_api()
            .get_dynamic_fields(self.system_object_id, None, None)
            .await?
            .data
            .into_iter()
            .next()
        else {
            return Err(anyhow!("system object does not have a dynamic field").into());
        };

        let inner = get_sui_object(&self.sui_client, dynamic_field_info.object_id).await?;
        Ok(SystemObject { id, version, inner })
    }

    async fn get_staking_object(&self) -> SuiClientResult<StakingObject> {
        let StakingObjectForDeserialization { id, version } =
            get_sui_object(&self.sui_client, self.staking_object_id).await?;
        let Some(dynamic_field_info) = self
            .sui_client
            .read_api()
            .get_dynamic_fields(self.staking_object_id, None, None)
            .await?
            .data
            .into_iter()
            .next()
        else {
            return Err(anyhow!("staking object does not have a dynamic field").into());
        };

        let inner = get_sui_object(&self.sui_client, dynamic_field_info.object_id).await?;
        Ok(StakingObject { id, version, inner })
    }
}

impl ReadClient for SuiReadClient {
    #[instrument(err, skip(self))]
    async fn storage_price_per_unit_size(&self) -> SuiClientResult<u64> {
        Ok(self
            .get_system_object()
            .await?
            .inner
            .storage_price_per_unit_size)
    }

    async fn write_price_per_unit_size(&self) -> SuiClientResult<u64> {
        Ok(self
            .get_system_object()
            .await?
            .inner
            .write_price_per_unit_size)
    }

    async fn blob_events(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = BlobEvent>> {
        let (tx_event, rx_event) = mpsc::channel::<BlobEvent>(EVENT_CHANNEL_CAPACITY);

        let event_api = self.sui_client.event_api().clone();

        let event_filter = EventFilter::MoveEventModule {
            package: self.system_pkg_id,
            module: Identifier::new(EVENT_MODULE)?,
        };
        tokio::spawn(async move {
            poll_for_events(tx_event, polling_interval, event_api, event_filter, cursor).await
        });
        Ok(ReceiverStream::new(rx_event))
    }

    async fn get_blob_event(&self, event_id: EventID) -> SuiClientResult<BlobEvent> {
        self.sui_client
            .event_api()
            .get_events(event_id.tx_digest)
            .await?
            .into_iter()
            .find(|e| e.id == event_id)
            .and_then(|e| e.try_into().ok())
            .ok_or(SuiClientError::NoCorrespondingBlobEvent(event_id))
    }

    async fn current_committee(&self) -> SuiClientResult<Committee> {
        let staking_object = self.get_staking_object().await?;
        let shard_assignment = staking_object.inner.committee;
        let mut node_object_responses = vec![];
        for obj_id_batch in shard_assignment.chunks(MULTI_GET_OBJ_LIMIT) {
            node_object_responses.extend(
                self.sui_client
                    .read_api()
                    .multi_get_object_with_options(
                        obj_id_batch
                            .iter()
                            .map(|(obj_id, _shards)| *obj_id)
                            .collect(),
                        SuiObjectDataOptions::new().with_type().with_bcs(),
                    )
                    .await?,
            );
        }

        let nodes = shard_assignment
            .iter()
            .zip(node_object_responses)
            .map(|((obj_id, shards), obj_response)| {
                let mut storage_node =
                    get_sui_object_from_object_response::<StakingPool>(&obj_response)?.node_info;
                storage_node.shard_ids = shards.iter().map(|index| index.into()).collect();
                ensure!(
                    *obj_id == storage_node.node_id,
                    anyhow!("the object id of the staking pool does not match the node id")
                );
                Ok::<StorageNode, anyhow::Error>(storage_node)
            })
            .collect::<Result<Vec<_>>>()?;
        Committee::new(
            nodes,
            staking_object.inner.epoch,
            staking_object.inner.n_shards,
        )
        .map_err(|err| SuiClientError::Internal(err.into()))
    }
}

impl fmt::Debug for SuiReadClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SuiReadClient")
            .field("system_pkg", &self.system_pkg_id)
            .field("sui_client", &"<redacted>")
            .field("system_object", &self.system_object_id)
            .finish()
    }
}

/// Checks if the Walrus system object exist on chain and returns the Walrus package ID.
async fn get_system_package_id(
    sui_client: &SuiClient,
    system_object_id: ObjectID,
) -> SuiClientResult<ObjectID> {
    let response = sui_client
        .read_api()
        .get_object_with_options(
            system_object_id,
            SuiObjectDataOptions::default().with_type().with_bcs(),
        )
        .await
        .map_err(|error| {
            tracing::debug!(%error, "unable to get the Walrus system object");
            SuiClientError::WalrusSystemObjectDoesNotExist(system_object_id)
        })?;

    get_sui_object_from_object_response::<SystemObjectForDeserialization>(&response).map_err(
        |error| {
            tracing::debug!(%error, "error when trying to deserialize the system object");
            SuiClientError::WalrusSystemObjectDoesNotExist(system_object_id)
        },
    )?;

    let object_pkg_id = get_package_id_from_object_response(&response).map_err(|error| {
        tracing::debug!(%error, "unable to get the Walrus package ID");
        SuiClientError::WalrusSystemObjectDoesNotExist(system_object_id)
    })?;
    Ok(object_pkg_id)
}

#[tracing::instrument(err, skip_all)]
async fn poll_for_events<U>(
    tx_event: mpsc::Sender<U>,
    initial_polling_interval: Duration,
    event_api: EventApi,
    event_filter: EventFilter,
    mut last_event: Option<EventID>,
) -> Result<()>
where
    U: TryFrom<SuiEvent> + Send + Sync + Debug + 'static,
{
    // The actual interval with which we poll, increases if there is an RPC error
    let mut polling_interval = initial_polling_interval;
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
                    let span = tracing::error_span!(
                        "sui-event",
                        event_id = ?event.id,
                        event_type = ?event.type_
                    );
                    let _guard = span.enter();
                    let event_obj = match event.try_into() {
                        Ok(event_obj) => event_obj,
                        Err(_) => {
                            tracing::error!("could not convert event");
                            continue;
                        }
                    };
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
                tracing::warn!(
                    event_cursor = ?last_event,
                    backoff = ?polling_interval,
                    rpc_error = ?e,
                    "RPC error for otherwise valid RPC call, retrying event polling after backoff",
                );
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
