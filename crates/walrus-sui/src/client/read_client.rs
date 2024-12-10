// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    future::Future,
    num::NonZeroU16,
    ops::ControlFlow,
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use sui_sdk::{
    apis::EventApi,
    rpc_types::{
        Coin,
        EventFilter,
        SuiEvent,
        SuiObjectData,
        SuiObjectDataFilter,
        SuiObjectDataOptions,
        SuiObjectResponseQuery,
        SuiRawData,
    },
    types::base_types::ObjectID,
    SuiClient,
    SuiClientBuilder,
};
use sui_types::{
    base_types::{ObjectRef, SequenceNumber, SuiAddress},
    event::EventID,
    object::Owner,
    transaction::ObjectArg,
    Identifier,
    TypeTag,
};
use tokio::sync::{mpsc, OnceCell};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tracing::Instrument as _;
use walrus_core::{ensure, Epoch};

use super::{SuiClientError, SuiClientResult};
use crate::{
    contracts::{self, AssociatedContractStruct, TypeOriginMap},
    types::{
        move_structs::{
            EpochState,
            StakingInnerV1,
            StakingObjectForDeserialization,
            StakingPool,
            SystemObjectForDeserialization,
            SystemStateInnerV1,
        },
        BlobEvent,
        Committee,
        ContractEvent,
        StakingObject,
        StorageNode,
        StorageNodeCap,
        SystemObject,
    },
    utils::{
        get_dynamic_field_object,
        get_package_id_from_object_response,
        get_sui_object,
        get_sui_object_from_object_response,
        handle_pagination,
    },
};

const EVENT_MODULE: &str = "events";
const MULTI_GET_OBJ_LIMIT: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The type of coin.
pub enum CoinType {
    /// The WAL coin type.
    Wal,
    /// The SUI coin type.
    Sui,
}

/// The current, previous, and next committee, and the current epoch state.
///
/// This struct is only used to pass the information on committees and state. No invariants are
/// checked here, but possibly enforced by the crators and consumers of the struct.
#[derive(Debug)]
pub struct CommitteesAndState {
    /// The current committee.
    pub current: Committee,
    /// The previous committee.
    pub previous: Option<Committee>,
    /// The next committee.
    pub next: Option<Committee>,
    /// The epoch state for the current epoch.
    pub epoch_state: EpochState,
}

/// Walrus parameters that do not change across epochs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedSystemParameters {
    /// The number of shards in the system.
    pub n_shards: NonZeroU16,
    /// The maximum number of epochs ahead that the system can account for, and therefore that blobs
    /// can be stored for.
    pub max_epochs_ahead: u32,
    /// The duration of an epoch for epochs 1 onwards.
    pub epoch_duration: Duration,
    /// The time at which the genesis epoch, epoch 0, can change to epoch 1.
    pub epoch_zero_end: DateTime<Utc>,
}

/// Trait to read system state information and events from chain.
pub trait ReadClient: Send + Sync {
    /// Returns the price for one unit of storage per epoch.
    fn storage_price_per_unit_size(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;

    /// Returns the price to write one unit of storage.
    fn write_price_per_unit_size(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;

    /// Returns the storage and write price for one unit of storage.
    fn storage_and_write_price_per_unit_size(
        &self,
    ) -> impl Future<Output = SuiClientResult<(u64, u64)>> + Send;

    /// Returns a stream of new blob events.
    ///
    /// The `polling_interval` defines how often the connected full node is polled for events.
    /// If a `cursor` is provided, the stream will contain only events that are emitted
    /// after the event with the provided [`EventID`]. Otherwise the event stream contains all
    /// events available from the connected full node. Since the full node may prune old
    /// events, the stream is not guaranteed to contain historic events.
    fn event_stream(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> impl Future<Output = SuiClientResult<impl Stream<Item = ContractEvent> + Send>> + Send;

    /// Returns the blob event with the given Event ID.
    fn get_blob_event(
        &self,
        event_id: EventID,
    ) -> impl Future<Output = SuiClientResult<BlobEvent>> + Send;

    /// Returns the current committee.
    fn current_committee(&self) -> impl Future<Output = SuiClientResult<Committee>> + Send;

    /// Returns the previous committee.
    // INV: current_committee.epoch == previous_committee.epoch + 1
    fn previous_committee(&self) -> impl Future<Output = SuiClientResult<Committee>> + Send;

    /// Returns the committee that will become active in the next epoch.
    ///
    /// This committee is `None` until known.
    // INV: next_committee.epoch == current_committee.epoch + 1
    fn next_committee(&self) -> impl Future<Output = SuiClientResult<Option<Committee>>> + Send;

    /// Returns the current epoch state.
    fn epoch_state(&self) -> impl Future<Output = SuiClientResult<EpochState>> + Send;

    /// Returns the current epoch.
    fn current_epoch(&self) -> impl Future<Output = SuiClientResult<Epoch>> + Send;

    /// Returns the current, previous, and next committee, along with the current epoch state.
    ///
    /// The order of the returned tuple is `(current, previous, Option<next>, epoch_state)`.
    fn get_committees_and_state(
        &self,
    ) -> impl Future<Output = SuiClientResult<CommitteesAndState>> + Send;

    /// Returns the non-variable system parameters.
    ///
    /// These include the number of shards, epoch duration, and the time at which epoch zero ends
    /// and epoch 1 can start.
    fn fixed_system_parameters(
        &self,
    ) -> impl Future<Output = SuiClientResult<FixedSystemParameters>> + Send;

    /// Returns the mapping between node IDs and stake in the staking object.
    fn stake_assignment(
        &self,
    ) -> impl Future<Output = SuiClientResult<HashMap<ObjectID, u64>>> + Send;
}

/// The mutability of a shared object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mutability {
    /// The object is mutable.
    Mutable,
    /// The object is immutable.
    Immutable,
}

impl From<bool> for Mutability {
    fn from(value: bool) -> Self {
        if value {
            Self::Mutable
        } else {
            Self::Immutable
        }
    }
}

impl From<Mutability> for bool {
    fn from(value: Mutability) -> Self {
        matches!(value, Mutability::Mutable)
    }
}

/// Client implementation for interacting with the Walrus smart contracts.
#[derive(Clone)]
pub struct SuiReadClient {
    pub(crate) walrus_package_id: ObjectID,
    pub(crate) sui_client: SuiClient,
    pub(crate) system_object_id: ObjectID,
    pub(crate) staking_object_id: ObjectID,
    pub(crate) type_origin_map: TypeOriginMap,
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
        package_id: Option<ObjectID>,
    ) -> SuiClientResult<Self> {
        let walrus_package_id = package_id.unwrap_or(
            // We use `unwrap_or` here because we want to call the function even if the package ID
            // is provided to check if the system and staking objects exist.
            get_system_package_id_from_system_object(&sui_client, system_object_id).await?,
        );
        let type_origin_map = type_origin_map_for_package(&sui_client, walrus_package_id).await?;
        Ok(Self {
            walrus_package_id,
            sui_client,
            system_object_id,
            staking_object_id,
            type_origin_map,
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
        package_id: Option<ObjectID>,
    ) -> SuiClientResult<Self> {
        let client = SuiClientBuilder::default().build(rpc_address).await?;
        Self::new(client, system_object, staking_object, package_id).await
    }

    pub(crate) async fn object_arg_for_shared_obj(
        &self,
        object_id: ObjectID,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let initial_shared_version = self.get_shared_object_initial_version(object_id).await?;
        Ok(ObjectArg::SharedObject {
            id: object_id,
            initial_shared_version,
            mutable: mutable.into(),
        })
    }

    pub(crate) async fn object_arg_for_system_obj(
        &self,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let initial_shared_version = self.system_object_initial_version().await?;
        Ok(ObjectArg::SharedObject {
            id: self.system_object_id,
            initial_shared_version,
            mutable: mutable.into(),
        })
    }

    async fn system_object_initial_version(&self) -> SuiClientResult<SequenceNumber> {
        let initial_shared_version = self
            .sys_obj_initial_version
            .get_or_try_init(|| self.get_shared_object_initial_version(self.system_object_id))
            .await?;
        Ok(*initial_shared_version)
    }

    pub(crate) async fn object_arg_for_staking_obj(
        &self,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let initial_shared_version = self.staking_object_initial_version().await?;
        Ok(ObjectArg::SharedObject {
            id: self.staking_object_id,
            initial_shared_version,
            mutable: mutable.into(),
        })
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
        self.walrus_package_id
    }

    /// Returns the system object ID.
    pub fn get_system_object_id(&self) -> ObjectID {
        self.system_object_id
    }

    /// Returns the staking object ID.
    pub fn get_staking_object_id(&self) -> ObjectID {
        self.staking_object_id
    }

    /// Returns the balance of the owner for the given coin type.
    pub(crate) async fn balance(
        &self,
        owner_address: SuiAddress,
        coin_type: CoinType,
    ) -> SuiClientResult<u64> {
        let coin_type_option = match coin_type {
            CoinType::Wal => Some(self.wal_coin_type()),
            CoinType::Sui => None,
        };
        Ok(self
            .sui_client
            .coin_read_api()
            .get_balance(owner_address, coin_type_option)
            .await?
            .total_balance
            .try_into()
            .expect("balances should fit into a u64"))
    }

    /// Returns a vector of coins of provided `coin_type` whose total balance is at least `balance`.
    ///
    /// Returns a [`SuiClientError::NoCompatibleGasCoins`] or
    /// [`SuiClientError::NoCompatibleWalCoins`] error if no coins of sufficient total balance are
    /// found.
    pub async fn get_coins_with_total_balance(
        &self,
        owner_address: SuiAddress,
        coin_type: CoinType,
        min_balance: u64,
        exclude: Vec<ObjectID>,
    ) -> SuiClientResult<Vec<Coin>> {
        let coin_type_option = match coin_type {
            CoinType::Wal => Some(self.wal_coin_type()),
            CoinType::Sui => None,
        };
        self.sui_client
            .coin_read_api()
            .select_coins(owner_address, coin_type_option, min_balance.into(), exclude)
            .await
            .map_err(|err| match err {
                sui_sdk::error::Error::InsufficientFund {
                    address: _,
                    amount: _,
                } => match coin_type {
                    CoinType::Wal => SuiClientError::NoCompatibleWalCoins,
                    CoinType::Sui => SuiClientError::NoCompatibleGasCoins,
                },
                err => SuiClientError::from(err),
            })
    }

    /// Get the [`StorageNodeCap`] object associated with the address.
    ///
    /// Returns an error if there is more than one [`StorageNodeCap`] object associated with the
    /// address.
    pub async fn get_address_capability_object(
        &self,
        owner: SuiAddress,
    ) -> SuiClientResult<Option<StorageNodeCap>> {
        let mut node_capabilities = self.get_owned_objects::<StorageNodeCap>(owner, &[]).await?;

        match node_capabilities.next() {
            Some(cap) => {
                if node_capabilities.next().is_some() {
                    return Err(SuiClientError::MultipleStorageNodeCapabilities);
                }
                Ok(Some(cap))
            }
            None => Ok(None),
        }
    }

    /// Get all the owned objects of the specified type for the specified owner.
    ///
    /// If some of the returned objects cannot be converted to the expected type, they are ignored.
    pub(crate) async fn get_owned_objects<'a, U>(
        &'a self,
        owner: SuiAddress,
        type_args: &'a [TypeTag],
    ) -> Result<impl Iterator<Item = U> + 'a>
    where
        U: AssociatedContractStruct,
    {
        let results = self
            .get_owned_object_data(owner, type_args, U::CONTRACT_STRUCT)
            .await?;

        Ok(results.filter_map(|object_data| {
            object_data.map_or_else(
                |error| {
                    tracing::warn!(?error, "failed to convert to local type");
                    None
                },
                |object_data| match U::try_from_object_data(&object_data) {
                    Result::Ok(value) => Some(value),
                    Result::Err(error) => {
                        tracing::warn!(?error, "failed to convert to local type");
                        None
                    }
                },
            )
        }))
    }

    /// Get all the [`SuiObjectData`] objects of the specified type for the specified owner.
    async fn get_owned_object_data<'a>(
        &'a self,
        owner: SuiAddress,
        type_args: &'a [TypeTag],
        object_type: contracts::StructTag<'a>,
    ) -> Result<impl Iterator<Item = Result<SuiObjectData>> + 'a> {
        let struct_tag =
            object_type.to_move_struct_tag_with_type_map(&self.type_origin_map, type_args)?;
        Ok(handle_pagination(move |cursor| {
            self.sui_client.read_api().get_owned_objects(
                owner,
                Some(SuiObjectResponseQuery {
                    filter: Some(SuiObjectDataFilter::StructType(struct_tag.clone())),
                    options: Some(SuiObjectDataOptions::new().with_bcs().with_type()),
                }),
                cursor,
                None,
            )
        })
        .await?
        .map(|resp| {
            resp.data
                .ok_or_else(|| anyhow!("response does not contain object data"))
        }))
    }

    /// Get the latest object reference given an [`ObjectID`].
    pub(crate) async fn get_object_ref(
        &self,
        object_id: ObjectID,
    ) -> Result<ObjectRef, anyhow::Error> {
        Ok(self
            .sui_client
            .read_api()
            .get_object_with_options(object_id, SuiObjectDataOptions::new())
            .await?
            .into_object()?
            .object_ref())
    }

    pub(crate) async fn object_arg_for_object(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<ObjectArg> {
        Ok(ObjectArg::ImmOrOwnedObject(
            self.get_object_ref(object_id).await?,
        ))
    }

    /// Returns the type of the WAL coin.
    pub fn wal_coin_type(&self) -> String {
        format!("{}::wal::WAL", self.walrus_package_id)
    }

    async fn get_system_object(&self) -> SuiClientResult<SystemObject> {
        let SystemObjectForDeserialization { id, version } =
            get_sui_object(&self.sui_client, self.system_object_id).await?;
        let inner = get_dynamic_field_object::<u64, SystemStateInnerV1>(
            &self.sui_client,
            self.system_object_id,
            TypeTag::U64,
            version,
        )
        .await?;

        Ok(SystemObject { id, version, inner })
    }

    async fn get_staking_object(&self) -> SuiClientResult<StakingObject> {
        let StakingObjectForDeserialization { id, version } =
            get_sui_object(&self.sui_client, self.staking_object_id).await?;

        let inner = get_dynamic_field_object::<u64, StakingInnerV1>(
            &self.sui_client,
            self.staking_object_id,
            TypeTag::U64,
            version,
        )
        .await?;
        Ok(StakingObject { id, version, inner })
    }

    async fn shard_assignment_to_committee(
        &self,
        epoch: Epoch,
        n_shards: NonZeroU16,
        shard_assignment: &[(ObjectID, Vec<u16>)],
    ) -> SuiClientResult<Committee> {
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
        Committee::new(nodes, epoch, n_shards).map_err(|err| SuiClientError::Internal(err.into()))
    }

    /// Queries the full note and gets the requested committee from the staking object.
    async fn query_staking_for_committee(
        &self,
        which_committee: WhichCommittee,
    ) -> SuiClientResult<Option<Committee>> {
        let staking_object = self.get_staking_object().await?;
        let epoch = staking_object.inner.epoch;
        let n_shards = staking_object.inner.n_shards;

        let (committee, committee_epoch) = match which_committee {
            WhichCommittee::Current => (Some(staking_object.inner.committee), epoch),
            WhichCommittee::Previous => (Some(staking_object.inner.previous_committee), epoch - 1),
            WhichCommittee::Next => (staking_object.inner.next_committee, epoch + 1),
        };

        if let Some(shard_assignment) = committee {
            Ok(Some(
                self.shard_assignment_to_committee(committee_epoch, n_shards, &shard_assignment)
                    .await?,
            ))
        } else {
            Ok(None)
        }
    }
}

enum WhichCommittee {
    Current,
    Previous,
    Next,
}

impl ReadClient for SuiReadClient {
    #[tracing::instrument(err, skip(self))]
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

    async fn storage_and_write_price_per_unit_size(&self) -> SuiClientResult<(u64, u64)> {
        let system_object = self.get_system_object().await?.inner;
        Ok((
            system_object.storage_price_per_unit_size,
            system_object.write_price_per_unit_size,
        ))
    }

    async fn event_stream(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = ContractEvent>> {
        let (tx_event, rx_event) = mpsc::channel::<ContractEvent>(EVENT_CHANNEL_CAPACITY);

        let event_api = self.sui_client.event_api().clone();

        let event_filter = EventFilter::MoveEventModule {
            package: self.walrus_package_id,
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
        tracing::debug!("getting current committee from Sui");
        self.query_staking_for_committee(WhichCommittee::Current)
            .await
            .map(|committee| {
                committee.expect("the current committee is always defined in the staking object")
            })
    }

    async fn previous_committee(&self) -> SuiClientResult<Committee> {
        tracing::debug!("getting previous committee from Sui");
        self.query_staking_for_committee(WhichCommittee::Previous)
            .await
            .map(|committee| {
                committee.expect("the previous committee is always defined in the staking object")
            })
    }

    async fn next_committee(&self) -> SuiClientResult<Option<Committee>> {
        tracing::debug!("getting next committee from Sui");
        self.query_staking_for_committee(WhichCommittee::Next).await
    }

    async fn epoch_state(&self) -> SuiClientResult<EpochState> {
        self.get_staking_object()
            .await
            .map(|staking| staking.inner.epoch_state)
    }

    async fn current_epoch(&self) -> SuiClientResult<Epoch> {
        self.get_staking_object()
            .await
            .map(|staking| staking.inner.epoch)
    }

    async fn get_committees_and_state(&self) -> SuiClientResult<CommitteesAndState> {
        let staking_object = self.get_staking_object().await?;
        let epoch = staking_object.inner.epoch;
        let n_shards = staking_object.inner.n_shards;

        let current = self
            .shard_assignment_to_committee(epoch, n_shards, &staking_object.inner.committee)
            .await?;
        let previous = if epoch == 0 {
            // There is no previous epoch.
            None
        } else {
            Some(
                self.shard_assignment_to_committee(
                    epoch - 1,
                    n_shards,
                    &staking_object.inner.previous_committee,
                )
                .await?,
            )
        };
        let epoch_state = staking_object.inner.epoch_state;
        let next = if let Some(next_committee_assignment) = staking_object.inner.next_committee {
            Some(
                self.shard_assignment_to_committee(epoch + 1, n_shards, &next_committee_assignment)
                    .await?,
            )
        } else {
            None
        };

        Ok(CommitteesAndState {
            current,
            previous,
            next,
            epoch_state,
        })
    }

    async fn fixed_system_parameters(&self) -> SuiClientResult<FixedSystemParameters> {
        let staking_object = self.get_staking_object().await?.inner;
        let system_object = self.get_system_object().await?.inner;
        let first_epoch_start = i64::try_from(staking_object.first_epoch_start)
            .context("first-epoch start time does not fit in i64")?;

        Ok(FixedSystemParameters {
            n_shards: staking_object.n_shards,
            max_epochs_ahead: system_object.future_accounting.length(),
            epoch_duration: Duration::from_millis(staking_object.epoch_duration),
            epoch_zero_end: DateTime::<Utc>::from_timestamp_millis(first_epoch_start).ok_or_else(
                || anyhow!("invalid first_epoch_start timestamp received from contracts"),
            )?,
        })
    }

    async fn stake_assignment(&self) -> SuiClientResult<HashMap<ObjectID, u64>> {
        let staking_object = self.get_staking_object().await?.inner;
        Ok(staking_object.active_set.nodes.into_iter().collect())
    }
}

impl fmt::Debug for SuiReadClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SuiReadClient")
            .field("system_pkg", &self.walrus_package_id)
            .field("sui_client", &"<redacted>")
            .field("system_object", &self.system_object_id)
            .finish()
    }
}

/// Checks if the Walrus system object exist on chain and returns the Walrus package ID.
async fn get_system_package_id_from_system_object(
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

/// Gets the type origin map for a given package.
async fn type_origin_map_for_package(
    sui_client: &SuiClient,
    package_id: ObjectID,
) -> Result<TypeOriginMap> {
    let Ok(Some(SuiRawData::Package(raw_package))) = sui_client
        .read_api()
        .get_object_with_options(
            package_id,
            SuiObjectDataOptions::default().with_type().with_bcs(),
        )
        .await?
        .into_object()
        .map(|object| object.bcs)
    else {
        bail!(SuiClientError::WalrusPackageNotFound(package_id));
    };
    Ok(raw_package
        .type_origin_table
        .into_iter()
        .map(|origin| ((origin.module_name, origin.datatype_name), origin.package))
        .collect())
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
                let tx_event_ref = &tx_event;
                page_available = events.has_next_page;
                polling_interval = initial_polling_interval;

                for event in events.data {
                    last_event = Some(event.id);
                    let span = tracing::error_span!(
                        "sui-event",
                        event_id = ?event.id,
                        event_type = ?event.type_
                    );

                    let continue_or_exit = async move {
                        let event_obj = match event.try_into() {
                            Ok(event_obj) => event_obj,
                            Err(_) => {
                                tracing::error!("could not convert event");
                                return ControlFlow::Continue(());
                            }
                        };

                        match tx_event_ref.send(event_obj).await {
                            Ok(()) => {
                                tracing::debug!("received event");
                                ControlFlow::Continue(())
                            }
                            Err(_) => {
                                tracing::debug!("channel was closed by receiver");
                                ControlFlow::Break(())
                            }
                        }
                    }
                    .instrument(span)
                    .await;

                    if continue_or_exit.is_break() {
                        return Ok(());
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
