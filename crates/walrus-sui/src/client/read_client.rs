// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to call Walrus move functions from rust.

use std::{
    collections::HashMap,
    fmt::{self, Debug},
    future::Future,
    num::NonZeroU16,
    ops::ControlFlow,
    path::PathBuf,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
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
    },
    types::base_types::ObjectID,
};
use sui_types::{
    Identifier,
    TypeTag,
    base_types::{ObjectRef, SequenceNumber, SuiAddress},
    event::EventID,
    transaction::ObjectArg,
};
use tokio::sync::{OnceCell, mpsc};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tracing::Instrument as _;
use walrus_core::{Epoch, ensure};
use walrus_utils::backoff::ExponentialBackoffConfig;

use super::{
    SuiClientError,
    SuiClientResult,
    contract_config::ContractConfig,
    retry_client::{MULTI_GET_OBJ_LIMIT, RetriableSuiClient},
};
use crate::{
    contracts::{self, AssociatedContractStruct, TypeOriginMap},
    system_setup::compile_package,
    types::{
        BlobEvent,
        Committee,
        ContractEvent,
        StakingObject,
        StorageNode,
        StorageNodeCap,
        SystemObject,
        move_structs::{
            Blob,
            BlobAttribute,
            BlobWithAttribute,
            EpochState,
            EventBlob,
            NodeMetadata,
            SharedBlob,
            StakingInnerV1,
            StakingObjectForDeserialization,
            StakingPool,
            SubsidiesInnerKey,
            SystemObjectForDeserialization,
            SystemStateInnerV1,
            SystemStateInnerV1Enum,
            SystemStateInnerV1Testnet,
            WalrusSubsidies,
            WalrusSubsidiesForDeserialization,
            WalrusSubsidiesInner,
        },
    },
    utils::{get_sui_object_from_object_response, handle_pagination},
};

const EVENT_MODULE: &str = "events";

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

    /// Returns the storage nodes in the active set.
    fn get_storage_nodes_from_active_set(
        &self,
    ) -> impl Future<Output = Result<Vec<StorageNode>>> + Send;

    /// Returns the storage nodes in the current committee.
    fn get_storage_nodes_from_committee(
        &self,
    ) -> impl Future<Output = SuiClientResult<Vec<StorageNode>>> + Send;

    /// Returns the storage nodes with the given IDs.
    fn get_storage_nodes_by_ids(
        &self,
        node_ids: &[ObjectID],
    ) -> impl Future<Output = Result<Vec<StorageNode>>> + Send;

    /// Returns the metadata associated with a blob object.
    fn get_blob_attribute(
        &self,
        blob_object_id: &ObjectID,
    ) -> impl Future<Output = SuiClientResult<Option<BlobAttribute>>> + Send;

    /// Returns the blob object and its associated attributes given the object ID of either
    /// a blob object or a shared blob.
    fn get_blob_by_object_id(
        &self,
        blob_id: &ObjectID,
    ) -> impl Future<Output = SuiClientResult<BlobWithAttribute>> + Send;

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

    /// Returns the last certified event blob.
    fn last_certified_event_blob(
        &self,
    ) -> impl Future<Output = SuiClientResult<Option<EventBlob>>> + Send;

    /// Refreshes the Walrus package ID.
    ///
    /// Should be called after the contract is upgraded.
    fn refresh_package_id(&self) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Refreshes the credits package ID.
    ///
    /// Should be called after the `subsidies` contract for credits is upgraded.
    fn refresh_credits_package_id(&self) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Refreshes the walrus subsidies package ID.
    ///
    /// Should be called after the `walrus_subsidies` contract is upgraded.
    fn refresh_walrus_subsidies_package_id(
        &self,
    ) -> impl Future<Output = SuiClientResult<()>> + Send;

    /// Returns the version of the system object.
    fn system_object_version(&self) -> impl Future<Output = SuiClientResult<u64>> + Send;
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

/// Configuration and state for a shared object with a package ID that is not the walrus package.
/// E.g. for credits or walrus subsidies.
#[derive(Clone, Debug)]
pub struct SharedObjectWithPkgConfig {
    /// The package ID of the corresponding package
    pub package_id: ObjectID,
    /// The object ID of the object
    pub object_id: ObjectID,
    /// The initial version of the shared object when it was created
    initial_version: SequenceNumber,
    /// The type origin map for the package
    type_origin_map: TypeOriginMap,
}

/// Client implementation for interacting with the Walrus smart contracts.
#[derive(Clone)]
pub struct SuiReadClient {
    walrus_package_id: Arc<RwLock<ObjectID>>,
    sui_client: RetriableSuiClient,
    system_object_id: ObjectID,
    staking_object_id: ObjectID,
    type_origin_map: Arc<RwLock<TypeOriginMap>>,
    sys_obj_initial_version: OnceCell<SequenceNumber>,
    staking_obj_initial_version: OnceCell<SequenceNumber>,
    credits: Arc<RwLock<Option<SharedObjectWithPkgConfig>>>,
    walrus_subsidies: Arc<RwLock<Option<SharedObjectWithPkgConfig>>>,
    wal_type: String,
}

const MAX_POLLING_INTERVAL: Duration = Duration::from_secs(5);
const EVENT_CHANNEL_CAPACITY: usize = 1024;

impl SuiReadClient {
    /// Constructor for `SuiReadClient`.
    pub async fn new(
        sui_client: RetriableSuiClient,
        contract_config: &ContractConfig,
    ) -> SuiClientResult<Self> {
        let walrus_package_id = sui_client
            .get_system_package_id_from_system_object(contract_config.system_object)
            .await?;
        let type_origin_map = sui_client
            .type_origin_map_for_package(walrus_package_id)
            .await?;
        let wal_type = sui_client.wal_type_from_package(walrus_package_id).await?;
        let client = Self {
            walrus_package_id: Arc::new(RwLock::new(walrus_package_id)),
            sui_client,
            system_object_id: contract_config.system_object,
            staking_object_id: contract_config.staking_object,
            type_origin_map: Arc::new(RwLock::new(type_origin_map)),
            sys_obj_initial_version: OnceCell::new(),
            staking_obj_initial_version: OnceCell::new(),
            credits: Arc::new(RwLock::new(None)),
            walrus_subsidies: Arc::new(RwLock::new(None)),
            wal_type,
        };
        if let Some(credits_object_id) = contract_config.credits_object {
            client.set_credits_object(credits_object_id).await?;
        }
        if let Some(walrus_subsidies_object_id) = contract_config.walrus_subsidies_object {
            client
                .set_walrus_subsidies_object(walrus_subsidies_object_id)
                .await?;
        }
        Ok(client)
    }

    /// Constructs a new `SuiReadClient` around a [`RetriableSuiClient`] constructed for the
    /// provided fullnode's RPC address.
    pub async fn new_for_rpc_urls<S: AsRef<str>>(
        rpc_addresses: &[S],
        contract_config: &ContractConfig,
        backoff_config: ExponentialBackoffConfig,
    ) -> SuiClientResult<Self> {
        let client =
            RetriableSuiClient::new_for_rpc_urls(rpc_addresses, backoff_config, None).await?;
        Self::new(client, contract_config).await
    }

    /// Gets the [`RetriableSuiClient`] from the associated read client.
    pub fn sui_client(&self) -> &RetriableSuiClient {
        &self.sui_client
    }

    pub(crate) async fn object_arg_for_shared_obj(
        &self,
        object_id: ObjectID,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let initial_shared_version = self
            .sui_client
            .get_shared_object_initial_version(object_id)
            .await?;
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
            .get_or_try_init(|| {
                self.sui_client
                    .get_shared_object_initial_version(self.system_object_id)
            })
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

    pub(crate) async fn object_arg_for_credits_obj(
        &self,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let credits = self.credits.read().expect("mutex should not be poisoned");
        let credits = credits
            .as_ref()
            .ok_or_else(|| SuiClientError::Internal(anyhow!("credits object ID not found")))?;

        Ok(ObjectArg::SharedObject {
            id: credits.object_id,
            initial_shared_version: credits.initial_version,
            mutable: mutable.into(),
        })
    }

    pub(crate) async fn object_arg_for_walrus_subsidies_obj(
        &self,
        mutable: Mutability,
    ) -> SuiClientResult<ObjectArg> {
        let walrus_subsidies = self
            .walrus_subsidies
            .read()
            .expect("mutex should not be poisoned");
        let walrus_subsidies = walrus_subsidies.as_ref().ok_or_else(|| {
            SuiClientError::Internal(anyhow!("walrus subsidies object ID not found"))
        })?;

        Ok(ObjectArg::SharedObject {
            id: walrus_subsidies.object_id,
            initial_shared_version: walrus_subsidies.initial_version,
            mutable: mutable.into(),
        })
    }

    async fn staking_object_initial_version(&self) -> SuiClientResult<SequenceNumber> {
        let initial_shared_version = self
            .staking_obj_initial_version
            .get_or_try_init(|| {
                self.sui_client
                    .get_shared_object_initial_version(self.staking_object_id)
            })
            .await?;
        Ok(*initial_shared_version)
    }

    /// Returns the system package ID.
    pub fn get_system_package_id(&self) -> ObjectID {
        *self.walrus_package_id()
    }

    /// Returns the credits package ID.
    pub fn get_credits_package_id(&self) -> Option<ObjectID> {
        self.credits
            .read()
            .expect("mutex should not be poisoned")
            .as_ref()
            .map(|s| s.package_id)
    }

    /// Returns the walrus subsidies package ID.
    pub fn get_walrus_subsidies_package_id(&self) -> Option<ObjectID> {
        self.walrus_subsidies
            .read()
            .expect("mutex should not be poisoned")
            .as_ref()
            .map(|s| s.package_id)
    }

    /// Returns the system object ID.
    pub fn get_system_object_id(&self) -> ObjectID {
        self.system_object_id
    }

    /// Returns the staking object ID.
    pub fn get_staking_object_id(&self) -> ObjectID {
        self.staking_object_id
    }

    /// Returns the credits object ID.
    pub fn get_credits_object_id(&self) -> Option<ObjectID> {
        self.credits
            .read()
            .expect("mutex should not be poisoned")
            .as_ref()
            .map(|s| s.object_id)
    }

    /// Returns the walrus subsidies object ID.
    pub fn get_walrus_subsidies_object_id(&self) -> Option<ObjectID> {
        self.walrus_subsidies
            .read()
            .expect("mutex should not be poisoned")
            .as_ref()
            .map(|s| s.object_id)
    }

    /// Returns the contract config.
    pub fn contract_config(&self) -> ContractConfig {
        ContractConfig {
            system_object: self.system_object_id,
            staking_object: self.staking_object_id,
            credits_object: self
                .credits
                .read()
                .expect("mutex should not be poisoned")
                .as_ref()
                .map(|s| s.object_id),
            walrus_subsidies_object: self
                .walrus_subsidies
                .read()
                .expect("mutex should not be poisoned")
                .as_ref()
                .map(|s| s.object_id),
        }
    }

    /// Returns the staking pool for the given node ID.
    pub async fn get_staking_pool(&self, node_id: ObjectID) -> SuiClientResult<StakingPool> {
        self.sui_client.get_sui_object(node_id).await
    }

    fn walrus_package_id(&self) -> RwLockReadGuard<ObjectID> {
        self.walrus_package_id
            .read()
            .expect("mutex should not be poisoned")
    }

    fn walrus_package_id_mut(&self) -> RwLockWriteGuard<ObjectID> {
        self.walrus_package_id
            .write()
            .expect("mutex should not be poisoned")
    }

    /// Returns a mutable reference to the credits object.
    fn credits_mut(&self) -> RwLockWriteGuard<Option<SharedObjectWithPkgConfig>> {
        self.credits.write().expect("mutex should not be poisoned")
    }

    /// Returns a mutable reference to the walrus subsidies object.
    fn walrus_subsidies_mut(&self) -> RwLockWriteGuard<Option<SharedObjectWithPkgConfig>> {
        self.walrus_subsidies
            .write()
            .expect("mutex should not be poisoned")
    }

    pub(crate) fn type_origin_map(&self) -> RwLockReadGuard<TypeOriginMap> {
        self.type_origin_map
            .read()
            .expect("mutex should not be poisoned")
    }

    fn type_origin_map_mut(&self) -> RwLockWriteGuard<TypeOriginMap> {
        self.type_origin_map
            .write()
            .expect("mutex should not be poisoned")
    }

    /// Returns the balance of the owner for the given coin type.
    pub(crate) async fn balance(
        &self,
        owner_address: SuiAddress,
        coin_type: CoinType,
    ) -> SuiClientResult<u64> {
        let coin_type_option = match coin_type {
            CoinType::Wal => Some(self.wal_coin_type().to_owned()),
            CoinType::Sui => None,
        };
        Ok(self
            .sui_client
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
            CoinType::Wal => Some(self.wal_coin_type().to_owned()),
            CoinType::Sui => None,
        };
        self.sui_client
            .select_coins(owner_address, coin_type_option, min_balance.into(), exclude)
            .await
            .map_err(|err| match err {
                SuiClientError::SuiSdkError(sui_sdk::error::Error::InsufficientFund {
                    address: _,
                    amount,
                }) => match coin_type {
                    CoinType::Wal => SuiClientError::NoCompatibleWalCoins,
                    CoinType::Sui => SuiClientError::NoCompatibleGasCoins(Some(amount)),
                },
                err => err,
            })
    }

    /// Returns the digest of the package at `package_path` for the currently active sui network.
    pub async fn compute_package_digest(&self, package_path: PathBuf) -> SuiClientResult<[u8; 32]> {
        // Compile package to get the digest.
        let chain_id = self.sui_client().get_chain_identifier().await.ok();
        tracing::info!(?chain_id, "chain identifier");
        let (compiled_package, _build_config) =
            compile_package(package_path, Default::default(), chain_id).await?;
        let digest = compiled_package.get_package_digest(false);
        Ok(digest)
    }

    pub(crate) async fn get_compatible_gas_coins(
        &self,
        sender_address: SuiAddress,
        min_balance: u64,
    ) -> SuiClientResult<Vec<ObjectRef>> {
        Ok(self
            .get_coins_with_total_balance(sender_address, CoinType::Sui, min_balance, vec![])
            .await?
            .iter()
            .map(Coin::object_ref)
            .collect())
    }

    /// Get the reference gas price for the current epoch.
    pub async fn get_reference_gas_price(&self) -> SuiClientResult<u64> {
        self.sui_client.get_reference_gas_price().await
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
            object_type.to_move_struct_tag_with_type_map(&self.type_origin_map(), type_args)?;
        Ok(handle_pagination(move |cursor| {
            self.sui_client.get_owned_objects(
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
            resp.data.ok_or_else(|| {
                anyhow!(
                    "response does not contain object data [err={:?}]",
                    resp.error
                )
            })
        }))
    }

    /// Get the latest object reference given an [`ObjectID`].
    pub(crate) async fn get_object_ref(
        &self,
        object_id: ObjectID,
    ) -> Result<ObjectRef, anyhow::Error> {
        Ok(self
            .sui_client
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
    pub fn wal_coin_type(&self) -> &str {
        &self.wal_type
    }

    /// Returns the system object.
    pub async fn get_system_object(&self) -> SuiClientResult<SystemObject> {
        let SystemObjectForDeserialization {
            id,
            version,
            package_id,
            new_package_id,
        } = self.system_object_for_deserialization().await?;
        // Refresh the package ID if it is different from the current package ID.
        if package_id != *self.walrus_package_id() {
            self.refresh_package_id_with_id(package_id).await?;
        }
        let inner = if let Ok(inner) = self
            .sui_client
            .get_dynamic_field::<u64, SystemStateInnerV1>(
                self.system_object_id,
                TypeTag::U64,
                version,
            )
            .await
        {
            SystemStateInnerV1Enum::V1(inner)
        } else {
            let inner = self
                .sui_client
                .get_dynamic_field::<u64, SystemStateInnerV1Testnet>(
                    self.system_object_id,
                    TypeTag::U64,
                    version,
                )
                .await?;
            SystemStateInnerV1Enum::V1Testnet(inner)
        };
        Ok(SystemObject {
            id,
            version,
            package_id,
            new_package_id,
            inner,
        })
    }

    /// Checks if the walrus subsidies object
    /// ([`contracts::walrus_subsidies::WalrusSubsidies`]) exist on chain and returns the object.
    pub async fn get_walrus_subsidies_object(
        &self,
        with_inner: bool,
    ) -> SuiClientResult<WalrusSubsidies> {
        let walrus_subsidies = self
            .walrus_subsidies
            .read()
            .expect("RwLock should not be poisoned")
            .as_ref()
            .cloned();

        let Some(walrus_subsidies) = walrus_subsidies else {
            return Err(SuiClientError::WalrusSubsidiesNotConfigured);
        };

        let deserialized_object = self
            .sui_client
            .get_sui_object::<WalrusSubsidiesForDeserialization>(walrus_subsidies.object_id)
            .await?;
        let inner = if with_inner {
            let key_tag = contracts::walrus_subsidies::SubsidiesInnerKey
                .to_move_struct_tag_with_type_map(&walrus_subsidies.type_origin_map, &[])?;
            let inner = self
                .sui_client
                .get_dynamic_field::<SubsidiesInnerKey, WalrusSubsidiesInner>(
                    walrus_subsidies.object_id,
                    key_tag.into(),
                    SubsidiesInnerKey { dummy_field: false },
                )
                .await?;
            Some(inner)
        } else {
            None
        };
        Ok(WalrusSubsidies {
            id: deserialized_object.id,
            version: deserialized_object.version,
            package_id: deserialized_object.package_id,
            inner,
        })
    }

    /// Returns the staking object.
    pub async fn get_staking_object(&self) -> SuiClientResult<StakingObject> {
        let StakingObjectForDeserialization {
            id,
            version,
            package_id,
            new_package_id,
        } = self
            .sui_client
            .get_sui_object(self.staking_object_id)
            .await?;
        // Refresh the package ID if it is different from the current package ID.
        if package_id != *self.walrus_package_id() {
            self.refresh_package_id_with_id(package_id).await?;
        }
        let inner = self
            .sui_client
            .get_dynamic_field::<u64, StakingInnerV1>(self.staking_object_id, TypeTag::U64, version)
            .await?;
        let staking_object = StakingObject {
            id,
            version,
            package_id,
            new_package_id,
            inner,
        };
        Ok(staking_object)
    }

    /// Sets a credit (`subsidies::Subsidies` in Move) object to be used by the client.
    pub async fn set_credits_object(&self, object_id: ObjectID) -> SuiClientResult<()> {
        let package_id = self
            .sui_client
            .get_credits_object(object_id)
            .await?
            .package_id;
        let initial_version = self
            .sui_client
            .get_shared_object_initial_version(object_id)
            .await?;
        let type_origin_map = self
            .sui_client
            .type_origin_map_for_package(package_id)
            .await?;
        *self.credits_mut() = Some(SharedObjectWithPkgConfig {
            package_id,
            object_id,
            initial_version,
            type_origin_map,
        });
        Ok(())
    }

    /// Sets a walrus subsidies ([`contracts::walrus_subsidies::WalrusSubsidies`]) object to be
    /// used by the client.
    pub async fn set_walrus_subsidies_object(&self, object_id: ObjectID) -> SuiClientResult<()> {
        let package_id = self
            .sui_client
            .get_sui_object::<WalrusSubsidiesForDeserialization>(object_id)
            .await?
            .package_id;
        let initial_version = self
            .sui_client
            .get_shared_object_initial_version(object_id)
            .await?;
        let type_origin_map = self
            .sui_client
            .type_origin_map_for_package(package_id)
            .await?;
        *self.walrus_subsidies_mut() = Some(SharedObjectWithPkgConfig {
            package_id,
            object_id,
            initial_version,
            type_origin_map,
        });
        Ok(())
    }

    async fn refresh_package_id_with_id(&self, walrus_package_id: ObjectID) -> SuiClientResult<()> {
        let type_origin_map = self
            .sui_client
            .type_origin_map_for_package(walrus_package_id)
            .await?;
        *self.walrus_package_id_mut() = walrus_package_id;
        *self.type_origin_map_mut() = type_origin_map;
        Ok(())
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

    /// Returns the backoff configuration for the inner client.
    #[cfg(feature = "test-utils")]
    pub(crate) fn backoff_config(&self) -> &ExponentialBackoffConfig {
        self.sui_client.backoff_config()
    }

    /// Returns the node metadata for the given metadata ID.
    pub async fn get_node_metadata(&self, metadata_id: ObjectID) -> SuiClientResult<NodeMetadata> {
        let type_map = self.type_origin_map().clone();
        let metadata = self
            .sui_client
            .get_extended_field::<NodeMetadata>(metadata_id, &type_map)
            .await?;
        Ok(metadata)
    }

    /// Returns the system object for deserialization without querying the dynamic inner field.
    async fn system_object_for_deserialization(
        &self,
    ) -> SuiClientResult<SystemObjectForDeserialization> {
        self.sui_client.get_sui_object(self.system_object_id).await
    }

    /// Returns the time at which process_subsidies was last called on the walrus subsidies object.
    pub async fn last_walrus_subsidies_call(&self) -> SuiClientResult<DateTime<Utc>> {
        Ok(self
            .get_walrus_subsidies_object(true)
            .await?
            .inner
            .ok_or_else(|| anyhow!("could not retrieve inner subsidies object"))?
            .last_subsidized)
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
            .storage_price_per_unit_size())
    }

    async fn write_price_per_unit_size(&self) -> SuiClientResult<u64> {
        Ok(self.get_system_object().await?.write_price_per_unit_size())
    }

    async fn storage_and_write_price_per_unit_size(&self) -> SuiClientResult<(u64, u64)> {
        let system_object = self.get_system_object().await?;
        Ok((
            system_object.storage_price_per_unit_size(),
            system_object.write_price_per_unit_size(),
        ))
    }

    async fn event_stream(
        &self,
        polling_interval: Duration,
        cursor: Option<EventID>,
    ) -> SuiClientResult<impl Stream<Item = ContractEvent>> {
        let (tx_event, rx_event) = mpsc::channel::<ContractEvent>(EVENT_CHANNEL_CAPACITY);

        // Note: this code does not handle failing over in the event of an RPC connection error.
        #[allow(deprecated)]
        let event_api = self
            .sui_client
            .get_current_client()
            .await
            .event_api()
            .clone();

        let event_filter = EventFilter::MoveEventModule {
            package: *self
                .walrus_package_id
                .read()
                .expect("mutex should not be poisoned"),
            module: Identifier::new(EVENT_MODULE)?,
        };
        tokio::spawn(async move {
            poll_for_events(tx_event, polling_interval, event_api, event_filter, cursor).await
        });
        Ok(ReceiverStream::new(rx_event))
    }

    async fn last_certified_event_blob(&self) -> SuiClientResult<Option<EventBlob>> {
        let blob = self
            .get_system_object()
            .await?
            .latest_certified_event_blob();
        Ok(blob)
    }

    async fn get_blob_event(&self, event_id: EventID) -> SuiClientResult<BlobEvent> {
        self.sui_client
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

    async fn get_storage_nodes_from_active_set(&self) -> Result<Vec<StorageNode>> {
        let node_ids: Vec<ObjectID> = self.stake_assignment().await?.keys().copied().collect();
        self.get_storage_nodes_by_ids(&node_ids).await
    }

    async fn get_storage_nodes_from_committee(&self) -> SuiClientResult<Vec<StorageNode>> {
        let committee = self.current_committee().await?;
        Ok(committee.members().to_vec())
    }

    async fn get_storage_nodes_by_ids(&self, node_ids: &[ObjectID]) -> Result<Vec<StorageNode>> {
        Ok(self
            .sui_client
            .get_sui_objects::<StakingPool>(node_ids)
            .await
            .context("one or multiple node IDs were not found")?
            .into_iter()
            .map(|pool| pool.node_info)
            .collect())
    }

    async fn get_blob_attribute(
        &self,
        blob_object_id: &ObjectID,
    ) -> SuiClientResult<Option<BlobAttribute>> {
        self.sui_client
            .get_dynamic_field::<Vec<u8>, BlobAttribute>(
                *blob_object_id,
                TypeTag::Vector(Box::new(TypeTag::U8)),
                b"metadata".to_vec(),
            )
            .await
            .map(Some)
            .or_else(|_| Ok(None))
    }

    async fn get_blob_by_object_id(
        &self,
        blob_object_id: &ObjectID,
    ) -> SuiClientResult<BlobWithAttribute> {
        let blob_object_response = self
            .sui_client
            .get_object_with_options(
                *blob_object_id,
                SuiObjectDataOptions::new().with_bcs().with_type(),
            )
            .await?;
        let blob = if let Ok(blob) =
            get_sui_object_from_object_response::<Blob>(&blob_object_response)
        {
            blob
        } else {
            let shared_blob = get_sui_object_from_object_response::<SharedBlob>(
                &blob_object_response,
            )
            .map_err(|_| {
                anyhow!("could not retrieve blob or shared blob from object id {blob_object_id}")
            })?;
            shared_blob.blob
        };
        let attribute = self.get_blob_attribute(&blob.id).await?;
        Ok(BlobWithAttribute { blob, attribute })
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
        let system_object = self.get_system_object().await?;
        let first_epoch_start = i64::try_from(staking_object.first_epoch_start)
            .context("first-epoch start time does not fit in i64")?;

        Ok(FixedSystemParameters {
            n_shards: staking_object.n_shards,
            max_epochs_ahead: system_object.future_accounting().length(),
            epoch_duration: Duration::from_millis(staking_object.epoch_duration),
            epoch_zero_end: DateTime::<Utc>::from_timestamp_millis(first_epoch_start).ok_or_else(
                || anyhow!("invalid first_epoch_start timestamp received from contracts"),
            )?,
        })
    }

    async fn stake_assignment(&self) -> SuiClientResult<HashMap<ObjectID, u64>> {
        use crate::types::move_structs::ActiveSet;

        let staking_object = self.get_staking_object().await?;
        let active_set_id = staking_object.inner.active_set;
        let type_map = self.type_origin_map().clone();

        let active_set = self
            .sui_client
            .get_extended_field::<ActiveSet>(active_set_id, &type_map)
            .await?;
        Ok(active_set.nodes.into_iter().collect())
    }

    async fn refresh_package_id(&self) -> SuiClientResult<()> {
        let walrus_package_id = self
            .sui_client
            .get_system_package_id_from_system_object(self.system_object_id)
            .await?;
        self.refresh_package_id_with_id(walrus_package_id).await
    }

    async fn refresh_credits_package_id(&self) -> SuiClientResult<()> {
        if let Some(credits_object_id) = self.get_credits_object_id() {
            self.set_credits_object(credits_object_id).await?;
        }
        Ok(())
    }

    async fn refresh_walrus_subsidies_package_id(&self) -> SuiClientResult<()> {
        if let Some(walrus_subsidies_object_id) = self.get_walrus_subsidies_object_id() {
            self.set_walrus_subsidies_object(walrus_subsidies_object_id)
                .await?;
        }
        Ok(())
    }

    async fn system_object_version(&self) -> SuiClientResult<u64> {
        Ok(self.system_object_for_deserialization().await?.version)
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
                // TODO (WAL-213): Stop retrying and switch to a different full node.
                // Ideally, we cut off the stream after retrying for a few times and then switch to
                // a different full node. This logic would need to be handled by a consumer of the
                // stream. Until that is in place, retry indefinitely.
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
