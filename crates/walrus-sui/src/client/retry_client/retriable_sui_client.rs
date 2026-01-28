// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`DualClient`] to introduce retries.
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashMap},
    pin::pin,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::Duration,
};

use anyhow::Context;
use futures::{Stream, StreamExt, future, stream};
use move_core_types::language_storage::StructTag;
use rand::{
    Rng as _,
    RngCore,
    SeedableRng,
    rngs::{StdRng, ThreadRng},
};
use serde::{Serialize, de::DeserializeOwned};
use sui_rpc::proto::sui::rpc::v2::Bcs;
use sui_sdk::{
    error::Error as SuiSdkError,
    rpc_types::{
        Balance,
        Coin,
        DryRunTransactionBlockResponse,
        ObjectsPage,
        SuiCommittee,
        SuiEvent,
        SuiMoveNormalizedModule,
        SuiMoveNormalizedStructType,
        SuiMoveNormalizedType,
        SuiObjectData,
        SuiObjectDataOptions,
        SuiObjectResponse,
        SuiObjectResponseQuery,
        SuiRawData,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
        SuiTransactionBlockResponseOptions,
        SuiTransactionBlockResponseQuery,
        TransactionBlocksPage,
    },
};
#[cfg(msim)]
use sui_types::transaction::TransactionDataAPI;
use sui_types::{
    TypeTag,
    base_types::{
        ObjectID,
        ObjectInfo,
        ObjectRef,
        ObjectType,
        SequenceNumber,
        SuiAddress,
        TransactionDigest,
    },
    dynamic_field::derive_dynamic_field_id,
    event::EventID,
    object::Owner,
    sui_serde::BigInt,
    transaction::{Transaction, TransactionData, TransactionKind},
    transaction_driver_types::ExecuteTransactionRequestType::WaitForLocalExecution,
};
use tracing::Level;
use walrus_core::ensure;
use walrus_utils::backoff::{ExponentialBackoff, ExponentialBackoffConfig};

use super::{
    FailoverWrapper,
    GAS_SAFE_OVERHEAD,
    MULTI_GET_OBJ_LIMIT,
    SuiClientError,
    SuiClientResult,
    failover::{FailoverError, LazyClientBuilder},
    retry_rpc_errors,
};
use crate::{
    client::{
        SuiClientMetricSet,
        dual_client::{BcsDatapack, DualClient},
    },
    contracts::{self, AssociatedContractStruct, MoveConversionError, TypeOriginMap},
    types::{
        BlobEvent,
        move_structs::{
            BlobAttribute,
            Credits,
            Key,
            StakingObjectForDeserialization,
            StakingPool,
            StorageNode,
            SuiDynamicField,
            SystemObjectForDeserialization,
        },
    },
    utils::{get_sui_object_from_bcs, get_sui_object_from_object_response},
};

/// The maximum gas allowed in a transaction, in MIST (50 SUI). Used for gas budget estimation.
const MAX_GAS_BUDGET: u64 = 50_000_000_000;
/// A dummy gas price used in the dry-run for gas budget estimation when the actual gas price is not
/// yet available.
const DUMMY_GAS_PRICE: u64 = 1000;
/// The maximum number of gas payment objects allowed in a transaction by the Sui protocol
/// configuration
/// [here](https://github.com/MystenLabs/sui/blob/main/crates/sui-protocol-config/src/lib.rs#L2089).
pub(crate) const MAX_GAS_PAYMENT_OBJECTS: usize = 256;

/// Default backoff delays used when building a `SuiClient`.
const CLIENT_BUILD_RETRY_MIN_DELAY: Duration = Duration::from_secs(1);
const CLIENT_BUILD_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
/// The maximum number of retries for building a SuiClient.
const RPC_MAX_TRIES: u32 = 3;

/// [`LazySuiClientBuilder`] has enough information to create a [`DualClient`], when its
/// [`LazyClientBuilder`] trait implementation is used.
#[derive(Debug, Clone)]
pub struct LazySuiClientBuilder {
    /// The URL of the RPC server.
    rpc_url: String,
    /// Override the default timeout for any requests.
    request_timeout: Option<Duration>,
}

impl LazySuiClientBuilder {
    /// Creates a new [`LazySuiClientBuilder`] from a URL and an optional `request_timeout`.
    pub fn new(rpc_url: impl AsRef<str>, request_timeout: Option<Duration>) -> Self {
        Self {
            rpc_url: rpc_url.as_ref().to_string(),
            request_timeout,
        }
    }
}

impl LazyClientBuilder<DualClient> for LazySuiClientBuilder {
    // TODO: WAL-796 Out of concern for consistency, we are disabling the failover mechanism for
    // SuiClient for now.
    const DEFAULT_MAX_TRIES: usize = 5;

    async fn lazy_build_client(&self) -> Result<Arc<DualClient>, FailoverError> {
        // Inject sui client build failure for simtests.
        #[cfg(msim)]
        {
            let mut fail_client_creation = false;
            sui_macros::fail_point_arg!(
                "failpoint_sui_client_build_client",
                |url_to_fail: String| {
                    if self.rpc_url == url_to_fail {
                        fail_client_creation = true;
                    }
                }
            );

            if fail_client_creation {
                tracing::info!("injected sui client build failure {:?}", self.get_rpc_url());
                return Err(FailoverError::FailedToGetClient(format!(
                    "injected sui client build failure {:?}",
                    self.get_rpc_url()
                )));
            }
        }

        let dual_client = retry_rpc_errors(
            ExponentialBackoffConfig::new(
                CLIENT_BUILD_RETRY_MIN_DELAY,
                CLIENT_BUILD_RETRY_MAX_DELAY,
                Some(RPC_MAX_TRIES),
            )
            .get_strategy(StdRng::from_entropy().next_u64()),
            || async { DualClient::new(self.rpc_url.as_str(), self.request_timeout).await },
            None,
            "build_sui_client",
        )
        .await
        .map_err(|e| {
            tracing::info!(
                "failed to get sui client from url {}, error: {}",
                self.rpc_url,
                e
            );
            FailoverError::FailedToGetClient(e.to_string())
        })?;
        Ok(Arc::new(dual_client))
    }

    fn get_rpc_url(&self) -> &str {
        self.rpc_url.as_str()
    }
}

/// A struct that contains the gas price and gas budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GasBudgetAndPrice {
    /// The gas budget.
    pub gas_budget: u64,
    /// The gas price.
    pub gas_price: u64,
}

/// gRPC Client Migration levels.
///
/// Note that values in this enum are cumulative, so higher levels include all the migrations of the
/// lower levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct GrpcMigrationLevel(u32);

const GRPC_MIGRATION_LEVEL_LEGACY_U32: u32 = 0;
const GRPC_MIGRATION_LEVEL_GET_OBJECT: GrpcMigrationLevel = GrpcMigrationLevel(1);
const GRPC_MIGRATION_LEVEL_BATCH_OBJECTS: GrpcMigrationLevel = GrpcMigrationLevel(2);

impl Default for GrpcMigrationLevel {
    fn default() -> Self {
        static VALUE: LazyLock<u32> = LazyLock::new(|| {
            std::env::var("WALRUS_GRPC_MIGRATION_LEVEL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(GRPC_MIGRATION_LEVEL_LEGACY_U32)
        });
        Self(*VALUE)
    }
}

pub(crate) fn get_initial_version_from_object_response(
    object_response: &SuiObjectResponse,
) -> SuiClientResult<SequenceNumber> {
    if let Some(Owner::Shared {
        initial_shared_version,
    }) = object_response.owner()
    {
        Ok(initial_shared_version)
    } else {
        Err(SuiClientError::Internal(anyhow::anyhow!(
            "trying to get the initial version of a non-shared object; object_id: {:?}",
            object_response.object_id(),
        )))
    }
}

pub(crate) fn get_initial_version_from_grpc_object(
    object: &sui_types::object::Object,
) -> SuiClientResult<SequenceNumber> {
    if let Owner::Shared {
        initial_shared_version,
    } = object.owner()
    {
        Ok(*initial_shared_version)
    } else {
        Err(SuiClientError::Internal(anyhow::anyhow!(
            "trying to get the initial version of a non-shared object; object_id: {:?}",
            object.id(),
        )))
    }
}

/// A [`DualClient`] that retries RPC calls with backoff in case of network errors.
///
/// This retriable client wraps functions from the [`CoinReadApi`][sui_sdk::apis::CoinReadApi] and
/// the [`ReadApi`][sui_sdk::apis::ReadApi] of the [`DualClient`], and
/// additionally provides some convenience methods.
#[derive(Clone, Debug)]
pub struct RetriableSuiClient {
    failover_sui_client: FailoverWrapper<DualClient, LazySuiClientBuilder>,
    backoff_config: ExponentialBackoffConfig,
    metrics: Option<Arc<SuiClientMetricSet>>,
    grpc_migration_level: GrpcMigrationLevel,
}

impl RetriableSuiClient {
    /// Creates a new retriable client.
    pub fn new(
        lazy_client_builders: Vec<LazySuiClientBuilder>,
        backoff_config: ExponentialBackoffConfig,
    ) -> anyhow::Result<Self> {
        Ok(RetriableSuiClient {
            failover_sui_client: FailoverWrapper::new(lazy_client_builders)
                .context("creating failover wrapper")?,
            backoff_config,
            grpc_migration_level: GrpcMigrationLevel::default(),
            metrics: None,
        })
    }

    /// Sets the metrics for the client.
    pub fn with_metrics(mut self, metrics: Option<Arc<SuiClientMetricSet>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Returns a reference to the inner backoff configuration.
    pub fn backoff_config(&self) -> &ExponentialBackoffConfig {
        &self.backoff_config
    }

    /// Creates a new retriable client from an RCP address.
    pub fn new_for_rpc_urls<S: AsRef<str>>(
        rpc_addresses: &[S],
        backoff_config: ExponentialBackoffConfig,
        request_timeout: Option<Duration>,
    ) -> SuiClientResult<Self> {
        let failover_clients = rpc_addresses
            .iter()
            .map(|rpc_url| LazySuiClientBuilder::new(rpc_url, request_timeout))
            .collect::<Vec<_>>();
        Ok(Self::new(failover_clients, backoff_config)?)
    }

    /// Fetches both the system object and the staking object.
    pub async fn fetch_system_and_staking_objects(
        &self,
        system_object_id: ObjectID,
        staking_object_id: ObjectID,
    ) -> SuiClientResult<(
        SystemObjectForDeserialization,
        SequenceNumber,
        StakingObjectForDeserialization,
        SequenceNumber,
    )> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_BATCH_OBJECTS {
            let objects_bcs_datapacks = self
                .multi_get_objects_bcs_versions(&[system_object_id, staking_object_id])
                .await?;
            let [system_object_bcs_datapack, staking_object_bcs_datapack] =
                objects_bcs_datapacks.as_slice()
            else {
                return Err(SuiClientError::Internal(anyhow::anyhow!(
                    "received an unexpected response when getting the system and staking objects",
                )));
            };
            Ok((
                get_sui_object_from_bcs(
                    system_object_bcs_datapack.bcs.value(),
                    &system_object_bcs_datapack.struct_tag,
                )?,
                system_object_bcs_datapack
                    .owner_version
                    .context("system object has missing owner_version")?
                    .into(),
                get_sui_object_from_bcs(
                    staking_object_bcs_datapack.bcs.value(),
                    &staking_object_bcs_datapack.struct_tag,
                )?,
                staking_object_bcs_datapack
                    .owner_version
                    .context("staking object has missing owner_version")?
                    .into(),
            ))
        } else {
            let object_responses = self
                .multi_get_object_with_options(
                    &[system_object_id, staking_object_id],
                    SuiObjectDataOptions::new()
                        .with_owner()
                        .with_bcs()
                        .with_type(),
                )
                .await?;
            let [system_object_response, staking_object_response] = object_responses.as_slice()
            else {
                return Err(SuiClientError::Internal(anyhow::anyhow!(
                    "received an unexpected response when getting the system and staking objects",
                )));
            };

            let system_object_for_deserialization: SystemObjectForDeserialization =
                get_sui_object_from_object_response(system_object_response)?;
            let system_object_initial_version =
                get_initial_version_from_object_response(system_object_response)?;
            let staking_object_for_deserialization: StakingObjectForDeserialization =
                get_sui_object_from_object_response(staking_object_response)?;
            let staking_object_initial_version =
                get_initial_version_from_object_response(staking_object_response)?;
            Ok((
                system_object_for_deserialization,
                system_object_initial_version,
                staking_object_for_deserialization,
                staking_object_initial_version,
            ))
        }
    }
    // Re-implementation of the `SuiClient` methods.

    /// Return a list of coins for the given address, or an error upon failure.
    ///
    /// Reimplements the functionality of [`sui_sdk::apis::CoinReadApi::select_coins`] with the
    /// addition of retries on network errors.
    #[tracing::instrument(skip(self, address, exclude), level = Level::DEBUG)]
    pub async fn select_coins(
        &self,
        address: SuiAddress,
        coin_type: Option<String>,
        amount: u128,
        exclude: Vec<ObjectID>,
    ) -> SuiClientResult<Vec<Coin>> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.select_coins_with_limit(
                    address,
                    coin_type.clone(),
                    amount,
                    exclude.clone(),
                    MAX_GAS_PAYMENT_OBJECTS,
                )
                .await
            },
            self.metrics.clone(),
            "select_coins",
        )
        .await
    }

    /// Returns a list of all coins for the given address, with a filter on the coin type. Note
    /// that coin_type of None is implicitly filtering for SUI.
    pub async fn select_all_coins(
        &self,
        address: SuiAddress,
        coin_type: Option<String>,
    ) -> SuiClientResult<Vec<Coin>> {
        let mut coins_stream = pin!(self.get_coins_stream_retry(address, coin_type.clone()));

        let mut selected_coins = Vec::new();
        while let Some(coin) = coins_stream.as_mut().next().await {
            selected_coins.push(coin);
        }
        Ok(selected_coins)
    }

    /// Returns a list of coins for the given address, or an error upon failure. This method always
    /// filters on coin types. When `coin_type` is `None`, it will filter for SUI. Otherwise, it
    /// will filter to the given coin type. It will attempt to gather coins to satisfy the given
    /// `amount`. `exclude` is a list of coin object IDs to exclude from the result.
    /// `max_num_coins` puts a hard cap on the number of coins returned.
    pub async fn select_coins_with_limit(
        &self,
        address: SuiAddress,
        coin_type: Option<String>,
        amount: u128,
        exclude: Vec<ObjectID>,
        max_num_coins: usize,
    ) -> SuiClientResult<Vec<Coin>> {
        let mut coins_stream = pin!(
            self.get_coins_stream_retry(address, coin_type.clone())
                .filter(|coin: &Coin| future::ready(!exclude.contains(&coin.coin_object_id)))
        );

        let mut selected_coins: BinaryHeap<Reverse<OrderedCoin>> = BinaryHeap::new();

        let mut total_selected = 0u128;
        let mut total_available = 0u128;

        while let Some(coin) = coins_stream.as_mut().next().await {
            let coin_balance = u128::from(coin.balance);
            total_available += coin_balance;
            if selected_coins.len() >= max_num_coins {
                let min_coin_balance = selected_coins
                    .peek()
                    .expect("heap is not empty")
                    .0
                    .balance();
                if min_coin_balance < coin.balance {
                    selected_coins.pop();
                    total_selected -= u128::from(min_coin_balance);
                } else {
                    continue;
                }
            }
            total_selected += coin_balance;
            selected_coins.push(Reverse(OrderedCoin::from(coin)));

            if total_selected >= amount {
                return Ok(selected_coins
                    .into_iter()
                    .map(|rev_coin| rev_coin.0.0)
                    .collect());
            }
        }

        if total_available < amount {
            // We don't have a sufficient balance in any case (given the excluded objects).
            Err(SuiSdkError::InsufficientFund { address, amount }.into())
        } else {
            // We ran out of coins and cannot get to `amount` with `max_num_coins`.
            Err(SuiClientError::InsufficientFundsWithMaxCoins(
                coin_type.unwrap_or_else(|| sui_sdk::SUI_COIN_TYPE.to_string()),
            ))
        }
    }

    /// Returns a stream of coins for the given address.
    ///
    /// This is a re-implementation of the [`sui_sdk::apis::CoinReadApi::get_coins_stream`] method
    /// in the [`sui_sdk::SuiClient`] struct. Unlike the original implementation, this version will
    /// retry failed RPC calls.
    fn get_coins_stream_retry(
        &self,
        owner: SuiAddress,
        coin_type: Option<String>,
    ) -> impl Stream<Item = Coin> + '_ {
        stream::unfold(
            (
                vec![],
                /* cursor */ None,
                /* has_next_page */ true,
                coin_type,
            ),
            move |(mut data, cursor, has_next_page, coin_type)| async move {
                if let Some(item) = data.pop() {
                    Some((item, (data, cursor, has_next_page, coin_type)))
                } else if has_next_page {
                    let page = self
                        .failover_sui_client
                        .with_failover(
                            async |client, method| {
                                retry_rpc_errors(
                                    self.get_strategy(),
                                    || async {
                                        client
                                            .sui_client()
                                            .coin_read_api()
                                            .get_coins(
                                                owner,
                                                coin_type.clone(),
                                                cursor.clone(),
                                                Some(100),
                                            )
                                            .await
                                            .map_err(SuiClientError::from)
                                    },
                                    self.metrics.clone(),
                                    method,
                                )
                                .await
                                .inspect_err(|error| {
                                    tracing::warn!(%error,
                                            "failed to get coins after retries")
                                })
                            },
                            None,
                            "get_coins",
                        )
                        .await
                        .ok()?;

                    let mut data = page.data;
                    data.reverse();
                    data.pop().map(|item| {
                        (
                            item,
                            (data, page.next_cursor, page.has_next_page, coin_type),
                        )
                    })
                } else {
                    None
                }
            },
        )
    }

    /// Returns the balance for the given coin type owned by address.
    ///
    /// Calls [`sui_sdk::apis::CoinReadApi::get_balance`] internally.
    #[tracing::instrument(level = Level::TRACE, skip_all)]
    pub async fn get_balance(
        &self,
        owner: SuiAddress,
        coin_type: Option<String>,
    ) -> SuiClientResult<Balance> {
        self.failover_sui_client
            .with_failover(
                async |client, method| {
                    Ok(retry_rpc_errors(
                        self.get_strategy(),
                        || async {
                            client
                                .sui_client()
                                .coin_read_api()
                                .get_balance(owner, coin_type.clone())
                                .await
                        },
                        self.metrics.clone(),
                        method,
                    )
                    .await?)
                },
                None,
                "get_balance",
            )
            .await
    }

    /// Return a paginated response with all transaction blocks information, or an error upon
    /// failure.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::query_transaction_blocks`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn query_transaction_blocks(
        &self,
        query: SuiTransactionBlockResponseQuery,
        cursor: Option<TransactionDigest>,
        limit: Option<usize>,
        descending_order: bool,
    ) -> SuiClientResult<TransactionBlocksPage> {
        async fn make_request(
            client: Arc<DualClient>,
            query: SuiTransactionBlockResponseQuery,
            cursor: Option<TransactionDigest>,
            limit: Option<usize>,
            descending_order: bool,
        ) -> SuiClientResult<TransactionBlocksPage> {
            Ok(client
                .sui_client()
                .read_api()
                .query_transaction_blocks(query.clone(), cursor, limit, descending_order)
                .await?)
        }
        let request = |client: Arc<DualClient>, method: &'static str| {
            let query = query.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || {
                    let query = query.clone();
                    make_request(client.clone(), query, cursor, limit, descending_order)
                },
                self.metrics.clone(),
                method,
            )
        };

        self.failover_sui_client
            .with_failover(request, None, "query_transaction_blocks")
            .await
    }

    /// Return a paginated response with the objects owned by the given address.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_owned_objects`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_owned_objects(
        &self,
        address: SuiAddress,
        query: Option<SuiObjectResponseQuery>,
        cursor: Option<ObjectID>,
        limit: Option<usize>,
    ) -> SuiClientResult<ObjectsPage> {
        async fn make_request(
            client: Arc<DualClient>,
            address: SuiAddress,
            query: Option<SuiObjectResponseQuery>,
            cursor: Option<ObjectID>,
            limit: Option<usize>,
        ) -> SuiClientResult<ObjectsPage> {
            Ok(client
                .sui_client()
                .read_api()
                .get_owned_objects(address, query, cursor, limit)
                .await?)
        }
        let request = |client: Arc<DualClient>, method: &'static str| {
            let query = query.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || {
                    let query = query.clone();
                    make_request(client.clone(), address, query, cursor, limit)
                },
                self.metrics.clone(),
                method,
            )
        };

        self.failover_sui_client
            .with_failover(request, None, "get_owned_objects")
            .await
    }

    /// Returns a [`sui_types::object::Object`] based on the provided [`ObjectID`].
    #[tracing::instrument(level = Level::DEBUG, skip(self))]
    async fn get_object_by_grpc(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<sui_types::object::Object> {
        debug_assert!(self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT);
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
        ) -> SuiClientResult<sui_types::object::Object> {
            client.get_object(object_id).await
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object")
            .await
    }

    /// Returns a [`sui_types::object::Object`] based on the provided [`ObjectID`].
    #[tracing::instrument(level = Level::DEBUG, skip(self))]
    async fn get_object_ref_with_grpc(&self, object_id: ObjectID) -> SuiClientResult<ObjectRef> {
        debug_assert!(self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT);
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
        ) -> SuiClientResult<ObjectRef> {
            client.get_object_ref(object_id).await
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object_ref")
            .await
    }

    /// Returns a [`SuiObjectResponse`] based on the provided [`ObjectID`].
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_object_with_options`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip(self))]
    async fn get_object_with_json_rpc(
        &self,
        object_id: ObjectID,
        options: SuiObjectDataOptions,
    ) -> SuiClientResult<SuiObjectResponse> {
        debug_assert!(self.grpc_migration_level < GRPC_MIGRATION_LEVEL_GET_OBJECT);
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
            options: SuiObjectDataOptions,
        ) -> SuiClientResult<SuiObjectResponse> {
            Ok(client
                .sui_client()
                .read_api()
                .get_object_with_options(object_id, options.clone())
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            let options = options.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id, options.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object_with_options")
            .await
    }

    /// Returns a [`Bcs`] based on the provided [`ObjectID`].
    #[tracing::instrument(level = Level::DEBUG, skip(self))]
    pub async fn get_object_contents(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<(StructTag, Bcs)> {
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
        ) -> SuiClientResult<(StructTag, Bcs)> {
            client.get_object_contents(object_id).await
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object_contents")
            .await
    }

    /// Returns a [`SuiTransactionBlockResponse`] based on the provided [`TransactionDigest`].
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_transaction_with_options`] internally.
    pub async fn get_transaction_with_options(
        &self,
        digest: TransactionDigest,
        options: SuiTransactionBlockResponseOptions,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        async fn make_request(
            client: Arc<DualClient>,
            digest: TransactionDigest,
            options: SuiTransactionBlockResponseOptions,
        ) -> SuiClientResult<SuiTransactionBlockResponse> {
            Ok(client
                .sui_client()
                .read_api()
                .get_transaction_with_options(digest, options.clone())
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            let options = options.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), digest, options.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_transaction")
            .await
    }

    /// Return a list of Objects from the given vector of [ObjectID]s.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn multi_get_objects(
        &self,
        object_ids: &[ObjectID],
    ) -> SuiClientResult<Vec<sui_types::object::Object>> {
        async fn make_request(
            client: Arc<DualClient>,
            object_ids: &[ObjectID],
        ) -> SuiClientResult<Vec<sui_types::object::Object>> {
            client.multi_get_objects(object_ids).await
        }

        let request = move |client: Arc<DualClient>, method| {
            let object_ids = object_ids;
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_ids),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "multi_get_objects")
            .await
    }

    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    async fn multi_get_objects_bcs_versions(
        &self,
        object_ids: &[ObjectID],
    ) -> SuiClientResult<Vec<BcsDatapack>> {
        async fn make_request(
            client: Arc<DualClient>,
            object_ids: &[ObjectID],
        ) -> SuiClientResult<Vec<BcsDatapack>> {
            client.multi_get_objects_contents_bcs(object_ids).await
        }

        let request = move |client: Arc<DualClient>, method| {
            let object_ids = object_ids;
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_ids),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "multi_get_objects_bcs_versions")
            .await
    }

    /// Return a list of [SuiObjectResponse] from the given vector of [ObjectID]s.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::multi_get_object_with_options`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn multi_get_object_with_options(
        &self,
        object_ids: &[ObjectID],
        options: SuiObjectDataOptions,
    ) -> SuiClientResult<Vec<SuiObjectResponse>> {
        async fn make_request(
            client: Arc<DualClient>,
            object_ids: &[ObjectID],
            options: SuiObjectDataOptions,
        ) -> SuiClientResult<Vec<SuiObjectResponse>> {
            Ok(client
                .sui_client()
                .read_api()
                .multi_get_object_with_options(object_ids.to_vec(), options)
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            let object_ids = object_ids;
            let options = options.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_ids, options.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "multi_get_object")
            .await
    }

    /// Returns the Sui Objects with the provided [`ObjectID`]s calling
    /// [`Self::multi_get_object_with_options`] in batches of the maximum number of objects allowed
    /// in a single RPC call.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn multi_get_object_with_options_batched(
        &self,
        object_ids: &[ObjectID],
        options: SuiObjectDataOptions,
    ) -> SuiClientResult<Vec<SuiObjectResponse>> {
        let results = futures::future::try_join_all(object_ids.chunks(MULTI_GET_OBJ_LIMIT).map(
            move |obj_id_batch| {
                let options = options.clone();
                async move {
                    self.multi_get_object_with_options(obj_id_batch, options)
                        .await
                }
            },
        ))
        .await?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Returns a map consisting of the move package name and the normalized module.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_normalized_move_modules_by_package`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_normalized_move_modules_by_package(
        &self,
        package_id: ObjectID,
    ) -> SuiClientResult<BTreeMap<String, SuiMoveNormalizedModule>> {
        async fn make_request(
            client: Arc<DualClient>,
            package_id: ObjectID,
        ) -> SuiClientResult<BTreeMap<String, SuiMoveNormalizedModule>> {
            Ok(client
                .sui_client()
                .read_api()
                .get_normalized_move_modules_by_package(package_id)
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), package_id),
                self.metrics.clone(),
                method,
            )
        };

        self.failover_sui_client
            .with_failover(request, None, "get_normalized_move_modules_by_package")
            .await
    }

    /// Returns the committee information for the given epoch.
    ///
    /// Calls [`sui_sdk::apis::GovernanceApi::get_committee_info`] internally.
    pub async fn get_committee_info(
        &self,
        epoch: Option<BigInt<u64>>,
    ) -> SuiClientResult<SuiCommittee> {
        async fn make_request(
            client: Arc<DualClient>,
            epoch: Option<BigInt<u64>>,
        ) -> SuiClientResult<SuiCommittee> {
            Ok(client
                .sui_client()
                .governance_api()
                .get_committee_info(epoch)
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), epoch),
                self.metrics.clone(),
                method,
            )
        };

        self.failover_sui_client
            .with_failover(request, None, "get_committee_info")
            .await
    }

    /// Returns the reference gas price.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_reference_gas_price`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_reference_gas_price(&self) -> SuiClientResult<u64> {
        async fn make_request(client: Arc<DualClient>) -> SuiClientResult<u64> {
            Ok(client
                .sui_client()
                .read_api()
                .get_reference_gas_price()
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_reference_gas_price")
            .await
    }

    /// Executes a transaction dry run.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::dry_run_transaction_block`] internally.
    pub async fn dry_run_transaction_block(
        &self,
        transaction: TransactionData,
    ) -> SuiClientResult<DryRunTransactionBlockResponse> {
        let transaction = Arc::new(transaction);
        async fn make_request(
            client: Arc<DualClient>,
            transaction: Arc<TransactionData>,
        ) -> SuiClientResult<DryRunTransactionBlockResponse> {
            let tx = TransactionData::clone(&transaction);
            Ok(client
                .sui_client()
                .read_api()
                .dry_run_transaction_block(tx)
                .await?)
        }

        let request = move |client: Arc<DualClient>, method| {
            let transaction = transaction.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), transaction.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "dry_run_transaction_block")
            .await
    }

    /// Returns a reference to the current internal [`DualClient`]. Avoid using this function.
    ///
    // TODO: WAL-778 find callsites to this method, and replace them with implementations that make
    // use of failover, since this call is a cheat to bypass the failover mechanism.
    #[deprecated(
        note = "please implement a full treatment in RetriableSuiClient for your use case"
    )]
    pub async fn get_current_client(&self) -> Arc<DualClient> {
        self.failover_sui_client
            .get_current_client()
            .await
            .expect("client must have been created")
    }

    /// Returns the Sui Object of type `U` with the provided [`ObjectID`].
    #[tracing::instrument(
        level = Level::DEBUG, skip_all, fields(object_type = %U::CONTRACT_STRUCT)
    )]
    pub async fn get_sui_object<U>(&self, object_id: ObjectID) -> SuiClientResult<U>
    where
        U: AssociatedContractStruct,
    {
        self.get_move_object_from_bcs(object_id, |_, struct_tag, bcs| {
            get_sui_object_from_bcs::<U>(bcs, struct_tag)
        })
        .await
    }

    /// Returns the Sui Object of type `U` with the provided [`ObjectID`] and a specified conversion
    /// function.
    #[tracing::instrument(
        level = Level::DEBUG, skip_all, fields(object_type = %U::CONTRACT_STRUCT)
    )]
    pub async fn get_move_object_from_bcs<F, U>(
        &self,
        object_id: ObjectID,
        conversion_fn: F,
    ) -> SuiClientResult<U>
    where
        U: AssociatedContractStruct,
        F: FnOnce(ObjectID, &StructTag, &[u8]) -> SuiClientResult<U>,
    {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            let (struct_tag, bcs) = self.get_object_contents(object_id).await?;
            conversion_fn(object_id, &struct_tag, bcs.value())
        } else {
            use sui_sdk::rpc_types::SuiData as _;
            let sui_object_response: SuiObjectResponse = self
                .get_object_with_json_rpc(
                    object_id,
                    SuiObjectDataOptions::new().with_bcs().with_type(),
                )
                .await?;
            let sui_object_data: SuiObjectData = sui_object_response
                .data
                .context("missing bcs data on object")?;
            let sui_raw_move_object = sui_object_data
                .bcs
                .as_ref()
                .ok_or(MoveConversionError::NoBcs)?
                .try_as_move()
                .ok_or(MoveConversionError::NotMoveObject)?;

            conversion_fn(
                object_id,
                &sui_raw_move_object.type_,
                &sui_raw_move_object.bcs_bytes,
            )
        }
    }

    /// Returns the Sui Objects of type `U` with the provided [`ObjectID`]s.
    #[tracing::instrument(
        level = Level::DEBUG, skip_all, fields(object_type = %U::CONTRACT_STRUCT)
    )]
    pub async fn get_sui_objects<U>(&self, object_ids: &[ObjectID]) -> SuiClientResult<Vec<U>>
    where
        U: AssociatedContractStruct,
    {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_BATCH_OBJECTS {
            let bcs_datapacks = self.multi_get_objects_bcs_versions(object_ids).await?;
            bcs_datapacks
                .into_iter()
                .map(|bcs_datapack| {
                    get_sui_object_from_bcs::<U>(
                        &bcs_datapack
                            .bcs
                            .value
                            .context("missing bcs data on object")?,
                        &bcs_datapack.struct_tag,
                    )
                })
                .collect()
        } else {
            let responses = self
                .multi_get_object_with_options_batched(
                    object_ids,
                    SuiObjectDataOptions::new().with_bcs().with_type(),
                )
                .await?;
            responses
                .into_iter()
                .map(|r| get_sui_object_from_object_response(&r))
                .collect::<Result<Vec<_>, _>>()
        }
    }

    /// Returns the [`ObjectRef`]s of the Sui Objects with the provided [`ObjectID`]s.
    pub async fn get_object_refs(
        &self,
        object_ids: &[ObjectID],
    ) -> SuiClientResult<Vec<ObjectRef>> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_BATCH_OBJECTS {
            let objects = self.multi_get_objects(object_ids).await?;
            objects
                .into_iter()
                .map(|object| {
                    let object_info = ObjectInfo::from_object(&object);
                    let object_ref: ObjectRef = object_info.into();
                    Ok(object_ref)
                })
                .collect()
        } else {
            let responses = self
                .multi_get_object_with_options_batched(object_ids, SuiObjectDataOptions::new())
                .await?;
            responses
                .into_iter()
                .map(|r| {
                    Ok(r.into_object()
                        .map_err(|e| SuiClientError::Internal(e.into()))?
                        .object_ref())
                })
                .collect()
        }
    }

    /// Returns the chain identifier.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_chain_identifier`] internally.
    pub async fn get_chain_identifier(&self) -> SuiClientResult<String> {
        async fn make_request(client: Arc<DualClient>) -> SuiClientResult<String> {
            Ok(client
                .sui_client()
                .read_api()
                .get_chain_identifier()
                .await?)
        }
        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_chain_identifier")
            .await
    }

    // Other wrapper methods.

    pub(crate) async fn get_shared_object_initial_version(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<SequenceNumber> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            get_initial_version_from_grpc_object(&self.get_object_by_grpc(object_id).await?)
        } else {
            get_initial_version_from_object_response(
                &self
                    .get_object_with_json_rpc(object_id, SuiObjectDataOptions::new().with_owner())
                    .await?,
            )
        }
    }

    pub(crate) async fn get_extended_field<V>(
        &self,
        object_id: ObjectID,
        type_origin_map: &TypeOriginMap,
    ) -> SuiClientResult<V>
    where
        V: DeserializeOwned,
    {
        let key_tag = contracts::extended_field::Key
            .to_move_struct_tag_with_type_map(type_origin_map, &[])?;
        self.get_dynamic_field::<Key, V>(object_id, key_tag.into(), Key { dummy_field: false })
            .await
    }

    #[allow(unused)]
    pub(crate) async fn get_dynamic_field_object<K, V>(
        &self,
        parent: ObjectID,
        key_type: TypeTag,
        key: K,
    ) -> SuiClientResult<V>
    where
        V: AssociatedContractStruct,
        K: DeserializeOwned + Serialize,
    {
        let key_tag = key_type.to_canonical_string(true);
        let key_tag = TypeTag::from_str(&format!("0x2::dynamic_object_field::Wrapper<{key_tag}>"))
            .expect("valid type tag");
        let inner_object_id = self.get_dynamic_field(parent, key_tag, key).await?;
        let inner = self.get_sui_object(inner_object_id).await?;
        Ok(inner)
    }

    pub(crate) async fn get_dynamic_field<K, V>(
        &self,
        parent: ObjectID,
        key_type: TypeTag,
        key: K,
    ) -> SuiClientResult<V>
    where
        K: DeserializeOwned + Serialize,
        V: DeserializeOwned,
    {
        let object_id = derive_dynamic_field_id(
            parent,
            &key_type,
            &bcs::to_bytes(&key).expect("key should be serializable"),
        )
        .map_err(|err| SuiClientError::Internal(err.into()))?;

        let field: SuiDynamicField<K, V> = self.get_sui_object(object_id).await?;
        Ok(field.value)
    }

    /// Checks if the Walrus system object exist on chain and returns the Walrus package ID.
    pub(crate) async fn get_system_package_id_from_system_object(
        &self,
        system_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        let system_object = self
            .get_sui_object::<SystemObjectForDeserialization>(system_object_id)
            .await?;

        let pkg_id = system_object.package_id;
        Ok(pkg_id)
    }

    /// Checks if the credits object (`subsidies::Subsidies` in Move) exist on chain and returns
    /// the object.
    pub(crate) async fn get_credits_object(&self, object_id: ObjectID) -> SuiClientResult<Credits> {
        self.get_sui_object::<Credits>(object_id).await
    }

    /// Returns the package ID from the type of the given object.
    ///
    /// Note: This returns the package address from the object type, not the newest package ID.
    pub async fn get_package_id_from_object(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            let object = self
                .get_object_by_grpc(object_id)
                .await
                .inspect_err(|error| {
                    tracing::debug!(%error, %object_id, "unable to get the object");
                })?;

            let pkg_id =
                crate::utils::get_package_id_from_object(&object).inspect_err(|error| {
                    tracing::debug!(%error, %object_id,
                        "unable to get the package ID from the object");
                })?;
            Ok(pkg_id)
        } else {
            let object = self
                .get_object_with_json_rpc(
                    object_id,
                    SuiObjectDataOptions::default().with_type().with_bcs(),
                )
                .await
                .inspect_err(|error| {
                    tracing::debug!(%error, %object_id, "unable to get the object");
                })?;

            let pkg_id = crate::utils::get_package_id_from_object_response(&object).inspect_err(
                |error| {
                    tracing::debug!(%error, %object_id,
                    "unable to get the package ID from the object response");
                },
            )?;
            Ok(pkg_id)
        }
    }

    /// Gets the type origin map for a given package.
    pub(crate) async fn type_origin_map_for_package(
        &self,
        package_id: ObjectID,
    ) -> SuiClientResult<TypeOriginMap> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            async fn make_request(
                client: Arc<DualClient>,
                package_id: ObjectID,
            ) -> SuiClientResult<TypeOriginMap> {
                client.get_type_origin_map_for_package(package_id).await
            }

            let request = move |client: Arc<DualClient>, method| {
                retry_rpc_errors(
                    self.get_strategy(),
                    move || make_request(client.clone(), package_id),
                    self.metrics.clone(),
                    method,
                )
            };
            self.failover_sui_client
                .with_failover(request, None, "type_origin_map_for_package")
                .await
        } else {
            let Ok(Some(SuiRawData::Package(raw_package))) = self
                .get_object_with_json_rpc(
                    package_id,
                    SuiObjectDataOptions::default().with_type().with_bcs(),
                )
                .await?
                .into_object()
                .map(|object| object.bcs)
            else {
                return Err(SuiClientError::WalrusPackageNotFound(package_id));
            };
            Ok(raw_package
                .type_origin_table
                .into_iter()
                .map(|origin| ((origin.module_name, origin.datatype_name), origin.package))
                .collect())
        }
    }

    /// Retrieves the WAL type from the walrus package by getting the type tag of the `Balance`
    /// in the `StakedWal` Move struct.
    #[tracing::instrument(err, skip(self))]
    pub(crate) async fn wal_type_from_package(
        &self,
        package_id: ObjectID,
    ) -> SuiClientResult<String> {
        let normalized_move_modules = self
            .get_normalized_move_modules_by_package(package_id)
            .await?;

        let staked_wal_struct = normalized_move_modules
            .get("staked_wal")
            .and_then(|module| module.structs.get("StakedWal"))
            .ok_or_else(|| SuiClientError::WalTypeNotFound(package_id))?;
        let principal_field_type = staked_wal_struct.fields.iter().find_map(|field| {
            if field.name == "principal" {
                Some(&field.type_)
            } else {
                None
            }
        });
        let Some(SuiMoveNormalizedType::Struct {
            inner: principal_field_type_inner,
        }) = principal_field_type
        else {
            return Err(SuiClientError::WalTypeNotFound(package_id));
        };
        let wal_type = principal_field_type_inner
            .type_arguments
            .first()
            .ok_or_else(|| SuiClientError::WalTypeNotFound(package_id))?;
        let SuiMoveNormalizedType::Struct {
            inner: wal_type_inner,
        } = wal_type
        else {
            return Err(SuiClientError::WalTypeNotFound(package_id));
        };
        let SuiMoveNormalizedStructType {
            address,
            module,
            name,
            ..
        } = wal_type_inner.as_ref();
        ensure!(
            module == "wal" && name == "WAL",
            SuiClientError::WalTypeNotFound(package_id)
        );
        let wal_type = format!("{address}::{module}::{name}");

        tracing::debug!(?wal_type, "WAL type");
        Ok(wal_type)
    }

    /// If the `gas_budget` is passed in, this returns the gas budget and the current gas price.
    /// Otherwise, it estimates the gas budget and the gas price concurrently.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub(crate) async fn gas_budget_and_price(
        &self,
        gas_budget: Option<u64>,
        signer: SuiAddress,
        transaction_kind: TransactionKind,
    ) -> SuiClientResult<GasBudgetAndPrice> {
        if let Some(gas_budget) = gas_budget {
            Ok(GasBudgetAndPrice {
                gas_budget,
                gas_price: self.get_reference_gas_price().await?,
            })
        } else {
            self.estimate_gas_budget(signer, transaction_kind).await
        }
    }

    /// Calls a dry run with the transaction data to estimate the gas budget.
    ///
    /// Returns the estimated gas budget and the current gas price.
    ///
    /// This performs the same calculation as the Sui CLI and the TypeScript SDK with the following
    /// differences:
    /// - uses a dummy gas price of [`DUMMY_GAS_PRICE`] to estimate the gas budget,
    /// - requests the gas price concurrently, and
    /// - scales the computation cost of the dry run accordingly.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    async fn estimate_gas_budget(
        &self,
        signer: SuiAddress,
        kind: TransactionKind,
    ) -> SuiClientResult<GasBudgetAndPrice> {
        let dry_run_tx_data = TransactionData::new_with_gas_coins_allow_sponsor(
            kind,
            signer,
            vec![],
            MAX_GAS_BUDGET,
            DUMMY_GAS_PRICE,
            signer,
        );
        let dry_run_future = self.dry_run_transaction_block(dry_run_tx_data);

        let (gas_price, dry_run_result) =
            tokio::try_join!(self.get_reference_gas_price(), dry_run_future)?;
        let gas_cost_summary = dry_run_result.effects.gas_cost_summary();

        let computation_cost =
            (gas_cost_summary.computation_cost * gas_price).div_ceil(DUMMY_GAS_PRICE);
        let safe_overhead = GAS_SAFE_OVERHEAD * gas_price;
        let net_gas_usage_non_negative = (computation_cost + gas_cost_summary.storage_cost)
            .saturating_sub(gas_cost_summary.storage_rebate);
        let gas_budget = computation_cost.max(net_gas_usage_non_negative) + safe_overhead;

        Ok(GasBudgetAndPrice {
            gas_budget,
            gas_price,
        })
    }

    /// Executes a transaction.
    #[tracing::instrument(level = Level::DEBUG, err, skip(self, transaction))]
    pub(crate) async fn execute_transaction(
        &self,
        transaction: Transaction,
        method: &'static str,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        async fn make_request(
            client: Arc<DualClient>,
            transaction: Transaction,
        ) -> SuiClientResult<SuiTransactionBlockResponse> {
            #[cfg(msim)]
            {
                maybe_return_injected_error_in_stake_pool_transaction(&transaction)?;
            }
            Ok(client
                .sui_client()
                .quorum_driver_api()
                .execute_transaction_block(
                    transaction.clone(),
                    SuiTransactionBlockResponseOptions::new()
                        .with_effects()
                        .with_input()
                        .with_events()
                        .with_object_changes()
                        .with_balance_changes(),
                    Some(WaitForLocalExecution),
                )
                .await?)
        }
        let request = move |client: Arc<DualClient>, method| {
            let transaction = transaction.clone();
            // Retry here must use the exact same transaction to avoid locked objects.
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), transaction.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, method)
            .await
    }

    /// Gets a backoff strategy, seeded from the internal RNG.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.backoff_config
            .get_strategy(ThreadRng::default().r#gen())
    }

    /// Returns the events for the given transaction digest.
    pub async fn get_events(&self, tx_digest: TransactionDigest) -> SuiClientResult<Vec<SuiEvent>> {
        self.failover_sui_client
            .with_failover(
                async |client, method| {
                    retry_rpc_errors(
                        self.get_strategy(),
                        || async {
                            Ok(client
                                .sui_client()
                                .event_api()
                                .get_events(tx_digest)
                                .await?)
                        },
                        self.metrics.clone(),
                        method,
                    )
                    .await
                },
                None,
                "get_events",
            )
            .await
    }

    /// Get the latest object reference given an [`ObjectID`].
    #[tracing::instrument(skip(self), level = Level::DEBUG)]
    pub async fn get_object_ref(&self, object_id: ObjectID) -> SuiClientResult<ObjectRef> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            self.get_object_ref_with_grpc(object_id).await
        } else {
            let object_data = self
                .get_object_with_json_rpc(object_id, SuiObjectDataOptions::new().with_type())
                .await?
                .data
                .context("no object data returned")?;
            let object_ref = object_data.object_ref();
            Ok(object_ref)
        }
    }

    /// Get the latest object reference given an [`ObjectID`].
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn get_object_ref_and_type_tag(
        &self,
        object_id: ObjectID,
    ) -> Result<(ObjectRef, TypeTag), SuiClientError> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            self.get_object_ref_and_type_tag_with_grpc(object_id).await
        } else {
            let object_data = self
                .get_object_with_json_rpc(object_id, SuiObjectDataOptions::new().with_type())
                .await?
                .data
                .context("no object data returned")?;
            let ObjectType::Struct(object_type) = object_data.object_type()? else {
                return Err(anyhow::anyhow!("object is not a struct").into());
            };
            let object_ref = object_data.object_ref();
            Ok((object_ref, object_type.into()))
        }
    }

    async fn get_object_ref_and_type_tag_with_grpc(
        &self,
        object_id: ObjectID,
    ) -> Result<(ObjectRef, TypeTag), SuiClientError> {
        debug_assert!(self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT);
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
        ) -> SuiClientResult<(ObjectRef, TypeTag)> {
            client.get_object_ref_and_type_tag(object_id).await
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object_ref_and_type_tag")
            .await
    }

    /// Returns the owner address of the given object.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn get_object_owner_address(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<SuiAddress> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            let object: sui_types::object::Object = self.get_object_by_grpc(object_id).await?;
            Ok(object
                .owner()
                .get_owner_address()
                .context("no object owner address returned from rpc")?)
        } else {
            let object: SuiObjectResponse = self
                .get_object_with_json_rpc(object_id, SuiObjectDataOptions::default().with_owner())
                .await?;
            Ok(object
                .owner()
                .context("no object owner returned from rpc")?
                .get_owner_address()?)
        }
    }

    /// Returns the node objects for the given node IDs.
    #[tracing::instrument(skip_all, level = Level::DEBUG)]
    pub async fn get_node_objects<I>(
        &self,
        node_ids: I,
    ) -> SuiClientResult<HashMap<ObjectID, StakingPool>>
    where
        I: IntoIterator<Item = ObjectID>,
    {
        let node_ids = node_ids.into_iter().collect::<Vec<_>>();
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_BATCH_OBJECTS {
            self.get_sui_objects(&node_ids)
                .await?
                .into_iter()
                .map(|pool: StakingPool| Ok((pool.id, pool)))
                .collect()
        } else {
            let sui_object_responses =
                futures::future::try_join_all(node_ids.chunks(MULTI_GET_OBJ_LIMIT).map(
                    |obj_id_batch| async move {
                        self.multi_get_object_with_options(
                            obj_id_batch,
                            SuiObjectDataOptions::new().with_type().with_bcs(),
                        )
                        .await
                    },
                ))
                .await?;
            sui_object_responses
                .into_iter()
                .flat_map(|responses| {
                    responses.into_iter().map(|response| {
                        get_sui_object_from_object_response::<StakingPool>(&response)
                    })
                })
                .map(|result| result.map(|pool| (pool.id, pool)))
                .collect()
        }
    }

    /// Returns the blob event with the given Event ID.
    pub async fn get_blob_event(&self, event_id: EventID) -> SuiClientResult<BlobEvent> {
        self.get_events(event_id.tx_digest)
            .await?
            .into_iter()
            .find(|e| e.id == event_id)
            .and_then(|e| e.try_into().ok())
            .ok_or(SuiClientError::NoCorrespondingBlobEvent(event_id))
    }

    /// Returns the storage nodes with the given IDs.
    pub async fn get_storage_nodes_by_ids(
        &self,
        node_ids: &[ObjectID],
    ) -> SuiClientResult<Vec<StorageNode>> {
        Ok(self
            .get_sui_objects::<StakingPool>(node_ids)
            .await?
            .into_iter()
            .map(|pool| pool.node_info)
            .collect())
    }

    /// Returns the metadata associated with a blob object.
    pub async fn get_blob_attribute(
        &self,
        blob_object_id: &ObjectID,
    ) -> SuiClientResult<Option<BlobAttribute>> {
        self.get_dynamic_field::<Vec<u8>, BlobAttribute>(
            *blob_object_id,
            TypeTag::Vector(Box::new(TypeTag::U8)),
            b"metadata".to_vec(),
        )
        .await
        .map(Some)
        .or_else(|_| Ok(None))
    }

    /// Returns the previous transaction digest for the given object ID.
    pub async fn get_previous_transaction(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<TransactionDigest> {
        if self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT {
            Ok(self.get_previous_transaction_with_grpc(object_id).await?)
        } else {
            Ok(self
                .get_object_with_json_rpc(
                    object_id,
                    SuiObjectDataOptions::new().with_previous_transaction(),
                )
                .await?
                .data
                .context("missing data on object")?
                .previous_transaction
                .context("missing previous transaction on object")?)
        }
    }

    async fn get_previous_transaction_with_grpc(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<TransactionDigest> {
        debug_assert!(self.grpc_migration_level >= GRPC_MIGRATION_LEVEL_GET_OBJECT);
        async fn make_request(
            client: Arc<DualClient>,
            object_id: ObjectID,
        ) -> SuiClientResult<TransactionDigest> {
            client.get_previous_transaction(object_id).await
        }

        let request = move |client: Arc<DualClient>, method| {
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_previous_transaction")
            .await
    }
}

/// Injects a simulated error for testing retry behavior executing sui transactions.
/// We use stake_with_pool as an example here to incorporate with the test logic in
/// `test_ptb_executor_retriable_error` in `test_client.rs`.
#[cfg(msim)]
fn maybe_return_injected_error_in_stake_pool_transaction(
    transaction: &sui_types::transaction::Transaction,
) -> anyhow::Result<()> {
    // Check if this transaction contains a stake_with_pool operation

    use rand::thread_rng;
    use sui_macros::fail_point_if;

    let is_stake_pool_tx =
        transaction
            .transaction_data()
            .move_calls()
            .iter()
            .any(|(_, _, _, function_name)| {
                *function_name == crate::contracts::staking::stake_with_pool.name
            });

    // Early return if this isn't a stake pool transaction
    if !is_stake_pool_tx {
        return Ok(());
    }

    // Check if we should inject an error via the fail point
    let mut should_inject_error = false;
    fail_point_if!("ptb_executor_stake_pool_retriable_error", || {
        should_inject_error = true;
    });

    if should_inject_error {
        tracing::warn!("injecting a retriable RPC error for stake pool transaction");

        let retriable_error = if thread_rng().gen_bool(0.5) {
            // Simulate a retriable RPC error (502 Bad Gateway).
            jsonrpsee::core::ClientError::Transport(
                "server returned an error status code: 502".into(),
            )
        } else {
            // Simulate a request timeout error.
            jsonrpsee::core::ClientError::RequestTimeout
        };

        Err(sui_sdk::error::Error::RpcError(retriable_error))?;
    }

    Ok(())
}

/// A representation of Coin with an `Ord` implementation, allowing for sorting by balance.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedCoin(pub Coin);

impl From<Coin> for OrderedCoin {
    fn from(coin: Coin) -> Self {
        OrderedCoin(coin)
    }
}

impl From<OrderedCoin> for Coin {
    fn from(val: OrderedCoin) -> Self {
        val.0
    }
}

impl PartialOrd for OrderedCoin {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedCoin {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.balance.cmp(&other.0.balance)
    }
}

impl OrderedCoin {
    fn balance(&self) -> u64 {
        self.0.balance
    }
}
