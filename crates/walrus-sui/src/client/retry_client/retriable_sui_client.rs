// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`SuiClient`] to introduce retries.
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap},
    fmt,
    pin::pin,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use futures::{Stream, StreamExt, future, stream};
use rand::{
    Rng as _,
    rngs::{StdRng, ThreadRng},
};
use serde::{Serialize, de::DeserializeOwned};
use sui_sdk::{
    SuiClient,
    SuiClientBuilder,
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
    base_types::{ObjectID, SuiAddress, TransactionDigest},
    dynamic_field::derive_dynamic_field_id,
    quorum_driver_types::ExecuteTransactionRequestType::WaitForLocalExecution,
    sui_serde::BigInt,
    transaction::{Transaction, TransactionData, TransactionKind},
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
    client::SuiClientMetricSet,
    contracts::{self, AssociatedContractStruct, TypeOriginMap},
    types::move_structs::{Key, Subsidies, SuiDynamicField, SystemObjectForDeserialization},
    utils::get_sui_object_from_object_response,
};

/// The maximum gas allowed in a transaction, in MIST (50 SUI). Used for gas budget estimation.
const MAX_GAS_BUDGET: u64 = 50_000_000_000;
/// The maximum number of gas payment objects allowed in a transaction by the Sui protocol
/// configuration
/// [here](https://github.com/MystenLabs/sui/blob/main/crates/sui-protocol-config/src/lib.rs#L2089).
pub(crate) const MAX_GAS_PAYMENT_OBJECTS: usize = 256;

/// [`LazySuiClientBuilder`] has enough information to create a [`SuiClient`], when its
/// [`LazyClientBuilder`] trait implementation is used.
#[derive(Clone)]
pub enum LazySuiClientBuilder {
    /// A client that is built dynamically from a URL.
    Url {
        /// The URL of the RPC server.
        rpc_url: String,
        /// Override the default timeout for any requests.
        request_timeout: Option<Duration>,
    },
    /// Use a pre-existing client.
    Client(Arc<SuiClient>),
}

impl fmt::Debug for LazySuiClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url {
                rpc_url,
                request_timeout,
            } => f
                .debug_tuple("Url")
                .field(rpc_url)
                .field(request_timeout)
                .finish(),
            Self::Client(_) => f.debug_tuple("Client").finish(),
        }
    }
}

impl From<SuiClient> for LazySuiClientBuilder {
    fn from(client: SuiClient) -> Self {
        Self::Client(Arc::new(client))
    }
}

impl LazySuiClientBuilder {
    /// Creates a new [`LazySuiClientBuilder`] from a URL and an optional `request_timeout`.
    pub fn new(rpc_url: impl AsRef<str>, request_timeout: Option<Duration>) -> Self {
        Self::Url {
            rpc_url: rpc_url.as_ref().to_string(),
            request_timeout,
        }
    }
}

impl LazyClientBuilder<SuiClient> for LazySuiClientBuilder {
    // TODO: WAL-796 Out of concern for consistency, we are disabling the failover mechanism for
    // SuiClient for now.
    const DEFAULT_MAX_TRIES: usize = 5;

    async fn lazy_build_client(&self) -> Result<Arc<SuiClient>, FailoverError> {
        // Inject sui client build failure for simtests.
        #[cfg(msim)]
        {
            let mut fail_client_creation = false;
            sui_macros::fail_point_arg!(
                "failpoint_sui_client_build_client",
                |url_to_fail: String| {
                    match self {
                        Self::Url { rpc_url, .. } => {
                            if *rpc_url == url_to_fail {
                                fail_client_creation = true;
                            }
                        }
                        Self::Client(_) => {}
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

        match self {
            Self::Client(client) => Ok(client.clone()),
            Self::Url {
                rpc_url,
                request_timeout,
            } => {
                let mut client_builder = SuiClientBuilder::default();
                if let Some(request_timeout) = request_timeout {
                    client_builder = client_builder.request_timeout(*request_timeout);
                }
                Ok(Arc::new(client_builder.build(rpc_url).await.map_err(
                    |e| FailoverError::FailedToGetClient(e.to_string()),
                )?))
            }
        }
    }
    fn get_rpc_url(&self) -> Option<&str> {
        match self {
            Self::Url { rpc_url, .. } => Some(rpc_url.as_str()),
            Self::Client(_) => None,
        }
    }
}

/// A [`SuiClient`] that retries RPC calls with backoff in case of network errors.
///
/// This retriable client wraps functions from the [`CoinReadApi`][sui_sdk::apis::CoinReadApi] and
/// the [`ReadApi`][sui_sdk::apis::ReadApi] of the [`SuiClient`], and
/// additionally provides some convenience methods.
#[allow(missing_debug_implementations)]
#[derive(Clone)]
pub struct RetriableSuiClient {
    failover_sui_client: FailoverWrapper<SuiClient, LazySuiClientBuilder>,
    backoff_config: ExponentialBackoffConfig,
    metrics: Option<Arc<SuiClientMetricSet>>,
}

impl RetriableSuiClient {
    /// Creates a new retriable client.
    pub async fn new(
        lazy_client_builders: Vec<LazySuiClientBuilder>,
        backoff_config: ExponentialBackoffConfig,
    ) -> anyhow::Result<Self> {
        Ok(RetriableSuiClient {
            failover_sui_client: FailoverWrapper::new(lazy_client_builders)
                .await
                .context("creating failover wrapper")?,
            backoff_config,
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
    pub async fn new_for_rpc_urls<S: AsRef<str>>(
        rpc_addresses: &[S],
        backoff_config: ExponentialBackoffConfig,
        request_timeout: Option<Duration>,
    ) -> SuiClientResult<Self> {
        let failover_clients = rpc_addresses
            .iter()
            .map(|rpc_url| LazySuiClientBuilder::new(rpc_url, request_timeout))
            .collect::<Vec<_>>();
        Ok(Self::new(failover_clients, backoff_config).await?)
    }

    // Reimplementation of the `SuiClient` methods.

    /// Return a list of coins for the given address, or an error upon failure.
    ///
    /// Reimplements the functionality of [`sui_sdk::apis::CoinReadApi::select_coins`] with the
    /// addition of retries on network errors.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
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
    /// This is a reimplementation of the [`sui_sdk::apis::CoinReadApi:::get_coins_stream`] method
    /// in the `SuiClient` struct. Unlike the original implementation, this version will retry
    /// failed RPC calls.
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
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
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
            client: Arc<SuiClient>,
            query: SuiTransactionBlockResponseQuery,
            cursor: Option<TransactionDigest>,
            limit: Option<usize>,
            descending_order: bool,
        ) -> SuiClientResult<TransactionBlocksPage> {
            Ok(client
                .read_api()
                .query_transaction_blocks(query.clone(), cursor, limit, descending_order)
                .await?)
        }
        let request = |client: Arc<SuiClient>, method: &'static str| {
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
            client: Arc<SuiClient>,
            address: SuiAddress,
            query: Option<SuiObjectResponseQuery>,
            cursor: Option<ObjectID>,
            limit: Option<usize>,
        ) -> SuiClientResult<ObjectsPage> {
            Ok(client
                .read_api()
                .get_owned_objects(address, query, cursor, limit)
                .await?)
        }
        let request = |client: Arc<SuiClient>, method: &'static str| {
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

    /// Returns a [`SuiObjectResponse`] based on the provided [`ObjectID`].
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_object_with_options`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_object_with_options(
        &self,
        object_id: ObjectID,
        options: SuiObjectDataOptions,
    ) -> SuiClientResult<SuiObjectResponse> {
        async fn make_request(
            client: Arc<SuiClient>,
            object_id: ObjectID,
            options: SuiObjectDataOptions,
        ) -> SuiClientResult<SuiObjectResponse> {
            Ok(client
                .read_api()
                .get_object_with_options(object_id, options.clone())
                .await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
            let options = options.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_id, options.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "get_object")
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
            client: Arc<SuiClient>,
            digest: TransactionDigest,
            options: SuiTransactionBlockResponseOptions,
        ) -> SuiClientResult<SuiTransactionBlockResponse> {
            Ok(client
                .read_api()
                .get_transaction_with_options(digest, options.clone())
                .await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
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

    /// Return a list of [SuiObjectResponse] from the given vector of [ObjectID]s.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::multi_get_object_with_options`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn multi_get_object_with_options(
        &self,
        object_ids: Vec<ObjectID>,
        options: SuiObjectDataOptions,
    ) -> SuiClientResult<Vec<SuiObjectResponse>> {
        async fn make_request(
            client: Arc<SuiClient>,
            object_ids: Vec<ObjectID>,
            options: SuiObjectDataOptions,
        ) -> SuiClientResult<Vec<SuiObjectResponse>> {
            Ok(client
                .read_api()
                .multi_get_object_with_options(object_ids.clone(), options.clone())
                .await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
            let object_ids = object_ids.clone();
            let options = options.clone();
            retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), object_ids.clone(), options.clone()),
                self.metrics.clone(),
                method,
            )
        };
        self.failover_sui_client
            .with_failover(request, None, "multi_get_object")
            .await
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
            client: Arc<SuiClient>,
            package_id: ObjectID,
        ) -> SuiClientResult<BTreeMap<String, SuiMoveNormalizedModule>> {
            Ok(client
                .read_api()
                .get_normalized_move_modules_by_package(package_id)
                .await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
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
            client: Arc<SuiClient>,
            epoch: Option<BigInt<u64>>,
        ) -> SuiClientResult<SuiCommittee> {
            Ok(client.governance_api().get_committee_info(epoch).await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
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
    pub async fn get_reference_gas_price(&self) -> SuiClientResult<u64> {
        async fn make_request(client: Arc<SuiClient>) -> SuiClientResult<u64> {
            Ok(client.read_api().get_reference_gas_price().await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
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
            client: Arc<SuiClient>,
            transaction: Arc<TransactionData>,
        ) -> SuiClientResult<DryRunTransactionBlockResponse> {
            let tx = TransactionData::clone(&transaction);
            Ok(client.read_api().dry_run_transaction_block(tx).await?)
        }

        let request = move |client: Arc<SuiClient>, method| {
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

    /// Returns a reference to the current internal [`SuiClient`]. Avoid using this function.
    ///
    // TODO: WAL-778 find callsites to this method, and replace them with implementations that make
    // use of failover, since this call is a cheat to bypass the failover mechanism.
    #[deprecated(
        note = "please implement a full treatment in RetriableSuiClient for your use case"
    )]
    pub async fn get_current_client(&self) -> Arc<SuiClient> {
        self.failover_sui_client
            .get_current_client()
            .await
            .expect("client must have been created")
    }

    /// Returns the Sui Object of type `U` with the provided [`ObjectID`].
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_sui_object<U>(&self, object_id: ObjectID) -> SuiClientResult<U>
    where
        U: AssociatedContractStruct,
    {
        let sui_object_response = self
            .get_object_with_options(
                object_id,
                SuiObjectDataOptions::new().with_bcs().with_type(),
            )
            .await?;
        get_sui_object_from_object_response(&sui_object_response)
    }

    /// Returns the Sui Objects of type `U` with the provided [`ObjectID`]s.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_sui_objects<U>(&self, object_ids: &[ObjectID]) -> SuiClientResult<Vec<U>>
    where
        U: AssociatedContractStruct,
    {
        let mut responses = vec![];
        for obj_id_batch in object_ids.chunks(MULTI_GET_OBJ_LIMIT) {
            responses.extend(
                self.multi_get_object_with_options(
                    obj_id_batch.to_vec(),
                    SuiObjectDataOptions::new().with_bcs().with_type(),
                )
                .await?,
            );
        }

        responses
            .iter()
            .map(|r| get_sui_object_from_object_response(r))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Returns the chain identifier.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_chain_identifier`] internally.
    pub async fn get_chain_identifier(&self) -> SuiClientResult<String> {
        async fn make_request(client: Arc<SuiClient>) -> SuiClientResult<String> {
            Ok(client.read_api().get_chain_identifier().await?)
        }
        let request = move |client: Arc<SuiClient>, method| {
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
        let key_tag =
            TypeTag::from_str(&format!("0x2::dynamic_object_field::Wrapper<{}>", key_tag))
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

    /// Checks if the Walrus subsidies object exist on chain and returns the subsidies package ID.
    pub(crate) async fn get_subsidies_package_id_from_subsidies_object(
        &self,
        subsidies_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        let subsidies_object = self
            .get_sui_object::<Subsidies>(subsidies_object_id)
            .await?;

        let pkg_id = subsidies_object.package_id;
        Ok(pkg_id)
    }

    /// Returns the package ID from the type of the given object.
    ///
    /// Note: This returns the package address from the object type, not the newest package ID.
    pub async fn get_package_id_from_object(
        &self,
        object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        let response = self
            .get_object_with_options(
                object_id,
                SuiObjectDataOptions::default().with_type().with_bcs(),
            )
            .await
            .inspect_err(|error| {
                tracing::debug!(%error, %object_id, "unable to get the object");
            })?;

        let pkg_id =
            crate::utils::get_package_id_from_object_response(&response).inspect_err(|error| {
                tracing::debug!(%error, %object_id, "unable to get the package ID from the object");
            })?;
        Ok(pkg_id)
    }

    /// Gets the type origin map for a given package.
    pub(crate) async fn type_origin_map_for_package(
        &self,
        package_id: ObjectID,
    ) -> SuiClientResult<TypeOriginMap> {
        let Ok(Some(SuiRawData::Package(raw_package))) = self
            .get_object_with_options(
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

    /// Calls a dry run with the transaction data to estimate the gas budget.
    ///
    /// This performs the same calculation as the Sui CLI and the TypeScript SDK.
    pub(crate) async fn estimate_gas_budget(
        &self,
        signer: SuiAddress,
        kind: TransactionKind,
        gas_price: u64,
    ) -> SuiClientResult<u64> {
        let dry_run_tx_data = self
            .failover_sui_client
            .get_current_client()
            .await
            .expect("client must have been created")
            .transaction_builder()
            .tx_data_for_dry_run(signer, kind, MAX_GAS_BUDGET, gas_price, None, None)
            .await;
        let effects = self
            .dry_run_transaction_block(dry_run_tx_data)
            .await
            .inspect_err(|error| {
                tracing::debug!(%error, "transaction dry run failed");
            })?
            .effects;
        let gas_cost_summary = effects.gas_cost_summary();

        let safe_overhead = GAS_SAFE_OVERHEAD * gas_price;
        let computation_cost_with_overhead = gas_cost_summary.computation_cost + safe_overhead;
        let gas_usage_with_overhead = gas_cost_summary.net_gas_usage() + safe_overhead as i64;
        Ok(computation_cost_with_overhead.max(gas_usage_with_overhead.max(0) as u64))
    }

    /// Executes a transaction.
    #[tracing::instrument(err, skip(self))]
    pub(crate) async fn execute_transaction(
        &self,
        transaction: Transaction,
        method: &'static str,
    ) -> SuiClientResult<SuiTransactionBlockResponse> {
        async fn make_request(
            client: Arc<SuiClient>,
            transaction: Transaction,
        ) -> SuiClientResult<SuiTransactionBlockResponse> {
            #[cfg(msim)]
            {
                maybe_return_injected_error_in_stake_pool_transaction(&transaction)?;
            }
            Ok(client
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
        let request = move |client: Arc<SuiClient>, method| {
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
                        || async { Ok(client.event_api().get_events(tx_digest).await?) },
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
            .any(|(_, _, function_name)| {
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
