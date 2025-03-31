// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`SuiClient`] to introduce retries.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::Future,
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use futures::{
    future::{self},
    stream,
    Stream,
    StreamExt,
};
use rand::{
    rngs::{StdRng, ThreadRng},
    Rng as _,
};
use reqwest::Url;
use serde::{de::DeserializeOwned, Serialize};
#[cfg(msim)]
use sui_macros::fail_point_if;
use sui_rpc_api::{client::ResponseExt as _, Client as RpcClient};
use sui_sdk::{
    apis::{EventApi, GovernanceApi},
    error::SuiRpcResult,
    rpc_types::{
        Balance,
        Coin,
        DryRunTransactionBlockResponse,
        ObjectsPage,
        SuiCommittee,
        SuiMoveNormalizedModule,
        SuiMoveNormalizedType,
        SuiObjectDataOptions,
        SuiObjectResponse,
        SuiObjectResponseQuery,
        SuiRawData,
        SuiTransactionBlockEffectsAPI,
        SuiTransactionBlockResponse,
        SuiTransactionBlockResponseOptions,
    },
    wallet_context::WalletContext,
    SuiClient,
    SuiClientBuilder,
};
use sui_storage::blob::Blob;
#[cfg(msim)]
use sui_types::transaction::TransactionDataAPI;
use sui_types::{
    base_types::{ObjectID, SuiAddress, TransactionDigest},
    dynamic_field::derive_dynamic_field_id,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::CertifiedCheckpointSummary,
    object::Object,
    quorum_driver_types::ExecuteTransactionRequestType::WaitForLocalExecution,
    sui_serde::BigInt,
    transaction::{Transaction, TransactionData, TransactionKind},
    TypeTag,
};
use thiserror::Error;
use tokio::time::Instant;
use tracing::Level;
use url::ParseError;
use walrus_core::ensure;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff, ExponentialBackoffConfig};

use super::{rpc_config::RpcFallbackConfig, SuiClientError, SuiClientResult};
use crate::{
    client::SuiClientMetricSet,
    contracts::{self, AssociatedContractStruct, TypeOriginMap},
    types::move_structs::{Key, Subsidies, SuiDynamicField, SystemObjectForDeserialization},
    utils::get_sui_object_from_object_response,
};

/// The list of HTTP status codes that are retriable.
const RETRIABLE_RPC_ERRORS: &[&str] = &["429", "500", "502"];
/// The list of gRPC status codes that are retriable.
const RETRIABLE_GRPC_ERRORS: &[tonic::Code] = &[
    tonic::Code::ResourceExhausted,
    tonic::Code::Internal,
    tonic::Code::Unavailable,
    tonic::Code::DeadlineExceeded,
];

/// The gas overhead to add to the gas budget to ensure that the transaction will succeed.
/// Set based on `GAS_SAFE_OVERHEAD` in the sui CLI. Used for gas budget estimation.
const GAS_SAFE_OVERHEAD: u64 = 1000;

/// The maximum gas allowed in a transaction, in MIST (50 SUI). Used for gas budget estimation.
const MAX_GAS_BUDGET: u64 = 50_000_000_000;

/// The maximum number of objects to get in a single RPC call.
pub(crate) const MULTI_GET_OBJ_LIMIT: usize = 50;

/// Trait to test if an error is produced by a temporary RPC failure and can be retried.
pub trait RetriableRpcError: Debug {
    /// Returns `true` if the error is a retriable network error.
    fn is_retriable_rpc_error(&self) -> bool;
}

/// Trait to convert an error to a string.
pub trait ToErrorType {
    /// Returns the error type as a string.
    fn to_error_type(&self) -> String;
}

impl RetriableRpcError for anyhow::Error {
    fn is_retriable_rpc_error(&self) -> bool {
        self.downcast_ref::<sui_sdk::error::Error>()
            .map(|error| error.is_retriable_rpc_error())
            .unwrap_or(false)
    }
}

impl RetriableRpcError for sui_sdk::error::Error {
    fn is_retriable_rpc_error(&self) -> bool {
        if let sui_sdk::error::Error::RpcError(rpc_error) = self {
            let error_string = rpc_error.to_string();
            if RETRIABLE_RPC_ERRORS
                .iter()
                .any(|&s| error_string.contains(s))
            {
                return true;
            }
        }
        false
    }
}

impl RetriableRpcError for SuiClientError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            SuiClientError::SuiSdkError(error) => error.is_retriable_rpc_error(),
            SuiClientError::Internal(error) => error.is_retriable_rpc_error(),
            _ => false,
        }
    }
}

impl RetriableRpcError for tonic::Status {
    fn is_retriable_rpc_error(&self) -> bool {
        RETRIABLE_GRPC_ERRORS.contains(&self.code())
    }
}

impl RetriableRpcError for CheckpointDownloadError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            CheckpointDownloadError::RequestFailed(error) => error
                .status()
                .map(|status| status.is_server_error() || status.as_u16() == 429)
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl RetriableRpcError for RetriableClientError {
    fn is_retriable_rpc_error(&self) -> bool {
        match self {
            Self::RpcError(status) => status.is_retriable_rpc_error(),
            Self::TimeoutError(_) => true,
            Self::FallbackError(fallback_error) => fallback_error.is_retriable_rpc_error(),
            Self::Other(_) => false,
        }
    }
}

/// A guard that records the number of retries and the result of an RPC call.
struct RetryCountGuard {
    method: String,
    count: u64,
    metrics: Arc<SuiClientMetricSet>,
    status: String,
}

impl RetryCountGuard {
    fn new(metrics: Arc<SuiClientMetricSet>, method: &str) -> Self {
        Self {
            method: method.to_string(),
            count: 0,
            metrics,
            status: "success".to_string(),
        }
    }

    pub fn record_result<T, E>(&mut self, value: Result<&T, &E>)
    where
        E: ToErrorType,
    {
        match value {
            Ok(_) => self.status = "success".to_string(),
            Err(e) => self.status = e.to_error_type(),
        }
        self.count += 1;
    }
}

impl Drop for RetryCountGuard {
    fn drop(&mut self) {
        if self.count > 1 {
            self.metrics
                .record_rpc_retry_count(self.method.as_str(), self.count, &self.status);
        }
    }
}

/// Retries the given function while it returns retriable errors.[
async fn retry_rpc_errors<S, F, T, E, Fut>(
    mut strategy: S,
    mut func: F,
    metrics: Option<Arc<SuiClientMetricSet>>,
    method: &str,
) -> Result<T, E>
where
    S: BackoffStrategy,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetriableRpcError + ToErrorType,
{
    let mut retry_guard = metrics
        .as_ref()
        .map(|m| RetryCountGuard::new(m.clone(), method));

    loop {
        let start = Instant::now();
        let value = func().await;

        if let Some(metrics) = &metrics {
            metrics.record_rpc_call(
                method,
                &match value.as_ref() {
                    Ok(_) => "success".to_string(),
                    Err(e) => e.to_error_type(),
                },
                start.elapsed(),
            );
        }

        if let Some(retry_guard) = &mut retry_guard {
            retry_guard.record_result(value.as_ref());
        }

        match value {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retriable_rpc_error() => {
                if let Some(delay) = strategy.next_delay() {
                    tracing::debug!(
                        ?delay,
                        ?error,
                        "attempt failed with retriable RPC error, waiting before retrying"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    tracing::debug!(
                        "last attempt failed with retriable RPC error, returning last failure value"
                    );
                    return Err(error);
                }
            }
            Err(error) => {
                tracing::debug!("non-retriable error, returning last failure value");
                return Err(error);
            }
        }
    }
}

/// A wrapper that provides failover functionality for any inner type.
/// When an operation fails on the current inner instance, it will try the next one.
#[derive(Clone, Debug)]
pub struct FailoverWrapper<T> {
    inner: Arc<Vec<(Arc<T>, String)>>,
    current_index: Arc<AtomicUsize>,
    client_count: usize,
}

impl<T> FailoverWrapper<T> {
    /// Creates a new failover wrapper.
    pub fn new(instances: Vec<(T, String)>) -> anyhow::Result<Self> {
        let count = instances.len();
        if count == 0 {
            return Err(anyhow::anyhow!("No clients available"));
        }
        Ok(Self {
            inner: Arc::new(
                instances
                    .into_iter()
                    .map(|(client, name)| (Arc::new(client), name))
                    .collect(),
            ),
            current_index: Arc::new(AtomicUsize::new(0)),
            client_count: count,
        })
    }

    /// Gets a client at the specified index (wrapped around if needed).
    async fn get_client(&self, index: usize) -> Option<Arc<T>> {
        if self.client_count == 0 {
            return None;
        }

        let wrapped_index = index % self.client_count;
        Some(self.inner[wrapped_index].0.clone())
    }

    /// Gets the name of the client at the specified index.
    fn get_name(&self, index: usize) -> Option<&str> {
        if self.client_count == 0 {
            return None;
        }
        Some(&self.inner[index % self.client_count].1)
    }

    /// Executes an operation on the current inner instance, falling back to the next one
    /// if it fails.
    pub async fn with_failover<F, Fut, R>(
        &self,
        operation: F,
        timeout: Duration,
    ) -> Result<R, RetriableClientError>
    where
        F: for<'a> Fn(Arc<T>) -> Fut,
        Fut: Future<Output = Result<R, RetriableClientError>>,
    {
        match tokio::time::timeout(timeout, async {
            let mut last_error = None;
            let start_index = self
                .current_index
                .load(std::sync::atomic::Ordering::Relaxed);
            let mut current_index = start_index;

            if self.client_count == 0 {
                panic!("No clients available in FailoverWrapper");
            }

            for _ in 0..self.client_count {
                let instance = match self.get_client(current_index).await {
                    Some(client) => client,
                    None => break,
                };

                let result = {
                    #[cfg(msim)]
                    {
                        let mut inject_error = false;
                        fail_point_if!("fallback_client_inject_error", || {
                            inject_error = should_inject_error(current_index);
                        });
                        if inject_error {
                            Err(RetriableClientError::RpcError(tonic::Status::internal(
                                "injected error for testing",
                            )))
                        } else {
                            operation(instance).await
                        }
                    }
                    #[cfg(not(msim))]
                    {
                        operation(instance).await
                    }
                };

                match result {
                    Ok(result) => {
                        self.current_index
                            .store(current_index, std::sync::atomic::Ordering::Relaxed);
                        return Ok(result);
                    }
                    Err(e) => {
                        // If the error is not retriable, return it immediately.
                        if !e.is_retriable_rpc_error() {
                            tracing::warn!(
                                "Non-retriable error on client {}, returning last failure value",
                                self.get_name(current_index).unwrap_or("unknown")
                            );
                            return Err(e);
                        }
                        last_error = Some(e);
                        tracing::warn!(
                            "Failed to execute operation on client {}, retrying \
                            with next client {}",
                            self.get_name(current_index).unwrap_or("unknown"),
                            self.get_name(current_index + 1).unwrap_or("unknown")
                        );
                        current_index += 1;
                    }
                }
            }

            Err(last_error.expect("No clients available or all operations failed"))
        })
        .await
        {
            Ok(result) => result,
            Err(_timeout) => Err(RetriableClientError::TimeoutError(_timeout)),
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
    sui_client: SuiClient,
    backoff_config: ExponentialBackoffConfig,
    metrics: Option<Arc<SuiClientMetricSet>>,
}

impl RetriableSuiClient {
    /// Creates a new retriable client.
    ///
    /// NB: If you are creating the sui client from a wallet context, you should use
    /// [`RetriableSuiClient::new_from_wallet`] instead. This is because the wallet context will
    /// make a call to the RPC server in [`WalletContext::get_client`], which may fail without any
    /// retries. `new_from_wallet` will handle this case correctly.
    pub fn new(sui_client: SuiClient, backoff_config: ExponentialBackoffConfig) -> Self {
        RetriableSuiClient {
            sui_client,
            backoff_config,
            metrics: None,
        }
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
    pub async fn new_for_rpc<S: AsRef<str>>(
        rpc_address: S,
        backoff_config: ExponentialBackoffConfig,
    ) -> SuiClientResult<Self> {
        let client = SuiClientBuilder::default().build(rpc_address).await?;
        Ok(Self::new(client, backoff_config))
    }

    /// Creates a new retriable client from a wallet context.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn new_from_wallet(
        wallet: &WalletContext,
        backoff_config: ExponentialBackoffConfig,
    ) -> SuiClientResult<Self> {
        let strategy = backoff_config.get_strategy(ThreadRng::default().gen());
        let client = retry_rpc_errors(
            strategy,
            || async { wallet.get_client().await },
            None,
            "get_client",
        )
        .await?;
        Ok(Self::new(client, backoff_config))
    }

    /// Creates a new retriable client from a wallet context with metrics.
    pub async fn new_from_wallet_with_metrics(
        wallet: &WalletContext,
        backoff_config: ExponentialBackoffConfig,
        metrics: Arc<SuiClientMetricSet>,
    ) -> SuiClientResult<Self> {
        let client = Self::new_from_wallet(wallet, backoff_config).await?;
        Ok(client.with_metrics(Some(metrics)))
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
    ) -> SuiRpcResult<Vec<Coin>> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.select_coins_inner(address, coin_type.clone(), amount, exclude.clone())
                    .await
            },
            self.metrics.clone(),
            "select_coins",
        )
        .await
    }

    /// Returns a list of coins for the given address, or an error upon failure.
    ///
    /// This is a reimplementation of the [`sui_sdk::apis::CoinReadApi::select_coins`] method, but
    /// using [`get_coins_stream_retry`] to handle retriable failures.
    async fn select_coins_inner(
        &self,
        address: SuiAddress,
        coin_type: Option<String>,
        amount: u128,
        exclude: Vec<ObjectID>,
    ) -> SuiRpcResult<Vec<Coin>> {
        let mut total = 0u128;
        let coins = self
            .get_coins_stream_retry(address, coin_type)
            .filter(|coin: &Coin| future::ready(!exclude.contains(&coin.coin_object_id)))
            .take_while(|coin: &Coin| {
                let ready = future::ready(total < amount);
                total += coin.balance as u128;
                ready
            })
            .collect::<Vec<_>>()
            .await;

        if total < amount {
            return Err(sui_sdk::error::Error::InsufficientFund { address, amount });
        }
        Ok(coins)
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
                    Some((item, (data, cursor, /* has_next_page */ true, coin_type)))
                } else if has_next_page {
                    let page = retry_rpc_errors(
                        self.get_strategy(),
                        || async {
                            self.sui_client
                                .coin_read_api()
                                .get_coins(owner, coin_type.clone(), cursor.clone(), Some(100))
                                .await
                        },
                        self.metrics.clone(),
                        "get_coins",
                    )
                    .await
                    .inspect_err(
                        |error| tracing::warn!(%error, "failed to get coins after retries"),
                    )
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
    ) -> SuiRpcResult<Balance> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .coin_read_api()
                    .get_balance(owner, coin_type.clone())
                    .await
            },
            self.metrics.clone(),
            "get_balance",
        )
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
    ) -> SuiRpcResult<ObjectsPage> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .get_owned_objects(address, query.clone(), cursor, limit)
                    .await
            },
            self.metrics.clone(),
            "get_owned_objects",
        )
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
    ) -> SuiRpcResult<SuiObjectResponse> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .get_object_with_options(object_id, options.clone())
                    .await
            },
            self.metrics.clone(),
            "get_object",
        )
        .await
    }

    /// Returns a [`SuiTransactionBlockResponse`] based on the provided [`TransactionDigest`].
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_transaction_with_options`] internally.
    pub async fn get_transaction_with_options(
        &self,
        digest: TransactionDigest,
        options: SuiTransactionBlockResponseOptions,
    ) -> SuiRpcResult<SuiTransactionBlockResponse> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .get_transaction_with_options(digest, options.clone())
                    .await
            },
            self.metrics.clone(),
            "get_transaction",
        )
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
    ) -> SuiRpcResult<Vec<SuiObjectResponse>> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .multi_get_object_with_options(object_ids.clone(), options.clone())
                    .await
            },
            self.metrics.clone(),
            "multi_get_object",
        )
        .await
    }

    /// Returns a map consisting of the move package name and the normalized module.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_normalized_move_modules_by_package`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_normalized_move_modules_by_package(
        &self,
        package_id: ObjectID,
    ) -> SuiRpcResult<BTreeMap<String, SuiMoveNormalizedModule>> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .get_normalized_move_modules_by_package(package_id)
                    .await
            },
            self.metrics.clone(),
            "get_normalized_move_modules_by_package",
        )
        .await
    }

    /// Returns the committee information for the given epoch.
    ///
    /// Calls [`sui_sdk::apis::GovernanceApi::get_committee_info`] internally.
    pub async fn get_committee_info(
        &self,
        epoch: Option<BigInt<u64>>,
    ) -> SuiRpcResult<SuiCommittee> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .governance_api()
                    .get_committee_info(epoch)
                    .await
            },
            self.metrics.clone(),
            "get_committee_info",
        )
        .await
    }

    /// Returns the reference gas price.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_reference_gas_price`] internally.
    pub async fn get_reference_gas_price(&self) -> SuiRpcResult<u64> {
        retry_rpc_errors(
            self.get_strategy(),
            || async { self.sui_client.read_api().get_reference_gas_price().await },
            self.metrics.clone(),
            "get_reference_gas_price",
        )
        .await
    }

    /// Executes a transaction dry run.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::dry_run_transaction_block`] internally.
    pub async fn dry_run_transaction_block(
        &self,
        transaction: TransactionData,
    ) -> SuiRpcResult<DryRunTransactionBlockResponse> {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                self.sui_client
                    .read_api()
                    .dry_run_transaction_block(transaction.clone())
                    .await
            },
            self.metrics.clone(),
            "dry_run_transaction_block",
        )
        .await
    }

    /// Returns a reference to the [`EventApi`].
    ///
    /// Internally calls the [`SuiClient::event_api`] function. Note that no retries are
    /// implemented for this function.
    pub fn event_api(&self) -> &EventApi {
        self.sui_client.event_api()
    }

    /// Returns a reference to the [`GovernanceApi`].
    ///
    /// Internally calls the [`SuiClient::governance_api`] function. Note that no retries are
    /// implemented for this function.
    pub fn governance_api(&self) -> &GovernanceApi {
        self.sui_client.governance_api()
    }

    /// Returns a [`SuiObjectResponse`] based on the provided [`ObjectID`].
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_object_with_options`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_sui_object<U>(&self, object_id: ObjectID) -> SuiClientResult<U>
    where
        U: AssociatedContractStruct,
    {
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                get_sui_object_from_object_response(
                    &self
                        .get_object_with_options(
                            object_id,
                            SuiObjectDataOptions::new().with_bcs().with_type(),
                        )
                        .await?,
                )
            },
            self.metrics.clone(),
            "get_sui_object",
        )
        .await
    }

    /// Returns the chain identifier.
    ///
    /// Calls [`sui_sdk::apis::ReadApi::get_chain_identifier`] internally.
    pub async fn get_chain_identifier(&self) -> SuiRpcResult<String> {
        retry_rpc_errors(
            self.get_strategy(),
            || async { self.sui_client.read_api().get_chain_identifier().await },
            self.metrics.clone(),
            "get_chain_identifier",
        )
        .await
    }

    // Other wrapper methods.

    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn get_sui_objects<U>(
        &self,
        object_ids: &[ObjectID],
    ) -> SuiClientResult<Vec<U>>
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
            ref type_arguments, ..
        }) = principal_field_type
        else {
            return Err(SuiClientError::WalTypeNotFound(package_id));
        };
        let wal_type = type_arguments
            .first()
            .ok_or_else(|| SuiClientError::WalTypeNotFound(package_id))?;
        let SuiMoveNormalizedType::Struct {
            address,
            module,
            name,
            ..
        } = wal_type
        else {
            return Err(SuiClientError::WalTypeNotFound(package_id));
        };
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
            .sui_client
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
        method: &str,
    ) -> anyhow::Result<SuiTransactionBlockResponse> {
        // Retry here must use the exact same transaction to avoid locked objects.
        retry_rpc_errors(
            self.get_strategy(),
            || async {
                #[cfg(msim)]
                {
                    maybe_return_injected_error_in_stake_pool_transaction(&transaction)?;
                }
                Ok(self
                    .sui_client
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
            },
            self.metrics.clone(),
            method,
        )
        .await
    }

    /// Gets a backoff strategy, seeded from the internal RNG.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.backoff_config.get_strategy(ThreadRng::default().gen())
    }
}

/// Error type for checkpoint download errors.
#[derive(Error, Debug)]
pub enum CheckpointDownloadError {
    /// Failed to construct URL.
    #[error("failed to construct URL: {0}")]
    UrlConstruction(#[from] ParseError),

    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    /// Failed to deserialize checkpoint data.
    #[error("failed to deserialize checkpoint data: {0}")]
    DeserializationError(String),
}

/// A client for downloading checkpoint data from a remote server.
#[derive(Clone, Debug)]
pub struct CheckpointBucketClient {
    client: reqwest::Client,
    base_url: Url,
}

impl CheckpointBucketClient {
    /// Creates a new checkpoint download client.
    pub fn new(base_url: Url, timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .expect("should be able to build reqwest client");
        Self { client, base_url }
    }

    /// Downloads a checkpoint from the remote server.
    pub async fn get_full_checkpoint(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, CheckpointDownloadError> {
        let url = self.base_url.join(&format!("{}.chk", sequence_number))?;
        tracing::debug!(%url, "downloading checkpoint from fallback bucket");
        let response = self.client.get(url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;
        let checkpoint = Blob::from_bytes::<CheckpointData>(&bytes)
            .map_err(|e| CheckpointDownloadError::DeserializationError(e.to_string()))?;
        tracing::debug!(sequence_number, "checkpoint download successful");
        Ok(checkpoint)
    }
}

/// Custom error type for RetriableRpcClient operations
#[derive(Error, Debug)]
pub enum RetriableClientError {
    /// RPC error from the primary client.
    #[error("RPC error: {0}")]
    RpcError(#[from] tonic::Status),

    /// Timeout error from the primary client.
    #[error("primary RPC timeout")]
    TimeoutError(#[from] tokio::time::error::Elapsed),

    /// Error from the fallback client.
    #[error("fallback error: {0}")]
    FallbackError(#[from] CheckpointDownloadError),

    /// Generic error
    #[error("client error: {0}")]
    Other(#[from] anyhow::Error),
}

impl RetriableClientError {
    /// Returns `true` if the error should be handled by the fallback client immediately instead of
    /// retrying the primary client.
    fn should_use_fallback_directly(&self, next_checkpoint: u64) -> bool {
        match self {
            Self::TimeoutError(_) => true,
            Self::RpcError(status) => {
                (status.code() == tonic::Code::NotFound
                    && status
                        .checkpoint_height()
                        .is_some_and(|height| next_checkpoint < height))
                    || (status.code() == tonic::Code::Internal
                        && status.message().contains("missing event"))
            }
            _ => false,
        }
    }
}

/// A [`sui_rpc_api::Client`] that retries RPC calls with backoff in case of network errors.
/// RpcClient is used primarily for retrieving checkpoint data from the Sui RPC server while
/// SuiClient is used for all other RPC calls.
#[derive(Clone)]
pub struct RetriableRpcClient {
    client: Arc<FailoverWrapper<RpcClient>>,
    request_timeout: Duration,
    main_backoff_config: ExponentialBackoffConfig,
    fallback_client: Option<CheckpointBucketClient>,
    quick_retry_config: ExponentialBackoffConfig,
    metrics: Option<Arc<SuiClientMetricSet>>,
}

impl std::fmt::Debug for RetriableRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetriableRpcClient")
            .field("main_backoff_config", &self.main_backoff_config)
            // Skip detailed client debug info since it's not typically useful
            .field("client", &"FailoverWrapper<RpcClient>")
            .field("fallback_client", &"CheckpointDownloadClient")
            .finish()
    }
}

impl RetriableRpcClient {
    // TODO(WAL-718): The timeout should ideally be set directly on the tonic client.

    /// Creates a new retriable client.
    pub fn new(
        clients: Vec<(RpcClient, String)>,
        request_timeout: Duration,
        backoff_config: ExponentialBackoffConfig,
        fallback_config: Option<RpcFallbackConfig>,
    ) -> anyhow::Result<Self> {
        let fallback_client = fallback_config.as_ref().map(|config| {
            let url = config.checkpoint_bucket.clone();
            CheckpointBucketClient::new(url, request_timeout)
        });

        let client = Arc::new(FailoverWrapper::new(clients)?);
        Ok(Self {
            client,
            request_timeout,
            main_backoff_config: backoff_config,
            fallback_client,
            quick_retry_config: fallback_config
                .map(|config| config.quick_retry_config)
                .unwrap_or_else(|| {
                    ExponentialBackoffConfig::new(
                        Duration::from_millis(100),
                        Duration::from_millis(300),
                        Some(5),
                    )
                }),
            metrics: None,
        })
    }

    /// Gets a backoff strategy, seeded from the internal RNG.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.main_backoff_config
            .get_strategy(ThreadRng::default().gen())
    }

    /// Gets a quick retry strategy, seeded from the internal RNG.
    fn get_quick_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.quick_retry_config
            .get_strategy(ThreadRng::default().gen())
    }

    /// Gets the full checkpoint data for the given sequence number.
    ///
    /// This function will first try to fetch the checkpoint from the primary client. If that fails,
    /// it will try to fetch the checkpoint from the fallback client if configured. If that also
    /// fails, it will return the error from the primary client.
    ///
    /// When the primary client returns a "not found" or "missing event" error, it will use the
    /// fallback client to fetch the checkpoint immediately. For all other errors, it will first
    /// try a quick retry on the primary client before falling back to the fallback client.
    ///
    /// If fallback is not configured, it will retry the primary client with the main retry
    /// strategy.
    #[tracing::instrument(skip(self))]
    pub async fn get_full_checkpoint(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        // First try with failover between endpoints.
        let primary_result = self
            .client
            .with_failover(
                |client| {
                    Box::pin(async move {
                        retry_rpc_errors(
                            self.get_strategy(),
                            || async { client.get_full_checkpoint(sequence_number).await },
                            self.metrics.clone(),
                            "get_full_checkpoint",
                        )
                        .await
                        .map_err(RetriableClientError::from)
                    })
                },
                self.request_timeout,
            )
            .await;
        let Err(err) = &primary_result else {
            return Ok(primary_result.unwrap());
        };

        tracing::debug!(?err, "primary client error while fetching checkpoint");
        let Some(ref fallback) = self.fallback_client else {
            tracing::debug!("No fallback client configured, retrying primary client");
            return self
                .client
                .with_failover(
                    |client| {
                        Box::pin(async move {
                            retry_rpc_errors(
                                self.get_strategy(),
                                || async { client.get_full_checkpoint(sequence_number).await },
                                self.metrics.clone(),
                                "get_full_checkpoint",
                            )
                            .await
                            .map_err(RetriableClientError::from)
                        })
                    },
                    self.request_timeout,
                )
                .await;
        };

        // For "not found" (pruned) and "missing event" errors, use fallback immediately as the
        // events in full node get pruned by event digest and checkpoint sequence number and newer
        // transactions could have events with the same digest (as the already pruned ones).
        if err.should_use_fallback_directly(sequence_number) {
            return self
                .get_full_checkpoint_from_fallback_with_retries(fallback, sequence_number)
                .await;
        }

        // Try a quick retry on the primary client before falling back
        tracing::debug!(
            "Not a missing event or not found error, trying quick retry on primary client"
        );
        let quick_retry_result = self
            .client
            .with_failover(
                |client| {
                    Box::pin(async move {
                        retry_rpc_errors(
                            self.get_quick_strategy(),
                            || async { client.get_full_checkpoint(sequence_number).await },
                            self.metrics.clone(),
                            "get_full_checkpoint_quick_retry",
                        )
                        .await
                        .map_err(RetriableClientError::from)
                    })
                },
                self.request_timeout,
            )
            .await;

        let Err(quick_retry_error) = quick_retry_result else {
            tracing::debug!("quick retry on primary client succeeded");
            return quick_retry_result;
        };

        // Fall back to the fallback client with the main retry strategy.
        tracing::debug!("falling back to fallback client after quick retry failed");
        self.get_full_checkpoint_from_fallback_with_retries(fallback, sequence_number)
            .await
            // If fallback fails as well, return the error from the quick retry.
            .map_err(|_| quick_retry_error)
    }

    async fn get_full_checkpoint_from_fallback_with_retries(
        &self,
        fallback: &CheckpointBucketClient,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        tracing::info!(sequence_number, "fetching checkpoint from fallback client");
        return retry_rpc_errors(
            self.get_strategy(),
            || async {
                fallback
                    .get_full_checkpoint(sequence_number)
                    .await
                    .map_err(RetriableClientError::from)
            },
            self.metrics.clone(),
            "get_full_checkpoint_from_fallback_with_retries",
        )
        .await;
    }

    /// Gets the checkpoint summary for the given sequence number.
    pub async fn get_checkpoint_summary(
        &self,
        sequence: u64,
    ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
        let primary_error = match self
            .client
            .with_failover(
                |client| {
                    Box::pin(async move {
                        retry_rpc_errors(
                            self.get_strategy(),
                            || async { client.get_checkpoint_summary(sequence).await },
                            self.metrics.clone(),
                            "get_checkpoint_summary",
                        )
                        .await
                        .map_err(RetriableClientError::from)
                    })
                },
                self.request_timeout,
            )
            .await
        {
            Ok(summary) => return Ok(summary),
            Err(error) => error,
        };

        let Some(ref fallback) = self.fallback_client else {
            return Err(primary_error);
        };

        tracing::info!("falling back to fallback client to fetch checkpoint summary");
        let checkpoint = self
            .get_full_checkpoint_from_fallback_with_retries(fallback, sequence)
            .await
            // If fallback fails as well, return the error from the quick retry.
            .map_err(|_| primary_error)?;
        Ok(checkpoint.checkpoint_summary)
    }

    /// Gets the latest checkpoint sequence number.
    pub async fn get_latest_checkpoint_summary(
        &self,
    ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
        self.client
            .with_failover(
                |client| {
                    Box::pin(async move {
                        retry_rpc_errors(
                            self.get_strategy(),
                            || async { client.get_latest_checkpoint().await },
                            self.metrics.clone(),
                            "get_latest_checkpoint_summary",
                        )
                        .await
                        .map_err(RetriableClientError::from)
                    })
                },
                self.request_timeout,
            )
            .await
    }

    /// Gets the object with the given ID.
    pub async fn get_object(&self, id: ObjectID) -> Result<Object, RetriableClientError> {
        self.client
            .with_failover(
                |client| {
                    Box::pin(async move {
                        retry_rpc_errors(
                            self.get_strategy(),
                            || async { client.get_object(id).await },
                            self.metrics.clone(),
                            "get_object",
                        )
                        .await
                        .map_err(RetriableClientError::from)
                    })
                },
                self.request_timeout,
            )
            .await
    }
}

/// Injects a simulated error for testing retry behavior executing sui transactions.
/// We use stake_with_pool as an example here to incorporate with the test logic in
/// `test_ptb_executor_retriable_error` in `test_client.rs`.
#[cfg(msim)]
fn maybe_return_injected_error_in_stake_pool_transaction(
    transaction: &Transaction,
) -> anyhow::Result<()> {
    // Check if this transaction contains a stake_with_pool operation
    let is_stake_pool_tx = transaction
        .transaction_data()
        .move_calls()
        .iter()
        .any(|(_, _, function_name)| *function_name == contracts::staking::stake_with_pool.name);

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

        // Simulate a retriable RPC error (502 Bad Gateway)
        Err(sui_sdk::error::Error::RpcError(
            jsonrpsee::core::ClientError::Custom(
                "server returned an error status code: 502".into(),
            ),
        ))?;
    }

    Ok(())
}

impl ToErrorType for anyhow::Error {
    fn to_error_type(&self) -> String {
        self.downcast_ref::<sui_sdk::error::Error>()
            .map(|error| error.to_error_type())
            .unwrap_or_else(|| "other".to_string())
    }
}

impl ToErrorType for sui_sdk::error::Error {
    fn to_error_type(&self) -> String {
        match self {
            Self::RpcError(e) => {
                let err_str = e.to_string();
                if err_str.contains("Call") {
                    "rpc_call"
                } else if err_str.contains("Transport") {
                    "rpc_transport"
                } else if err_str.contains("RequestTimeout") {
                    "rpc_timeout"
                } else {
                    "rpc_other"
                }
                .to_string()
            }
            Self::JsonRpcError(_) => "json_rpc".to_string(),
            Self::BcsSerialisationError(_) => "bcs_ser".to_string(),
            Self::JsonSerializationError(_) => "json_ser".to_string(),
            Self::UserInputError(_) => "user_input".to_string(),
            Self::Subscription(_) => "subscription".to_string(),
            Self::FailToConfirmTransactionStatus(_, _) => "tx_confirm".to_string(),
            Self::DataError(_) => "data_error".to_string(),
            Self::ServerVersionMismatch { .. } => "version_mismatch".to_string(),
            Self::InsufficientFund { .. } => "insufficient_fund".to_string(),
            Self::InvalidSignature => "invalid_sig".to_string(),
        }
    }
}

impl ToErrorType for SuiClientError {
    fn to_error_type(&self) -> String {
        match self {
            Self::SuiSdkError(error) => error.to_error_type(),
            _ => "sui_client_error_other".to_string(),
        }
    }
}

impl ToErrorType for tonic::Status {
    fn to_error_type(&self) -> String {
        "other".to_string()
    }
}

impl ToErrorType for RetriableClientError {
    fn to_error_type(&self) -> String {
        match self {
            Self::RpcError(status) => status.to_error_type(),
            Self::FallbackError(_) => "fallback_error".to_string(),
            Self::Other(_) => "other".to_string(),
            Self::TimeoutError(_) => "timeout".to_string(),
        }
    }
}

#[cfg(msim)]
fn should_inject_error(current_index: usize) -> bool {
    // For odd indices, always inject error
    if current_index % 2 == 1 {
        tracing::warn!(
            "Injecting a RPC error for fallback client {} (odd index)",
            current_index
        );
        true
    } else {
        // For even indices, 50% chance
        let should_fail = rand::random::<bool>();
        if should_fail {
            tracing::warn!(
                "Injecting a RPC error for fallback client {} (even index, random)",
                current_index
            );
            true
        } else {
            false
        }
    }
}
