// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`SuiClient`] to introduce retries.
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Duration};

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
use serde::{de::DeserializeOwned, Serialize};
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
use sui_types::{
    base_types::{ObjectID, SuiAddress, TransactionDigest},
    dynamic_field::derive_dynamic_field_id,
    quorum_driver_types::ExecuteTransactionRequestType::WaitForLocalExecution,
    sui_serde::BigInt,
    transaction::{Transaction, TransactionData, TransactionKind},
    TypeTag,
};
use tracing::Level;
use walrus_core::ensure;
use walrus_utils::backoff::{ExponentialBackoff, ExponentialBackoffConfig};

#[cfg(msim)]
use super::maybe_return_injected_error_in_stake_pool_transaction;
use super::{retry_rpc_errors, SuiClientError, SuiClientResult};
use crate::{
    client::SuiClientMetricSet,
    contracts::{self, AssociatedContractStruct, TypeOriginMap},
    types::move_structs::{Key, Subsidies, SuiDynamicField, SystemObjectForDeserialization},
    utils::get_sui_object_from_object_response,
};

/// The maximum gas allowed in a transaction, in MIST (50 SUI). Used for gas budget estimation.
const MAX_GAS_BUDGET: u64 = 50_000_000_000;

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
        request_timeout: Option<Duration>,
    ) -> SuiClientResult<Self> {
        let mut client_builder = SuiClientBuilder::default();

        if let Some(request_timeout) = request_timeout {
            client_builder = client_builder.request_timeout(request_timeout);
        }

        let client = client_builder.build(rpc_address).await?;
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
                    Some((item, (data, cursor, has_next_page, coin_type)))
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
        for obj_id_batch in object_ids.chunks(super::MULTI_GET_OBJ_LIMIT) {
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

        let safe_overhead = super::GAS_SAFE_OVERHEAD * gas_price;
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
