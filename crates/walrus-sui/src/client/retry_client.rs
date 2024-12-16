// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`SuiClient`] to introduce retries.

use std::{fmt::Debug, future::Future, str::FromStr};

use rand::{
    rngs::{StdRng, ThreadRng},
    Rng as _,
};
use serde::{de::DeserializeOwned, Serialize};
use sui_sdk::{
    apis::EventApi,
    error::SuiRpcResult,
    rpc_types::{
        Balance,
        Coin,
        ObjectsPage,
        SuiObjectDataOptions,
        SuiObjectResponse,
        SuiObjectResponseQuery,
        SuiRawData,
    },
    wallet_context::WalletContext,
    SuiClient,
    SuiClientBuilder,
};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    dynamic_field::derive_dynamic_field_id,
    TypeTag,
};
use tracing::Level;
use walrus_utils::backoff::{BackoffStrategy, ExponentialBackoff, ExponentialBackoffConfig};

use super::{SuiClientError, SuiClientResult};
use crate::{
    contracts::{AssociatedContractStruct, TypeOriginMap},
    types::move_structs::{SuiDynamicField, SystemObjectForDeserialization},
    utils::{get_package_id_from_object_response, get_sui_object_from_object_response},
};

/// The list of HTTP status codes that are retriable.
const RETRIABLE_RPC_ERRORS: &[&str] = &["429", "500", "502"];

/// Trait to test if an error is produced by a temporary RPC failure and can be retried.
pub trait RetriableRpcError: Debug {
    /// Returns `true` if the error is a retriable network error.
    fn is_retriable_rpc_error(&self) -> bool;
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

/// Retries the given function while it returns retriable errors.[
async fn retry_rpc_errors<S, F, T, E, Fut>(mut strategy: S, mut func: F) -> Result<T, E>
where
    S: BackoffStrategy,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetriableRpcError,
{
    loop {
        let value = func().await;

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
        }
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
        let client = retry_rpc_errors(strategy, || async { wallet.get_client().await }).await?;
        Ok(Self::new(client, backoff_config))
    }

    // Reimplementation of the `SuiClient` methods.

    /// Return a list of coins for the given address, or an error upon failure.
    ///
    /// Calls [`sui_sdk::apis::CoinReadApi::select_coins`] internally.
    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub async fn select_coins(
        &self,
        address: SuiAddress,
        coin_type: Option<String>,
        amount: u128,
        exclude: Vec<ObjectID>,
    ) -> SuiRpcResult<Vec<Coin>> {
        retry_rpc_errors(self.get_strategy(), || async {
            self.sui_client
                .coin_read_api()
                .select_coins(address, coin_type.clone(), amount, exclude.clone())
                .await
        })
        .await
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
        retry_rpc_errors(self.get_strategy(), || async {
            self.sui_client
                .coin_read_api()
                .get_balance(owner, coin_type.clone())
                .await
        })
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
        retry_rpc_errors(self.get_strategy(), || async {
            self.sui_client
                .read_api()
                .get_owned_objects(address, query.clone(), cursor, limit)
                .await
        })
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
        retry_rpc_errors(self.get_strategy(), || async {
            self.sui_client
                .read_api()
                .get_object_with_options(object_id, options.clone())
                .await
        })
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
        retry_rpc_errors(self.get_strategy(), || async {
            self.sui_client
                .read_api()
                .multi_get_object_with_options(object_ids.clone(), options.clone())
                .await
        })
        .await
    }

    /// Returns a reference to the [`EventApi`].
    ///
    /// Internally calls the [`SuiClient::event_api`] function. Note that no retries are
    /// implemented for this function.
    pub fn event_api(&self) -> &EventApi {
        self.sui_client.event_api()
    }

    // Other wrapper methods.

    #[tracing::instrument(level = Level::DEBUG, skip_all)]
    pub(crate) async fn get_sui_object<U>(&self, object_id: ObjectID) -> SuiClientResult<U>
    where
        U: AssociatedContractStruct,
    {
        retry_rpc_errors(self.get_strategy(), || async {
            get_sui_object_from_object_response(
                &self
                    .get_object_with_options(
                        object_id,
                        SuiObjectDataOptions::new().with_bcs().with_type(),
                    )
                    .await?,
            )
        })
        .await
    }

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
        let object_id = derive_dynamic_field_id(
            parent,
            &TypeTag::from_str(&format!("0x2::dynamic_object_field::Wrapper<{}>", key_tag))
                .expect("valid type tag"),
            &bcs::to_bytes(&key).expect("key should be serializable"),
        )
        .map_err(|err| SuiClientError::Internal(err.into()))?;

        let field: SuiDynamicField<K, ObjectID> = self.get_sui_object(object_id).await?;
        let inner = self.get_sui_object(field.value).await?;
        Ok(inner)
    }

    /// Checks if the Walrus system object exist on chain and returns the Walrus package ID.
    pub(crate) async fn get_system_package_id_from_system_object(
        &self,
        system_object_id: ObjectID,
    ) -> SuiClientResult<ObjectID> {
        let response = self
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

    /// Gets a backoff strategy, seeded from the internal RNG.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.backoff_config.get_strategy(ThreadRng::default().gen())
    }
}
