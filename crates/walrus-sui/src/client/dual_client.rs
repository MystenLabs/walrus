// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to access Sui via both JSON RPC and gRPC. This module is intended to facilitate a
//! migration from Sui JSON RPC to gRPC by gradually migrating callsites away from the JSON RPC
//! Client [`SuiClient`].

use std::{str::FromStr, time::Duration};

use anyhow::Context;
use bytes::Bytes;
use move_core_types::{account_address::AccountAddress, language_storage::StructTag};
use sui_rpc::{
    Client as GrpcClient,
    field::{FieldMask, FieldMaskUtil},
    proto::sui::rpc::v2::{
        BatchGetObjectsRequest,
        BatchGetObjectsResponse,
        Bcs,
        DynamicField,
        ExecutedTransaction,
        GetBalanceRequest,
        GetObjectRequest,
        GetTransactionRequest,
        ListDynamicFieldsRequest,
        ListOwnedObjectsRequest,
        ListOwnedObjectsResponse,
        Object,
        get_object_result,
        state_service_client::StateServiceClient,
    },
};
use sui_sdk::{
    SuiClient,
    SuiClientBuilder,
    rpc_types::{
        BalanceChange as SuiBalanceChange,
        BcsEvent,
        SuiEvent,
        SuiTransactionBlockResponse,
        SuiTransactionBlockResponseOptions,
    },
};
use sui_types::{
    TypeTag,
    base_types::{ObjectID, ObjectRef, SuiAddress},
    crypto::ToFromBytes,
    digests::TransactionDigest,
    event::EventID,
    object::Owner,
    signature::GenericSignature,
    transaction::{SenderSignedData, TransactionData},
};
use tonic::service::interceptor::InterceptedService;
use walrus_core::ensure;

use crate::{
    client::SuiClientError,
    coin::Coin,
    contracts::{AssociatedContractStruct, TypeOriginMap},
    dynamic_field_info::{DynamicFieldPage, dynamic_field_info_from_grpc},
};

/// The maximum number of objects to request in a single "batch" gRPC call.
pub const MAX_GET_OBJECTS_BATCH_SIZE: usize = 100;
const MAX_OWNED_OBJECTS_BATCH_SIZE: u32 = 1000;
const MAX_SELECT_COINS_BATCH_SIZE: u32 = 1000;

/// A client that combines the Sui SDK client and a gRPC client in order to facilitate a migration
/// from Sui JSON RPC to gRPC by gradually migrating callsites away from [`SuiClient`].
#[derive(Clone)]
pub struct DualClient {
    /// The Sui SDK client for JSON RPC calls. This will eventually be removed.
    sui_client: Option<SuiClient>,
    grpc_client: GrpcClient,
}

impl std::fmt::Debug for DualClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualClient").finish()
    }
}

/// A helper function to convert an `ObjectID` to a `sui_sdk_types::Address` without any
/// branching.
fn address_from_object_id(object_id: sui_types::base_types::ObjectID) -> sui_sdk_types::Address {
    sui_sdk_types::Address::from(<[u8; sui_sdk_types::Address::LENGTH]>::from(
        AccountAddress::from(object_id),
    ))
}

/// A BCS-encoded object along with its version, and type.
#[derive(Debug)]
pub struct BcsDatapack {
    /// The BCS-encoded object.
    pub bcs: Bcs,
    /// The BCS-encoded object's type.
    pub struct_tag: StructTag,
    /// The version of the object.
    pub version: u64,
    /// `initial_shared_version` for shared objects.
    pub initial_shared_version: Option<u64>,
}

/// A batch of objects returned from Sui via gRPC.
pub(crate) struct ObjectBatch<U> {
    pub objects_with_refs: Vec<(U, ObjectRef)>,
    pub next_page_token: Option<Bytes>,
}

/// A batch of coins returned from Sui via gRPC.
pub(crate) struct CoinBatch {
    pub coins: Vec<Coin>,
    pub next_page_token: Option<Bytes>,
}

impl DualClient {
    /// Create a new DualClient with the given RPC URL and optional request timeout.
    pub async fn new(
        rpc_url: impl AsRef<str>,
        request_timeout: Option<Duration>,
    ) -> Result<Self, SuiClientError> {
        let mut client_builder = SuiClientBuilder::default();
        if let Some(request_timeout) = request_timeout {
            client_builder = client_builder.request_timeout(request_timeout);
        }
        let rpc_url = rpc_url.as_ref();
        let sui_client = Some(client_builder.build(rpc_url).await?);
        let grpc_client = GrpcClient::new(rpc_url).context("unable to create grpc client")?;
        Ok(Self {
            sui_client,
            grpc_client,
        })
    }

    /// Accessor for the SuiClient. Note that when the migration is complete, we will check the
    /// migration level and if it is at its "max" setting, then we will not create SuiClients and
    /// this method will panic if called.
    pub fn sui_client(&self) -> &SuiClient {
        self.sui_client
            .as_ref()
            .expect("DualClient should have a SuiClient until migration is complete")
    }

    /// Get the BCS representation of an object from the Sui network.
    pub async fn get_object_bcs(&self, object_id: ObjectID) -> Result<Bcs, SuiClientError> {
        let request = GetObjectRequest::new(&address_from_object_id(object_id)).with_read_mask(
            FieldMask::from_paths([Object::path_builder().bcs().finish()]),
        );
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;
        Ok(response
            .into_inner()
            .object
            .context("no object in get_object_response")?
            .bcs
            .context("no bcs in object")?)
    }

    /// Get the BCS representation of an object's contents and its type from the Sui network.
    pub async fn get_previous_transaction(
        &self,
        object_id: ObjectID,
    ) -> Result<TransactionDigest, SuiClientError> {
        let paths = [Object::path_builder().previous_transaction()];
        let request: GetObjectRequest = GetObjectRequest::new(&address_from_object_id(object_id))
            .with_read_mask(FieldMask::from_paths(&paths));
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;
        Ok(response
            .into_inner()
            .object
            .context("no contents in get_object_response")?
            .previous_transaction
            .context("no previous_transaction in object")?
            .parse()
            .context("parsing previous_transaction")?)
    }

    /// Get a transaction response from the Sui network via gRPC.
    ///
    /// Handles: checkpoint, timestamp, raw_input, balance_changes, and events.
    /// Falls back to JSON-RPC for show_input, show_effects, show_object_changes,
    /// show_raw_effects.
    pub async fn get_transaction_response_grpc(
        &self,
        digest: TransactionDigest,
        options: &SuiTransactionBlockResponseOptions,
    ) -> Result<SuiTransactionBlockResponse, SuiClientError> {
        let field_mask = build_transaction_field_mask(options);
        let executed_tx = self.get_transaction_raw(digest, field_mask).await?;
        executed_transaction_to_response(digest, options, &executed_tx)
    }

    /// Get events for a transaction from the Sui network via gRPC.
    pub async fn get_transaction_events_grpc(
        &self,
        tx_digest: TransactionDigest,
    ) -> Result<Vec<SuiEvent>, SuiClientError> {
        let field_mask = FieldMask::from_paths(&[ExecutedTransaction::path_builder()
            .events()
            .events()
            .finish()]);
        let executed_tx = self.get_transaction_raw(tx_digest, field_mask).await?;
        let empty = Vec::new();
        executed_tx
            .events
            .as_ref()
            .map(|e| &e.events)
            .unwrap_or(&empty)
            .iter()
            .enumerate()
            .map(|(idx, event)| grpc_event_to_sui_event(tx_digest, idx, event))
            .collect::<Result<Vec<_>, _>>()
    }

    /// Low-level gRPC call to get a transaction with a specific field mask.
    ///
    /// Uses `Box::pin` to erase the future type and avoid `Send` recursion overflow
    /// from the deep `ExecutedTransaction` proto type tree.
    fn get_transaction_raw(
        &self,
        digest: TransactionDigest,
        field_mask: FieldMask,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ExecutedTransaction, SuiClientError>>
                + Send
                + '_,
        >,
    > {
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        Box::pin(async move {
            let sui_digest = sui_sdk_types::Digest::new(digest.into_inner());
            let request = GetTransactionRequest::new(&sui_digest).with_read_mask(field_mask);
            let response = grpc_client.ledger_client().get_transaction(request).await?;
            Ok(response
                .into_inner()
                .transaction
                .context("no transaction in get_transaction_response")?)
        })
    }

    /// Get the BCS representation of an object's contents and its type from the Sui network.
    pub async fn get_object_contents(
        &self,
        object_id: ObjectID,
    ) -> Result<(StructTag, Bcs), SuiClientError> {
        let paths = [
            Object::path_builder().contents().finish(),
            Object::path_builder().object_type(),
        ];
        let request: GetObjectRequest = GetObjectRequest::new(&address_from_object_id(object_id))
            .with_read_mask(FieldMask::from_paths(&paths));
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;
        let object = response
            .into_inner()
            .object
            .context("no contents in get_object_response")?;
        Ok((
            object
                .object_type()
                .parse()
                .context("parsing move object_type")?,
            object.contents.context("no contents in object")?,
        ))
    }

    /// Get the full object from the Sui network.
    pub async fn get_object(
        &self,
        object_id: ObjectID,
    ) -> Result<sui_types::object::Object, SuiClientError> {
        Ok(self
            .get_object_bcs(object_id)
            .await?
            .deserialize()
            .context("failed to deserialize object from bcs")?)
    }

    /// Get an [`ObjectRef`] from the Sui network.
    pub async fn get_object_ref(&self, object_id: ObjectID) -> Result<ObjectRef, SuiClientError> {
        let request = GetObjectRequest::new(&address_from_object_id(object_id)).with_read_mask(
            FieldMask::from_paths([
                Object::path_builder().version(),
                Object::path_builder().digest(),
            ]),
        );
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;
        let object = response
            .into_inner()
            .object
            .context("no object in get_object_response")?;
        Ok((
            object_id,
            object.version.context("no version in object")?.into(),
            object
                .digest
                .context("no digest in object")?
                .parse()
                .context("parsing digest")?,
        ))
    }

    /// Get an [`ObjectRef`] from the Sui network.
    pub async fn get_object_ref_and_type_tag(
        &self,
        object_id: ObjectID,
    ) -> Result<(ObjectRef, TypeTag), SuiClientError> {
        let request = GetObjectRequest::new(&address_from_object_id(object_id)).with_read_mask(
            FieldMask::from_paths([
                Object::path_builder().version(),
                Object::path_builder().digest(),
                Object::path_builder().object_type(),
            ]),
        );
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;
        let object = response
            .into_inner()
            .object
            .context("no object in get_object_response")?;
        Ok((
            // ObjectRef
            (
                object_id,
                object.version.context("no version in object")?.into(),
                object
                    .digest
                    .context("no digest in object")?
                    .parse()
                    .context("parsing digest")?,
            ),
            // TypeTag
            object
                .object_type
                .context("no object_type in object")?
                .parse::<StructTag>()
                .context("parsing move object_type")?
                .into(),
        ))
    }

    /// Get multiple objects' Sui ObjectRefs.
    pub async fn get_object_refs(
        &self,
        object_ids: &[ObjectID],
    ) -> Result<Vec<ObjectRef>, SuiClientError> {
        batch_get_objects(
            self.grpc_client.clone(),
            object_ids,
            FieldMask::from_paths([
                Object::path_builder().object_id(),
                Object::path_builder().version(),
                Object::path_builder().digest(),
            ]),
            |object| {
                Ok((
                    object
                        .object_id
                        .context("no object_id in object")?
                        .parse()
                        .context("parsing object_id")?,
                    object.version.context("no version in object")?.into(),
                    object
                        .digest
                        .context("no digest in object")?
                        .parse()
                        .context("parsing digest")?,
                ))
            },
        )
        .await
    }

    /// Get multiple objects' BCS representations and versions from the Sui network.
    pub async fn multi_get_objects_bcs_datapacks(
        &self,
        object_ids: &[ObjectID],
    ) -> Result<Vec<BcsDatapack>, SuiClientError> {
        batch_get_objects(
            self.grpc_client.clone(),
            object_ids,
            FieldMask::from_paths([
                Object::path_builder().contents().finish(),
                Object::path_builder().object_type(),
                Object::path_builder().version(),
                Object::path_builder().owner().version(),
            ]),
            |object| {
                Ok(BcsDatapack {
                    bcs: object.contents.context("no contents in object")?,
                    struct_tag: object
                        .object_type
                        .context("no object_type in object")?
                        .parse()
                        .context("parsing move object_type")?,
                    version: object.version.context("no version in object")?,
                    initial_shared_version: object.owner.and_then(|owner| owner.version),
                })
            },
        )
        .await
    }

    /// Get the type origin map for a package from the Sui network.
    pub async fn get_type_origin_map_for_package(
        &self,
        package_id: ObjectID,
    ) -> Result<TypeOriginMap, SuiClientError> {
        let request = GetObjectRequest::new(&address_from_object_id(package_id)).with_read_mask(
            FieldMask::from_paths([Object::path_builder().package().type_origins().finish()]),
        );

        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client.ledger_client().get_object(request).await?;

        let package = response
            .into_inner()
            .object
            .context("no contents in get_object_response")?
            .package
            .context("no package in object")?;

        let mut type_origins = TypeOriginMap::default();
        for origin in package.type_origins() {
            type_origins.insert(
                (
                    origin
                        .module_name
                        .clone()
                        .context("missing module_name in type_origin")?,
                    origin
                        .datatype_name
                        .clone()
                        .context("missing datatype_name in type_origin")?,
                ),
                origin
                    .package_id
                    .as_ref()
                    .and_then(|package_id| package_id.parse().ok())
                    .context("could not parse package_id")?,
            );
        }
        Ok(type_origins)
    }

    pub(crate) async fn fetch_batch_of_objects<U: AssociatedContractStruct>(
        &self,
        owner: SuiAddress,
        object_type: &str,
        page_token: Option<Bytes>,
    ) -> Result<ObjectBatch<U>, SuiClientError> {
        tracing::debug!(
            ?owner,
            ?object_type,
            ?page_token,
            "DualClient::fetch_batch_of_objects called"
        );
        let mut grpc_client = self.grpc_client.clone();
        let state_client = grpc_client.state_client();

        // NB: in the event of failover handled at the callsite, the `page_token` here might have
        // come from a previous gRPC response from a different host. The token is intentionally
        // opaque to us, but according to current Sui gRPC server semantics, it should be valid
        // across different servers - but this is not by design, and may change in the future, so
        // this is something to keep an eye on. If pagination tokens become server-specific, we may
        // need to rewrite the client to perform a full retry of the stream from the beginning,
        // instead of using this incremental approach across servers.
        let response = list_owned_objects(
            owner,
            object_type,
            page_token,
            state_client,
            MAX_OWNED_OBJECTS_BATCH_SIZE,
        )
        .await
        .with_context(|| {
            format!(
                "error listing owned objects for owner [owner={:?}, object_type={:?}]",
                owner, object_type
            )
        })?;

        let mut objects_with_refs = Vec::new();
        let list_owned_objects_response = response.into_inner();
        for object in list_owned_objects_response.objects {
            match convert_grpc_object_to_object_with_ref(object) {
                Ok(object_with_ref) => objects_with_refs.push(object_with_ref),
                Err(error) => {
                    return Err(anyhow::anyhow!(
                        "error converting object [owner={:?}, coin_type={:?}]: {error:?}",
                        owner,
                        object_type,
                    )
                    .into());
                }
            }
        }

        Ok(ObjectBatch {
            objects_with_refs,
            next_page_token: list_owned_objects_response.next_page_token,
        })
    }

    pub(crate) async fn fetch_batch_of_coins(
        &self,
        owner: SuiAddress,
        object_type: &str,
        page_token: Option<Bytes>,
    ) -> Result<CoinBatch, SuiClientError> {
        tracing::debug!(
            ?owner,
            ?object_type,
            ?page_token,
            "DualClient::fetch_batch_of_coins called"
        );
        let mut grpc_client = self.grpc_client.clone();
        let state_client = grpc_client.state_client();

        // NB: in the event of failover handled at the callsite, the `page_token` here might have
        // come from a previous gRPC response from a different host. The token is intentionally
        // opaque to us, but according to current Sui gRPC server semantics, it should be valid
        // across different servers - but this is not by design, and may change in the future, so
        // this is something to keep an eye on. If pagination tokens become server-specific, we may
        // need to rewrite the client to perform a full retry of the stream from the beginning,
        // instead of using this incremental approach across servers.
        let response = list_owned_coin_objects(owner, object_type, page_token, state_client).await;

        let mut coins = Vec::new();
        match response {
            Ok(response) => {
                let list_owned_objects_response = response.into_inner();
                for object in list_owned_objects_response.objects {
                    match convert_grpc_object_to_coin(object) {
                        Ok(coin) => coins.push(coin),
                        Err(error) => {
                            return Err(anyhow::anyhow!(
                                "error converting object to coin for owner \
                                    [owner={:?}, object_type={:?}]: {error:?}",
                                owner,
                                object_type,
                            )
                            .into());
                        }
                    }
                }

                Ok(CoinBatch {
                    coins,
                    next_page_token: list_owned_objects_response.next_page_token,
                })
            }
            Err(error) => Err(anyhow::anyhow!(
                "error listing owned objects for owner [owner={:?}, object_type={:?}]: {error:?}",
                owner,
                object_type,
            )
            .into()),
        }
    }

    /// Get the total balance for a given owner and coin type. This routine avoids enumerating all
    /// the coin objects on the client side by using the gRPC `GetBalance` method.
    pub async fn get_total_balance(
        &self,
        owner: SuiAddress,
        coin_type: &str,
    ) -> Result<u64, SuiClientError> {
        let get_balance_request = GetBalanceRequest::default()
            .with_owner(owner.to_string())
            .with_coin_type(coin_type.to_string());
        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let mut state_client = grpc_client.state_client();
        let coin_balance = state_client
            .get_balance(get_balance_request)
            .await
            .context("failed to get total balance")?
            .into_inner()
            .balance()
            .coin_balance();
        Ok(coin_balance)
    }

    /// List dynamic fields of an object via gRPC.
    pub(crate) async fn list_dynamic_fields(
        &self,
        parent: ObjectID,
        page_token: Option<Bytes>,
        limit: u32,
    ) -> Result<DynamicFieldPage, SuiClientError> {
        let mut request = ListDynamicFieldsRequest::default()
            .with_parent(parent.to_string())
            .with_read_mask(FieldMask::from_paths([
                DynamicField::path_builder().kind(),
                DynamicField::path_builder().field_id(),
                DynamicField::path_builder().name().finish(),
                DynamicField::path_builder().value_type(),
            ]))
            .with_page_size(limit);
        if let Some(page_token) = page_token {
            request = request.with_page_token(page_token);
        }

        let mut grpc_client: GrpcClient = self.grpc_client.clone();
        let response = grpc_client
            .state_client()
            .list_dynamic_fields(request)
            .await
            .context("failed to list dynamic fields")?
            .into_inner();

        let mut fields = Vec::new();
        for df in response.dynamic_fields {
            fields.push(
                dynamic_field_info_from_grpc(df)
                    .context("converting gRPC DynamicField to DynamicFieldInfo")?,
            );
        }

        Ok(DynamicFieldPage {
            fields,
            has_next_page: response.next_page_token.is_some(),
            next_cursor: response.next_page_token,
        })
    }
}

async fn list_owned_objects(
    owner: SuiAddress,
    object_type: &str,
    next_page_token: Option<Bytes>,
    mut state_client: StateServiceClient<
        InterceptedService<&mut tonic::transport::Channel, &sui_rpc::client::HeadersInterceptor>,
    >,
    batch_size: u32,
) -> Result<tonic::Response<ListOwnedObjectsResponse>, tonic::Status> {
    let mut request = ListOwnedObjectsRequest::default()
        .with_owner(owner.to_string())
        .with_read_mask(FieldMask::from_paths([
            Object::path_builder().object_type(),
            Object::path_builder().object_id(),
            Object::path_builder().contents().finish(),
            Object::path_builder().version(),
            Object::path_builder().digest(),
            Object::path_builder().balance(),
            Object::path_builder().previous_transaction(),
        ]))
        .with_page_size(batch_size)
        .with_object_type(object_type);
    if let Some(next_page_token) = next_page_token {
        request = request.with_page_token(next_page_token);
    }
    state_client.list_owned_objects(request).await
}

async fn list_owned_coin_objects(
    owner: SuiAddress,
    object_type: &str,
    next_page_token: Option<Bytes>,
    state_client: StateServiceClient<
        InterceptedService<&mut tonic::transport::Channel, &sui_rpc::client::HeadersInterceptor>,
    >,
) -> Result<tonic::Response<ListOwnedObjectsResponse>, tonic::Status> {
    list_owned_objects(
        owner,
        object_type,
        next_page_token,
        state_client,
        MAX_SELECT_COINS_BATCH_SIZE,
    )
    .await
}

fn convert_grpc_object_to_object_with_ref<U: AssociatedContractStruct>(
    object: Object,
) -> Result<(U, ObjectRef), anyhow::Error> {
    let user_object: U = object
        .contents
        .context("no contents in object")?
        .deserialize()
        .with_context(|| {
            format!(
                "deserializing object contents into struct {}",
                U::CONTRACT_STRUCT
            )
        })?;
    Ok((
        user_object,
        (
            object
                .object_id
                .context("no object_id in object")?
                .parse()
                .context("parsing object_id")?,
            object.version.context("no version in object")?.into(),
            object
                .digest
                .context("no digest in object")?
                .parse()
                .context("parsing digest")?,
        ),
    ))
}

fn convert_grpc_object_to_coin(object: Object) -> Result<Coin, anyhow::Error> {
    let struct_tag = object
        .object_type
        .context("no object_type in object")?
        .parse::<StructTag>()
        .context("parsing move object_type")?;
    ensure!(
        struct_tag.module.as_str() == "coin" && struct_tag.name.as_str() == "Coin",
        "object is not a coin"
    );
    Ok(Coin {
        coin_type: struct_tag.to_string(),
        coin_object_id: object
            .object_id
            .context("no object_id in object")?
            .parse()
            .context("parsing object_id")?,
        version: object.version.context("no version in object")?.into(),
        digest: object
            .digest
            .context("no digest in object")?
            .parse()
            .context("parsing digest")?,
        balance: object.balance.context("no balance in object")?,
        previous_transaction: object
            .previous_transaction
            .context("no previous_transaction in object")?
            .parse()
            .context("parsing previous_transaction")?,
    })
}

/// Build a [`FieldMask`] for `GetTransactionRequest` based on the requested options.
///
/// Always requests `digest`, `checkpoint`, and `timestamp`. Conditionally adds paths for
/// `raw_input` (transaction BCS + signature BCS), `balance_changes`, and `events`.
fn build_transaction_field_mask(options: &SuiTransactionBlockResponseOptions) -> FieldMask {
    let mut paths: Vec<String> = vec![
        ExecutedTransaction::path_builder().digest(),
        ExecutedTransaction::path_builder().checkpoint(),
        ExecutedTransaction::path_builder().timestamp(),
    ];
    if options.show_raw_input {
        paths.push(
            ExecutedTransaction::path_builder()
                .transaction()
                .bcs()
                .finish(),
        );
        paths.push(
            ExecutedTransaction::path_builder()
                .signatures()
                .bcs()
                .finish(),
        );
    }
    if options.show_balance_changes {
        paths.push(
            ExecutedTransaction::path_builder()
                .balance_changes()
                .finish(),
        );
    }
    if options.show_events {
        paths.push(
            ExecutedTransaction::path_builder()
                .events()
                .events()
                .finish(),
        );
    }
    FieldMask::from_paths(&paths)
}

/// Convert a gRPC [`ExecutedTransaction`] into a [`SuiTransactionBlockResponse`].
fn executed_transaction_to_response(
    digest: TransactionDigest,
    options: &SuiTransactionBlockResponseOptions,
    executed_tx: &ExecutedTransaction,
) -> Result<SuiTransactionBlockResponse, SuiClientError> {
    let mut response = SuiTransactionBlockResponse::new(digest);

    response.checkpoint = executed_tx.checkpoint;
    response.timestamp_ms = executed_tx
        .timestamp
        .as_ref()
        .map(|ts| {
            u64::try_from(ts.seconds)
                .context("negative timestamp")
                .map(|s| s * 1000 + u64::try_from(ts.nanos).unwrap_or(0) / 1_000_000)
        })
        .transpose()?;

    if options.show_raw_input {
        response.raw_transaction = reconstruct_raw_transaction(executed_tx)?;
    }

    if options.show_balance_changes {
        response.balance_changes = Some(
            executed_tx
                .balance_changes
                .iter()
                .map(convert_grpc_balance_change)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }

    if options.show_events
        && let Some(events) = &executed_tx.events
    {
        let sui_events: Vec<SuiEvent> = events
            .events
            .iter()
            .enumerate()
            .map(|(idx, event)| grpc_event_to_sui_event(digest, idx, event))
            .collect::<Result<Vec<_>, _>>()?;
        response.events = Some(sui_sdk::rpc_types::SuiTransactionBlockEvents { data: sui_events });
    }

    Ok(response)
}

/// Reconstruct the BCS-encoded `SenderSignedData` (raw_transaction) from gRPC fields.
fn reconstruct_raw_transaction(
    executed_tx: &ExecutedTransaction,
) -> Result<Vec<u8>, SuiClientError> {
    let tx_bcs = executed_tx
        .transaction
        .as_ref()
        .context("no transaction in executed_transaction")?
        .bcs
        .as_ref()
        .context("no bcs in transaction")?;

    let tx_data: TransactionData = bcs::from_bytes(
        tx_bcs
            .value
            .as_ref()
            .context("no value in transaction bcs")?,
    )
    .context("deserializing TransactionData from gRPC bcs")?;

    let tx_signatures: Vec<GenericSignature> = executed_tx
        .signatures
        .iter()
        .map(|sig| {
            let sig_bcs = sig.bcs.as_ref().context("no bcs in user signature")?;
            let sig_bytes = sig_bcs
                .value
                .as_ref()
                .context("no value in signature bcs")?;
            GenericSignature::from_bytes(sig_bytes)
                .or_else(|_| {
                    bcs::from_bytes::<GenericSignature>(sig_bytes).map_err(|e| {
                        fastcrypto::error::FastCryptoError::GeneralError(e.to_string())
                    })
                })
                .context("parsing GenericSignature from gRPC bcs")
        })
        .collect::<Result<Vec<_>, _>>()?;

    let sender_signed_data = SenderSignedData::new(tx_data, tx_signatures);
    Ok(bcs::to_bytes(&sender_signed_data).context("serializing SenderSignedData")?)
}

/// Convert a gRPC `BalanceChange` to the SDK `BalanceChange`.
fn convert_grpc_balance_change(
    change: &sui_rpc::proto::sui::rpc::v2::BalanceChange,
) -> Result<SuiBalanceChange, SuiClientError> {
    let address: SuiAddress = change
        .address
        .as_ref()
        .context("no address in balance change")?
        .parse()
        .context("parsing balance change address")?;
    let coin_type: TypeTag = change
        .coin_type
        .as_ref()
        .context("no coin_type in balance change")?
        .parse()
        .context("parsing balance change coin_type")?;
    let amount: i128 = change
        .amount
        .as_ref()
        .context("no amount in balance change")?
        .parse()
        .context("parsing balance change amount")?;
    Ok(SuiBalanceChange {
        owner: Owner::AddressOwner(address),
        coin_type,
        amount,
    })
}

/// Convert a gRPC `Event` to a `SuiEvent`.
fn grpc_event_to_sui_event(
    tx_digest: TransactionDigest,
    event_seq: usize,
    event: &sui_rpc::proto::sui::rpc::v2::Event,
) -> Result<SuiEvent, SuiClientError> {
    let package_id: ObjectID = event
        .package_id
        .as_ref()
        .context("no package_id in event")?
        .parse()
        .context("parsing event package_id")?;
    let transaction_module = move_core_types::identifier::Identifier::from_str(
        event.module.as_ref().context("no module in event")?,
    )
    .context("parsing event module")?;
    let sender: SuiAddress = event
        .sender
        .as_ref()
        .context("no sender in event")?
        .parse()
        .context("parsing event sender")?;
    let type_: StructTag = event
        .event_type
        .as_ref()
        .context("no event_type in event")?
        .parse()
        .context("parsing event type")?;
    let contents = event
        .contents
        .as_ref()
        .context("no contents in event")?
        .value
        .as_ref()
        .context("no value in event contents")?
        .to_vec();

    Ok(SuiEvent {
        id: EventID {
            tx_digest,
            event_seq: u64::try_from(event_seq).context("event_seq overflow")?,
        },
        package_id,
        transaction_module,
        sender,
        type_,
        parsed_json: serde_json::Value::Null,
        bcs: BcsEvent::new(contents),
        timestamp_ms: None,
    })
}

async fn batch_get_objects<T>(
    mut grpc_client: GrpcClient,
    object_ids: &[ObjectID],
    read_mask: FieldMask,
    mut extract: impl FnMut(Object) -> anyhow::Result<T>,
) -> Result<Vec<T>, SuiClientError> {
    let mut results: Vec<T> = Vec::with_capacity(object_ids.len());

    for chunk in object_ids.chunks(MAX_GET_OBJECTS_BATCH_SIZE) {
        let requests: Vec<_> = chunk
            .iter()
            .map(|object_id| GetObjectRequest::new(&address_from_object_id(*object_id)))
            .collect();

        let batch_get_objects = BatchGetObjectsRequest::default()
            .with_requests(requests)
            .with_read_mask(read_mask.clone());

        let response = grpc_client
            .ledger_client()
            .batch_get_objects(batch_get_objects)
            .await
            .context("grpc request error")?;

        append_batch_get_objects_response(&mut results, response, &mut extract)?;
    }
    Ok(results)
}

fn append_batch_get_objects_response<T>(
    batch_results: &mut Vec<T>,
    response: tonic::Response<BatchGetObjectsResponse>,
    mut extract: impl FnMut(Object) -> anyhow::Result<T>,
) -> Result<(), SuiClientError> {
    use get_object_result::Result as GetObjectResult;

    for get_object_result in response.into_inner().objects.into_iter() {
        match get_object_result
            .result
            .context("no result in get_object_result")?
        {
            GetObjectResult::Object(object) => batch_results.push(extract(object)?),
            GetObjectResult::Error(status) => {
                return Err(anyhow::anyhow!(
                    "error getting object: code {}, message {}, details {:?}",
                    status.code,
                    status.message,
                    status.details
                )
                .into());
            }
            _ => {
                return Err(
                    anyhow::anyhow!("encountered unknown get_object_result variant").into(),
                );
            }
        }
    }
    Ok(())
}
