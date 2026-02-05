// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to access Sui via both JSON RPC and gRPC. This module is intended to facilitate a
//! migration from Sui JSON RPC to gRPC by gradually migrating callsites away from the JSON RPC
//! Client [`SuiClient`].

use std::time::Duration;

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
        GetBalanceRequest,
        GetObjectRequest,
        ListOwnedObjectsRequest,
        ListOwnedObjectsResponse,
        Object,
        get_object_result,
        state_service_client::StateServiceClient,
    },
};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_types::{
    TypeTag,
    base_types::{ObjectID, ObjectRef, SuiAddress},
    digests::TransactionDigest,
};
use tonic::service::interceptor::InterceptedService;
use walrus_core::ensure;

use crate::{client::SuiClientError, coin::Coin, contracts::TypeOriginMap};

/// The maximum number of objects to request in a single "batch" gRPC call.
pub const MAX_GET_OBJECTS_BATCH_SIZE: usize = 100;
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

    pub(crate) async fn fetch_batch_of_coins(
        &self,
        owner: SuiAddress,
        coin_type: &str,
        page_token: Option<Bytes>,
    ) -> Result<CoinBatch, SuiClientError> {
        tracing::debug!(
            ?owner,
            ?coin_type,
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
        let response = list_owned_coin_objects(owner, coin_type, page_token, state_client).await;

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
                                    [owner={:?}, coin_type={:?}]: {error:?}",
                                owner,
                                coin_type,
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
                "error listing owned objects for owner [owner={:?}, coin_type={:?}]: {error:?}",
                owner,
                coin_type,
            )
            .into()),
        }
    }

    /// Get the total balance for a given owner and coin type. This routine avoids enumerating all
    /// the coin objects on the client side by using the gRPC `GetBalance` method.
    pub async fn get_total_balance(
        &self,
        owner: SuiAddress,
        coin_type: Option<String>,
    ) -> Result<u64, SuiClientError> {
        let coin_type_for_request = coin_type.unwrap_or_else(|| "0x2::sui::SUI".to_string());
        let get_balance_request = GetBalanceRequest::default()
            .with_owner(owner.to_string())
            .with_coin_type(coin_type_for_request);
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
}

async fn list_owned_coin_objects(
    owner: SuiAddress,
    coin_type: &str,
    next_page_token: Option<Bytes>,
    mut state_client: StateServiceClient<
        InterceptedService<&mut tonic::transport::Channel, &sui_rpc::client::HeadersInterceptor>,
    >,
) -> Result<tonic::Response<ListOwnedObjectsResponse>, tonic::Status> {
    let mut request = ListOwnedObjectsRequest::default()
        .with_owner(owner.to_string())
        .with_read_mask(FieldMask::from_paths([
            Object::path_builder().object_type(),
            Object::path_builder().object_id(),
            Object::path_builder().version(),
            Object::path_builder().digest(),
            Object::path_builder().balance(),
            Object::path_builder().previous_transaction(),
        ]))
        .with_page_size(MAX_SELECT_COINS_BATCH_SIZE)
        .with_object_type(format!("0x2::coin::Coin<{coin_type}>"));
    if let Some(next_page_token) = next_page_token {
        request = request.with_page_token(next_page_token);
    }
    state_client.list_owned_objects(request).await
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
