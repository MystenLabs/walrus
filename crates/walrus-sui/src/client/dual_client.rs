// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to access Sui via both JSON RPC and gRPC. This module is intended to facilitate a
//! migration from Sui JSON RPC to gRPC by gradually migrating callsites away from the JSON RPC
//! Client [`SuiClient`].

use std::time::Duration;

use anyhow::Context;
use move_core_types::{account_address::AccountAddress, language_storage::StructTag};
use sui_rpc::{
    Client as GrpcClient,
    field::{FieldMask, FieldMaskUtil},
    proto::sui::rpc::v2::{Bcs, GetObjectRequest, Object},
};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_types::base_types::ObjectID;

use crate::{client::SuiClientError, contracts::TypeOriginMap};

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
        let response = grpc_client
            .ledger_client()
            .get_object(request)
            .await
            .context("grpc request error")?;
        Ok(response
            .into_inner()
            .object
            .context("no object in get_object_response")?
            .bcs
            .context("no bcs in object")?)
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
        let response = grpc_client
            .ledger_client()
            .get_object(request)
            .await
            .context("grpc request error")?;
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
            .context("failed to deserialize object from BCS")?)
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
        let response = grpc_client
            .ledger_client()
            .get_object(request)
            .await
            .context("grpc request error")?;

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
}
