// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Client to access Sui via both JSON RPC and gRPC. This module is intended to facilitate a
//! migration from Sui JSON RPC to gRPC by gradually migrating callsites away from the JSON RPC
//! Client [`SuiClient`].

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use sui_rpc::{
    Client as GrpcClient,
    field::{FieldMask, FieldMaskUtil as _},
    proto::sui::rpc::v2::{Bcs, GetObjectRequest, Object},
};
use sui_sdk::{SuiClient, SuiClientBuilder};
use sui_types::base_types::ObjectID;
use tokio::sync::Mutex;

use crate::client::SuiClientError;

/// A client that combines the Sui SDK client and a gRPC client in order to facilitate a migration
/// from Sui JSON RPC to gRPC by gradually migrating callsites away from [`SuiClient`].
#[derive(Clone)]
pub struct DualClient {
    /// The Sui SDK client for JSON RPC calls. This will eventually be removed.
    pub sui_client: SuiClient,
    grpc_client: Arc<Mutex<GrpcClient>>,
}

impl std::fmt::Debug for DualClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DualClient").finish()
    }
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
        let sui_client = client_builder.build(rpc_url).await?;
        // REVIEW(wbbradley): How do we set request timeout in SuiClient?
        // REVIEW(wbbradley): Do we need to set headers or max decoding message size?
        let grpc_client = GrpcClient::new(rpc_url).context("unable to create grpc client")?;
        Ok(Self {
            sui_client,
            grpc_client: Arc::new(Mutex::new(grpc_client)),
        })
    }

    /// Get the BCS representation of an object from the Sui network.
    pub async fn get_object_bcs(&self, object_id: ObjectID) -> Result<Bcs, SuiClientError> {
        let request = GetObjectRequest::new(&sui_sdk_types::Address::from(
            <[u8; 32]>::try_from(object_id.as_slice()).context("invalid object_id: {e}")?,
        ))
        .with_read_mask(FieldMask::from_paths([Object::path_builder()
            .bcs()
            .finish()]));
        let response = self
            .grpc_client
            .lock()
            .await
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

    /// Get the full object from the Sui network.
    pub async fn get_object(
        &self,
        object_id: ObjectID,
    ) -> Result<sui_types::object::Object, SuiClientError> {
        Ok(self
            .get_object_bcs(object_id)
            .await?
            .deserialize()
            .context("Failed to deserialize object from BCS")?)
    }
}
