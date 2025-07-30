// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Module for the RPC client.

use std::sync::Arc;

use sui_rpc_api::Client as RpcClient;

use crate::client::rpc_config::{RpcAuthConfig, RpcEndpointAuth};

/// Creates a new RPC client with authentication if configured
fn create_client(rpc_url: &str, auth_config: &RpcAuthConfig) -> anyhow::Result<Arc<RpcClient>> {
    let mut client = RpcClient::new(rpc_url.to_string())?;

    let auth_config = auth_config.get_auth_for_url(rpc_url);
    if let Some(auth_config) = auth_config {
        let auth_interceptor =
            if let RpcEndpointAuth::BasicAuth { username, password } = auth_config {
                tracing::debug!("Configuring basic authentication for RPC client");
                sui_rpc_api::client::AuthInterceptor::basic(username, Some(password))
            } else if let RpcEndpointAuth::BearerToken(token) = auth_config {
                tracing::debug!("Configuring bearer token authentication for RPC client");
                sui_rpc_api::client::AuthInterceptor::bearer(token)
            } else {
                return Ok(Arc::new(client));
            };
        client = client.with_auth(auth_interceptor);
    }

    Ok(Arc::new(client))
}

/// Creates a new RPC client with authentication if configured from environment variables
pub fn create_sui_rpc_client(rpc_url: &str) -> anyhow::Result<Arc<RpcClient>> {
    let auth_config = RpcAuthConfig::from_environment();
    create_client(rpc_url, &auth_config)
}
