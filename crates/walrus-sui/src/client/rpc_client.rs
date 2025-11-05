// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Module for the RPC client.

use sui_rpc_api::Client as RpcClient;

use crate::client::rpc_config::{RpcAuthConfig, RpcEndpointAuth};

/// Creates a new RPC client with authentication if configured
fn create_client(rpc_url: &str, auth_config: &RpcAuthConfig) -> anyhow::Result<RpcClient> {
    let mut client = RpcClient::new(rpc_url.to_string())?;

    if let Some(auth_config) = auth_config.get_auth_for_url(rpc_url) {
        let mut headers_interceptor = sui_rpc_api::client::HeadersInterceptor::new();
        match auth_config {
            RpcEndpointAuth::BasicAuth { username, password } => {
                tracing::debug!("configuring basic authentication for RPC client");
                headers_interceptor.basic_auth(username, Some(password));
            }
            RpcEndpointAuth::BearerToken(token) => {
                tracing::debug!("configuring bearer token authentication for RPC client");
                headers_interceptor.bearer_auth(token);
            }
            RpcEndpointAuth::None => {
                tracing::debug!("no authentication configured for RPC client");
                return Ok(client);
            }
        }
        client = client.with_headers(headers_interceptor);
    }

    Ok(client)
}

/// Creates a new RPC client with authentication if configured from environment variables
pub fn create_sui_rpc_client(rpc_url: &str) -> anyhow::Result<RpcClient> {
    let auth_config = RpcAuthConfig::from_environment();
    create_client(rpc_url, &auth_config)
}
