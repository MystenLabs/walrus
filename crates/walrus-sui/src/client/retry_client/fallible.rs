// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! An RPC client that power-cycles the client if it has failed too many times.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use atomic_time::AtomicInstant;
use sui_rpc_api::Client as RpcClient;
use tokio::sync::RwLock;

use crate::client::rpc_client;

/// A wrapper around a `RpcClient` that recreates the client if it has failed too many times.
/// This is useful for clients that sometimes get stuck and never recover.
pub struct FallibleRpcClient {
    /// The RPC URL.
    rpc_url: String,
    /// The underlying RPC client.
    client: Arc<RwLock<Arc<RpcClient>>>,
    /// The instant of the last successful RPC call.
    last_success: AtomicInstant,
    /// The number of failures since the last successful RPC call.
    num_failures: AtomicUsize,
}

impl FallibleRpcClient {
    /// The time window during which failures are counted.
    const FAILURE_WINDOW: Duration = Duration::from_secs(600);
    /// The maximum number of failures allowed.
    const MAX_FAILURES: usize = 100;

    /// Creates a new `FallibleRpcClient`.
    pub fn new(rpc_url: String) -> anyhow::Result<Self> {
        let client = rpc_client::create_sui_rpc_client(&rpc_url)?;
        Ok(Self {
            rpc_url,
            client: Arc::new(RwLock::new(client)),
            last_success: AtomicInstant::new(Instant::now()),
            num_failures: AtomicUsize::new(0),
        })
    }

    /// Returns the underlying RPC client.
    pub async fn inner(&self) -> Arc<RpcClient> {
        self.client.read().await.clone()
    }

    /// Returns the RPC URL.
    pub fn get_rpc_url(&self) -> &str {
        &self.rpc_url
    }

    /// Calls the underlying RPC client with the given function.
    pub async fn call<F, Fut, T>(&self, f: F, timeout: Duration) -> Result<T, tonic::Status>
    where
        F: FnOnce(Arc<RpcClient>) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let client = self.client.read().await;
        let result = tokio::time::timeout(timeout, f(client.clone())).await;

        let result = match result {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(status)) => Err(status),
            Err(_) => Err(tonic::Status::deadline_exceeded("request timed out")),
        };

        let Ok(inner_result) = result else {
            self.num_failures.fetch_add(1, Ordering::Relaxed);
            let last_success = self.last_success.load(Ordering::Relaxed);
            // The two conditions together mean that the client has been failing for more than 10
            // minutes and has had more than 100 failures. Given we request checkpoint data at 4.2
            // checkpoints per second, this means that we would have requested ~2400 checkpoints in
            // the last 10 minutes. So this is a good proxy for the client being stuck in a failed
            // state.
            if last_success.elapsed() > Self::FAILURE_WINDOW
                && self.num_failures.load(Ordering::Relaxed) > Self::MAX_FAILURES
            {
                tracing::info!("rpc client is stuck in a failed state, recreating client");
                drop(client);
                let mut client = self.client.write().await;
                *client = rpc_client::create_sui_rpc_client(&self.rpc_url).map_err(|e| {
                    tracing::error!(error = ?e, "failed to recreate rpc client");
                    tonic::Status::internal(e.to_string())
                })?;
            }
            return result;
        };

        self.last_success.store(Instant::now(), Ordering::Relaxed);
        self.num_failures.store(0, Ordering::Relaxed);
        Ok(inner_result)
    }
}

impl std::fmt::Debug for FallibleRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleRpcClient")
            .field("rpc_url", &self.rpc_url)
            .field("last_success", &self.last_success.load(Ordering::Relaxed))
            .field("num_failures", &self.num_failures.load(Ordering::Relaxed))
            .finish()
    }
}
