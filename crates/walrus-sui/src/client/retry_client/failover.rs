// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A failover client wrapper for Sui (HTTP and gRPC) clients.
use std::{collections::BTreeSet, future::Future, iter::once, sync::Arc, time::Duration};

#[cfg(msim)]
use sui_macros::fail_point_if;
use tokio::sync::Mutex;

use super::{RetriableClientError, ToErrorType, retry_count_guard::RetryCountGuard};
use crate::client::{SuiClientError, SuiClientMetricSet};

/// A trait that defines the implementation of a thunk to build (or re-use) a client lazily.
pub trait LazyClientBuilder<C> {
    /// The maximum number of allowable retries (by way of failing over) for this client.
    const DEFAULT_MAX_TRIES: usize;
    /// Should lazily create a new client instance.
    fn lazy_build_client(&self) -> impl Future<Output = Result<Arc<C>, FailoverError>>;
    /// Should return the RPC URL of the client, if one exists.
    fn get_rpc_url(&self) -> Option<&str>;
}

/// The inner state of the FailoverWrapper.
struct FailoverState<ClientT> {
    client: Arc<ClientT>,
    rpc_url: Option<String>,
    current_index: usize,
}

/// An error that can occur when using the failover wrapper.
#[derive(Debug, thiserror::Error)]
pub enum FailoverError {
    /// Invalid configuration.
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Zero clients provided.
    #[error("Zero clients provided")]
    ZeroClientsProvided,
    /// Failed to get client.
    #[error("Failed to get client from url: {0}")]
    FailedToGetClient(String),
}

impl From<FailoverError> for SuiClientError {
    fn from(err: FailoverError) -> Self {
        SuiClientError::Internal(err.into())
    }
}

/// A trait that defines whether an error is eligible for failover.
pub trait EligibleForFailover {
    /// Whether the error is eligible for failover.
    fn is_eligible_for_failover(&self) -> bool;
}

impl EligibleForFailover for SuiClientError {
    fn is_eligible_for_failover(&self) -> bool {
        true
    }
}

impl EligibleForFailover for RetriableClientError {
    fn is_eligible_for_failover(&self) -> bool {
        // When checkpoint is not produced, we don't need to failover, but simply wait and retry.
        self.is_checkpoint_produced()
    }
}

/// A trait that defines the implementation of a function to create an injectable error. Note that
/// this is only used by the `msim` feature, and is not used in production code.
pub trait MakeRetriableError {
    /// Create an error of this type that would be deemed retriable by the RetriableRpcError trait.
    fn make_retriable_error() -> Self;
}

impl MakeRetriableError for SuiClientError {
    fn make_retriable_error() -> Self {
        SuiClientError::SuiSdkError(sui_sdk::error::Error::RpcError(
            jsonrpsee::core::ClientError::RequestTimeout,
        ))
    }
}

impl MakeRetriableError for RetriableClientError {
    fn make_retriable_error() -> Self {
        RetriableClientError::RetryableTimeoutError
    }
}

/// A wrapper that provides failover functionality for any inner type.
/// When an operation fails on the current inner instance, it will try the next one.
#[derive(Clone)]
pub struct FailoverWrapper<ClientT, BuilderT: LazyClientBuilder<ClientT> + std::fmt::Debug> {
    lazy_client_builders: Vec<BuilderT>,
    state: Arc<Mutex<Option<FailoverState<ClientT>>>>,
    max_tries: usize,
}

impl<ClientT, BuilderT: LazyClientBuilder<ClientT> + std::fmt::Debug> std::fmt::Debug
    for FailoverWrapper<ClientT, BuilderT>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailoverWrapper")
            .field("lazy_client_builders", &self.lazy_client_builders)
            .field("max_tries", &self.max_tries)
            .finish()
    }
}

impl<ClientT, BuilderT: LazyClientBuilder<ClientT> + std::fmt::Debug>
    FailoverWrapper<ClientT, BuilderT>
{
    const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

    /// Creates a new failover wrapper.
    pub async fn new(lazy_client_builders: Vec<BuilderT>) -> anyhow::Result<Self> {
        if lazy_client_builders.is_empty() {
            return Err(anyhow::anyhow!("No clients available"));
        }

        let max_tries = BuilderT::DEFAULT_MAX_TRIES.min(lazy_client_builders.len());

        // Precondition check (a bit redundant, but just for extra safety) to ensure we have more
        // clients than we allow failovers. Note that if this condition changes, `fetch_next_client`
        // will need to be updated to track a failover count separately.
        assert!(lazy_client_builders.len() >= max_tries);

        Ok(Self {
            lazy_client_builders,
            state: Arc::new(Mutex::new(None)),
            max_tries,
        })
    }

    /// Returns the current client.
    pub async fn get_current_client(&self) -> Result<Arc<ClientT>, FailoverError> {
        let mut state = self.state.lock().await;
        match state.as_ref() {
            Some(state) => Ok(state.client.clone()),
            None => {
                let client = self
                    .fetch_next_client_locked(&mut BTreeSet::new(), &mut state)
                    .await?;
                Ok(client)
            }
        }
    }

    /// Returns the current client's RPC URL.
    // This is a helper function is mainly used for logging.
    pub async fn get_current_rpc_url(&self) -> String {
        match self.state.lock().await.as_ref() {
            Some(state) => state
                .rpc_url
                .clone()
                .unwrap_or_else(|| "unknown_url".to_string()),
            None => "uninitialized_client".to_string(),
        }
    }

    fn client_count(&self) -> usize {
        self.lazy_client_builders.len()
    }

    /// Gets a client at the specified index (wrapped around if needed) and sets it as the current
    /// client.
    async fn fetch_next_client(
        &self,
        tried_client_indices: &mut BTreeSet<usize>,
    ) -> Result<Arc<ClientT>, FailoverError> {
        if self.client_count() == 1 {
            return Err(FailoverError::FailedToGetClient(
                "only one client available, try adding rpc urls".to_string(),
            ));
        }
        let mut state = self.state.lock().await;
        self.fetch_next_client_locked(tried_client_indices, &mut state)
            .await
    }

    /// Fetches the next client set in the FailoverWrapper. Excluding the clients that have already
    /// been tried in the `tried_client_indices` set.
    ///
    /// This function is locked on the `state` mutex, and will update the state if a new client is
    /// fetched.
    async fn fetch_next_client_locked(
        &self,
        tried_client_indices: &mut BTreeSet<usize>,
        state: &mut Option<FailoverState<ClientT>>,
    ) -> Result<Arc<ClientT>, FailoverError> {
        // Check if the state of the FailoverWrapper has already advanced to a client we haven't
        // tried yet. If so, go ahead and use that one. Note that this condition is subtle as it
        // mutates the `tried_client_indices`.
        if let Some(state) = state.as_mut() {
            if tried_client_indices.len() < self.max_tries
                && tried_client_indices.insert(state.current_index)
            {
                return Ok(state.client.clone());
            }
        };

        // Compute the next wrapped index.
        let mut next_index = if let Some(state) = state.as_ref() {
            (state.current_index + 1) % self.client_count()
        } else {
            0
        };

        // It may be the case that the LazyClientBuilder fails to connect to a client, but let's not
        // let that stop us from trying the next one.
        loop {
            // PLAN phase.
            if tried_client_indices.len() >= self.max_tries {
                return Err(FailoverError::FailedToGetClient(format!(
                    "max failovers {} exceeded",
                    self.max_tries
                )));
            }

            if !tried_client_indices.insert(next_index) {
                // We've already tried this client, so let's skip it.
                next_index = (next_index + 1) % self.client_count();
                continue;
            }

            // Load the next client.
            let client = match self.lazy_client_builders[next_index]
                .lazy_build_client()
                .await
            {
                Ok(client) => client,
                Err(error) => {
                    // Log the error and failover to the next client.
                    tracing::warn!(
                        "Failed to get client from url: {}, failover to next client",
                        error
                    );
                    next_index = (next_index + 1) % self.client_count();
                    continue;
                }
            };

            // COMMIT phase. NB: never fail during this phase.
            *state = Some(FailoverState {
                client: client.clone(),
                rpc_url: self.lazy_client_builders[next_index]
                    .get_rpc_url()
                    .map(|s| s.to_string()),
                current_index: next_index,
            });

            // We're done, return the client.
            return Ok(client);
        }
    }

    /// Executes an operation on the current inner instance, falling back to the next one
    /// if it fails.
    pub async fn with_failover<E, F, Fut, R>(
        &self,
        mut operation: F,
        metrics: Option<Arc<SuiClientMetricSet>>,
        method: &'static str,
    ) -> Result<R, E>
    where
        F: FnMut(Arc<ClientT>, &'static str) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: MakeRetriableError,
        E: ToErrorType + std::fmt::Debug + From<FailoverError>,
        E: EligibleForFailover,
    {
        let mut retry_guard = metrics
            .as_ref()
            .map(|m| RetryCountGuard::new(m.clone(), format!("{method}_with_failover").as_str()));
        let (mut client, mut tried_client_indices) = {
            let mut state = self.state.lock().await;

            if let Some(state) = state.as_ref() {
                let tried_client_indices = once(state.current_index).collect::<BTreeSet<_>>();
                (state.client.clone(), tried_client_indices)
            } else {
                // First time using a new FailoverWrapper, fetch the first client.
                let mut tried_client_indices = BTreeSet::new();
                let client = self
                    .fetch_next_client_locked(&mut tried_client_indices, &mut state)
                    .await?;
                (client, tried_client_indices)
            }
        };
        loop {
            let result = {
                #[cfg(msim)]
                {
                    let mut inject_error = false;
                    fail_point_if!("fallback_client_inject_error", || {
                        // Only inject an error if we have multiple clients and we have one valid
                        // client in the tank.
                        inject_error =
                            self.client_count() < self.max_tries && rand::random::<bool>();
                    });
                    if inject_error {
                        tracing::warn!(
                            "Injecting an RPC error during failover loop [
                                method={method}, \
                                client_count={client_count}, \
                                try_count={try_count}, \
                                max_tries={max_tries}\
                            ]",
                            client_count = self.client_count(),
                            try_count = tried_client_indices.len(),
                            max_tries = self.max_tries,
                        );
                        Err(E::make_retriable_error())
                    } else {
                        operation(client, method).await
                    }
                }
                #[cfg(not(msim))]
                {
                    operation(client, method).await
                }
            };

            if let Some(retry_guard) = retry_guard.as_mut() {
                retry_guard.record_result(result.as_ref());
            }

            match result {
                Ok(result) => {
                    return Ok(result);
                }
                Err(error) => {
                    // If the error is not eligible for failover, return it immediately.
                    if !error.is_eligible_for_failover() {
                        tracing::debug!(
                            ?error,
                            "error is not eligible for failover, returning the last failed error",
                        );
                        return Err(error);
                    }

                    let failed_rpc_url = self.get_current_rpc_url().await;
                    match self.fetch_next_client(&mut tried_client_indices).await {
                        Ok(next_client) => {
                            client = next_client;
                            let next_rpc_url = self.get_current_rpc_url().await;
                            tracing::event!(
                                // A custom target for filtering.
                                target: "walrus_sui::client::retry_client::failover",
                                tracing::Level::DEBUG,
                                last_error = ?error,
                                failed_rpc_url,
                                next_rpc_url,
                                "failed to execute operation on client, retrying with next client"
                            );
                        }
                        Err(fetch_client_error) => {
                            tracing::event!(
                                // A custom target for filtering.
                                target: "walrus_sui::client::retry_client::failover",
                                tracing::Level::DEBUG,
                                last_error = ?error,
                                failed_rpc_url,
                                ?fetch_client_error,
                                "failed to fetch_next_client, failing rpc"
                            );
                            return Err(error);
                        }
                    }
                    // Sleep for a short duration to avoid aggressive retries.
                    tokio::time::sleep(Self::DEFAULT_RETRY_DELAY).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicUsize};

    use super::{FailoverError, FailoverWrapper, LazyClientBuilder, MakeRetriableError};
    use crate::client::{
        SuiClientError,
        retry_client::{CheckpointRpcError, RetriableClientError, RetriableRpcError},
    };

    #[test]
    fn test_retriable_errors() {
        // REVIEW: make sure this test fails! then remove this line
        assert!(SuiClientError::make_retriable_error().is_retriable_rpc_error());
        assert!(RetriableClientError::make_retriable_error().is_retriable_rpc_error());
    }

    // Mock client that counts number of calls and returns configurable results.
    #[derive(Debug)]
    struct MockClient {
        call_count: Arc<AtomicUsize>,
        should_fail: bool,
    }

    impl LazyClientBuilder<MockClient> for Arc<MockClient> {
        const DEFAULT_MAX_TRIES: usize = 2;
        async fn lazy_build_client(&self) -> Result<Arc<MockClient>, FailoverError> {
            Ok(self.clone())
        }
        fn get_rpc_url(&self) -> Option<&str> {
            Some("mock_rpc_url")
        }
    }

    impl MockClient {
        fn new(should_fail: bool) -> Self {
            Self {
                call_count: Arc::new(AtomicUsize::new(0)),
                should_fail,
            }
        }

        async fn operation(&self) -> Result<String, RetriableClientError> {
            self.call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.should_fail {
                Err(RetriableClientError::RpcError(CheckpointRpcError {
                    status: tonic::Status::internal("mock error"),
                    checkpoint_seq_num: None,
                }))
            } else {
                Ok("success".to_string())
            }
        }
    }

    #[tokio::test]
    async fn test_failover_wrapper() {
        // Create mock clients - first fails, second succeeds.
        let failing_client = Arc::new(MockClient::new(true));
        let succeeding_client = Arc::new(MockClient::new(false));

        let failing_calls = failing_client.call_count.clone();
        let succeeding_calls = succeeding_client.call_count.clone();

        let clients = vec![failing_client, succeeding_client];

        let failover_wrapper = FailoverWrapper::new(clients)
            .await
            .expect("failed to create wrapper");

        // Execute operation that should failover from first to second client.
        let result = failover_wrapper
            .with_failover(
                |client, _method| {
                    Box::pin(async move {
                        let client = client.as_ref();
                        client.operation().await
                    })
                },
                None,
                "operation",
            )
            .await;
        assert!(matches!(result, Ok(ref s) if s == "success"));
        assert_eq!(failing_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            succeeding_calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            !failover_wrapper
                .get_current_client()
                .await
                .expect("client must have been created")
                .should_fail,
        );
    }

    #[tokio::test]
    async fn test_failover_wrapper_all_fail() {
        // Create mock clients - both fail.
        let clients = vec![
            Arc::new(MockClient::new(true)),
            Arc::new(MockClient::new(true)),
        ];

        let failover_wrapper = FailoverWrapper::new(clients).await.unwrap();

        // Execute operation that should try both clients and fail.
        let result = failover_wrapper
            .with_failover(
                |client, _method| Box::pin(async move { client.operation().await }),
                None,
                "operation",
            )
            .await;

        // Verify result is error.
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RetriableClientError::RpcError(_)
        ));
    }
}
