// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A failover client wrapper for Sui (HTTP and gRPC) clients.
use std::{future::Future, sync::Arc, time::Duration};

#[cfg(msim)]
use sui_macros::fail_point_if;
use tokio::sync::Mutex;

#[cfg(msim)]
use super::should_inject_error;
use super::{retry_count_guard::RetryCountGuard, RetriableClientError, ToErrorType};
use crate::client::{SuiClientError, SuiClientMetricSet};

/// A trait that defines the implementation of a thunk to build (or re-use) a client lazily.
pub trait LazyClientBuilder<C> {
    /// Should lazily create a new client instance.
    fn lazy_build_client(&self) -> impl Future<Output = Result<Arc<C>, FailoverError>>;
    /// Should return the RPC URL of the client, if one exists.
    fn get_rpc_url(&self) -> Option<&str>;
}

/// The inner state of the FailoverWrapper.
struct FailoverState<ClientT> {
    client: Arc<ClientT>,
    rpc_url: Option<String>,
    index: usize,
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
    state: Arc<Mutex<FailoverState<ClientT>>>,
    max_failovers: usize,
}

impl<ClientT, BuilderT: LazyClientBuilder<ClientT> + std::fmt::Debug> std::fmt::Debug
    for FailoverWrapper<ClientT, BuilderT>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailoverWrapper")
            .field("lazy_client_builders", &self.lazy_client_builders)
            .field("max_failovers", &self.max_failovers)
            .finish()
    }
}

impl<ClientT, BuilderT: LazyClientBuilder<ClientT> + std::fmt::Debug>
    FailoverWrapper<ClientT, BuilderT>
{
    /// The default maximum number of retries.
    const DEFAULT_MAX_RETRIES: usize = 3;
    const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

    /// Creates a new failover wrapper.
    pub async fn new(lazy_client_builders: Vec<BuilderT>) -> anyhow::Result<Self> {
        if lazy_client_builders.is_empty() {
            return Err(anyhow::anyhow!("No clients available"));
        }
        Ok(Self {
            max_failovers: Self::DEFAULT_MAX_RETRIES.min(lazy_client_builders.len()),
            state: Arc::new(Mutex::new(FailoverState {
                client: lazy_client_builders[0].lazy_build_client().await?,
                rpc_url: lazy_client_builders[0].get_rpc_url().map(|s| s.to_string()),
                index: 0,
            })),
            lazy_client_builders,
        })
    }

    /// Returns the name of the current client.
    pub async fn get_current_client(&self) -> Arc<ClientT> {
        self.state.lock().await.client.clone()
    }

    /// Returns the name of the current client.
    pub async fn get_current_rpc_url(&self) -> String {
        self.state
            .lock()
            .await
            .rpc_url
            .clone()
            .unwrap_or("unknown_url".to_string())
    }

    fn client_count(&self) -> usize {
        self.lazy_client_builders.len()
    }

    /// Gets a client at the specified index (wrapped around if needed) and sets it as the current
    /// client.
    async fn fetch_next_client(
        &self,
        failover_count: &mut usize,
    ) -> Result<Arc<ClientT>, FailoverError> {
        if self.client_count() == 1 {
            return Err(FailoverError::FailedToGetClient(
                "only one client available, try adding rpc urls".to_string(),
            ));
        }

        let mut state = self.state.lock().await;

        // It may be the case that the LazyClientBuilder fails to connect to a client, but let's not
        // let that stop us from trying the next one.
        loop {
            // Increment the retry count.
            *failover_count += 1;

            if *failover_count > self.max_failovers {
                return Err(FailoverError::FailedToGetClient(
                    "max retries exceeded".to_string(),
                ));
            }

            // PLAN phase.

            // Compute the next wrapped index.
            let next_index = (state.index + 1) % self.client_count();

            // Load the next client.
            let client = match self.lazy_client_builders[next_index]
                .lazy_build_client()
                .await
            {
                Ok(client) => client,
                Err(error) => {
                    // Log the error and failover to the next client.
                    tracing::warn!("Failed to get client from url: {}", error);
                    continue;
                }
            };

            // COMMIT phase. NB: never fail during this phase.
            state.index = next_index;
            state.client = client.clone();
            state.rpc_url = self.lazy_client_builders[next_index]
                .get_rpc_url()
                .map(|s| s.to_string());

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
    {
        let mut retry_guard = metrics
            .as_ref()
            .map(|m| RetryCountGuard::new(m.clone(), format!("{method}_with_failover").as_str()));
        let mut client = self.get_current_client().await;
        let mut retry_count = 0;
        loop {
            let result = {
                #[cfg(msim)]
                {
                    let mut inject_error = false;
                    fail_point_if!("fallback_client_inject_error", || {
                        inject_error = should_inject_error(retry_count);
                    });
                    if inject_error {
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
                    let failed_rpc_url = self.get_current_rpc_url().await;
                    match self.fetch_next_client(&mut retry_count).await {
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
                                "Failed to execute operation on client, retrying with next client"
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
                                "Failed to execute operation on client, retrying with next client"
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
    use std::sync::{atomic::AtomicUsize, Arc};

    use super::{FailoverError, FailoverWrapper, LazyClientBuilder, MakeRetriableError};
    use crate::client::{
        retry_client::{CheckpointRpcError, RetriableClientError, RetriableRpcError},
        SuiClientError,
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
        assert!(!failover_wrapper.get_current_client().await.should_fail,);
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
