// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A failover client wrapper for Sui (HTTP and gRPC) clients.
use std::{
    future::Future,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

#[cfg(msim)]
use sui_macros::fail_point_if;

#[cfg(msim)]
use super::should_inject_error;
use super::{
    retriable_rpc_error::RetriableRpcError,
    retry_count_guard::RetryCountGuard,
    ToErrorType,
};
use crate::client::SuiClientMetricSet;

/// A wrapper for a client that provides failover functionality, the wrapper struct includes the
/// actual client, as well as its name for logging purposes.
#[derive(Clone, Debug)]
pub struct FailoverClient<T> {
    /// The name of the client, used for logging.
    pub name: String,
    /// The actual client instance.
    pub client: Arc<T>,
}

/// A wrapper that provides failover functionality for any inner type.
/// When an operation fails on the current inner instance, it will try the next one.
#[derive(Clone, Debug)]
pub struct FailoverWrapper<T> {
    pub(crate) failover_clients: Arc<Vec<FailoverClient<T>>>,
    pub(crate) current_index: Arc<AtomicUsize>,
    pub(crate) max_retries: usize,
}

impl<T> FailoverWrapper<T> {
    /// The default maximum number of retries.
    pub(crate) const DEFAULT_MAX_RETRIES: usize = 3;
    pub(crate) const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(100);

    /// Creates a new failover wrapper.
    pub fn new(failover_clients: Vec<FailoverClient<T>>) -> anyhow::Result<Self> {
        if failover_clients.is_empty() {
            return Err(anyhow::anyhow!("No clients available"));
        }
        Ok(Self {
            max_retries: Self::DEFAULT_MAX_RETRIES.min(failover_clients.len()),
            failover_clients: Arc::new(failover_clients),
            current_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Returns the name of the current client.
    pub fn get_current_client_name(&self) -> &str {
        &self.failover_clients[self
            .current_index
            .load(std::sync::atomic::Ordering::Relaxed)
            % self.client_count()]
        .name
    }

    pub(crate) fn client_count(&self) -> usize {
        self.failover_clients.len()
    }

    /// Gets a client at the specified index (wrapped around if needed).
    pub(crate) async fn get_client(&self, index: usize) -> Arc<T> {
        let wrapped_index = index % self.client_count();
        self.failover_clients[wrapped_index].client.clone()
    }

    /// Gets the name of the client at the specified index.
    pub(crate) fn get_name(&self, index: usize) -> &str {
        &self.failover_clients[index % self.client_count()].name
    }

    /// Executes an operation on the current inner instance, falling back to the next one
    /// if it fails.
    pub(crate) async fn with_failover<E, F, Fut, R>(
        &self,
        operation: F,
        metrics: Option<Arc<SuiClientMetricSet>>,
        method: &str,
    ) -> Result<R, E>
    where
        F: for<'a> Fn(Arc<T>) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: RetriableRpcError + ToErrorType + From<tonic::Status>,
    {
        let mut last_error = None;
        let start_index = self
            .current_index
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut current_index = start_index;

        let mut retry_guard = metrics
            .as_ref()
            .map(|m| RetryCountGuard::new(m.clone(), format!("{method}_with_failover").as_str()));

        for i in 0..self.max_retries {
            let instance = self.get_client(current_index).await;

            let result = {
                #[cfg(msim)]
                {
                    let mut inject_error = false;
                    fail_point_if!("fallback_client_inject_error", || {
                        inject_error = should_inject_error(current_index);
                    });
                    if inject_error {
                        Err(E::from(tonic::Status::internal(
                            "injected error for testing",
                        )))
                    } else {
                        operation(instance).await
                    }
                }
                #[cfg(not(msim))]
                {
                    operation(instance).await
                }
            };

            if let Some(retry_guard) = retry_guard.as_mut() {
                retry_guard.record_result(result.as_ref());
            }

            match result {
                Ok(result) => {
                    self.current_index
                        .store(current_index, std::sync::atomic::Ordering::Relaxed);
                    return Ok(result);
                }
                Err(error) => {
                    last_error = Some(error);
                    if i < self.max_retries - 1 {
                        tracing::event!(
                            // A custom target for filtering.
                            target: "walrus_sui::client::retry_client::failover",
                            tracing::Level::DEBUG,
                            ?last_error,
                            current_client = self.get_name(current_index),
                            next_client = self.get_name(current_index + 1),
                            "Failed to execute operation on client, retrying with next client"
                        );
                        // Sleep for a short duration to avoid aggressive retries.
                        tokio::time::sleep(Self::DEFAULT_RETRY_DELAY).await;
                        current_index += 1;
                    }
                }
            }
        }

        Err(last_error.expect("No clients available or all operations failed"))
    }
}
