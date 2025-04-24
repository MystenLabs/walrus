// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`sui_sdk::SuiClient`] to introduce retries.

use std::{future::Future, sync::Arc, time::Instant};

#[cfg(msim)]
use rand::Rng;
use retriable_rpc_client::FallbackError;
#[cfg(msim)]
use sui_macros::fail_point_if;
#[cfg(msim)]
use sui_types::transaction::TransactionDataAPI;
use walrus_utils::backoff::BackoffStrategy;

pub use self::{
    failover::{FailoverClient, FailoverWrapper},
    fallible::FallibleRpcClient,
    retriable_rpc_client::{CheckpointRpcError, RetriableClientError, RetriableRpcClient},
    retriable_rpc_error::RetriableRpcError,
    retriable_sui_client::RetriableSuiClient,
};
use super::{SuiClientError, SuiClientResult};
use crate::client::SuiClientMetricSet;

pub mod failover;
pub mod fallible;
pub mod retriable_rpc_client;
pub mod retriable_rpc_error;
pub mod retriable_sui_client;
// pub mod mock_retriable_sui_client;
mod retry_count_guard;

/// The gas overhead to add to the gas budget to ensure that the transaction will succeed.
/// Set based on `GAS_SAFE_OVERHEAD` in the sui CLI. Used for gas budget estimation.
const GAS_SAFE_OVERHEAD: u64 = 1000;

/// The maximum number of objects to get in a single RPC call.
pub(crate) const MULTI_GET_OBJ_LIMIT: usize = 50;

/// Trait to convert an error to a string.
pub trait ToErrorType {
    /// Returns the error type as a string.
    fn to_error_type(&self) -> String;
}

/// Retries the given function while it returns retriable errors.[
async fn retry_rpc_errors<S, F, T, E, Fut>(
    mut strategy: S,
    mut func: F,
    metrics: Option<Arc<SuiClientMetricSet>>,
    method: &str,
) -> Result<T, E>
where
    S: BackoffStrategy,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetriableRpcError + ToErrorType,
{
    let mut retry_guard = metrics
        .as_ref()
        .map(|m| retry_count_guard::RetryCountGuard::new(m.clone(), method));

    loop {
        let start = Instant::now();
        let value = func().await;

        if let Some(metrics) = &metrics {
            metrics.record_rpc_call(
                method,
                &match value.as_ref() {
                    Ok(_) => "success".to_string(),
                    Err(e) => e.to_error_type(),
                },
                start.elapsed(),
            );
        }

        if let Some(retry_guard) = &mut retry_guard {
            retry_guard.record_result(value.as_ref());
        }

        match value {
            Ok(value) => return Ok(value),
            Err(error) if error.is_retriable_rpc_error() => {
                if let Some(delay) = strategy.next_delay() {
                    tracing::debug!(
                        ?delay,
                        ?error,
                        "attempt failed with retriable RPC error, waiting before retrying"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    tracing::debug!(
                        "last attempt failed with retriable RPC error, returning last failure value"
                    );
                    return Err(error);
                }
            }
            Err(error) => {
                tracing::debug!("non-retriable error, returning last failure value");
                return Err(error);
            }
        }
    }
}

/// Injects a simulated error for testing retry behavior executing sui transactions.
/// We use stake_with_pool as an example here to incorporate with the test logic in
/// `test_ptb_executor_retriable_error` in `test_client.rs`.
#[cfg(msim)]
fn maybe_return_injected_error_in_stake_pool_transaction(
    transaction: &sui_types::transaction::Transaction,
) -> anyhow::Result<()> {
    // Check if this transaction contains a stake_with_pool operation

    use rand::thread_rng;
    let is_stake_pool_tx =
        transaction
            .transaction_data()
            .move_calls()
            .iter()
            .any(|(_, _, function_name)| {
                *function_name == crate::contracts::staking::stake_with_pool.name
            });

    // Early return if this isn't a stake pool transaction
    if !is_stake_pool_tx {
        return Ok(());
    }

    // Check if we should inject an error via the fail point
    let mut should_inject_error = false;
    fail_point_if!("ptb_executor_stake_pool_retriable_error", || {
        should_inject_error = true;
    });

    if should_inject_error {
        tracing::warn!("injecting a retriable RPC error for stake pool transaction");

        let retriable_error = if thread_rng().gen_bool(0.5) {
            // Simulate a retriable RPC error (502 Bad Gateway).
            jsonrpsee::core::ClientError::Transport(
                "server returned an error status code: 502".into(),
            )
        } else {
            // Simulate a request timeout error.
            jsonrpsee::core::ClientError::RequestTimeout
        };

        Err(sui_sdk::error::Error::RpcError(retriable_error))?;
    }

    Ok(())
}

impl ToErrorType for anyhow::Error {
    fn to_error_type(&self) -> String {
        self.downcast_ref::<sui_sdk::error::Error>()
            .map(|error| error.to_error_type())
            .unwrap_or_else(|| "other".to_string())
    }
}

impl ToErrorType for sui_sdk::error::Error {
    fn to_error_type(&self) -> String {
        match self {
            Self::RpcError(e) => {
                let err_str = e.to_string();
                if err_str.contains("Call") {
                    "rpc_call"
                } else if err_str.contains("Transport") {
                    "rpc_transport"
                } else if err_str.contains("RequestTimeout") {
                    "rpc_timeout"
                } else {
                    "rpc_other"
                }
                .to_string()
            }
            Self::JsonRpcError(_) => "json_rpc".to_string(),
            Self::BcsSerialisationError(_) => "bcs_ser".to_string(),
            Self::JsonSerializationError(_) => "json_ser".to_string(),
            Self::UserInputError(_) => "user_input".to_string(),
            Self::Subscription(_) => "subscription".to_string(),
            Self::FailToConfirmTransactionStatus(_, _) => "tx_confirm".to_string(),
            Self::DataError(_) => "data_error".to_string(),
            Self::ServerVersionMismatch { .. } => "version_mismatch".to_string(),
            Self::InsufficientFund { .. } => "insufficient_fund".to_string(),
            Self::InvalidSignature => "invalid_sig".to_string(),
        }
    }
}

impl ToErrorType for SuiClientError {
    fn to_error_type(&self) -> String {
        match self {
            Self::SuiSdkError(error) => error.to_error_type(),
            _ => "sui_client_error_other".to_string(),
        }
    }
}

impl ToErrorType for tonic::Status {
    fn to_error_type(&self) -> String {
        "other".to_string()
    }
}

impl ToErrorType for RetriableClientError {
    fn to_error_type(&self) -> String {
        match self {
            Self::RpcError(_) => "checkpoint_rpc_error".to_string(),
            Self::FallbackError(_) => "fallback_error".to_string(),
            Self::Other(_) => "other".to_string(),
            Self::RetryableTimeoutError => "retryable_timeout".to_string(),
            Self::NonRetryableTimeoutError => "non_retryable_timeout".to_string(),
        }
    }
}

#[cfg(msim)]
fn should_inject_error(current_index: usize) -> bool {
    // For odd indices, always inject error
    if current_index % 2 == 1 {
        tracing::warn!(
            "Injecting a RPC error for fallback client {} (odd index)",
            current_index
        );
        true
    } else {
        // For even indices, 50% chance
        let should_fail = rand::random::<bool>();
        if should_fail {
            tracing::warn!(
                "Injecting a RPC error for fallback client {} (even index, random)",
                current_index
            );
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicUsize, Arc};

    use super::*;
    use crate::client::retry_client::failover::{FailoverClient, FailoverWrapper};

    // Mock client that counts number of calls and returns configurable results.
    #[derive(Debug)]
    struct MockClient {
        call_count: Arc<AtomicUsize>,
        should_fail: bool,
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
        let failing_client = MockClient::new(true);
        let succeeding_client = MockClient::new(false);

        let failing_calls = failing_client.call_count.clone();
        let succeeding_calls = succeeding_client.call_count.clone();

        let clients = vec![
            FailoverClient {
                client: Arc::new(failing_client),
                name: "failing".to_string(),
            },
            FailoverClient {
                client: Arc::new(succeeding_client),
                name: "succeeding".to_string(),
            },
        ];

        let wrapper = FailoverWrapper::new(clients).unwrap();

        // Execute operation that should failover from first to second client.
        let result = wrapper
            .with_failover(
                |client| {
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
        assert_eq!(
            wrapper
                .current_index
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn test_failover_wrapper_all_fail() {
        // Create mock clients - both fail.
        let clients = vec![
            FailoverClient {
                client: Arc::new(MockClient::new(true)),
                name: "failing1".to_string(),
            },
            FailoverClient {
                client: Arc::new(MockClient::new(true)),
                name: "failing2".to_string(),
            },
        ];

        let wrapper = FailoverWrapper::new(clients).unwrap();

        // Execute operation that should try both clients and fail.
        let result = wrapper
            .with_failover(
                |client| {
                    Box::pin(async move {
                        let client = client.as_ref();
                        client.operation().await
                    })
                },
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
