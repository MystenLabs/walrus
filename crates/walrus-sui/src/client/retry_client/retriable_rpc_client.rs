// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Infrastructure for retrying RPC calls with backoff, in case there are network errors.
//!
//! Wraps the [`FallibleRpcClient`] to introduce retries, and handles failover with
//! [`FailoverWrapper`].

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use rand::{
    Rng as _,
    rngs::{StdRng, ThreadRng},
};
use sui_rpc_api::{Client, client::ResponseExt};
use sui_types::{
    base_types::ObjectID,
    full_checkpoint_content::CheckpointData,
    messages_checkpoint::CertifiedCheckpointSummary,
    object::Object,
};
use thiserror::Error;
use walrus_utils::{
    backoff::{ExponentialBackoff, ExponentialBackoffConfig},
    metrics::monitored_scope,
    tracing_sampled,
};

use self::fallback_client::FallbackClient;
pub use self::fallback_client::FallbackError;
use super::{
    FailoverWrapper,
    FallibleRpcClient,
    download_handler::{CheckpointDownloadDecision, CheckpointDownloadHandler},
    failover::{FailoverError, LazyClientBuilder},
    retry_rpc_errors,
};
use crate::client::{SuiClientMetricSet, rpc_config::RpcFallbackConfig};
pub mod fallback_client;

/// Checks if the full node provides the required REST endpoint for event processing.
pub(crate) async fn check_experimental_rest_endpoint_exists(
    client: Arc<Client>,
) -> anyhow::Result<bool> {
    // TODO: https://github.com/MystenLabs/walrus/issues/1049
    // TODO: Use utils::retry once it is outside walrus-service such that it doesn't trigger
    // cyclic dependency errors
    let latest_checkpoint = client.get_latest_checkpoint().await?;
    let mut total_remaining_attempts = 5;
    while let Err(e) = client
        .get_full_checkpoint(latest_checkpoint.sequence_number)
        .await
    {
        total_remaining_attempts -= 1;
        if total_remaining_attempts == 0 {
            tracing::error!(
                error = ?e,
                "failed to get full checkpoint after {} attempts. \
                REST endpoint may not be available.",
                5
            );
            return Ok(false);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(true)
}

/// Ensures that the full node provides the required REST endpoint for event processing.
pub(crate) async fn ensure_experimental_rest_endpoint_exists(
    client: Arc<Client>,
) -> anyhow::Result<()> {
    if !check_experimental_rest_endpoint_exists(client.clone()).await? {
        anyhow::bail!(
            "the configured full node *does not* provide the required REST endpoint for event \
            processing; make sure to configure a full node in the node's configuration file, which \
            provides the necessary endpoint"
        );
    } else {
        tracing::info!(
            "the configured full node provides the required REST endpoint for event processing"
        );
    }
    Ok(())
}
/// A builder for creating a [`FallibleRpcClient`] lazily. For use in failover.
#[derive(Debug)]
pub enum LazyFallibleRpcClientBuilder {
    /// The URL of the RPC server.
    Url {
        /// The URL of the RPC server.
        rpc_url: String,
        /// Whether to run a check to see if the endpoint exists.
        ensure_experimental_rest_endpoint: bool,
    },
    /// A pre-existing client.
    Client(Arc<FallibleRpcClient>),
}

impl LazyClientBuilder<FallibleRpcClient> for LazyFallibleRpcClientBuilder {
    const DEFAULT_MAX_TRIES: usize = 5;

    async fn lazy_build_client(&self) -> Result<Arc<FallibleRpcClient>, FailoverError> {
        // Inject rpc client build failure for simtests.
        #[cfg(msim)]
        {
            let mut fail_client_creation = false;
            sui_macros::fail_point_arg!(
                "failpoint_rpc_client_build_client",
                |url_to_fail: String| {
                    match self {
                        Self::Url { rpc_url, .. } => {
                            if *rpc_url == url_to_fail {
                                fail_client_creation = true;
                            }
                        }
                        Self::Client(_) => {}
                    }
                }
            );

            if fail_client_creation {
                tracing::info!("injected rpc client build failure {:?}", self.get_rpc_url());
                return Err(FailoverError::FailedToGetClient(format!(
                    "injected rpc client build failure {:?}",
                    self.get_rpc_url()
                )));
            }
        }

        match self {
            Self::Url {
                rpc_url,
                ensure_experimental_rest_endpoint,
            } => {
                let client = FallibleRpcClient::new(rpc_url.to_owned())
                    .map(Arc::new)
                    .map_err(|e| FailoverError::FailedToGetClient(e.to_string()))?;
                if *ensure_experimental_rest_endpoint {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        ensure_experimental_rest_endpoint_exists(client.inner().await),
                    )
                    .await
                    .map_err(|_| {
                        FailoverError::FailedToGetClient(format!(
                            "Client validation timed out for {rpc_url:?}"
                        ))
                    })?
                    .map_err(|e| {
                        FailoverError::FailedToGetClient(format!(
                            "Client validation failed for {rpc_url:?}: {e:?}"
                        ))
                    })?;
                }
                Ok(client)
            }
            Self::Client(client) => Ok(client.clone()),
        }
    }

    fn get_rpc_url(&self) -> Option<&str> {
        match self {
            Self::Url { rpc_url, .. } => Some(rpc_url.as_str()),
            Self::Client(client) => Some(client.get_rpc_url()),
        }
    }
}

impl From<FallibleRpcClient> for LazyFallibleRpcClientBuilder {
    fn from(client: FallibleRpcClient) -> Self {
        Self::Client(Arc::new(client))
    }
}

/// A [`sui_rpc_api::Client`] that retries RPC calls with backoff in case of network errors.
/// RpcClient is used primarily for retrieving checkpoint data from the Sui RPC server while
/// SuiClient is used for all other RPC calls.
#[derive(Clone)]
pub struct RetriableRpcClient {
    client: Arc<FailoverWrapper<FallibleRpcClient, LazyFallibleRpcClientBuilder>>,
    request_timeout: Duration,
    backoff_config: ExponentialBackoffConfig,
    fallback_client: Option<FallbackClient>,
    metrics: Option<Arc<SuiClientMetricSet>>,
    download_handler: Arc<CheckpointDownloadHandler>,
}

impl std::fmt::Debug for RetriableRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetriableRpcClient")
            .field("backoff_config", &self.backoff_config)
            .field("client", &"FailoverWrapper<FallibleRpcClient>")
            .field("fallback_client", &"CheckpointDownloadClient")
            .field("download_handler", &self.download_handler)
            .finish()
    }
}

impl RetriableRpcClient {
    // TODO(WAL-718): The timeout should ideally be set directly on the tonic client.

    /// Creates a new retriable client.
    pub async fn new(
        lazy_client_builders: Vec<LazyFallibleRpcClientBuilder>,
        request_timeout: Duration,
        backoff_config: ExponentialBackoffConfig,
        fallback_config: Option<RpcFallbackConfig>,
        metrics: Option<Arc<SuiClientMetricSet>>,
    ) -> anyhow::Result<Self> {
        let fallback_client = fallback_config.as_ref().map(|config| {
            let url = config.checkpoint_bucket.clone();
            FallbackClient::new(url, request_timeout)
        });

        Ok(Self {
            client: Arc::new(FailoverWrapper::new(lazy_client_builders).await?),
            request_timeout,
            backoff_config,
            fallback_client,
            metrics: metrics.clone(),
            download_handler: Arc::new(CheckpointDownloadHandler::new(fallback_config, metrics)),
        })
    }

    /// Gets an extended retry strategy, seeded from the internal RNG.
    /// This strategy is used for primary client operations that are not supported
    /// by the fallback client or when the fallback client is not configured.
    ///
    /// This strategy is also used for all fallback client operations.
    fn get_strategy(&self) -> ExponentialBackoff<StdRng> {
        self.backoff_config
            .get_strategy(ThreadRng::default().r#gen())
    }

    /// Gets the checkpoint summary for the given sequence number from the primary client.
    async fn get_checkpoint_summary_from_primary(
        &self,
        sequence_number: u64,
    ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
        async fn make_request(
            client: Arc<FallibleRpcClient>,
            sequence_number: u64,
            request_timeout: Duration,
        ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
            client
                .call(
                    |rpc_client| {
                        async move { rpc_client.get_checkpoint_summary(sequence_number).await }
                    },
                    request_timeout,
                )
                .await
                .map_err(|status| CheckpointRpcError::from((status, sequence_number)).into())
        }

        let request = |client: Arc<FallibleRpcClient>, method| {
            let inner_request = retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), sequence_number, self.request_timeout),
                self.metrics.clone(),
                method,
            );
            Box::pin(inner_request)
        };

        self.client
            .with_failover(request, self.metrics.clone(), "get_checkpoint_summary")
            .await
    }

    /// Gets the full checkpoint data for the given sequence number from the primary client.
    async fn get_full_checkpoint_from_primary(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        let start_time = Instant::now();
        async fn make_request(
            client: Arc<FallibleRpcClient>,
            sequence_number: u64,
            request_timeout: Duration,
        ) -> Result<CheckpointData, RetriableClientError> {
            client
                .call(
                    |rpc_client| {
                        async move {
                            rpc_client
                                .get_full_checkpoint(sequence_number)
                                .await
                        }
                    },
                    request_timeout,
                )
                .await
                .map_err(|status| CheckpointRpcError::from((status, sequence_number)).into())
        }

        let request = |client: Arc<FallibleRpcClient>, method| {
            let inner_request = retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), sequence_number, self.request_timeout),
                self.metrics.clone(),
                method,
            );
            Box::pin(inner_request)
        };

        let result = self
            .client
            .with_failover(request, self.metrics.clone(), "get_full_checkpoint")
            .await;

        if let Some(metrics) = self.metrics.as_ref() {
            let status = match result {
                Ok(_) => "success",
                Err(_) => "failure",
            };
            metrics.record_rpc_latency(
                "get_full_checkpoint",
                &self
                    .client
                    .get_current_rpc_url()
                    .await
                    .unwrap_or_else(|_| "unknown_url".to_string()),
                status,
                start_time.elapsed(),
            );
        }

        result
    }

    /// Gets the full checkpoint data for the given sequence number.
    ///
    /// This function will first try to fetch the checkpoint from the primary client with retries.
    /// If that fails, it will try to fetch the checkpoint from the fallback client if configured.
    ///
    /// When the client is experiencing extended failure to fetch from RPC nodes, it will skip
    /// the RPC node and use the fallback client for a limited duration, and try to fetch from
    /// the RPC node again.
    #[tracing::instrument(skip(self))]
    pub async fn get_full_checkpoint(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        let _scope = monitored_scope::monitored_scope("RetriableRpcClient::get_full_checkpoint");

        match self.download_handler.initial_download_decision() {
            CheckpointDownloadDecision::AttemptPrimary => {
                self.handle_primary_attempt(sequence_number).await
            }
            CheckpointDownloadDecision::DirectFallback => {
                tracing_sampled::info!("30s", "Direct fallback for checkpoint {}", sequence_number);
                self.handle_fallback_attempt(sequence_number).await
            }
            CheckpointDownloadDecision::FallbackUponPrimaryFailure(_err) => {
                unreachable!("Invalid initial decision to fallback upon primary failure")
            }
            CheckpointDownloadDecision::UsePrimaryError(_err) => {
                unreachable!("Invalid initial decision to use primary error")
            }
        }
    }

    /// Handles the primary RPC attempt and potential fallback
    async fn handle_primary_attempt(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        match self.get_full_checkpoint_from_primary(sequence_number).await {
            Ok(checkpoint) => {
                self.download_handler
                    .record_primary_success(sequence_number);
                Ok(checkpoint)
            }
            Err(primary_error) => {
                match self
                    .download_handler
                    .decision_upon_primary_failure(sequence_number, primary_error)
                {
                    CheckpointDownloadDecision::FallbackUponPrimaryFailure(err) => {
                        tracing_sampled::error!(
                            "30s",
                            ?err,
                            "Rpc fallback for checkpoint {}",
                            sequence_number
                        );
                        self.handle_fallback_attempt(sequence_number).await
                    }
                    CheckpointDownloadDecision::UsePrimaryError(err) => Err(err),
                    CheckpointDownloadDecision::DirectFallback => {
                        unreachable!(
                            "Invalid decision to direct fallback after \
                            primary failure assessment"
                        )
                    }
                    CheckpointDownloadDecision::AttemptPrimary => {
                        unreachable!(
                            "Invalid decision to attempt primary again after \
                            primary failure assessment"
                        )
                    }
                }
            }
        }
    }

    /// Handles the fallback attempt
    async fn handle_fallback_attempt(
        &self,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        let Some(fb_client) = &self.fallback_client else {
            return Err(RetriableClientError::Other(anyhow::anyhow!(
                "Misconfiguration: Skip primary indicated but no fallback client."
            )));
        };

        fb_client
            .get_full_checkpoint(sequence_number)
            .await
            .map_err(RetriableClientError::from)
            .inspect(|_| {
                self.download_handler
                    .record_fallback_success_for_sequence(sequence_number);
            })
    }

    /// Gets the checkpoint summary for the given sequence number.
    ///
    /// This function will first try to fetch the checkpoint summary from the primary client with
    /// retries. If that fails, it will try to fetch the full checkpoint summary from the fallback
    /// client if configured and return checkpoint summary.
    #[tracing::instrument(skip(self))]
    pub async fn get_checkpoint_summary(
        &self,
        sequence: u64,
    ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
        match self.get_checkpoint_summary_from_primary(sequence).await {
            Ok(summary) => Ok(summary),
            Err(error) => {
                tracing::debug!(
                    ?error,
                    "primary client error while fetching checkpoint summary"
                );

                let Some(fallback) = &self.fallback_client else {
                    return Err(error);
                };

                tracing::info!("falling back to fallback client to fetch checkpoint summary");
                self.call_get_full_checkpoint_from_fallback_with_retries(fallback, sequence)
                    .await
                    .map_err(|_| error)
                    .map(|checkpoint| checkpoint.checkpoint_summary)
            }
        }
    }

    /// Gets the latest checkpoint sequence number.
    pub async fn get_latest_checkpoint_summary(
        &self,
    ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
        async fn make_request(
            client: Arc<FallibleRpcClient>,
            request_timeout: Duration,
        ) -> Result<CertifiedCheckpointSummary, RetriableClientError> {
            client
                .call(
                    |rpc_client| async move { rpc_client.get_latest_checkpoint().await },
                    request_timeout,
                )
                .await
                .map_err(RetriableClientError::from)
        }

        let request = |client: Arc<FallibleRpcClient>, method| {
            let inner_request = retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), self.request_timeout),
                self.metrics.clone(),
                method,
            );
            Box::pin(inner_request)
        };

        self.client
            .with_failover(
                request,
                self.metrics.clone(),
                "get_latest_checkpoint_summary",
            )
            .await
    }

    /// Gets the object with the given ID.
    pub async fn get_object(&self, id: ObjectID) -> Result<Object, RetriableClientError> {
        async fn make_request(
            client: Arc<FallibleRpcClient>,
            id: ObjectID,
            request_timeout: Duration,
        ) -> Result<Object, RetriableClientError> {
            client
                .call(
                    |rpc_client| async move { rpc_client.get_object(id).await },
                    request_timeout,
                )
                .await
                .map_err(RetriableClientError::from)
        }

        let request = |client: Arc<FallibleRpcClient>, method| {
            let inner_request = retry_rpc_errors(
                self.get_strategy(),
                move || make_request(client.clone(), id, self.request_timeout),
                self.metrics.clone(),
                method,
            );
            Box::pin(inner_request)
        };

        self.client
            .with_failover(request, self.metrics.clone(), "get_object")
            .await
    }

    /// Calls the fallback service (checkpoint archival) with retries to fetch the full checkpoint.
    async fn call_get_full_checkpoint_from_fallback_with_retries(
        &self,
        fallback: &FallbackClient,
        sequence_number: u64,
    ) -> Result<CheckpointData, RetriableClientError> {
        tracing::debug!(sequence_number, "fetching checkpoint from fallback client");
        return retry_rpc_errors(
            self.get_strategy(),
            || async {
                fallback
                    .get_full_checkpoint(sequence_number)
                    .await
                    .map_err(RetriableClientError::from)
            },
            self.metrics.clone(),
            "get_full_checkpoint_from_fallback_with_retries",
        )
        .await;
    }
}

/// Custom error type for RetriableRpcClient operations
#[derive(Error, Debug)]
pub enum RetriableClientError {
    /// RPC error from the primary client.
    #[error("RPC error: {0}")]
    RpcError(#[from] CheckpointRpcError),

    /// Timeout error from the primary client.
    #[error("primary RPC timeout")]
    RetryableTimeoutError,

    /// Non-retryable timeout error from the primary client.
    #[error("primary RPC timeout")]
    NonRetryableTimeoutError,

    /// Error from the fallback client.
    #[error("fallback error: {0}")]
    FallbackError(#[from] FallbackError),

    /// Error from the failover client.
    #[error("failover error: {0}")]
    FailoverError(#[from] FailoverError),

    /// Generic error
    #[error("client error: {0}")]
    Other(#[from] anyhow::Error),
}

impl From<tonic::Status> for RetriableClientError {
    fn from(status: tonic::Status) -> Self {
        Self::RpcError(CheckpointRpcError::from((status, 0)))
    }
}

impl RetriableClientError {
    /// Returns `true` if the error is eligible for fallback.
    ///
    /// For pruned checkpoints (indicated by a `NotFound` error and sequence number <= height),
    /// we will fallback immediately. For missing events, we will also fallback immediately.
    pub fn is_eligible_for_fallback_immediately(&self, next_checkpoint: u64) -> bool {
        match self {
            Self::RpcError(rpc_error)
                if rpc_error.status.code() == tonic::Code::NotFound
                    && rpc_error
                        .status
                        .checkpoint_height()
                        .is_some_and(|height| next_checkpoint <= height) =>
            {
                true
            }
            Self::RpcError(rpc_error)
                if rpc_error.status.code() == tonic::Code::Internal
                    && rpc_error.status.message().contains("missing event") =>
            {
                true
            }
            _ => false,
        }
    }
}

/// Error type for RPC operations
#[derive(Error, Debug)]
pub struct CheckpointRpcError {
    /// The status of the RPC error.
    pub status: tonic::Status,
    /// The sequence number of the checkpoint.
    pub checkpoint_seq_num: Option<u64>,
}

impl std::fmt::Display for CheckpointRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RPC error at checkpoint {}: {}",
            self.checkpoint_seq_num.unwrap_or(0),
            self.status
        )
    }
}

impl From<(tonic::Status, u64)> for CheckpointRpcError {
    fn from((status, checkpoint_seq_num): (tonic::Status, u64)) -> Self {
        Self {
            status,
            checkpoint_seq_num: Some(checkpoint_seq_num),
        }
    }
}

impl From<tonic::Status> for CheckpointRpcError {
    fn from(status: tonic::Status) -> Self {
        Self {
            status,
            checkpoint_seq_num: None,
        }
    }
}

impl CheckpointRpcError {
    pub(crate) fn is_checkpoint_not_produced(&self) -> bool {
        self.status.code() == tonic::Code::NotFound
            && self
                .status
                .checkpoint_height()
                .is_some_and(|height| self.checkpoint_seq_num.unwrap_or(0) > height)
    }
}
