// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Implements a checkpoint download handler for the RPC client.
use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use atomic_time::AtomicInstant;

use crate::client::{
    SuiClientMetricSet,
    retry_client::RetriableClientError,
    rpc_config::RpcFallbackConfig,
};

/// The decision to make where to download a checkpoint.
#[derive(Debug)]
pub enum CheckpointDownloadDecision {
    /// Attempt primary RPC first.
    AttemptPrimary,
    /// Direct fallback for the checkpoint.
    DirectFallback,
    /// Try fallback if primary fails.
    FallbackUponPrimaryFailure(RetriableClientError),
    /// Use primary error if fallback fails.
    UsePrimaryError(RetriableClientError),
}

/// Updated failure stats for the current sequence number.
#[derive(Debug)]
pub struct CheckpointFailureStats {
    /// Overall number of failures since last_success.
    /// Note: This is not the number of failures for the current sequence number.
    /// This is the number of failures since the last successful RPC call.
    overall_failures_count: usize,
    /// Number of consecutive failures for the current sequence number.
    consecutive_failures_for_this_seq: usize,
}

/// Checkpoint download handler for the RPC client.
pub struct CheckpointDownloadHandler {
    // Configuration for fallback.
    config: Option<RpcFallbackConfig>,
    // Overall last success time from primary RPC.
    last_success: Arc<AtomicInstant>,
    // Overall number of failures since last_success.
    num_failures: Arc<AtomicUsize>,
    // When to skip primary due to overall failures.
    skip_primary_until: Arc<AtomicInstant>,
    // Map of checkpoint sequence numbers to their failure counts.
    checkpoint_failures: Arc<Mutex<HashMap<u64, usize>>>,
    // Metrics for the client.
    metrics: Option<Arc<SuiClientMetricSet>>,
}

impl std::fmt::Debug for CheckpointDownloadHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckpointDownloadHandler {{ config: {:?}, last_success: {:?}, \
            num_failures: {:?}, skip_primary_until: {:?}, \
            checkpoint_failures: {:?} }}",
            self.config,
            self.last_success.load(Ordering::Relaxed),
            self.num_failures.load(Ordering::Relaxed),
            self.skip_primary_until.load(Ordering::Relaxed),
            self.checkpoint_failures
                .lock()
                .expect("mutex should not be poisoned")
        )
    }
}

impl CheckpointDownloadHandler {
    /// Creates a new FallbackHandler with the given configuration.
    pub fn new(
        config: Option<RpcFallbackConfig>,
        metrics: Option<Arc<SuiClientMetricSet>>,
    ) -> Self {
        Self {
            config: config.clone(),
            last_success: Arc::new(AtomicInstant::new(Instant::now())),
            num_failures: Arc::new(AtomicUsize::new(0)),
            skip_primary_until: Arc::new(AtomicInstant::new(Instant::now())),
            checkpoint_failures: Arc::new(Mutex::new(HashMap::new())),
            metrics,
        }
    }

    /// Called on primary success for a specific sequence_number.
    pub fn record_primary_success(&self, sequence_number: u64) {
        self.last_success.store(Instant::now(), Ordering::Relaxed);
        self.num_failures.store(0, Ordering::Relaxed);

        // Remove this checkpoint from the failures map.
        let mut failures = self
            .checkpoint_failures
            .lock()
            .expect("mutex should not be poisoned");
        failures.remove(&sequence_number);
    }

    /// Called on fallback success for a specific sequence_number.
    pub fn record_fallback_success_for_sequence(&self, sequence_number: u64) {
        // Remove this checkpoint from the failures map.
        let mut failures = self
            .checkpoint_failures
            .lock()
            .expect("mutex should not be poisoned");
        failures.remove(&sequence_number);
    }

    /// Determines the initial action for fetching a checkpoint.
    pub fn initial_download_decision(&self) -> CheckpointDownloadDecision {
        if self.config.is_none() {
            if let Some(metrics) = &self.metrics {
                metrics.record_checkpoint_download_source("primary_only_no_fallback_config");
            }
            return CheckpointDownloadDecision::AttemptPrimary;
        }
        if Instant::now() >= self.skip_primary_until.load(Ordering::Relaxed) {
            if let Some(metrics) = &self.metrics {
                metrics.record_checkpoint_download_source("primary");
            }
            CheckpointDownloadDecision::AttemptPrimary
        } else {
            if let Some(metrics) = &self.metrics {
                metrics.record_checkpoint_download_source("direct_fallback");
            }
            tracing::debug!(
                "Skipping primary RPC (overall policy) until {:?}, attempting fallback directly.",
                self.skip_primary_until.load(Ordering::Relaxed)
            );
            CheckpointDownloadDecision::DirectFallback
        }
    }

    /// Updates all relevant failure counters based on the current failure
    /// and returns a snapshot of the counts relevant to the decision.
    /// Prioritizes tracking failures for smaller sequence numbers.
    pub fn update_and_get_failure_stats(&self, sequence_number: u64) -> CheckpointFailureStats {
        let overall_failures_count = self.num_failures.fetch_add(1, Ordering::Relaxed) + 1;

        let mut failures = self
            .checkpoint_failures
            .lock()
            .expect("mutex should not be poisoned");
        let consecutive_failures_for_this_seq = failures
            .entry(sequence_number)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        CheckpointFailureStats {
            overall_failures_count,
            consecutive_failures_for_this_seq: *consecutive_failures_for_this_seq,
        }
    }

    /// Assesses the primary checkpoint failure and returns a download decision
    /// based on the failure stats.
    /// This function is called when the primary RPC fails to fetch a checkpoint.
    pub fn decision_upon_primary_failure(
        &self,
        sequence_number: u64,
        primary_error: RetriableClientError,
    ) -> CheckpointDownloadDecision {
        let Some(ref cfg) = self.config else {
            // If no fallback config is provided, we will return the primary error.
            if let Some(metrics) = &self.metrics {
                metrics.record_checkpoint_download_source("primary_error_no_fallback_config");
            }
            return CheckpointDownloadDecision::UsePrimaryError(primary_error);
        };

        // Update all failure counters and get their current relevant values.
        let failure_stats = self.update_and_get_failure_stats(sequence_number);
        let last_success_time = self.last_success.load(Ordering::Relaxed);

        tracing::debug!(
            seq = sequence_number, error = ?primary_error,
            stats.overall_failures = failure_stats.overall_failures_count,
            stats.consecutive_for_this_seq = failure_stats.consecutive_failures_for_this_seq,
            stats.last_success_elapsed = ?last_success_time.elapsed(),
            "Primary failure stats updated and assessed."
        );

        // Make fallback decisions based on the updated stats.
        if primary_error.is_eligible_for_fallback_immediately(sequence_number) {
            tracing::debug!(
                seq = sequence_number,
                "Error eligible for immediate fallback."
            );
            if let Some(metrics) = &self.metrics {
                metrics.record_checkpoint_download_source("fallback_immediate");
            }
            return CheckpointDownloadDecision::FallbackUponPrimaryFailure(primary_error);
        }

        // Overall failure window exceeded.
        let is_overall_window_exceeded = failure_stats.overall_failures_count
            >= cfg.min_failures_to_start_fallback
            && last_success_time.elapsed() >= cfg.failure_window_to_start_fallback_duration;

        // Consecutive failures for this specific checkpoint limit met.
        let max_consecutive_from_config = cfg.max_consecutive_failures;
        let is_specific_consecutive_limit_met =
            failure_stats.consecutive_failures_for_this_seq >= max_consecutive_from_config;

        if is_overall_window_exceeded || is_specific_consecutive_limit_met {
            if is_overall_window_exceeded {
                // If overall window is exceeded, set the flag to skip primary for a duration.
                self.skip_primary_until.store(
                    Instant::now() + cfg.skip_rpc_for_checkpoint_duration,
                    Ordering::Relaxed,
                );
                tracing::debug!(
                    seq = sequence_number,
                    "Overall failure window exceeded. Skipping primary for checkpoints until {:?}.",
                    self.skip_primary_until.load(Ordering::Relaxed)
                );
                if let Some(metrics) = &self.metrics {
                    metrics.record_checkpoint_download_source(
                        "fallback_after_primary_failure_overall_window_exceeded",
                    );
                }
            }
            if is_specific_consecutive_limit_met {
                tracing::debug!(
                    seq = sequence_number,
                    consecutive = failure_stats.consecutive_failures_for_this_seq,
                    threshold = max_consecutive_from_config,
                    "Consecutive failure limit met for this specific checkpoint."
                );
                if let Some(metrics) = &self.metrics {
                    metrics.record_checkpoint_download_source(
                        "fallback_after_primary_failure_consecutive_limit_met",
                    );
                }
            }
            return CheckpointDownloadDecision::FallbackUponPrimaryFailure(primary_error);
        }

        // If no fallback conditions are met.
        tracing::debug!(
            seq = sequence_number,
            "No fallback condition met. Using primary error."
        );
        if let Some(metrics) = &self.metrics {
            metrics.record_checkpoint_download_source("primary_error_no_fallback");
        }
        CheckpointDownloadDecision::UsePrimaryError(primary_error)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::Ordering, time::Duration};

    use walrus_utils::backoff::ExponentialBackoffConfig;

    use super::*;
    use crate::client::retry_client::CheckpointRpcError;
    fn dummy_rpc_fallback_config() -> Option<RpcFallbackConfig> {
        Some(RpcFallbackConfig {
            checkpoint_bucket: "https://checkpoint-bucket.com".parse().unwrap(),
            min_failures_to_start_fallback: 0,
            failure_window_to_start_fallback_duration: Duration::from_secs(0),
            skip_rpc_for_checkpoint_duration: Duration::from_secs(0),
            max_consecutive_failures: 3,
            quick_retry_config: ExponentialBackoffConfig::default(),
        })
    }

    fn test_config(
        min_overall_failures: usize,
        window: Duration,
        max_consecutive: usize,
        skip_duration: Duration,
    ) -> RpcFallbackConfig {
        RpcFallbackConfig {
            checkpoint_bucket: "https://checkpoint-bucket.com".parse().unwrap(),
            min_failures_to_start_fallback: min_overall_failures,
            failure_window_to_start_fallback_duration: window,
            skip_rpc_for_checkpoint_duration: skip_duration,
            max_consecutive_failures: max_consecutive,
            quick_retry_config: ExponentialBackoffConfig::default(),
        }
    }

    fn rpc_error_generic(seq: u64) -> RetriableClientError {
        RetriableClientError::RpcError(CheckpointRpcError {
            status: tonic::Status::internal("Network blip"),
            checkpoint_seq_num: Some(seq),
        })
    }

    fn rpc_error_pruned(seq_to_process: u64) -> RetriableClientError {
        let mut status = tonic::Status::not_found("Pruned");
        status.metadata_mut().insert(
            "x-sui-checkpoint-height",
            (seq_to_process + 1).to_string().parse().unwrap(),
        );
        RetriableClientError::RpcError(CheckpointRpcError {
            status,
            checkpoint_seq_num: Some(seq_to_process),
        })
    }

    #[test]
    fn track_first_failure_when_none_tracked() {
        let handler = CheckpointDownloadHandler::new(dummy_rpc_fallback_config(), None);
        let seq = 100;

        let stats = handler.update_and_get_failure_stats(seq);

        assert_eq!(stats.overall_failures_count, 1);
        assert_eq!(stats.consecutive_failures_for_this_seq, 1);
        assert_eq!(
            handler
                .checkpoint_failures
                .lock()
                .unwrap()
                .get(&seq)
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn increment_failure_for_currently_tracked_sequence() {
        let handler = CheckpointDownloadHandler::new(dummy_rpc_fallback_config(), None);
        let seq = 100;

        // Simulate it's already being tracked with 1 failure
        handler.num_failures.store(1, Ordering::Relaxed);
        handler.checkpoint_failures.lock().unwrap().insert(seq, 1);

        let stats = handler.update_and_get_failure_stats(seq);

        // 1 existing + 1 new
        assert_eq!(stats.overall_failures_count, 2);
        assert_eq!(stats.consecutive_failures_for_this_seq, 2);
        assert_eq!(
            handler
                .checkpoint_failures
                .lock()
                .unwrap()
                .get(&seq)
                .copied(),
            Some(2)
        );
    }

    #[test]
    fn update_failure_stats_for_multiple_sequences() {
        let handler = CheckpointDownloadHandler::new(dummy_rpc_fallback_config(), None);
        let initially_tracked_seq = 200;
        let failing_smaller_seq = 100;

        handler.num_failures.store(1, Ordering::Relaxed);
        handler
            .checkpoint_failures
            .lock()
            .unwrap()
            .insert(initially_tracked_seq, 2);

        let stats = handler.update_and_get_failure_stats(failing_smaller_seq);

        assert_eq!(stats.overall_failures_count, 2);
        // For failing_smaller_seq (100), its consecutive count should now be 1
        assert_eq!(stats.consecutive_failures_for_this_seq, 1);
        assert_eq!(
            handler
                .checkpoint_failures
                .lock()
                .unwrap()
                .get(&failing_smaller_seq)
                .copied(),
            Some(1)
        );
    }

    #[test]
    fn assess_no_fallback_config() {
        let handler = CheckpointDownloadHandler::new(None, None);
        let seq = 100;
        let err = rpc_error_generic(seq);

        let decision = handler.decision_upon_primary_failure(seq, err);
        assert!(matches!(
            decision,
            CheckpointDownloadDecision::UsePrimaryError(_)
        ));
    }

    #[test]
    fn assess_immediate_fallback_error() {
        let config = test_config(5, Duration::from_secs(300), 3, Duration::from_secs(60));
        let handler = CheckpointDownloadHandler::new(Some(config), None);
        let seq = 100;
        let err = rpc_error_pruned(seq);

        let decision = handler.decision_upon_primary_failure(seq, err);
        assert!(matches!(
            decision,
            CheckpointDownloadDecision::FallbackUponPrimaryFailure(_)
        ));
    }

    #[tokio::test]
    async fn assess_overall_window_exceeded_triggers_fallback_and_sets_skip() {
        let skip_duration = Duration::from_secs(60);
        let config = test_config(3, Duration::from_secs(10), 5, skip_duration);
        let handler = CheckpointDownloadHandler::new(Some(config), None);
        let seq = 100;
        let err = rpc_error_generic(seq);

        // Setup state to exceed overall window
        handler.num_failures.store(2, Ordering::Relaxed);

        // Wait for the failure window to pass
        tokio::time::sleep(Duration::from_secs(11)).await;

        let initial_skip_until = handler.skip_primary_until.load(Ordering::Relaxed);

        let decision = handler.decision_upon_primary_failure(seq, err);
        assert!(matches!(
            decision,
            CheckpointDownloadDecision::FallbackUponPrimaryFailure(_)
        ));
        assert_ne!(
            handler.skip_primary_until.load(Ordering::Relaxed),
            initial_skip_until
        );
        // Check if skip_primary_until is roughly now + skip_duration
        let expected_skip_roughly = Instant::now() + skip_duration;
        let actual_skip = handler.skip_primary_until.load(Ordering::Relaxed);
        assert!(actual_skip > Instant::now() && actual_skip <= expected_skip_roughly);
    }

    #[test]
    fn assess_specific_consecutive_failure_triggers_fallback() {
        let config = test_config(10, Duration::from_secs(300), 3, Duration::from_secs(60));
        let handler = CheckpointDownloadHandler::new(Some(config.clone()), None);
        let seq = 100;
        let err = rpc_error_generic(seq);

        // Setup state for specific consecutive failure
        // update_and_get_failure_stats will make this the 3rd consecutive for seq 100
        handler.checkpoint_failures.lock().unwrap().insert(seq, 3);
        // Ensure overall window is NOT met
        handler.num_failures.store(0, Ordering::Relaxed);
        handler
            .last_success
            .store(Instant::now(), Ordering::Relaxed);

        let decision = handler.decision_upon_primary_failure(seq, err);
        assert!(matches!(
            decision,
            CheckpointDownloadDecision::FallbackUponPrimaryFailure(_)
        ));
        assert!(handler.skip_primary_until.load(Ordering::Relaxed) < Instant::now());
    }
}
