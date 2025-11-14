// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Coordination module for managing operation between checkpoint downloader and catchup manager.

use std::{sync::Arc, time::Instant};

use tokio::sync::{Mutex, Notify, mpsc};

/// Messages for coordinating between catchup manager and checkpoint tailing
#[derive(Debug, Clone)]
pub enum CoordinationMessage {
    /// Stop checkpoint tailing for catchup processing
    StopCheckpointTailing,
    /// Restart checkpoint tailing after catchup completion
    RestartCheckpointTailing,
    /// Catchup operation failed, resume normal operation
    CatchupFailed,
}

/// Coordination state between checkpoint downloader and catchup manager.
///
/// This struct manages the synchronization between the checkpoint downloader and
/// catchup manager during runtime catchup operations using message-passing.
/// The coordination works in phases:
/// 1. Download phase: Event blobs are downloaded in parallel with checkpoint tailing
/// 2. Processing phase: Send StopCheckpointTailing message, process event blobs
/// 3. Resume phase: Send RestartCheckpointTailing message (picks up from new latest checkpoint)
#[derive(Debug)]
pub struct CatchupCoordinationState {
    /// Channel for sending coordination messages to checkpoint tailing task
    pub coordination_tx: mpsc::UnboundedSender<CoordinationMessage>,
    /// Whether catchup is currently active
    pub catchup_active: Arc<std::sync::atomic::AtomicBool>,
    /// Timestamp when catchup was last started
    pub last_catchup_start: Arc<Mutex<Option<Instant>>>,
    /// Notifier signaled when checkpoint tailing has fully stopped
    tailing_stopped_notify: Arc<Notify>,
    /// Flag indicating whether checkpoint tailing is currently stopped
    is_tailing_stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl CatchupCoordinationState {
    /// Attempt to mark catchup as active. Returns true if we successfully
    /// transitioned from inactive to active, false if another catchup is already running.
    pub async fn try_start_catchup(&self) -> bool {
        match self.catchup_active.compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => {
                *self.last_catchup_start.lock().await = Some(Instant::now());
                true
            }
            Err(_) => false,
        }
    }

    /// Explicitly mark catchup as inactive.
    pub fn mark_catchup_inactive(&self) {
        self.catchup_active
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Creates a new coordination state instance and returns it with a receiver.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<CoordinationMessage>) {
        let (coordination_tx, coordination_rx) = mpsc::unbounded_channel();

        (
            Self {
                coordination_tx,
                catchup_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                last_catchup_start: Arc::new(Mutex::new(None)),
                tailing_stopped_notify: Arc::new(Notify::new()),
                is_tailing_stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
            coordination_rx,
        )
    }

    /// Mark that checkpoint tailing has started.
    pub fn mark_tailing_started(&self) {
        self.is_tailing_stopped
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Notify that checkpoint tailing has fully stopped and wake any waiters.
    pub fn notify_tailing_stopped(&self) {
        self.is_tailing_stopped
            .store(true, std::sync::atomic::Ordering::Release);
        self.tailing_stopped_notify.notify_one();
    }

    /// Wait until checkpoint tailing is stopped, with timeout.
    /// Returns true if stopped, false on timeout.
    pub async fn wait_for_tailing_stopped(&self, timeout: std::time::Duration) -> bool {
        if self
            .is_tailing_stopped
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return true;
        }
        match tokio::time::timeout(timeout, self.tailing_stopped_notify.notified()).await {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    /// Start the download phase of catchup.
    /// During this phase, event blobs are downloaded in parallel with checkpoint tailing.
    #[cfg(test)]
    pub async fn start_catchup_download_phase(&self) {
        tracing::info!(
            "starting catchup download phase - checkpoint tailing continues in parallel"
        );

        self.catchup_active
            .store(true, std::sync::atomic::Ordering::Release);
        *self.last_catchup_start.lock().await = Some(Instant::now());
    }

    /// Transition from download phase to processing phase.
    /// This sends a message to stop checkpoint tailing.
    pub fn start_catchup_processing_phase(
        &self,
    ) -> Result<(), mpsc::error::SendError<CoordinationMessage>> {
        tracing::info!(
            "starting catchup processing phase - sending stop message to checkpoint tailing"
        );

        self.coordination_tx
            .send(CoordinationMessage::StopCheckpointTailing)?;
        Ok(())
    }

    /// Complete catchup processing and signal for checkpoint tailing restart.
    /// This sends a message to restart checkpoint tailing from the new latest checkpoint.
    pub fn complete_catchup(&self) -> Result<(), mpsc::error::SendError<CoordinationMessage>> {
        tracing::info!("completing catchup - sending restart message to checkpoint tailing");

        self.catchup_active
            .store(false, std::sync::atomic::Ordering::Release);
        self.coordination_tx
            .send(CoordinationMessage::RestartCheckpointTailing)?;
        Ok(())
    }

    /// Signal catchup failure and resume normal operation.
    pub fn catchup_failed(&self) -> Result<(), mpsc::error::SendError<CoordinationMessage>> {
        tracing::warn!("catchup failed - sending failure message to checkpoint tailing");

        self.catchup_active
            .store(false, std::sync::atomic::Ordering::Release);
        self.coordination_tx
            .send(CoordinationMessage::CatchupFailed)?;
        Ok(())
    }

    /// Check if catchup is currently active.
    pub fn is_catchup_active(&self) -> bool {
        self.catchup_active
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get statistics about the current coordination state.
    pub async fn get_stats(&self) -> CoordinationStats {
        let last_start = *self.last_catchup_start.lock().await;

        CoordinationStats {
            catchup_active: self.is_catchup_active(),
            time_since_last_catchup: last_start.map(|t| t.elapsed()),
        }
    }
}

impl Default for CatchupCoordinationState {
    fn default() -> Self {
        Self::new().0
    }
}

/// Statistics about the current coordination state.
#[derive(Debug, Clone)]
pub struct CoordinationStats {
    /// Whether catchup is currently active
    pub catchup_active: bool,
    /// Time elapsed since last catchup started
    pub time_since_last_catchup: Option<std::time::Duration>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_coordination_message_passing() {
        let (state, mut rx) = CatchupCoordinationState::new();

        // Test sending stop message
        state.start_catchup_processing_phase().unwrap();

        match rx.recv().await {
            Some(CoordinationMessage::StopCheckpointTailing) => {
                // Expected
            }
            other => panic!("Expected StopCheckpointTailing, got {:?}", other),
        }

        // Test sending restart message
        state.complete_catchup().unwrap();

        match rx.recv().await {
            Some(CoordinationMessage::RestartCheckpointTailing) => {
                // Expected
            }
            other => panic!("Expected RestartCheckpointTailing, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_cooldown_management_like_behavior() {
        let (state, _rx) = CatchupCoordinationState::new();
        let short_wait = Duration::from_millis(10);

        // Initially no catchup and no timestamp
        let stats = state.get_stats().await;
        assert!(!stats.catchup_active);
        assert!(stats.time_since_last_catchup.is_none());

        // Start catchup download phase should set active and timestamp
        state.start_catchup_download_phase().await;
        let stats = state.get_stats().await;
        assert!(stats.catchup_active);
        assert!(stats.time_since_last_catchup.is_some());

        // After a short wait, elapsed should still be some (monotonic increase implied)
        sleep(short_wait + Duration::from_millis(5)).await;
        let stats = state.get_stats().await;
        assert!(stats.catchup_active);
        assert!(stats.time_since_last_catchup.is_some());
    }

    #[tokio::test]
    async fn test_coordination_stats() {
        let (state, _rx) = CatchupCoordinationState::new();

        // Initial stats
        let stats = state.get_stats().await;
        assert!(!stats.catchup_active);
        assert_eq!(stats.time_since_last_catchup, None);

        // After starting catchup
        state.start_catchup_download_phase().await;

        let stats = state.get_stats().await;
        assert!(stats.catchup_active);
        assert!(stats.time_since_last_catchup.is_some());
    }
}
