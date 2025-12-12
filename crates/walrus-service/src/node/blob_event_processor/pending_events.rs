// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use crate::node::metrics::NodeMetricSet;

// Poll interval for checking pending background events.
pub(crate) const PENDING_EVENTS_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A utility struct that wraps an `Arc<AtomicU32>` for tracking pending events.
/// Provides convenient `inc()` and `dec()` methods that return the new value.
#[derive(Default, Debug, Clone)]
pub struct PendingEventCounter {
    inner: Arc<AtomicU32>,
}

impl PendingEventCounter {
    /// Increments the counter and returns a guard that will decrement it when dropped.
    #[must_use]
    pub(crate) fn track_event(&self, metrics: Arc<NodeMetricSet>) -> PendingEventGuard {
        PendingEventGuard::new(self.clone(), metrics)
    }

    /// Increments the counter and returns the new value.
    fn inc(&self) -> u32 {
        // We must ensure the increment happens-before the event is observable by the worker via
        // the channel. Without a strong ordering, the send could be observed before the increment,
        // allowing the receiver to drop the guard and do fetch_sub first, which would underflow
        // the counter.
        self.inner.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Decrements the counter and returns the new value.
    fn dec(&self) -> u32 {
        // Same reasoning as inc() applies hereto use SeqCst ordering.
        let prev = self.inner.fetch_sub(1, Ordering::SeqCst);
        if prev == 0 {
            // This should never happen, since the counter is incremented before the event is
            // observable by the worker via the channel.
            panic!("pending event counter underflow");
        }
        prev.saturating_sub(1)
    }

    /// Gets the current value of the counter.
    fn get(&self) -> u32 {
        self.inner.load(Ordering::SeqCst)
    }

    /// Waits for all events to be processed in the background processors.
    pub async fn wait_for_all_events_to_be_processed(&self) {
        while self.get() > 0 {
            tokio::time::sleep(PENDING_EVENTS_POLL_INTERVAL).await;
        }
        tracing::info!("All pending blob events have been processed in background processors.");
    }
}

/// A guard that automatically decrements the pending event counter when dropped.
/// This ensures robust tracking of pending events even if processing errors occur.
#[derive(Debug)]
pub(crate) struct PendingEventGuard {
    pending_event_count: PendingEventCounter,
    metrics: Arc<NodeMetricSet>,
}

impl PendingEventGuard {
    fn new(pending_event_count: PendingEventCounter, metrics: Arc<NodeMetricSet>) -> Self {
        let current_pending_event_count = pending_event_count.inc();
        metrics
            .pending_processing_blob_events_in_background_processors
            .set(current_pending_event_count.into());
        Self {
            pending_event_count,
            metrics,
        }
    }
}

impl Drop for PendingEventGuard {
    fn drop(&mut self) {
        let current_pending_event_count = self.pending_event_count.dec();
        self.metrics
            .pending_processing_blob_events_in_background_processors
            .set(current_pending_event_count.into());
    }
}
