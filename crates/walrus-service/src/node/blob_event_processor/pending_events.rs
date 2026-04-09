// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU32, Ordering},
    },
    time::Duration,
};

use crate::node::metrics::NodeMetricSet;

// Poll interval for checking pending background events.
pub(crate) const PENDING_EVENTS_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Tracks pending background tasks and the highest event index whose background processing has
/// completed.
#[derive(Debug, Clone)]
pub struct PendingEventCounter {
    inner: Arc<AtomicU32>,
    highest_processed_event_index: Arc<AtomicI64>,
}

impl Default for PendingEventCounter {
    fn default() -> Self {
        Self {
            inner: Arc::new(AtomicU32::new(0)),
            highest_processed_event_index: Arc::new(AtomicI64::new(-1)),
        }
    }
}

impl PendingEventCounter {
    pub(crate) fn reset_highest_processed_event_index(&self, metrics: &NodeMetricSet) {
        self.highest_processed_event_index
            .store(-1, Ordering::SeqCst);
        metrics.reset_highest_background_processed_event_index();
    }

    pub(crate) fn observe_processed_event(&self, event_index: u64, metrics: &NodeMetricSet) {
        let event_index = event_index
            .try_into()
            .expect("event index should fit in i64");
        let previous_highest = self
            .highest_processed_event_index
            .fetch_max(event_index, Ordering::SeqCst);
        if event_index > previous_highest {
            metrics.set_highest_background_processed_event_index(event_index);
        }
    }

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use walrus_utils::metrics::Registry;

    use super::PendingEventCounter;
    use crate::node::metrics::NodeMetricSet;

    #[test]
    fn tracks_highest_processed_event_index() {
        let metrics = Arc::new(NodeMetricSet::new(&Registry::default()));
        let pending_event_counter = PendingEventCounter::default();
        pending_event_counter.reset_highest_processed_event_index(metrics.as_ref());

        assert_eq!(metrics.highest_background_processed_event_index(), -1);

        pending_event_counter.observe_processed_event(12, metrics.as_ref());
        assert_eq!(metrics.highest_background_processed_event_index(), 12);

        pending_event_counter.observe_processed_event(7, metrics.as_ref());
        assert_eq!(metrics.highest_background_processed_event_index(), 12);

        pending_event_counter.observe_processed_event(20, metrics.as_ref());
        assert_eq!(metrics.highest_background_processed_event_index(), 20);
    }
}
