// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus events observed by the storage node.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    future::ready,
    pin::Pin,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use anyhow::Error;
use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream;
use sui_types::{digests::TransactionDigest, event::EventID};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_stream::{Stream, wrappers::IntervalStream};
use tracing::Level;

use super::{STATUS_PENDING, STATUS_PERSISTED, StorageNodeInner};
use crate::{
    event::{
        event_processor::processor::EventProcessor,
        events::{EventStreamCursor, InitState, PositionedStreamEvent},
    },
    node::{
        metrics::{NodeMetricSet, STATUS_HIGHEST_FINISHED},
        storage::EventProgress,
    },
};

/// The capacity of the event channel.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;
const OLDEST_UNFINISHED_EVENT_AGE_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// The event ID for checkpoint events.
pub const EVENT_ID_FOR_CHECKPOINT_EVENTS: EventID = EventID {
    tx_digest: TransactionDigest::ZERO,
    event_seq: 0,
};

#[derive(Debug, Clone)]
pub(crate) struct OldestUnfinishedEventTracker {
    metrics: Arc<NodeMetricSet>,
    inner: Arc<Mutex<OldestUnfinishedEventTrackerInner>>,
}

#[derive(Debug, Default)]
struct OldestUnfinishedEventTrackerInner {
    unfinished_events: BTreeMap<u64, UnfinishedEventInfo>,
    current_oldest_event_type: Option<&'static str>,
    age_refresh_task_running: bool,
}

#[derive(Debug)]
struct UnfinishedEventInfo {
    event_type: &'static str,
    started_at: Instant,
}

impl OldestUnfinishedEventTracker {
    pub fn new(metrics: Arc<NodeMetricSet>) -> Self {
        metrics.reset_oldest_unfinished_event_metrics();
        Self {
            metrics,
            inner: Arc::new(Mutex::new(OldestUnfinishedEventTrackerInner::default())),
        }
    }

    pub fn track_event(&self, index: u64, event_type: &'static str) {
        let mut inner = self
            .inner
            .lock()
            .expect("unfinished event tracker mutex should not be poisoned");
        let previous = inner.unfinished_events.insert(
            index,
            UnfinishedEventInfo {
                event_type,
                started_at: Instant::now(),
            },
        );
        debug_assert!(
            previous.is_none(),
            "event index should only be tracked once: {}",
            index
        );
        self.update_metrics_locked(&mut inner);

        if !inner.age_refresh_task_running {
            inner.age_refresh_task_running = true;
            let tracker = self.clone();
            tokio::spawn(async move {
                tracker.refresh_age_metrics().await;
            });
        }
    }

    pub fn finish_event(&self, index: u64) {
        let mut inner = self
            .inner
            .lock()
            .expect("unfinished event tracker mutex should not be poisoned");
        inner.unfinished_events.remove(&index);
        self.update_metrics_locked(&mut inner);
    }

    fn update_metrics_locked(&self, inner: &mut OldestUnfinishedEventTrackerInner) {
        let oldest_event = inner
            .unfinished_events
            .first_key_value()
            .map(|(index, info)| {
                (
                    *index,
                    info.event_type,
                    info.started_at.elapsed().as_secs_f64(),
                )
            });

        match oldest_event {
            Some((index, event_type, age_seconds)) => {
                self.metrics.set_oldest_unfinished_event_index(
                    index.try_into().expect("event index fits in i64"),
                );
                self.metrics
                    .set_oldest_unfinished_event_age_seconds(age_seconds);
                if inner.current_oldest_event_type != Some(event_type) {
                    if let Some(previous_event_type) =
                        inner.current_oldest_event_type.replace(event_type)
                    {
                        self.metrics
                            .set_oldest_unfinished_event_type(previous_event_type, false);
                    }
                    self.metrics
                        .set_oldest_unfinished_event_type(event_type, true);
                }
            }
            None => {
                self.metrics.reset_oldest_unfinished_event_metrics();
                if let Some(previous_event_type) = inner.current_oldest_event_type.take() {
                    self.metrics
                        .set_oldest_unfinished_event_type(previous_event_type, false);
                }
            }
        }
    }

    async fn refresh_age_metrics(self) {
        loop {
            tokio::time::sleep(OLDEST_UNFINISHED_EVENT_AGE_UPDATE_INTERVAL).await;
            let mut inner = self
                .inner
                .lock()
                .expect("unfinished event tracker mutex should not be poisoned");
            if inner.unfinished_events.is_empty() {
                inner.age_refresh_task_running = false;
                return;
            }
            self.update_metrics_locked(&mut inner);
        }
    }
}

/// Represents a Walrus event and the obligation to completely process that event.
///
/// A function that obtains an `EventHandle` must do one of the following:
///
/// 1. Completely handle the event and use the handle to mark it as complete.
/// 2. Pass the `EventHandle` on to a different function, which takes over responsibility to handle
///    the event.
/// 3. Return the `EventHandle` to the caller and let them retry/finish handling the event.
/// 4. If none of the above are possible, the event can never be processed, so the node should
///    panic. This can happen through a direct panic or by returning an error that is then
///    propagated through the call stack.
///
/// The `EventHandle` should not be dropped before calling `is_marked_as_complete`; otherwise, it
/// logs an error when it is dropped.
#[must_use]
// Important: Don't derive or implement `Clone` or `Copy`; every event should have a single handle
// that is passed around until it is completely handled or the thread panics.
pub(crate) struct EventHandle {
    index: u64,
    event_id: EventID,
    event_type: &'static str,
    node: Arc<StorageNodeInner>,
    can_be_dropped: bool,
}

impl EventHandle {
    pub fn new(
        index: u64,
        event_id: Option<EventID>,
        event_type: &'static str,
        node: Arc<StorageNodeInner>,
    ) -> Self {
        node.unfinished_event_tracker.track_event(index, event_type);
        Self {
            index,
            event_id: event_id.unwrap_or(EVENT_ID_FOR_CHECKPOINT_EVENTS),
            event_type,
            node,
            can_be_dropped: false,
        }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn event_id(&self) -> EventID {
        self.event_id
    }

    fn mark_as_complete(mut self) {
        tracing::trace!(
            index = self.index,
            event_id = ?self.event_id,
            "marking event as complete",
        );
        let EventProgress {
            persisted,
            pending,
            highest_finished_event_index,
        } = self
            .node
            .storage
            .maybe_advance_event_cursor(self.index, &self.event_id)
            .expect("DB operations should succeed");

        let event_cursor_progress = &self.node.metrics.event_cursor_progress;
        walrus_utils::with_label!(event_cursor_progress, STATUS_PERSISTED).set(persisted);
        walrus_utils::with_label!(event_cursor_progress, STATUS_PENDING).set(pending);
        walrus_utils::with_label!(event_cursor_progress, STATUS_HIGHEST_FINISHED)
            .set(highest_finished_event_index);
        self.node.unfinished_event_tracker.finish_event(self.index);
        self.can_be_dropped = true;
    }
}

impl Drop for EventHandle {
    #[tracing::instrument(level = Level::ERROR)]
    fn drop(&mut self) {
        if self.can_be_dropped {
            return;
        }

        match () {
            _ if thread::panicking() => {
                self.node.unfinished_event_tracker.finish_event(self.index);
                tracing::debug!("event handle dropped during panic",);
            }
            _ if self.node.is_shutting_down() => {
                self.node.unfinished_event_tracker.finish_event(self.index);
                tracing::debug!("event handle dropped during shutdown",);
            }
            _ => {
                tracing::error!("event handle dropped before being marked as complete",);
                // Panic in tests (not in simtests) if an event handle is dropped.
                // TODO: Enable panics in simtests as well (#1232).
                #[cfg(not(msim))]
                debug_assert!(
                    self.can_be_dropped,
                    "event handle dropped before being marked as complete; \
                        event ID: {:?}, index: {}",
                    self.event_id, self.index,
                );
            }
        }
    }
}

impl Debug for EventHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventHandle")
            .field("index", &self.index)
            .field("event_id", &self.event_id)
            .field("event_type", &self.event_type)
            // Exclude `node` field.
            .field("can_be_dropped", &self.can_be_dropped)
            .finish()
    }
}

pub(super) trait CompletableHandle {
    /// This marks the handle as complete.
    fn mark_as_complete(self);
}

impl CompletableHandle for EventHandle {
    fn mark_as_complete(self) {
        self.mark_as_complete();
    }
}

impl CompletableHandle for Option<EventHandle> {
    fn mark_as_complete(self) {
        if let Some(handle) = self {
            handle.mark_as_complete();
        }
    }
}

/// A provider of system events to a storage node.
#[async_trait]
pub trait SystemEventProvider: std::fmt::Debug + Sync + Send {
    /// Returns a new stream over [`PositionedStreamEvent`]s starting from those
    /// specified by `from`.
    async fn events(
        &self,
        from: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>, Error>;

    /// Returns the state that is required to initialize the event blob writer and storage node's
    /// event tracking system when requesting an event stream starting from event cursor `from` via
    /// [`Self::events`].
    ///
    /// The returned value is `None` if the event stream starts from `from` (i.e., no special
    /// initialization is required). Otherwise, the returned value is `Some(init_state)`, and the
    /// event stream starts from an event cursor `actual_from` that is different from `from` (and
    /// `actual_from` > `from`). This happens when the local event DB is initialized via event blobs
    /// and older event blobs are expired (i.e., the first event in the local event DB is not the
    /// first event that was emitted by the system contract and its event index > 0) but upon first
    /// time initialization of storage node, the event stream is requested to start from event
    /// cursor 0.
    async fn init_state(&self, from: EventStreamCursor)
    -> Result<Option<InitState>, anyhow::Error>;

    /// Returns a reference to this provider as a [`EventProcessor`].
    fn as_event_processor(&self) -> Option<&EventProcessor>;
}

/// A manager for event retention. This is used to drop events that are no longer needed.
#[async_trait]
pub trait EventRetentionManager: std::fmt::Debug + Sync + Send {
    /// Remove events before the specified cursor.
    async fn drop_events_before(&self, cursor: EventStreamCursor) -> Result<(), anyhow::Error>;
}

/// A manager for system events. This is used to start the event manager.
#[async_trait]
pub trait EventManager: SystemEventProvider + EventRetentionManager {
    /// Get the latest checkpoint sequence number.
    fn latest_checkpoint_sequence_number(&self) -> Option<u64>;
}

#[async_trait]
impl SystemEventProvider for EventProcessor {
    /// Returns a new stream over [`PositionedStreamEvent`]s starting from those specified by
    /// `from`.
    ///
    /// This expects a contiguous range of events present in the events store starting at the
    /// provided `cursor`.
    ///
    /// # Panics
    ///
    /// This function panics if the event store contains a gap in the event sequence.
    async fn events<'life0>(
        &'life0 self,
        cursor: EventStreamCursor,
    ) -> anyhow::Result<
        Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>,
        anyhow::Error,
    > {
        let mut interval = tokio::time::interval(self.event_polling_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let event_stream = stream::unfold(
            (interval, cursor.element_index),
            move |(mut interval, element_index)| async move {
                interval.tick().await;
                let events = self
                    .poll(element_index)
                    .inspect_err(|error| tracing::error!(?error, "failed to poll event stream"))
                    .ok()?;

                // Update the index such that the next future continues the sequence.
                let n_events = u64::try_from(events.len()).expect("number of events is within u64");
                let next_element_index = element_index + n_events;
                if n_events > 0 {
                    let last_event_index = events
                        .last()
                        .expect("we just checked that events is not empty")
                        .index;
                    assert!(
                        last_event_index == next_element_index - 1,
                        "event index inconsistency in event store",
                    );
                };
                Some((
                    stream::iter(events.into_iter().map(|e| e.element)),
                    (interval, next_element_index),
                ))
            },
        )
        .flatten();
        Ok(Box::new(event_stream))
    }

    async fn init_state(&self, cursor: EventStreamCursor) -> Result<Option<InitState>, Error> {
        let interval = tokio::time::interval(self.event_polling_interval);
        let stream = IntervalStream::new(interval)
            .then(move |_| ready(self.poll_next(cursor.element_index)))
            .filter_map(|res| async move { res.ok().flatten() })
            .map(|element| element.init_state);

        let mut pinned_stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(stream);
        Ok(pinned_stream.next().await.flatten())
    }

    fn as_event_processor(&self) -> Option<&EventProcessor> {
        Some(self)
    }
}

#[async_trait]
impl EventRetentionManager for EventProcessor {
    async fn drop_events_before(
        &self,
        cursor: EventStreamCursor,
    ) -> anyhow::Result<(), anyhow::Error> {
        *self.event_store_commit_index.lock().await = cursor.element_index;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use walrus_utils::metrics::Registry;

    use super::OldestUnfinishedEventTracker;
    use crate::node::metrics::NodeMetricSet;

    #[tokio::test]
    async fn oldest_unfinished_metrics_follow_oldest_outstanding_event() {
        let metrics = Arc::new(NodeMetricSet::new(&Registry::default()));
        let tracker = OldestUnfinishedEventTracker::new(metrics.clone());

        assert_eq!(metrics.oldest_unfinished_event_index(), -1);
        assert_eq!(metrics.oldest_unfinished_event_age_seconds(), 0.0);

        tracker.track_event(12, "epoch-change-start");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(metrics.oldest_unfinished_event_index(), 12);
        assert!(metrics.oldest_unfinished_event_age_seconds() > 0.0);
        assert_eq!(
            metrics.oldest_unfinished_event_type("epoch-change-start"),
            1
        );

        tracker.track_event(20, "certified");
        assert_eq!(metrics.oldest_unfinished_event_index(), 12);
        assert_eq!(
            metrics.oldest_unfinished_event_type("epoch-change-start"),
            1
        );
        assert_eq!(metrics.oldest_unfinished_event_type("certified"), 0);

        tracker.finish_event(12);
        assert_eq!(metrics.oldest_unfinished_event_index(), 20);
        assert_eq!(
            metrics.oldest_unfinished_event_type("epoch-change-start"),
            0
        );
        assert_eq!(metrics.oldest_unfinished_event_type("certified"), 1);

        tracker.finish_event(20);
        assert_eq!(metrics.oldest_unfinished_event_index(), -1);
        assert_eq!(metrics.oldest_unfinished_event_age_seconds(), 0.0);
        assert_eq!(metrics.oldest_unfinished_event_type("certified"), 0);
    }
}

#[async_trait]
impl EventManager for EventProcessor {
    fn latest_checkpoint_sequence_number(&self) -> Option<u64> {
        self.get_latest_checkpoint_sequence_number()
    }
}

#[async_trait]
impl SystemEventProvider for Arc<EventProcessor> {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>, Error> {
        self.as_ref().events(cursor).await
    }

    async fn init_state(
        &self,
        from: EventStreamCursor,
    ) -> Result<Option<InitState>, anyhow::Error> {
        self.as_ref().init_state(from).await
    }

    fn as_event_processor(&self) -> Option<&EventProcessor> {
        Some(self.as_ref())
    }
}

#[async_trait]
impl EventRetentionManager for Arc<EventProcessor> {
    async fn drop_events_before(&self, cursor: EventStreamCursor) -> Result<(), Error> {
        self.as_ref().drop_events_before(cursor).await
    }
}

#[async_trait]
impl EventManager for Arc<EventProcessor> {
    fn latest_checkpoint_sequence_number(&self) -> Option<u64> {
        self.as_ref().get_latest_checkpoint_sequence_number()
    }
}
