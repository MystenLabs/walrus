// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Walrus events observed by the storage node.

use std::{fmt::Debug, future::ready, pin::Pin, sync::Arc, thread, time::Duration};

use anyhow::Error;
use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream;
use sui_types::{digests::TransactionDigest, event::EventID};
use tokio::time::MissedTickBehavior;
use tokio_stream::{Stream, wrappers::IntervalStream};
use tracing::Level;
use walrus_sui::client::{ReadClient, SuiReadClient};

use super::{STATUS_PENDING, STATUS_PERSISTED, StorageNodeInner};
use crate::{
    common::config::SuiConfig,
    event::{
        event_processor::processor::EventProcessor,
        events::{CheckpointEventPosition, EventStreamCursor, InitState, PositionedStreamEvent},
    },
    node::{metrics::STATUS_HIGHEST_FINISHED, storage::EventProgress},
};

/// The capacity of the event channel.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// The event ID for checkpoint events.
pub const EVENT_ID_FOR_CHECKPOINT_EVENTS: EventID = EventID {
    tx_digest: TransactionDigest::ZERO,
    event_seq: 0,
};

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
pub(super) struct EventHandle {
    index: u64,
    event_id: EventID,
    node: Arc<StorageNodeInner>,
    can_be_dropped: bool,
}

impl EventHandle {
    pub fn new(index: u64, event_id: Option<EventID>, node: Arc<StorageNodeInner>) -> Self {
        Self {
            index,
            event_id: event_id.unwrap_or(EVENT_ID_FOR_CHECKPOINT_EVENTS),
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
                tracing::debug!("event handle dropped during panic",);
            }
            _ if self.node.is_shutting_down() => {
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

/// A [`SystemEventProvider`] that uses a [`SuiReadClient`] to fetch events.
#[derive(Debug, Clone)]
pub struct SuiSystemEventProvider {
    read_client: SuiReadClient,
    polling_interval: Duration,
}

impl SuiSystemEventProvider {
    /// Creates a new provider with the supplied [`SuiReadClient`], which polls
    /// for new events every polling_interval.
    pub fn new(read_client: SuiReadClient, polling_interval: Duration) -> Self {
        Self {
            read_client,
            polling_interval,
        }
    }

    /// Creates a new provider with for a [`SuiReadClient`] constructed from the config.
    pub async fn from_config(config: &SuiConfig) -> Result<Self, anyhow::Error> {
        Ok(Self::new(
            config.new_read_client().await?,
            config.event_polling_interval,
        ))
    }
}

/// A provider of system events to a storage node.
#[async_trait]
pub trait SystemEventProvider: std::fmt::Debug + Sync + Send {
    /// Return a new stream over [`PositionedStreamEvent`]s starting from those
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

    /// Return a reference to this provider as a [`EventProcessor`].
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
impl SystemEventProvider for SuiSystemEventProvider {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = PositionedStreamEvent> + Send + Sync + 'life0>, anyhow::Error>
    {
        tracing::info!(?cursor, "resuming from event");
        let events = self
            .read_client
            .event_stream(self.polling_interval, cursor.event_id)
            .await?;
        let event_stream = events
            .map(|event| PositionedStreamEvent::new(event, CheckpointEventPosition::new(0, 0)));
        Ok(Box::new(event_stream))
    }

    async fn init_state(
        &self,
        _from: EventStreamCursor,
    ) -> Result<Option<InitState>, anyhow::Error> {
        Ok(None)
    }

    fn as_event_processor(&self) -> Option<&EventProcessor> {
        None
    }
}

#[async_trait]
impl EventRetentionManager for SuiSystemEventProvider {
    async fn drop_events_before(&self, _cursor: EventStreamCursor) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
impl EventManager for SuiSystemEventProvider {
    fn latest_checkpoint_sequence_number(&self) -> Option<u64> {
        None
    }
}

#[async_trait]
impl SystemEventProvider for EventProcessor {
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
                tracing::info!(
                    "EventProcessor polled {} events starting from {}",
                    events.len(),
                    element_index
                );
                // Update the index such that the next future continues the sequence.
                let n_events = u64::try_from(events.len()).expect("number of events is within u64");
                Some((stream::iter(events), (interval, element_index + n_events)))
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
