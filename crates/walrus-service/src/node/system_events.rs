// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! System events observed by the storage node.

use std::{sync::Arc, time::Duration};

use anyhow::Error;
use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream;
use tokio::time::MissedTickBehavior;
use tokio_stream::Stream;
use walrus_event::{
    event_processor::EventProcessor,
    EventSequenceNumber,
    EventStreamCursor,
    IndexedStreamElement,
};
use walrus_sui::client::{ReadClient, SuiReadClient};

use super::config::SuiConfig;

/// The capacity of the event channel.
pub const EVENT_CHANNEL_CAPACITY: usize = 1024;

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
    /// Return a new stream over [`walrus_event::IndexedStreamElement`]s starting from those
    /// specified by `from`.
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>, anyhow::Error>;
}

/// A manager for event retention. This is used to drop events that are no longer needed.
#[async_trait]
pub trait EventRetentionManager: std::fmt::Debug + Sync + Send {
    /// Remove events before the specified cursor.
    async fn drop_events_before(&self, cursor: EventStreamCursor) -> Result<(), anyhow::Error>;
}

/// A manager for system events. This is used to start the event manager.
#[async_trait]
pub trait EventManager: SystemEventProvider + EventRetentionManager {}

#[async_trait]
impl SystemEventProvider for SuiSystemEventProvider {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>, anyhow::Error>
    {
        tracing::info!(?cursor, "resuming from event");
        let events = self
            .read_client
            .event_stream(self.polling_interval, cursor.event_id)
            .await?;
        let event_stream =
            events.map(|event| IndexedStreamElement::new(event, EventSequenceNumber::new(0, 0)));
        Ok(Box::new(event_stream))
    }
}

#[async_trait]
impl EventRetentionManager for SuiSystemEventProvider {
    async fn drop_events_before(&self, _cursor: EventStreamCursor) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
impl EventManager for SuiSystemEventProvider {}

#[async_trait]
impl SystemEventProvider for EventProcessor {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> anyhow::Result<
        Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>,
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
                    .await
                    .inspect_err(|error| tracing::error!(?error, "failed to poll event stream"))
                    .ok()?;
                // Update the index such that the next future continues the sequence.
                let n_events = u64::try_from(events.len()).expect("number of events is within u64");
                Some((stream::iter(events), (interval, element_index + n_events)))
            },
        )
        .flatten();
        Ok(Box::new(event_stream))
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
impl EventManager for EventProcessor {}

#[async_trait]
impl SystemEventProvider for Arc<EventProcessor> {
    async fn events(
        &self,
        cursor: EventStreamCursor,
    ) -> Result<Box<dyn Stream<Item = IndexedStreamElement> + Send + Sync + 'life0>, Error> {
        self.as_ref().events(cursor).await
    }
}

#[async_trait]
impl EventRetentionManager for Arc<EventProcessor> {
    async fn drop_events_before(&self, cursor: EventStreamCursor) -> Result<(), Error> {
        self.as_ref().drop_events_before(cursor).await
    }
}

#[async_trait]
impl EventManager for Arc<EventProcessor> {}
