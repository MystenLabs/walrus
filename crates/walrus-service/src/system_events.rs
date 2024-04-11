// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! System events observed by the storage node.

use std::time::Duration;

use async_trait::async_trait;
use sui_sdk::SuiClientBuilder;
use sui_types::event::EventID;
use tokio_stream::{Stream, StreamExt};
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::BlobEvent,
};

use crate::config::SuiConfig;

/// Set of cursors indicating the starting point for events returned by a [`SystemEventProvider`].
#[derive(Debug, Copy, Clone)]
pub struct SystemEventCursorSet {
    /// The cursor for [`BlobRegistered`][walrus_sui::types::BlobRegistered] events.
    pub registered: Option<EventID>,
    /// The cursor for [`BlobCertified`][walrus_sui::types::BlobCertified] events.
    pub certified: Option<EventID>,
}

/// A provider of system events to a storage node.
#[async_trait]
pub trait SystemEventProvider: std::fmt::Debug + Sync + Send {
    /// Return a new stream over [`BlobEvent`]s starting from those specified by `from`.
    async fn events(
        &self,
        cursors: SystemEventCursorSet,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error>;
}

/// A [`SystemEventProvider`] that uses a [`SuiReadClient`] to fetch events.
#[derive(Debug)]
pub struct SuiSystemEventProvider {
    read_client: SuiReadClient,
    polling_interval: Duration,
}

impl SuiSystemEventProvider {
    /// Creates a new provider with for a [`SuiReadClient`] constructed from the config.
    pub async fn new(config: &SuiConfig) -> Result<Self, anyhow::Error> {
        let client = SuiClientBuilder::default().build(&config.rpc).await?;
        let read_client = SuiReadClient::new(client, config.pkg_id, config.system_object).await?;

        Ok(Self {
            read_client,
            polling_interval: config.event_polling_interval,
        })
    }
}

#[async_trait]
impl SystemEventProvider for SuiSystemEventProvider {
    async fn events(
        &self,
        cursors: SystemEventCursorSet,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error> {
        tracing::info!("resuming from events: {cursors:?}");

        let registered_events = self
            .read_client
            .blob_registered_events(self.polling_interval, cursors.registered)
            .await?
            .map(BlobEvent::from);

        let certified_events = self
            .read_client
            .blob_registered_events(self.polling_interval, cursors.certified)
            .await?
            .map(BlobEvent::from);

        Ok(Box::new(registered_events.merge(certified_events)))
    }
}
