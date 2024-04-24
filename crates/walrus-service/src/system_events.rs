// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! System events observed by the storage node.

use std::time::Duration;

use async_trait::async_trait;
use sui_types::event::EventID;
use tokio_stream::Stream;
use walrus_sui::{
    client::{ReadClient, SuiReadClient},
    types::BlobEvent,
};

use crate::config::SuiConfig;

/// A provider of system events to a storage node.
#[async_trait]
pub trait SystemEventProvider: std::fmt::Debug + Sync + Send {
    /// Return a new stream over [`BlobEvent`]s starting from those specified by `from`.
    async fn events(
        &self,
        cursor: Option<EventID>,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error>;
}

/// A [`SystemEventProvider`] that uses a [`SuiReadClient`] to fetch events.
#[derive(Debug)]
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
        let client =
            SuiReadClient::new_for_rpc(&config.rpc, config.pkg_id, config.system_object).await?;
        Ok(Self::new(client, config.event_polling_interval))
    }
}

#[async_trait]
impl SystemEventProvider for SuiSystemEventProvider {
    async fn events(
        &self,
        cursor: Option<EventID>,
    ) -> Result<Box<dyn Stream<Item = BlobEvent> + Send + Sync + 'life0>, anyhow::Error> {
        tracing::info!("resuming from event: {cursor:?}");

        let events = self
            .read_client
            .blob_events(self.polling_interval, cursor)
            .await?;

        Ok(Box::new(events))
    }
}
