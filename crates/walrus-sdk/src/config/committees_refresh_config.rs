// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use tokio::sync::{Notify, mpsc};
use walrus_sui::client::ReadClient;

use crate::client::refresh::{CommitteesRefresher, CommitteesRefresherHandle};

/// The configuration for the committees refresher.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct CommitteesRefreshConfig {
    /// The interval after a new refresh can be requested by the client.
    ///
    /// If multiple refreshes are issued within `REFRESH_GRACE_PERIOD` of each other, only the first
    /// one is executed.
    #[serde(rename = "refresh_grace_period_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub refresh_grace_period: Duration,
    /// The maximum interval after which the cache is force-refreshed automatically.
    #[serde(rename = "max_auto_refresh_interval_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub max_auto_refresh_interval: Duration,
    /// The minimum interval after which the cache is force-refreshed automatically.
    #[serde(rename = "min_auto_refresh_interval_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub min_auto_refresh_interval: Duration,
    /// A threshold of time from the expected epoch change, after which the auto-refresh interval
    /// switches from max to min.
    #[serde(rename = "epoch_change_distance_threshold_secs")]
    #[serde_as(as = "DurationSeconds")]
    pub epoch_change_distance_threshold: Duration,
    /// The size of the refresher channel.
    pub refresher_channel_size: usize,
}

impl CommitteesRefreshConfig {
    /// Builds a new [`CommitteesRefresher`], spawns it on a separate task, and
    /// returns the [`CommitteesRefresherHandle`].
    pub async fn build_refresher_and_run(
        &self,
        sui_client: impl ReadClient + 'static,
    ) -> Result<CommitteesRefresherHandle> {
        let (mut refresher, handle) = build_refresher_and_handle(sui_client, self.clone()).await?;

        tokio::spawn(async move {
            refresher.run().await;
        });

        Ok(handle)
    }
}

#[tracing::instrument(skip_all)]
async fn build_refresher_and_handle(
    sui_client: impl ReadClient,
    committees_refresh_config: CommitteesRefreshConfig,
) -> Result<(
    CommitteesRefresher<impl ReadClient>,
    CommitteesRefresherHandle,
)> {
    let notify = Arc::new(Notify::new());
    let (req_tx, req_rx) = mpsc::channel(committees_refresh_config.refresher_channel_size);

    let refresher = CommitteesRefresher::new(
        committees_refresh_config,
        sui_client,
        req_rx,
        notify.clone(),
    )
    .await?;

    Ok((refresher, CommitteesRefresherHandle::new(notify, req_tx)))
}

impl Default for CommitteesRefreshConfig {
    fn default() -> Self {
        Self {
            refresh_grace_period: default::REFRESH_GRACE_PERIOD,
            max_auto_refresh_interval: default::MAX_AUTO_REFRESH_INTERVAL,
            min_auto_refresh_interval: default::MIN_AUTO_REFRESH_INTERVAL,
            epoch_change_distance_threshold: default::EPOCH_CHANGE_DISTANCE_THRS,
            refresher_channel_size: default::REFRESHER_CHANNEL_SIZE,
        }
    }
}

mod default {
    use std::time::Duration;

    pub(crate) const REFRESH_GRACE_PERIOD: Duration = Duration::from_secs(10);
    pub(crate) const MAX_AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
    pub(crate) const MIN_AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
    pub(crate) const EPOCH_CHANGE_DISTANCE_THRS: Duration = Duration::from_mins(5);
    pub const REFRESHER_CHANNEL_SIZE: usize = 100;
}
