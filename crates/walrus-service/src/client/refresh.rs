// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A cache for the active committee and the price computation, that refreshed them periodically
//! when needed.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot, Notify};
use walrus_sui::{client::ReadClient, types::move_structs::EpochState};

use crate::{client::resource::PriceComputation, common::active_committees::ActiveCommittees};

pub(crate) type CommitteesRequest = oneshot::Sender<(Arc<ActiveCommittees>, PriceComputation)>;

// TODO(giac): make configurable.
/// The interval after which the cache is considered stale.
pub(crate) const DEFAULT_CACHE_VALIDITY: Duration = Duration::from_secs(30);
/// The maximum interval after which the cache is force-refreshed automatically.
pub(crate) const MAX_AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// The minimum interval after which the cache is force-refreshed automatically.
pub(crate) const MIN_AUTO_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// A threshold of time from the expected epoch change, after which the auto-refresh interval
/// switches from max to min.
pub(crate) const EPOCH_CHANGE_DISTANCE_THRS: Duration = Duration::from_secs(300);

/// The kind of refresh that the client can request.
#[derive(Debug)]
pub enum RefreshKind {
    /// A soft refresh, that is executed only if the cache is stale.
    Soft(CommitteesRequest),
    /// A hard refresh, that is executed even if a soft refresh was done recently.
    Hard(CommitteesRequest),
}

impl RefreshKind {
    /// Returns the reply oneshot channel.
    pub fn into_reply_channel(self) -> CommitteesRequest {
        match self {
            RefreshKind::Soft(req_tx) | RefreshKind::Hard(req_tx) => req_tx,
        }
    }

    /// Returns `true` if the refresh kind is hard.
    pub fn is_hard(&self) -> bool {
        matches!(self, RefreshKind::Hard(_))
    }
}

/// An actor that caches the active committees and the price computation data, and refreshes them
/// periodically if needed.
pub(crate) struct CommitteeRefresher<T> {
    cache_validity: Duration,
    last_refresh: Instant,
    last_hard_refresh: Instant,
    last_committees: Arc<ActiveCommittees>,
    last_price_computation: PriceComputation,
    // The `epoch_state` is used to compute when the next epoch will likely start.
    epoch_state: EpochState,
    // The `epoch_duration` is used to compute when the next epoch will likely start.
    // NOTE: `epoch_duration` is set at creation, and never refreshed, since it cannot be changed.
    epoch_duration: Duration,
    notify: Arc<Notify>,
    sui_client: T,
    req_rx: mpsc::Receiver<RefreshKind>,
}

impl<T: ReadClient> CommitteeRefresher<T> {
    pub async fn new(
        refresh_interval: Duration,
        sui_client: T,
        req_rx: mpsc::Receiver<RefreshKind>,
        notify: Arc<Notify>,
    ) -> Result<Self> {
        let (committees, last_price_computation, epoch_state) =
            Self::get_latest(&sui_client).await?;
        // Get the epoch duration, this time only only.
        let epoch_duration = sui_client.fixed_system_parameters().await?.epoch_duration;

        Ok(Self {
            cache_validity: refresh_interval,
            last_refresh: Instant::now(),
            last_hard_refresh: Instant::now(),
            last_committees: Arc::new(committees),
            epoch_state,
            epoch_duration,
            last_price_computation,
            notify,
            sui_client,
            req_rx,
        })
    }

    /// Runs the refresher cache.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let timer_interval = self.next_refresh_interval();
            tokio::select! {
                _ = tokio::time::sleep(timer_interval) => {
                    // Refresh automatically.
                    // This is a safeguard against the case where only very long operations occur
                    // during epoch change. When close to the next epoch change, the refresh timer
                    // goes down to `MIN_AUTO_REFRESH_INTERVAL`, ensuring that when epoch change
                    // occurs it is detected and the operations are notified.
                    //
                    // This is a force refresh (ignores cache staleness). However, the
                    // `last_hard_refresh` instant is not updated, so that if a running store
                    // operation detects epoch change it can still force the refresh.
                    tracing::debug!(
                        ?timer_interval,
                        "auto-refreshing the active committee"
                    );
                    self.refresh().await?;
                    self.last_refresh = Instant::now();
                }
                refresh = self.req_rx.recv() => {
                    if let Some(refresh) = refresh {
                    tracing::trace!(
                        is_hard = refresh.is_hard(),
                        "received a refresh request"
                    );
                    self.refresh_if_stale(refresh.is_hard()).await?;
                    let _ = refresh
                        .into_reply_channel()
                        .send((
                            self.last_committees.clone(),
                            self.last_price_computation.clone(),
                        ))
                        .inspect_err(|_| {
                            // This may happen because the client was notified of a committee
                            // change, and therefore the receiver end of the channel was dropped.
                            tracing::info!("failed to send the refreshed committee and price")
                        });
                    } else {
                        tracing::info!("the refresh channel is closed, stopping the refresher");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Refreshes the data in the cache if the last refresh is older than the refresh interval.
    pub async fn refresh_if_stale(&mut self, is_hard: bool) -> Result<()> {
        if self.last_refresh.elapsed() > self.cache_validity {
            tracing::debug!(
                elapsed = ?self.last_refresh.elapsed(),
                "the active committee is stale, refreshing"
            );
            self.refresh().await?;
            self.last_refresh = Instant::now();
        } else if is_hard && self.last_hard_refresh.elapsed() > self.cache_validity {
            tracing::debug!(
                elapsed = ?self.last_hard_refresh.elapsed(),
                "the active committee is forced to refresh, refreshing"
            );
            self.refresh().await?;
            self.last_hard_refresh = Instant::now();
            self.last_refresh = Instant::now();
        } else {
            tracing::trace!(
                elapsed = ?self.last_refresh.elapsed(),
                "the active committee is fresh, skipping refresh"
            );
        }
        Ok(())
    }

    /// Refreshes the data in the cache and _notifies_ the clients if the committee has changed.
    ///
    /// This function does _not_ update the last refresh time.
    pub async fn refresh(&mut self) -> Result<()> {
        tracing::debug!("getting the latest active committee and price computation from chain");
        let (committees, price_computation, epoch_state) =
            Self::get_latest(&self.sui_client).await?;

        // If the committee has changed, send a notification to the clients.
        if are_committees_different(&committees, self.last_committees.as_ref()) {
            tracing::info!("the active committee has changed, notifying the clients");
            self.notify.notify_waiters();
        } else {
            tracing::trace!("the active committee has not changed");
        }

        self.last_committees = Arc::new(committees);
        self.last_price_computation = price_computation;
        self.epoch_state = epoch_state;
        Ok(())
    }

    /// Gets the latest active committees, price computation, and epoch state from the Sui client.
    async fn get_latest(
        sui_client: &T,
    ) -> Result<(ActiveCommittees, PriceComputation, EpochState)> {
        let committees_and_state = sui_client.get_committees_and_state().await?;
        let epoch_state = committees_and_state.epoch_state.clone();
        let committees = ActiveCommittees::from_committees_and_state(committees_and_state);

        let (storage_price, write_price) =
            sui_client.storage_and_write_price_per_unit_size().await?;
        let price_computation = PriceComputation::new(storage_price, write_price);
        Ok((committees, price_computation, epoch_state))
    }

    /// Computes the start of the next epoch, based on current information.
    fn next_epoch_start(&self) -> DateTime<Utc> {
        let estimated_start_of_current_epoch = match self.epoch_state {
            EpochState::EpochChangeDone(epoch_start)
            | EpochState::NextParamsSelected(epoch_start) => epoch_start,
            EpochState::EpochChangeSync(_) => Utc::now(),
        };

        estimated_start_of_current_epoch + self.epoch_duration
    }

    /// Computes the time from now to the start of the next epoch.
    ///
    /// Returns `Duration::default()` (duration of `0`) if the subtraction overflows, i.e., if the
    /// estimated start of the next epoch is in the past.
    fn time_to_next_epoch(&self) -> Duration {
        self.next_epoch_start()
            .signed_duration_since(Utc::now())
            .to_std()
            .unwrap_or_default()
    }

    /// Returns the duration until the next refresh timer.
    ///
    /// The duration to the next timer is a function of the time to the next epoch. The refresh
    /// timer is:
    /// - `MAX_AUTO_REFRESH_INTERVAL` if the expected epoch change is more than
    ///   `EPOCH_CHANGE_DISTANCE_THRS` in the future.
    /// - `MIN_AUTO_REFRESH_INTERVAL` otherwise.
    fn next_refresh_interval(&self) -> Duration {
        if self.time_to_next_epoch() > EPOCH_CHANGE_DISTANCE_THRS {
            MAX_AUTO_REFRESH_INTERVAL
        } else {
            MIN_AUTO_REFRESH_INTERVAL
        }
    }
}

/// Checks if two committes are different enough to require a notification to the clients.
fn are_committees_different(first: &ActiveCommittees, second: &ActiveCommittees) -> bool {
    if first == second {
        // They are identical.
        return false;
    }

    if first.current_committee() == second.current_committee()
        && first.previous_committee() == second.previous_committee()
    {
        // The relevant committees for storing and reading are the same.
        return false;
    }

    true
}
