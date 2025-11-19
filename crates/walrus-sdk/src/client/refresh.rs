// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! A cache for the active committee and the price computation, that refreshed them periodically
//! when needed.

use std::{
    num::NonZeroU16,
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::Level;
use walrus_sui::{client::ReadClient, types::move_structs::EpochState};

use super::resource::PriceComputation;
use crate::{active_committees::ActiveCommittees, config::CommitteesRefreshConfig};

pub(crate) type CommitteesRequestChannel =
    oneshot::Sender<(Arc<ActiveCommittees>, PriceComputation)>;

/// The kind of request that the client can request.
#[derive(Debug, Clone)]
pub enum RequestKind {
    /// Request the current committees and the price computation.
    Get,
    /// Get the current committees and the price computation, and refresh the cache.
    Refresh,
}

/// The request that the client can make to the refresher.
#[derive(Debug)]
pub struct CommitteesRequest {
    /// The kind of request.
    kind: RequestKind,
    /// The reply channel.
    tx: CommitteesRequestChannel,
}

impl CommitteesRequest {
    /// Returns the reply oneshot channel.
    pub fn into_reply_channel(self) -> CommitteesRequestChannel {
        self.tx
    }

    /// Returns true if the request is of kind `Refresh`.
    pub fn is_refresh(&self) -> bool {
        matches!(self.kind, RequestKind::Refresh)
    }
}

/// An actor that caches the active committees and the price computation data, and refreshes them
/// periodically or on demand.
#[derive(Debug)]
pub struct CommitteesRefresher<T> {
    last_refresh: Instant,
    last_committees: Arc<ActiveCommittees>,
    last_price_computation: PriceComputation,
    // The `epoch_state` is used to compute when the next epoch will likely start.
    epoch_state: EpochState,
    // The `epoch_duration` is used to compute when the next epoch will likely start.
    // NOTE: `epoch_duration` is set at creation, and never refreshed, since it cannot be changed.
    epoch_duration: Duration,
    notify: Arc<Notify>,
    sui_client: T,
    req_rx: mpsc::Receiver<CommitteesRequest>,
    config: CommitteesRefreshConfig,
}

impl<T: ReadClient> CommitteesRefresher<T> {
    /// Creates a new refresher cache.
    #[tracing::instrument(name = "CommitteesRefresher::new", skip_all, level = Level::DEBUG)]
    pub async fn new(
        config: CommitteesRefreshConfig,
        sui_client: T,
        req_rx: mpsc::Receiver<CommitteesRequest>,
        notify: Arc<Notify>,
    ) -> Result<Self> {
        let (committees, last_price_computation, epoch_state) =
            Self::get_latest(&sui_client).await?;
        // Get the epoch duration, this time only.
        let epoch_duration = sui_client.fixed_system_parameters().epoch_duration;

        Ok(Self {
            config,
            last_refresh: Instant::now(),
            last_committees: Arc::new(committees),
            epoch_state,
            epoch_duration,
            last_price_computation,
            notify,
            sui_client,
            req_rx,
        })
    }

    #[tracing::instrument(skip_all)]
    async fn run_refresh_once(&mut self) -> ControlFlow<()> {
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
                // `last_refresh` instant is not updated, so that if a running store
                // operation detects epoch change it can still force the refresh.
                tracing::debug!(
                    ?timer_interval,
                    "auto-refreshing the active committee"
                );
                let _ = self.refresh().await.inspect_err(|error| {
                    tracing::warn!(
                        %error,
                        "failed to refresh the active committee; \
                        retrying again at the next interval",
                    )
                });
            }
            request = self.req_rx.recv() => {
                if let Some(request) = request {
                    tracing::debug!(?request, "received a request");
                    if request.is_refresh() {
                        let _ = self.refresh_if_stale().await.inspect_err(|error| {
                            tracing::warn!(
                                %error,
                                "failed to refresh the active committee; \
                                retrying again at the next interval",
                            )
                        });
                    }
                    let _ = request
                        .into_reply_channel()
                        .send((
                            self.last_committees.clone(),
                            self.last_price_computation,
                        ))
                        .inspect_err(|_| {
                            // This may happen because the client was notified of a committee
                            // change, and therefore the receiver end of the channel was
                            // dropped.
                            tracing::info!("failed to send the committee and price")
                        });
                } else {
                    tracing::debug!("the channel is closed, stopping the refresher");
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// Runs the refresher cache.
    pub async fn run(&mut self) {
        loop {
            if self.run_refresh_once().await.is_break() {
                break;
            }
        }
    }

    /// Refreshes the data in the cache if the last refresh is older than the refresh interval.
    pub async fn refresh_if_stale(&mut self) -> Result<()> {
        if self.last_refresh.elapsed() > self.config.refresh_grace_period {
            tracing::debug!(
                elapsed = ?self.last_refresh.elapsed(),
                "the active committee is stale, refreshing"
            );
            self.refresh().await?;
            self.last_refresh = Instant::now();
        } else {
            tracing::trace!(
                elapsed = ?self.last_refresh.elapsed(),
                "the active committee was force-refreshed recently, skipping refresh"
            );
        }
        Ok(())
    }

    /// Refreshes the data in the cache and _notifies_ the clients if the committee has changed.
    ///
    /// This function does _not_ update the last refresh time.
    async fn refresh(&mut self) -> Result<()> {
        tracing::debug!("getting the latest active committee and price computation from chain");
        let (committees, price_computation, epoch_state) =
            Self::get_latest(&self.sui_client).await?;

        // First update, then notify if needed.
        let are_different =
            are_current_previous_different(&committees, self.last_committees.as_ref());
        self.last_committees = Arc::new(committees);
        self.last_price_computation = price_computation;
        self.epoch_state = epoch_state;

        // If the committee has changed, send a notification to the clients.
        if are_different {
            tracing::debug!("the active committees have changed, notifying the clients");
            self.notify.notify_waiters();
        } else {
            tracing::trace!("the active committee has not changed");
        }

        Ok(())
    }

    /// Gets the latest active committees, price computation, and epoch state from the Sui client.
    async fn get_latest(
        sui_client: &T,
    ) -> Result<(ActiveCommittees, PriceComputation, EpochState)> {
        tracing::debug!("getting the latest active committees and price computation from chain");
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
        self.epoch_state
            .start_of_current_epoch()
            .unwrap_or_else(|| Utc::now())
            + self.epoch_duration
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
        if self.time_to_next_epoch() > self.config.epoch_change_distance_threshold {
            self.config.max_auto_refresh_interval
        } else {
            self.config.min_auto_refresh_interval
        }
    }
}

/// An error that occurs when communicating with the refresher.
#[derive(Debug, thiserror::Error)]
pub enum RefresherCommunicationError {
    /// An error occurred while sending a request to the refresher.
    #[error("error communicating with the refresher: {0}")]
    Send(#[from] mpsc::error::SendError<CommitteesRequest>),
    /// An error occurred while sending a reply to the client.
    #[error("error receiving from the refresher: {0}")]
    Receive(#[from] oneshot::error::RecvError),
}

/// A handle to communicate with the [`CommitteesRefresher`].
#[derive(Debug, Clone)]
pub struct CommitteesRefresherHandle {
    notify: Arc<Notify>,
    req_tx: mpsc::Sender<CommitteesRequest>,
    n_shards: NonZeroU16,
}

impl CommitteesRefresherHandle {
    /// Creates a new handle to communicate with the refresher.
    pub fn new(
        notify: Arc<Notify>,
        req_tx: mpsc::Sender<CommitteesRequest>,
        n_shards: NonZeroU16,
    ) -> Self {
        Self {
            notify,
            req_tx,
            n_shards,
        }
    }

    /// Sends a request to the refresher to refresh and get the active committees and the price
    /// computation.
    pub async fn send_committees_and_price_request(
        &self,
        kind: RequestKind,
    ) -> Result<(Arc<ActiveCommittees>, PriceComputation), RefresherCommunicationError> {
        tracing::debug!(?kind, "sending a request to the refresher");
        let (tx, rx) = oneshot::channel();
        self.req_tx.send(CommitteesRequest { kind, tx }).await?;
        let (committees, price_computation) = rx.await?;
        tracing::debug!("received the response from the refresher");
        Ok((committees, price_computation))
    }

    /// Awaits for a notification from the refresher that the active committee has changed.
    pub async fn change_notified(&self) {
        self.notify.notified().await
    }

    /// Returns the configured number of shards for the Walrus network.
    pub fn n_shards(&self) -> NonZeroU16 {
        self.n_shards
    }
}

/// Checks if two committees are different enough to require a notification to the clients.
pub fn are_current_previous_different(first: &ActiveCommittees, second: &ActiveCommittees) -> bool {
    // Compare the current committees.
    if let Err(error) = first
        .current_committee()
        .compare_functional_equivalence(second.current_committee())
    {
        tracing::debug!(differences = %error, "current committees are different");
        return true;
    }

    // Compare the previous committees, if present.
    let previous_comparison = match (first.previous_committee(), second.previous_committee()) {
        (Some(first_previous), Some(second_previous)) => {
            first_previous.compare_functional_equivalence(second_previous)
        }
        (None, None) => Ok(()),
        _ => Err(anyhow::anyhow!(
            "one of the two sets has a previous committee, the other does not"
        )),
    };
    if let Err(error) = previous_comparison {
        tracing::debug!(differences = %error, "previous committees are different");
        return true;
    }

    false
}
