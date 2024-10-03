// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Drive voting end and epoch change.

use std::{
    fmt::Formatter,
    future::Future,
    num::NonZero,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

use chrono::{DateTime, TimeDelta, Utc};
use futures::{future::BoxFuture, FutureExt};
use rand::{rngs::StdRng, Rng, SeedableRng};
use walrus_core::Epoch;
use walrus_sui::{
    client::FixedSystemParameters,
    types::{move_structs::EpochState, GENESIS_EPOCH},
};

use super::contract_service::SystemContractService;
use crate::utils::ExponentialBackoffState;

/// Maximum number of seconds that an operation can be randomly delayed by.
const MAX_SCHEDULE_JITTER: Duration = Duration::from_secs(180);
/// The minimum exponential backoff when performing retries.
const MIN_BACKOFF: Duration = Duration::from_secs(5);
/// The maximum exponential backoff when performing retries.
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// Function returning the current time in Utc.
type UtcNowFn = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Worker that drives epoch change by invoking Walrus contract calls.
// TODO(jsmith): Add metrics for this module
// - time that futures are scheduled for
// - whether or not a future is scheduled
pub(super) struct EpochChangeDriver {
    /// Future that processes scheduled calls.
    inner: Arc<Mutex<Pin<Box<EpochChangeDriverInner<'static>>>>>,
    /// The contract service used to interact with the chain.
    contract_service: Arc<dyn SystemContractService>,
    /// Epoch duration and the end time for the 0th epoch.
    system_parameters: FixedSystemParameters,
    /// Function used to get the current time in Utc.
    utc_now_fn: UtcNowFn,
}

impl EpochChangeDriver {
    /// Returns a new instance of the `EpochChangeDriver`.
    pub fn new(
        system_parameters: FixedSystemParameters,
        contract_service: Arc<dyn SystemContractService>,
        rng: StdRng,
    ) -> Self {
        Self::new_with_time_provider(system_parameters, contract_service, rng, Arc::new(Utc::now))
    }

    /// Returns a new instance of the `EpochChangeDriver` which uses `utc_now_fn` to get the current
    /// time when required.
    fn new_with_time_provider(
        system_parameters: FixedSystemParameters,
        contract_service: Arc<dyn SystemContractService>,
        rng: StdRng,
        utc_now_fn: UtcNowFn,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::pin(EpochChangeDriverInner {
                rng,
                waker: None,
                end_voting_future: None,
                epoch_change_start_future: None,
            }))),
            system_parameters,
            contract_service,
            utc_now_fn,
        }
    }

    /// Dispatch scheduled calls.
    ///
    /// This method never returns and instead continues to await the scheduling of
    /// new calls related to epoch change.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Multiple calls to this method will all drive the same set of
    /// scheduled operations.
    #[tracing::instrument(skip_all)]
    pub async fn run(&self) {
        let inner = self.inner.clone();
        std::future::poll_fn(move |cx| {
            let mut inner_guard = inner.lock().expect("mutex is not poisoned");
            inner_guard.poll_unpin(cx)
        })
        .await
    }

    /// Schedule a call to end voting for the next epoch that will be dispatched by
    /// [`run()`][Self::run].
    ///
    /// Should be called after [`EpochChangeDone`] events. Subsequent calls to this
    /// method cancel any earlier scheduled calls to end voting.
    pub fn schedule_voting_end(&self, next_epoch: NonZero<Epoch>) {
        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if inner.end_voting_future.is_some() {
            tracing::debug!("replacing end-voting future");
        }
        inner.end_voting_future = Some(
            ScheduleVotingEnd {
                rng: StdRng::from_seed(inner.rng.gen()),
                system_params: self.system_parameters.clone(),
                epoch_under_vote: next_epoch,
                contract_service: self.contract_service.clone(),
                time_fn: self.utc_now_fn.clone(),
            }
            .wait_then_end_voting()
            .boxed(),
        );

        inner.wake();
    }

    #[cfg(test)]
    /// Returns true if there is currently a scheduled call to end voting.
    pub fn is_voting_end_scheduled(&self) -> bool {
        let inner = self.inner.lock().expect("thread did not panic with lock");
        inner.end_voting_future.is_some()
    }
}

impl std::fmt::Debug for EpochChangeDriver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochChangeDriver")
            .field("inner", &self.inner)
            .field("contract_service", &self.contract_service)
            .field("system_parameters", &self.system_parameters)
            .field("utc_now_fn", &"UtcNowFn")
            .finish()
    }
}

struct EpochChangeDriverInner<'pin> {
    /// Random state used for jitter.
    rng: StdRng,
    /// A stored waker used to trigger re-evaluation of the future after an inner future has been
    /// replaced.
    waker: Option<Waker>,
    /// An optional future that will end voting for an epoch.
    end_voting_future: Option<BoxFuture<'pin, ()>>,
    /// An optional future that will start epoch change for a given epoch.
    epoch_change_start_future: Option<BoxFuture<'pin, ()>>,
}

impl EpochChangeDriverInner<'_> {
    /// Wake the task containing the future so that it will be polled.
    ///
    /// This should be called after replacing the contained futures, as the new futures should be
    /// immediately polled.
    fn wake(&mut self) {
        // Wake the task, consuming the stored waker in the process.
        //
        // The absence of a waker implicitly signals that the task is already signalled to be
        // awoken, as the waker has been consumed since the last invocation of the future.
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl Future for EpochChangeDriverInner<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Set the waker, which may be used to wake the future's task.
        // Uses the `Waker::clone_from` implementation that avoids the clone if it's unnecessary.
        this.waker
            .get_or_insert_with(|| cx.waker().clone())
            .clone_from(cx.waker());

        if let Some(end_voting_future) = this.end_voting_future.as_mut() {
            if let Poll::Ready(()) = end_voting_future.poll_unpin(cx) {
                tracing::trace!("voting-end future completed");
                this.end_voting_future = None;
            }
        }

        if let Some(epoch_change_start) = this.epoch_change_start_future.as_mut() {
            if let Poll::Ready(()) = epoch_change_start.poll_unpin(cx) {
                tracing::trace!("epoch-change-start future completed");
                this.epoch_change_start_future = None;
            }
        }

        // This future never completes, as this is an infinite task.
        Poll::Pending
    }
}

impl std::fmt::Debug for EpochChangeDriverInner<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_repr = f.debug_struct("EpochChangeDriverInner");

        for (name, future) in [
            ("end_voting_future", &self.end_voting_future),
            ("epoch_change_start_future", &self.epoch_change_start_future),
        ] {
            let representation = if future.is_some() {
                "Some(BoxFuture)"
            } else {
                "None"
            };
            debug_repr.field(name, &representation);
        }

        debug_repr.finish()
    }
}

/// Future-like that ends voting after waiting until the appropriate time.
struct ScheduleVotingEnd {
    /// The epoch for which voting is to be ended.
    epoch_under_vote: NonZero<Epoch>,

    contract_service: Arc<dyn SystemContractService>,
    system_params: FixedSystemParameters,
    time_fn: UtcNowFn,
    rng: StdRng,
}

impl ScheduleVotingEnd {
    #[tracing::instrument(skip_all, fields(epoch_under_vote = self.epoch_under_vote))]
    async fn wait_then_end_voting(mut self) {
        let mut backoff = ExponentialBackoffState::new(MIN_BACKOFF, MAX_BACKOFF, None);

        while let Err(error) = self.try_to_end_voting().await {
            let backoff_duration = backoff.next_delay(&mut self.rng).expect("infinite backoff");
            tracing::debug!(
                %error,
                "attempt to end voting failed, waiting {backoff_duration:?} until next attempt"
            );
            tokio::time::sleep(backoff_duration).await;
        }
    }

    #[tracing::instrument(skip_all)]
    async fn try_to_end_voting(&mut self) -> Result<(), anyhow::Error> {
        let (current_epoch, state) = self.get_epoch_and_state().await?;

        // Since we should only ever be trailing the state on chain, there is something seriously
        // wrong if a storage node thinks the epoch being voted upon is in the far future.
        assert!(
            current_epoch + 1 >= self.epoch_under_vote.get(),
            "cannot end voting for epoch in the far future"
        );

        if current_epoch >= self.epoch_under_vote.get() {
            tracing::debug!("the epoch has already advanced");
            return Ok(());
        }
        debug_assert_eq!(current_epoch + 1, self.epoch_under_vote.get());

        let change_completed_at = match state {
            EpochState::EpochChangeDone(change_completed_at) => change_completed_at,
            EpochState::EpochChangeSync(_) => {
                // The genesis epoch does not have sync, so we are NOT transitioning to epoch 1.
                debug_assert!(self.epoch_under_vote.get() > GENESIS_EPOCH + 1);
                let voting_duration = default_voting_duration(self.system_params.epoch_duration);

                tracing::warn!("attempted to end voting before the epoch change has completed");

                tracing::debug!("waiting {voting_duration:?} before backing off and trying again");
                tokio::time::sleep(voting_duration).await;

                anyhow::bail!("epoch change had not yet completed");
            }
            EpochState::NextParamsSelected(_) => {
                tracing::debug!("voting has already ended");
                return Ok(());
            }
        };

        let duration_until_vote_end = self.duration_until_vote_ends(change_completed_at);
        tracing::debug!("voting ends in {duration_until_vote_end:?}");

        let wait_duration = duration_until_vote_end + jitter(MAX_SCHEDULE_JITTER, &mut self.rng);
        tracing::debug!("waiting {wait_duration:?} before ending vote");

        tokio::time::sleep(wait_duration).await;
        tracing::debug!("attempting to end voting");

        self.contract_service.end_voting().await?;
        tracing::debug!("voting successfully ended");

        Ok(())
    }

    fn duration_until_vote_ends(&self, change_completed_at: DateTime<Utc>) -> Duration {
        let utc_now = (self.time_fn)();

        let delta_until_vote_end = if self.epoch_under_vote.get() == GENESIS_EPOCH + 1 {
            self.system_params
                .epoch_zero_end
                .signed_duration_since(utc_now)
        } else {
            debug_assert!(self.epoch_under_vote.get() > GENESIS_EPOCH + 1);
            let voting_duration = default_voting_duration(self.system_params.epoch_duration);
            let vote_can_be_ended_after = change_completed_at + voting_duration;
            vote_can_be_ended_after - utc_now
        };

        delta_until_vote_end
            .max(TimeDelta::zero())
            .to_std()
            .expect("delta is at least zero")
    }

    async fn get_epoch_and_state(&self) -> Result<(Epoch, EpochState), anyhow::Error> {
        let (current_epoch, state) = self.contract_service.get_epoch_and_state().await?;
        tracing::debug!(
            walrus.epoch = current_epoch,
            ?state,
            "successfully retrieved current epoch and committee state"
        );
        Ok((current_epoch, state))
    }
}

/// Return a random duration uniformly sampled from the interval `[0..max)`.
fn jitter(max: Duration, rng: &mut StdRng) -> Duration {
    rng.gen_range(Duration::ZERO..max)
}

/// Returns the duration of voting for epoch parameters as function of the epoch duration
pub fn default_voting_duration(epoch_duration: Duration) -> Duration {
    epoch_duration / 2
}

#[cfg(test)]
mod tests {
    use std::{iter, num::NonZero};

    use tokio::time::Instant;
    use walrus_test_utils::{async_param_test, nonzero, Result as TestResult};

    use super::*;
    use crate::node::contract_service::MockSystemContractService;

    const EPOCH_ZERO_VOTE_DURATION: Duration = Duration::from_secs(1000);
    const EPOCH_DURATION: Duration = Duration::from_secs(10_000);

    fn driver_under_test<S: SystemContractService + 'static>(
        service: S,
        seed: u64,
    ) -> EpochChangeDriver {
        let utc_at_start = Utc::now();
        let instant_at_start = Instant::now();

        EpochChangeDriver::new_with_time_provider(
            FixedSystemParameters {
                epoch_duration: EPOCH_DURATION,
                epoch_zero_end: utc_at_start + EPOCH_ZERO_VOTE_DURATION,
            },
            Arc::new(service),
            StdRng::seed_from_u64(seed),
            // Compute the time in Utc as a (possibly) fast-forwarded offset from the utc when this
            // object was created.
            Arc::new(move || utc_at_start + instant_at_start.elapsed()),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn waits_until_voting_is_endable() -> TestResult {
        let upcoming_epoch: NonZero<Epoch> = nonzero!(2);
        let voting_duration = default_voting_duration(EPOCH_DURATION);
        let voting_duration_elapsed = voting_duration / 10;
        let voting_duration_remaining = voting_duration - voting_duration_elapsed;
        let epoch_change_completed_at = Utc::now() - voting_duration_elapsed;
        let test_start_time = Instant::now();

        let mut service = MockSystemContractService::new();
        // Voting should only be called once, between the time remaining until the voting ends and
        // the maximum jitter.
        service.expect_end_voting().once().returning(move || {
            let called_at = Instant::now();
            let voting_can_end_at = test_start_time + voting_duration_remaining;
            assert!(called_at >= voting_can_end_at);
            assert!(called_at <= voting_can_end_at + MAX_SCHEDULE_JITTER);
            Ok(())
        });
        // Set that epoch change has already completed to the current epoch.
        service.expect_get_epoch_and_state().returning(move || {
            Ok((
                upcoming_epoch.get() - 1,
                EpochState::EpochChangeDone(epoch_change_completed_at),
            ))
        });

        let driver = driver_under_test(service, /*seed=*/ 0);

        // Schedule voting end for the next epoch.
        driver.schedule_voting_end(upcoming_epoch);

        // Run the driver for a finite amount of time, in which the call should be dispatched.
        let _ = tokio::time::timeout(
            voting_duration_remaining + MAX_SCHEDULE_JITTER,
            driver.run(),
        )
        .await;

        // Drop the driver to ensure that all conditions of the mock service have been met.
        drop(driver);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn waits_until_voting_is_endable_epoch_zero() -> TestResult {
        let upcoming_epoch: NonZero<Epoch> = nonzero!(1);
        let voting_duration = EPOCH_ZERO_VOTE_DURATION;
        let voting_duration_elapsed = voting_duration / 10;
        let voting_duration_remaining = voting_duration - voting_duration_elapsed;
        let epoch_change_completed_at = Utc::now() - voting_duration_elapsed;
        let test_start_time = Instant::now();

        let mut service = MockSystemContractService::new();
        // Voting should only be called once, between the time remaining until the voting ends and
        // the maximum jitter.
        service.expect_end_voting().once().returning(move || {
            let called_at = Instant::now();
            let voting_can_end_at = test_start_time + voting_duration_remaining;
            assert!(called_at >= voting_can_end_at);
            assert!(called_at <= voting_can_end_at + MAX_SCHEDULE_JITTER);
            Ok(())
        });
        // Set that epoch change has already completed to the current epoch.
        service.expect_get_epoch_and_state().returning(move || {
            Ok((
                upcoming_epoch.get() - 1,
                EpochState::EpochChangeDone(epoch_change_completed_at),
            ))
        });

        let driver = driver_under_test(service, /*seed=*/ 0);

        // Schedule voting end for the next epoch.
        driver.schedule_voting_end(upcoming_epoch);

        // Run the driver for a finite amount of time, in which the call should be dispatched.
        let _ = tokio::time::timeout(
            voting_duration_remaining + MAX_SCHEDULE_JITTER,
            driver.run(),
        )
        .await;

        // Drop the driver to ensure that all conditions of the mock service have been met.
        drop(driver);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_epoch_sync_done() -> TestResult {
        let upcoming_epoch: NonZero<Epoch> = nonzero!(2);
        let voting_duration = default_voting_duration(EPOCH_DURATION);
        let voting_starts_after = voting_duration / 10;
        let voting_duration_remaining = voting_duration + voting_starts_after;
        let epoch_change_completed_at = Utc::now() + voting_starts_after;
        let test_start_time = Instant::now();

        let mut service = MockSystemContractService::new();
        // Voting should only be called once, between the time remaining until the voting ends and
        // the maximum jitter.
        service.expect_end_voting().once().returning(move || {
            let called_at = Instant::now();
            let voting_can_end_at = test_start_time + voting_duration_remaining;
            assert!(called_at >= voting_can_end_at);
            assert!(called_at <= voting_can_end_at + MAX_SCHEDULE_JITTER);
            Ok(())
        });

        let mut states = iter::once(EpochState::EpochChangeSync(16)).chain(iter::repeat(
            EpochState::EpochChangeDone(epoch_change_completed_at),
        ));
        service
            .expect_get_epoch_and_state()
            .returning(move || Ok((upcoming_epoch.get() - 1, states.next().unwrap())));

        let driver = driver_under_test(service, /*seed=*/ 0);

        // Schedule voting end for the next epoch.
        driver.schedule_voting_end(upcoming_epoch);

        // Run the driver for a finite amount of time, in which the call should be dispatched.
        let _ = tokio::time::timeout(
            voting_duration_remaining + MAX_SCHEDULE_JITTER,
            driver.run(),
        )
        .await;

        // Drop the driver to ensure that all conditions of the mock service have been met.
        drop(driver);

        Ok(())
    }

    async_param_test! {
        #[tokio::test(start_paused = true)]
        skips_voting_end -> TestResult: [
            already_in_epoch_during_sync: (2, 2, EpochState::EpochChangeSync(0)),
            already_in_epoch_and_change_is_done: (2, 3, EpochState::EpochChangeDone(Utc::now())),
            already_in_epoch_and_next_vote_done: (2, 4, EpochState::NextParamsSelected(Utc::now())),
            voting_already_done: (2, 1, EpochState::NextParamsSelected(Utc::now())),
        ]
    }
    async fn skips_voting_end(epoch: Epoch, system_epoch: Epoch, state: EpochState) -> TestResult {
        let mut service = MockSystemContractService::new();

        // Return the configured system epoch and state.
        service
            .expect_get_epoch_and_state()
            .returning(move || Ok((system_epoch, state.clone())));
        // End voting should not be called since epoch has already advanced.
        service.expect_end_voting().never();

        let driver = driver_under_test(service, /*seed=*/ 1);

        // Schedule voting end for the epoch.
        driver.schedule_voting_end(NonZero::new(epoch).unwrap());

        // Run the driver for a finite amount of time, in which the call should be dispatched.
        let _ = tokio::time::timeout(EPOCH_DURATION, driver.run()).await;

        assert!(
            !driver.is_voting_end_scheduled(),
            "scheduled call should have completed"
        );
        // Drop the driver to ensure that all conditions of the mock service have been met.
        drop(driver);

        Ok(())
    }
}
