// Copyright (c) Walrus Foundation
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
use futures::{FutureExt, future::BoxFuture};
use rand::{Rng, SeedableRng, rngs::StdRng};
use walrus_core::Epoch;
use walrus_sui::{
    client::FixedSystemParameters,
    types::{GENESIS_EPOCH, move_structs::EpochState},
};
use walrus_utils::backoff::ExponentialBackoffState;

use super::contract_service::SystemContractService;

/// Maximum number of seconds that an operation can be randomly delayed by.
const MAX_SCHEDULE_JITTER: Duration = Duration::from_secs(180);
/// The minimum exponential backoff when performing retries.
const MIN_BACKOFF: Duration = Duration::from_secs(5);
/// The maximum exponential backoff when performing retries.
const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// The maximum amount of time before the epoch change that process subsidies is scheduled.
const MAX_SUBSIDIES_TIME_BEFORE_EPOCH_CHANGE: Duration = Duration::from_secs(300);

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
                subsidies_future: None,
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
            let mut inner_guard = inner.lock().expect("mutex should not be poisoned");
            inner_guard.poll_unpin(cx)
        })
        .await
    }

    /// Fetches the state of the current epoch, and schedules all applicable calls.
    ///
    /// This method should be used after a restart of the storage node to schedule calls for the
    /// current state, in the event that the events that would otherwise trigger such calls has
    /// already been processed.
    ///
    /// This function may fail if it is unable to get the current epoch's state. See the individual
    /// `schedule_*` methods for alternatives that will retry until successful.
    pub async fn schedule_relevant_calls_for_current_epoch(&self) -> Result<(), anyhow::Error> {
        let (epoch, state) = self.contract_service.get_epoch_and_state().await?;

        if epoch == 0 {
            tracing::info!("not scheduling any epoch-change calls in genesis epoch");
            return Ok(());
        }

        let next_epoch = NonZero::new(epoch + 1).expect("incremented value is non-zero");

        match state {
            EpochState::EpochChangeSync(_) => (),
            EpochState::EpochChangeDone(_) => self.schedule_voting_end(next_epoch),
            EpochState::NextParamsSelected(_) => {
                self.schedule_initiate_epoch_change(next_epoch);
                self.schedule_process_subsidies();
            }
        }

        Ok(())
    }

    /// Schedule a call to end voting for the next epoch that will be dispatched by
    /// [`run()`][Self::run].
    ///
    /// Should be called after [`EpochChangeDone`] events. Subsequent calls to this
    /// method cancel any earlier scheduled calls to end voting.
    #[tracing::instrument(skip_all)]
    pub fn schedule_voting_end(&self, next_epoch: NonZero<Epoch>) {
        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if let Some((ref epoch_of_future, _)) = inner.end_voting_future {
            tracing::debug!(
                prior_epoch = epoch_of_future,
                new_epoch = next_epoch.get(),
                "replacing end-voting future"
            );
        }

        let end_voting_future = ScheduledEpochOperation::new(
            VotingEndOperation {
                epoch_under_vote: next_epoch,
                system_params: self.system_parameters,
            },
            self.system_parameters.epoch_duration,
            self.contract_service.clone(),
            self.utc_now_fn.clone(),
            &mut inner.rng,
        )
        .wait_then_call();

        inner.end_voting_future = Some((next_epoch.get(), end_voting_future.boxed()));

        tracing::debug!(walrus.epoch = next_epoch, "scheduled voting end");
        inner.wake();
    }

    /// Cancels the call to end voting that was scheduled with
    /// [`schedule_voting_end()`][Self::schedule_voting_end].
    pub fn cancel_scheduled_voting_end(&self, epoch: Epoch) {
        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if let Some((ref epoch_of_scheduled_future, _)) = inner.end_voting_future {
            if epoch == *epoch_of_scheduled_future {
                inner.end_voting_future = None;
                tracing::debug!(walrus.epoch = epoch, "cancelled voting end operation");
            } else {
                tracing::debug!(
                    scheduled_epoch = epoch_of_scheduled_future,
                    requested_epoch = epoch,
                    "did not cancel voting end operation as epochs did not match"
                );
            }
        }
    }

    /// Schedules a call to start epoch change to the next epoch that will be dispatched by
    /// [`run()`][Self::run].
    ///
    /// Should be called after [`NextParamsSelected`] events. Subsequent calls to this
    /// method cancel any earlier scheduled calls.
    #[tracing::instrument(skip_all)]
    pub fn schedule_initiate_epoch_change(&self, next_epoch: NonZero<Epoch>) {
        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if let Some((ref epoch_of_future, _)) = inner.epoch_change_start_future {
            tracing::debug!(
                prior_epoch = epoch_of_future,
                new_epoch = next_epoch.get(),
                "replacing epoch-change-start future"
            );
        }

        let epoch_change_start_future = ScheduledEpochOperation::new(
            InitiateEpochChangeOperation {
                next_epoch,
                system_params: self.system_parameters,
            },
            self.system_parameters.epoch_duration,
            self.contract_service.clone(),
            self.utc_now_fn.clone(),
            &mut inner.rng,
        )
        .wait_then_call();

        inner.epoch_change_start_future =
            Some((next_epoch.get(), epoch_change_start_future.boxed()));

        tracing::debug!(
            walrus.epoch = next_epoch,
            "scheduled initiation of epoch change"
        );
        inner.wake();
    }

    /// Cancels the call to initiate epoch change that was scheduled with
    /// [`schedule_initiate_epoch_change()`][Self::schedule_initiate_epoch_change].
    pub fn cancel_scheduled_epoch_change_initiation(&self, epoch: Epoch) {
        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if let Some((ref epoch_of_scheduled_future, _)) = inner.end_voting_future {
            if epoch == *epoch_of_scheduled_future {
                inner.epoch_change_start_future = None;
                tracing::debug!(walrus.epoch = epoch, "cancelled epoch change initiation");
            } else {
                tracing::debug!(
                    scheduled_epoch = epoch_of_scheduled_future,
                    requested_epoch = epoch,
                    "did not cancel epoch change initiation as epochs did not match"
                );
            }
        }
    }

    /// Schedules a call to process subsidies for the next epoch that will be dispatched by
    /// [`run()`][Self::run] if subsidies are configured in the contract service.
    ///
    /// Should be called after [`NextParamsSelected`] events. Subsequent calls to this
    /// method cancel any earlier scheduled calls.
    #[tracing::instrument(skip_all)]
    pub fn schedule_process_subsidies(&self) {
        if !self.contract_service.is_subsidies_object_configured() {
            tracing::debug!("subsidies are not configured in the contract");
            return;
        }

        let mut inner = self.inner.lock().expect("thread did not panic with lock");

        if inner.subsidies_future.is_some() {
            tracing::debug!("replacing subsidies future");
            inner.subsidies_future = None;
        }

        let subsidies_future = ScheduledEpochOperation::new(
            SubsidiesOperation {
                time_before_epoch_change: (self.system_parameters.epoch_duration / 10)
                    .min(MAX_SUBSIDIES_TIME_BEFORE_EPOCH_CHANGE),
                system_params: self.system_parameters,
                time_fn: self.utc_now_fn.clone(),
            },
            self.system_parameters.epoch_duration,
            self.contract_service.clone(),
            self.utc_now_fn.clone(),
            &mut inner.rng,
        )
        .wait_then_call();

        inner.subsidies_future = Some(subsidies_future.boxed());

        tracing::debug!("scheduled process subsidies");
        inner.wake();
    }

    #[cfg(test)]
    /// Returns true if there is currently a scheduled call to end voting.
    pub fn is_voting_end_scheduled(&self) -> bool {
        let inner = self.inner.lock().expect("thread did not panic with lock");
        inner.end_voting_future.is_some()
    }

    #[cfg(test)]
    pub fn is_epoch_change_start_scheduled(&self) -> bool {
        let inner = self.inner.lock().expect("thread did not panic with lock");
        inner.epoch_change_start_future.is_some()
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
    end_voting_future: Option<(Epoch, BoxFuture<'pin, ()>)>,
    /// An optional future that will start epoch change for a given epoch.
    epoch_change_start_future: Option<(Epoch, BoxFuture<'pin, ()>)>,
    /// An optional future that will process subsidies.
    subsidies_future: Option<BoxFuture<'pin, ()>>,
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

        tracing::info_span!("scheduled_end_voting").in_scope(|| {
            if let Some((_, end_voting_future)) = this.end_voting_future.as_mut()
                && let Poll::Ready(()) = end_voting_future.poll_unpin(cx)
            {
                tracing::trace!("voting-end future completed");
                this.end_voting_future = None;
            }
        });

        tracing::info_span!("scheduled_initiate_epoch_change").in_scope(|| {
            if let Some((_, epoch_change_start)) = this.epoch_change_start_future.as_mut()
                && let Poll::Ready(()) = epoch_change_start.poll_unpin(cx)
            {
                tracing::trace!("epoch-change-start future completed");
                this.epoch_change_start_future = None;
            }
        });

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

/// An operation that drives epoch change.
trait EpochOperation {
    /// Returns the duration until the operation is ready to be called or None if the operation has
    /// already completed.
    fn time_until_ready(
        &self,
        utc_now: DateTime<Utc>,
        current_epoch: Epoch,
        state: EpochState,
    ) -> Option<Duration>;

    /// Invokes the operation with the provided contract service.
    async fn invoke(&self, contract: Arc<dyn SystemContractService>) -> Result<(), anyhow::Error>;
}

/// An [`EpochOperation`] scheduled to be called.
struct ScheduledEpochOperation<T> {
    /// The operation that will be scheduled and invoked.
    operation: T,
    /// The maximum amount of jitter added to a scheduled call.
    ///
    /// Is set to `max(0.1 * epoch_duration, MAX_SCHEDULE_JITTER)`.
    max_jitter: Duration,

    contract_service: Arc<dyn SystemContractService>,
    time_fn: UtcNowFn,
    rng: StdRng,
}

impl<T: EpochOperation> ScheduledEpochOperation<T> {
    fn new(
        operation: T,
        epoch_duration: Duration,
        contract_service: Arc<dyn SystemContractService>,
        time_fn: UtcNowFn,
        rng: &mut StdRng,
    ) -> Self {
        Self {
            operation,
            contract_service,
            time_fn,
            max_jitter: std::cmp::min(MAX_SCHEDULE_JITTER, epoch_duration / 10),
            rng: StdRng::seed_from_u64(rng.r#gen()),
        }
    }

    /// Invoke the stored operation after its selected delay, retrying if it fails.
    #[tracing::instrument(skip_all)]
    async fn wait_then_call(mut self) {
        let mut backoff = ExponentialBackoffState::new(MIN_BACKOFF, MAX_BACKOFF, None);

        while let Err(error) = self.try_wait_then_call().await {
            let backoff_duration = backoff.next_delay(&mut self.rng).expect("infinite backoff");
            tracing::debug!(
                %error,
                "attempt failed, waiting {backoff_duration:?} until next attempt"
            );
            tokio::time::sleep(backoff_duration).await;
        }
    }

    #[tracing::instrument(skip_all)]
    async fn try_wait_then_call(&mut self) -> Result<(), anyhow::Error> {
        let (current_epoch, state) = self.contract_service.get_epoch_and_state().await?;
        tracing::debug!(
            walrus.epoch = current_epoch,
            ?state,
            "successfully retrieved current epoch and committee state"
        );

        let Some(wait_duration) =
            self.operation
                .time_until_ready((self.time_fn)(), current_epoch, state)
        else {
            tracing::debug!("operation signalled that it is already complete");
            return Ok(());
        };
        let schedule_jitter = jitter(self.max_jitter, &mut self.rng);

        tracing::debug!(
            "scheduling operation to be called in {wait_duration:.0?} (+{schedule_jitter:.0?})"
        );
        tokio::time::sleep(wait_duration + schedule_jitter).await;

        tracing::debug!("invoking scheduled operation");
        self.operation.invoke(self.contract_service.clone()).await
    }
}

/// Operation that ends voting for the next epoch parameters.
struct VotingEndOperation {
    /// The epoch for which voting is to be ended.
    epoch_under_vote: NonZero<Epoch>,
    system_params: FixedSystemParameters,
}

impl EpochOperation for VotingEndOperation {
    fn time_until_ready(
        &self,
        utc_now: DateTime<Utc>,
        current_epoch: Epoch,
        state: EpochState,
    ) -> Option<Duration> {
        // Since we should only ever be trailing the state on chain, there is something seriously
        // wrong if a storage node thinks the epoch being voted upon is in the far future.
        assert!(
            current_epoch + 1 >= self.epoch_under_vote.get(),
            "cannot end voting for epoch in the far future"
        );

        if current_epoch >= self.epoch_under_vote.get() {
            tracing::debug!("the epoch has already advanced");
            return None;
        }
        debug_assert_eq!(current_epoch + 1, self.epoch_under_vote.get());

        let change_completed_at = match state {
            EpochState::EpochChangeDone(change_completed_at) => change_completed_at,
            EpochState::EpochChangeSync(_) => {
                panic!("must not end voting before being notified that the epoch change is done");
            }
            EpochState::NextParamsSelected(_) => {
                tracing::debug!("voting has already ended");
                return None;
            }
        };

        let voting_ends_at = if self.epoch_under_vote.get() == GENESIS_EPOCH + 1 {
            self.system_params.epoch_zero_end
        } else {
            debug_assert!(self.epoch_under_vote.get() > GENESIS_EPOCH + 1);
            change_completed_at + default_voting_duration(self.system_params.epoch_duration)
        };

        let duration_until_vote_ends = voting_ends_at
            .signed_duration_since(utc_now)
            .max(TimeDelta::zero())
            .to_std()
            .expect("max makes the value always positive");
        tracing::debug!("voting ends in {duration_until_vote_ends:.0?}");

        Some(duration_until_vote_ends)
    }

    async fn invoke(&self, contract: Arc<dyn SystemContractService>) -> Result<(), anyhow::Error> {
        let epoch_under_vote = self.epoch_under_vote.get();
        let (current_epoch, state) = contract.get_epoch_and_state().await?;
        if current_epoch >= epoch_under_vote
            || (current_epoch + 1 == epoch_under_vote
                && matches!(state, EpochState::NextParamsSelected(_)))
        {
            tracing::debug!(epoch_under_vote, "voting already ended");
            return Ok(());
        }
        tracing::info!(epoch_under_vote, "attempting to end voting");
        // Move transaction execution to a separate task so that it cannot be cancelled when
        // invoke() is cancelled. Otherwise, cancelling inflight transaction may cause object
        // conflicts.
        tokio::spawn({
            let contract = contract.clone();
            async move { contract.end_voting().await }
        })
        .await
        .map_err(|e| anyhow::anyhow!("end voting panicked: {:?}", e))??;
        tracing::debug!("voting successfully ended");
        Ok(())
    }
}

/// Operation that initiates epoch change.
struct InitiateEpochChangeOperation {
    /// The next epoch.
    next_epoch: NonZero<Epoch>,
    system_params: FixedSystemParameters,
}

impl EpochOperation for InitiateEpochChangeOperation {
    fn time_until_ready(
        &self,
        utc_now: DateTime<Utc>,
        current_epoch: Epoch,
        state: EpochState,
    ) -> Option<Duration> {
        // Since we should only ever be trailing the state on chain, there is something seriously
        // wrong if a storage node thinks the next epoch is in the far future.
        assert!(
            current_epoch + 1 >= self.next_epoch.get(),
            "cannot start epoch change for an epoch in the far future"
        );

        if current_epoch >= self.next_epoch.get() {
            tracing::debug!("the epoch has already advanced");
            return None;
        }
        debug_assert_eq!(current_epoch + 1, self.next_epoch.get());

        let current_epoch_started_at = match state {
            EpochState::EpochChangeDone(_) | EpochState::EpochChangeSync(_) => {
                panic!("call to start epoch change initiated before the required event");
            }
            EpochState::NextParamsSelected(current_epoch_started_at) => current_epoch_started_at,
        };

        let epoch_ends_at = if self.next_epoch.get() == GENESIS_EPOCH + 1 {
            self.system_params.epoch_zero_end
        } else {
            debug_assert!(self.next_epoch.get() > GENESIS_EPOCH + 1);
            current_epoch_started_at + self.system_params.epoch_duration
        };

        let duration_until_epoch_ends = epoch_ends_at
            .signed_duration_since(utc_now)
            .max(TimeDelta::zero())
            .to_std()
            .expect("max(., 0) makes the value always positive");
        tracing::debug!("epoch ends in {duration_until_epoch_ends:.0?}");

        Some(duration_until_epoch_ends)
    }

    async fn invoke(&self, contract: Arc<dyn SystemContractService>) -> Result<(), anyhow::Error> {
        let next_epoch = self.next_epoch.get();
        if contract.current_epoch() >= next_epoch {
            tracing::debug!("epoch change already started");
            return Ok(());
        }
        tracing::info!(next_epoch, "attempting to start epoch change");
        // Move transaction execution to a separate task so that it cannot be cancelled when
        // invoke() is cancelled. Otherwise, cancelling inflight transaction may cause object
        // conflicts.
        tokio::spawn({
            let contract = contract.clone();
            async move { contract.initiate_epoch_change().await }
        })
        .await
        .map_err(|e| anyhow::anyhow!("initiate epoch change panicked: {:?}", e))??;
        tracing::debug!("epoch change successfully started");
        Ok(())
    }
}

/// Operation that calls the walrus subsidies contract if set.
struct SubsidiesOperation {
    time_before_epoch_change: Duration,
    system_params: FixedSystemParameters,
    time_fn: UtcNowFn,
}

impl EpochOperation for SubsidiesOperation {
    fn time_until_ready(
        &self,
        utc_now: DateTime<Utc>,
        current_epoch: Epoch,
        state: EpochState,
    ) -> Option<Duration> {
        let current_epoch_started_at = match state {
            EpochState::EpochChangeDone(_) | EpochState::EpochChangeSync(_) => {
                todo!("How should we handle this? Panic? Or return time?")
            }
            EpochState::NextParamsSelected(current_epoch_started_at) => current_epoch_started_at,
        };
        if current_epoch == GENESIS_EPOCH {
            return None;
        }

        let subsidies_call_at = current_epoch_started_at + self.system_params.epoch_duration
            - self.time_before_epoch_change;

        let duration_until_subsidies_call = subsidies_call_at
            .signed_duration_since(utc_now)
            .max(TimeDelta::zero())
            .to_std()
            .expect("max(., 0) makes the value always positive");
        tracing::debug!("subsidies call in {duration_until_subsidies_call:.0?}");

        Some(duration_until_subsidies_call)
    }

    async fn invoke(&self, contract: Arc<dyn SystemContractService>) -> Result<(), anyhow::Error> {
        let last_subsidies_call = contract.last_walrus_subsidies_call().await?;
        let current_time = (self.time_fn)();
        // If the last subsidies call was less than time_before_epoch_change ago, return without
        // calling them again. We use the same duration here to avoid having to configure too
        // many parameters.
        if current_time - self.time_before_epoch_change > last_subsidies_call {
            tracing::debug!("subsidies already called recently");
            return Ok(());
        }

        tracing::info!("initiating subsidy distribution");
        // Move transaction execution to a separate task so that it cannot be cancelled when
        // invoke() is cancelled. Otherwise, cancelling inflight transaction may cause object
        // conflicts.
        tokio::spawn({
            let contract = contract.clone();
            async move { contract.process_subsidies().await }
        })
        .await
        .map_err(|e| {
            anyhow::anyhow!("call to initiate subsidy distribution panicked: {:?}", e)
        })??;
        tracing::debug!("successfully initiated subsidy distribution");
        Ok(())
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
    use std::num::{NonZero, NonZeroU16};

    use tokio::time::Instant;
    use walrus_test_utils::{Result as TestResult, async_param_test, nonzero};

    use super::*;
    use crate::node::contract_service::MockSystemContractService;

    const EPOCH_ZERO_DURATION: Duration = Duration::from_secs(1000);
    const EPOCH_ZERO_VOTE_DURATION: Duration = EPOCH_ZERO_DURATION;
    const EPOCH_DURATION: Duration = Duration::from_secs(10_000);

    #[derive(Debug, Copy, Clone)]
    struct UtcInstant {
        utc: DateTime<Utc>,
        instant: Instant,
    }

    impl UtcInstant {
        fn now() -> Self {
            Self {
                utc: Utc::now(),
                instant: Instant::now(),
            }
        }
    }

    fn driver_under_test<S: SystemContractService + 'static>(
        service: S,
        seed: u64,
        start: UtcInstant,
    ) -> EpochChangeDriver {
        driver_under_test_with_epoch_zero_end(
            service,
            seed,
            start,
            start.utc + EPOCH_ZERO_VOTE_DURATION,
        )
    }

    fn driver_under_test_with_epoch_zero_end<S: SystemContractService + 'static>(
        service: S,
        seed: u64,
        start: UtcInstant,
        epoch_zero_end: DateTime<Utc>,
    ) -> EpochChangeDriver {
        EpochChangeDriver::new_with_time_provider(
            FixedSystemParameters {
                n_shards: NonZeroU16::new(1000).expect("1000 > 0"),
                max_epochs_ahead: 200,
                epoch_duration: EPOCH_DURATION,
                epoch_zero_end,
            },
            Arc::new(service),
            StdRng::seed_from_u64(seed),
            // Compute the time in Utc as a (possibly) fast-forwarded offset from the utc when this
            // object was created.
            Arc::new(move || start.utc + start.instant.elapsed()),
        )
    }

    mod end_voting {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn waits_until_voting_is_endable() -> TestResult {
            let start = UtcInstant::now();

            let upcoming_epoch: NonZero<Epoch> = nonzero!(2);
            let voting_duration = default_voting_duration(EPOCH_DURATION);
            let voting_duration_elapsed = voting_duration / 10;
            let voting_duration_remaining = voting_duration - voting_duration_elapsed;
            let epoch_change_completed_at = start.utc - voting_duration_elapsed;

            let mut service = MockSystemContractService::new();
            // Voting should only be called once, between the time remaining until the voting ends
            // and the maximum jitter.
            service.expect_end_voting().once().returning(move || {
                let called_at = Instant::now();
                let voting_can_end_at = start.instant + voting_duration_remaining;
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
            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));

            let driver = driver_under_test(service, /*seed=*/ 0, start);

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
            let start = UtcInstant::now();
            let voting_duration = EPOCH_ZERO_VOTE_DURATION;
            let voting_duration_elapsed = voting_duration / 10;
            let voting_duration_remaining = voting_duration - voting_duration_elapsed;
            let epoch_change_completed_at = start.utc - voting_duration_elapsed;

            let mut service = MockSystemContractService::new();
            // Voting should only be called once, between the time remaining until the voting ends
            // and the maximum jitter.
            service.expect_end_voting().once().returning(move || {
                let called_at = Instant::now();
                let voting_can_end_at = start.instant + voting_duration_remaining;
                assert!(called_at >= voting_can_end_at);
                assert!(called_at <= voting_can_end_at + MAX_SCHEDULE_JITTER);
                Ok(())
            });
            // Set that epoch change has already completed to the current epoch.
            service.expect_get_epoch_and_state().returning(move || {
                Ok((
                    GENESIS_EPOCH,
                    EpochState::EpochChangeDone(epoch_change_completed_at),
                ))
            });
            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));

            let driver = driver_under_test_with_epoch_zero_end(
                service,
                /*seed=*/ 1,
                start,
                start.utc + voting_duration_remaining,
            );

            // Schedule voting end for the next epoch.
            driver.schedule_voting_end(NonZero::new(GENESIS_EPOCH + 1).unwrap());

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
                already_in_epoch_and_change_is_done: (
                    2, 3, EpochState::EpochChangeDone(Utc::now())
                ),
                already_in_epoch_and_next_vote_done: (
                    2, 4, EpochState::NextParamsSelected(Utc::now())
                ),
                voting_already_done: (2, 1, EpochState::NextParamsSelected(Utc::now())),
            ]
        }
        async fn skips_voting_end(
            epoch: Epoch,
            system_epoch: Epoch,
            state: EpochState,
        ) -> TestResult {
            let mut service = MockSystemContractService::new();

            // Return the configured system epoch and state.
            service
                .expect_get_epoch_and_state()
                .returning(move || Ok((system_epoch, state.clone())));
            // End voting should not be called since epoch has already advanced.
            service.expect_end_voting().never();

            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));

            let driver = driver_under_test(service, /*seed=*/ 2, UtcInstant::now());

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

    mod start_epoch_change {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn waits_until_epoch_change_may_start() -> TestResult {
            let start = UtcInstant::now();
            let upcoming_epoch: NonZero<Epoch> = nonzero!(2);
            let epoch_duration_elapsed = EPOCH_DURATION / 8;
            let epoch_duration_remaining = EPOCH_DURATION - epoch_duration_elapsed;
            let current_epoch_started_at = start.utc - epoch_duration_elapsed;

            let mut service = MockSystemContractService::new();

            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));
            service
                .expect_initiate_epoch_change()
                .once()
                .returning(move || {
                    let called_at = Instant::now();
                    let epoch_can_end_at = start.instant + epoch_duration_remaining;
                    assert!(called_at >= epoch_can_end_at);
                    assert!(called_at <= epoch_can_end_at + MAX_SCHEDULE_JITTER);
                    Ok(())
                });

            service.expect_get_epoch_and_state().returning(move || {
                Ok((
                    upcoming_epoch.get() - 1,
                    EpochState::NextParamsSelected(current_epoch_started_at),
                ))
            });

            service
                .expect_current_epoch()
                .returning(move || upcoming_epoch.get() - 1);

            let driver = driver_under_test(service, /*seed=*/ 3, start);

            // Schedule epoch change for the next epoch.
            driver.schedule_initiate_epoch_change(upcoming_epoch);

            // Run the driver for a finite amount of time, in which the call should be dispatched.
            let _ =
                tokio::time::timeout(epoch_duration_remaining + MAX_SCHEDULE_JITTER, driver.run())
                    .await;

            // Drop the driver to ensure that all conditions of the mock service have been met.
            drop(driver);

            Ok(())
        }

        #[tokio::test(start_paused = true)]
        async fn waits_until_epoch_zero_can_end() -> TestResult {
            let start = UtcInstant::now();

            let epoch_duration_elapsed = EPOCH_ZERO_DURATION / 8;
            let epoch_duration_remaining = EPOCH_ZERO_DURATION - epoch_duration_elapsed;
            let current_epoch_started_at = start.utc - epoch_duration_elapsed;

            let mut service = MockSystemContractService::new();

            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));
            service
                .expect_initiate_epoch_change()
                .once()
                .returning(move || {
                    let called_at = Instant::now();
                    let epoch_can_end_at = start.instant + epoch_duration_remaining;
                    assert!(called_at >= epoch_can_end_at);
                    assert!(called_at <= epoch_can_end_at + MAX_SCHEDULE_JITTER);
                    Ok(())
                });

            service.expect_get_epoch_and_state().returning(move || {
                Ok((
                    GENESIS_EPOCH,
                    EpochState::NextParamsSelected(current_epoch_started_at),
                ))
            });

            service
                .expect_current_epoch()
                .returning(move || GENESIS_EPOCH);

            let driver = driver_under_test_with_epoch_zero_end(
                service,
                /*seed=*/ 1,
                start,
                start.utc + epoch_duration_remaining,
            );

            // Schedule epoch change for the next epoch.
            driver.schedule_initiate_epoch_change(NonZero::new(GENESIS_EPOCH + 1).unwrap());

            // Run the driver for a finite amount of time, in which the call should be dispatched.
            let _ =
                tokio::time::timeout(epoch_duration_remaining + MAX_SCHEDULE_JITTER, driver.run())
                    .await;

            // Drop the driver to ensure that all conditions of the mock service have been met.
            drop(driver);

            Ok(())
        }

        async_param_test! {
            #[tokio::test(start_paused = true)]
            skips_starting_in_epoch_change -> TestResult: [
                already_in_next_epoch_sync: (2, 2, EpochState::EpochChangeSync(0)),
                already_in_further_epoch_and_change_is_done: (
                    2, 3, EpochState::EpochChangeDone(Utc::now())
                ),
                already_in_further_epoch_and_vote_done: (
                    2, 4, EpochState::NextParamsSelected(Utc::now())
                ),
            ]
        }
        async fn skips_starting_in_epoch_change(
            epoch: Epoch,
            system_epoch: Epoch,
            state: EpochState,
        ) -> TestResult {
            let mut service = MockSystemContractService::new();

            service
                .expect_sync_node_params()
                .returning(|_config, _node_cap_id| Ok(()));
            // Return the configured system epoch and state.
            service
                .expect_get_epoch_and_state()
                .returning(move || Ok((system_epoch, state.clone())));
            // Should not initiate epoch change since epoch has already advanced.
            service.expect_initiate_epoch_change().never();

            let driver = driver_under_test(service, /*seed=*/ 2, UtcInstant::now());

            // Schedule epoch change
            driver.schedule_initiate_epoch_change(NonZero::new(epoch).unwrap());

            // Run the driver for a finite amount of time, in which the call should be dispatched.
            let _ = tokio::time::timeout(EPOCH_DURATION, driver.run()).await;

            assert!(
                !driver.is_epoch_change_start_scheduled(),
                "scheduled call should have completed"
            );
            // Drop the driver to ensure that all conditions of the mock service have been met.
            drop(driver);

            Ok(())
        }
    }
}
