// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, num::Saturating, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationMilliSeconds};

/// Wrapper for the configuration for the exponential backoff strategy.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ExponentialBackoffConfig {
    /// The minimum backoff duration.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "min_backoff_millis")]
    pub min_backoff: Duration,
    /// The maximum backoff duration.
    #[serde_as(as = "DurationMilliSeconds")]
    #[serde(rename = "max_backoff_millis")]
    pub max_backoff: Duration,
    /// The maximum number of retries.
    ///
    /// If `None`, the backoff strategy will keep retrying indefinitely.
    pub max_retries: Option<u32>,
}

impl ExponentialBackoffConfig {
    /// Creates a new configuration with the given parameters.
    pub fn new(min_backoff: Duration, max_backoff: Duration, max_retries: Option<u32>) -> Self {
        ExponentialBackoffConfig {
            min_backoff,
            max_backoff,
            max_retries,
        }
    }

    /// Gets a new [`ExponentialBackoff`] strategy with the given seed from the configuration.
    pub fn get_strategy(&self, seed: u64) -> ExponentialBackoff<StdRng> {
        ExponentialBackoff::new_with_seed(
            self.min_backoff,
            self.max_backoff,
            self.max_retries,
            seed,
        )
    }
}

impl Default for ExponentialBackoffConfig {
    fn default() -> Self {
        ExponentialBackoffConfig {
            min_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            max_retries: Some(5),
        }
    }
}

#[derive(Debug)]
pub struct ExponentialBackoffState {
    min_backoff: Duration,
    max_backoff: Duration,
    sequence_index: u32,
    max_retries: Option<u32>,
}

/// The representation of a backoff strategy.
pub trait BackoffStrategy {
    /// Steps the backoff iterator, returning the next delay and advances the backoff.
    ///
    /// Returns `None` if the strategy mandates that the consumer should stop backing off.
    fn next_delay(&mut self) -> Option<Duration>;
}

impl ExponentialBackoffState {
    /// Creates new state with the provided minimum and maximum bounds.
    ///
    /// If `max_retries` is `None`, this backoff strategy will keep retrying indefinitely.
    pub fn new(min_backoff: Duration, max_backoff: Duration, max_retries: Option<u32>) -> Self {
        Self {
            min_backoff,
            max_backoff,
            sequence_index: 0,
            max_retries,
        }
    }

    /// Creates a new `ExponentialBackoffTracker` that yields an infinite sequence of backoffs
    /// between the min and max specified.
    pub fn new_infinite(min_backoff: Duration, max_backoff: Duration) -> Self {
        Self::new(min_backoff, max_backoff, None)
    }

    pub fn next_delay<R: Rng + ?Sized>(&mut self, rng: &mut R) -> Option<Duration> {
        if let Some(max_retries) = self.max_retries {
            if self.sequence_index >= max_retries {
                return None;
            }
        }

        let next_delay_value = self
            .min_backoff
            .saturating_mul(Saturating(2u32).pow(self.sequence_index).0)
            .saturating_add(Self::random_offset(rng))
            .min(self.max_backoff);

        self.sequence_index = self.sequence_index.saturating_add(1);

        Some(next_delay_value)
    }

    fn random_offset<R: Rng + ?Sized>(rng: &mut R) -> Duration {
        let millis = rng.gen_range(0..=ExponentialBackoff::MAX_RAND_OFFSET_MS);
        Duration::from_millis(millis)
    }
}

/// An iterator over exponential wait durations.
///
/// Returns the wait duration for an exponential backoff with a multiplicative factor of 2, and
/// where each duration includes a random positive offset.
///
/// For the `i`-th iterator element and bounds `min_backoff` and `max_backoff`, this returns the
/// sequence `min(max_backoff, 2^i * min_backoff + rand_i)`.
#[derive(Debug)]
pub struct ExponentialBackoff<R> {
    state: ExponentialBackoffState,
    rng: R,
}

impl ExponentialBackoff<StdRng> {
    /// Maximum number of milliseconds to randomly add to the delay time.
    const MAX_RAND_OFFSET_MS: u64 = 1000;

    /// Creates a new iterator with the provided minimum and maximum bounds,
    /// and seeded with the provided value.
    ///
    /// If `max_retries` is `None`, this backoff strategy will keep retrying indefinitely.
    pub fn new_with_seed(
        min_backoff: Duration,
        max_backoff: Duration,
        max_retries: Option<u32>,
        seed: u64,
    ) -> ExponentialBackoff<StdRng> {
        ExponentialBackoff::<StdRng>::new_with_rng(
            min_backoff,
            max_backoff,
            max_retries,
            StdRng::seed_from_u64(seed),
        )
    }
}

impl<R: Rng> ExponentialBackoff<R> {
    /// Creates a new iterator with the provided minimum and maximum bounds, with the provided
    /// iterator.
    ///
    /// If `max_retries` is `None`, this backoff strategy will keep retrying indefinitely.
    pub fn new_with_rng(
        min_backoff: Duration,
        max_backoff: Duration,
        max_retries: Option<u32>,
        rng: R,
    ) -> Self {
        Self {
            state: ExponentialBackoffState::new(min_backoff, max_backoff, max_retries),
            rng,
        }
    }

    fn next_delay(&mut self) -> Option<Duration> {
        self.state.next_delay(&mut self.rng)
    }
}

impl<R: Rng> Iterator for ExponentialBackoff<R> {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_delay()
    }
}

impl<I> BackoffStrategy for I
where
    I: Iterator<Item = Duration>,
{
    fn next_delay(&mut self) -> Option<Duration> {
        self.next()
    }
}

/// Trait to unify checking for success on `Result` and `Option` types.
pub trait SuccessOrFailure {
    /// Returns true iff the value is considered successful.
    fn is_success(&self) -> bool;
}

impl<T, E> SuccessOrFailure for anyhow::Result<T, E> {
    fn is_success(&self) -> bool {
        self.is_ok()
    }
}

impl<T> SuccessOrFailure for Option<T> {
    fn is_success(&self) -> bool {
        self.is_some()
    }
}

impl SuccessOrFailure for bool {
    fn is_success(&self) -> bool {
        *self
    }
}

/// Repeatedly calls the provided function with the provided backoff strategy until it returns
/// successfully.
pub async fn retry<S, F, T, Fut>(mut strategy: S, mut func: F) -> T
where
    S: BackoffStrategy,
    F: FnMut() -> Fut,
    T: SuccessOrFailure,
    Fut: Future<Output = T>,
{
    loop {
        let value = func().await;

        if value.is_success() {
            return value;
        }

        if let Some(delay) = strategy.next_delay() {
            tracing::debug!(?delay, "attempt failed, waiting before retrying");
            tokio::time::sleep(delay).await;
        } else {
            tracing::debug!("last attempt failed, returning last failure value");
            return value;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::backoff::ExponentialBackoff;

    #[test]
    fn backoff_is_exponential() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(3600);

        let expected: Vec<_> = (0u32..)
            .map(|i| min * 2u32.pow(i))
            .take_while(|d| *d < max)
            .chain([max; 2])
            .collect();

        let actual: Vec<_> = ExponentialBackoff::new_with_seed(min, max, None, 42)
            .take(expected.len())
            .collect();

        assert_eq!(expected.len(), actual.len());
        assert_ne!(expected, actual, "retries must have a random component");

        for (expected, actual) in expected.iter().zip(actual) {
            let expected_min = *expected;
            let expected_max =
                *expected + Duration::from_millis(ExponentialBackoff::MAX_RAND_OFFSET_MS);

            assert!(actual >= expected_min, "{actual:?} >= {expected_min:?}");
            assert!(actual <= expected_max);
        }
    }

    #[test]
    fn backoff_stops_after_max_retries() {
        let retries = 5;
        let mut strategy = ExponentialBackoff::new_with_seed(
            Duration::from_millis(1),
            Duration::from_millis(5),
            Some(retries),
            42,
        );
        let mut actual = 0;
        while let Some(_d) = strategy.next_delay() {
            actual += 1;
        }
        assert_eq!(retries, actual);
    }
}
