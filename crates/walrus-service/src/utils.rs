// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utility functions used internally in the crate.

use std::{future::Future, num::Saturating, sync::Arc, time::Duration};

use futures::FutureExt;
use rand::{
    distributions::{DistIter, Uniform},
    rngs::StdRng,
    Rng,
    SeedableRng,
};
use tokio::sync::Semaphore;

/// An iterator over exponential wait durations.
///
/// Returns the wait duration for an exponential backoff with a multiplicative factor of 2, and
/// where each duration includes a random positive offset.
///
/// For the `i`-th iterator element and bounds `min_backoff` and `max_backoff`, this returns the
/// sequence `min(max_backoff, 2^i * min_backoff + rand_i)`.
#[derive(Debug)]
pub(crate) struct ExponentialBackoff<R> {
    min_backoff: Duration,
    max_backoff: Duration,
    sequence_index: u32,
    rand_offsets: DistIter<Uniform<u64>, R, u64>,
}

impl ExponentialBackoff<StdRng> {
    /// Maximum number of milliseconds to randomly add to the delay time.
    const MAX_RAND_OFFSET_MS: u64 = 1000;

    /// Creates a new iterator with the provided minimum and maximum bounds,
    /// and seeded with the provided value.
    pub fn new_with_seed(
        min_backoff: Duration,
        max_backoff: Duration,
        seed: u64,
    ) -> ExponentialBackoff<StdRng> {
        ExponentialBackoff::<StdRng>::new_with_rng(
            min_backoff,
            max_backoff,
            StdRng::seed_from_u64(seed),
        )
    }
}

impl<R: Rng> ExponentialBackoff<R> {
    /// Creates a new iterator with the provided minimum and maximum bounds, with the provided
    /// iterator.
    pub fn new_with_rng(min_backoff: Duration, max_backoff: Duration, rng: R) -> Self {
        let uniform = Uniform::new_inclusive(0, ExponentialBackoff::MAX_RAND_OFFSET_MS);
        let rand_offsets = rng.sample_iter(uniform);

        Self {
            min_backoff,
            max_backoff,
            sequence_index: 0,
            rand_offsets,
        }
    }

    /// Steps the iterator, returning the next delay and advances the backoff.
    pub fn next_delay(&mut self) -> Duration {
        let next_delay_value = self
            .min_backoff
            .saturating_mul(Saturating(2u32).pow(self.sequence_index).0)
            .saturating_add(self.random_offset())
            .min(self.max_backoff);

        self.sequence_index = self.sequence_index.saturating_add(1);

        next_delay_value
    }

    fn random_offset(&mut self) -> Duration {
        let millis = self
            .rand_offsets
            .next()
            .expect("infinite sequence of random values");
        Duration::from_millis(millis)
    }
}

impl<R: Rng> Iterator for ExponentialBackoff<R> {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_delay())
    }
}

pub(crate) trait SuccessOrFailure {
    fn is_success(&self) -> bool;
}

impl<T, E> SuccessOrFailure for Result<T, E> {
    fn is_success(&self) -> bool {
        self.is_ok()
    }
}

impl<T> SuccessOrFailure for Option<T> {
    fn is_success(&self) -> bool {
        self.is_some()
    }
}

pub(crate) trait FutureHelpers: Future {
    async fn limit(self, permits: Arc<Semaphore>) -> Self::Output
    where
        <Self as Future>::Output: SuccessOrFailure,
        Self: Sized,
    {
        let permit = permits
            .acquire_owned()
            .await
            .expect("semaphore never closed");

        self.inspect(|result| {
            if result.is_success() {
                permit.forget()
            }
        })
        .await
    }

    async fn timeout_after<T>(self, duration: Duration) -> <Self as Future>::Output
    where
        Self: Sized,
        Self: Future<Output = Option<T>>,
    {
        match tokio::time::timeout(duration, self).await {
            Ok(output) => output,
            Err(_) => {
                tracing::debug!("request timed out");
                None
            }
        }
    }
}

impl<T: Future> FutureHelpers for T {}

pub(crate) async fn retry<R, F, T, Fut>(mut strategy: ExponentialBackoff<R>, mut func: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
    R: rand::RngCore,
{
    loop {
        if let Some(value) = func().await {
            return value;
        }

        let delay = strategy.next().expect("infinite iterator");
        tracing::debug!(?delay, "attempt failed, waiting before retrying");
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod exponential_backoff {
        use super::*;

        #[test]
        fn backoff_is_exponential() {
            let min = Duration::from_millis(500);
            let max = Duration::from_secs(3600);

            let expected: Vec<_> = (0u32..)
                .map(|i| min * 2u32.pow(i))
                .take_while(|d| *d < max)
                .chain([max; 2])
                .collect();

            let actual: Vec<_> = ExponentialBackoff::new_with_seed(min, max, 42)
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
    }
}
