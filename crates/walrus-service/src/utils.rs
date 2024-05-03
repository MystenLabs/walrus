// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::Saturating, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};

/// Exponentially spaced delays.
///
/// Use [`next_delay()`][Self::next_delay] to get the values or [`wait()`][Self::wait] to
/// asynchronously pause for the expected delay.
#[derive(Debug)]
pub(crate) struct ExponentialBackoff<R> {
    min_backoff: Duration,
    max_backoff: Duration,
    sequence_index: u32,
    rng: R,
}

impl<R> ExponentialBackoff<R> {
    /// Default amount of milliseconds to randomly add to the delay time.
    const DEFAULT_RAND_OFFSET_MS: u64 = 1000;

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
    pub fn new_with_rng(min_backoff: Duration, max_backoff: Duration, rng: R) -> Self {
        Self {
            min_backoff,
            max_backoff,
            sequence_index: 0,
            rng,
        }
    }

    /// Returns the next delay and advances the backoff.
    pub fn next_delay(&mut self) -> Duration {
        let next_delay_value = self
            .min_backoff
            .saturating_mul(Saturating(2u32).pow(self.sequence_index).0)
            .min(self.max_backoff);

        self.sequence_index = self.sequence_index.saturating_add(1);

        // Only add the random delay if we've not yet hit the maximum
        if next_delay_value < self.max_backoff {
            next_delay_value.saturating_add(self.random_offset())
        } else {
            next_delay_value
        }
    }

    /// Wait for the amount of time returned by [`next_delay()`][Self::next_delay].
    pub async fn wait(&mut self) {
        let wait = self.next_delay();
        tracing::debug!(?wait, "exponentially backing off");
        tokio::time::sleep(wait).await
    }

    fn random_offset(&mut self) -> Duration {
        Duration::from_millis(self.rng.gen_range(0..=Self::DEFAULT_RAND_OFFSET_MS))
    }
}

impl<R: Rng> Iterator for ExponentialBackoff<R> {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_delay())
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

            let actual: Vec<_> = ExponentialBackoff::<()>::new_with_seed(min, max, 42)
                .take(expected.len())
                .collect();

            assert_eq!(expected.len(), actual.len());
            assert_ne!(expected, actual, "retries must have a random component");

            for (expected, actual) in expected.iter().zip(actual) {
                let expected_min = *expected;
                let expected_max = *expected
                    + Duration::from_millis(ExponentialBackoff::<()>::DEFAULT_RAND_OFFSET_MS);

                assert!(actual >= expected_min, "{actual:?} >= {expected_min:?}");
                assert!(actual <= expected_max);
            }
        }
    }
}
