// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utility functions used internally in the crate.

use std::{num::Saturating, slice::Iter, time::Duration};

use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use walrus_core::bft;
use walrus_sui::types::{Committee, StorageNode as CommitteeMember};

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

impl ExponentialBackoff<StdRng> {
    /// Maximum number of milliseconds to randomly add to the delay time.
    const MAX_RAND_OFFSET_MS: u64 = 1000;

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
        Duration::from_millis(
            self.rng
                .gen_range(0..=ExponentialBackoff::MAX_RAND_OFFSET_MS),
        )
    }
}

impl<R: Rng> Iterator for ExponentialBackoff<R> {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.next_delay())
    }
}

/// Randomly samples from the committee.
pub(crate) struct CommitteeSampler<'a> {
    committee: &'a Committee,
    visit_order: Vec<usize>,
}

impl CommitteeSampler<'_> {
    pub fn new(committee: &Committee) -> CommitteeSampler<'_> {
        let indices = (0..committee.members().len()).collect();
        CommitteeSampler {
            committee,
            visit_order: indices,
        }
    }

    /// Sample members from the committee up to a quorum, returning their indices in the commmittee.
    #[cfg(test)]
    pub fn sample_quorum<R>(&mut self, rng: &mut R) -> CommitteeSample<fn(&CommitteeMember) -> bool>
    where
        R: Rng + ?Sized,
    {
        self.visit_order.shuffle(rng);
        CommitteeSample {
            committee: self.committee,
            visit_order: self.visit_order.iter(),
            total_weight: 0,
            threshold: bft::min_n_correct(self.committee.n_shards()).get().into(),
            predicate: |_| true,
        }
    }

    /// Sample members from the committee up to a quorum, returning their indices in the commmittee.
    ///
    /// Excludes members where the provided predicate evaluates to false.
    pub fn sample_quorum_filtered<R, P>(&mut self, predicate: P, rng: &mut R) -> CommitteeSample<P>
    where
        R: Rng + ?Sized,
        P: FnMut(&CommitteeMember) -> bool,
    {
        self.visit_order.shuffle(rng);

        CommitteeSample {
            committee: self.committee,
            visit_order: self.visit_order.iter(),
            total_weight: 0,
            threshold: bft::min_n_correct(self.committee.n_shards()).get().into(),
            predicate,
        }
    }
}

pub(crate) struct CommitteeSample<'a, P> {
    committee: &'a Committee,
    visit_order: Iter<'a, usize>,
    total_weight: usize,
    threshold: usize,
    predicate: P,
}

impl<P> Iterator for CommitteeSample<'_, P>
where
    P: FnMut(&CommitteeMember) -> bool,
{
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.total_weight >= self.threshold {
            return None;
        }

        // Advance the internal iterator until the next index that satisfies the predicate.
        let next_index = self
            .visit_order
            .find(|&&i| (self.predicate)(&self.committee.members()[i]))?;

        let weight = self.committee.members()[*next_index].shard_ids.len();
        self.total_weight = self.total_weight.saturating_add(weight);

        Some(*next_index)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::rngs::mock::StepRng;

    use super::*;
    use crate::test_utils;

    macro_rules! assert_all_unique {
        ($into_iter:expr) => {
            let original_count = $into_iter.len();
            let unique_count = $into_iter.into_iter().collect::<HashSet<_>>().len();
            assert_eq!(original_count, unique_count, "elements are not all unique");
        };
    }

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

    mod committee_sampler {

        use super::*;

        #[test]
        fn sample_quorum_filtered_excludes_based_on_predicate() {
            let weights = [1, 1, 2, 3, 3];
            let committee = test_utils::test_committee(&weights);

            let index_of_excluded_member = 2;
            let key_of_excluded_member = committee.members()[index_of_excluded_member]
                .public_key
                .clone();

            let sample_including: Vec<_> = CommitteeSampler::new(&committee)
                .sample_quorum_filtered(|_| true, &mut StepRng::new(42, 7))
                .collect();
            assert!(sample_including.contains(&index_of_excluded_member));

            // Use the same RNG, so that the same sample would be returned.
            let sample_excluding: Vec<_> = CommitteeSampler::new(&committee)
                .sample_quorum_filtered(
                    |member| member.public_key != key_of_excluded_member,
                    // Use the same RNG as above, to ensure the same set.
                    &mut StepRng::new(42, 7),
                )
                .collect();
            assert!(!sample_excluding.contains(&index_of_excluded_member));
        }

        #[test]
        fn sample_quorum_filtered_returns_all_if_less_than_2fp1() {
            let weights = [1, 1, 2, 3, 3];
            let committee = test_utils::test_committee(&weights);
            let mut rng = StepRng::new(39, 5);

            let weight_of_all_returned: u16 = CommitteeSampler::new(&committee)
                .sample_quorum_filtered(|member| member.shard_ids.len() != 3, &mut rng)
                .map(|i| weights[i])
                .sum();

            assert!(!committee.is_quorum(weight_of_all_returned.into()));
            assert_eq!(weight_of_all_returned, 4);
        }

        #[test]
        fn sample_quorum_returns_only_2fp1_shards() {
            let weights = [1, 1, 2, 3, 3];
            let committee = test_utils::test_committee(&weights);
            let mut rng = StepRng::new(42, 7);

            let sample: Vec<_> = CommitteeSampler::new(&committee)
                .sample_quorum(&mut rng)
                .collect();

            let last_weight = weights[*sample.last().unwrap()];

            let weight_of_all = sample.iter().map(|&i| weights[i]).sum::<u16>();
            let weight_of_all_except_last = weight_of_all - last_weight;

            assert!(!committee.is_quorum(weight_of_all_except_last.into()));
            assert!(committee.is_quorum(weight_of_all.into()));
        }

        #[test]
        fn sample_quorum_returns_unique_indices() {
            let committee = test_utils::test_committee(&[1; 10]);
            let n_nodes = committee.n_members();
            let mut rng = StepRng::new(42, 7);

            let sample: Vec<_> = CommitteeSampler::new(&committee)
                .sample_quorum(&mut rng)
                .collect();

            assert!(!sample.is_empty());
            assert!(*sample.iter().max().unwrap() < n_nodes);
            assert_all_unique!(sample);
        }

        #[test]
        fn sample_quorum_randomizes_indices_each_iteration() {
            let mut rng = StepRng::new(42, 7);
            let committee = test_utils::test_committee(&[1; 10]);
            let n_nodes = committee.n_members();

            let mut sampler = CommitteeSampler::new(&committee);

            let first_sample: Vec<_> = sampler.sample_quorum(&mut rng).collect();
            let second_sample: Vec<_> = sampler.sample_quorum(&mut rng).collect();

            assert_ne!(
                first_sample,
                (0..n_nodes).collect::<Vec<_>>(),
                "the order should be randomized"
            );
            assert_ne!(
                first_sample, second_sample,
                "the order after a reset should be different"
            );
        }
    }
}
