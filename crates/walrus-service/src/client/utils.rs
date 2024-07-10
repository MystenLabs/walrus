// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fmt::{self, Display},
    hash::Hash,
    time::Duration,
};

use anyhow::Result;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use tokio::time::timeout;

/// A trait representing a result that has a weight.
pub trait WeightedResult {
    /// The type `T` in the inner `Result<T,E>`.
    type Inner;
    /// The type `E` in the inner `Result<T,E>`.
    type Error;
    /// Returns true if the inner result is `Ok`.
    fn is_ok(&self) -> bool {
        self.inner_result().is_ok()
    }
    /// Returns true if the inner result is `Err`.
    #[allow(dead_code)]
    fn is_err(&self) -> bool {
        self.inner_result().is_err()
    }
    /// Returns the weight of the `WeightedResult`.
    fn weight(&self) -> usize;
    /// Converts `self` into an [`Option<Self>`], consuming `self`, and returning `None` if
    /// `self.is_err()`.
    #[allow(dead_code)]
    fn ok(self) -> Option<Self>
    where
        Self: Sized,
    {
        self.is_ok().then_some(self)
    }
    /// Converts `self` into an [`Option<T>`], where `T` is the type of the inner result, consuming
    /// `self`, and discarding the error, if any.
    #[allow(dead_code)]
    fn inner_ok(self) -> Option<Self::Inner>
    where
        Self: Sized,
    {
        self.take_inner_result().ok()
    }
    /// Returns a reference to the inner result.
    fn inner_result(&self) -> &Result<Self::Inner, Self::Error>;
    /// Returns the inner result, consuming `self`.
    #[allow(dead_code)]
    fn take_inner_result(self) -> Result<Self::Inner, Self::Error>;
}

/// A set of weighted futures that return a [`WeightedResult`]. The futures can be awaited on for a
/// certain time, or until a set cumulative weight of futures return successfully.
pub(crate) struct WeightedFutures<I, Fut, T> {
    futures: I,
    being_executed: FuturesUnordered<Fut>,
    results: Vec<T>,
    /// The cumulative weight of successful `WeightedResult`s that have been executed.
    ///
    /// This is necessary to to keep track of the weight of successful results across calls to
    /// `next_threshold`. Calls to `execute_weight` begin by resetting `total_weight = 0`.
    total_weight: usize,
}

impl<I, Fut, T> WeightedFutures<I, Fut, T>
where
    I: Iterator<Item = Fut>,
    Fut: Future<Output = T>,
    T: WeightedResult,
{
    /// Creates a new [`WeightedFutures`] struct from an iterator of futures.
    pub fn new(futures: I) -> Self {
        WeightedFutures {
            futures,
            being_executed: FuturesUnordered::new(),
            results: vec![],
            total_weight: 0,
        }
    }

    /// Executes the futures until the provided threshold is met or all futures have been executed.
    ///
    /// Stops executing in two cases:
    ///
    /// 1. If the `threshold` closure applied to `self.total_weight` returns `true`; in this case a
    ///    [`CompletedReasonWeight::ThresholdReached`] is returned.
    /// 1. If there are no more futures to execute; in this case a
    ///    [`CompletedReasonWeight::FuturesConsumed`] is returned containing the total weight of
    ///    successful futures.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_weight(
        &mut self,
        threshold: &impl Fn(usize) -> bool,
        n_concurrent: usize,
    ) -> CompletedReasonWeight {
        self.total_weight = 0;
        while let Some(result) = self.next_threshold(n_concurrent, threshold).await {
            self.results.push(result);
        }
        if threshold(self.total_weight) {
            CompletedReasonWeight::ThresholdReached
        } else {
            CompletedReasonWeight::FuturesConsumed(self.total_weight)
        }
    }

    /// Executes the futures until the set `duration` is elapsed, collecting all the futures that
    /// return without error within this time.
    ///
    /// If all futures complete before the `duration` is elapsed, the function returns early.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_time(
        &mut self,
        duration: Duration,
        n_concurrent: usize,
    ) -> CompletedReasonTime {
        match timeout(duration, self.execute_all(n_concurrent)).await {
            Ok(_) => CompletedReasonTime::FuturesConsumed,
            Err(_) => CompletedReasonTime::Timeout,
        }
    }

    pub async fn execute_all(&mut self, n_concurrent: usize) {
        while let Some(result) = self.next(n_concurrent).await {
            self.results.push(result);
        }
    }

    /// Returns the next result returned by the futures.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    ///
    /// Returns `None` if it cannot produce further results.
    pub async fn next(&mut self, n_concurrent: usize) -> Option<T> {
        self.next_threshold(n_concurrent, &|_weight| false).await
    }

    /// Returns the next result returned by the futures, up to the given cumulative threshold.
    ///
    /// Executes the futures, returns the results, and accumulate the weight of the _successful_
    /// results (`Ok`) in `total_weight`, as long as `threshold(total_weight) == false`. Then, when
    /// `threshold(total_weight) == true`, the function returns `None`.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn next_threshold(
        &mut self,
        n_concurrent: usize,
        threshold: &impl Fn(usize) -> bool,
    ) -> Option<T> {
        if threshold(self.total_weight) {
            return None;
        }

        while self.being_executed.len() < n_concurrent {
            if let Some(future) = self.futures.next() {
                self.being_executed.push(future);
            } else {
                break;
            }
        }
        if let Some(completed) = self.being_executed.next().await {
            // Add more futures to the ones being awaited.
            if let Some(future) = self.futures.next() {
                self.being_executed.push(future);
            }
            if completed.is_ok() {
                self.total_weight += completed.weight();
            }
            Some(completed)
        } else {
            None
        }
    }

    /// Gets all the results in the struct, consuming `self`.
    pub fn into_results(self) -> Vec<T> {
        self.results
    }

    /// Gets all the results in the struct, emptying `self.results`.
    pub fn take_results(&mut self) -> Vec<T> {
        std::mem::take(&mut self.results)
    }

    /// Gets all the `Ok` results in the struct, returning `T::Inner`, while discarding the errors
    /// and emptying `self.results`.
    #[allow(dead_code)]
    pub fn take_inner_ok(&mut self) -> Vec<T::Inner> {
        let results = self.take_results();
        results
            .into_iter()
            .filter_map(WeightedResult::inner_ok)
            .collect()
    }
}

impl<I, Fut, T> WeightedFutures<I, Fut, T>
where
    I: Iterator<Item = Fut>,
    Fut: Future<Output = T>,
    T: WeightedResult,
    T::Inner: Hash + Eq,
{
    /// Returns a [`HashMap`] mapping all successful results to the aggregated weight of requests
    /// that returned that result.
    pub fn take_unique_results_with_aggregate_weight(&mut self) -> HashMap<T::Inner, usize> {
        let mut unique_results = HashMap::new();
        for result in self.take_results() {
            let weight = result.weight();
            let value = result.inner_ok();
            if let Some(value) = value {
                unique_results
                    .entry(value)
                    .and_modify(|counter| *counter += weight)
                    .or_insert(weight);
            }
        }
        unique_results
    }
}

/// Represents the reason why the [`WeightedFutures::execute_weight`] completed.
pub enum CompletedReasonWeight {
    ThresholdReached,
    /// Contains the weight of successful futures.
    FuturesConsumed(usize),
}

/// Represents the reason why the [`WeightedFutures::execute_time`] completed.
pub enum CompletedReasonTime {
    Timeout,
    FuturesConsumed,
}

impl Display for CompletedReasonTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let reason = match self {
            Self::Timeout => "timeout elapsed",
            Self::FuturesConsumed => "all futures consumed",
        };
        write!(f, "{}", reason)
    }
}

/// Returns the first 8 characters of the string representing the object.
pub fn string_prefix<T: ToString>(s: &T) -> String {
    let mut string = s.to_string();
    string.truncate(8);
    format!("{}...", string)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;

    type SimpleWeightedResult = (usize, Result<u64>);
    impl WeightedResult for SimpleWeightedResult {
        type Inner = u64;
        type Error = anyhow::Error;
        fn weight(&self) -> usize {
            self.0
        }
        fn take_inner_result(self) -> Result<Self::Inner, Self::Error> {
            self.1
        }
        fn inner_result(&self) -> &Result<Self::Inner, Self::Error> {
            &self.1
        }
    }

    macro_rules! create_weighted_futures {
        ($var:ident, $iter:expr) => {
            let futures = $iter.into_iter().map(|&i| async move {
                tokio::time::sleep(Duration::from_millis((i) * 10)).await;
                (1, Ok(i)) // Every result has a weight of 1.
            });
            let mut $var = WeightedFutures::new(futures);
        };
    }

    #[tokio::test(start_paused = true)]
    async fn test_weighted_futures() {
        create_weighted_futures!(weighted_futures, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        weighted_futures.execute_weight(&|w| w >= 3, 10).await;
        assert_eq!(weighted_futures.take_inner_ok(), vec![1, 2, 3]);
        // Add to the existing runtime (~30ms) another 32ms to get to ~62ms of total execution.
        weighted_futures
            .execute_time(Duration::from_millis(32), 10)
            .await;
        assert_eq!(weighted_futures.take_inner_ok(), vec![4, 5, 6]);
        weighted_futures.execute_weight(&|w| w >= 1, 10).await;
        assert_eq!(weighted_futures.take_inner_ok(), vec![7]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_return_early() {
        // Ensures that the `WeightedFutures::execute_time` implementation returns once all the
        // futures have completed, and before the timer fires.
        create_weighted_futures!(weighted_futures, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let start = Instant::now();
        weighted_futures
            .execute_time(Duration::from_millis(1000), 10)
            .await;
        // `execute_time` should return within ~100 millis.
        println!("elapsed {:?}", start.elapsed());
        assert!(start.elapsed() < Duration::from_millis(200));
        assert_eq!(
            weighted_futures.take_inner_ok(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_execute_time_n_concurrent() {
        create_weighted_futures!(weighted_futures, &[1, 1, 1, 1, 1]);
        let start = Instant::now();
        weighted_futures
            // Execute them one by one, for a total of ~50ms
            .execute_time(Duration::from_millis(1000), 1)
            .await;
        println!("elapsed {:?}", start.elapsed());
        assert!(start.elapsed() < Duration::from_millis(70));
        assert_eq!(weighted_futures.take_inner_ok(), vec![1, 1, 1, 1, 1]);
    }
}
