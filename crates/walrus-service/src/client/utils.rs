// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{marker::PhantomData, time::Duration};

use anyhow::Result;
use futures::{stream::FuturesUnordered, Future, Stream, StreamExt};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use super::error::CommunicationError;
use crate::server::ServiceResponse;

/// Takes a [`Response`], deserializes the json content, removes the [`ServiceResponse`] and
/// returns the contained type.
pub(crate) async fn unwrap_response<T>(response: Response) -> Result<T, CommunicationError>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    if response.status().is_success() {
        let body = response.json::<ServiceResponse<T>>().await?;
        match body {
            ServiceResponse::Success { code: _code, data } => Ok(data),
            ServiceResponse::Error { code, message } => {
                Err(CommunicationError::ServiceResponseError { code, message })
            }
        }
    } else {
        Err(CommunicationError::HttpFailure(response.status()))
    }
}

/// A Result that is weighted with an integer.
///
/// Used to represent the number of shards owned by the node that returned the result, to keep track
/// of how many correct responses we received towards the quorum.
pub(crate) type WeightedResult<T, E> = Result<(usize, T), E>;

/// A set of weighted futures that return a [`WeightedResult`]. The futures can be awaited on for a
/// certain time, or until a set cumulative weight of futures return successfully.
pub(crate) struct WeightedFutures<I, Fut, T, E> {
    futures: I,
    being_executed: FuturesUnordered<Fut>,
    results: Vec<T>,
    // TODO(giac): remove phantomdata when the error propagation is completed.
    error: PhantomData<E>,
}

impl<I, Fut, T, E> WeightedFutures<I, Fut, T, E>
where
    I: Iterator<Item = Fut>,
    Fut: Future<Output = WeightedResult<T, E>>,
    FuturesUnordered<Fut>: Stream<Item = WeightedResult<T, E>>,
{
    /// Creates a new [`WeightedFutures`] struct from an iterator of futures.
    pub fn new(futures: I) -> Self {
        WeightedFutures {
            futures,
            being_executed: FuturesUnordered::new(),
            results: vec![],
            error: PhantomData,
        }
    }

    /// Executes the futures until futures that have a total weight of at least `threshold` return
    /// successfully without error.
    ///
    /// # Arguments
    ///
    /// * `threshold` - the minimum cumulative weight of [`WeightedResults`][WeightedResult] that is
    /// to be collected before the function returns and stops awaiting.  If all futures have
    /// returned before this `threshold` is reached, the function returns early and without errors.
    /// * `n_concurrent` - the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_weight(&mut self, threshold: usize, n_concurrent: usize) -> &mut Self {
        self.first_weighted_successes(Some(threshold), n_concurrent)
            .await
    }

    /// Executes the futures until the set duration is elapsed, collecting all the futures that
    /// return without error within this time.
    ///
    /// # Arguments
    ///
    /// * `duration` - the maximum duration to await the futures for. If all futures complete before
    /// this `duration` is elapsed, the function returns early.
    /// * `n_concurrent` - the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_time(&mut self, duration: Duration, n_concurrent: usize) -> &mut Self {
        let _ = timeout(duration, self.execute_all(n_concurrent)).await;
        self
    }

    /// Returns the next result returned by the futures. If a result is already present, it returns
    /// it directly. Otherwise, it awaits on the available futures to produce a new one.
    ///
    /// # Arguments
    ///
    /// * `n_concurrent` - the maximum number of futures that are awaited at any one time to produce
    /// results.
    ///
    /// Returns `None` if it cannot produce further results.
    pub async fn execute_next(&mut self, n_concurrent: usize) -> Option<T> {
        if self.results.is_empty() {
            // Execute to get the next result.
            self.first_weighted_successes(Some(1), n_concurrent).await;
        }
        self.results.pop()
    }

    /// Executes all futures provided.
    async fn execute_all(&mut self, n_concurrent: usize) -> &mut Self {
        self.first_weighted_successes(None, n_concurrent).await
    }

    /// Returns the first weighted results of the futures that successfully complete, that amount to
    /// at least `min_weight`.
    ///
    /// If the all the futures have completed, it returns the results that were successfully
    /// gathered, even though they may have a smaller weight than the minimum weight.
    ///
    /// Futures that return an error are not counted towards the total weight of the results.
    async fn first_weighted_successes(
        &mut self,
        threshold: Option<usize>,
        n_concurrent: usize,
    ) -> &mut Self {
        let mut total_weight = 0;
        // Refill the in-flight futures.
        while self.being_executed.len() < n_concurrent {
            if let Some(future) = self.futures.next() {
                self.being_executed.push(future);
            } else {
                break;
            }
        }
        while let Some(completed) = self.being_executed.next().await {
            if let Ok((weight, result)) = completed {
                self.results.push(result);
                total_weight += weight;
                if let Some(threshold) = threshold {
                    if total_weight >= threshold {
                        break;
                    }
                }
            }
            // Add more futures to the ones being awaited.
            if let Some(future) = self.futures.next() {
                self.being_executed.push(future);
            }
        }
        self
    }

    /// Returns a reference to the `results`.
    #[allow(dead_code)]
    pub fn results(&self) -> &Vec<T> {
        &self.results
    }

    /// Gets all the results in the struct, emptying `self.results`.
    pub fn take_results(&mut self) -> Vec<T> {
        std::mem::take(&mut self.results)
    }

    /// Consumes the struct and returns the `results`.
    pub fn into_results(self) -> Vec<T> {
        self.results
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;

    macro_rules! create_weighted_futures {
        ($var:ident, $iter:expr) => {
            let futures = $iter.into_iter().map(|&i| async move {
                tokio::time::sleep(Duration::from_millis((i) * 10)).await;
                Ok((1, i)) // Every result has a weight of 1.
            });
            let mut $var: WeightedFutures<_, _, _, anyhow::Error> = WeightedFutures::new(futures);
        };
    }

    #[tokio::test(start_paused = true)]
    async fn test_weighted_futures() {
        create_weighted_futures!(weighted_futures, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        weighted_futures.execute_weight(3, 10).await;
        assert_eq!(weighted_futures.results(), &vec![1, 2, 3]);
        // Add to the existing runtime (~30ms) another 32ms to get to ~62ms of total execution.
        weighted_futures
            .execute_time(Duration::from_millis(32), 10)
            .await;
        assert_eq!(weighted_futures.results(), &vec![1, 2, 3, 4, 5, 6]);
        weighted_futures.execute_weight(1, 10).await;
        assert_eq!(weighted_futures.results(), &vec![1, 2, 3, 4, 5, 6, 7]);
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
            weighted_futures.results(),
            &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
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
        assert_eq!(weighted_futures.results(), &vec![1, 1, 1, 1, 1]);
    }
}
