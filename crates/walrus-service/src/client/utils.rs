// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

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
    results: Vec<Result<T, E>>,
    total_weight: usize,
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
            total_weight: 0,
        }
    }

    /// Executes the futures until futures that have a total weight of at least `threshold` return
    /// successfully without error.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_weight(&mut self, threshold: usize, n_concurrent: usize) {
        self.total_weight = 0;
        while let Some(result) = self.next_threshold(n_concurrent, Some(threshold)).await {
            self.results.push(result);
        }
    }

    /// Executes the futures until the set `duration` is elapsed, collecting all the futures that
    /// return without error within this time.
    ///
    /// If all futures complete before the `duration` is elapsed, the function returns early.
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn execute_time(&mut self, duration: Duration, n_concurrent: usize) {
        let _ = timeout(duration, self.execute_all(n_concurrent)).await;
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
    pub async fn next(&mut self, n_concurrent: usize) -> Option<Result<T, E>> {
        self.next_threshold(n_concurrent, None).await
    }

    /// Returns the next result returned by the futures, up to the given cumulative threshold.  If
    /// `threshold` is not `None`, the function will return the results as long as the total weight
    /// of the _valid_ results (`Ok`) is _strictly_ `< threshold`.  Otherwise, if `threshold` is
    /// `None`, the function will return results until there are no more futures to await.  Returns
    /// `None` if it cannot produce further results or if the threshold has been passed.
    ///
    /// `n_concurrent` is the maximum number of futures that are awaited at any one time to produce
    /// results.
    pub async fn next_threshold(
        &mut self,
        n_concurrent: usize,
        threshold: Option<usize>,
    ) -> Option<Result<T, E>> {
        if let Some(threshold) = threshold {
            if self.total_weight >= threshold {
                return None;
            }
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
            match completed {
                Ok((weight, result)) => {
                    self.total_weight += weight;
                    Some(Ok(result))
                }
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }

    /// Returns a reference to the `results`.
    #[allow(dead_code)]
    pub fn results(&self) -> &Vec<Result<T, E>> {
        &self.results
    }

    /// Gets all the results in the struct, emptying `self.results`.
    pub fn take_results(&mut self) -> Vec<Result<T, E>> {
        std::mem::take(&mut self.results)
    }

    /// Consumes the struct and returns the `results`.
    pub fn into_results(self) -> Vec<Result<T, E>> {
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

    fn only_ok<T: Clone, E>(results: &[Result<T, E>]) -> Vec<T> {
        results
            .iter()
            .filter_map(|result| result.as_ref().ok().cloned())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn test_weighted_futures() {
        create_weighted_futures!(weighted_futures, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        weighted_futures.execute_weight(3, 10).await;
        assert_eq!(only_ok(weighted_futures.results()), vec![1, 2, 3]);
        // Add to the existing runtime (~30ms) another 32ms to get to ~62ms of total execution.
        weighted_futures
            .execute_time(Duration::from_millis(32), 10)
            .await;
        assert_eq!(only_ok(weighted_futures.results()), vec![1, 2, 3, 4, 5, 6]);
        weighted_futures.execute_weight(1, 10).await;
        assert_eq!(
            only_ok(weighted_futures.results()),
            vec![1, 2, 3, 4, 5, 6, 7]
        );
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
            only_ok(weighted_futures.results()),
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
        assert_eq!(only_ok(weighted_futures.results()), vec![1, 1, 1, 1, 1]);
    }
}
