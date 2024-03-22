// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{pin::Pin, time::Duration};

use anyhow::{anyhow, Result};
use futures::{stream::FuturesUnordered, Stream, StreamExt};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use walrus_service::server::ServiceResponse;

/// Takes a [`Response`], deserializes the json content, removes the [`ServiceResponse`] and
/// returns the contained type.
pub(crate) async fn unwrap_response<T>(response: Response) -> Result<T>
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    if response.status().is_success() {
        let body = response.json::<ServiceResponse<T>>().await?;
        match body {
            ServiceResponse::Success { code: _code, data } => Ok(data),
            ServiceResponse::Error { code, message } => Err(anyhow!(
                "unexpected error in the response: {code} {message}"
            )),
        }
    } else {
        Err(anyhow!("request failed with status: {}", response.status()))
    }
}

/// A Result that is weighted with an integer.
///
/// Used to represent the number of shards owned by the node that returned the result, to keep track
/// of how many correct responses we received towards the quorum.
pub(crate) type WeightedResult<T> = Result<(usize, T)>;

/// Returns the first weighted results of the futures that successfully complete, that amount to at
/// least `min_weight`.
///
/// Futures that return an error are not counted towards the total weight of the results.
pub(crate) async fn first_weighted_successes<T, U>(
    futures: &mut FuturesUnordered<T>,
    min_weight: usize,
) -> Result<Vec<U>>
where
    FuturesUnordered<T>: Stream<Item = WeightedResult<U>>,
{
    let mut results = vec![];
    let mut total = 0;
    while let Some(completed) = futures.next().await {
        if let Ok((weight, result)) = completed {
            results.push(result);
            total += weight;
            if total >= min_weight {
                break;
            }
        }
    }
    if results.len() >= min_weight {
        Ok(results)
    } else {
        Err(anyhow!("could not gather enough results"))
    }
}

/// A set of futures that can be awaited on for a certain time, or until a set count of futures
/// return successfully.
pub(crate) struct WeightedFutures<T, U> {
    futures: FuturesUnordered<T>,
    results: Vec<U>,
}

impl<T, U> WeightedFutures<T, U>
where
    FuturesUnordered<T>: Stream<Item = WeightedResult<U>>,
    T: futures::Future,
{
    /// Creates a new [`WeightedFutures`] struct from an iterator of futures.
    pub fn new<I>(futures: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        WeightedFutures {
            futures: futures.into_iter().collect(),
            results: vec![],
        }
    }

    /// Executes the futures until the first `k` futures return successfully without error.
    pub async fn execute_count(&mut self, k: usize) -> Result<&mut Self> {
        self.results
            .extend(first_weighted_successes(&mut self.futures, k).await?);
        Ok(self)
    }

    /// Executes the futures until the set duration is elapsed, collecting all the futures that
    /// return without error within this time.
    pub async fn execute_time(&mut self, duration: Duration) -> Result<&mut Self> {
        let stream = Pin::new(&mut self.futures).take_until(tokio::time::sleep(duration));
        let results = stream.collect::<Vec<_>>().await;
        self.results.extend(
            results
                .into_iter()
                // When executing by time we don't care about the weight.
                .filter_map(|r| r.ok().map(|(_weight, res)| res)),
        );
        Ok(self)
    }

    /// Returns a reference to the `results`.
    #[allow(dead_code)]
    pub fn results(&self) -> &Vec<U> {
        &self.results
    }

    /// Consumes the struct and returns the `results`.
    pub fn into_results(self) -> Vec<U> {
        self.results
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_weighted_futures() {
        // WARNING: potentially finicky test
        // NOTE(giac): should be fixed now but not deleting the warning yet. (TODO(giac))
        let futures = (0..10).map(|i| async move {
            tokio::time::sleep(Duration::from_millis((i + 1) * 10)).await;
            Ok((1, i)) // Every result has a weight of 1.
        });
        let mut tqf = WeightedFutures::new(futures);
        tqf.execute_count(3).await.unwrap();
        assert_eq!(tqf.results(), &vec![0, 1, 2]);
        // Add to the existing runtime (~30ms) another 32ms to get to ~62ms of total execution.
        tqf.execute_time(Duration::from_millis(32)).await.unwrap();
        assert_eq!(tqf.results(), &vec![0, 1, 2, 3, 4, 5]);
        tqf.execute_count(1).await.unwrap();
        assert_eq!(tqf.results(), &vec![0, 1, 2, 3, 4, 5, 6]);
    }
}
