// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Implementation of a simple value cache with element expiration.
//!
//! The cache can be used for replay suppression purposes, where each element is
//! the unique ID of a request.
//!
//! The uses an "actor" pattern, where it accepts requests from other tasks
//! through an mpsc channel and responds through the given oneshot channel.
//! Other tasks can either check the existence of an element in the cache or
//! insert a new element into the cache.
//!
//! The cache has a maximum size; if an insertion is attempted when the cache is
//! full, and there are no expired elements, an warning is returned and the
//! element is not inserted.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Debug, Display},
    hash::Hash,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{DurationSeconds, serde_as};
use tokio::sync::{mpsc, oneshot};

use super::auth::PublisherAuthError;

pub(crate) const DEFAULT_CACHE_CHANNEL_SIZE: usize = 100;

/// The configuration for the cache.
#[serde_as]
#[derive(Debug, Clone, clap::Parser, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[command(rename_all = "kebab-case")]
#[serde(rename_all = "camelCase")]
pub struct CacheConfig {
    /// The maximum number of elements the cache can hold.
    #[arg(long = "jwt-cache-size", default_value_t = default::max_size())]
    max_size: usize,
    /// The interval at which the cache should check for expired elements.
    #[serde(rename = "refresh_interval_secs")]
    #[arg(
        long = "jwt-cache-refresh-interval",
        value_parser = humantime::parse_duration,
        default_value = "5s"
    )]
    #[serde_as(as = "DurationSeconds")]
    pub(crate) refresh_interval: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: default::max_size(),
            refresh_interval: default::refresh_interval(),
        }
    }
}

mod default {
    pub(crate) fn max_size() -> usize {
        10_000
    }

    pub(crate) fn refresh_interval() -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }
}

impl CacheConfig {
    /// Builds a cache from the config.
    pub(crate) fn build<T>(&self) -> (Cache<T>, CacheHandle<T>)
    where
        T: Eq + Debug + Hash,
    {
        let (insert_tx, query_rx) = mpsc::channel(DEFAULT_CACHE_CHANNEL_SIZE);
        let cache = Cache::new(self.max_size, self.refresh_interval, query_rx);
        let handle = CacheHandle {
            request_tx: insert_tx,
        };

        (cache, handle)
    }

    /// Builds a cache from the config, and runs it a new task.
    pub(crate) fn build_and_run<T>(&self) -> CacheHandle<T>
    where
        T: Eq + Debug + Hash + Send + Sync + 'static,
    {
        let (mut cache, handle) = self.build();
        tokio::spawn(async move { cache.run().await });
        handle
    }
}

/// A request to the cache.
#[derive(Debug)]
pub(crate) struct CacheRequest<T> {
    /// The kind of request.
    kind: CacheRequestKind<T>,
    /// The channel used to send the response to the request.
    response_tx: oneshot::Sender<CacheResponse>,
}

/// The kind of request to the cache.
#[derive(Debug)]
pub(crate) enum CacheRequestKind<T> {
    /// Check if an element exists in the cache.
    #[allow(unused)]
    Exists(T),
    /// Insert an element into the cache.
    Insert((T, DateTime<Utc>)),
    /// Remove an element from the cache.
    Remove(T),
}

/// The response to a request to the cache.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CacheResponse {
    /// Whether the element exists in the cache.
    Exists(bool),
    /// The cache is full and the element could not be inserted.
    CacheFull,
}

/// A simple value cache with element expiration.
pub(crate) struct Cache<T> {
    /// The maximum number of elements the cache can hold.
    max_size: usize,
    /// The elements in the cache, mapped to their expiration time.
    elements: HashMap<T, DateTime<Utc>>,
    /// The interval at which the cache should check for expired elements.
    refresh_interval: Duration,
    /// The channel used to receive requests from other tasks.
    query_rx: mpsc::Receiver<CacheRequest<T>>,
}

impl<T> Cache<T>
where
    T: Eq + Debug + Hash,
{
    /// Create a new cache with the given maximum size.
    pub(crate) fn new(
        max_size: usize,
        refresh_interval: Duration,
        query_rx: mpsc::Receiver<CacheRequest<T>>,
    ) -> Self {
        Self {
            max_size,
            elements: HashMap::new(),
            refresh_interval,
            query_rx,
        }
    }

    /// Runs the cache, handling requests and checking for expired elements.
    pub(crate) async fn run(&mut self) -> anyhow::Result<()> {
        let mut refresh_interval = tokio::time::interval(self.refresh_interval);
        loop {
            tokio::select! {
                request = self.query_rx.recv() => {
                    if let Some(request) = request {
                        self.handle_request(request);
                    } else {
                        tracing::info!("the channel is closed, stopping the cache");
                        break;
                    }
                },
                _ = refresh_interval.tick() =>
                {
                    tracing::debug!("auto-refreshing cache");
                    self.evict_expired();
                }
            }
        }

        Ok(())
    }

    /// Handles a request to the cache.
    fn handle_request(&mut self, request: CacheRequest<T>) {
        let response = match request.kind {
            CacheRequestKind::Exists(key) => {
                tracing::debug!(key=?key, "checking if element exists in the cache");
                let exists = self.elements.contains_key(&key);
                CacheResponse::Exists(exists)
            }

            CacheRequestKind::Insert((key, expiration)) => {
                tracing::debug!(
                    key=?key,
                    expiration=?expiration,
                    "inserting element into the cache"
                );
                match self.has_space_or_try_evict() {
                    Ok(_) => {
                        tracing::debug!("cache has space; inserting element");
                        // Insert only if the key is not already present.
                        if let Entry::Vacant(entry) = self.elements.entry(key) {
                            entry.insert(expiration);
                            CacheResponse::Exists(false)
                        } else {
                            tracing::warn!("key already present in the cache; not inserting");
                            CacheResponse::Exists(true)
                        }
                    }
                    Err(_) => {
                        tracing::warn!("failed to free space in the cache");
                        CacheResponse::CacheFull
                    }
                }
            }
            CacheRequestKind::Remove(key) => {
                tracing::debug!(key=?key, "removing element from the cache");
                self.elements.remove(&key);
                CacheResponse::Exists(false)
            }
        };

        let _ = request.response_tx.send(response).inspect_err(|_| {
            // The receiver has been dropped.
            tracing::debug!("failed to send back the response from the cache")
        });
    }

    /// Checks if there is still space in the cache, and attempts to free space if not.
    ///
    /// If the cache is still full after eviction, returns an error.
    fn has_space_or_try_evict(&mut self) -> anyhow::Result<()> {
        if self.elements.len() < self.max_size {
            tracing::trace!("cache has space; no eviction needed");
            return Ok(());
        }

        tracing::trace!("cache is full; evicting expired elements");
        self.evict_expired();

        if self.elements.len() < self.max_size {
            tracing::trace!("cache has space after eviction");
            Ok(())
        } else {
            tracing::trace!("cache is still full after eviction");
            Err(anyhow::anyhow!("cache is still full after eviction"))
        }
    }

    /// Evicts expired elements from the cache.
    fn evict_expired(&mut self) {
        let now = Utc::now();
        self.elements.retain(|_, expiration| *expiration > now);
    }
}

/// Errors returned by the cache handle.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CacheError<T>
where
    T: Debug,
{
    /// The key is already present in the cache.
    #[error("the key is already present in the cache: {0}")]
    KeyAlreadyPresent(T),
    /// The cache is full.
    #[error("the cache is full")]
    CacheFull,
    /// Error sending to the cache.
    #[error("error sending to the cache: {0}")]
    Send(#[from] mpsc::error::SendError<CacheRequest<T>>),
    /// Error receiving from the cache.
    #[error("error receiving from the cache: {0}")]
    Receive(#[from] oneshot::error::RecvError),
}

impl<T> From<CacheError<T>> for PublisherAuthError
where
    T: Debug + Display + Send + Sync + 'static,
{
    fn from(error: CacheError<T>) -> Self {
        match error {
            CacheError::KeyAlreadyPresent(_) => PublisherAuthError::TokenAlreadyUsed,
            CacheError::CacheFull | CacheError::Send(_) | CacheError::Receive(_) => {
                PublisherAuthError::Internal(anyhow::anyhow!(error))
            }
        }
    }
}

/// A handle to the cache, used to send requests to the cache.
#[derive(Debug, Clone)]
pub(crate) struct CacheHandle<T> {
    /// The channel used to send requests to the cache.
    request_tx: mpsc::Sender<CacheRequest<T>>,
}

impl<T> CacheHandle<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Sends a request to the cache, and returns the response.
    async fn send(&self, request: CacheRequestKind<T>) -> Result<CacheResponse, CacheError<T>> {
        let (response_tx, response_rx) = oneshot::channel();
        self.request_tx
            .send(CacheRequest {
                kind: request,
                response_tx,
            })
            .await?;
        Ok(response_rx.await?)
    }

    /// Checks if an element exists in the cache.
    #[allow(unused)]
    pub(crate) async fn exists(&self, key: T) -> Result<bool, CacheError<T>> {
        match self.send(CacheRequestKind::Exists(key)).await? {
            CacheResponse::Exists(exists) => Ok(exists),
            CacheResponse::CacheFull => {
                panic!("invalid response from the cache; this should never happen")
            }
        }
    }

    /// Inserts an element into the cache.
    pub(crate) async fn insert_if_not_present(
        &self,
        key: T,
        expiration: DateTime<Utc>,
    ) -> Result<(), CacheError<T>> {
        match self
            .send(CacheRequestKind::Insert((key.clone(), expiration)))
            .await?
        {
            CacheResponse::Exists(false) => Ok(()),
            CacheResponse::Exists(true) => Err(CacheError::KeyAlreadyPresent(key)),
            CacheResponse::CacheFull => Err(CacheError::CacheFull),
        }
    }

    /// Removes an element from the cache.
    pub(crate) async fn remove(&self, key: T) -> Result<(), CacheError<T>> {
        match self.send(CacheRequestKind::Remove(key)).await? {
            CacheResponse::Exists(false) => Ok(()),
            CacheResponse::Exists(true) | CacheResponse::CacheFull => {
                panic!("invalid response from the cache; this should never happen")
            }
        }
    }
}

#[cfg(all(test, not(msim)))]
mod tests {
    use walrus_test_utils::async_param_test;

    use super::*;

    // 3025-01-01 00:00:00 UTC
    const EXP_FAR: DateTime<Utc> =
        DateTime::from_timestamp(33292598400, 0).expect("this is a valid timestamp");

    // Runs a cache and accepts a vector of requests. Returns a vector of responses.
    async fn run_cache(
        requests: Vec<CacheRequestKind<String>>,
    ) -> Vec<Result<CacheResponse, CacheError<String>>> {
        let handle = CacheConfig::default().build_and_run();

        let mut responses = Vec::new();
        for request in requests {
            let response = handle.send(request).await;
            responses.push(response);
        }

        responses
    }

    async_param_test! {
        run_cache_and_check_responses: [
            nonexistent: (
                vec![CacheRequestKind::Exists("test".to_owned())],
                vec![CacheResponse::Exists(false)]
            ),
            insert: (
                vec![
                    CacheRequestKind::Insert(("test".to_owned(), EXP_FAR)),
                    CacheRequestKind::Exists("test".to_owned())
                ],
                vec![CacheResponse::Exists(false), CacheResponse::Exists(true)]
            ),
            insert_existing: (
                vec![
                    CacheRequestKind::Insert(("test".to_owned(), EXP_FAR)),
                    CacheRequestKind::Insert(("test".to_owned(), EXP_FAR)),
                ],
                vec![
                    CacheResponse::Exists(false),
                    CacheResponse::Exists(true),
                ]
            ),
            remove: (
                vec![
                    CacheRequestKind::Insert(("test".to_owned(), EXP_FAR)),
                    CacheRequestKind::Remove("test".to_owned()),
                    CacheRequestKind::Exists("test".to_owned())
                ],
                vec![
                    CacheResponse::Exists(false),
                    CacheResponse::Exists(false),
                    CacheResponse::Exists(false)
                ]
            )
        ]
    }
    async fn run_cache_and_check_responses(
        requests: Vec<CacheRequestKind<String>>,
        expected_responses: Vec<CacheResponse>,
    ) {
        let responses = run_cache(requests).await;
        for (response, expected_response) in
            responses.into_iter().zip(expected_responses.into_iter())
        {
            assert_eq!(response.expect("response is ok"), expected_response);
        }
    }

    /// Tests the cache handle commands.
    #[tokio::test]
    async fn handle_commands() {
        let handle = CacheConfig::default().build_and_run();
        handle
            .insert_if_not_present("test".to_owned(), EXP_FAR)
            .await
            .expect("cache is empty");
        let exists = handle
            .exists("test".to_owned())
            .await
            .expect("exists does not fail");
        assert!(exists);
        handle
            .remove("test".to_owned())
            .await
            .expect("remove does not fail");
        let exists: bool = handle
            .exists("test".to_owned())
            .await
            .expect("exists does not fail");
        assert!(!exists);

        // Try inserting an existing key.
        handle
            .insert_if_not_present("test".to_owned(), EXP_FAR)
            .await
            .expect("first insertion after removal is ok");
        let result = handle
            .insert_if_not_present("test".to_owned(), EXP_FAR)
            .await;

        assert!(matches!(result, Err(CacheError::KeyAlreadyPresent(_))));
    }
}
