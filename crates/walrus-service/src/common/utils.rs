// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Utility functions for the Walrus service.

use std::{
    collections::HashSet,
    fmt::Debug,
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::Saturating,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
    time::Duration,
};

use anyhow::{Context as _, Result};
use futures::future::FusedFuture;
use pin_project::pin_project;
use prometheus::{HistogramVec, Registry};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{
    de::{DeserializeOwned, Error},
    Deserialize,
    Deserializer,
};
use telemetry_subscribers::{TelemetryGuards, TracingHandle};
use tokio::{
    runtime::{self, Runtime},
    sync::Semaphore,
    time::Instant,
};

/// Defines a constant containing the version consisting of the package version and git revision.
///
/// We are using a macro as placing this logic into a library can result in unnecessary builds.
#[macro_export]
macro_rules! version {
    () => {{
        /// The Git revision obtained through `git describe` at compile time.
        const GIT_REVISION: &str = {
            if let Some(revision) = option_env!("GIT_REVISION") {
                revision
            } else {
                let version = git_version::git_version!(
                    args = ["--always", "--abbrev=12", "--dirty", "--exclude", "*"],
                    fallback = ""
                );
                if version.is_empty() {
                    panic!("unable to query git revision");
                }
                version
            }
        };

        // The version consisting of the package version and Git revision.
        walrus_core::concat_const_str!(env!("CARGO_PKG_VERSION"), "-", GIT_REVISION)
    }};
}
pub use version;
use walrus_core::{PublicKey, ShardIndex};

use super::active_committees::ActiveCommittees;

/// Trait for loading configuration from a YAML file.
pub trait LoadConfig: DeserializeOwned {
    /// Load the configuration from a YAML file located at the provided path.
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, anyhow::Error> {
        let path = path.as_ref();
        tracing::debug!(path = %path.display(), "reading config from file");

        let reader = std::fs::File::open(path)
            .with_context(|| format!("Unable to load config from {}", path.display()))?;

        Ok(serde_yaml::from_reader(reader)?)
    }
}

/// The representation of a backoff strategy.
pub(crate) trait BackoffStrategy {
    /// Steps the backoff iterator, returning the next delay and advances the backoff.
    ///
    /// Returns `None` if the strategy mandates that the consumer should stop backing off.
    fn next_delay(&mut self) -> Option<Duration>;
}

#[derive(Debug)]
pub(crate) struct ExponentialBackoffState {
    min_backoff: Duration,
    max_backoff: Duration,
    sequence_index: u32,
    max_retries: Option<u32>,
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
pub(crate) struct ExponentialBackoff<R> {
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
pub(crate) trait SuccessOrFailure {
    /// Returns true iff the value is considered successful.
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

impl SuccessOrFailure for bool {
    fn is_success(&self) -> bool {
        *self
    }
}

/// Helper functions applied to futures.
pub(crate) trait FutureHelpers: Future {
    /// Limits the number of simultaneously executing futures.
    #[cfg(feature = "client")]
    async fn batch_limit(self, permits: Arc<Semaphore>) -> Self::Output
    where
        Self: Future,
        Self: Sized,
    {
        let _permit = permits
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        self.await
    }

    /// Reports metrics for the future.
    fn observe<F, const N: usize>(
        self,
        histograms: HistogramVec,
        get_labels: F,
    ) -> Observe<Self, F, N>
    where
        Self: Sized,
        F: FnOnce(Option<&<Self as Future>::Output>) -> [&'static str; N],
    {
        Observe::new(self, histograms, get_labels)
    }
}

impl<T: Future> FutureHelpers for T {}

/// Structure that can wrap a future to report metrics for that future.
#[pin_project(PinnedDrop)]
#[derive(Debug)]
pub(crate) struct Observe<Fut, F, const N: usize>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    #[pin]
    inner: Fut,
    start: Instant,
    histograms: HistogramVec,
    // INV: Is Some until the future has completed.
    get_labels: Option<F>,
}

impl<Fut, F, const N: usize> Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    fn new(inner: Fut, histograms: HistogramVec, get_labels: F) -> Self {
        Self {
            inner,
            histograms,
            start: Instant::now(),
            get_labels: Some(get_labels),
        }
    }

    fn observe(self: Pin<&mut Self>, output: Option<&<Self as Future>::Output>) {
        let this = self.project();

        let Some(get_labels) = this.get_labels.take() else {
            panic!("must not be called if terminated");
        };

        this.histograms
            .with_label_values(&get_labels(output))
            .observe(this.start.elapsed().as_secs_f64());
    }
}

impl<Fut, F, const N: usize> FusedFuture for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    fn is_terminated(&self) -> bool {
        self.get_labels.is_none()
    }
}

impl<Fut, F, const N: usize> Future for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    type Output = Fut::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.is_terminated() {
            panic!("observe must not be called after it is terminated");
        }

        let this = self.as_mut().project();
        let output = ready!(this.inner.poll(cx));

        self.observe(Some(&output));

        Poll::Ready(output)
    }
}

#[pin_project::pinned_drop]
impl<Fut, F, const N: usize> PinnedDrop for Observe<Fut, F, N>
where
    Fut: Future,
    F: FnOnce(Option<&Fut::Output>) -> [&'static str; N],
{
    fn drop(self: Pin<&mut Self>) {
        if !self.is_terminated() {
            self.observe(None);
        }
    }
}

/// Repeatedly calls the provided function with the provided backoff strategy until it returns
/// successfully.
pub(crate) async fn retry<S, F, T, Fut>(mut strategy: S, mut func: F) -> T
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

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path: PathBuf = Deserialize::deserialize(deserializer)?;
    path_with_resolved_home_dir(path).map_err(D::Error::custom)
}

/// Can be used to deserialize optional paths such that the `~` is resolved to the user's home
/// directory.
pub fn resolve_home_dir_option<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path: Option<PathBuf> = Deserialize::deserialize(deserializer)?;
    let Some(path) = path else { return Ok(None) };
    Ok(Some(
        path_with_resolved_home_dir(path).map_err(D::Error::custom)?,
    ))
}

fn path_with_resolved_home_dir(path: PathBuf) -> Result<PathBuf> {
    if path.starts_with("~/") {
        let home = home::home_dir().context("unable to resolve home directory")?;
        return Ok(home.join(
            path.strip_prefix("~")
                .expect("we just checked for this prefix"),
        ));
    } else {
        Ok(path)
    }
}

/// A runtime for metrics and logging.
#[allow(missing_debug_implementations)]
pub struct MetricsAndLoggingRuntime {
    /// The Prometheus registry.
    pub registry: Registry,
    _telemetry_guards: TelemetryGuards,
    _tracing_handle: TracingHandle,
    /// The runtime for metrics and logging.
    // INV: Runtime must be dropped last.
    pub runtime: Runtime,
}

impl MetricsAndLoggingRuntime {
    /// Start metrics and log collection in a new runtime
    pub fn start(mut metrics_address: SocketAddr) -> anyhow::Result<Self> {
        let runtime = runtime::Builder::new_multi_thread()
            .thread_name("metrics-runtime")
            .worker_threads(2)
            .enable_all()
            .build()
            .context("metrics runtime creation failed")?;
        let _guard = runtime.enter();

        metrics_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let registry_service = mysten_metrics::start_prometheus_server(metrics_address);
        let walrus_registry = registry_service.default_registry();

        // Initialize logging subscriber
        let (telemetry_guards, tracing_handle) = telemetry_subscribers::TelemetryConfig::new()
            .with_env()
            .with_prom_registry(&walrus_registry)
            .with_log_level("debug")
            .init();

        Ok(Self {
            runtime,
            registry: walrus_registry,
            _telemetry_guards: telemetry_guards,
            _tracing_handle: tracing_handle,
        })
    }
}

/// The difference between shard allocations in different epochs.
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub(crate) struct ShardDiff {
    /// Shards lost from the assignment.
    pub lost: Vec<ShardIndex>,
    /// Shards gained in the assignment.
    pub gained: Vec<ShardIndex>,
    /// Shards which are common to both assignments.
    pub unchanged: Vec<ShardIndex>,
}

impl ShardDiff {
    /// Returns a new `ShardDiff` when moving from the allocation in
    /// `committees.previous_committee()` to `committees.current_committee()` for the node
    /// identified by the provided public key.
    #[allow(unused)]
    pub fn diff_previous(committees: &ActiveCommittees, id: &PublicKey) -> ShardDiff {
        let from: &[ShardIndex] = committees
            .previous_committee()
            .map_or(&[], |committee| committee.shards_for_node_public_key(id));
        let to = committees
            .current_committee()
            .shards_for_node_public_key(id);
        Self::diff(from, to)
    }

    /// Returns a new `ShardDiff` when moving from the allocation in `from` to `to`.
    pub fn diff(from: &[ShardIndex], to: &[ShardIndex]) -> ShardDiff {
        let from: HashSet<ShardIndex> = from.iter().copied().collect();
        let to: HashSet<ShardIndex> = to.iter().copied().collect();

        ShardDiff {
            unchanged: from.intersection(&to).copied().collect(),
            lost: from.difference(&to).copied().collect(),
            gained: to.difference(&from).copied().collect(),
        }
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
}
