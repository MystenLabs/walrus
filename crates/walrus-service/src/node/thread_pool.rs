// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    any::Any,
    future::Future,
    num::NonZero,
    panic,
    pin::Pin,
    sync::Arc,
    task::{self, Context, Poll},
    time::Duration,
};

use futures::FutureExt;
#[cfg(unix)]
use libc;
use prometheus::{HistogramVec, IntGaugeVec};
use rayon::ThreadPoolBuilder as RayonThreadPoolBuilder;
use tokio::{
    sync::oneshot::{self, Receiver as OneshotReceiver},
    time::Instant,
};
use tower::{Service, limit::ConcurrencyLimit};
use tracing::span::Span;
use walrus_utils::metrics::{OwnedGaugeGuard, Registry};

use self::metrics::{STATUS_IN_PROGRESS, STATUS_QUEUED};
use super::metrics;

pub(crate) type ThreadPanicError = Box<dyn Any + Send + 'static>;

/// The assumed mean amount of time for processing requests in the thread pool.
///
/// If this value is higher than the actual, then the actual wait time in the queue will be lower
/// than [`DEFAULT_MAX_WAIT_TIME`]. If this value is lower than the actual value, then the actual
/// wait time in the queue with be higher than [`DEFAULT_MAX_WAIT_TIME`].
//
// As the service is currently only used to serve recovery symbols, this is calculated from that.
// First, the time taken to encode + construct the merkle tree for the sliver of a blob of size
// 50 MB (~250 B symbols) in a system with 1000 shards, as per the `merkle_tree` benchmark, is
// around 1 ms. However, the service is usually polled before touching the database, and so we must
// include time for that access as well. We therefore currently set this to 10 ms on span latencies
// for `retrieve_recovery_symbol` from grafana.
pub(crate) const ASSUMED_REQUEST_LATENCY: Duration = Duration::from_millis(10);

/// Given [`ASSUMED_REQUEST_LATENCY`], the maximum amount of time for which we are okay with a
/// task being queued. Used to calculate the default max number of in-flight requests for
/// [`BlockingThreadPool::bounded`].
pub(crate) const DEFAULT_MAX_WAIT_TIME: Duration = Duration::from_secs(1);

/// The default number of workers for the tokio blocking pool.
pub(crate) const DEFAULT_TOKIO_BLOCKING_POOL_NUM_WORKERS: usize = 4;

walrus_utils::metrics::define_metric_set! {
    #[namespace = "walrus_thread_pool"]
    struct ThreadPoolMetrics {
        #[help = "The latency (in seconds) of both queued and processing operations on the queue."]
        latency_seconds: HistogramVec {
            labels: ["pool", "status"],
            // Buckets from around 1ms up to 30s
            buckets: prometheus::exponential_buckets(0.001, 2.0, 16)
                .expect("count, start, and factor are valid")
        },

        #[help = "The number of tasks queued to the thread-pool"]
        queue_length: IntGaugeVec["pool"],
    }

}

#[derive(Default)]
pub(crate) struct ThreadPoolBuilder {
    metrics_registry: Option<Registry>,
    max_concurrent: Option<usize>,
    thread_pool: Option<BlockingThreadPoolInner>,
    pool_name: Option<&'static str>,
    pool_thread_name: Option<&'static str>,
    nice_level: Option<i32>,
}

impl ThreadPoolBuilder {
    /// Sets the prometheus registry on which to store metrics.
    ///
    /// Defaults to [`prometheus::default_registry()`] if unspecified.
    pub fn metrics_registry(&mut self, registry: Registry) -> &mut Self {
        self.metrics_registry = Some(registry);
        self
    }

    /// Sets the maximum number of concurrent requests to use when building a [`BoundedThreadPool`]
    /// with [`ThreadPoolBuilder::build_bounded`].
    ///
    /// Reverts the the default computation if `None`.
    pub fn max_concurrent(&mut self, max_concurrent: Option<usize>) -> &mut Self {
        self.max_concurrent = max_concurrent;
        self
    }

    /// Sets the name of the pool, used as a label in Prometheus metrics to distinguish multiple
    /// pools registered against the same registry. Defaults to `"default"` if unspecified.
    pub fn name(&mut self, name: &'static str) -> &mut Self {
        self.pool_name = Some(name);
        self
    }

    pub fn thread_name(&mut self, thread_name: &'static str) -> &mut Self {
        self.pool_thread_name = Some(thread_name);
        self
    }

    pub fn nice_level(&mut self, nice_level: i32) -> &mut Self {
        self.nice_level = Some(nice_level);
        self
    }

    #[cfg(test)]
    pub(crate) fn rayon(&mut self, pool: RayonThreadPool) -> &mut Self {
        self.thread_pool = Some(BlockingThreadPoolInner::Rayon(pool));
        self
    }

    #[cfg(test)]
    pub(crate) fn tokio(&mut self, pool: TokioBlockingPool) -> &mut Self {
        self.thread_pool = Some(BlockingThreadPoolInner::Tokio(pool));
        self
    }

    #[cfg(not(msim))]
    fn make_inner(&mut self) -> BlockingThreadPoolInner {
        self.thread_pool.take().unwrap_or_else(|| {
            BlockingThreadPoolInner::Rayon(RayonThreadPool::build(
                self.pool_thread_name.unwrap_or("worker"),
                self.nice_level,
            ))
        })
    }

    #[cfg(msim)]
    fn make_inner(&mut self) -> BlockingThreadPoolInner {
        self.thread_pool
            .take()
            .unwrap_or_else(|| BlockingThreadPoolInner::Tokio(TokioBlockingPool::default()))
    }

    /// Creates a [`BlockingThreadPool`] with the specified configuration.
    pub fn build(&mut self) -> BlockingThreadPool {
        let inner = self.make_inner();
        let pool_name = self.pool_name.unwrap_or("default");
        let metrics = ThreadPoolMetrics::new(&self.metrics_registry.clone().unwrap_or_default());

        BlockingThreadPool {
            inner,
            metrics,
            pool_name,
        }
    }

    /// Creates a [`BoundedThreadPool`] with the specified configuration.
    pub fn build_bounded(&mut self) -> BoundedThreadPool {
        let pool = self.build();
        if let Some(max_concurrent) = self.max_concurrent {
            pool.bounded_with_limit(max_concurrent)
        } else {
            pool.bounded()
        }
    }
}

/// A [`BlockingThreadPool`] with a limit to the number of concurrent jobs.
pub(crate) type BoundedThreadPool = ConcurrencyLimit<BlockingThreadPool>;

/// Extract the result from a [`std::thread::Result`] or resume the panic that caused
/// the thread to exit.
pub(crate) fn unwrap_or_resume_panic<T>(result: std::thread::Result<T>) -> T {
    match result {
        Ok(value) => value,
        Err(panic_payload) => {
            std::panic::resume_unwind(panic_payload);
        }
    }
}

/// A cloneable [`tower::Service`] around a [`rayon::ThreadPool`].
///
/// Accepts functions as requests (`FnOnce() -> T`), runs them on the thread-pool and
/// returns the result.
///
/// The error type of the service is returned if the function panics and
/// is the result of [`std::panic::catch_unwind`].
#[derive(Debug, Clone)]
pub(crate) struct RayonThreadPool {
    inner: Arc<rayon::ThreadPool>,
}

impl Default for RayonThreadPool {
    fn default() -> Self {
        RayonThreadPool::new(
            RayonThreadPoolBuilder::new()
                .thread_name(|i| format!("rayon-worker-{i}"))
                .build()
                .expect("non-argument thread-pool construction must succeed")
                .into(),
        )
    }
}

impl RayonThreadPool {
    /// Returns a new `RayonThreadPool` service.
    pub fn new(pool: Arc<rayon::ThreadPool>) -> Self {
        // Note that in simtest, we cannot use Rayon thread pool due to that it causes simtest to be
        // non-deterministic. This is because Rayon threads are actually run in different threads
        // than the tokio main threads, and not controlled by the tokio runtime.
        if cfg!(msim) {
            panic!("`RayonThreadPool` cannot be used in msim and so must not be constructed");
        }
        Self { inner: pool }
    }

    pub fn build(name: &'static str, nice_level: Option<i32>) -> Self {
        let pool = RayonThreadPoolBuilder::new()
            .thread_name(move |i| format!("{name}-{i}"))
            .start_handler(move |_| {
                #[cfg(unix)]
                if let Some(nice_level) = nice_level
                    && nice_level != 0
                {
                    let ret = unsafe { libc::nice(nice_level) };
                    if ret == -1 {
                        tracing::warn!(
                            nice_level,
                            "failed to set nice level for rayon thread pool worker"
                        );
                    }
                }
                #[cfg(not(unix))]
                let _ = nice_level;
            })
            .build()
            .expect("rayon thread pool construction must succeed");
        Self::new(Arc::new(pool))
    }
}

impl<Req, Resp> Service<Req> for RayonThreadPool
where
    Req: FnOnce() -> Resp + Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Error = ThreadPanicError;
    type Future = ThreadPoolFuture<Resp>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let pool = self.inner.clone();
        let (tx, rx) = oneshot::channel();
        let span = Span::current();

        pool.spawn(move || {
            let _guard = span.entered();
            let output = panic::catch_unwind(panic::AssertUnwindSafe(req));
            let _ = tx.send(output);
        });

        ThreadPoolFuture { rx }
    }
}

/// A cloneable [`tower::Service`] around a [`tokio::task::spawn_blocking`].
///
/// Accepts functions as requests (`FnOnce() -> T`), runs them on the thread-pool and
/// returns the result.
///
/// The error type of the service is returned if the function panics and
/// is the result of [`std::panic::catch_unwind`].
#[derive(Debug, Clone, Default)]
pub(crate) struct TokioBlockingPool(());

impl TokioBlockingPool {
    /// Returns a new `TokioBlockingPool` service.
    #[allow(unused)]
    pub fn new() -> Self {
        Self(())
    }
}

impl<Req, Resp> Service<Req> for TokioBlockingPool
where
    Req: FnOnce() -> Resp + Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Error = ThreadPanicError;
    type Future = ThreadPoolFuture<Resp>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let (tx, rx) = oneshot::channel();
        let span = Span::current();
        tokio::task::spawn_blocking(move || {
            let _guard = span.entered();
            let output = panic::catch_unwind(panic::AssertUnwindSafe(req));
            let _ = tx.send(output);
        });

        ThreadPoolFuture { rx }
    }
}

/// A cloneable [`tower::Service`] around a [`rayon::ThreadPool`] or
/// [`tokio::task::spawn_blocking`].
///
/// Accepts functions as requests (`FnOnce() -> T`), runs them on the thread-pool and
/// returns the result.
///
/// The error type of the service is returned if the function panics and
/// is the result of [`std::panic::catch_unwind`].
#[derive(Debug, Clone)]
pub(crate) struct BlockingThreadPool {
    inner: BlockingThreadPoolInner,
    metrics: ThreadPoolMetrics,
    pool_name: &'static str,
}

#[derive(Debug, Clone)]
enum BlockingThreadPoolInner {
    Rayon(RayonThreadPool),
    #[allow(dead_code)] // Only constructed in msim/tests.
    Tokio(TokioBlockingPool),
}

impl BlockingThreadPool {
    /// Converts this pool into a [`BoundedThreadPool`] with the specified limit on
    /// the maximum number in-flight requests.
    pub fn bounded_with_limit(self, max_in_flight: usize) -> BoundedThreadPool {
        ConcurrencyLimit::new(self, max_in_flight)
    }

    /// Converts this pool into a [`BoundedThreadPool`], whose inner pool is from `self`.
    ///
    /// For `N` workers in the thread pool, the limit is set to
    /// `N * DEFAULT_MAX_WAIT_TIME / ASSUMED_REQUEST_LATENCY` which aims to limit the amount
    /// of time spent in the queue to [`DEFAULT_MAX_WAIT_TIME`].
    pub fn bounded(self) -> BoundedThreadPool {
        let n_workers = match self.inner {
            BlockingThreadPoolInner::Rayon(ref pool) => pool.inner.current_num_threads(),
            BlockingThreadPoolInner::Tokio(_) => std::thread::available_parallelism()
                .unwrap_or(
                    NonZero::new(DEFAULT_TOKIO_BLOCKING_POOL_NUM_WORKERS)
                        .expect("default tokio blocking pool concurrent jobs must be non-zero"),
                )
                .get(),
        };

        let time_in_queue = DEFAULT_MAX_WAIT_TIME.as_secs_f64();
        let task_latency = ASSUMED_REQUEST_LATENCY.as_secs_f64();
        #[allow(clippy::cast_possible_truncation)] // truncation is intentional here
        let limit = ((n_workers as f64) * time_in_queue / task_latency) as usize;

        tracing::debug!(%limit, "calculated limit for `bounded RayonThreadPool`");

        self.bounded_with_limit(limit)
    }
}

// Implement Service generically for BlockingThreadPool.
impl<Req, Resp> Service<Req> for BlockingThreadPool
where
    Req: FnOnce() -> Resp + Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Error = ThreadPanicError;
    type Future = ThreadPoolFuture<Resp>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match &mut self.inner {
            BlockingThreadPoolInner::Rayon(pool) => {
                <RayonThreadPool as Service<fn()>>::poll_ready(pool, cx)
            }
            BlockingThreadPoolInner::Tokio(pool) => {
                <TokioBlockingPool as Service<fn()>>::poll_ready(pool, cx)
            }
        }
    }

    fn call(&mut self, request: Req) -> Self::Future {
        let pool_name = self.pool_name;
        let in_queue_guard = OwnedGaugeGuard::acquire(walrus_utils::with_label!(
            self.metrics.queue_length,
            pool_name
        ));
        let queued_start = Instant::now();
        let queued_latency =
            walrus_utils::with_label!(self.metrics.latency_seconds, pool_name, STATUS_QUEUED);
        let processing_latency =
            walrus_utils::with_label!(self.metrics.latency_seconds, pool_name, STATUS_IN_PROGRESS);

        let request = move || {
            // Drop the guard as soon as the task begins being processed.
            drop(in_queue_guard);

            let processing_start = Instant::now();
            queued_latency.observe(processing_start.duration_since(queued_start).as_secs_f64());

            let output = (request)();
            processing_latency.observe(processing_start.elapsed().as_secs_f64());

            output
        };

        match &mut self.inner {
            BlockingThreadPoolInner::Rayon(pool) => pool.call(request),
            BlockingThreadPoolInner::Tokio(pool) => pool.call(request),
        }
    }
}

/// Future returned by the `Service` implementation of [`RayonThreadPool`].
#[pin_project::pin_project]
pub(crate) struct ThreadPoolFuture<T> {
    rx: OneshotReceiver<std::thread::Result<T>>,
}

impl<T> Future for ThreadPoolFuture<T> {
    type Output = std::thread::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let output = task::ready!(self.project().rx.poll_unpin(cx));

        Poll::Ready(
            output.expect("panics are captured and thread does not drop tx before sending result"),
        )
    }
}
