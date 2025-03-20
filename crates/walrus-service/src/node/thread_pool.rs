// Copyright (c) Mysten Labs, Inc.
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
#[cfg(not(msim))]
use rayon::ThreadPoolBuilder;
use tokio::sync::{oneshot, oneshot::Receiver as OneshotReceiver};
use tower::{limit::ConcurrencyLimit, Service};
use tracing::span::Span;

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
/// [`RayonThreadPool::bounded`].
pub(crate) const DEFAULT_MAX_WAIT_TIME: Duration = Duration::from_secs(1);

/// The default number of workers for the tokio blocking pool.
pub(crate) const DEFAULT_TOKIO_BLOCKING_POOL_NUM_WORKERS: usize = 4;

/// A [`BlockingThreadPool`] with a limit to the number of concurrent jobs.
pub(crate) type BoundedThreadPool = ConcurrencyLimit<BlockingThreadPool>;

/// Returns a default constructed `BoundedThreadPool`.
pub(crate) fn default_bounded() -> BoundedThreadPool {
    #[cfg(not(msim))]
    {
        BlockingThreadPool::new_rayon(RayonThreadPool::new(
            ThreadPoolBuilder::new()
                .build()
                .expect("thread pool construction must succeed")
                .into(),
        ))
        .bounded()
    }

    // Note that in simtest, we cannot use Rayon thread pool due to that it causes simtest to be
    // non-deterministic. This is because Rayon threads are actually run in different threads than
    // the tokio main threads, and not controlled by the tokio runtime.
    #[cfg(msim)]
    {
        BlockingThreadPool::new_tokio(TokioBlockingPool::new()).bounded()
    }
}

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

impl RayonThreadPool {
    /// Returns a new `RayonThreadPool` service.
    #[allow(unused)]
    pub fn new(pool: Arc<rayon::ThreadPool>) -> Self {
        Self { inner: pool }
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
pub(crate) struct TokioBlockingPool;

impl TokioBlockingPool {
    /// Returns a new `TokioBlockingPool` service.
    #[allow(unused)]
    pub fn new() -> Self {
        Self
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
pub(crate) enum BlockingThreadPool {
    Rayon(RayonThreadPool),
    Tokio(TokioBlockingPool),
}

impl BlockingThreadPool {
    #[allow(unused)]
    pub fn new_rayon(pool: RayonThreadPool) -> Self {
        Self::Rayon(pool)
    }

    #[allow(unused)]
    pub fn new_tokio(pool: TokioBlockingPool) -> Self {
        Self::Tokio(pool)
    }

    // Converts this pool into a [`BoundedThreadPool`], whose inner pool is from `self`.
    //
    // For `N` workers in the thread pool, the limit is set to
    // `N * DEFAULT_MAX_WAIT_TIME / ASSUMED_REQUEST_LATENCY` which aims to limit the amount
    // of time spent in the queue to [`DEFAULT_MAX_WAIT_TIME`].
    pub fn bounded(self) -> ConcurrencyLimit<BlockingThreadPool> {
        let n_workers = match self {
            Self::Rayon(ref pool) => pool.inner.current_num_threads() as f64,
            Self::Tokio(ref _pool) => std::thread::available_parallelism()
                .unwrap_or(
                    NonZero::new(DEFAULT_TOKIO_BLOCKING_POOL_NUM_WORKERS)
                        .expect("default tokio blocking pool concurrent jobs must be non-zero"),
                )
                .get() as f64,
        };

        let time_in_queue = DEFAULT_MAX_WAIT_TIME.as_secs_f64();
        let task_latency = ASSUMED_REQUEST_LATENCY.as_secs_f64();
        let limit = (n_workers * time_in_queue / task_latency) as usize;

        tracing::debug!(%limit, "calculated limit for `bounded RayonThreadPool`");

        ConcurrencyLimit::new(self, limit)
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
        match self {
            Self::Rayon(pool) => <RayonThreadPool as Service<fn()>>::poll_ready(pool, cx),
            Self::Tokio(pool) => <TokioBlockingPool as Service<fn()>>::poll_ready(pool, cx),
        }
    }

    fn call(&mut self, req: Req) -> Self::Future {
        match self {
            Self::Rayon(pool) => pool.call(req),
            Self::Tokio(pool) => pool.call(req),
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
