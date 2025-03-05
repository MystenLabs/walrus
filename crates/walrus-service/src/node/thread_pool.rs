// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    any::Any,
    future::Future,
    panic,
    pin::Pin,
    sync::Arc,
    task::{self, Context, Poll},
};

use futures::FutureExt;
use rayon::ThreadPoolBuilder;
use tokio::sync::{oneshot, oneshot::Receiver as OneshotReceiver};
use tower::{limit::ConcurrencyLimit, Service};
use tracing::span::Span;

pub(crate) type ThreadPanicError = Box<dyn Any + Send + 'static>;

/// Limit of the number of inflight requests on a [`BoundedRayonThreadPool`] to
/// `DEFAULT_IN_FLIGHT_RATIO * pool.current_num_threads`.
pub(crate) const DEFAULT_IN_FLIGHT_RATIO: f64 = 1.5;
/// Minimum value for the number of inflight requests on a [`BoundedRayonThreadPool`].
pub(crate) const MIN_IN_FLIGHT_REQUESTS: usize = 3;

/// A [`RayonThreadPool`] with a limit to the number of concurrent jobs.
pub(crate) type BoundedRayonThreadPool = ConcurrencyLimit<RayonThreadPool>;

/// Returns a default constructed `BoundedRayonThreadPool`.
pub(crate) fn default_bounded() -> BoundedRayonThreadPool {
    RayonThreadPool::new(
        ThreadPoolBuilder::new()
            .build()
            .expect("thread pool construction must succeed")
            .into(),
    )
    .bounded()
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
    pub fn new(pool: Arc<rayon::ThreadPool>) -> Self {
        Self { inner: pool }
    }

    /// Converts this pool into a [`BoundedRayonThreadPool`].
    ///
    /// The limit is set to the number of threads in the pool times [`DEFAULT_IN_FLIGHT_RATIO`] or
    /// a minimum of [`MIN_IN_FLIGHT_REQUESTS`].
    pub fn bounded(self) -> BoundedRayonThreadPool {
        let limit = ((self.inner.current_num_threads() as f64) * DEFAULT_IN_FLIGHT_RATIO) as usize;
        ConcurrencyLimit::new(self, limit.max(MIN_IN_FLIGHT_REQUESTS))
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
