// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    pin::Pin,
    task::{self, Context, Poll},
};

pub use bytes;
use bytes::Buf;
pub use http_body;
use http_body::{Body, Frame};
use pin_project::{pin_project, pinned_drop};

/// Trait that defines a visitor for a [`Body`] and is driven with [`VisitBody`].
///
/// When an HTTP request or response contains a [`VisitBody`], the implementor of this trait
/// is notified when a frame is polled or the body is dropped.
///
/// This is implemented for `FnMut(Option<Result<&http_body::Frame<D>, &E>>)` and so functions can
/// be used as the visitor.
pub trait BodyVisitor<D, E> {
    /// Called whenever a frame of the body is polled with [`Body::poll_frame`], which returns the
    /// result of some frame.
    fn frame_polled(&mut self, maybe_result: Option<Result<&http_body::Frame<D>, &E>>);

    /// Called from the destructor of the body and indicates the result of [`Body::is_end_stream`].
    ///
    /// The default implementation of this method does nothing.
    fn body_dropped(&mut self, _is_end_stream: bool) {}
}

impl<D, E, F> BodyVisitor<D, E> for F
where
    D: Buf,
    F: FnMut(Option<Result<&http_body::Frame<D>, &E>>),
{
    fn frame_polled(&mut self, maybe_result: Option<Result<&http_body::Frame<D>, &E>>) {
        (self)(maybe_result)
    }
}

/// Wraps a [`Body`] and drives a [`BodyVisitor`] whenever frames are polled.
#[pin_project(PinnedDrop)]
#[derive(Debug, Clone, Default)]
pub struct VisitBody<T, V>
where
    T: Body,
    V: BodyVisitor<T::Data, T::Error>,
{
    #[pin]
    body: T,
    visitor: V,
}

impl<T, V> VisitBody<T, V>
where
    T: Body,
    V: BodyVisitor<T::Data, T::Error>,
{
    /// Creates a new instance with the provided body and visitor.
    pub fn new(body: T, visitor: V) -> Self {
        Self { body, visitor }
    }
}

impl<T, V> Body for VisitBody<T, V>
where
    T: Body,
    V: BodyVisitor<T::Data, T::Error>,
{
    type Data = T::Data;
    type Error = T::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let maybe_result = task::ready!(this.body.poll_frame(cx));

        this.visitor
            .frame_polled(maybe_result.as_ref().map(|r| r.as_ref()));

        Poll::Ready(maybe_result)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

#[pinned_drop]
impl<T, V> PinnedDrop for VisitBody<T, V>
where
    T: Body,
    V: BodyVisitor<T::Data, T::Error>,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        this.visitor.body_dropped(this.body.is_end_stream());
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use bytes::Buf;
    use http_body_util::Full;

    use super::*;

    #[tokio::test]
    async fn body_visitor_notified_on_poll() {
        #[derive(Debug, Default)]
        struct MyVisitor {
            some_count: usize,
            none_count: usize,
        }

        impl<D, E> BodyVisitor<D, E> for MyVisitor
        where
            D: Buf,
        {
            fn frame_polled(&mut self, maybe_result: Option<Result<&Frame<D>, &E>>) {
                match maybe_result {
                    Some(_) => self.some_count += 1,
                    None => self.none_count += 1,
                }
            }
            fn body_dropped(&mut self, _: bool) {}
        }

        let body = Full::new(VecDeque::from(vec![1u8, 2, 3]));
        let mut body_with_visitor = Box::pin(VisitBody::new(body, MyVisitor::default()));

        let _frame = std::future::poll_fn(|cx| body_with_visitor.as_mut().poll_frame(cx))
            .await
            .expect("must be `Some` since `Full` has data")
            .expect("must be `Ok` since `Full` does not err");

        assert!(
            std::future::poll_fn(|cx| body_with_visitor.as_mut().poll_frame(cx))
                .await
                .is_none(),
            "must be None since `Full` yields data only once"
        );
        assert_eq!(body_with_visitor.as_ref().visitor.some_count, 1);
        assert_eq!(body_with_visitor.as_ref().visitor.none_count, 1);
    }

    #[tokio::test]
    async fn body_visitor_notified_on_drop() {
        #[derive(Debug, Default, Clone)]
        struct MyVisitor {
            notified_of_drop: Arc<AtomicBool>,
            is_end_stream: Arc<AtomicBool>,
        }

        impl<D, E> BodyVisitor<D, E> for MyVisitor
        where
            D: Buf,
        {
            fn frame_polled(&mut self, _maybe_result: Option<Result<&Frame<D>, &E>>) {}
            fn body_dropped(&mut self, is_end_stream: bool) {
                self.notified_of_drop.store(true, Ordering::Relaxed);
                self.is_end_stream.store(is_end_stream, Ordering::Relaxed);
            }
        }

        let body = Full::new(VecDeque::from(vec![1u8, 2, 3]));
        let visitor = MyVisitor::default();
        let body_with_visitor = Box::pin(VisitBody::new(body, visitor.clone()));
        drop(body_with_visitor);

        assert!(
            visitor.notified_of_drop.load(Ordering::Relaxed),
            "must be notified of the drop"
        );
        assert!(
            !visitor.is_end_stream.load(Ordering::Relaxed),
            "stream had not ended when dropped"
        );
    }
}
