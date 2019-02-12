use futures::{Async, Future, Poll, Stream};
use parking_lot::Mutex;
use std::sync::Arc;

pub struct PollableStream<I, E> {
    inner: Arc<Mutex<Stream<Item = I, Error = E>>>,
}

pub struct StreamFuture<I, E> {
    inner: Arc<Mutex<Stream<Item = I, Error = E>>>,
}

unsafe impl<I, E> Send for PollableStream<I, E> {}
unsafe impl<I, E> Sync for PollableStream<I, E> {}
unsafe impl<I, E> Send for StreamFuture<I, E> {}
unsafe impl<I, E> Sync for StreamFuture<I, E> {}

impl<I, E> Future for StreamFuture<I, E> {
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let async = self.inner.lock().poll()?;
        Ok(match async {
            Async::Ready(Some(i)) => Async::Ready(i),
            _ => Async::NotReady,
        })
    }
}

impl<I, E> PollableStream<I, E> {
    pub fn from_stream<S>(stream: S) -> PollableStream<I, E>
    where
        S: Stream<Item = I, Error = E> + 'static,
    {
        PollableStream {
            inner: Arc::new(Mutex::new(stream)),
        }
    }
    pub fn poll_future(&self) -> StreamFuture<I, E> {
        StreamFuture {
            inner: self.inner.clone(),
        }
    }
}
