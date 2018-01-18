use futures::{Stream, Future};
use parking_lot::{Mutex};
use std::sync::Arc;

pub struct PollableStream<I, E> {
    inner: Arc<Mutex<Stream<Item = I, Error = E>>>
}

pub struct StreamFuture<I, E> {
    inner: Arc<Mutex<Stream<Item = I, Error = E>>>
}

impl <I, E> Future for StreamFuture<I, E> {
    type Item = I;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.lock().poll()
    }
}

impl <I, E> PollableStream<I, E> {
    pub fn from_stream(stream: Stream<Item = I, Error = E>) {
        PollableStream {
            inner: Arc::new(Mutex::new(stream))
        }
    }
}