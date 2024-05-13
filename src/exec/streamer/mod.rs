// Streamer bridges an async data source to local iterators
// Async streams are relatively expensive to been used for local data
// It acts as a buffer zone, which it streams certain number of items
// as a block to local memory, then invokes local iterators for processing

use std::{pin::Pin, vec};

use futures::{Stream, StreamExt};
use tokio::runtime::Handle;

pub mod cell_id;

pub trait Streamer<T>: Iterator<Item = T> {
    fn new(stream: impl Stream<Item = T> + 'static) -> Self;
}

pub struct BufferedStreamer<T, const N: usize> {
    buffer: vec::IntoIter<T>,
    rt: Handle,
    stream: Pin<Box<dyn Stream<Item = T>>>,
}

impl<T, const N: usize> Streamer<T> for BufferedStreamer<T, N> {
    fn new(stream: impl Stream<Item = T> + 'static) -> Self {
        Self {
            buffer: Vec::new().into_iter(),
            rt: Handle::current(),
            stream: Box::pin(stream),
        }
    }
}

impl<T, const N: usize> Iterator for BufferedStreamer<T, N> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.buffer.next() {
            return Some(item);
        }
        let rt = &self.rt;
        let new_buffer = rt.block_on(async {
            let mut buffer = Vec::with_capacity(N);
            for _i in 0..N {
                if let Some(item) = self.stream.next().await {
                    buffer.push(item);
                } else {
                    break;
                }
            }
            buffer.into_iter()
        });
        self.buffer = new_buffer;
        return self.buffer.next();
    }
}

pub struct PassthroughStreamer<T> {
    rt: Handle,
    stream: Pin<Box<dyn Stream<Item = T>>>,
}

impl<T> Streamer<T> for PassthroughStreamer<T> {
    fn new(stream: impl Stream<Item = T> + 'static) -> Self {
        Self {
            rt: Handle::current(),
            stream: Box::pin(stream),
        }
    }
}

impl<T> Iterator for PassthroughStreamer<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        return self.rt.block_on(self.stream.next());
    }
}
