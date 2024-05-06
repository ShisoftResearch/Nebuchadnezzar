// Streamer bridges an async data source to local iterators
// Async streams are relatively expensive to been used for local data
// It acts as a buffer zone, which it streams certain number of items
// as a block to local memory, then invokes local iterators for processing

use std::{pin::Pin, vec};

use futures::{Stream, StreamExt};
use tokio::runtime::Handle;

pub struct Streamer<T> {
    buffer: vec::IntoIter<T>,
    capacity: usize,
    rt: Handle,
    stream: Pin<Box<dyn Stream<Item = T>>>,
}

impl<T> Streamer<T> {
    pub fn new(stream: impl Stream<Item = T> + 'static, capacity: usize) -> Self {
        Self {
            buffer: Vec::new().into_iter(),
            rt: Handle::current(),
            stream: Box::pin(stream),
            capacity,
        }
    }
}

impl<T> Iterator for Streamer<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.buffer.next() {
            return Some(item);
        }
        let rt = &self.rt;
        let new_buffer = rt.block_on(async {
            let mut buffer = Vec::with_capacity(self.capacity);
            for _i in 0..self.capacity {
                if let Some(item) = self.stream.next().await {
                    buffer.push(item);
                }
            }
            buffer.into_iter()
        });
        self.buffer = new_buffer;
        return self.buffer.next();
    }
}
