use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const DEFAULT_ORDERING: Ordering = Ordering::Relaxed;

pub struct RingBuffer {
    size: usize,
    buffer: Vec<AtomicUsize>,
    start: AtomicUsize,  // reader
    end: AtomicUsize,    // writer
    waiting: AtomicBool, // spin lock
}

pub struct RingBufferIter<'a> {
    inner: &'a RingBuffer,
}

impl RingBuffer {
    pub fn new(size: usize) -> RingBuffer {
        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(AtomicUsize::new(0));
        }
        assert!(size > 0);
        RingBuffer {
            size,
            buffer,
            start: AtomicUsize::new(0),
            end: AtomicUsize::new(0),
            waiting: AtomicBool::new(false),
        }
    }

    pub fn iter(&self) -> RingBufferIter {
        RingBufferIter { inner: self }
    }

    pub fn push(&self, val: usize) {
        // blocking when buffer is full
        // no resizing occurred in the process
        loop {
            self.wait_lock();
            let size = self.size;
            let start = self.start.load(DEFAULT_ORDERING);
            let end = self.end.load(DEFAULT_ORDERING);
            let _start_idx = start % size;
            let end_idx = end % size;
            if end >= size && end - size >= start {
                // one loop ahead
                // debug!("Buffer full {} -> {}, {}", start, end, size);
                self.set_free();
                continue; // try again
            }
            self.buffer[end_idx].store(val, DEFAULT_ORDERING);
            self.end.store(end + 1, DEFAULT_ORDERING);
            self.set_free();
            break;
        }
    }

    #[inline]
    fn wait_lock(&self) {
        while self.waiting.compare_and_swap(false, true, DEFAULT_ORDERING) {}
    }

    #[inline]
    fn set_free(&self) {
        assert!(self.waiting.compare_and_swap(true, false, DEFAULT_ORDERING));
    }
}

impl<'a> RingBufferIter<'a> {
    #[inline]
    fn wait_lock(&self) {
        self.inner.wait_lock();
    }

    #[inline]
    fn set_free(&self) {
        self.inner.set_free();
    }
}

impl<'a> Iterator for RingBufferIter<'a> {
    type Item = usize;

    // single consumer
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let inner = self.inner;
        self.wait_lock();
        let start = inner.start.load(DEFAULT_ORDERING);
        let end = inner.end.load(DEFAULT_ORDERING);
        if start >= end {
            // read to end
            // debug!("Nothing to consume {} - {}", start, end);
            self.set_free();
            return None;
        }
        let val = inner.buffer[start % inner.size].load(DEFAULT_ORDERING);
        inner.start.store(start + 1, DEFAULT_ORDERING);
        self.set_free();
        return Some(val);
    }
}
