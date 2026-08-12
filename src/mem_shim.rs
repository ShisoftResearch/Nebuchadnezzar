//! Global-allocator shim: exact, always-on heap accounting.
//!
//! Wraps the real allocator (mimalloc) and maintains relaxed atomic
//! counters — total live bytes plus a size-class histogram. Two relaxed
//! RMWs per alloc/free; no locks, no allocator-internal walks. This
//! replaces `mallinfo2` in the status path, whose bin walk holds the glibc
//! arena lock for seconds at hundred-GB heaps and stalled every allocating
//! thread each time status was polled (measured on the TB12 import: the
//! busiest thread spent 78-99% of trough time inside `int_mallinfo`).
//!
//! The histogram buckets match the diagnostic bands from the TB11/TB12
//! post-mortems (the "mmap band" was 128 KiB+ allocations): they answer
//! "what size class is growing" continuously, without smaps walks or
//! profilers, so heap growth during an import is attributable live.
//!
//! This gauge is also the input for budgeting heap against the configured
//! physical memory limit (tier manager reads `live_bytes()` and shrinks
//! the hot-tier target accordingly), which is why it counts LIVE bytes
//! owned by Rust allocations rather than allocator-held pages: the tier
//! must respond to demand, not to the allocator's retention policy.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Size-class boundaries for the live-allocation histogram. `BUCKETS[i]`
/// is the exclusive upper bound of bucket i; the last bucket is unbounded.
const BUCKET_BOUNDS: [usize; 4] = [128 << 10, 1 << 20, 4 << 20, 16 << 20];
pub const BUCKET_COUNT: usize = BUCKET_BOUNDS.len() + 1;
pub const BUCKET_NAMES: [&str; BUCKET_COUNT] = ["lt128k", "128k_1m", "1m_4m", "4m_16m", "ge16m"];

/// Stripe count for the counters. A single global pair of atomics was the
/// first implementation and it was a 5x THROUGHPUT REGRESSION at 192
/// writer threads (72 cores burned on cache-line ping-pong, measured on
/// the wikidata2016 A/B): every alloc/free in the process serialized on
/// two cache lines. Counters the whole process writes must be striped.
const STRIPES: usize = 128;

/// One cache line per stripe so neighbours never false-share.
#[repr(align(64))]
struct Stripe {
    live: AtomicUsize,
    bucket_bytes: [AtomicUsize; BUCKET_COUNT],
    bucket_counts: [AtomicUsize; BUCKET_COUNT],
}

#[allow(clippy::declare_interior_mutable_const)]
const STRIPE_ZERO: Stripe = Stripe {
    live: AtomicUsize::new(0),
    bucket_bytes: [
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ],
    bucket_counts: [
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    ],
};

static STRIPED: [Stripe; STRIPES] = [STRIPE_ZERO; STRIPES];

/// Stripe by the allocation's page bits: no TLS (unsafe inside a global
/// allocator — lazy TLS init itself allocates), alloc and dealloc of the
/// same block deterministically hit the same stripe (counters stay
/// consistent under cross-thread frees), and mimalloc's per-thread pages
/// spread concurrent threads across stripes naturally.
#[inline(always)]
fn stripe_of(ptr: *mut u8) -> &'static Stripe {
    &STRIPED[(ptr as usize >> 12) & (STRIPES - 1)]
}

#[inline(always)]
fn bucket_of(size: usize) -> usize {
    // Branchless-enough: sizes are overwhelmingly in bucket 0, so test it
    // first and let the predictor do the rest.
    for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
        if size < *bound {
            return i;
        }
    }
    BUCKET_COUNT - 1
}

#[inline(always)]
fn note_alloc(ptr: *mut u8, size: usize) {
    let s = stripe_of(ptr);
    s.live.fetch_add(size, Relaxed);
    let b = bucket_of(size);
    s.bucket_bytes[b].fetch_add(size, Relaxed);
    s.bucket_counts[b].fetch_add(1, Relaxed);
}

#[inline(always)]
fn note_dealloc(ptr: *mut u8, size: usize) {
    let s = stripe_of(ptr);
    s.live.fetch_sub(size, Relaxed);
    let b = bucket_of(size);
    s.bucket_bytes[b].fetch_sub(size, Relaxed);
    s.bucket_counts[b].fetch_sub(1, Relaxed);
}

/// Live heap bytes as requested from the global allocator (layout sizes,
/// not allocator-internal rounding — a demand gauge, not an RSS gauge).
/// Sums the stripes; per-stripe values can transiently go "negative"
/// (wrapping) when a block's alloc and dealloc race across the sum, so
/// the total uses wrapping arithmetic — it is exact whenever the heap is
/// quiescent and off by at most in-flight operations otherwise.
pub fn live_bytes() -> usize {
    STRIPED
        .iter()
        .fold(0usize, |acc, s| acc.wrapping_add(s.live.load(Relaxed)))
}

/// (bytes, count) per size-class bucket; see `BUCKET_NAMES`.
pub fn bucket_stats() -> [(usize, usize); BUCKET_COUNT] {
    let mut out = [(0usize, 0usize); BUCKET_COUNT];
    for s in STRIPED.iter() {
        for i in 0..BUCKET_COUNT {
            out[i].0 = out[i].0.wrapping_add(s.bucket_bytes[i].load(Relaxed));
            out[i].1 = out[i].1.wrapping_add(s.bucket_counts[i].load(Relaxed));
        }
    }
    out
}

pub struct CountingAlloc<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    #[inline(always)]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = self.0.alloc(layout);
        if !p.is_null() {
            note_alloc(p, layout.size());
        }
        p
    }

    #[inline(always)]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.0.dealloc(ptr, layout);
        note_dealloc(ptr, layout.size());
    }

    #[inline(always)]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = self.0.alloc_zeroed(layout);
        if !p.is_null() {
            note_alloc(p, layout.size());
        }
        p
    }

    #[inline(always)]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = self.0.realloc(ptr, layout, new_size);
        if !p.is_null() {
            note_dealloc(ptr, layout.size());
            note_alloc(p, new_size);
        }
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Attempts allowed when reading a process-global gauge. These counters are
    /// net (`fetch_add` on alloc, `fetch_sub` on dealloc) and the shim is the
    /// global allocator for the whole test binary, so every other test running
    /// in parallel moves them between our two reads. A single window can show
    /// less than we allocated -- observed at 420 KiB for a 1 MiB allocation
    /// once the suite runs concurrently, which read as "allocation not visible"
    /// and had nothing to do with the shim. The property is deterministic in a
    /// quiet window, so retry until we get one rather than weaken the
    /// assertion.
    const GAUGE_ATTEMPTS: usize = 32;

    #[test]
    fn counters_track_alloc_and_dealloc() {
        let mut last = None;
        for _ in 0..GAUGE_ATTEMPTS {
            let before = live_bytes();
            let v: Vec<u8> = Vec::with_capacity(1 << 20);
            let mid = live_bytes();
            if mid < before + (1 << 20) {
                // Concurrent frees ate the window; nothing was learned.
                last = Some((before, mid));
                drop(v);
                continue;
            }
            drop(v);
            let after = live_bytes();
            if after < mid {
                return; // alloc and dealloc both observed in one clean window
            }
            last = Some((mid, after));
        }
        panic!(
            "no quiet window in {GAUGE_ATTEMPTS} attempts; last reading {last:?} \
             (gauges are process-global and every parallel test moves them)"
        );
    }

    #[test]
    fn buckets_split_by_size_class() {
        // Same process-global gauges as `counters_track_alloc_and_dealloc`, so
        // same retry: a parallel test freeing a 64 KiB or 8 MiB buffer between
        // our two reads hides ours.
        for attempt in 0..GAUGE_ATTEMPTS {
            let b_before = bucket_stats();
            let small: Vec<u8> = Vec::with_capacity(64 << 10); // -> lt128k
            let big: Vec<u8> = Vec::with_capacity(8 << 20); // -> 4m_16m
            let b_mid = bucket_stats();
            let small_seen = b_mid[0].0 >= b_before[0].0 + (64 << 10);
            let big_seen = b_mid[3].0 >= b_before[3].0 + (8 << 20);
            drop(small);
            drop(big);
            if small_seen && big_seen {
                return;
            }
            assert!(
                attempt + 1 < GAUGE_ATTEMPTS,
                "no quiet window in {GAUGE_ATTEMPTS} attempts: \
                 lt128k {} -> {}, 4m_16m {} -> {}",
                b_before[0].0,
                b_mid[0].0,
                b_before[3].0,
                b_mid[3].0
            );
        }
    }
}
