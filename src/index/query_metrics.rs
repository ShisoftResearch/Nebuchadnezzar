//! Optional operation counts for query benchmarks. Counts are process-local.
use crate::ram::cell::OwnedCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

static ENABLED: AtomicBool = AtomicBool::new(false);
static FULL_READS: AtomicU64 = AtomicU64::new(0);
static SELECTED_READS: AtomicU64 = AtomicU64::new(0);
static FULL_BYTES: AtomicU64 = AtomicU64::new(0);
static SELECTED_BYTES: AtomicU64 = AtomicU64::new(0);
static READ_BATCHES: AtomicU64 = AtomicU64::new(0);
static METADATA_REQUESTS: AtomicU64 = AtomicU64::new(0);
static CANDIDATE_QUERIES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct QueryMetrics {
    pub full_reads: u64,
    pub selected_reads: u64,
    /// Serialized returned data, not storage-page or network bytes.
    pub full_payload_bytes: u64,
    pub selected_payload_bytes: u64,
    pub read_batches: u64,
    /// Schema metadata API calls, including any cache hits.
    pub metadata_requests: u64,
    pub candidate_queries: u64,
}

/// Count one untimed invocation. Payload accounting must not run in timing phases.
pub fn start() {
    assert!(!ENABLED.swap(false, Relaxed));
    for counter in [
        &FULL_READS,
        &SELECTED_READS,
        &FULL_BYTES,
        &SELECTED_BYTES,
        &READ_BATCHES,
        &METADATA_REQUESTS,
        &CANDIDATE_QUERIES,
    ] {
        counter.store(0, Relaxed);
    }
    ENABLED.store(true, Relaxed);
}
pub fn finish() -> QueryMetrics {
    ENABLED.store(false, Relaxed);
    QueryMetrics {
        full_reads: FULL_READS.load(Relaxed),
        selected_reads: SELECTED_READS.load(Relaxed),
        full_payload_bytes: FULL_BYTES.load(Relaxed),
        selected_payload_bytes: SELECTED_BYTES.load(Relaxed),
        read_batches: READ_BATCHES.load(Relaxed),
        metadata_requests: METADATA_REQUESTS.load(Relaxed),
        candidate_queries: CANDIDATE_QUERIES.load(Relaxed),
    }
}
pub(crate) fn reads(selected: bool, count: usize) {
    if !ENABLED.load(Relaxed) {
        return;
    }
    READ_BATCHES.fetch_add(1, Relaxed);
    (if selected {
        &SELECTED_READS
    } else {
        &FULL_READS
    })
    .fetch_add(count as u64, Relaxed);
}
pub(crate) fn payload(selected: bool, cell: &OwnedCell) {
    if !ENABLED.load(Relaxed) {
        return;
    }
    let bytes = bincode::serde::encode_to_vec(&cell.data, bincode::config::standard())
        .expect("benchmark cell encoding")
        .len();
    (if selected {
        &SELECTED_BYTES
    } else {
        &FULL_BYTES
    })
    .fetch_add(bytes as u64, Relaxed);
}
pub(crate) fn metadata() {
    if ENABLED.load(Relaxed) {
        METADATA_REQUESTS.fetch_add(1, Relaxed);
    }
}
pub(crate) fn candidates() {
    if ENABLED.load(Relaxed) {
        CANDIDATE_QUERIES.fetch_add(1, Relaxed);
    }
}
