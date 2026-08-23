use crate::query::statistics::{
    merge_statistics, schema_tracks_statistics, ChunkStatistics, SchemaStatistics,
};
use crate::ram::entry::{Entry, EntryContent, EntryType, ENTRY_HEAD_SIZE};
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::schema::LocalSchemasCache;
use crate::ram::segment_list::SegmentList;
use crate::ram::segs::{Segment, SegmentAllocator, SegmentClass, SEGMENT_SIZE, SEGMENT_SIZE_U32};
use crate::ram::tombstone::{Tombstone, TOMBSTONE_ENTRY_SIZE};
use crate::ram::types::Id;
use crate::server::ServerMeta;
use crate::{index::builder::IndexBuilder, ram::cell::*};
use crate::{
    index::builder::{probe_cell_indices, IndexRes},
    ram::cleaner::Cleaner,
};

use super::schema::Schema;
use dovahkiin::types::OwnedValue;
use lightning::aarc::Arc as AArc;
use lightning::map::{Map, WordMap, WordMutexGuard};
use lightning::spin_hint::Backoff;
use lightning::ttl_cache::TTLCache;
use parking_lot::Mutex;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Per-phase cost of a cell write, in nanoseconds, against the number of cells.
///
/// Import throughput is flat as client concurrency rises, so latency is rising
/// to match and something is served at a fixed rate. Aggregate CPU and blocked
/// -thread counts cannot say which stage that is -- the stage that holds the
/// rate is the one whose per-cell cost does not fall as load is added.
pub static WRITE_CELLS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_PLAN_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_ALLOC_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_COPY_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_INDEX_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_SECONDARY_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WRITE_STATS_NANOS: AtomicU64 = AtomicU64::new(0);

/// Decomposition of the allocation phase, which is most of a bulk write's
/// cost. `alloc` was one opaque number; these split it into what a writer
/// spends WAITING (on the ALLOCATING slot or a busy head) versus what the one
/// rotating writer spends doing the rotation itself -- and, within rotation,
/// what the seal-time archive costs, since that is a full segment compress +
/// write + fsync taken inline on the write path.
pub static ALLOC_SPIN_NANOS: AtomicU64 = AtomicU64::new(0);
pub static ROTATE_NANOS: AtomicU64 = AtomicU64::new(0);
pub static ROTATE_DRAIN_NANOS: AtomicU64 = AtomicU64::new(0);
/// Entries whose WAL journal failed at PendingEntry drop (sealed underneath).
pub static WAL_JOURNAL_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Allocation refusals because the chunk had no room left.
///
/// A full store is not a durability failure, but it looks exactly like one
/// from the outside: index write-back cannot allocate, its batches are
/// abandoned, the barrier is never established, and entries go missing. Any
/// harness judging durability has to be able to tell the two apart, so the
/// refusal is counted where it happens.
pub static ALLOCATION_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
pub static ROTATE_ARCHIVE_NANOS: AtomicU64 = AtomicU64::new(0);
pub static ROTATIONS: AtomicU64 = AtomicU64::new(0);
/// Within rotation: allocating the fresh segment (including its WAL file
/// creation, if that is where the time turns out to be).
pub static ROTATE_ALLOC_SEG_NANOS: AtomicU64 = AtomicU64::new(0);

/// Snapshot for baseline subtraction in measurements.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocPhaseCounters {
    pub spin: u64,
    pub rotate: u64,
    pub drain: u64,
    pub archive: u64,
    pub rotations: u64,
    pub alloc_seg: u64,
}

impl AllocPhaseCounters {
    pub fn minus(&self, b: &AllocPhaseCounters) -> AllocPhaseCounters {
        AllocPhaseCounters {
            spin: self.spin.saturating_sub(b.spin),
            rotate: self.rotate.saturating_sub(b.rotate),
            drain: self.drain.saturating_sub(b.drain),
            archive: self.archive.saturating_sub(b.archive),
            rotations: self.rotations.saturating_sub(b.rotations),
            alloc_seg: self.alloc_seg.saturating_sub(b.alloc_seg),
        }
    }
}

/// Accumulates rotation wall time on every exit path, panic included.
struct RotateTimer(std::time::Instant);
impl Drop for RotateTimer {
    fn drop(&mut self) {
        ROTATE_NANOS.fetch_add(self.0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

pub fn alloc_phase_counters() -> AllocPhaseCounters {
    AllocPhaseCounters {
        spin: ALLOC_SPIN_NANOS.load(Ordering::Relaxed),
        rotate: ROTATE_NANOS.load(Ordering::Relaxed),
        drain: ROTATE_DRAIN_NANOS.load(Ordering::Relaxed),
        archive: ROTATE_ARCHIVE_NANOS.load(Ordering::Relaxed),
        rotations: ROTATIONS.load(Ordering::Relaxed),
        alloc_seg: ROTATE_ALLOC_SEG_NANOS.load(Ordering::Relaxed),
    }
}

/// The write path's per-phase timers, for subtracting a baseline around a
/// measured region.
#[derive(Debug, Clone, Copy, Default)]
pub struct WritePhaseCounters {
    pub cells: u64,
    pub plan: u64,
    pub alloc: u64,
    pub copy: u64,
    pub index: u64,
    pub secondary: u64,
    pub stats: u64,
}

impl WritePhaseCounters {
    pub fn minus(&self, before: &WritePhaseCounters) -> WritePhaseCounters {
        WritePhaseCounters {
            cells: self.cells.saturating_sub(before.cells),
            plan: self.plan.saturating_sub(before.plan),
            alloc: self.alloc.saturating_sub(before.alloc),
            copy: self.copy.saturating_sub(before.copy),
            index: self.index.saturating_sub(before.index),
            secondary: self.secondary.saturating_sub(before.secondary),
            stats: self.stats.saturating_sub(before.stats),
        }
    }
    pub fn total_nanos(&self) -> u64 {
        self.plan + self.alloc + self.copy + self.index + self.secondary + self.stats
    }
}

pub fn write_phase_counters() -> WritePhaseCounters {
    WritePhaseCounters {
        cells: WRITE_CELLS.load(Ordering::Relaxed),
        plan: WRITE_PLAN_NANOS.load(Ordering::Relaxed),
        alloc: WRITE_ALLOC_NANOS.load(Ordering::Relaxed),
        copy: WRITE_COPY_NANOS.load(Ordering::Relaxed),
        index: WRITE_INDEX_NANOS.load(Ordering::Relaxed),
        secondary: WRITE_SECONDARY_NANOS.load(Ordering::Relaxed),
        stats: WRITE_STATS_NANOS.load(Ordering::Relaxed),
    }
}
use std::sync::Arc;

pub type CellReadGuard<'a> = WordMutexGuard<'a>;
pub type CellWriteGuard<'a> = WordMutexGuard<'a>;

// Global chunk allocation state for unified address space
static GLOBAL_CHUNK_BASE: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNK_SIZE_BITS: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNK_COUNT: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_ALLOCATED_SIZE: AtomicUsize = AtomicUsize::new(0);
static GLOBAL_CHUNKS_PTR: AtomicUsize = AtomicUsize::new(0);

static MAX_SEGMENTS_FOR_CLEANER: usize = 16;

/// Ceiling on one chunk's cell index, in slots. Only guards against a nonsense
/// estimate; a legitimate 16 GB chunk of 8 MiB segments wants ~17 M.
const MAX_CELL_INDEX_CAPACITY: usize = 1 << 26;

static DEAD_RATE_FOR_COMBINE_CLEANER: f32 = 0.50f32;
/// Victim bar while the chunk has ample free space: only combine segments
/// that are at least three-quarters dead.
static DEAD_RATE_FOR_COMBINE_CLEANER_RELAXED: f32 = 0.25f32;

/// Drain passes a retired segment may wait before each report. A retirement
/// that never drains means a reference was leaked; that should be visible.
const RETIRED_SEGMENT_REPORT_INTERVAL: usize = 64;

/// Holds the write-head slot while a new segment is allocated.
///
/// The slot reads `HEAD_SEG_ID_ALLOCATING` for that window and every other
/// writer spins on it, so an early exit that leaves it there wedges the whole
/// chunk -- permanently, since only this function ever replaces the value.
/// Dropping without `publish` restores the previous head, which makes the
/// window panic-safe as well as error-safe.
struct HeadSlotGuard<'a> {
    slot: &'a AtomicU64,
    restore_to: u64,
}

impl<'a> HeadSlotGuard<'a> {
    /// Install the newly allocated segment as the head and disarm the guard.
    fn publish(self, new_seg_id: u64) {
        self.slot.store(new_seg_id, Ordering::Release);
        std::mem::forget(self);
    }
}

impl<'a> Drop for HeadSlotGuard<'a> {
    fn drop(&mut self) {
        self.slot.store(self.restore_to, Ordering::Release);
    }
}

const HEAD_SEG_ID_EMPTY: u64 = u64::MAX;
const HEAD_SEG_ID_ALLOCATING: u64 = u64::MAX - 1;

/// Pool sizes per class. Elastic in USE (slots fill only under real
/// contention), fixed in CAPACITY. The .239 contention spike showed K=4
/// degrading badly at 96-192 threads and K=8-16 behaving well; blobs see a
/// fraction of the traffic and 8 MiB per resident head is real money.
fn head_pool_len(segment_class: SegmentClass) -> usize {
    static REGULAR: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    static BLOB: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let read = |var: &str, default: usize| {
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            // Floor of 2, not 1: with a single slot, a thread that ever
            // needed a second entry while holding its first would spin on
            // its own ownership forever.
            .map(|v| v.clamp(2, 64))
            .unwrap_or(default)
    };
    match segment_class {
        SegmentClass::Regular => *REGULAR.get_or_init(|| read("NEB_HEAD_POOL", 8)),
        SegmentClass::Blob => *BLOB.get_or_init(|| read("NEB_BLOB_HEAD_POOL", 2)),
    }
}

/// Stable per-thread starting slot, so threads spread across the pool and a
/// thread keeps returning to "its" head while uncontended -- the affinity
/// shape the contention spike validated.
fn head_affinity() -> usize {
    use std::cell::Cell;
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    thread_local! {
        static AFFINITY: Cell<usize> = const { Cell::new(usize::MAX) };
    }
    AFFINITY.with(|a| {
        if a.get() == usize::MAX {
            a.set(NEXT.fetch_add(1, Ordering::Relaxed));
        }
        a.get()
    })
}

/// Get the current global chunk base address
pub fn get_global_chunk_base() -> usize {
    GLOBAL_CHUNK_BASE.load(Ordering::Acquire)
}

/// Get chunk size as power-of-2 bits
pub fn get_chunk_size_bits() -> usize {
    GLOBAL_CHUNK_SIZE_BITS.load(Ordering::Acquire)
}

/// Calculate chunk ID and segment ID from a fault address against the
/// LAST-CREATED Chunks instance. The globals are last-writer-wins, so
/// this is best-effort diagnostics only (multi-database hosts and
/// concurrent tests each create their own instance); for deterministic
/// decoding use [`Chunks::chunk_and_segment_from_addr`].
pub fn chunk_and_segment_from_addr(fault_addr: usize) -> Option<(usize, usize)> {
    let base = GLOBAL_CHUNK_BASE.load(Ordering::Acquire);
    if base == 0 {
        return None;
    }
    decode_chunk_addr(
        fault_addr,
        base,
        GLOBAL_ALLOCATED_SIZE.load(Ordering::Acquire),
        GLOBAL_CHUNK_SIZE_BITS.load(Ordering::Acquire),
    )
}

fn decode_chunk_addr(
    fault_addr: usize,
    base: usize,
    total_size: usize,
    chunk_size_bits: usize,
) -> Option<(usize, usize)> {
    use crate::ram::segs::SEGMENT_BITS_SHIFT;

    if fault_addr < base {
        return None;
    }
    let offset = fault_addr - base;
    if offset >= total_size {
        return None;
    }
    let chunk_id = offset >> chunk_size_bits;
    let offset_in_chunk = offset & ((1 << chunk_size_bits) - 1);
    let segment_id = offset_in_chunk >> SEGMENT_BITS_SHIFT;
    Some((chunk_id, segment_id))
}

/// Set the global Chunks pointer (called by Chunks::new_with_recovery)
pub fn set_global_chunks(chunks: &Arc<Chunks>) {
    let ptr = Arc::as_ptr(chunks) as usize;
    GLOBAL_CHUNKS_PTR.store(ptr, Ordering::Release);
}

/// Get a reference to the global Chunks instance
/// SAFETY: Only safe to call if Chunks instance is still alive
pub unsafe fn get_global_chunks() -> Option<&'static Chunks> {
    let ptr = GLOBAL_CHUNKS_PTR.load(Ordering::Acquire);
    if ptr == 0 {
        None
    } else {
        Some(&*(ptr as *const Chunks))
    }
}

/// Access a segment by chunk_id and segment_id from the global Chunks
/// Used by signal handler to flip reference bits
pub fn get_segment_for_fault(
    chunk_id: usize,
    segment_id: usize,
) -> Option<AArc<crate::ram::segs::Segment>> {
    unsafe {
        get_global_chunks().and_then(|chunks| {
            chunks
                .list
                .get(chunk_id)
                .and_then(|chunk| chunk.segs.get(&segment_id))
        })
    }
}

// /// Reset global chunk allocation (for tests)
// ///
// /// IMPORTANT: Reset GLOBAL_CHUNKS_PTR BEFORE unmapping memory to prevent
// /// the signal handler from accessing unmapped memory during cleanup.
// pub fn reset_global_chunk_allocation() {
//     // Reset GLOBAL_CHUNKS_PTR first to prevent signal handler from accessing chunks
//     // This must happen BEFORE unmapping memory to avoid SIGSEGV in signal handler
//     GLOBAL_CHUNKS_PTR.store(0, Ordering::Release);

//     let base = GLOBAL_CHUNK_BASE.swap(0, Ordering::AcqRel);
//     let size = GLOBAL_ALLOCATED_SIZE.swap(0, Ordering::AcqRel);

//     // Reset other globals before unmapping
//     GLOBAL_CHUNK_SIZE_BITS.store(0, Ordering::Release);
//     GLOBAL_CHUNK_COUNT.store(0, Ordering::Release);

//     // Now safe to unmap memory - signal handler won't try to access it
//     if base != 0 && size != 0 {
//         unsafe {
//             println!("unmapping memory from {}", base);
//             libc::munmap(base as *mut libc::c_void, size);
//         }
//     }
// }

// Thread-local flag to indicate if we're currently in a transaction
// When true, WAL writes will skip fsync (will be synced at commit instead)
thread_local! {
    static IN_TRANSACTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Segments this thread appended to while inside a transaction.
    ///
    /// Transactional writes skip the group-commit sync deliberately -- the
    /// promise is that the commit syncs them instead. Keeping the list here,
    /// where the skip happens, is what makes that promise keepable: the
    /// commit path used to sync segments derived from the OLD addresses of
    /// updated and removed cells, which are not where the new entries went,
    /// and a transaction of pure inserts recorded nothing at all and so
    /// synced nothing.
    static TXN_TOUCHED_SEGMENTS: std::cell::RefCell<Vec<AArc<Segment>>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Set the transaction context for the current thread
pub fn set_transaction_context(in_txn: bool) {
    IN_TRANSACTION.with(|flag| flag.set(in_txn));
}

/// Record a segment whose transactional entry is not yet durable.
fn note_transactional_write(segment: &AArc<Segment>) {
    TXN_TOUCHED_SEGMENTS.with(|segments| {
        let mut segments = segments.borrow_mut();
        // Runs are common and land in one segment; a linear scan over a
        // handful of entries beats hashing them.
        if !segments.iter().any(|held| held.id == segment.id
            && held.chunk_id == segment.chunk_id
            && held.seq_id == segment.seq_id)
        {
            segments.push(segment.clone());
        }
    });
}

/// Take the segments this thread wrote to under a transaction, leaving the
/// list empty for the next one.
pub fn take_transactional_segments() -> Vec<AArc<Segment>> {
    TXN_TOUCHED_SEGMENTS.with(|segments| std::mem::take(&mut *segments.borrow_mut()))
}

/// Check if the current thread is in a transaction context
pub fn is_in_transaction() -> bool {
    IN_TRANSACTION.with(|flag| flag.get())
}

/// Segments waiting for their post-rotation drain/sync/archive/seal.
///
/// A plain locked vec, not a channel: rotations are rare (once per 8 MB of
/// writes), the consumer is one background thread, and `enqueue` must be able
/// to answer "no" for backpressure -- past `SEAL_QUEUE_LIMIT` the rotating
/// writer archives inline, throttling writers to what archiving sustains.
pub struct SealQueue {
    pending: parking_lot::Mutex<Vec<(usize, u64)>>,
}

/// Beyond this backlog, rotation archives inline. Small on purpose: the queue
/// exists to absorb bursts, not to let backup creation lag a sustained import
/// without bound.
const SEAL_QUEUE_LIMIT: usize = 64;

impl SealQueue {
    pub fn new() -> Self {
        Self {
            pending: parking_lot::Mutex::new(Vec::new()),
        }
    }
    /// True if accepted; false means the caller must do the work inline.
    fn enqueue(&self, chunk_id: usize, seg_id: u64) -> bool {
        let mut pending = self.pending.lock();
        if pending.len() >= SEAL_QUEUE_LIMIT {
            return false;
        }
        pending.push((chunk_id, seg_id));
        true
    }
    pub fn drain(&self) -> Vec<(usize, u64)> {
        std::mem::take(&mut *self.pending.lock())
    }
    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }
}

pub struct Chunk {
    pub id: usize,
    /// Shared with every chunk of the store and drained by the sealer thread.
    pub seal_queue: Arc<SealQueue>,
    pub cell_index: WordMap,
    /// Live bytes per slot, shared across every chunk of this store. See
    /// [`crate::slots::SlotLiveBytes`] for the contract; the hooks live on the
    /// logical transitions of `cell_index` -- insert, replace, remove -- so
    /// dead space and abandoned race-loser entries never count.
    pub slot_bytes: Arc<crate::slots::SlotLiveBytes>,
    pub segs: SegmentList,
    /// Head POOLS, one slot array per segment class. Every slot is EMPTY,
    /// ALLOCATING, or a live segment id, exactly as the single head slot
    /// was -- the rotation machinery (HeadSlotGuard, the ALLOCATING
    /// sentinel, the seal queue) is slot-generic and unchanged. What is new
    /// is that a writer must OWN a head (Segment::try_own) before touching
    /// its cursor, and holds that ownership through its journal write, so
    /// each segment has a single writer at a time: mid-segment holes become
    /// impossible by construction, and each segment's WAL is offset-ordered
    /// and prefix-complete.
    ///
    /// Slots fill lazily: a writer only claims an EMPTY slot when it found
    /// no ownable head, so the pool grows exactly with real contention and
    /// an idle chunk keeps one head, as before.
    pub head_pool: Box<[AtomicU64]>,
    pub blob_head_pool: Box<[AtomicU64]>,
    pub meta: Arc<ServerMeta>,
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub file_manager: Arc<SegmentFileManager>,
    pub total_space: AtomicUsize,
    pub capacity: usize,
    pub gc_lock: Mutex<()>,
    pub allocator: SegmentAllocator,
    pub index_builder: Option<Arc<IndexBuilder>>,
    pub statistics: ChunkStatistics,
    /// Shared tiered memory manager for eviction/promotion
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    /// Shared wake signal to the background cleaner; allocation-pressure
    /// paths request a pass instead of cleaning inline.
    pub cleaner_wake: Arc<crate::ram::cleaner::CleanerWake>,
    /// Segments unpublished from `segs` and awaiting reclamation.
    ///
    /// Reclaiming a segment is destructive three times over: its pages are
    /// dropped (`MADV_DONTNEED`, so they read back as zeros), its backup and
    /// WAL are deleted, and its address returns to the allocator's free list
    /// to be handed to the next segment. Doing any of that while a reader is
    /// still inside is a use-after-free: the reader sees zeros, and once the
    /// address is recycled it sees another segment's live bytes.
    ///
    /// Unpublishing stops new readers. Each entry carries the QSBR stamp taken
    /// at that moment, and is reclaimed once every thread has passed through a
    /// quiescent state since. Holding 8 MiB longer than necessary is the cost;
    /// the alternative is silent corruption with no durable copy left.
    pub retired_segments: Mutex<Vec<RetiredSegment>>,
    /// Set once shutdown has begun archiving; no entry may be acquired after.
    ///
    /// Shutdown archives every dirty segment and only then stops the RPC
    /// server, so cells kept arriving after their segment's image had already
    /// been written. Those writes could not reach the backup, so they went to
    /// a WAL re-created at a seq id that already had one -- the twin that
    /// costs the whole post-archive suffix at the next crash. Sealing makes
    /// such a write impossible; this makes it *visible*, refusing it with an
    /// error instead of accepting a cell that no longer has anywhere durable
    /// to go.
    writes_closed: std::sync::atomic::AtomicBool,
}

/// A segment unpublished from its chunk, waiting for readers to drain.
pub struct RetiredSegment {
    pub segment: AArc<Segment>,
    /// QSBR epoch stamped after the segment left `Chunk::segs`.
    pub stamp: usize,
    /// Drain passes this entry has survived, for reporting one that never
    /// drains rather than retaining it in silence.
    pub attempts: usize,
}

impl Chunk {
    #[inline]
    fn refresh_statistics_for_schema(&self, schema_id: u32) {
        if schema_tracks_statistics(schema_id) {
            self.refresh_statistics();
        }
    }

    /// Debug-only validation for cell locations
    /// Checks alignment and basic sanity of addresses stored in cell index
    #[cfg(debug_assertions)]
    fn validate_cell_location(&self, addr: usize, context: &str) -> bool {
        // Check for obviously invalid addresses
        if addr == 0 {
            error!(
                "[Chunk {}] Invalid cell location at {}: address is NULL (0x0)",
                self.id, context
            );
            return false;
        }

        // Cell data should be at least 8-byte aligned for proper struct access
        if addr % 8 != 0 {
            error!(
                "[Chunk {}] Invalid cell location at {}: address 0x{:x} is not 8-byte aligned",
                self.id, context, addr
            );
            return false;
        }

        // Check if address looks suspicious (too high bits set)
        // Valid pointers on x86-64 typically use only lower 48 bits
        if addr > 0x0000_FFFF_FFFF_FFFF {
            error!(
                "[Chunk {}] Invalid cell location at {}: address 0x{:x} has invalid high bits",
                self.id, context, addr
            );
            return false;
        }

        // Check if the address is within reasonable segment bounds
        // We can't do precise bounds checking without segment info, but we can check basic sanity
        if let Some(segment) = self.locate_segment(addr) {
            let seg_start = segment.addr;
            let seg_end = seg_start + SEGMENT_SIZE;

            if addr < seg_start || addr >= seg_end {
                error!(
                    "[Chunk {}] Invalid cell location at {}: address 0x{:x} outside segment bounds [0x{:x}, 0x{:x})",
                    self.id, context, addr, seg_start, seg_end
                );
                return false;
            }
        } else {
            warn!(
                "[Chunk {}] Cannot validate cell location at {}: address 0x{:x} - segment not found (may be valid for new writes)",
                self.id, context, addr
            );
            // Don't fail validation if segment not found - might be a newly allocated address
        }

        true
    }

    /// Validate address before storing it in cell_index (WRITE operation)
    #[cfg(debug_assertions)]
    #[inline]
    fn assert_address_aligned_for_write(&self, addr: usize, operation: &str, hash: u64) {
        debug_assert!(
            addr % 8 == 0,
            "WRITE POINT: {} attempting to store MISALIGNED address 0x{:016x} (offset: {}) for hash {}",
            operation, addr, addr % 8, hash
        );
        if addr % 8 != 0 {
            error!(
                "WRITE POINT: {} attempting to store misaligned address 0x{:016x} (offset: {}) in cell_index for hash {}",
                operation, addr, addr % 8, hash
            );
        }
    }

    /// Validate address after retrieving it from cell_index (READ operation)
    #[cfg(debug_assertions)]
    #[inline]
    fn assert_address_aligned_for_read(&self, addr: usize, operation: &str, hash: u64) {
        debug_assert!(
            addr % 8 == 0,
            "READ POINT: {} retrieved MISALIGNED address 0x{:016x} (offset: {}) for hash {}",
            operation,
            addr,
            addr % 8,
            hash
        );
        if addr % 8 != 0 {
            error!(
                "READ POINT: {} retrieved misaligned address 0x{:016x} (offset: {}) from cell_index for hash {}",
                operation, addr, addr % 8, hash
            );
        }
    }

    fn new(
        id: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        cleaner_wake: Arc<crate::ram::cleaner::CleanerWake>,
        slot_bytes: Arc<crate::slots::SlotLiveBytes>,
        seal_queue: Arc<SealQueue>,
    ) -> Chunk {
        // Call new_with_base with base_addr=0 to use old allocation behavior
        Self::new_with_base(
            id,
            0,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            cleaner_wake,
            0,
            slot_bytes,
            seal_queue,
        )
    }

    fn new_with_base(
        id: usize,
        base_addr: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        cleaner_wake: Arc<crate::ram::cleaner::CleanerWake>,
        estimated_cells: usize,
        slot_bytes: Arc<crate::slots::SlotLiveBytes>,
        seal_queue: Arc<SealQueue>,
    ) -> Chunk {
        let allocate_memory = base_addr == 0;
        let allocator = SegmentAllocator::new_with_base(id, base_addr, size, allocate_memory);

        // Create file manager
        let file_manager = Arc::new(SegmentFileManager::new(
            backup_storage.clone(),
            wal_storage.clone(),
        ));

        // Initialize storage directories
        if let Err(e) = file_manager.init_directories() {
            panic!("Failed to initialize storage directories: {}", e);
        }

        let bootstrap_segment = allocator
            .alloc_seg(&file_manager)
            .expect(&format!("No space left for first segment in chunk {}", id));
        let num_segs = {
            let n = size / SEGMENT_SIZE;
            if n > 0 {
                n
            } else {
                n + 1
            }
        };
        assert!(
            !(base_addr == 0 && tiered_manager.is_some()),
            "Should not enable tiered memory if the memory is not allocated by Chunks"
        );
        debug!("Creating chunk {}, num segments {}", id, num_segs);
        let segs = SegmentList::new(num_segs);
        // Sized for the cells this chunk is about to hold, because a WordMap
        // cannot be resized afterwards and each partition that outgrows itself
        // doubles, copies, and frees the old table into a per-thread allocator
        // arena that never returns it.
        //
        // A RECOVERED chunk gets `estimated_cells`, derived from its files via
        // `estimated_cells_per_segment`. A FRESH chunk used to fall back to
        // `num_segs * 64`, which is the same guess expressed as 128 KB per
        // cell -- 128x the 1 KB default the estimator uses, and the direction
        // the estimator's own doc calls dangerous: "set too high it means too
        // few cells per segment, under-sizing the index and paying a doubling
        // for every factor of two it is short; set too low it merely
        // over-allocates, bounded and predictable."
        //
        // So a recovered store indexed itself correctly and a fresh one did
        // not, which is why this only ever showed up on freshly-written
        // stores. Measured on .239: a reshard of 2.1M cells across 768 fresh
        // chunks spent ALL of its recipient-write time in the index insert, at
        // 2619 us/cell, and ran at 45 MB/s; the same shape with half the cells
        // ran at 634 MB/s. Both halves of the fallback now use one estimator,
        // which is also the knob (`NEB_ESTIMATED_CELL_BYTES`) a workload far
        // from 1 KB/cell is supposed to set.
        let index_capacity = estimated_cells
            .max(num_segs.saturating_mul(crate::ram::segs::estimated_cells_per_segment()))
            .clamp(4_096, MAX_CELL_INDEX_CAPACITY)
            .next_power_of_two();
        let index = WordMap::with_capacity(index_capacity);
        let chunk = Chunk {
            id,
            segs,
            cell_index: index,
            slot_bytes,
            seal_queue,
            meta,
            backup_storage,
            wal_storage,
            file_manager,
            allocator,
            index_builder,
            capacity: size,
            total_space: AtomicUsize::new(0),
            head_pool: {
                let pool: Vec<AtomicU64> = (0..head_pool_len(SegmentClass::Regular))
                    .map(|i| {
                        AtomicU64::new(if i == 0 {
                            bootstrap_segment.id
                        } else {
                            HEAD_SEG_ID_EMPTY
                        })
                    })
                    .collect();
                pool.into_boxed_slice()
            },
            blob_head_pool: (0..head_pool_len(SegmentClass::Blob))
                .map(|_| AtomicU64::new(HEAD_SEG_ID_EMPTY))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            gc_lock: Mutex::new(()),
            statistics: ChunkStatistics::new(),
            tiered_manager,
            cleaner_wake,
            retired_segments: Mutex::new(Vec::new()),
            writes_closed: std::sync::atomic::AtomicBool::new(false),
        };
        chunk.put_segment(bootstrap_segment);
        return chunk;
    }

    #[inline]
    fn head_slots(&self, segment_class: SegmentClass) -> &[AtomicU64] {
        match segment_class {
            SegmentClass::Regular => &self.head_pool,
            SegmentClass::Blob => &self.blob_head_pool,
        }
    }

    /// First live regular head, for logging and single-threaded tests. With
    /// a pool there is no single "the head"; callers that need the full
    /// picture scan the pools themselves.
    pub fn get_head_seg_id(&self) -> u64 {
        for slot in self.head_pool.iter() {
            let id = slot.load(Ordering::Acquire);
            if id != HEAD_SEG_ID_EMPTY && id != HEAD_SEG_ID_ALLOCATING {
                return id;
            }
        }
        HEAD_SEG_ID_EMPTY
    }

    /// Re-establish "sealed implies archived" after recovery.
    ///
    /// A crash, a kill, or any shutdown that loses its tail leaves segments
    /// whose only durable copy is a WAL file. Recovery is the one point where
    /// that can be repaired unconditionally: by here the write heads have been
    /// reset, so every OTHER segment is sealed and immutable and must have a
    /// backup. Archiving them once restores the invariant the tier, the
    /// cleaner and the archiver all rely on — before any of them run.
    ///
    /// Only the heads are allowed to remain WAL-only, because only they are
    /// still mutable. Returns how many segments had to be repaired; a nonzero
    /// count is normal after an unclean stop and is reported so a run that
    /// inherits a large backlog says so.
    pub(crate) fn archive_unarchived_after_recovery(&self) -> usize {
        let mut repaired = 0usize;
        for segment in self.segments() {
            if self.is_active_head(segment.id) {
                continue;
            }
            let has_backup = self
                .file_manager
                .backup_path(segment.chunk_id, segment.id, segment.seq_id)
                .map(|p| std::path::Path::new(&p).exists())
                .unwrap_or(false);
            if has_backup {
                continue;
            }
            match segment.archive() {
                Ok(true) => repaired += 1,
                Ok(false) => {}
                Err(e) => error!(
                    "recovery repair: failed to archive sealed segment {} (chunk {}): {}.                      Its only durable copy remains its WAL.",
                    segment.id, self.id, e
                ),
            }
        }
        repaired
    }

    pub fn is_active_head(&self, seg_id: u64) -> bool {
        self.head_pool
            .iter()
            .chain(self.blob_head_pool.iter())
            .any(|slot| slot.load(Ordering::Acquire) == seg_id)
    }

    #[inline]
    pub fn has_blob_head(&self) -> bool {
        self.blob_head_pool
            .iter()
            .any(|slot| slot.load(Ordering::Acquire) != HEAD_SEG_ID_EMPTY)
    }

    pub fn try_acquire(&self, size: u32, full_gc: bool) -> Result<PendingEntry, WriteError> {
        self.try_acquire_in_class(size, full_gc, SegmentClass::Regular)
    }

    /// Claim one span for a run of entries whose sizes are known.
    ///
    /// Returns the span and how many of `sizes` it covers: a run is claimed
    /// from ONE segment, so if the head cannot hold the whole batch this takes
    /// the prefix that fits and the caller comes back for the rest. Refusing
    /// the whole batch instead would need cross-segment bookkeeping for no
    /// gain, since the second call simply rotates to a fresh head.
    ///
    /// Falls back to a single-entry claim when only one entry fits, which is
    /// exactly `try_acquire_in_class` with one more atomic.
    pub fn try_acquire_run(
        &self,
        sizes: &[u32],
        segment_class: SegmentClass,
    ) -> Result<Option<(PendingRun, usize)>, WriteError> {
        if sizes.is_empty() {
            return Ok(None);
        }
        if self.writes_closed.load(Ordering::Acquire) {
            return Err(WriteError::ServerShuttingDown);
        }
        let slots = self.head_slots(segment_class);
        let start = head_affinity();
        // One pass over the pool: the first head this writer can OWN gets
        // the run. Anything unownable (owned by another writer, allocating,
        // or empty) is someone else's business -- the caller falls back to
        // the single-entry path, which owns the rotation machinery.
        for i in 0..slots.len() {
            let slot = &slots[(start + i) % slots.len()];
            let head_seg_id = slot.load(Ordering::Acquire);
            if head_seg_id == HEAD_SEG_ID_ALLOCATING || head_seg_id == HEAD_SEG_ID_EMPTY {
                continue;
            }
            {
                let head = match self.segs.get(&(head_seg_id as usize)) {
                    Some(seg) => seg,
                    None => {
                        continue;
                    }
                };
                // OWN the head before anything else: under the pool, a
                // segment has one writer at a time, and the ownership window
                // (through the last journal write, released in
                // PendingRun::drop) is what makes its WAL offset-ordered and
                // its image hole-free by construction.
                if !head.try_own() {
                    continue;
                }
                // Reference BEFORE claiming, for the reason spelled out in
                // `try_acquire_in_class`: claiming first lets a rotation see
                // zero references and seal the segment out from under a writer
                // that has already moved the cursor.
                if !head.incr_references() {
                    head.release_own();
                    continue;
                }
                // How much of the batch fits in what is left of this segment.
                let remaining = head
                    .bound()
                    .saturating_sub(head.append_header.load(Ordering::Acquire));
                let mut total: u32 = 0;
                let mut covered = 0usize;
                for size in sizes {
                    match total.checked_add(*size) {
                        Some(next) if (next as usize) <= remaining => {
                            total = next;
                            covered += 1;
                        }
                        _ => break,
                    }
                }
                // A sealing head takes the same route as in the
                // single-entry path: return None so the caller writes one
                // cell the ordinary way, which rotates to a fresh segment.
                if covered > 0 && !head.begin_pending_journal() {
                    head.decr_references();
                    head.release_own();
                    return Ok(None);
                }
                if covered > 0 {
                    if let Some(base) = head.try_acquire_run(total) {
                        // Same reason as the single-entry path, and it matters
                        // more here: a run reserves the whole batch at once,
                        // so an unfilled one leaves a much larger hole.
                        crate::ram::entry::stamp_reservation_padding(base, total);
                        return Ok(Some((
                            PendingRun {
                                base,
                                seg: head,
                                size: total,
                                skip_sync: is_in_transaction(),
                                consumed: std::cell::Cell::new(0),
                            },
                            covered,
                        )));
                    }
                    // The claim was taken before the cursor was tried; give
                    // it back, or this segment can never seal.
                    head.end_pending_journal();
                }
                // Nothing fits in this head: give everything back and try
                // the next slot.
                head.decr_references();
                head.release_own();
            }
        }
        // No ownable head fits the run. Rotation, emergency GC and the
        // near-capacity refusal all live in the single-entry path; rather
        // than duplicate that (and risk the two copies drifting), say so and
        // let the caller write one cell the ordinary way. That call rotates
        // or fills a slot, and the next run claim finds a fresh segment.
        Ok(None)
    }

    pub fn try_acquire_in_class(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
    ) -> Result<PendingEntry, WriteError> {
        if self.writes_closed.load(Ordering::Acquire) {
            return Err(WriteError::ServerShuttingDown);
        }
        let mut tried_gc = false;
        let backoff = Backoff::new();
        let slots = self.head_slots(segment_class);
        let start = head_affinity();
        loop {
            // Re-checked every pass, not just on entry. A writer already
            // inside this loop when shutdown begins would otherwise keep
            // looking for a home: `archive_all` seals each head in turn, so
            // every fresh segment this loop allocates is closed behind it,
            // and the writer walks the chunk to capacity instead of
            // stopping. Shutdown means stop writing, from wherever you are.
            if self.writes_closed.load(Ordering::Acquire) {
                return Err(WriteError::ServerShuttingDown);
            }
            // Scan the pool from this thread's affinity slot. The head this
            // writer can OWN takes the entry; a full one becomes the
            // rotation target; an EMPTY slot is the fallback target so the
            // pool grows exactly when contention demands it.
            let mut rotate_target: Option<(&AtomicU64, u64)> = None;
            let mut empty_slot: Option<&AtomicU64> = None;
            for i in 0..slots.len() {
                let slot = &slots[(start + i) % slots.len()];
                let head_seg_id = slot.load(Ordering::Acquire);
                if head_seg_id == HEAD_SEG_ID_ALLOCATING {
                    continue;
                }
                if head_seg_id == HEAD_SEG_ID_EMPTY {
                    if empty_slot.is_none() {
                        empty_slot = Some(slot);
                    }
                    continue;
                }
                let head = match self.segs.get(&(head_seg_id as usize)) {
                    Some(seg) => seg,
                    None => {
                        debug!(
                            "Head segment {} was removed, trying the next slot",
                            head_seg_id
                        );
                        continue;
                    }
                };
                // OWN the head first. Under the pool a segment has ONE
                // writer at a time, from here through the journal write
                // (released in PendingEntry::drop): the cursor can never run
                // ahead of another writer's unwritten bytes, so mid-segment
                // holes are impossible by construction and the WAL is
                // offset-ordered. A head someone else owns is simply the
                // next slot's problem.
                if !head.try_own() {
                    continue;
                }
                // Take the reference BEFORE claiming space, not after.
                //
                // The reference is what rotation waits on before it archives a
                // segment. Claiming space first left a gap in which a rotation
                // could see zero references, conclude every writer had
                // finished, and seal the segment out from under a writer that
                // had already advanced its append cursor -- whose WAL write
                // then had nowhere to go. One entry in 845M hit exactly that
                // during a TB import, which is rare enough to look like noise
                // and is still a lost write.
                //
                // And the reference must actually be TAKEN. `incr_references`
                // returns false when the segment is exclusively held -- an
                // evictor, a promoter or the cleaner owns it and is about to
                // free or replace its pages. Every reader honours that answer;
                // this path used to discard it and append anyway, which puts a
                // cell into memory that `evict_segment` is about to
                // `madvise(MADV_DONTNEED)`. The append cursor moves, the WAL
                // entry is written, the cell index takes the address, and the
                // pages are then zeroed underneath it -- so the next read finds
                // `Id(0)` where the cell should be and reports it missing.
                //
                // That is the signature behind the silent cell loss under tier
                // pressure: `stale cell read: ... found Id(0)`, only ever with
                // eviction active, intermittent because it needs the exclusive
                // CAS to land inside this window.
                if !head.incr_references() {
                    // An evictor/promoter/cleaner holds this segment
                    // exclusively. Not our head; next slot.
                    head.release_own();
                    continue;
                }
                // Claimed BEFORE the cursor moves. A writer that has taken
                // space but not yet claimed is invisible to a seal's drain,
                // and that is precisely the entry that ends up with nowhere
                // to journal.
                if head.begin_pending_journal() {
                    if let Some(addr) = head.try_acquire(size) {
                        // Describe the reservation before filling it. A crash
                        // between here and the entry write would otherwise
                        // leave zeros mid-segment, and a scan that stops at
                        // zeros in the MIDDLE discards every entry appended
                        // after them. The real header overwrites this one.
                        crate::ram::entry::stamp_reservation_padding(addr, size);
                        trace!(
                            "Chunk {} acquired address {} for size {} in segment {} ({:?})",
                            self.id,
                            addr,
                            size,
                            head.id,
                            segment_class
                        );
                        return Ok(PendingEntry {
                            addr,
                            seg: head,
                            size,
                            skip_sync: is_in_transaction(),
                        });
                    }
                    // No space in this segment: give everything back and
                    // make THIS slot the rotation target.
                    head.end_pending_journal();
                    head.decr_references();
                    head.release_own();
                    rotate_target = Some((slot, head_seg_id));
                    break;
                } else {
                    // This head is sealing; it will never accept another
                    // write. Next slot.
                    head.decr_references();
                    head.release_own();
                    continue;
                }
            }

            // Decide where the allocation work goes: a full head's slot
            // rotates; failing that, an EMPTY slot gets its first segment;
            // failing THAT, every slot is owned or allocating -- spin
            // briefly and rescan.
            let (head_slot, head_seg_id) = match rotate_target {
                Some(target) => target,
                None => match empty_slot {
                    Some(slot) => (slot, HEAD_SEG_ID_EMPTY),
                    None => {
                        let t_spin = std::time::Instant::now();
                        backoff.spin();
                        ALLOC_SPIN_NANOS
                            .fetch_add(t_spin.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        continue;
                    }
                },
            };

            let total_space = self.segs.len() * SEGMENT_SIZE;
            // Trigger emergency cleaning one segment early: a moving combine
            // needs at least one free segment as its destination, and hitting
            // the exact wall leaves it nothing to relocate into. Allocation
            // itself is refused only at the original wall, so usable capacity
            // is unchanged.
            let reserve_boundary = self.capacity.saturating_sub(2 * SEGMENT_SIZE);

            // GROWTH IS OPTIONAL; a wait is not a failure. This allocation
            // fills an EMPTY slot when every live head was owned by someone
            // else -- parallelism, not capacity. Near the wall, the right
            // trade is the one the single-head design always made: queue on
            // an existing head until its owner releases (microseconds), and
            // never refuse a write that yesterday's store would have
            // absorbed just because the pool WANTED another segment. Without
            // this, every small store failed under concurrency with
            // CannotAllocateSpace the moment writers outnumbered segments.
            let growing = head_seg_id == HEAD_SEG_ID_EMPTY;
            let any_live_head = || {
                slots.iter().any(|slot| {
                    let id = slot.load(Ordering::Acquire);
                    id != HEAD_SEG_ID_EMPTY && id != HEAD_SEG_ID_ALLOCATING
                })
            };
            if growing && total_space >= reserve_boundary && any_live_head() {
                backoff.spin();
                continue;
            }
            if total_space >= reserve_boundary && !tried_gc {
                if full_gc {
                    warn!("Chunk {} near capacity, emergency full GC", self.id);
                    let _ = Cleaner::clean(self, true, true);
                } else {
                    warn!("Chunk {} near capacity, emergency best effort GC", self.id);
                    let _ = Cleaner::clean(self, true, false);
                }
                tried_gc = true;
                continue;
            }
            if total_space >= self.capacity - SEGMENT_SIZE {
                if tried_gc {
                    debug!(
                        "chunk-allocation-failure: chunk={}, total_space={}, capacity={}, head_seg_id={}, seg_count={}, full_gc={}, segment_class={:?}",
                        self.id,
                        total_space,
                        self.capacity,
                        head_slot.load(Ordering::Relaxed),
                        self.segs.len(),
                        full_gc,
                        segment_class
                    );
                    error!("No space left for chunk {}, cannot allocate space", self.id);
                    ALLOCATION_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                    return Err(WriteError::CannotAllocateSpace);
                } else if full_gc {
                    warn!("No space left for chunk {}, emergency full GC", self.id);
                    let _ = Cleaner::clean(self, true, true);
                    tried_gc = true;
                    continue;
                } else {
                    warn!(
                        "No space left for chunk {}, emergency best effort GC",
                        self.id
                    );
                    let _ = Cleaner::clean(self, true, false);
                    tried_gc = true;
                    continue;
                }
            }
            if self.allocator.meet_gc_threshold() {
                // Wake the background cleaner instead of running a partial GC
                // inline: a synchronous combine stalls this writer for a full
                // collection cycle, and the cleaner's pressure pacing keeps
                // reclamation tracking the fill rate once woken.
                debug!("Allocator meet GC threshold, waking the cleaner");
                self.cleaner_wake.request();
            }

            if head_slot
                .compare_exchange(
                    head_seg_id,
                    HEAD_SEG_ID_ALLOCATING,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_err()
            {
                backoff.spin();
                continue;
            }

            // From here the head slot reads HEAD_SEG_ID_ALLOCATING, and every
            // other writer spins waiting for it to be replaced. Leaving it
            // that way is not a failed write, it is a chunk that never accepts
            // another write: the old `.expect("No space left after full GCs")`
            // panicked right here, and the survivors spun on ALLOCATING at
            // 300% CPU forever. The guard restores the previous head on ANY
            // early exit, panic included, so the worst case is an error the
            // caller can act on.
            let t_rotate = std::time::Instant::now();
            ROTATIONS.fetch_add(1, Ordering::Relaxed);
            let _rotate_timer = RotateTimer(t_rotate);
            let restore = HeadSlotGuard {
                slot: head_slot,
                restore_to: head_seg_id,
            };

            let t_alloc_seg = std::time::Instant::now();
            let new_seg_opt = match self
                .allocator
                .alloc_seg_for_writer(&self.file_manager, segment_class)
            {
                Some(seg) => Some(seg),
                None => {
                    // The capacity check above counts `segs`, but a segment
                    // that has been unpublished still owns its address until
                    // its readers drain and it is reclaimed. So the chunk can
                    // read as having room while the allocator has none. Give
                    // the reclaimer its chance before declaring exhaustion --
                    // those addresses are ours, just not yet handed back.
                    let reclaimed = self.drain_retired_segments();
                    if reclaimed > 0 {
                        debug!(
                            "Chunk {} reclaimed {} retired segments to satisfy an allocation",
                            self.id, reclaimed
                        );
                        self.allocator
                            .alloc_seg_for_writer(&self.file_manager, segment_class)
                    } else {
                        None
                    }
                }
            };
            ROTATE_ALLOC_SEG_NANOS
                .fetch_add(t_alloc_seg.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let Some(new_seg) = new_seg_opt else {
                // Space was there when we checked, and is gone now -- another
                // writer took the last segment between the check and here.
                drop(restore);
                // A failed GROWTH still is not a failed write while live
                // heads exist: put the slot back and queue on one of them.
                if growing && any_live_head() {
                    backoff.spin();
                    continue;
                }
                error!(
                    "chunk-allocation-failure: chunk={}, segment_class={:?}, seg_count={}, \
                     capacity={}: the allocator has no segment left after GC",
                    self.id,
                    segment_class,
                    self.segs.len(),
                    self.capacity
                );
                return Err(WriteError::CannotAllocateSpace);
            };
            let new_seg_id = new_seg.id;

            // Publish new head segment id FIRST
            // This creates a window where the head_seg_id points to a segment not yet in self.segs.
            // Readers of head_seg_id must handle this by retrying.
            restore.publish(new_seg_id);

            self.put_segment(new_seg);

            if head_seg_id != HEAD_SEG_ID_EMPTY {
                // The old head's drain -> WAL sync -> archive -> seal. The
                // archive is a full segment compress + write + fsync, measured
                // at 68% of ALL rotation time and rotation at ~95% of a bulk
                // write's allocation cost -- taken inline, on the write path,
                // by whichever writer happened to fill the segment. Nothing
                // downstream needs it to be synchronous: the WAL already makes
                // the data durable, and the skip path below has always left
                // un-drained segments dirty and unsealed for eviction,
                // shutdown or recovery to archive later. So hand the work to
                // the background sealer, which runs the identical sequence.
                //
                // Backpressure instead of an unbounded queue: archiving
                // sustains ~190 MB/s per thread while the write path bursts
                // past 1 GB/s, so a long import could outrun the sealer. Past
                // a small backlog the rotating writer archives inline again,
                // which throttles the writers to what archiving keeps up
                // with -- the exact behaviour before this change, only now it
                // costs the write path nothing until the sealer is actually
                // behind.
                let sealed_in_background = std::env::var("NEB_SYNC_SEAL_ARCHIVE").as_deref()
                    != Ok("1")
                    && self.seal_queue.enqueue(self.id, head_seg_id);
                if !sealed_in_background {
                    self.finish_rotated_head(head_seg_id);
                }
            }
        }
    }

    /// Drain, sync, archive and seal a segment that has just stopped being the
    /// head. Runs inline from rotation (under backpressure or the
    /// NEB_SYNC_SEAL_ARCHIVE toggle) or on the background sealer; the sequence
    /// is identical either way.
    pub(crate) fn finish_rotated_head(&self, head_seg_id: u64) {
            if let Some(old_head) = self.segs.get(&(head_seg_id as usize)) {
                // Let the writers that acquired from this segment finish
                // before it stops being writable.
                //
                // Rotation only publishes a new head; entries acquired from
                // the old one moments earlier are still in flight, and each
                // holds a reference until its `PendingEntry` drops (which is
                // where the WAL write happens). Closing its WAL and
                // archiving underneath them is what made the twin: the late
                // write found no WAL and re-created one beside a backup that
                // did not contain it. Sealing turned that silent corruption
                // into a refused write -- 3,958 failed batches in one import
                // -- which is the same race being reported instead of
                // hidden. Draining first is the actual fix.
                //
                // Bounded, because a reference can also be a long-lived read
                // guard and rotation runs on the write path. If it does not
                // drain, the archive is simply skipped: the segment stays
                // dirty and unsealed, so late writes still land in its WAL
                // and eviction, shutdown or recovery archive it later.
                let t_drain = std::time::Instant::now();
                let drain = Backoff::new();
                let mut drained = old_head.no_references();
                for _ in 0..512 {
                    if drained {
                        break;
                    }
                    // We are waiting on other threads to finish their
                    // writes; the head slot already points at the new
                    // segment, so nothing is blocked behind this. Yield
                    // periodically rather than burning a core on a wait
                    // that is normally over in microseconds.
                    drain.spin();
                    std::thread::yield_now();
                    drained = old_head.no_references();
                }
                ROTATE_DRAIN_NANOS
                    .fetch_add(t_drain.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if !drained {
                    debug!(
                        "Segment {} (chunk {}) still has {} references at rotation; leaving it \
                         dirty and unsealed rather than archiving under an active writer",
                        head_seg_id,
                        self.id,
                        old_head.references_count()
                    );
                    return;
                }
                if let Err(e) = old_head.force_wal_sync() {
                    warn!(
                        "Failed to sync WAL for old head segment {}: {}",
                        head_seg_id, e
                    );
                }
                let mut state = old_head.file_state.lock();
                if let Some(wal) = state.wal.take() {
                    if let Err(e) = wal.sync_all() {
                        warn!(
                            "Failed to sync WAL during close for old head segment {}: {}",
                            head_seg_id, e
                        );
                    }
                    drop(wal);
                    debug!(
                        "Closed WAL file for old head segment {} (freed file descriptor)",
                        head_seg_id
                    );
                }
                drop(state);

                // Seal-time archive. A segment stops being head exactly
                // once, and from then on it is immutable, so this is the
                // one place its durable copy should be written -- and the
                // only place that makes "sealed implies archived" an
                // invariant the rest of the system can rely on.
                //
                // Without it, archiving only happened incidentally: from
                // eviction, from combine, or from archive_all at
                // shutdown. A sealed segment that was never evicted stayed
                // durable only in its WAL, so a restart restored it from
                // WAL with no backup at all -- and everything downstream
                // (the tier believing it droppable, the cleaner freeing
                // it, the archiver later writing a zero-filled image over
                // it) followed from that. The 2016 boundary snapshot shows
                // 977 such segments against 20,697 archived ones; TB13
                // carried 18,336 of them into the restart that lost data.
                let t_archive = std::time::Instant::now();
                let archive_result = old_head.archive();
                ROTATE_ARCHIVE_NANOS
                    .fetch_add(t_archive.elapsed().as_nanos() as u64, Ordering::Relaxed);
                match archive_result {
                    Ok(true) => debug!("Sealed and archived segment {}", head_seg_id),
                    // Already archived: re-sealing must never rewrite an
                    // immutable segment.
                    Ok(false) => {}
                    Err(e) => error!(
                        "SEAL ARCHIVE FAILED for segment {} (chunk {}): {}. It stays dirty \
                         and resident; its only durable copy is its WAL.",
                        head_seg_id, self.id, e
                    ),
                }
        }
    }

    #[cfg(test)]
    /// Both heads, with `None` for "no head yet". An empty regular head is
    /// an ordinary state now, not just a blob one: recovery leaves both
    /// empty, and the first write of each class allocates a fresh segment.
    pub fn head_seg_ids_for_test(&self) -> (Option<u64>, Option<u64>) {
        let head_or_none = |id: u64| {
            (id != HEAD_SEG_ID_EMPTY && id != HEAD_SEG_ID_ALLOCATING).then_some(id)
        };
        let first_blob = self
            .blob_head_pool
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .find(|id| *id != HEAD_SEG_ID_EMPTY && *id != HEAD_SEG_ID_ALLOCATING);
        (head_or_none(self.get_head_seg_id()), first_blob)
    }

    pub(crate) fn reset_write_heads_after_recovery(&self) -> io::Result<()> {
        for slot in self.blob_head_pool.iter() {
            slot.store(HEAD_SEG_ID_EMPTY, Ordering::Release);
        }

        // EVERY recovered segment is a closed incarnation, whether it was
        // rebuilt from a backup or from a WAL. Backup-recovered ones were
        // already excluded (resuming one produced backup/WAL twins at a
        // single seq id). WAL-recovered ones were not, and that was worse:
        // recovery rewinds `append_header` past a torn tail but the WAL is
        // reopened in APPEND mode, so the next write lands at file offset
        // "old file length" while describing segment offset "append_header".
        // The positional invariant the log depended on is broken from that
        // moment, and every write to that segment is unrecoverable at the
        // next crash -- silently, because nothing checks.
        //
        // So recovery resumes nothing. Each recovered segment is sealed and
        // archived by `archive_unarchived_after_recovery` below, and the
        // first write allocates a fresh segment with a fresh seq id. The
        // cost is the unfilled tail of the segments that were open when the
        // process stopped: at most one per chunk, bounded by the segment
        // size, and reclaimed by the cleaner like any other dead space.
        for segment in self.segments() {
            if !segment.is_sealed() {
                segment.seal();
            }
        }
        for slot in self.head_pool.iter() {
            slot.store(HEAD_SEG_ID_EMPTY, Ordering::Release);
        }

        Ok(())
    }

    pub fn location_for_read<'a>(&self, hash: u64) -> Result<CellReadGuard<'_>, ReadError> {
        let guard = self.cell_index.lock(hash as usize);
        match guard {
            Some(index) => {
                if *index == 0 {
                    warn!("Cannot find cell with hash {} for index is zero", hash);
                    return Err(ReadError::CellDoesNotExisted);
                }
                return Ok(index);
            }
            None => {
                if hash == 0 {
                    Err(ReadError::CellIdIsUnitId)
                } else {
                    trace!(
                        "Cannot find cell with hash {} for it is not in the map",
                        hash
                    );
                    Err(ReadError::CellDoesNotExisted)
                }
            }
        }
    }

    pub fn location_for_write(&self, hash: u64, has_read: bool) -> Option<CellWriteGuard<'_>> {
        let guard = self.cell_index.lock(hash as usize);
        match guard {
            Some(index) => {
                if *index == 0 {
                    return None;
                }

                #[cfg(debug_assertions)]
                self.assert_address_aligned_for_read(*index, "location_for_write", hash);
                Some(index)
            }
            None => None,
        }
    }

    pub fn lock_or_insert_cell(&self, hash: u64) -> CellGuard<'_> {
        let backoff = Backoff::new();
        loop {
            let guard = self.cell_index.lock_or_insert(hash as usize, 0);
            if let Some(guard) = CellGuard::from_guard(hash, guard, self) {
                return guard;
            }
            backoff.spin();
        }
    }

    pub(crate) fn head_cell(&self, hash: u64) -> Result<CellHeader, ReadError> {
        header_from_chunk_raw(*CellGuard::for_read(hash, self)?).map(|pair| pair.0)
    }

    // Cheap capture of the current cell's raw address and version: an index
    // lookup plus a header decode, with no value materialization. Used by
    // repeatable-read pinning, where parsing the payload just to learn where it
    // lives would defeat the point of pinning.
    pub(crate) fn cell_location_and_version(&self, hash: u64) -> Result<(usize, u64), ReadError> {
        let location = *CellGuard::for_read(hash, self)?;
        let (header, _) = header_from_chunk_raw(location)?;
        Ok((location, header.version))
    }

    // By-address header read: decodes the header stored at a caller-pinned raw
    // `location` instead of resolving through the cell index. Used by
    // repeatable-read pinning, where the caller already holds a segment guard
    // that keeps the bytes at `location` alive even after the cell index has
    // moved on to a newer version.
    pub(crate) fn head_at(&self, location: usize) -> Result<CellHeader, ReadError> {
        // Decodes a header straight out of segment memory; same reasoning as
        // `read_cell_at`.
        let _qsbr = crate::ram::qsbr::QsbrSection::new();
        header_from_chunk_raw(location).map(|pair| pair.0)
    }

    pub fn read_cell(&self, hash: u64) -> Result<SharedCell<'_>, ReadError> {
        SharedCell::from_chunk_raw(hash, CellGuard::for_read(hash, self)?, self).map(|(c, _)| c)
    }

    // By-address full-cell read: materializes the cell exactly as stored at
    // `location`, bypassing the cell index entirely. See `head_at`.
    pub fn read_cell_at(&self, hash: u64, location: usize) -> Result<OwnedCell, ReadError> {
        // Reads segment memory directly at `location`, so it needs a section
        // of its own: there is no CellGuard here to carry one. The caller's
        // pin keeps the segment alive; this keeps the reclaimer from freeing
        // underneath the read itself.
        let _qsbr = crate::ram::qsbr::QsbrSection::new();
        SharedCellData::from_chunk_raw(hash, location, self).map(|(cell, _)| cell.to_owned())
    }

    fn read_selected(
        &self,
        hash: u64,
        fields: &[u64],
        need_header: bool,
    ) -> Result<SharedCell<'_>, ReadError> {
        let loc = CellGuard::for_read(hash, self)?;
        let (val, hdr) = select_from_chunk_raw(*loc, self, fields, need_header)?;
        Ok(SharedCell::compose(
            SharedCellData::from_data(hdr, val),
            loc,
        ))
    }

    // By-address projected read: same field-projection logic as `read_selected`,
    // but pinned to `location` instead of following the cell index.
    fn read_selected_at(
        &self,
        location: usize,
        fields: &[u64],
        need_header: bool,
    ) -> Result<OwnedCell, ReadError> {
        // By-address selection; same reasoning as `read_cell_at`.
        let _qsbr = crate::ram::qsbr::QsbrSection::new();
        let (val, hdr) = select_from_chunk_raw(location, self, fields, need_header)?;
        Ok(SharedCellData::from_data(hdr, val).to_owned())
    }

    fn read_partial_raw(&self, hash: u64, offset: usize, len: usize) -> Result<Vec<u8>, ReadError> {
        let loc = CellGuard::for_read(hash, self)?;
        let head_ptr = *loc + offset;
        let mut data = Vec::with_capacity(len);
        for ptr in head_ptr..(head_ptr + len) {
            data.push(unsafe { *(ptr as *const u8) });
        }
        Ok(data.to_vec())
    }

    pub fn write_cell_to_chunk<'a>(
        &self,
        cell: &OwnedCell,
        write_plan: &WritePlan,
        pending_entry: &PendingEntry,
        old_version: u64,
    ) -> Result<WriteToChunkResult, WriteError> {
        cell.write_to_chunk_with(write_plan, pending_entry, old_version)
    }

    fn ensure_indices(&self, new_cell: &OwnedCell, old_cell: Option<&SharedCell>, schema: &Schema) {
        if let Some(index_builder) = &self.index_builder {
            let old_indices = old_cell.map(|cell| probe_cell_indices(cell, &*schema));
            index_builder.ensure_indices(new_cell, &*schema, old_indices);
        }
    }

    fn remove_indices(&self, cell: &SharedCell, schema: &Schema) {
        if let Some(indexer) = &self.index_builder {
            indexer.remove_indices(&cell, &*schema)
        }
    }

    fn ensure_indices_with_res(
        &self,
        cell: &OwnedCell,
        old_indices: Option<Vec<IndexRes>>,
        schema: &Schema,
    ) {
        if let Some(index_builder) = &self.index_builder {
            index_builder.ensure_indices(cell, schema, old_indices)
        }
    }

    fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        debug!("Writing cell {:?} to chunk {}", cell.id(), self.id);
        // Per-phase timing. Throughput here is flat against concurrency, which
        // means latency rises to match and some phase is being served at a
        // fixed rate. Totals alone cannot show which; a phase whose per-cell
        // cost is invariant as load rises is the one holding the rate.
        let t_plan = std::time::Instant::now();
        let write_plan = cell.plan_write(self)?;
        WRITE_PLAN_NANOS.fetch_add(t_plan.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Existing-cell fast path, BEFORE any allocation. Refusing the write
        // is not enough: this is exactly the retry path a crashed index heals
        // through -- a SIGKILL preserves acked cells in the WAL while the
        // ranged tree loses its un-flushed tail, and the retrying writer's
        // insert then finds the cell already present. Re-assert the indices
        // from the EXISTING image (identical old/new pairs cancel for every
        // index type except ranged, whose insert is idempotent and
        // re-established on purpose; see ensure_indices_). Ordering matters:
        // this must run before `allocate`, or a chunk too full to accept new
        // entries fails with CannotAllocateSpace first and the heal path is
        // unreachable exactly when nothing else can re-create the entries.
        if let Some(mut cell_guard) = CellGuard::for_write(cell.header.id.bits(), true, self) {
            if self.index_builder.is_some() {
                if let Ok(existing) = cell_guard.read_cell_owned() {
                    if let Some(schema) = self.meta.schemas.get(&existing.header.schema) {
                        let old_indices = Some(probe_cell_indices(&existing, &*schema));
                        drop(cell_guard);
                        self.ensure_indices_with_res(&existing, old_indices, &*schema);
                    }
                }
            }
            return Err(WriteError::CellAlreadyExisted);
        }

        let t_alloc = std::time::Instant::now();
        let pending_entry = write_plan.allocate(self, true)?;
        WRITE_ALLOC_NANOS.fetch_add(t_alloc.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let t_copy = std::time::Instant::now();
        let write_result =
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell.header.version)?;
        WRITE_COPY_NANOS.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let cell_loc = write_result.addr;
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                self.validate_cell_location(
                    cell_loc,
                    &format!("write_cell(hash={})", cell.header.id.bits())
                ),
                "Attempting to store invalid cell location 0x{:x} in cell index for hash {}",
                cell_loc,
                cell.header.id.bits()
            );
        }

        let t_index = std::time::Instant::now();
        let insert = self.cell_index.try_insert_locked(cell.header.id.bits() as usize);
        WRITE_INDEX_NANOS.fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
        match insert {
            Some(mut guard) => {
                #[cfg(debug_assertions)]
                self.assert_address_aligned_for_write(cell_loc, "write_cell", cell.header.id.bits());

                *guard = cell_loc;
                drop(guard);
                self.slot_bytes
                    .add(cell.header.id.bits(), write_result.content_length);
                let t_sec = std::time::Instant::now();
                self.ensure_indices(cell, None, &*write_plan.schema);
                WRITE_SECONDARY_NANOS.fetch_add(t_sec.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let t_stats = std::time::Instant::now();
                self.refresh_statistics_for_schema(write_plan.schema.id);
                WRITE_STATS_NANOS.fetch_add(t_stats.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            None => {
                // The cell image is already appended; make sure recovery can
                // never resurrect it over the write that won the race.
                abandon_entry_version(pending_entry.addr);
                // The write is refused, but this exact path is how a crashed
                // index heals: a SIGKILL preserves acked cells in the WAL
                // while the ranged tree loses its un-flushed tail, and the
                // retrying writer's insert then finds the cell already
                // present. Re-assert the indices from the EXISTING image --
                // identical old/new pairs cancel for every index type except
                // ranged, whose insert is idempotent and re-established on
                // purpose (see ensure_indices_). This needs no allocation,
                // so it converges even in a chunk too full to accept writes.
                if self.index_builder.is_some() {
                    if let Some(mut cell_guard) =
                        CellGuard::for_write(cell.header.id.bits(), true, self)
                    {
                        if let Ok(existing) = cell_guard.read_cell_owned() {
                            if let Some(schema) = self.meta.schemas.get(&existing.header.schema)
                            {
                                let old_indices =
                                    Some(probe_cell_indices(&existing, &*schema));
                                drop(cell_guard);
                                self.ensure_indices_with_res(&existing, old_indices, &*schema);
                            }
                        }
                    }
                }
                return Err(WriteError::CellAlreadyExisted);
            }
        }
        WRITE_CELLS.fetch_add(1, Ordering::Relaxed);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        Ok(cell.header)
    }

    fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.id.bits();
        let write_plan = cell.plan_write(self)?;
        let pending_entry = write_plan.allocate(self, true)?;
        if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
            let cell_location = cell_guard.get_ptr();
            let cell_version =
                cell_version_from_chunk_raw(cell_location).map_err(|e| WriteError::ReadError(e))?;
            let write_result =
                self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell_version)?;
            let new_cell_loc = write_result.addr;
            #[cfg(debug_assertions)]
            {
                if cell_location != 0 {
                    self.assert_address_aligned_for_read(cell_location, "update_cell(old)", hash);
                }
                self.assert_address_aligned_for_write(new_cell_loc, "update_cell", hash);
            }

            let schema = &*write_plan.schema;
            let old_indices = cell_guard.old_index_res(schema)?;
            // Old entry length, read under the cell lock while the address is
            // still guaranteed live; used below to keep the slot counter exact.
            let old_content_length = if cell_location != 0 {
                Entry::decode_from(cell_location, |_, _| {}).0.content_length
            } else {
                0
            };
            cell_guard.set_ptr(new_cell_loc);
            drop(cell_guard);
            self.slot_bytes.add(hash, write_result.content_length);
            self.slot_bytes.sub(hash, old_content_length);
            self.ensure_indices_with_res(cell, old_indices, schema);
            self.mark_dead_entry_with_cell(cell_location, cell);
            self.refresh_statistics_for_schema(schema.id);
            drop(write_plan);
            cell.header.version = write_result.new_version;
            cell.header.timestamp = write_result.new_timestamp;
        } else {
            // Optimistic update will remove the new inserted one
            let write_result =
                self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell.header.version)?;
            let new_cell_loc = write_result.addr;
            // The image carries `old_version + 1`: recovery would pick it
            // over the tombstone of a deleted cell and resurrect the data.
            abandon_entry_version(new_cell_loc);
            self.mark_dead_entry_with_cell(new_cell_loc, cell);
            return Err(WriteError::CellDoesNotExisted);
        }
        Ok(cell.header)
    }

    /// Upsert a run of cells that all belong to this chunk, claiming ONE span
    /// of the append log for the whole run.
    ///
    /// This is the batched form of `upsert_cell`, and it exists because of how
    /// a migration batch maps onto storage: `locate_chunk_by_partition` keys
    /// the chunk off the id's LOCALITY, and a migration batch is one slot,
    /// which is one locality. So every cell of a batch lands in the same
    /// chunk and contends on the same append head -- 1024 CAS loops on one
    /// atomic, 1024 reference pairs, and 1024 WAL records that each take that
    /// segment's `file_state` lock across a write and an fsync.
    ///
    /// Claiming once collapses all three: one reference, one CAS, one WAL
    /// record per run. Nothing about the per-cell work changes -- each cell is
    /// still encoded, indexed and accounted individually -- because that part
    /// is not where the contention was.
    ///
    /// Every cell is planned BEFORE anything is claimed, so the only fallible
    /// step happens while a partial claim is still impossible; the span is
    /// then filled completely, which it must be, since the append cursor has
    /// already moved past it.
    pub fn upsert_run(&self, cells: &[OwnedCell]) -> Vec<Result<CellHeader, WriteError>> {
        let mut results: Vec<Result<CellHeader, WriteError>> = Vec::with_capacity(cells.len());
        if cells.is_empty() {
            return results;
        }

        // Plan first: a planning failure must not leave a claimed span with a
        // hole in it, and planning is the only step here that can fail. The
        // plans borrow their cells, which is why this path takes them by
        // reference and hands headers back rather than writing through.
        let mut plans = Vec::with_capacity(cells.len());
        for cell in cells.iter() {
            match cell.plan_write(self) {
                Ok(plan) => plans.push(plan),
                Err(_) => {
                    return cells
                        .iter()
                        .map(|cell| self.upsert_cell(&mut cell.clone()))
                        .collect();
                }
            }
        }
        let sizes: Vec<u32> = plans.iter().map(|p| p.total_size()).collect();
        let schema_id = plans[0].schema.id;

        let mut i = 0usize;
        while i < cells.len() {
            let t_alloc = std::time::Instant::now();
            let claimed = self.try_acquire_run(&sizes[i..], SegmentClass::Regular);
            WRITE_ALLOC_NANOS.fetch_add(t_alloc.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let claimed = match claimed {
                Ok(claimed) => claimed,
                Err(e) => {
                    while i < cells.len() {
                        results.push(Err(e.clone()));
                        i += 1;
                    }
                    break;
                }
            };
            let Some((run, covered)) = claimed else {
                // The head cannot take even one entry. The single-entry path
                // owns rotation, emergency GC and the capacity refusal, so let
                // it place this cell and rotate; the next claim finds a fresh
                // head. The clone is the price of that fallback and it is the
                // rare path -- once per segment, not once per cell.
                results.push(self.upsert_cell(&mut cells[i].clone()));
                i += 1;
                continue;
            };

            let mut addr = run.base;
            for j in i..(i + covered) {
                let hash = cells[j].header.id.bits();
                // Decide the version BEFORE encoding, because it is written
                // into the image. An existing cell continues its own version
                // line; a new one keeps what the caller set, which is how a
                // migration lands a cell on the version it had.
                let t_index = std::time::Instant::now();
                let existing = CellGuard::for_write(hash, true, self);
                WRITE_INDEX_NANOS.fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let (old_version, old_loc) = match &existing {
                    Some(guard) => {
                        let loc = guard.get_ptr();
                        (cell_version_from_chunk_raw(loc).unwrap_or(0), Some(loc))
                    }
                    None => (cells[j].header.version, None),
                };

                let entry_addr = addr;
                addr += sizes[j] as usize;
                let t_copy = std::time::Instant::now();
                let encoded = cells[j].write_to_addr(&plans[j], entry_addr, old_version);
                WRITE_COPY_NANOS.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
                // Journalled per entry, matching `PendingEntry`; see the note
                // on `PendingRun::journal` for why it is not coalesced.
                if run.journal(entry_addr, sizes[j]).is_err() {
                    results.push(Err(WriteError::CannotAllocateSpace));
                    continue;
                }
                let write_result = match encoded {
                    Ok(result) => result,
                    Err(e) => {
                        results.push(Err(e));
                        continue;
                    }
                };

                match existing {
                    Some(mut guard) => {
                        let old_indices = guard.old_index_res(&*plans[j].schema).ok().flatten();
                        guard.set_ptr(write_result.addr);
                        drop(guard);
                        self.slot_bytes.add(hash, write_result.content_length);
                        if let Some(loc) = old_loc.filter(|loc| *loc != 0) {
                            let old_len = Entry::decode_from(loc, |_, _| {}).0.content_length;
                            self.slot_bytes.sub(hash, old_len);
                        }
                        self.ensure_indices_with_res(&cells[j], old_indices, &*plans[j].schema);
                        if let Some(loc) = old_loc.filter(|loc| *loc != 0) {
                            self.mark_dead_entry_with_cell(loc, &cells[j]);
                        }
                    }
                    None => match self.cell_index.try_insert_locked(hash as usize) {
                        Some(mut guard) => {
                            *guard = write_result.addr;
                            drop(guard);
                            self.slot_bytes.add(hash, write_result.content_length);
                            self.ensure_indices(&cells[j], None, &*plans[j].schema);
                        }
                        None => {
                            // Someone inserted between the check and here. The
                            // bytes are already in the claimed span, which must
                            // stay filled, so the loser becomes dead space --
                            // the same outcome `write_cell` gives this race.
                            abandon_entry_version(write_result.addr);
                            self.mark_dead_entry_with_cell(write_result.addr, &cells[j]);
                            results.push(Err(WriteError::CellAlreadyExisted));
                            continue;
                        }
                    },
                }

                let mut header = cells[j].header;
                header.version = write_result.new_version;
                header.timestamp = write_result.new_timestamp;
                results.push(Ok(header));
                WRITE_CELLS.fetch_add(1, Ordering::Relaxed);
            }
            i += covered;
            // `run` drops here: ONE WAL record for the whole span, and the
            // segment reference released once.
        }
        self.refresh_statistics_for_schema(schema_id);
        results
    }

    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let hash = cell.header.id.bits();
        // Same phase timers as `write_cell`. A migration's recipient goes
        // through here for every cell it receives, and without these the
        // recipient's share of a reshard is one opaque number.
        let t_plan = std::time::Instant::now();
        let write_plan = cell.plan_write(self)?;
        WRITE_PLAN_NANOS.fetch_add(t_plan.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let t_alloc = std::time::Instant::now();
        let pending_entry = write_plan.allocate(self, true)?;
        WRITE_ALLOC_NANOS.fetch_add(t_alloc.elapsed().as_nanos() as u64, Ordering::Relaxed);
        loop {
            if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
                trace!("Cell {} exists, will update for upsert", hash);
                let cell_location = cell_guard.get_ptr();
                let cell_version = cell_version_from_chunk_raw(cell_location)
                    .map_err(|e| WriteError::ReadError(e))?;
                let write_result =
                    self.write_cell_to_chunk(cell, &write_plan, &pending_entry, cell_version)?;
                let new_cell_loc = write_result.addr;
                #[cfg(debug_assertions)]
                {
                    if cell_location != 0 {
                        self.assert_address_aligned_for_read(
                            cell_location,
                            "upsert_cell(update/old)",
                            hash,
                        );
                    }
                    self.assert_address_aligned_for_write(
                        new_cell_loc,
                        "upsert_cell(update)",
                        hash,
                    );
                }

                let old_indices = cell_guard.old_index_res(&*write_plan.schema)?;
                let old_content_length = if cell_location != 0 {
                    Entry::decode_from(cell_location, |_, _| {}).0.content_length
                } else {
                    0
                };
                cell_guard.set_ptr(new_cell_loc);
                drop(cell_guard);
                self.slot_bytes.add(hash, write_result.content_length);
                self.slot_bytes.sub(hash, old_content_length);
                self.ensure_indices_with_res(cell, old_indices, &*write_plan.schema);
                self.mark_dead_entry_with_cell(cell_location, cell);
                self.refresh_statistics_for_schema(write_plan.schema.id);
                drop(write_plan);
                cell.header.version = write_result.new_version;
                cell.header.timestamp = write_result.new_timestamp;
            } else {
                let t_index = std::time::Instant::now();
                let reservation = self.cell_index.try_insert_locked(hash as usize);
                WRITE_INDEX_NANOS.fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if let Some(mut guard) = reservation {
                    // New cell
                    trace!("Cell {} does not exists, will insert for upsert", hash);
                    let t_copy = std::time::Instant::now();
                    let write_result = self.write_cell_to_chunk(
                        cell,
                        &write_plan,
                        &pending_entry,
                        cell.header.version,
                    )?;
                    WRITE_COPY_NANOS.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    let new_cell_loc = write_result.addr;
                    #[cfg(debug_assertions)]
                    self.assert_address_aligned_for_write(
                        new_cell_loc,
                        "upsert_cell(insert)",
                        hash,
                    );

                    *guard = new_cell_loc;
                    drop(guard);
                    self.slot_bytes.add(hash, write_result.content_length);
                    let t_sec = std::time::Instant::now();
                    self.ensure_indices(cell, None, &*write_plan.schema);
                    WRITE_SECONDARY_NANOS
                        .fetch_add(t_sec.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    let t_stats = std::time::Instant::now();
                    self.refresh_statistics_for_schema(write_plan.schema.id);
                    WRITE_STATS_NANOS
                        .fetch_add(t_stats.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    WRITE_CELLS.fetch_add(1, Ordering::Relaxed);
                    drop(write_plan);
                    cell.header.version = write_result.new_version;
                    cell.header.timestamp = write_result.new_timestamp;
                } else {
                    trace!("Cell {} was not exists, but found exists, will try", hash);
                    continue;
                }
            }
            return Ok(cell.header);
        }
    }

    fn update_cell_by<U>(&self, hash: u64, update: U) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        if let Some(mut cell_guard) = CellGuard::for_write(hash, true, self) {
            let old_loc = cell_guard.get_ptr();
            match SharedCellData::from_chunk_raw(hash, *cell_guard, self) {
                Ok((cell, schema)) => {
                    let old_indices = self
                        .index_builder
                        .as_ref()
                        .map(|_| probe_cell_indices(&cell, &*schema));

                    // Get old entry size BEFORE releasing lock to avoid race condition
                    // where old_loc could be corrupted after we update cell_index
                    let old_entry_size = if old_loc != 0 {
                        match Entry::decode_from(old_loc, |_, _| {}) {
                            (entry, _) => Some(entry.content_length),
                        }
                    } else {
                        None
                    };

                    let new_cell = update(&cell);
                    if let Some(mut new_cell) = new_cell {
                        let write_plan = new_cell.plan_write(self)?;
                        let pending_entry = write_plan.allocate(self, false)?;
                        let write_result = self.write_cell_to_chunk(
                            &new_cell,
                            &write_plan,
                            &pending_entry,
                            cell.header.version,
                        )?;
                        let new_cell_loc = write_result.addr;

                        #[cfg(debug_assertions)]
                        self.assert_address_aligned_for_write(new_cell_loc, "update_cell_by", hash);

                        **cell_guard.word_mutex_guard() = new_cell_loc;
                        self.slot_bytes.add(hash, write_result.content_length);
                        self.slot_bytes.sub(hash, old_entry_size.unwrap_or(0));
                        if let Some(indexer) = &self.index_builder {
                            indexer.ensure_indices(&new_cell, &*schema, old_indices);
                        }

                        // Mark old entry as dead using size we captured earlier
                        // This avoids decoding old_loc after lock is released (race condition)
                        if let Some(size) = old_entry_size {
                            let seg = self.locate_segment_ensured(old_loc, &new_cell.id());
                            self.mark_dead_entry_with_size(old_loc, size, &seg);
                        }

                        self.refresh_statistics_for_schema(write_plan.schema.id);
                        drop(write_plan);
                        new_cell.header.version = write_result.new_version;
                        new_cell.header.timestamp = write_result.new_timestamp;
                        return Ok(new_cell);
                    } else {
                        return Err(WriteError::UserCanceledUpdate);
                    }
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        } else {
            return Err(WriteError::CellDoesNotExisted);
        }
    }

    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        // Use location_for_read to ensure promotion happens if segment is cold
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = guard.get_ptr();

        if let Some(indexer) = &self.index_builder {
            match SharedCell::from_chunk_raw(hash, guard, self) {
                Ok((cell, schema)) => {
                    indexer.remove_indices(&cell, &*schema);
                    cell.into_cell_guard().remove_cell();
                }
                Err(e) => return Err(WriteError::ReadError(e)),
            }
        } else {
            guard.remove_cell();
        }
        let removed_len = self.put_tombstone_by_cell_loc(cell_location)?;
        self.slot_bytes.sub(hash, removed_len);
        Ok(())
    }

    /// Remove a cell's body but leave its index entries in place.
    ///
    /// For a migration reclaiming a donor copy, and correct **only** there.
    ///
    /// A ranged index entry is keyed `[schema][field][feature][id]`, so there is
    /// one logical entry per id for the whole cluster -- it does not name the
    /// member holding the cell. A migration therefore inserts and deletes the same
    /// entry: the recipient's upsert calls `ensure_indices` and creates it, then
    /// the donor's ordinary `remove_cell` calls `remove_indices` and destroys it.
    /// Net effect, measured: every migrated cell vanished from the index -- a scan
    /// found 6 before a migration and 0 after, which in Morpheus surfaced as
    /// "enumerated no vertices ... the index could not be enumerated".
    ///
    /// Keeping the entries is not a leak: the cell still exists, on the recipient,
    /// and the entry still names it correctly. Removing them is what was wrong.
    fn remove_cell_keeping_indices(&self, hash: u64) -> Result<(), WriteError> {
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = guard.get_ptr();
        guard.remove_cell();
        let removed_len = self.put_tombstone_by_cell_loc(cell_location)?;
        self.slot_bytes.sub(hash, removed_len);
        Ok(())
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        // Use location_for_read to ensure promotion happens if segment is cold
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = *guard;

        match SharedCell::from_chunk_raw(hash, guard, self) {
            Ok((cell, schema)) => {
                if predict(&cell) {
                    self.remove_indices(&cell, &schema);
                    cell.into_cell_guard().remove_cell();
                    let removed_len = self.put_tombstone_by_cell_loc(cell_location)?;
                    self.slot_bytes.sub(hash, removed_len);
                    return Ok(());
                }
                Err(WriteError::CellDoesNotExisted)
            }
            Err(e) => Err(WriteError::ReadError(e)),
        }
    }

    #[inline(always)]
    pub fn put_segment(&self, segment: Segment) {
        // PROBE: no two live segments may overlap in address space. If the
        // allocator ever double-hands an address range, every write and every
        // cold fault-in for one segment lands inside the other -- which reads
        // as "requested id X, found valid id Y at the same offsets", the
        // 20-second reshard loss signature. Cheap relative to rotation.
        {
            let base = segment.addr;
            let bound = segment.addr + SEGMENT_SIZE;
            for existing in self.segs.iter_front_values() {
                let eb = existing.addr;
                let ee = existing.addr + SEGMENT_SIZE;
                if base < ee && eb < bound {
                    error!(
                        "SEGMENT ADDRESS OVERLAP in chunk {}: publishing segment {} (seq {}) \
                         at {:#x} overlapping live segment {} (seq {}) at {:#x}",
                        self.id, segment.id, segment.seq_id, base, existing.id, existing.seq_id, eb
                    );
                }
            }
        }
        debug!(
            "Putting segment for chunk {} with id {}",
            self.id, segment.id
        );
        let segment_id = segment.id;
        let segment_key = segment_id as usize;
        let is_hot = segment.is_hot();

        // Update cached hot count BEFORE adding to list to avoid race with full scan
        // If we increment after adding, a full scan could count the new segment
        // and update the cache, then we'd increment again, leading to over-counting
        if is_hot {
            if let Some(ref tiered_manager) = self.tiered_manager {
                tiered_manager.increment_hot_count_for(self);
            }
        }

        self.segs.insert_back(segment_key, AArc::new(segment));
    }

    pub fn remove_segment(&self, segment_id: u64) {
        debug!(
            "Removing segment for chunk {} with id {}",
            self.id, segment_id
        );

        // Check if segment is hot BEFORE removing to avoid race with full scan
        // If we decrement after removing, a full scan could miss the removed segment
        // and update the cache, then we'd decrement again, leading to under-counting
        let should_decrement = if let Some(seg) = self.segs.get(&(segment_id as usize)) {
            let is_hot = seg.is_hot();
            if !is_hot {
                error!(
                    "Segment {} is not hot in chunk {} to remove",
                    segment_id, self.id
                );
            }
            is_hot
        } else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
            false
        };

        // Decrement cache BEFORE removing from list
        if should_decrement {
            if let Some(ref tiered_manager) = self.tiered_manager {
                tiered_manager.decrement_hot_count_for(self);
            }
        }

        // Unpublish only. Freeing here would be a use-after-free: readers
        // already inside hold pointers into this memory, and `segs.remove`
        // only stops the ones that have not looked it up yet. The segment is
        // stamped and reclaimed once every thread has been quiescent since.
        if let Some(seg) = self.segs.remove(&(segment_id as usize)) {
            if let Some(ref tiered) = self.tiered_manager {
                tiered.release_cold_resident(seg.take_block_resident_bytes());
            }
            // The stamp must be taken AFTER the removal, so that a thread
            // whose section began at or after it provably could not have
            // found this segment.
            let stamp = crate::ram::qsbr::segment_qsbr().retire_stamp();
            self.retired_segments.lock().push(RetiredSegment {
                segment: seg,
                stamp,
                attempts: 0,
            });
        } else {
            error!(
                "Segment {} not found in chunk {} to remove",
                segment_id, self.id
            );
        }
    }

    /// Reclaim retired segments whose readers have drained.
    ///
    /// Returns how many were freed. Called from the cleaner's round, so a
    /// retirement costs at most one round of delay in the common case.
    pub fn drain_retired_segments(&self) -> usize {
        // Nothing is freed while the lock is held longer than the swap: the
        // list is short, but reclamation touches the allocator and the file
        // system, and the cleaner is not the only writer here.
        let pending = {
            let mut retired = self.retired_segments.lock();
            if retired.is_empty() {
                return 0;
            }
            std::mem::take(&mut *retired)
        };

        let qsbr = crate::ram::qsbr::segment_qsbr();
        let mut freed = 0usize;
        let mut still_pending = Vec::new();
        for mut entry in pending {
            // Two independent conditions, both cheap here. QSBR covers the
            // window between resolving a raw address from the cell index and
            // taking a reference on its segment, which a per-segment count
            // cannot see; the count covers a guard that outlived the thread
            // that took it, which QSBR's per-thread accounting cannot see.
            let quiesced = qsbr.is_quiesced(entry.stamp);
            let unreferenced = entry.segment.no_references();
            if quiesced && unreferenced {
                entry.segment.free_memory();
                entry.segment.dispense();
                entry.segment.mem_drop(self);
                freed += 1;
                continue;
            }

            entry.attempts += 1;
            // A retirement that never drains is a bug worth seeing rather
            // than a slow leak worth ignoring.
            if entry.attempts % RETIRED_SEGMENT_REPORT_INTERVAL == 0 {
                warn!(
                    "Segment {} (chunk {}) has waited {} drain passes to be reclaimed: \
                     quiesced={} references={} blocking_threads={}. Its memory is held \
                     until readers leave.",
                    entry.segment.id,
                    self.id,
                    entry.attempts,
                    quiesced,
                    entry.segment.references_count(),
                    qsbr.blocking_threads(entry.stamp),
                );
            }
            still_pending.push(entry);
        }

        if !still_pending.is_empty() {
            self.retired_segments.lock().extend(still_pending);
        }
        freed
    }

    /// Segments unpublished but not yet reclaimed, for tests and diagnostics.
    pub fn retired_segment_count(&self) -> usize {
        self.retired_segments.lock().len()
    }

    pub fn locate_segment(&self, addr: usize) -> Option<AArc<Segment>> {
        let seg_id = self.allocator.id_by_addr(addr);
        let res = self.segs.get(&seg_id);
        if res.is_none() {
            // Segment doesn't exist - this can happen when the cleaner combines segments
            // and removes old ones. The address in cell_index may be stale.
            // Callers should handle this by re-reading from cell_index or retrying.
            debug!(
                "Cannot locate segment for {:?}, got id {}, chunk segs {:?} (segment may have been removed by cleaner)",
                addr,
                seg_id,
                self.segs.iter_front_keys().collect::<Vec<_>>()
            );
        }
        return res;
    }

    #[inline]
    fn put_tombstone(
        &self,
        cell_header: &CellHeader,
        cell_seg: &AArc<Segment>,
    ) -> Result<(), WriteError> {
        // Bounded, and never retried when retrying cannot possibly work.
        //
        // This used to loop forever on ANY acquire error. Two ways that
        // wedges: a delete arriving during shutdown retries
        // `ServerShuttingDown` -- an answer that will never change -- and
        // holds the shutdown open for as long as the caller lives; and a
        // genuinely full chunk spins at full speed printing the same line,
        // instead of telling the caller its delete did not happen. The
        // crash-churn fuzzer found the first one within three cycles: a
        // graceful shutdown that never completed, with this message
        // repeating thousands of times.
        //
        // `try_acquire` already runs emergency GC internally, so a handful
        // of attempts is all that can help; past that the honest answer is
        // the error.
        const TOMBSTONE_ACQUIRE_ATTEMPTS: usize = 16;
        let backoff = Backoff::new();
        let mut acquired = None;
        let mut last_error = WriteError::CannotAllocateSpace;
        for attempt in 0..TOMBSTONE_ACQUIRE_ATTEMPTS {
            match self.try_acquire(TOMBSTONE_ENTRY_SIZE as u32, true) {
                Ok(pending_entry) => {
                    acquired = Some(pending_entry);
                    break;
                }
                Err(WriteError::ServerShuttingDown) => {
                    return Err(WriteError::ServerShuttingDown);
                }
                Err(error) => {
                    last_error = error;
                    if attempt + 1 < TOMBSTONE_ACQUIRE_ATTEMPTS {
                        warn!(
                            "Chunk {} is too full to put a tombstone (attempt {}/{}). Retrying.",
                            self.id,
                            attempt + 1,
                            TOMBSTONE_ACQUIRE_ATTEMPTS
                        );
                        backoff.spin();
                    }
                }
            }
        }
        let Some(pending_entry) = acquired else {
            error!(
                "Chunk {} could not place a tombstone for {:?} after {} attempts: {:?}. The \
                 delete is REFUSED rather than retried forever, so the caller learns it did \
                 not happen.",
                self.id, cell_header.id, TOMBSTONE_ACQUIRE_ATTEMPTS, last_error
            );
            return Err(last_error);
        };
        Tombstone::put(
            pending_entry.addr,
            cell_seg.seq_id,
            cell_header.version,
            cell_header.id,
        );
        pending_entry.seg.tombstones.fetch_add(1, Ordering::Relaxed);
        pending_entry.seg.note_dead_bytes_change();
        Ok(())
    }

    /// Returns the removed entry's content length, so the logical remove
    /// paths can settle the slot counter without a second decode.
    pub fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<u32, WriteError> {
        debug!(
            "Put tombstone for chunk {} for cell {}",
            self.id, cell_location
        );
        let header = header_from_chunk_raw(cell_location)
            .map_err(|e| WriteError::ReadError(e))?
            .0;

        // Get entry size while we know the memory is still valid
        let entry_size = {
            let (entry, _) = Entry::decode_from(cell_location, |_, _| {});
            entry.content_length
        };

        let cell_seg = self.locate_segment_ensured(cell_location, &header.id());
        self.put_tombstone(&header, &cell_seg)?;
        self.mark_dead_entry_with_size(cell_location, entry_size, &cell_seg);
        Ok(entry_size)
    }

    fn locate_segment_ensured(&self, cell_location: usize, cell_id: &Id) -> AArc<Segment> {
        self.locate_segment(cell_location).expect(
            format!(
                "Cannot locate cell segment for cell id: {:?} at {}",
                cell_id, cell_location
            )
            .as_str(),
        )
    }

    // Mark entry as dead with explicit size (safer - doesn't need to decode)
    #[inline]
    pub fn mark_dead_entry_with_size(&self, addr: usize, size: u32, seg: &Segment) {
        trace!(
            "Marking {} bytes as dead at addr 0x{:016x} in segment {}",
            size,
            addr,
            seg.id
        );
        seg.dead_space.fetch_add(size, Ordering::Relaxed);
        seg.mark_dead_bit(addr);
        seg.note_dead_bytes_change();
    }

    // Decodes entry to get size and marks it dead
    // WARNING: Will panic if memory at addr is corrupted!
    // Prefer mark_dead_entry_with_size when size is known
    #[inline]
    pub fn mark_dead_entry_with_seg(&self, addr: usize, seg: &Segment) {
        #[cfg(debug_assertions)]
        {
            if addr % 8 != 0 {
                panic!(
                    "CORRUPTION: mark_dead_entry_with_seg received misaligned addr=0x{:016x} (offset: {}) for segment {}. \
                    This address should have been validated earlier.",
                    addr, addr % 8, seg.id
                );
            }
        }

        // Decode entry to get its content_length
        // This will PANIC if memory is corrupted - which is intentional!
        // We want to know about memory corruption issues immediately
        let (entry, _) = Entry::decode_from(addr, |_, _| {});
        self.mark_dead_entry_with_size(addr, entry.content_length, seg);
    }

    pub fn mark_dead_entry_with_cell<C: Cell>(&self, addr: usize, cell: &C) {
        let seg = self.locate_segment_ensured(addr, &cell.id());
        self.mark_dead_entry_with_seg(addr, &seg)
    }

    pub fn contains_seg(&self, seg_id: u64) -> bool {
        self.segs.contains_key(&(seg_id as usize))
    }

    pub fn segment_ids(&self) -> Vec<usize> {
        self.segs.iter_front_keys().collect()
    }

    pub fn segments(&self) -> Vec<AArc<Segment>> {
        self.segs.iter_front_values().collect()
    }

    pub fn segs_for_combine_cleaner(&self) -> Vec<(AArc<Segment>, f32)> {
        self.segs_for_combine_cleaner_impl(false)
    }

    pub fn segs_for_combine_cleaner_full(&self) -> Vec<(AArc<Segment>, f32)> {
        self.segs_for_combine_cleaner_impl(true)
    }

    fn choose_combine_candidate_class(mapping: &[(AArc<Segment>, f32)]) -> Option<SegmentClass> {
        let preferred_class = mapping.first().map(|(seg, _)| seg.segment_class())?;
        let mut blob_count = 0;
        let mut regular_count = 0;

        for (seg, _) in mapping {
            match seg.segment_class() {
                SegmentClass::Blob => blob_count += 1,
                SegmentClass::Regular => regular_count += 1,
            }
        }

        let preferred_count = match preferred_class {
            SegmentClass::Blob => blob_count,
            SegmentClass::Regular => regular_count,
        };

        if preferred_count >= 2 {
            return Some(preferred_class);
        }

        if blob_count >= 2 {
            return Some(SegmentClass::Blob);
        }

        if regular_count >= 2 {
            return Some(SegmentClass::Regular);
        }

        None
    }

    fn segs_for_combine_cleaner_impl(&self, full: bool) -> Vec<(AArc<Segment>, f32)> {
        let mut mapping: Vec<_> = self
            .segments()
            .into_iter()
            .map(|seg| {
                let living = seg.living_space() as f32;
                let segment_utilization = living / SEGMENT_SIZE_U32 as f32;
                (seg, segment_utilization)
            })
            .filter(|(seg, utilization)| {
                // Always require some dead space (utilization < 100%)
                // For full GC, accept any segment with dead space
                // For partial GC, only consider high-dead segments. The bar
                // adapts to space pressure: with plenty of room, wait until a
                // segment is three-quarters dead — under churn it will get
                // there on its own, and combining it later halves the live
                // cells relocated (and the foreground conflicts relocation
                // causes). Under pressure, fall back to the eager bar.
                let fill_x8 = (self.segs.len() * SEGMENT_SIZE).saturating_mul(8)
                    / self.capacity.max(1);
                let dead_bar = if fill_x8 >= 6 {
                    DEAD_RATE_FOR_COMBINE_CLEANER
                } else {
                    DEAD_RATE_FOR_COMBINE_CLEANER_RELAXED
                };
                *utilization < 1.0
                    && (full || *utilization < dead_bar)
                    && !self.is_active_head(seg.id)
                    && seg.no_references() // Includes transaction protection via SegmentReferenceGuards
                    // A reservation in flight means the image may hold a gap:
                    // the cursor moves before the bytes land, and the combine
                    // walks segment memory FORWARD, so a gap makes it
                    // under-enumerate and then free a source that still held
                    // live entries past the zeros. The claim strictly
                    // brackets that window (taken before the cursor moves,
                    // released after fill+journal), and no new claims land on
                    // a rotated segment, so a zero reading here is stable --
                    // this is an invariant, not a heuristic.
                    && seg.pending_journal_count() == 0
                    && seg.is_hot() // Don't clean cold segments (tiered memory)
                    && !seg.cleaned_without_progress()
            })
            .collect();
        mapping.sort_by(|(_, util1), (_, util2)| util1.partial_cmp(util2).unwrap());

        let Some(preferred_class) = Self::choose_combine_candidate_class(&mapping) else {
            return Vec::new();
        };

        mapping.retain(|(seg, _)| seg.segment_class() == preferred_class);

        let max_segments = if full {
            mapping.len()
        } else {
            MAX_SEGMENTS_FOR_CLEANER
        };
        mapping.truncate(max_segments);
        return mapping;
    }

    /// Get segment information for a cell based on its memory address.
    /// Returns (segment_id, seq_id) for the segment containing the cell.
    pub fn get_cell_segment_info(&self, cell_addr: usize) -> (u64, u64) {
        let segment_id = self.allocator.id_by_addr(cell_addr) as u64;
        if let Some(segment) = self.segs.get(&(segment_id as usize)) {
            (segment.id, segment.seq_id)
        } else {
            unreachable!("Cannot find segment for cell at address {}", cell_addr)
        }
    }

    pub fn live_entries<'a>(&'a self, seg: &'a Segment) -> impl Iterator<Item = Entry> + 'a {
        seg.entry_iter()
            .filter_map(move |entry_meta| {
                let chunk_id = &self.id;
                let chunk_index = &self.cell_index;
                let chunk_segs = &self.segs;
                let entry_size = entry_meta.entry_size;
                let entry_header = entry_meta.entry_header;
                trace!("Iterating live entries on chunk {} segment {}. Got {:?} at {} size {}",
                       chunk_id, seg.id, entry_header.entry_type, entry_meta.entry_pos, entry_size);
                // Validate entry type is a known valid type (CELL or TOMBSTONE)
                // Invalid types indicate we're reading garbage (possibly from inside another entry)
                // which can happen if append_header was set incorrectly by a previous operation
                debug_assert!(entry_header.entry_type == EntryType::CELL || entry_header.entry_type == EntryType::TOMBSTONE);

                // Validate that entry size is reasonable (must be at least header size and 8-byte aligned)
                // Real entries are always 8-byte aligned; non-aligned sizes indicate corruption
                debug_assert!(entry_meta.entry_size % 8 == 0);
                debug_assert!(entry_meta.entry_size >= ENTRY_HEAD_SIZE);
                if entry_header.entry_type == EntryType::CELL {
                        // Entries marked dead at mark_dead time need no header
                        // read or index probe; this keeps collection cost
                        // proportional to live entries instead of the whole
                        // backlog.
                        if seg.is_dead_at(entry_meta.entry_pos) {
                            return None;
                        }
                        trace!("Entry at {} is a cell", entry_meta.entry_pos);
                        let cell_header =
                            cell_header_from_entry_content_addr(entry_meta.body_pos);
                        trace!("Cell header read, id is {:?}", cell_header.id());
                        let expect = Some(entry_meta.entry_pos);
                        let actual = chunk_index.get_from_mutex(&(cell_header.id.bits() as usize));
                        if expect == actual {
                            trace!(
                                "Cell entry {:?} is valid", cell_header.id()
                            );
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Cell(cell_header)
                            });
                        } else {
                            trace!(
                                "Cell entry index mismatch for {:?}. Expect {:?}, actual {:?}, will be ditched", 
                                cell_header.id(), expect, actual
                            );
                        }
                } else if entry_header.entry_type == EntryType::TOMBSTONE {
                        trace!("Entry at {} is a tombstone", entry_meta.entry_pos);
                        let tombstone =
                            Tombstone::read_from_entry_content_addr(entry_meta.body_pos);
                        let contains_seg = chunk_segs.contains_seq_id(tombstone.segment_seq_id);
                        if contains_seg {
                            trace!("Tomestone entry {:?} at seq_id {} is valid",
                                   tombstone.id.bits(), tombstone.segment_seq_id);
                            return Some(Entry {
                                meta: entry_meta,
                                content: EntryContent::Tombstone(tombstone)
                            });
                        } else {
                            trace!("Tombstone target at seq_id {} have been removed, will be ditched", tombstone.segment_seq_id)
                        }
                } else if entry_header.entry_type == EntryType::PADDING {
                    // Space that holds no data: recovery stamps it over the
                    // gap an abandoned reservation left, and the writer
                    // stamps it over a span it has claimed but not yet
                    // filled. Never live, always skipped -- but it MUST be
                    // walked past rather than treated as impossible, or the
                    // cleaner panics the first time it meets a recovered
                    // segment.
                    trace!("Entry at {} is padding; skipping", entry_meta.entry_pos);
                } else {
                    unreachable!(
                        "Unexpected cell type on getting live entries at {}: type {:?}, size {}, append header {}, ends at {}",
                        entry_meta.entry_pos,
                        entry_header.entry_type.bits(),
                        entry_size,
                        seg.append_header.load(Ordering::Relaxed),
                        entry_meta.entry_pos + entry_size
                    )
                }
                return None
            })
    }

    pub fn cell_count(&self) -> usize {
        self.cell_index.len()
    }

    pub fn seg_count(&self) -> usize {
        self.segs.len()
    }

    pub fn count(&self) -> usize {
        self.cell_index.len()
    }

    /// Ids of the live cells this chunk holds whose slot is in `slots`.
    ///
    /// The enumeration primitive migration is built on. `cell_index` is keyed by
    /// `id.bits()` (see the insert in `apply_cell`), so a cell's slot is bits
    /// 62..48 of the key and this needs to read no cell bodies at all — which
    /// is what makes it cheap enough to run over a whole chunk, and what makes
    /// it work whether or not indexing is enabled.
    ///
    /// Takes a set rather than one slot so a caller planning many moves makes
    /// one pass instead of one pass per slot.
    pub fn cell_ids_in_slots(&self, slots: &std::collections::HashSet<u16>) -> Vec<Id> {
        self.cell_index
            .entries()
            .into_iter()
            .filter_map(|(key, address)| {
                // A zero address is a reserved-but-unwritten slot in the index,
                // not a live cell; migrating it would move nothing and the
                // recipient would answer for a cell that does not exist.
                if address == 0 {
                    return None;
                }
                let id = Id::from_bits(key as u64);
                slots.contains(&id.locality()).then_some(id)
            })
            .collect()
    }

    #[inline]
    fn refresh_statistics(&self) {
        self.statistics.refresh_from_chunk(self)
    }

    pub fn lock_cell_for_read(&self, hash: u64) -> Result<CellGuard<'_>, ReadError> {
        CellGuard::for_read(hash, self)
    }

    pub fn lock_cell_for_write(
        &self,
        hash: u64,
        has_read: bool,
    ) -> Result<CellGuard<'_>, ReadError> {
        CellGuard::for_write(hash, has_read, self).ok_or(ReadError::CellDoesNotExisted)
    }

    pub fn compare_version_and_update_cell(
        &self,
        hash: u64,
        version: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let cell_version = guard.cell_version().map_err(WriteError::ReadError)?;
        if cell_version == version {
            cell.header.version = version; // update version to the latest version
            guard.update_cell(cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellVersionMismatch);
    }

    pub fn compare_version_and_set_field(
        &self,
        hash: u64,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        let mut cell = guard.read_cell_owned().map_err(WriteError::ReadError)?;
        if cell.header.version == version {
            cell.data[field] = value;
            guard.update_cell(&mut cell)?;
            return Ok(cell.header);
        }
        return Err(WriteError::CellVersionMismatch);
    }
}

/// A contiguous span of a segment claimed for a run of entries.
///
/// The batched form of [`PendingEntry`]. One reference, one CAS and one WAL
/// record for a whole run, instead of one of each per cell -- which is the
/// difference between 1024 of each and 3 for a default migration batch, all
/// of them on the SAME segment because a batch shares a locality and locality
/// picks the chunk.
///
/// The caller must fill the entire span: the append cursor has already moved
/// past it, so anything left unwritten is garbage that recovery would try to
/// parse. `receive_migrated_cells` satisfies this by planning every cell
/// before claiming anything, so the only fallible step happens first.
pub struct PendingRun {
    pub seg: AArc<Segment>,
    pub base: usize,
    pub size: u32,
    skip_sync: bool,
    /// High-water mark of journaled bytes within the run, so Drop knows how
    /// much of the reservation was actually used. Whatever was reserved but
    /// never journaled is re-stamped as padding on the way out: an unwind
    /// mid-run must not leave bytes that walk like entries but exist in no
    /// log, nor zeros that stop a walk dead.
    consumed: std::cell::Cell<u32>,
}

impl PendingRun {
    /// Journal one entry of the run.
    ///
    /// Deliberately NOT coalesced into a single record for the whole span,
    /// though the entries are contiguous and it would halve the write count.
    /// Measured: coalescing took WAL writes from 2.0 to 1.0 per cell and made
    /// the reshard 3.9x SLOWER (1129 -> 290 MB/s), because `write_wal` holds
    /// the segment's `file_state` across the write AND the fsync -- so one
    /// 4 MB record blocks every other writer on that segment for its whole
    /// duration. Lock hold time went from 10.6s to 121.3s. Fewer, larger
    /// critical sections only win when the lock is not shared.
    fn journal(&self, addr: usize, size: u32) -> io::Result<()> {
        let result = self.seg.write_wal(addr, size, self.skip_sync);
        if result.is_ok() {
            let end = (addr - self.base) as u32 + size;
            self.consumed.set(self.consumed.get().max(end));
        }
        result
    }
}

impl Drop for PendingRun {
    fn drop(&mut self) {
        // Repair the reservation before releasing the claim. The claim is
        // what stops the archive and the cleaner consuming this segment
        // while the run is open; the moment it drops, the image must stand
        // on its own. Everything past the journaled high-water mark is
        // therefore stamped back to padding: on a clean run that is zero
        // bytes, and on an unwind it covers both the never-written tail and
        // any entry that was encoded but failed to journal -- bytes that
        // exist in no log must not walk like data.
        if self.consumed.get() < self.size {
            let from = self.base + self.consumed.get() as usize;
            crate::ram::entry::stamp_reservation_padding(
                from,
                self.size - self.consumed.get(),
            );
        }
        self.seg.end_pending_journal();
        // Same ordering as PendingEntry::drop: the run's journal writes all
        // happened inside the ownership window (run.journal is called by the
        // writer loop before this drop), so releasing here keeps the WAL
        // offset-ordered.
        self.seg.release_own();
        if !self.skip_sync {
            crate::ram::segs::queue_wal_sync(&self.seg);
        } else {
            note_transactional_write(&self.seg);
        }
        self.seg.set_dirty();
        self.seg.decr_references();
    }
}

pub struct PendingEntry {
    pub seg: AArc<Segment>,
    pub addr: usize,
    pub size: u32,
    pub skip_sync: bool, // Skip fsync if part of a transaction (will be synced at commit)
}

impl Drop for PendingEntry {
    // dealing with entry write ahead log
    fn drop(&mut self) {
        let journal_result = self.seg.write_wal(self.addr, self.size, self.skip_sync);
        // Release the journal slot as soon as the attempt is over, whatever
        // its outcome: a seal waiting on this segment must not be held up by
        // a writer that has already finished trying.
        self.seg.end_pending_journal();
        // Ownership ends HERE, after the journal write -- that ordering IS
        // the invariant. Released earlier, the next owner could append and
        // journal before this record reached the log, and the segment's WAL
        // would no longer be offset-ordered: a crash would then leave a gap
        // the "torn tail = end of segment" rule mistakes for the end,
        // silently dropping every later owner's durable entries.
        self.seg.release_own();
        if let Err(error) = journal_result {
            // A straggler whose segment was sealed (archived) between its
            // append and this journal write cannot journal -- the recorded
            // seal-vs-pending-writer race. The entry's durability is already
            // decided at this point (the archive that sealed the segment
            // either carried the bytes or did not), and this Drop runs on
            // WHATEVER thread holds the last handle: unwrap here took down
            // the b-tree write-back workers, which silently ended index
            // durability for the whole process -- every insert after the
            // panic lived only in memory, and the next restart lost them
            // all. Scream, count, survive.
            crate::ram::chunk::WAL_JOURNAL_FAILURES.fetch_add(1, Ordering::Relaxed);
            error!(
                "WAL journal failed for segment {} (chunk of addr {:#x}, size {}): {error}; \
                 the entry rides the segment's archive if one carried it, and the caller's \
                 next write-back supersedes it -- but this indicates the seal/pending-writer \
                 race and must stay loud",
                self.seg.id, self.addr, self.size
            );
            // Give the reference back even here. Returning early used to skip
            // it, so every straggler pinned its segment against eviction and
            // reclamation permanently -- a leak that grew with exactly the
            // race this counter exists to close.
            self.seg.decr_references();
            return;
        }
        if !self.skip_sync {
            // Timer-based group commit rides the WAL syncer thread now;
            // transactional entries keep syncing at commit instead.
            crate::ram::segs::queue_wal_sync(&self.seg);
        } else {
            note_transactional_write(&self.seg);
        }
        self.seg.set_dirty();
        self.seg.decr_references();
    }
}

pub struct Chunks {
    pub list: Vec<Chunk>,
    /// Live bytes per slot across the whole store; the same instance every
    /// chunk holds, so this is already the server-wide answer.
    pub slot_bytes: Arc<crate::slots::SlotLiveBytes>,
    pub statistics: TTLCache<Arc<SchemaStatistics>>,
    pub tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    /// Shared wake signal registered by the background cleaner thread.
    pub cleaner_wake: Arc<crate::ram::cleaner::CleanerWake>,
    /// Max allocated-class sequence observed per origin during recovery.
    /// The id allocator's belt-and-suspenders floor: even if the durable
    /// lease record was lost, no recovered cell's sequence is ever reissued.
    pub recovered_origin_floors: Arc<Vec<std::sync::atomic::AtomicU64>>,
    /// Base of this instance's contiguous chunk mapping.
    pub base_addr: usize,
    /// log2 of the per-chunk size within the mapping.
    pub chunk_size_bits: usize,
    /// Total mapped bytes across all chunks of this instance.
    pub total_size: usize,
    /// Set when the owning server goes away, so the background sweeper exits
    /// without waiting for the store itself to be dropped. The store can
    /// outlive its server -- an RPC service still holds it -- and a thread that
    /// only notices a dropped store would then never exit at all.
    background_stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl Chunks {
    /// Stop this store's background threads.
    ///
    /// Called when the owning server goes away. Separate from dropping the
    /// store because the two are not the same event: an RPC service registered
    /// on the server holds the store, so it can outlive the server that made
    /// it, and a sweeper waiting for the store to drop would then wait forever.
    pub fn stop_background(&self) {
        self.background_stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// See [`stop_background`]. Observable so a test can assert the teardown
    /// happened without a process-wide thread census.
    pub fn is_background_stopped(&self) -> bool {
        self.background_stopped
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Decodes an address inside this instance's mapping into
    /// `(chunk_id, segment_id)`. Deterministic per instance, unlike the
    /// module-level last-writer-wins diagnostic helper.
    pub fn chunk_and_segment_from_addr(&self, addr: usize) -> Option<(usize, usize)> {
        decode_chunk_addr(addr, self.base_addr, self.total_size, self.chunk_size_bits)
    }

    /// Segment lookup by decoded ids on this instance.
    pub fn segment_by_ids(
        &self,
        chunk_id: usize,
        segment_id: usize,
    ) -> Option<AArc<crate::ram::segs::Segment>> {
        self.list
            .get(chunk_id)
            .and_then(|chunk| chunk.segs.get(&segment_id))
    }

    /// Max sequence observed during recovery for `origin` (0 if none).
    pub fn recovered_origin_floor(&self, origin: u16) -> u64 {
        self.recovered_origin_floors
            .get(origin as usize)
            .map(|floor| floor.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Records an allocated-class id observed during recovery.
    pub fn note_recovered_id(&self, id: &Id) {
        if !id.is_hashed() {
            if let Some(floor) = self.recovered_origin_floors.get(id.origin() as usize) {
                floor.fetch_max(id.sequence(), std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    pub fn new(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
    ) -> Arc<Chunks> {
        Self::new_with_recovery(
            count,
            size,
            meta,
            index_builder,
            backup_storage,
            wal_storage,
            tiered_manager,
            false,
            None,
        )
    }

    pub fn new_with_recovery(
        count: usize,
        size: usize,
        meta: Arc<ServerMeta>,
        index_builder: Option<Arc<IndexBuilder>>,
        backup_storage: Option<String>,
        wal_storage: Option<String>,
        tiered_manager: Option<Arc<crate::ram::tiered::manager::TieredMemoryManager>>,
        enable_recovery: bool,
        raft_storage: Option<String>,
    ) -> Arc<Chunks> {
        use libc::{MAP_ANONYMOUS, MAP_PRIVATE, PROT_READ, PROT_WRITE};
        use std::ptr;

        // Calculate exact chunk size
        let chunk_size = size.next_power_of_two();
        let chunk_size_bits = chunk_size.trailing_zeros() as usize;

        // Allocate one giant mmap for all chunks
        let total_size = chunk_size * count;
        let global_base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0,
            )
        };

        if global_base == libc::MAP_FAILED {
            let errno = std::io::Error::last_os_error();
            panic!(
                "Failed to allocate {} bytes for {} chunks (chunk_size: {} bytes). \
                Error: {} (errno: {}). \
                This could be due to: insufficient memory, system limits (ulimit -v), \
                or memory fragmentation. Try reducing total_size or chunk_count.",
                total_size,
                count,
                chunk_size,
                errno,
                errno.raw_os_error().unwrap_or(-1)
            );
        }

        let global_base_addr = global_base as usize;

        // Store global state
        GLOBAL_CHUNK_BASE.store(global_base_addr, Ordering::Release);
        GLOBAL_CHUNK_SIZE_BITS.store(chunk_size_bits, Ordering::Release);
        GLOBAL_CHUNK_COUNT.store(count, Ordering::Release);
        GLOBAL_ALLOCATED_SIZE.store(total_size, Ordering::Release);

        info!(
            "Allocated global chunk space: base={:#x}, chunk_size={} (2^{}), count={}, total={}",
            global_base_addr, chunk_size, chunk_size_bits, count, total_size
        );

        if let Some(ref manager) = tiered_manager {
            info!(
                "Tiered memory enabled: threshold={}, limit={} MB, shared across all {} chunks",
                manager.shared_pool().threshold,
                manager.shared_pool().physical_memory_limit / (1024 * 1024),
                count,
            );
        }

        let mut chunks = Vec::new();
        assert!(size >= SEGMENT_SIZE);
        debug!(
            "Creating chunks, count {} , chunk_size {} bytes",
            count, size
        );

        // Discover before building the chunks: a chunk's cell index is sized at
        // construction and cannot grow afterwards, so recovery has to know its
        // own footprint first. Empty on a fresh store, where the per-chunk
        // directories do not exist yet and the estimate falls back to zero.
        let recovery_files = if enable_recovery {
            crate::ram::recovery::discover_segment_files(&backup_storage, &wal_storage)
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let estimated_cells = crate::ram::recovery::estimate_cells_per_chunk(&recovery_files, count);
        if !recovery_files.is_empty() {
            info!(
                "Discovered {} segment files; sizing cell indexes for ~{} cells",
                recovery_files.len(),
                estimated_cells.iter().sum::<usize>()
            );
        }

        let cleaner_wake = Arc::new(crate::ram::cleaner::CleanerWake::new());
        let slot_bytes = Arc::new(crate::slots::SlotLiveBytes::new());
        let seal_queue = Arc::new(SealQueue::new());
        for i in 0..count {
            let chunk_base = global_base_addr + (i * chunk_size);
            let backup_storage = backup_storage
                .clone()
                .map(|dir| format!("{}/chunk-bk-{}", dir, i));
            let wal_storage = wal_storage
                .clone()
                .map(|dir| format!("{}/chunk-wal-{}", dir, i));
            chunks.push(Chunk::new_with_base(
                i,
                chunk_base,
                chunk_size,
                meta.clone(),
                index_builder.clone(),
                backup_storage,
                wal_storage,
                tiered_manager.clone(),
                cleaner_wake.clone(),
                estimated_cells[i],
                slot_bytes.clone(),
                seal_queue.clone(),
            ));
        }
        let num_schemas = meta.schemas.count() + 1;
        let chunks_arc = Arc::new(Chunks {
            list: chunks,
            slot_bytes,
            statistics: TTLCache::with_capacity(num_schemas.next_power_of_two()),
            tiered_manager,
            cleaner_wake,
            recovered_origin_floors: Arc::new(
                (0..4096)
                    .map(|_| std::sync::atomic::AtomicU64::new(0))
                    .collect(),
            ),
            base_addr: global_base_addr,
            chunk_size_bits,
            total_size,
            background_stopped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });

        if let Some(ref manager) = chunks_arc.tiered_manager {
            manager.register_chunks(&chunks_arc);
        }

        // Sealer: drains the post-rotation archive queue. Same lifetime rules
        // as the stats sweeper below -- weak, so the thread never keeps the
        // store alive, and it exits on stop_background or drop. A 10 ms poll
        // adds nothing to durability (the WAL is what makes writes durable;
        // the archive is the tier's backup copy) and is invisible against the
        // ~43 ms an archive itself takes.
        //
        // Anything still queued when the store shuts down is covered by
        // `archive_all`, which archives every dirty segment at shutdown and
        // always has.
        {
            let weak = Arc::downgrade(&chunks_arc);
            let stopped = chunks_arc.background_stopped.clone();
            std::thread::Builder::new()
                .name("seal-archiver".into())
                .spawn(move || loop {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    let Some(chunks) = weak.upgrade() else { break };
                    for (chunk_id, seg_id) in chunks.seal_queue().drain() {
                        if let Some(chunk) = chunks.list.get(chunk_id) {
                            chunk.finish_rotated_head(seg_id);
                        }
                    }
                })
                .expect("spawn seal-archiver");
        }

        // Statistics sweeper: the only place chunk statistics are rebuilt.
        // Write paths just bump a change counter (a refresh walks every cell
        // and can grind for minutes at scale; on a writer's thread that
        // grind froze id-list shard workers inside their polls and wedged
        // the whole edge phase). Weak: the thread must not keep the store
        // alive, and exits when the Chunks drops (server-spawning tests).
        //
        // It checks EVERY SECOND and sweeps every thirtieth check, rather than
        // sleeping thirty seconds and then checking. Same sweep cadence, but a
        // dropped store gets its thread back in about a second instead of up to
        // half a minute -- which matters to anything that creates and drops
        // stores in a loop, where those seconds accumulate into hundreds of
        // live threads.
        {
            let weak = Arc::downgrade(&chunks_arc);
            let stopped = chunks_arc.background_stopped.clone();
            std::thread::Builder::new()
                .name("stats-sweeper".into())
                .spawn(move || {
                    const SWEEP_EVERY: u32 = 30;
                    let mut ticks: u32 = 0;
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                            break;
                        }
                        // Upgrade too: a store that is dropped outright never
                        // gets the chance to set the flag.
                        let Some(chunks) = weak.upgrade() else { break };
                        ticks += 1;
                        if ticks < SWEEP_EVERY {
                            continue;
                        }
                        ticks = 0;
                        for chunk in &chunks.list {
                            chunk.statistics.sweep_from_chunk(chunk);
                        }
                    }
                })
                .expect("spawn stats-sweeper");
        }

        // Store global pointer for signal handler access
        set_global_chunks(&chunks_arc);

        // Attempt recovery if enabled
        if enable_recovery {
            info!("Recovery enabled, attempting to recover from storage");

            let config = crate::ram::recovery::RecoveryConfig {
                num_chunks: count,
                chunk_size,
                max_threads: Some(64), // Cap recovery parallelism to reduce contention storms
            };

            match crate::ram::recovery::recover_chunks(
                &config,
                &backup_storage,
                &wal_storage,
                &raft_storage,
                &chunks_arc.list,
                &chunks_arc.recovered_origin_floors,
                Some(recovery_files),
            ) {
                Ok(()) => {
                    info!("Recovery completed successfully");
                }
                Err(e) => {
                    // Refuse to start rather than come up empty on top of a
                    // store that exists.
                    //
                    // "Starting with fresh storage" was not a fallback, it was
                    // silent data loss with a log line: the process continued
                    // with empty chunks while the real segments sat on disk,
                    // and every write from that point layered new state over a
                    // store the operator still believed was intact. TB14 came
                    // up this way after one unscannable segment, and the
                    // ranged index then wiped 31 of its 40 trees against the
                    // empty store it found.
                    //
                    // A store that cannot be recovered is a situation for a
                    // human. Unscannable segments no longer reach here -- they
                    // are quarantined individually -- so this is now reserved
                    // for whole-store failures: unreadable directories, I/O
                    // errors, a file larger than a segment. Every one of those
                    // is worth stopping for, and none is improved by writing
                    // more data on top.
                    error!(
                        "RECOVERY FAILED for this database: {:?}. REFUSING TO START. The \
                         existing store is left untouched. Starting empty would hide it behind \
                         new writes; individual unscannable segments are quarantined and do not \
                         reach this path, so this is a whole-store failure that needs looking at.",
                        e
                    );
                    panic!("recovery failed, refusing to start over an existing store: {e:?}");
                }
            }
        }

        chunks_arc
    }

    /// Sync all buffered WAL data to disk across all chunks
    pub fn sync_all(&self) {
        info!("Syncing WAL for all chunks...");
        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                segment.force_wal_sync();
            }
        }
        info!("All WAL data synced to disk.");
    }

    /// Refuse further entry allocation, everywhere, before shutdown archives.
    ///
    /// This must be called before `archive_all`, and it is one-way: a segment
    /// that has been archived is sealed and can never take another append, so
    /// a write accepted after this point would have no durable home. Callers
    /// see `WriteError::ServerShuttingDown`, which is honest and retryable --
    /// the alternative is accepting the cell and losing it silently at the
    /// next crash.
    pub fn close_writes(&self) {
        for chunk in &self.list {
            chunk.writes_closed.store(true, Ordering::Release);
        }
        info!("Entry allocation closed on {} chunks for shutdown", self.list.len());
    }

    /// Archive all dirty segments to backup storage across all chunks
    /// This ensures all in-memory data is persisted to backup files before shutdown
    /// Live bytes for each named slot, from the write-path counters.
    /// One atomic load per slot -- safe to call from anything, including an
    /// RPC handler or a balancer's poll.
    pub fn seal_queue(&self) -> &Arc<SealQueue> {
        &self.list[0].seal_queue
    }

    pub fn slot_live_bytes(&self, slots: &[u32]) -> Vec<u64> {
        slots.iter().map(|slot| self.slot_bytes.get(*slot)).collect()
    }

    /// Total live bytes this store holds across all slots.
    pub fn total_live_bytes(&self) -> u64 {
        self.slot_bytes.total()
    }

    pub fn archive_all(&self) {
        info!("Archiving all dirty segments to backup storage...");
        let mut total_archived = 0;
        let mut total_skipped = 0;
        let mut total_errors = 0;

        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                // Check if segment needs archiving
                let is_clean = !segment.is_dirty();

                if is_clean {
                    total_skipped += 1;
                    continue;
                }

                // Archive the segment
                match segment.archive() {
                    Ok(true) => {
                        debug!("Archived segment {} (chunk {})", segment.id, chunk.id);
                        total_archived += 1;
                    }
                    Ok(false) => {
                        debug!("Segment {} already archived", segment.id);
                        total_skipped += 1;
                    }
                    Err(e) => {
                        error!(
                            "Failed to archive segment {} (chunk {}): {}",
                            segment.id, chunk.id, e
                        );
                        total_errors += 1;
                    }
                }
            }
        }

        info!(
            "Segment archiving complete: {} archived, {} skipped (clean), {} errors",
            total_archived, total_skipped, total_errors
        );
    }

    /// A dummy chunk set with backup storage configured, for tests that need
    /// segments which can actually be archived.
    pub fn new_dummy_with_backup(
        count: usize,
        size: usize,
        backup_storage: Option<String>,
    ) -> Arc<Chunks> {
        Chunks::new(
            count,
            size,
            Arc::<ServerMeta>::new(ServerMeta {
                schemas: LocalSchemasCache::new_local(""),
            }),
            None,
            backup_storage,
            None,
            None,
        )
    }

    pub fn new_dummy(count: usize, size: usize) -> Arc<Chunks> {
        // Dummy doesn't use tiered memory or recovery
        Chunks::new(
            count,
            size,
            Arc::<ServerMeta>::new(ServerMeta {
                schemas: LocalSchemasCache::new_local(""),
            }),
            None,
            None,
            None,
            None,
        )
    }
    pub fn locate_chunk_by_partition(&self, partition: u64) -> &Chunk {
        let chunk_id = partition as usize % self.list.len();
        return &self.list[chunk_id];
    }
    fn locate_chunk_by_key(&self, key: &Id) -> (&Chunk, u64) {
        return (self.locate_chunk_by_partition(key.locality() as u64), key.bits());
    }
    pub fn read_cell(&self, key: &Id) -> Result<SharedCell<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell(hash);
    }
    // By-address full-cell read: materializes the cell exactly as stored at
    // `location`, regardless of where the cell index currently points. Used by
    // repeatable-read pinning to re-read a specific version whose address and
    // segment guard were captured earlier.
    pub fn read_cell_at(&self, key: &Id, location: usize) -> Result<OwnedCell, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_cell_at(hash, location);
    }
    pub fn read_selected(
        &self,
        key: &Id,
        fields: &[u64],
        need_header: bool,
    ) -> Result<SharedCell<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_selected(hash, fields, need_header);
    }
    // By-address projected read: same field-projection logic as `read_selected`,
    // pinned to `location` instead of the cell index.
    pub fn read_selected_at(
        &self,
        key: &Id,
        location: usize,
        fields: &[u64],
    ) -> Result<OwnedCell, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.locality() as u64);
        return chunk.read_selected_at(location, fields, true);
    }
    pub fn read_partial_raw(
        &self,
        key: &Id,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.read_partial_raw(hash, offset, len);
    }
    pub fn head_cell(&self, key: &Id) -> Result<CellHeader, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.head_cell(hash);
    }
    // By-address header read: same as `head_cell` but pinned to `location`
    // instead of resolving through the cell index.
    pub fn head_at(&self, key: &Id, location: usize) -> Result<CellHeader, ReadError> {
        let chunk = self.locate_chunk_by_partition(key.locality() as u64);
        return chunk.head_at(location);
    }
    // Cheap capture of the current cell's raw address and version (index lookup
    // + header decode, no value materialization). See `Chunk::cell_location_and_version`.
    pub fn cell_location_and_version(&self, key: &Id) -> Result<(usize, u64), ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.cell_location_and_version(hash);
    }
    pub fn location_for_read(&self, key: &Id) -> Result<CellReadGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.location_for_read(hash)
    }
    pub fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.id.locality() as u64);
        return chunk.write_cell(cell);
    }
    pub fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.id.locality() as u64);
        return chunk.update_cell(cell);
    }
    pub fn update_cell_by<U>(&self, key: &Id, update: U) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.update_cell_by(hash, update);
    }
    /// Upsert many cells, batching the append-log claim per chunk.
    ///
    /// Cells are grouped by the chunk they belong to, which for a migration
    /// batch is ONE chunk: `locate_chunk_by_partition` keys off the id's
    /// locality and a batch is one slot. Results come back in the caller's
    /// order regardless of how the grouping fell out.
    pub fn upsert_run(&self, cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>> {
        if cells.is_empty() {
            return Vec::new();
        }
        // The overwhelmingly common case, and the one this exists for: every
        // cell of a migration batch shares a locality, so they all belong to
        // one chunk and the slice can go straight through with no copying.
        let first_chunk = cells[0].header.id.locality() as usize % self.list.len();
        if cells
            .iter()
            .all(|cell| cell.header.id.locality() as usize % self.list.len() == first_chunk)
        {
            return self.list[first_chunk].upsert_run(&cells);
        }

        let mut by_chunk: std::collections::HashMap<usize, Vec<usize>> = Default::default();
        for (index, cell) in cells.iter().enumerate() {
            let chunk_id = cell.header.id.locality() as usize % self.list.len();
            by_chunk.entry(chunk_id).or_default().push(index);
        }
        let mut results: Vec<Option<Result<CellHeader, WriteError>>> =
            (0..cells.len()).map(|_| None).collect();
        for (chunk_id, indices) in by_chunk {
            let group: Vec<OwnedCell> = indices.iter().map(|i| cells[*i].clone()).collect();
            let group_results = self.list[chunk_id].upsert_run(&group);
            for (slot, result) in indices.into_iter().zip(group_results.into_iter()) {
                results[slot] = Some(result);
            }
        }
        results
            .into_iter()
            .map(|r| r.unwrap_or(Err(WriteError::CannotAllocateSpace)))
            .collect()
    }

    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let chunk = self.locate_chunk_by_partition(cell.header.id.locality() as u64);
        return chunk.upsert_cell(cell);
    }
    /// See `Chunk::remove_cell_keeping_indices`: for a migration's reclaim only.
    pub fn remove_cell_keeping_indices(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.remove_cell_keeping_indices(hash)
    }

    pub fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell(hash);
    }
    pub fn remove_cell_by<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.remove_cell_by(hash, predict);
    }
    pub fn address_of(&self, key: &Id) -> usize {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return *chunk.location_for_read(hash).unwrap();
    }

    /// Ids of every live cell on this server whose slot is in `slots`.
    ///
    /// One pass per chunk, and the caller supplies the whole slot set so a
    /// migration plan covering many slots costs one sweep rather than one per
    /// slot.
    pub fn cell_ids_in_slots(&self, slots: &std::collections::HashSet<u16>) -> Vec<Id> {
        self.list
            .iter()
            .flat_map(|chunk| chunk.cell_ids_in_slots(slots))
            .collect()
    }

    pub fn count(&self) -> usize {
        self.list.iter().map(|c| c.count()).sum()
    }

    pub fn clear_cell_index(&self) -> usize {
        let mut removed = 0usize;
        for chunk in &self.list {
            removed += chunk.cell_index.len();
            chunk.cell_index.clear();
            chunk.statistics.ensured_refresh_chunk(chunk);
        }
        removed
    }

    pub fn all_chunk_statistics(&self, schema_id: u32) -> Vec<Option<Arc<SchemaStatistics>>> {
        self.list
            .iter()
            .map(|c| c.statistics.schemas.get(&schema_id))
            .collect()
    }
    pub fn ensure_statistics(&self) {
        self.list
            .iter()
            .for_each(|c| c.statistics.ensured_refresh_chunk(c));
    }
    pub fn overall_statistics(&self, schema: u32) -> Arc<SchemaStatistics> {
        self.statistics
            .get(schema as usize, 5 * 60, |schema| {
                let schema = schema as u32;
                let all_stats = self
                    .all_chunk_statistics(schema)
                    .into_iter()
                    .filter_map(|s| s)
                    .collect::<Vec<_>>();
                merge_statistics(all_stats).map(|s| Arc::new(s))
            })
            .unwrap()
    }

    pub fn lock_cell_for_read(&self, key: &Id) -> Result<CellGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.lock_cell_for_read(hash);
    }
    pub fn lock_cell_for_write(
        &self,
        key: &Id,
        has_read: bool,
    ) -> Result<CellGuard<'_>, ReadError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.lock_cell_for_write(hash, has_read);
    }

    pub fn compare_version_and_update_cell(
        &self,
        key: &Id,
        version: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_version_and_update_cell(hash, version, cell);
    }

    pub fn compare_version_and_set_field(
        &self,
        key: &Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        return chunk.compare_version_and_set_field(hash, version, field, value);
    }
}

pub struct CellGuard<'a> {
    segment: Option<AArc<Segment>>,
    guard: Option<WordMutexGuard<'a>>,
    chunk: &'a Chunk,
    hash: u64,
    version: u64,
    /// The QSBR section covering this read.
    ///
    /// A read resolves a raw address out of the cell index and then touches
    /// segment memory at it; between those two steps it holds no reference,
    /// so no per-segment count can see it. The section does. It lives here
    /// rather than on the reference count because a `CellGuard` borrows the
    /// chunk and so cannot outlive the stack frame that made it -- which is
    /// exactly the property a thread-scoped quiescent state needs.
    _qsbr: crate::ram::qsbr::QsbrSection,
}

impl<'a> CellGuard<'a> {
    pub fn from_guard(hash: u64, guard: WordMutexGuard<'a>, chunk: &'a Chunk) -> Option<Self> {
        let mut segment = None;
        let mut version = 0;
        if *guard != 0 {
            #[cfg(feature = "tiered_memory")]
            {
                segment = chunk.locate_segment(*guard);
                if let Some(seg) = &segment {
                    if seg.is_cold() {
                        // Serve the read from a single backup block where we can,
                        // rather than materialising all 8 MiB to reach one cell.
                        // The block is decompressed back to its own offset inside
                        // this segment's mapping, so `*guard` stays valid and the
                        // rest of the read path is unaffected.
                        //
                        // Falls through to promotion when the backup predates the
                        // block-indexed format, or when the block read fails --
                        // promotion is slower but always works.
                        //
                        // The block read is always attempted first and is never
                        // skipped in favour of promotion. Promotion waits for
                        // every reference on the segment to drain, and a block
                        // read hands the caller a live reference to a segment
                        // that is still cold -- so a caller holding one and then
                        // demanding promotion of the same segment waits on
                        // itself. That livelocked 33 threads in sched_yield for
                        // 50 minutes during a sidecar build, which reads several
                        // cells of one segment at a time.
                        // Pin BEFORE faulting the block in, not after.
                        //
                        // `try_reclaim_resident_blocks` hands a cold segment's
                        // faulted-in blocks back under pressure, and it takes the
                        // segment exclusively to do it -- which only excludes
                        // readers that already hold a reference. Faulting first
                        // and referencing second left a window: the block is
                        // resident, the reclaimer sees zero references, takes
                        // exclusivity, `madvise`s the segment away and releases
                        // it, and the reader's `incr_references` then SUCCEEDS on
                        // a segment whose pages are gone. It returns a guard onto
                        // freed memory and reads `Id(0)`.
                        //
                        // Caught by `a_read_racing_eviction_never_sees_a_stale_pointer`
                        // at 1 stale read in ~295k, whose verdict named it exactly:
                        // "segment 14 is COLD, offset INSIDE the written range,
                        // promoted never".
                        //
                        // The reference is released again on every path that does
                        // not return a guard, because promotion below waits for
                        // references to drain and would otherwise wait on us --
                        // the livelock that cost 33 threads 50 minutes once.
                        if !seg.incr_references() {
                            return None;
                        }
                        match seg.fault_in_block_for(*guard) {
                            Ok(Some(newly_resident)) => {
                                // These bytes just came off disk through the
                                // one path that verifies nothing: the
                                // whole-file CRC is checked only on a full
                                // read, so a block served directly was never
                                // checked against anything. Now the entry's
                                // own checksum answers for it.
                                //
                                // Returning None on a mismatch is not a
                                // silent drop: it falls through to promotion
                                // below, which reads the whole backup and
                                // DOES verify its CRC, so a genuinely
                                // corrupt file is refused there with the
                                // full diagnosis.
                                if crate::ram::entry::verify_entry_at(*guard) == Some(false) {
                                    error!(
                                        "Cold block read of segment {} (chunk {}) produced an \
                                         entry that fails its content checksum at {:#x}. \
                                         Refusing to serve it; falling back to a full read of \
                                         the backup, which verifies the whole image.",
                                        seg.id, chunk.id, *guard
                                    );
                                    seg.decr_references();
                                    return None;
                                }
                                seg.mark_referenced();
                                if let Some(ref tiered) = chunk.tiered_manager {
                                    tiered.add_cold_resident(newly_resident);
                                    tiered.note_cold_block_read();
                                }
                                return Some(CellGuard {
                                _qsbr: crate::ram::qsbr::QsbrSection::new(),
                                    hash,
                                    guard: Some(guard),
                                    chunk,
                                    segment,
                                    version,
                                });
                            }
                            Ok(None) => {
                                seg.decr_references();
                            }
                            Err(e) => {
                                seg.decr_references();
                                debug!(
                                    "Partial read of segment {} in chunk {} failed, promoting: {}",
                                    seg.id, chunk.id, e
                                )
                            }
                        }

                        // CRITICAL: Release the cell lock BEFORE promotion to avoid deadlock.
                        // The deadlock scenario:
                        // - Thread A holds cell lock, calls promote_segment, waits for segment exclusive access
                        // - Thread B holds segment reference (via another cell), waits for the same cell lock
                        // By releasing the guard first, we break this circular wait.
                        // The caller's retry loop will re-acquire the lock after promotion completes.
                        drop(guard);

                        if let Some(ref tiered_manager) = chunk.tiered_manager {
                            if let Err(e) = tiered_manager.promote(chunk, seg) {
                                warn!(
                                    "Failed to promote segment {} in chunk {}: {}",
                                    seg.id, chunk.id, e
                                );
                            }
                        } else {
                            crate::ram::tiered::promotion::promote_segment(&seg);
                        }
                        // Return None to signal caller to retry (now segment should be hot)
                        return None;
                    }
                    if !seg.incr_references() {
                        return None;
                    }
                    seg.mark_referenced();
                } else {
                    trace!(
                        "Segment not found for cell at {:?} for chunk {}. Should retry.",
                        *guard,
                        chunk.id
                    );
                    return None;
                }
            }
            version = cell_version_from_chunk_raw(*guard).unwrap();
        }

        Some(Self {
            _qsbr: crate::ram::qsbr::QsbrSection::new(),
            guard: Some(guard),
            chunk,
            hash,
            segment,
            version,
        })
    }

    pub fn for_read(hash: u64, chunk: &'a Chunk) -> Result<Self, ReadError> {
        let backoff = Backoff::new();
        loop {
            let guard = chunk.location_for_read(hash)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Ok(guard);
            }
            backoff.spin();
        }
    }

    pub fn for_write(hash: u64, has_read: bool, chunk: &'a Chunk) -> Option<Self> {
        let backoff = Backoff::new();
        loop {
            let guard = chunk.location_for_write(hash, has_read)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Some(guard);
            }
            backoff.spin();
        }
    }

    fn update_version(&mut self, version: u64) {
        if self.version < version {
            self.version = version;
        }
    }

    pub fn head_cell(&mut self) -> Result<CellHeader, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (header, _) = header_from_chunk_raw(self.get_ptr())?;
        self.update_version(header.version);
        Ok(header)
    }

    pub fn cell_version(&mut self) -> Result<u64, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let version = cell_version_from_chunk_raw(self.get_ptr())?;
        self.update_version(version);
        Ok(version)
    }

    pub fn read_cell_owned(&mut self) -> Result<OwnedCell, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_version(data.header.version);
        Ok(data.to_owned())
    }

    pub fn read_cell_shared(&mut self) -> Result<SharedCellData<'_>, ReadError> {
        if self.is_unassigned() {
            return Err(ReadError::CellDoesNotExisted);
        }
        let (data, _) = SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)?;
        self.update_version(data.header.version);
        Ok(data)
    }

    pub fn is_unassigned(&self) -> bool {
        self.get_ptr() == 0
    }

    pub fn update_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let old_cell_loc = self.get_ptr();
        if cell.header.version < self.version {
            cell.header.version = self.version;
        }
        if self.is_unassigned() {
            return Err(WriteError::CellDoesNotExisted);
        }
        let write_plan = cell.plan_write(self.chunk)?;
        let pending_entry = write_plan.allocate(self.chunk, false)?;
        let write_result = self.chunk.write_cell_to_chunk(
            cell,
            &write_plan,
            &pending_entry,
            cell.header.version,
        )?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let old_indices = self.old_index_res(schema)?;
        let guard = self.guard.as_mut().unwrap();
        **guard = new_cell_loc;
        self.chunk
            .ensure_indices_with_res(cell, old_indices, schema);
        self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        self.chunk.refresh_statistics_for_schema(schema.id);
        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        Ok(cell.header)
    }

    /// Upsert a cell - updates if the guard points to an existing cell, inserts if empty.
    /// This is useful when you have a guard from `try_insert_locked` which may point to
    /// an empty slot (insert case) or an existing cell (update case).
    pub fn upsert_cell(&mut self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        let old_cell_loc = self.get_ptr();
        if cell.header.version < self.version {
            cell.header.version = self.version;
        }
        let write_plan = cell.plan_write(self.chunk)?;
        let pending_entry = write_plan.allocate(self.chunk, false)?;
        let write_result = self.chunk.write_cell_to_chunk(
            cell,
            &write_plan,
            &pending_entry,
            cell.header.version,
        )?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let schema_id = schema.id;
        if old_cell_loc != 0 {
            // Update case - cell already exists
            let old_indices = self.old_index_res(&*schema)?;
            let guard = self.guard.as_mut().unwrap();
            **guard = new_cell_loc;
            self.chunk
                .ensure_indices_with_res(cell, old_indices, &*schema);
            self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        } else {
            // Insert case - new cell
            let guard = self.guard.as_mut().unwrap();
            **guard = new_cell_loc;
            self.chunk.ensure_indices(cell, None, &*schema);
        }

        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;

        self.chunk.refresh_statistics_for_schema(schema_id);
        Ok(cell.header)
    }

    pub fn word_mutex_guard(&mut self) -> &mut WordMutexGuard<'a> {
        self.guard.as_mut().unwrap()
    }

    pub fn get_ptr(&self) -> usize {
        **self.guard.as_ref().unwrap() as usize
    }

    pub fn remove_cell(mut self) {
        self.decrement_segment_references();
        self.segment = None;
        self.guard.take().unwrap().remove();
    }

    #[inline(always)]
    fn decrement_segment_references(&self) {
        if let Some(segment) = &self.segment {
            segment.decr_references();
        }
    }

    fn old_index_res(&self, schema: &Schema) -> Result<Option<Vec<IndexRes>>, WriteError> {
        if self.chunk.index_builder.is_some() {
            SharedCellData::from_chunk_raw(self.hash, self.get_ptr(), self.chunk)
                .map(|(c, _)| Some(probe_cell_indices(&c, schema)))
                .map_err(|e| WriteError::ReadError(e))
        } else {
            Ok(None)
        }
    }

    fn set_ptr(&mut self, ptr: usize) {
        let guard = self.guard.as_mut().unwrap();
        **guard = ptr;
    }
}

impl<'a> Drop for CellGuard<'a> {
    fn drop(&mut self) {
        #[cfg(feature = "tiered_memory")]
        self.decrement_segment_references();
    }
}

impl<'a> Deref for CellGuard<'a> {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &**self.guard.as_ref().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::{Field, LocalSchemasCache};
    use crate::ram::types::Map;
    use bifrost_hasher::hash_str;
    use dovahkiin::types::Type;
    use env_logger;

    const TEST_CHUNK_SIZE: usize = 8 * 1024 * 1024;

    /// Drain until the retired segment is reclaimed, or give up.
    ///
    /// QSBR's epoch is process-global, so a thread inside a segment critical
    /// section anywhere -- including another test running concurrently --
    /// holds back every drain. Reclamation is therefore "once readers have
    /// left", not "immediately", and a test must poll for it rather than
    /// demand it on the first call.
    fn drain_until_reclaimed(chunk: &Chunk, deadline: std::time::Duration) -> usize {
        let started = std::time::Instant::now();
        loop {
            let freed = chunk.drain_retired_segments();
            if freed > 0 || started.elapsed() > deadline {
                return freed;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// Two databases each have their own `Chunks`, their own cleaner and their
    /// own retire list, but they share one QSBR epoch. What that sharing does
    /// and does not couple is worth pinning down.
    ///
    /// A *reference* held in database A must not block database B: references
    /// are per-segment, and B's segment has none. Only an in-flight read
    /// section is global, and only for as long as the read runs.
    #[test]
    fn a_reference_in_one_database_does_not_block_another_from_reclaiming() {
        let _ = env_logger::try_init();
        let (chunks_a, _schema_a) = setup_test_chunks();
        let (chunks_b, _schema_b) = setup_test_chunks();
        let chunk_a = &chunks_a.list[0];
        let chunk_b = &chunks_b.list[0];

        // A long-lived pin in database A, of the kind a transaction holds.
        let seg_a = chunk_a
            .segs
            .get(&(chunk_a.get_head_seg_id() as usize))
            .expect("database A has a head segment");
        assert!(seg_a.incr_references());

        let b_seg_id = chunk_b.get_head_seg_id();
        chunk_b.remove_segment(b_seg_id);
        assert_eq!(chunk_b.retired_segment_count(), 1);

        assert_eq!(
            drain_until_reclaimed(chunk_b, std::time::Duration::from_secs(5)),
            1,
            "a pin in database A must not stop database B reclaiming its own segment"
        );

        seg_a.decr_references();
    }

    /// The coupling that IS real: a read in flight anywhere holds back every
    /// reclamation, because the reclaimer cannot tell which segment that
    /// reader is about to touch. It lasts only as long as the read.
    #[test]
    fn an_in_flight_read_holds_back_reclamation_until_it_finishes() {
        let _ = env_logger::try_init();
        let (chunks_a, schema_a) = setup_test_chunks();
        let (chunks_b, _schema_b) = setup_test_chunks();
        let chunk_a = &chunks_a.list[0];
        let chunk_b = &chunks_b.list[0];

        let id = Id::allocated(1, 0, 11);
        let mut cell = payload_cell(schema_a.id, &id, 32);
        chunks_a.write_cell(&mut cell).expect("write a cell to read");

        // A read in progress in database A: the guard is the open section.
        let reading = CellGuard::for_read(id.bits(), chunk_a).expect("read guard");

        let b_seg_id = chunk_b.get_head_seg_id();
        chunk_b.remove_segment(b_seg_id);
        assert_eq!(
            chunk_b.drain_retired_segments(),
            0,
            "a read in flight anywhere must hold back reclamation everywhere"
        );

        drop(reading);
        assert_eq!(
            drain_until_reclaimed(chunk_b, std::time::Duration::from_secs(5)),
            1,
            "once the read finishes, reclamation proceeds"
        );
    }

    /// A segment reference taken on one thread and released on another must
    /// not strand QSBR.
    ///
    /// This is not hypothetical: `PinnedReadSet` stores a
    /// `SegmentReferenceGuard` for a transaction's whole lifetime, so the
    /// thread that pins a repeatable read is rarely the thread that ends the
    /// transaction. If entering and leaving the quiescent state are bound to
    /// the acquiring thread, the pinning thread never returns to quiescence
    /// and reclamation stalls for every database in the process.
    #[test]
    fn a_reference_released_on_another_thread_does_not_strand_reclamation() {
        let _ = env_logger::try_init();
        let (chunks, _schema) = setup_test_chunks();
        let chunk = &chunks.list[0];

        let segment = chunk
            .segs
            .get(&(chunk.get_head_seg_id() as usize))
            .expect("head segment");

        // Pin on one thread that then STAYS ALIVE doing other work, the way a
        // tokio worker does after handling the request that pinned the read.
        // A thread that exits would have its slot recycled and hide the bug.
        let (pinned_tx, pinned_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let pinning = std::thread::spawn({
            let segment = segment.clone();
            move || {
                assert!(segment.incr_references());
                pinned_tx.send(()).unwrap();
                // Still running, holding nothing on its stack.
                finish_rx.recv().unwrap();
            }
        });
        pinned_rx.recv().unwrap();

        // The transaction ends on a different thread, dropping the guard there.
        let releasing = std::thread::spawn({
            let segment = segment.clone();
            move || {
                segment.decr_references();
            }
        });
        releasing.join().expect("releasing thread");

        let seg_id = chunk.get_head_seg_id();
        chunk.remove_segment(seg_id);
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)),
            1,
            "a reference acquired and released on different threads stranded reclamation"
        );
        finish_tx.send(()).unwrap();
        pinning.join().expect("pinning thread");
    }

    /// A chunk whose allocator is empty must refuse the write, not wedge.
    ///
    /// `try_acquire_in_class` publishes `HEAD_SEG_ID_ALLOCATING` into the head
    /// slot while it allocates, and every other writer spins waiting for that
    /// to be replaced. The allocation used to end in
    /// `.expect("No space left after full GCs")`, so the slot stayed at
    /// ALLOCATING forever and every writer spun on it -- three cores pinned
    /// and no further write to that chunk, ever. The tiered stress test hung
    /// on exactly this for over 25 minutes.
    ///
    /// The capacity check cannot prevent it, which is the point of this test:
    /// it counts `segs`, so segments that are unpublished but not yet
    /// reclaimed read as free space that the allocator does not have.
    #[test]
    fn an_empty_allocator_refuses_the_write_instead_of_wedging_the_head_slot() {
        let _ = env_logger::try_init();
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("wedge_test", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        // Room for several segments, so the capacity guard stays out of the way.
        let chunks = Chunks::new(
            1,
            TEST_CHUNK_SIZE * 8,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let chunk = &chunks.list[0];

        // Take every segment the allocator has without publishing any of them,
        // so `segs.len()` still reports room while the supply is gone. This is
        // the state combine reaches when its sources are unpublished and their
        // addresses have not come back yet.
        let mut hoarded = Vec::new();
        while let Some(seg) = chunk.allocator.alloc_seg(&chunk.file_manager) {
            hoarded.push(seg);
        }
        assert!(!hoarded.is_empty(), "the allocator should have had segments");

        // Fill the published head. The write that overflows it is the one that
        // has to allocate, and the allocator has nothing left to give.
        let mut refusal = None;
        for i in 0..5_000u64 {
            let id = Id::allocated(1, 0, i + 1);
            let mut cell = payload_cell(schema.id, &id, 4096);
            if let Err(error) = chunks.write_cell(&mut cell) {
                refusal = Some(error);
                break;
            }
        }
        assert!(
            refusal.is_some(),
            "overflowing the head with an empty allocator must fail, not succeed"
        );
        assert!(
            chunk
                .head_pool
                .iter()
                .all(|slot| slot.load(Ordering::Acquire) != HEAD_SEG_ID_ALLOCATING),
            "the failed allocation left a head slot poisoned; every later writer would spin on it"
        );

        // And the chunk still answers instead of spinning.
        let (tx, rx) = std::sync::mpsc::channel();
        let chunks_for_probe = chunks.clone();
        let schema_id = schema.id;
        std::thread::spawn(move || {
            let id = Id::allocated(1, 0, 9_999_999);
            let mut cell = payload_cell(schema_id, &id, 4096);
            let _ = tx.send(chunks_for_probe.write_cell(&mut cell).is_ok());
        });
        assert!(
            rx.recv_timeout(std::time::Duration::from_secs(20)).is_ok(),
            "a write against an exhausted chunk never returned -- the head slot is wedged"
        );
    }

    /// The corruption itself: a reader holding a reference must still be able
    /// to read the bytes it was pointed at.
    ///
    /// Pre-fix, `remove_segment` called `free_memory` immediately, and
    /// `madvise(MADV_DONTNEED)` discards private anonymous pages at once --
    /// so the reader's cell turned to zeros under its hands. That is the
    /// `SchemaDoesNotExisted(0)` seen in the field, and the zero-filled
    /// resident images the archiver later refused to persist.
    #[test]
    fn a_reader_inside_a_retired_segment_still_reads_its_data() {
        let _ = env_logger::try_init();
        let (chunks, schema) = setup_test_chunks();
        let chunk = &chunks.list[0];

        let id = Id::allocated(1, 0, 7);
        let mut cell = payload_cell(schema.id, &id, 64);
        chunks.write_cell(&mut cell).expect("write the cell to read back");

        let addr = {
            let stored = chunks.read_cell(&id).expect("read the cell");
            stored.cell_guard().get_ptr()
        };
        let segment_id = chunk.get_head_seg_id();
        let segment = chunk
            .segs
            .get(&(segment_id as usize))
            .expect("segment is published");
        assert!(
            segment.incr_references(),
            "the reader takes a live reference, as the read paths do"
        );

        // What the reader can see before the segment is taken away.
        let before =
            unsafe { std::slice::from_raw_parts(addr as *const u8, 64) }.to_vec();
        assert!(
            before.iter().any(|b| *b != 0),
            "precondition: the cell has non-zero bytes to lose"
        );

        chunk.remove_segment(segment_id);

        let after = unsafe { std::slice::from_raw_parts(addr as *const u8, 64) }.to_vec();
        assert_eq!(
            before, after,
            "the reader's own data was discarded underneath it by unpublishing"
        );

        segment.decr_references();
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(10)),
            1,
            "the segment should be reclaimed once its reader has left"
        );
    }

    /// A writer must never append into a segment somebody holds exclusively.
    ///
    /// `incr_references` returns false when a segment is exclusively held -- an
    /// evictor, a promoter or the cleaner owns it and is about to free or
    /// replace its pages. Every READ path honours that answer. The write path
    /// discarded it and appended anyway, so a cell could be placed in memory
    /// that `evict_segment` was about to `madvise(MADV_DONTNEED)`: the append
    /// cursor moved, the WAL entry was written, the cell index took the address,
    /// and the pages were then zeroed underneath it.
    ///
    /// That is the mechanism behind the silent cell loss under tier pressure --
    /// `stale cell read: ... found Id(0)` -- which only ever appeared with
    /// eviction active and was intermittent because it needs the exclusive CAS
    /// to land inside this window. A scale reshard lost 129 of 1048576 cells
    /// that way while reporting 0 failures.
    ///
    /// The test holds the head exclusively for the whole run: a writer that
    /// respects the protocol makes no progress into it (it waits for the head to
    /// rotate or the owner to finish), and one that does not returns an address
    /// inside it immediately. Whether the writer waits or rotates is its
    /// business; putting a cell inside a segment being freed is not.
    #[test]
    fn a_writer_never_appends_into_an_exclusively_held_segment() {
        let _ = env_logger::try_init();
        let (chunks, _schema) = setup_test_chunks();
        let chunk = &chunks.list[0];

        let head_id = chunk.get_head_seg_id();
        let head = chunk
            .segs
            .get(&(head_id as usize))
            .expect("the bootstrap segment is published");
        let head_start = head.addr;
        let head_end = head.bound();

        // Stand in for an evictor, which takes the segment exclusively before it
        // archives and frees the pages.
        assert!(
            head.obtain_exclusive_references(),
            "a quiet segment should be claimable exclusively"
        );

        let acquired: Arc<std::sync::Mutex<Option<usize>>> = Arc::new(std::sync::Mutex::new(None));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let chunks_for_writer = chunks.clone();
        let acquired_for_writer = acquired.clone();
        let done_for_writer = done.clone();
        let writer = std::thread::spawn(move || {
            let chunk = &chunks_for_writer.list[0];
            if let Ok(entry) = chunk.try_acquire(64, false) {
                *acquired_for_writer.lock().unwrap() = Some(entry.addr);
                // Do NOT run the WAL/dirty/decr path on a segment we should
                // never have been given: just record the address and leak the
                // entry, which is all the assertion needs.
                std::mem::forget(entry);
            }
            done_for_writer.store(true, std::sync::atomic::Ordering::Release);
        });

        // Generous: the unfixed path returns in microseconds.
        std::thread::sleep(std::time::Duration::from_millis(500));
        let inside = acquired
            .lock()
            .unwrap()
            .map(|addr| addr >= head_start && addr < head_end)
            .unwrap_or(false);
        assert!(
            !inside,
            "a writer was handed an address inside a segment held exclusively for eviction, \
             so its cell would be zeroed by the madvise that follows"
        );

        // Release and let the writer finish, so the test leaves nothing spinning.
        head.release_exclusive_references();
        for _ in 0..200 {
            if done.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = writer.join();
        assert!(
            done.load(std::sync::atomic::Ordering::Acquire),
            "the writer must make progress once the segment is released; if it never does, \
             honouring the reference has turned a rare loss into a hang"
        );
    }

    /// Unpublishing a segment must never free it while a reader is inside.
    ///
    /// `remove_segment` used to drop the pages (`MADV_DONTNEED`, so they read
    /// back as zeros), delete the backup and the WAL, and return the address
    /// to the allocator's free list -- all while combine held only a shared
    /// reference and other readers could be mid-decode. The reader then saw
    /// zeros, and once the address was recycled, another segment's live bytes.
    #[test]
    fn a_retired_segment_is_not_reclaimed_while_a_reader_is_inside() {
        let _ = env_logger::try_init();
        let (chunks, _schema) = setup_test_chunks();
        let chunk = &chunks.list[0];

        // The chunk's bootstrap segment stands in for any combined-away source.
        let segment_id = chunk.get_head_seg_id();

        // A reader takes a reference the way the read paths do.
        let reader = chunk
            .segs
            .get(&(segment_id as usize))
            .expect("segment is published");
        assert!(reader.incr_references(), "reader takes a live reference");

        chunk.remove_segment(segment_id);
        assert!(
            chunk.segs.get(&(segment_id as usize)).is_none(),
            "removal must unpublish immediately so no new reader finds it"
        );
        assert_eq!(
            chunk.retired_segment_count(),
            1,
            "the segment is retired, not freed"
        );

        assert_eq!(
            chunk.drain_retired_segments(),
            0,
            "a segment with a reader inside must not be reclaimed"
        );
        assert_eq!(chunk.retired_segment_count(), 1);

        // The reader leaves, announcing quiescence.
        reader.decr_references();

        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(10)),
            1,
            "once the reader has left, the segment is reclaimed"
        );
        assert_eq!(chunk.retired_segment_count(), 0);
    }

    fn setup_test_chunks() -> (Arc<Chunks>, Schema) {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("cell_stored_len_test", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            TEST_CHUNK_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        (chunks, schema)
    }

    fn payload_cell(schema_id: u32, id: &Id, payload_len: usize) -> OwnedCell {
        let data: Vec<u8> = std::iter::repeat(id.bits() as u8).take(payload_len).collect();
        OwnedCell {
            header: CellHeader::new(schema_id, id),
            data: data_map_value!(id: id.bits() as i32, data: data),
        }
    }

    #[test]
    fn read_at_returns_pinned_version_after_update() {
        let _ = env_logger::try_init();
        let (chunks, schema) = setup_test_chunks();

        let id = Id::allocated(1, 0, 42);
        let mut cell = payload_cell(schema.id, &id, 16);
        chunks.write_cell(&mut cell).unwrap();

        // Capture version A's raw address and its full/selected contents.
        let addr = {
            let sc = chunks.read_cell(&id).unwrap();
            sc.cell_guard().get_ptr()
        };
        let full_before = chunks.read_cell(&id).unwrap().to_owned();
        let selected_before = chunks
            .read_selected(&id, &[hash_str("data")], true)
            .unwrap()
            .to_owned();

        // Update the cell in place: the cell index now serves version B at a
        // different address.
        let mut updated = payload_cell(schema.id, &id, 32);
        chunks.update_cell(&mut updated).unwrap();

        // Sanity check: by-id reads now observe the new version.
        let full_after = chunks.read_cell(&id).unwrap().to_owned();
        assert_ne!(full_after.data, full_before.data);
        assert!(full_after.header.version > full_before.header.version);

        // Reading BY ADDRESS still returns the OLD version (copy-on-write).
        let pinned = chunks.read_cell_at(&id, addr).unwrap();
        assert_eq!(pinned.data, full_before.data);
        assert_eq!(pinned.header.version, full_before.header.version);

        // head_at agrees with the pinned header.
        let h = chunks.head_at(&id, addr).unwrap();
        assert_eq!(h.version, full_before.header.version);

        // read_selected_at returns the pinned projection, not the new one.
        let selected_pinned = chunks
            .read_selected_at(&id, addr, &[hash_str("data")])
            .unwrap();
        assert_eq!(selected_pinned.data, selected_before.data);

        // An invalid (unit) address still errors like the by-id path.
        assert!(chunks.head_at(&id, 0).is_err());
        assert!(chunks.read_cell_at(&id, 0).is_err());
    }
}
