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

use super::schema::{Schema, SchemaUid, SchemaVid};
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
/// Entries whose explicit WAL append failed.
pub static WAL_JOURNAL_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Allocation refusals because the chunk had no room left.
///
/// A full store is not a durability failure, but it looks exactly like one
/// from the outside: index write-back cannot allocate, its batches are
/// abandoned, the barrier is never established, and entries go missing. Any
/// harness judging durability has to be able to tell the two apart, so the
/// refusal is counted where it happens.
pub static ALLOCATION_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
/// Bytes marked dead, including versions replaced by the cleaner.
pub static DEAD_BYTES_PRODUCED: AtomicU64 = AtomicU64::new(0);
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
///
/// The blob pool is 4 rather than 2 because a transaction leases a head for
/// its whole lifetime, and `can_spare_a_head_for_lease` will not hand out
/// the second-to-last one. At 2 exactly one blob transaction per chunk could
/// be bracketed and the rest wrote unbracketed. The extra slots cost nothing
/// resident until blob traffic actually contends -- an EMPTY slot counts as
/// available to a lease without a segment behind it -- so this buys blob
/// atomicity at zero cost in the quiet case, which is the common one.
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
        SegmentClass::Blob => *BLOB.get_or_init(|| read("NEB_BLOB_HEAD_POOL", 4)),
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

/// What happened when a write tried the head its transaction already holds.
///
/// The distinction between FULL and UNUSABLE is load-bearing: only a full
/// head may be closed and continued in a fresh part. A head that is merely
/// busy -- being reclaimed, sealing, or of the wrong class -- must be left
/// exactly as it is, because closing it stamps a tail link over space the
/// segment still owns.
enum LeasedPlacement {
    Placed(PendingEntry),
    Full,
    Unusable,
    NoLease,
    /// The transaction was ended (aborted, expired) while this thread was
    /// still writing for it. Its segments are rewound and may belong to
    /// someone else; the write must fail.
    Revoked,
}

/// One transaction's exclusive hold on a chunk write head.
///
/// The pool already gives every writer exclusive ownership of a head; a
/// transaction differs only in how long it holds one. Plain writes hold for
/// a cell, batches for a run, transactions until the 2PC decision -- which
/// is what makes a transaction's entries contiguous in a chunk, and what
/// will let an abort physically rewind them (Step 4) instead of logging undo
/// records for them.
pub struct HeadLease {
    seg: AArc<Segment>,
    /// Where this segment's cursor stood before the transaction wrote into
    /// it. This is what an abort rewinds to, and it can only be captured at
    /// claim time -- after the fact it cannot be reconstructed.
    pre_cursor: usize,
}

struct TransactionLeases {
    /// chunk KEY -> heads this transaction holds there, oldest first.
    ///
    /// The key is (allocator base address, chunk id), not the id alone: ids
    /// restart at 0 in every `Chunks` instance, and a process holds several
    /// -- one per database, and one per server in any test that starts more
    /// than one. Keyed by id alone, two unrelated stores shared a lease, so a
    /// write meant for one landed in the other's segment and put a foreign
    /// address into this chunk's cell index. That address is outside this
    /// allocator's range, so every later lookup failed and the caller's retry
    /// loop spun forever.
    ///
    /// A Vec rather than one entry because a write set that outgrows its
    /// head rotates onto a fresh one, and a rewind has to undo both. Until
    /// brackets land (Step 2) a rotation is harmless: nothing yet depends on
    /// a transaction's entries sharing one segment.
    per_chunk: std::collections::HashMap<(usize, usize), Vec<HeadLease>>,
    opened_at: std::time::Instant,
    /// `TransactionScope`s currently naming this transaction. While any is
    /// live a thread may still write for it, so ending its leases must leave
    /// a REVOKED record here rather than nothing: a thread that found no
    /// record would open a fresh bracket and carry on writing for a
    /// transaction that no longer exists.
    live_scopes: usize,
    /// The ownership fence: the stores in which this transaction's leases
    /// were ended by an abort or an expiry. Every placement checks it under
    /// the registry lock, so a stale owner's write fails instead of landing
    /// in a segment that has been rewound and handed to another writer.
    /// Per STORE, like everything else here: one process hosts a store per
    /// database, and ending a transaction in one says nothing about another.
    revoked: Vec<StoreRange>,
}

impl TransactionLeases {
    fn is_revoked_in(&self, chunk_base: usize) -> bool {
        self.revoked.iter().any(|store| store.contains(&chunk_base))
    }

    fn new() -> Self {
        TransactionLeases {
            per_chunk: std::collections::HashMap::new(),
            opened_at: std::time::Instant::now(),
            live_scopes: 0,
            revoked: Vec::new(),
        }
    }
}

lazy_static! {
    /// Leases of in-flight transactions. Global and keyed by transaction id
    /// because a lease outlives the apply burst: writes land in one
    /// synchronous region, but the decision that releases them arrives in a
    /// LATER RPC, on another thread. Thread-locals cannot express that.
    static ref TXN_LEASES: Mutex<std::collections::HashMap<crate::server::transactions::TxnId, TransactionLeases>> =
        Mutex::new(std::collections::HashMap::new());
}

/// Guard acquisitions abandoned at the deadline. Non-zero means a cell index
/// entry outlived its segment: real damage, and previously a hang.
pub static CELL_GUARD_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// Writes refused because no head came free in time.
pub static HEAD_QUEUE_TIMEOUTS: AtomicU64 = AtomicU64::new(0);
/// How long a write may queue for a head before refusing. Generous next to
/// the microseconds an ordinary writer holds one, and short next to a
/// transaction that is never going to let go.
const HEAD_QUEUE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
pub static TXN_LEASES_OPENED: AtomicU64 = AtomicU64::new(0);
pub static TXN_LEASES_RELEASED: AtomicU64 = AtomicU64::new(0);
pub static TXN_LEASES_EXPIRED: AtomicU64 = AtomicU64::new(0);

/// Leases ended by padding over the transaction's span (an abort).
pub static TXN_SPANS_REWOUND: AtomicU64 = AtomicU64::new(0);
/// Heads retired (padded to their end and left full) because their bracket
/// had to stay as it was: a sweeper that cannot vouch for the cell index, an
/// abort whose rollback failed, or a rewind that could not be journaled.
pub static TXN_HEADS_RETIRED: AtomicU64 = AtomicU64::new(0);
/// Rewinds that could not be journaled. Each one retired its head instead.
pub static TXN_REWIND_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Writes refused because their transaction had already been ended (the
/// ownership fence caught a stale owner).
pub static TXN_STALE_OWNER_WRITES: AtomicU64 = AtomicU64::new(0);
/// Transactional writes refused because no head could be spared for the
/// transaction within the bounded wait. Each one failed its transaction
/// instead of writing outside the bracket.
pub static TXN_HEAD_UNAVAILABLE: AtomicU64 = AtomicU64::new(0);

/// How long a transactional write waits for a head of its own before the
/// transaction fails. Long next to a plain writer's microsecond hold, short
/// next to a client's patience.
fn txn_head_wait() -> std::time::Duration {
    static WAIT: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *WAIT.get_or_init(|| {
        std::env::var("NEB_TXN_HEAD_WAIT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(std::time::Duration::from_millis)
            .unwrap_or(std::time::Duration::from_secs(2))
    })
}

thread_local! {
    /// The transaction whose apply burst is running on this thread, if any.
    /// Thread-local is right HERE -- the burst is synchronous -- while the
    /// lease registry it indexes must be global.
    static CURRENT_TXN: std::cell::RefCell<Option<crate::server::transactions::TxnId>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    /// The participant set of the transaction in `CURRENT_TXN`. Each BEGIN
    /// records it, so that a site that restarts in doubt knows the commit
    /// point of the transaction.
    static CURRENT_PARTICIPANTS: std::cell::RefCell<Vec<u64>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn current_txn() -> Option<crate::server::transactions::TxnId> {
    CURRENT_TXN.with(|t| t.borrow().clone())
}

fn current_participants() -> Vec<u64> {
    CURRENT_PARTICIPANTS.with(|p| p.borrow().clone())
}

/// Name (or un-name) the transaction whose writes this thread is making.
///
/// Private on purpose: the only way in is [`TransactionScope`], which puts
/// the previous value back on every exit, unwinding included.
fn set_transaction_id(
    txn: Option<crate::server::transactions::TxnId>,
) -> Option<crate::server::transactions::TxnId> {
    IN_TRANSACTION.with(|flag| flag.set(txn.is_some()));
    CURRENT_TXN.with(|t| t.replace(txn))
}

/// The region of a thread's work that writes on behalf of one transaction.
///
/// While it lives, every write on this thread is leased to `txn`: it lands in
/// the head the transaction holds in its chunk, behind the transaction's
/// BEGIN. Dropping it restores whatever was named before, on EVERY exit.
///
/// It replaces a bare set/reset pair, which an unwind skipped: a panic
/// between BEGIN and COMMIT left the thread still naming the dead
/// transaction, so the next ORDINARY write on that thread was leased to it,
/// landed inside its bracket, and was recovered as one of its uncommitted
/// entries -- discarded although it had been acknowledged.
///
/// The scope only names. Ending the transaction's leases is the decision's
/// job (commit, or abort with its rollback), because only the decider knows
/// whether the cell index still names the span. A panicking apply is aborted
/// by its caller, which holds that knowledge (see `DataManager`).
#[must_use = "the transaction is named only while the scope lives"]
pub struct TransactionScope {
    previous: Option<crate::server::transactions::TxnId>,
    previous_participants: Vec<u64>,
    /// Thread-bound: dropping it on another thread would reset the wrong
    /// thread's naming.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl TransactionScope {
    /// Name `txn` with no participant set. Its BEGIN records an empty set,
    /// which recovery reads as a transaction of this site only.
    pub fn enter(txn: crate::server::transactions::TxnId) -> Self {
        Self::enter_with_participants(txn, Vec::new())
    }

    /// Name `txn` and the servers that hold a bracket for it. Each BEGIN
    /// written in this scope records `participants`.
    pub fn enter_with_participants(
        txn: crate::server::transactions::TxnId,
        participants: Vec<u64>,
    ) -> Self {
        TXN_LEASES
            .lock()
            .entry(txn.clone())
            .or_insert_with(TransactionLeases::new)
            .live_scopes += 1;
        TransactionScope {
            previous: set_transaction_id(Some(txn)),
            previous_participants: CURRENT_PARTICIPANTS.with(|p| p.replace(participants)),
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for TransactionScope {
    fn drop(&mut self) {
        let previous_participants = std::mem::take(&mut self.previous_participants);
        CURRENT_PARTICIPANTS.with(|p| p.replace(previous_participants));
        if let Some(txn) = set_transaction_id(self.previous.take()) {
            let mut held = TXN_LEASES.lock();
            if let Some(leases) = held.get_mut(&txn) {
                leases.live_scopes = leases.live_scopes.saturating_sub(1);
                if leases.live_scopes == 0 && leases.per_chunk.is_empty() {
                    held.remove(&txn);
                }
            }
        }
        if std::thread::panicking() {
            // The commit that would have synced these is not going to run;
            // the next transaction on this thread must not inherit them.
            drop(take_transactional_segments());
        }
    }
}

/// A store identifies itself by the address window its chunks live in.
///
/// `TXN_LEASES` is process-global while a store is not: one process hosts a
/// store per DATABASE, and two of them can hold leases under the same
/// transaction id. Every entry point below therefore takes the caller's
/// window and touches only the leases inside it. Without that, the first
/// store to reach the decision removes the map entry and the second finds
/// nothing to close -- its BEGIN stands with no COMMIT and recovery discards
/// half of a committed transaction.
pub type StoreRange = std::ops::Range<usize>;

/// Take one store's leases out of the map, leaving every other store's alone.
/// The transaction's entry disappears only once no store still holds a head.
fn take_store_leases(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
    revoke: bool,
) -> Vec<((usize, usize), Vec<HeadLease>)> {
    let mut held = TXN_LEASES.lock();
    take_store_leases_locked(&mut held, txn, store, revoke)
}

/// `revoke`: the transaction is being ENDED without a commit, so any thread
/// still writing for it must be fenced off from here on. Done under the same
/// lock every placement takes, which is what makes the fence atomic with
/// respect to a stale owner's next write.
fn take_store_leases_locked(
    held: &mut std::collections::HashMap<crate::server::transactions::TxnId, TransactionLeases>,
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
    revoke: bool,
) -> Vec<((usize, usize), Vec<HeadLease>)> {
    let Some(leases) = held.get_mut(txn) else {
        return Vec::new();
    };
    if revoke && !leases.revoked.contains(store) {
        leases.revoked.push(store.clone());
    }
    let mine: Vec<(usize, usize)> = leases
        .per_chunk
        .keys()
        .filter(|key| store.contains(&key.0))
        .copied()
        .collect();
    let taken: Vec<_> = mine
        .into_iter()
        .filter_map(|key| leases.per_chunk.remove(&key).map(|heads| (key, heads)))
        .collect();
    if leases.per_chunk.is_empty() && leases.live_scopes == 0 {
        held.remove(txn);
    }
    taken
}

/// Why a transaction's leases did not end cleanly. Never a silent
/// fallthrough: each variant says which promise failed, and what the lease
/// code did instead so that no head with an open bracket reaches a writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseExitError {
    /// A plain release found a bracket still open on a head that could take
    /// more appends. NOTHING was released: the leases are still held, and
    /// the caller must close the bracket (commit) or end it (rewind/retire).
    OpenBracket(crate::ram::segs::OpenBracketRefused),
    /// The aborted span could not be padded over in the log. The head was
    /// RETIRED instead -- full, so nothing can land behind its BEGIN, and
    /// rotated out and sealed by the next writer to find it -- and the span
    /// stays an uncommitted bracket, which recovery discards.
    RewindFailed {
        chunk_id: usize,
        segment_id: u64,
        reason: String,
        write_error: WriteError,
    },
    /// A rewind was asked for a bracket that already carries its COMMIT. The
    /// span is left as it is: padding over a committed bracket loses a
    /// decided transaction. This is the TLA+ rule from the other side -- the
    /// abort path keys on transaction STATE, and the state said abort while
    /// the log says commit.
    RewindOfCommitted { chunk_id: usize, segment_id: u64 },
}

impl std::fmt::Display for LeaseExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseExitError::OpenBracket(refused) => write!(f, "{refused}"),
            LeaseExitError::RewindFailed {
                chunk_id,
                segment_id,
                reason,
                ..
            } => write!(
                f,
                "could not rewind the aborted span in segment {segment_id} (chunk {chunk_id}): \
                 {reason}; the head was retired and the span stays an uncommitted bracket"
            ),
            LeaseExitError::RewindOfCommitted {
                chunk_id,
                segment_id,
            } => write!(
                f,
                "refused to rewind segment {segment_id} (chunk {chunk_id}): its bracket is \
                 already committed"
            ),
        }
    }
}

impl std::error::Error for LeaseExitError {}

impl LeaseExitError {
    pub(crate) fn into_write_error(self) -> WriteError {
        match self {
            Self::RewindFailed { write_error, .. } => write_error,
            error => WriteError::IOError {
                os_error: None,
                message: error.to_string(),
            },
        }
    }
}

/// How a transaction's leases end.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum LeaseExit {
    /// Abort, AFTER the caller rolled the cell index back: pad over each span
    /// (journaled), then release.
    Rewind,
    /// Leave every bracket exactly as it is and make each head that still
    /// holds an open one full, then release. For callers that cannot vouch
    /// that nothing in the index names the span.
    Retire,
}

/// End one lease so that the head can go back to the pool. Never hands over
/// a head with an open bracket and room behind it.
fn end_lease(
    txn: &crate::server::transactions::TxnId,
    chunk_id: usize,
    lease: HeadLease,
    exit: LeaseExit,
    first_error: &mut Option<LeaseExitError>,
) {
    let seg = &lease.seg;
    match exit {
        LeaseExit::Rewind if seg.bracket_is_committed() => {
            error!(
                "transaction {:?}: asked to rewind segment {} (chunk {}) whose bracket already \
                 carries its COMMIT; leaving it committed",
                txn, seg.id, chunk_id
            );
            first_error.get_or_insert(LeaseExitError::RewindOfCommitted {
                chunk_id,
                segment_id: seg.id,
            });
        }
        LeaseExit::Rewind => {
            // Everything from where the transaction found the cursor to where
            // the cursor is now is the transaction's own: it owned the head the
            // whole time. Includes a span with no BEGIN (a lease taken by the
            // unbracketed fallback), whose entries must vanish just the same.
            // A stale owner may still be inside a write it claimed before
            // the revocation (claims are taken under the lock that revoked
            // it). Let that entry land first, so the padding covers it.
            #[cfg(feature = "abort_profile")]
            let drain_timer =
                crate::ram::abort_profile::guard(crate::ram::abort_profile::Phase::Drain);
            let drained = seg.drain_pending_journals(std::time::Duration::from_secs(5));
            #[cfg(feature = "abort_profile")]
            drop(drain_timer);
            let end = seg.append_header.load(Ordering::Acquire);
            let outcome = if !drained {
                Err(io::Error::other(
                    "a write into the span did not finish; its extent is unknown",
                ))
            } else if lease.pre_cursor < end {
                // Journal now. The head goes back at once, below: the next
                // owner's records follow the padding in this segment's WAL,
                // and replay stops at the first bad record, so no state
                // keeps a later record without the padding before it.
                seg.disown_span(lease.pre_cursor, end)
            } else {
                Ok(())
            };
            match outcome {
                Ok(()) => {
                    seg.mark_bracket_disowned();
                    seg.protection.abort_transaction(txn);
                    TXN_SPANS_REWOUND.fetch_add(1, Ordering::Relaxed);
                    // The group-commit timer makes the padding durable.
                    if lease.pre_cursor < end {
                        crate::ram::segs::queue_wal_sync(seg);
                    }
                }
                Err(error) => {
                    TXN_REWIND_FAILURES.fetch_add(1, Ordering::Relaxed);
                    error!(
                        "transaction {:?}: could not rewind its span in segment {} (chunk {}): \
                         {}; retiring the head instead",
                        txn, seg.id, chunk_id, error
                    );
                    seg.retire_tail();
                    TXN_HEADS_RETIRED.fetch_add(1, Ordering::Relaxed);
                    first_error.get_or_insert(LeaseExitError::RewindFailed {
                        chunk_id,
                        segment_id: seg.id,
                        reason: error.to_string(),
                        write_error: WriteError::from_wal_io(error),
                    });
                }
            }
        }
        LeaseExit::Retire => {
            if seg.bracket_is_open() && seg.can_still_append() {
                seg.retire_tail();
                TXN_HEADS_RETIRED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    // Ownership first, then the reference: a segment that is still owned
    // cannot be sealed, so dropping the reference first would briefly leave a
    // head nobody can reclaim.
    match seg.try_release_own() {
        Ok(()) => seg.decr_references(),
        Err(refused) => {
            // Unreachable by construction -- every branch above leaves the
            // bracket closed, disowned or the head full -- and kept out of the
            // pool if it ever is reached: still owned, still referenced.
            error!("transaction {:?}: {}", txn, refused);
            first_error.get_or_insert(LeaseExitError::OpenBracket(refused));
        }
    }
}

fn end_transaction_leases(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
    exit: LeaseExit,
) -> Result<usize, LeaseExitError> {
    let leases = take_store_leases(txn, store, true);
    let mut first_error = None;
    let mut ended = 0usize;
    for (key, heads) in leases {
        // Newest first, so no moment exists where an earlier part is ended
        // while a later part of the same transaction is still in use.
        for lease in heads.into_iter().rev() {
            end_lease(txn, key.1, lease, exit, &mut first_error);
            ended += 1;
        }
    }
    TXN_LEASES_RELEASED.fetch_add(ended as u64, Ordering::Relaxed);
    match first_error {
        Some(error) => Err(error),
        None => Ok(ended),
    }
}

/// Make an aborted transaction's writes vanish from the log, then give its
/// heads back. The group-commit timer syncs the padding later.
///
/// Call it AFTER the transaction's effects were rolled back in the cell
/// index, and BEFORE anything else may write into its heads -- which is
/// automatic, because the transaction owns them until it releases them. Each
/// span is padded over and the padding journaled (see [`Segment::disown_span`]), so
/// recovery walks straight past it and the next owner's entries are ordinary.
///
/// Keyed on the transaction's STATE, never on whether a COMMIT record exists:
/// TLC found that the cleaner legitimately drops COMMIT records, so "no
/// COMMIT present" is not the same question as "not committed". The one place
/// the log is consulted is the other way round: a bracket already carrying
/// its COMMIT is never padded over (`RewindOfCommitted`).
///
/// `Err` still ends every lease. A span that could not be rewound is left as
/// an uncommitted bracket in a RETIRED head (`RewindFailed`): never reused,
/// rotated out and sealed by the ordinary full-head path.
pub fn rewind_transaction_writes(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> Result<usize, LeaseExitError> {
    end_transaction_leases(txn, store, LeaseExit::Rewind)
}

/// End a transaction's leases without touching what it wrote: every head
/// still holding an open bracket is retired (padded to its end, full), then
/// all are released.
///
/// The catch-all for a caller that cannot vouch for the cell index -- the
/// lease-expiry sweeper for a transaction no participant object knows about,
/// an abort whose rollback failed, the final cleanup of a transaction whose
/// decision already closed or rewound everything. The open brackets stay
/// uncommitted and recovery discards them, exactly what a crash in the same
/// window gets; the full head guarantees nobody appends behind them.
pub fn retire_transaction_leases(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> Result<usize, LeaseExitError> {
    end_transaction_leases(txn, store, LeaseExit::Retire)
}

/// Release every head a transaction holds, refusing if any of them still has
/// an open bracket it could append behind.
///
/// For the decision that CLOSED every bracket: after a commit, every bracket
/// carries its COMMIT and the heads go back to the pool with their room
/// intact. Anything still open means the caller skipped a step, and the
/// refusal changes nothing -- the leases are still held for the right exit
/// to end.
pub fn release_transaction_leases(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> Result<usize, LeaseExitError> {
    let leases = {
        let mut held = TXN_LEASES.lock();
        let open = held.get(txn).and_then(|leases| {
            leases
                .per_chunk
                .iter()
                .filter(|(key, _)| store.contains(&key.0))
                .flat_map(|(_, heads)| heads.iter())
                .find(|lease| lease.seg.bracket_is_open() && lease.seg.can_still_append())
                .map(|lease| crate::ram::segs::OpenBracketRefused {
                    chunk_id: lease.seg.chunk_id,
                    segment_id: lease.seg.id,
                    seq_id: lease.seg.seq_id,
                })
        });
        if let Some(refused) = open {
            crate::ram::segs::OPEN_BRACKET_RELEASES_REFUSED.fetch_add(1, Ordering::Relaxed);
            error!(
                "transaction {:?}: refusing to release its heads: {}",
                txn, refused
            );
            return Err(LeaseExitError::OpenBracket(refused));
        }
        take_store_leases_locked(&mut held, txn, store, false)
    };
    let mut first_error = None;
    let mut released = 0usize;
    for (key, heads) in leases {
        for lease in heads.into_iter().rev() {
            // Nothing is open, so Retire changes nothing; it is used for its
            // release half and its backstop.
            end_lease(txn, key.1, lease, LeaseExit::Retire, &mut first_error);
            released += 1;
        }
    }
    TXN_LEASES_RELEASED.fetch_add(released as u64, Ordering::Relaxed);
    match first_error {
        Some(error) => Err(error),
        None => Ok(released),
    }
}

/// Transactions whose leases have been idle longer than `idle`, WITHOUT
/// releasing anything.
///
/// The selection half of `expire_transaction_leases`, split out because the
/// participant has to act BEFORE the release: resolving a transaction to
/// commit means closing its bracket, presuming abort means rolling it back
/// and rewinding its span, and both need the leases a release throws away.
pub fn transactions_with_idle_leases(
    idle: std::time::Duration,
    store: &StoreRange,
) -> Vec<crate::server::transactions::TxnId> {
    let now = std::time::Instant::now();
    let held = TXN_LEASES.lock();
    held.iter()
        .filter(|(_, leases)| now.duration_since(leases.opened_at) >= idle)
        .filter(|(_, leases)| leases.per_chunk.keys().any(|key| store.contains(&key.0)))
        .map(|(txn, _)| txn.clone())
        .collect()
}

/// Reclaim leases whose transaction has been silent longer than `idle`.
///
/// Mandatory, not a refinement: a coordinator that vanishes between prepare
/// and decision otherwise holds its heads forever, and the pool has no refill
/// path -- one hostage head is one less writer slot in that chunk,
/// permanently.
///
/// This is the LAST resort and knows nothing about the cell index, so it
/// RETIRES: each head with an open bracket is padded to its end and left
/// full -- never handed to a writer that would append behind the BEGIN --
/// and the bracket stays uncommitted, which recovery discards. The
/// participant aborts the transactions it knows about properly (rollback,
/// then rewind) before calling this; what reaches here has no participant
/// object to roll back. Returns the transactions whose leases were reclaimed.
pub fn expire_transaction_leases(
    idle: std::time::Duration,
    store: &StoreRange,
    hold: &std::collections::BTreeSet<crate::server::transactions::TxnId>,
) -> Vec<crate::server::transactions::TxnId> {
    let expired: Vec<_> = transactions_with_idle_leases(idle, store)
        .into_iter()
        // Held back by cooperative termination: the question "did anyone
        // commit this?" is still open, and closing a bracket needs the
        // leases this release would throw away.
        .filter(|txn| !hold.contains(txn))
        .collect();
    for txn in &expired {
        match retire_transaction_leases(txn, store) {
            Ok(heads) => warn!(
                "transaction {:?} held {} write head(s) for more than {:?} without a decision; \
                 retired them. Its open brackets stay uncommitted and are discarded by \
                 recovery.",
                txn, heads, idle
            ),
            Err(error) => error!(
                "transaction {:?} held write heads for more than {:?} without a decision and \
                 they did not end cleanly: {}",
                txn, idle, error
            ),
        }
    }
    TXN_LEASES_EXPIRED.fetch_add(expired.len() as u64, Ordering::Relaxed);
    expired
}

/// `Chunks::sync_all` could not sync every segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAllFailed {
    pub segments: usize,
}

impl std::fmt::Display for SyncAllFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} segment(s) could not be synced", self.segments)
    }
}

impl std::error::Error for SyncAllFailed {}

/// The manifest of a transaction's brackets: every chunk it wrote and the
/// seq id of the segment holding that chunk's bracket.
///
/// Exact rather than estimated, which is what lets any single part decide
/// the whole transaction: a COMMIT is honoured only when every member it
/// names is present.
#[cfg(test)]
lazy_static! {
    static ref INJECTED_COMMIT_REFUSALS: Mutex<Vec<(usize, crate::server::transactions::TxnId, u32)>> =
        Mutex::new(Vec::new());
    static ref INJECTED_SYNC_FAILURES: Mutex<Vec<(usize, crate::server::transactions::TxnId)>> =
        Mutex::new(Vec::new());
}

/// Test hook: the next COMMIT decision of `txn` in the store at `base_addr`
/// is refused after its COMMIT records are placed and before they are
/// synced, as a full store or a failed sync would refuse it.
#[cfg(test)]
pub(crate) fn refuse_next_commit_for_test(
    base_addr: usize,
    txn: crate::server::transactions::TxnId,
) {
    refuse_commits_for_test(base_addr, txn, 1);
}

/// Test hook: the next `times` COMMIT decisions of `txn` are refused, as a
/// store that stays full refuses them.
#[cfg(test)]
pub(crate) fn refuse_commits_for_test(
    base_addr: usize,
    txn: crate::server::transactions::TxnId,
    times: u32,
) {
    INJECTED_COMMIT_REFUSALS.lock().push((base_addr, txn, times));
}

#[cfg(test)]
fn take_injected_commit_refusal(
    base_addr: usize,
    txn: &crate::server::transactions::TxnId,
) -> bool {
    let mut refusals = INJECTED_COMMIT_REFUSALS.lock();
    match refusals
        .iter()
        .position(|(base, refused, _)| *base == base_addr && refused == txn)
    {
        Some(index) => {
            refusals[index].2 -= 1;
            if refusals[index].2 == 0 {
                refusals.remove(index);
            }
            true
        }
        None => false,
    }
}

/// Test hook: in the next COMMIT decision of `txn`, the sync of a segment
/// fails after a segment that holds a COMMIT was synced. One COMMIT is then
/// durable and another is not.
#[cfg(test)]
pub(crate) fn fail_commit_sync_after_first_for_test(
    base_addr: usize,
    txn: crate::server::transactions::TxnId,
) {
    INJECTED_SYNC_FAILURES.lock().push((base_addr, txn));
}

#[cfg(test)]
fn take_injected_sync_failure(base_addr: usize, txn: &crate::server::transactions::TxnId) -> bool {
    let mut failures = INJECTED_SYNC_FAILURES.lock();
    match failures
        .iter()
        .position(|(base, failed)| *base == base_addr && failed == txn)
    {
        Some(index) => {
            failures.remove(index);
            true
        }
        None => false,
    }
}

#[cfg(test)]
static REFUSED_RESOLVED_COMMITS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Test hook: every chunk in the process refuses a resolved COMMIT until the
/// hook is cleared, as a full store refuses it.
#[cfg(test)]
pub(crate) fn refuse_resolved_commits_for_test(refuse: bool) {
    REFUSED_RESOLVED_COMMITS.store(refuse, Ordering::Release);
}

#[cfg(test)]
pub(crate) fn resolved_commits_refused_for_test() -> bool {
    REFUSED_RESOLVED_COMMITS.load(Ordering::Acquire)
}

/// Why [`Chunks::commit_transaction_brackets`] did not close every bracket.
///
/// The three cases differ in what a power loss can bring back, and so in
/// what the caller may still do with the transaction.
#[derive(Debug)]
pub enum CommitRefusal {
    /// No COMMIT of the transaction can be on disk: none was synced, and the
    /// retraction of the records this attempt placed is synced. The caller
    /// may roll the transaction back.
    Retracted(WriteError),
    /// At least one COMMIT of the transaction is synced. Recovery commits
    /// the whole transaction from it, so the transaction is decided COMMIT
    /// and must be finished, never rolled back.
    Durable(WriteError),
    /// No COMMIT was synced, and the retraction could not be synced either.
    /// A COMMIT can come back after a power loss, so the caller must not
    /// roll back and must not report the transaction committed.
    Unresolved(WriteError),
}

impl CommitRefusal {
    pub fn error(&self) -> &WriteError {
        match self {
            CommitRefusal::Retracted(error)
            | CommitRefusal::Durable(error)
            | CommitRefusal::Unresolved(error) => error,
        }
    }
}

pub fn transaction_manifest(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> Vec<crate::ram::bracket::ManifestEntry> {
    let held = TXN_LEASES.lock();
    let Some(leases) = held.get(txn) else {
        return Vec::new();
    };
    let mut manifest: Vec<_> = leases
        .per_chunk
        .iter()
        // SCOPED TO ONE STORE. `TXN_LEASES` is process-global and its key is
        // (store base address, chunk id) exactly because a chunk id alone is
        // not unique across stores. The manifest keeps only the chunk id, so
        // without this filter a process hosting two stores builds a manifest
        // naming the OTHER store's chunks -- and `commit_transaction_brackets`
        // then looks for leases in chunks that never had any and fails the
        // whole decision with CannotAllocateSpace.
        //
        // Invisible in production, where a store is a process. Immediate in
        // any test that puts two servers in one process, which is what a
        // cross-node transaction test has to do.
        // NOTE the key's first element is the CHUNK's allocator base
        // (store base + i * chunk size), not the store base -- comparing it
        // to the store base directly matches chunk 0 and nothing else.
        .filter(|(chunk_id, _)| store.contains(&chunk_id.0))
        .flat_map(|(chunk_id, heads)| {
            heads
                .iter()
                .map(move |lease| crate::ram::bracket::ManifestEntry {
                    chunk_id: chunk_id.1 as u16,
                    seq_id: lease.seg.seq_id,
                })
        })
        .collect();
    // Sorted so the same transaction produces the same manifest bytes in
    // every chunk it wrote; a reader comparing two parts should not have to
    // treat ordering as meaningful.
    manifest.sort_unstable_by_key(|member| (member.chunk_id, member.seq_id));
    manifest.dedup();
    manifest
}

/// Whether this transaction holds any lease at all -- i.e. whether it wrote
/// anything that needs a bracket closed.
pub fn transaction_has_leases(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> bool {
    TXN_LEASES
        .lock()
        .get(txn)
        .is_some_and(|leases| leases.per_chunk.keys().any(|key| store.contains(&key.0)))
}

/// The segments a transaction holds, for syncing its brackets at the decision.
pub fn transaction_leased_segments(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> Vec<AArc<Segment>> {
    let held = TXN_LEASES.lock();
    held.get(txn)
        .map(|leases| {
            leases
                .per_chunk
                .iter()
                .filter(|(key, _)| store.contains(&key.0))
                .map(|(_, heads)| heads)
                .flat_map(|heads| heads.iter().map(|lease| lease.seg.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// How many heads a transaction currently holds; for tests and metrics.
pub fn transaction_lease_count(
    txn: &crate::server::transactions::TxnId,
    store: &StoreRange,
) -> usize {
    TXN_LEASES
        .lock()
        .get(txn)
        .map(|leases| {
            leases
                .per_chunk
                .iter()
                .filter(|(key, _)| store.contains(&key.0))
                .map(|(_, v)| v.len())
                .sum()
        })
        .unwrap_or(0)
}

/// Record a segment whose transactional entry is not yet durable.
fn note_transactional_write(segment: &AArc<Segment>) {
    TXN_TOUCHED_SEGMENTS.with(|segments| {
        let mut segments = segments.borrow_mut();
        // Runs are common and land in one segment; a linear scan over a
        // handful of entries beats hashing them.
        if !segments.iter().any(|held| {
            held.id == segment.id
                && held.chunk_id == segment.chunk_id
                && held.seq_id == segment.seq_id
        }) {
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
    // The sequence distinguishes generations that reuse the same physical slot.
    pending: parking_lot::Mutex<Vec<(usize, u64, u64)>>,
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
    fn enqueue(&self, chunk_id: usize, seg_id: u64, seq_id: u64) -> bool {
        let mut pending = self.pending.lock();
        if pending.len() >= SEAL_QUEUE_LIMIT {
            return false;
        }
        pending.push((chunk_id, seg_id, seq_id));
        true
    }
    /// A refusal already owns a segment. Retain it even under queue backpressure.
    fn retry(&self, chunk_id: usize, seg_id: u64, seq_id: u64) {
        let mut pending = self.pending.lock();
        let job = (chunk_id, seg_id, seq_id);
        if !pending.contains(&job) {
            pending.push(job);
            crate::ram::segs::SEAL_RETRIES.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn drain(&self) -> Vec<(usize, u64, u64)> {
        std::mem::take(&mut *self.pending.lock())
    }
    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct DeletedVersion {
    pub version: u64,
    pub tombstones: usize,
}

/// Floors and physical tombstone counts share a shard, so reclamation and
/// recreation observe each update atomically without a chunk-wide lock.
pub(crate) struct DeletedVersions {
    shards: [parking_lot::Mutex<std::collections::HashMap<u64, DeletedVersion, ahash::RandomState>>;
        64],
}

impl DeletedVersions {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| {
                parking_lot::Mutex::new(std::collections::HashMap::with_hasher(
                    ahash::RandomState::new(),
                ))
            }),
        }
    }

    fn shard(
        &self,
        id: u64,
    ) -> &parking_lot::Mutex<std::collections::HashMap<u64, DeletedVersion, ahash::RandomState>>
    {
        // Fold locality and origin as well as the sequence for allocated ids.
        &self.shards[((id ^ (id >> 32)) as usize) & (self.shards.len() - 1)]
    }

    fn note(&self, id: u64, version: u64) {
        let mut shard = self.shard(id).lock();
        let floor = shard.entry(id).or_default();
        floor.version = floor.version.max(version);
        floor.tombstones += 1;
    }

    fn version(&self, id: u64) -> u64 {
        self.shard(id)
            .lock()
            .get(&id)
            .map_or(0, |floor| floor.version)
    }

    fn reclaim(&self, id: u64, count: usize) {
        let mut shard = self.shard(id).lock();
        if let std::collections::hash_map::Entry::Occupied(mut floor) = shard.entry(id) {
            if floor.get().tombstones <= count {
                floor.remove();
            } else {
                floor.get_mut().tombstones -= count;
            }
        }
        if shard.is_empty() {
            shard.shrink_to_fit();
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.lock().len()).sum()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().capacity())
            .sum()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct Chunk {
    pub id: usize,
    /// Shared with every chunk of the store and drained by the sealer thread.
    pub seal_queue: Arc<SealQueue>,
    pub cell_index: WordMap,
    /// Last deleted version and number of physical tombstones, per id.
    pub(crate) deleted_versions: DeletedVersions,
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
    pub(crate) retired_segment_files: AtomicUsize,
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
    /// Set at the START of shutdown, ahead of `writes_closed`.
    ///
    /// Shutdown drains index tasks, flushes the LSM and takes the write-back
    /// barrier BEFORE closing writes at step 1.6, and the RPC server keeps
    /// serving until step 3. So a client write can be acked in that window --
    /// `with_indices_ensured` completes its index inserts before replying, so
    /// the entries reach the in-memory tree, but they have missed the only
    /// flush. The cell is archived and survives; its index entry does not.
    ///
    /// Closing ALL writes earlier is not the fix and was tried: the
    /// write-back's own tree pages are cells in these same chunks, and a
    /// closed store refuses them, so the ranged index died at shutdown while
    /// every data cell survived. Hence two stages -- client writes stop here,
    /// the flush's own writes keep going until 1.6.
    client_writes_closed: std::sync::atomic::AtomicBool,
}

/// A segment unpublished from its chunk, waiting for readers to drain.
pub struct RetiredSegment {
    pub segment: AArc<Segment>,
    /// QSBR epoch stamped after the segment left `Chunk::segs`.
    pub stamp: usize,
    /// Drain passes this entry has survived, for reporting one that never
    /// drains rather than retaining it in silence.
    pub attempts: usize,
    deleted_ids: Vec<(u64, usize)>,
}

impl Chunk {
    #[inline]
    fn refresh_statistics_for_schema(&self, schema_id: SchemaVid) {
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
            deleted_versions: DeletedVersions::new(),
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
            retired_segment_files: AtomicUsize::new(0),
            writes_closed: std::sync::atomic::AtomicBool::new(false),
            client_writes_closed: std::sync::atomic::AtomicBool::new(false),
        };
        bootstrap_segment
            .protection
            .head
            .store(true, Ordering::Release);
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
        // A run claims a head for one batch and lets it go: it cannot be
        // bracketed. Inside a transaction, say "no run" and let the caller
        // write cell by cell through the transactional path.
        if is_in_transaction() {
            return Ok(None);
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

    /// Record a freshly claimed head as the current transaction's, keeping it
    /// owned past this entry. Returns whether a lease was taken.
    ///
    /// The extra reference is the point: `PendingEntry::drop` gives back the
    /// per-entry one, and without a second the segment could be reclaimed
    /// while the transaction still means to write into it.
    /// Whether this chunk can spare a head for a transaction to hold.
    ///
    /// A lease is held until the DECISION, so whatever it takes is gone from
    /// the pool for that whole time. If it takes the last usable head, every
    /// other writer in this chunk queues on it -- including the secondary
    /// index writes that the committing transaction is itself waiting for.
    /// Both sides then wait forever. That is not hypothetical: it wedged a
    /// server whose chunk held exactly one segment.
    ///
    /// A chunk sized to one segment cannot hold both heads, so it refuses
    /// a transaction before writing its BEGIN.
    fn can_spare_a_head_for_lease(&self, segment_class: SegmentClass) -> bool {
        let slots = self.head_slots(segment_class);
        let mut usable = 0usize;
        let mut empty = 0usize;
        for slot in slots.iter() {
            let id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                empty += 1;
            } else if id != HEAD_SEG_ID_ALLOCATING {
                if self
                    .segs
                    .get(&(id as usize))
                    .map(|seg| !seg.bracket_is_open() && seg.can_still_append())
                    .unwrap_or(false)
                {
                    usable += 1;
                }
            }
        }
        // Count actual grantable addresses, not one imaginary address for
        // every empty slot. One ordinary head must remain beside the lease.
        usable + empty.min(self.allocator.writer_headroom()) > 1
    }

    fn lease_head_for_current_txn(&self, head: &AArc<Segment>, addr: usize) -> bool {
        let Some(txn) = current_txn() else {
            return false;
        };
        // The caller checked before claiming; re-checked now that the head is
        // owned, because other transactions lease concurrently. A refusal here
        // is NOT an unbracketed write any more: the caller drops the claim
        // and waits for a head, or fails the transaction.
        if !self.can_spare_a_head_for_lease(head.segment_class()) {
            return false;
        }
        if !head.incr_references() {
            // Being reclaimed underneath us: do not lease it.
            return false;
        }
        let mut held = TXN_LEASES.lock();
        let leases = held.entry(txn).or_insert_with(TransactionLeases::new);
        if leases.is_revoked_in(self.lease_key().0) {
            drop(held);
            head.decr_references();
            return false;
        }
        if leases.per_chunk.is_empty() {
            // Idle time for expiry counts from the first head held.
            leases.opened_at = std::time::Instant::now();
        }
        head.protection.retain_transaction(&txn);
        leases
            .per_chunk
            .entry(self.lease_key())
            .or_insert_with(Vec::new)
            .push(HeadLease {
                seg: head.clone(),
                pre_cursor: addr,
            });
        // From here a live transaction owns this segment: the release point
        // refuses to hand it to anyone else until its bracket is closed by a
        // COMMIT, rewound, or the head retired. Set while the head is still
        // owned by this claim, so there is no moment it is held by nobody.
        head.mark_bracket_open();
        TXN_LEASES_OPENED.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Close every bracket this transaction opened in this chunk by writing
    /// its COMMIT into it.
    ///
    /// A bracket is committed IF AND ONLY IF a COMMIT for its transaction is
    /// found, so this is the commit point for everything the transaction
    /// wrote here. Each record goes into its own part, behind the part's own
    /// entries, with nobody else's in between -- which is what the lease
    /// bought.
    ///
    /// EVERY open part gets its own record, never "some other part's":
    /// parts are decided together but COMPACTED INDEPENDENTLY, and compaction
    /// drops a segment's bracket markers. If the only COMMIT lived in another
    /// part, compacting that part would leave this part's BEGIN closed by
    /// nothing, and its committed cells would come back from the next restart
    /// as an undecided transaction. That is why parts are claimed with
    /// `MIN_COMMIT_ENTRY_SIZE` kept free behind every entry: the full-manifest
    /// record is written where it fits, and where it does not, the minimal
    /// one (no manifest; recovery decides on the transaction id alone and uses
    /// the manifest for nothing but a log line) always does.
    ///
    /// `Err` means some part of this chunk has NO COMMIT, and the caller must
    /// not report the transaction committed. Parts closed by an earlier
    /// attempt are skipped, so a retry finishes the job without duplicating.
    pub fn close_transaction_bracket(
        &self,
        txn: &crate::server::transactions::TxnId,
        manifest: &[crate::ram::bracket::ManifestEntry],
    ) -> Result<(), WriteError> {
        let heads: Vec<AArc<Segment>> = {
            let held = TXN_LEASES.lock();
            match held
                .get(txn)
                .and_then(|leases| leases.per_chunk.get(&self.lease_key()))
            {
                Some(heads) => heads.iter().map(|lease| lease.seg.clone()).collect(),
                None => Vec::new(),
            }
        };
        if heads.is_empty() {
            return Err(WriteError::CannotAllocateSpace);
        }
        let full = crate::ram::bracket::encode_commit(txn, manifest);
        let minimal = crate::ram::bracket::encode_commit(txn, &[]);
        debug_assert_eq!(
            crate::ram::entry::ENTRY_HEAD_SIZE + minimal.len(),
            crate::ram::bracket::MIN_COMMIT_ENTRY_SIZE
        );
        for head in heads {
            // A part with no BEGIN (a lease the unbracketed fallback took) has
            // nothing to close, and one already closed must not be closed
            // twice.
            if !head.bracket_is_open() {
                continue;
            }
            let placed = match Self::place_commit(&head, &full) {
                Ok(true) => Ok(true),
                Ok(false) => Self::place_commit(&head, &minimal),
                Err(error) => Err(error),
            };
            match placed {
                Ok(true) => head.mark_bracket_committed(),
                Ok(false) => {
                    error!(
                        "chunk {} could not place even a minimal COMMIT in segment {} for \
                         transaction {:?}: its reserve is gone (the segment was sealed or \
                         retired underneath the lease). The commit FAILS rather than report a \
                         transaction recovery would not find committed.",
                        self.id, head.id, txn
                    );
                    return Err(WriteError::CannotAllocateSpace);
                }
                Err(error) => {
                    error!(
                        "chunk {} could not journal the COMMIT in segment {} for transaction \
                         {:?}: {}. The commit FAILS.",
                        self.id, head.id, txn, error
                    );
                    return Err(WriteError::from_wal_io(error));
                }
            }
        }
        Ok(())
    }

    /// Write `content` as a COMMIT behind the transaction's entries in `head`
    /// and journal it, reporting the journal's outcome.
    ///
    /// The caller receives the append result. `Ok(false)` means no room.
    fn place_commit(head: &AArc<Segment>, content: &[u8]) -> io::Result<bool> {
        let size = (crate::ram::entry::ENTRY_HEAD_SIZE + content.len()) as u32;
        if !head.incr_references() {
            return Err(io::Error::other(format!(
                "segment {} is held exclusively",
                head.id
            )));
        }
        if !head.begin_pending_journal() {
            head.decr_references();
            return Err(io::Error::other(format!("segment {} is sealing", head.id)));
        }
        let Some(addr) = head.try_acquire(size) else {
            head.end_pending_journal();
            head.decr_references();
            return Ok(false);
        };
        crate::ram::entry::stamp_reservation_padding(addr, size);
        crate::ram::entry::Entry::encode_to(
            addr,
            crate::ram::entry::EntryType::COMMIT,
            content.len() as u32,
            |content_addr| unsafe {
                std::ptr::copy_nonoverlapping(
                    content.as_ptr(),
                    content_addr as *mut u8,
                    content.len(),
                );
            },
        );
        // Synced by the caller with the rest of the decision, not here.
        let journaled = head.write_wal(addr, size, true);
        if journaled.is_err() {
            // The record is in memory and not in the log: make it padding, so
            // neither a walk of this image nor a later archive of it can find
            // a COMMIT the log does not have.
            crate::ram::entry::stamp_reservation_padding(addr, size);
        }
        head.end_pending_journal();
        head.set_dirty();
        head.decr_references();
        journaled.map(|()| true)
    }

    /// Write the status cell of a multi-site transaction into this chunk
    /// (`crate::ram::txn_status`). The caller runs inside the transaction's
    /// `TransactionScope`, so the cell lands in the transaction's bracket,
    /// before its COMMIT. An upsert: a resolution that retries after a failed
    /// attempt writes the cell again into its new bracket.
    pub(crate) fn write_status_cell(
        &self,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        self.upsert_cell(cell)
    }

    /// Tombstone a status cell of this chunk: every participant's COMMIT is
    /// synced, so nobody can ask about the transaction any more (TLA+ F4).
    pub(crate) fn remove_status_cell(&self, id: &Id) -> Result<(), WriteError> {
        self.remove_cell(id.bits())
    }

    /// The status cell of `txn` in this chunk, if one exists: its id and the
    /// participants it names.
    pub(crate) fn find_status_cell(
        &self,
        txn: &crate::server::transactions::TxnId,
    ) -> Option<(Id, Vec<u64>)> {
        for probe in 0..crate::ram::txn_status::STATUS_PROBES {
            let id = crate::ram::txn_status::status_id(txn, self.id, probe);
            let Ok(cell) = self.read_cell(id.bits()) else {
                continue;
            };
            if let Ok((found, participants)) =
                crate::ram::txn_status::decode_status_cell(&cell.to_owned())
            {
                if &found == txn {
                    return Some((id, participants));
                }
            }
        }
        None
    }

    /// The first probe id for `txn` in this chunk that is free or already
    /// holds the status cell of `txn`.
    pub(crate) fn free_status_id(&self, txn: &crate::server::transactions::TxnId) -> Option<Id> {
        for probe in 0..crate::ram::txn_status::STATUS_PROBES {
            let id = crate::ram::txn_status::status_id(txn, self.id, probe);
            match self.read_cell(id.bits()) {
                Err(_) => return Some(id),
                Ok(cell) => {
                    if crate::ram::txn_status::decode_status_cell(&cell.to_owned())
                        .is_ok_and(|(found, _)| &found == txn)
                    {
                        return Some(id);
                    }
                }
            }
        }
        None
    }

    /// This chunk's lease key: unique across every store in the process.
    fn lease_key(&self) -> (usize, usize) {
        (self.allocator.base_addr(), self.id)
    }

    /// Whether the calling thread's transaction holds a head of
    /// `segment_class` in this chunk.
    ///
    /// Used to refuse a wait that can never end: the acquire loop queues on
    /// live heads when it cannot grow, and a head held by THIS transaction
    /// will not be released until a decision that cannot arrive while this
    /// call is still blocking. Waiting on yourself is the shape that
    /// livelocked cold reads, and it is worth refusing explicitly rather
    /// than discovering it as a hang.
    ///
    /// Scoped to the class, because the pools are: the acquire loop only ever
    /// queues on slots of the class it was asked for, so a regular head this
    /// transaction holds is not something a blob write can be waiting on.
    /// Answering the question class-blind refused writes that were in no
    /// danger at all.
    fn current_txn_holds_head_here(&self, segment_class: SegmentClass) -> bool {
        let Some(txn) = current_txn() else {
            return false;
        };
        TXN_LEASES
            .lock()
            .get(&txn)
            .and_then(|leases| leases.per_chunk.get(&self.lease_key()))
            .map(|heads| {
                heads
                    .iter()
                    .any(|lease| lease.seg.segment_class() == segment_class)
            })
            .unwrap_or(false)
    }

    /// Place an entry in the head this transaction already holds in this
    /// chunk. `None` means "no lease here, or it no longer fits" and the
    /// caller claims a head the ordinary way.
    fn try_place_in_leased_head(&self, size: u32, segment_class: SegmentClass) -> LeasedPlacement {
        let Some(txn) = current_txn() else {
            return LeasedPlacement::NoLease;
        };
        let head = {
            let held = TXN_LEASES.lock();
            let Some(leases) = held.get(&txn) else {
                return LeasedPlacement::NoLease;
            };
            // The fence. Checked under the lock that ending the leases takes,
            // and the journal claim below is taken under it too: an abort or
            // expiry that revokes after this point finds the claim and waits
            // for it, and any placement after the revocation is refused.
            if leases.is_revoked_in(self.lease_key().0) {
                return LeasedPlacement::Revoked;
            }
            // Newest first: a rotation appended a fresh head, and that is
            // where the write set continues.
            let Some(head) = leases
                .per_chunk
                .get(&self.lease_key())
                .and_then(|heads| {
                    heads
                        .iter()
                        .rev()
                        .find(|lease| lease.seg.segment_class() == segment_class)
                })
                .map(|lease| lease.seg.clone())
            else {
                return LeasedPlacement::NoLease;
            };
            // Ownership is NOT re-taken -- this transaction already holds it.
            // Everything else is exactly the ordinary path: the per-entry
            // reference and journal claim still bracket this write, because
            // sealing still has to see it.
            if !head.incr_references() {
                // Being reclaimed or held exclusively: not full, just not
                // usable right now.
                return LeasedPlacement::Unusable;
            }
            if !head.begin_pending_journal() {
                head.decr_references();
                return LeasedPlacement::Unusable;
            }
            head
        };
        // Keep a minimal COMMIT's worth free behind every entry: the part
        // must be closable by its own record at the decision, whatever the
        // write set does to it now.
        match head.try_acquire_reserving(size, crate::ram::bracket::MIN_COMMIT_ENTRY_SIZE) {
            Some(addr) => {
                crate::ram::entry::stamp_reservation_padding(addr, size);
                LeasedPlacement::Placed(PendingEntry {
                    addr,
                    seg: head,
                    size,
                    skip_sync: true,
                    leased: true,
                    journaled: std::cell::Cell::new(false),
                })
            }
            None => {
                head.end_pending_journal();
                head.decr_references();
                LeasedPlacement::Full
            }
        }
    }

    pub fn try_acquire_in_class(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
    ) -> Result<PendingEntry, WriteError> {
        // Brackets cover BOTH segment classes. A transaction that writes a
        // blob cell and a regular cell into the same chunk holds one head in
        // each pool and opens one bracket chain in each; every lookup below
        // is scoped to the class so the two never reach for each other's
        // head. The COMMIT is written into every part of both chains, and
        // the manifest names them all, so the transaction is decided as a
        // whole -- which is what "outside the bracket" used to cost.
        if current_txn().is_some() {
            return self.acquire_for_transaction(size, full_gc, segment_class);
        }
        self.try_acquire_claim(size, full_gc, segment_class)
    }

    /// Place a transactional entry INSIDE the transaction's bracket, or fail.
    ///
    /// There is no unbracketed fallback. An entry written outside the bracket
    /// is an ordinary entry: it survives a crash that discards the rest of
    /// its transaction, which is a half-applied transaction, and recovery
    /// cannot tell it from any other write. So a write that cannot get a head
    /// of its transaction's own WAITS for one -- bounded -- and then fails
    /// with `TransactionHeadUnavailable`, so the transaction is not
    /// acknowledged.
    ///
    /// The wait cannot deadlock, only time out: what it waits for is another
    /// transaction's decision (or a chunk growing), and nothing here holds
    /// anything that decision needs. It is bounded because the decision may be
    /// slow, and the thread waiting may be one a decision needs to run on.
    fn acquire_for_transaction(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
    ) -> Result<PendingEntry, WriteError> {
        // The common case never waits: try once without.
        match self.place_transactional(size, full_gc, segment_class, false) {
            Err(WriteError::TransactionHeadUnavailable) if self.can_ever_spare_a_head() => {}
            other => return other,
        }
        // Waiting for a head is waiting for ANOTHER transaction's decision,
        // which arrives as an RPC this runtime has to run. A worker thread
        // that sleeps here takes itself away from that, and enough of them
        // starve the very decisions they wait on until every one times out.
        // So the wait leaves the runtime's worker pool first; on a
        // single-threaded runtime it cannot, and the write fails at once.
        use tokio::runtime::{Handle, RuntimeFlavor};
        match Handle::try_current().map(|handle| handle.runtime_flavor()) {
            Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(|| {
                self.place_transactional(size, full_gc, segment_class, true)
            }),
            Ok(_) => {
                TXN_HEAD_UNAVAILABLE.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "chunk {} has no head to spare for transaction {:?}, and waiting for one on \
                     a single-threaded runtime would block the decisions it waits on; failing \
                     the write",
                    self.id,
                    current_txn()
                );
                Err(WriteError::TransactionHeadUnavailable)
            }
            Err(_) => self.place_transactional(size, full_gc, segment_class, true),
        }
    }

    /// One attempt at `acquire_for_transaction`; `may_wait` bounds a wait for
    /// a head, otherwise "no head now" is answered at once (uncounted: the
    /// caller decides whether that becomes a wait or a failure).
    fn place_transactional(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
        may_wait: bool,
    ) -> Result<PendingEntry, WriteError> {
        // A part just opened can still refuse the entry (the head picked
        // could hold BEGIN but not what follows); a few fresh parts settle
        // that, and an entry no fresh part can hold is not going to fit.
        const PART_ATTEMPTS: usize = 3;
        let mut parts_opened = 0usize;
        let mut waiting_since: Option<std::time::Instant> = None;
        let backoff = Backoff::new();
        let mut spins = 0usize;
        loop {
            match self.try_place_in_leased_head(size, segment_class) {
                LeasedPlacement::Placed(entry) => return Ok(entry),
                LeasedPlacement::Revoked => {
                    TXN_STALE_OWNER_WRITES.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        "chunk {} refused a write for transaction {:?}: it was ended while this \
                         thread was still writing for it",
                        self.id,
                        current_txn()
                    );
                    return Err(WriteError::TransactionRevoked);
                }
                // No part here yet, the newest one is out of room, or it is
                // busy: each continues in a new part of the same transaction.
                LeasedPlacement::NoLease | LeasedPlacement::Full | LeasedPlacement::Unusable => {}
            }
            if parts_opened >= PART_ATTEMPTS {
                error!(
                    "chunk {} could not place a {}-byte transactional entry after {} fresh \
                     bracket part(s)",
                    self.id, size, PART_ATTEMPTS
                );
                return Err(WriteError::CannotAllocateSpace);
            }
            if self.can_spare_a_head_for_lease(segment_class) {
                match self.open_transaction_bracket(full_gc, segment_class, size) {
                    Ok(true) => {
                        parts_opened += 1;
                        continue;
                    }
                    Ok(false) => {}
                    // The chunk counted room for another head, but the
                    // allocator had no segment to give it (heads held by
                    // transactions are not reclaimable). That is the same
                    // scarcity as no spare head: wait for a decision.
                    Err(WriteError::CannotAllocateSpace) => {}
                    Err(error) => return Err(error),
                }
            }
            if !self.can_ever_spare_a_head() {
                TXN_HEAD_UNAVAILABLE.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "chunk {} can never give a transaction a head of its own (capacity {} bytes); \
                     refusing the transactional write rather than writing it outside its \
                     bracket",
                    self.id, self.capacity
                );
                return Err(WriteError::TransactionHeadUnavailable);
            }
            if !may_wait {
                return Err(WriteError::TransactionHeadUnavailable);
            }
            let since = waiting_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() >= txn_head_wait() {
                TXN_HEAD_UNAVAILABLE.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "chunk {} had no head to spare for transaction {:?} within {:?}; failing the \
                     write rather than writing it outside its bracket",
                    self.id,
                    current_txn(),
                    txn_head_wait()
                );
                return Err(WriteError::TransactionHeadUnavailable);
            }
            // Spin briefly for a plain writer's microsecond hold; sleep
            // through a transaction's.
            if spins < 64 {
                spins += 1;
                backoff.spin();
            } else {
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        }
    }

    /// Whether this chunk could EVER hold a head for a transaction while
    /// leaving one for everyone else: it needs room for two live heads.
    /// A chunk smaller than that (one
    /// segment is common in tests) refuses transactional writes at once
    /// instead of waiting for something that cannot happen.
    fn can_ever_spare_a_head(&self) -> bool {
        self.capacity >= 2 * SEGMENT_SIZE
    }

    /// Claim space for a transaction's BEGIN and write it, taking the lease.
    /// `first_entry_size` is what the caller is about to write. The part is
    /// claimed with room for BEGIN, that entry AND a minimal COMMIT, because a
    /// head that fits a 24-byte marker need not fit the entry behind it, and
    /// a part that cannot hold its own COMMIT cannot be closed.
    ///
    /// The part being left behind (if any) is NOT sealed with a TXN_CONT
    /// chain link any more. Sealing zeroed the space up to the fixed tail and
    /// so took the room that part's own COMMIT needs: the part then depended
    /// on another part's record, and the cleaner, which compacts parts
    /// independently and drops their markers, could take that record away
    /// and leave the earlier part's committed cells undecided at the next
    /// restart. Every part is a self-contained `BEGIN .. entries .. COMMIT`
    /// bracket instead; recovery still decides a transaction by id across
    /// all its parts, and still reads a TXN_CONT if it finds one.
    fn open_transaction_bracket(
        &self,
        full_gc: bool,
        segment_class: SegmentClass,
        first_entry_size: u32,
    ) -> Result<bool, WriteError> {
        let Some(txn) = current_txn() else {
            return Ok(false);
        };
        let content = crate::ram::bracket::encode_begin(&txn, &current_participants());
        let begin_size = (crate::ram::entry::ENTRY_HEAD_SIZE + content.len()) as u32;
        let commit_reserve = crate::ram::bracket::MIN_COMMIT_ENTRY_SIZE as u32;
        // Claim all three, so the head chosen can hold the entry that follows
        // and still close.
        let mut pending = self.try_acquire_claim(
            begin_size + first_entry_size + commit_reserve,
            full_gc,
            segment_class,
        )?;
        if !pending.leased {
            // The claim could not take a lease -- the head was being
            // reclaimed underneath it, the chunk could no longer spare it, or
            // the transaction was revoked. Without a lease there is no bracket
            // to open. Cancel the reservation before another writer uses the head.
            drop(pending);
            return Ok(false);
        }
        crate::ram::entry::Entry::encode_to(
            pending.addr,
            crate::ram::entry::EntryType::BEGIN,
            content.len() as u32,
            |content_addr| unsafe {
                std::ptr::copy_nonoverlapping(
                    content.as_ptr(),
                    content_addr as *mut u8,
                    content.len(),
                );
            },
        );
        // Hand the rest of the claim back so the caller's entry lands
        // immediately behind the BEGIN. Those bytes were never journaled and
        // the head is leased, so nothing else can have taken them.
        let entry_start = pending.addr + begin_size as usize;
        pending.size = begin_size;
        if !pending.seg.rewind_to(entry_start) {
            warn!(
                "chunk {} could not hand back the tail of a bracket claim in segment {}; the \
                 caller's entry takes fresh space and these bytes become padding",
                self.id, pending.seg.id
            );
        }
        pending.append()?;
        drop(pending);
        Ok(true)
    }

    fn try_acquire_claim(
        &self,
        size: u32,
        full_gc: bool,
        segment_class: SegmentClass,
    ) -> Result<PendingEntry, WriteError> {
        // **A writer must not wait inside its own QSBR section.** The caller
        // holds a `CellGuard`, and with it a section, for the whole write;
        // this function is where a write waits -- for a head to come free,
        // for a GC pass to finish, and for the emergency collection it runs
        // itself -- and a thread inside a section is, to `is_quiesced`, a
        // reader every retirement must wait for. Twelve writers waiting here
        // were `blocking_threads=12` on segments nobody referenced, and the
        // collections they were waiting on freed nothing. Sound to suspend:
        // the guard has already pinned its own segment by reference, and a
        // pinned segment is never freed; the section's lookup-to-pin job is
        // done before allocation starts.
        #[cfg(test)]
        TEST_CAS_HEAD_WAIT.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let _outside_section = crate::ram::qsbr::segment_qsbr().suspend();
        if self.writes_closed.load(Ordering::Acquire) {
            return Err(WriteError::ServerShuttingDown);
        }
        let mut tried_gc = false;
        let mut queueing_since: Option<std::time::Instant> = None;
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
            let mut rotate_target: Option<(&AtomicU64, u64, u64)> = None;
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
                        // Under a transaction this claim becomes a LEASE:
                        // the head stays owned past this entry, so the rest
                        // of the write set lands behind it and nobody else
                        // appends in between.
                        let leased = self.lease_head_for_current_txn(&head, addr);
                        return Ok(PendingEntry {
                            addr,
                            seg: head,
                            size,
                            skip_sync: is_in_transaction(),
                            leased,
                            journaled: std::cell::Cell::new(false),
                        });
                    }
                    // No space in this segment: give everything back and
                    // make THIS slot the rotation target.
                    head.end_pending_journal();
                    head.decr_references();
                    head.release_own();
                    if rotate_target.is_none() {
                        rotate_target = Some((slot, head_seg_id, head.seq_id));
                    }
                    // A later head can still fit this entry. Rotation needs a
                    // new segment; do not refuse at capacity before trying it.
                    continue;
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
            let (head_slot, head_seg_id, head_seq_id) = match rotate_target {
                Some(target) => target,
                None => match empty_slot {
                    Some(slot) => (slot, HEAD_SEG_ID_EMPTY, 0),
                    None => {
                        if self.current_txn_holds_head_here(segment_class) {
                            error!(
                                "chunk {} has no head this write can take, and one of them is \
                                 held by this very transaction: waiting would wait on ourselves. \
                                 Refusing the write instead of hanging.",
                                self.id
                            );
                            return Err(WriteError::CannotAllocateSpace);
                        }
                        let t_spin = std::time::Instant::now();
                        backoff.spin();
                        ALLOC_SPIN_NANOS
                            .fetch_add(t_spin.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        continue;
                    }
                },
            };

            // Where the wall is, in the ALLOCATOR's terms. The writer-path
            // allocator refuses once the segments it can grant are down to
            // the compaction reserve (3 for a 512 MiB chunk), and this loop
            // used to measure the wall separately, in live bytes against
            // capacity minus two segments. For any chunk whose reserve is 3
            // the two never met: the allocator refused at 61 of 64 live
            // segments, the emergency collection below was armed for 62, and
            // a chunk full of reclaimable dead space failed the write with
            // "the chunk is FULL" without one collection attempted. A
            // 13.6M-edge import overflowed one chunk that way while the store
            // as a whole was 12% used.
            //
            // Retired segments (unpublished, readers not yet drained) hold
            // addresses the allocator cannot grant yet. Near the wall, give
            // them back before judging: they are ours.
            let mut headroom = self.allocator.writer_headroom();
            if headroom <= 1 && self.drain_retired_segments() > 0 {
                headroom = self.allocator.writer_headroom();
            }
            // Emergency cleaning arms one segment early: a moving combine
            // needs a free segment as its destination, and hitting the exact
            // wall leaves it nothing to relocate into. Allocation itself is
            // refused only at zero, so usable capacity is unchanged.
            let near_wall = headroom <= 1;
            let at_wall = headroom == 0;

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
            // Any live head is worth queueing on, INCLUDING an owned one.
            //
            // Ownership is normally measured in microseconds, and with a
            // single-segment chunk under concurrency the one head is owned by
            // somebody almost always -- refusing to wait for it turned
            // ordinary contention into CannotAllocateSpace and failed 48
            // concurrent transactions that used to succeed.
            //
            // The wait is BOUNDED instead. A head held for longer than any
            // writer could plausibly hold one is a head held by a transaction
            // lease, and waiting on that can deadlock: the writes a commit
            // waits for need a head in the same chunk. A lease can no longer
            // take the last head (`can_spare_a_head_for_lease`), so this is
            // the second line of defence rather than the first.
            let any_live_head = || {
                slots.iter().any(|slot| {
                    let id = slot.load(Ordering::Acquire);
                    id != HEAD_SEG_ID_EMPTY
                        && id != HEAD_SEG_ID_ALLOCATING
                        && self
                            .segs
                            .get(&(id as usize))
                            .map(|seg| {
                                !seg.bracket_is_open()
                                    && seg.can_still_append()
                                    && (growing
                                        || seg.bound().saturating_sub(
                                            seg.append_header.load(Ordering::Acquire),
                                        ) >= size as usize)
                            })
                            .unwrap_or(false)
                })
            };
            // A full rotation target does not imply a full pool. Other
            // writers can own heads with room. Use the same bounded queue
            // as pool growth instead of refusing while they finish.
            if near_wall && any_live_head() {
                #[cfg(test)]
                TEST_BEFORE_HEAD_QUEUE.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
                if self.current_txn_holds_head_here(segment_class) {
                    error!(
                        "chunk {} is at capacity and the live head(s) this write would queue on \
                         include one held by this very transaction; refusing rather than waiting \
                         on ourselves.",
                        self.id
                    );
                    return Err(WriteError::CannotAllocateSpace);
                }
                if queueing_since
                    .get_or_insert_with(std::time::Instant::now)
                    .elapsed()
                    >= HEAD_QUEUE_DEADLINE
                {
                    error!(
                        "chunk {} waited {:?} for a head and none came free; the live head(s) are \
                         held by something that is not releasing them. Refusing the write rather \
                         than waiting forever.",
                        self.id, HEAD_QUEUE_DEADLINE
                    );
                    HEAD_QUEUE_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                    return Err(WriteError::CannotAllocateSpace);
                }
                backoff.spin();
                continue;
            }
            if near_wall && !tried_gc {
                // The caller can hold a cell-index lock that a running
                // cleaner needs. Waiting for its GC lock creates a lock cycle.
                // Best-effort collection uses try_lock and must stay nonblocking.
                warn!(
                    "Chunk {} near capacity ({} writer segment(s) left), emergency {} GC",
                    self.id,
                    headroom,
                    if full_gc { "full" } else { "best effort" }
                );
                let _ = Cleaner::clean(self, true, full_gc);
                tried_gc = true;
                continue;
            }
            if at_wall {
                if tried_gc {
                    #[cfg(test)]
                    TEST_BEFORE_CAPACITY_REFUSAL.with(|hook| {
                        if let Some(hook) = hook.borrow_mut().take() {
                            hook();
                        }
                    });
                    // Another writer can replace the selected full head while
                    // this writer checks capacity or runs GC. Rescan its new
                    // head before refusing the allocation.
                    if head_slot.load(Ordering::Acquire) != head_seg_id {
                        continue;
                    }
                    if self.wait_for_capacity_progress(full_gc, segment_class, &mut queueing_since)
                    {
                        tried_gc = false;
                        continue;
                    }
                    debug!(
                        "chunk-allocation-failure: chunk={}, live_bytes={}, capacity={}, head_seg_id={}, seg_count={}, full_gc={}, segment_class={:?}",
                        self.id,
                        self.segs.len() * SEGMENT_SIZE,
                        self.capacity,
                        head_slot.load(Ordering::Relaxed),
                        self.segs.len(),
                        full_gc,
                        segment_class
                    );
                    self.log_allocation_exhausted(segment_class);
                    ALLOCATION_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                    return Err(WriteError::CannotAllocateSpace);
                }
                warn!(
                    "No space left for chunk {}, emergency {} GC",
                    self.id,
                    if full_gc { "full" } else { "best effort" }
                );
                let _ = Cleaner::clean(self, true, full_gc);
                tried_gc = true;
                continue;
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
            #[cfg(test)]
            TEST_BEFORE_ROTATION_ALLOCATE.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
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
                // Another writer can consume the last free segment. Restore
                // the slot and retry usable heads, including owned heads.
                if any_live_head() {
                    backoff.spin();
                    continue;
                }
                if self.wait_for_capacity_progress(full_gc, segment_class, &mut queueing_since) {
                    tried_gc = false;
                    continue;
                }
                self.log_allocation_exhausted(segment_class);
                // COUNTED HERE TOO, and that omission cost a day.
                //
                // There are two ways to run out, and they are not the same
                // shape: the chunk hitting its own capacity (above), and the
                // shared allocator having no segment left for a chunk that is
                // nowhere near its capacity (here -- seg_count 17 against a
                // 256 MiB chunk). Only the first was counted, so a store that
                // exhausted the ALLOCATOR reported nothing.
                //
                // What that silence looks like from outside is a durability
                // bug. A full store cannot place index pages, so write-back
                // abandons them, the barrier is never established, no head
                // pointer is published, and the next start reconstructs the
                // tree from its old head with every recent entry gone. That
                // is correct behaviour for a full store and identical to data
                // loss from any distance -- which is exactly why the counter
                // has to fire on BOTH paths.
                ALLOCATION_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
                return Err(WriteError::CannotAllocateSpace);
            };
            let new_seg_id = new_seg.id;

            // Publish new head segment id FIRST
            // This creates a window where the head_seg_id points to a segment not yet in self.segs.
            // Readers of head_seg_id must handle this by retrying.
            new_seg.protection.head.store(true, Ordering::Release);
            if let Some(old) = self.segs.get(&(head_seg_id as usize)) {
                old.protection.seal_pending.store(true, Ordering::Release);
                old.protection.head.store(false, Ordering::Release);
            }
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
                    && self.seal_queue.enqueue(self.id, head_seg_id, head_seq_id);
                if !sealed_in_background {
                    self.finish_rotated_head(head_seg_id, head_seq_id);
                }
            }
        }
    }

    /// A GC snapshot can exclude segments that the sealer or reclaimer still
    /// owns. They are not proof that the store is full. Only callers permitted
    /// to wait for GC may wait here: a best-effort caller can hold a cell word
    /// or a reference needed by that work. Transaction leases cannot wait on
    /// their own heads either. Use the existing head-queue deadline.
    fn wait_for_capacity_progress(
        &self,
        full_gc: bool,
        segment_class: SegmentClass,
        waiting_since: &mut Option<std::time::Instant>,
    ) -> bool {
        if !full_gc || self.current_txn_holds_head_here(segment_class) {
            return false;
        }
        #[cfg(debug_assertions)]
        HELD_CELL_GUARDS.with(|held| {
            debug_assert_eq!(held.get(), 0, "capacity progress caller holds a cell word");
        });
        let pending = self.retired_segment_files.load(Ordering::Acquire) != 0
            || self.gc_lock.is_locked()
            || self.segments().iter().any(|segment| {
                segment.protection.seal_pending.load(Ordering::Acquire)
                    && !segment.protection.transaction_pending()
            });
        if !pending
            || waiting_since
                .get_or_insert_with(std::time::Instant::now)
                .elapsed()
                >= HEAD_QUEUE_DEADLINE
        {
            return false;
        }
        // Release CPU time so the work that makes capacity available can run.
        std::thread::sleep(std::time::Duration::from_millis(1));
        true
    }

    /// Drain, sync, archive and seal a segment that has just stopped being the
    /// head. Runs inline from rotation (under backpressure or the
    /// NEB_SYNC_SEAL_ARCHIVE toggle) or on the background sealer; the sequence
    /// is identical either way.
    pub(crate) fn finish_rotated_head(&self, head_seg_id: u64, head_seq_id: u64) {
        let lookup = crate::ram::qsbr::QsbrSection::new();
        if let Some(old_head) = self.segs.get(&(head_seg_id as usize)) {
            // Reject an obsolete physical-slot generation before touching protection.
            if old_head.seq_id != head_seq_id {
                return;
            }
            let Some(_job) = old_head.protection.job() else {
                self.seal_queue.retry(self.id, head_seg_id, head_seq_id);
                return;
            };
            if self.is_active_head(old_head.id) || old_head.is_head_owned() {
                self.seal_queue.retry(self.id, head_seg_id, head_seq_id);
                return;
            }
            old_head.protection.head.store(false, Ordering::Release);
            old_head
                .protection
                .seal_pending
                .store(true, Ordering::Release);
            // Keep the lookup protected until the exact generation is pinned.
            // A reference snapshot alone permits reclamation during the archive.
            let t_drain = std::time::Instant::now();
            let drain = Backoff::new();
            let mut exclusive = crate::ram::segs::SegmentExclusiveRefGuard::new(&old_head);
            for _ in 0..512 {
                if exclusive.is_some() {
                    break;
                }
                drain.spin();
                std::thread::yield_now();
                exclusive = crate::ram::segs::SegmentExclusiveRefGuard::new(&old_head);
            }
            ROTATE_DRAIN_NANOS.fetch_add(t_drain.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let Some(_exclusive) = exclusive else {
                self.seal_queue.retry(self.id, head_seg_id, head_seq_id);
                return;
            };
            drop(lookup);
            if let Err(e) = old_head.force_wal_sync() {
                warn!(
                    "Failed to sync WAL for old head segment {}: {}",
                    head_seg_id, e
                );
            }
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
            old_head
                .protection
                .seal_pending
                .store(true, Ordering::Release);
            drop(_job);
            let archive_result = old_head.archive_rotated();
            ROTATE_ARCHIVE_NANOS
                .fetch_add(t_archive.elapsed().as_nanos() as u64, Ordering::Relaxed);
            match archive_result {
                Ok(crate::ram::segs::ArchiveOutcome::Refused) => {
                    self.seal_queue.retry(self.id, head_seg_id, head_seq_id);
                }
                Ok(_) => {
                    old_head
                        .protection
                        .seal_pending
                        .store(false, Ordering::Release);
                }
                Err(e) => {
                    self.seal_queue.retry(self.id, head_seg_id, head_seq_id);
                    error!(
                        "SEAL ARCHIVE FAILED for segment {} (chunk {}): {}. It stays dirty \
                         and resident; its only durable copy is its WAL.",
                        head_seg_id, self.id, e
                    );
                }
            }
        }
    }

    #[cfg(test)]
    /// Both heads, with `None` for "no head yet". An empty regular head is
    /// an ordinary state now, not just a blob one: recovery leaves both
    /// empty, and the first write of each class allocates a fresh segment.
    pub fn head_seg_ids_for_test(&self) -> (Option<u64>, Option<u64>) {
        let head_or_none =
            |id: u64| (id != HEAD_SEG_ID_EMPTY && id != HEAD_SEG_ID_ALLOCATING).then_some(id);
        let first_blob = self
            .blob_head_pool
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .find(|id| *id != HEAD_SEG_ID_EMPTY && *id != HEAD_SEG_ID_ALLOCATING);
        (head_or_none(self.get_head_seg_id()), first_blob)
    }

    #[cfg(test)]
    pub(crate) fn release_append_heads_for_test(&self) -> Vec<u64> {
        let mut heads = Vec::new();
        for slot in self.head_pool.iter().chain(self.blob_head_pool.iter()) {
            let lookup = crate::ram::qsbr::QsbrSection::new();
            let old = slot.swap(HEAD_SEG_ID_EMPTY, Ordering::AcqRel);
            if old != HEAD_SEG_ID_EMPTY && old != HEAD_SEG_ID_ALLOCATING {
                if let Some(segment) = self.segs.get(&(old as usize)) {
                    segment.protection.head.store(false, Ordering::Release);
                }
                heads.push(old);
            }
        }
        self.wait_for_segment_jobs_for_test();
        heads
    }

    #[cfg(test)]
    pub(crate) fn rotate_heads_for_scrub_test(&self) {
        for old in self.release_append_heads_for_test() {
            let sequence = self.segs.get(&(old as usize)).unwrap().seq_id;
            self.finish_rotated_head(old, sequence);
        }
        self.wait_for_segment_jobs_for_test();
    }

    #[cfg(test)]
    pub(crate) fn wait_for_segment_jobs_for_test(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while self.segments().iter().any(|segment| {
            segment.wal_sync_queued.load(Ordering::Acquire)
                || segment.protection.seal_pending.load(Ordering::Acquire)
                || !segment.protection.jobs_drained()
        }) {
            assert!(
                std::time::Instant::now() < deadline,
                "segment jobs did not drain"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
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
            segment.protection.head.store(false, Ordering::Release);
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
        #[cfg(test)]
        TEST_BEFORE_WORD.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
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

    #[cfg(test)]
    pub(crate) fn deleted_version_for_test(&self, hash: u64) -> u64 {
        self.deleted_versions.version(hash)
    }

    pub(crate) fn note_deleted_version(&self, hash: u64, version: u64) {
        self.deleted_versions.note(hash, version);
    }

    fn create_version(&self, cell: &OwnedCell) -> u64 {
        cell.header
            .version
            .max(self.deleted_versions.version(cell.header.id.bits()))
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

    /// Whether this write must be refused because shutdown has begun.
    ///
    /// The index's own schemas are exempt: the flush that runs during
    /// shutdown writes its pages and its tree metadata as cells in these very
    /// chunks, and refusing those is what killed the ranged index the last
    /// time writes were closed early. Everything else is a client write, and
    /// a client write acked after that flush loses its index entries -- the
    /// cell is archived at step 1.6 and survives, its entries are not.
    fn shutdown_refuses(&self, schema_id: SchemaVid) -> Result<(), WriteError> {
        if !self.client_writes_closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if schema_id == *crate::index::ranged::tree::btree::PAGE_SCHEMA_ID
            || schema_id == *crate::index::ranged::tree::tree::RANGED_TREE_SCHEMA_ID
        {
            return Ok(());
        }
        Err(WriteError::ServerShuttingDown)
    }

    fn write_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        self.shutdown_refuses(cell.header.schema)?;
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
            self.write_cell_to_chunk(cell, &write_plan, &pending_entry, self.create_version(cell))?;
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
        let insert = self
            .cell_index
            .try_insert_locked(cell.header.id.bits() as usize);
        WRITE_INDEX_NANOS.fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
        match insert {
            Some(mut guard) => {
                #[cfg(debug_assertions)]
                self.assert_address_aligned_for_write(
                    cell_loc,
                    "write_cell",
                    cell.header.id.bits(),
                );

                if let Err(error) = pending_entry.append() {
                    guard.remove();
                    return Err(error);
                }
                *guard = cell_loc;
                drop(guard);
                self.slot_bytes
                    .add(cell.header.id.bits(), write_result.content_length);
                let t_sec = std::time::Instant::now();
                self.ensure_indices(cell, None, &*write_plan.schema);
                WRITE_SECONDARY_NANOS
                    .fetch_add(t_sec.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let t_stats = std::time::Instant::now();
                self.refresh_statistics_for_schema(write_plan.schema.vid);
                WRITE_STATS_NANOS.fetch_add(t_stats.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            None => {
                // The cell image is already appended; make sure recovery can
                // never resurrect it over the write that won the race.
                abandon_entry(pending_entry.addr);
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
                            if let Some(schema) = self.meta.schemas.get(&existing.header.schema) {
                                let old_indices = Some(probe_cell_indices(&existing, &*schema));
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
        cell.header.schema = write_result.new_schema;
        Ok(cell.header)
    }

    fn update_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        self.shutdown_refuses(cell.header.schema)?;
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
                Entry::decode_from(cell_location, |_, _| {})
                    .0
                    .content_length
            } else {
                0
            };
            pending_entry.append()?;
            cell_guard.set_ptr(new_cell_loc);
            drop(cell_guard);
            self.slot_bytes.add(hash, write_result.content_length);
            self.slot_bytes.sub(hash, old_content_length);
            self.ensure_indices_with_res(cell, old_indices, schema);
            self.mark_dead_entry_with_cell(cell_location, cell);
            self.refresh_statistics_for_schema(schema.vid);
            drop(write_plan);
            cell.header.version = write_result.new_version;
            cell.header.timestamp = write_result.new_timestamp;
            cell.header.schema = write_result.new_schema;
        } else {
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
        let schema_id = plans[0].schema.vid;

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
                let mut guard = self.lock_or_insert_cell(hash);
                WRITE_INDEX_NANOS.fetch_add(t_index.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let loc = guard.get_ptr();
                let (old_version, old_loc) = if loc == 0 {
                    (self.create_version(&cells[j]), None)
                } else {
                    (cell_version_from_chunk_raw(loc).unwrap_or(0), Some(loc))
                };

                let entry_addr = addr;
                addr += sizes[j] as usize;
                let t_copy = std::time::Instant::now();
                let encoded = cells[j].write_to_addr(&plans[j], entry_addr, old_version);
                WRITE_COPY_NANOS.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let write_result = match encoded {
                    Ok(result) => result,
                    Err(error) => {
                        results.extend((j..cells.len()).map(|_| Err(error.clone())));
                        return results;
                    }
                };
                if let Err(error) = run.journal(entry_addr, sizes[j]) {
                    if old_loc.is_none() {
                        guard.remove_cell();
                    }
                    let error = WriteError::from_wal_io(error);
                    results.push(Err(error));
                    results.extend((j + 1..cells.len()).map(|_| Err(WriteError::BatchAborted)));
                    return results;
                }

                let old_indices = if old_loc.is_some() {
                    guard.old_index_res(&*plans[j].schema).ok().flatten()
                } else {
                    None
                };
                guard.set_ptr(write_result.addr);
                drop(guard);
                self.slot_bytes.add(hash, write_result.content_length);
                if let Some(loc) = old_loc.filter(|loc| *loc != 0) {
                    let old_len = Entry::decode_from(loc, |_, _| {}).0.content_length;
                    self.slot_bytes.sub(hash, old_len);
                    self.ensure_indices_with_res(&cells[j], old_indices, &*plans[j].schema);
                    self.mark_dead_entry_with_cell(loc, &cells[j]);
                } else {
                    self.ensure_indices(&cells[j], None, &*plans[j].schema);
                }

                let mut header = cells[j].header;
                header.version = write_result.new_version;
                header.timestamp = write_result.new_timestamp;
                header.schema = write_result.new_schema;
                results.push(Ok(header));
                WRITE_CELLS.fetch_add(1, Ordering::Relaxed);
            }
            i += covered;
            // Drop releases the run after all accepted frames reached the WAL.
        }
        self.refresh_statistics_for_schema(schema_id);
        results
    }

    pub fn upsert_cell(&self, cell: &mut OwnedCell) -> Result<CellHeader, WriteError> {
        self.shutdown_refuses(cell.header.schema)?;
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
                    Entry::decode_from(cell_location, |_, _| {})
                        .0
                        .content_length
                } else {
                    0
                };
                pending_entry.append()?;
                cell_guard.set_ptr(new_cell_loc);
                drop(cell_guard);
                self.slot_bytes.add(hash, write_result.content_length);
                self.slot_bytes.sub(hash, old_content_length);
                self.ensure_indices_with_res(cell, old_indices, &*write_plan.schema);
                self.mark_dead_entry_with_cell(cell_location, cell);
                self.refresh_statistics_for_schema(write_plan.schema.vid);
                drop(write_plan);
                cell.header.version = write_result.new_version;
                cell.header.timestamp = write_result.new_timestamp;
                cell.header.schema = write_result.new_schema;
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
                        self.create_version(cell),
                    )?;
                    WRITE_COPY_NANOS
                        .fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    let new_cell_loc = write_result.addr;
                    #[cfg(debug_assertions)]
                    self.assert_address_aligned_for_write(
                        new_cell_loc,
                        "upsert_cell(insert)",
                        hash,
                    );

                    if let Err(error) = pending_entry.append() {
                        guard.remove();
                        return Err(error);
                    }
                    *guard = new_cell_loc;
                    drop(guard);
                    self.slot_bytes.add(hash, write_result.content_length);
                    let t_sec = std::time::Instant::now();
                    self.ensure_indices(cell, None, &*write_plan.schema);
                    WRITE_SECONDARY_NANOS
                        .fetch_add(t_sec.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    let t_stats = std::time::Instant::now();
                    self.refresh_statistics_for_schema(write_plan.schema.vid);
                    WRITE_STATS_NANOS
                        .fetch_add(t_stats.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    WRITE_CELLS.fetch_add(1, Ordering::Relaxed);
                    drop(write_plan);
                    cell.header.version = write_result.new_version;
                    cell.header.timestamp = write_result.new_timestamp;
                    cell.header.schema = write_result.new_schema;
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
        self.update_cell_by_inner(hash, update, true)
    }

    fn update_cell_by_inner<U>(
        &self,
        hash: u64,
        update: U,
        retire_old: bool,
    ) -> Result<OwnedCell, WriteError>
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

                        pending_entry.append()?;
                        **cell_guard.word_mutex_guard() = new_cell_loc;
                        self.slot_bytes.add(hash, write_result.content_length);
                        self.slot_bytes.sub(hash, old_entry_size.unwrap_or(0));
                        if let Some(indexer) = &self.index_builder {
                            indexer.ensure_indices(&new_cell, &*schema, old_indices);
                        }

                        // Mark old entry as dead using size we captured earlier
                        // This avoids decoding old_loc after lock is released (race condition)
                        if retire_old {
                            if let Some(size) = old_entry_size {
                                let seg = self.locate_segment_ensured(old_loc, &new_cell.id());
                                self.mark_dead_entry_with_size(old_loc, size, &seg);
                            }
                        }

                        self.refresh_statistics_for_schema(write_plan.schema.vid);
                        drop(write_plan);
                        new_cell.header.version = write_result.new_version;
                        new_cell.header.timestamp = write_result.new_timestamp;
                        new_cell.header.schema = write_result.new_schema;
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

    // Deletes reserve before reacquiring the cell word, as do ordinary writes,
    // upserts and CAS. update_cell_by and CellGuard updates still hold the word
    // first. Near capacity, their best-effort head queue can conflict with a
    // delete holding that head; it refuses at HEAD_QUEUE_DEADLINE, releasing the
    // word so the delete can finish. These paths must not wait for full GC.
    fn reserve_tombstone_for_remove(
        &self,
        hash: u64,
        predict: Option<&dyn Fn(&SharedCell) -> bool>,
    ) -> Result<PendingEntry, WriteError> {
        // Preserve the absent-cell result even when the store has no capacity.
        let guard = CellGuard::for_read(hash, self).map_err(|error| match error {
            ReadError::CellDoesNotExisted | ReadError::CellIdIsUnitId => {
                WriteError::CellDoesNotExisted
            }
            other => WriteError::ReadError(other),
        })?;
        // Reject a false predicate even at a full store. The caller checks it
        // again under the second guard because the cell can change meanwhile.
        let guard = if let Some(predict) = predict {
            let (cell, _) = SharedCell::from_chunk_raw(hash, guard, self)
                .map_err(WriteError::ReadError)?;
            if !predict(&cell) {
                return Err(WriteError::CellDoesNotExisted);
            }
            cell.into_cell_guard()
        } else {
            guard
        };
        #[cfg(test)]
        TEST_DELETE_BEFORE_RESERVE.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        drop(guard);
        // Preserve best-effort collection for transaction apply callers.
        let pending = self.acquire_tombstone(false)?;
        #[cfg(test)]
        TEST_DELETE_AFTER_RESERVE.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        Ok(pending)
    }

    fn remove_cell(&self, hash: u64) -> Result<(), WriteError> {
        let pending_entry = self.reserve_tombstone_for_remove(hash, None)?;
        // Use location_for_read to ensure promotion happens if segment is cold
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = guard.get_ptr();

        let removed_len =
            self.put_tombstone_by_cell_loc_inner(cell_location, true, &pending_entry)?;
        drop(pending_entry);

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
        let pending_entry = self.reserve_tombstone_for_remove(hash, None)?;
        let guard = match CellGuard::for_read(hash, self) {
            Ok(guard) => guard,
            Err(ReadError::CellDoesNotExisted) => return Err(WriteError::CellDoesNotExisted),
            Err(ReadError::CellIdIsUnitId) => return Err(WriteError::CellDoesNotExisted),
            Err(e) => return Err(WriteError::ReadError(e)),
        };
        let cell_location = guard.get_ptr();
        let removed_len =
            self.put_tombstone_by_cell_loc_inner(cell_location, true, &pending_entry)?;
        drop(pending_entry);
        guard.remove_cell();
        self.slot_bytes.sub(hash, removed_len);
        Ok(())
    }

    fn remove_cell_by<P>(&self, hash: u64, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        self.remove_cell_by_inner(hash, predict, true)
    }

    fn remove_cell_by_inner<P>(
        &self,
        hash: u64,
        predict: P,
        retire_old: bool,
    ) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let pending_entry = self.reserve_tombstone_for_remove(hash, Some(&predict))?;
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
                    let removed_len = self.put_tombstone_by_cell_loc_inner(
                        cell_location,
                        retire_old,
                        &pending_entry,
                    )?;
                    drop(pending_entry);
                    self.remove_indices(&cell, &schema);
                    cell.into_cell_guard().remove_cell();
                    self.slot_bytes.sub(hash, removed_len);
                    return Ok(());
                }
                Err(WriteError::CellDoesNotExisted)
            }
            Err(e) => Err(WriteError::ReadError(e)),
        }
    }

    /// Restore the index before the participant pads its leased spans.
    /// The participant keeps the previous segment referenced until this returns.
    fn restore_cell_location(
        &self,
        hash: u64,
        previous: usize,
        current_version: u64,
    ) -> Result<(), WriteError> {
        let mut guard = self.lock_or_insert_cell(hash);
        let current = guard.get_ptr();
        if (current_version == 0 && current != 0)
            || (current_version != 0
                && (current == 0
                    || guard.cell_version().map_err(WriteError::ReadError)? != current_version))
        {
            if current == 0 {
                guard.remove_cell();
            }
            return Err(WriteError::CellVersionMismatch);
        }
        let current_size = if current == 0 {
            0
        } else {
            Entry::decode_from(current, |_, _| {}).0.content_length
        };
        let previous_size = if previous == 0 {
            0
        } else {
            Entry::decode_from(previous, |_, _| {}).0.content_length
        };
        let mut schemas = [None, None];
        let mut current_indices = Vec::new();
        let mut current_scannable_key = None;
        if current != 0 {
            schemas[0] = Some(
                header_from_chunk_raw(current)
                    .map_err(WriteError::ReadError)?
                    .0
                    .schema,
            );
            if self.index_builder.is_some() {
                let (cell, schema) =
                    SharedCell::from_chunk_raw(hash, guard, self).map_err(WriteError::ReadError)?;
                if previous == 0 {
                    self.remove_indices(&cell, &schema);
                } else {
                    current_indices = probe_cell_indices(&cell, &schema);
                    if schema.is_scannable {
                        current_scannable_key = Some(crate::index::EntryKey::for_scannable(
                            &cell.id(),
                            schema.uid,
                        ));
                    }
                }
                guard = cell.into_cell_guard();
            }
        }
        if previous != 0 {
            schemas[1] = Some(
                header_from_chunk_raw(previous)
                    .map_err(WriteError::ReadError)?
                    .0
                    .schema,
            );
            if let Some(indexer) = &self.index_builder {
                let (cell, schema) = SharedCellData::from_chunk_raw(hash, previous, self)
                    .map_err(WriteError::ReadError)?;
                indexer.restore_indices(
                    &cell.to_owned(),
                    &schema,
                    current_indices,
                    current_scannable_key,
                );
            }
        }
        if previous == 0 {
            guard.remove_cell();
        } else {
            guard.set_ptr(previous);
        }
        self.slot_bytes.sub(hash, current_size);
        self.slot_bytes.add(hash, previous_size);
        for schema in schemas.into_iter().flatten() {
            self.refresh_statistics_for_schema(schema);
        }
        Ok(())
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
        let Some(segment) = self.segs.get(&(segment_id as usize)) else {
            return;
        };
        if !segment.protection.claim_cleaner() {
            return;
        }
        self.remove_segment_for_cleaner(segment_id);
        segment.protection.release_cleaner();
    }

    pub(crate) fn remove_segment_for_cleaner(&self, segment_id: u64) {
        let Some(segment) = self.segs.get(&(segment_id as usize)) else {
            return;
        };
        if self.is_active_head(segment_id)
            || segment.protection.in_progress()
            || segment.is_head_owned()
            || segment.pending_journal_count() != 0
            || segment.wal_sync_queued.load(Ordering::Acquire)
        {
            return;
        }
        segment.protection.retire();
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
        self.retired_segment_files.fetch_add(1, Ordering::AcqRel);
        if let Some(seg) = self.segs.remove(&(segment_id as usize)) {
            let mut deleted_ids = std::collections::HashMap::<u64, usize>::new();
            for entry in seg.entry_iter() {
                if entry.entry_header.entry_type == EntryType::TOMBSTONE {
                    let tombstone = Tombstone::read_from_entry_content_addr(entry.body_pos);
                    *deleted_ids.entry(tombstone.id.bits()).or_default() += 1;
                }
            }
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
                deleted_ids: deleted_ids.into_iter().collect(),
            });
        } else {
            self.retired_segment_files.fetch_sub(1, Ordering::AcqRel);
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
            if quiesced && unreferenced && entry.segment.protection.jobs_drained() {
                entry.segment.free_memory();
                entry.segment.dispense();
                for (hash, count) in entry.deleted_ids {
                    self.deleted_versions.reclaim(hash, count);
                }
                self.retired_segment_files.fetch_sub(1, Ordering::AcqRel);
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
    /// The accounting behind a refused allocation, on one line: where every
    /// segment address of this chunk is, and WHICH kind of "no space" this is.
    /// A chunk that is simply full, with its compaction reserve intact, is the
    /// design working; a chunk whose addresses have gone missing is a bug.
    /// Both used to print "the allocator has no segment left after GC", and
    /// reading a full store as a durability failure cost a day.
    fn log_allocation_exhausted(&self, segment_class: SegmentClass) {
        let (cap_segs, free_segs, unbumped) = self.allocator.segment_accounting();
        let reserve = self.allocator.compaction_reserve();
        let retired = self.retired_segment_count();
        let cause = if free_segs + unbumped <= reserve && free_segs + unbumped > 0 {
            // Not "give the store more room": capacity is per CHUNK
            // (chunk_size over the segment size; db_size only sets how many
            // chunks there are), and a cell routes to its chunk by its id's
            // locality bits. A store that is mostly empty can still fill one
            // chunk when a few localities take far more than their share, so
            // the first question is how this chunk's live count compares
            // with the others, not how big the store is.
            "the chunk is FULL and its remaining segments are the compaction reserve. \
             Capacity is per chunk and cells route by locality, so this is either a store \
             that is too small or one chunk taking far more than its share: compare this \
             chunk's live count with the others before adding db_size"
        } else if cap_segs > self.segs.len() + free_segs + unbumped + retired {
            "addresses are UNACCOUNTED FOR -- neither live, free, retired nor unbumped. \
             That is a leak, not a full store"
        } else {
            "the chunk is genuinely out of segments"
        };
        error!(
            "chunk-allocation-failure: chunk={}, segment_class={:?}, live={}, \
             capacity_segments={}, free_list={}, never_bumped={}, retired_pending={}, \
             unaccounted={}, returned_ever={}, reserve={}: {}",
            self.id,
            segment_class,
            self.segs.len(),
            cap_segs,
            free_segs,
            unbumped,
            retired,
            cap_segs
                .saturating_sub(self.segs.len())
                .saturating_sub(free_segs)
                .saturating_sub(unbumped)
                .saturating_sub(retired),
            self.allocator.segments_returned(),
            reserve,
            cause,
        );
    }

    pub fn retired_segment_count(&self) -> usize {
        self.retired_segments.lock().len()
    }

    pub fn locate_segment(&self, addr: usize) -> Option<AArc<Segment>> {
        // An address outside this chunk's range names no segment. Zero is the
        // common case -- it is how the cell index spells "no cell here" -- and
        // it used to underflow the id arithmetic rather than answer.
        let seg_id = self.allocator.try_id_by_addr(addr)?;
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

    fn acquire_tombstone(&self, full_gc: bool) -> Result<PendingEntry, WriteError> {
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
            match self.try_acquire(TOMBSTONE_ENTRY_SIZE as u32, full_gc) {
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
                "Chunk {} could not place a tombstone after {} attempts: {:?}. The \
                 delete is REFUSED rather than retried forever, so the caller learns it did \
                 not happen.",
                self.id, TOMBSTONE_ACQUIRE_ATTEMPTS, last_error
            );
            return Err(last_error);
        };
        Ok(pending_entry)
    }

    fn put_tombstone(
        &self,
        cell_header: &CellHeader,
        cell_seg: &AArc<Segment>,
        record_deleted_version: bool,
        pending_entry: &PendingEntry,
    ) -> Result<(), WriteError> {
        Tombstone::put(
            pending_entry.addr,
            cell_seg.seq_id,
            cell_header.version,
            cell_header.id,
        );
        pending_entry.append()?;
        if record_deleted_version {
            self.note_deleted_version(cell_header.id.bits(), cell_header.version);
        }
        pending_entry.seg.tombstones.fetch_add(1, Ordering::Relaxed);
        pending_entry.seg.note_dead_bytes_change();
        Ok(())
    }

    /// Returns the removed entry's content length, so the logical remove
    /// paths can settle the slot counter without a second decode.
    pub fn put_tombstone_by_cell_loc(&self, cell_location: usize) -> Result<u32, WriteError> {
        let pending_entry = self.acquire_tombstone(true)?;
        self.put_tombstone_by_cell_loc_inner(cell_location, true, &pending_entry)
    }

    fn put_tombstone_by_cell_loc_inner(
        &self,
        cell_location: usize,
        retire_old: bool,
        pending_entry: &PendingEntry,
    ) -> Result<u32, WriteError> {
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
        self.put_tombstone(&header, &cell_seg, retire_old, pending_entry)?;
        if retire_old {
            self.mark_dead_entry_with_size(cell_location, entry_size, &cell_seg);
        }
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
        DEAD_BYTES_PRODUCED.fetch_add(size as u64, Ordering::Relaxed);
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
                let fill_x8 =
                    (self.segs.len() * SEGMENT_SIZE).saturating_mul(8) / self.capacity.max(1);
                let dead_bar = if fill_x8 >= 6 {
                    DEAD_RATE_FOR_COMBINE_CLEANER
                } else {
                    DEAD_RATE_FOR_COMBINE_CLEANER_RELAXED
                };
                *utilization < 1.0
                    && (full || *utilization < dead_bar)
                    && !self.is_active_head(seg.id)
                    && !seg.protection.in_progress()
                    && !seg.is_head_owned()
                    && !seg.wal_sync_queued.load(Ordering::Acquire)
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
                let entry_size = entry_meta.entry_size;
                let entry_header = entry_meta.entry_header;
                trace!("Iterating live entries on chunk {} segment {}. Got {:?} at {} size {}",
                       chunk_id, seg.id, entry_header.entry_type, entry_meta.entry_pos, entry_size);
                // Validate entry type is a known valid type (CELL or TOMBSTONE)
                // Invalid types indicate we're reading garbage (possibly from inside another entry)
                // which can happen if append_header was set incorrectly by a previous operation
                debug_assert!(matches!(
                    entry_header.entry_type,
                    EntryType::CELL
                        | EntryType::TOMBSTONE
                        | EntryType::PADDING
                        | EntryType::BEGIN
                        | EntryType::COMMIT
                        | EntryType::TXN_CONT
                ));

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
                        // Combine decides retention after it checks every other segment.
                        return Some(Entry {
                            meta: entry_meta,
                            content: EntryContent::Tombstone(tombstone)
                        });
                } else if matches!(
                    entry_header.entry_type,
                    EntryType::BEGIN | EntryType::COMMIT | EntryType::TXN_CONT
                ) {
                    // Bracket markers are never live, and DROPPING them is
                    // the correct thing for a compaction to do rather than an
                    // omission.
                    //
                    // A compacted segment no longer contains the bracket: the
                    // cells that survive are the ones the index still points
                    // at, which are exactly the committed ones, and they come
                    // out the far side as ordinary entries that recovery
                    // applies unconditionally. Carrying a BEGIN across would
                    // be worse than useless -- it would make whatever cells
                    // follow it in the new segment look like they belonged to
                    // a transaction that has no COMMIT there.
                    //
                    // This is safe only because an UNDECIDED bracket cannot
                    // be compacted at all: its segment is still leased by the
                    // transaction, and the cleaner skips leased heads (they
                    // are active heads and they hold a reference). The lease
                    // is what gates the cleaner, structurally, rather than a
                    // check that could be forgotten.
                    trace!(
                        "Entry at {} is a bracket marker; dropped by compaction",
                        entry_meta.entry_pos
                    );
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

    /// The ids of every live cell this chunk holds, ascending.
    ///
    /// `cell_index` is the authoritative id -> address map: a cell exists if
    /// and only if it is in here with a non-zero address (a zero address is a
    /// slot reserved by a writer that has not published its cell yet, exactly
    /// the entries `cell_ids_in_slots` skips). Unlike a ranged-index scan this
    /// does not depend on any secondary structure being complete.
    ///
    /// Memory: `WordMap::entries()` materialises the chunk's `(key, address)`
    /// pairs, 16 bytes per cell, before they are reduced to 8-byte ids. A
    /// caller pages ONE chunk at a time (`Chunks::live_cell_ids_of_chunk`), so
    /// the transient peak is `16 * cells_in_the_largest_chunk`, plus
    /// `8 * cells_in_that_chunk` retained while it is paged out -- never a
    /// function of the whole store.
    pub fn live_cell_ids(&self) -> Vec<u64> {
        let entries = self.cell_index.entries();
        let mut ids: Vec<u64> = Vec::with_capacity(entries.len());
        ids.extend(
            entries
                .iter()
                .filter(|(_, address)| *address != 0)
                .map(|(key, _)| *key as u64),
        );
        drop(entries);
        ids.sort_unstable();
        ids
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

    /// The caller must hold no other cell word. Allocation can wait for the
    /// cleaner, which acquires cell words while it holds the GC lock.
    pub fn compare_version_and_update_cell(
        &self,
        hash: u64,
        version: u64,
        cell: &mut OwnedCell,
    ) -> Result<CellHeader, WriteError> {
        #[cfg(debug_assertions)]
        HELD_CELL_GUARDS
            .with(|held| debug_assert_eq!(held.get(), 0, "CAS caller holds a cell word"));
        // Match ordinary upserts: claim a head before taking the cell word.
        // The expected version is checked after allocation, under that word.
        cell.header.version = version;
        let write_plan = cell.plan_write(self)?;
        // No cell word is held here, so a concurrent cleaner can finish
        // before we decide whether the chunk has space.
        struct CasAllocation;
        impl Drop for CasAllocation {
            fn drop(&mut self) {
                CAS_ALLOCATING.with(|active| active.set(false));
            }
        }
        CAS_ALLOCATING.with(|active| active.set(true));
        let allocation = CasAllocation;
        let pending_entry = write_plan.allocate(self, true)?;
        drop(allocation);
        let mut guard = self
            .lock_cell_for_write(hash, true)
            .map_err(WriteError::ReadError)?;
        if guard.cell_version().map_err(WriteError::ReadError)? != version {
            return Err(WriteError::CellVersionMismatch);
        }
        let result = guard.update_cell_in_entry(cell, &write_plan, &pending_entry, version)?;
        drop(write_plan);
        cell.header.version = result.new_version;
        cell.header.timestamp = result.new_timestamp;
        cell.header.schema = result.new_schema;
        Ok(cell.header)
    }

    /// The caller must hold no cell word; see `compare_version_and_update_cell`.
    pub fn compare_version_and_set_field(
        &self,
        hash: u64,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> Result<CellHeader, WriteError> {
        #[cfg(debug_assertions)]
        HELD_CELL_GUARDS
            .with(|held| debug_assert_eq!(held.get(), 0, "CAS caller holds a cell word"));
        let mut cell = {
            let mut guard = self
                .lock_cell_for_write(hash, true)
                .map_err(WriteError::ReadError)?;
            guard.read_cell_owned().map_err(WriteError::ReadError)?
        };
        if cell.header.version != version {
            return Err(WriteError::CellVersionMismatch);
        }
        cell.data[field] = value;
        self.compare_version_and_update_cell(hash, version, &mut cell)
    }
}

/// A contiguous span of a segment claimed for a run of entries.
///
/// The batched form of [`PendingEntry`]. A run holds one segment reference
/// and one reservation. Each accepted cell has its own WAL frame.
/// The caller appends each entry before it publishes the cell pointer.
/// Drop cancels the unused suffix before it releases the head.
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
            crate::ram::entry::stamp_reservation_padding(from, self.size - self.consumed.get());
            if self.seg.can_still_append() {
                self.seg.rewind_to(from);
            }
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
    /// This entry went into a head its TRANSACTION owns, so finishing the
    /// entry must not hand the head back: the lease runs to the 2PC
    /// decision. Releasing here would let another writer append behind the
    /// transaction, which is exactly what makes an abort unable to rewind.
    pub leased: bool,
    journaled: std::cell::Cell<bool>,
}

impl PendingEntry {
    /// Wait for the append result before the caller publishes the entry.
    /// A future durable wait can extend this step.
    pub fn append(&self) -> Result<(), WriteError> {
        assert!(!self.journaled.get(), "an entry is appended only once");
        match self.seg.write_wal(self.addr, self.size, self.skip_sync) {
            Ok(()) => {
                self.journaled.set(true);
                if self.skip_sync {
                    note_transactional_write(&self.seg);
                } else {
                    crate::ram::segs::queue_wal_sync(&self.seg);
                }
                self.seg.set_dirty();
                Ok(())
            }
            Err(error) => {
                WAL_JOURNAL_FAILURES.fetch_add(1, Ordering::Relaxed);
                Err(WriteError::from_wal_io(error))
            }
        }
    }
}

impl Drop for PendingEntry {
    fn drop(&mut self) {
        // An unsubmitted reservation has no WAL record and no published cell.
        // The owner still holds the head, so cancellation can restore its cursor.
        if !self.journaled.get() {
            crate::ram::entry::stamp_reservation_padding(self.addr, self.size);
            if self.seg.can_still_append() {
                self.seg.rewind_to(self.addr);
            }
        }
        self.seg.end_pending_journal();
        if !self.leased {
            self.seg.release_own();
        }
        self.seg.decr_references();
    }
}

/// What a store durably knows about a transaction. See
/// [`Chunks::durable_txn_status`].
///
/// `Aborted` merges "I aborted it" with "I never saw it", deliberately: both
/// license the same action, and neither can be a committed transaction in
/// disguise. What it must NEVER merge with is a committed one whose records
/// were compacted -- see the method's doc for why that cannot happen.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DurableTxnStatus {
    Committed,
    Prepared,
    Aborted,
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
    /// What recovery learned about in-flight transactions, until the store
    /// can ask its peers about them. Empty on a fresh store and after
    /// resolution.
    pub recovered_brackets: Mutex<crate::ram::recovery::RecoveredBrackets>,
    /// The status cells of this store and the transactions it resolves. See
    /// `crate::ram::txn_status`.
    txn_status: crate::ram::txn_status::TxnStatusRegistry,
    /// The participant fences this site promised (D29).
    fences: crate::ram::fence::FenceLog,
    /// In-doubt transactions that wait for their participants' answers. The
    /// background resolver and the operator tool take one from here under
    /// this lock, so only one of them resolves it.
    unresolved: Mutex<
        std::collections::BTreeMap<
            crate::server::transactions::TxnId,
            crate::ram::recovery::InDoubtBracket,
        >,
    >,
    /// Set when the owning server goes away, so the background sweeper exits
    /// without waiting for the store itself to be dropped. The store can
    /// outlive its server -- an RPC service still holds it -- and a thread that
    /// only notices a dropped store would then never exit at all.
    background_stopped: Arc<std::sync::atomic::AtomicBool>,
}

impl Chunks {
    /// Release only after a final discard or durable plain-entry rewrite.
    pub(crate) fn release_transaction_pins(&self, txn: &crate::server::transactions::TxnId) {
        for chunk in &self.list {
            for segment in chunk.segments() {
                segment.protection.abort_transaction(txn);
            }
        }
        self.cleaner_wake.request();
    }

    /// Whole segments retained by transaction decisions, including dead bytes.
    pub fn pinned_segments(&self) -> usize {
        self.list
            .iter()
            .flat_map(|chunk| chunk.segments())
            .filter(|segment| segment.protection.transaction_pending())
            .count()
    }

    pub fn pinned_bytes(&self) -> usize {
        self.pinned_segments() * SEGMENT_SIZE
    }

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

    /// Keep what recovery learned about transactions, for termination.
    ///
    /// The status cells that recovery found are registered (D31): this site
    /// committed their transactions, and keeps answering `Committed` for them
    /// until every participant's COMMIT is synced. Recovery ends with a sync of
    /// every WAL file, so each of them is synced here (TLA+ F1). The in-doubt
    /// transactions are held in `Protection` by recovery already
    /// (`BracketLedger::settle`).
    pub fn remember_recovered_brackets(
        &self,
        mut brackets: crate::ram::recovery::RecoveredBrackets,
    ) {
        brackets.committed.clear();
        for found in std::mem::take(&mut brackets.status_cells) {
            self.txn_status.register(
                found.txn,
                crate::ram::txn_status::StatusEntry {
                    id: found.id,
                    chunk: found.chunk,
                    participants: found.participants,
                    self_synced: true,
                    synced_peers: std::collections::BTreeSet::new(),
                    segments: Vec::new(),
                    announced: false,
                    since: std::time::Instant::now(),
                },
            );
        }
        for bracket in &brackets.in_doubt {
            self.txn_status
                .begin_resolving(
                    bracket.txn.clone(),
                    bracket.participants.clone(),
                    bracket.cells.iter().map(|cell| cell.id_bits()).collect(),
                );
        }
        *self.recovered_brackets.lock() = brackets;
    }

    /// The status cells and in-doubt transactions of this store.
    pub fn txn_status(&self) -> &crate::ram::txn_status::TxnStatusRegistry {
        &self.txn_status
    }

    /// The status cell of `txn` in this store, by probing every chunk: its
    /// chunk, its id and the participants it names.
    pub fn find_status_cell(
        &self,
        txn: &crate::server::transactions::TxnId,
    ) -> Option<(usize, Id, Vec<u64>)> {
        for chunk in &self.list {
            if let Some((id, participants)) = chunk.find_status_cell(txn) {
                return Some((chunk.id, id, participants));
            }
        }
        None
    }

    /// The participant fences of this store.
    pub fn fences(&self) -> &crate::ram::fence::FenceLog {
        &self.fences
    }

    /// Keep an in-doubt transaction that waits for its participants.
    pub fn park_unresolved(&self, bracket: crate::ram::recovery::InDoubtBracket) {
        self.unresolved.lock().insert(bracket.txn.clone(), bracket);
    }

    pub fn take_unresolved(
        &self,
        txn: &crate::server::transactions::TxnId,
    ) -> Option<crate::ram::recovery::InDoubtBracket> {
        self.unresolved.lock().remove(txn)
    }

    /// The parked transactions and their participant sets.
    pub fn unresolved_transactions(&self) -> Vec<(crate::server::transactions::TxnId, Vec<u64>)> {
        self.unresolved
            .lock()
            .iter()
            .map(|(txn, bracket)| (txn.clone(), bracket.participants.clone()))
            .collect()
    }

    /// Take the transactions recovery could not decide, for resolution.
    pub fn take_in_doubt_brackets(&self) -> Vec<crate::ram::recovery::InDoubtBracket> {
        std::mem::take(&mut self.recovered_brackets.lock().in_doubt)
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

        // The hidden schema of transaction status cells, before recovery
        // reads them (`crate::ram::txn_status`).
        meta.schemas
            .register_internal_schema(crate::ram::txn_status::txn_status_schema());

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
        let estimated_cells =
            crate::ram::recovery::estimate_cells_per_chunk(&recovery_files, count);
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
            recovered_brackets: Mutex::new(crate::ram::recovery::RecoveredBrackets::empty()),
            txn_status: crate::ram::txn_status::TxnStatusRegistry::new(),
            fences: crate::ram::fence::FenceLog::new(wal_storage.as_deref()),
            unresolved: Mutex::new(std::collections::BTreeMap::new()),
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
                    for (chunk_id, seg_id, seq_id) in chunks.seal_queue().drain() {
                        if let Some(chunk) = chunks.list.get(chunk_id) {
                            chunk.finish_rotated_head(seg_id, seq_id);
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
                Ok(brackets) => {
                    info!(
                        "Recovery completed successfully: {} committed and {} in-doubt \
                         transaction(s) found in brackets",
                        brackets.committed.len(),
                        brackets.in_doubt.len()
                    );
                    // Before the store answers anything: what recovery read may
                    // be page cache that a power loss still drops, and a
                    // `Committed` answer must come from synced records only
                    // (TLA+ F1).
                    if let Err(error) = chunks_arc.sync_all() {
                        error!(
                            "the recovered store cannot be synced: {}. REFUSING TO START: a \
                             recovered status cell would answer `Committed` from a record \
                             that is not on disk.",
                            error
                        );
                        panic!("cannot sync the recovered store, refusing to start: {error}");
                    }
                    chunks_arc.remember_recovered_brackets(brackets);
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

        // The fences of the last run that still matter: those of the
        // transactions in doubt now, and every recent one, also of a
        // transaction with no bracket here (TLA+ F3). A log this build cannot
        // read stops the start, like any store file that does not decode.
        let previous = chunks_arc
            .fences
            .load(enable_recovery)
            .unwrap_or_else(|error| panic!("cannot read the fence log, refusing to start: {error}"));
        let in_doubt: std::collections::BTreeSet<_> = chunks_arc
            .recovered_brackets
            .lock()
            .in_doubt
            .iter()
            .map(|bracket| bracket.txn.clone())
            .collect();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        chunks_arc
            .fences
            .reset(crate::ram::fence::fences_to_keep(&previous, &in_doubt, now_ms))
            .unwrap_or_else(|error| panic!("cannot write the fence log, refusing to start: {error}"));

        chunks_arc
    }

    /// The address window this store's chunks were carved out of. Used to
    /// tell this store's leases apart from another store's in the same
    /// process, where the process-global lease map holds both.
    pub fn address_range(&self) -> std::ops::Range<usize> {
        self.base_addr..self.base_addr + self.total_size
    }

    /// Close every bracket this transaction opened, in every chunk it wrote,
    /// and make the COMMIT records durable.
    ///
    /// Runs at the DECISION, after the transaction's entries were fsynced at
    /// apply. That two-round order is the cross-chunk rule: a surviving
    /// COMMIT then proves every entry of the transaction was already durable,
    /// so one COMMIT anywhere decides the whole transaction at recovery.
    ///
    /// `Ok(true)` means every open part of the transaction carries its own
    /// COMMIT and every one of those records is fsynced. `Ok(false)` means
    /// the records are appended and their sync is queued. Any failure
    /// refuses the commit; [`CommitRefusal`] says what a power loss can bring
    /// back. If no COMMIT was synced, the failure path retracts the records
    /// this attempt placed from each private WAL tail, and syncs the
    /// retraction. If one COMMIT was synced, nothing is retracted: the
    /// transaction is decided, and a retry closes the parts that are still
    /// open. The caller retains the heads during rollback.
    ///
    /// A transaction holding no lease here has nothing bracketed to close: a
    /// read-only one, one whose writes all failed, or one whose writes could
    /// not be given a head and went out unbracketed (and were fsynced at
    /// apply). That is answered `Ok`, which makes it the CALLER's job to know
    /// that no other exit -- an abort, a sweeper -- already ended the leases
    /// of a transaction that did write; `DataManager` keeps that knowledge in
    /// the transaction object it serializes every decision through.
    ///
    /// `durable` (D7): the COMMIT records are synced before this returns.
    /// Otherwise (D19 option 1, the default) they are appended and their
    /// sync is queued: the receipts request it from the group syncer, and
    /// the segments stay in progress (D20) until it lands. A process crash
    /// keeps the appended records; a power loss may drop them at every
    /// participant, and the participant fence then aborts or commits the
    /// transaction as a whole.
    pub fn commit_transaction_brackets(
        &self,
        txn: &crate::server::transactions::TxnId,
        durable: bool,
    ) -> Result<bool, CommitRefusal> {
        let manifest = transaction_manifest(txn, &self.address_range());
        if manifest.is_empty() {
            return Ok(true);
        }
        let chunk_ids: Vec<u16> = {
            let mut ids: Vec<u16> = manifest.iter().map(|member| member.chunk_id).collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };
        let segments = transaction_leased_segments(txn, &self.address_range());
        // Only the parts still open now get a record from this attempt, so
        // only they are retracted on a refusal. A part an earlier attempt
        // closed keeps its record.
        let checkpoints: Vec<_> = segments
            .iter()
            .map(|segment| {
                (
                    segment.wal_append_position(),
                    segment.append_header.load(Ordering::Acquire),
                    segment.bracket_is_open(),
                )
            })
            .collect();
        // Set once a segment that holds a COMMIT of this transaction is
        // synced. From then on recovery commits the transaction.
        let mut synced_commit = false;
        let mut forced = false;
        let mut proof = None;
        let decision: Result<(), WriteError> = 'decision: {
            for chunk_id in chunk_ids {
                let Some(chunk) = self.list.get(chunk_id as usize) else {
                    break 'decision Err(WriteError::CannotAllocateSpace);
                };
                if let Err(error) = chunk.close_transaction_bracket(txn, &manifest) {
                    break 'decision Err(error);
                }
            }
            #[cfg(test)]
            if take_injected_commit_refusal(self.base_addr, txn) {
                break 'decision Err(WriteError::CannotAllocateSpace);
            }
            let mut receipts = Vec::new();
            let mut all_parts_proven = true;
            for segment in &segments {
                {
                    let state = segment.file_state.lock();
                    if state.wal.is_none()
                        && (state.manager.wal_storage().is_some()
                            || state.manager.backup_storage().is_some())
                    {
                        all_parts_proven = false;
                    }
                }
                match segment.durable_receipt() {
                    Ok(Some(receipt)) => receipts.push(receipt),
                    Ok(None) => {}
                    Err(error) => break 'decision Err(WriteError::from_wal_io(error)),
                }
            }
            // Queued (the default): the receipts below request the sync.
            // Forced: also when a part cannot be proven by a receipt.
            let force = durable || !all_parts_proven;
            forced = force;
            for segment in segments.iter().filter(|_| force) {
                #[cfg(test)]
                if synced_commit && take_injected_sync_failure(self.base_addr, txn) {
                    break 'decision Err(WriteError::from_wal_io(io::Error::other(
                        "injected WAL sync failure",
                    )));
                }
                if let Err(error) = segment.force_wal_sync() {
                    break 'decision Err(WriteError::from_wal_io(error));
                }
                if segment.bracket_is_committed() {
                    synced_commit = true;
                }
            }
            proof = Some((Arc::new(receipts), all_parts_proven));
            Ok(())
        };
        let Err(error) = decision else {
            let (receipts, all_parts_proven) = proof.expect("a decision that succeeded has a proof");
            if all_parts_proven {
                for segment in &segments {
                    segment.protection.decide_transaction(txn, receipts.clone());
                }
            }
            return Ok(forced);
        };
        if synced_commit {
            // Retracting now would make the client hear "failed" about a
            // transaction that recovery commits. The heads stay leased, so
            // the segments stay in progress until a retry closes them.
            error!(
                "transaction {:?}: a COMMIT is durable but closing every part failed: {:?}; \
                 the transaction is decided and the open parts are closed later",
                txn, error
            );
            return Err(CommitRefusal::Durable(error));
        }
        let mut retraction_synced = true;
        for (segment, (position, cursor, was_open)) in segments.iter().zip(checkpoints) {
            if !was_open {
                continue;
            }
            // Synced: an unsynced truncation can come back after a power
            // loss with the COMMIT behind it.
            if let Err(truncate_error) = segment.truncate_wal_to(position) {
                error!(
                    "cannot retract refused COMMIT in segment {}: {truncate_error}",
                    segment.id
                );
                retraction_synced = false;
                segment.retire_tail();
            }
            let end = segment.append_header.load(Ordering::Acquire);
            if end > cursor {
                crate::ram::entry::stamp_reservation_padding(cursor, (end - cursor) as u32);
            }
            segment.mark_bracket_open();
        }
        if retraction_synced {
            Err(CommitRefusal::Retracted(error))
        } else {
            Err(CommitRefusal::Unresolved(error))
        }
    }

    /// Sync all buffered WAL data to disk across all chunks.
    /// Returns how many segments could not be synced: their most recent
    /// writes are not durable, and a caller that promises durability must
    /// stop (review finding 6).
    pub fn sync_all(&self) -> Result<(), SyncAllFailed> {
        info!("Syncing WAL for all chunks...");
        let mut failures = 0usize;
        for chunk in &self.list {
            for segment in chunk.segs.iter_values() {
                // A swallowed sync failure here is a store that reports
                // itself synced while its log is not on disk -- the exact
                // shape of promise this campaign exists to stop making.
                if let Err(error) = segment.drain_wal_sync() {
                    error!(
                        "sync_all could not sync segment {} (chunk {}): {:?}",
                        segment.id, segment.chunk_id, error
                    );
                    failures += 1;
                }
            }
        }
        if failures > 0 {
            error!(
                "sync_all left {} segment(s) unsynced; their most recent writes are NOT durable",
                failures
            );
            Err(SyncAllFailed { segments: failures })
        } else {
            info!("All WAL data synced to disk.");
            Ok(())
        }
    }

    /// Refuse further entry allocation, everywhere, before shutdown archives.
    ///
    /// This must be called before `archive_all`, and it is one-way: a segment
    /// that has been archived is sealed and can never take another append, so
    /// a write accepted after this point would have no durable home. Callers
    /// see `WriteError::ServerShuttingDown`, which is honest and retryable --
    /// the alternative is accepting the cell and losing it silently at the
    /// next crash.
    /// Where this store's backup files live, if it has any.
    ///
    /// The index durability mark is written beside them on purpose: recovery
    /// reads the store from the backups, so a mark that outlived them would
    /// describe positions in files that no longer exist.
    pub fn file_manager_backup_path(&self) -> Option<String> {
        self.list
            .first()
            .and_then(|chunk| chunk.file_manager.backup_storage().map(|s| s.to_string()))
    }

    /// Stop accepting CLIENT writes; the index's own writes continue.
    ///
    /// Called first thing in shutdown so that nothing a client is told
    /// succeeded can land after the flush that was supposed to persist its
    /// index entries.
    /// What this store durably knows about a transaction.
    ///
    /// Answered ONLY from the segments, never from the in-memory transaction
    /// map: the caller that needs this most is a participant that restarted
    /// and has no map. This is the evidence cooperative termination runs on.
    ///
    /// The three answers are exhaustive and none of them is a guess:
    ///
    /// - a COMMIT record for T exists  => it was TOLD to commit;
    /// - an open BEGIN and no COMMIT   => it prepared and never heard;
    /// - nothing at all                => aborted, or never prepared here.
    ///
    /// The last one is safe to treat as abort ONLY because of two rules the
    /// format already keeps: every bracket part carries its own COMMIT, and
    /// compaction drops a segment's BEGIN and COMMIT together. So a committed
    /// transaction whose records were compacted comes back as ordinary
    /// entries -- it is not a transaction this store is holding open, and it
    /// cannot masquerade as "never decided".
    pub fn durable_txn_status(&self, txn: &crate::server::transactions::TxnId) -> DurableTxnStatus {
        let mut prepared = false;
        for chunk in &self.list {
            for segment in chunk.segments() {
                for meta in segment.entry_iter() {
                    let content_len = meta.entry_header.content_length as usize;
                    let content = unsafe {
                        std::slice::from_raw_parts(meta.body_pos as *const u8, content_len)
                    };
                    match meta.entry_header.entry_type {
                        crate::ram::entry::EntryType::COMMIT => {
                            if let Some((committed, _)) =
                                crate::ram::bracket::decode_commit(content)
                            {
                                if &committed == txn {
                                    return DurableTxnStatus::Committed;
                                }
                            }
                        }
                        crate::ram::entry::EntryType::BEGIN => {
                            if let Ok(began) = crate::ram::bracket::decode_begin(content) {
                                if &began.txn == txn {
                                    prepared = true;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if prepared {
            DurableTxnStatus::Prepared
        } else {
            DurableTxnStatus::Aborted
        }
    }

    pub fn close_client_writes(&self) {
        for chunk in &self.list {
            chunk.client_writes_closed.store(true, Ordering::Release);
        }
        info!(
            "Client writes closed on {} chunks; index write-back continues until the flush \
             completes",
            self.list.len()
        );
    }

    pub fn close_writes(&self) {
        for chunk in &self.list {
            chunk.writes_closed.store(true, Ordering::Release);
        }
        info!(
            "Entry allocation closed on {} chunks for shutdown",
            self.list.len()
        );
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
        slots
            .iter()
            .map(|slot| self.slot_bytes.get(*slot))
            .collect()
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
            if chunk.writes_closed.load(Ordering::Acquire) {
                for slot in chunk.head_pool.iter().chain(chunk.blob_head_pool.iter()) {
                    let id = slot.swap(HEAD_SEG_ID_EMPTY, Ordering::AcqRel);
                    if let Some(segment) = chunk.segs.get(&(id as usize)) {
                        segment.protection.head.store(false, Ordering::Release);
                    }
                }
            }
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
        return (
            self.locate_chunk_by_partition(key.locality() as u64),
            key.bits(),
        );
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
    /// Certify existing cell WAL prefixes. Queued ranged tree writeback is excluded.
    pub async fn flush_wal_durable(&self) -> Result<(), WriteError> {
        let mut receipts = Vec::new();
        for chunk in &self.list {
            for segment in chunk.segments() {
                if let Some(receipt) = segment.durable_receipt().map_err(WriteError::from_wal_io)? {
                    receipts.push(receipt);
                }
            }
        }
        crate::ram::durability::wait(receipts).await
    }

    pub fn durability_failures(&self) -> Vec<crate::ram::durability::DurabilityError> {
        self.list
            .iter()
            .flat_map(|chunk| {
                chunk
                    .segs
                    .iter_values()
                    .filter_map(|seg| seg.durability_failure())
                    .collect::<Vec<_>>()
            })
            .collect()
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
    pub(crate) fn apply_transaction_update<U>(
        &self,
        key: &Id,
        update: U,
    ) -> Result<OwnedCell, WriteError>
    where
        U: FnOnce(&SharedCellData) -> Option<OwnedCell>,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.update_cell_by_inner(hash, update, false)
    }

    pub(crate) fn apply_transaction_remove<P>(&self, key: &Id, predict: P) -> Result<(), WriteError>
    where
        P: Fn(&SharedCell) -> bool,
    {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.remove_cell_by_inner(hash, predict, false)
    }

    pub(crate) fn restore_transaction_cell(
        &self,
        key: &Id,
        previous: usize,
        current_version: u64,
    ) -> Result<(), WriteError> {
        let (chunk, hash) = self.locate_chunk_by_key(key);
        chunk.restore_cell_location(hash, previous, current_version)
    }

    pub(crate) fn retire_transaction_version(&self, key: &Id, location: usize, deleted: bool) {
        let (chunk, _) = self.locate_chunk_by_key(key);
        let seg = chunk.locate_segment_ensured(location, key);
        chunk.mark_dead_entry_with_seg(location, &seg);
        if deleted {
            let header = header_from_chunk_raw(location)
                .expect("the transaction pins this version")
                .0;
            chunk.note_deleted_version(key.bits(), header.version);
        }
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
        // A status cell belongs to this site and is never migrated.
        self.list
            .iter()
            .flat_map(|chunk| chunk.cell_ids_in_slots(slots))
            .filter(|id| !self.is_status_cell(id.bits()))
            .collect()
    }

    /// Whether the live cell `id` is a status cell (D31). Status cells have
    /// the reserved origin `STATUS_ORIGIN`; for such an id the registry
    /// answers, and the header answers for a status cell that is not
    /// registered yet (written, its close still running). Other ids cost one
    /// shift and compare.
    pub fn is_status_cell(&self, id: u64) -> bool {
        let id = Id::from_bits(id);
        if id.is_hashed() || id.origin() != crate::ram::txn_status::STATUS_ORIGIN {
            return false;
        }
        self.txn_status.is_status_id(id.bits())
            || self.head_cell(&id).is_ok_and(|header| {
                header.schema == *crate::ram::txn_status::TXN_STATUS_SCHEMA_ID
            })
    }

    pub fn count(&self) -> usize {
        self.list.iter().map(|c| c.count()).sum()
    }

    /// How many chunks this store has. A cell scan visits them one at a time.
    pub fn chunk_count(&self) -> usize {
        self.list.len()
    }

    /// Live cell ids of one chunk, ascending. See [`Chunk::live_cell_ids`] for
    /// what this costs; an out-of-range index is an empty chunk.
    pub fn live_cell_ids_of_chunk(&self, chunk: usize) -> Vec<u64> {
        let mut ids = self
            .list
            .get(chunk)
            .map(Chunk::live_cell_ids)
            .unwrap_or_default();
        // Status cells are hidden: a scan, a count or a dump never sees them.
        ids.retain(|id| !self.is_status_cell(*id));
        ids
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

    pub fn all_chunk_statistics(&self, schema_id: SchemaUid) -> Vec<Option<Arc<SchemaStatistics>>> {
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
    pub fn overall_statistics(&self, schema: SchemaUid) -> Arc<SchemaStatistics> {
        self.statistics
            .get(schema.get() as usize, 5 * 60, |schema| {
                let schema = SchemaUid(schema as u32);
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

thread_local! {
    #[cfg(debug_assertions)]
    static HELD_CELL_GUARDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(crate) static CAS_ALLOCATING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
                                // checked against anything. The entry's own
                                // checksum answers for it here.
                                //
                                // A refusal FALLS THROUGH to promotion below,
                                // which reads the whole backup and does verify
                                // its CRC -- so a genuinely corrupt file is
                                // refused there with the full diagnosis, and a
                                // block that merely came in wrong is re-read
                                // properly. It must not `return None` on its
                                // own: that signals "retry" to the caller
                                // without promoting anything, so the retry
                                // lands on the same cold block and fails the
                                // same way, forever.
                                let verdict =
                                    crate::ram::entry::verify_entry_at(*guard, seg.bound());
                                if verdict == Some(true) {
                                    seg.mark_referenced();
                                    if let Some(ref tiered) = chunk.tiered_manager {
                                        tiered.add_cold_resident(newly_resident);
                                        tiered.note_cold_block_read();
                                    }
                                    #[cfg(debug_assertions)]
                                    HELD_CELL_GUARDS.with(|held| held.set(held.get() + 1));
                                    return Some(CellGuard {
                                        _qsbr: crate::ram::qsbr::QsbrSection::new(),
                                        hash,
                                        guard: Some(guard),
                                        chunk,
                                        segment,
                                        version,
                                    });
                                }
                                // `None` is not an entry exempt from checking.
                                // It means "no content to vouch for" -- padding,
                                // or a reservation never filled -- and the cell
                                // index only ever points at CELL entries, which
                                // always carry a checksum. `None` at a cell
                                // address is the WRONG ENTRY there, which is
                                // exactly what this check exists to catch.
                                error!(
                                    "Cold block read of segment {} (chunk {}) produced an entry \
                                     that does not vouch for itself at {:#x} (verdict {:?}). \
                                     Refusing to serve it; promoting and reading the whole \
                                     backup, which verifies the image end to end.",
                                    seg.id, chunk.id, *guard, verdict
                                );
                                seg.decr_references();
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

        #[cfg(debug_assertions)]
        HELD_CELL_GUARDS.with(|held| held.set(held.get() + 1));
        Some(Self {
            _qsbr: crate::ram::qsbr::QsbrSection::new(),
            guard: Some(guard),
            chunk,
            hash,
            segment,
            version,
        })
    }

    /// How long acquiring a cell guard may retry before giving up.
    ///
    /// The retry loop exists for states that RESOLVE: a cold segment being
    /// promoted, or one briefly held exclusively by an evictor, promoter or
    /// the cleaner. `from_guard` answers None for those and the next pass
    /// succeeds.
    ///
    /// It also covers one that NEVER resolves -- a cell index entry pointing
    /// at a segment that no longer exists -- and there the loop spun forever,
    /// burning a core and holding whatever the caller held. Found with gdb on
    /// a transaction suite that had made no progress in over an hour: one
    /// thread inside `DataManager::rollback` -> `update_cell_by` ->
    /// `for_write`, sched_yield'ing in `Backoff::spin`, with 88 other threads
    /// parked behind the abort it never finished.
    ///
    /// A deadline turns that from a wedge into an error the caller already
    /// knows how to report: `update_cell_by` maps None to
    /// `CellDoesNotExisted`, and a stale pointer really is a cell this chunk
    /// can no longer produce.
    const GUARD_ACQUIRE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

    pub fn for_read(hash: u64, chunk: &'a Chunk) -> Result<Self, ReadError> {
        let backoff = Backoff::new();
        let started = std::time::Instant::now();
        loop {
            let guard = chunk.location_for_read(hash)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Ok(guard);
            }
            if started.elapsed() >= Self::GUARD_ACQUIRE_DEADLINE {
                error!(
                    "read guard for cell {} in chunk {} could not be acquired in {:?}; the index \
                     entry names a segment this chunk cannot produce. Reporting a stale pointer \
                     rather than spinning forever.",
                    hash,
                    chunk.id,
                    Self::GUARD_ACQUIRE_DEADLINE
                );
                CELL_GUARD_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                return Err(ReadError::StaleCellPointer);
            }
            backoff.spin();
        }
    }

    pub fn for_write(hash: u64, has_read: bool, chunk: &'a Chunk) -> Option<Self> {
        let backoff = Backoff::new();
        let started = std::time::Instant::now();
        loop {
            let guard = chunk.location_for_write(hash, has_read)?;
            if let Some(guard) = CellGuard::from_guard(hash, guard, chunk) {
                return Some(guard);
            }
            if started.elapsed() >= Self::GUARD_ACQUIRE_DEADLINE {
                error!(
                    "write guard for cell {} in chunk {} could not be acquired in {:?}; the index \
                     entry names a segment this chunk cannot produce. Refusing the write rather \
                     than spinning forever -- a wedged writer takes its caller with it, and this \
                     one wedged an abort.",
                    hash,
                    chunk.id,
                    Self::GUARD_ACQUIRE_DEADLINE
                );
                CELL_GUARD_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
                return None;
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
        if cell.header.version < self.version {
            cell.header.version = self.version;
        }
        if self.is_unassigned() {
            return Err(WriteError::CellDoesNotExisted);
        }
        let write_plan = cell.plan_write(self.chunk)?;
        let pending_entry = write_plan.allocate(self.chunk, false)?;
        let write_result =
            self.update_cell_in_entry(cell, &write_plan, &pending_entry, cell.header.version)?;
        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        cell.header.schema = write_result.new_schema;
        Ok(cell.header)
    }

    fn update_cell_in_entry(
        &mut self,
        cell: &OwnedCell,
        write_plan: &WritePlan<'_>,
        pending_entry: &PendingEntry,
        version: u64,
    ) -> Result<WriteToChunkResult, WriteError> {
        let old_cell_loc = self.get_ptr();
        let write_result =
            self.chunk
                .write_cell_to_chunk(cell, write_plan, pending_entry, version)?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let old_indices = self.old_index_res(schema)?;
        let guard = self.guard.as_mut().unwrap();
        pending_entry.append()?;
        **guard = new_cell_loc;
        self.chunk
            .ensure_indices_with_res(cell, old_indices, schema);
        self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        self.chunk.refresh_statistics_for_schema(schema.vid);
        Ok(write_result)
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
            if old_cell_loc == 0 {
                self.chunk.create_version(cell)
            } else {
                cell.header.version
            },
        )?;
        let new_cell_loc = write_result.addr;
        let schema = &*write_plan.schema;
        let schema_id = schema.vid;
        if old_cell_loc != 0 {
            // Update case - cell already exists
            let old_indices = self.old_index_res(&*schema)?;
            let guard = self.guard.as_mut().unwrap();
            pending_entry.append()?;
            **guard = new_cell_loc;
            self.chunk
                .ensure_indices_with_res(cell, old_indices, &*schema);
            self.chunk.mark_dead_entry_with_cell(old_cell_loc, cell);
        } else {
            // Insert case - new cell
            let guard = self.guard.as_mut().unwrap();
            pending_entry.append()?;
            **guard = new_cell_loc;
            self.chunk.ensure_indices(cell, None, &*schema);
        }

        drop(write_plan);
        cell.header.version = write_result.new_version;
        cell.header.timestamp = write_result.new_timestamp;
        cell.header.schema = write_result.new_schema;

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
        #[cfg(debug_assertions)]
        HELD_CELL_GUARDS.with(|held| held.set(held.get() - 1));
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
thread_local! {
    static TEST_DELETE_AFTER_RESERVE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_DELETE_BEFORE_RESERVE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_BEFORE_ROTATION_ALLOCATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_BEFORE_HEAD_QUEUE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_BEFORE_CAPACITY_REFUSAL: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_BEFORE_WORD: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_CAS_HEAD_WAIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::{Field, LocalSchemasCache};
    use crate::ram::segs::SegmentReferenceGuard;
    use crate::ram::types::Map;
    use bifrost_hasher::hash_str;
    use dovahkiin::types::Type;
    use env_logger;

    #[test]
    fn refused_seal_archives_after_jobs_and_pins_clear() {
        for pin_transaction in [false, true] {
            let directory = tempfile::TempDir::new().unwrap();
            let chunks = Chunks::new_dummy_with_backup(
                1,
                4 * SEGMENT_SIZE,
                Some(directory.path().to_string_lossy().into_owned()),
            );
            chunks.stop_background();
            let chunk = &chunks.list[0];
            let id = chunk.get_head_seg_id();
            let segment = chunk.segs.get(&(id as usize)).unwrap();
            crate::ram::entry::stamp_reservation_padding(segment.addr, 8);
            segment
                .append_header
                .store(segment.addr + 8, Ordering::Release);
            segment.set_dirty();
            segment.seal();
            chunk.head_pool[0].store(HEAD_SEG_ID_EMPTY, Ordering::Release);
            segment.protection.head.store(false, Ordering::Release);
            segment
                .protection
                .seal_pending
                .store(true, Ordering::Release);
            let txn = crate::server::transactions::test_hlc(9841, 1);
            let job = if pin_transaction {
                segment.protection.retain_transaction(&txn);
                None
            } else {
                Some(segment.protection.job().unwrap())
            };
            assert_eq!(
                segment.archive_rotated().unwrap(),
                crate::ram::segs::ArchiveOutcome::Refused
            );
            chunk.finish_rotated_head(id, segment.seq_id);
            assert!(segment.is_dirty());
            assert_eq!(chunk.seal_queue.len(), 1, "a refusal must remain queued");
            if pin_transaction {
                assert_eq!(chunks.pinned_segments(), 1);
                assert_eq!(chunks.pinned_bytes(), SEGMENT_SIZE);
                chunks.release_transaction_pins(&txn);
            }
            drop(job);
            for (chunk_id, id, sequence) in chunk.seal_queue.drain() {
                chunks.list[chunk_id].finish_rotated_head(id, sequence);
            }
            assert_eq!(chunks.pinned_segments(), 0);
            assert_eq!(
                chunk
                    .segments()
                    .iter()
                    .filter(|segment| segment.is_sealed() && segment.is_dirty())
                    .count(),
                0
            );
            assert!(chunk.seal_queue.drain().is_empty());
            assert!(!segment.protection.seal_pending.load(Ordering::Acquire));
        }
    }

    #[test]
    fn compose_scrub_rotation_archives_the_selected_generation() {
        let directory = tempfile::TempDir::new().unwrap();
        let chunks = Chunks::new_dummy_with_backup(
            1,
            4 * SEGMENT_SIZE,
            Some(directory.path().to_string_lossy().into_owned()),
        );
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = chunk.get_head_seg_id();
        let segment = chunk.segs.get(&(id as usize)).unwrap();
        crate::ram::entry::stamp_reservation_padding(segment.addr, 8);
        segment
            .append_header
            .store(segment.addr + 8, Ordering::Release);

        chunk.rotate_heads_for_scrub_test();

        assert_eq!(chunk.head_seg_ids_for_test(), (None, None));
        assert!(segment.is_sealed());
        assert!(std::path::Path::new(
            &chunk
                .file_manager
                .backup_path(chunk.id, segment.id, segment.seq_id)
                .unwrap()
        )
        .exists());
    }

    #[test]
    fn rotated_seal_protects_its_generation_until_archive_finishes() {
        let directory = tempfile::TempDir::new().unwrap();
        let chunks = Chunks::new_dummy_with_backup(
            1,
            4 * SEGMENT_SIZE,
            Some(directory.path().to_string_lossy().into_owned()),
        );
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = chunk.get_head_seg_id();
        let segment = chunk.segs.get(&(id as usize)).unwrap();
        let sequence = segment.seq_id;
        crate::ram::entry::stamp_reservation_padding(segment.addr, 8);
        segment
            .append_header
            .store(segment.addr + 8, Ordering::Release);
        for slot in chunk.head_pool.iter() {
            slot.store(HEAD_SEG_ID_EMPTY, Ordering::Release);
        }
        // Hold the archive at its first file operation after it takes the pin.
        let file_state = segment.file_state.lock();
        let worker_chunks = chunks.clone();
        let worker = std::thread::spawn(move || {
            worker_chunks.list[0].finish_rotated_head(id, sequence);
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while segment.no_references() && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        let pinned = !segment.no_references();
        let reclaimed = if pinned {
            chunk.remove_segment(id);
            chunk.drain_retired_segments()
        } else {
            0
        };
        drop(file_state);
        worker.join().unwrap();
        assert!(
            pinned,
            "the sealer must pin the selected generation during file operations"
        );
        assert_eq!(
            reclaimed, 0,
            "the cleaner must not recycle the archive source"
        );
        assert!(
            segment.is_sealed(),
            "a matching request must still archive the original segment"
        );
        chunk.remove_segment(id);
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)),
            1
        );
    }

    fn delayed_seal_preserves_reused_segment(active_head: bool) {
        let directory = tempfile::TempDir::new().unwrap();
        let chunks = Chunks::new_dummy_with_backup(
            1,
            4 * SEGMENT_SIZE,
            Some(directory.path().to_string_lossy().into_owned()),
        );
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let old_id = chunk.get_head_seg_id();
        let old_sequence = chunk.segs.get(&(old_id as usize)).unwrap().seq_id;
        let queue = SealQueue::new();
        assert!(queue.enqueue(chunk.id, old_id, old_sequence));
        for slot in chunk.head_pool.iter() {
            slot.store(HEAD_SEG_ID_EMPTY, Ordering::Release);
        }
        chunk
            .segs
            .get(&(old_id as usize))
            .unwrap()
            .protection
            .head
            .store(false, Ordering::Release);
        chunk.remove_segment(old_id);
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)),
            1
        );

        let replacement = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
        assert_eq!(
            replacement.id, old_id,
            "the allocator must reuse the physical slot"
        );
        assert_ne!(replacement.seq_id, old_sequence);
        crate::ram::entry::stamp_reservation_padding(replacement.addr, 8);
        replacement
            .append_header
            .store(replacement.addr + 8, Ordering::Release);
        chunk.put_segment(replacement);
        if active_head {
            chunk.head_pool[0].store(old_id, Ordering::Release);
        }
        let replacement = chunk.segs.get(&(old_id as usize)).unwrap();
        for (chunk_id, segment_id, sequence_id) in queue.drain() {
            assert_eq!(chunk_id, chunk.id);
            chunk.finish_rotated_head(segment_id, sequence_id);
        }
        assert!(
            !replacement.is_sealed(),
            "a delayed seal must not close a different segment generation"
        );
        assert!(
            replacement.begin_pending_journal(),
            "the replacement must still accept journal claims"
        );
        replacement.end_pending_journal();
        assert!(!std::path::Path::new(
            &chunk
                .file_manager
                .backup_path(chunk.id, replacement.id, replacement.seq_id)
                .unwrap()
        )
        .exists());
    }

    #[test]
    fn delayed_seal_does_not_close_a_reused_head() {
        delayed_seal_preserves_reused_segment(true);
    }

    #[test]
    fn delayed_seal_does_not_archive_a_reused_non_head() {
        delayed_seal_preserves_reused_segment(false);
    }

    #[test]
    fn deleted_version_map_memory_cost() {
        fn rss_kb() -> usize {
            std::fs::read_to_string("/proc/self/status")
                .unwrap()
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .parse()
                .unwrap()
        }
        let before = rss_kb();
        let mut map = std::collections::HashMap::with_hasher(ahash::RandomState::new());
        for id in 0..1_000_000u64 {
            map.insert(
                id,
                DeletedVersion {
                    version: 4,
                    tombstones: 1,
                },
            );
        }
        let after = rss_kb();
        println!(
            "METRIC deleted_id_entry_bytes {}",
            std::mem::size_of::<(u64, DeletedVersion)>()
        );
        println!(
            "METRIC deleted_id_rss_bytes {:.2}",
            (after - before) as f64 * 1024.0 / map.len() as f64
        );
        println!("METRIC deleted_id_map_capacity {}", map.capacity());
        std::hint::black_box(map);
    }

    const TEST_CHUNK_SIZE: usize = 8 * 1024 * 1024;
    /// A chunk with room for several segments.
    ///
    /// The lease tests need one: a transaction may only hold a head when the
    /// chunk can spare it, and a chunk sized to a SINGLE segment can never --
    /// its one head is everybody's head. Testing leases in that shape would be
    /// testing the fallback, not the lease.
    const TEST_CHUNK_SIZE_MULTI: usize = 8 * TEST_CHUNK_SIZE;

    fn test_txn_id(n: u64) -> crate::server::transactions::TxnId {
        crate::server::transactions::test_hlc(n, n)
    }

    #[test]
    fn missing_and_zero_address_reads_return_typed_errors() {
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE);
        let chunk = &chunks.list[0];
        let hash = Id::allocated(1, 0, 55).bits();
        for zero_address in [false, true] {
            if zero_address {
                drop(chunk.cell_index.lock_or_insert(hash as usize, 0));
            }
            let started = std::time::Instant::now();
            assert!(matches!(
                chunk.read_cell(hash),
                Err(ReadError::CellDoesNotExisted)
            ));
            assert!(matches!(
                chunk.head_cell(hash),
                Err(ReadError::CellDoesNotExisted)
            ));
            assert!(matches!(
                chunk.read_selected(hash, &[], false),
                Err(ReadError::CellDoesNotExisted)
            ));
            assert!(started.elapsed() < std::time::Duration::from_secs(5));
        }
    }

    /// Every chunk's lease key must fall inside its own store's address
    /// window -- the assumption the whole store-scoped lease API rests on.
    ///
    /// The key's first element is `allocator.base_addr()`, and the window is
    /// `base_addr .. base_addr + total_size`. Those agree only because the
    /// store mmaps ONE contiguous region and hands each chunk a slice of it.
    /// A chunk that allocated its own memory instead -- which the allocator
    /// still supports, via `base_addr == 0` -- would sit outside the window,
    /// and then EVERY lease operation silently becomes a no-op: no bracket
    /// closed, no head released, no manifest, and `commit_transaction_brackets`
    /// returning Ok because it found nothing to do.
    ///
    /// That failure has no symptom until a restart, which is why it is
    /// asserted here rather than left to be noticed.
    #[test]
    fn every_chunk_lease_key_falls_inside_its_own_store() {
        let _ = env_logger::try_init();
        for (count, size) in [(1usize, TEST_CHUNK_SIZE), (4, TEST_CHUNK_SIZE_MULTI)] {
            let chunks = Chunks::new_dummy(count, size);
            let range = chunks.address_range();
            assert!(
                range.start > 0 && range.end > range.start,
                "a store must have a real address window, got {:?}",
                range
            );
            for chunk in &chunks.list {
                let key = chunk.lease_key();
                assert!(
                    range.contains(&key.0),
                    "chunk {} leases under base {:#x}, outside its store's window {:#x}..{:#x}; \
                     every lease in this chunk would be invisible to its own store",
                    chunk.id,
                    key.0,
                    range.start,
                    range.end
                );
            }
            // And the window is exactly the chunks, with nothing left over for
            // a stray address to land in.
            assert_eq!(
                range.end - range.start,
                chunks.list.len() * (size.next_power_of_two()),
                "the window must be exactly the chunks it covers"
            );
        }
    }

    /// One process hosts one store per DATABASE, and two of them can hold
    /// leases under the SAME transaction id. Deciding one must not disturb
    /// the other.
    ///
    /// The lease map is process-global; a store is not. Every entry point
    /// took only the transaction id, so the first store to reach the
    /// decision removed the whole map entry -- and the second then found no
    /// leases, closed no bracket, and returned success. Its BEGIN stood with
    /// no COMMIT, so recovery discarded its half of a transaction the other
    /// half had durably committed: a tear inside ONE process, with no
    /// coordinator failure involved at all.
    ///
    /// Found while building the two-participant termination test, where two
    /// servers necessarily share a process. It is not a test artifact.
    #[test]
    fn deciding_one_store_leaves_another_stores_leases_alone() {
        let _ = env_logger::try_init();
        let first = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let second = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        assert!(
            !first
                .address_range()
                .contains(&second.address_range().start),
            "the two stores must occupy distinct address windows"
        );
        let txn = test_txn_id(77);

        let scope = TransactionScope::enter(txn.clone());
        let a = first.list[0]
            .try_acquire(64, false)
            .expect("lease in the first store");
        let b = second.list[0]
            .try_acquire(64, false)
            .expect("lease in the second store");
        assert!(a.leased && b.leased);
        drop(a);
        drop(b);
        drop(scope);

        assert_eq!(transaction_lease_count(&txn, &first.address_range()), 1);
        assert_eq!(transaction_lease_count(&txn, &second.address_range()), 1);

        // The first store decides. It must close and release ITS head and
        // nothing else.
        first
            .commit_transaction_brackets(&txn, true)
            .expect("first store commits");
        assert_eq!(
            release_transaction_leases(&txn, &first.address_range()),
            Ok(1)
        );
        assert_eq!(transaction_lease_count(&txn, &first.address_range()), 0);
        assert_eq!(
            transaction_lease_count(&txn, &second.address_range()),
            1,
            "the second store still holds its head and has a bracket to close"
        );
        assert!(
            transaction_has_leases(&txn, &second.address_range()),
            "the second store must still see work to do at its own decision"
        );
        assert!(
            !transaction_has_leases(&txn, &first.address_range()),
            "the store that already decided must not see the other's leases as its own"
        );
        assert_eq!(
            transaction_manifest(&txn, &second.address_range()).len(),
            1,
            "the manifest a store writes names only chunks it owns"
        );

        second
            .commit_transaction_brackets(&txn, true)
            .expect("second store commits");
        assert_eq!(
            release_transaction_leases(&txn, &second.address_range()),
            Ok(1)
        );
        assert_eq!(transaction_lease_count(&txn, &second.address_range()), 0);
    }

    /// A transaction's writes go behind ONE head per chunk, and that head
    /// stays owned between entries.
    ///
    /// This is the whole point of the lease: with a head owned across the
    /// write set, nobody else appends between a transaction's entries, which
    /// is what makes the entries contiguous (so they can be bracketed) and
    /// what will let an abort rewind them physically instead of logging undo
    /// records. Without it every entry claims and releases a head, so two
    /// transactions interleave freely in one segment.
    #[test]
    fn a_transaction_keeps_one_head_across_its_writes() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(41);

        let scope = TransactionScope::enter(txn.clone());
        let first = chunk.try_acquire(64, false).expect("first entry");
        let first_seg = first.seg.id;
        assert!(first.leased, "a transactional claim must take a lease");
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 1);
        first.append().unwrap();
        drop(first);

        // The head is STILL owned after the entry finished, so the next
        // entry lands in the same segment.
        let second = chunk.try_acquire(64, false).expect("second entry");
        assert!(second.leased);
        assert_eq!(
            second.seg.id, first_seg,
            "the second entry must land in the head the transaction already holds"
        );
        assert_eq!(
            transaction_lease_count(&txn, &chunks.address_range()),
            1,
            "reusing a leased head must not open a second lease"
        );
        second.append().unwrap();
        drop(second);
        drop(scope);

        // Decided: every bracket closed, so the heads may go back.
        chunks.commit_transaction_brackets(&txn, true).expect("commit");
        assert_eq!(
            release_transaction_leases(&txn, &chunks.address_range()),
            Ok(1)
        );
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 0);

        // Released: an ordinary writer can take the head again.
        let plain = chunk.try_acquire(64, false).expect("plain entry");
        assert!(!plain.leased, "a non-transactional claim takes no lease");
        assert_eq!(
            plain.seg.id, first_seg,
            "the released head returns to the pool rather than being stranded"
        );
    }

    fn sequential_small_transactions_reuse_heads(abort: bool) {
        let chunks = Chunks::new_dummy(1, 2 * SEGMENT_SIZE);
        let chunk = &chunks.list[0];
        let mut ids = std::collections::HashSet::new();
        for n in 0..200 {
            let txn = test_txn_id(10_000 + n);
            let scope = TransactionScope::enter(txn.clone());
            let entry = chunk
                .try_acquire(64, false)
                .expect("sequential transaction");
            let segment_id = entry.seg.id;
            if n > 1 {
                assert!(
                    ids.contains(&segment_id),
                    "later transactions must reuse earlier heads"
                );
            }
            ids.insert(segment_id);
            entry.append().unwrap();
            drop(entry);
            drop(scope);
            if abort {
                assert_eq!(
                    rewind_transaction_writes(&txn, &chunks.address_range()),
                    Ok(1)
                );
            } else {
                chunks.commit_transaction_brackets(&txn, true).expect("commit");
                assert_eq!(
                    release_transaction_leases(&txn, &chunks.address_range()),
                    Ok(1)
                );
            }
        }
        assert!(ids.len() < 200);
        assert!(chunk.seg_count() <= 2);
    }

    #[test]
    fn sequential_200_commits_reuse_heads_in_a_16_mb_chunk() {
        sequential_small_transactions_reuse_heads(false);
    }

    #[test]
    fn sequential_200_aborts_reuse_heads_in_a_16_mb_chunk() {
        sequential_small_transactions_reuse_heads(true);
    }

    /// An aborted transaction's writes VANISH: the span becomes ONE padding
    /// entry a walk steps over, and the head goes back to the pool with no
    /// bracket open in it.
    ///
    /// Only ownership makes this expressible. If another writer could have
    /// appended behind the transaction, its bytes would sit inside the span
    /// being disowned -- so the lease running to the decision is not a
    /// convenience here, it is the precondition.
    ///
    /// The cursor does NOT go back (see `Segment::disown_span`): the next
    /// ordinary entry lands BEHIND the padding, in the same head.
    #[test]
    fn an_aborted_transaction_leaves_no_entries_behind() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(77);

        let scope = TransactionScope::enter(txn.clone());
        let first = chunk.try_acquire(64, false).expect("first entry");
        let seg = first.seg.clone();
        // Where the transaction found the cursor. Its BEGIN is the first
        // thing in the run, so the span to disown starts at the BEGIN.
        let before = {
            let leases = TXN_LEASES.lock();
            leases
                .get(&txn)
                .unwrap()
                .per_chunk
                .get(&chunk.lease_key())
                .unwrap()
                .first()
                .unwrap()
                .pre_cursor
        };
        first.append().unwrap();
        drop(first);
        let second = chunk.try_acquire(64, false).expect("second entry");
        second.append().unwrap();
        drop(second);
        drop(scope);
        let after = seg.append_header.load(Ordering::Acquire);
        assert!(
            after > before,
            "the transaction must have advanced the cursor before we undo it"
        );
        assert!(seg.bracket_is_open(), "the BEGIN must be marked open");
        assert!(
            seg.try_release_own().is_err(),
            "a head with an open bracket and room behind it must never be released"
        );

        assert_eq!(
            rewind_transaction_writes(&txn, &chunks.address_range()),
            Ok(1)
        );
        assert_eq!(
            seg.append_header.load(Ordering::Acquire),
            after,
            "the cursor stays: the span is disowned, not handed out again"
        );
        // The bytes are still in the WAL, so what matters is that a walk over
        // them finds padding rather than entries -- one entry, covering the
        // whole span, BEGIN included.
        let (header, _) = crate::ram::entry::Entry::decode_from(before, |_, _| {});
        assert_eq!(
            header.entry_type,
            crate::ram::entry::EntryType::PADDING,
            "the disowned span must read as padding, not as the entries it used to hold"
        );
        assert_eq!(
            crate::ram::entry::ENTRY_HEAD_SIZE + header.content_length as usize,
            after - before,
            "one padding entry must cover the whole span"
        );
        assert_ne!(
            crate::ram::entry::verify_entry_at(before, seg.bound()),
            Some(false),
            "and a walk must accept it"
        );
        assert!(!seg.bracket_is_open());
        // The head is back in circulation, with the next entry behind the
        // padding.
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 0);
        let plain = chunk
            .try_acquire(64, false)
            .expect("plain entry after abort");
        assert_eq!(plain.seg.id, seg.id);
        assert_eq!(plain.addr, after);
    }

    /// A rewind that cannot be journaled is a typed error, the head is never
    /// reused, and nothing is ever appended behind the BEGIN it leaves.
    ///
    /// The failure injected is the real one: the head's journal gate closes,
    /// which is what a shutdown archive does to a head it seals.
    #[test]
    fn a_rewind_that_cannot_be_journaled_retires_the_head() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(78);

        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).expect("entry");
        let seg = entry.seg.clone();
        entry.append().unwrap();
        drop(entry);
        drop(scope);
        let begin_at = seg.addr;
        let cursor = seg.append_header.load(Ordering::Acquire);

        seg.close_journal_gate();
        let failures = TXN_REWIND_FAILURES.load(Ordering::Relaxed);
        match rewind_transaction_writes(&txn, &chunks.address_range()) {
            Err(LeaseExitError::RewindFailed { segment_id, .. }) => {
                assert_eq!(segment_id, seg.id)
            }
            other => panic!("expected a typed RewindFailed, got {other:?}"),
        }
        assert!(TXN_REWIND_FAILURES.load(Ordering::Relaxed) > failures);
        // The memory still agrees with the log: the BEGIN stands, uncommitted.
        let (header, _) = crate::ram::entry::Entry::decode_from(begin_at, |_, _| {});
        assert_eq!(header.entry_type, crate::ram::entry::EntryType::BEGIN);
        // And the head is RETIRED: full, so nothing lands behind that BEGIN.
        assert!(!seg.can_still_append());
        assert_eq!(seg.append_header.load(Ordering::Acquire), seg.bound());
        let (tail, _) = crate::ram::entry::Entry::decode_from(cursor, |_, _| {});
        assert_eq!(tail.entry_type, crate::ram::entry::EntryType::PADDING);
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 0);

        seg.reopen_journal_gate();
        let plain = chunk.try_acquire(64, false).expect("plain entry");
        assert_ne!(
            plain.seg.id, seg.id,
            "a retired head must never take another writer's entry"
        );
    }

    /// The class-closing invariant at the lease level: a plain release of a
    /// transaction that still has an open bracket is REFUSED, and changes
    /// nothing, so the right exit can still be taken afterwards.
    #[test]
    fn releasing_an_open_bracket_is_refused() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(79);

        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).expect("entry");
        let seg = entry.seg.clone();
        entry.append().unwrap();
        drop(entry);
        drop(scope);

        let refused = crate::ram::segs::OPEN_BRACKET_RELEASES_REFUSED.load(Ordering::Relaxed);
        match release_transaction_leases(&txn, &chunks.address_range()) {
            Err(LeaseExitError::OpenBracket(open)) => assert_eq!(open.segment_id, seg.id),
            other => panic!("expected the release to be refused, got {other:?}"),
        }
        assert!(crate::ram::segs::OPEN_BRACKET_RELEASES_REFUSED.load(Ordering::Relaxed) > refused);
        assert_eq!(
            transaction_lease_count(&txn, &chunks.address_range()),
            1,
            "a refused release must leave the lease exactly as it was"
        );
        assert!(seg.is_head_owned() && seg.bracket_is_open());

        // The ordinary writer that would have appended behind the BEGIN goes
        // elsewhere.
        let plain = chunk.try_acquire(64, false).expect("plain entry");
        assert_ne!(plain.seg.id, seg.id);
        plain.append().unwrap();
        drop(plain);

        // The single release point refuses on its own, too.
        assert!(seg.try_release_own().is_err());
        assert!(seg.is_head_owned());

        // Closing the bracket is what makes the release legal.
        chunks.commit_transaction_brackets(&txn, true).expect("commit");
        assert!(seg.bracket_is_committed());
        assert_eq!(
            release_transaction_leases(&txn, &chunks.address_range()),
            Ok(1)
        );
        assert!(!seg.is_head_owned());
    }

    /// INVARIANT B, the fence: a thread still writing for a transaction that
    /// has been ended -- the slow owner of an expired lease -- must fail,
    /// never land in the segment it used to own, which by then has been
    /// rewound and handed to another writer.
    #[test]
    fn a_stale_owner_cannot_write_after_its_transaction_was_ended() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(81);
        let range = chunks.address_range();

        // The owner: its scope stays live throughout, as a slow thread's would.
        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).expect("owner's first write");
        let seg = entry.seg.clone();
        entry.append().unwrap();
        drop(entry);

        // Expiry (= abort) on another thread: rewind, then the head goes back.
        std::thread::scope(|s| {
            s.spawn(|| assert_eq!(rewind_transaction_writes(&txn, &range), Ok(1)))
                .join()
                .unwrap();
            // Reassignment: an ordinary writer elsewhere takes the head.
            s.spawn(|| {
                let plain = chunks.list[0].try_acquire(64, false).expect("plain write");
                plain.append().unwrap();
                drop(plain);
            })
            .join()
            .unwrap();
        });
        assert!(!seg.bracket_is_open());

        // The owner wakes up and writes.
        let stale = TXN_STALE_OWNER_WRITES.load(Ordering::Relaxed);
        assert!(
            matches!(
                chunk.try_acquire(64, false),
                Err(WriteError::TransactionRevoked)
            ),
            "a stale owner's write must fail"
        );
        assert!(TXN_STALE_OWNER_WRITES.load(Ordering::Relaxed) > stale);
        assert_eq!(
            transaction_lease_count(&txn, &range),
            0,
            "and must not open a fresh bracket for a transaction that is gone"
        );
        drop(scope);
        assert!(
            TXN_LEASES.lock().get(&txn).is_none(),
            "the fence record lives only while a scope can still write"
        );
    }

    /// The fence also covers a write the stale owner had ALREADY claimed when
    /// the revocation came: the rewind waits for it to land and pads over it.
    #[test]
    fn a_rewind_waits_for_the_stale_owners_write_in_flight() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(82);
        let range = chunks.address_range();

        let scope = TransactionScope::enter(txn.clone());
        let first = chunk.try_acquire(64, false).expect("first write");
        first.append().unwrap();
        drop(first);
        let in_flight = chunk.try_acquire(64, false).expect("second write, claimed");
        let seg = in_flight.seg.clone();
        let in_flight_end = in_flight.addr + in_flight.size as usize;

        std::thread::scope(|s| {
            let rewind = s.spawn(|| rewind_transaction_writes(&txn, &range));
            std::thread::sleep(std::time::Duration::from_millis(200));
            assert!(
                !rewind.is_finished(),
                "the rewind must wait for the claimed write"
            );
            in_flight.append().unwrap();
            drop(in_flight);
            assert_eq!(rewind.join().unwrap(), Ok(1));
        });
        let (header, _) = crate::ram::entry::Entry::decode_from(seg.addr, |_, _| {});
        assert_eq!(header.entry_type, crate::ram::entry::EntryType::PADDING);
        assert!(
            seg.addr + crate::ram::entry::ENTRY_HEAD_SIZE + header.content_length as usize
                >= in_flight_end,
            "the padding must cover the write that was in flight"
        );
        drop(scope);
    }

    /// A transaction spanning chunks is rewound in every one of them, and
    /// fenced in every one of them.
    #[test]
    fn a_multi_chunk_transaction_is_rewound_and_fenced_everywhere() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(2, 2 * TEST_CHUNK_SIZE_MULTI);
        let txn = test_txn_id(83);
        let range = chunks.address_range();

        let scope = TransactionScope::enter(txn.clone());
        let segs: Vec<_> = chunks
            .list
            .iter()
            .map(|chunk| {
                let entry = chunk.try_acquire(64, false).expect("write");
                assert!(entry.leased);
                entry.seg.clone()
            })
            .collect();
        assert_eq!(transaction_lease_count(&txn, &range), 2);
        assert_eq!(rewind_transaction_writes(&txn, &range), Ok(2));
        for seg in &segs {
            let (header, _) = crate::ram::entry::Entry::decode_from(seg.addr, |_, _| {});
            assert_eq!(header.entry_type, crate::ram::entry::EntryType::PADDING);
            assert!(!seg.bracket_is_open() && !seg.is_head_owned());
        }
        for chunk in chunks.list.iter() {
            assert!(matches!(
                chunk.try_acquire(64, false),
                Err(WriteError::TransactionRevoked)
            ));
        }
        drop(scope);
    }

    /// No unbracketed fallback: a chunk that can never spare a head refuses a
    /// transactional write outright, typed, instead of writing it outside
    /// the bracket (where it would survive a crash its transaction does not).
    #[test]
    fn a_transaction_with_no_head_to_spare_fails_instead_of_writing_unbracketed() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(84);
        let scope = TransactionScope::enter(txn.clone());
        assert!(matches!(
            chunk.try_acquire(64, false),
            Err(WriteError::TransactionHeadUnavailable)
        ));
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 0);
        drop(scope);
        // Ordinary writes are unaffected.
        drop(chunk.try_acquire(64, false).expect("plain write"));
    }

    /// INVARIANT B at context exit: a scope restores what was named before,
    /// on a normal exit and on an unwind alike.
    #[test]
    fn a_transaction_scope_never_outlives_its_region() {
        let outer = test_txn_id(85);
        let inner = test_txn_id(86);
        {
            let _outer = TransactionScope::enter(outer.clone());
            {
                let _inner = TransactionScope::enter(inner.clone());
                assert_eq!(current_txn(), Some(inner.clone()));
            }
            assert_eq!(current_txn(), Some(outer.clone()));
            let unwound = std::panic::catch_unwind(|| {
                let _inner = TransactionScope::enter(inner.clone());
                panic!("dies inside the transaction");
            });
            assert!(unwound.is_err());
            assert_eq!(current_txn(), Some(outer.clone()), "restored on unwind");
        }
        assert_eq!(current_txn(), None);
        assert!(!is_in_transaction());
        assert!(TXN_LEASES.lock().get(&outer).is_none());
        assert!(TXN_LEASES.lock().get(&inner).is_none());
    }

    /// The commit reserve: a part whose write set runs it to the last bytes
    /// still closes with its OWN COMMIT, so no part ever depends on another
    /// part's record (which the cleaner may compact away).
    #[test]
    fn a_full_part_still_closes_with_its_own_commit() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(80);

        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).expect("entry");
        let first_seg = entry.seg.clone();
        entry.append().unwrap();
        drop(entry);
        // Fill the part: ask for exactly what is left. The reserve refuses
        // it, so it continues in a fresh part instead of eating the room the
        // first part needs for its COMMIT.
        let left = first_seg.bound() - first_seg.append_header.load(Ordering::Acquire);
        let filler = chunk.try_acquire(left as u32, false).expect("filler");
        assert_ne!(filler.seg.id, first_seg.id, "the reserve must not be spent");
        let second_seg = filler.seg.clone();
        filler.append().unwrap();
        drop(filler);
        // And the largest entry the first part can still take leaves exactly
        // the reserve.
        let fits = first_seg.bound()
            - first_seg.append_header.load(Ordering::Acquire)
            - crate::ram::bracket::MIN_COMMIT_ENTRY_SIZE;
        assert!(first_seg
            .try_acquire_reserving(fits as u32 + 8, crate::ram::bracket::MIN_COMMIT_ENTRY_SIZE)
            .is_none());
        drop(scope);

        chunks.commit_transaction_brackets(&txn, true).expect("commit");
        for seg in [&first_seg, &second_seg] {
            let types = entry_types_in(seg);
            assert_eq!(types.first(), Some(&crate::ram::entry::EntryType::BEGIN));
            assert_eq!(
                types.last(),
                Some(&crate::ram::entry::EntryType::COMMIT),
                "every part must close with its own COMMIT: {types:?}"
            );
        }
        assert_eq!(
            release_transaction_leases(&txn, &chunks.address_range()),
            Ok(2)
        );
    }

    /// Shutdown stops CLIENT writes before the flush, and lets the index's
    /// own writes through.
    ///
    /// Both halves matter and they pull opposite ways. Leave client writes
    /// open and one acked after the flush loses its index entries -- the cell
    /// is archived and survives, the entries never reach disk. Close
    /// everything instead and the write-back's own tree pages, which are
    /// cells in these same chunks, are refused: that killed the ranged index
    /// at shutdown while every data cell survived, and is why the close sits
    /// at step 1.6 in the first place.
    #[test]
    fn shutdown_stops_client_writes_before_index_writes() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];

        // A client schema, and the two the index writes itself.
        let client_schema = SchemaVid(4242);
        let page_schema = *crate::index::ranged::tree::btree::PAGE_SCHEMA_ID;
        let tree_schema = *crate::index::ranged::tree::tree::RANGED_TREE_SCHEMA_ID;
        assert_ne!(client_schema, page_schema);
        assert_ne!(client_schema, tree_schema);

        // Open for business: everything is allowed.
        assert!(chunk.shutdown_refuses(client_schema).is_ok());
        assert!(chunk.shutdown_refuses(page_schema).is_ok());

        chunks.close_client_writes();

        assert!(
            matches!(
                chunk.shutdown_refuses(client_schema),
                Err(WriteError::ServerShuttingDown)
            ),
            "a client write must be refused once shutdown starts, so nothing it \
             is told succeeded can land after the flush"
        );
        assert!(
            chunk.shutdown_refuses(page_schema).is_ok(),
            "the write-back's own pages must still be placeable, or the flush \
             this close exists to protect cannot run"
        );
        assert!(
            chunk.shutdown_refuses(tree_schema).is_ok(),
            "and so must the tree metadata the flush rewrites"
        );

        // Stage two closes everything, index included.
        chunks.close_writes();
        assert!(
            chunk.try_acquire(64, false).is_err(),
            "after close_writes nothing may allocate at all"
        );
    }

    /// A transaction that writes both segment classes gets a bracket in EACH,
    /// and neither chain reaches for the other's head.
    ///
    /// Blob cells used to sit outside the bracket entirely. The reason was
    /// mechanical rather than principled: the lease lookup asked for "the
    /// newest head this transaction holds in this chunk", which after a blob
    /// write would hand the next regular write a blob head -- so the gate
    /// simply refused to bracket blobs at all. Every lookup is scoped to the
    /// class now, which is what lets the two chains exist side by side, and
    /// the COMMIT goes into both.
    #[test]
    fn a_transaction_brackets_both_segment_classes() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(91);

        let scope = TransactionScope::enter(txn.clone());
        let regular = chunk
            .try_acquire_in_class(64, false, SegmentClass::Regular)
            .expect("regular entry");
        assert!(
            regular.leased,
            "a transactional regular claim takes a lease"
        );
        let regular_seg = regular.seg.id;
        let regular_seg_ref = regular.seg.clone();
        drop(regular);

        let blob = chunk
            .try_acquire_in_class(64, false, SegmentClass::Blob)
            .expect("blob entry");
        assert!(
            blob.leased,
            "a blob write inside a transaction must be bracketed too"
        );
        assert_eq!(blob.seg.segment_class(), SegmentClass::Blob);
        assert_ne!(
            blob.seg.id, regular_seg,
            "the classes must not share a segment"
        );
        let blob_seg_ref = blob.seg.clone();
        drop(blob);
        assert_eq!(
            transaction_lease_count(&txn, &chunks.address_range()),
            2,
            "one head per class, not one per chunk"
        );

        // Back to the regular class: it must find its OWN head, not the blob
        // head that happens to have been leased more recently.
        let regular_again = chunk
            .try_acquire_in_class(64, false, SegmentClass::Regular)
            .expect("second regular entry");
        assert!(regular_again.leased);
        assert_eq!(
            regular_again.seg.id, regular_seg,
            "the newer blob lease must not displace the regular chain"
        );
        drop(regular_again);
        assert_eq!(
            transaction_lease_count(&txn, &chunks.address_range()),
            2,
            "and continuing a chain opens no further lease"
        );

        // Both parts are named by the manifest, so the transaction is decided
        // as a whole rather than one class at a time.
        let manifest = transaction_manifest(&txn, &chunks.address_range());
        assert_eq!(
            manifest.len(),
            2,
            "the manifest must name the blob part as well: {:?}",
            manifest
        );
        chunk
            .close_transaction_bracket(&txn, &manifest)
            .expect("close both brackets");

        // What actually makes an entry transactional is the pair of markers
        // around it, NOT the lease -- a leased head with no BEGIN recovers as
        // ordinary entries, so an uncommitted transaction's blob cells would
        // come back visible. Assert the markers, in both classes.
        for (class, seg) in [
            (SegmentClass::Regular, regular_seg_ref),
            (SegmentClass::Blob, blob_seg_ref),
        ] {
            let types = entry_types_in(&seg);
            assert_eq!(
                types.first(),
                Some(&crate::ram::entry::EntryType::BEGIN),
                "{:?} part must open with a BEGIN, got {:?}",
                class,
                types
            );
            assert_eq!(
                types.last(),
                Some(&crate::ram::entry::EntryType::COMMIT),
                "{:?} part must close with a COMMIT, got {:?}",
                class,
                types
            );
        }

        drop(scope);
        assert_eq!(
            release_transaction_leases(&txn, &chunks.address_range()),
            Ok(2)
        );
    }

    /// Walk a segment's live extent and report what kind of entry sits where.
    /// Only the shape matters here, so the content read is a no-op.
    fn entry_types_in(seg: &AArc<Segment>) -> Vec<crate::ram::entry::EntryType> {
        let end = seg.append_header.load(Ordering::Acquire);
        let mut cursor = seg.addr;
        let mut types = Vec::new();
        while cursor < end {
            let (header, _) = crate::ram::entry::Entry::decode_from(cursor, |_, _| {});
            let size = crate::ram::entry::ENTRY_HEAD_SIZE + header.content_length as usize;
            if size == 0 {
                break;
            }
            types.push(header.entry_type);
            cursor += size;
        }
        types
    }

    /// A head held past its transaction is a writer slot gone from the pool,
    /// and the pool has no refill path -- so a transaction that never
    /// reaches a decision must have its heads reclaimed on a timer.
    ///
    /// Reclaimed by RETIRING: the sweeper knows nothing about the cell index,
    /// so it leaves the bracket exactly as it is (uncommitted, discarded by
    /// recovery) and fills the head, which the next writer rotates out. The
    /// slot comes back; the segment with the open BEGIN never takes another
    /// writer's entry.
    #[test]
    fn an_undecided_transaction_has_its_heads_reclaimed() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_CHUNK_SIZE_MULTI);
        let chunk = &chunks.list[0];
        let txn = test_txn_id(42);

        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).expect("entry");
        let seg = entry.seg.clone();
        entry.append().unwrap();
        drop(entry);
        drop(scope);
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 1);

        // Not yet: a transaction that is merely slow keeps its heads.
        let expired = expire_transaction_leases(
            std::time::Duration::from_secs(3600),
            &chunks.address_range(),
            &Default::default(),
        );
        assert!(
            expired.is_empty(),
            "a lease inside its window must not be reclaimed"
        );
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 1);

        // Past the window, the head comes back and the caller is told which
        // transaction to settle.
        let expired = expire_transaction_leases(
            std::time::Duration::from_secs(0),
            &chunks.address_range(),
            &Default::default(),
        );
        assert!(
            expired.contains(&txn),
            "an undecided transaction must be reported so it can be settled"
        );
        assert_eq!(transaction_lease_count(&txn, &chunks.address_range()), 0);
        assert!(!seg.is_head_owned(), "the head must not stay a hostage");
        assert!(
            !seg.can_still_append(),
            "the head still holds an open BEGIN, so it must be full"
        );

        let plain = chunk.try_acquire(64, false).expect("entry after expiry");
        assert_ne!(
            plain.seg.id, seg.id,
            "nothing may be appended behind the expired transaction's BEGIN"
        );
    }

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
    fn a_writer_inside_its_own_qsbr_section_still_reclaims_during_emergency() {
        // The shape of the 13.6M-edge stall: the thread asking for the
        // collection is itself inside a section, so the collection sees a
        // reader that never leaves. Suspending the section for the duration
        // of the collection is what lets it reclaim.
        let _ = env_logger::try_init();
        let (chunks, _schema) = setup_test_chunks();
        let chunk = &chunks.list[0];
        let seg_id = chunk.get_head_seg_id();
        let _section = crate::ram::qsbr::QsbrSection::new();
        chunk.rotate_heads_for_scrub_test();
        chunk.remove_segment(seg_id);
        assert_eq!(chunk.retired_segment_count(), 1);
        assert_eq!(
            chunk.drain_retired_segments(),
            0,
            "inside our own section the retirement must still be blocked -- by us"
        );
        let freed = crate::ram::qsbr::segment_qsbr()
            .suspended(|| drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)));
        assert_eq!(freed, 1, "suspended, the same thread's collection reclaims");
        assert_eq!(chunk.retired_segment_count(), 0);
    }

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
        chunk_b.rotate_heads_for_scrub_test();
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
        let mut cell = payload_cell(schema_a.vid.get(), &id, 32);
        chunks_a
            .write_cell(&mut cell)
            .expect("write a cell to read");

        // A read in progress in database A: the guard is the open section.
        let reading = CellGuard::for_read(id.bits(), chunk_a).expect("read guard");

        let b_seg_id = chunk_b.get_head_seg_id();
        chunk_b.rotate_heads_for_scrub_test();
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
        chunk.rotate_heads_for_scrub_test();
        chunk.remove_segment(seg_id);
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)),
            1,
            "a reference acquired and released on different threads stranded reclamation"
        );
        finish_tx.send(()).unwrap();
        pinning.join().expect("pinning thread");
    }

    #[test]
    fn delete_reserves_before_locking_the_cell() {
        delete_head_lock_order(0);
    }

    #[test]
    fn migration_delete_reserves_before_locking_the_cell() {
        delete_head_lock_order(1);
    }

    #[test]
    fn conditional_delete_reserves_before_locking_the_cell() {
        delete_head_lock_order(2);
    }

    fn delete_head_lock_order(path: usize) {
        let schema = Schema::new(
            "delete_head_order",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("id", Type::I32),
                Field::new_unindexed_array("data", Type::U8),
            ]),
            false,
            false,
        );
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        chunks.stop_background();
        let id = Id::allocated(1, 0, 123);
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        chunks.write_cell(&mut cell).unwrap();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (owned_tx, owned_rx) = std::sync::mpsc::channel();
        let writer_chunks = chunks.clone();
        let writer = std::thread::spawn(move || {
            start_rx.recv().unwrap();
            let chunk = &writer_chunks.list[0];
            let pending = chunk.try_acquire(64, true).unwrap();
            owned_tx.send(()).unwrap();
            // Model an updater holding the only head before taking its word.
            // Release on timeout so the reverted control fails an assertion.
            let started = std::time::Instant::now();
            let obtained = loop {
                if let Some(Some(guard)) = chunk.cell_index.try_lock(id.bits() as usize) {
                    drop(guard);
                    break true;
                }
                if started.elapsed() >= std::time::Duration::from_secs(1) {
                    break false;
                }
                std::thread::yield_now();
            };
            drop(pending);
            obtained
        });
        TEST_DELETE_BEFORE_RESERVE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                start_tx.send(()).unwrap();
                owned_rx.recv().unwrap();
            }))
        });
        let chunk = &chunks.list[0];
        let result = match path {
            0 => chunk.remove_cell(id.bits()),
            1 => chunk.remove_cell_keeping_indices(id.bits()),
            _ => chunk.remove_cell_by(id.bits(), |_| true),
        };
        let obtained = writer.join().unwrap();
        result.expect("the delete must finish after the head becomes available");
        assert!(
            obtained,
            "delete held the cell word while waiting for the head owner"
        );
        assert!(matches!(
            chunks.read_cell(&id),
            Err(ReadError::CellDoesNotExisted)
        ));
    }

    /// A best-effort allocation must return while another cleaner owns the
    /// GC lock. The caller holds a cell-index lock that the cleaner needs.
    #[test]
    fn an_update_does_not_wait_for_gc_while_holding_the_cell_index() {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("gc_index_lock_cycle", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            4 * SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let chunk = &chunks.list[0];
        let id = Id::allocated(1, 0, 123);
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        let before = chunks.write_cell(&mut cell).unwrap();
        while chunk.allocator.writer_headroom() > 1 {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            chunk.put_segment(seg);
        }
        assert_eq!(chunk.allocator.writer_headroom(), 1);
        for slot in chunk.head_pool.iter() {
            let id = slot.load(Ordering::Acquire);
            if let Some(seg) = chunk.segs.get(&(id as usize)) {
                seg.retire_tail();
            }
        }

        // Model the cleaner's GC lock. Keep it held until the update has
        // returned. A cleaner can need the same cell-index lock during combine.
        let gc = chunk.gc_lock.lock();
        let (tx, rx) = std::sync::mpsc::channel();
        let writer_chunks = chunks.clone();
        let schema_id = schema.vid;
        let writer = std::thread::spawn(move || {
            let result =
                writer_chunks.update_cell_by(&id, |_| Some(payload_cell(schema_id.get(), &id, 64)));
            tx.send(result).unwrap();
        });
        let result = rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(gc);
        writer.join().unwrap();
        let updated = result
            .expect("the update waited for GC while holding the cell-index lock")
            .expect("the last writer segment can hold the update");
        assert_eq!(updated.header.version, before.version + 1);
        assert_eq!(
            chunks.read_cell(&id).unwrap().header.version,
            updated.header.version
        );
    }

    fn delete_at_the_wall_does_not_wait_for_gc(transactional: bool) {
        let (_, schema) = setup_test_chunks();
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            4 * SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let chunk = &chunks.list[0];
        let id = Id::allocated(0, 0, 99123);
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        let before = chunks.write_cell(&mut cell).unwrap();
        // Apply normally captures this source before a cleaner can start.
        let _source_pin = transactional.then(|| {
            crate::ram::segs::SegmentReferenceGuard::new(
                chunk.locate_segment(chunks.address_of(&id)).unwrap(),
            )
            .unwrap()
        });
        let txn = test_txn_id(99123);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let (guard_tx, guard_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer_chunks = chunks.clone();
        let writer_txn = txn.clone();
        let writer = std::thread::spawn(move || {
            let scope = transactional.then(|| TransactionScope::enter(writer_txn));
            if transactional {
                let mut inserted =
                    payload_cell(cell.header.schema.get(), &Id::allocated(0, 0, 99124), 64);
                writer_chunks.write_cell(&mut inserted).unwrap();
            }
            ready_tx.send(()).unwrap();
            go_rx.recv().unwrap();
            let checks = std::cell::Cell::new(0);
            let predict = |_: &SharedCell| {
                checks.set(checks.get() + 1);
                if checks.get() == 2 {
                    guard_tx.send(()).unwrap();
                }
                true
            };
            let result = if transactional {
                writer_chunks.apply_transaction_remove(&id, predict)
            } else {
                writer_chunks.remove_cell_by(&id, predict)
            };
            if checks.get() < 2 {
                guard_tx.send(()).unwrap();
            }
            drop(scope);
            done_tx.send(result).unwrap();
        });
        ready_rx.recv().unwrap();
        while chunk.allocator.writer_headroom() > 1 {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            let seg_id = seg.id;
            chunk.put_segment(seg);
            if let Some(slot) = chunk
                .head_pool
                .iter()
                .find(|slot| slot.load(Ordering::Acquire) == HEAD_SEG_ID_EMPTY)
            {
                slot.store(seg_id, Ordering::Release);
            }
        }
        assert_eq!(chunk.allocator.writer_headroom(), 1);
        for seg in chunk.segments() {
            if seg.bracket_is_open() {
                seg.retire_tail();
            } else {
                // Leave a live ordinary head that is too small for a tombstone or BEGIN.
                let cursor = seg.append_header.load(Ordering::Acquire);
                let end = seg.bound() - 8;
                crate::ram::entry::stamp_checked_padding(cursor, (end - cursor) as u32);
                seg.append_header.store(end, Ordering::Release);
            }
        }
        let (gc_ready_tx, gc_ready_rx) = std::sync::mpsc::channel();
        let (probe_tx, probe_rx) = std::sync::mpsc::channel();
        let cleaner_chunks = chunks.clone();
        let cleaner = std::thread::spawn(move || {
            Cleaner::clean_with_test_probe(&cleaner_chunks.list[0], move |chunk| {
                // Pause a real round after it takes gc_lock, before it probes X.
                gc_ready_tx.send(()).unwrap();
                guard_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                probe_tx.send(chunk.head_cell(id.bits())).unwrap();
            });
        });
        gc_ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        go_tx.send(()).unwrap();
        let result = done_rx.recv_timeout(std::time::Duration::from_secs(5));
        assert!(
            result.is_ok(),
            "delete waited for GC under the cell-index lock"
        );
        writer.join().unwrap();
        cleaner.join().unwrap();
        let probed = probe_rx.recv().unwrap();
        match result.expect("delete waited for GC under the cell-index lock") {
            Ok(()) => assert!(probed.is_err()),
            Err(WriteError::CannotAllocateSpace | WriteError::TransactionHeadUnavailable) => {
                assert_eq!(probed.unwrap().version, before.version);
            }
            other => panic!("unexpected delete result: {:?}", other),
        }
        if transactional {
            retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
        }
    }

    #[test]
    fn transactional_delete_at_the_wall_does_not_wait_for_gc_under_the_cell_lock() {
        delete_at_the_wall_does_not_wait_for_gc(true);
    }

    #[test]
    fn conditional_delete_at_the_wall_does_not_wait_for_gc_under_the_cell_lock() {
        delete_at_the_wall_does_not_wait_for_gc(false);
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
        assert!(
            !hoarded.is_empty(),
            "the allocator should have had segments"
        );

        // Fill the published head. The write that overflows it is the one that
        // has to allocate, and the allocator has nothing left to give.
        let mut refusal = None;
        for i in 0..5_000u64 {
            let id = Id::allocated(1, 0, i + 1);
            let mut cell = payload_cell(schema.vid.get(), &id, 4096);
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
        let schema_id = schema.vid;
        std::thread::spawn(move || {
            let id = Id::allocated(1, 0, 9_999_999);
            let mut cell = payload_cell(schema_id.get(), &id, 4096);
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
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        chunks
            .write_cell(&mut cell)
            .expect("write the cell to read back");

        let addr = {
            let stored = chunks.read_cell(&id).expect("read the cell");
            stored.cell_guard().get_ptr()
        };
        let segment_id = chunk.get_head_seg_id();
        let segment = chunk
            .segs
            .get(&(segment_id as usize))
            .expect("segment is published");
        // Seal before the reader pins the segment; sealing requires exclusive access.
        chunk.rotate_heads_for_scrub_test();
        assert!(
            segment.incr_references(),
            "the reader takes a live reference, as the read paths do"
        );

        // What the reader can see before the segment is taken away.
        let before = unsafe { std::slice::from_raw_parts(addr as *const u8, 64) }.to_vec();
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
        // Seal before the reader pins the segment; sealing requires exclusive access.
        chunk.rotate_heads_for_scrub_test();
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
        setup_test_chunks_with_size(TEST_CHUNK_SIZE)
    }

    fn setup_test_chunks_with_size(size: usize) -> (Arc<Chunks>, Schema) {
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("cell_stored_len_test", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            size,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        (chunks, schema)
    }

    fn payload_cell(schema_id: u32, id: &Id, payload_len: usize) -> OwnedCell {
        let data: Vec<u8> = std::iter::repeat(id.bits() as u8)
            .take(payload_len)
            .collect();
        OwnedCell {
            header: CellHeader::new(SchemaVid(schema_id), id),
            data: data_map_value!(id: id.bits() as i32, data: data),
        }
    }

    #[test]
    fn cas_allocation_waits_for_inflight_cleaner() {
        use std::sync::mpsc;
        use std::time::Duration;
        let (chunks, schema) = setup_test_chunks_with_size(12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = Id::allocated(1, 0, 42);
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        let version = chunks.write_cell(&mut cell).unwrap().version;
        for slot in chunk.head_pool.iter() {
            let mut id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                id = seg.id;
                chunk.put_segment(seg);
                slot.store(id, Ordering::Release);
            }
            chunk.segs.get(&(id as usize)).unwrap().retire_tail();
        }
        let mut pins = Vec::new();
        while chunk.allocator.writer_headroom() > 0 {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            let id = seg.id;
            chunk.put_segment(seg);
            pins.push((
                id,
                SegmentReferenceGuard::new(chunk.segs.get(&(id as usize)).unwrap()).unwrap(),
            ));
        }
        let gc = chunk.gc_lock.lock();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                TEST_CAS_HEAD_WAIT.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        ready_tx.send(()).unwrap();
                    }))
                });
                result_tx
                    .send(chunks.list[0].compare_version_and_update_cell(
                        id.bits(),
                        version,
                        &mut cell,
                    ))
                    .unwrap();
            }
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let early = result_rx.recv_timeout(Duration::from_millis(200));
        // Finish the simulated cleaner pass by retiring an unowned segment.
        // No active head or transaction-owned segment is compacted.
        let (victim, pin) = pins.pop().unwrap();
        drop(pin);
        chunk.remove_segment(victim);
        drop(gc);
        let result =
            early.unwrap_or_else(|_| result_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        assert!(
            result.is_ok(),
            "CAS refused while a cleaner could release space: {:?}",
            result
        );
        assert!(chunks.head_cell(&id).unwrap().version > version);
        drop(pins);
    }

    #[test]
    fn allocation_waits_for_pending_seals_before_repeating_gc() {
        allocation_waits_for_reclamation(false);
    }

    #[test]
    fn allocation_waits_for_retired_readers_before_refusing_capacity() {
        allocation_waits_for_reclamation(true);
    }

    fn assert_delete_head_released(seg: &AArc<Segment>, cursor: usize) {
        assert_eq!(seg.append_header.load(Ordering::Acquire), cursor);
        assert_eq!(seg.references_count(), 0);
        let seg = seg.clone();
        assert!(std::thread::spawn(move || {
            let owned = seg.try_own();
            if owned { seg.release_own(); }
            owned
        }).join().unwrap(), "delete left the head owned");
    }

    #[test]
    fn review_false_predicate_and_absent_deletes_at_full_capacity() {
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = Id::allocated(0, 0, 99701);
        chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let seg = chunk.locate_segment(chunks.address_of(&id)).unwrap();
        seg.retire_tail();
        assert!(matches!(chunk.try_acquire(32, false), Err(WriteError::CannotAllocateSpace)));
        let cursor = seg.append_header.load(Ordering::Acquire);
        assert!(matches!(chunks.remove_cell_by(&id, |_| false), Err(WriteError::CellDoesNotExisted)));
        let absent = Id::allocated(0, 0, 99702);
        assert!(matches!(chunks.remove_cell(&absent), Err(WriteError::CellDoesNotExisted)));
        assert!(matches!(chunks.remove_cell_keeping_indices(&absent), Err(WriteError::CellDoesNotExisted)));
        assert!(matches!(chunks.remove_cell_by(&absent, |_| true), Err(WriteError::CellDoesNotExisted)));
        assert_delete_head_released(&seg, cursor);
    }

    #[test]
    fn review_delete_removed_between_guards_releases_reservation() {
        for path in 0..3 {
            let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
            chunks.stop_background();
            let chunk = &chunks.list[0];
            let id = Id::allocated(0, 0, 99703);
            chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
            let seg = chunk.locate_segment(chunks.address_of(&id)).unwrap();
            let cursor = seg.append_header.load(Ordering::Acquire);
            TEST_DELETE_AFTER_RESERVE.with(|hook| {
                let chunks = chunks.clone();
                *hook.borrow_mut() = Some(Box::new(move || {
                    CellGuard::for_read(id.bits(), &chunks.list[0]).unwrap().remove_cell();
                }));
            });
            let result = match path {
                0 => chunks.remove_cell(&id),
                1 => chunks.remove_cell_keeping_indices(&id),
                _ => chunks.remove_cell_by(&id, |_| true),
            };
            assert!(matches!(result, Err(WriteError::CellDoesNotExisted)));
            assert_delete_head_released(&seg, cursor);
        }
    }

    #[test]
    fn review_delete_changed_predicate_releases_reservation() {
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = Id::allocated(0, 0, 99704);
        chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let seg = chunk.locate_segment(chunks.address_of(&id)).unwrap();
        let cursor = seg.append_header.load(Ordering::Acquire);
        let calls = std::cell::Cell::new(0);
        let result = chunks.remove_cell_by(&id, |_| {
            calls.set(calls.get() + 1);
            calls.get() == 1
        });
        assert!(matches!(result, Err(WriteError::CellDoesNotExisted)));
        assert_eq!(calls.get(), 2);
        assert!(chunks.read_cell(&id).is_ok());
        assert_delete_head_released(&seg, cursor);
    }

    #[test]
    fn review_transaction_delete_version_mismatch_releases_reservation() {
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = Id::allocated(0, 0, 99705);
        let first = chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let old_ptr = chunks.address_of(&id);
        let second = chunks.update_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let new_ptr = chunks.address_of(&id);
        assert_ne!(first.version, second.version);
        CellGuard::for_read(id.bits(), chunk).unwrap().set_ptr(old_ptr);
        let seg = chunk.locate_segment(old_ptr).unwrap();
        let cursor = seg.append_header.load(Ordering::Acquire);
        TEST_DELETE_AFTER_RESERVE.with(|hook| {
            let chunks = chunks.clone();
            *hook.borrow_mut() = Some(Box::new(move || {
                CellGuard::for_read(id.bits(), &chunks.list[0]).unwrap().set_ptr(new_ptr);
            }));
        });
        // apply_transaction_remove also supports an already resolved apply;
        // no active lease is needed to exercise its version predicate.
        let result = chunks.apply_transaction_remove(&id, |cell| cell.header.version == first.version);
        assert!(matches!(result, Err(WriteError::CellDoesNotExisted)));
        assert_eq!(chunks.read_cell(&id).unwrap().header.version, second.version);
        assert_delete_head_released(&seg, cursor);
    }

    #[test]
    fn review_delete_bad_second_schema_releases_reservation() {
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = Id::allocated(0, 0, 99706);
        chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let original = chunks.address_of(&id);
        let seg = chunk.locate_segment(original).unwrap();
        let cursor = seg.append_header.load(Ordering::Acquire);
        TEST_DELETE_AFTER_RESERVE.with(|hook| {
            let chunks = chunks.clone();
            *hook.borrow_mut() = Some(Box::new(move || {
                // Point at valid mapped padding, not a CELL entry.
                CellGuard::for_read(id.bits(), &chunks.list[0]).unwrap().set_ptr(cursor);
            }));
        });
        let result = chunks.remove_cell_by(&id, |_| true);
        assert!(matches!(result, Err(WriteError::ReadError(_))));
        assert_delete_head_released(&seg, cursor);
        chunk.lock_or_insert_cell(id.bits()).set_ptr(original);
    }

    #[test]
    fn review_pending_seal_without_progress_refuses_within_deadline() {
        use std::time::{Duration, Instant};
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        for slot in chunk.head_pool.iter() {
            let mut id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                id = seg.id;
                chunk.put_segment(seg);
                slot.store(id, Ordering::Release);
            }
            chunk.segs.get(&(id as usize)).unwrap().retire_tail();
        }
        while chunk.allocator.writer_headroom() > 0 {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            seg.protection.seal_pending.store(true, Ordering::Release);
            chunk.put_segment(seg);
        }
        let (tx, rx) = std::sync::mpsc::channel();
        let started = Instant::now();
        let worker = std::thread::spawn(move || {
            let result = chunks.list[0].try_acquire(64, true).map(drop);
            let _ = tx.send(result);
        });
        let result = rx.recv_timeout(HEAD_QUEUE_DEADLINE + Duration::from_secs(5))
            .expect("capacity retry exceeded its deadline plus an uncontended GC margin");
        worker.join().unwrap();
        assert!(matches!(result, Err(WriteError::CannotAllocateSpace)));
        assert!(started.elapsed() >= HEAD_QUEUE_DEADLINE, "pending work was not awaited");
    }

    #[test]
    fn review_own_transaction_lease_does_not_wait_for_capacity_progress() {
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let txn = test_txn_id(99707);
        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).unwrap();
        assert!(entry.leased);
        drop(entry);
        // A GC owner would count as pending without the own-lease check.
        let gc = chunk.gc_lock.lock();
        let mut since = None;
        assert!(!chunk.wait_for_capacity_progress(true, SegmentClass::Regular, &mut since));
        assert!(since.is_none());
        drop(gc);
        drop(scope);
        retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
    }

    #[test]
    fn review_transaction_pinned_seal_does_not_cause_capacity_wait() {
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let txn = test_txn_id(99708);
        let scope = TransactionScope::enter(txn.clone());
        let entry = chunk.try_acquire(64, false).unwrap();
        let seg = entry.seg.clone();
        drop(entry);
        drop(scope);
        seg.protection.seal_pending.store(true, Ordering::Release);
        assert!(seg.protection.transaction_pending());
        let mut since = None;
        assert!(!chunk.wait_for_capacity_progress(true, SegmentClass::Regular, &mut since));
        assert!(since.is_none());
        seg.protection.seal_pending.store(false, Ordering::Release);
        retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "capacity progress caller holds a cell word")]
    fn review_capacity_wait_rejects_a_held_cell_word() {
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let id = Id::allocated(0, 0, 99709);
        chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let chunk = &chunks.list[0];
        let _guard = CellGuard::for_read(id.bits(), chunk).unwrap();
        // Best-effort callers must keep their nonblocking path.
        assert!(!chunk.wait_for_capacity_progress(false, SegmentClass::Regular, &mut None));
        chunk.wait_for_capacity_progress(true, SegmentClass::Regular, &mut None);
    }

    #[test]
    fn review_rotation_allocator_refusal_waits_for_pending_seals() {
        use std::time::Duration;
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        for slot in chunk.head_pool.iter() {
            let mut id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                id = seg.id;
                chunk.put_segment(seg);
                slot.store(id, Ordering::Release);
            }
            chunk.segs.get(&(id as usize)).unwrap().retire_tail();
        }
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                TEST_BEFORE_ROTATION_ALLOCATE.with(|hook| {
                    let chunks = chunks.clone();
                    *hook.borrow_mut() = Some(Box::new(move || {
                        let chunk = &chunks.list[0];
                        let mut seals = Vec::new();
                        while chunk.allocator.writer_headroom() > 0 {
                            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                            seg.protection.seal_pending.store(true, Ordering::Release);
                            seals.push((seg.id, seg.seq_id));
                            chunk.put_segment(seg);
                        }
                        ready_tx.send(seals).unwrap();
                    }));
                });
                let result = chunks.list[0].try_acquire(64, true).map(drop);
                done_tx.send(result).unwrap();
            }
        });
        let seals = ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(100));
        for (id, seq) in seals { chunk.finish_rotated_head(id, seq); }
        let waited = early.is_err();
        let result = early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        writer.join().unwrap();
        assert!(waited, "allocator refusal did not wait for pending seals");
        assert!(result.is_ok(), "{:?}", result);
    }

    #[test]
    fn review_delete_and_word_first_update_cycle_is_bounded() {
        use std::time::{Duration, Instant};
        let (chunks, schema) = setup_test_chunks_with_size(SEGMENT_SIZE);
        chunks.stop_background();
        let id = Id::allocated(0, 0, 99710);
        chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64)).unwrap();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (word_tx, word_rx) = std::sync::mpsc::channel();
        let (update_tx, update_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                start_rx.recv().unwrap();
                let result = chunks.update_cell_by(&id, |_| {
                    word_tx.send(()).unwrap();
                    Some(payload_cell(schema.vid.get(), &id, 64))
                });
                update_tx.send(result).unwrap();
            }
        });
        let (delete_tx, delete_rx) = std::sync::mpsc::channel();
        let started = Instant::now();
        let deleter = std::thread::spawn(move || {
            TEST_DELETE_AFTER_RESERVE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    start_tx.send(()).unwrap();
                    word_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }));
            });
            let _ = delete_tx.send(chunks.remove_cell(&id));
        });
        let updated = update_rx.recv_timeout(HEAD_QUEUE_DEADLINE + Duration::from_secs(5))
            .expect("word-first updater exceeded the head-queue deadline");
        assert!(matches!(updated, Err(WriteError::CannotAllocateSpace)));
        assert!(started.elapsed() >= HEAD_QUEUE_DEADLINE);
        assert!(delete_rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
        updater.join().unwrap();
        deleter.join().unwrap();
    }

    #[test]
    fn review_delete_wal_failure_releases_reservation() {
        for path in 0..3 {
            let (_, schema) = setup_test_chunks();
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(schema.clone());
            let directory = tempfile::TempDir::new().unwrap();
            let chunks = Chunks::new(
                1, SEGMENT_SIZE, Arc::new(ServerMeta { schemas }), None, None,
                Some(directory.path().to_string_lossy().into_owned()), None,
            );
            chunks.stop_background();
            let chunk = &chunks.list[0];
            let id = Id::allocated(0, 0, 99711);
            let (written, _) = crate::ram::durability::capture(|| {
                chunks.write_cell(&mut payload_cell(schema.vid.get(), &id, 64))
            });
            let before = written.unwrap();
            let seg = chunk.locate_segment(chunks.address_of(&id)).unwrap();
            let cursor = seg.append_header.load(Ordering::Acquire);
            TEST_DELETE_AFTER_RESERVE.with(|hook| {
                let seg = seg.clone();
                *hook.borrow_mut() = Some(Box::new(move || {
                    seg.durable_file().unwrap().inject_failure();
                    assert!(seg.force_wal_sync().is_err());
                }));
            });
            let result = match path {
                0 => chunks.remove_cell(&id),
                1 => chunks.remove_cell_keeping_indices(&id),
                _ => chunks.remove_cell_by(&id, |_| true),
            };
            assert!(matches!(result, Err(WriteError::Durability(_))), "{:?}", result);
            assert_eq!(chunks.read_cell(&id).unwrap().header.version, before.version);
            assert_delete_head_released(&seg, cursor);
        }
    }

    fn allocation_waits_for_reclamation(retired: bool) {
        use std::sync::mpsc;
        use std::time::Duration;
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        for slot in chunk.head_pool.iter() {
            let mut id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                id = seg.id;
                chunk.put_segment(seg);
                slot.store(id, Ordering::Release);
            }
            chunk.segs.get(&(id as usize)).unwrap().retire_tail();
        }
        let mut seals = Vec::new();
        let mut pins = Vec::new();
        while chunk.allocator.writer_headroom() > 0 {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            let id = seg.id;
            let seq = seg.seq_id;
            chunk.put_segment(seg);
            let seg = chunk.segs.get(&(id as usize)).unwrap();
            if retired {
                pins.push(SegmentReferenceGuard::new(seg.clone()).unwrap());
                chunk.remove_segment(id);
            } else {
                seg.protection.seal_pending.store(true, Ordering::Release);
                seals.push((id, seq));
            }
        }
        // A caller holding a cell word must not wait for these same segments.
        assert!(matches!(
            chunk.try_acquire(64, false),
            Err(WriteError::CannotAllocateSpace)
        ));
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                TEST_BEFORE_CAPACITY_REFUSAL.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || ready_tx.send(()).unwrap()));
                });
                let result = chunks.list[0]
                    .try_acquire(64, true)
                    .map(|entry| drop(entry));
                done_tx.send(result).unwrap();
            }
        });
        ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let early = done_rx.recv_timeout(Duration::from_millis(100));
        // Complete the deferred work. The next GC must see the newly eligible
        // sealed segments, or drain the retired segments after their reader.
        for (id, seq) in seals {
            let segment = chunk.segs.get(&(id as usize)).unwrap();
            chunk.finish_rotated_head(id, seq);
            assert!(!segment.protection.seal_pending.load(Ordering::Acquire));
        }
        drop(pins);
        let waited = early.is_err();
        let result =
            early.unwrap_or_else(|_| done_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        assert!(
            waited,
            "allocation refused before deferred reclamation finished: {:?}",
            result
        );
        assert!(
            result.is_ok(),
            "reclaimed space was not retried: {:?}",
            result
        );
    }

    #[test]
    fn allocation_uses_space_in_later_heads_before_rotating() {
        allocation_at_full_capacity(false, false, false);
    }

    #[test]
    fn allocation_rescans_a_head_replaced_before_capacity_refusal() {
        allocation_at_full_capacity(true, false, false);
    }

    #[test]
    fn allocation_queues_for_an_owned_head_with_space() {
        allocation_at_full_capacity(false, true, false);
    }

    #[test]
    fn allocation_rechecks_heads_after_losing_rotation_capacity() {
        allocation_at_full_capacity(false, true, true);
    }

    fn allocation_at_full_capacity(replace_head: bool, own_available: bool, lose_rotation: bool) {
        let chunks = Chunks::new_dummy(1, 12 * SEGMENT_SIZE);
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let last = (head_affinity() + chunk.head_pool.len() - 1) % chunk.head_pool.len();
        let mut available = 0;
        for (i, slot) in chunk.head_pool.iter().enumerate() {
            let mut id = slot.load(Ordering::Acquire);
            if id == HEAD_SEG_ID_EMPTY {
                let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                id = seg.id;
                chunk.put_segment(seg);
                slot.store(id, Ordering::Release);
            }
            let seg = chunk.segs.get(&(id as usize)).unwrap();
            if !replace_head && i == last {
                available = id;
            } else {
                seg.retire_tail();
            }
        }
        // No address for another head. Keep non-head segments pinned so this
        // tests use of existing head space, not cleaner scheduling.
        let mut pins = Vec::new();
        while chunk.allocator.writer_headroom() > if lose_rotation { 2 } else { 0 } {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            let id = seg.id;
            chunk.put_segment(seg);
            pins.push(SegmentReferenceGuard::new(chunk.segs.get(&(id as usize)).unwrap()).unwrap());
            if replace_head && available == 0 {
                available = id;
            }
        }
        if replace_head {
            let chunks = chunks.clone();
            TEST_BEFORE_CAPACITY_REFUSAL.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let chunk = &chunks.list[0];
                    let first = head_affinity() % chunk.head_pool.len();
                    chunk.head_pool[first].store(available, Ordering::Release);
                }))
            });
        }
        let available_seg = chunk.segs.get(&(available as usize)).unwrap();
        if own_available {
            assert!(available_seg.try_own());
            let seg = available_seg.clone();
            TEST_BEFORE_HEAD_QUEUE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    seg.release_own();
                }))
            });
        }
        if lose_rotation {
            let chunks = chunks.clone();
            TEST_BEFORE_ROTATION_ALLOCATE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    let chunk = &chunks.list[0];
                    while chunk.allocator.writer_headroom() > 0 {
                        let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
                        chunk.put_segment(seg);
                    }
                }))
            });
        }
        let result = chunk.try_acquire(64, true);
        if own_available {
            if result.is_err() {
                available_seg.release_own();
            }
            TEST_BEFORE_HEAD_QUEUE.with(|hook| *hook.borrow_mut() = None);
        }
        let pending = result.expect("later head still has room");
        assert_eq!(pending.seg.id, available);
        drop(pending);
        drop(pins);
    }

    #[test]
    fn delayed_seal_job_does_not_seal_a_reused_head() {
        let dir = tempfile::tempdir().unwrap();
        let chunks = Chunks::new(
            1,
            4 * SEGMENT_SIZE,
            Arc::new(ServerMeta {
                schemas: LocalSchemasCache::new_local(""),
            }),
            None,
            Some(dir.path().join("backup").to_string_lossy().into_owned()),
            Some(dir.path().join("wal").to_string_lossy().into_owned()),
            None,
        );
        chunks.stop_background();
        let chunk = &chunks.list[0];
        let id = chunk.head_pool[0].load(Ordering::Acquire);
        let old = chunk.segs.get(&(id as usize)).unwrap();
        assert!(chunk.seal_queue.enqueue(chunk.id, id, old.seq_id));
        chunk.head_pool[0].store(HEAD_SEG_ID_EMPTY, Ordering::Release);
        old.protection.head.store(false, Ordering::Release);
        chunk.remove_segment(id);
        assert_eq!(
            drain_until_reclaimed(chunk, std::time::Duration::from_secs(5)),
            1
        );
        let replacement = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
        assert_eq!(replacement.id, id);
        assert_ne!(replacement.seq_id, old.seq_id);
        chunk.put_segment(replacement);
        chunk.head_pool[0].store(id, Ordering::Release);
        let current = chunk.segs.get(&(id as usize)).unwrap();
        for (chunk_id, seg_id, seq_id) in chunk.seal_queue.drain() {
            chunks.list[chunk_id].finish_rotated_head(seg_id, seq_id);
        }
        assert!(
            !current.is_sealed(),
            "a queued job for the retired incarnation sealed the current append head"
        );
        assert!(
            current.begin_pending_journal(),
            "the new head's journal gate must remain open"
        );
        current.end_pending_journal();
    }

    // Prepare the observed CAS word -> head / upsert head -> word cycle.
    // The recovery phase frees a second head, so even the failing control joins.
    #[test]
    fn cas_field_allocation_does_not_hold_the_cell_word() {
        use std::sync::mpsc;
        use std::time::Duration;
        let fields = Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ]);
        let schema = Schema::new("cas_head_word_cycle", None, fields, false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new(
            1,
            12 * SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let chunk = &chunks.list[0];
        let id = Id::allocated(1, 0, 4242);
        let mut seed = payload_cell(schema.vid.get(), &id, 64);
        let version = chunks.write_cell(&mut seed).unwrap().version;
        let mut exclusive = Vec::new();
        for slot in chunk.head_pool.iter().skip(1) {
            let seg = chunk.allocator.alloc_seg(&chunk.file_manager).unwrap();
            let seg_id = seg.id;
            chunk.put_segment(seg);
            slot.store(seg_id, Ordering::Release);
            let seg = chunk.segs.get(&(seg_id as usize)).unwrap();
            assert!(seg.obtain_exclusive_references());
            exclusive.push(seg_id);
        }
        let head = chunk
            .segs
            .get(&(chunk.head_pool[0].load(Ordering::Acquire) as usize))
            .unwrap()
            .clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (owned_tx, owned_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let (cas_tx, cas_rx) = mpsc::channel();
        let (upsert_tx, upsert_rx) = mpsc::channel();
        let cas = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                TEST_CAS_HEAD_WAIT.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        ready_tx.send(()).unwrap();
                        owned_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        assert!(
                            head.is_head_owned(),
                            "upsert must own the only available head"
                        );
                        go_tx.send(()).unwrap();
                    }))
                });
                cas_tx
                    .send(chunks.list[0].compare_version_and_set_field(
                        id.bits(),
                        version,
                        hash_str("id"),
                        OwnedValue::I32(7),
                    ))
                    .unwrap();
            }
        });
        let upsert = std::thread::spawn({
            let chunks = chunks.clone();
            move || {
                ready_rx.recv().unwrap();
                TEST_BEFORE_WORD.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        owned_tx.send(()).unwrap();
                        go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    }))
                });
                let mut cell = payload_cell(schema.vid.get(), &id, 80);
                upsert_tx
                    .send(chunks.list[0].upsert_cell(&mut cell))
                    .unwrap();
            }
        });
        let cas_result = cas_rx.recv_timeout(Duration::from_secs(5));
        let upsert_result = upsert_rx.recv_timeout(Duration::from_secs(5));
        let stalled = cas_result.is_err() || upsert_result.is_err();
        for seg_id in exclusive {
            chunk
                .segs
                .get(&(seg_id as usize))
                .unwrap()
                .release_exclusive_references();
        }
        if cas_result.is_err() {
            assert!(cas_rx.recv_timeout(Duration::from_secs(5)).is_ok());
        }
        if upsert_result.is_err() {
            assert!(upsert_rx.recv_timeout(Duration::from_secs(5)).is_ok());
        }
        cas.join().unwrap();
        upsert.join().unwrap();
        assert!(!stalled, "CAS held the cell word while awaiting a head owned by upsert; freeing another head broke the cycle");
        assert!(
            matches!(cas_result.unwrap(), Err(WriteError::CellVersionMismatch)),
            "CAS must revalidate after upsert changes the version"
        );
        assert!(upsert_result.unwrap().is_ok());
        assert!(chunks.head_cell(&id).unwrap().version > version);
    }

    #[test]
    fn transaction_restore_rejects_a_changed_index_without_a_placeholder() {
        let (chunks, schema) = setup_test_chunks();
        let id = Id::allocated(1, 0, 43);
        let count = chunks.list[0].cell_count();
        assert_eq!(
            chunks.restore_transaction_cell(&id, 0, 1),
            Err(WriteError::CellVersionMismatch)
        );
        assert_eq!(chunks.list[0].cell_count(), count);
        let mut cell = payload_cell(schema.vid.get(), &id, 64);
        let header = chunks.write_cell(&mut cell).unwrap();
        let address = chunks.address_of(&id);
        assert_eq!(
            chunks.restore_transaction_cell(&id, 0, header.version + 1),
            Err(WriteError::CellVersionMismatch)
        );
        assert_eq!(chunks.address_of(&id), address);
        assert_eq!(chunks.head_cell(&id).unwrap().version, header.version);
    }

    #[test]
    fn read_at_returns_pinned_version_after_update() {
        let _ = env_logger::try_init();
        let (chunks, schema) = setup_test_chunks();

        let id = Id::allocated(1, 0, 42);
        let mut cell = payload_cell(schema.vid.get(), &id, 16);
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
        let mut updated = payload_cell(schema.vid.get(), &id, 32);
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
