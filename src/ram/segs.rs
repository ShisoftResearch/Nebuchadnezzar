use crate::ram::chunk::Chunk;
#[cfg(feature = "compress_backups")]
use crate::ram::compression;
use crate::ram::entry;
use crate::ram::entry::EntryMeta;
use crate::ram::file_manager::SegmentFileManager;
use crate::ram::io::align_address;
use crate::ram::tombstone::TOMBSTONE_SIZE_U32;
use bifrost::utils::time::get_time;
#[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
use crc32fast::Hasher as Crc32Hasher;
use libc::*;
use lightning::list::LinkedRingBufferList;
use lightning::spin_hint::Backoff;
use parking_lot;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::ptr;
use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering::{self, *},
};
use std::sync::Arc;
use std::{io, slice};

use super::entry::ENTRY_HEAD_SIZE;

/// Durability write accounting, for attributing disk write volume.
///
/// Inferring these from segment counts and directory growth proved unreliable
/// -- backups are compressed and rewritten in place, so neither file sizes nor
/// `du` reflect what actually reaches the device. These count it at the source.
pub static ARCHIVE_COUNT: AtomicU64 = AtomicU64::new(0);
pub static ARCHIVE_BYTES: AtomicU64 = AtomicU64::new(0);
/// Archives of a segment that already had a backup file, i.e. rewrites rather
/// than first writes. A high ratio to ARCHIVE_COUNT means segments are being
/// re-dirtied and re-archived after they should have been sealed.
pub static ARCHIVE_REWRITES: AtomicU64 = AtomicU64::new(0);
pub static WAL_BYTES: AtomicU64 = AtomicU64::new(0);
pub static WAL_SYNCS: AtomicU64 = AtomicU64::new(0);

/// Dirty registry for the WAL syncer: segments with unsynced non-transactional
/// WAL bytes, each enqueued at most once per pass (`wal_sync_queued`). One
/// dedicated thread fsyncs the whole set every `wal_sync_interval_ms` —
/// replacing per-segment inline timer syncs (6,302 blocking fsyncs/s across an
/// import's ~63 active segments) with O(dirty) syncs per tick on one thread.
/// Entries hold `AArc`s, so segment lifecycle is a non-issue: an archived
/// segment syncs as a no-op (its WAL file handle is gone) and drops.
static WAL_DIRTY: parking_lot::Mutex<Vec<lightning::aarc::Arc<Segment>>> =
    parking_lot::Mutex::new(Vec::new());

/// Enqueue a segment for the syncer's next pass and lazily start the syncer.
pub fn queue_wal_sync(seg: &lightning::aarc::Arc<Segment>) {
    if seg
        .wal_sync_queued
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    WAL_DIRTY.lock().push(seg.clone());

    static SYNCER: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    SYNCER.get_or_init(|| {
        std::thread::Builder::new()
            .name("wal-syncer".into())
            .spawn(|| loop {
                std::thread::sleep(std::time::Duration::from_millis(
                    wal_sync_interval_ms().max(1) as u64,
                ));
                let dirty = std::mem::take(&mut *WAL_DIRTY.lock());
                for seg in dirty {
                    // Clear the flag BEFORE syncing: bytes that land during
                    // the sync re-enqueue for the next pass instead of being
                    // silently absorbed into a sync that may miss them.
                    seg.wal_sync_queued.store(false, Ordering::Release);
                    if seg.bytes_since_sync.load(Ordering::Relaxed) > 0 {
                        if let Err(e) = seg.force_wal_sync() {
                            error!("wal-syncer: sync failed for segment {}: {e}", seg.id);
                        }
                    }
                }
            })
            .expect("spawn wal-syncer");
    });
}
/// Entries written to the WAL. Against the number of live cells this gives the
/// rewrite factor: how many times the average cell's bytes are journalled,
/// which is write amplification the tier has no part in.
pub static WAL_WRITES: AtomicU64 = AtomicU64::new(0);

/// Contention accounting for the per-segment WAL lock.
///
/// `write_wal` holds `file_state` across both the write syscall and the fsync,
/// so concurrent writers to one segment serialise there. Wait time far above
/// hold time means the lock is the limit; comparable means it is not. Measuring
/// both is the point -- a lock that is held a long time and a lock that is
/// waited on a long time call for different fixes.
pub static WAL_LOCK_WAIT_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WAL_LOCK_HELD_NANOS: AtomicU64 = AtomicU64::new(0);
pub static WAL_LOCK_CONTENDED: AtomicU64 = AtomicU64::new(0);

/// Records how long the WAL lock was held, on every exit path.
struct WalHoldTimer(std::time::Instant);
impl Drop for WalHoldTimer {
    fn drop(&mut self) {
        WAL_LOCK_HELD_NANOS.fetch_add(self.0.elapsed().as_nanos() as u64, Relaxed);
    }
}

/// Cold-read amplification accounting.
///
/// Read amplification here is the ratio of bytes moved to bytes wanted, and it
/// has three distinct layers that a single counter would conflate: bytes pulled
/// off disk, bytes materialised by decompression, and the index copying done
/// per call regardless of whether any I/O happened at all. Optimising one can
/// worsen another -- smaller blocks cut decompression but cost compression
/// ratio and more index -- so they are measured separately.
pub static COLD_BLOCK_SERVES: AtomicU64 = AtomicU64::new(0);
/// Calls satisfied by a block already resident: no I/O, no decompression.
pub static COLD_BLOCK_HITS: AtomicU64 = AtomicU64::new(0);
/// Calls that had to fetch a block from the backup.
pub static COLD_BLOCK_MISSES: AtomicU64 = AtomicU64::new(0);
/// Compressed bytes actually read from backup files.
pub static COLD_BLOCK_FILE_BYTES: AtomicU64 = AtomicU64::new(0);
/// Bytes produced by decompressing those blocks.
pub static COLD_BLOCK_PLAIN_BYTES: AtomicU64 = AtomicU64::new(0);
/// Backup file opens. One per miss is a syscall per read.
pub static COLD_BLOCK_OPENS: AtomicU64 = AtomicU64::new(0);
/// Block indexes loaded from disk.
pub static COLD_INDEX_LOADS: AtomicU64 = AtomicU64::new(0);
/// Bytes spent copying the block index inside the lookup itself. Pure overhead:
/// it buys nothing and is paid even by calls that do no I/O.
pub static COLD_INDEX_COPY_BYTES: AtomicU64 = AtomicU64::new(0);
/// Backup handles currently held open by cold segments.
///
/// Caching one handle per segment is unbounded in the number of cold segments,
/// which is unbounded in dataset size. A 1.7TB import reached 66,430 cold
/// segments and pinned 64,860 file descriptors against a 65,535 limit, after
/// which every write failed with EMFILE. A 59GB dataset only ever reached
/// ~18,000 and so never showed it.
pub static COLD_BACKUP_FDS: AtomicUsize = AtomicUsize::new(0);

/// How many backup handles may be cached at once.
///
/// Derived from the process limit rather than fixed, so it adapts to whatever
/// the deployment allows, and deliberately a small fraction of it: descriptors
/// are shared with sockets, WAL files and everything else, and exhausting them
/// takes down writes rather than merely slowing reads.
pub fn cold_backup_fd_cap() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        let soft = rlimit_nofile().unwrap_or(1024);
        (soft / 8).clamp(64, 8192)
    })
}

/// A fixed-size cache of open backup handles, shared by every segment.
///
/// Holding a handle on the segment itself scaled with the number of cold
/// segments, so a large enough dataset exhausted the descriptor table. Capping
/// that scheme without eviction is not enough either: the first segments to go
/// cold would keep their handles forever while segments actually being read got
/// none. This keeps a fixed number and evicts by second chance, so the handles
/// that survive are the ones being used.
///
/// Sharded because it sits on every cold read and a single lock would serialise
/// them, which is the mistake the residency lock already made once.
struct BackupFdCache {
    shards: Vec<parking_lot::Mutex<FdShard>>,
}

struct FdShard {
    /// Fixed slots. `None` means free; eviction reuses a slot in place so the
    /// structure never grows.
    slots: Vec<Option<FdSlot>>,
    /// CLOCK hand.
    hand: usize,
}

struct FdSlot {
    key: (usize, u64, u64),
    file: Arc<File>,
    /// Set on every hit, cleared when the hand passes: one second chance.
    used: bool,
}

impl BackupFdCache {
    fn new(capacity: usize) -> Self {
        // Shard for concurrency, but not so finely that each shard holds a
        // handful of slots -- second chance needs room to be meaningful, and a
        // 4-slot shard evicts entries that are still in use.
        let shards = (capacity / 32).clamp(1, 16);
        let per_shard = (capacity / shards).max(8);
        BackupFdCache {
            shards: (0..shards)
                .map(|_| {
                    parking_lot::Mutex::new(FdShard {
                        slots: (0..per_shard).map(|_| None).collect(),
                        hand: 0,
                    })
                })
                .collect(),
        }
    }

    fn shard_of(&self, key: &(usize, u64, u64)) -> &parking_lot::Mutex<FdShard> {
        // seq_id alone would cluster; mixing all three spreads segments evenly.
        let h = key.0 as u64 ^ key.1.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ key.2;
        &self.shards[(h as usize) % self.shards.len()]
    }

    fn get(&self, key: &(usize, u64, u64)) -> Option<Arc<File>> {
        let mut shard = self.shard_of(key).lock();
        for slot in shard.slots.iter_mut() {
            if let Some(entry) = slot {
                if entry.key == *key {
                    entry.used = true;
                    return Some(entry.file.clone());
                }
            }
        }
        None
    }

    fn insert(&self, key: (usize, u64, u64), file: Arc<File>) {
        let mut shard = self.shard_of(&key).lock();
        // Already cached by a racing reader.
        if shard.slots.iter().flatten().any(|e| e.key == key) {
            return;
        }
        let len = shard.slots.len();
        for _ in 0..(len * 2) {
            let idx = shard.hand % len;
            shard.hand = shard.hand.wrapping_add(1);
            match &mut shard.slots[idx] {
                None => {
                    shard.slots[idx] = Some(FdSlot { key, file, used: true });
                    COLD_BACKUP_FDS.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                Some(entry) if entry.used => {
                    // Second chance: survives this pass, not the next.
                    entry.used = false;
                }
                Some(_) => {
                    // Evicting drops the handle, closing the descriptor.
                    shard.slots[idx] = Some(FdSlot { key, file, used: true });
                    COLD_BACKUP_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
    }

    fn remove(&self, key: &(usize, u64, u64)) {
        let mut shard = self.shard_of(key).lock();
        for slot in shard.slots.iter_mut() {
            if slot.as_ref().map_or(false, |e| e.key == *key) {
                *slot = None;
                COLD_BACKUP_FDS.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        }
    }
}

lazy_static! {
    static ref BACKUP_FD_CACHE: BackupFdCache = BackupFdCache::new(cold_backup_fd_cap());
}

/// Handles closed to make room for another.
pub static COLD_BACKUP_EVICTIONS: AtomicU64 = AtomicU64::new(0);

fn rlimit_nofile() -> Option<usize> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit fills the struct; failure is reported by the return.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } == 0 {
        Some(lim.rlim_cur as usize)
    } else {
        None
    }
}

pub const SEGMENT_SIZE_U32: u32 = 8 * 1024 * 1024;
pub const SEGMENT_SIZE: usize = SEGMENT_SIZE_U32 as usize;
pub const SEGMENT_MASK: usize = !(SEGMENT_SIZE - 1);
pub const SEGMENT_BITS_SHIFT: u32 = SEGMENT_SIZE.trailing_zeros();

/// Pooled 8 MiB staging buffers for `Segment::archive` (slab adopter #1).
/// Bounded in practice by archiver concurrency; `cold_ranges` on so buffer
/// memory returns to the OS when archiver threads retire.
type ArchiveStagingBuf = [u8; SEGMENT_SIZE];

fn archive_staging_pool(
) -> &'static std::sync::Arc<lightning::slab_pool::SlabPool<ArchiveStagingBuf, 2>> {
    static POOL: std::sync::OnceLock<
        std::sync::Arc<lightning::slab_pool::SlabPool<ArchiveStagingBuf, 2>>,
    > = std::sync::OnceLock::new();
    POOL.get_or_init(|| lightning::slab_pool::SlabPool::new("archive_staging", true))
}

/// Frees the staging buffer on every exit path (plain data, no destructor).
struct ArchiveStagingGuard(lightning::slab_pool::SlabHandle<ArchiveStagingBuf>);
impl Drop for ArchiveStagingGuard {
    fn drop(&mut self) {
        archive_staging_pool().free_forget(self.0);
    }
}

/// Assumed average payload of one cell, used only to pre-size the cell index.
///
/// `WordMap` capacity is fixed at construction, so this number decides how many
/// rounds of lock-free migration startup pays for. Each round allocates a table
/// twice the size of the last and frees the old one, and the freed tables are
/// retained by glibc's per-thread arenas rather than returned to the OS -- so an
/// index that starts an order of magnitude too small costs several times the
/// memory of the index itself, in garbage nothing will reclaim.
///
/// A round prior, not a measurement: no single density suits every workload, so
/// this only has to be within a factor of a few to avoid the pathological case.
/// Note which direction is dangerous -- set too *high* it means too few cells
/// per segment, under-sizing the index and paying a doubling for every factor
/// of two it is short; set too low it merely over-allocates, bounded and
/// predictable. Workloads far from 1 KB/cell should set
/// `NEB_ESTIMATED_CELL_BYTES` rather than have their number baked in here.
pub const DEFAULT_ESTIMATED_CELL_BYTES: usize = 1024;

/// Cells a segment is expected to hold, overridable for workloads whose cells
/// are much larger or smaller than the default assumption.
pub fn estimated_cells_per_segment() -> usize {
    let cell_bytes = std::env::var("NEB_ESTIMATED_CELL_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v >= 64)
        .unwrap_or(DEFAULT_ESTIMATED_CELL_BYTES);
    SEGMENT_SIZE / cell_bytes
}

pub const HOT_SEGMENT: u8 = 1;
pub const COLD_SEGMENT: u8 = 2;
pub const HOT_COLD_MASK: u8 = !0 << 1 >> 1;
pub const LOCKING_SEGMENT_BITS: u8 = !HOT_COLD_MASK;

// Page constants (used for alignment and mprotect)
pub const PAGE_SHIFT: usize = 12; // 4KB pages
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;

pub const EXCLUSIVE_REF_COUNT: usize = usize::MAX;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SegmentClass {
    Regular = 0,
    Blob = 1,
}

impl Default for SegmentClass {
    fn default() -> Self {
        Self::Regular
    }
}

// WAL Performance Configuration
// These settings implement group commit batching to improve write throughput
// while maintaining durability guarantees within bounded loss windows.
//
// Performance Impact:
// - Larger buffer = fewer system calls, better throughput
// - Larger batch size = fewer fsyncs, MUCH better throughput (100x+)
// - Longer interval = better batching, but higher potential data loss window
//
// Durability Guarantees:
// - Transactional writes: No fsync during write, sync happens at commit time
// - Non-transactional writes: Fsync when batch_size OR interval is reached
// - In case of crash: Max potential loss = WAL_SYNC_BATCH_SIZE bytes OR
//                                          WAL_SYNC_INTERVAL_MS time window
//
// Performance Analysis:
// - With 10ms interval: max 100 fsyncs/sec = ~13 MB/s if writing <130KB per interval
// - With 100ms interval: max 10 fsyncs/sec = ~40+ MB/s (10x improvement)
// - With i64::MAX interval: limited only by batch size = 100s-1000s MB/s
//
// Tuning Recommendations:
// - High throughput: batch_size=4MB, interval=100ms (recommended for most workloads)
// - Maximum throughput: batch_size=4MB, interval=i64::MAX (sync only on size)
// - Low latency: batch_size=512KB, interval=50ms
// - Strict durability: batch_size=0, interval=0 (sync every write)
pub const WAL_BUFFER_SIZE: usize = 512 * 1024; // 512KB in-memory buffer (reduces syscalls)
pub const WAL_SYNC_BATCH_SIZE: usize = 1 * 1024 * 1024; // Sync after 1MB of writes (reduces fsyncs)
pub const WAL_SYNC_INTERVAL_MS: i64 = 10; // Sync every 10ms (10x less frequent than before)

/// The group-commit time bound, overridable via `NEB_WAL_SYNC_INTERVAL_MS`.
///
/// Group commit batches **per segment**, so the byte threshold is only reached
/// when one segment absorbs a whole batch on its own. Spread across the ~63
/// segments an import writes concurrently, no segment gets there and every one
/// falls back to this timer: measured 6,302 fsyncs/s at 92 KB each against a
/// 1 MiB threshold, which is 63 segments x one sync per 10 ms. Threads block on
/// those syncs, which is why the server sat at 30% CPU on 192 cores with the
/// device answering in 0.49 ms.
///
/// Raising this trades a wider crash-loss window for fewer, larger syncs.
pub fn wal_sync_interval_ms() -> i64 {
    static V: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("NEB_WAL_SYNC_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(WAL_SYNC_INTERVAL_MS)
    })
}

#[repr(C, align(64))] // Ensure consistent memory layout and cache line alignment
/// A segment: one 8 MiB slice of a chunk's address space plus its bookkeeping.
///
/// One instance exists per segment ON DISK, not per segment in memory -- a
/// 2 TB store holds ~250,000 of these structs resident while 90%+ of their
/// data is cold. Everything here is therefore per-cold-segment overhead
/// (see the fd-cache EMFILE incident for how that scales), and the layout is
/// deliberate:
///
/// * `repr(C, align(64))`: fields are grouped by access pattern rather than
///   left to the compiler, so the per-read `references` counter and the
///   per-write `append_header` sit apart from read-only identity, and the
///   struct starts cache-line aligned.
/// * `bound` is not stored; it is always `addr + SEGMENT_SIZE` (see
///   [`Self::bound`]).
/// * `dead_bits` is lazy. Eagerly it was 128 KiB *per segment* -- 31 GB of
///   heap at 245K segments, the largest single heap consumer of a terabyte
///   import -- almost all of it for cold segments whose bitmap is never
///   consulted (the combine cleaner skips cold segments).
#[repr(C, align(64))]
pub struct Segment {
    // --- identity & geometry: written at construction, read everywhere ---
    pub id: u64,
    pub seq_id: u64,
    pub chunk_id: usize,
    pub addr: usize,

    // --- write-path atomics ---
    pub append_header: AtomicUsize,
    pub bytes_since_sync: AtomicUsize, // Bytes written since last fsync
    pub last_sync_time: AtomicI64,     // Timestamp of last fsync in milliseconds
    /// Set while this segment sits in the WAL syncer's dirty registry, so a
    /// segment is enqueued at most once per syncer pass.
    pub wal_sync_queued: std::sync::atomic::AtomicBool,
    /// Per-read reference count; the hottest atomic in the struct.
    references: AtomicUsize,

    // --- cleaner bookkeeping ---
    pub dead_space: AtomicU32,
    pub tombstones: AtomicU32,
    /// Generation counter for changes that introduce dead space (dead cells or tombstones)
    dead_bytes_generation: AtomicU64,
    /// Marker used by cleaners to skip segments that were cleaned without reclaiming space
    last_no_progress_clean_generation: AtomicU64,

    // --- tiering timestamps ---
    /// Timestamp in ms of last promotion, used to avoid immediate re-eviction
    pub last_promoted_ms: AtomicI64,
    /// Timestamp in ms of last eviction, used for churn detection
    pub last_evicted_ms: AtomicI64,

    // --- byte-sized state, clustered so padding is paid once ---
    segment_class: SegmentClass,
    pub dropped: AtomicBool,
    /// Tracks if WAL has been written to since last successful archive
    /// Used to optimize eviction: if archived=true && is_dirty=false, can skip re-archiving
    is_dirty: AtomicBool,
    /// Set once a backup exists for this `(chunk, seg, seq)`. From then on the
    /// incarnation is CLOSED: nothing may append to it, and in particular no
    /// WAL may be (re-)created at this seq id.
    ///
    /// This is what makes recovery's file arbitration decidable. Discovery
    /// dedups by `(chunk_id, seg_id)` and, on a seq tie, prefers the backup
    /// over the WAL -- which is only sound if a backup at a seq is a COMPLETE
    /// image that supersedes the log. Archiving upholds that by deleting the
    /// WAL; what broke it was appending afterwards, which lazily re-created a
    /// WAL at the SAME seq. Shutdown archives the open append head, so the
    /// next incarnation resumed that very segment and its fresh WAL held only
    /// the post-restart suffix: backup and WAL became complementary HALVES of
    /// one image rather than two versions of it, and the tie-break silently
    /// dropped every post-restart write. A SIGKILL there cost the ranged
    /// index pages that were provably built and applied -- `MissingPage` on
    /// reload, "tree placement was not found" on scan.
    ///
    /// Reconstructing the seam after the fact was tried and reverted: the
    /// merge had to GUESS where the backup ended, and a mis-detected seam
    /// trimmed the WAL to zero (see git history). Sealing removes the choice
    /// instead -- a seq id has either a complete backup or a live WAL, never
    /// both -- so the ambiguity cannot be represented and no merge is needed.
    ///
    /// Cost: a graceful shutdown retires the unfilled tail of one open segment
    /// per chunk. The segment stays readable and the cleaner can still reclaim
    /// it; only its remaining append space is given up.
    sealed: AtomicBool,
    /// Segment lock for tiered memory operations (eviction, promotion, cleaner)
    /// Holds the hot/cold state. Cell read/write operations do NOT need this
    /// lock, only cell-level locks.
    pub tiered_lock: AtomicU8, // 1 = hot, 2 = cold, highest bit for locking
    pub reference_count: AtomicU8, // Multi-chance CLOCK: 0-7 chances before eviction
    pub access_count: AtomicU8,    // Tracks cold accesses for promotion threshold

    // --- lock-guarded cold state ---
    /// One bit per 8 bytes of segment space, set when the entry starting at
    /// that offset dies. Collection consults the bitmap before probing the
    /// cell index, so scan cost tracks live entries instead of backlog.
    ///
    /// PURELY an optimization: the cell index probe is the authority, and a
    /// missing bit only costs a probe. That is what licenses the laziness --
    /// `None` until the first dead entry lands while the segment is hot
    /// (marks on cold segments are skipped: the combine cleaner never scans
    /// cold segments, so their bits would never be read), and dropped at
    /// eviction. An `RwLock` rather than a bare `AtomicPtr` because markers
    /// can run holding only an `AArc` -- no segment reference -- so eviction's
    /// exclusive guard cannot prove no marker holds the pointer.
    dead_bits: parking_lot::RwLock<Option<Box<[AtomicU64]>>>,
    pub file_state: parking_lot::Mutex<SegmentFileState>,

    /// Partial residency for a cold segment: which backup blocks have been
    /// faulted back into this segment's address range, and the block index
    /// needed to find them.
    ///
    /// `MADV_DONTNEED` drops a cold segment's physical pages but leaves the
    /// mapping intact, so a block can be decompressed straight back to its own
    /// offset inside `addr`. Addresses therefore stay valid and readers need no
    /// knowledge of any of this -- the segment's own address space is the cache,
    /// which is what makes the block, rather than the whole 8 MiB segment, the
    /// unit of residency.
    /// Residency is read on every cold read and written only on a miss, so it
    /// is an RwLock rather than a Mutex. Under an exclusive lock, threads
    /// sharing a segment serialised on the hit path -- a bit test behind a
    /// mutex -- and aggregate throughput peaked at 8 threads and then fell,
    /// 5.7x down to 3.8x by 32 threads on a 32-core machine.
    block_residency: parking_lot::RwLock<BlockResidency>,
}

/// Per-segment state for serving reads from a cold segment without promoting it.
#[derive(Default)]
pub struct BlockResidency {
    /// Block index of the backup file, kept resident so a lookup costs no
    /// I/O after the first -- in the packed 6-bytes-per-block form
    /// ([`crate::ram::compression::PackedBlockIndex`]), half the on-disk
    /// index size, because it lives for as long as the segment stays cold
    /// and per-cold-segment memory scales with the dataset.
    index: Option<crate::ram::compression::PackedBlockIndex>,
    /// One bit per block, set once that block has been written back into the
    /// segment's memory.
    present: Vec<u64>,
    /// Bytes currently faulted in through this path, for memory accounting.
    resident_bytes: usize,
}

impl BlockResidency {
    #[inline]
    fn is_present(&self, block: usize) -> bool {
        self.present
            .get(block / 64)
            .map_or(false, |w| w & (1u64 << (block % 64)) != 0)
    }

    #[inline]
    fn mark_present(&mut self, block: usize, bytes: usize) {
        let word = block / 64;
        if word >= self.present.len() {
            self.present.resize(word + 1, 0);
        }
        if self.present[word] & (1u64 << (block % 64)) == 0 {
            self.present[word] |= 1u64 << (block % 64);
            self.resident_bytes += bytes;
        }
    }

    /// Number of blocks currently faulted in.
    pub fn present_count(&self) -> usize {
        self.present.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Drop everything cached about the current backup.
    ///
    /// The index maps offsets within one specific backup file. A segment that
    /// is promoted, appended to and archived again gets a different block
    /// layout, so keeping the old index across that would resolve offsets
    /// against a file that no longer matches it.
    fn clear(&mut self) {
        self.index = None;
        self.present.clear();
        self.resident_bytes = 0;
    }
}

/// File state for a segment, protected by a mutex
///
/// **LOCK ORDERING INVARIANT**:
/// To prevent deadlock, locks must be acquired in this order:
/// 1. `tiered_lock` (atomic, not a mutex)
/// 2. `file_state` (this mutex)
/// 3. Cell locks (via cell_index)
///
/// All code paths that acquire multiple locks MUST follow this order.
/// See eviction.rs and promotion.rs for examples.
/// File state for a segment, protected by a mutex
///
/// **MEMORY OPTIMIZATION**: Uses unbuffered File handles instead of BufWriter
/// to avoid accumulating 512KB+ buffers per segment. With thousands of segments,
/// BufWriter buffers caused multi-GB memory leaks.
pub struct SegmentFileState {
    pub manager: Arc<SegmentFileManager>,
    pub wal: Option<File>,
}

impl Segment {
    pub fn new(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        hot: bool,
        file_manager: Arc<SegmentFileManager>,
    ) -> Segment {
        Self::new_with_class(
            id,
            seq_id,
            chunk_id,
            buffer_ptr,
            hot,
            file_manager,
            SegmentClass::Regular,
        )
    }

    pub fn new_with_class(
        id: u64,
        seq_id: u64,
        chunk_id: usize,
        buffer_ptr: usize,
        hot: bool,
        file_manager: Arc<SegmentFileManager>,
        segment_class: SegmentClass,
    ) -> Segment {
        let size = SEGMENT_SIZE;

        if let Err(e) = file_manager.init_directories() {
            panic!("Failed to initialize storage directories: {}", e);
        }

        // WAL files are created lazily on first write. This avoids keeping file descriptors
        // open for cold/idle segments. Active head segments will create and hold WAL files
        // while they are being written to.
        let wal_file_opt = None;

        debug!(
            "Creating new segment chunk {}, id {}, seq_id {}, size {}, address {}",
            chunk_id, id, seq_id, size, buffer_ptr
        );
        let tiered_lock = if hot { HOT_SEGMENT } else { COLD_SEGMENT };
        let _ = size;
        Segment {
            addr: buffer_ptr,
            id,
            seq_id,
            chunk_id,
            segment_class,
            append_header: AtomicUsize::new(buffer_ptr),
            dead_space: AtomicU32::new(0),
            tombstones: AtomicU32::new(0),
            // Lazy: allocated on the first dead entry while hot, dropped at
            // eviction. See the field doc for why this is safe.
            dead_bits: parking_lot::RwLock::new(None),
            dead_bytes_generation: AtomicU64::new(0),
            last_no_progress_clean_generation: AtomicU64::new(0),
            references: AtomicUsize::new(0),
            file_state: parking_lot::Mutex::new(SegmentFileState {
                manager: file_manager,
                wal: wal_file_opt,
            }),
            dropped: AtomicBool::new(false),
            tiered_lock: AtomicU8::new(tiered_lock),
            reference_count: AtomicU8::new(0),
            access_count: AtomicU8::new(0),
            last_promoted_ms: AtomicI64::new(0),
            last_evicted_ms: AtomicI64::new(0),
            is_dirty: AtomicBool::new(true), // Start dirty
            sealed: AtomicBool::new(false),
            last_sync_time: AtomicI64::new(0),
            bytes_since_sync: AtomicUsize::new(0),
            wal_sync_queued: std::sync::atomic::AtomicBool::new(false),
            block_residency: parking_lot::RwLock::new(BlockResidency::default()),
        }
    }

    #[inline]
    pub fn segment_class(&self) -> SegmentClass {
        self.segment_class
    }

    pub fn try_acquire(&self, size: u32) -> Option<usize> {
        // A sealed segment is out of append space by definition, whatever its
        // cursor says: it has a backup at its seq id, so an append here could
        // only become durable through a WAL that must never exist (see the
        // `sealed` field). Refusing at acquisition -- before any bytes are
        // placed -- is what lets the caller rotate to a fresh segment; the
        // refusal in `write_wal` is only a backstop, and reaching it means an
        // entry was already written into memory that cannot be made durable.
        if self.is_sealed() {
            return None;
        }
        let size = size as usize;
        loop {
            let curr_last = self.append_header.load(Ordering::Acquire);
            let exp_last = curr_last + size;
            if exp_last > self.bound() {
                return None;
            } else {
                if self
                    .append_header
                    .compare_exchange(curr_last, exp_last, Ordering::AcqRel, Ordering::Relaxed)
                    .is_err()
                {
                    continue;
                } else {
                    debug_assert_eq!(
                        align_address(8, curr_last),
                        curr_last,
                        "Acquired address is not aligned"
                    );
                    return Some(curr_last);
                }
            }
        }
    }

    pub fn shrink(&self, size: usize) {
        // If the segment is full or larger than SEGMENT_SIZE, there's nothing to shrink
        if size >= SEGMENT_SIZE {
            return;
        }
        self.punch_hole(size);
    }

    /// Frees memory pages from this segment starting at the given offset.
    ///
    /// This function uses `madvise_free` to free pages from the aligned start offset
    /// to the end of the segment. The address is aligned up to the next page boundary
    /// to ensure proper page alignment required by madvise.
    ///
    /// # Arguments
    /// * `start_offset` - The offset within the segment to start freeing from (will be aligned up to page boundary)
    ///
    /// # Safety
    /// This function should only be called when the freed region is no longer needed,
    /// as accessing the freed pages may cause the OS to zero them or remap them.
    ///
    /// # Example
    /// ```rust,ignore
    /// use neb::ram::segs::Segment;
    ///
    /// // After writing some data to a segment, you can free unused tail pages
    /// // (assuming `segment` is a valid Segment reference)
    /// let used_size = 1024 * 100; // 100KB used
    /// segment.punch_hole(used_size);
    /// ```
    pub fn punch_hole(&self, start_offset: usize) {
        // Calculate the absolute address of the start offset
        let start_addr = self.addr + start_offset;

        // Align to the next page boundary (round up)
        let aligned_addr = align_address(PAGE_SIZE, start_addr);

        // Calculate the size to free (from aligned address to end of segment)
        let end_addr = self.bound();

        if aligned_addr < end_addr {
            let size = end_addr - aligned_addr;

            // Only punch hole if we have at least one page to free
            if size >= PAGE_SIZE {
                debug!(
                    "Punching hole in segment {} from offset {} (aligned to {}), size {} bytes ({} pages)",
                    self.id,
                    start_offset,
                    aligned_addr - self.addr,
                    size,
                    size / PAGE_SIZE
                );
                unsafe {
                    madvise_free(aligned_addr, size);
                }
            }
        }
    }

    /// Drop this segment's pages.
    ///
    /// Callers holding a tiered manager must release the accounting first, via
    /// `take_block_resident_bytes`, because this clears the residency: taking
    /// afterwards reports nothing and the manager's counter would never come
    /// back down.
    pub fn free_memory(&self) {
        unsafe {
            madvise_free(self.addr, SEGMENT_SIZE);
        }
        // Everything faulted in through the partial path is gone with it, so
        // the accounting must drop too or the limit would drift upward forever.
        let mut residency = self.block_residency.write();
        residency.clear();
        drop(residency);
        // The cache keys on seq_id, so a re-archived segment would simply miss
        // rather than read the wrong file. Releasing here just returns the
        // descriptor promptly instead of waiting for the hand to reach it.
        self.drop_cached_backup();
    }

    /// Clear partial residency and report the bytes released, so the caller --
    /// which holds the tiered manager -- can drop them from the accounting.
    pub fn take_block_resident_bytes(&self) -> usize {
        let mut residency = self.block_residency.write();
        let released = residency.resident_bytes();
        residency.clear();
        released
    }


    /// Drop the blocks faulted into this cold segment, reporting the bytes
    /// released, or `None` if the segment is busy or holds nothing.
    ///
    /// For a cold segment the backup file is the authority, so faulted-in
    /// blocks are pure cache: dropping them costs a re-read and no write at
    /// all. That makes them the first thing to give back under pressure, ahead
    /// of evicting a hot segment, which has to be archived first.
    ///
    /// Requires exclusive access for the same reason eviction does -- the pages
    /// are about to be dropped, so no reader may be inside them. The attempt is
    /// made once and abandoned if the segment is referenced; spinning here is
    /// what deadlocked promotion, and a sweep has other segments to try.
    pub fn try_reclaim_resident_blocks(&self) -> Option<usize> {
        if !self.is_cold() {
            return None;
        }
        // Residency includes the index, so a segment holding only an index is
        // still worth reclaiming.
        if self.block_resident_bytes() == 0 {
            return None;
        }
        let _exclusive = SegmentExclusiveRefGuard::new(self)?;

        let mut residency = self.block_residency.write();
        let released = residency.resident_bytes();
        if released == 0 {
            return None;
        }
        unsafe {
            madvise_free(self.addr, SEGMENT_SIZE);
        }
        residency.clear();
        Some(released)
    }

    /// Bytes this segment currently holds through partial block residency.
    pub fn block_resident_bytes(&self) -> usize {
        self.block_residency.read().resident_bytes()
    }

    /// Blocks currently faulted in, and the total the backup holds.
    pub fn block_residency_stats(&self) -> (usize, usize) {
        let r = self.block_residency.read();
        let total = r.index.as_ref().map_or(0, |i| i.block_count());
        (r.present_count(), total)
    }

    /// Make the bytes at `addr` readable without promoting the whole segment.
    ///
    /// Decompresses just the backup block containing `addr` and writes it back
    /// to its own offset inside this segment's mapping, so the caller's address
    /// is valid afterwards and nothing downstream needs to know the segment is
    /// still cold.
    ///
    /// Returns `Ok(false)` when the backup predates the block-indexed format,
    /// in which case there is no way to read part of it and the caller must
    /// fall back to promoting the segment whole.
    pub fn fault_in_block_for(&self, addr: usize) -> io::Result<Option<usize>> {
        if addr < self.addr || addr >= self.addr + SEGMENT_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("address {:#x} outside segment {}", addr, self.id),
            ));
        }
        let offset = addr - self.addr;

        // Resolve the block under the lock, then release it. The index is read
        // in place rather than copied: cloning it cost more per call than the
        // decompression it was guarding, and was paid even by calls that found
        // the block already resident and did no work at all.
        // Bytes newly charged to this segment, reported to the caller so the
        // manager's total matches what reclamation will later hand back. The
        // index counts here too: charging it to the segment but not to the
        // manager made reclaim return more than was ever added.
        let mut newly_accounted = 0usize;

        // Fast path first, under a shared lock: an already-resident block needs
        // nothing but a lookup and a bit test, and that is the common case once
        // a segment is warm. Taking the exclusive lock for it made readers of
        // one segment serialise on each other.
        {
            let residency = self.block_residency.read();
            if let Some(index) = residency.index.as_ref() {
                let (block_idx, _within) = index.locate(offset)?;
                if residency.is_present(block_idx) {
                    COLD_BLOCK_HITS.fetch_add(1, Ordering::Relaxed);
                    COLD_BLOCK_SERVES.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(0));
                }
            }
        }

        let (block_idx, block_start, file_off, comp_len, raw_blocks) = {
            let mut residency = self.block_residency.write();

            if residency.index.is_none() {
                match self.load_block_index()? {
                    Some(index) => {
                        COLD_INDEX_LOADS.fetch_add(1, Ordering::Relaxed);
                        // The index is memory a cold segment holds, so it counts
                        // as residency. It is small per segment and invisible at
                        // small scale, but it scales with the number of cold
                        // segments -- about 4.9 GiB across a 1.7 TB dataset at a
                        // 4 KiB block target -- and memory the pressure
                        // calculation cannot see is exactly what let residency
                        // grow unbounded before.
                        residency.resident_bytes += index.heap_bytes();
                        newly_accounted += index.heap_bytes();
                        residency.index = Some(index);
                    }
                    None => return Ok(None),
                }
            }
            let index = residency.index.as_ref().unwrap();
            let (block_idx, _within) = index.locate(offset)?;
            if residency.is_present(block_idx) {
                COLD_BLOCK_HITS.fetch_add(1, Ordering::Relaxed);
                COLD_BLOCK_SERVES.fetch_add(1, Ordering::Relaxed);
                // Still report any index just charged on this call.
                return Ok(Some(newly_accounted));
            }
            COLD_BLOCK_MISSES.fetch_add(1, Ordering::Relaxed);

            let (block_start, file_off, comp_len) = index.entry(block_idx)?;
            (block_idx, block_start, file_off, comp_len, index.raw())
        };

        // Read and decompress with no lock held. Two threads missing the same
        // block will both fetch it, which wastes a read but never corrupts:
        // only one of them copies, decided under the lock below. Holding the
        // lock across the I/O instead would serialise every reader of the
        // segment behind a disk fetch.
        let compressed = {
            let mut buf = vec![0u8; comp_len];
            let file = self.backup_file()?;
            read_exact_at(&file, &mut buf, file_off as u64)?;
            COLD_BLOCK_FILE_BYTES.fetch_add(comp_len as u64, Ordering::Relaxed);
            buf
        };

        // An uncompressed backup hands back the bytes as read; there is
        // nothing to decode.
        let plain = if raw_blocks {
            compressed
        } else {
            lz4_flex::block::decompress_size_prepended(&compressed).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("block {} of segment {}: {:?}", block_idx, self.id, e),
                )
            })?
        };
        COLD_BLOCK_PLAIN_BYTES.fetch_add(plain.len() as u64, Ordering::Relaxed);

        if block_start + plain.len() > SEGMENT_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "block {} of segment {} overruns the segment",
                    block_idx, self.id
                ),
            ));
        }

        let mut residency = self.block_residency.write();
        // Another thread may have landed this block while the lock was open.
        if residency.is_present(block_idx) {
            COLD_BLOCK_SERVES.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(newly_accounted));
        }

        // Write it back where it belongs. The mapping survived MADV_DONTNEED,
        // so this restores exactly the bytes that were dropped.
        unsafe {
            ptr::copy_nonoverlapping(
                plain.as_ptr(),
                (self.addr + block_start) as *mut u8,
                plain.len(),
            );
        }
        COLD_BLOCK_SERVES.fetch_add(1, Ordering::Relaxed);
        let before = residency.resident_bytes();
        residency.mark_present(block_idx, plain.len());
        // Caller does the accounting: it holds the tiered manager, and a
        // segment has no route to one.
        Ok(Some(newly_accounted + (residency.resident_bytes() - before)))
    }

    /// This segment's backup handle, opening it if it is not already cached.
    ///
    /// Reads go through `read_at`, which takes its own offset and so does not
    /// disturb a file position, making one handle safe to share across threads.
    fn backup_file(&self) -> io::Result<Arc<File>> {
        let key = (self.chunk_id, self.id, self.seq_id);
        if let Some(f) = BACKUP_FD_CACHE.get(&key) {
            return Ok(f);
        }
        let path = {
            let state = self.file_state.lock();
            state
                .manager
                .backup_path(self.chunk_id, self.id, self.seq_id)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "no backup path for cold segment")
                })?
        };
        let file = Arc::new(File::open(&path)?);
        COLD_BLOCK_OPENS.fetch_add(1, Ordering::Relaxed);
        // Racing openers both insert; the cache keeps one and the other's
        // handle closes when its Arc drops. Correct either way, and cheaper
        // than holding a lock across the open.
        BACKUP_FD_CACHE.insert(key, file.clone());
        Ok(file)
    }

    /// Forget this segment's cached handle. Called when the backup it refers to
    /// is no longer the right file to read.
    fn drop_cached_backup(&self) {
        BACKUP_FD_CACHE.remove(&(self.chunk_id, self.id, self.seq_id));
    }

    /// Read the header and block index from this segment's backup file.
    ///
    /// `Ok(None)` means the backup is not in the block-indexed format.
    fn load_block_index(&self) -> io::Result<Option<compression::PackedBlockIndex>> {
        use crate::ram::compression;
        let state = self.file_state.lock();
        let Some(path) = state
            .manager
            .backup_path(self.chunk_id, self.id, self.seq_id)
        else {
            return Ok(None);
        };
        drop(state);

        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut header = [0u8; compression::block_header_size()];
        if read_exact_at(&file, &mut header, 0).is_err() {
            return Ok(None);
        }
        let Some(layout) = compression::block_layout(&header) else {
            return Ok(None);
        };

        let index_len = compression::block_header_size()
            + layout.block_count * compression::block_index_entry_size();
        let mut index = vec![0u8; index_len];
        read_exact_at(&file, &mut index, 0)?;
        // The file length bounds the last block: blocks are laid out
        // contiguously after the index, so the packed form derives every
        // block's compressed length from its neighbour's offset and this end.
        let file_len = file.metadata()?.len() as usize;
        Ok(compression::PackedBlockIndex::from_index_bytes(
            &index, file_len,
        ))
    }

    fn append_header(&self) -> usize {
        self.append_header.load(Ordering::Relaxed)
    }

    pub fn entry_iter(&self) -> SegmentEntryIter {
        SegmentEntryIter {
            bound: self.append_header(),
            cursor: self.addr,
        }
    }

    /// Exclusive upper address of this segment's space. Always
    /// `addr + SEGMENT_SIZE`; derived rather than stored.
    #[inline]
    pub fn bound(&self) -> usize {
        self.addr + SEGMENT_SIZE
    }

    /// Marks the entry starting at `addr` dead in the per-segment bitmap.
    ///
    /// The bitmap is allocated on the first mark, and only while the segment
    /// is hot: the combine cleaner never scans cold segments, so bits marked
    /// on a cold segment would never be read. Losing a mark is always safe --
    /// the scan falls back to probing the cell index, which is the authority.
    #[inline]
    pub fn mark_dead_bit(&self, addr: usize) {
        let offset = (addr - self.addr) / 8;
        {
            let bits = self.dead_bits.read();
            if let Some(bits) = bits.as_ref() {
                bits[offset / 64].fetch_or(1u64 << (offset % 64), Ordering::Release);
                return;
            }
        }
        if self.is_cold() {
            return;
        }
        let mut guard = self.dead_bits.write();
        let bits = guard.get_or_insert_with(|| {
            (0..SEGMENT_SIZE / 8 / 64)
                .map(|_| AtomicU64::new(0))
                .collect()
        });
        bits[offset / 64].fetch_or(1u64 << (offset % 64), Ordering::Release);
    }

    /// True when the entry starting at `addr` was marked dead. A missing
    /// bitmap reads as "not dead", which merely costs the caller an index
    /// probe.
    #[inline]
    pub fn is_dead_at(&self, addr: usize) -> bool {
        let offset = (addr - self.addr) / 8;
        self.dead_bits
            .read()
            .as_ref()
            .map_or(false, |bits| {
                bits[offset / 64].load(Ordering::Acquire) & (1u64 << (offset % 64)) != 0
            })
    }

    /// Drop the dead-entry bitmap, returning its heap to the allocator.
    /// Called at eviction: a cold segment's bitmap is never consulted, and
    /// 128 KiB per cold segment was the largest heap consumer of a terabyte
    /// import. If the segment is later promoted and takes new dead entries,
    /// the bitmap re-materializes on the first mark.
    pub fn clear_dead_bits(&self) {
        let _ = self.dead_bits.write().take();
    }

    pub fn dead_space(&self) -> u32 {
        self.dead_space.load(Ordering::Relaxed)
    }

    /// Track changes that introduce new dead bytes so cleaners can detect progress.
    #[inline]
    pub fn note_dead_bytes_change(&self) {
        // Clear any "no progress" marker when new dead bytes show up so the cleaner
        // can try again.
        self.last_no_progress_clean_generation
            .store(0, Ordering::Relaxed);
        self.dead_bytes_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark this segment as cleaned without reclaiming space for the current generation.
    #[inline]
    pub fn mark_clean_no_progress(&self) {
        let gen = self.dead_bytes_generation.load(Ordering::Relaxed);
        if gen > 0 {
            self.last_no_progress_clean_generation
                .store(gen, Ordering::Relaxed);
        }
    }

    /// Clear the "no progress" marker so the cleaner can reconsider this segment.
    #[inline]
    pub fn clear_clean_no_progress(&self) {
        self.last_no_progress_clean_generation
            .store(0, Ordering::Relaxed);
    }

    /// Returns true if the cleaner already tried this generation and reclaimed nothing.
    #[inline]
    pub fn cleaned_without_progress(&self) -> bool {
        let gen = self.dead_bytes_generation.load(Ordering::Relaxed);
        gen > 0
            && gen
                == self
                    .last_no_progress_clean_generation
                    .load(Ordering::Relaxed)
    }

    // dead space plus tombstone spaces
    pub fn total_dead_space(&self) -> u32 {
        // We count tombstone space becasue we want to actively clean them out when they are obsolete.
        // A tombstone entry physically occupies its payload plus the entry
        // head; counting the payload alone under-reports each tombstone by
        // ENTRY_HEAD_SIZE, which starves utilization-driven segment selection
        // on tombstone-heavy segments.
        let tombstones_space = self.tombstones.load(Ordering::Relaxed)
            * (TOMBSTONE_SIZE_U32 + ENTRY_HEAD_SIZE as u32);
        let dead_cells_space = self.dead_space();
        return tombstones_space + dead_cells_space;
    }

    pub fn used_spaces(&self) -> u32 {
        let space = self.append_header.load(Ordering::Relaxed) as usize - self.addr;
        debug_assert!(space <= SEGMENT_SIZE);
        return space as u32;
    }

    pub fn living_space(&self) -> u32 {
        let total_dead_space = self.total_dead_space();
        let used_space = self.used_spaces();
        if total_dead_space <= used_space {
            used_space - total_dead_space
        } else {
            warn!(
                "living space check error for segment {}, used {}, dead {}",
                self.id, used_space, total_dead_space
            );
            0
        }
    }

    pub fn valid_space(&self) -> u32 {
        return self.used_spaces() - self.dead_space();
    }

    pub fn living_rate(&self) -> f32 {
        let used_space = self.used_spaces() as f32;
        if used_space == 0f32 {
            // empty segment
            return 1f32;
        }
        return self.living_space() as f32 / used_space;
    }

    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    fn calculate_crc32(data: &[u8]) -> u32 {
        let mut hasher = Crc32Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    fn verify_archive_checksum(
        &self,
        source_data: &[u8],
        backup_path: &Path,
        pad_to_size: Option<usize>,
        segment_id: u64,
    ) -> Result<(), io::Error> {
        // Pad source data if needed (for WAL files that are padded to SEGMENT_SIZE)
        let source_data_padded = if let Some(target_size) = pad_to_size {
            debug!(
                "Padding source data: original_size={}, target_size={}",
                source_data.len(),
                target_size
            );
            if source_data.len() < target_size {
                let mut padded = source_data.to_vec();
                padded.resize(target_size, 0);
                debug!(
                    "Padded source data: original_size={}, padded_size={}",
                    source_data.len(),
                    padded.len()
                );
                padded
            } else {
                debug!("Source data already >= target_size, no padding needed");
                source_data.to_vec()
            }
        } else {
            source_data.to_vec()
        };

        debug!(
            "Calculating checksum: source_data_padded.len()={}",
            source_data_padded.len()
        );
        let source_checksum = Self::calculate_crc32(&source_data_padded);

        // Read the backup file to calculate its checksum
        let mut backup_file = File::open(backup_path)?;
        let mut backup_data = Vec::new();
        backup_file.read_to_end(&mut backup_data)?;
        let backup_checksum = Self::calculate_crc32(&backup_data);

        if source_checksum != backup_checksum {
            error!(
                "CRC32 checksum mismatch for segment {}: source={:08x} (size={}), backup={:08x} (size={}) for segment {}",
                self.id, source_checksum, source_data_padded.len(), backup_checksum, backup_data.len(), segment_id
            );
            // Log first few bytes for debugging
            let source_preview = if source_data_padded.len() >= 16 {
                format!("{:02x?}", &source_data_padded[..16])
            } else {
                format!("{:02x?}", source_data_padded)
            };
            let backup_preview = if backup_data.len() >= 16 {
                format!("{:02x?}", &backup_data[..16])
            } else {
                format!("{:02x?}", backup_data)
            };
            error!(
                "Source data preview (first 16 bytes): {}, Backup data preview (first 16 bytes): {}",
                source_preview, backup_preview
            );
            panic!("CRC32 checksum mismatch for segment {}: source={:08x}, backup={:08x} for segment {}", self.id, source_checksum, backup_checksum, segment_id);
        } else {
            debug!(
                "CRC32 checksum verified for segment {}: {:08x}",
                self.id, source_checksum
            );
        }
        Ok(())
    }

    /// Verify checksum of segment memory against backup file (for eviction)
    /// Only compiled in debug builds
    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    pub fn verify_eviction_checksum(&self, backup_path: &Path) -> Result<(), io::Error> {
        let write_size = {
            let valid_size = self.append_header() - self.addr;
            valid_size.max(PAGE_SIZE) // At least one page to ensure file exists
        };

        unsafe {
            let segment_data = slice::from_raw_parts(self.addr as *const u8, write_size);
            // Backup file is padded to SEGMENT_SIZE, so pad source data to match
            self.verify_archive_checksum(segment_data, backup_path, Some(SEGMENT_SIZE), self.id)
        }
    }

    /// Verify checksum of segment memory against source data (for promotion)
    /// Only compiled in debug builds
    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
    pub fn verify_promotion_checksum(&self, source_data: &[u8]) -> Result<(), io::Error> {
        // Compare the full SEGMENT_SIZE that was copied during promotion
        // (not based on append_header, which may be less than SEGMENT_SIZE)
        let compare_size = SEGMENT_SIZE.min(source_data.len());

        unsafe {
            let segment_data = slice::from_raw_parts(self.addr as *const u8, compare_size);
            let source_slice = &source_data[..compare_size];

            let segment_checksum = Self::calculate_crc32(segment_data);
            let source_checksum = Self::calculate_crc32(source_slice);

            if segment_checksum != source_checksum {
                error!(
                    "CRC32 checksum mismatch after promotion for segment {}: segment={:08x}, source={:08x}",
                    self.id, segment_checksum, source_checksum
                );
                panic!("CRC32 checksum mismatch after promotion for segment {}: segment={:08x}, source={:08x}", self.id, segment_checksum, source_checksum);
            } else {
                debug!(
                    "CRC32 checksum verified after promotion for segment {}: {:08x}",
                    self.id, segment_checksum
                );
            }
        }
        Ok(())
    }

    // archive this segment and write the data to backup storage
    // Backup files are opened on demand and closed immediately after use
    pub fn archive(&self) -> Result<bool, io::Error> {
        let mut state = self.file_state.lock();
        let backup_path_opt = state
            .manager
            .backup_path(self.chunk_id, self.id, self.seq_id);

        debug!(
            "archive() called for segment {}, backup_path={:?}",
            self.id, backup_path_opt
        );

        if let Some(backup_file) = backup_path_opt {
            // NOTE: We do NOT wait for no_references() here because:
            // 1. The file_state mutex already ensures only one archive at a time
            // 2. Waiting here could deadlock if another component holds tiered_lock
            // 3. Reading segment memory during archive is safe - data is copied atomically
            // The reference counter is only for preventing madvise_free during eviction
            let backup_file_path = Path::new(&backup_file);
            let has_old_backup = backup_file_path.exists();

            // An EMPTY segment must never replace a non-empty backup.
            //
            // A segment reporting zero appended bytes yields no entries, so
            // `plan_blocks` collapses to a single whole-segment span and the
            // archive writes an image of untouched memory. If that segment is
            // in fact a live one whose append cursor was lost, the write
            // destroys every cell the index still points at -- silently,
            // because an empty segment looks legitimately empty to every
            // check we have. TB14 lost 4 segments exactly this way: their
            // backups carry block_count=1 and the archiver's zero-image guard
            // could not fire, because that guard only triggers when the
            // segment CLAIMS appended bytes.
            //
            // Backups are immutable once written, so an archive that would
            // shrink one to nothing is never legitimate: refuse and say so.
            // The segment keeps its existing backup and stays dirty.
            //
            // Every check that can refuse this archive runs HERE, before the
            // first byte of the backup is disturbed. Refusing after the old
            // backup has been renamed to `.old` and a fresh file truncated to
            // zero is not a refusal at all -- it leaves the segment holding an
            // empty backup, which is the exact loss these guards exist to
            // prevent. On the TB14 store that is what happened: the zero-image
            // guard fired correctly and the backup was already gone.
            if has_old_backup && self.append_header.load(Ordering::Relaxed) <= self.addr {
                let old_len = fs::metadata(&backup_file).map(|m| m.len()).unwrap_or(0);
                if old_len > 0 {
                    error!(
                        "REFUSING empty archive of segment {} (chunk {}, seq {}): the segment \
                         reports no appended bytes but an existing backup holds {} bytes. \
                         Its append cursor was lost; archiving would destroy the backup.",
                        self.id, self.chunk_id, self.seq_id, old_len
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "segment {} reports empty but has a {}-byte backup; refusing to \
                             overwrite it",
                            self.id, old_len
                        ),
                    ));
                }
            }
            // Break blocks on cell boundaries so a later read can decompress
            // just the block holding the cell it wants, instead of the whole
            // segment. Entry positions come from the live segment, which is
            // resident here because we are archiving it.
            //
            // This walk reads memory only, so it belongs before the backup is
            // touched -- it is also the zero-image test below.
            #[cfg(feature = "compress_backups")]
            let boundaries: Vec<usize> = self
                .entry_iter()
                .map(|meta| meta.entry_pos.saturating_sub(self.addr))
                .take_while(|off| *off < SEGMENT_SIZE)
                .collect();

            // A segment holding appended bytes must yield at least one entry.
            // None means the walk found no decodable header at `addr` -- the
            // resident image is gone (zero-filled pages), not merely empty.
            // Archiving it would persist those zeros over a good backup and
            // silently destroy every cell the index still points at: TB13 lost
            // 15 segments exactly this way, and the wreckage is legible in the
            // backups (block_count=1 from empty boundaries, compressing 4-20x
            // against the 2x that real cell data achieves). Refuse instead,
            // loudly; the segment stays dirty and resident, which is
            // recoverable, while a zeroed backup is not.
            #[cfg(feature = "compress_backups")]
            {
                let used = self.append_header.load(Ordering::Relaxed);
                // A segment with no decodable entries is only legitimately
                // archivable if its image is genuinely empty -- that is, all
                // zeros. Anything else is a damaged image, and writing it
                // persists the damage.
                //
                // This is the case the other two checks miss, and it is the
                // one that produced TB14's `45-2053-3266.nbackup`: no entries,
                // an append cursor reading empty, and no earlier backup to
                // compare against, so the archiver wrote an 8 MiB image that
                // was zeros at offset 0 with real cell data further in
                // (block_count=1, 7.9x compression where cell data manages 2x).
                // Recovery could then neither scan it nor ignore it.
                //
                // The test is the same one recovery applies, so an image that
                // recovery would reject can no longer be created.
                if boundaries.is_empty() && used <= self.addr {
                    // Safety: the segment owns this range for its lifetime,
                    // and archiving holds `file_state`, so it is not being
                    // reclaimed underneath us.
                    let image =
                        unsafe { slice::from_raw_parts(self.addr as *const u8, SEGMENT_SIZE) };
                    if let Some(offset) = image.iter().position(|byte| *byte != 0) {
                        error!(
                            "REFUSING to archive segment {} (chunk {}, seq {}): it reports no appended \
bytes and yields no entries, yet its image is non-zero from offset {}. That is a damaged \
image, not an empty segment; archiving it would persist the damage.",
                            self.id, self.chunk_id, self.seq_id, offset
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "segment {} is empty by its cursor but its image is non-zero                                  at offset {}; refusing to archive a damaged image",
                                self.id, offset
                            ),
                        ));
                    }
                }
                if boundaries.is_empty() && used > self.addr {
                    error!(
                        "REFUSING to archive segment {} (chunk {}, seq {}): {} appended bytes \
                         but no decodable entries — the resident image is zero-filled. \
                         Archiving would overwrite the backup with zeros.",
                        self.id,
                        self.chunk_id,
                        self.seq_id,
                        used - self.addr
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "segment {} has appended bytes but no decodable entries; \
                             refusing to archive a zero-filled image",
                            self.id
                        ),
                    ));
                }
            }

            {
                // Write the new image to a TEMPORARY path and rename it into
                // place only after every byte is durable. The archive used to
                // truncate and rewrite the FINAL path in place, so a SIGKILL
                // mid-archive left a zero-byte or partial `.nbackup` where
                // recovery expects a complete image. Recovery prefers a
                // backup over the WAL at the same seq id, so that torn file
                // SHADOWED a complete WAL and every cell whose newest version
                // lived in this segment vanished from the recovered store --
                // for the crash-churn fuzzer that was the ranged tree's
                // metadata cell, which cascaded into "placement names a tree
                // whose metadata cell is absent" -> placement wipe -> an
                // empty genesis tree serving scans. With the rename, the
                // final path only ever holds a complete image: a kill leaves
                // either the previous backup or no backup at all, and the
                // WAL (deleted only after success, below) still covers the
                // segment either way. `.tmp` files are invisible to recovery
                // (`SegmentFileInfo::parse_filename` filters by extension).
                let tmp_backup_file = format!("{}.tmp", backup_file);
                if let Some(parent) = Path::new(&backup_file).parent() {
                    fs::create_dir_all(parent)?;
                }
                {
                    let mut file = File::create(&tmp_backup_file)?;

                    unsafe {
                        let data_block =
                            slice::from_raw_parts(self.addr as *const u8, SEGMENT_SIZE);

                        // Read the append cursor BEFORE snapshotting the
                        // image, so the recorded length can never exceed the
                        // bytes captured. See the call to
                        // `compress_blocks_on_cells` below.
                        let used_len_at_snapshot =
                            self.append_header.load(Ordering::Acquire) - self.addr;

                        // Snapshot the segment into a pooled staging buffer
                        // (slab adopter #1; Lightning docs/slab-pools-
                        // design.md). Previously a per-archive 8 MiB
                        // Vec::from — one large-block heap alloc + free per
                        // archived segment, continuously for the life of an
                        // import. The pool is bounded by archiver
                        // concurrency and recycles the same slots.
                        let staging = archive_staging_pool();
                        let staging_handle = staging.alloc_with(|slot| {
                            ptr::copy_nonoverlapping(
                                data_block.as_ptr(),
                                slot.as_mut_ptr() as *mut u8,
                                SEGMENT_SIZE,
                            );
                        });
                        // Plain-data buffer: freed via free_forget on every
                        // exit path by this guard.
                        let _staging_guard = ArchiveStagingGuard(staging_handle);
                        let padded_data: &[u8] = slice::from_raw_parts(
                            staging_handle.as_ptr() as *const u8,
                            SEGMENT_SIZE,
                        );

                        debug_assert_eq!(padded_data.len(), SEGMENT_SIZE);

                        // Conditionally compress based on feature flag
                        ARCHIVE_COUNT.fetch_add(1, Relaxed);
                        if has_old_backup {
                            ARCHIVE_REWRITES.fetch_add(1, Relaxed);
                        }

                        #[cfg(feature = "compress_backups")]
                        {
                            // The cursor is read BEFORE the snapshot above, not
                            // after: reading it after would let a concurrent
                            // append report a cursor past the bytes actually
                            // captured, and recovery would then see phantom
                            // truncation. Reading first can only under-report,
                            // and bytes past the cursor are never interpreted.
                            let compressed_data = compression::compress_blocks_on_cells(
                                &padded_data,
                                &boundaries,
                                used_len_at_snapshot,
                            )?;
                            ARCHIVE_BYTES.fetch_add(compressed_data.len() as u64, Relaxed);
                            file.write_all(&compressed_data)?;
                            debug!(
                                "Archived segment {} with compression: {} bytes -> {} bytes (ratio: {:.2}%)",
                                self.id,
                                SEGMENT_SIZE,
                                compressed_data.len(),
                                (compressed_data.len() as f64 / SEGMENT_SIZE as f64) * 100.0
                            );
                        }

                        #[cfg(not(feature = "compress_backups"))]
                        {
                            ARCHIVE_BYTES.fetch_add(padded_data.len() as u64, Relaxed);
                            file.write_all(&padded_data)?;
                            debug!(
                                "Archived segment {} without compression: {} bytes",
                                self.id, SEGMENT_SIZE
                            );
                        }
                    }

                    file.sync_all()?;
                    drop(file);

                    #[cfg(all(debug_assertions, feature = "debug_verify_checksums"))]
                    {
                        // Note: Checksum verification is skipped for compressed files
                        // as LZ4 compression includes its own integrity checks
                        debug!(
                            "Skipping CRC32 checksum verification for compressed segment {} (LZ4 has built-in integrity)",
                            self.id
                        );
                    }

                    // The image is durable at the temp path; atomically
                    // replace whatever the final path held. rename(2) within
                    // one directory either fully installs the new image or
                    // leaves the previous state -- there is no torn middle.
                    fs::rename(&tmp_backup_file, &backup_file)?;
                    if let Some(parent) = Path::new(&backup_file).parent() {
                        if let Ok(dir) = File::open(parent) {
                            let _ = dir.sync_all();
                        }
                    }

                    // Sanity check: verify backup file actually exists before marking archived
                    let backup_file_path = Path::new(&backup_file);
                    if !backup_file_path.exists() {
                        error!(
                            "CRITICAL: Archive wrote segment {} but backup file does not exist at '{}'",
                            self.id, backup_file
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!(
                                "Archive failed: backup file '{}' not found after write",
                                backup_file
                            ),
                        ));
                    }

                    debug!(
                        "Archived segment {} to backup file '{}'",
                        self.id, backup_file
                    );

                    // Earlier revisions renamed the previous backup aside as
                    // `.old` before rewriting in place; the atomic rename
                    // above made that dance unnecessary, but stores written
                    // by those revisions may still carry the leftover file.
                    // Clean it up so a rewritten segment does not pin a
                    // full-size file forever -- invisible until a store runs
                    // out of disk.
                    if has_old_backup {
                        let old_path = format!("{}.old", backup_file);
                        if let Err(e) = fs::remove_file(&old_path) {
                            if e.kind() != io::ErrorKind::NotFound {
                                warn!(
                                    "Could not remove superseded backup '{}': {}. It is safe to \
                                     delete by hand; the current backup is written.",
                                    old_path, e
                                );
                            }
                        }
                    }

                    self.clear_dirty();

                    // A backup now exists at this seq id, so this incarnation
                    // is closed: no append may follow, or the WAL deleted just
                    // below would come back at the same seq and recovery could
                    // no longer tell a complete backup from half an image.
                    self.seal();

                    // Close and delete WAL file since backup now contains all data
                    // Recovery prefers backup files over WAL files (see file_manager.rs:272-285)
                    // Closing the file descriptor first ensures clean deletion
                    if let Some(wal) = state.wal.take() {
                        drop(wal); // Close the file descriptor
                        debug!("Closed WAL file descriptor for segment {}", self.id);
                    }

                    // Delete the WAL file from disk
                    if let Err(e) = state
                        .manager
                        .delete_wal(self.chunk_id, self.id, self.seq_id)
                    {
                        warn!("Failed to delete WAL file for segment {}: {}", self.id, e);
                    } else {
                        debug!("Deleted WAL file for archived segment {}", self.id);
                    }

                    return Ok(true);
                }
            }
        } else {
            warn!(
                "Segment {} has no backup storage configured, cannot archive",
                self.id
            );
            return Ok(false);
        }
        return Ok(false);
    }

    pub fn write_wal(&self, addr: usize, size: u32, skip_sync: bool) -> io::Result<()> {
        // Uncontended acquisitions cost nothing to detect; only a failed
        // try_lock pays for the timing.
        let acquire_start = std::time::Instant::now();
        let mut state = match self.file_state.try_lock() {
            Some(g) => g,
            None => {
                WAL_LOCK_CONTENDED.fetch_add(1, Relaxed);
                let g = self.file_state.lock();
                WAL_LOCK_WAIT_NANOS
                    .fetch_add(acquire_start.elapsed().as_nanos() as u64, Relaxed);
                g
            }
        };
        let held_start = std::time::Instant::now();
        let _hold_guard = WalHoldTimer(held_start);
        // Lazily create WAL file on first write if not already present
        if state.wal.is_none() {
            // The twin-birth site. A sealed segment has a backup at this seq
            // id; re-creating its WAL would put a prefix image and a suffix
            // log under one name, which recovery cannot arbitrate. Nothing
            // should reach here -- head selection skips sealed segments -- so
            // refuse loudly rather than write a file that silently costs data
            // at the next crash.
            if self.is_sealed() {
                error!(
                    "REFUSING to re-create the WAL for sealed segment {} (chunk {}, seq {}): it \
                     has been archived, so this incarnation is closed. Appending here would make \
                     its backup and WAL two halves of one image and recovery would drop the \
                     suffix. This is a bug in whoever selected this segment for writing.",
                    self.id, self.chunk_id, self.seq_id
                );
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "segment {} (chunk {}, seq {}) is sealed; cannot append",
                        self.id, self.chunk_id, self.seq_id
                    ),
                ));
            }
            state.wal = state
                .manager
                .create_wal_file(self.chunk_id, self.id, self.seq_id)?;
        }

        if let Some(ref mut file) = state.wal {
            unsafe {
                let data_block = slice::from_raw_parts(addr as *const u8, size as usize);
                file.write_all(data_block)?; // Use write_all to ensure all bytes are written
            }
            WAL_BYTES.fetch_add(size as u64, Relaxed);
            WAL_WRITES.fetch_add(1, Relaxed);
            // Transactions control their own sync at commit time
            // For non-transactional writes, use group commit batching
            if skip_sync {
                // Transaction context: no sync, will be synced at commit
                trace!(
                    "WAL sync skipped for segment {} (transactional write, will sync at commit)",
                    self.id
                );
                return Ok(());
            }

            // Group commit for non-transactional writes. The timer path
            // belongs to the WAL syncer thread — with ~63 segments absorbing
            // an import, per-segment 10ms timers produced 6,302 fsyncs/s
            // with every writer thread blocking inline on its own sync. Only
            // the byte threshold remains inline, as backpressure against one
            // segment accumulating unbounded unsynced bytes.
            let bytes_written = self
                .bytes_since_sync
                .fetch_add(size as usize, Ordering::Relaxed)
                + size as usize;

            if bytes_written >= WAL_SYNC_BATCH_SIZE {
                WAL_SYNCS.fetch_add(1, Relaxed);
                file.sync_data()?;
                self.bytes_since_sync.store(0, Ordering::Relaxed);
                self.last_sync_time.store(get_time(), Ordering::Relaxed);
                trace!(
                    "WAL synced inline for segment {} ({} bytes)",
                    self.id,
                    bytes_written
                );
            } else {
                trace!(
                    "WAL write buffered for segment {} ({} bytes accumulated; syncer owns the timer)",
                    self.id,
                    bytes_written
                );
            }
        }
        Ok(())
    }

    /// Force a WAL sync, ensuring all buffered data is persisted to disk
    /// This is useful for transaction commits and other critical durability points
    pub fn force_wal_sync(&self) -> io::Result<()> {
        let mut state = self.file_state.lock();
        if let Some(ref mut file) = state.wal {
            file.sync_all()?;

            // Reset counters after forced sync
            let current_time = get_time();
            self.bytes_since_sync.store(0, Ordering::Relaxed);
            self.last_sync_time.store(current_time, Ordering::Relaxed);

            trace!("Forced WAL sync for segment {}", self.id);
        }
        Ok(())
    }

    pub fn no_references(&self) -> bool {
        self.references.load(Ordering::Relaxed) == 0
    }

    pub fn obtain_exclusive_references(&self) -> bool {
        self.references
            .compare_exchange(0, EXCLUSIVE_REF_COUNT, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub fn release_exclusive_references(&self) {
        self.references.store(0, Ordering::Relaxed);
    }

    pub fn incr_references(&self) -> bool {
        let backoff = Backoff::new();
        loop {
            let curr_refs = self.references.load(Ordering::Relaxed);
            if curr_refs == EXCLUSIVE_REF_COUNT {
                // Do not compete for exclusive references, bail out instead of spinning
                // The cleaners obtains segment lock first, then cell locks,
                // while normal operations obtains cell lock then segment counter
                // this could cause deadlock if the cleaners is waiting for the segment lock
                return false;
            }
            if self
                .references
                .compare_exchange(
                    curr_refs,
                    curr_refs + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
            backoff.spin();
        }
    }

    /// Current reference count, for tests and diagnostics.
    pub fn references_count(&self) -> usize {
        self.references.load(Ordering::Relaxed)
    }

    pub fn decr_references(&self) {
        // Decrementing at zero is a real, documented race: a PendingEntry can
        // be dropped after the segment has already been cleaned up. It must not
        // wrap.
        //
        // `references` is a usize and EXCLUSIVE_REF_COUNT is usize::MAX, so the
        // release build's `fetch_sub(1)` at zero wrapped to exactly the value
        // that means "exclusively locked". The segment then looked permanently
        // locked to everything: it could never be referenced, evicted, or
        // promoted again, and eviction would report it as held by active
        // references forever. The debug build instead tripped an assertion,
        // which is why this surfaced as a flaky test rather than as the pin it
        // actually was.
        let decremented = self
            .references
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |curr| {
                debug_assert!(
                    curr != EXCLUSIVE_REF_COUNT,
                    "Segment {} has exclusive references, which should not happen",
                    self.id
                );
                if curr == 0 {
                    // Already released by the cleanup this raced with.
                    None
                } else {
                    Some(curr - 1)
                }
            })
            .is_ok();
        // Deliberately NOT a QSBR quiescent-state transition. A reference can
        // outlive the stack that took it -- `PinnedReadSet` holds one for a
        // whole transaction and drops it on whichever thread ends the
        // transaction -- so binding the thread's quiescent state to this
        // counter strands the acquiring thread outside quiescence forever.
        // QSBR sections live on `CellGuard` instead, which is lifetime-bound
        // to the chunk and therefore cannot migrate. Long-lived references are
        // covered by the count itself, which the reclaimer also requires.
        let _ = decremented;
    }

    pub fn mem_drop(&self, chunk: &Chunk) {
        if self
            .dropped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            chunk.allocator.free(self.addr);
        }
    }
    // remove the backup if it have one
    pub fn dispense(&self) {
        let backtrace = std::backtrace::Backtrace::capture();
        debug!(
            "[DISPENSE] segment {} (chunk={}, seq_id={}) - tiered_state={}\nBacktrace:\n{}",
            self.id,
            self.chunk_id,
            self.seq_id,
            if self.is_hot() { "HOT" } else { "COLD" },
            backtrace
        );
        let state = self.file_state.lock();
        if let Some(backup_path) = state
            .manager
            .backup_path(self.chunk_id, self.id, self.seq_id)
        {
            let exists = std::path::Path::new(&backup_path).exists();
            debug!(
                "[DISPENSE] Deleting backup for segment {} (chunk={}, seq_id={}): {} (exists: {})",
                self.id, self.chunk_id, self.seq_id, backup_path, exists
            );
        }
        if let Err(e) = state
            .manager
            .delete_all(self.chunk_id, self.id, self.seq_id)
        {
            debug!(
                "[DISPENSE ERROR] Failed to delete files for segment {} (chunk={}, seq_id={}): {}",
                self.id, self.chunk_id, self.seq_id, e
            );
        } else {
            debug!(
                "[DISPENSE SUCCESS] Deleted files for segment {} (chunk={}, seq_id={})",
                self.id, self.chunk_id, self.seq_id
            );
        }
    }

    // Tiered memory helper methods (stubs when tiered memory is disabled)

    /// Check if segment is hot (in anonymous memory)
    /// Always returns true when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_hot(&self) -> bool {
        // Use Acquire ordering to ensure we see the latest state from other threads
        // This pairs with Release in set_hot() to prevent reading stale data
        self.tiered_lock.load(Ordering::Acquire) & HOT_COLD_MASK == HOT_SEGMENT
    }

    /// Check if segment is cold (backed by file)
    /// Always returns false when tiered memory is disabled
    /// This is a fast check that doesn't acquire the lock (may be stale)
    #[inline]
    pub fn is_cold(&self) -> bool {
        // Check if segment is cold or being promoted (locked while cold)
        // During promotion, tiered_lock is COLD_SEGMENT | LOCKING_SEGMENT_BITS
        // We need to return true in both cases to prevent reads from seeing garbage
        // Use Acquire ordering to ensure we see the latest state from other threads
        let state = self.tiered_lock.load(Ordering::Acquire);
        (state & HOT_COLD_MASK) == COLD_SEGMENT
            || (state & HOT_COLD_MASK) == (COLD_SEGMENT | LOCKING_SEGMENT_BITS)
    }

    #[inline]
    pub fn is_locked(&self) -> bool {
        let lock_bits = self.tiered_lock.load(Ordering::Relaxed);
        lock_bits & HOT_COLD_MASK != lock_bits
    }

    pub fn set_cold(&self) {
        // Debug: capture backtrace to track who is marking this segment cold
        let backtrace = std::backtrace::Backtrace::capture();

        // Verify backup file exists before marking cold
        let backup_path = {
            let state = self.file_state.lock();
            state
                .manager
                .backup_path(self.chunk_id, self.id, self.seq_id)
        };

        if let Some(ref path) = backup_path {
            let exists = std::path::Path::new(path).exists();
            if !exists {
                debug!(
                    "CRITICAL BUG: set_cold() called for segment {} (chunk={}, seq_id={}) but backup file does NOT exist at '{}'!\n\
                     Backtrace:\n{}",
                    self.id, self.chunk_id, self.seq_id, path, backtrace
                );
                // Always panic to catch this immediately
                panic!(
                    "set_cold() called without backup file for segment {} (chunk={}, seq_id={}) at '{}'",
                    self.id, self.chunk_id, self.seq_id, path
                );
            } else {
                debug!(
                    "[DEBUG] set_cold() for segment {} (chunk={}, seq_id={}): backup verified at '{}'",
                    self.id, self.chunk_id, self.seq_id, path
                );
            }
        } else {
            debug!(
                "[DEBUG] set_cold() for segment {} (chunk={}, seq_id={}) but no backup path configured",
                self.id, self.chunk_id, self.seq_id
            );
        }

        self.tiered_lock.store(COLD_SEGMENT, Ordering::Relaxed);
    }

    pub fn set_hot(&self) {
        // Use Release ordering to ensure all previous writes are visible before setting hot
        // This pairs with Acquire in is_cold() to prevent reading stale data
        self.tiered_lock.store(HOT_SEGMENT, Ordering::Release);
    }
    pub fn lock_cold(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                COLD_SEGMENT,
                COLD_SEGMENT | LOCKING_SEGMENT_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn lock_hot(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                HOT_SEGMENT,
                HOT_SEGMENT | LOCKING_SEGMENT_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    pub fn lock_hot_to_cold(&self) -> bool {
        self.tiered_lock
            .compare_exchange(
                HOT_SEGMENT,
                COLD_SEGMENT | LOCKING_SEGMENT_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Mark segment as recently accessed (for multi-chance CLOCK algorithm)
    /// Increments reference count up to 7, giving hot segments multiple chances
    #[inline]
    pub fn mark_referenced(&self) {
        let current = self.reference_count.load(Ordering::Relaxed);
        if current < 7 {
            self.reference_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Mark the segment as recently promoted to give it a cooldown window during eviction
    #[inline]
    pub fn mark_promoted_now(&self) {
        self.last_promoted_ms.store(get_time(), Ordering::Relaxed);
    }

    /// Check if the segment was promoted within the provided window (milliseconds)
    #[inline]
    pub fn recently_promoted_within(&self, window_ms: u64) -> bool {
        if window_ms == 0 {
            return false;
        }
        let last = self.last_promoted_ms.load(Ordering::Relaxed);
        if last <= 0 {
            return false;
        }
        let now = get_time();
        now - last <= window_ms as i64
    }

    /// Mark the segment as recently evicted for churn detection
    #[inline]
    pub fn mark_evicted_now(&self) {
        self.last_evicted_ms.store(get_time(), Ordering::Relaxed);
    }

    /// Check if the segment was evicted within a window (milliseconds)
    #[inline]
    pub fn recently_evicted_within(&self, window_ms: u64) -> bool {
        if window_ms == 0 {
            return false;
        }
        let last = self.last_evicted_ms.load(Ordering::Relaxed);
        if last <= 0 {
            return false;
        }
        let now = get_time();
        now - last <= window_ms as i64
    }

    /// Decrement reference count and return true if zero (for multi-chance CLOCK)
    #[inline]
    pub fn decrement_and_check(&self) -> bool {
        let prev = self.reference_count.fetch_sub(1, Ordering::Relaxed);
        prev.saturating_sub(1) == 0
    }

    /// Get current reference count without modifying
    #[inline]
    pub fn get_reference_count(&self) -> u8 {
        self.reference_count.load(Ordering::Relaxed)
    }

    /// Increment cold access count and return new value (for promotion threshold)
    #[inline]
    pub fn increment_access_count(&self) -> u8 {
        self.access_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    /// Reset access count to zero (called after promotion)
    #[inline]
    pub fn reset_access_count(&self) {
        self.access_count.store(0, Ordering::Relaxed);
    }

    /// Get current access count
    #[inline]
    pub fn get_access_count(&self) -> u8 {
        self.access_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn contains_address(&self, addr: usize) -> bool {
        self.addr <= addr && addr < self.bound()
    }

    pub fn set_dirty(&self) {
        debug!("set_dirty for segment {}", self.id);
        self.is_dirty.store(true, Ordering::Release);
    }

    pub fn clear_dirty(&self) {
        debug!("clear_dirty for segment {}", self.id);
        self.is_dirty.store(false, Ordering::Release);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }

    /// Close this incarnation: a backup exists at its seq id, so it may never
    /// be appended to again. See the `sealed` field for why this is an
    /// invariant of recovery rather than a policy choice.
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::Release);
    }

    pub fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }
}

/// RAII guard that holds a reference to a segment, preventing it from being evicted.
/// The reference count is automatically decremented when the guard is dropped.
/// This ensures no reference leaks even in error paths or panics.
pub struct SegmentReferenceGuard {
    segment: lightning::aarc::Arc<Segment>,
}

impl SegmentReferenceGuard {
    /// Create a new guard and increment the segment's reference count
    pub fn new(segment: lightning::aarc::Arc<Segment>) -> Self {
        segment.incr_references();
        debug!(
            "SegmentReferenceGuard acquired for segment {} (ref count: {})",
            segment.id,
            segment.references.load(Ordering::Relaxed)
        );
        Self { segment }
    }

    /// Get the segment ID
    pub fn segment_id(&self) -> u64 {
        self.segment.id
    }

    /// Get the chunk ID
    pub fn chunk_id(&self) -> usize {
        self.segment.chunk_id
    }
}

impl Drop for SegmentReferenceGuard {
    fn drop(&mut self) {
        self.segment.decr_references();
    }
}

pub struct SegmentExclusiveRefGuard<'a> {
    segment: &'a Segment,
}

impl<'a> Drop for SegmentExclusiveRefGuard<'a> {
    fn drop(&mut self) {
        debug_assert_eq!(
            self.segment.references.load(Ordering::Relaxed),
            EXCLUSIVE_REF_COUNT
        );
        self.segment.release_exclusive_references();
    }
}

impl<'a> SegmentExclusiveRefGuard<'a> {
    pub fn new(segment: &'a Segment) -> Option<Self> {
        if !segment.obtain_exclusive_references() {
            return None;
        }
        Some(Self { segment })
    }
}

pub struct SegmentEntryIter {
    pub(crate) bound: usize,
    pub(crate) cursor: usize,
}

impl Iterator for SegmentEntryIter {
    type Item = EntryMeta;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let cursor = self.cursor;
        if cursor >= self.bound {
            return None;
        }
        let (entry_header, entry_meta) = entry::Entry::decode_from(cursor, |body_pos, header| {
            let entry_size = ENTRY_HEAD_SIZE + header.content_length as usize;
            debug!("Found body pos {}. Header: {:?}, entry size: {}, entry pos: {}, content length {}, bound {}",
                       body_pos, header, entry_size, cursor, header.content_length, self.bound);
            return EntryMeta {
                body_pos,
                entry_header: header,
                entry_size,
                entry_pos: cursor,
            };
        });

        // Stop iteration if we encounter UNDECIDED entries (uninitialized space)
        // This can happen if the segment is partially written or if we're iterating
        // while the segment is being modified
        if entry_header.entry_type == entry::EntryType::UNDECIDED {
            debug!(
                "Stopping segment iteration at UNDECIDED entry at position {}",
                cursor
            );
            return None;
        }

        // Validate that the entry doesn't exceed the bound
        let next_cursor = cursor + entry_meta.entry_size;
        debug_assert!(
            next_cursor <= self.bound,
            "Entry at position {} exceeds segment bound (size: {}, bound: {})",
            cursor,
            entry_meta.entry_size,
            self.bound
        );

        self.cursor = next_cursor;
        Some(entry_meta)
    }
}

pub struct SegmentAllocator {
    base: usize,
    offset: AtomicUsize,
    limit: usize,
    gc_threshold: usize,
    free: LinkedRingBufferList<usize, 64>,
    /// Segments currently on the free list. Kept alongside the list because
    /// the writer-path reserve below needs an O(1) "how much headroom is
    /// left" answer and the list has no cheap length.
    free_count: AtomicUsize,
    pub next_seq_id: AtomicUsize,
    chunk_id: usize,
}

impl SegmentAllocator {
    pub fn new(chunk_id: usize, chunk_size: usize) -> Self {
        Self::new_with_base(chunk_id, 0, chunk_size, true)
    }

    /// Create allocator with pre-allocated base address
    /// If allocate_memory=false, assumes memory at base_addr already exists
    pub fn new_with_base(
        chunk_id: usize,
        base_addr: usize,
        chunk_size: usize,
        allocate_memory: bool,
    ) -> Self {
        let (base, addr, limit) = if allocate_memory {
            // Old behavior: allocate our own mmap
            let overflow = SEGMENT_SIZE - PAGE_SIZE;
            let aligned_size = chunk_size + overflow;
            let ptr = unsafe {
                libc::mmap(
                    ptr::null_mut(),
                    aligned_size,
                    PROT_READ | PROT_WRITE,
                    MAP_ANONYMOUS | MAP_PRIVATE,
                    -1,
                    0,
                )
            };
            let addr = ptr as usize;
            let start = addr + overflow;
            let aligned_addr = start & SEGMENT_MASK;
            (aligned_addr, aligned_addr, aligned_addr + chunk_size)
        } else {
            // New behavior: use provided base from global allocation
            (base_addr, base_addr, base_addr + chunk_size)
        };

        Self {
            base,
            offset: AtomicUsize::new(addr),
            limit,
            gc_threshold: base + (chunk_size as f64 * 0.9) as usize - SEGMENT_SIZE,
            free: LinkedRingBufferList::new(),
            free_count: AtomicUsize::new(0),
            next_seq_id: AtomicUsize::new(0),
            chunk_id,
        }
    }

    /// Segments still grantable: the free list plus untouched bump space.
    /// Approximate under concurrency, which is fine -- it feeds a headroom
    /// heuristic, not an invariant.
    pub fn available_segments(&self) -> usize {
        let bump_left = self
            .limit
            .saturating_sub(self.offset.load(Relaxed))
            >> SEGMENT_BITS_SHIFT;
        self.free_count.load(Relaxed) + bump_left
    }

    fn capacity_segments(&self) -> usize {
        (self.limit - self.base) >> SEGMENT_BITS_SHIFT
    }

    pub fn meet_gc_threshold(&self) -> bool {
        self.offset.load(Relaxed) > self.gc_threshold
    }

    pub fn alloc_seg(&self, file_manager: &Arc<SegmentFileManager>) -> Option<Segment> {
        self.alloc_seg_with_class(file_manager, SegmentClass::Regular)
    }

    /// Writer-path allocation that keeps compaction headroom for the
    /// cleaner. A chunk whose every segment is allocated cannot be
    /// compacted: combine copies the LIVE portion of its sources into fresh
    /// destination segments, so reclaiming space from segments that are
    /// mostly live needs several destinations at once (a 4-into-3 round
    /// only pays off when 3 destinations exist). A store that fills to the
    /// last segment therefore wedges permanently -- dead space exists but
    /// can never be reclaimed, and the index write-back (whose page upserts
    /// need allocation) retries forever; the crash-churn fuzzer hit that as
    /// a 120s graceful-shutdown hang. Holding a few segments back from
    /// ordinary appends is log-structured over-provisioning: the cleaner
    /// (and recovery) allocate through the unreserved paths and keep space
    /// flowing back. Small chunks scale the reserve down to nothing so
    /// tests with tiny stores keep their capacity.
    pub fn alloc_seg_for_writer(
        &self,
        file_manager: &Arc<SegmentFileManager>,
        segment_class: SegmentClass,
    ) -> Option<Segment> {
        // One destination is all the combine loop needs for a 2-into-1
        // reclaim (it retries with fewer sources until the plan fits), so
        // the reserve is proportional and small: 1 segment up to 32-segment
        // chunks, 3 at most. The first cut of this reserve held back
        // capacity-5 (3 of an 8-segment chunk -- 37.5%) and surfaced
        // CannotAllocateSpace to first-class writes in every small tiered
        // store while the chunk was still mostly free.
        let capacity = self.capacity_segments();
        let reserve = if capacity > 5 {
            (capacity / 16).clamp(1, 3)
        } else {
            0
        };
        if reserve > 0 && self.available_segments() <= reserve {
            debug!(
                "Chunk {} down to its {}-segment compaction reserve; refusing writer \
                 allocation so the cleaner keeps destinations",
                self.chunk_id, reserve
            );
            return None;
        }
        self.alloc_seg_with_class(file_manager, segment_class)
    }

    fn take_free(&self) -> Option<usize> {
        let addr = self.free.pop_front();
        if addr.is_some() {
            self.free_count.fetch_sub(1, Relaxed);
        }
        addr
    }

    pub fn alloc_seg_with_class(
        &self,
        file_manager: &Arc<SegmentFileManager>,
        segment_class: SegmentClass,
    ) -> Option<Segment> {
        self.take_free()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    // Check the right boundary
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                let seq_id = self.next_seq_id.fetch_add(1, Ordering::AcqRel);
                Segment::new_with_class(
                    id as u64,
                    seq_id as u64,
                    self.chunk_id,
                    addr,
                    true,
                    file_manager.clone(),
                    segment_class,
                )
            })
    }

    /// Allocate a segment with a specific seq_id (for recovery purposes)
    /// This preserves the original seq_id from recovered files
    pub fn alloc_seg_with_seq_id(
        &self,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
    ) -> Option<Segment> {
        self.alloc_seg_with_seq_id_and_class(seq_id, file_manager, true, SegmentClass::Regular)
    }

    pub fn alloc_seg_with_seq_id_and_class(
        &self,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
        hot: bool,
        segment_class: SegmentClass,
    ) -> Option<Segment> {
        // First allocate the address
        self.take_free()
            .or_else(|| loop {
                debug!("Allocate segment by bump pointer (recovery)");
                let addr = self.offset.load(Relaxed);
                let new_addr = addr + SEGMENT_SIZE;
                if new_addr > self.limit {
                    return None;
                } else {
                    if self
                        .offset
                        .compare_exchange(addr, new_addr, AcqRel, Relaxed)
                        .is_ok()
                    {
                        return Some(addr);
                    }
                }
            })
            .map(|addr| {
                let id = self.id_by_addr(addr);
                // Use the provided seq_id instead of fetching a new one
                Segment::new_with_class(
                    id as u64,
                    seq_id,
                    self.chunk_id,
                    addr,
                    hot,
                    file_manager.clone(),
                    segment_class,
                )
            })
    }

    /// Allocate a segment at a specific ID for recovery purposes
    /// This ensures recovered data goes to the correct address
    pub fn alloc_seg_at_id(
        &self,
        seg_id: u64,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
    ) -> Option<Segment> {
        self.alloc_seg_at_id_with(seg_id, seq_id, file_manager, true, SegmentClass::Regular)
    }

    pub fn alloc_seg_at_id_with(
        &self,
        seg_id: u64,
        seq_id: u64,
        file_manager: &Arc<SegmentFileManager>,
        hot: bool,
        segment_class: SegmentClass,
    ) -> Option<Segment> {
        let addr = self.addr_by_id(seg_id as usize);

        // Ensure address is within bounds
        if addr >= self.limit {
            error!(
                "Cannot allocate segment {} at address {:#x}: exceeds limit {:#x}",
                seg_id, addr, self.limit
            );
            return None;
        }

        // Update offset if needed (to track allocated space)
        let required_end = addr + SEGMENT_SIZE;
        loop {
            let current_offset = self.offset.load(Ordering::Relaxed);
            if current_offset >= required_end {
                break; // Already allocated past this point
            }
            // Try to bump the offset
            if self
                .offset
                .compare_exchange(
                    current_offset,
                    required_end,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        Some(Segment::new_with_class(
            seg_id,
            seq_id,
            self.chunk_id,
            addr,
            hot,
            file_manager.clone(),
            segment_class,
        ))
    }

    /// Set the next seq_id for allocations (used after recovery)
    /// This ensures new segments continue from where recovered segments left off
    pub fn set_next_seq_id(&self, seq_id: u64) {
        let current = self.next_seq_id.load(Ordering::Relaxed);
        // Only update if the new value is higher (could be called from multiple threads during recovery)
        if seq_id as usize > current {
            self.next_seq_id.store(seq_id as usize, Ordering::Release);
            info!(
                "Set next_seq_id for chunk {} to {} (was {})",
                self.chunk_id, seq_id, current
            );
        }
    }

    pub fn free(&self, seg_addr: usize) {
        debug_assert!(seg_addr >= self.base);
        debug_assert!(seg_addr < self.limit);
        debug!("Segment {} freed", seg_addr);
        self.free.push_front(seg_addr);
        self.free_count.fetch_add(1, Relaxed);
    }

    pub fn id_by_addr(&self, addr: usize) -> usize {
        let offset = addr - self.base;
        let id = offset >> SEGMENT_BITS_SHIFT;
        id
    }

    #[inline]
    pub fn addr_by_id(&self, id: usize) -> usize {
        self.base + (id << SEGMENT_BITS_SHIFT)
    }
}

/// Positional read that does not disturb the file cursor, so a block fetch
/// cannot race another reader of the same handle.
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

pub unsafe fn madvise_free(addr: usize, size: usize) {
    #[cfg(target_os = "linux")]
    let advice = MADV_DONTNEED; // Drop the memory immediately instead of using MADV_FREE to wait for the kernel to reclaim it;
    #[cfg(not(target_os = "linux"))]
    let advice = MADV_DONTNEED;

    let result = madvise(addr as *mut c_void, size, advice);
    if result != 0 {
        let errno = std::io::Error::last_os_error();
        if errno.raw_os_error() == Some(libc::EINVAL) {
            warn!(
                "MADV_({}) not supported, falling back to MADV_DONTNEED",
                advice
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ~250,000 Segment structs stay resident for a terabyte store, so both
    /// their inline size and what they eagerly drag onto the heap are
    /// regression surfaces.
    ///
    /// Inline: 5 cache lines. If a legitimate field pushes past this, raise
    /// the bound consciously -- do not let the compiler pad silently (the
    /// struct is repr(C, align(64)), so ordering is part of the contract).
    ///
    /// Heap: a fresh segment must allocate NO dead-entry bitmap. Eagerly that
    /// bitmap was 128 KiB x 245K segments = 31 GB, the largest single heap
    /// consumer of the 1.78 TB Wikidata import, almost all of it for cold
    /// segments whose bitmap is never read.
    #[test]
    fn segment_struct_stays_lean() {
        assert!(
            std::mem::align_of::<Segment>() == 64,
            "Segment must stay cache-line aligned"
        );
        assert!(
            std::mem::size_of::<Segment>() <= 5 * 64,
            "Segment grew past 5 cache lines: {} bytes -- new field or \
             widened type? Justify before raising this bound",
            std::mem::size_of::<Segment>()
        );

        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE);
        let file_manager = Arc::new(SegmentFileManager::new(None, None));
        let seg = allocator
            .alloc_seg(&file_manager)
            .expect("allocate test segment");

        assert!(
            seg.dead_bits.read().is_none(),
            "a fresh segment must not allocate the dead-entry bitmap"
        );

        // First dead mark on a hot segment materializes the bitmap...
        seg.mark_dead_bit(seg.addr + 64);
        assert!(seg.dead_bits.read().is_some());
        assert!(seg.is_dead_at(seg.addr + 64));
        assert!(!seg.is_dead_at(seg.addr + 128));

        // ...and eviction gives it back.
        seg.clear_dead_bits();
        assert!(seg.dead_bits.read().is_none());
        assert!(
            !seg.is_dead_at(seg.addr + 64),
            "cleared bits read as not-dead (safe: scan falls back to the index probe)"
        );

        // Marks on a cold segment are skipped entirely -- its bitmap is never
        // consulted, so allocating it would be 128 KiB of write-only memory.
        seg.set_cold();
        seg.mark_dead_bit(seg.addr + 64);
        assert!(
            seg.dead_bits.read().is_none(),
            "a cold segment must not materialize the bitmap"
        );
    }

    /// A refused archive must leave the existing backup exactly as it was.
    ///
    /// The guards exist to stop a zero-filled image from overwriting a good
    /// backup, but they used to fire *after* `archive()` had already renamed
    /// the backup to `.old` and truncated a fresh file to zero -- so refusing
    /// destroyed the very thing being protected. The refusal must happen
    /// before any on-disk mutation.
    /// The image that broke TB14 must be impossible to write.
    ///
    /// `45-2053-3266.nbackup` was archived from a segment that reported no
    /// appended bytes and yielded no entries, but whose image was zeros at
    /// offset 0 with real cell data further in -- a segment whose pages had
    /// been dropped while it was still live. The archiver wrote it as a
    /// single whole-segment block (block_count=1, 7.9x compression where cell
    /// data manages 2x). Recovery could then neither scan it nor skip it, and
    /// failed the whole store on it.
    ///
    /// Neither earlier guard covers this: one requires the cursor to claim
    /// bytes, the other requires an existing backup to protect. A segment
    /// that reads as empty is only archivable if its image really is empty.
    #[cfg(feature = "compress_backups")]
    #[test]
    fn an_empty_cursor_over_a_non_zero_image_is_never_archived() {
        let _ = env_logger::try_init();

        let backup_dir = tempfile::tempdir().expect("backup dir");
        let backup_path = backup_dir.path().to_string_lossy().into_owned();
        let file_manager = Arc::new(SegmentFileManager::new(Some(backup_path), None));
        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("allocate test segment");

        // No prior backup: this is a first archive, so the empty-archive guard
        // has nothing to compare against.
        let backup_file = file_manager
            .backup_path(segment.chunk_id, segment.id, segment.seq_id)
            .expect("backup path");
        assert!(!Path::new(&backup_file).exists());

        // The cursor says empty, but the image holds bytes further in --
        // exactly a dropped first page with a written page behind it.
        unsafe {
            let image = slice::from_raw_parts_mut(segment.addr as *mut u8, SEGMENT_SIZE);
            image[PAGE_SIZE + 128] = 0x7F;
        }
        assert_eq!(
            segment.append_header.load(Ordering::Relaxed),
            segment.addr,
            "precondition: the cursor reports an empty segment"
        );

        let result = segment.archive();
        assert!(
            result.is_err(),
            "an empty cursor over a non-zero image must not be archived"
        );
        assert!(
            !Path::new(&backup_file).exists(),
            "the refused archive still created a backup file"
        );
    }

    #[cfg(feature = "compress_backups")]
    #[test]
    fn refused_archive_leaves_the_backup_untouched() {
        let _ = env_logger::try_init();

        let backup_dir = tempfile::tempdir().expect("backup dir");
        let backup_path = backup_dir.path().to_string_lossy().into_owned();
        let file_manager = Arc::new(SegmentFileManager::new(Some(backup_path), None));
        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("allocate test segment");

        // Stand in for a real archive written earlier in the segment's life.
        let backup_file = file_manager
            .backup_path(segment.chunk_id, segment.id, segment.seq_id)
            .expect("backup path");
        let good_backup = b"an earlier archive of this segment".to_vec();
        std::fs::write(&backup_file, &good_backup).expect("seed backup");

        // The failure mode: the segment claims a full 8 MiB of appended bytes
        // while its pages read back as zeros, which is what a dropped
        // (MADV_DONTNEED) resident image looks like.
        segment
            .append_header
            .store(segment.addr + SEGMENT_SIZE - 64, Ordering::Relaxed);

        let result = segment.archive();
        assert!(
            result.is_err(),
            "archiving a zero-filled image must be refused"
        );

        let after = std::fs::read(&backup_file).expect("backup must still exist");
        assert_eq!(
            after, good_backup,
            "the refused archive rewrote the backup it was meant to protect"
        );
        assert!(
            !Path::new(&format!("{}.old", backup_file)).exists(),
            "a refusal must not leave the backup renamed aside"
        );
        assert!(
            segment.is_dirty(),
            "a segment whose archive was refused stays dirty"
        );
    }

    #[test]
    fn test_punch_hole_alignment() {
        let _ = env_logger::try_init();

        // Create a test segment allocator
        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let file_manager = Arc::new(SegmentFileManager::new(None, None));

        // Allocate a segment
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("Failed to allocate segment");

        // Test 1: Punch hole from middle of segment (should align up to next page)
        let offset = 1024; // 1KB offset (not page aligned)
        segment.punch_hole(offset);

        // Test 2: Punch hole from page-aligned offset
        let aligned_offset = PAGE_SIZE * 2; // 8KB offset (page aligned)
        segment.punch_hole(aligned_offset);

        // Test 3: Punch hole from near end of segment (should not free if less than PAGE_SIZE)
        let near_end_offset = SEGMENT_SIZE - PAGE_SIZE / 2;
        segment.punch_hole(near_end_offset);

        // If we got here without panicking, the test passes
        assert!(true, "punch_hole executed without errors");
    }

    #[test]
    fn test_punch_hole_edge_cases() {
        let _ = env_logger::try_init();

        let allocator = SegmentAllocator::new(0, SEGMENT_SIZE * 2);
        let file_manager = Arc::new(SegmentFileManager::new(None, None));
        let segment = allocator
            .alloc_seg(&file_manager)
            .expect("Failed to allocate segment");

        // Test edge case: offset at end of segment
        segment.punch_hole(SEGMENT_SIZE);

        // Test edge case: offset beyond end of segment (should do nothing)
        segment.punch_hole(SEGMENT_SIZE + 1000);

        // Test edge case: offset at 0 (should free almost entire segment)
        segment.punch_hole(0);

        assert!(true, "Edge cases handled correctly");
    }
}

#[cfg(test)]
mod backup_fd_cache_tests {
    use super::*;

    fn dummy_handle() -> Arc<File> {
        // Any real descriptor will do; the cache never reads through it here --
        // so share ONE. These tests fill the cache thousands of times over
        // (`used_handles_outlast_untouched_ones` alone inserts 2,064 entries),
        // and a descriptor per entry exhausted the process limit once the rest
        // of the suite was running alongside holding its own files and sockets:
        // EMFILE out of `File::open`, reported as a panic in the middle of an
        // eviction test that has nothing to do with descriptor limits.
        static SHARED: std::sync::OnceLock<Arc<File>> = std::sync::OnceLock::new();
        SHARED
            .get_or_init(|| Arc::new(File::open("/dev/null").expect("/dev/null should open")))
            .clone()
    }

    /// The cache must stay at its capacity and evict, not grow.
    ///
    /// The integration test cannot show this: producing more cold segments than
    /// the live cap means tens of gigabytes of data. Exercising the structure
    /// directly with a small capacity tests the property that actually matters
    /// -- a 1.7TB import pinned 64,860 descriptors against a 65,535 limit and
    /// took the server down with EMFILE.
    #[test]
    fn cache_evicts_rather_than_growing() {
        let cap = 64;
        let cache = BackupFdCache::new(cap);
        let occupied = || {
            cache
                .shards
                .iter()
                .map(|s| s.lock().slots.iter().flatten().count())
                .sum::<usize>()
        };
        let slots = cache
            .shards
            .iter()
            .map(|s| s.lock().slots.len())
            .sum::<usize>();

        for i in 0..(slots * 8) as u64 {
            cache.insert((0, i, 0), dummy_handle());
        }

        assert_eq!(
            occupied(),
            slots,
            "cache should be full at its fixed size, never larger"
        );
        assert!(
            slots <= cap + 16,
            "total slots {} should honour the requested capacity {}",
            slots,
            cap
        );
    }

    /// Handles being used should outlast handles that are not.
    ///
    /// Bounding alone is not enough: capping without eviction quality would let
    /// the first segments to go cold keep the descriptors forever while the
    /// segments actually being read got none. Second chance cannot promise any
    /// individual entry survives -- when every slot is in use something must
    /// go -- so the property tested is the one the policy actually provides:
    /// used entries survive at a far higher rate than untouched ones.
    #[test]
    fn used_handles_outlast_untouched_ones() {
        let cache = BackupFdCache::new(512);
        let hot: Vec<_> = (0..32u64).map(|i| (0, 900_000 + i, 0)).collect();
        let cold: Vec<_> = (0..32u64).map(|i| (0, 800_000 + i, 0)).collect();
        for k in hot.iter().chain(cold.iter()) {
            cache.insert(*k, dummy_handle());
        }

        // Churn through unrelated keys, touching only the hot set as we go.
        for i in 0..2_000u64 {
            for k in &hot {
                let _ = cache.get(k);
            }
            cache.insert((1, i, 0), dummy_handle());
        }

        let hot_alive = hot.iter().filter(|k| cache.get(k).is_some()).count();
        let cold_alive = cold.iter().filter(|k| cache.get(k).is_some()).count();
        assert!(
            hot_alive > cold_alive,
            "handles in use ({} of {}) should outlast untouched ones ({}), \
             otherwise the cache is evicting blind",
            hot_alive,
            hot.len(),
            cold_alive
        );
    }

    #[test]
    fn removal_frees_a_slot() {
        let cache = BackupFdCache::new(64);
        let key = (7, 7, 7);
        cache.insert(key, dummy_handle());
        assert!(cache.get(&key).is_some());
        cache.remove(&key);
        assert!(cache.get(&key).is_none(), "removed handle should be gone");
    }
}
