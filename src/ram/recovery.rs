use super::cell::cell_header_from_entry_content_addr;
use super::chunk::{CellGuard, Chunk};
use super::entry::{Entry, EntryType, ENTRY_HEAD_SIZE};
use super::file_manager::{SegmentFileInfo, SegmentFileManager};
use super::schema::SchemaVid;
use super::segs::{
    estimated_cells_per_segment, Segment, SegmentClass, DEFAULT_ESTIMATED_CELL_BYTES, SEGMENT_SIZE,
};
use super::tombstone::Tombstone;
use crate::ram::types::Id;
use lightning::aarc::Arc as AArc;
use lightning::map::WordMap;
use parking_lot::Mutex;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

/// Discover all segment files in storage directories
pub fn discover_segment_files(
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
) -> io::Result<Vec<SegmentFileInfo>> {
    let file_manager = SegmentFileManager::new(backup_storage.clone(), wal_storage.clone());
    file_manager.discover_files()
}

/// Load file content into memory buffer
pub fn load_file_to_memory(path: &Path) -> io::Result<Vec<u8>> {
    let file_manager = SegmentFileManager::new(None, None);
    file_manager.read_file(path)
}

/// Load a segment file with the live extent its image declares, if any.
pub fn load_file_with_used_len(path: &Path) -> io::Result<(Vec<u8>, Option<usize>)> {
    let file_manager = SegmentFileManager::new(None, None);
    file_manager.read_file_with_used_len(path)
}

/// Find the actual append_header by scanning segment data
pub fn find_append_header(seg_addr: usize, file_size: usize) -> usize {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let mut cursor = seg_addr;
    let bound = seg_addr + file_size;
    let mut entries_found = 0;

    while cursor < bound {
        // Ensure we have at least ENTRY_HEAD_SIZE bytes remaining to read the header
        // This prevents SIGBUS when accessing memory beyond the mapped region
        if cursor + ENTRY_HEAD_SIZE > bound {
            // Not enough bytes to read a complete entry header
            break;
        }

        // Pre-validate entry header before calling decode_from to avoid panics on corrupted data
        let entry_type_bits = unsafe {
            let mut reader = Cursor::new(std::slice::from_raw_parts(cursor as *const u8, 8));
            reader.read_u32::<LittleEndian>().unwrap()
        };

        // Validate entry type bits before attempting decode
        if matches!(
            crate::ram::entry::unpack_type_word(entry_type_bits),
            crate::ram::entry::TypeWord::Invalid
        ) {
            // Invalid entry type - likely corrupted data
            warn!(
                "Corrupted entry header detected at offset {} (address 0x{:016x}): \
                invalid entry_type_bits={} (0x{:08x}). \
                Treating as end of valid data. Found {} valid entries before corruption.",
                cursor - seg_addr,
                cursor,
                entry_type_bits,
                entry_type_bits,
                entries_found
            );
            break;
        }

        // Try to decode entry header (should succeed now that we've validated)
        let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);

        if entry_header.entry_type == EntryType::UNDECIDED || entry_header.content_length == 0 {
            // Found uninitialized space
            break;
        }

        entries_found += 1;
        let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;

        // Check if the full entry fits within bounds before advancing
        if cursor + entry_size > bound {
            // Entry would exceed bounds, stop here
            break;
        }

        cursor += entry_size;
    }

    cursor
}

/// Tombstone that needs to be checked after all cells are recovered
#[derive(Debug, Clone)]
struct StashedTombstone {
    hash: u64,
    version: u64,
    chunk_id: usize,
}

#[derive(Debug)]
struct RecoveryScanResult {
    stashed_tombstones: Vec<StashedTombstone>,
    segment_class: SegmentClass,
    append_offset: usize,
    dead_space: u32,
    tombstones: u32,
    /// Set when the segment's fixed tail holds a chain link: the short
    /// transaction id and the previous part's seq id.
    chain_link: Option<(u64, u64)>,
}

/// Recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub num_chunks: usize,
    pub chunk_size: usize,
    /// Maximum number of threads to use for parallel recovery operations.
    /// If None, defaults to 64 threads.
    pub max_threads: Option<usize>,
}

const DEFAULT_RECOVERY_THREADS: usize = 64;

/// Autopsy tracing for ranged-index page cells during recovery, gated by
/// NEB_RECOVERY_TRACE_PAGES=1. Names every page image the scan judges --
/// winner or loser -- so a corrupted tree can be traced back to the exact
/// file and decision that shaped it. Off, it costs one atomic load.
fn trace_pages_enabled() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    static STATE: AtomicU8 = AtomicU8::new(0);
    match STATE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let on = std::env::var("NEB_RECOVERY_TRACE_PAGES").map_or(false, |v| v == "1");
            STATE.store(if on { 1 } else { 2 }, Ordering::Relaxed);
            on
        }
    }
}

lazy_static::lazy_static! {
    /// Must match `index::ranged::tree::btree::external::PAGE_SCHEMA_ID`,
    /// computed here because that module is private to the btree.
    static ref TRACED_PAGE_SCHEMA_ID: SchemaVid =
        SchemaVid(dovahkiin::types::key_hash("NEB_BTREE_PAGE") as u32);
    /// Must match `index::ranged::tree::tree::RANGED_TREE_SCHEMA_NAME`: the
    /// per-tree metadata cell, the single cell whose loss unloads a tree.
    static ref TRACED_TREE_META_SCHEMA_ID: SchemaVid =
        SchemaVid(dovahkiin::types::key_hash("NEB_RANGED_TREE") as u32);
}

fn traced_schema(schema: SchemaVid) -> Option<&'static str> {
    if schema == *TRACED_PAGE_SCHEMA_ID {
        Some("page")
    } else if schema == *TRACED_TREE_META_SCHEMA_ID {
        Some("tree-meta")
    } else {
        None
    }
}

const MIN_RECOVERY_WORD_MAP_CAPACITY: usize = 4_096;
/// Ceiling on a single chunk's version map. A chunk holds at most
/// `chunk_size / SEGMENT_SIZE` segments, so this only has to stay above the
/// largest legitimate chunk; the previous `1 << 22` sat *below* it at terabyte
/// scale and silently capped the map at a fifth of what it had to hold.
const MAX_RECOVERY_WORD_MAP_CAPACITY: usize = 1 << 26;
/// Bytes of *on-disk* backup per cell. Backups are compressed, so this
/// under-counts cells and the segment-derived estimate below normally wins --
/// which is the intent: segment count is exact, file size is not.
const RECOVERY_ENTRY_BYTES_ESTIMATE: u64 = DEFAULT_ESTIMATED_CELL_BYTES as u64;

fn recovery_parallelism(config: &RecoveryConfig) -> usize {
    config
        .max_threads
        .unwrap_or(DEFAULT_RECOVERY_THREADS)
        .max(1)
}

/// Cells a chunk's recovery is expected to restore.
///
/// `discover_files` deduplicates to one file per segment, so `files.len()` is
/// the chunk's segment count exactly; only the cells-per-segment density is
/// assumed. Shared with the permanent cell index so both are sized alike.
fn estimate_cells(segment_count: usize, total_bytes: u64) -> usize {
    let bytes_estimate = (total_bytes / RECOVERY_ENTRY_BYTES_ESTIMATE)
        .try_into()
        .unwrap_or(usize::MAX);
    let segment_estimate = segment_count.saturating_mul(estimated_cells_per_segment());

    bytes_estimate.max(segment_estimate)
}

fn estimate_recovered_cells(files: &[SegmentFileInfo]) -> usize {
    estimate_cells(files.len(), files.iter().map(|file| file.size).sum())
}

/// Tracks the newest version seen per cell hash while a chunk is scanned.
///
/// Deliberately *not* a concurrent map. Recovery runs one task per chunk and
/// each task walks its segments serially, so this is owned by a single thread
/// and lock-free machinery would be paid for and never used. Measured against
/// `WordMap` on one chunk's 21.7M cells: 3.1x faster, and -- because a resize
/// here frees one large table straight back to the OS instead of leaving
/// per-partition tables behind in an allocator arena -- it does not blow up
/// when the estimate is short (0.53 GiB vs 2.88 GiB at 5x undersized).
/// What the scan currently believes about one hash: the winning version, and
/// the winner's content length when the winner is a live cell (zero when it is
/// a tombstone). The size rides in the map because it CANNOT be recovered
/// later by decoding the superseded entry -- a superseded entry can live in a
/// cold segment whose mapping holds nothing, where a decode faults. Everything
/// the scan reads comes from file data, so recording it here is free.
#[derive(Clone, Copy)]
struct VersionSeen {
    version: u64,
    live_size: u32,
}

type VersionMap = HashMap<u64, VersionSeen, ahash::RandomState>;

fn new_version_map(files: &[SegmentFileInfo]) -> VersionMap {
    let capacity = estimate_recovered_cells(files)
        .max(MIN_RECOVERY_WORD_MAP_CAPACITY)
        .min(MAX_RECOVERY_WORD_MAP_CAPACITY);
    HashMap::with_capacity_and_hasher(capacity, ahash::RandomState::new())
}

/// Cells each chunk is expected to recover, indexed by chunk id.
///
/// Called before the chunks exist so their cell indexes can be sized for the
/// data they are about to hold: `WordMap` capacity cannot be changed afterwards.
pub fn estimate_cells_per_chunk(files: &[SegmentFileInfo], num_chunks: usize) -> Vec<usize> {
    let mut segments = vec![0usize; num_chunks];
    let mut bytes = vec![0u64; num_chunks];
    for file in files {
        if file.chunk_id < num_chunks {
            segments[file.chunk_id] += 1;
            bytes[file.chunk_id] += file.size;
        }
    }
    segments
        .iter()
        .zip(bytes.iter())
        .map(|(count, total)| estimate_cells(*count, *total))
        .collect()
}

fn group_files_by_chunk(
    files: Vec<SegmentFileInfo>,
    num_chunks: usize,
) -> Vec<Vec<SegmentFileInfo>> {
    let mut grouped = vec![Vec::new(); num_chunks];
    for file in files {
        grouped[file.chunk_id].push(file);
    }
    for chunk_files in &mut grouped {
        // CHRONOLOGICAL, not by address. Two images of one cell at the same
        // version are reconciled by "last one scanned wins" (`>=` below), so
        // the scan order decides which survives -- and ordering by seg_id
        // first made that decision by segment address, which is arbitrary.
        // seq_id is the incarnation counter and increases with time, so
        // ordering by it means the most recently written image wins a tie,
        // which is the only answer that is ever right.
        chunk_files.sort_unstable_by_key(|file| (file.seq_id, file.seg_id));
    }
    grouped
}

impl RecoveryConfig {
    /// Check if all discovered segments can fit in the current configuration
    pub fn can_fit(&self, files: &[SegmentFileInfo]) -> bool {
        // Group by chunk_id and find max seg_id per chunk
        let mut max_seg_per_chunk: HashMap<usize, u64> = HashMap::new();

        for file in files {
            let entry = max_seg_per_chunk.entry(file.chunk_id).or_insert(0);
            *entry = (*entry).max(file.seg_id);
        }

        // Check if all chunks fit
        for (chunk_id, max_seg_id) in max_seg_per_chunk {
            if chunk_id >= self.num_chunks {
                warn!(
                    "Chunk {} in recovery files exceeds configured chunks {}",
                    chunk_id, self.num_chunks
                );
                return false;
            }

            // Calculate max segments this chunk can hold
            let max_segs = self.chunk_size / SEGMENT_SIZE;
            if (max_seg_id as usize) >= max_segs {
                warn!(
                    "Chunk {} segment {} exceeds capacity {} segments",
                    chunk_id, max_seg_id, max_segs
                );
                return false;
            }
        }

        true
    }
}

/// Phase 1: Discover segment files
fn phase1_discover_files(
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
) -> io::Result<Vec<SegmentFileInfo>> {
    info!("Phase 1: Discovering segment files...");
    let files = discover_segment_files(backup_storage, wal_storage)?;

    if files.is_empty() {
        info!("No segment files found, starting fresh");
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No segment files found",
        ));
    }

    info!("Discovered {} segment files", files.len());

    Ok(files)
}

/// Phase 1.5: Set initial next_seq_id values for each chunk
fn phase1_5_set_initial_seq_ids(chunks: &[Chunk], files: &[SegmentFileInfo]) {
    info!("Phase 1.5: Setting initial next_seq_id values...");

    for chunk in chunks {
        // Find max seq_id for this chunk from discovered files
        let max_seq_id = files
            .iter()
            .filter(|f| f.chunk_id == chunk.id)
            .map(|f| f.seq_id)
            .max()
            .unwrap_or(0);

        // Set next_seq_id so newly allocated segments get higher seq_ids
        chunk
            .allocator
            .next_seq_id
            .store((max_seq_id + 1) as usize, Ordering::Release);

        info!(
            "Chunk {} initial next_seq_id set to {}",
            chunk.id,
            max_seq_id + 1
        );
    }
}

/// Check if we should recover this segment as cold based on tiered memory settings
///
/// Cold recovery (mmapping the backup file directly) is only possible when:
/// 1. Tiered memory is enabled
/// 2. The hot memory limit would be exceeded
/// 3. The backup file is NOT compressed (compressed files cannot be mmap'd directly)
fn should_recover_as_cold(
    chunk: &Chunk,
    file_info: &SegmentFileInfo,
    current_hot_segments: usize,
) -> bool {
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        let physical_limit = tiered_manager.shared_pool().physical_memory_limit;
        let existing_hot = chunk
            .segs
            .get(&(file_info.seg_id as usize))
            .map(|segment| segment.is_hot())
            .unwrap_or(false);
        let additional_hot_segments = usize::from(!existing_hot);
        let would_exceed_limit = current_hot_segments
            .checked_add(additional_hot_segments)
            .and_then(|segments| segments.checked_mul(SEGMENT_SIZE))
            .map(|bytes| bytes > physical_limit)
            .unwrap_or(true);

        // Recover as cold if: backup file and would exceed hot memory limit
        // Compressed files are now supported via Vec-based recovery
        let recover_as_cold = file_info.is_backup && would_exceed_limit;

        debug!(
            "Recovery decision: global_hot_segments={}, existing_hot={}, limit={} MB, would_exceed={}, recover_as_cold={}",
            current_hot_segments,
            existing_hot,
            physical_limit / (1024 * 1024),
            would_exceed_limit,
            recover_as_cold
        );

        recover_as_cold
    } else {
        false // Tiered memory not enabled
    }
}

fn newer_resident_segment(chunk: &Chunk, seg_id: u64, seq_id: u64) -> Option<AArc<Segment>> {
    let existing_segment = chunk.segs.get(&(seg_id as usize))?;
    if existing_segment.seq_id > seq_id {
        Some(existing_segment)
    } else {
        None
    }
}

/// What recovery knows about the transactions whose brackets it has seen.
///
/// A transaction spanning several chunks writes a bracket in each, and N
/// fsyncs cannot be made atomic -- so a crash can leave one chunk's COMMIT
/// durable and another's not. The commit path therefore makes every ENTRY
/// durable before it writes any COMMIT, which turns one surviving COMMIT into
/// a decision for the whole transaction: its manifest names every member, and
/// every member's entries are known to be on disk.
///
/// That decision cannot be made while scanning, because the COMMIT may live
/// in a chunk this thread has not reached (chunks are scanned in parallel). So
/// bracketed entries are BUFFERED here and applied once every segment has
/// been read and every COMMIT is known.
pub struct BracketLedger {
    parts:
        Mutex<HashMap<crate::server::transactions::TxnId, std::collections::HashSet<(usize, u64)>>>,
    closed_parts:
        Mutex<HashMap<crate::server::transactions::TxnId, std::collections::HashSet<(usize, u64)>>>,
    /// Every (chunk, seq) this recovery discovered. A manifest member counts
    /// as present iff it is here.
    present: std::collections::HashSet<(u16, u64)>,
    pending: Mutex<HashMap<crate::server::transactions::TxnId, Vec<PendingBracketCell>>>,
    commits:
        Mutex<HashMap<crate::server::transactions::TxnId, Vec<crate::ram::bracket::ManifestEntry>>>,
    /// The participant set each BEGIN names, per transaction.
    participants: Mutex<HashMap<crate::server::transactions::TxnId, Vec<u64>>>,
    /// Every plain (unbracketed) tombstone the scan met, newest version per
    /// `(chunk, hash)`. Segment order is NOT write order across segments (a
    /// chunk writes into several heads at once), so the scan cannot decide
    /// alone whether a tombstone met before or after its victim applies;
    /// this lets the settle step re-decide with the tombstone's identity
    /// (victim segment and version), and lets bracketed writes, which are
    /// applied later, respect it.
    plain_deletes: Mutex<Vec<(IdKey, DeleteMark)>>,
    /// Equal-version ties, see `DeleteBook::ties`.
    ties: Mutex<HashMap<IdKey, u32>>,
    /// Plain cells the scan discarded for carrying a LOWER version than what
    /// was already installed although they sit LATER in write order. That is
    /// either a stale copy, or a re-created id (versions restart at 1) whose
    /// predecessor was deleted by a bracket the scan could not yet apply.
    /// Settled after the brackets, when the deletes are known.
    late_losers: Mutex<Vec<PendingBracketCell>>,
    resolved_rewrites: Mutex<Vec<InDoubtBracket>>,
}

#[derive(Clone)]
pub struct PendingBracketCell {
    chunk_id: usize,
    hash: u64,
    /// A transaction can DELETE as well as write, and a committed delete has
    /// to land as surely as a committed write. Buffering only the writes made
    /// a transaction's deletes vanish -- the cell stayed alive, which is the
    /// same class of wrongness as a lost write and harder to notice.
    tombstone: bool,
    /// The virtual address the entry occupies once its segment is installed,
    /// which stays valid after the scan -- the cells live in the segment's
    /// mapped memory, not in the scan's buffer.
    addr: usize,
    version: u64,
    content_length: u32,
    /// For a tombstone: the `seq_id` of the segment holding the cell it
    /// deleted. Zero for cells.
    target_seq: u64,
}

/// Transactions discarded because they were never committed, and cells
/// applied from committed brackets. Loud on purpose: both are normal, and
/// both are things an operator will want to see after a crash.
pub static BRACKETS_DISCARDED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BRACKETS_APPLIED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl BracketLedger {
    pub fn new(files: &[SegmentFileInfo]) -> Self {
        BracketLedger {
            parts: Mutex::new(HashMap::new()),
            closed_parts: Mutex::new(HashMap::new()),
            present: files
                .iter()
                .map(|file| (file.chunk_id as u16, file.seq_id))
                .collect(),
            pending: Mutex::new(HashMap::new()),
            commits: Mutex::new(HashMap::new()),
            participants: Mutex::new(HashMap::new()),
            plain_deletes: Mutex::new(Vec::new()),
            ties: Mutex::new(HashMap::new()),
            late_losers: Mutex::new(Vec::new()),
            resolved_rewrites: Mutex::new(Vec::new()),
        }
    }

    fn note_late_loser(&self, cell: PendingBracketCell) {
        self.late_losers.lock().push(cell);
    }

    fn note_plain_delete(&self, chunk_id: usize, hash: u64, mark: DeleteMark) {
        // A flat list while the scan runs: one entry per tombstone in the
        // store, so it must not pay for a hash table's growth spikes. It is
        // indexed once the scan's per-chunk version maps are gone.
        self.plain_deletes.lock().push(((chunk_id, hash), mark));
    }

    /// A second cell entry with the very same version as the one installed.
    fn note_tie(&self, chunk_id: usize, hash: u64) {
        *self.ties.lock().entry((chunk_id, hash)).or_insert(0) += 1;
    }

    fn note_pending(&self, txn: &crate::server::transactions::TxnId, cell: PendingBracketCell) {
        self.pending
            .lock()
            .entry(txn.clone())
            .or_insert_with(Vec::new)
            .push(cell);
    }

    fn note_commit(
        &self,
        txn: &crate::server::transactions::TxnId,
        manifest: Vec<crate::ram::bracket::ManifestEntry>,
    ) {
        self.commits.lock().insert(txn.clone(), manifest);
    }

    /// Every part of a transaction names the same set; the first one seen
    /// is kept.
    fn note_participants(&self, txn: &crate::server::transactions::TxnId, participants: Vec<u64>) {
        self.participants
            .lock()
            .entry(txn.clone())
            .or_insert(participants);
    }

    /// Apply every committed bracket and report the rest.
    ///
    /// Called once, after every segment has been scanned. Applying here rather
    /// than during the scan is what makes a cross-chunk transaction atomic:
    /// the chunk whose own COMMIT was lost still applies its part, because the
    /// decision came from the transaction, not from that chunk's bytes.
    ///
    /// It no longer DISCARDS the rest. A bracket with no COMMIT here means
    /// this store was in flight when it stopped -- which is not the same as
    /// "this transaction aborted", because another participant may have been
    /// told to commit. Both halves are handed back so the store can ask its
    /// peers before deciding: see [`RecoveredBrackets`].
    pub(crate) fn settle(
        &self,
        chunks: &[Chunk],
    ) -> Result<RecoveredBrackets, RecoveryCellReadError> {
        let commits = std::mem::take(&mut *self.commits.lock());
        let mut participants = std::mem::take(&mut *self.participants.lock());
        let closed = self.closed_parts.lock();
        let mut incomplete = std::collections::HashSet::new();
        for (txn, parts) in self.parts.lock().iter() {
            // A missing local decision cannot be replaced by lease absence.
            // Keep every part, including the surviving COMMIT, until settlement.
            if !parts
                .iter()
                .all(|part| closed.get(txn).is_some_and(|set| set.contains(part)))
            {
                incomplete.insert(txn.clone());
                for &(chunk_id, segment_id) in parts {
                    if let Some(segment) = chunks[chunk_id].segs.get(&(segment_id as usize)) {
                        segment.protection.retain_transaction(txn);
                    }
                }
            }
        }
        drop(closed);
        let parts = self.parts.lock();
        let mut pending = std::mem::take(&mut *self.pending.lock());
        // Floors count physical tombstones, including undecided brackets.
        // Their versions constrain a future create without deciding the bracket.
        for cells in pending.values() {
            for tombstone in cells.iter().filter(|cell| cell.tombstone) {
                chunks[tombstone.chunk_id].note_deleted_version(tombstone.hash, tombstone.version);
            }
        }
        let mut committed_txns = Vec::with_capacity(commits.len());
        let mut committed_cells: Vec<PendingBracketCell> = Vec::new();
        for (txn, manifest) in commits {
            let txn_participants = participants.remove(&txn).unwrap_or_default();
            committed_txns.push(crate::ram::evidence::RecoveredCommit {
                txn: txn.clone(),
                segments: parts
                    .get(&txn)
                    .map(|set| set.iter().copied().collect())
                    .unwrap_or_default(),
                participants: txn_participants.clone(),
            });
            let missing: Vec<_> = manifest
                .iter()
                .filter(|member| !self.present.contains(&(member.chunk_id, member.seq_id)))
                .collect();
            let cells = pending.remove(&txn).unwrap_or_default();
            // A missing member means COMPACTED, not lost -- and the
            // transaction still applies.
            //
            // Two facts make that sound. Entries are fsynced BEFORE any
            // COMMIT is written, so a durable COMMIT already proves every
            // entry of that transaction was durable; the manifest adds
            // nothing to that proof. And the cleaner can only touch a segment
            // whose bracket is DECIDED, because an undecided one is still
            // leased and leased heads are skipped -- so a member that is gone
            // was compacted after the decision, with its live cells carried
            // into a new segment (under a new seq id) and its bracket markers
            // dropped.
            //
            // Discarding on a missing member is what TORE transactions: the
            // compacted halves came back as ordinary entries while the intact
            // halves were thrown away, so a four-cell transaction recovered
            // 2/4. The rule the design started with predates the two-round
            // fsync that made the manifest redundant.
            if !missing.is_empty() {
                debug!(
                    "transaction {:?} has a COMMIT and {} of its {} manifest member(s) are no \
                     longer present; they were compacted after the decision, so its {} buffered \
                     cell(s) still apply",
                    txn,
                    missing.len(),
                    manifest.len(),
                    cells.len()
                );
            }
            if incomplete.contains(&txn) {
                self.resolved_rewrites.lock().push(InDoubtBracket {
                    txn: txn.clone(),
                    chunks: cells.iter().map(|cell| cell.chunk_id).collect(),
                    cells: cells.clone(),
                    participants: txn_participants.clone(),
                });
            }
            BRACKETS_APPLIED.fetch_add(cells.len() as u64, std::sync::atomic::Ordering::Relaxed);
            committed_cells.extend(cells);
        }
        // Applied together, in the order the entries were WRITTEN. `commits`
        // and `pending` are hash maps, so applying transaction by transaction
        // replayed several transactions that touched one id in arbitrary
        // order: a delete could run before the write it follows (the id came
        // back) and a re-create before the delete it follows (the id was
        // lost).
        let mut plain_deletes = DeleteBook::from_scan(
            std::mem::take(&mut *self.plain_deletes.lock()),
            std::mem::take(&mut *self.ties.lock()),
        );
        for &((chunk_id, hash), mark) in &plain_deletes.plain {
            chunks[chunk_id].note_deleted_version(hash, mark.version);
        }
        revalidate_plain_deletes(chunks, &plain_deletes)?;
        apply_bracketed_cells(chunks, committed_cells, Some(&mut plain_deletes))?;
        // A late loser comes back only if it is provably a re-create (see
        // `DeleteMark::kills`); otherwise the version rule keeps it out.
        let late_losers = std::mem::take(&mut *self.late_losers.lock());
        apply_bracketed_cells(chunks, late_losers, Some(&mut plain_deletes))?;
        // What is left has no COMMIT anywhere, and that now means exactly one
        // thing: the transaction was in flight when the store stopped.
        //
        // It used to mean two things, which is why the design carried a
        // decided-watermark to tell them apart -- a bracket could also lose
        // its COMMIT to the cleaner. That ambiguity is gone: every bracket
        // carries its own COMMIT, and compaction drops a segment's BEGIN and
        // COMMIT together, so a compacted transaction's cells come back as
        // ordinary entries rather than as a bracket missing its closer. With
        // nothing left for the watermark to decide, deciding on it would be
        // resting correctness on a quantity nothing exercises.
        let in_doubt: Vec<InDoubtBracket> = pending
            .into_iter()
            .map(|(txn, cells)| {
                info!(
                    "transaction {:?} left {} bracketed cell(s) with no COMMIT; it was in flight \
                     when the store stopped and is IN DOUBT until its peers are asked",
                    txn,
                    cells.len()
                );
                let mut chunks: Vec<usize> = cells.iter().map(|cell| cell.chunk_id).collect();
                chunks.sort_unstable();
                chunks.dedup();
                let participants = participants.remove(&txn).unwrap_or_default();
                InDoubtBracket {
                    txn,
                    cells,
                    chunks,
                    participants,
                }
            })
            .collect();
        Ok(RecoveredBrackets {
            committed: committed_txns,
            in_doubt,
        })
    }
}

/// A transaction this store cannot decide on its own.
pub struct InDoubtBracket {
    pub txn: crate::server::transactions::TxnId,
    /// Which chunks hold a part of this transaction. A resolved COMMIT is
    /// written into each of them rather than into one: parts are decided
    /// together but COMPACTED independently, so a COMMIT that lives in a
    /// chunk this transaction never touched is one the cleaner can carry away
    /// while the BEGINs it closes are still on disk.
    pub chunks: Vec<usize>,
    /// Held rather than dropped: if a peer reports the transaction committed,
    /// these are the cells that must be installed. The entries themselves are
    /// still in the segments -- only the index placement is deferred.
    pub cells: Vec<PendingBracketCell>,
    /// The servers that hold a bracket for it, from its BEGIN records. The
    /// lowest id is the commit point: see
    /// `crate::server::transactions::commit_point`. Empty for a BEGIN written
    /// outside a coordinated transaction, which this site decides alone.
    pub participants: Vec<u64>,
}

/// What recovery learned about transactions, for cooperative termination.
///
/// `committed` matters as much as `in_doubt`: a site that RESTARTED has no
/// live decision record, so without this it would answer "I never heard of
/// it" about a transaction it durably committed -- and a peer in doubt would
/// read that as abort and tear the transaction the restart was supposed to
/// preserve.
pub struct RecoveredBrackets {
    pub committed: Vec<crate::ram::evidence::RecoveredCommit>,
    pub in_doubt: Vec<InDoubtBracket>,
}

impl RecoveredBrackets {
    pub fn empty() -> Self {
        RecoveredBrackets {
            committed: Vec::new(),
            in_doubt: Vec::new(),
        }
    }
}

/// Which segment an entry lives in, as `(seq_id, segment id)`.
fn segment_of(chunk: &Chunk, addr: usize) -> Option<(u64, u64)> {
    chunk
        .locate_segment(addr)
        .map(|segment| (segment.seq_id, segment.id))
}

/// What a tombstone says about the entry for its id.
#[derive(Clone, Copy)]
struct DeleteMark {
    /// The tombstone's own address (the latest of `count` identical ones).
    addr: usize,
    /// Version of the cell it deleted.
    version: u64,
    /// `seq_id` of the segment that cell lived in.
    target_seq: u64,
    /// How many tombstones name exactly this victim `(segment, version)`.
    /// Equal-version copies can name the same victim more than once.
    count: u32,
}

type IdKey = (usize, u64);

/// Everything recovery knows about deletes, per `(chunk, hash)`.
#[derive(Default)]
struct DeleteBook {
    /// Plain tombstones from the scan, sorted by id and then read-only: one
    /// per tombstone in the store, so no per-entry allocation.
    plain: Vec<(IdKey, DeleteMark)>,
    /// Tombstones met later, inside committed brackets.
    extra: HashMap<IdKey, DeleteMarks>,
    /// Extra cell entries that carried the same version as the one installed:
    /// an id with `n` entries at one version has `n - 1` ties.
    ties: HashMap<IdKey, u32>,
}

/// The tombstones of one id. Nearly every id has exactly one, and recovery
/// holds one of these per tombstone in the store, so the first lives inline
/// and `more` stays unallocated until an id is deleted more than once.
#[derive(Clone)]
struct DeleteMarks {
    first: DeleteMark,
    more: Vec<DeleteMark>,
}

impl DeleteMarks {
    fn iter(&self) -> impl Iterator<Item = &DeleteMark> {
        std::iter::once(&self.first).chain(self.more.iter())
    }

    fn add(&mut self, mark: DeleteMark) {
        let same = |m: &DeleteMark| m.version == mark.version && m.target_seq == mark.target_seq;
        let found = if same(&self.first) {
            Some(&mut self.first)
        } else {
            self.more.iter_mut().find(|m| same(m))
        };
        match found {
            Some(existing) => {
                existing.count += mark.count;
                existing.addr = existing.addr.max(mark.addr);
            }
            None => self.more.push(mark),
        }
    }
}

impl DeleteBook {
    fn from_scan(mut plain: Vec<(IdKey, DeleteMark)>, ties: HashMap<IdKey, u32>) -> Self {
        plain.sort_unstable_by_key(|(key, mark)| (*key, mark.version, mark.target_seq));
        DeleteBook {
            plain,
            extra: HashMap::new(),
            ties,
        }
    }

    fn add_mark(&mut self, key: IdKey, mark: DeleteMark) {
        match self.extra.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => entry.get_mut().add(mark),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(DeleteMarks {
                    first: mark,
                    more: Vec::new(),
                });
            }
        }
    }

    /// Every tombstone known for `key`, identical victims merged.
    fn marks_for(&self, key: IdKey) -> Vec<DeleteMark> {
        let start = self.plain.partition_point(|(k, _)| *k < key);
        let run = self.plain[start..].iter().take_while(|(k, _)| *k == key);
        let mut merged: Option<DeleteMarks> = None;
        for (_, mark) in run {
            match merged.as_mut() {
                Some(marks) => marks.add(*mark),
                None => {
                    merged = Some(DeleteMarks {
                        first: *mark,
                        more: Vec::new(),
                    })
                }
            }
        }
        if let Some(extra) = self.extra.get(&key) {
            for mark in extra.iter() {
                match merged.as_mut() {
                    Some(marks) => marks.add(*mark),
                    None => {
                        merged = Some(DeleteMarks {
                            first: *mark,
                            more: Vec::new(),
                        })
                    }
                }
            }
        }
        merged
            .map(|m| m.iter().copied().collect())
            .unwrap_or_default()
    }
}

impl DeleteMark {
    /// A re-create continues above the deleted version. Copies at or below
    /// that version belong to the deleted life, wherever the cleaner put them.
    fn kills(&self, _chunk: &Chunk, _addr: usize, version: u64, _rank: u32) -> bool {
        version <= self.version
    }
}

fn any_kills<'a>(
    chunk: &Chunk,
    mut marks: impl Iterator<Item = &'a DeleteMark>,
    addr: usize,
    version: u64,
    rank: u32,
) -> bool {
    marks.any(|mark| mark.kills(chunk, addr, version, rank))
}

#[derive(Debug)]
pub(crate) struct RecoveryCellReadError(super::cell::ReadError);

impl std::fmt::Display for RecoveryCellReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot read a recovered cell: {:?}", self.0)
    }
}

impl std::error::Error for RecoveryCellReadError {}

// Recovery can install cold segments without resident cell bytes. Use the
// normal read guard to load and pin the block before reading its header.
fn recovered_cell_guard(
    chunk: &Chunk,
    hash: u64,
) -> Result<Option<CellGuard<'_>>, RecoveryCellReadError> {
    match chunk.lock_cell_for_read(hash) {
        Ok(guard) => Ok(Some(guard)),
        Err(
            super::cell::ReadError::CellDoesNotExisted | super::cell::ReadError::CellIdIsUnitId,
        ) => Ok(None),
        Err(error) => Err(RecoveryCellReadError(error)),
    }
}

/// Revalidate deletes after the scan installs every segment.
fn revalidate_plain_deletes(
    chunks: &[Chunk],
    book: &DeleteBook,
) -> Result<(), RecoveryCellReadError> {
    let mut at = 0;
    while at < book.plain.len() {
        let key = book.plain[at].0;
        while at < book.plain.len() && book.plain[at].0 == key {
            at += 1;
        }
        let (chunk_id, hash) = key;
        let Some(chunk) = chunks.get(chunk_id) else {
            continue;
        };
        if let Some(mut guard) = recovered_cell_guard(chunk, hash)? {
            let existing = *guard;
            if existing != 0 {
                let version = guard.cell_version().map_err(RecoveryCellReadError)?;
                // The index holds the LAST of the entries that tied.
                let rank = book.ties.get(&key).copied().unwrap_or(0);
                if any_kills(chunk, book.marks_for(key).iter(), existing, version, rank) {
                    guard.remove_cell();
                }
            }
        }
    }
    Ok(())
}

/// Install committed bracket entries, grouped per id. `book` is what the
/// scan knows about unbracketed deletes; without it (late in-doubt
/// resolution) only the index is consulted.
pub(crate) fn apply_bracketed_cells(
    chunks: &[Chunk],
    mut cells: Vec<PendingBracketCell>,
    mut book: Option<&mut DeleteBook>,
) -> Result<(), RecoveryCellReadError> {
    cells.sort_by_key(|cell| (cell.chunk_id, cell.hash, cell.addr));
    let mut start = 0;
    while start < cells.len() {
        let mut end = start + 1;
        while end < cells.len()
            && cells[end].chunk_id == cells[start].chunk_id
            && cells[end].hash == cells[start].hash
        {
            end += 1;
        }
        apply_bracketed_group(chunks, &cells[start..end], book.as_deref_mut())?;
        start = end;
    }
    Ok(())
}

/// Replace resolved entries with durable plain entries before releasing their parts.
/// The caller resolves the index first and excludes client writes during recovery.
pub(crate) fn rewrite_resolved_entries(
    chunks: &[Chunk],
    cells: &[PendingBracketCell],
) -> Result<(), super::cell::WriteError> {
    for cell in cells {
        let chunk = &chunks[cell.chunk_id];
        if !cell.tombstone {
            let guard = match chunk.lock_cell_for_read(cell.hash) {
                Ok(guard) => guard,
                Err(super::cell::ReadError::CellDoesNotExisted) => continue,
                Err(error) => return Err(super::cell::WriteError::ReadError(error)),
            };
            if *guard != cell.addr {
                continue;
            }
        }
        let size =
            super::io::align_address(8, ENTRY_HEAD_SIZE + cell.content_length as usize) as u32;
        let source = chunk
            .locate_segment(cell.addr)
            .ok_or(super::cell::WriteError::CannotAllocateSpace)?;
        let pending = chunk.try_acquire_in_class(size, false, source.segment_class())?;
        // Source parts stay pinned until every replacement is durable.
        unsafe {
            std::ptr::copy_nonoverlapping(
                cell.addr as *const u8,
                pending.addr as *mut u8,
                size as usize,
            );
        }
        pending.append()?;
        pending
            .seg
            .force_wal_sync()
            .map_err(super::cell::WriteError::from_wal_io)?;
        if !cell.tombstone {
            let mut guard = chunk
                .lock_cell_for_write(cell.hash, true)
                .map_err(super::cell::WriteError::ReadError)?;
            if *guard == cell.addr {
                **guard.word_mutex_guard() = pending.addr;
            }
        }
    }
    Ok(())
}

/// Install one cell from a committed bracket.
pub(crate) fn apply_bracketed_cell(
    chunks: &[Chunk],
    cell: &PendingBracketCell,
) -> Result<(), RecoveryCellReadError> {
    apply_bracketed_group(chunks, std::slice::from_ref(cell), None)
}

/// All committed bracket entries of ONE id, oldest address first.
///
/// Tombstones act first -- on what is installed and on the group's own cells
/// -- and of the cells that remain the highest version is installed (ties go
/// to the later address).
fn apply_bracketed_group(
    chunks: &[Chunk],
    group: &[PendingBracketCell],
    mut book: Option<&mut DeleteBook>,
) -> Result<(), RecoveryCellReadError> {
    let first = &group[0];
    let Some(chunk) = chunks.get(first.chunk_id) else {
        error!(
            "a committed bracket names chunk {} which this store does not have",
            first.chunk_id
        );
        return Ok(());
    };
    let hash = first.hash;
    let key = (first.chunk_id, hash);

    let plain_ties = book
        .as_deref()
        .and_then(|b| b.ties.get(&key).copied())
        .unwrap_or(0);
    // Plain tombstones already known for this id, then this group's own.
    let mut known = book
        .as_deref()
        .map(|b| b.marks_for(key))
        .unwrap_or_default();
    for tombstone in group.iter().filter(|cell| cell.tombstone) {
        let mark = DeleteMark {
            addr: tombstone.addr,
            version: tombstone.version,
            target_seq: tombstone.target_seq,
            count: 1,
        };
        match known
            .iter_mut()
            .find(|m| m.version == mark.version && m.target_seq == mark.target_seq)
        {
            Some(existing) => {
                existing.count += 1;
                existing.addr = existing.addr.max(mark.addr);
            }
            None => known.push(mark),
        }
        if let Some(b) = book.as_deref_mut() {
            b.add_mark(key, mark);
        }
    }
    let marks = known;

    // The installed entry first: a delete in this group may be for it.
    if !marks.is_empty() {
        if let Some(mut guard) = recovered_cell_guard(chunk, hash)? {
            let existing = *guard;
            if existing != 0 {
                let version = guard.cell_version().map_err(RecoveryCellReadError)?;
                if any_kills(chunk, marks.iter(), existing, version, plain_ties) {
                    guard.remove_cell();
                }
            }
        }
    }

    // Then the group's own cells, ranked among equals by write order.
    let cells: Vec<&PendingBracketCell> = group.iter().filter(|cell| !cell.tombstone).collect();
    let survivor = cells
        .iter()
        .enumerate()
        .filter(|(index, cell)| {
            let seq = segment_of(chunk, cell.addr).map(|(seq, _)| seq);
            let equals_before = cells[..*index]
                .iter()
                .filter(|other| {
                    other.version == cell.version
                        && segment_of(chunk, other.addr).map(|(seq, _)| seq) == seq
                })
                .count() as u32;
            !any_kills(
                chunk,
                marks.iter(),
                cell.addr,
                cell.version,
                plain_ties + equals_before,
            )
        })
        .map(|(_, cell)| *cell)
        // The group is sorted by address, and addresses do not follow write
        // order: concurrent transactions write into different segment heads.
        // The highest version is the newest write; `max_by_key` keeps the
        // later address when versions tie.
        .max_by_key(|cell| cell.version);
    let Some(cell) = survivor else {
        return Ok(());
    };

    let mut guard = chunk.lock_or_insert_cell(hash);
    let existing = *guard;
    if existing == cell.addr {
        chunk.slot_bytes.add(hash, cell.content_length);
        return Ok(());
    }
    if existing == 0 {
        **guard.word_mutex_guard() = cell.addr;
        chunk.slot_bytes.add(hash, cell.content_length);
        return Ok(());
    }
    // Something else already claims this id and no delete took it away: the
    // higher version wins, and the later address breaks a tie.
    let existing_version = guard.cell_version().map_err(RecoveryCellReadError)?;
    if cell.version > existing_version || (cell.version == existing_version && cell.addr > existing)
    {
        **guard.word_mutex_guard() = cell.addr;
        chunk.slot_bytes.add(hash, cell.content_length);
    }
    Ok(())
}

/// Entries a scan stepped over because they failed their own checksum.
/// Loud on purpose: it is real damage, just damage that no longer costs the
/// rest of the segment.
pub static RECOVERY_DAMAGED_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Read a segment image's fixed tail link, if it has one.
///
/// One read, no scanning: that is the point of putting the link at a FIXED
/// position. A segment that answers Some is a chain part, so its live entries
/// stop before the link and the zeros between are expected rather than
/// suspicious -- which is exactly the ambiguity that forces PADDING entries in
/// shared segments and does not exist here, because the link bounds the live
/// region from outside.
fn read_chain_link(data: &[u8], usable_len: usize) -> Option<(usize, u64, u64)> {
    let tail = usable_len.saturating_sub(crate::ram::bracket::TXN_CONT_ENTRY_SIZE);
    let tail = tail - (tail % ENTRY_HEAD_SIZE);
    if tail + crate::ram::bracket::TXN_CONT_ENTRY_SIZE > data.len() || tail == 0 {
        return None;
    }
    let addr = data.as_ptr() as usize + tail;
    let word = u32::from_le_bytes(data[tail..tail + 4].try_into().ok()?);
    match crate::ram::entry::unpack_type_word(word) {
        crate::ram::entry::TypeWord::Checked(EntryType::TXN_CONT, _) => {}
        _ => return None,
    }
    // A link is only a link if it vouches for itself; anything else at that
    // offset is ordinary data that happens to sit there.
    //
    // Bounded by the BUFFER, which is the whole point: the type word above
    // can be matched by garbage, and the length that follows it is garbage
    // too. Unbounded, this checksummed ~3 GB past the end and took the
    // process down from a recovery thread -- reproduced on two machines.
    let data_end = data.as_ptr() as usize + data.len();
    if crate::ram::entry::verify_entry_at(addr, data_end) != Some(true) {
        return None;
    }
    let (header, _) = Entry::decode_from(addr, |_, _| {});
    let content = unsafe {
        std::slice::from_raw_parts(
            Entry::content_pos(addr) as *const u8,
            header.content_length as usize,
        )
    };
    let (short, prev_seq) = crate::ram::bracket::decode_txn_cont(content)?;
    Some((tail, short, prev_seq))
}

/// Whether the entry at `addr` stands on its own: aligned, a well-formed
/// header, a length that fits inside the scanned extent, and either a
/// checksum that verifies or a padding stamp.
///
/// This is the resync anchor. A damaged entry's own length is still
/// bounds-checked, so the position of its successor is knowable; if THAT
/// entry vouches for itself, the damage is one entry wide and the rest of
/// the segment is still readable. Demanding a positively-good successor is
/// what keeps this from walking into garbage: a corrupt length lands
/// somewhere arbitrary, and arbitrary bytes do not pass a checksum.
fn entry_stands_alone(addr: usize, bound: usize) -> bool {
    use crate::ram::entry::{unpack_type_word, verify_entry_at, TypeWord};
    if addr % 8 != 0 || addr + ENTRY_HEAD_SIZE > bound {
        return false;
    }
    let word = unsafe {
        let head = std::slice::from_raw_parts(addr as *const u8, 4);
        u32::from_le_bytes(head.try_into().unwrap())
    };
    if matches!(unpack_type_word(word), TypeWord::Invalid) {
        return false;
    }
    let (header, _) = Entry::decode_from(addr, |_, _| {});
    if header.entry_type == EntryType::UNDECIDED || header.content_length == 0 {
        return false;
    }
    let size = ENTRY_HEAD_SIZE + header.content_length as usize;
    if size > SEGMENT_SIZE || addr + size > bound {
        return false;
    }
    match verify_entry_at(addr, bound) {
        Some(true) => true,
        Some(false) => false,
        // Unchecked: only a reservation stamp, which carries no content to
        // check, is trusted as an anchor.
        None => header.entry_type == EntryType::PADDING,
    }
}

/// Whether everything from `start_offset` to the LIVE end is zero.
///
/// Bounded by the live extent, not by the buffer: a chain part's link sits
/// past that extent, and reading it as "non-zero bytes after the padding"
/// turns an expected layout into a damage report -- and stops the walk.
fn has_only_zero_padding(data: &[u8], start_offset: usize) -> bool {
    data[start_offset..].iter().all(|byte| *byte == 0)
}

fn observe_recovered_segment_class(
    chunk: &Chunk,
    seg_id: u64,
    schema_id: SchemaVid,
    detected_class: &mut Option<SegmentClass>,
    mixed_class_warning_emitted: &mut bool,
    missing_schema_entries: &mut usize,
    first_missing_schema_id: &mut Option<u32>,
) -> io::Result<()> {
    if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
        let entry_class = if schema.blobs {
            SegmentClass::Blob
        } else {
            SegmentClass::Regular
        };

        match *detected_class {
            None => *detected_class = Some(entry_class),
            Some(existing_class) if existing_class == entry_class => {}
            Some(existing_class) => {
                *detected_class = Some(SegmentClass::Blob);
                if !*mixed_class_warning_emitted {
                    warn!(
                        "recovery scan found mixed runtime classes in segment {} (saw {:?} and {:?}); treating the recovered segment as Blob for safety",
                        seg_id,
                        existing_class,
                        entry_class
                    );
                    *mixed_class_warning_emitted = true;
                }
            }
        }
    } else {
        *missing_schema_entries += 1;
        first_missing_schema_id.get_or_insert(schema_id.get());
    }

    Ok(())
}

fn ensure_backup_for_cold_recovery(chunk: &Chunk, file_info: &SegmentFileInfo) -> io::Result<()> {
    if file_info.is_backup {
        return Ok(());
    }

    let copied = chunk.file_manager.copy_wal_to_backup(
        file_info.chunk_id,
        file_info.seg_id,
        file_info.seq_id,
        Some(SEGMENT_SIZE),
    )?;
    if copied {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "cold recovery for segment {} requires a backup file, but WAL copy failed",
                file_info.seg_id
            ),
        ))
    }
}

fn apply_recovery_scan_result(segment: &Segment, scan_result: &RecoveryScanResult) {
    segment
        .append_header
        .store(segment.addr + scan_result.append_offset, Ordering::Release);
    segment
        .dead_space
        .store(scan_result.dead_space, Ordering::Release);
    segment
        .tombstones
        .store(scan_result.tombstones, Ordering::Release);
    if scan_result.dead_space > 0 || scan_result.tombstones > 0 {
        segment.note_dead_bytes_change();
    }
}

fn prepare_recovered_segment(
    chunk: &Chunk,
    file_info: &SegmentFileInfo,
    hot: bool,
    segment_class: SegmentClass,
) -> io::Result<AArc<Segment>> {
    if let Some(existing_segment) =
        newer_resident_segment(chunk, file_info.seg_id, file_info.seq_id)
    {
        return Ok(existing_segment);
    }

    if let Some(existing_segment) = chunk.segs.get(&(file_info.seg_id as usize)) {
        let needs_replacement = existing_segment.seq_id < file_info.seq_id
            || existing_segment.segment_class() != segment_class
            || existing_segment.is_hot() != hot;

        if needs_replacement {
            let replacement = chunk
                .allocator
                .alloc_seg_at_id_with(
                    file_info.seg_id,
                    file_info.seq_id,
                    &chunk.file_manager,
                    hot,
                    segment_class,
                )
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::OutOfMemory, "Cannot allocate segment")
                })?;
            return Ok(install_recovered_segment(chunk, replacement));
        }

        return Ok(existing_segment);
    }

    let new_segment = chunk
        .allocator
        .alloc_seg_at_id_with(
            file_info.seg_id,
            file_info.seq_id,
            &chunk.file_manager,
            hot,
            segment_class,
        )
        .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "Cannot allocate segment"))?;
    let segment_id = new_segment.id as usize;
    chunk.put_segment(new_segment);
    Ok(chunk.segs.get(&segment_id).unwrap())
}

fn install_recovered_segment(chunk: &Chunk, segment: Segment) -> AArc<Segment> {
    let segment_id = segment.id as usize;
    let new_is_hot = segment.is_hot();
    let replaced = chunk.segs.insert_back(segment_id, AArc::new(segment));

    if let Some(ref tiered_manager) = chunk.tiered_manager {
        let replaced_hot = replaced
            .as_ref()
            .map(|segment| segment.is_hot())
            .unwrap_or(false);
        match (replaced_hot, new_is_hot) {
            (false, true) => tiered_manager.increment_hot_count_for(chunk),
            (true, false) => tiered_manager.decrement_hot_count_for(chunk),
            _ => {}
        }
    }

    chunk.segs.get(&segment_id).unwrap()
}

/// Recover a segment as hot by copying file data to memory
fn recover_segment_as_hot(segment: &Segment, file_data: &[u8]) {
    // Verify the data looks like valid segment data (should start with entry headers, not compression magic)
    if file_data.len() >= 4 {
        let magic = &file_data[..4];
        // NEB\x01 or NEB\x02 are compression magic headers - data should NOT start with these
        if magic == [0x4E, 0x45, 0x42, 0x01] || magic == [0x4E, 0x45, 0x42, 0x02] {
            panic!(
                "CRITICAL: Attempting to copy COMPRESSED data to segment memory!\n\
                 Segment ID: {}, Address: {:#x}\n\
                 Data starts with compression magic: {:02X} {:02X} {:02X} {:02X}\n\
                 This indicates decompression was skipped. Data size: {} bytes",
                segment.id,
                segment.addr,
                magic[0],
                magic[1],
                magic[2],
                magic[3],
                file_data.len()
            );
        }
    }

    // Traditional hot recovery - copy data to memory
    unsafe {
        let src_ptr = file_data.as_ptr();
        let dst_ptr = segment.addr as *mut u8;
        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, file_data.len());
    }

    // Note: We don't have chunk_id in this function, but segment addresses are unique per chunk
    debug!(
        "HOT recovery: copied {} bytes to segment {} at address {:#x} (first 8 bytes: {:02x?})",
        file_data.len(),
        segment.id,
        segment.addr,
        &file_data[..8.min(file_data.len())]
    );
}

fn current_segment_entry_content_length(
    data_base: usize,
    segment_base: usize,
    data_len: usize,
    addr: usize,
) -> Option<u32> {
    let offset = addr.checked_sub(segment_base)?;
    if offset >= data_len {
        return None;
    }

    let entry_addr = data_base + offset;
    let (entry, _) = Entry::decode_from(entry_addr, |_, _| {});
    Some(entry.content_length)
}

/// Scan recovered segment data from a slice and update the recovery index state.
///
/// The scan runs against the deterministic virtual address range for the segment
/// before a Segment object is installed. That lets recovery discover the final
/// segment class and statistics in the same pass that rebuilds the cell index.
/// An undecodable bracket record, as the error that stops recovery.
fn bracket_format_io_error(
    seg_id: u64,
    offset: usize,
    error: crate::ram::bracket::BracketFormatError,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        BracketRecordError {
            seg_id,
            offset,
            error,
        },
    )
}

/// A bracket record that recovery cannot read. Unlike other scan errors, it
/// stops the start instead of quarantining the segment: it means the store
/// holds another record format, and every segment would be quarantined.
#[derive(Debug)]
pub struct BracketRecordError {
    pub seg_id: u64,
    pub offset: usize,
    pub error: crate::ram::bracket::BracketFormatError,
}

impl std::fmt::Display for BracketRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "segment {} has an unreadable bracket record at offset {}: {}",
            self.seg_id, self.offset, self.error
        )
    }
}

impl std::error::Error for BracketRecordError {}

fn is_bracket_record_error(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.is::<BracketRecordError>())
}

fn scan_segment_from_data(
    chunk: &Chunk,
    seg_id: u64,
    segment_base: usize,
    data: &[u8],
    version_map: &mut VersionMap,
    origin_floors: &[std::sync::atomic::AtomicU64],
    declared_used_len: Option<usize>,
    ledger: &BracketLedger,
) -> io::Result<RecoveryScanResult> {
    use byteorder::{LittleEndian, ReadBytesExt};
    use std::io::Cursor;

    let data_base = data.as_ptr() as usize;
    // An image that records its live extent is read to exactly that point.
    // Everything past it is untouched segment memory and carries no meaning,
    // so it is never parsed -- which is what makes "empty" and "damaged"
    // distinguishable at all. Without the cursor both look like "no entries",
    // and recovery used to fail the whole store rather than guess.
    let live_len = declared_used_len
        .map(|len| len.min(data.len()))
        .unwrap_or(data.len());
    // A chain part's live entries stop before its link. Read that first, so
    // the walk never meets the zeros between the last entry and the tail.
    let chain_link = read_chain_link(data, live_len);
    let live_len = match chain_link {
        Some((tail, short, prev_seq)) => {
            debug!(
                "segment {} is a chain part (txn {:#x}, previous seq {}); its entries end at {}",
                seg_id, short, prev_seq, tail
            );
            tail
        }
        None => live_len,
    };
    let bound = data_base + live_len;

    let mut cursor = data_base;
    let mut stashed_tombstones = Vec::new();
    let mut tombstone_count = 0u32;
    let mut dead_space = 0u64;
    let mut entries_processed = 0u32;
    let mut append_header = data_base;
    let mut detected_class = None;
    let mut mixed_class_warning_emitted = false;
    let mut missing_schema_entries = 0usize;
    let mut first_missing_schema_id = None;
    // The bracket currently open: where its entries start, and whose it is.
    let mut open_bracket: Option<(usize, crate::server::transactions::TxnId)> = None;

    while cursor < bound {
        entries_processed += 1;
        if entries_processed > 1_000_000 {
            warn!(
                "Recovered segment {} scan exceeded 1M entries, breaking",
                seg_id
            );
            break;
        }
        let prev_cursor = cursor;

        if cursor + ENTRY_HEAD_SIZE > bound {
            if append_header > data_base {
                warn!(
                    "Recovered segment {} scan stopped at a crash-truncated tail (offset {})",
                    seg_id,
                    cursor - data_base
                );
                break;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recovery cannot scan segment {}: truncated entry header at offset {}",
                    seg_id,
                    cursor - data_base
                ),
            ));
        }

        let entry_type_bits = unsafe {
            let mut reader = Cursor::new(std::slice::from_raw_parts(cursor as *const u8, 8));
            reader.read_u32::<LittleEndian>().unwrap()
        };
        if matches!(
            crate::ram::entry::unpack_type_word(entry_type_bits),
            crate::ram::entry::TypeWord::Invalid
        ) {
            if append_header > data_base {
                warn!(
                    "Recovered segment {} scan stopped at a malformed tail (offset {}, invalid entry type bits {})",
                    seg_id,
                    cursor - data_base,
                    entry_type_bits
                );
                break;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recovery cannot scan segment {}: invalid entry type bits {} at offset {}",
                    seg_id,
                    entry_type_bits,
                    cursor - data_base
                ),
            ));
        }

        let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);
        if entry_header.entry_type == EntryType::UNDECIDED || entry_header.content_length == 0 {
            let padding_offset = cursor - data_base;
            if !has_only_zero_padding(&data[..live_len], padding_offset) {
                if append_header > data_base {
                    warn!(
                        "Recovered segment {} scan stopped at a non-zero truncated tail (offset {})",
                        seg_id,
                        padding_offset
                    );
                    break;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "recovery cannot scan segment {}: non-zero bytes after padding at offset {}",
                        seg_id,
                        padding_offset
                    ),
                ));
            }
            break;
        }

        let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;

        if entry_size == 0
            || entry_size > SEGMENT_SIZE
            || entry_size < ENTRY_HEAD_SIZE
            || cursor + entry_size > bound
        {
            if append_header > data_base {
                warn!(
                    "Recovered segment {} scan stopped at a truncated tail (offset {}, entry size {})",
                    seg_id,
                    cursor - data_base,
                    entry_size
                );
                break;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recovery cannot scan segment {}: invalid entry size {} at offset {}",
                    seg_id,
                    entry_size,
                    cursor - data_base
                ),
            ));
        }

        // Content integrity, where a length check cannot reach: an entry
        // whose header is perfectly well-formed can still hold bytes that
        // were never written by any writer. Entries from before checksums
        // report `None` and are taken on trust, as they always were.
        if crate::ram::entry::verify_entry_at(cursor, bound) == Some(false) {
            // One damaged entry must not cost the rest of the segment.
            //
            // Stopping here was the whole loss: a single entry whose content
            // had been mutated after its header was published truncated its
            // segment at that offset, and every cell appended after it --
            // including, in the corpse that found this, a ranged tree's
            // metadata cell -- was dropped from a store that held every byte
            // on disk. The mutation itself is fixed (`abandon_entry`), but
            // the walk should not be that brittle in the first place.
            //
            // Resync only onto a successor that vouches for itself; if
            // nothing there does, the tail really is unreadable and the old
            // behaviour is the right one.
            let next = cursor + entry_size;
            if entry_stands_alone(next, bound) {
                warn!(
                    "Recovered segment {} has a damaged entry at offset {} ({} bytes): its content                      does not match its checksum. The entry after it verifies, so it is skipped as                      dead space and the rest of the segment is kept.",
                    seg_id,
                    cursor - data_base,
                    entry_size
                );
                RECOVERY_DAMAGED_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                dead_space += entry_size as u64;
                append_header = cursor + entry_size;
                cursor = next;
                continue;
            }
            if append_header > data_base {
                warn!(
                    "Recovered segment {} scan stopped at offset {}: the entry's content does \
                     not match its checksum. Everything before it is intact and is kept.",
                    seg_id,
                    cursor - data_base
                );
                break;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recovery cannot scan segment {}: the FIRST entry (offset {}) fails its \
                     content checksum, so nothing in this segment can be trusted",
                    seg_id,
                    cursor - data_base
                ),
            ));
        }

        let offset = cursor - data_base;
        let virtual_cursor = segment_base + offset;

        // The bracket state machine.
        //
        // A transaction's entries are contiguous here -- one writer owns the
        // segment while it writes -- so attributing them needs no per-entry
        // stamps, only the two markers around them. Entries inside a bracket
        // are NOT applied until the COMMIT is seen; when it is, the walk
        // REWINDS to the start of the bracket and replays it through the very
        // same apply code below. Replaying rather than buffering keeps one
        // code path for applying a recovered cell: a second copy of that
        // logic is a second place for the two to drift, and this is the code
        // that decides what a store contains.
        match entry_header.entry_type {
            EntryType::BEGIN => {
                let content = unsafe {
                    std::slice::from_raw_parts(
                        Entry::content_pos(cursor) as *const u8,
                        entry_header.content_length as usize,
                    )
                };
                if let Some((start, txn)) = open_bracket.take() {
                    // One writer per segment means this cannot happen from a
                    // legal interleaving; say so rather than guess, and treat
                    // the older bracket as unfinished, which is the safe read.
                    warn!(
                        "Recovered segment {} has a BEGIN at offset {} while the bracket for {:?} \
                         opened at offset {} is still open; treating the earlier one as \
                         uncommitted",
                        seg_id,
                        offset,
                        txn,
                        start - data_base
                    );
                }
                match crate::ram::bracket::decode_begin(content) {
                    Ok(begin) => {
                        ledger.note_participants(&begin.txn, begin.participants);
                        ledger
                            .parts
                            .lock()
                            .entry(begin.txn.clone())
                            .or_default()
                            .insert((chunk.id, seg_id));
                        open_bracket = Some((cursor + entry_size, begin.txn));
                    }
                    // The frame or the image checked out, so the record is
                    // what was written: another format, or damage. Reading
                    // the entries behind it as ordinary would apply writes
                    // that were never committed. Stop the start instead.
                    Err(error) => {
                        return Err(bracket_format_io_error(seg_id, offset, error));
                    }
                }
                append_header = cursor + entry_size;
                cursor += entry_size;
                continue;
            }
            EntryType::COMMIT => {
                let content = unsafe {
                    std::slice::from_raw_parts(
                        Entry::content_pos(cursor) as *const u8,
                        entry_header.content_length as usize,
                    )
                };
                let decoded = crate::ram::bracket::decode_commit(content);
                match (&open_bracket, decoded) {
                    (Some((_start, open_txn)), Some((commit_txn, manifest)))
                        if *open_txn == commit_txn =>
                    {
                        // The bracket closed here. Whether it is APPLIED is
                        // not decided in this segment: a cross-chunk
                        // transaction is committed as a whole, so the ledger
                        // settles it once every COMMIT is known.
                        ledger
                            .closed_parts
                            .lock()
                            .entry(commit_txn.clone())
                            .or_default()
                            .insert((chunk.id, seg_id));
                        ledger.note_commit(&commit_txn, manifest);
                        open_bracket = None;
                    }
                    (Some((start, open_txn)), Some((commit_txn, _))) => {
                        warn!(
                            "Recovered segment {} has a COMMIT for {:?} at offset {} while the \
                             open bracket belongs to {:?} (from offset {}); neither is applied",
                            seg_id,
                            commit_txn,
                            offset,
                            open_txn,
                            start - data_base
                        );
                        open_bracket = None;
                    }
                    (None, _) => {
                        // A COMMIT whose BEGIN is not in this segment: the
                        // final part of a chain, which Step 3 resolves. On its
                        // own it closes nothing here. A resolved site writes a
                        // complete BEGIN+COMMIT carrier instead
                        // (`Chunk::write_resolved_commit`).
                        debug!(
                            "Recovered segment {} has a COMMIT at offset {} with no open bracket",
                            seg_id, offset
                        );
                    }
                    (Some(_), None) => warn!(
                        "Recovered segment {} has an undecodable COMMIT at offset {}; its bracket \
                         stays uncommitted",
                        seg_id, offset
                    ),
                }
                append_header = cursor + entry_size;
                cursor += entry_size;
                continue;
            }
            _ => {}
        }
        // Inside a bracket: buffer, do not apply.
        //
        // The entry is real and durable; whether it counts depends on the
        // transaction, and that is a decision no single segment can make.
        if let Some((_, txn)) = &open_bracket {
            if entry_header.entry_type == EntryType::CELL {
                let cell_header = cell_header_from_entry_content_addr(Entry::content_pos(cursor));
                observe_recovered_segment_class(
                    chunk,
                    seg_id,
                    cell_header.schema,
                    &mut detected_class,
                    &mut mixed_class_warning_emitted,
                    &mut missing_schema_entries,
                    &mut first_missing_schema_id,
                )?;
                // The id floor is raised even for a transaction that turns
                // out uncommitted: the id reached disk, so reissuing it would
                // be wrong regardless of whether the write counted.
                if !cell_header.id.is_hashed() {
                    if let Some(floor) = origin_floors.get(cell_header.id.origin() as usize) {
                        floor.fetch_max(
                            cell_header.id.sequence(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                }
                ledger.note_pending(
                    txn,
                    PendingBracketCell {
                        chunk_id: chunk.id,
                        hash: cell_header.id.bits(),
                        tombstone: false,
                        addr: virtual_cursor,
                        version: cell_header.version,
                        content_length: entry_header.content_length,
                        target_seq: 0,
                    },
                );
            } else if entry_header.entry_type == EntryType::TOMBSTONE {
                let tombstone = Tombstone::read(cursor);
                ledger.note_pending(
                    txn,
                    PendingBracketCell {
                        chunk_id: chunk.id,
                        hash: tombstone.id.bits(),
                        tombstone: true,
                        addr: virtual_cursor,
                        version: tombstone.version,
                        content_length: entry_header.content_length,
                        target_seq: tombstone.segment_seq_id,
                    },
                );
            }
            append_header = cursor + entry_size;
            cursor += entry_size;
            continue;
        }

        if entry_header.entry_type == EntryType::CELL {
            let content_addr = Entry::content_pos(cursor);
            let cell_header = cell_header_from_entry_content_addr(content_addr);
            observe_recovered_segment_class(
                chunk,
                seg_id,
                cell_header.schema,
                &mut detected_class,
                &mut mixed_class_warning_emitted,
                &mut missing_schema_entries,
                &mut first_missing_schema_id,
            )?;
            let hash = cell_header.id.bits();
            let new_version = cell_header.version;
            // Any recovered allocated-class id raises its origin's floor so
            // the allocator can never reissue a sequence that reached disk,
            // even if the durable lease record was lost with it.
            if !cell_header.id.is_hashed() {
                if let Some(floor) = origin_floors.get(cell_header.id.origin() as usize) {
                    floor.fetch_max(
                        cell_header.id.sequence(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }

            let seen = VersionSeen {
                version: new_version,
                live_size: entry_header.content_length,
            };
            let existing_version = match version_map.entry(hash) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let version = entry.get().version;
                    if version == new_version && entry.get().live_size != 0 {
                        ledger.note_tie(chunk.id, hash);
                    }
                    version
                }
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(seen).version,
            };

            if trace_pages_enabled() {
                if let Some(kind) = traced_schema(cell_header.schema) {
                    warn!(
                        "PAGE-TRACE {} cell id={:?} ver={} chunk={} seg={} off={} prior_ver={} {}",
                        kind,
                        cell_header.id,
                        new_version,
                        chunk.id,
                        seg_id,
                        offset,
                        existing_version,
                        if new_version >= existing_version {
                            "WIN"
                        } else {
                            "LOSE"
                        }
                    );
                }
            }
            if new_version >= existing_version {
                version_map.insert(hash, seen);

                let mut cell_guard = chunk
                    .cell_index
                    .lock_or_insert(hash as usize, virtual_cursor);
                if *cell_guard != virtual_cursor {
                    let existing_addr = *cell_guard;
                    if existing_addr == 0 {
                        *cell_guard = virtual_cursor;
                    } else {
                        if let Some(old_size) = current_segment_entry_content_length(
                            data_base,
                            segment_base,
                            data.len(),
                            existing_addr,
                        ) {
                            dead_space += old_size as u64;
                        } else if let Some(old_seg) = chunk.locate_segment(existing_addr) {
                            // Update dead space for old entry if the previous segment is hot.
                            if !old_seg.is_cold() {
                                let (entry, _) = Entry::decode_from(existing_addr, |_, _| {});
                                old_seg
                                    .dead_space
                                    .fetch_add(entry.content_length, Ordering::Relaxed);
                                old_seg.note_dead_bytes_change();
                            }
                        }
                        *cell_guard = virtual_cursor;
                    }
                }
            } else {
                // Existing version is newer, this entry is dead
                dead_space += entry_header.content_length as u64;
                ledger.note_late_loser(PendingBracketCell {
                    chunk_id: chunk.id,
                    hash,
                    tombstone: false,
                    addr: virtual_cursor,
                    version: new_version,
                    content_length: entry_header.content_length,
                    target_seq: 0,
                });
            }
        } else if entry_header.entry_type == EntryType::TOMBSTONE {
            let content_addr = Entry::content_pos(cursor);
            let tombstone = Tombstone::read_from_entry_content_addr(content_addr);
            let hash = tombstone.id.bits();
            tombstone_count += 1;

            ledger.note_plain_delete(
                chunk.id,
                hash,
                DeleteMark {
                    addr: virtual_cursor,
                    version: tombstone.version,
                    target_seq: tombstone.segment_seq_id,
                    count: 1,
                },
            );
            let seen = VersionSeen {
                version: tombstone.version,
                live_size: 0,
            };
            let existing_version = version_map.entry(hash).or_insert(seen).version;

            if trace_pages_enabled() {
                warn!(
                    "PAGE-TRACE tombstone id={:?} hash={} ver={} seg={} prior_ver={} {}",
                    tombstone.id,
                    hash,
                    tombstone.version,
                    seg_id,
                    existing_version,
                    if tombstone.version >= existing_version {
                        "WIN"
                    } else {
                        "LOSE"
                    }
                );
            }
            if tombstone.version >= existing_version {
                version_map.insert(hash, seen);

                // Tombstone is newer, delete cell from index
                if let Some(mut cell_guard) = chunk.cell_index.lock(hash as usize) {
                    let existing_addr = *cell_guard;
                    if existing_addr != 0 {
                        cell_guard.remove();
                        if let Some(old_size) = current_segment_entry_content_length(
                            data_base,
                            segment_base,
                            data.len(),
                            existing_addr,
                        ) {
                            dead_space += old_size as u64;
                        } else if let Some(target_seg) = chunk.locate_segment(existing_addr) {
                            if !target_seg.is_cold() {
                                let (entry, _) = Entry::decode_from(existing_addr, |_, _| {});
                                target_seg
                                    .dead_space
                                    .fetch_add(entry.content_length, Ordering::Relaxed);
                                target_seg.note_dead_bytes_change();
                            }
                        }
                    }
                }
            } else {
                stashed_tombstones.push(StashedTombstone {
                    hash,
                    version: tombstone.version,
                    chunk_id: chunk.id,
                });
            }
        } else {
        }

        append_header = prev_cursor + entry_size;
        let new_cursor = prev_cursor + entry_size;
        if new_cursor <= prev_cursor {
            break;
        }
        cursor = new_cursor;
    }
    let final_append_offset = append_header - data_base;
    let dead_space_u32 = dead_space.min(u32::MAX as u64) as u32;

    if missing_schema_entries > 0 {
        let first_missing_schema_id = first_missing_schema_id.unwrap_or_default();
        if let Some(segment_class) = detected_class {
            warn!(
                "recovery scan skipped {} cell entries with missing schemas (first schema {}) and kept {:?} based on known entries",
                missing_schema_entries,
                first_missing_schema_id,
                segment_class
            );
        } else {
            warn!(
                "recovery scan skipped {} cell entries with missing schemas (first schema {}) and defaulted the segment to Regular until schemas are registered",
                missing_schema_entries,
                first_missing_schema_id
            );
        }
    }

    debug!(
        "Recovered segment {} scanned from data: {} entries, {} dead bytes, {} stashed tombstones",
        seg_id,
        entries_processed,
        dead_space_u32,
        stashed_tombstones.len()
    );

    Ok(RecoveryScanResult {
        stashed_tombstones,
        segment_class: detected_class.unwrap_or(SegmentClass::Regular),
        append_offset: final_append_offset,
        dead_space: dead_space_u32,
        tombstones: tombstone_count,
        chain_link: chain_link.map(|(_, short, prev)| (short, prev)),
    })
}

/// Merge stashed tombstones from all threads, keeping latest version per hash
fn merge_stashed_tombstones(all_stashed: Vec<Vec<StashedTombstone>>) -> Vec<StashedTombstone> {
    let mut merged: HashMap<u64, StashedTombstone> = HashMap::new();

    for stashed_list in all_stashed {
        for tombstone in stashed_list {
            merged
                .entry(tombstone.hash)
                .and_modify(|existing| {
                    if tombstone.version > existing.version {
                        *existing = tombstone.clone();
                    }
                })
                .or_insert(tombstone);
        }
    }

    let result: Vec<_> = merged.into_values().collect();
    info!("Merged {} unique stashed tombstones", result.len());
    result
}

/// Apply stashed tombstones to cell indices
fn apply_stashed_tombstones(
    stashed: &[StashedTombstone],
    chunks: &[Chunk],
    origin_floors: &[std::sync::atomic::AtomicU64],
) -> Result<(), RecoveryCellReadError> {
    info!("Applying {} stashed tombstones...", stashed.len());

    let mut applied_count = 0;

    for tombstone in stashed {
        let chunk = &chunks[tombstone.chunk_id];

        if let Some(guard) = recovered_cell_guard(chunk, tombstone.hash)? {
            let existing_addr = *guard;
            if existing_addr == 0 {
                continue;
            }
            let cell_header =
                cell_header_from_entry_content_addr(Entry::content_pos(existing_addr));

            if tombstone.version >= cell_header.version {
                let removed = Id::from_bits(tombstone.hash);
                if !removed.is_hashed() {
                    if let Some(floor) = origin_floors.get(removed.origin() as usize) {
                        floor.fetch_max(removed.sequence(), std::sync::atomic::Ordering::Relaxed);
                    }
                }
                // Decode the entry before removing the guard that pins its bytes.
                // The scan counted this cell in the slot, so subtract its size.
                let (removed_entry, _) = Entry::decode_from(existing_addr, |_, _| {});
                chunk
                    .slot_bytes
                    .sub(tombstone.hash, removed_entry.content_length);
                if let Some(seg) = chunk.locate_segment(existing_addr) {
                    seg.dead_space
                        .fetch_add(removed_entry.content_length, Ordering::Relaxed);
                    seg.note_dead_bytes_change();
                }
                guard.remove_cell();
                applied_count += 1;
                debug!(
                "Applied stashed tombstone: deleted cell hash={} version={} with tombstone version={}",
                tombstone.hash, cell_header.version, tombstone.version
            );
            }
        }
    }

    info!("Applied {} stashed tombstones", applied_count);
    Ok(())
}

/// Count total cells recovered across all chunks
fn count_recovered_cells(chunks: &[Chunk]) -> usize {
    chunks.iter().map(|chunk| chunk.cell_count()).sum()
}

/// Main recovery coordinator
pub fn recover_chunks(
    config: &RecoveryConfig,
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
    raft_storage: &Option<String>,
    chunks: &[Chunk],
    origin_floors: &[std::sync::atomic::AtomicU64],
    discovered: Option<Vec<SegmentFileInfo>>,
) -> io::Result<RecoveredBrackets> {
    info!("=== Starting streamlined recovery from storage directories ===");

    // Phase 1: Discover files, unless the caller already did it to size the
    // cell indexes. Rescanning means a second walk of every segment file.
    let files = match discovered
        .filter(|files| !files.is_empty())
        .map(Ok)
        .unwrap_or_else(|| phase1_discover_files(backup_storage, wal_storage))
    {
        Ok(files) => files,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            info!("No segment files found, starting fresh");
            return Ok(RecoveredBrackets::empty());
        }
        Err(e) => return Err(e),
    };

    // Check if configuration can fit all segments
    if !config.can_fit(&files) {
        error!(
            "Recovery configuration mismatch: {} chunks x {} bytes cannot fit all segments",
            config.num_chunks, config.chunk_size
        );
        error!("You may need to adjust chunk count or size, or manually migrate data");
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Configuration cannot fit recovered segments",
        ));
    }

    // We have files to recover; ensure raft storage is configured
    if files.len() > 0 {
        match raft_storage {
            Some(path) => {
                if let Err(e) = fs::create_dir_all(Path::new(path)) {
                    panic!(
                        "Segment files found for recovery but failed to prepare raft_storage at {}: {}. \
                        Schema recovery cannot proceed safely.",
                        path, e
                    );
                }
            }
            None => {
                panic!(
                    "Segment files found for recovery but raft_storage is not configured. \
                    Recovery would restore data without schemas; please configure raft_storage."
                );
            }
        }
    }

    // Pre-set next_seq_id for each chunk BEFORE allocating segments
    phase1_5_set_initial_seq_ids(chunks, &files);

    info!("Processing {} segment files...", files.len());

    let all_stashed_tombstones: Mutex<Vec<StashedTombstone>> = Mutex::new(Vec::new());
    let hot_count = std::sync::atomic::AtomicUsize::new(0);
    let cold_count = std::sync::atomic::AtomicUsize::new(0);
    let segments_processed = std::sync::atomic::AtomicUsize::new(0);
    /// Segments recovery could not scan. They are skipped rather than fatal,
    /// so the count has to reach the operator: the store came up incomplete.
    let quarantined_segments = std::sync::atomic::AtomicUsize::new(0);

    // Built from the discovered files, so a manifest member's presence is a
    // lookup rather than a search. Taken before grouping consumes the list.
    let bracket_ledger = BracketLedger::new(&files);
    let files_by_chunk = group_files_by_chunk(files, chunks.len());
    let total_files: usize = files_by_chunk.iter().map(Vec::len).sum();
    info!(
        "Recovering {} segments with up to {} recovery threads",
        total_files,
        recovery_parallelism(config)
    );

    // Track max seq_id per chunk to update allocators after recovery
    // This ensures new segments continue from where recovered segments left off
    let max_seq_ids: Vec<std::sync::atomic::AtomicU64> = chunks
        .iter()
        .map(|_| std::sync::atomic::AtomicU64::new(0))
        .collect();

    let planned_global_hot_segments = std::sync::atomic::AtomicUsize::new(
        chunks
            .iter()
            .find_map(|chunk| chunk.tiered_manager.as_ref())
            .map(|manager| manager.shared_pool().total_hot_segments())
            .unwrap_or(0),
    );

    let recovery_pool = ThreadPoolBuilder::new()
        .num_threads(recovery_parallelism(config))
        .thread_name(|idx| format!("recovery-{}", idx))
        .build()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to build recovery thread pool: {e}"),
            )
        })?;

    recovery_pool.install(|| -> io::Result<()> {
        files_by_chunk.into_par_iter().enumerate().try_for_each(
            |(chunk_id, chunk_files)| -> io::Result<()> {
                let chunk = &chunks[chunk_id];
                // Built here rather than up front so a chunk's version map is
                // freed as soon as that chunk finishes. Held for the whole run
                // they are all co-resident with the permanent cell index at the
                // exact moment recovery peaks, roughly doubling index memory.
                let mut version_map = new_version_map(&chunk_files);
                let mut local_stashed = Vec::new();

                for file_info in chunk_files {
                    if newer_resident_segment(chunk, file_info.seg_id, file_info.seq_id).is_some() {
                        // warn, not debug: a skipped file is a whole segment's
                        // worth of entries deliberately not read, and every
                        // autopsy of index loss starts by asking whether one
                        // of these fired.
                        warn!(
                            "Skipping stale recovery file for chunk {} seg {} seq {} ({} bytes, backup={})",
                            chunk_id, file_info.seg_id, file_info.seq_id, file_info.size, file_info.is_backup
                        );
                        continue;
                    }

                    // Reading the file is per-segment too. A torn archive --
                    // the OOM kill on TB15 landed mid-write and left "block 214
                    // extends past end of buffer" -- is one segment's problem,
                    // and propagating it from here failed the whole store just
                    // as surely as an unscannable image did. Quarantine covers
                    // the read, the size check and the scan alike: everything
                    // whose blast radius is a single file.
                    //
                    // Before quarantining, though: a BACKUP that cannot be
                    // read or scanned may have a complete WAL twin at the same
                    // seq id -- the archive deletes the WAL only after the
                    // backup is fully durable, so a torn backup implies the
                    // WAL is still whole. Discovery preferred the backup;
                    // falling back to the WAL here recovers every cell the
                    // torn file would have dropped. (The crash-churn fuzzer's
                    // corpse was exactly this: a mid-archive SIGKILL left a
                    // partial backup shadowing an intact WAL, and the ranged
                    // tree's metadata cell vanished with the segment.)
                    let segment_base = chunk.allocator.addr_by_id(file_info.seg_id as usize);
                    let mut attempt = |path: &std::path::Path,
                                       version_map: &mut VersionMap|
                     -> io::Result<(Vec<u8>, Option<usize>, RecoveryScanResult)> {
                        let (file_data, declared_used_len) = load_file_with_used_len(path)?;
                        if file_data.len() > SEGMENT_SIZE {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "{} bytes is larger than a segment",
                                    file_data.len()
                                ),
                            ));
                        }
                        let scan_result = scan_segment_from_data(
                            chunk,
                            file_info.seg_id,
                            segment_base,
                            &file_data,
                            version_map,
                            origin_floors,
                            declared_used_len,
                            &bracket_ledger,
                        )?;
                        Ok((file_data, declared_used_len, scan_result))
                    };
                    if trace_pages_enabled() {
                        warn!(
                            "FILE-TRACE begin chunk={} seg={} seq={} backup={} size={} path={}",
                            chunk_id,
                            file_info.seg_id,
                            file_info.seq_id,
                            file_info.is_backup,
                            file_info.size,
                            file_info.path.display()
                        );
                    }
                    let mut source_is_backup = file_info.is_backup;
                    let (file_data, _declared_used_len, scan_result) =
                        match attempt(&file_info.path, &mut version_map) {
                            Ok(loaded) => loaded,
                            Err(error) => {
                                let wal_twin = if file_info.is_backup {
                                    chunk
                                        .file_manager
                                        .wal_path(chunk_id, file_info.seg_id, file_info.seq_id)
                                        .map(std::path::PathBuf::from)
                                        .filter(|path| path.exists())
                                } else {
                                    None
                                };
                                let mut recovered_from_wal = None;
                                if let Some(wal_path) = wal_twin {
                                    match attempt(&wal_path, &mut version_map) {
                                        Ok(loaded) => {
                                            warn!(
                                                "Backup '{}' for segment {} (chunk {}, seq {}) is unreadable ({}); \
                                                 recovered the segment from its WAL twin '{}' instead. The segment \
                                                 stays dirty so the archiver rewrites a good backup.",
                                                file_info.path.display(),
                                                file_info.seg_id,
                                                chunk_id,
                                                file_info.seq_id,
                                                error,
                                                wal_path.display()
                                            );
                                            source_is_backup = false;
                                            recovered_from_wal = Some(loaded);
                                        }
                                        Err(wal_error) => {
                                            error!(
                                                "WAL twin '{}' for segment {} also failed: {}",
                                                wal_path.display(),
                                                file_info.seg_id,
                                                wal_error
                                            );
                                        }
                                    }
                                }
                                if is_bracket_record_error(&error) {
                                    error!(
                                        "segment {} (chunk {}, seq {}) from '{}': {}. Recovery \
                                         stops.",
                                        file_info.seg_id,
                                        chunk_id,
                                        file_info.seq_id,
                                        file_info.path.display(),
                                        error
                                    );
                                    return Err(error);
                                }
                                match recovered_from_wal {
                                    Some(loaded) => loaded,
                                    None => {
                                        // Quarantine the segment; do not abandon the store.
                                        //
                                        // This used to be `?`, so one unreadable segment
                                        // failed the whole recovery -- every chunk, and
                                        // every segment not yet reached. On TB14 segment
                                        // 2053 did exactly that at 174,900 of 350,902
                                        // segments: the remaining 176,002 were never even
                                        // read, the caller logged "Starting with fresh
                                        // storage", and the ranged index then found an
                                        // empty store and replaced 31 of its 40 trees with
                                        // empty ones. One bad segment cost the database.
                                        //
                                        // Its files are left alone for forensics and for a
                                        // later repair; what is lost is this segment's
                                        // cells, which is the actual damage rather than
                                        // the whole store.
                                        error!(
                                            "QUARANTINED segment {} (chunk {}, seq {}) from '{}': {}. \
                                             Recovery continues; the cells in this segment are missing \
                                             from the recovered store.",
                                            file_info.seg_id,
                                            chunk_id,
                                            file_info.seq_id,
                                            file_info.path.display(),
                                            error
                                        );
                                        quarantined_segments.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                }
                            }
                        };
                    // Downstream decisions (cold recovery, clear_dirty) key
                    // off which SOURCE actually supplied the image.
                    let file_info = SegmentFileInfo {
                        is_backup: source_is_backup,
                        ..file_info
                    };
                    let segment_class = scan_result.segment_class;
                    if trace_pages_enabled() {
                        warn!(
                            "FILE-TRACE scanned chunk={} seg={} seq={} backup={} append_offset={} dead={} tombstones={} class={:?} chain_link={:?}",
                            chunk_id,
                            file_info.seg_id,
                            file_info.seq_id,
                            file_info.is_backup,
                            scan_result.append_offset,
                            scan_result.dead_space,
                            scan_result.tombstones,
                            segment_class,
                            scan_result.chain_link
                        );
                    }

                    let existing_hot = chunk
                        .segs
                        .get(&(file_info.seg_id as usize))
                        .map(|segment| segment.is_hot())
                        .unwrap_or(false);

                    let undecided_wal = !file_info.is_backup && {
                        let parts = bracket_ledger.parts.lock();
                        let closed = bracket_ledger.closed_parts.lock();
                        parts.iter().any(|(txn, members)| {
                            members.contains(&(chunk_id, file_info.seg_id))
                                && !closed.get(txn).is_some_and(|members| members.contains(&(chunk_id, file_info.seg_id)))
                        })
                    };
                    let recover_as_cold = if undecided_wal {
                        false
                    } else if segment_class == SegmentClass::Blob {
                        true
                    } else if chunk.tiered_manager.is_some() {
                        let current_hot_segments =
                            planned_global_hot_segments.load(Ordering::Relaxed);
                        should_recover_as_cold(chunk, &file_info, current_hot_segments)
                    } else {
                        false
                    };

                    match (existing_hot, recover_as_cold) {
                        (true, true) => {
                            planned_global_hot_segments
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                                    Some(current.saturating_sub(1))
                                })
                                .ok();
                        }
                        (true, false) => {}
                        (false, false) => {
                            planned_global_hot_segments.fetch_add(1, Ordering::Relaxed);
                        }
                        (false, true) => {}
                    }

                    if recover_as_cold {
                        ensure_backup_for_cold_recovery(chunk, &file_info)?;
                    }

                    let segment = prepare_recovered_segment(
                        chunk,
                        &file_info,
                        !recover_as_cold,
                        segment_class,
                    )?;

                    if !recover_as_cold {
                        recover_segment_as_hot(&segment, &file_data);
                        hot_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        cold_count.fetch_add(1, Ordering::Relaxed);
                    }

                    apply_recovery_scan_result(&segment, &scan_result);

                    // `clear_dirty` asserts "this segment is archived; its
                    // bytes are on disk, so the tier may drop it from RAM at
                    // will". That is only true when the segment came FROM a
                    // backup file, or when cold recovery just synthesized one
                    // (`ensure_backup_for_cold_recovery`). A segment restored
                    // hot from a WAL file has NO backup: clearing it let
                    // eviction release the pages, the read path went looking
                    // for a `.nbackup` that never existed ("CRITICAL: segment
                    // was marked archived but file is missing"), and every
                    // cell in it read back as zeros -> SchemaDoesNotExisted(0)
                    // and durable loss. TB13 hit this on the phase-1/phase-2
                    // restart: 15 segments lost, 5.5K failed id-list updates,
                    // and phase 3 could not enumerate neighbours at all.
                    // Leaving such segments dirty makes the archiver write a
                    // real backup before the tier is allowed to evict them.
                    if file_info.is_backup || recover_as_cold {
                        segment.clear_dirty();
                        // Both arms mean a backup exists for this seq id --
                        // the image came from one, or `ensure_backup_for_cold_recovery`
                        // is about to write one. Either way the incarnation is
                        // closed and must never be picked up as a write head
                        // again; see `Segment::sealed`.
                        segment.seal();
                    }
                    local_stashed.extend(scan_result.stashed_tombstones);
                    max_seq_ids[chunk_id].fetch_max(file_info.seq_id + 1, Ordering::Relaxed);

                    let processed = segments_processed.fetch_add(1, Ordering::Relaxed) + 1;
                    if processed % 100 == 0 || processed == total_files {
                        info!("Recovery progress: {}/{} segments", processed, total_files);
                    }
                }

                // Seed the per-slot live-bytes counters from what this
                // chunk's scan resolved. The winner's size is in the version
                // map; the index says whether that winner survived (a
                // same-scan tombstone zeroes it). Stashed tombstones applied
                // after this loop decrement the counter themselves.
                use lightning::map::Map as _;
                for (hash, seen) in version_map.iter() {
                    if seen.live_size == 0 {
                        continue;
                    }
                    match chunk.cell_index.get(&(*hash as usize)) {
                        Some(addr) if addr != 0 => {
                            chunk.slot_bytes.add(*hash, seen.live_size)
                        }
                        _ => {}
                    }
                }

                if !local_stashed.is_empty() {
                    all_stashed_tombstones.lock().extend(local_stashed);
                }
                Ok(())
            },
        )
    })?;

    // Every segment has been read, so every COMMIT is known: settle the
    // transactions. This is where a cross-chunk transaction becomes atomic --
    // the chunk whose own COMMIT was lost still applies its part, because the
    // decision came from the transaction rather than from that chunk's bytes.
    let recovered_brackets = bracket_ledger
        .settle(chunks)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    // Update allocator next_seq_id for each chunk to continue from max recovered seq_id
    for (chunk_id, max_seq) in max_seq_ids.iter().enumerate() {
        let next_seq = max_seq.load(Ordering::Relaxed);
        if next_seq > 0 {
            chunks[chunk_id].allocator.set_next_seq_id(next_seq);
        }
    }

    // Give back the addresses recovery bumped past but never restored.
    //
    // Segments are allocated AT an id taken from a filename, which drags the
    // bump pointer over every lower slot whether or not anything lives there.
    // Without this a chunk loses those slots for the life of the process and
    // refuses writes while sitting at half capacity -- worse after every
    // restart. Safe here and nowhere earlier: recovery has restored
    // everything it is going to, so "not in segs" finally means "nobody owns
    // this".
    for chunk in chunks.iter() {
        let reclaimed = chunk
            .allocator
            .reclaim_skipped_slots(|id| chunk.segs.get(&id).is_some());
        if reclaimed > 0 {
            info!(
                "Chunk {}: returned {} segment slot(s) recovery bumped past but never used",
                chunk.id, reclaimed
            );
        }
    }

    for chunk in chunks {
        chunk.reset_write_heads_after_recovery()?;
    }

    for bracket in std::mem::take(&mut *bracket_ledger.resolved_rewrites.lock()) {
        rewrite_resolved_entries(chunks, &bracket.cells).map_err(|error| {
            io::Error::other(format!("resolved transaction rewrite failed: {error:?}"))
        })?;
        for chunk in chunks {
            for segment in chunk.segments() {
                segment.protection.abort_transaction(&bracket.txn);
            }
        }
    }

    // Heads are known now, so every other segment is sealed and must be
    // archived. Repair any that are not before the tier, the cleaner or the
    // archiver get to act on them.
    let repaired: usize = chunks
        .iter()
        .map(|chunk| chunk.archive_unarchived_after_recovery())
        .sum();
    if repaired > 0 {
        info!(
            "Recovery repaired {} sealed segments that had no backup (WAL-only);              sealed-implies-archived restored",
            repaired
        );
    }

    let final_hot = hot_count.load(Ordering::Relaxed);
    let final_cold = cold_count.load(Ordering::Relaxed);
    let final_processed = segments_processed.load(Ordering::Relaxed);
    info!(
        "Segment processing complete: {} hot, {} cold",
        final_hot, final_cold
    );

    // Merge and apply stashed tombstones
    let stashed = all_stashed_tombstones.into_inner();
    let merged_tombstones = merge_stashed_tombstones(vec![stashed]);
    apply_stashed_tombstones(&merged_tombstones, chunks, origin_floors)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    // Count recovered cells
    let total_cells = count_recovered_cells(chunks);

    let quarantined = quarantined_segments.load(Ordering::Relaxed);
    if quarantined > 0 {
        error!(
            "=== Recovery QUARANTINED {} unscannable segment(s). Their cells are NOT in the recovered store; everything else recovered normally. Grep 'QUARANTINED segment' for the files. ===",
            quarantined
        );
    }

    info!(
        "=== Recovery complete: {} cells across {} segments ({} hot, {} cold) ===",
        total_cells, final_processed, final_hot, final_cold
    );

    Ok(recovered_brackets)
}

/// Create a recovery marker file to indicate recovery is in progress
pub fn create_recovery_marker(storage_dir: &str) -> io::Result<()> {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    let mut file = File::create(&marker_path)?;
    file.write_all(b"recovery")?;
    file.sync_all()?;
    Ok(())
}

/// Remove recovery marker file after successful recovery
pub fn remove_recovery_marker(storage_dir: &str) -> io::Result<()> {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    if marker_path.exists() {
        fs::remove_file(&marker_path)?;
    }
    Ok(())
}

/// Check if recovery marker exists (indicates previous crash during recovery)
pub fn check_recovery_marker(storage_dir: &str) -> bool {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    marker_path.exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dovahkiin::types::Map;
    use crate::ram::cell::*;
    use crate::ram::chunk::Chunks;
    use crate::ram::schema::{Field, LocalSchemasCache, Schema};
    use crate::ram::segs::{SegmentClass, SEGMENT_SIZE};
    use crate::ram::tiered::{manager::TieredMemoryManager, SharedMemoryPool, TieredConfig};
    use crate::ram::types::Id;
    use crate::server::ServerMeta;
    use dovahkiin::types::Type;
    use lightning::map::WordMap;
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::TempDir;

    const TEST_SEGMENT_SIZE: usize = 8 * 1024 * 1024; // 8MB
    const DATA_SIZE: usize = 1024; // 1KB per cell

    fn segment_files(chunk_id: usize, count: usize, size: u64) -> Vec<SegmentFileInfo> {
        (0..count)
            .map(|seg_id| SegmentFileInfo {
                chunk_id,
                seg_id: seg_id as u64,
                seq_id: 0,
                path: PathBuf::from(format!("{}-{}-0.nbackup", chunk_id, seg_id)),
                size,
                is_backup: true,
            })
            .collect()
    }

    /// A chunk of a terabyte-scale store must be sized for the cells it will
    /// actually hold. The regression this guards is not "the estimate is a bit
    /// low" but a hard `1 << 22` ceiling that capped a chunk needing ~17M
    /// entries at 4M, forcing every partition through repeated doubling whose
    /// freed tables the allocator never returns.
    #[test]
    fn version_map_capacity_covers_a_terabyte_scale_chunk() {
        // 128 chunks over ~255k segments, as a 1.7 TB store.
        let files = segment_files(0, 1_991, 3 * 1024 * 1024);
        let expected_cells = 1_991 * estimated_cells_per_segment();
        let capacity = new_version_map(&files).capacity();

        assert!(
            capacity >= expected_cells,
            "capacity {} must cover the {} cells the chunk is expected to hold",
            capacity,
            expected_cells
        );
        assert!(
            capacity > 1 << 22,
            "capacity {} fell back under the old ceiling that caused the migration storm",
            capacity
        );
    }

    /// Sizing is per chunk, so files must be attributed to the chunk that owns
    /// them; a mix-up here would size one index huge and the rest empty.
    #[test]
    fn cells_are_estimated_per_chunk() {
        let mut files = segment_files(0, 10, 1024);
        files.extend(segment_files(2, 40, 1024));

        let per_chunk = estimate_cells_per_chunk(&files, 4);

        assert_eq!(per_chunk.len(), 4);
        assert_eq!(per_chunk[0], 10 * estimated_cells_per_segment());
        assert_eq!(per_chunk[1], 0);
        assert_eq!(per_chunk[2], 40 * estimated_cells_per_segment());
        assert_eq!(per_chunk[3], 0);
    }

    /// A fresh store discovers nothing, and must not pre-allocate for a chunk
    /// that may stay empty.
    #[test]
    fn a_fresh_store_estimates_nothing() {
        assert_eq!(estimate_cells_per_chunk(&[], 8), vec![0; 8]);
        assert!(new_version_map(&[]).capacity() >= MIN_RECOVERY_WORD_MAP_CAPACITY);
    }

    /// Files outside the configured chunk count must not panic the sizing pass.
    #[test]
    fn out_of_range_chunk_ids_are_ignored() {
        let files = segment_files(9, 5, 1024);
        assert_eq!(estimate_cells_per_chunk(&files, 2), vec![0, 0]);
    }

    fn default_cell(id: &Id) -> OwnedCell {
        let data: Vec<_> = std::iter::repeat(id.bits() as u8).take(DATA_SIZE).collect();
        OwnedCell {
            header: CellHeader::new(SchemaVid(0), id),
            data: data_map_value!(id: id.bits() as i32, data: data),
        }
    }

    fn default_fields() -> Field {
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ])
    }

    fn setup_test_schema() -> LocalSchemasCache {
        let schema = Schema::new("recovery_test", None, default_fields(), false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema);
        schemas
    }

    fn schema_with_id(schema_id: u32, name: &str, blobs: bool) -> Schema {
        let schema = Schema::new_with_id(schema_id, name, None, default_fields(), false, false);
        if blobs {
            schema.with_blobs(true)
        } else {
            schema
        }
    }

    fn tiered_manager_for_test(physical_memory_limit: usize) -> Arc<TieredMemoryManager> {
        Arc::new(TieredMemoryManager::new(SharedMemoryPool::new(
            &TieredConfig {
                threshold: 0.95,
                lower_watermark: 0.8,
                physical_memory_limit,
                promotion_cooldown_ms: 0,
            },
        )))
    }

    fn entry_bytes_at(addr: usize) -> Vec<u8> {
        let (entry, _) = Entry::decode_from(addr, |_, header| header);
        let entry_size = ENTRY_HEAD_SIZE + entry.content_length as usize;
        unsafe { std::slice::from_raw_parts(addr as *const u8, entry_size).to_vec() }
    }

    fn empty_segment_bytes() -> Vec<u8> {
        vec![0_u8; SEGMENT_SIZE]
    }

    fn tombstone_only_segment_bytes() -> Vec<u8> {
        let mut segment = empty_segment_bytes();
        Tombstone::put(
            segment.as_mut_ptr() as usize,
            41,
            7,
            Id::from_parts(3, 9_999),
        );
        segment
    }

    /// Write a framed WAL holding one entry at segment offset 0 -- the only
    /// WAL format there is, so fixtures build it the same way the writer does.
    fn write_wal_segment(
        wal_dir: &TempDir,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
        entry: &[u8],
    ) {
        let dir = wal_dir.path().join(format!("chunk-wal-{}", chunk_id));
        fs::create_dir_all(&dir).unwrap();
        let mut bytes = crate::ram::wal_format::wal_file_header(seq_id).to_vec();
        bytes.extend_from_slice(&crate::ram::wal_format::frame_record(0, entry));
        fs::write(
            dir.join(format!("{}-{}-{}.nlog", chunk_id, seg_id, seq_id)),
            &bytes,
        )
        .unwrap();
    }

    fn write_backup_segment(
        backup_dir: &TempDir,
        chunk_id: usize,
        seg_id: u64,
        seq_id: u64,
        data: &[u8],
    ) {
        let manager =
            SegmentFileManager::new(Some(backup_dir.path().to_str().unwrap().to_string()), None);
        let path = manager
            .backup_path(chunk_id, seg_id, seq_id)
            .expect("backup storage should be configured for test recovery files");
        fs::write(path, data).unwrap();
    }

    fn temp_raft_dir() -> (TempDir, String) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        (dir, path)
    }

    fn total_hot_segments(chunks: &Arc<Chunks>) -> usize {
        chunks
            .list
            .iter()
            .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_hot()).count())
            .sum()
    }

    fn total_cold_segments(chunks: &Arc<Chunks>) -> usize {
        chunks
            .list
            .iter()
            .map(|chunk| chunk.segments().iter().filter(|seg| seg.is_cold()).count())
            .sum()
    }

    fn write_until_segment_count(chunks: &Arc<Chunks>, partition: u64, target_segments: usize) {
        let mut next_id = 0_u64;
        while chunks.list[0].segments().len() < target_segments {
            let mut cell = default_cell(&Id::allocated(partition as u16, 0, next_id));
            chunks.write_cell(&mut cell).unwrap();
            next_id += 1;
        }
    }

    fn scan_segment_for_recovery_test(
        chunk: &Chunk,
        data: &[u8],
    ) -> io::Result<RecoveryScanResult> {
        let mut version_map = new_version_map(&[]);
        let floors: Vec<std::sync::atomic::AtomicU64> = (0..4096)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        scan_segment_from_data(
            chunk,
            1,
            chunk.allocator.addr_by_id(1),
            data,
            &mut version_map,
            &floors,
            None,
            &BracketLedger::new(&[]),
        )
    }

    /// Scan helper for images that declare their live extent.
    fn scan_segment_with_used_len_for_test(
        chunk: &Chunk,
        data: &[u8],
        used_len: usize,
    ) -> io::Result<RecoveryScanResult> {
        let mut version_map = new_version_map(&[]);
        let floors: Vec<std::sync::atomic::AtomicU64> = (0..4096)
            .map(|_| std::sync::atomic::AtomicU64::new(0))
            .collect();
        scan_segment_from_data(
            chunk,
            1,
            chunk.allocator.addr_by_id(1),
            data,
            &mut version_map,
            &floors,
            Some(used_len),
            &BracketLedger::new(&[]),
        )
    }

    // Purpose: Validate parsing of segment filenames `{chunk}-{seg}-{seq}.{nlog|nbackup}`
    // and correct discrimination of WAL vs backup extensions.
    #[test]
    fn test_parse_filename() {
        use std::fs::File;
        let temp_dir = TempDir::new().unwrap();

        // Create test files
        let path1 = temp_dir.path().join("0-12345-67.nlog");
        File::create(&path1).unwrap();
        let info = SegmentFileInfo::parse_filename(&path1).unwrap();
        assert_eq!(info.chunk_id, 0);
        assert_eq!(info.seg_id, 12345);
        assert_eq!(info.seq_id, 67);
        assert!(!info.is_backup);

        let path2 = temp_dir.path().join("1-98765-43.nbackup");
        File::create(&path2).unwrap();
        let info = SegmentFileInfo::parse_filename(&path2).unwrap();
        assert_eq!(info.chunk_id, 1);
        assert_eq!(info.seg_id, 98765);
        assert_eq!(info.seq_id, 43);
        assert!(info.is_backup);

        // Invalid format
        let path3 = temp_dir.path().join("invalid-file.log");
        File::create(&path3).unwrap();
        assert!(SegmentFileInfo::parse_filename(&path3).is_none());
    }

    #[test]
    fn test_scan_segment_from_data_prefers_blob_when_schema_classes_are_mixed() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let regular_schema = schema_with_id(100, "recovery_mixed_regular", false);
        let blob_schema = schema_with_id(101, "recovery_mixed_blob", true);
        chunk
            .meta
            .schemas
            .debug_only_new_schema(regular_schema.clone());
        chunk
            .meta
            .schemas
            .debug_only_new_schema(blob_schema.clone());

        let regular_id = Id::allocated(10, 0, 1);
        let blob_id = Id::allocated(10, 0, 2);
        let mut regular_cell = OwnedCell {
            header: CellHeader::new(regular_schema.vid, &regular_id),
            data: data_map_value!(id: 1_i32, data: vec![0x11_u8; DATA_SIZE]),
        };
        let mut blob_cell = OwnedCell {
            header: CellHeader::new(blob_schema.vid, &blob_id),
            data: data_map_value!(id: 2_i32, data: vec![0x22_u8; DATA_SIZE]),
        };

        chunks.write_cell(&mut regular_cell).unwrap();
        chunks.write_cell(&mut blob_cell).unwrap();

        let regular_entry = entry_bytes_at(chunks.address_of(&regular_id));
        let blob_entry = entry_bytes_at(chunks.address_of(&blob_id));
        let mut mixed_segment = vec![0_u8; SEGMENT_SIZE];
        mixed_segment[..regular_entry.len()].copy_from_slice(&regular_entry);
        mixed_segment[regular_entry.len()..regular_entry.len() + blob_entry.len()]
            .copy_from_slice(&blob_entry);

        let scan = scan_segment_for_recovery_test(chunk, &mixed_segment)
            .expect("mixed blob and regular cells should recover as a blob segment");
        assert_eq!(scan.segment_class, SegmentClass::Blob);
    }

    #[test]
    fn test_scan_segment_from_data_accepts_empty_segment() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];

        let scan = scan_segment_for_recovery_test(chunk, &empty_segment_bytes())
            .expect("empty archived segment should recover as a regular segment");
        assert_eq!(scan.segment_class, SegmentClass::Regular);
    }

    #[test]
    fn test_scan_segment_from_data_accepts_tombstone_only_segment() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];

        let scan = scan_segment_for_recovery_test(chunk, &tombstone_only_segment_bytes())
            .expect("tombstone-only recovered segment should stay recoverable");
        assert_eq!(scan.segment_class, SegmentClass::Regular);
    }

    #[test]
    fn test_scan_segment_from_data_accepts_valid_prefix_with_truncated_tail() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let blob_schema = schema_with_id(102, "recovery_truncated_blob", true);
        chunk
            .meta
            .schemas
            .debug_only_new_schema(blob_schema.clone());

        let first_id = Id::allocated(12, 0, 1);
        let second_id = Id::allocated(12, 0, 2);
        let mut first_cell = OwnedCell {
            header: CellHeader::new(blob_schema.vid, &first_id),
            data: data_map_value!(id: 1_i32, data: vec![0x55_u8; DATA_SIZE]),
        };
        let mut second_cell = OwnedCell {
            header: CellHeader::new(blob_schema.vid, &second_id),
            data: data_map_value!(id: 2_i32, data: vec![0x66_u8; DATA_SIZE]),
        };

        chunks.write_cell(&mut first_cell).unwrap();
        chunks.write_cell(&mut second_cell).unwrap();

        let first_entry = entry_bytes_at(chunks.address_of(&first_id));
        let second_entry = entry_bytes_at(chunks.address_of(&second_id));
        let mut truncated_segment = first_entry.clone();
        truncated_segment.extend_from_slice(&second_entry[..ENTRY_HEAD_SIZE / 2]);

        let scan = scan_segment_for_recovery_test(chunk, &truncated_segment)
            .expect("a recoverable valid prefix should survive a crash-truncated tail");
        assert_eq!(scan.segment_class, SegmentClass::Blob);
    }

    #[test]
    fn test_scan_segment_from_data_accepts_missing_schema_ids() {
        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let regular_schema = schema_with_id(103, "recovery_missing_schema_regular", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(regular_schema.clone());

        let cell_id = Id::allocated(13, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(regular_schema.vid, &cell_id),
            data: data_map_value!(id: 3_i32, data: vec![0x99_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut cell).unwrap();

        let segment_bytes = entry_bytes_at(writer_chunks.address_of(&cell_id));
        let classifier_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let classifier_chunk = &classifier_chunks.list[0];

        let scan = scan_segment_for_recovery_test(classifier_chunk, &segment_bytes)
            .expect("missing startup schemas should not abort recovery classification");
        assert_eq!(scan.segment_class, SegmentClass::Regular);
    }

    #[test]
    fn test_scan_segment_from_data_rejects_malformed_input() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];

        let mut malformed_segment = vec![0_u8; ENTRY_HEAD_SIZE];
        malformed_segment[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        malformed_segment[4..8].copy_from_slice(&(DATA_SIZE as u32).to_le_bytes());

        let err = scan_segment_for_recovery_test(chunk, &malformed_segment)
            .expect_err("malformed recovered data must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_recovery_allows_empty_archived_segment() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        write_backup_segment(&backup_dir, 0, 0, 1, &empty_segment_bytes());

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        let chunk = &chunks.list[0];
        let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
        let segment = chunk
            .segments()
            .into_iter()
            .next()
            .expect("recovery should keep an empty regular segment resident");

        // Resident, but not the write head: it was recovered from a backup, so
        // its seq id already has a complete image and appending to it would
        // re-create a WAL alongside that backup. See `Segment::sealed`.
        assert!(segment.is_sealed());
        assert_ne!(regular_head, Some(segment.id));
        assert_eq!(blob_head, None);
        assert_eq!(chunk.segments().len(), 1);
        assert_eq!(segment.segment_class(), SegmentClass::Regular);
        assert_eq!(segment.append_header.load(Ordering::Acquire), segment.addr);
        assert_eq!(total_hot_segments(&chunks), 1);
        assert_eq!(total_cold_segments(&chunks), 0);
    }

    /// One unscannable segment must cost that segment, not the store.
    ///
    /// Recovery used to propagate a segment's scan error out of the whole run
    /// (`scan_segment_from_data(...)?`), and the caller answered by logging
    /// "Starting with fresh storage". On TB14 segment 2053 tripped it at
    /// 174,900 of 350,902 segments: the remaining 176,002 were never read, the
    /// store came up empty, and the ranged index then replaced 31 of its 40
    /// trees with empty ones because every page read returned
    /// CellDoesNotExisted. One bad segment cost 1.1 TB.
    /// A rewritten segment must not leave its previous backup behind.
    ///
    /// `archive()` renames the existing backup to `.old` before writing, as a
    /// safety net for the window where neither copy is complete. Nothing
    /// removed it afterwards, so every rewrite left a full-size file on disk
    /// forever -- invisible until a store runs out of space.
    #[cfg(feature = "compress_backups")]
    #[test]
    fn a_rewritten_segment_does_not_leave_its_old_backup_behind() {
        let _ = env_logger::try_init();
        let backup_dir = TempDir::new().unwrap();
        let chunks = Chunks::new_dummy_with_backup(
            1,
            TEST_SEGMENT_SIZE * 4,
            Some(backup_dir.path().to_str().unwrap().to_string()),
        );
        let chunk = &chunks.list[0];
        let schema = schema_with_id(214, "old_backup_cleanup", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        // Real content: archiving an empty segment over a non-empty backup is
        // refused by design, so the segment has to hold decodable entries for
        // the rewrite to be legitimate.
        let id = Id::allocated(24, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &id),
            data: data_map_value!(id: 13_i32, data: vec![0x4D_u8; DATA_SIZE]),
        };
        chunks.write_cell(&mut cell).expect("write a cell");

        let segment = chunk
            .segs
            .get(&(chunk.get_head_seg_id() as usize))
            .expect("head segment");
        let backup_file = chunk
            .file_manager
            .backup_path(segment.chunk_id, segment.id, segment.seq_id)
            .expect("backup path");

        chunk.rotate_heads_for_scrub_test();
        segment.archive().expect("first archive");
        assert!(
            Path::new(&backup_file).exists(),
            "first archive wrote nothing"
        );

        segment.set_dirty();
        segment.archive().expect("second archive");

        assert!(
            Path::new(&backup_file).exists(),
            "the rewrite lost the backup"
        );
        assert!(
            !Path::new(&format!("{}.old", backup_file)).exists(),
            "the superseded backup was left on disk"
        );
    }

    /// An archived segment is closed forever: it never becomes a write head
    /// again, and nothing may re-create its WAL.
    ///
    /// This is the fix for the twin-file corpse. Shutdown archives the OPEN
    /// append head (backup written, WAL deleted); the next incarnation used to
    /// resume that same segment, whose fresh WAL then held only the
    /// post-restart suffix. Backup and WAL at one seq id stopped being two
    /// versions of an image and became two halves of one, so discovery's
    /// "prefer the backup on a seq tie" silently dropped every write made
    /// after the archive -- ranged pages that were provably built and applied
    /// came back as `MissingPage`. Sealing makes the twin unrepresentable
    /// rather than making the seam recoverable (reconstructing it by content
    /// was tried, and a mis-detected seam trimmed the WAL to zero).
    #[test]
    fn an_archived_segment_is_never_appended_to_again() {
        let _ = env_logger::try_init();
        let backup_dir = TempDir::new().unwrap();
        let wal_dir = TempDir::new().unwrap();
        let chunks = Chunks::new(
            1,
            TEST_SEGMENT_SIZE * 8,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
        );
        let chunk = &chunks.list[0];
        let schema = schema_with_id(215, "seal_on_archive", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        let id = Id::allocated(25, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &id),
            data: data_map_value!(id: 13_i32, data: vec![0x5E_u8; DATA_SIZE]),
        };
        chunks.write_cell(&mut cell).expect("write a cell");

        let head_id = chunk.get_head_seg_id();
        let head = chunk.segs.get(&(head_id as usize)).expect("head segment");
        let wal_file = chunk
            .file_manager
            .wal_path(head.chunk_id, head.id, head.seq_id)
            .expect("wal path");

        // Shutdown archives the open head -- this is `archive_all`'s job, and
        // the head is not sealed by rotation because it never stopped being
        // the head.
        chunk.rotate_heads_for_scrub_test();
        head.archive().expect("archive the relinquished head");
        assert!(head.is_sealed(), "archiving must close the incarnation");
        assert!(
            !Path::new(&wal_file).exists(),
            "archive is supposed to delete the WAL it supersedes"
        );

        // What recovery does on the next start: pick a write head from the
        // recovered segments. The archived one must not be eligible, however
        // much room is left in it.
        assert!(
            head.append_header.load(Ordering::Acquire) < head.bound(),
            "the test needs a head with space left, or it proves nothing"
        );
        chunk
            .reset_write_heads_after_recovery()
            .expect("reset write heads");
        assert_ne!(
            chunk.get_head_seg_id(),
            head_id,
            "a segment with a backup was resumed as the write head; its next append \
             re-creates a WAL at a seq id that already has a complete backup"
        );

        // And the backstop, in case some other path ever selects it anyway.
        assert!(
            head.write_wal(head.addr, 8, true).is_err(),
            "a sealed segment must refuse to re-create its WAL"
        );
        assert!(
            !Path::new(&wal_file).exists(),
            "the refused append left a WAL twin next to the backup"
        );

        // Writing still works -- it lands in a fresh segment with a fresh seq
        // id, which is where the retired tail goes.
        let id2 = Id::allocated(25, 0, 2);
        let mut cell2 = OwnedCell {
            header: CellHeader::new(schema.vid, &id2),
            data: data_map_value!(id: 14_i32, data: vec![0x5F_u8; DATA_SIZE]),
        };
        chunks
            .write_cell(&mut cell2)
            .expect("writes continue after the head is retired");
        assert_ne!(
            chunk.get_head_seg_id(),
            head_id,
            "the write went back into the sealed segment"
        );
    }

    /// A torn backup file costs its own segment, not the store.
    ///
    /// TB15's OOM kill landed mid-archive and left an image whose index
    /// pointed past the end of the file ("block 214 extends past end of
    /// buffer"). Recovery read that before it ever reached the scan, so the
    /// quarantine did not cover it and the whole store failed to start.
    #[test]
    fn test_recovery_quarantines_a_torn_backup_and_keeps_the_rest() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // A real segment, so there is something to lose.
        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(213, "recovery_torn_backup", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let good_id = Id::allocated(23, 0, 1);
        let mut good_cell = OwnedCell {
            header: CellHeader::new(schema.vid, &good_id),
            data: data_map_value!(id: 11_i32, data: vec![0x2B_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut good_cell).unwrap();
        let mut good_segment = empty_segment_bytes();
        let good_entry = entry_bytes_at(writer_chunks.address_of(&good_id));
        good_segment[..good_entry.len()].copy_from_slice(&good_entry);

        // A compressed image truncated mid-write, as SIGKILL leaves one.
        let compressed = crate::ram::compression::compress_blocks_on_cells(
            &good_segment,
            &[0],
            good_entry.len(),
        )
        .expect("compress");
        let torn = &compressed[..compressed.len() / 2];

        write_backup_segment(&backup_dir, 0, 0, 1, torn);
        write_backup_segment(&backup_dir, 0, 1, 2, &good_segment);

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        let recovered = count_recovered_cells(&chunks.list);
        assert!(
            recovered > 0,
            "a torn backup abandoned the entire store: {} cells recovered",
            recovered
        );
    }

    /// A zero-byte backup -- the artifact of a SIGKILL between creating the
    /// backup file and writing its image -- must not shadow the complete WAL
    /// at the same seq id.
    ///
    /// Discovery prefers a backup over a WAL for the same (chunk, seg, seq),
    /// so the torn file used to win the dedup, scan as "no entries", and
    /// silently drop every cell whose newest version lived in that segment.
    /// The crash-churn fuzzer's corpse was exactly this: the ranged tree's
    /// metadata cell sat in the shadowed segment, recovery answered
    /// CellDoesNotExisted, and the placement layer wiped itself and
    /// re-established an empty genesis tree ("B-tree loaded with 0 keys").
    #[test]
    fn a_zero_byte_backup_does_not_shadow_a_complete_wal() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(213, "recovery_torn_backup_wal_twin", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let cell_id = Id::allocated(23, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &cell_id),
            data: data_map_value!(id: 11_i32, data: vec![0x77_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut cell).unwrap();
        let entry = entry_bytes_at(writer_chunks.address_of(&cell_id));

        // The complete WAL for (chunk 0, seg 0, seq 0), in the per-chunk
        // subdirectory the chunk's own file manager uses...
        write_wal_segment(&wal_dir, 0, 0, 0, &entry);
        // ...shadowed by the torn zero-byte backup at the same seq.
        write_backup_segment(&backup_dir, 0, 0, 0, &[]);

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        assert_eq!(
            count_recovered_cells(&chunks.list),
            1,
            "the zero-byte backup shadowed the complete WAL and lost its cells"
        );
    }

    /// An unreadable (non-empty but garbage) backup with a complete WAL twin
    /// at the same seq id must recover the segment from the WAL instead of
    /// quarantining it.
    ///
    /// The archive deletes a segment's WAL only after its backup is fully
    /// durable, so a backup that fails to load or scan implies the WAL is
    /// still whole -- falling back to it recovers every cell the torn file
    /// would have dropped.
    #[test]
    fn an_unreadable_backup_falls_back_to_its_wal_twin() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(214, "recovery_partial_backup_wal_twin", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let cell_id = Id::allocated(24, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &cell_id),
            data: data_map_value!(id: 12_i32, data: vec![0x66_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut cell).unwrap();
        let entry = entry_bytes_at(writer_chunks.address_of(&cell_id));

        write_wal_segment(&wal_dir, 0, 0, 0, &entry);
        // A partial backup: bytes that decode as neither a compressed image
        // nor a valid entry stream, as a kill mid-write leaves behind.
        write_backup_segment(&backup_dir, 0, 0, 0, &[0xFF_u8; 64]);

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        assert_eq!(
            count_recovered_cells(&chunks.list),
            1,
            "an unreadable backup must fall back to its complete WAL twin, not quarantine"
        );
    }

    /// A write that loses the exists race leaves its already-appended image
    /// behind as dead space -- and that image carries `old_version + 1`, the
    /// SAME version as the write that won. Recovery resolves each cell by
    /// max version with a later-scanned-wins tie-break, so the abandoned
    /// image used to shadow the winner. The crash-churn fuzzer hit this on
    /// every fresh start: the genesis `crate_tree` re-create lost its
    /// metadata write race, and every post-SIGKILL recovery then served the
    /// loser's image -- a head pointer to an orphan empty page, loaded as
    /// "B-tree loaded with 0 keys". Failed writes now zero their image's
    /// version, which must lose to any live version.
    #[test]
    fn a_failed_insert_image_cannot_shadow_the_winning_write() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(215, "recovery_failed_insert_ghost", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let cell_id = Id::allocated(25, 0, 1);
        let mut winner = OwnedCell {
            header: CellHeader::new(schema.vid, &cell_id),
            data: data_map_value!(id: 1_i32, data: vec![0x11_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut winner).unwrap();
        let winner_addr = writer_chunks.address_of(&cell_id);
        let winner_entry = entry_bytes_at(winner_addr);

        // A losing write that already appended its image before discovering
        // the collision. The public write path now refuses before allocating
        // (and heals indices instead), so the append-then-lose window only
        // remains for a genuine check/insert race -- reproduce its exact
        // mechanics here: append the full image, then abandon it as the race
        // arm does.
        let loser = OwnedCell {
            header: CellHeader::new(schema.vid, &cell_id),
            data: data_map_value!(id: 2_i32, data: vec![0x22_u8; DATA_SIZE]),
        };
        let ghost_addr = {
            let write_plan = loser.plan_write(writer_chunk).unwrap();
            let pending = write_plan.allocate(writer_chunk, true).unwrap();
            let result = writer_chunk
                .write_cell_to_chunk(&loser, &write_plan, &pending, loser.header.version)
                .unwrap();
            crate::ram::cell::abandon_entry(result.addr);
            pending.append().unwrap();
            result.addr
        };
        assert_eq!(
            ghost_addr,
            winner_addr + winner_entry.len(),
            "appends should be sequential in this idle chunk"
        );
        let (ghost_entry_header, _) = Entry::decode_from(ghost_addr, |_, _| {});
        assert_eq!(
            ghost_entry_header.entry_type,
            EntryType::PADDING,
            "a failed write must abandon its image as PADDING, so no walk reads a cell from it"
        );
        let ghost_entry = entry_bytes_at(ghost_addr);

        // Recovery over [winner][ghost] must keep the winner, whatever the
        // scan order tie-break would have said.
        let mut image = empty_segment_bytes();
        image[..winner_entry.len()].copy_from_slice(&winner_entry);
        image[winner_entry.len()..winner_entry.len() + ghost_entry.len()]
            .copy_from_slice(&ghost_entry);
        write_backup_segment(&backup_dir, 0, 0, 0, &image);

        let reader_schemas = setup_test_schema();
        reader_schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: reader_schemas,
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        assert_eq!(count_recovered_cells(&chunks.list), 1);
        let recovered = chunks.read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(
            recovered.data, winner.data,
            "recovery resurrected the failed write's image over the winner"
        );
    }

    /// Build a bracket entry (BEGIN or COMMIT) as bytes, the way the writer
    /// publishes one.
    fn bracket_entry_bytes(entry_type: EntryType, content: &[u8]) -> Vec<u8> {
        let mut buffer = vec![0u8; ENTRY_HEAD_SIZE + content.len()];
        let base = buffer.as_mut_ptr() as usize;
        Entry::encode_to(
            base,
            entry_type,
            content.len() as u32,
            |content_addr| unsafe {
                std::ptr::copy_nonoverlapping(
                    content.as_ptr(),
                    content_addr as *mut u8,
                    content.len(),
                );
            },
        );
        buffer
    }

    /// A committed bracket applies; an uncommitted one leaves NOTHING; and
    /// ordinary entries on either side are untouched by both.
    ///
    /// The third clause is the one that needs saying: a bracket sits in a
    /// shared segment, so entries before and after it belong to other
    /// writers and apply unconditionally. A recovery that dropped them along
    /// with an uncommitted transaction would trade a transaction's atomicity
    /// for everyone else's durability.
    #[test]
    fn a_bracket_applies_only_when_its_commit_is_present() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let schema = schema_with_id(219, "recovery_bracket", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        // One cell per role, each written so we have real entry bytes.
        let ids: Vec<Id> = (0..4).map(|i| Id::allocated(29, 0, i + 1)).collect();
        let mut entries = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: i as i32, data: vec![0x66_u8; DATA_SIZE]),
            };
            chunks.write_cell(&mut cell).unwrap();
            entries.push(entry_bytes_at(chunks.address_of(id)));
        }

        let txn = crate::server::transactions::test_hlc(4242, 7);
        let begin = bracket_entry_bytes(EntryType::BEGIN, &crate::ram::bracket::encode_begin(&txn, &[]));
        let manifest = vec![crate::ram::bracket::ManifestEntry {
            chunk_id: 0,
            seq_id: 0,
        }];
        let commit = bracket_entry_bytes(
            EntryType::COMMIT,
            &crate::ram::bracket::encode_commit(&txn, &manifest),
        );

        // Committed:   [ord 0][BEGIN][txn 1][txn 2][COMMIT][ord 3]
        // Uncommitted: [ord 0][ord 3][BEGIN][txn 1][txn 2]
        //
        // The uncommitted shape puts the bracket LAST on purpose, because
        // that is the only place a crash can leave one: the transaction holds
        // the head lease until its decision, so no other writer can append
        // behind its entries. An ordinary entry after an unclosed bracket is
        // not a state the writer can produce, and asserting about it would be
        // asserting about fiction.
        let build = |with_commit: bool| {
            let mut image = empty_segment_bytes();
            let mut at = 0usize;
            let mut put = |bytes: &[u8], at: &mut usize| {
                image[*at..*at + bytes.len()].copy_from_slice(bytes);
                *at += bytes.len();
            };
            put(&entries[0], &mut at);
            if !with_commit {
                put(&entries[3], &mut at);
            }
            put(&begin, &mut at);
            put(&entries[1], &mut at);
            put(&entries[2], &mut at);
            if with_commit {
                put(&commit, &mut at);
                put(&entries[3], &mut at);
            }
            (image, at)
        };

        for with_commit in [true, false] {
            let (image, used) = build(with_commit);
            let scanned = scan_segment_for_recovery_test(chunk, &image)
                .expect("a bracket must not fail the scan");
            assert_eq!(
                scanned.append_offset, used,
                "the walk must reach the end of the image whether or not the bracket committed"
            );

            let wal_dir = TempDir::new().unwrap();
            let backup_dir = TempDir::new().unwrap();
            let (_raft_dir, raft_path) = temp_raft_dir();
            write_backup_segment(&backup_dir, 0, 0, 0, &image);
            let reader_schemas = setup_test_schema();
            reader_schemas.debug_only_new_schema(schema.clone());
            let recovered = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta {
                    schemas: reader_schemas,
                }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path),
            );

            // The transaction's own cells: all or nothing.
            for id in [&ids[1], &ids[2]] {
                assert_eq!(
                    recovered.read_cell(id).is_ok(),
                    with_commit,
                    "cell {:?} inside a bracket must be present iff the COMMIT is (commit={})",
                    id,
                    with_commit
                );
            }
            // Everyone else's, either way.
            for id in [&ids[0], &ids[3]] {
                assert!(
                    recovered.read_cell(id).is_ok(),
                    "cell {:?} outside the bracket must survive regardless (commit={})",
                    id,
                    with_commit
                );
            }
        }
    }

    /// A chain part's link bounds its live region, and the zeros before the
    /// link are expected rather than damage.
    ///
    /// Without the fixed-tail read, the walk meets those zeros mid-image,
    /// finds non-zero bytes after them (the link itself), and reports the
    /// segment unscannable -- which would take out every entry in the part.
    #[test]
    fn a_chain_part_is_bounded_by_its_link_not_by_zeros() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let schema = schema_with_id(220, "recovery_chain_part", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        let txn = crate::server::transactions::test_hlc(909, 3);
        let begin = bracket_entry_bytes(EntryType::BEGIN, &crate::ram::bracket::encode_begin(&txn, &[]));
        let id = Id::allocated(30, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &id),
            data: data_map_value!(id: 1_i32, data: vec![0x77_u8; DATA_SIZE]),
        };
        chunks.write_cell(&mut cell).unwrap();
        let cell_entry = entry_bytes_at(chunks.address_of(&id));

        // [BEGIN][cell][zeros...................][TXN_CONT]
        let mut image = empty_segment_bytes();
        let mut at = 0usize;
        image[at..at + begin.len()].copy_from_slice(&begin);
        at += begin.len();
        image[at..at + cell_entry.len()].copy_from_slice(&cell_entry);
        at += cell_entry.len();
        let link_content = crate::ram::bracket::encode_txn_cont(&txn, 41);
        let link = bracket_entry_bytes(EntryType::TXN_CONT, &link_content);
        let tail = {
            let raw = image.len() - crate::ram::bracket::TXN_CONT_ENTRY_SIZE;
            raw - (raw % ENTRY_HEAD_SIZE)
        };
        assert!(tail > at, "the fixture must leave a gap before the link");
        image[tail..tail + link.len()].copy_from_slice(&link);

        let scanned = scan_segment_for_recovery_test(chunk, &image)
            .expect("a chain part must scan, zeros and all");
        assert_eq!(
            scanned.chain_link,
            Some((crate::ram::bracket::short_txn_id(&txn), 41)),
            "the fixed tail must identify this segment as a chain part without scanning for it"
        );
        assert_eq!(
            scanned.append_offset, at,
            "the walk must stop at the last real entry, not run into the zeros"
        );

        // And the part alone commits nothing: no COMMIT lives here.
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();
        write_backup_segment(&backup_dir, 0, 0, 0, &image);
        let reader_schemas = setup_test_schema();
        reader_schemas.debug_only_new_schema(schema.clone());
        let recovered = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: reader_schemas,
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );
        assert!(
            recovered.read_cell(&id).is_err(),
            "a chain part whose transaction never committed must contribute nothing"
        );
    }

    /// An abandoned image must not take the rest of its segment with it.
    ///
    /// THE bug behind "the store came back with 45% of its keys": a failed
    /// write abandoned its appended image by zeroing the cell version in
    /// place, which left the entry's published content checksum describing
    /// bytes that no longer existed. Recovery verifies that checksum, and a
    /// mismatch means "damaged from here" -- so the walk stopped AT the
    /// abandoned entry and every cell appended after it in that segment was
    /// silently dropped. In the fuzzer's corpse the casualty was a ranged
    /// tree's metadata cell, 66 KB further into the segment: the placement
    /// map named a tree whose metadata no longer existed, every seek
    /// retried, and the whole index refused to load.
    ///
    /// The winner-then-ghost ordering the older test uses cannot see this --
    /// with nothing after the ghost, truncating at it costs nothing. The
    /// cells AFTER the ghost are the entire point.
    #[test]
    fn an_abandoned_image_does_not_truncate_its_segment() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(216, "recovery_ghost_truncation", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());

        // [ghost][survivor 0][survivor 1][survivor 2] in one segment.
        let ghost_id = Id::allocated(26, 0, 1);
        let ghost = OwnedCell {
            header: CellHeader::new(schema.vid, &ghost_id),
            data: data_map_value!(id: 9_i32, data: vec![0x99_u8; DATA_SIZE]),
        };
        let ghost_addr = {
            let write_plan = ghost.plan_write(writer_chunk).unwrap();
            let pending = write_plan.allocate(writer_chunk, true).unwrap();
            let result = writer_chunk
                .write_cell_to_chunk(&ghost, &write_plan, &pending, ghost.header.version)
                .unwrap();
            crate::ram::cell::abandon_entry(result.addr);
            pending.append().unwrap();
            result.addr
        };
        let ghost_entry = entry_bytes_at(ghost_addr);

        let survivor_ids: Vec<Id> = (0..3).map(|i| Id::allocated(26, 1, i + 1)).collect();
        let mut survivor_entries = Vec::new();
        for (i, id) in survivor_ids.iter().enumerate() {
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: i as i32, data: vec![0x33_u8; DATA_SIZE]),
            };
            writer_chunks.write_cell(&mut cell).unwrap();
            survivor_entries.push(entry_bytes_at(writer_chunks.address_of(id)));
        }

        let mut image = empty_segment_bytes();
        let mut at = 0usize;
        image[at..at + ghost_entry.len()].copy_from_slice(&ghost_entry);
        at += ghost_entry.len();
        for entry in &survivor_entries {
            image[at..at + entry.len()].copy_from_slice(entry);
            at += entry.len();
        }
        write_backup_segment(&backup_dir, 0, 0, 0, &image);

        let reader_schemas = setup_test_schema();
        reader_schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: reader_schemas,
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        for id in &survivor_ids {
            assert!(
                chunks.read_cell(id).is_ok(),
                "cell {:?}, appended AFTER an abandoned image, must survive recovery",
                id
            );
        }
        assert_eq!(
            count_recovered_cells(&chunks.list),
            survivor_ids.len(),
            "exactly the survivors, and never the abandoned image, may be recovered"
        );
    }

    /// A corrupted entry costs itself, not the segment behind it.
    ///
    /// Damage in the MIDDLE of a segment used to end the walk there, so a
    /// single bad entry took every entry after it. That is the difference
    /// between losing one cell and losing a ranged tree's metadata cell 66 KB
    /// downstream, which is what made a whole index unloadable. The scan now
    /// steps over damage whose successor vouches for itself.
    #[test]
    fn damage_in_the_middle_costs_one_entry_not_the_tail() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let schema = schema_with_id(217, "recovery_midsegment_damage", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        let ids: Vec<Id> = (0..4).map(|i| Id::allocated(27, 0, i + 1)).collect();
        let mut entries = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: i as i32, data: vec![0x44_u8; DATA_SIZE]),
            };
            chunks.write_cell(&mut cell).unwrap();
            entries.push(entry_bytes_at(chunks.address_of(id)));
        }

        let mut image = empty_segment_bytes();
        let mut offsets = Vec::new();
        let mut at = 0usize;
        for entry in &entries {
            offsets.push(at);
            image[at..at + entry.len()].copy_from_slice(entry);
            at += entry.len();
        }

        // Corrupt entry 1's CONTENT, leaving its header (and therefore its
        // length) intact -- exactly the shape an in-place content mutation
        // leaves behind.
        let victim = offsets[1] + ENTRY_HEAD_SIZE + 4;
        image[victim] ^= 0xFF;

        let scanned = scan_segment_for_recovery_test(chunk, &image)
            .expect("a damaged entry mid-segment must not fail the scan");
        assert_eq!(
            scanned.append_offset, at,
            "the walk must reach the end of the image, not stop at the damage"
        );

        // And end to end: every cell except the damaged one comes back.
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();
        write_backup_segment(&backup_dir, 0, 0, 0, &image);
        let reader_schemas = setup_test_schema();
        reader_schemas.debug_only_new_schema(schema.clone());
        let recovered = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: reader_schemas,
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );
        for (i, id) in ids.iter().enumerate() {
            if i == 1 {
                continue;
            }
            assert!(
                recovered.read_cell(id).is_ok(),
                "cell {} of 4 must survive damage to cell 1",
                i
            );
        }
    }

    /// Resync is not a licence to guess: a tail that cannot vouch for itself
    /// still ends the walk, and everything before it is still kept.
    #[test]
    fn damage_with_an_unreadable_successor_still_stops_the_walk() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let schema = schema_with_id(218, "recovery_unreadable_tail", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        let ids: Vec<Id> = (0..3).map(|i| Id::allocated(28, 0, i + 1)).collect();
        let mut entries = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: i as i32, data: vec![0x55_u8; DATA_SIZE]),
            };
            chunks.write_cell(&mut cell).unwrap();
            entries.push(entry_bytes_at(chunks.address_of(id)));
        }

        let mut image = empty_segment_bytes();
        let mut offsets = Vec::new();
        let mut at = 0usize;
        for entry in &entries {
            offsets.push(at);
            image[at..at + entry.len()].copy_from_slice(entry);
            at += entry.len();
        }

        // Damage entry 1 AND shred everything after it: nothing downstream
        // can anchor a resync.
        image[offsets[1] + ENTRY_HEAD_SIZE + 4] ^= 0xFF;
        for byte in image[offsets[2]..at].iter_mut() {
            *byte = 0xAB;
        }

        let scanned = scan_segment_for_recovery_test(chunk, &image)
            .expect("entries before the damage are still readable");
        assert_eq!(
            scanned.append_offset, offsets[1],
            "with no trustworthy successor the walk must stop at the damage"
        );
    }

    /// The image that failed TB14 is no longer ambiguous.
    ///
    /// A segment whose cursor reads empty but whose image holds bytes used to
    /// be unscannable: the walk found no entry at offset 0, saw non-zero bytes
    /// after it, and had no way to tell "empty segment with junk in untouched
    /// memory" from "damaged segment". It failed, and the failure took the
    /// whole store with it.
    ///
    /// With the cursor recorded in the image there is nothing to decide: zero
    /// live bytes means nothing is read, whatever the rest of the image holds.
    #[test]
    fn a_declared_empty_image_is_recovered_regardless_of_its_content() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];

        let mut image = empty_segment_bytes();
        image[ENTRY_HEAD_SIZE + 4] = 0xAA;

        // Inferring the extent: unscannable, and fatal.
        let inferred = scan_segment_for_recovery_test(chunk, &image);
        assert!(
            inferred.is_err(),
            "without a recorded cursor this image is indistinguishable from damage"
        );

        // Declared empty: read nothing, recover the segment as empty.
        let declared = scan_segment_with_used_len_for_test(chunk, &image, 0)
            .expect("an image that declares no live bytes must recover as an empty segment");
        assert_eq!(declared.segment_class, SegmentClass::Regular);
    }

    /// A recorded extent also stops the walk from wandering into untouched
    /// memory past the live bytes.
    #[test]
    fn a_declared_extent_stops_the_scan_at_the_live_bytes() {
        let _ = env_logger::try_init();
        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(212, "recovery_declared_extent", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let cell_id = Id::allocated(22, 0, 1);
        let mut cell = OwnedCell {
            header: CellHeader::new(schema.vid, &cell_id),
            data: data_map_value!(id: 9_i32, data: vec![0x33_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut cell).unwrap();
        let entry = entry_bytes_at(writer_chunks.address_of(&cell_id));

        // One real entry, then garbage in memory the segment never used.
        let mut image = empty_segment_bytes();
        image[..entry.len()].copy_from_slice(&entry);
        image[entry.len() + 32] = 0xC3;

        let reader_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let reader_chunk = &reader_chunks.list[0];
        let scan = scan_segment_with_used_len_for_test(reader_chunk, &image, entry.len())
            .expect("the live prefix must recover with the garbage past it ignored");
        assert_eq!(scan.segment_class, SegmentClass::Regular);
    }

    #[test]
    fn test_recovery_quarantines_an_unscannable_segment_and_keeps_the_rest() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // A real cell, written so we have genuine segment bytes to recover.
        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(211, "recovery_quarantine_regular", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());
        let good_id = Id::allocated(21, 0, 1);
        let mut good_cell = OwnedCell {
            header: CellHeader::new(schema.vid, &good_id),
            data: data_map_value!(id: 7_i32, data: vec![0x5A_u8; DATA_SIZE]),
        };
        writer_chunks.write_cell(&mut good_cell).unwrap();
        let mut good_segment = empty_segment_bytes();
        let good_entry = entry_bytes_at(writer_chunks.address_of(&good_id));
        good_segment[..good_entry.len()].copy_from_slice(&good_entry);

        // A segment that claims to be empty yet holds non-zero bytes: exactly
        // the "non-zero bytes after padding at offset 0" that aborted TB14.
        let mut bad_segment = empty_segment_bytes();
        bad_segment[ENTRY_HEAD_SIZE + 4] = 0xAA;

        // The bad one first, so it aborts before the good one is ever read if
        // the failure is still fatal.
        write_backup_segment(&backup_dir, 0, 0, 1, &bad_segment);
        write_backup_segment(&backup_dir, 0, 1, 2, &good_segment);

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        // The store must still hold what was readable.
        let recovered = count_recovered_cells(&chunks.list);
        assert!(
            recovered > 0,
            "an unscannable segment abandoned the entire store: {} cells recovered",
            recovered
        );
    }

    #[test]
    fn test_recovery_allows_tombstone_only_segment() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        write_backup_segment(&backup_dir, 0, 0, 1, &tombstone_only_segment_bytes());

        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        let chunk = &chunks.list[0];
        let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
        let segment = chunk
            .segments()
            .into_iter()
            .next()
            .expect("recovery should keep a tombstone-only regular segment resident");

        // Resident but sealed, as for any backup-recovered segment.
        assert!(segment.is_sealed());
        assert_ne!(regular_head, Some(segment.id));
        assert_eq!(blob_head, None);
        assert_eq!(segment.segment_class(), SegmentClass::Regular);
        assert_eq!(segment.tombstones.load(Ordering::Acquire), 1);
        assert_eq!(
            segment.append_header.load(Ordering::Acquire),
            segment.addr + crate::ram::tombstone::TOMBSTONE_ENTRY_SIZE
        );
        assert_eq!(total_hot_segments(&chunks), 1);
        assert_eq!(total_cold_segments(&chunks), 0);
    }

    #[test]
    fn test_recovery_reads_cells_after_late_schema_registration() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let regular_schema = schema_with_id(113, "recovery_late_schema_registration", false);
        let cell_id = Id::allocated(13, 0, 7);

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(regular_schema.clone());
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );

            let mut cell = OwnedCell {
                header: CellHeader::new(regular_schema.vid, &cell_id),
                data: data_map_value!(id: 7_i32, data: vec![0x5A_u8; DATA_SIZE]),
            };
            chunks.write_cell(&mut cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        {
            let schemas = LocalSchemasCache::new_local("");
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            assert_eq!(chunks.list[0].cell_count(), 1);

            chunks.list[0]
                .meta
                .schemas
                .debug_only_new_schema(regular_schema.clone());

            let recovered_cell = chunks.read_cell(&cell_id).unwrap();
            assert_eq!(*recovered_cell.data["id"].i32().unwrap(), 7);
        }
    }

    #[test]
    fn test_recovery_keeps_blob_segments_cold() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let regular_schema = schema_with_id(110, "recovery_regular_lane", false);
        let blob_schema = schema_with_id(111, "recovery_blob_lane", true);
        let regular_id = Id::allocated(11, 0, 1);
        let blob_id = Id::allocated(11, 0, 2);

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(regular_schema.clone());
            schemas.debug_only_new_schema(blob_schema.clone());
            let manager = tiered_manager_for_test(8 * SEGMENT_SIZE);
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(manager),
                false,
                Some(raft_path.clone()),
            );

            let mut regular_cell = OwnedCell {
                header: CellHeader::new(regular_schema.vid, &regular_id),
                data: data_map_value!(id: 1_i32, data: vec![0x33_u8; DATA_SIZE]),
            };
            let mut blob_cell = OwnedCell {
                header: CellHeader::new(blob_schema.vid, &blob_id),
                data: data_map_value!(id: 2_i32, data: vec![0x44_u8; DATA_SIZE]),
            };

            chunks.write_cell(&mut regular_cell).unwrap();
            chunks.write_cell(&mut blob_cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(regular_schema.clone());
            schemas.debug_only_new_schema(blob_schema.clone());
            let manager = tiered_manager_for_test(8 * SEGMENT_SIZE);
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(manager),
                true,
                Some(raft_path.clone()),
            );

            let chunk = &chunks.list[0];
            let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
            let regular_segments: Vec<_> = chunk
                .segments()
                .into_iter()
                .filter(|seg| seg.segment_class() == SegmentClass::Regular)
                .collect();
            let blob_segments: Vec<_> = chunk
                .segments()
                .into_iter()
                .filter(|seg| seg.segment_class() == SegmentClass::Blob)
                .collect();

            assert_eq!(blob_head, None, "recovery should leave the blob head empty");
            assert_eq!(
                blob_segments.len(),
                1,
                "setup should recover exactly one blob segment"
            );
            assert_eq!(
                regular_segments.len(),
                1,
                "recovery should keep the recovered regular segment resident"
            );
            assert!(
                blob_segments[0].is_cold(),
                "recovered blob segments must come back cold"
            );
            // The recovered regular segment was archived by the setup above,
            // so it comes back sealed and cannot serve as the write head --
            // appending to it would put a WAL next to its backup at one seq
            // id, which is the twin recovery cannot arbitrate. The first write
            // after recovery allocates a fresh segment instead.
            assert!(
                regular_segments[0].is_sealed(),
                "a segment recovered from a backup must come back sealed"
            );
            assert_ne!(
                regular_head,
                Some(regular_segments[0].id),
                "an archived segment was resumed as the write head"
            );
            // The recovered segment keeps its append position -- its cells are
            // there and readable, which is what the cursor is for. It is simply
            // no longer a place new entries may go.
            assert!(
                regular_segments[0].append_header.load(Ordering::Relaxed)
                    > regular_segments[0].addr,
                "the recovered regular segment lost its append position"
            );

            let blob_cell = chunks.read_cell(&blob_id).unwrap();
            assert_eq!(*blob_cell.data["id"].i32().unwrap(), 2);
        }
    }

    /// H1 (Morpheus docs/EDGE_INTENT_LOG.md): a head returned to the pool with
    /// its BEGIN still open.
    ///
    /// `release_transaction_leases` is what the lease-expiry sweeper calls, and
    /// it hands the head back WITHOUT a COMMIT and WITHOUT a rewind. The next
    /// ordinary writer then appends behind the open BEGIN in the same segment,
    /// is acknowledged, and the recovery state machine buffers that entry as
    /// part of the bracket and discards it. This test writes exactly that
    /// sequence through the live write path and recovers it from the WAL.
    ///
    /// `closed` is the control: the same sequence with a proper
    /// `commit_transaction_brackets` keeps everything.
    fn run_open_bracket_then_ordinary_write(how: &str) -> (bool, bool, bool) {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();
        let schema = schema_with_id(431, "recovery_open_bracket", false);
        let before_id = Id::allocated(31, 0, 1);
        let txn_id_cell = Id::allocated(31, 0, 2);
        let after_id = Id::allocated(31, 0, 3);
        let txn = crate::server::transactions::test_hlc(8181, 9);
        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(schema.clone());
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );
            let mk = |id: &Id, n: i32| OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: n, data: vec![0x33_u8; 64]),
            };
            chunks.write_cell(&mut mk(&before_id, 1)).unwrap();

            {
                let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                chunks.write_cell(&mut mk(&txn_id_cell, 2)).unwrap();
            }
            let range = chunks.address_range();
            match how {
                "closed" => {
                    chunks.commit_transaction_brackets(&txn, false).unwrap();
                    crate::ram::chunk::release_transaction_leases(&txn, &range).unwrap();
                }
                "release" => {
                    // The class-closing invariant: handing back a head whose
                    // BEGIN is still open is REFUSED, and the lease is kept.
                    let released = crate::ram::chunk::release_transaction_leases(&txn, &range);
                    assert!(
                        matches!(
                            released,
                            Err(crate::ram::chunk::LeaseExitError::OpenBracket(_))
                        ),
                        "releasing an open bracket must be refused, got {released:?}"
                    );
                    assert_eq!(crate::ram::chunk::transaction_lease_count(&txn, &range), 1);
                }
                "rewind" => {
                    // The abort's physical half. (The participant rolls the
                    // index back first; this test only reads the log.)
                    assert_eq!(
                        crate::ram::chunk::rewind_transaction_writes(&txn, &range),
                        Ok(1)
                    );
                    // This variant checks reuse after durability, not the
                    // new deferred-release window (covered separately).
                    chunks.sync_all();
                }
                "expire" => {
                    let expired = crate::ram::chunk::expire_transaction_leases(
                        std::time::Duration::ZERO,
                        &range,
                        &Default::default(),
                    );
                    assert_eq!(expired, vec![txn.clone()]);
                }
                other => panic!("unknown variant {other}"),
            }
            drop(crate::ram::chunk::take_transactional_segments());

            // The ordinary, ACKNOWLEDGED write that follows.
            chunks.write_cell(&mut mk(&after_id, 3)).unwrap();
            let seg_of = |id: &Id| {
                chunks.list[0]
                    .locate_segment(chunks.address_of(id))
                    .expect("segment")
                    .id
            };
            match how {
                // A closed or disowned bracket gives its head back, and the
                // ordinary write lands BEHIND it in the same segment -- the
                // exact layout that lost data while the bracket stayed open.
                "closed" | "rewind" => assert_eq!(
                    seg_of(&txn_id_cell),
                    seg_of(&after_id),
                    "the ordinary write must land in the segment holding the bracket"
                ),
                // An open one never reaches another writer: refused, or
                // retired (full) by the expiry sweeper.
                _ => assert_ne!(
                    seg_of(&txn_id_cell),
                    seg_of(&after_id),
                    "an ordinary write must never land behind an open BEGIN"
                ),
            }
            chunks.sync_all();
        }
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );
        let before = chunks.read_cell(&before_id).is_ok();
        let in_txn = chunks.read_cell(&txn_id_cell).is_ok();
        let after = chunks.read_cell(&after_id).is_ok();
        (before, in_txn, after)
    }

    #[test]
    fn h1_control_a_closed_bracket_keeps_the_ordinary_write_behind_it() {
        let (before, txn_cell, after) = run_open_bracket_then_ordinary_write("closed");
        eprintln!("H1 closed: before={before} txn={txn_cell} after={after}");
        assert!(before && txn_cell && after);
    }

    #[test]
    fn h1_ordinary_write_behind_a_released_open_bracket_survives_recovery() {
        let (before, txn_cell, after) = run_open_bracket_then_ordinary_write("release");
        eprintln!("H1 release: before={before} txn={txn_cell} after={after}");
        assert!(before);
        assert!(
            !txn_cell,
            "the undecided transaction's cell must not recover"
        );
        assert!(
            after,
            "H1: an ACKNOWLEDGED ordinary write behind an open BEGIN was dropped by recovery"
        );
    }

    #[test]
    fn h1_ordinary_write_behind_a_rewound_bracket_survives_recovery() {
        let (before, txn_cell, after) = run_open_bracket_then_ordinary_write("rewind");
        eprintln!("H1 rewind: before={before} txn={txn_cell} after={after}");
        assert!(before);
        assert!(!txn_cell, "the aborted transaction's cell must not recover");
        assert!(
            after,
            "H1: an ACKNOWLEDGED ordinary write behind a rewound bracket was dropped by recovery"
        );
    }

    #[test]
    fn h1_ordinary_write_behind_an_expired_open_bracket_survives_recovery() {
        let (before, txn_cell, after) = run_open_bracket_then_ordinary_write("expire");
        eprintln!("H1 expire: before={before} txn={txn_cell} after={after}");
        assert!(before);
        assert!(!txn_cell);
        assert!(
            after,
            "H1: an ACKNOWLEDGED ordinary write behind an expired bracket was dropped by recovery"
        );
    }

    /// H1 variants beyond the lease-release one. `how`:
    ///  - "panic":    the writer panics between BEGIN and COMMIT. `set_transaction_id`
    ///                is a thread-local that is not reset on unwind, so the NEXT
    ///                ordinary write on that thread is leased to the dead
    ///                transaction and recovered as a member of its bracket.
    ///  - "no_room":  the head cannot hold the COMMIT. `close_transaction_bracket`
    ///                only warns, `commit_transaction_brackets` returns Ok, the
    ///                caller is told the transaction committed, and the bracket
    ///                has no COMMIT at all.
    /// Returns (txn cell present, ordinary-after present).
    fn run_unowned_begin_variant(how: &str) -> (bool, bool) {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();
        let schema = schema_with_id(432, "recovery_unowned_begin", false);
        let txn_cell = Id::allocated(32, 0, 1);
        let after = Id::allocated(32, 0, 2);
        let txn = crate::server::transactions::test_hlc(8282, 9);
        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(schema.clone());
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );
            let mk = |id: &Id, n: i32| OwnedCell {
                header: CellHeader::new(schema.vid, id),
                data: data_map_value!(id: n, data: vec![0x44_u8; 64]),
            };
            let range = chunks.address_range();
            match how {
                "panic" => {
                    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                        chunks.write_cell(&mut mk(&txn_cell, 1)).unwrap();
                        panic!("writer died between BEGIN and COMMIT");
                    }));
                    assert!(r.is_err());
                    // The unwind ended the scope: this thread names nothing.
                    assert!(
                        !crate::ram::chunk::is_in_transaction(),
                        "an unwind must not leave the thread naming the dead transaction"
                    );
                    assert!(
                        crate::ram::chunk::transaction_has_leases(&txn, &range),
                        "the lease should still be held"
                    );
                    // An ORDINARY write follows on the same thread.
                    chunks.write_cell(&mut mk(&after, 2)).unwrap();
                    assert_eq!(
                        crate::ram::chunk::transaction_lease_count(&txn, &range),
                        1,
                        "the ordinary write must not join the dead transaction's lease"
                    );
                    assert_ne!(
                        chunks.list[0]
                            .locate_segment(chunks.address_of(&after))
                            .map(|s| s.id),
                        chunks.list[0]
                            .locate_segment(chunks.address_of(&txn_cell))
                            .map(|s| s.id),
                        "nor land behind its BEGIN"
                    );
                    // A panicked transaction is an aborted one; its decider
                    // rewinds it (the participant does this in `apply`).
                    assert_eq!(
                        crate::ram::chunk::rewind_transaction_writes(&txn, &range),
                        Ok(1)
                    );
                }
                "no_room" => {
                    let scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                    chunks.write_cell(&mut mk(&txn_cell, 1)).unwrap();
                    // Use up the head so the COMMIT cannot fit behind the entry.
                    let chunk = &chunks.list[0];
                    let seg = chunk
                        .locate_segment(chunks.address_of(&txn_cell))
                        .expect("segment");
                    let remaining = seg
                        .bound()
                        .saturating_sub(seg.append_header.load(Ordering::Acquire));
                    let filler = (remaining - 16) & !7;
                    let pending = chunk.try_acquire(filler as u32, false).expect("filler");
                    crate::ram::entry::stamp_reservation_padding(pending.addr, filler as u32);
                    pending.append().unwrap();
                    drop(pending);
                    drop(scope);
                    // The caller is told this succeeded.
                    let committed = chunks.commit_transaction_brackets(&txn, false);
                    eprintln!("H1 no_room: commit_transaction_brackets -> {:?}", committed);
                    assert!(committed.is_ok());
                    drop(crate::ram::chunk::take_transactional_segments());
                    crate::ram::chunk::release_transaction_leases(&txn, &range).unwrap();
                    chunks.write_cell(&mut mk(&after, 2)).unwrap();
                }
                other => panic!("unknown variant {other}"),
            }
            drop(crate::ram::chunk::take_transactional_segments());
            chunks.sync_all();
        }
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );
        let a = chunks.read_cell(&txn_cell).is_ok();
        let b = chunks.read_cell(&after).is_ok();
        (a, b)
    }

    #[test]
    fn h1_panic_between_begin_and_commit_must_not_swallow_the_next_ordinary_write() {
        let (txn_cell, after) = run_unowned_begin_variant("panic");
        eprintln!("H1 panic: txn={txn_cell} after={after}");
        assert!(
            !txn_cell,
            "an undecided transaction's cell must not recover"
        );
        assert!(
            after,
            "H1: an ACKNOWLEDGED ordinary write after a panicked transaction was dropped"
        );
    }

    #[test]
    fn h1_a_commit_that_finds_no_room_must_not_report_success() {
        let (txn_cell, after) = run_unowned_begin_variant("no_room");
        eprintln!("H1 no_room: txn={txn_cell} after={after}");
        assert!(
            txn_cell,
            "H1: commit_transaction_brackets returned Ok but the transaction did not survive recovery"
        );
        assert!(after);
    }

    /// A store with WAL and backups, sized for several segments, plus the
    /// helpers the bracket/cleaner tests share.
    struct BracketCleanerStore {
        wal_dir: TempDir,
        backup_dir: TempDir,
        _raft_dir: TempDir,
        raft_path: String,
        schema: Schema,
    }

    impl BracketCleanerStore {
        fn new(schema_id: u32, name: &str) -> Self {
            let (raft_dir, raft_path) = temp_raft_dir();
            BracketCleanerStore {
                wal_dir: TempDir::new().unwrap(),
                backup_dir: TempDir::new().unwrap(),
                _raft_dir: raft_dir,
                raft_path,
                schema: schema_with_id(schema_id, name, false),
            }
        }

        fn open(&self, recover: bool) -> Arc<Chunks> {
            self.open_sized(recover, TEST_SEGMENT_SIZE * 8)
        }

        fn open_sized(&self, recover: bool, size: usize) -> Arc<Chunks> {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(self.schema.clone());
            Chunks::new_with_recovery(
                1,
                size,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(self.backup_dir.path().to_str().unwrap().to_string()),
                Some(self.wal_dir.path().to_str().unwrap().to_string()),
                None,
                recover,
                Some(self.raft_path.clone()),
            )
        }

        fn cell(&self, id: &Id, payload: usize) -> OwnedCell {
            OwnedCell {
                header: CellHeader::new(self.schema.vid, id),
                data: data_map_value!(id: id.bits() as i32, data: vec![0x5a_u8; payload]),
            }
        }
    }

    fn segment_of(chunks: &Chunks, id: &Id) -> lightning::aarc::Arc<crate::ram::segs::Segment> {
        chunks.list[0]
            .locate_segment(chunks.address_of(id))
            .expect("segment of a written cell")
    }

    /// Take a head out of the pool the way rotation does -- slot emptied,
    /// then drained, synced, archived and sealed -- so the cleaner may pick it.
    fn rotate_out(chunk: &Chunk, seg_id: u64) {
        for slot in chunk.head_pool.iter().chain(chunk.blob_head_pool.iter()) {
            let _ = slot.compare_exchange(
                seg_id,
                u64::MAX,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
        }
        assert!(!chunk.is_active_head(seg_id));
        let segment = chunk.segs.get(&(seg_id as usize)).unwrap();
        // An undecided part deliberately keeps its deferred seal queued.
        if !segment.protection.transaction_pending() {
            chunk.wait_for_segment_jobs_for_test();
        }
        let seq_id = chunk.segs.get(&(seg_id as usize)).unwrap().seq_id;
        chunk.finish_rotated_head(seg_id, seq_id);
        let segment = chunk.segs.get(&(seg_id as usize)).unwrap();
        // An undecided part deliberately keeps its deferred seal queued.
        if !segment.protection.transaction_pending() {
            chunk.wait_for_segment_jobs_for_test();
        }
    }

    /// Fill and rotate the head with ORDINARY cells, so the cleaner has a
    /// second candidate that has nothing to do with the transaction.
    fn ordinary_rotated_segment(
        store: &BracketCleanerStore,
        chunks: &Chunks,
        first_id: u64,
    ) -> (Vec<Id>, u64) {
        let chunk = &chunks.list[0];
        let mut ids = Vec::new();
        let start = chunk.get_head_seg_id();
        let mut n = first_id;
        // One thread, one affinity slot: these fill the same head until it
        // rotates.
        let first = Id::allocated(34, 0, n);
        chunks
            .write_cell(&mut store.cell(&first, 512 * 1024))
            .unwrap();
        let seg = segment_of(chunks, &first).id;
        ids.push(first);
        while chunk.get_head_seg_id() == start || chunk.is_active_head(seg) {
            n += 1;
            let id = Id::allocated(34, 0, n);
            chunks.write_cell(&mut store.cell(&id, 512 * 1024)).unwrap();
            if segment_of(chunks, &id).id == seg {
                ids.push(id);
            }
            assert!(n - first_id < 64, "the ordinary head never rotated");
        }
        // Let the background sealer finish with it before anything compacts
        // it, or the two race over its files.
        let rotated = chunk.segments().into_iter().find(|s| s.id == seg).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !rotated.is_sealed() {
            assert!(
                std::time::Instant::now() < deadline,
                "the rotated head was never sealed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        // Mostly dead, so combining it with a small segment saves one: the
        // combiner refuses a round that would not reduce the segment count.
        let survivor = ids[0];
        for id in &ids[1..] {
            chunks.remove_cell(id).unwrap();
        }
        (vec![survivor], seg)
    }

    /// The cleaner must never drop a COMMIT that another part of the same
    /// transaction still needs.
    ///
    /// Parts of one transaction are decided together but compacted
    /// INDEPENDENTLY, and compaction drops a segment's bracket markers. The
    /// old writer sealed a part that filled to within 48 bytes of its end
    /// with a TXN_CONT chain link, which took the room its own COMMIT needed,
    /// so that part relied on a later part's record. Compacting the later
    /// part alone then left the earlier part's BEGIN closed by nothing, and
    /// its committed cells came back from the restart as an undecided
    /// transaction. Every part now keeps room for its own COMMIT.
    #[test]
    fn compacting_one_part_never_orphans_another_parts_committed_cells() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(433, "cleaner_bracket_parts");
        let txn = crate::server::transactions::test_hlc(8383, 9);
        let small = Id::allocated(33, 0, 1);
        let sized = Id::allocated(33, 0, 2);
        let spill = Id::allocated(33, 0, 3);
        let mut fillers = Vec::new();
        let ordinary;
        {
            let chunks = store.open(false);
            let chunk = &chunks.list[0];
            let (ids, ordinary_seg) = ordinary_rotated_segment(&store, &chunks, 100);
            ordinary = ids;

            let scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&small, 64)).unwrap();
            let part1 = segment_of(&chunks, &small);
            assert_ne!(part1.id, ordinary_seg);
            // Size the next cell to leave EXACTLY 40 bytes in the first part:
            // under the 48 at which the old writer chain-sealed a full part,
            // over the 32-byte minimal COMMIT.
            let entry_of = |id: &Id| {
                let addr = chunks.address_of(id);
                let (header, _) = Entry::decode_from(addr, |_, header| header);
                ENTRY_HEAD_SIZE + header.content_length as usize
            };
            let overhead = entry_of(&small) - 64;
            let remaining = |seg: &crate::ram::segs::Segment| {
                seg.bound() - seg.append_header.load(Ordering::Acquire)
            };
            // Cells have a size cap, so fill most of the part with several.
            while remaining(&part1) > 768 * 1024 {
                let id = Id::allocated(33, 0, 10 + fillers.len() as u64);
                chunks.write_cell(&mut store.cell(&id, 512 * 1024)).unwrap();
                assert_eq!(segment_of(&chunks, &id).id, part1.id);
                fillers.push(id);
            }
            let remaining = remaining(&part1);
            chunks
                .write_cell(&mut store.cell(&sized, remaining - 40 - overhead))
                .unwrap();
            assert_eq!(segment_of(&chunks, &sized).id, part1.id);
            assert_eq!(
                part1.bound() - part1.append_header.load(Ordering::Acquire),
                40,
                "test setup: the first part must be left 40 bytes short of full"
            );
            // Does not fit: the transaction continues in a second part.
            chunks.write_cell(&mut store.cell(&spill, 64)).unwrap();
            let part2 = segment_of(&chunks, &spill);
            assert_ne!(part2.id, part1.id, "test setup: a second part must open");
            drop(scope);

            chunks.commit_transaction_brackets(&txn, false).expect("commit");
            crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range())
                .expect("release after commit");

            // Compact the SECOND part only -- with an unrelated segment, as
            // the combiner wants two -- and leave the first one where it is.
            rotate_out(chunk, part2.id);
            let candidates: Vec<_> = chunk
                .segments()
                .into_iter()
                .filter(|seg| seg.id == part2.id || seg.id == ordinary_seg)
                .collect();
            assert_eq!(candidates.len(), 2);
            let (_, collected) =
                crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
            assert!(collected > 0, "test setup: the combine must actually run");
            assert!(
                chunk.segments().iter().all(|seg| seg.id != part2.id),
                "test setup: the second part must be gone"
            );
            assert!(
                chunk.segments().iter().any(|seg| seg.id == part1.id),
                "test setup: the first part must still be there"
            );
            for id in [&small, &sized, &spill].into_iter().chain(fillers.iter()) {
                assert!(chunks.read_cell(id).is_ok(), "{id:?} before restart");
            }
            chunks.sync_all();
        }
        let chunks = store.open(true);
        for id in [&small, &sized, &spill].into_iter().chain(fillers.iter()) {
            assert!(
                chunks.read_cell(id).is_ok(),
                "{id:?}: a committed transaction's cell was lost after one of its parts was \
                 compacted"
            );
        }
        for id in &ordinary {
            assert!(chunks.read_cell(id).is_ok(), "ordinary cell {id:?}");
        }
    }

    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
    }

    /// What is on disk right now, as a crash at this instant would leave it.
    struct DiskSnapshot {
        wal: TempDir,
        backup: TempDir,
        raft: TempDir,
    }

    impl BracketCleanerStore {
        fn snapshot(&self) -> DiskSnapshot {
            let snap = DiskSnapshot {
                wal: TempDir::new().unwrap(),
                backup: TempDir::new().unwrap(),
                raft: TempDir::new().unwrap(),
            };
            copy_dir(self.wal_dir.path(), snap.wal.path());
            copy_dir(self.backup_dir.path(), snap.backup.path());
            copy_dir(Path::new(&self.raft_path), snap.raft.path());
            snap
        }

        fn recover_snapshot(&self, snap: &DiskSnapshot) -> Arc<Chunks> {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(self.schema.clone());
            Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(snap.backup.path().to_str().unwrap().to_string()),
                Some(snap.wal.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(snap.raft.path().to_str().unwrap().to_string()),
            )
        }
    }

    /// A crash at every point of an abort's rewind recovers the same thing:
    /// the aborted transaction gone, everything else intact -- and the
    /// ordinary write that reuses the head after the rewind survives.
    ///
    /// Points: (0) entries journaled, no rewind yet (an open BEGIN); (1) the
    /// padding journaled and synced, head not yet reused; (2) another writer
    /// appended behind the padding in the same head.
    #[test]
    fn a_crash_at_any_point_of_a_rewind_recovers_the_abort() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(435, "rewind_crash_points");
        let txn = crate::server::transactions::test_hlc(8585, 9);
        let before = Id::allocated(36, 0, 1);
        let aborted = [Id::allocated(36, 0, 2), Id::allocated(36, 0, 3)];
        let after = Id::allocated(36, 0, 4);
        let chunks = store.open(false);
        chunks.write_cell(&mut store.cell(&before, 128)).unwrap();
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            for id in &aborted {
                chunks.write_cell(&mut store.cell(id, 128)).unwrap();
            }
        }
        let part = segment_of(&chunks, &aborted[0]);
        let point0 = store.snapshot();
        for id in &aborted {
            chunks.remove_cell(id).unwrap();
        }
        assert_eq!(
            crate::ram::chunk::rewind_transaction_writes(&txn, &chunks.address_range()),
            Ok(1)
        );
        chunks.sync_all();
        let point1 = store.snapshot();
        // Reuse: make the rewound head the only live head in the pool, so the
        // next ordinary write lands in it whatever this thread's affinity.
        let chunk = &chunks.list[0];
        assert!(
            chunk.is_active_head(part.id),
            "the rewound head stays in the pool"
        );
        for slot in chunk.head_pool.iter() {
            if slot.load(Ordering::Acquire) != part.id {
                slot.store(u64::MAX, Ordering::Release);
            }
        }
        let mut reused = None;
        for n in 0..64u64 {
            let id = Id::allocated(36, 1, n);
            chunks.write_cell(&mut store.cell(&id, 128)).unwrap();
            if segment_of(&chunks, &id).id == part.id {
                reused = Some(id);
                break;
            }
        }
        let reused = reused.unwrap_or_else(|| {
            panic!(
                "the rewound head must be reusable: can_append={} owned={} open={} active={}",
                part.can_still_append(),
                part.is_head_owned(),
                part.bracket_is_open(),
                chunk.is_active_head(part.id)
            )
        });
        chunks.write_cell(&mut store.cell(&after, 128)).unwrap();
        chunks.sync_all();
        let point2 = store.snapshot();
        drop(chunks);

        for (name, snap, reuse_expected) in [
            ("0", &point0, false),
            ("1", &point1, false),
            ("2", &point2, true),
        ] {
            let recovered = store.recover_snapshot(snap);
            assert!(recovered.read_cell(&before).is_ok(), "point {name}: before");
            for id in &aborted {
                assert!(
                    recovered.read_cell(id).is_err(),
                    "point {name}: an aborted cell came back"
                );
            }
            assert_eq!(
                recovered.read_cell(&reused).is_ok(),
                reuse_expected,
                "point {name}: the write that reused the head"
            );
        }
    }

    #[test]
    fn sequential_200_durable_aborts_reuse_heads_in_a_16_mb_chunk() {
        let store = BracketCleanerStore::new(439, "durable_abort_reuse");
        let chunks = store.open_sized(false, TEST_SEGMENT_SIZE * 2);
        let mut segment_ids = std::collections::HashSet::new();
        for n in 0..200u64 {
            let id = Id::allocated(39, 0, n + 1);
            let txn = crate::server::transactions::test_hlc(9000 + n, 9);
            {
                let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                chunks.write_cell(&mut store.cell(&id, 128)).unwrap();
            }
            let segment = segment_of(&chunks, &id);
            if n > 1 {
                assert!(segment_ids.contains(&segment.id), "reuse a previous head");
            }
            segment_ids.insert(segment.id);
            chunks.remove_cell(&id).unwrap();
            assert_eq!(
                crate::ram::chunk::rewind_transaction_writes(&txn, &chunks.address_range()),
                Ok(1)
            );
        }
        chunks.sync_all();
        assert!(segment_ids.len() <= 2);
        assert!(chunks.list[0].seg_count() <= 2);
        for n in 0..200u64 {
            assert!(chunks.read_cell(&Id::allocated(39, 0, n + 1)).is_err());
        }
    }

    #[test]
    fn an_abort_releases_the_head_before_its_padding_is_synced_and_a_crash_keeps_the_next_write() {
        let store = BracketCleanerStore::new(438, "abort_before_sync");
        let chunks = store.open(false);
        let before = Id::allocated(38, 0, 1);
        let aborted = Id::allocated(38, 0, 2);
        let after = Id::allocated(38, 0, 3);
        chunks.write_cell(&mut store.cell(&before, 128)).unwrap();
        let txn = crate::server::transactions::test_hlc(8888, 9);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&aborted, 128)).unwrap();
        }
        let segment = segment_of(&chunks, &aborted);
        chunks.sync_all();
        // Model loss of the unsynced padding with the durable pre-abort WAL.
        let lost_padding = store.snapshot();
        let pause = crate::ram::segs::PauseRewindSync::new(&segment);
        chunks.remove_cell(&aborted).unwrap();
        assert_eq!(
            crate::ram::chunk::rewind_transaction_writes(&txn, &chunks.address_range()),
            Ok(1)
        );
        assert!(
            !segment.is_head_owned(),
            "the head is back in the pool at once"
        );
        let padding_survived = store.snapshot();
        // A torn final WAL record models a partially persisted rewind.
        let partial_padding = store.snapshot();
        let mut truncated = false;
        let mut directories = vec![partial_padding.wal.path().to_path_buf()];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    directories.push(entry.path());
                    continue;
                }
                let path = entry.path();
                let old = lost_padding
                    .wal
                    .path()
                    .join(path.strip_prefix(partial_padding.wal.path()).unwrap());
                if old.exists() && entry.metadata().unwrap().len() > old.metadata().unwrap().len() {
                    let old_len = old.metadata().unwrap().len();
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .unwrap()
                        .set_len(old_len + 1)
                        .unwrap();
                    truncated = true;
                }
            }
        }
        assert!(truncated, "the rewind appended a WAL record");
        // The syncer is held back: the next owner's acknowledged write must
        // survive although the padding before it was never synced by the timer.
        // Restrict the pool to the released head to prove actual reuse.
        for slot in chunks.list[0].head_pool.iter() {
            if slot.load(Ordering::Acquire) != segment.id {
                slot.store(u64::MAX, Ordering::Release);
            }
        }
        chunks.write_cell(&mut store.cell(&after, 128)).unwrap();
        assert_eq!(segment_of(&chunks, &after).id, segment.id);
        chunks.sync_all();
        drop(pause);
        let reused = store.snapshot();
        for (snapshot, has_after) in [
            (&lost_padding, false),
            (&partial_padding, false),
            (&padding_survived, false),
            (&reused, true),
        ] {
            let recovered = store.recover_snapshot(snapshot);
            assert!(recovered.read_cell(&before).is_ok());
            assert!(recovered.read_cell(&aborted).is_err());
            assert_eq!(recovered.read_cell(&after).is_ok(), has_after);
        }
    }

    /// After an abort the span is dead to the byte: the cells the rollback
    /// removed, their headers, and the BEGIN. Nothing is counted twice, so
    /// `living_space` neither stays too high (the cleaner never picks the
    /// segment) nor underflows into the warn-and-zero branch (the cleaner
    /// relocates it forever).
    #[test]
    fn living_space_is_exact_after_an_abort() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(440, "abort_living_space");
        let chunks = store.open(false);
        let aborted = [Id::allocated(40, 0, 1), Id::allocated(40, 0, 2)];
        let txn = crate::server::transactions::test_hlc(7777, 9);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            for id in &aborted {
                chunks.write_cell(&mut store.cell(id, 128)).unwrap();
            }
        }
        let segment = segment_of(&chunks, &aborted[0]);
        let begin_at = segment
            .entry_iter()
            .find(|meta| meta.entry_header.entry_type == crate::ram::entry::EntryType::BEGIN)
            .expect("the transaction's BEGIN")
            .entry_pos;
        let cursor = segment.append_header.load(Ordering::Acquire);
        let span = (cursor - begin_at) as u32;
        let used = segment.used_spaces();
        assert_eq!(segment.total_dead_space(), 0);
        for id in &aborted {
            chunks.remove_cell(id).unwrap();
        }
        assert_eq!(
            crate::ram::chunk::rewind_transaction_writes(&txn, &chunks.address_range()),
            Ok(1)
        );
        assert!(segment.try_own(), "the head is back in the pool at once");
        segment.release_own();
        assert_eq!(segment.used_spaces(), used, "the cursor stays");
        assert_eq!(segment.tombstones.load(Ordering::Relaxed), 0);
        assert_eq!(segment.total_dead_space(), span, "the whole span, once");
        assert_eq!(segment.living_space(), used - span);
    }

    /// H1 across a restart cannot happen: recovery resumes no segment as a
    /// write head, so a crash mid-transaction leaves its open BEGIN in a
    /// SEALED segment and the first write after the restart lands in a fresh
    /// one -- which a second restart keeps.
    #[test]
    fn an_open_bracket_left_by_a_crash_never_swallows_writes_after_the_restart() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(436, "open_bracket_restart");
        let txn = crate::server::transactions::test_hlc(8686, 9);
        let in_flight = Id::allocated(37, 0, 1);
        let after = Id::allocated(37, 0, 2);
        let open_seg;
        {
            let chunks = store.open(false);
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&in_flight, 128)).unwrap();
            open_seg = segment_of(&chunks, &in_flight).seq_id;
            chunks.sync_all();
            // Crash: no decision ever arrives.
        }
        {
            let chunks = store.open(true);
            assert!(chunks.read_cell(&in_flight).is_err());
            chunks.write_cell(&mut store.cell(&after, 128)).unwrap();
            assert_ne!(
                segment_of(&chunks, &after).seq_id,
                open_seg,
                "a recovered segment holding an open BEGIN must never take a write"
            );
            chunks.sync_all();
        }
        let chunks = store.open(true);
        assert!(chunks.read_cell(&in_flight).is_err());
        assert!(
            chunks.read_cell(&after).is_ok(),
            "the write after the restart survives"
        );
    }

    /// A delete whose bracket never got its COMMIT is dropped at the restart.
    /// The cell it named stays alive, and it must stay alive: a cleaner pass
    /// in the recovered store must not turn the dropped tombstone into a
    /// plain one that the next restart applies.
    #[test]
    fn a_dropped_open_delete_stays_dropped_after_compaction_and_a_second_restart() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(438, "dropped_open_delete");
        let txn = crate::server::transactions::test_hlc(8888, 9);
        let victim = Id::allocated(39, 0, 1);
        {
            let chunks = store.open(false);
            chunks.write_cell(&mut store.cell(&victim, 128)).unwrap();
            chunks.sync_all();
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.remove_cell(&victim).unwrap();
            chunks.sync_all();
            // Crash: no decision ever arrives.
        }
        {
            let chunks = store.open(true);
            assert!(
                chunks.read_cell(&victim).is_ok(),
                "the undecided delete is dropped"
            );
            let chunk = &chunks.list[0];
            let _ = ordinary_rotated_segment(&store, &chunks, 200);
            // Compact everything that is not a write head.
            let candidates: Vec<_> = chunk
                .segments()
                .into_iter()
                .filter(|seg| !chunk.is_active_head(seg.id) && seg.is_sealed())
                .collect();
            eprintln!("dropped_open_delete: {} candidate(s)", candidates.len());
            assert!(candidates.len() >= 2, "test setup: nothing to combine");
            let (_, collected) =
                crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
            eprintln!("dropped_open_delete: collected {collected}");
            assert!(chunks.read_cell(&victim).is_ok());
            chunks.sync_all();
        }
        let chunks = store.open(true);
        assert!(
            chunks.read_cell(&victim).is_ok(),
            "a delete that was dropped at one restart took effect at the next"
        );
    }

    /// Item 34: a transaction must never write outside its bracket. Before
    /// the fix, a transaction whose part filled up while every other head was
    /// busy with a plain write in flight sent its next cell out UNBRACKETED,
    /// so a crash before the decision kept that cell and discarded the first:
    /// a half-applied transaction.
    #[test]
    fn head_pressure_never_half_applies_a_transaction() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(437, "head_pressure_atomicity");
        let victim = crate::server::transactions::test_hlc(8787, 9);
        let first = Id::allocated(38, 0, 1);
        let second = Id::allocated(38, 0, 2);
        let mut second_written = false;
        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(store.schema.clone());
            // Plenty of segments: what runs out is the head pool (8 slots),
            // which is what a busy production chunk runs out of too.
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 16,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(store.backup_dir.path().to_str().unwrap().to_string()),
                Some(store.wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(store.raft_path.clone()),
            );
            // The victim fills most of its own part.
            {
                let _scope = crate::ram::chunk::TransactionScope::enter(victim.clone());
                chunks
                    .write_cell(&mut store.cell(&first, 512 * 1024))
                    .unwrap();
                let part = segment_of(&chunks, &first);
                let mut n = 10;
                while part.bound() - part.append_header.load(Ordering::Acquire) > 900 * 1024 {
                    let id = Id::allocated(38, 0, n);
                    chunks.write_cell(&mut store.cell(&id, 512 * 1024)).unwrap();
                    n += 1;
                }
            }
            // Every other head is busy with a PLAIN write in flight (held
            // here, as a concurrent writer would hold it for a few
            // microseconds), and exactly one is free when the victim needs
            // its next part. A head owned by a plain writer is back in
            // microseconds; the old spare-count read it as gone, found
            // nothing to spare, and sent the transaction's entry out
            // unbracketed into the one free head.
            let chunk = &chunks.list[0];
            let mut in_flight = Vec::new();
            while chunk
                .head_pool
                .iter()
                .any(|slot| slot.load(Ordering::Acquire) == u64::MAX)
            {
                in_flight.push(chunk.try_acquire(64, false).expect("plain claim"));
                assert!(in_flight.len() < 16, "test setup: the pool never filled");
            }
            drop(in_flight.pop());
            eprintln!(
                "head_pressure: {} plain write(s) in flight, one head free",
                in_flight.len()
            );
            // The victim's next cell needs a new part, and none can be had.
            {
                let _scope = crate::ram::chunk::TransactionScope::enter(victim.clone());
                let result = chunks.write_cell(&mut store.cell(&second, 900 * 1024));
                eprintln!(
                    "head_pressure: the second write returned {:?}",
                    result.as_ref().err()
                );
                second_written = result.is_ok();
                drop(in_flight);
            }
            chunks.sync_all();
            // Crash: nobody decides.
        }
        let chunks = store.open(true);
        let first_back = chunks.read_cell(&first).is_ok();
        let second_back = chunks.read_cell(&second).is_ok();
        eprintln!("head_pressure: second_written={second_written} first={first_back} second={second_back}");
        assert_eq!(
            first_back, second_back,
            "a transaction came back half-applied after a crash (first={first_back}, \
             second={second_back})"
        );
        assert!(
            !first_back,
            "an undecided transaction must not survive the crash"
        );
    }

    /// The cleaner must never resurrect an aborted span.
    ///
    /// The participant rolls the index back first, then pads over the span
    /// (journaled). Compaction only carries entries the index names, so the
    /// span's cells stay gone -- in memory now, and from the log after a
    /// restart, with the ordinary cells compacted alongside intact.
    #[test]
    fn compacting_a_rewound_span_never_resurrects_it() {
        let _ = env_logger::try_init();
        let store = BracketCleanerStore::new(434, "cleaner_rewound_span");
        let txn = crate::server::transactions::test_hlc(8484, 9);
        let gone = [Id::allocated(35, 0, 1), Id::allocated(35, 0, 2)];
        let after = Id::allocated(35, 0, 3);
        let ordinary;
        {
            let chunks = store.open(false);
            let chunk = &chunks.list[0];
            let (ids, ordinary_seg) = ordinary_rotated_segment(&store, &chunks, 200);
            ordinary = ids;

            {
                let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                for id in &gone {
                    chunks.write_cell(&mut store.cell(id, 256)).unwrap();
                }
            }
            let part = segment_of(&chunks, &gone[0]);
            // The participant's rollback: the transaction created these, so
            // undoing it removes them (ordinary writes, other heads).
            for id in &gone {
                chunks.remove_cell(id).unwrap();
            }
            assert_eq!(
                crate::ram::chunk::rewind_transaction_writes(&txn, &chunks.address_range()),
                Ok(1)
            );
            chunks.sync_all();
            // The head is back in the pool; an ordinary write follows (which
            // head it picks is the pool's affinity, not this test's business).
            assert!(!part.is_head_owned() && !part.bracket_is_open());
            chunks.write_cell(&mut store.cell(&after, 256)).unwrap();

            rotate_out(chunk, part.id);
            let candidates: Vec<_> = chunk
                .segments()
                .into_iter()
                .filter(|seg| seg.id == part.id || seg.id == ordinary_seg)
                .collect();
            assert_eq!(candidates.len(), 2);
            let (_, collected) =
                crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
            assert!(collected > 0, "test setup: the combine must actually run");
            assert!(chunk.segments().iter().all(|seg| seg.id != part.id));
            for id in &gone {
                assert!(
                    chunks.read_cell(id).is_err(),
                    "{id:?} resurrected in memory"
                );
            }
            assert!(chunks.read_cell(&after).is_ok());
            chunks.sync_all();
        }
        let chunks = store.open(true);
        for id in &gone {
            assert!(
                chunks.read_cell(id).is_err(),
                "{id:?}: an aborted transaction's cell came back after compaction and restart"
            );
        }
        assert!(
            chunks.read_cell(&after).is_ok(),
            "the ordinary write behind the padding"
        );
        for id in &ordinary {
            assert!(chunks.read_cell(id).is_ok(), "ordinary cell {id:?}");
        }
    }

    /// The crash this format exists for: a record's frame reached disk, its
    /// body did not.
    ///
    /// Unframed, the zeros an allocated-but-unwritten tail block reads back
    /// as were indistinguishable from a real entry -- the 8-byte entry
    /// header parsed, the length looked sane, and recovery ingested a cell
    /// whose header was all zeros. Framed, the record fails its CRC and the
    /// scan stops there, so the cells written BEFORE it still recover and
    /// the torn one simply never existed.
    #[test]
    fn a_torn_wal_record_is_refused_and_earlier_cells_still_recover() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let schema = schema_with_id(413, "recovery_torn_wal_record", false);
        let durable_id = Id::allocated(13, 0, 1);
        let torn_id = Id::allocated(13, 0, 2);

        let wal_path: String;
        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(schema.clone());
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );
            let mut durable = OwnedCell {
                header: CellHeader::new(schema.vid, &durable_id),
                data: data_map_value!(id: 1_i32, data: vec![0x41_u8; 64]),
            };
            let mut torn = OwnedCell {
                header: CellHeader::new(schema.vid, &torn_id),
                data: data_map_value!(id: 2_i32, data: vec![0x42_u8; 64]),
            };
            chunks.write_cell(&mut durable).unwrap();
            chunks.write_cell(&mut torn).unwrap();
            chunks.sync_all();

            let chunk = &chunks.list[0];
            let segment = chunk
                .locate_segment(chunks.address_of(&durable_id))
                .expect("the write should land in a segment");
            wal_path = chunk
                .file_manager
                .wal_path(chunk.id, segment.id, segment.seq_id)
                .expect("the test configures WAL storage");
        }

        // Tear the last record's body, the way a power cut does.
        let bytes = std::fs::read(&wal_path).unwrap();
        assert_eq!(
            &bytes[..4],
            &crate::ram::wal_format::FILE_MAGIC,
            "the WAL should be written framed"
        );
        std::fs::write(&wal_path, &bytes[..bytes.len() - 8]).unwrap();

        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        let durable = chunks
            .read_cell(&durable_id)
            .expect("the record before the tear must recover");
        assert_eq!(*durable.data["id"].i32().unwrap(), 1);
        drop(durable);
        assert!(
            chunks.read_cell(&torn_id).is_err(),
            "a torn record must never be replayed as a cell"
        );
    }

    /// Scribbled bytes INSIDE a record, with its length left intact.
    ///
    /// This is the case length-based scanning can never catch: every
    /// structural check passes, so an unframed log hands recovery a cell
    /// built from corrupt bytes and it is served as real data. Only a
    /// checksum can tell the difference, which is why one is now stored.
    #[test]
    fn a_scribbled_wal_record_is_refused_rather_than_served_as_data() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let schema = schema_with_id(414, "recovery_scribbled_wal_record", false);
        let durable_id = Id::allocated(14, 0, 1);
        let scribbled_id = Id::allocated(14, 0, 2);
        let wal_path: String;

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(schema.clone());
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );
            let mut durable = OwnedCell {
                header: CellHeader::new(schema.vid, &durable_id),
                data: data_map_value!(id: 1_i32, data: vec![0x41_u8; 64]),
            };
            let mut scribbled = OwnedCell {
                header: CellHeader::new(schema.vid, &scribbled_id),
                data: data_map_value!(id: 2_i32, data: vec![0x42_u8; 64]),
            };
            chunks.write_cell(&mut durable).unwrap();
            chunks.write_cell(&mut scribbled).unwrap();
            chunks.sync_all();

            let chunk = &chunks.list[0];
            let segment = chunk
                .locate_segment(chunks.address_of(&durable_id))
                .expect("the write should land in a segment");
            wal_path = chunk
                .file_manager
                .wal_path(chunk.id, segment.id, segment.seq_id)
                .expect("the test configures WAL storage");
        }

        // Flip bytes in the last record's payload, leaving every length and
        // magic exactly as written.
        let mut bytes = std::fs::read(&wal_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        bytes[last - 3] ^= 0xFF;
        std::fs::write(&wal_path, &bytes).unwrap();

        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        let durable = chunks
            .read_cell(&durable_id)
            .expect("the intact record must recover");
        assert_eq!(*durable.data["id"].i32().unwrap(), 1);
        drop(durable);
        assert!(
            chunks.read_cell(&scribbled_id).is_err(),
            "a record that fails its checksum must not be served as a cell"
        );
    }

    #[test]
    fn test_recovery_keeps_wal_only_blob_segments_cold_with_truncated_tail() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let blob_schema = schema_with_id(112, "recovery_blob_wal_only", true);
        let first_id = Id::allocated(12, 0, 1);
        let second_id = Id::allocated(12, 0, 2);
        let wal_path: String;
        let backup_path: String;
        let truncated_wal: Vec<u8>;
        let blob_segment_id: u64;
        let blob_seq_id: u64;

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(blob_schema.clone());
            let manager = tiered_manager_for_test(8 * SEGMENT_SIZE);
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(manager),
                false,
                Some(raft_path.clone()),
            );

            let mut first_cell = OwnedCell {
                header: CellHeader::new(blob_schema.vid, &first_id),
                data: data_map_value!(id: 1_i32, data: vec![0x77_u8; DATA_SIZE]),
            };
            let mut second_cell = OwnedCell {
                header: CellHeader::new(blob_schema.vid, &second_id),
                data: data_map_value!(id: 2_i32, data: vec![0x88_u8; DATA_SIZE]),
            };

            chunks.write_cell(&mut first_cell).unwrap();
            chunks.write_cell(&mut second_cell).unwrap();
            chunks.sync_all();

            let chunk = &chunks.list[0];
            let blob_segment = chunk
                .locate_segment(chunks.address_of(&first_id))
                .expect("blob write should land in a segment");
            let first_entry = entry_bytes_at(chunks.address_of(&first_id));
            let second_entry = entry_bytes_at(chunks.address_of(&second_id));

            blob_segment_id = blob_segment.id;
            blob_seq_id = blob_segment.seq_id;
            wal_path = chunk
                .file_manager
                .wal_path(chunk.id, blob_segment_id, blob_seq_id)
                .expect("wal path should exist for WAL-only blob recovery test");
            backup_path = chunk
                .file_manager
                .backup_path(chunk.id, blob_segment_id, blob_seq_id)
                .expect("backup path should be derivable for WAL-only blob recovery test");
            // A framed log whose LAST record is torn -- the only shape a
            // truncated WAL can take now that every log is framed. The first
            // record is whole and must survive; the second is cut mid-record
            // and must be dropped.
            truncated_wal = {
                // Records name the offsets they belong at, so the fixture
                // uses the offsets the writer actually used rather than
                // assuming the segment starts with this cell.
                let first_off = (chunks.address_of(&first_id) - blob_segment.addr) as u64;
                let second_off = (chunks.address_of(&second_id) - blob_segment.addr) as u64;
                let mut bytes = crate::ram::wal_format::wal_file_header(blob_seq_id).to_vec();
                bytes.extend_from_slice(&crate::ram::wal_format::frame_record(
                    first_off,
                    &first_entry,
                ));
                let torn = crate::ram::wal_format::frame_record(second_off, &second_entry);
                bytes.extend_from_slice(&torn[..crate::ram::wal_format::RECORD_HEADER_SIZE + 4]);
                bytes
            };

            assert!(Path::new(&wal_path).exists());
            assert!(!Path::new(&backup_path).exists());
        }

        fs::write(&wal_path, &truncated_wal).unwrap();
        assert!(!Path::new(&backup_path).exists());

        {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(blob_schema.clone());
            let manager = tiered_manager_for_test(8 * SEGMENT_SIZE);
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                Some(manager),
                true,
                Some(raft_path.clone()),
            );

            let chunk = &chunks.list[0];
            let (regular_head, blob_head) = chunk.head_seg_ids_for_test();
            let blob_segment = chunk
                .segs
                .get(&(blob_segment_id as usize))
                .expect("WAL-only recovery should restore the blob segment in place");
            let recovered_seg_ids: Vec<u64> = chunk
                .segments()
                .iter()
                .map(|segment| segment.seq_id)
                .collect();

            assert_eq!(blob_segment.seq_id, blob_seq_id);
            assert_eq!(blob_head, None, "recovery should leave the blob head empty");
            // Recovery resumes NOTHING. A recovered segment is a closed
            // incarnation whether it came from a backup or a WAL: resuming a
            // WAL-recovered one reopens its log in append mode against a
            // rewound append_header, which breaks the offset invariant the
            // log depends on and silently makes every later write to it
            // unrecoverable. The first write allocates a fresh segment
            // instead, which the next assertion exercises.
            assert_eq!(
                regular_head, None,
                "recovery should leave the regular head empty too"
            );
            assert_eq!(blob_segment.segment_class(), SegmentClass::Blob);
            assert!(
                blob_segment.is_cold(),
                "WAL-only recovered blob segments must start cold"
            );
            assert!(
                Path::new(&backup_path).exists(),
                "cold recovery from WAL should synthesize a backup file before future promotion"
            );

            let first_cell = chunks.read_cell(&first_id).unwrap();
            assert_eq!(*first_cell.data["id"].i32().unwrap(), 1);
            drop(first_cell);

            assert!(
                chunks.read_cell(&second_id).is_err(),
                "the crash-truncated tail entry should not be recovered"
            );
            assert!(
                blob_segment.is_hot(),
                "reading a cold WAL-only blob segment should later promote it"
            );

            // The point of resuming nothing: the next write must land
            // somewhere NEW. A segment recovered from a WAL has a log whose
            // length no longer matches its append cursor, so appending to it
            // writes records that describe the wrong offsets -- unnoticeably,
            // until the next crash tries to replay them.
            let mut fresh = OwnedCell {
                header: CellHeader::new(blob_schema.vid, &Id::allocated(12, 0, 3)),
                data: data_map_value!(id: 3_i32, data: vec![0x99_u8; DATA_SIZE]),
            };
            chunks.write_cell(&mut fresh).expect("post-recovery write");
            let landed_seq = chunk
                .locate_segment(chunks.address_of(&Id::allocated(12, 0, 3)))
                .expect("the post-recovery write must land in a segment")
                .seq_id;
            assert!(
                !recovered_seg_ids.contains(&landed_seq),
                "the write landed in a recovered incarnation (seq {landed_seq}); recovered \
                 segments are closed and must never be appended to"
            );
        }
    }

    #[test]
    fn test_prepare_recovered_segment_keeps_newer_resident_seq_across_class_changes() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let seg_id = 0_u64;
        let newer_regular_seq_id = 5_u64;
        let older_blob_seq_id = 4_u64;

        let resident = chunk
            .allocator
            .alloc_seg_at_id_with(
                seg_id,
                newer_regular_seq_id,
                &chunk.file_manager,
                true,
                SegmentClass::Regular,
            )
            .expect("should allocate the newer resident segment");
        let resident = install_recovered_segment(chunk, resident);

        let older_blob_file = SegmentFileInfo {
            chunk_id: chunk.id,
            seg_id,
            seq_id: older_blob_seq_id,
            path: Path::new("older-blob-segment.nbackup").to_path_buf(),
            size: 0,
            is_backup: true,
        };

        let prepared =
            prepare_recovered_segment(chunk, &older_blob_file, false, SegmentClass::Blob)
                .expect("older plan should not make preparation fail");

        assert_eq!(
            prepared.seq_id, newer_regular_seq_id,
            "older recovery plans must not displace a newer resident seq for the same seg_id"
        );
        assert_eq!(
            prepared.segment_class(),
            SegmentClass::Regular,
            "older cross-class recovery plans must not change the surviving runtime class"
        );
        assert!(
            prepared.is_hot(),
            "the newer resident segment should stay hot"
        );
        assert_eq!(
            prepared.id, seg_id,
            "the surviving segment must keep the same seg_id"
        );
        assert_eq!(
            resident.seq_id,
            chunk.segs.get(&(seg_id as usize)).unwrap().seq_id,
            "segment installation must preserve the newer resident seq_id"
        );
    }

    #[test]
    fn test_prepare_recovered_segment_replaces_same_seq_when_class_changes() {
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let seg_id = 0_u64;
        let seq_id = 0_u64;

        let blob_file = SegmentFileInfo {
            chunk_id: chunk.id,
            seg_id,
            seq_id,
            path: Path::new("bootstrap-blob-segment.nbackup").to_path_buf(),
            size: 0,
            is_backup: true,
        };

        let prepared = prepare_recovered_segment(chunk, &blob_file, false, SegmentClass::Blob)
            .expect("same-seq class change should allocate a replacement segment");

        assert_eq!(prepared.seq_id, seq_id);
        assert_eq!(prepared.id, seg_id);
        assert_eq!(prepared.segment_class(), SegmentClass::Blob);
        assert!(prepared.is_cold());
    }

    #[test]
    fn test_recovery_preserves_bootstrap_segment_seq_id_when_reused() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let recovered_seq_id = 77_u64;
        let cell_id = Id::allocated(0, 0, 1);

        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );

            let mut cell = default_cell(&cell_id);
            chunks.write_cell(&mut cell).unwrap();

            for chunk in &chunks.list {
                chunk.rotate_heads_for_scrub_test();
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            let original_backup = chunks.list[0]
                .file_manager
                .backup_path(0, 0, 0)
                .expect("backup storage should be configured for the bootstrap segment");
            let recovered_backup = chunks.list[0]
                .file_manager
                .backup_path(0, 0, recovered_seq_id)
                .expect("backup storage should be configured for the recovered bootstrap segment");
            std::fs::copy(&original_backup, &recovered_backup).expect(
                "should be able to synthesize a recovered bootstrap segment with a higher seq_id",
            );
        }

        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            let bootstrap_segment = chunks.list[0]
                .segs
                .get(&0)
                .expect("recovery should reuse segment 0 in place");

            assert_eq!(
                bootstrap_segment.seq_id, recovered_seq_id,
                "recovery should preserve the seq_id from the recovered bootstrap segment file"
            );
            assert!(
                chunks.list[0].segs.contains_seq_id(recovered_seq_id),
                "segment lookup by seq_id should point at the recovered bootstrap segment"
            );
            assert!(
                !chunks.list[0].segs.contains_seq_id(0),
                "bootstrap recovery should replace the preallocated seq_id 0 entry"
            );

            let recovered_cell = chunks.read_cell(&cell_id).unwrap();
            assert_eq!(*recovered_cell.data["id"].i32().unwrap(), 1);
        }
    }

    // Purpose: End-to-end sanity check. Write cells, archive segments,
    // restart with recovery enabled, and verify all cells are restored.
    #[test]
    fn test_recovery_basic_write_and_recover() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Phase 1: Create chunks, write data, and let it persist
        let cell_ids: Vec<Id> = (0..10).map(|i| Id::allocated(0, 0, i)).collect();
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,  // no tiered memory
                false, // no recovery on first run
                Some(raft_path.clone()),
            );

            // Write cells
            for id in &cell_ids {
                let mut cell = default_cell(id);
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive segments to create backup files
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            // Verify data exists before recovery
            assert_eq!(chunks.list[0].cell_count(), 10);
        }

        // Phase 2: Create new chunks instance with recovery enabled
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true, // enable recovery
                Some(raft_path.clone()),
            );

            // Verify recovered data
            assert_eq!(chunks.list[0].cell_count(), 10);
            for id in &cell_ids {
                let cell = chunks.read_cell(id).unwrap();
                let expected = default_cell(id);
                assert_eq!(cell.to_owned().data, expected.data);
            }
        }
    }

    // Purpose: Verify version handling across recovery. After recovering an
    // initial write, write a newer version, archive, and finally recover again;
    // latest version must be returned.
    #[test]
    fn test_recovery_with_updates() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let cell_id = Id::allocated(0, 0, 42);

        println!("=== Phase 1: Write initial data ===");
        // Phase 1: Write initial data
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            println!("Writing cell...");
            let mut cell = default_cell(&cell_id);
            chunks.write_cell(&mut cell).unwrap();
            println!("Cell written successfully");

            // Archive
            println!("Archiving segments...");
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
            println!("Phase 1 complete, dropping chunks...");
        }
        println!("Phase 1 chunks dropped");

        println!("=== Phase 2: Recover and update ===");
        // Phase 2: Update the cell (higher version)
        {
            let schemas = setup_test_schema();
            println!("Creating chunks with recovery enabled...");
            use std::fs;
            fn list_files_recursively(dir: &Path) -> Vec<String> {
                let mut files = Vec::new();
                if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path = entry.path();
                            if path.is_dir() {
                                files.extend(list_files_recursively(&path));
                            } else {
                                files.push(path.display().to_string());
                            }
                        }
                    }
                }
                files
            }

            println!(
                "Files before recovery: WAL={:?}, Backup={:?}",
                list_files_recursively(wal_dir.path()),
                list_files_recursively(backup_dir.path())
            );
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true, // recover first
                Some(raft_path.clone()),
            );
            println!("Chunks recovered successfully");
            println!(
                "Files after recovery: WAL={:?}, Backup={:?}",
                list_files_recursively(wal_dir.path()),
                list_files_recursively(backup_dir.path())
            );

            // Update with new data (version will be higher)
            println!("Updating cell...");
            let updated_data: Vec<u8> = vec![0xFF; DATA_SIZE];
            let mut updated_cell = OwnedCell {
                header: CellHeader {
                    version: 2,
                    timestamp: 200,
                    schema: SchemaVid(0),
                    id: cell_id,
                },
                data: data_map_value!(id: 999 as i32, data: updated_data),
            };
            println!("About to update cell...");
            chunks.update_cell(&mut updated_cell).unwrap();
            println!("Cell updated successfully");
            println!(
                "Cell count before archiving: {}",
                chunks.list[0].cell_count()
            );

            // Archive updated segment
            println!("Archiving updated segments...");
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    println!("Archiving segment {} with seq_id {}", seg.id, seg.seq_id);
                    let result = seg.archive().unwrap();
                    println!("Archive result: {}", result);
                }
            }

            // List files
            println!(
                "WAL files: {:?}",
                std::fs::read_dir(wal_dir.path())
                    .unwrap()
                    .collect::<Vec<_>>()
            );
            println!(
                "Backup files: {:?}",
                std::fs::read_dir(backup_dir.path())
                    .unwrap()
                    .collect::<Vec<_>>()
            );

            println!("Phase 2 complete, dropping chunks...");
        }
        println!("Phase 2 chunks dropped");

        println!("=== Phase 3: Final recovery and verification ===");
        println!(
            "WAL files: {:?}",
            std::fs::read_dir(wal_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path().display().to_string())
                .collect::<Vec<_>>()
        );
        println!(
            "Backup files: {:?}",
            std::fs::read_dir(backup_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path().display().to_string())
                .collect::<Vec<_>>()
        );
        // Phase 3: Recover and verify we get the latest version
        {
            let schemas = setup_test_schema();
            println!("Creating chunks with recovery for verification...");
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );
            println!(
                "Chunks recovered, cell count: {}",
                chunks.list[0].cell_count()
            );
            println!(
                "Trying to read cell with id {:?}, hash {}",
                cell_id,
                cell_id.bits()
            );

            println!("Reading cell...");
            let cell = chunks.read_cell(&cell_id).unwrap();
            // Should have the updated data
            let cell_owned = cell.to_owned();
            let id_val = cell_owned.data["id"].i32().unwrap();
            assert_eq!(*id_val, 999);
        }
    }

    // Purpose: Ensure multiple versions land in different segments with distinct
    // header versions and that recovery restores the latest version/data.
    #[test]
    fn test_recovery_with_multi_segment_versions() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Helpers scoped to this test
        fn collect_versions_for_hash(chunk: &Chunk, hash: u64) -> Vec<(u64, u64)> {
            let mut versions = Vec::new();
            for seg in chunk.segments() {
                let mut cursor = seg.addr;
                let bound = seg.append_header.load(Ordering::Relaxed);
                while cursor < bound {
                    let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);
                    let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;
                    if entry_header.entry_type == EntryType::CELL {
                        let content_addr = Entry::content_pos(cursor);
                        let header = cell_header_from_entry_content_addr(content_addr);
                        if header.id.bits() == hash {
                            versions.push((seg.id, header.version));
                        }
                    }
                    cursor += entry_size;
                }
            }
            versions
        }

        fn list_all_files(dir: &Path) -> Vec<String> {
            let mut files = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        files.extend(list_all_files(&path));
                    } else if let Some(name) = path.file_name() {
                        files.push(name.to_string_lossy().to_string());
                    }
                }
            }
            files
        }

        fn force_new_segment(
            chunks: &Chunks,
            chunk: &Chunk,
            filler_counter: &mut i32,
            payload_size: usize,
        ) {
            let initial_head = chunk.get_head_seg_id();
            let mut attempts = 0;
            while chunk.get_head_seg_id() == initial_head {
                let filler_id = Id::allocated(0, 0, *filler_counter as u64);
                let mut filler = OwnedCell {
                    header: CellHeader::new(SchemaVid(0), &filler_id),
                    data: data_map_value!(id: *filler_counter, data: vec![0xEEu8; payload_size]),
                };
                chunks.write_cell(&mut filler).unwrap();
                *filler_counter += 1;
                attempts += 1;
                assert!(
                    attempts < 128,
                    "failed to switch to a new segment after {} filler writes",
                    attempts
                );
            }
        }

        let cell_id = Id::allocated(0, 0, 7);
        let payload_size = 128 * 1024; // 128KB payload to rotate segments with moderate writes
        let latest_version: u64;
        let latest_marker: i32;

        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4, // allow multiple segments for this chunk
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            let chunk = &chunks.list[0];
            let mut current_version = 1u64;
            let mut filler_counter: i32 = 10_000;

            // Version 1
            let mut v1_cell = OwnedCell {
                header: CellHeader::new(SchemaVid(0), &cell_id),
                data: data_map_value!(id: 101i32, data: vec![0x11u8; payload_size]),
            };
            v1_cell.header.version = current_version;
            let v1_header = chunks.write_cell(&mut v1_cell).unwrap();
            current_version = v1_header.version;

            // Version 2 in a new segment
            force_new_segment(&chunks, chunk, &mut filler_counter, payload_size);
            let mut v2_cell = OwnedCell {
                header: CellHeader::new(SchemaVid(0), &cell_id),
                data: data_map_value!(id: 202i32, data: vec![0x22u8; payload_size]),
            };
            v2_cell.header.version = current_version;
            let v2_header = chunks.update_cell(&mut v2_cell).unwrap();
            current_version = v2_header.version;

            // Version 3 in yet another segment
            force_new_segment(&chunks, chunk, &mut filler_counter, payload_size);
            let mut v3_cell = OwnedCell {
                header: CellHeader::new(SchemaVid(0), &cell_id),
                data: data_map_value!(id: 303i32, data: vec![0x33u8; payload_size]),
            };
            v3_cell.header.version = current_version;
            let v3_header = chunks.update_cell(&mut v3_cell).unwrap();
            latest_marker = 303;

            let versions = collect_versions_for_hash(chunk, cell_id.bits());
            println!("Written versions: {:?}", versions);
            assert_eq!(versions.len(), 3, "expected three stored versions");
            let version_set: HashSet<u64> = versions.iter().map(|(_, v)| *v).collect();
            assert_eq!(
                version_set,
                HashSet::from_iter([v1_header.version, v2_header.version, v3_header.version]),
                "stored versions should match written versions"
            );
            latest_version = *version_set.iter().max().unwrap();
            let seg_set: HashSet<u64> = versions.iter().map(|(seg, _)| *seg).collect();
            assert_eq!(
                seg_set.len(),
                3,
                "each version should reside in a different segment"
            );

            chunk.release_append_heads_for_test();
            let mut archived_segments = Vec::new();
            for seg in chunk.segments() {
                let archived = seg.archive().unwrap();
                archived_segments.push((seg.id, seg.seq_id, archived));
            }
            println!("Archived segments: {:?}", archived_segments);

            let backup_files = list_all_files(backup_dir.path());
            let wal_files = list_all_files(wal_dir.path());
            println!(
                "Backup files: {} {:?}, WAL files: {} {:?}",
                backup_files.len(),
                backup_files,
                wal_files.len(),
                wal_files
            );
            assert!(
                backup_files.len() >= 3,
                "expected backups for all segments, found {}",
                backup_files.len()
            );
        }

        // Simulate crash and recover
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 4,
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true, // recover from backups
                Some(raft_path.clone()),
            );

            let recovered = chunks.read_cell(&cell_id).unwrap().to_owned();
            let recovered_versions = collect_versions_for_hash(&chunks.list[0], cell_id.bits());
            println!("Recovered versions: {:?}", recovered_versions);
            assert_eq!(
                recovered.header.version, latest_version,
                "recovered cell should have the latest version"
            );
            assert_eq!(
                *recovered.data["id"].i32().unwrap(),
                latest_marker,
                "recovered cell should contain the latest payload"
            );
        }
    }

    // Purpose: Verify delete/tombstone semantics across recovery. After initial
    // writes and archive, recover and delete a subset (tombstones), archive, then
    // recover; only non-deleted cells should remain.
    // Note: Currently ignored due to file cleanup semantics that can remove older
    // segment files still holding live cells.
    #[test]
    fn test_recovery_with_tombstones() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let cell_ids: Vec<Id> = (0..5).map(|i| Id::allocated(0, 0, i)).collect();

        // Phase 1: Write cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            for id in &cell_ids {
                let mut cell = default_cell(id);
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 5);
        }

        // Phase 2: Delete some cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true, // recover
                Some(raft_path.clone()),
            );

            // Delete cells 0, 2, 4
            chunks.remove_cell(&cell_ids[0]).unwrap();
            chunks.remove_cell(&cell_ids[2]).unwrap();
            chunks.remove_cell(&cell_ids[4]).unwrap();

            assert_eq!(chunks.list[0].cell_count(), 2);

            // Archive with tombstones
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        // Phase 3: Recover and verify deletions
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );

            // Should only have cells 1 and 3
            assert_eq!(chunks.list[0].cell_count(), 2);
            assert!(chunks.read_cell(&cell_ids[1]).is_ok());
            assert!(chunks.read_cell(&cell_ids[3]).is_ok());

            // Deleted cells should not exist
            assert!(chunks.read_cell(&cell_ids[0]).is_err());
            assert!(chunks.read_cell(&cell_ids[2]).is_err());
            assert!(chunks.read_cell(&cell_ids[4]).is_err());
        }
    }

    // Purpose: Ensure allocator next_seq_id is restored to at least
    // max(recovered seq_id) + 1 so new segments get strictly higher seq_ids.
    #[test]
    fn test_recovery_preserves_seq_id() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let initial_seq_id: u64;

        // Phase 1: Write some data and capture seq_id
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            // Write enough cells to allocate multiple segments
            for i in 0..20 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            initial_seq_id = chunks.list[0].allocator.next_seq_id.load(Ordering::Relaxed) as u64;
            println!("Initial seq_id after writes: {}", initial_seq_id);
        }

        // Phase 2: Recover and verify seq_id continues from where it left off
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );

            let recovered_seq_id =
                chunks.list[0].allocator.next_seq_id.load(Ordering::Relaxed) as u64;
            println!("Recovered seq_id: {}", recovered_seq_id);

            // seq_id should be at least the initial value (may be higher if more segments were allocated)
            assert!(
                recovered_seq_id >= initial_seq_id,
                "Expected seq_id >= {}, got {}",
                initial_seq_id,
                recovered_seq_id
            );
        }
    }

    // Purpose: Ensure recovery on empty storage results in a clean state with
    // zero cells and no errors.
    #[test]
    fn test_recovery_handles_empty_storage() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Try to recover when no files exist
        let schemas = setup_test_schema();
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 2,
            Arc::new(ServerMeta { schemas }),
            None, // index_builder
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None, // no tiered memory
            true, // enable recovery even though nothing to recover
            Some(raft_path.clone()),
        );

        // Should create fresh chunks with no data
        assert_eq!(chunks.list[0].cell_count(), 0);
    }

    // Purpose: Confirm append_header offset (relative to segment base) is
    // reconstructed by scanning recovered segment contents.
    #[test]
    fn test_find_append_header() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Phase 1: Write data and get actual append position
        let actual_append_offset: usize;
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            // Write a few cells
            for i in 0..5 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            let seg = &chunks.list[0].segments()[0];
            let actual_append = seg.append_header.load(Ordering::Relaxed);
            actual_append_offset = actual_append - seg.addr;
            println!(
                "Actual append header: {} (offset: {})",
                actual_append, actual_append_offset
            );

            // Archive the segment
            seg.archive().unwrap();
        }

        // Phase 2: Recover and verify append_header is correctly restored
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );

            // Find the segment with data (non-zero offset)
            let segs = chunks.list[0].segments();
            println!("Total segments after recovery: {}", segs.len());

            let mut found_matching_segment = false;
            for seg in &segs {
                let recovered_append = seg.append_header.load(Ordering::Relaxed);
                let recovered_offset = recovered_append - seg.addr;
                println!(
                    "Segment {} append_header offset: {}",
                    seg.id, recovered_offset
                );

                if recovered_offset == actual_append_offset {
                    found_matching_segment = true;
                    println!("Found matching segment with correct offset!");
                    break;
                }
            }

            // Should find at least one segment with the correct offset
            assert!(
                found_matching_segment,
                "No segment found with offset {}. Segments: {:?}",
                actual_append_offset,
                segs.iter()
                    .map(|s| (s.id, s.append_header.load(Ordering::Relaxed) - s.addr))
                    .collect::<Vec<_>>()
            );
        }
    }

    // Purpose: Ensure discovery/dedup handles both WAL and backup variants for
    // the same segment-generation, preserving data integrity.
    #[test]
    fn test_recovery_deduplication() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Phase 1: Create backup for a segment
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            let mut cell = default_cell(&Id::allocated(0, 0, 1));
            chunks.write_cell(&mut cell).unwrap();

            let seg = &chunks.list[0].segments()[0];
            seg.archive().unwrap();
        }

        // Phase 2: Recover - should handle file discovery and deduplication gracefully
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );

            // Verify data integrity
            let cell = chunks.read_cell(&Id::allocated(0, 0, 1));
            assert!(cell.is_ok());
        }
    }

    // Purpose: Verify recovery marker helpers for crash-safety signaling
    // (create, check, remove).
    #[test]
    fn test_recovery_marker() {
        let marker_dir = TempDir::new().unwrap();
        let marker_path = marker_dir.path().to_str().unwrap();

        // Create marker
        create_recovery_marker(marker_path).unwrap();
        assert!(check_recovery_marker(marker_path));

        // Remove marker
        remove_recovery_marker(marker_path).unwrap();
        assert!(!check_recovery_marker(marker_path));
    }

    // Purpose: Test that backup files survive multiple shutdown/recovery cycles
    // without data loss. Simulates real-world scenario of repeated restarts.
    #[test]
    fn test_multiple_recovery_cycles_preserve_backups() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Cycle 1: Write initial batch of cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );

            // Write cells 0-9
            for i in 0..10 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive for backup
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 10);
        }

        // Cycle 2: Recover and add more cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true, // Recover from backups
                Some(raft_path.clone()),
            );

            // Verify cells from cycle 1
            assert_eq!(chunks.list[0].cell_count(), 10);
            for i in 0..10 {
                assert!(chunks.read_cell(&Id::allocated(0, 0, i)).is_ok());
            }

            // Write cells 10-19
            for i in 10..20 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 20);
        }

        // Cycle 3: Recover and add more cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            // Verify all cells from cycles 1 and 2
            assert_eq!(chunks.list[0].cell_count(), 20);
            for i in 0..20 {
                assert!(chunks.read_cell(&Id::allocated(0, 0, i)).is_ok());
            }

            // Write cells 20-29
            for i in 20..30 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 30);
        }

        // Cycle 4: Final recovery and verification
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            // Verify all 30 cells from all cycles survived
            assert_eq!(chunks.list[0].cell_count(), 30);
            for i in 0..30 {
                let cell = chunks.read_cell(&Id::allocated(0, 0, i)).unwrap();
                let expected = default_cell(&Id::allocated(0, 0, i));
                assert_eq!(cell.to_owned().data, expected.data);
            }
        }
    }

    // Purpose: Test that updates across multiple recovery cycles preserve
    // the latest version of each cell
    #[test]
    fn test_recovery_cycles_with_updates() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let cell_id = Id::allocated(0, 0, 42);

        // Cycle 1: Write initial version
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );

            let mut cell = default_cell(&cell_id);
            chunks.write_cell(&mut cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        // Cycle 2: Recover and update
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            // Update to version 2
            let updated_data: Vec<u8> = vec![0xAA; DATA_SIZE];
            let mut updated_cell = OwnedCell {
                header: CellHeader {
                    version: 2,
                    timestamp: 200,
                    schema: SchemaVid(0),
                    id: cell_id,
                },
                data: data_map_value!(id: 42 as i32, data: updated_data),
            };
            chunks.update_cell(&mut updated_cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        // Cycle 3: Recover and update again
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            // Update to version 3
            let updated_data: Vec<u8> = vec![0xFF; DATA_SIZE];
            let mut updated_cell = OwnedCell {
                header: CellHeader {
                    version: 3,
                    timestamp: 300,
                    schema: SchemaVid(0),
                    id: cell_id,
                },
                data: data_map_value!(id: 999 as i32, data: updated_data),
            };
            chunks.update_cell(&mut updated_cell).unwrap();

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        // Cycle 4: Verify latest version survived
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            let cell = chunks.read_cell(&cell_id).unwrap();
            let cell_owned = cell.to_owned();

            // Should have latest update with id=999 (version may be auto-incremented)
            assert!(
                cell_owned.header.version >= 3,
                "Version should be at least 3"
            );
            let id_val = cell_owned.data["id"].i32().unwrap();
            assert_eq!(*id_val, 999, "Should have the latest updated value");
        }
    }

    // Purpose: Test that deletions (tombstones) survive recovery cycles
    // Tombstones in newer segments remove the cells in older backup segments
    // at recovery; the older backups are not rewritten.
    #[test]
    fn test_recovery_cycles_with_deletions() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let cell_ids: Vec<Id> = (0..10).map(|i| Id::allocated(0, 0, i)).collect();

        // Cycle 1: Write all cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                false,
                Some(raft_path.clone()),
            );

            for id in &cell_ids {
                let mut cell = default_cell(id);
                chunks.write_cell(&mut cell).unwrap();
            }

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 10);
        }

        // Cycle 2: Recover and delete some cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            assert_eq!(chunks.list[0].cell_count(), 10);

            // Delete cells 0, 2, 4, 6, 8
            for i in (0..10).step_by(2) {
                chunks.remove_cell(&cell_ids[i]).unwrap();
            }

            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 5);
        }

        // Cycle 3: Verify deletions survived recovery
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None,
                true,
                Some(raft_path.clone()),
            );

            // Should have 5 cells (odd numbers: 1, 3, 5, 7, 9)
            assert_eq!(chunks.list[0].cell_count(), 5);

            // Verify deleted cells are gone
            for i in (0..10).step_by(2) {
                assert!(chunks.read_cell(&cell_ids[i]).is_err());
            }

            // Verify remaining cells exist
            for i in (1..10).step_by(2) {
                assert!(chunks.read_cell(&cell_ids[i]).is_ok());
            }
        }
    }

    // Purpose: Validate multi-chunk recovery and routing. Write across 3 chunks,
    // archive, recover, and verify all cells are present and readable. Confirms
    // partition-based mapping used during recovery population.
    #[test]
    fn test_multiple_chunks_recovery() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        // Phase 1: Create multiple chunks with data
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                3,                     // 3 chunks
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                false,
                Some(raft_path.clone()),
            );

            // Write data to different chunks
            for i in 0..30 {
                let mut cell = default_cell(&Id::allocated(0, 0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive all segments
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            let total_cells: usize = chunks.list.iter().map(|c| c.cell_count()).sum();
            println!("Total cells across all chunks: {}", total_cells);
            assert_eq!(total_cells, 30);
        }

        // Phase 2: Recover all chunks
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                3,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                None, // no tiered memory
                true,
                Some(raft_path.clone()),
            );

            let total_cells: usize = chunks.list.iter().map(|c| c.cell_count()).sum();
            println!("Total recovered cells across all chunks: {}", total_cells);
            assert_eq!(total_cells, 30);

            // Verify all cells are accessible
            for i in 0..30 {
                let cell = chunks.read_cell(&Id::allocated(0, 0, i));
                assert!(cell.is_ok(), "Cell {} should exist", i);
            }
        }
    }

    #[test]
    fn test_multi_database_recovery_respects_shared_hot_limit() {
        let _ = env_logger::try_init();

        let db1_wal_dir = TempDir::new().unwrap();
        let db1_backup_dir = TempDir::new().unwrap();
        let db2_wal_dir = TempDir::new().unwrap();
        let db2_backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let shared_pool =
            crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 4 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            });
        let shared_manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
            shared_pool,
        ));

        {
            let db1_chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta {
                    schemas: setup_test_schema(),
                }),
                None,
                Some(db1_backup_dir.path().to_str().unwrap().to_string()),
                Some(db1_wal_dir.path().to_str().unwrap().to_string()),
                Some(shared_manager.clone()),
                false,
                Some(raft_path.clone()),
            );
            let db2_chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8,
                Arc::new(ServerMeta {
                    schemas: setup_test_schema(),
                }),
                None,
                Some(db2_backup_dir.path().to_str().unwrap().to_string()),
                Some(db2_wal_dir.path().to_str().unwrap().to_string()),
                Some(shared_manager.clone()),
                false,
                Some(raft_path.clone()),
            );

            write_until_segment_count(&db1_chunks, 0, 3);
            write_until_segment_count(&db2_chunks, 0, 3);

            for chunks in [&db1_chunks, &db2_chunks] {
                for chunk in &chunks.list {
                    chunk.rotate_heads_for_scrub_test();
                    for seg in chunk.segments() {
                        seg.archive().unwrap();
                    }
                }
            }
        }

        let recovery_manager = Arc::new(crate::ram::tiered::manager::TieredMemoryManager::new(
            crate::ram::tiered::SharedMemoryPool::new(&crate::ram::tiered::TieredConfig {
                threshold: 0.75,
                lower_watermark: 0.5,
                physical_memory_limit: 4 * SEGMENT_SIZE,
                promotion_cooldown_ms: 0,
            }),
        ));

        let recovered_db1 = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 8,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(db1_backup_dir.path().to_str().unwrap().to_string()),
            Some(db1_wal_dir.path().to_str().unwrap().to_string()),
            Some(recovery_manager.clone()),
            true,
            Some(raft_path.clone()),
        );
        let recovered_db2 = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 8,
            Arc::new(ServerMeta {
                schemas: setup_test_schema(),
            }),
            None,
            Some(db2_backup_dir.path().to_str().unwrap().to_string()),
            Some(db2_wal_dir.path().to_str().unwrap().to_string()),
            Some(recovery_manager.clone()),
            true,
            Some(raft_path.clone()),
        );

        let combined_hot = total_hot_segments(&recovered_db1) + total_hot_segments(&recovered_db2);
        let combined_cold =
            total_cold_segments(&recovered_db1) + total_cold_segments(&recovered_db2);

        assert_eq!(
            combined_hot, 4,
            "recovery should keep total hot segments within the shared server-wide physical limit"
        );
        assert!(
            combined_cold >= 2,
            "the second recovered database should spill segments to cold storage when the shared hot budget is exhausted"
        );
        assert_eq!(
            recovery_manager.shared_pool().total_hot_segments(),
            combined_hot,
            "shared pool accounting should match recovered hot segments across databases"
        );
    }

    /// The per-slot live-bytes counters follow every logical mutation:
    /// insert adds the entry's content length, update adjusts by the delta,
    /// remove settles to zero. Checked against the entry actually written,
    /// not against assumed sizes.
    #[test]
    fn slot_live_bytes_follow_writes_updates_and_removes() {
        let _ = env_logger::try_init();
        let chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let chunk = &chunks.list[0];
        let schema = schema_with_id(214, "slot_bytes_live_path", false);
        chunk.meta.schemas.debug_only_new_schema(schema.clone());

        let len_at = |id: &Id| -> u64 {
            let (entry, _) = Entry::decode_from(chunks.address_of(id), |_, header| header);
            entry.content_length as u64
        };

        let a = Id::allocated(50, 0, 1);
        let b = Id::allocated(50, 0, 2);
        let c = Id::allocated(51, 0, 1);

        let mut cell_a = OwnedCell {
            header: CellHeader::new(schema.vid, &a),
            data: data_map_value!(id: 1_i32, data: vec![0x11_u8; DATA_SIZE]),
        };
        chunks.write_cell(&mut cell_a).unwrap();
        let len_a = len_at(&a);
        assert!(len_a > 0);
        assert_eq!(
            chunks.slot_bytes.get(50),
            len_a,
            "insert must add the entry length"
        );

        let mut cell_b = OwnedCell {
            header: CellHeader::new(schema.vid, &b),
            data: data_map_value!(id: 2_i32, data: vec![0x22_u8; DATA_SIZE]),
        };
        chunks.write_cell(&mut cell_b).unwrap();
        let len_b = len_at(&b);
        assert_eq!(chunks.slot_bytes.get(50), len_a + len_b);

        // Update A to a larger body: the counter moves by the delta.
        let mut cell_a2 = OwnedCell {
            header: CellHeader::new(schema.vid, &a),
            data: data_map_value!(id: 1_i32, data: vec![0x33_u8; DATA_SIZE * 3]),
        };
        chunks.update_cell(&mut cell_a2).unwrap();
        let len_a2 = len_at(&a);
        assert!(len_a2 > len_a, "the update was supposed to grow the entry");
        assert_eq!(chunks.slot_bytes.get(50), len_a2 + len_b);

        // Upsert into another slot counts there, not here.
        let mut cell_c = OwnedCell {
            header: CellHeader::new(schema.vid, &c),
            data: data_map_value!(id: 3_i32, data: vec![0x44_u8; DATA_SIZE]),
        };
        chunks.upsert_cell(&mut cell_c).unwrap();
        let len_c = len_at(&c);
        assert_eq!(chunks.slot_bytes.get(51), len_c);
        assert_eq!(chunks.slot_bytes.get(50), len_a2 + len_b);

        // Upsert over an existing cell behaves as the update it is.
        let mut cell_c2 = OwnedCell {
            header: CellHeader::new(schema.vid, &c),
            data: data_map_value!(id: 3_i32, data: vec![0x55_u8; DATA_SIZE * 2]),
        };
        chunks.upsert_cell(&mut cell_c2).unwrap();
        let len_c2 = len_at(&c);
        assert_eq!(chunks.slot_bytes.get(51), len_c2);

        chunks.remove_cell(&b).unwrap();
        assert_eq!(
            chunks.slot_bytes.get(50),
            len_a2,
            "remove must subtract what it removed"
        );
        chunks.remove_cell(&a).unwrap();
        assert_eq!(chunks.slot_bytes.get(50), 0);
        assert_eq!(
            chunks.total_live_bytes(),
            len_c2,
            "only C is left, so the total is exactly its entry"
        );
        assert_eq!(chunks.slot_live_bytes(&[50, 51]), vec![0, len_c2]);
    }

    /// Recovery seeds the counters to exactly what the live path had --
    /// including a cell whose newest entry in the image is a tombstone, which
    /// must not count.
    #[test]
    fn slot_live_bytes_survive_recovery() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let (_raft_dir, raft_path) = temp_raft_dir();

        let writer_chunks = Chunks::new_dummy(1, TEST_SEGMENT_SIZE * 4);
        let writer_chunk = &writer_chunks.list[0];
        let schema = schema_with_id(215, "slot_bytes_recovery", false);
        writer_chunk
            .meta
            .schemas
            .debug_only_new_schema(schema.clone());

        for (locality, seq, fill) in [
            (40_u16, 1_u64, 0x0A_u8),
            (40, 2, 0x0B),
            (40, 3, 0x0C),
            (41, 1, 0x0D),
            (41, 2, 0x0E),
        ] {
            let id = Id::allocated(locality, 0, seq);
            let mut cell = OwnedCell {
                header: CellHeader::new(schema.vid, &id),
                data: data_map_value!(id: seq as i32, data: vec![fill; DATA_SIZE]),
            };
            writer_chunks.write_cell(&mut cell).unwrap();
        }
        // One removal, so the image carries a tombstone newer than its cell.
        writer_chunks.remove_cell(&Id::allocated(40, 0, 2)).unwrap();

        let expected_40 = writer_chunks.slot_bytes.get(40);
        let expected_41 = writer_chunks.slot_bytes.get(41);
        assert!(expected_40 > 0 && expected_41 > 0);

        // The head segment's live image, exactly as written: entries, the
        // tombstone, real alignment.
        let head_id = writer_chunk.get_head_seg_id();
        let head = writer_chunk.segs.get(&(head_id as usize)).unwrap();
        let live_len = head.append_header.load(Ordering::Relaxed) - head.addr;
        let image =
            unsafe { std::slice::from_raw_parts(head.addr as *const u8, live_len).to_vec() };
        write_backup_segment(&backup_dir, 0, head_id, 1, &image);

        let recovered_schemas = setup_test_schema();
        recovered_schemas.debug_only_new_schema(schema.clone());
        let recovered = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 4,
            Arc::new(ServerMeta {
                schemas: recovered_schemas,
            }),
            None,
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            None,
            true,
            Some(raft_path),
        );

        // Not `count_recovered_cells`: a tombstoned hash keeps its (zeroed)
        // index key, so the length over-counts. Read the cells instead.
        for (locality, seq) in [(40_u16, 1_u64), (40, 3), (41, 1), (41, 2)] {
            recovered
                .read_cell(&Id::allocated(locality, 0, seq))
                .unwrap_or_else(|e| panic!("cell ({locality},{seq}) not recovered: {e:?}"));
        }
        assert!(
            recovered.read_cell(&Id::allocated(40, 0, 2)).is_err(),
            "the tombstoned cell must stay dead through recovery"
        );
        assert_eq!(
            recovered.slot_bytes.get(40),
            expected_40,
            "slot 40 must recover to its pre-crash live bytes, tombstoned cell excluded"
        );
        assert_eq!(recovered.slot_bytes.get(41), expected_41);
        assert_eq!(recovered.total_live_bytes(), expected_40 + expected_41);
    }
    #[test]
    fn cleaner_keeps_transaction_after_lease_retirement() {
        let store = BracketCleanerStore::new(8933, "cleaner_lease_retired");
        let chunks = store.open_sized(false, TEST_SEGMENT_SIZE * 8);
        let chunk = &chunks.list[0];
        let txn = crate::server::transactions::test_hlc(8933, 1);
        let id = Id::allocated(35, 0, 1);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&id, 1024)).unwrap();
        }
        let segment = segment_of(&chunks, &id);
        assert!(crate::ram::cleaner::SegmentCandidate::for_cleaner(&segment).is_none());
        crate::ram::chunk::retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
        rotate_out(chunk, segment.id);
        assert!(!segment.is_head_owned());
        assert!(segment.no_references());
        assert!(crate::ram::cleaner::SegmentCandidate::for_cleaner(&segment).is_none());
        assert!(!segment.archive().unwrap());
        chunk.remove_segment(segment.id);
        assert!(chunk.segs.get(&(segment.id as usize)).is_some());
        drop(crate::ram::chunk::take_transactional_segments());
    }

    #[test]
    fn recovered_resolved_parts_release_pins_after_plain_rewrite() {
        let store = BracketCleanerStore::new(9844, "resolved_parts_plain");
        let txn = crate::server::transactions::test_hlc(9844, 1);
        let id = Id::allocated(35, 0, 5);
        {
            let chunks = store.open(false);
            chunks.stop_background();
            {
                let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
                chunks.write_cell(&mut store.cell(&id, 1024)).unwrap();
            }
            for segment in crate::ram::chunk::take_transactional_segments() {
                segment.force_wal_sync().unwrap();
            }
            crate::ram::chunk::retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
            chunks.list[0].write_resolved_commit(&txn, &[]).unwrap();
        }
        {
            let recovered = store.open(true);
            recovered.stop_background();
            assert!(recovered.read_cell(&id).is_ok());
            assert_eq!(
                recovered.pinned_segments(),
                0,
                "a resolved commit must release its old parts"
            );
            let live = segment_of(&recovered, &id);
            assert!(
                live.entry_iter().all(|entry| !matches!(
                    entry.entry_header.entry_type,
                    EntryType::BEGIN | EntryType::COMMIT
                )),
                "the live entry must no longer depend on a transaction marker"
            );
            recovered.list[0].rotate_heads_for_scrub_test();
        }
        let restarted = store.open(true);
        assert!(
            restarted.read_cell(&id).is_ok(),
            "plain rewritten entries must survive another restart"
        );
        assert_eq!(restarted.pinned_segments(), 0);
    }

    #[test]
    fn prepare_and_decision_sync_wait_for_a_cleaner_claim() {
        let store = BracketCleanerStore::new(9842, "cleaner_sync_race");
        let chunks = store.open_sized(false, TEST_SEGMENT_SIZE * 8);
        let txn = crate::server::transactions::test_hlc(9842, 1);
        let id = Id::allocated(35, 0, 4);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&id, 1024)).unwrap();
        }
        let segment = segment_of(&chunks, &id);
        for decision in [false, true] {
            let claim = segment.protection.maintenance().unwrap();
            std::thread::scope(|scope| {
                let (send, receive) = std::sync::mpsc::channel();
                let chunks = &chunks;
                let txn = &txn;
                let segment = &segment;
                scope.spawn(move || {
                    let result = if decision {
                        chunks
                            .commit_transaction_brackets(txn, false)
                            .map_err(|refusal| refusal.error().clone())
                    } else {
                        segment
                            .force_wal_sync()
                            .map_err(crate::ram::cell::WriteError::from_wal_io)
                    };
                    send.send(result).unwrap();
                });
                let early = receive.recv_timeout(std::time::Duration::from_millis(30));
                drop(claim);
                assert!(
                    early.is_err(),
                    "sync refused the cleaner claim instead of waiting: {early:?}"
                );
                receive
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .unwrap()
                    .unwrap();
            });
        }
        crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range()).unwrap();
        assert!(!segment.protection.transaction_pending());
        drop(crate::ram::chunk::take_transactional_segments());
    }

    #[test]
    fn prepare_sync_proves_wal_durability_without_receipt_capture() {
        let store = BracketCleanerStore::new(8935, "cleaner_prepare_proof");
        let chunks = store.open_sized(false, TEST_SEGMENT_SIZE * 8);
        let txn = crate::server::transactions::test_hlc(8935, 1);
        let id = Id::allocated(35, 0, 3);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&id, 1024)).unwrap();
        }
        let segment = segment_of(&chunks, &id);
        segment.force_wal_sync().unwrap();
        assert!(
            segment.durable_receipt().unwrap().is_none(),
            "the prepare sync must cover both the WAL prefix and its namespace"
        );
        chunks.commit_transaction_brackets(&txn, false).unwrap();
        crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range()).unwrap();
        drop(crate::ram::chunk::take_transactional_segments());
    }

    #[test]
    fn cleaner_keeps_transaction_without_durable_decision_evidence() {
        let store = BracketCleanerStore::new(8934, "cleaner_archive_only");
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(store.schema.clone());
        let chunks = Chunks::new(
            1,
            TEST_SEGMENT_SIZE * 8,
            Arc::new(ServerMeta { schemas }),
            None,
            Some(store.backup_dir.path().to_string_lossy().into_owned()),
            None,
            None,
        );
        let txn = crate::server::transactions::test_hlc(8934, 1);
        let id = Id::allocated(35, 0, 2);
        {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            chunks.write_cell(&mut store.cell(&id, 1024)).unwrap();
        }
        let segment = segment_of(&chunks, &id);
        chunks.commit_transaction_brackets(&txn, false).unwrap();
        crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range()).unwrap();
        rotate_out(&chunks.list[0], segment.id);
        assert!(segment.protection.transaction_pending());
        assert!(crate::ram::cleaner::SegmentCandidate::for_cleaner(&segment).is_none());
        assert!(!segment.archive().unwrap());
        drop(crate::ram::chunk::take_transactional_segments());
    }

    /// A compacted part must not lose the only surviving transaction decision.

    #[test]
    fn durable_commit_power_cut_after_compacting_one_part_is_atomic() {
        power_cut_after_compacting_one_part(true);
    }

    fn power_cut_after_compacting_one_part(durable: bool) {
        let store = BracketCleanerStore::new(8931, "txn_power_cut_compact");
        let chunks = store.open_sized(false, TEST_SEGMENT_SIZE * 16);
        let chunk = &chunks.list[0];
        let (ordinary, ordinary_seg) = ordinary_rotated_segment(&store, &chunks, 500);
        let txn = crate::server::transactions::test_hlc(8931, 1);
        let ids: Vec<_> = (1..=18).map(|n| Id::allocated(33, 0, n)).collect();
        let (_, _receipts) = crate::ram::durability::capture(|| {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            for id in &ids {
                chunks.write_cell(&mut store.cell(id, 512 * 1024)).unwrap();
            }
        });
        let first = segment_of(&chunks, ids.last().unwrap());
        let last = segment_of(&chunks, &ids[0]);
        assert_ne!(first.id, last.id, "the transaction must span parts");
        let entries = crate::ram::chunk::take_transactional_segments();
        for segment in &entries {
            segment.force_wal_sync().unwrap();
        }
        for segment in &entries {
            assert!(
                segment.durable_receipt().unwrap().is_none(),
                "round 1 must cover every entry before a decision"
            );
        }
        let entries_synced = last.wal_append_position();
        let (result, _receipts) =
            crate::ram::durability::capture(|| chunks.commit_transaction_brackets(&txn, false));
        result.unwrap();
        let last_synced = last.durable_file().unwrap().watermark();
        assert!(last.wal_append_position() > entries_synced);
        if durable {
            assert_eq!(last_synced, last.wal_append_position());
        } else {
            assert_eq!(last_synced, entries_synced);
        }
        crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range()).unwrap();
        // Compact only the small part. Keep the full part's COMMIT above its synced prefix.
        rotate_out(chunk, first.id);
        let candidates: Vec<_> = chunk
            .segments()
            .into_iter()
            .filter(|seg| seg.id == first.id || seg.id == ordinary_seg)
            .collect();
        assert_eq!(candidates.len(), 2);
        let (_, collected) =
            crate::ram::cleaner::combine::CombinedCleaner::combine_segments(chunk, &candidates);
        if durable {
            assert!(collected > 0);
            assert!(chunk.segments().iter().all(|seg| seg.id != first.id));
        }
        // Complete physical retirement before copying the crash image.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !chunk.retired_segments.lock().is_empty() {
            chunk.drain_retired_segments();
            assert!(
                std::time::Instant::now() < deadline,
                "retired files did not drain"
            );
            std::thread::yield_now();
        }
        let cut = BracketCleanerStore::new(8931, "txn_power_cut_compact");
        fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
            std::fs::create_dir_all(to).unwrap();
            for item in std::fs::read_dir(from).unwrap() {
                let item = item.unwrap();
                let destination = to.join(item.file_name());
                if item.path().is_dir() {
                    copy_tree(&item.path(), &destination);
                } else {
                    std::fs::copy(item.path(), destination).unwrap();
                }
            }
        }
        copy_tree(store.backup_dir.path(), cut.backup_dir.path());
        copy_tree(store.wal_dir.path(), cut.wal_dir.path());
        let source = chunk
            .file_manager
            .wal_path(last.chunk_id, last.id, last.seq_id)
            .unwrap();
        let relative = std::path::Path::new(&source)
            .strip_prefix(store.wal_dir.path())
            .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(cut.wal_dir.path().join(relative))
            .unwrap()
            .set_len(last_synced)
            .unwrap();
        let recovered = cut.open_sized(true, TEST_SEGMENT_SIZE * 16);
        let present = ids
            .iter()
            .filter(|id| recovered.read_cell(id).is_ok())
            .count();
        assert!(
            present == 0 || present == ids.len(),
            "power loss recovered {present}/{} transaction cells",
            ids.len()
        );
        if !durable {
            assert_eq!(collected, 0, "an unsynced part must prevent compaction");
            assert!(chunk.segments().iter().any(|seg| seg.id == first.id));
        }
        for id in ordinary {
            assert!(recovered.read_cell(&id).is_ok());
        }
    }
    fn transaction_cut_across_chunks(sync_commits: usize, durable: bool, refuse: bool) -> usize {
        transaction_cut_at_decision(sync_commits, durable, refuse, false)
    }

    fn transaction_cut_at_decision(
        sync_commits: usize,
        durable: bool,
        refuse: bool,
        before_commit: bool,
    ) -> usize {
        let live = BracketCleanerStore::new(8932, "txn_power_cut_chunks");
        let cut = BracketCleanerStore::new(8932, "txn_power_cut_chunks");
        let open = |store: &BracketCleanerStore, recover| {
            let schemas = LocalSchemasCache::new_local("");
            schemas.debug_only_new_schema(store.schema.clone());
            Chunks::new_with_recovery(
                2,
                TEST_SEGMENT_SIZE * 16,
                Arc::new(ServerMeta { schemas }),
                None,
                Some(store.backup_dir.path().to_string_lossy().into_owned()),
                Some(store.wal_dir.path().to_string_lossy().into_owned()),
                None,
                recover,
                Some(store.raft_path.clone()),
            )
        };
        let chunks = open(&live, false);
        let txn = crate::server::transactions::test_hlc(8932, 1);
        let ids: Vec<_> = (0..2)
            .flat_map(|locality| (1..=18).map(move |n| Id::allocated(locality, 0, n)))
            .collect();
        let (_, _receipts) = crate::ram::durability::capture(|| {
            let _scope = crate::ram::chunk::TransactionScope::enter(txn.clone());
            for id in &ids {
                chunks.write_cell(&mut live.cell(id, 512 * 1024)).unwrap();
            }
        });
        let entries = crate::ram::chunk::take_transactional_segments();
        assert_eq!(entries.len(), 4, "2 parts in each of 2 chunks");
        for segment in &entries {
            segment.force_wal_sync().unwrap();
        }
        for segment in &entries {
            assert!(
                segment.durable_receipt().unwrap().is_none(),
                "round 1 must cover every entry before a decision"
            );
        }
        if refuse {
            // Chunk 0 closes before chunk 1 refuses its first COMMIT.
            let segment = entries
                .iter()
                .find(|segment| segment.chunk_id == 1)
                .unwrap();
            segment.retire_tail();
        }
        let (result, _receipts) = crate::ram::durability::capture(|| {
            if before_commit {
                Ok(())
            } else {
                chunks.commit_transaction_brackets(&txn, false)
            }
        });
        if refuse {
            assert!(result.is_err());
        } else {
            result.unwrap();
            if !before_commit {
                crate::ram::chunk::release_transaction_leases(&txn, &chunks.address_range())
                    .unwrap();
            }
        }
        for segment in entries.iter().take(sync_commits) {
            segment.force_wal_sync().unwrap();
        }
        for chunk in &chunks.list {
            for segment in chunk.segments() {
                let source = chunk
                    .file_manager
                    .wal_path(segment.chunk_id, segment.id, segment.seq_id)
                    .unwrap();
                if !std::path::Path::new(&source).exists() {
                    continue;
                }
                let relative = std::path::Path::new(&source)
                    .strip_prefix(live.wal_dir.path())
                    .unwrap();
                let destination = cut.wal_dir.path().join(relative);
                std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
                std::fs::copy(source, &destination).unwrap();
                let synced = segment
                    .durable_file()
                    .map(|file| file.watermark())
                    .unwrap_or(crate::ram::wal_format::FILE_HEADER_SIZE as u64);
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(destination)
                    .unwrap()
                    .set_len(synced)
                    .unwrap();
            }
        }
        let recovered = open(&cut, true);
        let present = ids
            .iter()
            .filter(|id| recovered.read_cell(id).is_ok())
            .count();
        assert!(
            present == 0 || present == ids.len(),
            "recovered {present}/{} cells",
            ids.len()
        );
        if before_commit {
            let parts: Vec<_> = recovered
                .list
                .iter()
                .flat_map(|chunk| chunk.segments())
                .filter(|segment| segment.protection.transaction_pending())
                .collect();
            assert_eq!(
                parts.len(),
                4,
                "every recovered part needs its own decision proof"
            );
            for part in parts {
                assert!(crate::ram::cleaner::SegmentCandidate::for_cleaner(&part).is_none());
            }
        }
        if !refuse && present == 0 {
            assert_eq!(recovered.take_in_doubt_brackets().len(), 1);
        }
        if refuse || before_commit {
            crate::ram::chunk::retire_transaction_leases(&txn, &chunks.address_range()).unwrap();
        }
        present
    }

    #[test]
    fn transaction_cut_before_any_commit_append_is_absent() {
        for durable in [false, true] {
            assert_eq!(transaction_cut_at_decision(0, durable, false, true), 0);
        }
    }

    #[test]
    fn durable_transaction_cut_after_ack_is_complete() {
        assert_eq!(transaction_cut_across_chunks(0, true, false), 36);
    }

    #[test]
    fn refused_commit_cut_retracts_the_closed_chunk() {
        assert_eq!(transaction_cut_across_chunks(0, false, true), 0);
        assert_eq!(transaction_cut_across_chunks(0, true, true), 0);
    }
}
