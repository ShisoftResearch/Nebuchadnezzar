//! How far the ranged index is known to be durable, expressed in the only
//! terms that survive a crash: positions in the segment journals.
//!
//! # Why a mark, and not just a scrub
//!
//! Index entries are DERIVABLE. `scrub_ranged_index` rebuilds them from the
//! cells in the segments through the write path's own `probe_cell_indices`,
//! so an entry missing after a restart is a repair job rather than a loss.
//! That makes "reconcile the index against the cells before serving" the
//! correct startup behaviour: the cells are authoritative, and anything else
//! is a cache of them.
//!
//! What stops that from being free is scale. Reconciling costs one derive and
//! one index lookup per live cell -- measured at roughly 800k cells/second --
//! which is seconds for a small store and tens of minutes for a terabyte one.
//! Paying it on every start, for cells that have been durably indexed for
//! weeks, is not a trade anyone wants.
//!
//! The mark bounds it. Written only when the index has actually been made
//! durable (drain, flush, write-back barrier, all successful), it records
//! where every segment's append cursor stood at the moment that flush began.
//! Everything before those positions is durably indexed; everything at or
//! after them is what a restart has to re-derive. So the cost scales with the
//! crash window rather than with the store.
//!
//! # What it deliberately does NOT assume
//!
//! **Not a single position per chunk.** The head pool gives a chunk several
//! segments taking writes at once, so there is no total order over a chunk's
//! appends to compare against. Every segment gets its own cursor.
//!
//! **Not "sealed implies indexed".** A segment can be sealed and archived
//! while entries for its cells are still only in memory, so segment lifecycle
//! says nothing about index durability. Only the flush does, which is why the
//! mark is written at the flush and nowhere else.
//!
//! **Not a reason to trust a missing file.** No mark means reconcile
//! everything. An absent, unreadable or checksum-failing mark all mean the
//! same thing and take the same path -- the expensive, correct one.
//!
//! # STATUS: OFF BY DEFAULT, and not because of caution
//!
//! Four conditions gate the mark and they are measurably NOT sufficient. On a
//! 96 GB store with room to work, a bounded pass skipped 3,139,279 cells and
//! an independent scrub then found 721 entries missing among them -- with the
//! previous cycle's reconciliation reporting everything it found repaired, no
//! index task failed, and no index page abandoned. Something else can lose an
//! entry from an in-memory tree between a reconcile proving agreement and the
//! shutdown that makes it durable, and none of the four conditions sees it.
//!
//! The arithmetic argues against hunting a fifth: a full reconcile costs
//! 1.0-4.6 s for 0.7-3.2M cells and reports missing=0 reliably, so the bound
//! saves a couple of seconds per restart while risking silently missing index
//! entries. Enable with `NEB_INDEX_MARK=trust` if you have measured your own
//! store and want the trade.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;

use crc_fast::{CrcAlgorithm, Digest};

use crate::ram::chunk::Chunks;

const MARK_MAGIC: &[u8; 8] = b"NEBIDXMK";
/// Bumped when what a mark MEANS changes, not only when its bytes do.
///
/// Version 1 meant "everything below these cursors was written before the last
/// flush", which is unsound: a cell whose index task died in a crash is OLD, so
/// it sits below any later cursor and a v1 mark declares it covered forever.
/// Version 2 means "a reconciliation PROVED the index agreed with the cells,
/// nothing failed to index afterwards, and then it was made durable".
///
/// The layout is byte-identical, so a v1 file would decode perfectly and be
/// believed. That is precisely why the version had to move: refusing an old
/// mark costs one full reconciliation, trusting one costs the holes it was
/// supposed to fill.
const MARK_VERSION: u32 = 2;
const MARK_FILENAME: &str = "index-durable.mark";

fn crc32c(data: &[u8]) -> u32 {
    let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
    digest.update(data);
    digest.finalize() as u32
}

/// Where one segment's append cursor stood when the index was last flushed.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SegmentMark {
    pub seq_id: u64,
    /// Offset from the segment's base, not an address: addresses move between
    /// runs and this record has to mean the same thing after a restart.
    pub offset: u64,
}

/// The whole store's position, keyed by chunk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexMark {
    chunks: HashMap<u32, Vec<SegmentMark>>,
}

impl IndexMark {
    /// Read every chunk's segment cursors as they stand right now.
    ///
    /// Call this BEFORE draining index tasks, never after. The point of the
    /// mark is "entries for everything below here are durable", and that only
    /// becomes true once the drain and the flush that follow have run -- so
    /// the position has to be the one they started from. Snapshotting after
    /// the flush would name cells the flush never saw.
    pub fn snapshot(chunks: &Chunks) -> Self {
        let mut map = HashMap::new();
        for chunk in &chunks.list {
            let marks: Vec<SegmentMark> = chunk
                .segments()
                .into_iter()
                .map(|seg| SegmentMark {
                    seq_id: seg.seq_id,
                    offset: (seg.append_header.load(Ordering::Acquire).saturating_sub(seg.addr))
                        as u64,
                })
                .collect();
            map.insert(chunk.id as u32, marks);
        }
        Self { chunks: map }
    }

    /// Whether a cell entry at `offset` in this segment was already covered.
    ///
    /// A segment the mark never saw is not covered: it was created after the
    /// flush, so nothing in it can have been in that flush.
    pub fn covers(&self, chunk_id: usize, seq_id: u64, offset: u64) -> bool {
        self.chunks
            .get(&(chunk_id as u32))
            .and_then(|marks| marks.iter().find(|m| m.seq_id == seq_id))
            .map(|m| offset < m.offset)
            .unwrap_or(false)
    }

    /// How many segment positions this mark carries; for logging and tests.
    pub fn len(&self) -> usize {
        self.chunks.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.len() * 16);
        out.extend_from_slice(MARK_MAGIC);
        out.extend_from_slice(&MARK_VERSION.to_le_bytes());
        out.extend_from_slice(&(self.chunks.len() as u32).to_le_bytes());
        // Sorted so the same state encodes to the same bytes; a record that
        // differs run to run for no reason is one nobody can diff.
        let mut chunk_ids: Vec<_> = self.chunks.keys().copied().collect();
        chunk_ids.sort_unstable();
        for chunk_id in chunk_ids {
            let marks = &self.chunks[&chunk_id];
            out.extend_from_slice(&chunk_id.to_le_bytes());
            out.extend_from_slice(&(marks.len() as u32).to_le_bytes());
            let mut sorted = marks.clone();
            sorted.sort_unstable_by_key(|m| m.seq_id);
            for mark in sorted {
                out.extend_from_slice(&mark.seq_id.to_le_bytes());
                out.extend_from_slice(&mark.offset.to_le_bytes());
            }
        }
        let checksum = crc32c(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 20 || &bytes[..8] != MARK_MAGIC {
            return None;
        }
        let body = &bytes[..bytes.len() - 4];
        let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().ok()?);
        if crc32c(body) != stored {
            return None;
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().ok()?);
        if version != MARK_VERSION {
            // No backward compatibility, by house rule and by safety. There is
            // no "read the old one carefully" path here: an older mark encodes
            // a WEAKER claim than this build acts on, and acting on a weaker
            // claim than you think is the whole failure mode. Refusing costs
            // one full reconciliation, which is always correct.
            return None;
        }
        let chunk_count = u32::from_le_bytes(bytes[12..16].try_into().ok()?) as usize;
        let mut cursor = 16usize;
        let mut chunks = HashMap::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            if cursor + 8 > body.len() {
                return None;
            }
            let chunk_id = u32::from_le_bytes(body[cursor..cursor + 4].try_into().ok()?);
            let seg_count =
                u32::from_le_bytes(body[cursor + 4..cursor + 8].try_into().ok()?) as usize;
            cursor += 8;
            let mut marks = Vec::with_capacity(seg_count);
            for _ in 0..seg_count {
                if cursor + 16 > body.len() {
                    return None;
                }
                let seq_id = u64::from_le_bytes(body[cursor..cursor + 8].try_into().ok()?);
                let offset = u64::from_le_bytes(body[cursor + 8..cursor + 16].try_into().ok()?);
                marks.push(SegmentMark { seq_id, offset });
                cursor += 16;
            }
            chunks.insert(chunk_id, marks);
        }
        Some(Self { chunks })
    }

    fn path(backup_storage: &str) -> String {
        format!("{}/{}", backup_storage, MARK_FILENAME)
    }

    /// Write the mark durably: temp file, fsync, rename, fsync the directory.
    ///
    /// The directory fsync is not optional. A rename that reaches the inode
    /// but not the directory entry leaves the mark absent after a crash --
    /// which is safe (reconcile everything) but wastes the whole point of
    /// having written it.
    pub fn save(&self, backup_storage: &str) -> io::Result<()> {
        use std::io::Write;
        let final_path = Self::path(backup_storage);
        let temp_path = format!("{}.tmp", final_path);
        {
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(&self.encode())?;
            file.sync_all()?;
        }
        std::fs::rename(&temp_path, &final_path)?;
        if let Ok(dir) = std::fs::File::open(Path::new(backup_storage)) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Read the mark, or `None` for absent / unreadable / corrupt.
    ///
    /// All three answers are the same answer on purpose. The caller's job when
    /// there is no trustworthy mark is to reconcile everything, and that is
    /// correct whichever of the three happened.
    pub fn load(backup_storage: &str) -> Option<Self> {
        let bytes = std::fs::read(Self::path(backup_storage)).ok()?;
        match Self::decode(&bytes) {
            Some(mark) => Some(mark),
            None => {
                log::warn!(
                    "index durability mark at {} is unreadable; reconciling the whole store \
                     against its cells instead",
                    Self::path(backup_storage)
                );
                None
            }
        }
    }

    /// Remove the mark, so a later start reconciles everything.
    ///
    /// Used when something has happened that the mark cannot describe.
    pub fn clear(backup_storage: &str) {
        let _ = std::fs::remove_file(Self::path(backup_storage));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark_of(entries: &[(u32, &[(u64, u64)])]) -> IndexMark {
        let mut chunks = HashMap::new();
        for (chunk_id, segs) in entries {
            chunks.insert(
                *chunk_id,
                segs.iter()
                    .map(|(seq_id, offset)| SegmentMark {
                        seq_id: *seq_id,
                        offset: *offset,
                    })
                    .collect(),
            );
        }
        IndexMark { chunks }
    }

    #[test]
    fn a_mark_round_trips() {
        let mark = mark_of(&[(0, &[(1, 4096), (2, 128)]), (7, &[(9, 0)])]);
        let decoded = IndexMark::decode(&mark.encode()).expect("decodes");
        assert_eq!(decoded, mark);
        assert_eq!(decoded.len(), 3);
    }

    /// Every way of being untrustworthy has to produce the same answer, because
    /// the caller's response to `None` -- reconcile everything -- is the only
    /// safe one and is correct for all of them.
    #[test]
    fn a_damaged_mark_reads_as_absent() {
        let mark = mark_of(&[(0, &[(1, 4096)])]);
        let good = mark.encode();

        let mut flipped = good.clone();
        let last = flipped.len() - 6;
        flipped[last] ^= 0xFF;
        assert_eq!(
            IndexMark::decode(&flipped),
            None,
            "a mutated body must fail its checksum"
        );

        let mut wrong_version = good.clone();
        wrong_version[8] = 99;
        assert_eq!(
            IndexMark::decode(&wrong_version),
            None,
            "an unknown version is refused, not guessed at"
        );

        // Specifically: the PREVIOUS version, whose layout is byte-identical
        // and whose claim was weaker. It has to be refused on the version
        // alone, because nothing in the bytes distinguishes it.
        let mut v1 = good.clone();
        v1[8..12].copy_from_slice(&1u32.to_le_bytes());
        let body = v1.len() - 4;
        let fixed = crc32c(&v1[..body]);
        v1[body..].copy_from_slice(&fixed.to_le_bytes());
        assert_eq!(
            IndexMark::decode(&v1),
            None,
            "a v1 mark decodes perfectly and means something weaker; trusting it \
             is how 883,009 entries stayed hidden"
        );

        assert_eq!(IndexMark::decode(b"NEBIDXMK"), None, "a truncated file");
        assert_eq!(IndexMark::decode(b"not a mark at all"), None, "no magic");
    }

    /// A mark may only be written when the chain that makes it TRUE held.
    ///
    /// The claim is "verified complete to here, then made durable". Position
    /// alone does not establish it: a cell whose index task died in a crash is
    /// OLD, so it sits below any later cursor, and a position-only mark
    /// declares it covered -- forever, because every subsequent mark is
    /// further ahead. Measured before this gate existed: one soak cycle
    /// repaired 878,578 entries, shut down gracefully, and the next start --
    /// bounded by that shutdown's own mark -- came back needing 883,009.
    ///
    /// So a reconciliation has to have PROVED agreement, and nothing may have
    /// failed to index since.
    #[test]
    fn a_mark_is_only_honest_after_a_verified_reconcile() {
        use crate::ram::chunk::Chunks;
        let chunks = Chunks::new_dummy(1, 8 * 1024 * 1024);

        assert!(
            !chunks.index_still_verified(0),
            "a fresh store has proved nothing; a mark now would be a guess"
        );

        chunks.note_index_verified_complete(7);
        assert!(
            chunks.index_still_verified(7),
            "a reconciliation that proved agreement, with the failure count \
             unchanged, is exactly when the mark is honest"
        );
        assert!(
            !chunks.index_still_verified(8),
            "one index task failing since is one cell whose entries are missing, \
             and a mark written now would bury it"
        );

        // And the failure that reports no failure. A store that runs out of
        // room abandons index page write-back, so entries go missing with
        // every index task returning success -- the task inserted into the
        // in-memory tree and the PAGE could not be placed. Measured: a bounded
        // pass skipped 3,434,589 cells on a store that had exhausted
        // mid-session, and an independent scrub found 75,085 entries missing
        // among them.
        crate::index::ranged::tree::btree::storage::INDEX_PAGES_ABANDONED
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(
            !chunks.index_still_verified(7),
            "an abandoned index page is an entry that never reached disk, however \
             healthy the task counters look"
        );
    }

    /// The whole point of the record: what it says is already indexed, and
    /// what a restart therefore still has to derive.
    #[test]
    fn coverage_stops_at_the_recorded_cursor() {
        let mark = mark_of(&[(3, &[(11, 4096)])]);

        assert!(mark.covers(3, 11, 0), "the start of a recorded segment");
        assert!(mark.covers(3, 11, 4095), "just below the cursor");
        assert!(
            !mark.covers(3, 11, 4096),
            "AT the cursor is not covered: the cursor is where the next append \
             goes, so nothing has been indexed there yet"
        );
        assert!(!mark.covers(3, 11, 9000), "past the cursor");

        assert!(
            !mark.covers(3, 12, 0),
            "a segment the mark never saw was created after the flush, so \
             nothing in it can have been in that flush"
        );
        assert!(!mark.covers(4, 11, 0), "a chunk the mark never saw");
    }
}
