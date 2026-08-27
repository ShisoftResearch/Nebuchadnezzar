use itertools::Itertools;
use lightning::map::{Map as LFMap, PtrHashMap};
use rayon::prelude::*;
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    iter,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use dovahkiin::types::{Map, SharedValue};

use crate::index::ranged::tree::{btree::PAGE_SCHEMA_ID, tree::RANGED_TREE_SCHEMA_ID};
use crate::ram::{
    cell::{header_from_chunk_raw, select_from_chunk_raw},
    chunk::Chunk,
    clock::now,
    entry::Entry,
    schema::{IndexType, SchemaUid, SchemaVid},
};

#[derive(Debug, Default)]
pub struct SchemaStatistics {
    pub histogram: HashMap<u64, TargetHistogram>,
    pub count: usize,
    pub segs: usize,
    pub bytes: usize,
    pub timestamp: u32,
}

pub struct ChunkStatistics {
    pub timestamp: AtomicU32,
    pub changes: AtomicU32,
    /// Single-flight guard for the refresh. A refresh walks the ENTIRE cell
    /// index -- `entries()` alone materializes ~400 MB for a full 16 GB chunk
    /// -- and the counter/timestamp gate does not stop concurrent writers
    /// from all passing it at once. Without this, a write-heavy phase against
    /// a large chunk had all 192 tokio workers re-scanning the same chunk
    /// concurrently: request handlers starved (edge batches timed out) and
    /// the discarded scan buffers churned >100 GB of allocator free lists.
    refreshing: std::sync::atomic::AtomicBool,
    pub schemas: PtrHashMap<SchemaUid, Arc<SchemaStatistics>>,
}

/// What the last gather actually saw, process-wide. A refresh that yields
/// the right `count` but no histograms is otherwise silent -- the read
/// succeeded, the schema resolved, and every value was skipped -- so the
/// only way to tell "no indexed fields" from "all values null" from "no
/// cells" is to count.
pub static STATS_CELLS_SCANNED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static STATS_FEATURES_PUSHED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static STATS_NULLS_SKIPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static STATS_CELLS_WITHOUT_INDEXED_FIELDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

const HISTOGRAM_PARTITATION_SIZE: usize = 1024;
const HISTOGRAM_PARTITATION_BUCKETS: usize = 100;
#[cfg(test)]
const HISTOGRAM_PARTITATION_KEYS: usize = HISTOGRAM_PARTITATION_BUCKETS + 1;
const HISTOGRAM_TARGET_BUCKETS: usize = HISTOGRAM_PARTITATION_BUCKETS;
const HISTOGRAM_TARGET_KEYS: usize = HISTOGRAM_TARGET_BUCKETS + 1;
const REFRESH_CHANGES_THRESHOLD: u32 = 512;
const MORPHEUS_SPARSE_SIDECAR_SCHEMA_ID_START: u32 = 0xF010;
const MORPHEUS_SPARSE_SIDECAR_SCHEMA_ID_END: u32 = 0xF017;

type HistogramKey = [u8; 8];
type TargetHistogram = [HistogramKey; HISTOGRAM_TARGET_KEYS];

/// Whether a schema's cells are counted at all.
///
/// Takes the GENERATION, deliberately, even though statistics are a per-family
/// aggregate: this runs once per live cell during a gather, and every schema it
/// rejects is an internal one that is registered with a fixed hash-derived id
/// and never evolves. Their family and generation are therefore the same
/// number and the rejection is exact -- while resolving the record first, just
/// to reject a b-tree page, would put a map lookup on the hottest path in the
/// gather.
#[inline]
pub fn schema_tracks_statistics(schema_id: SchemaVid) -> bool {
    schema_id != *RANGED_TREE_SCHEMA_ID
        && schema_id != *PAGE_SCHEMA_ID
        && !(MORPHEUS_SPARSE_SIDECAR_SCHEMA_ID_START..=MORPHEUS_SPARSE_SIDECAR_SCHEMA_ID_END)
            .contains(&schema_id.get())
}

/// At most this many statistics refreshes run process-wide. Refresh cost
/// scales with TOTAL cells in a chunk while its trigger scales with writes,
/// so on a large store every chunk wants to refresh constantly; the refresh
/// runs synchronously on the write path (a tokio worker), and 128 chunks
/// refreshing at once occupied every worker in the runtime -- request
/// handlers starved and edge batches timed out wholesale. Statistics are
/// advisory; requests are not.
const MAX_CONCURRENT_REFRESHES: usize = 4;
static ACTIVE_REFRESHES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Clears the refresh flag even if the scan panics; a poisoned flag would
/// silently disable statistics forever.
struct ResetOnDrop<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for ResetOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl ChunkStatistics {
    pub fn new() -> Self {
        Self {
            timestamp: AtomicU32::new(0),
            changes: AtomicU32::new(0),
            refreshing: std::sync::atomic::AtomicBool::new(false),
            schemas: PtrHashMap::with_capacity(32),
        }
    }
    /// Write-path hook: RECORD ONLY, never refresh. A refresh walks every
    /// cell and can grind for minutes at scale; running it on the writer's
    /// thread put that grind inside tokio worker polls — the id-list shard
    /// workers froze mid-apply for exactly MAX_CONCURRENT_REFRESHES shards
    /// and the whole edge phase wedged (P2AB). Writers bump the change
    /// counter; the chunk sweeper thread does the walking.
    pub fn refresh_from_chunk(&self, _chunk: &Chunk) {
        self.changes.fetch_add(1, Ordering::Relaxed);
    }

    /// Sweeper entry: refresh if the change/interval thresholds say so.
    /// Runs on the dedicated statistics sweeper thread — never on a tokio
    /// worker, never on a write path.
    pub fn sweep_from_chunk(&self, chunk: &Chunk) {
        let last_update = self.timestamp.load(Ordering::Relaxed);
        let refresh_changes = self.changes.load(Ordering::Relaxed);
        // The interval grows with the chunk: a refresh walks every cell, so a
        // fixed 10 s cadence that suited a hundred-thousand-cell chunk turns
        // into a standing full-scan load at tens of millions. One second per
        // million cells keeps refresh cost a bounded fraction of scan cost.
        let min_interval = std::cmp::max(10, (chunk.cell_count() / 1_000_000) as u32);
        if refresh_changes < REFRESH_CHANGES_THRESHOLD || now() - last_update < min_interval {
            return;
        }
        let claimed_changes = self.changes.swap(0, Ordering::AcqRel);
        if claimed_changes < REFRESH_CHANGES_THRESHOLD {
            return;
        }
        // Single flight: exactly one refresh per chunk at a time. A loser
        // returns its claim so the changes still count toward the next
        // refresh instead of vanishing.
        if self
            .refreshing
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            self.changes.fetch_add(claimed_changes, Ordering::Relaxed);
            return;
        }
        let done = ResetOnDrop(&self.refreshing);
        // Process-wide cap, after the per-chunk flag: a loser here also
        // returns its claim and lets a later write retry.
        if ACTIVE_REFRESHES.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            >= MAX_CONCURRENT_REFRESHES
        {
            ACTIVE_REFRESHES.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            self.changes.fetch_add(claimed_changes, Ordering::Relaxed);
            return;
        }
        // Guarded like the flag: a panicking scan must not leak a slot.
        struct SlotGuard;
        impl Drop for SlotGuard {
            fn drop(&mut self) {
                ACTIVE_REFRESHES.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
        }
        let slot = SlotGuard;
        // A refresh walks every cell and can grind for minutes at scale. On a
        // tokio pool worker that grind runs inside poll — and if that worker
        // happens to hold the runtime's IO/time driver, tokio never steals it
        // back: every timer and socket in the process freezes until the walk
        // finishes (observed as a total server wedge in the P2AB edge-phase
        // runs: importer connections dead, coordinator timeouts never firing,
        // all runtime workers idle-parked off the driver). block_in_place
        // hands the worker's core — and with it the driver — to a fresh
        // thread before the walk starts. Non-runtime threads (cleaners,
        // combine pools) and current_thread runtimes (tests) run inline as
        // before; neither can hold a multi-thread driver.
        let on_multithread_runtime = tokio::runtime::Handle::try_current()
            .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
            .unwrap_or(false);
        if on_multithread_runtime {
            tokio::task::block_in_place(|| self.ensured_refresh_chunk(chunk));
        } else {
            self.ensured_refresh_chunk(chunk);
        }
        drop(slot);
        drop(done);
    }

    pub fn ensured_refresh_chunk(&self, chunk: &Chunk) {
        let refresh_changes = self.changes.load(Ordering::Relaxed);
        debug!(
            "Building histogram for chunk {}, changes {}",
            chunk.id, refresh_changes
        );
        let histogram_partitations = chunk
            .cell_index
            .entries()
            .chunks(HISTOGRAM_PARTITATION_SIZE)
            .map(|s| s.to_vec())
            .collect_vec();
        debug!(
            "Histogram for chunk {} have {} partitations",
            chunk.id,
            histogram_partitations.len()
        );
        let partitations: Vec<_> = histogram_partitations
            .into_par_iter()
            .map(|partitation| build_partitation_statistics(partitation, chunk))
            .collect();
        debug!("Total of {} partitations", partitations.len());
        // A SET, not `dedup()`. `dedup` removes only ADJACENT repeats, and
        // this iterator is the concatenation of every partition's key set in
        // hash order, so a schema present in many partitions recurred many
        // times. The loop below then visited it again: `remove` had already
        // taken its histogram, `unwrap_or_default` handed back an EMPTY one,
        // and the second insert overwrote the first with the right count and
        // no histogram. Every planner estimate on a chunk with more than one
        // partition fell back to row_count / 2 -- silently, because the count
        // looked fine. Any chunk past 1,024 cells was affected.
        let schema_ids: Vec<_> = partitations
            .iter()
            .flat_map(|(sizes, _, _, _)| sizes.keys())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let total_size = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(sizes, _, _, _)| sizes.get(sid).unwrap_or(&0))
                        .sum::<usize>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let total_counts = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(_, _, counts, _)| counts.get(sid).unwrap_or(&0))
                        .sum::<usize>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let total_segs = schema_ids
            .iter()
            .map(|sid| {
                (
                    *sid,
                    partitations
                        .iter()
                        .map(|(_, segs, _, _)| segs.get(sid).map(|set| set.len()).unwrap_or(0))
                        .sum::<usize>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let empty_histo: TargetHistogram = [[0u8; 8]; HISTOGRAM_TARGET_KEYS];
        let mut schema_histograms = schema_ids
            .iter()
            .map(|sid| {
                (*sid, {
                    let parted_histos = partitations
                        .iter()
                        .map(|(_, _, _, histo)| histo.get(sid))
                        .collect_vec();
                    let field_ids = parted_histos
                        .iter()
                        .filter_map(|opt_histo| *opt_histo)
                        .flat_map(|histo_map| histo_map.keys())
                        .dedup()
                        .collect::<Vec<_>>();
                    field_ids
                        .par_iter()
                        .map(|field_id| {
                            let schema_field_histograms: Vec<_> = parted_histos
                                .iter()
                                .filter_map(|opt_histo| *opt_histo)
                                .filter_map(|histo_map| histo_map.get(field_id))
                                .collect();
                            let histo: TargetHistogram = if schema_field_histograms.is_empty() {
                                empty_histo.clone()
                            } else {
                                build_histogram(schema_field_histograms)
                            };
                            (**field_id, histo)
                        })
                        .collect::<HashMap<u64, _>>()
                })
            })
            .collect::<HashMap<_, _>>();
        let now = now();
        for schema_id in schema_ids {
            let histogram = schema_histograms.remove(&schema_id).unwrap_or_default();
            let count = *total_counts.get(&schema_id).unwrap_or(&0);
            let segs = *total_segs.get(&schema_id).unwrap_or(&0);
            let bytes = *total_size.get(&schema_id).unwrap_or(&0);

            let statistics = SchemaStatistics {
                histogram,
                count,
                segs,
                bytes,
                timestamp: now,
            };
            self.schemas.insert(*schema_id, Arc::new(statistics));
        }
        self.timestamp.store(now, Ordering::Relaxed);
        info!(
            "statistics refreshed chunk {}: cells_scanned={} features_pushed={} nulls_skipped={} cells_without_indexed_fields={} (process-wide running totals)",
            chunk.id,
            STATS_CELLS_SCANNED.load(Ordering::Relaxed),
            STATS_FEATURES_PUSHED.load(Ordering::Relaxed),
            STATS_NULLS_SKIPPED.load(Ordering::Relaxed),
            STATS_CELLS_WITHOUT_INDEXED_FIELDS.load(Ordering::Relaxed),
        );
    }
}

fn build_partitation_statistics(
    partitation: Vec<(usize, usize)>,
    chunk: &Chunk,
) -> (
    HashMap<SchemaUid, usize>,
    HashMap<SchemaUid, HashSet<usize>>,
    HashMap<SchemaUid, usize>,
    HashMap<SchemaUid, HashMap<u64, (Vec<HistogramKey>, usize, usize)>>,
) {
    // Build exact histogram for each of the partitation and then approximate overall histogram
    debug!(
        "Building partitation for chunk {} with {} cells",
        chunk.id,
        partitation.len()
    );
    let mut sizes = HashMap::new();
    let mut segs = HashMap::new();
    let mut counts = HashMap::new();
    let mut exact_accumlators = HashMap::new();
    let partitation_size = partitation.len();
    for (hash, addr) in partitation {
        // Use the address directly from entries() instead of re-locking with location_for_read
        // This avoids deadlocks when multiple threads try to lock cells in parallel
        // For statistics, slightly stale data is acceptable

        // Validate address before using it - entries() may return stale/invalid addresses
        // Check: not null, 8-byte aligned, and within valid x86-64 address range
        if addr == 0 || addr % 8 != 0 || addr > 0x0000_FFFF_FFFF_FFFF {
            trace!("Skipping invalid address 0x{:x} for cell {}", addr, hash);
            continue;
        }
        let loc = addr;
        // This scan reads raw addresses without cell locks or segment
        // references, by design -- stale results are acceptable for
        // statistics. That bargain has two consequences it must honor itself:
        //
        // * A cold segment's pages are MADV_DONTNEED'd and read back zeroed,
        //   so decoding them yields garbage, silently and without I/O. Skip
        //   cold segments outright -- a histogram of zeros is worse than a
        //   histogram missing cold cells.
        // * Even for hot segments the entry may be mid-write or the address
        //   stale, so decode through the non-panicking path. A panic here
        //   runs inside a rayon worker and takes the whole server down, which
        //   is how a recovered store (218K cold segments) died on its first
        //   statistics refresh.
        match chunk.locate_segment(loc) {
            Some(seg) if !seg.is_cold() => {}
            _ => continue,
        }
        match header_from_chunk_raw(loc) {
            Ok((header, _)) => {
                let Some((entry_hdr, _)) = Entry::try_decode_from(loc, |_, _| ()) else {
                    continue;
                };
                let cell_size = entry_hdr.content_length as usize;
                let cell_seg = chunk.allocator.id_by_addr(loc);
                let schema_id = header.schema;
                if !schema_tracks_statistics(schema_id) {
                    continue;
                }
                if let Some(schema) = chunk.meta.schemas.get(&schema_id) {
                    // Past the cheap reject the record is resolved, so the
                    // aggregate can be keyed by the family it belongs to --
                    // which is what keeps one schema's statistics whole across
                    // an evolution instead of splitting per generation.
                    let schema_uid = schema.uid;
                    // Filter out fields that only have Fulltext or Vector indices
                    // as these don't support feature() for histogram building
                    let fields: Vec<u64> = schema
                        .index_fields
                        .iter()
                        .filter(|(_, indices)| {
                            indices.iter().any(|idx| {
                                matches!(
                                    idx,
                                    IndexType::Ranged | IndexType::Hashed | IndexType::Statistics
                                )
                            })
                        })
                        .map(|(field_id, _)| *field_id)
                        .collect();
                    STATS_CELLS_SCANNED.fetch_add(1, Ordering::Relaxed);
                    if fields.is_empty() {
                        STATS_CELLS_WITHOUT_INDEXED_FIELDS.fetch_add(1, Ordering::Relaxed);
                    }
                    if !fields.is_empty() {
                        trace!("Schema {} has fields {:?}", schema_id, fields);
                        if let Ok((partial_cell, _)) =
                            select_from_chunk_raw(loc, chunk, fields.as_slice(), true)
                        {
                            let field_array = match partial_cell {
                                SharedValue::Array(arr) => arr,
                                _ => unreachable!(
                                    "Other data structure is not possible. Got {:?}",
                                    partial_cell
                                ),
                            };
                            for (i, val) in field_array.into_iter().enumerate() {
                                if val == SharedValue::Null || val == SharedValue::NA {
                                    STATS_NULLS_SKIPPED.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                STATS_FEATURES_PUSHED.fetch_add(1, Ordering::Relaxed);
                                let field_id = fields[i];
                                exact_accumlators
                                    .entry(schema_uid)
                                    .or_insert_with(|| HashMap::new())
                                    .entry(field_id)
                                    .or_insert_with(|| Vec::with_capacity(partitation_size))
                                    .push(val.feature());
                            }
                        }
                    }
                    *counts.entry(schema_uid).or_insert(0) += 1;
                    *sizes.entry(schema_uid).or_insert(0) += cell_size;
                    segs.entry(schema_uid)
                        .or_insert_with(|| HashSet::new())
                        .insert(cell_seg);
                } else {
                    warn!("Cannot get schema {} for statistics", schema_id);
                }
            }
            Err(e) => {
                warn!("Failed to read {} for statistics, error: {:?}", hash, e);
            }
        }
    }
    let histograms: HashMap<_, _> = exact_accumlators
        .into_iter()
        .map(|(schema_id, schema_histograms)| {
            let compiled_histograms = schema_histograms
                .into_iter()
                .map(|(field, items)| {
                    let num_items = items.len();
                    let (histogram, depth) = build_partitation_histogram(items);
                    (field, (histogram, num_items, depth))
                })
                .collect::<HashMap<_, _>>();
            (schema_id, compiled_histograms)
        })
        .collect::<HashMap<_, _>>();
    (sizes, segs, counts, histograms)
}

fn build_partitation_histogram(mut items: Vec<HistogramKey>) -> (Vec<HistogramKey>, usize) {
    items.sort();
    if items.len() <= HISTOGRAM_PARTITATION_BUCKETS {
        return (items, 1);
    }
    let depth = items.len() / HISTOGRAM_PARTITATION_BUCKETS;
    let mut histogram = (0..HISTOGRAM_PARTITATION_BUCKETS)
        .map(|tile| items[tile * depth])
        .collect_vec();
    let last_item = &items[items.len() - 1];
    if histogram.last().unwrap() != last_item {
        histogram.push(*last_item);
    }
    (histogram, depth)
}

fn build_histogram(partitations: Vec<&(Vec<HistogramKey>, usize, usize)>) -> TargetHistogram {
    let num_all_keys: usize = partitations.iter().map(|(h, _, _)| h.len()).sum();
    if num_all_keys < HISTOGRAM_TARGET_KEYS {
        // debug!("Building histogram with repeatdly keys");
        return repeated_histogram(partitations);
    }
    // Build the approximated histogram from partitation histograms
    // https://arxiv.org/abs/1606.05633
    //
    // The k-way merge this used to do popped one key at a time with a
    // linear min_by over every partition: O(total_keys x partitions),
    // and both factors grow with chunk cell count, so the rebuild cost
    // grew as cells^2 — a single refresh of a ~30M-cell chunk ground a
    // thread for 25+ minutes (the TB13 P2AB wedge specimen). The merge
    // of sorted runs is just a sort: flatten to (key, weight) and sort
    // once, O(K log K) — milliseconds at the same scale.
    let num_total = partitations.iter().map(|(_, num, _)| num).sum::<usize>();
    let max_key = partitations
        .iter()
        .filter_map(|(part, _, _)| part.last())
        .max()
        .cloned()
        .unwrap_or_default();
    let mut all_keys: Vec<(HistogramKey, usize)> = Vec::with_capacity(num_all_keys);
    for (histo, _, depth) in partitations.iter() {
        all_keys.extend(histo.iter().map(|k| (*k, *depth)));
    }
    all_keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let target_width = num_total / HISTOGRAM_TARGET_BUCKETS;
    let mut target_histogram = [[0u8; 8]; HISTOGRAM_TARGET_KEYS];
    // Same emission walk as the old merge: each bucket takes the first
    // key once `target_width` worth of underlying rows has accumulated;
    // an exhausted stream repeats the last key into remaining buckets.
    let mut cursor = all_keys.iter();
    let mut filled = target_width;
    let mut last_key: (HistogramKey, usize) = Default::default();
    'HISTO_CONST: for i in 0..HISTOGRAM_TARGET_BUCKETS {
        loop {
            let (key, ended) = match cursor.next() {
                Some(entry) => (*entry, false),
                None => (last_key, true),
            };
            last_key = key;
            if filled >= target_width || ended {
                target_histogram[i] = last_key.0;
                filled = 0;
                continue 'HISTO_CONST;
            }
            filled += last_key.1;
        }
    }
    target_histogram[HISTOGRAM_TARGET_BUCKETS] = max_key.clone();
    target_histogram
}

fn repeated_histogram(partitations: Vec<&(Vec<HistogramKey>, usize, usize)>) -> TargetHistogram {
    let combined = partitations
        .iter()
        .map(|(histo, _, depth)| {
            let depth = *depth;
            histo.iter().map(move |k| (k, depth))
        })
        .flatten()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .collect_vec();
    let total_keys: usize = combined.iter().map(|(_, d)| *d).sum();
    let repeat_ratio: f64 = HISTOGRAM_TARGET_KEYS as f64 / total_keys as f64;
    let repeated = combined
        .iter()
        .map(|(k, d)| iter::repeat(*k).take((*d as f64 * repeat_ratio).ceil() as usize))
        .flatten()
        .collect_vec();
    let mut histo = empty_target_histogram();
    debug_assert!(repeated.len() >= histo.len());
    histo
        .iter_mut()
        .zip(repeated.into_iter())
        .for_each(|(h, k)| {
            *h = *k;
        });
    *histo.last_mut().unwrap() = combined.last().unwrap().0.clone();
    histo
}

fn empty_target_histogram() -> TargetHistogram {
    [[0u8; 8]; HISTOGRAM_TARGET_KEYS]
}

pub fn merge_statistics(all_stats: Vec<Arc<SchemaStatistics>>) -> Option<SchemaStatistics> {
    if all_stats.is_empty() {
        return None;
    }
    let mut count = 0;
    let mut segs = 0;
    let mut bytes = 0;
    let mut timestamp = 0;
    all_stats.iter().for_each(|s| {
        count += s.count;
        segs += s.segs;
        bytes += s.bytes;
        timestamp = max(timestamp, s.timestamp);
    });
    let histogram = all_stats
        .iter()
        .map(|s| {
            let s_count = s.count;
            s.histogram
                .iter()
                .map(move |(field, keys)| (field, keys, s_count))
        })
        .flatten()
        .sorted_by_key(|(field, _, _)| **field)
        .chunk_by(|(field, _, _)| **field)
        .into_iter()
        .map(|(field, parts)| {
            let parts = parts
                .into_iter()
                .map(|(_, keys, s_count)| {
                    let num_keys = keys.len();
                    (keys.to_vec(), s_count, s_count / num_keys)
                })
                .collect::<Vec<_>>();
            let histo = build_histogram(parts.iter().collect());
            (field, histo)
        })
        .collect::<HashMap<_, _>>();
    Some(SchemaStatistics {
        histogram,
        count,
        segs,
        bytes,
        timestamp,
    })
}

#[cfg(test)]
mod tests {
    use dovahkiin::types::{key_hash, Id, OwnedMap, OwnedValue};
    use rand::Rng;

    use crate::ram::cell::{CellHeader, OwnedCell};
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::types::RandValue;
    use crate::{
        ram::{
            chunk::Chunks,
            schema::{LocalSchemasCache, Schema},
            tests::default_fields,
        },
        server::ServerMeta,
    };

    use super::*;

    /// The original k-way merge, kept verbatim as the oracle for the
    /// sort-based rewrite: it popped one key at a time with a linear
    /// min_by over every partition — O(total_keys x partitions), which
    /// grows as cells^2 with chunk size (25+ minute refreshes at ~30M
    /// cells). Correct, just unusable at scale.
    fn build_histogram_kway_reference(
        partitations: Vec<&(Vec<HistogramKey>, usize, usize)>,
    ) -> TargetHistogram {
        let mut part_idxs = vec![0; partitations.len()];
        let part_histos = partitations
            .iter()
            .map(|(histo, _, _)| histo)
            .filter(|histo| !histo.is_empty())
            .collect_vec();
        let num_total = partitations.iter().map(|(_, num, _)| num).sum::<usize>();
        let part_depths = partitations
            .iter()
            .map(|(_, _, depth)| *depth)
            .collect_vec();
        let max_key = partitations
            .iter()
            .filter_map(|(part, _, _)| part.last())
            .max()
            .cloned()
            .unwrap_or_default();
        let target_width = num_total / HISTOGRAM_TARGET_BUCKETS;
        let mut target_histogram = [[0u8; 8]; HISTOGRAM_TARGET_KEYS];
        let mut filled = target_width;
        let mut last_key: (HistogramKey, usize) = Default::default();
        'HISTO_CONST: for i in 0..HISTOGRAM_TARGET_BUCKETS {
            loop {
                let (key, ended) = if let Some((part_idx, histo)) = part_histos
                    .iter()
                    .enumerate()
                    .filter(|(i, h)| {
                        let idx = part_idxs[*i];
                        idx < h.len()
                    })
                    .min_by(|(i1, h1), (i2, h2)| {
                        let h1_idx = part_idxs[*i1];
                        let h2_idx = part_idxs[*i2];
                        h1[h1_idx].cmp(&h2[h2_idx])
                    }) {
                    let histo_idx = part_idxs[part_idx];
                    part_idxs[part_idx] += 1;
                    ((histo[histo_idx], part_idx), false)
                } else {
                    (last_key, true)
                };
                last_key = key;
                let idx = last_key.1;
                if filled >= target_width || ended {
                    target_histogram[i] = last_key.0;
                    filled = 0;
                    continue 'HISTO_CONST;
                }
                filled += part_depths[idx];
            }
        }
        target_histogram[HISTOGRAM_TARGET_BUCKETS] = max_key.clone();
        target_histogram
    }

    /// Sort-based rewrite must reproduce the k-way merge exactly.
    /// Keys are globally unique here: with duplicate keys across
    /// partitions of different depths the two algorithms may order the
    /// equal-key run differently, shifting a bucket boundary within
    /// that run — immaterial for an approximation histogram, but it
    /// would make byte-equality flaky.
    #[test]
    fn sort_based_histogram_matches_kway_reference() {
        let mut rng = rand::thread_rng();
        for case in 0..24usize {
            let parts = 1 + case % 7;
            let mut next_key: u64 = 0;
            let partitions: Vec<(Vec<HistogramKey>, usize, usize)> = (0..parts)
                .map(|p| {
                    // Always at least HISTOGRAM_TARGET_KEYS per partition so
                    // build_histogram takes the merge path being tested, not
                    // the repeated_histogram redirect the reference lacks.
                    let keys = HISTOGRAM_TARGET_KEYS + rng.gen_range(0..(50 + case * 40));
                    let mut histo: Vec<HistogramKey> = (0..keys)
                        .map(|_| {
                            next_key += 1 + rng.gen_range(0..1000u64);
                            next_key.to_be_bytes()
                        })
                        .collect();
                    // Partitions arrive sorted but interleaved in key
                    // space; rotate ranges so partition order != key order.
                    if p % 2 == 1 {
                        histo.reverse();
                        histo.iter_mut().for_each(|k| {
                            let v = u64::from_be_bytes(*k);
                            *k = (u64::MAX / 2 - v).to_be_bytes();
                        });
                        histo.sort_unstable();
                    }
                    let depth = 1 + rng.gen_range(0..64usize);
                    let num = histo.len() * depth;
                    (histo, num, depth)
                })
                .collect();
            let refs: Vec<&(Vec<HistogramKey>, usize, usize)> = partitions.iter().collect();
            let expected = build_histogram_kway_reference(refs.clone());
            let actual = build_histogram(refs);
            assert_eq!(
                actual, expected,
                "sort-based histogram diverged from k-way reference (case {case})"
            );
        }
    }

    /// Wedge-specimen scale: ~30M cells => ~29.3K partitions x 101 keys.
    /// The old k-way merge was O(total_keys x partitions) — ~87 billion
    /// key comparisons here, a 25+ minute grind observed live. The sort
    /// path must stay interactive at the same scale.
    #[test]
    fn histogram_merge_at_wedge_specimen_scale() {
        let parts_n = 29_300usize;
        let mut next = 0u64;
        let partitions: Vec<(Vec<HistogramKey>, usize, usize)> = (0..parts_n)
            .map(|_| {
                let histo: Vec<HistogramKey> = (0..HISTOGRAM_PARTITATION_KEYS)
                    .map(|_| {
                        next += 3;
                        next.to_be_bytes()
                    })
                    .collect();
                (histo, HISTOGRAM_PARTITATION_SIZE, 10)
            })
            .collect();
        let refs: Vec<&(Vec<HistogramKey>, usize, usize)> = partitions.iter().collect();
        let start = std::time::Instant::now();
        let histo = build_histogram(refs);
        let elapsed = start.elapsed();
        assert!(histo[0] != [0u8; 8]);
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "full-scale histogram merge took {elapsed:?}; the quadratic merge is back"
        );
        println!("wedge-specimen-scale merge: {elapsed:?}");
    }

    #[test]
    fn partitation_histogram() {
        let small_set = (0..10).map(|n| OwnedValue::U64(n).feature()).collect_vec();
        assert_eq!(
            build_partitation_histogram(small_set.clone()),
            (small_set, 1)
        );
        let eq_set = (0..HISTOGRAM_PARTITATION_BUCKETS)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        assert_eq!(build_partitation_histogram(eq_set.clone()), (eq_set, 1));

        let double_set = (0..HISTOGRAM_PARTITATION_BUCKETS * 2)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        let mut expect = double_set.iter().step_by(2).cloned().collect_vec();
        expect.push(double_set.last().unwrap().to_owned());
        assert_eq!(build_partitation_histogram(double_set), (expect, 2));

        let triple_set = (0..HISTOGRAM_PARTITATION_BUCKETS * 3)
            .map(|n| OwnedValue::U64(n as u64).feature())
            .collect_vec();
        let mut expect = triple_set.iter().step_by(3).cloned().collect_vec();
        expect.push(triple_set.last().unwrap().to_owned());
        assert_eq!(build_partitation_histogram(triple_set), (expect, 3));
    }

    #[test]
    fn approximated_histogram() {
        // Test with example from the paper
        let histo_1 = vec![2, 7, 18, 25];
        let histo_1_height = 4;

        let histo_2 = vec![3, 15, 24, 30];
        let histo_2_height = 5;

        let test_data = vec![
            (
                histo_1
                    .iter()
                    .map(|n| OwnedValue::U64(*n).feature())
                    .collect::<Vec<_>>(),
                (histo_1.len() - 1) * histo_1_height,
                histo_1_height,
            ),
            (
                histo_2
                    .iter()
                    .map(|n| OwnedValue::U64(*n).feature())
                    .collect::<Vec<_>>(),
                (histo_2.len() - 1) * histo_2_height,
                histo_2_height,
            ),
        ];
        // Test for the repeatdly case
        let histogram = build_histogram(test_data.iter().collect_vec());
        assert!(histogram.is_sorted());
        assert_eq!(histogram.last().unwrap(), &OwnedValue::U64(30).feature());

        let histo_1 = (0..1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_2 = (0..1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_3 = (0..=1024)
            .map(|n| OwnedValue::U64(n).feature())
            .collect_vec();
        let histo_1_height = histo_1.len();
        let histo_2_height = histo_2.len();
        let test_data = vec![
            (histo_1, histo_1_height, 1),
            (histo_2, histo_2_height, 2),
            (histo_3, histo_2_height, 3),
        ];
        let histogram = build_histogram(test_data.iter().collect_vec());
        assert!(histogram.is_sorted(), "Got {:?}", histogram);
        assert_eq!(histogram.last().unwrap(), &OwnedValue::U64(1024).feature());
    }

    const CHUNK_TEST_SIZE: usize = REFRESH_CHANGES_THRESHOLD as usize * 16;
    /// Reproduction of the connectome symptom: a vertex-shaped schema -- a
    /// hashed String key, hashed Id link fields, and NULLABLE u16 Ranged label
    /// columns -- reported the right `count` but no histogram for any field,
    /// so every equality estimate fell back to row_count / 2. This pins that a
    /// SINGLE refresh yields the histograms; the existing test only checks
    /// them after a second one.
    #[test]
    fn nullable_u16_ranged_fields_get_a_histogram_on_first_refresh() {
        let _ = env_logger::try_init();
        use crate::ram::schema::{Field, IndexType};
        use dovahkiin::types::Type;
        let fields = Field::new_schema(vec![
            Field::new_indexed("root_id", Type::String, vec![IndexType::Hashed]),
            Field::new_indexed("_outbound", Type::Id, vec![IndexType::Hashed]),
            Field::new_indexed_nullable("super_class_id", Type::U16, vec![IndexType::Ranged]),
            Field::new_indexed_nullable("side_id", Type::U16, vec![IndexType::Ranged]),
            Field::new_unindexed_nullable("top_nt_conf", Type::F32),
        ]);
        // Distinct explicit ids: `Schema::new` does not hand out unique ids,
        // and two schemas sharing one would register over each other.
        let schema = Schema::new_with_id(9101, "vertexish", None, fields, false, false);
        // A second schema interleaved with the first so partitions' key sets
        // come out in differing orders -- the condition under which a
        // non-adjacent duplicate schema id used to wipe the histogram.
        let other = Schema::new_with_id(
            9102,
            "otherish",
            None,
            Field::new_schema(vec![Field::new_indexed("k", Type::I64, vec![IndexType::Ranged])]),
            false,
            false,
        );
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        schemas.debug_only_new_schema(other.clone());
        let schema_vid = schema.vid;
        let schema_uid = schema.uid;
        let chunks = Chunks::new(
            1,
            SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        // Well past one partition (HISTOGRAM_PARTITATION_SIZE cells), so the
        // gather has to merge many partitions per schema.
        let total = HISTOGRAM_PARTITATION_SIZE * 6 + 7;
        // Skewed like the real column: value 6 for 56%, a long tail otherwise,
        // and some rows with the label absent entirely.
        for i in 0..total {
            if i % 3 == 0 {
                let mut m = OwnedMap::new();
                m.insert(&"k".to_string(), OwnedValue::I64(i as i64));
                let header = CellHeader::new(other.vid, &Id::rand());
                let mut cell = OwnedCell { data: OwnedValue::Map(m), header };
                chunks.write_cell(&mut cell).unwrap();
            }
            let mut m = OwnedMap::new();
            m.insert(&"root_id".to_string(), OwnedValue::String(format!("N{i}")));
            m.insert(&"_outbound".to_string(), OwnedValue::Id(Id::rand()));
            let class: u16 = if i % 100 < 56 { 6 } else { (i % 7) as u16 + 1 };
            m.insert(&"super_class_id".to_string(), OwnedValue::U16(class));
            if i % 10 != 0 {
                m.insert(&"side_id".to_string(), OwnedValue::U16((i % 2) as u16 + 1));
            }
            m.insert(&"top_nt_conf".to_string(), OwnedValue::F32(0.5));
            let header = CellHeader::new(schema_vid, &Id::rand());
            let mut cell = OwnedCell { data: OwnedValue::Map(m), header };
            chunks.write_cell(&mut cell).unwrap();
        }
        chunks.ensure_statistics();
        let stats = chunks.all_chunk_statistics(schema_uid);
        let stat = stats[0].as_ref().expect("statistics for the schema");
        assert_eq!(stat.count, total, "count must be exact");
        info!("fields with histograms: {:?}", stat.histogram.keys());
        let class_key = key_hash("super_class_id");
        let side_key = key_hash("side_id");
        assert!(
            stat.histogram.contains_key(&class_key),
            "u16 Ranged field must have a histogram after ONE refresh; have {:?}",
            stat.histogram.keys()
        );
        assert!(
            stat.histogram.contains_key(&side_key),
            "a nullable field with some nulls must still get a histogram"
        );
        // And the histogram must reflect the skew: value 6 should occupy
        // roughly 56 of the ~100 keys.
        let h = stat.histogram[&class_key];
        let six = OwnedValue::U16(6).feature();
        let distinct: std::collections::BTreeSet<_> = h.iter().copied().collect();
        info!("u16 feature of 6 = {:?}; histogram distinct keys = {:?}", six, distinct);
        assert_ne!(six, 6u64.to_be_bytes(), "if these match, the encoding note below is stale");
        let sixes = h.iter().filter(|k| **k == six).count();
        assert!(
            (45..=70).contains(&sixes),
            "expected ~56 keys equal to 6 in an equi-depth histogram, got {sixes}"
        );
    }

    #[test]
    fn chunk_statistics() {
        let _ = env_logger::try_init();
        let fields = default_fields();
        let schema = Schema::new("dummy", None, fields, false, true);
        let schemas = LocalSchemasCache::new_local("");
        schemas.debug_only_new_schema(schema.clone());
        // Cells are written under the generation; statistics are read back
        // under the family.
        let schema_vid = schema.vid;
        let schema_uid = schema.uid;
        let chunks = Chunks::new(
            1,
            SEGMENT_SIZE,
            Arc::new(ServerMeta { schemas }),
            None,
            None,
            None,
            None,
        );
        let mut rng = rand::thread_rng();
        for i in 0..CHUNK_TEST_SIZE {
            let mut data_map = OwnedMap::new();
            data_map.insert(&String::from("id"), OwnedValue::I64(i as i64));
            data_map.insert(
                &String::from("score"),
                OwnedValue::U64(rng.gen_range(60..100)),
            );
            data_map.insert(
                &String::from("name"),
                OwnedValue::String(String::from("Jack")),
            );
            let data = OwnedValue::Map(data_map);
            let header = CellHeader::new(schema_vid, &Id::rand());
            let mut cell = OwnedCell { data, header };
            chunks.write_cell(&mut cell).unwrap();
        }
        // Writes only RECORD changes now — the refresh itself belongs to the
        // sweeper thread (or an explicit ensure), never the write path.
        chunks.ensure_statistics();
        let stats = chunks.all_chunk_statistics(schema_uid);
        assert_eq!(stats.len(), 1);
        let stat = stats[0].as_ref().unwrap();
        debug!("Stat {:?}", &*stat);
        assert!(stat.count > 0, "Statistics should be triggered");
        assert!(stat.bytes > 0, "Statistics should have bytes");
        assert!(stat.timestamp > 0, "timestamp should not be zero");
        assert!(stat.segs > 0, "Segs should not be zero");
        chunks.ensure_statistics();
        let stats = chunks.all_chunk_statistics(schema_uid);
        let stat = stats[0].as_ref().unwrap();
        info!("Statistics fields: {:?}", stat.histogram.keys());
        assert_eq!(stat.histogram.len(), 2, "Should have 2 statistics fields");
        let id_key = key_hash("id");
        let score_key = key_hash("score");
        assert!(stat.histogram.contains_key(&id_key));
        assert!(stat.histogram.contains_key(&score_key));
        let id_histo = stat.histogram[&id_key];
        let score_histo = stat.histogram[&score_key];
        assert_eq!(id_histo.len(), score_histo.len());
        assert_eq!(id_histo.len(), HISTOGRAM_PARTITATION_KEYS);
        assert_eq!(score_histo.len(), HISTOGRAM_PARTITATION_KEYS);
        assert_eq!(
            id_histo[0],
            0u64.to_be_bytes(),
            "Histogram does not include minimal"
        );
        assert_eq!(
            id_histo[id_histo.len() - 1],
            (CHUNK_TEST_SIZE as u64 - 1).to_be_bytes(),
            "Histogram does not include maxnimal"
        );
        // TODO: Test on the distribution
    }
}
