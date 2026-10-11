//! Index scrub: re-derive every live cell's index entries and compare them
//! against the index that is supposed to hold them.
//!
//! # Why this exists
//!
//! Cell data and the ranged index fail independently. A cell lives in a
//! segment and is recovered by scanning that segment; a ranged entry lives
//! in a B-tree whose pages are themselves cells, reached by following a
//! chain from a metadata cell. Damage anywhere on that chain makes the tree
//! unreconstructible, and `RangedTree::recover` then deliberately leaves the
//! tree ABSENT rather than installing an empty one -- because an empty tree
//! answers every scan with "no rows", which is indistinguishable from a
//! correct answer.
//!
//! Absent is the safe choice and it is also a dead end: every operation on
//! that range errors, retries re-read the same broken pages, and nothing in
//! the system ever rebuilds them. The data is entirely intact and entirely
//! unreachable. This module is the way out.
//!
//! # The two rules that make it safe
//!
//! **It derives entries through the write path's own function.**
//! [`probe_cell_indices`] is what the writer calls. A scrub with its own
//! copy of the derivation rules would drift, and every drift would surface
//! as a permanent disagreement that looks exactly like corruption.
//!
//! **It deletes only what it can prove.** Destructive pruning requires a
//! complete, stable singleton placement. Cluster pruning is deferred until
//! it can hold placement stable through deletion. The cell walk never deletes: an
//! entry that looks unaccounted-for may belong to a cell written after this
//! pass read that segment, or to a cell on another node this pass cannot
//! see. The prune walk does delete, and it earns that by asking a question
//! with one local answer -- it holds the entry, it reads the cell the entry
//! names, and it re-derives that cell's keys here. An entry naming a cell
//! this member does not own gets no verdict at all. The asymmetry is what
//! makes a single-node scrub sound in both directions: a cell HERE implies
//! entries HERE, and an entry HERE naming a cell HERE is either justified
//! by that cell or it is not.

use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;

use crate::index::builder::probe_cell_indices;
use crate::index::ranged::client::NextTree;
use crate::index::ranged::tree::btree::Ordering;
use crate::index::ranged::trees::min_entry_key;
use crate::index::EntryKey;
use crate::index::IndexerClients;
use crate::ram::cell::ReadError;
use crate::ram::chunk::Chunks;
use crate::ram::entry::EntryContent;
use crate::ram::types::Id;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ScrubMode {
    /// Read-only. Reports what is missing and changes nothing.
    Verify,
    /// Inserts every derived entry the index does not hold. Never deletes.
    Repair,
}

impl ScrubMode {
    fn repairs(&self) -> bool {
        matches!(self, ScrubMode::Repair)
    }
}

/// What one pass found. Every field is a count of something the pass
/// actually observed; nothing here is estimated.
#[derive(Debug, Default, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct ScrubReport {
    /// Live cells walked.
    pub cells_scanned: u64,
    /// Live cells whose schema declares no indexed field, so nothing is
    /// derivable from them and they are not read at all. Mostly the b-tree's
    /// own pages.
    pub cells_no_indices: u64,
    /// Cells whose entry the index still pointed at, but which could not be
    /// read. A nonzero count here is a CELL-level problem, not an index one,
    /// and this pass cannot fix it.
    pub cells_unreadable: u64,
    /// Cells whose schema is no longer in the catalog. Their entries cannot
    /// be derived at all, so they are neither verified nor repaired.
    pub cells_schema_missing: u64,
    /// Ranged keys derived from the cells walked.
    pub entries_derived: u64,
    /// Derived keys the index confirmed it holds.
    pub entries_present: u64,
    /// Repairs the index accepted and then did not hold when read straight
    /// back. A nonzero count means the reconcile cannot be trusted to have
    /// fixed what it reported fixing, which invalidates the whole
    /// reconcile-before-serving contract -- so it is reported separately from
    /// a repair that outright failed.
    pub repairs_not_stuck: u64,
    /// Derived keys the index confirmed it does NOT hold.
    pub entries_missing: u64,
    /// Derived keys inserted by a repair pass. In `Verify` this is 0.
    pub entries_repaired: u64,
    /// Derived keys the index could not be asked about -- the tree covering
    /// them is absent or unreadable. These are the ranges a reader is
    /// currently getting errors for, and they are the reason to run this.
    pub entries_unreachable: u64,
    /// Repairs the index refused. Distinct from `unreachable`: the tree
    /// answered and the insert still failed.
    pub repairs_failed: u64,
    /// Index keys the prune walk read. This is a walk of the INDEX, so it
    /// counts entries whether or not any cell of this node implies them.
    pub index_keys_scanned: u64,
    /// Index keys no local cell implies: the entry names a cell that is gone,
    /// or a cell whose value has moved on. This is the STALE direction, and it
    /// is the one the store had no way to reach before the prune walk existed.
    pub entries_stale: u64,
    /// Stale keys a repair pass deleted. In `Verify` this is 0.
    pub entries_pruned: u64,
    /// Deletes the index refused.
    pub prune_failed: u64,
    /// Entries the prune walk could not form an opinion about, because the cell
    /// they name sits on another member. A ranged tree covers a key range for
    /// the whole cluster; a node's chunks do not.
    pub entries_not_local: u64,
    /// The prune walk stopped before it covered the index. Its counts are a
    /// partial account, so they must never be read as "clean".
    pub prune_incomplete: u64,
}

impl ScrubReport {
    /// Whether the index is consistent with the cells this pass saw.
    ///
    /// Unreachable entries count as NOT clean: the pass could not form an
    /// opinion, and reporting "clean" for a range that is erroring would
    /// invert the tool's whole purpose.
    pub fn is_clean(&self) -> bool {
        self.entries_missing == 0
            && self.entries_unreachable == 0
            && self.cells_unreadable == 0
            && self.cells_schema_missing == 0
            && self.repairs_failed == 0
            && self.entries_stale == 0
            && self.prune_failed == 0
            && self.prune_incomplete == 0
    }

    /// Fold another node's report into this one. Public because a
    /// cluster-wide scrub is the sum of its nodes': a ranged tree covers a
    /// key range across the WHOLE cluster, so only the union of every
    /// node's cells is a complete account of what the index should hold.
    pub fn merge(&mut self, other: &ScrubReport) {
        self.cells_scanned += other.cells_scanned;
        self.cells_no_indices += other.cells_no_indices;
        self.cells_unreadable += other.cells_unreadable;
        self.cells_schema_missing += other.cells_schema_missing;
        self.entries_derived += other.entries_derived;
        self.entries_present += other.entries_present;
        self.entries_missing += other.entries_missing;
        self.entries_repaired += other.entries_repaired;
        self.entries_unreachable += other.entries_unreachable;
        self.repairs_failed += other.repairs_failed;
        self.repairs_not_stuck += other.repairs_not_stuck;
        self.index_keys_scanned += other.index_keys_scanned;
        self.entries_stale += other.entries_stale;
        self.entries_pruned += other.entries_pruned;
        self.prune_failed += other.prune_failed;
        self.entries_not_local += other.entries_not_local;
        self.prune_incomplete += other.prune_incomplete;
    }
}

impl std::fmt::Display for ScrubReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cells={} (no-indices={}, unreadable={}, schema-missing={}) entries={} \
             present={} missing={} repaired={} not-stuck={} unreachable={} \
             repair-failed={} index-keys={} stale={} pruned={} prune-failed={} \
             not-local={} prune-incomplete={}",
            self.cells_scanned,
            self.cells_no_indices,
            self.cells_unreadable,
            self.cells_schema_missing,
            self.entries_derived,
            self.entries_present,
            self.entries_missing,
            self.entries_repaired,
            self.repairs_not_stuck,
            self.entries_unreachable,
            self.repairs_failed,
            self.index_keys_scanned,
            self.entries_stale,
            self.entries_pruned,
            self.prune_failed,
            self.entries_not_local,
            self.prune_incomplete,
        )
    }
}

/// Start a scrub after recovery, if this deployment asked for one.
///
/// `NEB_SCRUB_ON_RECOVERY`: `off` (default), `verify`, or `repair`.
///
/// **Why this is not on by default.** The pass walks every live cell, which
/// measured 5.7s for 800k cells -- fine there, hours on a store with
/// billions. A default that silently adds hours to every restart is worse
/// than the problem it fixes, so the deployment that knows its size chooses.
///
/// What the last reconciliation cost, for anything that wants to report it.
///
/// A blocking pass on the startup path has to be answerable about its price,
/// and a number that only exists inside a log line at whatever level happened
/// to be enabled is not an answer.
pub static LAST_RECONCILE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static LAST_RECONCILE_SCANNED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static LAST_RECONCILE_NOIDX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static LAST_RECONCILE_REPAIRED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Stale entries the last reconciliation removed. Alongside `REPAIRED`, this
/// is the number that says the prune direction ran at all: a store that comes
/// back with `REPAIRED=0 PRUNED=0` after a kill has an index that was already
/// current, and a store with `PRUNED=0` while scans disagree with cells has a
/// prune that did not run or did not finish.
pub static LAST_RECONCILE_PRUNED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// **Why it BLOCKS rather than running in the background.** The index is a
/// cache of the cells, and a cache that has not caught up answers queries
/// with fewer rows and no error. Serving during the pass therefore does not
/// merely inherit the status quo -- it hands out wrong answers that look
/// right, at the one moment the store is most likely to be wrong.
///
/// **Why the full walk, every time.** A durability mark recording how far the
/// index was last known good would bound this to the crash window. One was
/// built and then removed: every cheap version of that claim was measurably
/// wrong, hiding the very holes this pass exists to fill. The walk is
/// affordable anyway -- 1.0-5.9 s for 0.7-4M cells -- so the bound bought
/// seconds at the risk of silently missing entries.
///
/// **Why it is worth running at all.** A crash leaves the index holding the
/// changes that reached disk and losing the ones that had not, and that cuts
/// both ways: entries MISSING for cells written since the tree's last flush,
/// and entries STALE for cells whose delete, or whose value change, never
/// reached the tree. The write path re-asserts an entry only when its cell is
/// written AGAIN, and it never removes one it did not just write, so hot data
/// heals itself and everything else does not: a cell written once keeps a lost
/// entry for the life of the store, and a cell whose value moved keeps the old
/// entry beside the new one forever.
pub async fn reconcile_index_before_serving(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    group: &str,
    database: &str,
) {
    if mode_from_env().is_none() && std::env::var("NEB_SCRUB_ON_RECOVERY").is_ok() {
        info!(
            "Index reconciliation disabled by NEB_SCRUB_ON_RECOVERY; the index may be behind \
             the cells until those cells are written again"
        );
        return;
    }
    let mode = mode_from_env().unwrap_or(ScrubMode::Repair);
    let label = format!("{}/{}", group, database);
    info!("Reconciling {}'s index against its cells", label);
    let began = std::time::Instant::now();
    let report = scrub_ranged_index(chunks, indexers, mode).await;
    let elapsed = began.elapsed();
    use std::sync::atomic::Ordering as AtomicOrdering;
    LAST_RECONCILE_MS.store(elapsed.as_millis() as u64, AtomicOrdering::Relaxed);
    LAST_RECONCILE_SCANNED.store(report.cells_scanned, AtomicOrdering::Relaxed);
    LAST_RECONCILE_NOIDX.store(report.cells_no_indices, AtomicOrdering::Relaxed);
    LAST_RECONCILE_REPAIRED.store(report.entries_repaired, AtomicOrdering::Relaxed);
    LAST_RECONCILE_PRUNED.store(report.entries_pruned, AtomicOrdering::Relaxed);
    if report.is_clean() {
        info!(
            "Index reconciliation for {} found nothing to repair in {:?}: {}",
            label, elapsed, report
        );
    } else {
        warn!(
            "Index reconciliation for {} repaired {} missing and pruned {} stale entries in \
             {:?}: {}",
            label, report.entries_repaired, report.entries_pruned, elapsed, report
        );
    }
    if report.index_keys_scanned > 0 && report.entries_not_local == report.index_keys_scanned {
        // Every entry the walk saw named a cell this member does not own, so
        // the prune judged nothing at all. On a single-node store that is not a
        // placement fact but a placement FAILURE: the ring or the slot table
        // was empty when this ran, and the stale entries this pass exists to
        // remove are still there, reported as clean.
        warn!(
            "Index reconciliation for {} could not judge a single one of its {} index entries: \
             every one names a slot this member does not own. Check the placement ring before \
             believing a clean report.",
            label, report.index_keys_scanned
        );
    }
}

fn mode_from_env() -> Option<ScrubMode> {
    match std::env::var("NEB_SCRUB_ON_RECOVERY")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "verify" => Some(ScrubMode::Verify),
        "repair" => Some(ScrubMode::Repair),
        "" | "off" | "0" | "false" => None,
        other => {
            // Refusing beats guessing: a typo silently meaning "off" is how a
            // store runs for months believing it is being scrubbed.
            warn!(
                "NEB_SCRUB_ON_RECOVERY={:?} is not one of off/verify/repair; not scrubbing",
                other
            );
            None
        }
    }
}

/// Walk the ranged index and the live cells, and reconcile the two.
///
/// Both directions, in this order:
///
/// 1. [`prune_ranged_index`] walks the INDEX and removes what no cell implies.
/// 2. The cell walk derives every live cell's keys and inserts what the index
///    lacks.
///
/// Prune first, so the insert pass reads the index in its final shape and can
/// re-add anything the prune removed on a race. The cell walk is batched by
/// segment: the derivation half holds cell read guards and the reconciliation
/// half awaits RPCs, and those two must not overlap -- a guard held across an
/// await pins a segment for the length of a network round trip, which is how a
/// scrub of a large store would block the cleaner for its entire duration.
pub async fn scrub_ranged_index(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    mode: ScrubMode,
) -> ScrubReport {
    UNREACHABLE_LOGGED.store(0, std::sync::atomic::Ordering::Relaxed);
    MISSING_LOGGED.store(0, std::sync::atomic::Ordering::Relaxed);
    STALE_LOGGED.store(0, std::sync::atomic::Ordering::Relaxed);
    let mut total = prune_ranged_index(chunks, indexers, mode).await;
    total.merge(&reconcile_cell_entries(chunks, indexers, mode).await);
    total
}

async fn reconcile_cell_entries(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    mode: ScrubMode,
) -> ScrubReport {
    let mut total = ScrubReport::default();
    for chunk in &chunks.list {
        for segment in chunk.segments() {
            // --- derive (synchronous; all guards released before the await)
            let mut batch: Vec<EntryKey> = Vec::new();
            let mut derived_report = ScrubReport::default();
            for entry in chunk.live_entries(&segment) {
                let EntryContent::Cell(header) = entry.content else {
                    continue;
                };
                derived_report.cells_scanned += 1;
                // The entry header already names the schema, so check the
                // catalog BEFORE asking for the cell. `read_cell` treats a
                // missing schema as "this shall never happen" and panics in a
                // debug build, which is a fair rule for the write path and the
                // wrong one here: this pass walks whatever the store contains,
                // including cells whose schema the catalog has not registered
                // yet or has dropped. Reading first turned a countable
                // condition into a crash at startup.
                match chunk.meta.schemas.get(&header.schema) {
                    None => {
                        derived_report.cells_schema_missing += 1;
                        continue;
                    }
                    Some(schema) if schema.index_fields.is_empty() && !schema.is_scannable => {
                        // Derives nothing, so reading it can only confirm
                        // that. This is not a rare case: the b-tree's own
                        // pages are cells in these very chunks, and a flush
                        // writes a great many of them right before the
                        // shutdown whose mark bounds the next pass -- so
                        // without this the cheap bounded pass spends most of
                        // its time reading the index it is there to check.
                        derived_report.cells_no_indices += 1;
                        continue;
                    }
                    Some(_) => {}
                }
                derive_into(chunk, header.id(), &mut batch, &mut derived_report);
            }
            derived_report.entries_derived += batch.len() as u64;
            total.merge(&derived_report);

            // --- reconcile (asynchronous; nothing borrowed from the chunk)
            let batch_report = reconcile(indexers, &batch, mode).await;
            total.merge(&batch_report);
        }
    }
    total
}

/// Read one cell and push the ranged keys it should contribute.
///
/// Split out so the guard's scope is a function body rather than a comment
/// asking the next reader to keep the await out of it.
fn derive_into(
    chunk: &crate::ram::chunk::Chunk,
    id: Id,
    out: &mut Vec<EntryKey>,
    report: &mut ScrubReport,
) {
    let Ok(cell) = chunk.read_cell(id.bits()) else {
        // The index pointed here a moment ago -- `live_entries` only yields
        // a cell when the cell index agrees -- so a failure now is either a
        // concurrent move or real damage. Counted, not repaired: this pass
        // fixes indexes, and a cell it cannot read is not an index problem.
        report.cells_unreadable += 1;
        return;
    };
    let Some(schema) = chunk.meta.schemas.get(&cell.header.schema) else {
        // Nothing derivable without the rules. Silently skipping would make
        // a dropped schema look like a clean index.
        report.cells_schema_missing += 1;
        return;
    };
    for res in probe_cell_indices(&cell, &*schema) {
        out.extend(res.ranged_keys().cloned());
    }
    if schema.is_scannable {
        out.push(EntryKey::for_scannable(&id, schema.uid));
    }
}

// ---- the prune direction: what the index holds that no cell implies -------

/// Keys per `scan_keys` round trip while pruning.
///
/// Smaller than a query block on purpose: the walk judges every key it gets
/// against a cell read, so one RPC per 1024 keys already leaves the reads as
/// the cost, and a huge block just holds memory and delays the first delete.
const PRUNE_BLOCK_KEYS: u32 = 1024;

/// How many times the walk will ask for the next tree placement after an
/// answer that only means "not yet". Placement settles during startup, and a
/// reconcile that runs before it settles must retry rather than declare the
/// index pruned on the strength of a routing miss.
const PRUNE_PLACEMENT_RETRIES: usize = 32;

/// Blocks one prune pass may walk. Not a tuning knob -- a spin guard: every
/// unbounded loop on this index path has eventually hung a worker at 100% and
/// taken the process's timers down with it.
const PRUNE_MAX_BLOCKS: usize = 1 << 20;

/// Whether this member is the one that holds the cell an entry names.
///
/// A ranged tree covers a key range for the WHOLE cluster, so an entry sitting
/// here can name a cell that lives anywhere. Only the member holding the cell
/// can say what that cell implies, so everyone else abstains.
fn owns_cell(indexers: &IndexerClients, id: &Id) -> bool {
    indexers
        .conshash
        .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
        .is_some_and(|owner| owner == indexers.server_id)
}

/// Ask the local store whether it contradicts one index entry, and count the
/// answer.
///
/// Synchronous, and it must stay that way: the cell read guard it holds pins a
/// segment, and no delete RPC may be awaited with it live.
fn entry_contradicted(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    key: &EntryKey,
    report: &mut ScrubReport,
) -> bool {
    let id = key.id();
    if !owns_cell(indexers, &id) {
        report.entries_not_local += 1;
        return false;
    }
    let chunk = chunks.locate_chunk_by_partition(id.locality() as u64);
    judge_local_entry(chunk, id, key, report)
}

/// The verdict for one entry, taken from the chunk that would hold its cell.
///
/// The header is read before the cell, and the catalog is consulted before the
/// header is followed, for the reason `derive_into` states: `read_cell` treats a
/// schema it cannot find as "this shall never happen" and panics in a debug
/// build, and this walk sees whatever the store contains -- including cells
/// whose schema this restart never registered. A store restarted with a schema
/// it has to re-import therefore abstains here instead of crashing at startup.
fn judge_local_entry(
    chunk: &crate::ram::chunk::Chunk,
    id: Id,
    key: &EntryKey,
    report: &mut ScrubReport,
) -> bool {
    let guard = match crate::ram::chunk::CellGuard::for_read(id.bits(), chunk) {
        Ok(guard) => guard,
        // The cell index has nothing for this id: the cell is gone, so its
        // delete was durable and the index's own record of that delete was not.
        // This is the common stale case.
        Err(ReadError::CellDoesNotExisted) | Err(ReadError::CellIdIsUnitId) => {
            report.entries_stale += 1;
            return true;
        }
        // A guard this chunk could not produce says nothing about the entry.
        Err(_) => {
            report.cells_unreadable += 1;
            return false;
        }
    };
    let location = guard.get_ptr();
    if location == 0 {
        report.entries_stale += 1;
        return true;
    }
    let Ok((header, _)) = crate::ram::cell::header_from_chunk_raw(location) else {
        report.cells_unreadable += 1;
        return false;
    };
    let Some(schema) = chunk.meta.schemas.get(&header.schema) else {
        report.cells_schema_missing += 1;
        return false;
    };
    let Ok((cell, _)) = crate::ram::cell::SharedCell::from_chunk_raw(id.bits(), guard, chunk)
    else {
        report.cells_unreadable += 1;
        return false;
    };
    // Re-derive through the write path's own probe, the same function the
    // insert half uses, so the two directions cannot disagree about what a cell
    // means. An array-valued indexed field implies several keys and the entry
    // may be any of them, so membership is the question -- not "does the first
    // derived key match".
    let implied = probe_cell_indices(&cell, &*schema)
        .iter()
        .any(|res| res.ranged_keys().any(|derived| derived == key))
        || (schema.is_scannable && *key == EntryKey::for_scannable(&id, schema.uid));
    if implied {
        false
    } else {
        // The cell is here and no longer holds the value this entry lists.
        report.entries_stale += 1;
        true
    }
}

/// Walk every ranged tree and report -- and in `Repair` mode remove -- the
/// entries no local cell implies.
///
/// This is the direction the cell walk cannot see. A crash loses the index
/// changes that had not reached disk, and it loses them in BOTH directions:
/// an insert that never landed leaves a MISSING entry, which the cell walk
/// repairs, and a delete that never landed leaves a STALE one, which the cell
/// walk never notices because it only ever asks "does the index hold what this
/// cell needs". A store left with stale entries answers a range scan with rows
/// whose cells say something else, or with rows that have no cell at all, and
/// it keeps doing so across every later restart.
async fn prune_ranged_index(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    mode: ScrubMode,
) -> ScrubReport {
    prune_ranged_index_with_cap(chunks, indexers, mode, PRUNE_MAX_BLOCKS).await
}

/// A local miss is not proof of deletion in a cluster. Migration can remove
/// the donor copy while a cached placement still names that donor. Until the
/// prune can hold a cluster placement guard, restrict it to a confirmed
/// singleton. Include offline members; an offline owner still holds data.
/// Read-only verification can use a settled cluster snapshot without deleting.
async fn prune_placement_authority(indexers: &Arc<IndexerClients>, mode: ScrubMode) -> bool {
    let client = &indexers.neb_client;
    let Ok(Some((members, _))) = indexers
        .conshash
        .membership()
        .group_members(&client.group_name, false)
        .await
    else {
        return false;
    };
    if members.len() != indexers.conshash.server_count()
        || !members.iter().any(|member| member.id == indexers.server_id)
        || (mode.repairs() && members.len() != 1)
    {
        return false;
    }
    let Ok((Some(table), applied_index)) = crate::slots::load_table(
        client.group_name(),
        &client.raft_client,
        crate::server::SLOTS_SM_ID,
    )
    .await
    else {
        return false;
    };
    use bifrost::conshash::slots::SlotState;
    if table.len() != crate::slots::SLOT_COUNT
        || !table.values().all(|state| {
            matches!(state, SlotState::Stable { owner }
            if members.iter().any(|member| member.id == *owner))
        })
    {
        return false;
    }
    let mut owners = vec![0; crate::slots::SLOT_COUNT];
    for (slot, state) in table {
        let Some(owner) = owners.get_mut(slot as usize) else {
            return false;
        };
        *owner = state.serving_owner();
    }
    // Replace a startup fallback with this ordered, complete slot snapshot.
    indexers
        .conshash
        .set_slot_overrides(Some(owners), applied_index)
        && indexers.conshash.server_count() == members.len()
}

async fn prune_ranged_index_with_cap(
    chunks: &Arc<Chunks>,
    indexers: &Arc<IndexerClients>,
    mode: ScrubMode,
    max_blocks: usize,
) -> ScrubReport {
    let mut report = ScrubReport::default();
    if !prune_placement_authority(indexers, mode).await {
        report.prune_incomplete += 1;
        warn!("Index prune skipped: placement is unsettled or destructive pruning needs a singleton; cell insertion continues");
        return report;
    }
    let members_at_start = indexers.conshash.server_count();
    let mut cursor = min_entry_key();
    let mut blocks = 0usize;
    let mut stalled = 0usize;
    loop {
        blocks += 1;
        if blocks > max_blocks || indexers.conshash.server_count() != members_at_start {
            report.prune_incomplete += 1;
            warn!(
                "index prune gave up after {} blocks ({} keys walked, {} stale) at {:?}; the \
                 report this pass returns is a partial account",
                blocks,
                report.index_keys_scanned,
                report.entries_stale,
                cursor.id()
            );
            break;
        }
        let block = match indexers
            .ranged_client
            .scan_keys(&cursor, PRUNE_BLOCK_KEYS)
            .await
        {
            Ok(block) => block,
            Err(error) => {
                // The tree covering this key could not be reached or loaded.
                // Same accounting as an unreachable entry in the cell walk: no
                // opinion, and the pass is not clean.
                report.prune_incomplete += 1;
                report_unreachable(&cursor, &error);
                break;
            }
        };
        report.index_keys_scanned += block.keys.len() as u64;
        // Judge the whole block before the first delete, so no read guard is
        // alive across an RPC.
        let stale: Vec<EntryKey> = block
            .keys
            .iter()
            .filter(|key| entry_contradicted(chunks, indexers, key, &mut report))
            .cloned()
            .collect();
        for key in &stale {
            report_stale(key);
        }
        if mode.repairs() {
            for key in &stale {
                if indexers.conshash.server_count() != 1 || !owns_cell(indexers, &key.id()) {
                    report.prune_incomplete += 1;
                    return report;
                }
                match indexers.ranged_client.delete(key).await {
                    Ok(_) => {
                        report.entries_pruned += 1;
                    }
                    Err(error) => {
                        report.prune_failed += 1;
                        warn!(
                            "index prune could not delete the stale entry for {:?}: {error:?}",
                            key.id()
                        );
                    }
                }
            }
        }
        // Where to continue: the next key in this tree, or the lower bound of
        // the next tree, or nowhere.
        let advance = match block.next {
            Some(next) => Some(next),
            None => match next_prune_cursor(indexers, &cursor).await {
                Ok(next) => next,
                Err(()) => {
                    report.prune_incomplete += 1;
                    break;
                }
            },
        };
        match advance {
            // Nothing follows: the whole index is walked.
            None => break,
            Some(next) if next > cursor => {
                cursor = next;
                stalled = 0;
            }
            Some(next) => {
                // The walk did not move on. A split may have re-homed this key
                // range, in which case the tree now covering `cursor` really
                // does hold more keys and walking it again is right. If nothing
                // moves, this loop would never end -- so count it and stop
                // after a few goes rather than after a million blocks.
                cursor = next;
                stalled += 1;
                if stalled > 64 {
                    report.prune_incomplete += 1;
                    warn!(
                        "index prune stopped after {} rounds that did not move past {:?}; the \
                         placement map and the trees disagree, and the report this pass returns \
                         is a partial account",
                        stalled,
                        cursor.id()
                    );
                    break;
                }
            }
        }
    }
    report
}

/// The lower bound of the next tree after `at`, or `Ok(None)` when the state
/// machine says nothing follows. Failed placement resolution is incomplete.
async fn next_prune_cursor(
    indexers: &Arc<IndexerClients>,
    at: &EntryKey,
) -> Result<Option<EntryKey>, ()> {
    resolve_prune_cursor(
        || indexers.ranged_client.next_tree(at, Ordering::Forward),
        at,
    )
    .await
}

async fn resolve_prune_cursor<F, Fut>(mut fetch: F, at: &EntryKey) -> Result<Option<EntryKey>, ()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<
        Output = Result<NextTree, bifrost::raft::state_machine::master::ExecError>,
    >,
{
    for attempt in 0..PRUNE_PLACEMENT_RETRIES {
        match fetch().await {
            Ok(NextTree::Found(lower, _)) => return Ok(Some(lower)),
            Ok(NextTree::End) => return Ok(None),
            Ok(NextTree::Unresolved(reason)) => {
                if attempt == PRUNE_PLACEMENT_RETRIES - 1 {
                    warn!(
                        "index prune could not resolve the placement after {:?}: {}",
                        at.id(),
                        reason
                    );
                }
            }
            Err(error) => {
                if attempt == PRUNE_PLACEMENT_RETRIES - 1 {
                    warn!(
                        "index prune could not ask what follows the tree holding {:?}: {error:?}",
                        at.id()
                    );
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Err(())
}

/// Examples of stale entries logged this pass. Same reset discipline, and the
/// same reason: once a stale set proves STABLE across restarts, the ids in it
/// are the whole investigation.
static STALE_LOGGED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn report_stale(key: &EntryKey) {
    const EXAMPLES: usize = 12;
    let n = STALE_LOGGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if n < EXAMPLES {
        warn!(
            "Index prune: entry {:?} is not implied by its cell{}",
            key,
            if n + 1 == EXAMPLES {
                " (further stale entries will not be logged; see the report counts)"
            } else {
                ""
            }
        );
    }
}

/// Ask the index about each key, and in `Repair` mode insert the ones it
/// does not hold.
///
/// Examples logged this pass. Reset when a pass starts rather than left to
/// run for the life of the process -- an operator who runs the scrub a
/// second time is entitled to see examples again, and a counter that only
/// ever counts up would silently stop explaining itself after the first run.
static UNREACHABLE_LOGGED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Examples of MISSING keys logged this pass. Same reset discipline.
static MISSING_LOGGED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Name a few of the cells whose entries are absent.
///
/// A count alone says how bad it is and nothing about what it is. Once a
/// missing set proves STABLE across restarts -- the same number surviving
/// every recovery -- it stops being a durability window and becomes a set of
/// specific cells to go and look at, and then their ids are the whole
/// investigation. Capped like the unreachable examples, for the same reason.
/// A repair the index took and then did not have. Always logged, never
/// sampled: this should be impossible, so every instance is worth a line.
fn report_not_stuck(key: &EntryKey) {
    error!(
        "Index scrub: repaired entry for {:?} was NOT PRESENT when read straight back. The \
         insert was accepted by a tree that is not the one this key resolves to -- placement \
         moving underneath the repair. Reconciliation cannot be trusted while this happens.",
        key.id()
    );
}

fn report_missing(key: &EntryKey) {
    const EXAMPLES: usize = 12;
    let n = MISSING_LOGGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if n < EXAMPLES {
        warn!(
            "Index scrub: no entry for cell {:?} (key {:?}){}",
            key.id(),
            key,
            if n + 1 == EXAMPLES {
                " (further missing entries will not be logged; see the report counts)"
            } else {
                ""
            }
        );
    }
}

/// Log the first few unreachable keys and then stop.
///
/// One line per key is right for a handful and catastrophic for the case
/// that matters: a whole dead tree makes EVERY key under it unreachable, so
/// unthrottled this would emit millions of lines describing one fault --
/// burying, in the operator's log, the report that actually says what
/// happened. The count in the report is the measurement; these lines are
/// only there to name examples.
fn report_unreachable(key: &EntryKey, error: &impl std::fmt::Debug) {
    const EXAMPLES: usize = 8;
    let n = UNREACHABLE_LOGGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if n < EXAMPLES {
        warn!(
            "Index scrub could not reach {:?}: {:?}{}",
            key.id(),
            error,
            if n + 1 == EXAMPLES {
                " (further unreachable keys will not be logged; see the report counts)"
            } else {
                ""
            }
        );
    }
}

/// Ask the index about each key, in bounded-concurrency batches.
async fn reconcile(
    indexers: &Arc<IndexerClients>,
    keys: &[EntryKey],
    mode: ScrubMode,
) -> ScrubReport {
    let mut report = ScrubReport::default();
    let mut in_flight = FuturesUnordered::new();
    let mut pending = keys.iter();

    // Bounded concurrency, not one-at-a-time and not all-at-once. Serially,
    // a scrub costs one network round trip per derived entry, which on a
    // store with billions of them is not a maintenance command anybody can
    // run. Unbounded, it becomes a self-inflicted load spike against an
    // index that may already be the sick part of the system -- and this is a
    // tool for sick systems.
    for key in pending.by_ref().take(concurrency()) {
        in_flight.push(check_one(indexers, key, mode));
    }
    while let Some(outcome) = in_flight.next().await {
        match outcome {
            KeyOutcome::Present => report.entries_present += 1,
            KeyOutcome::Missing => report.entries_missing += 1,
            KeyOutcome::Repaired => {
                report.entries_missing += 1;
                report.entries_repaired += 1;
            }
            KeyOutcome::Unreachable => report.entries_unreachable += 1,
            KeyOutcome::RepairNotStuck => {
                report.entries_missing += 1;
                report.repairs_not_stuck += 1;
            }
        }
        if let Some(key) = pending.next() {
            in_flight.push(check_one(indexers, key, mode));
        }
    }
    report
}

enum KeyOutcome {
    /// Inserted, and then not there when read straight back.
    RepairNotStuck,
    Present,
    Missing,
    Repaired,
    Unreachable,
}

/// How many index round trips are outstanding at once.
///
/// Tunable because the right number depends on the cluster, not on this
/// code, and an operator scrubbing a struggling store needs to be able to
/// turn it down without a rebuild.
fn concurrency() -> usize {
    std::env::var("NEB_SCRUB_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(32)
        .min(1024)
}

/// Ask whether the index holds `key`, and in `Repair` mode insert it if not.
///
/// **Checked first, even in repair mode.** `insert` reports whether the key
/// was absent, so repairing by inserting unconditionally looked like the
/// cheaper design -- one round trip per key instead of two. It is not, and
/// the difference is not small: `contains` is a read, while `insert`
/// descends the B+ tree taking WRITE latches whether or not it ends up
/// changing anything. Measured on an 800k-cell store, a verify pass took
/// 5.7s and an unconditional-insert repair had not finished after three
/// minutes, because it was write-latching all 780,028 entries to fix 26,269.
///
/// Checking first costs a second round trip only for keys that are actually
/// missing, which is the rare case in every store worth repairing.
///
/// The count stays exact because the INSERT's return value decides it, not
/// the check: if a concurrent writer inserts between the two, `insert`
/// returns false and this reports the key as present, which is what it then
/// is. The check only decides whether to attempt the write.
async fn check_one(indexers: &Arc<IndexerClients>, key: &EntryKey, mode: ScrubMode) -> KeyOutcome {
    match indexers.ranged_client.contains(key).await {
        Ok(true) => KeyOutcome::Present,
        Ok(false) if mode.repairs() => match indexers.ranged_client.insert(key).await {
            Ok(true) => {
                report_missing(key);
                // READ IT BACK. An insert that returns true has been applied
                // to a tree; it has not necessarily been applied to the tree
                // this key will be looked up in a moment from now. Placement
                // settles during startup, and an insert into an object that is
                // then superseded -- re-hydrated from disk, replaced by a
                // split -- is silently discarded.
                //
                // That is not hypothetical. A reconcile repaired 8,537 entries
                // and a scrub minutes later, with no writer running in
                // between, found 136 of them still missing. The whole
                // reconcile-before-serving design rests on repairs sticking,
                // so it must not be assumed.
                match indexers.ranged_client.contains(key).await {
                    Ok(true) => KeyOutcome::Repaired,
                    Ok(false) => {
                        report_not_stuck(key);
                        KeyOutcome::RepairNotStuck
                    }
                    // Could not confirm either way; count it as repaired
                    // rather than invent a failure, and let the unreachable
                    // accounting speak for the tree.
                    Err(_) => KeyOutcome::Repaired,
                }
            }
            // Someone else inserted it in between; it is present now.
            Ok(false) => KeyOutcome::Present,
            Err(error) => {
                report_unreachable(key, &error);
                KeyOutcome::Unreachable
            }
        },
        Ok(false) => {
            report_missing(key);
            KeyOutcome::Missing
        }
        Err(error) => {
            // The tree covering this key is absent or unreadable -- exactly
            // the condition this tool is for. Not a missing entry: we do not
            // know what the tree holds.
            report_unreachable(key, &error);
            KeyOutcome::Unreachable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AsyncClient;
    use crate::index::ranged::tree::btree::page_schema;
    use crate::index::ranged::tree::tree::RANGED_TREE_SCHEMA;
    use crate::index::FEATURE_SIZE;
    use crate::ram::cell::OwnedCell;
    use crate::ram::schema::SchemaVid;
    use crate::ram::schema::{Field, IndexType, Schema, SchemaUid};
    use crate::ram::types::{Map, OwnedMap, OwnedValue, Type};
    use crate::server::{NebServer, ServerOptions, Service};
    use tempfile::TempDir;

    const PRICE_FIELD: &str = "price";
    const SCHEMA_ID: u32 = 4210;

    /// The scrub walks EVERY live cell, and the B-tree's own pages are cells
    /// in the same store. Indexing the index would be, at best, unbounded
    /// growth. Nothing filters them by id -- the exclusion is structural,
    /// because their schemas declare no indexed fields -- so this test pins
    /// that structure. If someone adds an indexed field to a page, the scrub
    /// starts indexing the index and this is the test that says so.
    #[test]
    fn index_internal_schemas_declare_no_indexed_fields() {
        assert!(
            page_schema().index_fields.is_empty(),
            "B-tree page cells must derive no index entries"
        );
        assert!(
            RANGED_TREE_SCHEMA.index_fields.is_empty(),
            "ranged tree metadata cells must derive no index entries"
        );
    }

    async fn server_with_indexed_schema(
        addr: &str,
        group: &str,
        dir: &TempDir,
    ) -> (Arc<NebServer>, Arc<AsyncClient>) {
        let backup = dir.path().join("backup");
        let wal = dir.path().join("wal");
        let raft = dir.path().join("raft");
        for d in [&backup, &wal, &raft] {
            std::fs::create_dir_all(d).unwrap();
        }
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(backup.to_str().unwrap().to_string()),
                wal_storage: Some(wal.to_str().unwrap().to_string()),
                raft_storage: Some(raft.to_str().unwrap().to_string()),
                index_enabled: true,
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &addr.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap();

        let schema = Schema::new_with_id(
            SCHEMA_ID,
            "scrub_products",
            None,
            Field::new_schema(vec![Field::new_indexed(
                PRICE_FIELD,
                Type::U64,
                vec![IndexType::Ranged],
            )]),
            false,
            true,
        );
        let client = server.data_client(&vec![addr.to_string()]).await.unwrap();
        client.new_schema_with_id(schema).await.unwrap().unwrap();
        (server, Arc::new(client))
    }

    async fn write_products(client: &Arc<AsyncClient>, count: u64) {
        for i in 0..count {
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[PRICE_FIELD] = OwnedValue::U64(i);
            let cell = OwnedCell::new_with_id(SchemaVid(SCHEMA_ID), &Id::from_parts(3, i), value);
            client.write_cell(cell).await.unwrap().unwrap();
        }
    }

    /// The baseline that gives every other result meaning: a store whose
    /// index was built normally must scrub clean. Without this, a scrub that
    /// reported everything missing would look like a successful repair.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_healthy_index_scrubs_clean() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_clean",
            &dir,
        )
        .await;
        write_products(&client, 40).await;
        crate::index::builder::IndexBuilder::await_all_indices().await;

        let indexers = &server.indexer().unwrap().clients;
        let report = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;

        println!("clean-store scrub: {}", report);
        assert!(
            report.entries_derived >= 40,
            "expected at least one entry per product, got {}",
            report.entries_derived
        );
        assert_eq!(report.entries_missing, 0, "healthy index reported holes");
        assert_eq!(report.entries_present, report.entries_derived);
        assert!(
            report.is_clean(),
            "healthy index did not scrub clean: {}",
            report
        );
    }

    /// The whole point: an entry lost from the index is found, and repair
    /// puts it back. The hole is made by deleting through the index client,
    /// which leaves the CELL untouched -- exactly the shape of the real
    /// failure, where data survives and the index that finds it does not.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hole_in_the_index_is_found_and_repaired() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_repair",
            &dir,
        )
        .await;
        write_products(&client, 40).await;
        crate::index::builder::IndexBuilder::await_all_indices().await;

        let indexers = &server.indexer().unwrap().clients;

        // Collect the keys the store's own cells imply, then punch a hole in
        // the index for a few of them.
        let baseline = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        assert!(baseline.is_clean(), "precondition: {}", baseline);

        let mut removed = Vec::new();
        for chunk in &server.chunks().list {
            for segment in chunk.segments() {
                for entry in chunk.live_entries(&segment) {
                    let EntryContent::Cell(header) = entry.content else {
                        continue;
                    };
                    let mut keys = Vec::new();
                    let mut ignored = ScrubReport::default();
                    derive_into(chunk, header.id(), &mut keys, &mut ignored);
                    for key in keys {
                        if removed.len() < 3 {
                            removed.push(key);
                        }
                    }
                }
            }
        }
        assert_eq!(removed.len(), 3, "need three keys to remove");
        for key in &removed {
            assert!(
                indexers.ranged_client.delete(key).await.unwrap(),
                "failed to punch a hole for {:?}",
                key.id()
            );
        }

        let found = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("after damage: {}", found);
        assert_eq!(
            found.entries_missing, 3,
            "scrub did not find the holes: {}",
            found
        );
        assert_eq!(found.entries_repaired, 0, "verify mode must not write");
        assert!(!found.is_clean());

        let repaired = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        println!("after repair: {}", repaired);
        assert_eq!(
            repaired.entries_repaired, 3,
            "repair did not fill the holes"
        );

        let after = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("after re-verify: {}", after);
        assert!(
            after.is_clean(),
            "index still holed after repair: {}",
            after
        );
        assert_eq!(after.entries_present, baseline.entries_present);
    }

    /// A typo must not silently mean "off". A store running for months in
    /// the belief that it is being scrubbed is the failure this guards.
    #[test]
    fn the_recovery_knob_refuses_what_it_does_not_understand() {
        // Serialised with the other env test by using distinct values only;
        // std::env is process-global, so this test owns the variable.
        let restore = std::env::var("NEB_SCRUB_ON_RECOVERY").ok();

        for (value, expected) in [
            ("verify", Some(ScrubMode::Verify)),
            ("repair", Some(ScrubMode::Repair)),
            ("REPAIR", Some(ScrubMode::Repair)),
            ("off", None),
            ("", None),
            ("0", None),
            // Anything else is refused rather than guessed at.
            ("repare", None),
            ("true", None),
            ("yes", None),
        ] {
            std::env::set_var("NEB_SCRUB_ON_RECOVERY", value);
            assert_eq!(mode_from_env(), expected, "for {:?}", value);
        }

        std::env::remove_var("NEB_SCRUB_ON_RECOVERY");
        assert_eq!(mode_from_env(), None, "unset must mean off");

        if let Some(v) = restore {
            std::env::set_var("NEB_SCRUB_ON_RECOVERY", v);
        }
    }

    /// The rule that keeps the tool honest about what it could not check.
    ///
    /// A tree whose pages are unreadable answers neither `contains` nor
    /// `insert`, so its entries land in `entries_unreachable` -- and that
    /// range is exactly the one an operator is investigating. Folding those
    /// into "clean" would make the scrub agree that a broken range is fine,
    /// which is worse than having no scrub at all.
    #[test]
    fn entries_it_could_not_check_are_never_reported_clean() {
        let mut report = ScrubReport::default();
        report.entries_derived = 10;
        report.entries_present = 9;
        report.entries_unreachable = 1;
        assert!(
            !report.is_clean(),
            "a range the scrub could not reach was reported clean"
        );

        report.entries_unreachable = 0;
        report.entries_present = 10;
        assert!(report.is_clean());

        // Unreadable cells are equally not-clean: the pass formed no opinion
        // about the entries they would have contributed.
        report.cells_unreadable = 1;
        assert!(!report.is_clean());
    }

    /// The library function being right is not the same as the command
    /// being reachable. This drives the whole path an operator uses --
    /// client fan-out, RPC, server-side walk -- because a scrub nobody can
    /// invoke fixes nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_scrub_is_reachable_over_rpc() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_rpc",
            &dir,
        )
        .await;
        write_products(&client, 20).await;
        crate::index::builder::IndexBuilder::await_all_indices().await;

        let over_rpc = client.scrub_ranged_index(false).await.unwrap();
        println!("scrub over rpc: {}", over_rpc);
        assert!(over_rpc.is_clean(), "rpc scrub not clean: {}", over_rpc);
        assert!(over_rpc.entries_derived >= 20);

        // The fan-out must agree with what the node itself sees; a
        // silently-empty report would pass every assertion above.
        let indexers = &server.indexer().unwrap().clients;
        let in_process = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        assert_eq!(
            over_rpc.entries_derived, in_process.entries_derived,
            "rpc scrub disagreed with the in-process walk"
        );
    }

    /// Repair must be idempotent: running it on a healthy store is a no-op,
    /// not a second copy of every entry. An operator's first instinct on any
    /// suspicion is to run the repair, so "safe to run when nothing is
    /// wrong" is a property the tool has to have.
    #[tokio::test(flavor = "multi_thread")]
    async fn repairing_a_healthy_index_changes_nothing() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_idempotent",
            &dir,
        )
        .await;
        write_products(&client, 25).await;
        crate::index::builder::IndexBuilder::await_all_indices().await;

        let indexers = &server.indexer().unwrap().clients;
        let first = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        println!("repair on healthy store: {}", first);
        assert_eq!(
            first.entries_repaired, 0,
            "repair invented entries on a healthy store: {}",
            first
        );

        let after = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        assert!(after.is_clean(), "{}", after);
        assert_eq!(after.entries_derived, first.entries_derived);
    }

    // ---- a crash, and what recovery owes the index -------------------------

    const CRASH_SCHEMA_ID: u32 = 4212;
    /// Half of these cells get a new price and half are deleted, so the count
    /// must be even and big enough to outgrow one B-tree leaf.
    const CRASH_CELLS: u64 = 240;

    /// Scratch on the big disk, never `/tmp`: a crash restart leaves WAL,
    /// backup and raft files behind.
    fn crash_scratch() -> TempDir {
        let base = std::path::PathBuf::from("/mnt/ext/morpheus-fixes/indexfix/scratch");
        std::fs::create_dir_all(&base).expect("scratch base");
        tempfile::Builder::new()
            .prefix("neb-index-crash-")
            .tempdir_in(base)
            .expect("scratch dir")
    }

    fn crash_schema() -> Schema {
        Schema::new_with_id(
            CRASH_SCHEMA_ID,
            "crash_products",
            None,
            Field::new_schema(vec![Field::new_indexed(
                PRICE_FIELD,
                Type::U64,
                vec![IndexType::Ranged],
            )]),
            false,
            true,
        )
    }

    fn crash_cell(id: &Id, price: u64) -> OwnedCell {
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[PRICE_FIELD] = OwnedValue::U64(price);
        OwnedCell::new_with_id(SchemaVid(CRASH_SCHEMA_ID), id, value)
    }

    /// Every ranged key one cell implies: its indexed-field entries and, for a
    /// scannable schema, its enumeration entry. Derived through the write
    /// path's own probe, which is what the reconcile uses too.
    fn implied_keys(cell: &OwnedCell, schema: &Schema) -> Vec<EntryKey> {
        let mut keys: Vec<EntryKey> = probe_cell_indices(cell, schema)
            .iter()
            .flat_map(|res| res.ranged_keys().cloned())
            .collect();
        if schema.is_scannable {
            keys.push(EntryKey::for_scannable(&cell.id(), schema.uid));
        }
        keys
    }

    async fn start_crash_server(
        dir: &std::path::Path,
        group: &str,
        addr: &str,
        enable_recovery: bool,
    ) -> (Arc<NebServer>, Arc<AsyncClient>) {
        for d in ["backup", "wal", "raft"] {
            std::fs::create_dir_all(dir.join(d)).unwrap();
        }
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 64 * 1024 * 1024,
                db_size: 64 * 1024 * 1024,
                tiered_config: None,
                backup_storage: Some(dir.join("backup").to_string_lossy().to_string()),
                wal_storage: Some(dir.join("wal").to_string_lossy().to_string()),
                raft_storage: Some(dir.join("raft").to_string_lossy().to_string()),
                index_enabled: true,
                services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
                enable_recovery,
                disable_storage_locks: true,
            },
            &addr.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap();
        let client = server.data_client(&vec![addr.to_string()]).await.unwrap();
        let client = Arc::new(client);
        // The first start registers it; a restart finds it already in the
        // recovered catalog and answers `NameExists`, which is not an error.
        let _ = client
            .new_schema_with_id(crash_schema())
            .await
            .expect("schema registration rpc");
        (server, client)
    }

    /// The crash window, made deterministic rather than raced.
    ///
    /// `flush_all_trees` is the checkpoint the balancer runs every 60 s, and it
    /// ends by resetting this server's write-back hub
    /// (`ranged/tree/service.rs:1217`), so every index change after it stays in
    /// memory. Dropping the server without `shutdown` is the SIGKILL: `impl
    /// Drop for NebServer` (`server/mod.rs:1381`) stops the threads and does no
    /// index drain, no WAL sync and no archive. The restarted store therefore
    /// holds cells at their new state and a ranged tree at its old one -- the
    /// shape the durability campaign reports as thousands of STALE entries that
    /// fail every later check (`crash-verify2/REPORT.md`, "Index check").
    #[tokio::test(flavor = "multi_thread")]
    async fn a_crash_leaves_the_recovered_index_agreeing_with_its_cells() {
        let _ = env_logger::try_init();
        let dir = crash_scratch();
        let group = "index_crash_reconcile";
        let (server, client) = start_crash_server(
            dir.path(),
            group,
            &crate::utils::test_port::unique_localhost_addr(),
            true,
        )
        .await;
        let schema = crash_schema();
        let ids: Vec<Id> = (0..CRASH_CELLS)
            .map(|i| Id::from_parts(3, 500_000 + i))
            .collect();

        for (i, id) in ids.iter().enumerate() {
            client
                .write_cell(crash_cell(id, i as u64))
                .await
                .unwrap()
                .unwrap();
        }
        crate::index::builder::IndexBuilder::await_all_indices().await;

        // Checkpoint the index: durable from here, and the write-back hub is
        // stopped, so what follows cannot reach disk.
        crate::index::ranged::tree::service::local_tree_service(group, group)
            .expect("the indexed database publishes its tree service")
            .flush_all_trees()
            .await;

        let updated = CRASH_CELLS / 2;
        let new_price = |i: u64| 10_000 + i;
        for i in 0..updated {
            client
                .update_cell(crash_cell(&ids[i as usize], new_price(i)))
                .await
                .unwrap()
                .unwrap();
        }
        for i in updated..CRASH_CELLS {
            client.remove_cell(ids[i as usize]).await.unwrap().unwrap();
        }
        crate::index::builder::IndexBuilder::await_all_indices().await;

        // The live store agrees with itself before the kill: the in-memory tree
        // already dropped the old keys. Without this, the assertions after the
        // restart could be satisfied by an index that never held them.
        let ranged = &server.indexer().unwrap().clients.ranged_client;
        for i in 0..CRASH_CELLS {
            let id = ids[i as usize];
            for key in implied_keys(&crash_cell(&id, i), &schema) {
                let gone = !ranged.contains(&key).await.unwrap();
                if i < updated && key == EntryKey::for_scannable(&id, schema.uid) {
                    // The enumeration entry of an UPDATED cell is unchanged: it
                    // names the id, not the value, and it must stay.
                    assert!(!gone, "pre-crash: the scannable entry of {:?} vanished", id);
                    continue;
                }
                assert!(
                    gone,
                    "pre-crash: the key {:?} of cell {:?} is still in the live index",
                    key, id
                );
            }
        }

        // The kill.
        drop(client);
        drop(server);

        let (server, client) = start_crash_server(
            dir.path(),
            group,
            &crate::utils::test_port::unique_localhost_addr(),
            true,
        )
        .await;
        let ranged = &server.indexer().unwrap().clients.ranged_client;

        let mut stale = Vec::new();
        let mut missing = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let i = i as u64;
            let live = i < updated;
            let price = if live { new_price(i) } else { i };
            assert_eq!(
                matches!(client.read_cell(*id).await.unwrap(), Ok(_)),
                live,
                "cell {:?} did not come back as expected after the crash",
                id
            );
            for key in implied_keys(&crash_cell(id, price), &schema) {
                let present = ranged.contains(&key).await.unwrap();
                match (live, present) {
                    (true, false) => missing.push((id, key)),
                    (false, true) => stale.push((id, key)),
                    _ => {}
                }
            }
        }
        println!(
            "after the crash: {} stale entries, {} missing entries of {} expected; the recovery \
             reconcile pruned {} and repaired {}",
            stale.len(),
            missing.len(),
            CRASH_CELLS * 2 - updated,
            LAST_RECONCILE_PRUNED.load(std::sync::atomic::Ordering::Relaxed),
            LAST_RECONCILE_REPAIRED.load(std::sync::atomic::Ordering::Relaxed)
        );
        assert!(
            missing.is_empty(),
            "{} index entries are MISSING for live cells after recovery: {:?}",
            missing.len(),
            &missing[..missing.len().min(4)]
        );
        assert!(
            stale.is_empty(),
            "{} STALE index entries survived recovery: the index still lists a value or an id \
             its cells no longer imply: {:?}",
            stale.len(),
            &stale[..stale.len().min(4)]
        );

        // The whole-store form of the same statement, as an operator sees it.
        let indexers = &server.indexer().unwrap().clients;
        let report = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("post-recovery scrub: {}", report);
        assert!(
            report.is_clean(),
            "recovered index is not clean: {}",
            report
        );

        drop(client);
        drop(server);
        let (server, _client) = start_crash_server(
            dir.path(),
            group,
            &crate::utils::test_port::unique_localhost_addr(),
            true,
        )
        .await;
        assert_eq!(
            LAST_RECONCILE_PRUNED.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a second recovery must not prune live entries"
        );
        assert_eq!(
            LAST_RECONCILE_REPAIRED.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "a second recovery must not insert duplicate entries"
        );
        let report = scrub_ranged_index(
            server.chunks(),
            &server.indexer().unwrap().clients,
            ScrubMode::Verify,
        )
        .await;
        assert!(report.is_clean(), "second recovery: {}", report);
    }

    /// The prune half of the reconcile, without a crash: an entry the cells do
    /// not imply must be reported in `Verify` and removed in `Repair`, and a
    /// repair must not invent one.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_entry_no_cell_implies_is_reported_and_pruned() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_prune",
            &dir,
        )
        .await;
        write_products(&client, 30).await;
        crate::index::builder::IndexBuilder::await_all_indices().await;

        let indexers = &server.indexer().unwrap().clients;
        let baseline = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        assert!(baseline.is_clean(), "precondition: {}", baseline);

        // Two entries no cell implies: one for a live cell under a value it does
        // not hold, and one for an id the store has never seen.
        let ghost = EntryKey::from_props(
            &Id::from_parts(3, 999_999),
            &[7u8; FEATURE_SIZE],
            1,
            SchemaUid(SCHEMA_ID),
        );
        indexers.ranged_client.insert(&ghost).await.unwrap();
        let wrong_value = EntryKey::from_props(
            &Id::from_parts(3, 7),
            &[9u8; FEATURE_SIZE],
            1,
            SchemaUid(SCHEMA_ID),
        );
        indexers.ranged_client.insert(&wrong_value).await.unwrap();

        let found = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("with two stale entries: {}", found);
        assert_eq!(
            found.entries_stale, 2,
            "the scrub did not see the stale entries: {}",
            found
        );
        assert_eq!(
            found.entries_pruned, 0,
            "verify mode must not write: {}",
            found
        );
        assert!(
            !found.is_clean(),
            "stale entries were reported clean: {}",
            found
        );

        let repaired = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        println!("after the prune: {}", repaired);
        assert_eq!(repaired.entries_stale, 2, "{}", repaired);
        assert_eq!(repaired.entries_pruned, 2, "{}", repaired);
        assert!(
            !indexers.ranged_client.contains(&ghost).await.unwrap(),
            "the stale entry for the absent id survived the repair"
        );
        assert!(
            !indexers.ranged_client.contains(&wrong_value).await.unwrap(),
            "the stale entry for the wrong value survived the repair"
        );

        let after = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("re-verify: {}", after);
        assert!(
            after.is_clean(),
            "index still stale after the prune: {}",
            after
        );
        assert_eq!(
            after.entries_derived, baseline.entries_derived,
            "the prune took live entries with it"
        );
    }
    fn wrong_key(id: Id, schema: u32) -> EntryKey {
        EntryKey::from_props(&id, &[9u8; FEATURE_SIZE], 1, SchemaUid(schema))
    }

    async fn forget_product_schema(server: &Arc<NebServer>, client: &Arc<AsyncClient>) -> Schema {
        let schema = (*server.chunks().list[0]
            .meta
            .schemas
            .get(&SchemaVid(SCHEMA_ID))
            .unwrap())
        .clone();
        client
            .del_schema(schema.name.clone())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while server.chunks().list[0]
                .meta
                .schemas
                .get(&SchemaVid(SCHEMA_ID))
                .is_some()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("schema deletion callback");
        schema
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_cell_is_not_a_reason_to_prune() {
        use lightning::map::Map as _;
        let dir = crash_scratch();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_unreadable",
            &dir,
        )
        .await;
        write_products(&client, 2).await;
        let indexers = &server.indexer().unwrap().clients;
        let key = wrong_key(Id::from_parts(3, 0), SCHEMA_ID);
        indexers.ranged_client.insert(&key).await.unwrap();
        let chunk = server.chunks().locate_chunk_by_partition(3);
        let bits = key.id().bits() as usize;
        let original = chunk.cell_index.get(&bits).unwrap();
        let other = chunk
            .cell_index
            .get(&(Id::from_parts(3, 1).bits() as usize))
            .unwrap();
        // A real stale-pointer read error, with intact bytes in both cells.
        chunk.cell_index.insert(bits, other);
        let report = prune_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        chunk.cell_index.insert(bits, original);
        assert!(
            indexers.ranged_client.contains(&key).await.unwrap(),
            "unreadable cell was pruned"
        );
        assert!(report.cells_unreadable > 0, "{}", report);
        assert_eq!(report.entries_pruned, 0, "{}", report);
        assert!(!report.is_clean(), "{}", report);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_schema_preserves_all_its_entries() {
        let dir = crash_scratch();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_missing_schema",
            &dir,
        )
        .await;
        write_products(&client, 1).await;
        let indexers = &server.indexer().unwrap().clients;
        let key = wrong_key(Id::from_parts(3, 0), SCHEMA_ID);
        indexers.ranged_client.insert(&key).await.unwrap();
        let _schema = forget_product_schema(&server, &client).await;
        let report = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        assert!(indexers.ranged_client.contains(&key).await.unwrap());
        assert!(report.cells_schema_missing > 0, "{}", report);
        assert_eq!(report.entries_pruned, 0, "{}", report);
        assert!(
            !report.is_clean(),
            "missing schema was called clean: {}",
            report
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_schema_imported_after_start_allows_a_later_repair() {
        let dir = crash_scratch();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_late_schema",
            &dir,
        )
        .await;
        write_products(&client, 1).await;
        let indexers = &server.indexer().unwrap().clients;
        let key = wrong_key(Id::from_parts(3, 0), SCHEMA_ID);
        indexers.ranged_client.insert(&key).await.unwrap();
        let schema = forget_product_schema(&server, &client).await;
        reconcile_index_before_serving(
            server.chunks(),
            indexers,
            "scrub_late_schema",
            "scrub_late_schema",
        )
        .await;
        assert!(indexers.ranged_client.contains(&key).await.unwrap());
        client
            .new_schema_with_id(schema.clone())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while server.chunks().list[0]
                .meta
                .schemas
                .get(&SchemaVid(SCHEMA_ID))
                .is_none()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("late schema callback");
        let report = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        assert_eq!(report.entries_pruned, 1, "{}", report);
        assert!(!indexers.ranged_client.contains(&key).await.unwrap());
        assert!(
            scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify)
                .await
                .is_clean()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_prune_crosses_scan_batches_without_skipping_keys() {
        let dir = crash_scratch();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_batches",
            &dir,
        )
        .await;
        write_products(&client, 12).await;
        let indexers = &server.indexer().unwrap().clients;
        let before = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        let ghosts: Vec<_> = (0..2200)
            .map(|i| wrong_key(Id::from_parts(3, 1_000_000 + i), SCHEMA_ID))
            .collect();
        for key in &ghosts {
            indexers.ranged_client.insert(key).await.unwrap();
        }
        let report = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        assert_eq!(
            report.index_keys_scanned,
            before.index_keys_scanned + 2200,
            "{}",
            report
        );
        assert_eq!(report.entries_pruned, 2200, "{}", report);
        assert_eq!(report.prune_incomplete, 0, "{}", report);
        for key in ghosts {
            assert!(!indexers.ranged_client.contains(&key).await.unwrap());
        }
        assert!(
            scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify)
                .await
                .is_clean()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_block_cap_reports_a_partial_prune() {
        let dir = crash_scratch();
        let (server, _client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_cap",
            &dir,
        )
        .await;
        let indexers = &server.indexer().unwrap().clients;
        for i in 0..1025 {
            indexers
                .ranged_client
                .insert(&wrong_key(Id::from_parts(3, 1_000_000 + i), SCHEMA_ID))
                .await
                .unwrap();
        }
        let report =
            prune_ranged_index_with_cap(server.chunks(), indexers, ScrubMode::Repair, 1).await;
        assert_eq!(report.index_keys_scanned, 1024, "{}", report);
        assert_eq!(report.entries_pruned, 1024, "{}", report);
        assert_eq!(report.prune_incomplete, 1, "{}", report);
        assert!(!report.is_clean());
        let rest = prune_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        assert_eq!(rest.entries_pruned, 1, "{}", rest);
        assert_eq!(rest.prune_incomplete, 0, "{}", rest);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_insert_pass_restores_field_and_scannable_entries_after_a_prune_race() {
        use lightning::map::Map as _;
        let dir = crash_scratch();
        let (server, client) = server_with_indexed_schema(
            &crate::utils::test_port::unique_localhost_addr(),
            "scrub_restore_scannable",
            &dir,
        )
        .await;
        let schema = Schema::new_with_id(
            4219,
            "enumeration_only",
            None,
            Field::new_schema(vec![Field::new_unindexed(PRICE_FIELD, Type::U64)]),
            false,
            true,
        );
        client
            .new_schema_with_id(schema.clone())
            .await
            .unwrap()
            .unwrap();
        let id = Id::from_parts(3, 99);
        let mut data = OwnedValue::Map(OwnedMap::new());
        data[PRICE_FIELD] = OwnedValue::U64(5);
        client
            .write_cell(OwnedCell::new_with_id(SchemaVid(4219), &id, data))
            .await
            .unwrap()
            .unwrap();
        write_products(&client, 1).await;
        let key = EntryKey::for_scannable(&id, schema.uid);
        let product = Id::from_parts(3, 0);
        let product_schema = (*server.chunks().list[0]
            .meta
            .schemas
            .get(&SchemaVid(SCHEMA_ID))
            .unwrap())
        .clone();
        let mut product_data = OwnedValue::Map(OwnedMap::new());
        product_data[PRICE_FIELD] = OwnedValue::U64(0);
        let product_keys = implied_keys(
            &OwnedCell::new_with_id(SchemaVid(SCHEMA_ID), &product, product_data),
            &product_schema,
        );
        let indexers = &server.indexer().unwrap().clients;
        assert!(indexers.ranged_client.contains(&key).await.unwrap());
        let chunk = server.chunks().locate_chunk_by_partition(3);
        // Stage the visibility race deterministically. The live bytes remain
        // intact. Restore the cell index before the insertion half sees them.
        let location = chunk.cell_index.remove(&(id.bits() as usize)).unwrap();
        let product_location = chunk.cell_index.remove(&(product.bits() as usize)).unwrap();
        let pruned = prune_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        chunk.cell_index.insert(id.bits() as usize, location);
        chunk
            .cell_index
            .insert(product.bits() as usize, product_location);
        assert_eq!(pruned.entries_pruned, 3, "{}", pruned);
        assert!(!indexers.ranged_client.contains(&key).await.unwrap());
        let inserted = reconcile_cell_entries(server.chunks(), indexers, ScrubMode::Repair).await;
        assert_eq!(inserted.entries_repaired, 3, "{}", inserted);
        assert!(
            indexers.ranged_client.contains(&key).await.unwrap(),
            "insertion did not restore enumeration"
        );
        for key in product_keys {
            assert!(
                indexers.ranged_client.contains(&key).await.unwrap(),
                "insertion did not restore {:?}",
                key
            );
        }
    }

    #[tokio::test]
    async fn exhausted_placement_retries_are_not_the_end_of_the_index() {
        let at = min_entry_key();
        let result = resolve_prune_cursor(
            || async { Ok(NextTree::Unresolved("not ready".into())) },
            &at,
        )
        .await;
        assert!(
            result.is_err(),
            "an unresolved placement was called the end"
        );
        assert_eq!(
            resolve_prune_cursor(|| async { Ok(NextTree::End) }, &at)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cluster_prune_preserves_foreign_cells_and_migrating_slots() {
        std::env::set_var("NEB_DISABLE_JOIN_FILL", "1");
        let addresses: Vec<_> = (0..2)
            .map(|_| crate::utils::test_port::unique_localhost_addr())
            .collect();
        let group = "scrub_cluster_guard".to_string();
        let opts = ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        };
        let mut servers = Vec::new();
        for address in &addresses {
            servers.push(
                NebServer::new_cluster_from_opts(&opts, address, &addresses, &group, async |_| {})
                    .await
                    .unwrap(),
            );
        }
        std::env::remove_var("NEB_DISABLE_JOIN_FILL");
        let client = Arc::new(servers[0].data_client(&addresses).await.unwrap());
        client
            .new_schema_with_id(crash_schema())
            .await
            .unwrap()
            .unwrap();
        let id = Id::from_parts(3, 77);
        let owner = servers[1].server_id;
        let placement = crate::migration::placement_client(&client);
        let slot_group = crate::slots::slot_group_id(&group);
        placement
            .set_slot_owner(&slot_group, &3, &owner)
            .await
            .unwrap();
        assert!(servers[0].refresh_slot_placement().await);
        assert!(servers[1].refresh_slot_placement().await);
        client.reload_slot_owners().await;
        client
            .write_cell(crash_cell(&id, 77))
            .await
            .unwrap()
            .unwrap();
        let key = implied_keys(&crash_cell(&id, 77), &crash_schema()).remove(0);
        let indexers = &servers[0].indexer().unwrap().clients;
        assert!(indexers.ranged_client.contains(&key).await.unwrap());
        let mut report = ScrubReport::default();
        assert!(!entry_contradicted(
            servers[0].chunks(),
            indexers,
            &key,
            &mut report
        ));
        assert_eq!(report.entries_not_local, 1);
        let healthy = client.scrub_ranged_index(false).await.unwrap();
        assert!(
            healthy.is_clean(),
            "stable cluster verification: {}",
            healthy
        );
        // During transfer the donor remains the owner. Deliberately install
        // a wrong local cache, reproducing the reviewer's dangerous miss.
        placement
            .begin_slot_migration(&slot_group, &3, &owner, &servers[0].server_id)
            .await
            .unwrap()
            .unwrap();
        let migrating = client.scrub_ranged_index(false).await.unwrap();
        assert!(
            migrating.prune_incomplete > 0,
            "migration was reported complete: {}",
            migrating
        );
        assert!(!migrating.is_clean());
        indexers.conshash.note_slot_owner(
            3,
            servers[0].server_id,
            crate::slots::SLOT_COUNT,
            u64::MAX,
        );
        assert!(servers[0].chunks().read_cell(&id).is_err());
        let report = prune_ranged_index(servers[0].chunks(), indexers, ScrubMode::Repair).await;
        assert!(
            indexers.ranged_client.contains(&key).await.unwrap(),
            "a remote live entry was deleted"
        );
        assert_eq!(report.entries_pruned, 0, "{}", report);
        assert_eq!(report.prune_incomplete, 1, "{}", report);
        assert!(servers[1].chunks().read_cell(&id).is_ok());
        placement
            .abort_slot_migration(&slot_group, &3)
            .await
            .unwrap()
            .unwrap();
        for server in servers {
            server.shutdown().await;
        }
    }
}
