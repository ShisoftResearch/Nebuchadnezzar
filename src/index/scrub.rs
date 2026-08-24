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
//! **It never deletes.** Repair inserts and nothing else. An entry that
//! looks unaccounted-for may belong to a cell written after this pass read
//! that segment, or to a cell on another node this pass cannot see -- a
//! ranged tree covers a key range across the WHOLE cluster, while this walk
//! sees one node's chunks. Deleting on a partial view is how a diagnostic
//! becomes an outage. The asymmetry is what makes a single-node scrub sound:
//! a cell HERE whose entry is missing is a genuine hole no matter what other
//! nodes hold, so missing entries are always safe to report and to insert.
//!
//! Consequently this pass can say "the index is missing these entries" but
//! never "the index holds entries it should not"; the latter needs a
//! cluster-wide pass and is not attempted here.

use std::sync::Arc;

use crate::index::builder::probe_cell_indices;
use crate::index::EntryKey;
use crate::index::IndexerClients;
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
#[derive(Debug, Default, Clone)]
pub struct ScrubReport {
    /// Live cells walked.
    pub cells_scanned: u64,
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
            && self.repairs_failed == 0
    }

    fn merge(&mut self, other: &ScrubReport) {
        self.cells_scanned += other.cells_scanned;
        self.cells_unreadable += other.cells_unreadable;
        self.cells_schema_missing += other.cells_schema_missing;
        self.entries_derived += other.entries_derived;
        self.entries_present += other.entries_present;
        self.entries_missing += other.entries_missing;
        self.entries_repaired += other.entries_repaired;
        self.entries_unreachable += other.entries_unreachable;
        self.repairs_failed += other.repairs_failed;
    }
}

impl std::fmt::Display for ScrubReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cells={} (unreadable={}, schema-missing={}) entries={} \
             present={} missing={} repaired={} unreachable={} repair-failed={}",
            self.cells_scanned,
            self.cells_unreadable,
            self.cells_schema_missing,
            self.entries_derived,
            self.entries_present,
            self.entries_missing,
            self.entries_repaired,
            self.entries_unreachable,
            self.repairs_failed,
        )
    }
}

/// Walk every live cell in `chunks` and reconcile its ranged index entries.
///
/// Batched by segment: the derivation half holds cell read guards and the
/// reconciliation half awaits RPCs, and those two must not overlap -- a
/// guard held across an await pins a segment for the length of a network
/// round trip, which is how a scrub of a large store would block the
/// cleaner for its entire duration.
pub async fn scrub_ranged_index(
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
}

/// Ask the index about each key, and in `Repair` mode insert the ones it
/// does not hold.
///
/// Repair inserts unconditionally rather than checking first: `insert`
/// already returns whether the key was absent, so a check would double the
/// RPCs to learn what the insert reports anyway -- and worse, it would open
/// a window in which a concurrent writer inserts between the check and the
/// repair, making the count wrong in the one mode where it must be exact.
async fn reconcile(
    indexers: &Arc<IndexerClients>,
    keys: &[EntryKey],
    mode: ScrubMode,
) -> ScrubReport {
    let mut report = ScrubReport::default();
    for key in keys {
        if mode.repairs() {
            match indexers.ranged_client.insert(key).await {
                Ok(true) => {
                    // The tree did not have it. That is the hole.
                    report.entries_missing += 1;
                    report.entries_repaired += 1;
                }
                Ok(false) => report.entries_present += 1,
                Err(error) => {
                    warn!("Index scrub could not repair {:?}: {:?}", key.id(), error);
                    report.entries_unreachable += 1;
                }
            }
        } else {
            match indexers.ranged_client.contains(key).await {
                Ok(true) => report.entries_present += 1,
                Ok(false) => report.entries_missing += 1,
                Err(error) => {
                    // The tree covering this key is absent or unreadable --
                    // exactly the condition this tool is for. Not a missing
                    // entry: we do not know what the tree holds.
                    debug!("Index scrub could not reach {:?}: {:?}", key.id(), error);
                    report.entries_unreachable += 1;
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AsyncClient;
    use crate::index::ranged::tree::btree::page_schema;
    use crate::index::ranged::tree::tree::RANGED_TREE_SCHEMA;
    use crate::ram::cell::OwnedCell;
    use crate::ram::schema::{Field, IndexType, Schema};
    use crate::ram::types::{Map, OwnedMap, OwnedValue, Type};
    use crate::server::{NebServer, Service, ServerOptions};
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
        client
            .new_schema_with_id(schema)
            .await
            .unwrap()
            .unwrap();
        (server, Arc::new(client))
    }

    async fn write_products(client: &Arc<AsyncClient>, count: u64) {
        for i in 0..count {
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[PRICE_FIELD] = OwnedValue::U64(i);
            let cell = OwnedCell::new_with_id(SCHEMA_ID, &Id::from_parts(3, i), value);
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
        let (server, client) =
            server_with_indexed_schema("127.0.0.1:6841", "scrub_clean", &dir).await;
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
        assert!(report.is_clean(), "healthy index did not scrub clean: {}", report);
    }

    /// The whole point: an entry lost from the index is found, and repair
    /// puts it back. The hole is made by deleting through the index client,
    /// which leaves the CELL untouched -- exactly the shape of the real
    /// failure, where data survives and the index that finds it does not.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_hole_in_the_index_is_found_and_repaired() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) =
            server_with_indexed_schema("127.0.0.1:6842", "scrub_repair", &dir).await;
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
        assert_eq!(found.entries_missing, 3, "scrub did not find the holes: {}", found);
        assert_eq!(found.entries_repaired, 0, "verify mode must not write");
        assert!(!found.is_clean());

        let repaired = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Repair).await;
        println!("after repair: {}", repaired);
        assert_eq!(repaired.entries_repaired, 3, "repair did not fill the holes");

        let after = scrub_ranged_index(server.chunks(), indexers, ScrubMode::Verify).await;
        println!("after re-verify: {}", after);
        assert!(after.is_clean(), "index still holed after repair: {}", after);
        assert_eq!(after.entries_present, baseline.entries_present);
    }

    /// Repair must be idempotent: running it on a healthy store is a no-op,
    /// not a second copy of every entry. An operator's first instinct on any
    /// suspicion is to run the repair, so "safe to run when nothing is
    /// wrong" is a property the tool has to have.
    #[tokio::test(flavor = "multi_thread")]
    async fn repairing_a_healthy_index_changes_nothing() {
        let _ = env_logger::try_init();
        let dir = TempDir::new().unwrap();
        let (server, client) =
            server_with_indexed_schema("127.0.0.1:6843", "scrub_idempotent", &dir).await;
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
}
