use crate::index::scrub::{ScrubMode, ScrubReport};
use crate::ram::cell::Cell;
use crate::ram::schema::{post_schema_add, post_schema_delete, post_schema_evolve};
use crate::ram::types::Id;
use crate::server::DatabaseRuntime;
use crate::{
    client::AsyncClient,
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, WriteError},
};
use bifrost::rpc::*;
use bifrost_hasher::hash_str;
use dovahkiin::expr::serde::Expr;
use dovahkiin::expr::symbols::utils::is_true;
use dovahkiin::integrated::lisp;
use dovahkiin::types::{OwnedValue, Value};
use futures::future::BoxFuture;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{self, AtomicU64};

use bifrost_plugins::hash_ident;

/// Index task failures that were surfaced to a caller. Paired with the
/// unscoped-failure counter in the index builder: together they say how much
/// index work was lost and how much of it anyone was told about.
pub static INDEX_TASK_FAILURES: AtomicU64 = AtomicU64::new(0);

/// A write outcome that can be failed after the fact by its own index work.
///
/// `with_indices_ensured` is generic over what the write returns, so folding
/// an index failure into that value needs a hook rather than a match.
#[cfg(test)]
pub(crate) static SPECIALIZED_BATCH_CALLS: atomic::AtomicU64 = atomic::AtomicU64::new(0);

pub trait IndexOutcome {
    fn fail_with_index_error(self, detail: String) -> Self;
    fn note_index_tasks(&mut self, _count: usize) {}
}

impl<T> IndexOutcome for Result<T, WriteError> {
    fn fail_with_index_error(self, detail: String) -> Self {
        match self {
            // The cell is already durable, so this reports a partial success,
            // not a rejected write -- see WriteError::IndexIncomplete. An
            // error already present is kept: it is more specific than this.
            Ok(_) => Err(WriteError::IndexIncomplete(detail)),
            err => err,
        }
    }
}

struct DurableOutcome<T> {
    result: Result<T, WriteError>,
    receipts: Vec<crate::ram::durability::Receipt>,
    derived: bool,
}
impl<T> IndexOutcome for DurableOutcome<T> {
    fn fail_with_index_error(mut self, detail: String) -> Self {
        self.result = self.result.fail_with_index_error(detail);
        self
    }
    fn note_index_tasks(&mut self, count: usize) {
        self.derived = count != 0;
    }
}

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

/// What the receiving member's memory looks like after a migration batch has
/// been given the chance to reach disk.
///
/// Reported back to the migration driver rather than only logged, because the
/// recipient's hot tier is the thing a bulk transfer can blow up and the driver
/// is the only party that knows how much more it intends to send. Zero
/// everywhere means there is no tier configured, not that nothing was received.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BulkReceiveReport {
    pub evicted_segments: u64,
    /// From the shared striped counter -- cheap, and what production pacing
    /// decides on. It is corrected by reconciliation rather than by every
    /// eviction, so straight after an eviction pass it can still include
    /// segments that have just gone cold.
    pub hot_segments: u64,
    pub hot_bytes: u64,
    /// Counted by scanning every registered chunk for actually-hot segments.
    /// Ground truth, and the only one of the two safe to draw a conclusion from;
    /// the gap between them is counter drift.
    pub hot_segments_scanned: u64,
    pub hot_bytes_scanned: u64,
}

pub fn generate_scoped_service_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!("NEB_CELL_RPC_SERVICE-{}-{}", group, database_name))
}

service! {
    rpc read_cell(key: Id) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells(keys: &Vec<Id>) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_all_cells_selected(keys: &Vec<Id>, colums: &Vec<u64>, need_header: bool) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_cell_select(id: Id, fields: &Vec<u64>, need_header: bool) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells_proced(keys: &Vec<Id>, colums: &Vec<u64>, filter: &Expr, proc: &Expr) -> Vec<Result<OwnedCell, ReadError>>;
    rpc existing_ids(keys: &Vec<Id>) -> Vec<Result<bool, ReadError>>;
    rpc read_all_cells_verified(keys: &Vec<Id>, colums: &Vec<u64>, filter: &Expr, proc: &Expr, checks: &Vec<crate::index::verification::IndexCheck>) -> Vec<Result<OwnedCell, ReadError>>;
    // Plain writes: `write_cell`, `update_cell`, `upsert_cell`,
    // `upsert_all_cells`, `remove_cell` and their durable and captured forms.
    // Contract: a cell takes plain (non-transactional) writes or transactions,
    // not both. If a caller mixes them on one cell, Neb does not guarantee that
    // an acknowledged plain write or delete survives the resolution of an
    // in-doubt transaction: after a restart with a transaction T in doubt, the
    // plain write and T's write can get the same version, and only one is kept;
    // an acknowledged delete is undone when the resolution installs T's cell.
    rpc write_cell(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc head_cell(key: Id) -> Result<CellHeader, ReadError>;
    rpc update_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_all_cells(cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc head_all_cells(keys: &Vec<Id>) -> Vec<Result<CellHeader, ReadError>>;
    rpc drop_migrated_cells(keys: &Vec<Id>) -> Vec<Result<(), WriteError>>;
    rpc receive_migrated_cells(cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>>;
    rpc push_cells_to(keys: &Vec<Id>, target: u64) -> Result<Vec<Id>, String>;
    // Refused with `SlotInDoubt` while a slot holds a cell of a bracket in
    // doubt after a restart: such a cell must not move (review R2).
    rpc cell_ids_in_slots(slots: &Vec<u32>) -> Result<Vec<Id>, crate::ram::txn_status::SlotInDoubt>;
    rpc slot_live_bytes(slots: &Vec<u32>) -> Vec<u64>;
    rpc total_live_bytes() -> u64;
    rpc can_admit_bytes(bytes: u64) -> bool;
    rpc settle_bulk_receive() -> BulkReceiveReport;
    rpc note_slot_owner(slot: u32, owner: u64, applied_index: u64) -> ();
    rpc note_slot_owners(owners: &Vec<(u32, u64)>, applied_index: u64) -> ();
    // Plain compare-and-set writes.
    // Contract: a cell takes plain (non-transactional) writes or transactions,
    // not both. If a caller mixes them on one cell, Neb does not guarantee that
    // an acknowledged plain write or delete survives the resolution of an
    // in-doubt transaction: after a restart with a transaction T in doubt, the
    // plain write and T's write can get the same version, and only one is kept;
    // an acknowledged delete is undone when the resolution installs T's cell.
    rpc compare_version_and_update_cell(key: Id, version: u64, cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc compare_version_and_set_field(key: Id, version: u64, field: u64, value: OwnedValue) -> Result<CellHeader, WriteError>;
    rpc count() -> u64;
    rpc post_schema_add(schema_id: u32) -> Result<(), String>;
    rpc post_schema_evolve(previous_id: u32, evolved_id: u32) -> Result<(), String>;
    rpc post_schema_delete(schema: u32) -> Result<(), String>;
    rpc scrub_index(repair: bool) -> Result<ScrubReport, String>;
    rpc cell_scan_open() -> u64;
    rpc cell_scan_next(cursor: u64, max: u32) -> Result<CellScanPage, String>;
    rpc cell_scan_close(cursor: u64) -> ();
    rpc live_cell_count() -> u64;
    rpc fence_engage(families: &Vec<u32>, drain_ms: u64, lease_ms: u64) -> Result<u64, String>;
    rpc fence_renew(token: u64, lease_ms: u64) -> bool;
    rpc fence_release(token: u64) -> ();

    // Plain writes, durable and captured forms.
    // Contract: a cell takes plain (non-transactional) writes or transactions,
    // not both. If a caller mixes them on one cell, Neb does not guarantee that
    // an acknowledged plain write or delete survives the resolution of an
    // in-doubt transaction: after a restart with a transaction T in doubt, the
    // plain write and T's write can get the same version, and only one is kept;
    // an acknowledged delete is undone when the resolution installs T's cell.
    rpc write_cell_durable(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc update_cell_durable(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell_durable(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc remove_cell_durable(key: Id) -> Result<(), WriteError>;
    rpc write_cell_captured(cell: OwnedCell) -> (Result<CellHeader, WriteError>, Vec<Id>);
    rpc remove_cell_captured(key: Id) -> (Result<(), WriteError>, Vec<Id>);
    rpc compare_version_and_update_cell_captured(key: Id, version: u64, cell: OwnedCell) -> (Result<CellHeader, WriteError>, Vec<Id>);
    rpc compare_version_and_set_field_captured(key: Id, version: u64, field: u64, value: OwnedValue) -> (Result<CellHeader, WriteError>, Vec<Id>);
    rpc finish_captured_receipts(tokens: Vec<Id>, commit: bool) -> Result<(), WriteError>;
    rpc flush_wal_durable() -> Result<(), WriteError>;
    rpc write_all_cells_durable(cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>>;
    rpc upsert_all_cells_durable(cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>>;
    rpc compare_version_and_update_cell_durable(key: Id, version: u64, cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc compare_version_and_set_field_durable(key: Id, version: u64, field: u64, value: OwnedValue) -> Result<CellHeader, WriteError>;
}

/// One page of a cell scan.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CellScanPage {
    /// Live cell ids of this server, ascending within the page. A page never
    /// spans two chunks, so it can be shorter than the page size without the
    /// scan being over.
    pub ids: Vec<Id>,
    /// The scan has visited every chunk. The cursor is closed by the server.
    pub done: bool,
}

/// Server-side state of one open cell scan: where it is in the chunk list and
/// the current chunk's ids. Holds one chunk's ids at most.
struct CellScan {
    next_chunk: usize,
    ids: Vec<u64>,
    position: usize,
    last_used: std::time::Instant,
}

/// A scan abandoned by a vanished client is dropped after this long without a
/// call, so a crashed dump cannot pin a chunk's worth of ids forever.
const CELL_SCAN_IDLE: std::time::Duration = std::time::Duration::from_secs(600);

/// A write fence held on behalf of a remote caller, kept alive by renewals.
///
/// The caller is another process, so the hold cannot be tied to a guard in its
/// stack: a dump that dies would leave the database read-only forever. The
/// lease makes the fence self-releasing -- it lapses unless the holder keeps
/// renewing it.
struct LeasedFence {
    // Held for its Drop, which lifts the fence.
    _hold: crate::server::write_fence::FenceHold,
    expires: std::time::Instant,
}

#[derive(Default)]
pub struct FenceLeases {
    next_token: std::sync::atomic::AtomicU64,
    holds: parking_lot::Mutex<std::collections::HashMap<u64, LeasedFence>>,
}

#[derive(Default)]
pub struct CellScans {
    next_id: std::sync::atomic::AtomicU64,
    open: parking_lot::Mutex<std::collections::HashMap<u64, CellScan>>,
}

service_with_id!(NebRPCService, DEFAULT_SERVICE_ID);

/// Bytes available to an unprivileged writer under `path`, or `None` if the
/// path cannot be interrogated. Walks up to the nearest existing ancestor: the
/// backup directory may not have been created yet on a fresh member.
fn available_bytes_for(path: &str) -> Option<u64> {
    let mut probe = std::path::Path::new(path);
    loop {
        if probe.exists() {
            break;
        }
        probe = probe.parent()?;
    }
    let c_path = std::ffi::CString::new(probe.to_string_lossy().as_bytes()).ok()?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

tokio::task_local! {
    static RPC_CAPTURE_HAS_INDICES: std::cell::Cell<bool>;
    static RPC_CAPTURE: std::cell::RefCell<Vec<crate::ram::durability::Receipt>>;
}

struct CapturedReceipts {
    incarnation: u64,
    next: u64,
    entries: std::collections::HashMap<
        Id,
        (
            crate::ram::durability::Receipt,
            std::time::Instant,
            bool,
            bool,
        ),
    >,
    last_cleanup: Option<std::time::Instant>,
}
impl Default for CapturedReceipts {
    fn default() -> Self {
        let incarnation = rand::random();
        #[cfg(test)]
        let incarnation = if std::env::var_os("NEB_TEST_REVERT_RECEIPT_INCARNATION").is_some() {
            0
        } else {
            incarnation
        };
        Self {
            incarnation,
            next: 0,
            entries: Default::default(),
            last_cleanup: None,
        }
    }
}
impl CapturedReceipts {
    // Abandoned RPC replies must not pin descriptors indefinitely. An expired
    // token is an error at certification, so expiry cannot produce success.
    fn store(
        &mut self,
        receipts: Vec<crate::ram::durability::Receipt>,
        derived: bool,
        acknowledge: bool,
    ) -> Vec<Id> {
        let now = std::time::Instant::now();
        if self
            .last_cleanup
            .is_none_or(|last| now.duration_since(last).as_secs() >= 600)
        {
            self.entries
                .retain(|_, (_, at, _, _)| now.duration_since(*at).as_secs() < 600);
            self.last_cleanup = Some(now);
        }
        receipts
            .into_iter()
            .enumerate()
            .map(|(index, receipt)| {
                self.next = self
                    .next
                    .checked_add(1)
                    .expect("receipt token space exhausted");
                let token = Id::from_parts(self.incarnation, self.next);
                self.entries
                    .insert(token, (receipt, now, derived, acknowledge && index == 0));
                token
            })
            .collect()
    }
    fn take(
        &mut self,
        tokens: Vec<Id>,
    ) -> Result<(Vec<crate::ram::durability::Receipt>, bool, u64), WriteError> {
        let mut receipts: Vec<crate::ram::durability::Receipt> = Vec::new();
        let mut missing = false;
        let mut derived = false;
        let mut acknowledged = 0;
        for token in tokens {
            let Some((receipt, at, has_indices, acknowledge)) = self.entries.remove(&token) else {
                missing = true;
                continue;
            };
            #[cfg(test)]
            let lease_reverted = std::env::var_os("NEB_TEST_REVERT_RECEIPT_LEASE").is_some();
            #[cfg(not(test))]
            let lease_reverted = false;
            if at.elapsed().as_secs() >= 600 && !lease_reverted {
                missing = true;
                continue;
            }
            derived |= has_indices;
            acknowledged += u64::from(acknowledge);
            if let Some(old) = receipts.iter_mut().find(|old| {
                std::sync::Arc::ptr_eq(&old.file, &receipt.file) && old.epoch == receipt.epoch
            }) {
                old.offset = old.offset.max(receipt.offset);
            } else {
                receipts.push(receipt);
            }
        }
        if missing {
            Err(WriteError::Durability(
                crate::ram::durability::DurabilityError::Rpc {
                    message: "captured WAL receipt is missing or expired".into(),
                },
            ))
        } else {
            Ok((receipts, derived, acknowledged))
        }
    }
}

pub struct NebRPCService {
    database_runtime: Arc<DatabaseRuntime>,
    neb_client: Arc<AsyncClient>,
    cell_scans: CellScans,
    captured_receipts: parking_lot::Mutex<CapturedReceipts>,
    fence_leases: Arc<FenceLeases>,
}

#[cfg(test)]
mod tests {
    use super::*;
    include!("cell_rpc/idcontract_tests.rs");

    #[test]
    fn index_failure_turns_a_successful_write_into_index_incomplete() {
        // The cell is written; only its index entry is missing. Reporting
        // success here is what let 688,045 dropped entries pass for a clean
        // import, so a success MUST NOT survive a failed index task.
        let ok: Result<u8, WriteError> = Ok(1);
        assert!(matches!(
            ok.fail_with_index_error("boom".into()),
            Err(WriteError::IndexIncomplete(_))
        ));
    }

    #[test]
    fn index_failure_keeps_a_more_specific_existing_error() {
        let err: Result<u8, WriteError> = Err(WriteError::CellAlreadyExisted);
        assert!(matches!(
            err.fail_with_index_error("boom".into()),
            Err(WriteError::CellAlreadyExisted)
        ));
    }

    #[test]
    fn scoped_cell_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
        );
        assert_eq!(
            generate_scoped_service_id(group, group),
            generate_scoped_service_id(group, group)
        );
    }
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<'_, Result<OwnedCell, ReadError>> {
        future::ready(
            self.database_runtime
                .chunks()
                .read_cell(&key)
                .map(|c| c.to_owned())
                .map_err(|e| self.redirect_read_if_not_owner(&key, e)),
        )
        .boxed()
    }
    fn read_all_cells(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| {
                    self.database_runtime
                        .chunks()
                        .read_cell(&id)
                        .map(|c| c.to_owned())
                        .map_err(|e| self.redirect_read_if_not_owner(id, e))
                })
                .collect(),
        )
        .boxed()
    }
    fn read_all_cells_selected(
        &self,
        keys: &Vec<Id>,
        colums: &Vec<u64>,
        need_header: bool,
    ) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| {
                    self.database_runtime
                        .chunks()
                        .read_selected(&id, colums.as_slice(), need_header)
                        .map(|c| c.to_owned())
                })
                .collect(),
        )
        .boxed()
    }
    fn read_cell_select(
        &self,
        id: Id,
        fields: &Vec<u64>,
        need_header: bool,
    ) -> BoxFuture<'_, Result<OwnedCell, ReadError>> {
        future::ready(
            self.database_runtime
                .chunks()
                .read_selected(&id, fields.as_slice(), need_header)
                .map(|c| c.to_owned())
                .map_err(|e| self.redirect_read_if_not_owner(&id, e)),
        )
        .boxed()
    }
    fn head_cell(&self, key: Id) -> BoxFuture<'_, Result<CellHeader, ReadError>> {
        future::ready(
            self.database_runtime
                .chunks()
                .head_cell(&key)
                .map_err(|e| self.redirect_read_if_not_owner(&key, e)),
        )
        .boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_indices_ensured(|| self.database_runtime.chunks().write_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| {
                        self.database_runtime.chunks().write_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn update_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_indices_ensured(|| self.database_runtime.chunks().update_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| {
                        self.database_runtime.chunks().update_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            self.remove_cell_unchecked(key).await
        }
        .boxed()
    }
    fn upsert_cell(&self, cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            self.upsert_cell_unchecked(cell).await
        }
        .boxed()
    }
    fn receive_migrated_cells(
        &self,
        cells: Vec<OwnedCell>,
    ) -> BoxFuture<'_, Vec<Result<CellHeader, WriteError>>> {
        // Migration transfers arrive here instead of through `upsert_all_cells`
        // because the recipient does **not** own the slot yet: the table entry
        // flips only after the data has landed, which is the whole point of
        // having a single commit point. So this path is exempt from the
        // ownership guard by construction.
        //
        // That exemption is exactly why it is a separate RPC rather than a flag.
        // A caller cannot reach it by accident, and anyone reading the service
        // can see at a glance which writes bypass the check -- which matters,
        // because the check is the only thing standing between a stale client
        // and a silently discarded write.
        //
        // Batch semantics mirror `upsert_all_cells`: in order, stopping at the
        // first failure. A migration aborts the whole slot on any failure
        // anyway, so there is nothing to gain from continuing.
        async move {
            // Land every cell on the SAME version it had on the donor.
            //
            // The write path assigns `old_version + 1` (`cell.rs`), so a
            // straight upsert would move a cell and quietly renumber it. That
            // is not cosmetic: callers derive *cell ids* from a container's
            // version -- Morpheus's id lists compute segment ids from
            // `(container, field, schema, root, root_version)` -- so a bumped
            // version repoints every derived id at a cell that does not exist.
            // The symptom appears nowhere near the cause: an edge append fails
            // with "root segment cell does not exist" on a vertex that
            // migrated perfectly, only under load, only after a migration.
            //
            // Pre-decrementing is the smallest change that survives the
            // existing write path; `migration_preserves_cell_versions` pins the
            // property so a future change to the increment rule fails loudly
            // here rather than silently downstream.
            let prepared: Vec<OwnedCell> = cells
                .into_iter()
                .map(|mut cell| {
                    cell.header.version = cell.header.version.saturating_sub(1);
                    cell
                })
                .collect();

            // Written as ONE run. Every cell of a batch shares a slot, a slot
            // is a locality, and a locality picks the chunk -- so they all
            // target one append head, where each used to take its own claim,
            // its own reference pair and its own WAL record. See
            // `Chunk::upsert_run`.
            // A/B switch, so the batched claim can be measured against the
            // per-cell one in a single binary rather than across builds.
            let results = if std::env::var("NEB_DISABLE_BATCHED_RECEIVE").as_deref() == Ok("1") {
                let mut per_cell = Vec::with_capacity(prepared.len());
                for cell in prepared {
                    per_cell.push(self.upsert_cell_unchecked(cell).await);
                }
                per_cell
            } else {
                self.database_runtime.chunks().upsert_run(prepared)
            };

            // A migration aborts the whole slot on any failure, so keep the
            // stop-at-first-failure reporting the driver expects.
            let mut aborted = false;
            results
                .into_iter()
                .map(|result| {
                    if aborted {
                        return Err(WriteError::BatchAborted);
                    }
                    if result.is_err() {
                        aborted = true;
                    }
                    result
                })
                .collect()
        }
        .boxed()
    }
    fn drop_migrated_cells(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<(), WriteError>>> {
        // The reclaim's removal path, and exempt for the mirror-image reason:
        // by the time a donor drops its copy the slot belongs to somebody else,
        // so the donor is deliberately deleting cells it no longer owns.
        //
        // Does NOT stop at the first failure, unlike the batch upsert above. A
        // reclaim needs to know exactly which keys are still present, and an
        // abort would hide that behind one error -- leaving it unable to tell
        // "not removed" from "not attempted".
        let keys = keys.clone();
        async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                // Index entries are deliberately LEFT BEHIND. They are keyed by
                // `[schema][field][feature][id]`, so there is one logical entry
                // per id for the whole cluster and the recipient's upsert has
                // already (re)created exactly it. Removing it here deletes the
                // entry that now describes the recipient's copy: measured, every
                // migrated cell vanished from the index, 6 found by a scan before
                // a migration and 0 after.
                results.push(
                    self.with_indices_ensured(|| {
                        self.database_runtime
                            .chunks()
                            .remove_cell_keeping_indices(&key)
                    })
                    .await,
                );
            }
            results
        }
        .boxed()
    }
    fn upsert_all_cells(
        &self,
        cells: Vec<OwnedCell>,
    ) -> BoxFuture<'_, Vec<Result<CellHeader, WriteError>>> {
        // One RPC round-trip for many pages: the chunk write and index
        // maintenance still happen per cell, but the per-call dispatch and
        // marshaling overhead is amortized across the batch. Reuses the
        // single-cell handler so schema-miss retry behavior is identical.
        //
        // Applied strictly IN ORDER, and the batch STOPS at the first
        // failure: the remaining cells are reported as BatchAborted without
        // being attempted. The only caller is the B-tree write-back flusher,
        // whose crash consistency rests on every durable prefix of its flush
        // stream being referentially closed -- pages are ordered so that a
        // referenced page always precedes the page that names it. Continuing
        // past a failed cell used to persist referrers whose referenced page
        // was never written (CannotAllocateSpace under chunk-full pressure),
        // and a kill in that window left an on-disk chain pointing at a page
        // recovery could not find: MissingPage, a tree that refuses to load,
        // and seeks retrying "tree placement was not found" forever.
        async move {
            let mut results = Vec::with_capacity(cells.len());
            let mut aborted = false;
            for cell in cells {
                if aborted {
                    results.push(Err(WriteError::BatchAborted));
                    continue;
                }
                let result = self.upsert_cell(cell).await;
                if result.is_err() {
                    aborted = true;
                }
                results.push(result);
            }
            results
        }
        .boxed()
    }
    fn head_all_cells(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<CellHeader, ReadError>>> {
        // Presence and version for many cells without moving a single body.
        // A migration reclaiming a donor copy has to ask "does the new owner
        // already have this?" for every id it is about to destroy, and reading
        // the cells back to answer that would double the cost of the move.
        future::ready(
            keys.into_iter()
                .map(|id| self.database_runtime.chunks().head_cell(id))
                .collect(),
        )
        .boxed()
    }

    fn cell_ids_in_slots(
        &self,
        slots: &Vec<u32>,
    ) -> BoxFuture<'_, Result<Vec<Id>, crate::ram::txn_status::SlotInDoubt>> {
        // Slots are `u32` on the wire because that is what the placement state
        // machine speaks, and `u16` at the storage layer because a Neb slot IS
        // an id's locality, which is 15 bits. A value too large to be a
        // locality can match no cell, so it is dropped rather than truncated
        // into some other slot's answer.
        let wanted: std::collections::HashSet<u16> = slots
            .iter()
            .filter(|slot| (**slot as usize) < crate::slots::SLOT_COUNT)
            .map(|slot| *slot as u16)
            .collect();
        future::ready(self.database_runtime.chunks().cell_ids_in_slots(&wanted)).boxed()
    }

    fn slot_live_bytes(&self, slots: &Vec<u32>) -> BoxFuture<'_, Vec<u64>> {
        // Positional, aligned with the request: a balancer asks about specific
        // slots and wants the answer keyed by its own question. A slot too
        // large to be a locality holds nothing and answers 0 -- same reasoning
        // as `cell_ids_in_slots`, but positionally rather than by omission.
        future::ready(self.database_runtime.chunks().slot_live_bytes(slots)).boxed()
    }

    fn total_live_bytes(&self) -> BoxFuture<'_, u64> {
        future::ready(self.database_runtime.chunks().total_live_bytes()).boxed()
    }

    fn can_admit_bytes(&self, bytes: u64) -> BoxFuture<'_, bool> {
        // The recipient's veto: somewhere to put the data, or "not now".
        //
        // Deliberately NOT gated on tier pressure, though the design sketch
        // suggested it. Being over the tier limit is the NORMAL state at
        // realistic limits -- measured 4x over at 256 MB and 2x at 1 GB -- and
        // a fresh member is already over it before holding a single cell,
        // because its per-chunk bootstrap segments alone exceed a small limit.
        // A veto on that signal declines every recipient, always: it turned an
        // automatic fill into a permanent RecipientDeclined the first time this
        // ran at scale. The tier is a spilling mechanism and receiving a
        // migration costs no more hot tier than writing the same volume
        // ordinarily (also measured), so tier pressure is a latency question,
        // not an admission one.
        //
        // Disk is different: spilling needs somewhere to spill TO, and a
        // member whose backup volume is nearly full cannot accept data at any
        // speed. That is a real "not now", and it is the one this answers.
        let admit = match self.database_runtime.chunks().list.first() {
            Some(chunk) => match chunk.backup_storage.as_deref() {
                Some(path) => match available_bytes_for(path) {
                    Some(available) => {
                        // Room for the batch plus headroom, since the batch is
                        // not the only thing that will need to spill.
                        let needed = bytes.saturating_mul(2).saturating_add(1 << 30);
                        if available < needed {
                            warn!(
                                "declining a migration of {} bytes: backup storage '{}' has \
                                 {} MB available, short of the {} MB this needs with headroom",
                                bytes,
                                path,
                                available / (1024 * 1024),
                                needed / (1024 * 1024)
                            );
                            false
                        } else {
                            true
                        }
                    }
                    // Cannot tell: admit. Refusing on a failed stat would make
                    // an unreadable mount point stop all rebalancing.
                    None => true,
                },
                None => true,
            },
            None => true,
        };
        future::ready(admit).boxed()
    }

    fn settle_bulk_receive(&self) -> BoxFuture<'_, BulkReceiveReport> {
        // Migration's memory contract on the receiving side.
        //
        // Received cells are ordinary writes: they append to the head segment,
        // which is archived the moment it seals, so the durable copy exists
        // without anything special. What is NOT automatic is when the resident
        // copy goes away -- that is the tier's job, and in production only the
        // background cleaner asks for it. A bulk transfer can push far more
        // into the hot tier between two cleaner passes than a normal write
        // workload would, so the driver asks here, once per batch, and the
        // received data becomes disk-resident at the pace of the migration
        // instead of at the cleaner's cadence.
        //
        // The pass respects the tier's own threshold, so this is a no-op when
        // there is no pressure -- it makes the existing bound act promptly, it
        // does not invent a second one.
        let chunks = self.database_runtime.chunks();
        let report = match chunks.tiered_manager.as_ref() {
            Some(manager) => {
                // Reconciled, and MEASURED to be worth it. This forces
                // `force_reconcile_all_chunks` -- a scan of every segment of every
                // chunk -- once per batch, which is 28% of a reshard's wall time
                // (2.3s without any settle, 3.0s with this one, for 1 GB). The
                // cheap `evict_for_allocation` was tried and is worse on BOTH
                // counts: 3.4s and a recipient peak of 1584 MB against this
                // version's 1336 MB. Acting on a stale counter makes the pass shed
                // less, the extra resident pressure costs more than the scan saves.
                let evicted = manager
                    .evict_for_allocation_reconciled()
                    .unwrap_or_else(|error| {
                        warn!("bulk-receive settle could not evict: {}", error);
                        0
                    });
                let hot_segments = manager.shared_hot_segments();
                // Gated, because it is O(every segment of every chunk) and the
                // driver calls this once per batch. Accurate is worth paying for
                // in a measurement and indefensible in a production transfer:
                // measured on .239 with 768 chunks it dominated the reshard,
                // turning ~20 minutes of transfer into a projected 2.5 hours.
                let scanned = if std::env::var("NEB_MEASURE_SCAN_HOT").is_ok() {
                    manager.scanned_hot_segments()
                } else {
                    0
                };
                BulkReceiveReport {
                    evicted_segments: evicted as u64,
                    hot_segments: hot_segments as u64,
                    hot_bytes: (hot_segments * crate::ram::segs::SEGMENT_SIZE) as u64,
                    hot_segments_scanned: scanned as u64,
                    hot_bytes_scanned: (scanned * crate::ram::segs::SEGMENT_SIZE) as u64,
                }
            }
            // No tier configured: everything is resident by design and there is
            // nothing to settle. Zeroes rather than an error, so the driver's
            // per-batch step stays unconditional.
            None => BulkReceiveReport::default(),
        };
        future::ready(report).boxed()
    }

    fn note_slot_owner(&self, slot: u32, owner: u64, applied_index: u64) -> BoxFuture<'_, ()> {
        // Pushed by the member that committed the migration, rather than pulled
        // by this one, and that direction is the whole point. A *query* for the
        // table can be served by a member that holds the committing log entry
        // and has not applied it, so a member asked to "go and re-read" can
        // install the state the commit replaced -- and then route writes to a
        // former owner while believing it is current. The committer already has
        // the authoritative answer; telling is safe where asking is not.
        //
        // Both rings, because a database client builds its own: updating one
        // would leave this server's read and write paths disagreeing.
        self.database_runtime.consh.note_slot_owner(
            slot as u64,
            owner,
            crate::slots::SLOT_COUNT,
            applied_index,
        );
        self.neb_client.note_slot_owner(slot, owner, applied_index);
        future::ready(()).boxed()
    }

    fn push_cells_to(&self, keys: &Vec<Id>, target: u64) -> BoxFuture<'_, Result<Vec<Id>, String>> {
        // Donor -> recipient directly, with the coordinator only orchestrating.
        //
        // The obvious shape -- coordinator reads from the donor, then writes to the
        // recipient -- makes every cell body cross the wire twice and pay two full
        // serialize/deserialize round trips, because the coordinator materializes
        // each `OwnedCell` only to hand it straight back. Measured in-process, the
        // byte path is what limits a transfer at realistic cell sizes: dropping the
        // payload from 4 KB to 256 B raised throughput 5.3x in cells/s, so per-byte
        // work dominates per-cell work for anything but tiny cells.
        //
        // Only the ids that landed come back, which is what the reclaim needs to
        // decide what it may destroy, and they are 8 bytes each instead of a cell.
        let keys = keys.clone();
        async move {
            let recipient = self
                .neb_client
                .client_by_server_id(target)
                .await
                .map_err(|error| format!("cannot reach recipient {target}: {error:?}"))?;

            let mut cells = Vec::with_capacity(keys.len());
            let t_read = std::time::Instant::now();
            for key in &keys {
                match self.database_runtime.chunks().read_cell(key) {
                    Ok(cell) => cells.push(cell.to_owned()),
                    // Enumerated a moment ago and gone now: nothing to move, and
                    // nothing wrong.
                    Err(crate::ram::cell::ReadError::CellDoesNotExisted) => {}
                    Err(error) => return Err(format!("donor could not read {key:?}: {error:?}")),
                }
            }
            crate::migration::MIGRATION_DONOR_READ_NANOS.fetch_add(
                t_read.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            crate::migration::MIGRATION_CELLS_READ
                .fetch_add(cells.len() as u64, std::sync::atomic::Ordering::Relaxed);
            if cells.is_empty() {
                return Ok(Vec::new());
            }
            let landed: Vec<Id> = cells.iter().map(|cell| cell.header.id).collect();
            let t_write = std::time::Instant::now();
            let written = recipient
                .receive_migrated_cells(cells)
                .await
                .map_err(|error| format!("recipient {target} unreachable mid-push: {error:?}"))?;
            crate::migration::MIGRATION_RECIPIENT_WRITE_NANOS.fetch_add(
                t_write.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            if let Some(error) = written
                .iter()
                .filter_map(|result| result.as_ref().err())
                .find(|error| !matches!(error, WriteError::BatchAborted))
            {
                return Err(format!(
                    "recipient {target} rejected a batch of {} cells: {error:?}",
                    landed.len()
                ));
            }
            Ok(landed)
        }
        .boxed()
    }

    fn note_slot_owners(&self, owners: &Vec<(u32, u64)>, applied_index: u64) -> BoxFuture<'_, ()> {
        // The batched form of `note_slot_owner`, for a bulk migration that has
        // just committed many slots at once. Same reasoning as the single version
        // -- pushed by the committer, because a query can be answered with the
        // state the commit replaced -- and one round trip instead of one per slot.
        for (slot, owner) in owners {
            self.database_runtime.consh.note_slot_owner(
                *slot as u64,
                *owner,
                crate::slots::SLOT_COUNT,
                applied_index,
            );
            self.neb_client
                .note_slot_owner(*slot, *owner, applied_index);
        }
        future::ready(()).boxed()
    }

    fn compare_version_and_update_cell(
        &self,
        key: Id,
        version: u64,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_indices_ensured(|| {
                    self.database_runtime
                        .chunks()
                        .compare_version_and_update_cell(&key, version, &mut cell)
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_update_cell(&key, version, &mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }
    fn compare_version_and_set_field(
        &self,
        key: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            let first_value = value.clone();
            let result = self
                .with_indices_ensured(|| {
                    self.database_runtime
                        .chunks()
                        .compare_version_and_set_field(&key, version, field, first_value)
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_set_field(&key, version, field, value)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }
    fn count(&self) -> BoxFuture<'_, u64> {
        future::ready(self.database_runtime.chunks().count() as u64).boxed()
    }

    fn read_all_cells_proced(
        &self,
        keys: &Vec<Id>,
        colums: &Vec<u64>,
        filter: &Expr,
        proc: &Expr,
    ) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        self.read_cells_checked(keys, colums, filter, proc, &[])
    }

    fn existing_ids(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<bool, ReadError>>> {
        // The word guard serializes this check with publication and deletion.
        // No segment address is dereferenced and no cold segment is promoted.
        let result = keys
            .iter()
            .map(|id| {
                if crate::index::verification::control("NEB_TEST_IDCONTRACT_SKIP_EXISTS") {
                    return Ok(true);
                }
                match self.database_runtime.chunks().location_for_read(id) {
                    Ok(_guard) => Ok(true),
                    Err(ReadError::CellDoesNotExisted) => Ok(false),
                    Err(error) => Err(error),
                }
            })
            .collect();
        future::ready(result).boxed()
    }

    fn read_all_cells_verified(
        &self,
        keys: &Vec<Id>,
        colums: &Vec<u64>,
        filter: &Expr,
        proc: &Expr,
        checks: &Vec<crate::index::verification::IndexCheck>,
    ) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        self.read_cells_checked(keys, colums, filter, proc, checks)
    }

    /// Re-derive this node's index entries and report (or fill) the holes, in
    /// both directions and for both secondary indexes. The maintenance command
    /// for a range that reads as errors, or as empty, while its cells are
    /// demonstrably still there.
    ///
    /// Server-side because the walk needs the chunks: no client API enumerates
    /// cells, and entries have to be derived from cell contents through the write
    /// path's own function.
    ///
    /// Refused when this node has no index builder. A store without one has no
    /// index to scrub, and answering "clean" for it would be a lie in exactly the
    /// situation the caller is checking for.
    fn scrub_index(&self, repair: bool) -> BoxFuture<'_, Result<ScrubReport, String>> {
        async move {
            let Some(indexer) = self.database_runtime.indexer.as_ref() else {
                return Err(String::from(
                    "no index builder on this node; nothing to scrub",
                ));
            };
            let mode = if repair {
                ScrubMode::Repair
            } else {
                ScrubMode::Verify
            };
            // Fully qualified: inside this impl the bare name finds the RPC
            // handler being defined, and an async fn cannot recurse through its
            // own future.
            let report = crate::index::scrub::scrub_all_indexes(
                self.database_runtime.chunks(),
                &indexer.clients,
                mode,
            )
            .await;
            if report.is_clean() {
                info!("Index scrub ({:?}) clean: {}", mode, report);
            } else {
                warn!("Index scrub ({:?}) found problems: {}", mode, report);
            }
            Ok(report)
        }
        .boxed()
    }

    fn cell_scan_open(&self) -> BoxFuture<'_, u64> {
        let scans = &self.cell_scans;
        let mut open = scans.open.lock();
        open.retain(|_, scan| scan.last_used.elapsed() < CELL_SCAN_IDLE);
        let id = scans
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        open.insert(
            id,
            CellScan {
                next_chunk: 0,
                ids: Vec::new(),
                position: 0,
                last_used: std::time::Instant::now(),
            },
        );
        future::ready(id).boxed()
    }

    fn cell_scan_next(&self, cursor: u64, max: u32) -> BoxFuture<'_, Result<CellScanPage, String>> {
        let max = (max as usize).max(1);
        let result = (|| {
            let chunks = self.database_runtime.chunks();
            let mut open = self.cell_scans.open.lock();
            let scan = open
                .get_mut(&cursor)
                .ok_or_else(|| format!("cell scan {cursor} is not open (expired or closed)"))?;
            scan.last_used = std::time::Instant::now();
            // Skip exhausted chunks until one has ids. The previous chunk's ids
            // are dropped before the next chunk's are built, so at most one
            // chunk is ever resident.
            while scan.position >= scan.ids.len() {
                if scan.next_chunk >= chunks.chunk_count() {
                    open.remove(&cursor);
                    return Ok(CellScanPage {
                        ids: Vec::new(),
                        done: true,
                    });
                }
                scan.ids = Vec::new();
                scan.ids = chunks.live_cell_ids_of_chunk(scan.next_chunk);
                scan.next_chunk += 1;
                scan.position = 0;
            }
            let end = (scan.position + max).min(scan.ids.len());
            let ids = scan.ids[scan.position..end]
                .iter()
                .map(|bits| Id::from_bits(*bits))
                .collect();
            scan.position = end;
            let exhausted =
                scan.position >= scan.ids.len() && scan.next_chunk >= chunks.chunk_count();
            if exhausted {
                open.remove(&cursor);
            }
            Ok(CellScanPage {
                ids,
                done: exhausted,
            })
        })();
        future::ready(result).boxed()
    }

    fn live_cell_count(&self) -> BoxFuture<'_, u64> {
        let chunks = self.database_runtime.chunks();
        let count: usize = (0..chunks.chunk_count())
            .map(|chunk| chunks.live_cell_ids_of_chunk(chunk).len())
            .sum();
        future::ready(count as u64).boxed()
    }

    fn fence_engage(
        &self,
        families: &Vec<u32>,
        drain_ms: u64,
        lease_ms: u64,
    ) -> BoxFuture<'_, Result<u64, String>> {
        let families: std::collections::HashSet<crate::ram::schema::SchemaUid> = families
            .iter()
            .map(|uid| crate::ram::schema::SchemaUid(*uid))
            .collect();
        async move {
            let hold = self
                .database_runtime
                .engage_write_fence(families, std::time::Duration::from_millis(drain_ms))
                .await
                .map_err(|e| e.to_string())?;
            let leases = &self.fence_leases;
            let token = leases
                .next_token
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            let lease = std::time::Duration::from_millis(lease_ms.max(1));
            leases.holds.lock().insert(
                token,
                LeasedFence {
                    _hold: hold,
                    expires: std::time::Instant::now() + lease,
                },
            );
            // The reaper: lifts the fence if the holder stops renewing.
            let leases = Arc::clone(&self.fence_leases);
            tokio::spawn(async move {
                loop {
                    let wait = {
                        let holds = leases.holds.lock();
                        match holds.get(&token) {
                            None => return,
                            Some(held) => held
                                .expires
                                .saturating_duration_since(std::time::Instant::now()),
                        }
                    };
                    if wait.is_zero() {
                        if leases.holds.lock().remove(&token).is_some() {
                            warn!(
                                "write fence lease {token} lapsed without a renewal; fence lifted"
                            );
                        }
                        return;
                    }
                    tokio::time::sleep(wait).await;
                }
            });
            Ok(token)
        }
        .boxed()
    }

    fn fence_renew(&self, token: u64, lease_ms: u64) -> BoxFuture<'_, bool> {
        let mut holds = self.fence_leases.holds.lock();
        let renewed = match holds.get_mut(&token) {
            Some(held) => {
                held.expires =
                    std::time::Instant::now() + std::time::Duration::from_millis(lease_ms.max(1));
                true
            }
            None => false,
        };
        future::ready(renewed).boxed()
    }

    fn fence_release(&self, token: u64) -> BoxFuture<'_, ()> {
        self.fence_leases.holds.lock().remove(&token);
        future::ready(()).boxed()
    }

    fn cell_scan_close(&self, cursor: u64) -> BoxFuture<'_, ()> {
        self.cell_scans.open.lock().remove(&cursor);
        future::ready(()).boxed()
    }

    fn post_schema_add<'a>(&'a self, schema_id: u32) -> BoxFuture<'a, Result<(), String>> {
        async move {
            let schema = self
                .neb_client
                .schema_by_id(schema_id)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(schema) = schema {
                self.database_runtime
                    .schemas()
                    .cache_schema_from_cluster(schema.clone());
                post_schema_add(&schema, &self.database_runtime).await
            } else {
                Err(format!(
                    "Schema not found for post_schema_add {}",
                    schema_id
                ))
            }
        }
        .boxed()
    }

    fn post_schema_evolve<'a>(
        &'a self,
        previous_id: u32,
        evolved_id: u32,
    ) -> BoxFuture<'a, Result<(), String>> {
        async move {
            let previous = self
                .neb_client
                .schema_by_id(previous_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("Superseded schema {} not found", previous_id))?;
            let evolved = self
                .neb_client
                .schema_by_id(evolved_id)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("Evolved schema {} not found", evolved_id))?;
            // Both generations, so the reconciler can tell an index namespace
            // that is merely carried over from one that is genuinely new.
            self.database_runtime
                .schemas()
                .apply_evolution(evolved.clone());
            post_schema_evolve(&previous, &evolved, &self.database_runtime).await
        }
        .boxed()
    }

    fn post_schema_delete<'a>(&'a self, schema_id: u32) -> BoxFuture<'a, Result<(), String>> {
        async move {
            let schema = self
                .neb_client
                .schema_by_id(schema_id)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(schema) = schema {
                post_schema_delete(&schema, &self.database_runtime).await
            } else {
                Err(format!(
                    "Schema not found for post_schema_delete {}",
                    schema_id
                ))
            }
        }
        .boxed()
    }
    fn write_cell_durable(
        &self,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_durable_indices(|| self.database_runtime.chunks().write_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| {
                        self.database_runtime.chunks().write_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn update_cell_durable(
        &self,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_durable_indices(|| self.database_runtime.chunks().update_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| {
                        self.database_runtime.chunks().update_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn upsert_cell_durable(
        &self,
        cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            self.upsert_cell_durable_unchecked(cell).await
        }
        .boxed()
    }

    fn remove_cell_durable(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            self.remove_cell_durable_unchecked(key).await
        }
        .boxed()
    }

    fn flush_wal_durable(&self) -> BoxFuture<'_, Result<(), WriteError>> {
        self.database_runtime.chunks().flush_wal_durable().boxed()
    }

    fn write_cell_captured(
        &self,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, (Result<CellHeader, WriteError>, Vec<Id>)> {
        self.with_captured_indices(async move {
            self.refuse_if_not_owner(&cell.header.id)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_captured_op(|| {
                    self.capture_for_rpc(|| self.database_runtime.chunks().write_cell(&mut cell))
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_captured_op(|| {
                        self.capture_for_rpc(|| {
                            self.database_runtime.chunks().write_cell(&mut cell)
                        })
                    })
                    .await
                }
                other => other,
            }
        })
        .boxed()
    }

    fn remove_cell_captured(&self, key: Id) -> BoxFuture<'_, (Result<(), WriteError>, Vec<Id>)> {
        self.with_captured_indices(async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            let result = self
                .with_captured_op(|| {
                    self.capture_for_rpc(|| self.database_runtime.chunks().remove_cell(&key))
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(schema)) => {
                    self.refresh_local_schema_cache_for_write(schema).await?;
                    self.with_captured_op(|| {
                        self.capture_for_rpc(|| self.database_runtime.chunks().remove_cell(&key))
                    })
                    .await
                }
                other => other,
            }
        })
        .boxed()
    }

    fn compare_version_and_update_cell_captured(
        &self,
        key: Id,
        version: u64,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, (Result<CellHeader, WriteError>, Vec<Id>)> {
        self.with_captured_indices(async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_captured_op(|| {
                    self.capture_for_rpc(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_update_cell(&key, version, &mut cell)
                    })
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_captured_op(|| {
                        self.capture_for_rpc(|| {
                            self.database_runtime
                                .chunks()
                                .compare_version_and_update_cell(&key, version, &mut cell)
                        })
                    })
                    .await
                }
                other => other,
            }
        })
        .boxed()
    }

    fn compare_version_and_set_field_captured(
        &self,
        key: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> BoxFuture<'_, (Result<CellHeader, WriteError>, Vec<Id>)> {
        self.with_captured_indices(async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            let first_value = value.clone();
            let result = self
                .with_captured_op(|| {
                    self.capture_for_rpc(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_set_field(&key, version, field, first_value)
                    })
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_captured_op(|| {
                        self.capture_for_rpc(|| {
                            self.database_runtime
                                .chunks()
                                .compare_version_and_set_field(&key, version, field, value)
                        })
                    })
                    .await
                }
                other => other,
            }
        })
        .boxed()
    }
    fn finish_captured_receipts(
        &self,
        tokens: Vec<Id>,
        commit: bool,
    ) -> BoxFuture<'_, Result<(), WriteError>> {
        async move {
            let (receipts, derived, acknowledged) = self.captured_receipts.lock().take(tokens)?;
            if !commit {
                return Ok(());
            }
            if derived {
                self.certify_derived_writes().await?;
                crate::ram::durability::wait_uncovered(receipts).await?;
            } else {
                crate::ram::durability::wait(receipts).await?;
            }
            crate::ram::durability::ACKNOWLEDGED_WRITES
                .fetch_add(acknowledged, atomic::Ordering::Relaxed);
            Ok(())
        }
        .boxed()
    }
    fn write_all_cells_durable(
        &self,
        cells: Vec<OwnedCell>,
    ) -> BoxFuture<'_, Vec<Result<CellHeader, WriteError>>> {
        crate::ram::durability::batch_with_derived(
            self.apply_durable_batch(cells, true, false),
            self.certify_derived_writes(),
        )
        .boxed()
    }

    fn upsert_all_cells_durable(
        &self,
        cells: Vec<OwnedCell>,
    ) -> BoxFuture<'_, Vec<Result<CellHeader, WriteError>>> {
        crate::ram::durability::batch_with_derived(
            self.apply_durable_batch(cells, false, true),
            self.certify_derived_writes(),
        )
        .boxed()
    }

    fn compare_version_and_update_cell_durable(
        &self,
        key: Id,
        version: u64,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_body(&cell)?;
            let result = self
                .with_durable_indices(|| {
                    self.database_runtime
                        .chunks()
                        .compare_version_and_update_cell(&key, version, &mut cell)
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_update_cell(&key, version, &mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn compare_version_and_set_field_durable(
        &self,
        key: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            self.refuse_if_not_owner(&key)?;
            let _fence = self.fence_for_cell_id(&key)?;
            let first_value = value.clone();
            let result = self
                .with_durable_indices(|| {
                    self.database_runtime
                        .chunks()
                        .compare_version_and_set_field(&key, version, field, first_value)
                })
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| {
                        self.database_runtime
                            .chunks()
                            .compare_version_and_set_field(&key, version, field, value)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }
}

dispatch_rpc_service_functions!(NebRPCService);
impl NebRPCService {
    fn read_cells_checked(
        &self,
        keys: &Vec<Id>,
        colums: &Vec<u64>,
        filter: &Expr,
        proc: &Expr,
        checks: &[crate::index::verification::IndexCheck],
    ) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        if !checks.is_empty() {
            crate::index::verification::record_verified_cell_reads(keys.len());
        }
        let filter_empty = filter.is_empty();
        let proc_empty = proc.is_empty();
        let mut interpreter = if !filter_empty || !proc_empty {
            Some(lisp::get_interpreter())
        } else {
            None
        };
        let filter_sexpr = if !filter_empty {
            Some(filter.clone().to_sexpr())
        } else {
            None
        };

        // Process cells one at a time: read → filter (SharedCell guard held) → to_owned
        // only for cells that pass. This ensures:
        //   1. Only one WordMutexGuard is live at a time — prevents the self-deadlock
        //      that occurs when the same cell ID appears twice in the batch.
        //   2. to_owned() (a full data clone) is skipped for cells rejected by the filter.
        let mut cells: Vec<Result<OwnedCell, ReadError>> = Vec::with_capacity(keys.len());
        let mut query_schemas = std::collections::HashMap::new();
        for id in keys.iter() {
            let cell_res = if colums.is_empty() || !checks.is_empty() {
                self.database_runtime.chunks.read_cell(id)
            } else {
                self.database_runtime
                    .chunks()
                    .read_selected(id, colums.as_slice(), true)
            };
            let owned = cell_res.and_then(|cell| {
                if !checks.is_empty() {
                    let schema = self
                        .database_runtime
                        .schemas()
                        .get(&cell.header.schema)
                        .ok_or_else(|| {
                            ReadError::ExecError("index candidate schema is absent".into())
                        })?;
                    let selected = query_schemas.entry(cell.header.schema).or_insert_with(|| {
                        crate::index::verification::query_schema(&schema, checks)
                    });
                    crate::index::verification::matches(&cell, selected, checks)?;
                }
                if !checks.is_empty() {
                    let mut owned = cell.to_owned();
                    if !colums.is_empty() {
                        let schema = self
                            .database_runtime
                            .schemas()
                            .get(&cell.header.schema)
                            .unwrap();
                        owned.data = OwnedValue::Array(
                            colums
                                .iter()
                                .map(|field| {
                                    let path = schema
                                        .id_index
                                        .get(field)
                                        .cloned()
                                        .unwrap_or_else(|| vec![*field]);
                                    owned.data.get_in_by_ids(&path).clone()
                                })
                                .collect(),
                        );
                    }
                    if let Some(filter) = &filter_sexpr {
                        let interp = interpreter.as_mut().unwrap();
                        let shared = owned.data.shared();
                        unsafe {
                            interp.unsafe_set_global_val(&shared);
                        }
                        let result = filter.clone().eval(interp.get_env());
                        interp.unset_global_val();
                        match result {
                            Ok(value) if is_true(&value) => {}
                            Ok(_) => return Err(ReadError::NotMatch),
                            Err(error) => return Err(ReadError::ExecError(error)),
                        }
                    }
                    return Ok(owned);
                }
                if let Some(filter) = &filter_sexpr {
                    // Filter is evaluated while the SharedCell guard is still held so the
                    // raw cell bytes remain valid. to_owned() is called only on a match;
                    // misses drop the SharedCell (and release the guard) without cloning.
                    let interp = interpreter.as_mut().unwrap();
                    unsafe {
                        interp.unsafe_set_global_val(&cell.data);
                    }
                    let check_res = filter.clone().eval(interp.get_env());
                    interp.unset_global_val();
                    match check_res {
                        Ok(sexp) if is_true(&sexp) => Ok(cell.to_owned()),
                        Ok(_) => Err(ReadError::NotMatch),
                        Err(e) => Err(ReadError::ExecError(e)),
                    }
                } else {
                    Ok(cell.to_owned())
                }
                // SharedCell (and its WordMutexGuard) is dropped here in all branches
            });
            cells.push(owned);
        }

        if !proc_empty {
            let proc_sexpr = proc.clone().to_sexpr();
            let interp = interpreter.as_mut().unwrap();
            let cells = cells
                .into_iter()
                .map(|cell_res| {
                    cell_res.and_then(|cell| {
                        let shared = cell.data.shared();
                        unsafe {
                            interp.unsafe_set_global_val(&shared);
                        }
                        let proc_res = proc_sexpr.clone().eval(interp.get_env());
                        interp.unset_global_val();
                        match proc_res {
                            Ok(sexp) => Ok(OwnedCell {
                                header: cell.header,
                                data: sexp.owned_val().unwrap_or(OwnedValue::NA),
                            }),
                            Err(e) => Err(ReadError::ExecError(e)),
                        }
                    })
                })
                .collect();
            return future::ready(cells).boxed();
        }
        return future::ready(cells).boxed();
    }

    pub fn new(
        database_runtime: Arc<DatabaseRuntime>,
        neb_client: Arc<AsyncClient>,
    ) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            database_runtime,
            neb_client,
            cell_scans: CellScans::default(),
            captured_receipts: parking_lot::Mutex::new(CapturedReceipts::default()),
            fence_leases: Arc::new(FenceLeases::default()),
        })
    }
    async fn refresh_local_schema_cache_for_write(&self, schema_id: u32) -> Result<(), WriteError> {
        match self.neb_client.schema_by_id(schema_id).await {
            Ok(Some(schema)) => {
                self.database_runtime
                    .schemas()
                    .cache_schema_from_cluster(schema);
                Ok(())
            }
            Ok(None) => Err(WriteError::SchemaDoesNotExisted(schema_id)),
            Err(error) => {
                warn!(
                    "Failed to refresh local schema cache for schema {} before write retry: {:?}",
                    schema_id, error
                );
                Err(WriteError::SchemaDoesNotExisted(schema_id))
            }
        }
    }
    fn upsert_cell_unchecked(
        &self,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            let result = self
                .with_indices_ensured(|| self.database_runtime.chunks().upsert_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| {
                        self.database_runtime.chunks().upsert_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }
    fn remove_cell_unchecked(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        async move {
            let result = self
                .with_indices_ensured(|| self.database_runtime.chunks().remove_cell(&key))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_indices_ensured(|| self.database_runtime.chunks().remove_cell(&key))
                        .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn upsert_cell_durable_unchecked(
        &self,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
            let result = self
                .with_durable_indices(|| self.database_runtime.chunks().upsert_cell(&mut cell))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| {
                        self.database_runtime.chunks().upsert_cell(&mut cell)
                    })
                    .await
                }
                other => other,
            }
        }
        .boxed()
    }

    fn remove_cell_durable_unchecked(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        async move {
            let result = self
                .with_durable_indices(|| self.database_runtime.chunks().remove_cell(&key))
                .await;
            match result {
                Err(WriteError::SchemaDoesNotExisted(missing_schema_id)) => {
                    self.refresh_local_schema_cache_for_write(missing_schema_id)
                        .await?;
                    self.with_durable_indices(|| self.database_runtime.chunks().remove_cell(&key))
                        .await
                }
                other => other,
            }
        }
        .boxed()
    }

    /// Admit a write of `cell` through the database's write fence.
    ///
    /// The cell names its own generation, which resolves to the family the
    /// fence protects. A write replaces whatever cell has its id, of whatever
    /// family, so while a fence is engaged the stored cell's family counts
    /// too: a body of an unprotected family must not overwrite a protected
    /// cell. Both lookups run only while a fence is engaged.
    fn fence_for_cell_body(
        &self,
        cell: &OwnedCell,
    ) -> Result<crate::server::write_fence::WriteGuard<'_>, WriteError> {
        let fence = self.database_runtime.write_fence();
        fence.enter_noted(cell.header.id.bits(), || {
            let schemas = self.database_runtime.schemas();
            let body = schemas.uid_of_vid(&cell.header.schema);
            match self.database_runtime.chunks().head_cell(&cell.header.id) {
                Ok(stored) => match schemas.uid_of_vid(&stored.schema) {
                    Some(stored) if fence.protects(stored) => Some(stored),
                    Some(_) => body,
                    // A stored cell whose family cannot be told: refused, as
                    // an undeterminable family always is.
                    None => None,
                },
                // Nothing stored: the body alone decides.
                Err(ReadError::CellDoesNotExisted) => body,
                Err(_) => None,
            }
        })
    }

    /// Admit a write that names only a cell id (a remove, a field set). The
    /// family is read off the stored cell's header, and only when a fence is
    /// engaged: the normal path never pays for the lookup.
    fn fence_for_cell_id(
        &self,
        id: &Id,
    ) -> Result<crate::server::write_fence::WriteGuard<'_>, WriteError> {
        use crate::server::write_fence::KeyedFamily;
        self.database_runtime
            .write_fence()
            .enter_keyed(id.bits(), || {
                match self.database_runtime.chunks().head_cell(id) {
                    Ok(header) => {
                        match self.database_runtime.schemas().uid_of_vid(&header.schema) {
                            Some(uid) => KeyedFamily::Family(uid),
                            None => KeyedFamily::Unknown,
                        }
                    }
                    Err(ReadError::CellDoesNotExisted) => KeyedFamily::Missing,
                    Err(_) => KeyedFamily::Unknown,
                }
            })
    }

    /// Refuse a write for a slot this member does not own.
    ///
    /// This is what makes a stale placement table a latency problem instead of a
    /// data-loss one. Reads are already safe: a migration defers dropping the
    /// donor's copy, so a member one migration behind still reads correct data.
    /// Writes are not, and they fail silently -- a client with an old table
    /// writes to a former owner, the write succeeds, and the data lands
    /// somewhere nothing will look again. Refusing turns that into a retry.
    ///
    /// Three conditions have to hold before refusing anything, and each one is
    /// load-bearing:
    ///
    /// 1. **A table must be installed.** With no table, placement is derived and
    ///    two members can legitimately disagree while the ring is still forming.
    ///    Refusing then would turn a placement question into an availability
    ///    failure during exactly the window that is hardest to get right -- the
    ///    window that already absorbed five reverted patches.
    /// 2. **The slot must have an owner in it.** A zero entry means unplaced, so
    ///    the answer came from the ring, so we are back in case 1 for that slot.
    /// 3. **The owner must be somebody else.** During a migration the donor is
    ///    still the serving owner and must keep accepting; only the *recipient*
    ///    would refuse, which is why migration transfers arrive through
    ///    `receive_migrated_cells` rather than as ordinary writes.
    /// Turn a read MISS into a redirect when this member does not own the slot.
    ///
    /// The mirror of `refuse_if_not_owner`, and deliberately not its twin: a
    /// write is refused up front, because writing to the wrong member loses
    /// data, while a read is only redirected when it would otherwise answer
    /// "does not exist". A member that still holds the cell keeps serving it,
    /// so the deferred-drop window this protects costs no extra hop.
    ///
    /// Same three preconditions as the write side (a table must be installed,
    /// the slot must have an owner, and the owner must be someone else); see
    /// `refuse_if_not_owner` for why each is load-bearing.
    fn redirect_read_if_not_owner(&self, id: &Id, error: ReadError) -> ReadError {
        if !matches!(error, ReadError::CellDoesNotExisted) {
            return error;
        }
        let conshash = &self.database_runtime.consh;
        if !conshash.has_slot_overrides() {
            return error;
        }
        let slot = crate::slots::slot_of(id) as u64;
        match conshash.slot_override_with_index(slot) {
            Some((owner, applied_index)) if owner != self.database_runtime.rpc.server_id => {
                ReadError::NotSlotOwner {
                    owner,
                    applied_index,
                }
            }
            _ => error,
        }
    }

    fn refuse_if_not_owner(&self, id: &Id) -> Result<(), WriteError> {
        let conshash = &self.database_runtime.consh;
        if !conshash.has_slot_overrides() {
            return Ok(());
        }
        let slot = crate::slots::slot_of(id) as u64;
        match conshash.slot_override_with_index(slot) {
            Some((owner, applied_index)) if owner != self.database_runtime.rpc.server_id => {
                Err(WriteError::NotSlotOwner {
                    owner,
                    applied_index,
                })
            }
            _ => Ok(()),
        }
    }

    async fn certify_derived_writes(&self) -> Result<(), WriteError> {
        self.neb_client.flush_wal_durable().await.map_err(|error| {
            WriteError::Durability(crate::ram::durability::DurabilityError::Rpc {
                message: format!("{error:?}"),
            })
        })?
    }

    fn capture_for_rpc<T>(&self, op: impl FnOnce() -> T) -> T {
        let (result, receipts) = crate::ram::durability::capture(op);
        RPC_CAPTURE.with(|capture| capture.borrow_mut().extend(receipts));
        result
    }

    async fn with_captured_op<T, F>(&self, op: F) -> Result<T, WriteError>
    where
        T: Send,
        F: FnOnce() -> Result<T, WriteError> + Send,
    {
        let outcome = self
            .with_indices_ensured(|| DurableOutcome {
                result: op(),
                receipts: Vec::new(),
                derived: false,
            })
            .await;
        if outcome.derived {
            RPC_CAPTURE_HAS_INDICES.with(|flag| flag.set(true));
        }
        outcome.result
    }

    async fn with_captured_indices<T>(
        &self,
        op: impl std::future::Future<Output = Result<T, WriteError>>,
    ) -> (Result<T, WriteError>, Vec<Id>) {
        RPC_CAPTURE_HAS_INDICES
            .scope(
                std::cell::Cell::new(false),
                RPC_CAPTURE.scope(std::cell::RefCell::new(Vec::new()), async {
                    let result = op.await;
                    let receipts = RPC_CAPTURE.with(|capture| capture.take());
                    #[cfg(test)]
                    if std::env::var_os("NEB_TEST_REVERT_DEFERRED_WAIT").is_some() {
                        crate::ram::durability::wait(receipts.clone())
                            .await
                            .unwrap();
                    }
                    let derived = RPC_CAPTURE_HAS_INDICES.with(|flag| flag.get());
                    let tokens =
                        self.captured_receipts
                            .lock()
                            .store(receipts, derived, result.is_ok());
                    (result, tokens)
                }),
            )
            .await
    }

    fn apply_batch_cell(
        &self,
        cell: &mut OwnedCell,
        insert: bool,
    ) -> Result<CellHeader, WriteError> {
        self.refuse_if_not_owner(&cell.header.id)?;
        let _fence = self.fence_for_cell_body(cell)?;
        if insert {
            self.database_runtime.chunks().write_cell(cell)
        } else {
            self.database_runtime.chunks().upsert_cell(cell)
        }
    }

    async fn apply_indexed_batch_cell(
        &self,
        cell: &mut OwnedCell,
        insert: bool,
    ) -> Result<CellHeader, WriteError> {
        self.refuse_if_not_owner(&cell.header.id)?;
        let _fence = self.fence_for_cell_body(cell)?;
        let (result, tasks) = IndexBuilder::with_request_index_scope(|| {
            crate::ram::durability::capture(|| {
                if insert {
                    self.database_runtime.chunks().write_cell(cell)
                } else {
                    self.database_runtime.chunks().upsert_cell(cell)
                }
            })
            .0
        })
        .await;
        if !tasks.is_empty() {
            crate::ram::durability::note_derived_writes();
        }
        let mut failed = 0;
        let mut first = None;
        for task in tasks {
            let error = match task {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => format!("{error:?}"),
                Err(error) => format!("index task join: {error}"),
            };
            failed += 1;
            first.get_or_insert(error);
        }
        if let Some(detail) = first {
            INDEX_TASK_FAILURES.fetch_add(failed, atomic::Ordering::Relaxed);
            result.fail_with_index_error(format!("{failed} index task(s) failed, first: {detail}"))
        } else {
            result
        }
    }

    /// Keep the common unindexed loop synchronous under one capture scope.
    /// Only a missing schema or actual index work needs to suspend application.
    async fn apply_durable_batch(
        &self,
        cells: Vec<OwnedCell>,
        insert: bool,
        stop: bool,
    ) -> Vec<Result<CellHeader, WriteError>> {
        #[cfg(test)]
        if std::env::var_os("NEB_TEST_REVERT_SPECIALIZED_BATCH").is_some() {
            let mut results = Vec::with_capacity(cells.len());
            let mut aborted = false;
            for cell in cells {
                let result = if aborted {
                    Err(WriteError::BatchAborted)
                } else if insert {
                    self.write_cell_durable(cell).await
                } else {
                    self.upsert_cell_durable(cell).await
                };
                if stop && result.is_err() {
                    aborted = true;
                }
                results.push(result);
            }
            return results;
        }
        #[cfg(test)]
        SPECIALIZED_BATCH_CALLS.fetch_add(1, atomic::Ordering::Relaxed);
        let indexed = self.database_runtime.indexer().is_some();
        let mut pending = cells.into_iter();
        let mut results = Vec::with_capacity(pending.len());
        loop {
            let retry = if indexed {
                match pending.next() {
                    None => break,
                    Some(mut cell) => {
                        let result = self.apply_indexed_batch_cell(&mut cell, insert).await;
                        Some((cell, result))
                    }
                }
            } else {
                crate::ram::durability::capture(|| {
                    for mut cell in pending.by_ref() {
                        let result = self.apply_batch_cell(&mut cell, insert);
                        if matches!(result, Err(WriteError::SchemaDoesNotExisted(_))) {
                            return Some((cell, result));
                        }
                        let failed = result.is_err();
                        results.push(result);
                        if stop && failed {
                            return None;
                        }
                    }
                    None
                })
                .0
            };
            if let Some((mut cell, mut result)) = retry {
                if let Err(WriteError::SchemaDoesNotExisted(schema)) = result {
                    result = match self.refresh_local_schema_cache_for_write(schema).await {
                        Err(error) => Err(error),
                        Ok(()) if indexed => self.apply_indexed_batch_cell(&mut cell, insert).await,
                        Ok(()) => {
                            crate::ram::durability::capture(|| {
                                self.apply_batch_cell(&mut cell, insert)
                            })
                            .0
                        }
                    };
                }
                let failed = result.is_err();
                results.push(result);
                if stop && failed {
                    break;
                }
            } else {
                break;
            }
        }
        results.extend(pending.map(|_| Err(WriteError::BatchAborted)));
        results
    }

    fn with_durable_indices<'a, T, F>(&'a self, op: F) -> BoxFuture<'a, Result<T, WriteError>>
    where
        T: Send + 'a,
        F: FnOnce() -> Result<T, WriteError> + Send + 'a,
    {
        async move {
            let mut outcome = self
                .with_indices_ensured(|| {
                    let (result, receipts) = crate::ram::durability::capture(op);
                    DurableOutcome {
                        result,
                        receipts,
                        derived: false,
                    }
                })
                .await;
            if crate::ram::durability::defer_to_batch(&mut outcome.receipts) {
                if outcome.derived {
                    crate::ram::durability::note_derived_writes();
                }
            } else {
                if outcome.derived {
                    self.certify_derived_writes().await?;
                    crate::ram::durability::wait_uncovered(outcome.receipts).await?;
                } else {
                    crate::ram::durability::wait(outcome.receipts).await?;
                }
                if outcome.result.is_ok() {
                    crate::ram::durability::ACKNOWLEDGED_WRITES
                        .fetch_add(1, atomic::Ordering::Relaxed);
                }
            }
            outcome.result
        }
        .boxed()
    }

    fn with_indices_ensured<'a, R, F>(&'a self, op: F) -> BoxFuture<'a, R>
    where
        R: Send + IndexOutcome + 'a,
        F: FnOnce() -> R + Send + 'a,
    {
        if self.database_runtime.indexer().is_some() {
            async move {
                // Wait only on the index work this request generated. Draining
                // the process-wide backlog here made every write wait for index
                // tasks belonging to unrelated concurrent requests, so offered
                // concurrency turned into a convoy instead of throughput.
                // Unscoped tasks are drained by the background reaper (and by
                // await_all_indices at shutdown).
                let (mut res, request_results) = IndexBuilder::with_request_index_scope(op).await;
                res.note_index_tasks(request_results.len());

                // The scope has already awaited these, so the outcome of this
                // write's index work is in hand right here. It used to be
                // logged and dropped, which meant a write whose index entry
                // failed still returned success and the store answered later
                // queries with that row missing. Report it instead: the first
                // failure names the cause, and the count says how much of the
                // request's index work was lost.
                let mut failed = 0usize;
                let mut first: Option<String> = None;
                for result in request_results.into_iter() {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            warn!("Index task failed during request: {:?}", e);
                            failed += 1;
                            first.get_or_insert_with(|| format!("{:?}", e));
                        }
                        Err(e) => {
                            warn!("Index task join failed during request: {:?}", e);
                            failed += 1;
                            first.get_or_insert_with(|| format!("index task join: {e}"));
                        }
                    }
                }

                match first {
                    Some(detail) => {
                        INDEX_TASK_FAILURES.fetch_add(failed as u64, atomic::Ordering::Relaxed);
                        res.fail_with_index_error(format!(
                            "{failed} index task(s) failed, first: {detail}"
                        ))
                    }
                    None => res,
                }
            }
            .boxed()
        } else {
            future::ready(op()).boxed()
        }
    }
}

#[cfg(test)]
#[path = "durable_write_tests.rs"]
mod durable_ack_tests;
