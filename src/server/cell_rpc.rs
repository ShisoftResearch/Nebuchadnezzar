use crate::ram::cell::Cell;
use crate::ram::schema::{post_schema_add, post_schema_delete};
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
use dovahkiin::types::OwnedValue;
use futures::future::BoxFuture;
use futures::prelude::*;
use serde::{Deserialize, Serialize};

use bifrost_plugins::hash_ident;

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
    pub hot_segments: u64,
    pub hot_bytes: u64,
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
    rpc write_cell(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc head_cell(key: Id) -> Result<CellHeader, ReadError>;
    rpc update_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_all_cells(cells: Vec<OwnedCell>) -> Vec<Result<CellHeader, WriteError>>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc head_all_cells(keys: &Vec<Id>) -> Vec<Result<CellHeader, ReadError>>;
    rpc remove_all_cells(keys: &Vec<Id>) -> Vec<Result<(), WriteError>>;
    rpc cell_ids_in_slots(slots: &Vec<u32>) -> Vec<Id>;
    rpc settle_bulk_receive() -> BulkReceiveReport;
    rpc note_slot_owner(slot: u32, owner: u64) -> ();
    rpc compare_version_and_update_cell(key: Id, version: u64, cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc compare_version_and_set_field(key: Id, version: u64, field: u64, value: OwnedValue) -> Result<CellHeader, WriteError>;
    rpc count() -> u64;
    rpc post_schema_add(schema_id: u32) -> Result<(), String>;
    rpc post_schema_delete(schema: u32) -> Result<(), String>;
}

service_with_id!(NebRPCService, DEFAULT_SERVICE_ID);

pub struct NebRPCService {
    database_runtime: Arc<DatabaseRuntime>,
    neb_client: Arc<AsyncClient>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
                .map(|c| c.to_owned()),
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
                .map(|c| c.to_owned()),
        )
        .boxed()
    }
    fn head_cell(&self, key: Id) -> BoxFuture<'_, Result<CellHeader, ReadError>> {
        future::ready(self.database_runtime.chunks().head_cell(&key)).boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
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
    fn upsert_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
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

    fn remove_all_cells(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<(), WriteError>>> {
        // Deliberately does NOT stop at the first failure, unlike
        // `upsert_all_cells`. That batch aborts because its caller's crash
        // consistency depends on every durable prefix being referentially
        // closed; removals carry no such ordering. The caller here is a
        // migration dropping a donor copy, and it needs to know exactly which
        // keys are still present -- an abort would hide that behind one error
        // and leave it unable to tell "not removed" from "not attempted".
        // Copied up front so the future borrows only `self`: removals are
        // sequential awaits, and a borrow of the caller's slice would have to
        // outlive the request.
        let keys = keys.clone();
        async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                results.push(self.remove_cell(key).await);
            }
            results
        }
        .boxed()
    }

    fn cell_ids_in_slots(&self, slots: &Vec<u32>) -> BoxFuture<'_, Vec<Id>> {
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
                // Reconciled rather than cached: a transfer that just wrote
                // several segments' worth is exactly the case where the shared
                // counter is most stale, and acting on a stale count is how a
                // settle silently does nothing.
                let evicted = manager
                    .evict_for_allocation_reconciled()
                    .unwrap_or_else(|error| {
                        warn!("bulk-receive settle could not evict: {}", error);
                        0
                    });
                let hot_segments = manager.shared_hot_segments();
                BulkReceiveReport {
                    evicted_segments: evicted as u64,
                    hot_segments: hot_segments as u64,
                    hot_bytes: (hot_segments * crate::ram::segs::SEGMENT_SIZE) as u64,
                }
            }
            // No tier configured: everything is resident by design and there is
            // nothing to settle. Zeroes rather than an error, so the driver's
            // per-batch step stays unconditional.
            None => BulkReceiveReport::default(),
        };
        future::ready(report).boxed()
    }

    fn note_slot_owner(&self, slot: u32, owner: u64) -> BoxFuture<'_, ()> {
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
        );
        self.neb_client.note_slot_owner(slot, owner);
        future::ready(()).boxed()
    }

    fn compare_version_and_update_cell(
        &self,
        key: Id,
        version: u64,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        async move {
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
        for id in keys.iter() {
            let cell_res = if colums.is_empty() {
                self.database_runtime.chunks.read_cell(id)
            } else {
                self.database_runtime
                    .chunks()
                    .read_selected(id, colums.as_slice(), true)
            };
            let owned = cell_res.and_then(|cell| {
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
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(
        database_runtime: Arc<DatabaseRuntime>,
        neb_client: Arc<AsyncClient>,
    ) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            database_runtime,
            neb_client,
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
    fn with_indices_ensured<'a, R, F>(&'a self, op: F) -> BoxFuture<'a, R>
    where
        R: Send + 'a,
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
                let (res, request_results) = IndexBuilder::with_request_index_scope(op).await;

                for result in request_results.into_iter() {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            warn!("Index task failed during request: {:?}", e);
                        }
                        Err(e) => {
                            warn!("Index task join failed during request: {:?}", e);
                        }
                    }
                }

                res
            }
            .boxed()
        } else {
            future::ready(op()).boxed()
        }
    }
}
