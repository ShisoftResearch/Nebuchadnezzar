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
use dovahkiin::expr::serde::Expr;
use dovahkiin::expr::symbols::utils::is_true;
use dovahkiin::integrated::lisp;
use dovahkiin::types::OwnedValue;
use futures::future::BoxFuture;
use futures::prelude::*;

use bifrost_plugins::hash_ident;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

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
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
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

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<'_, Result<OwnedCell, ReadError>> {
        future::ready(
            self.database_runtime
                .chunks
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
                        .chunks
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
                        .chunks
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
                .chunks
                .read_selected(&id, fields.as_slice(), need_header)
                .map(|c| c.to_owned()),
        )
        .boxed()
    }
    fn head_cell(&self, key: Id) -> BoxFuture<'_, Result<CellHeader, ReadError>> {
        future::ready(self.database_runtime.chunks.head_cell(&key)).boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.database_runtime.chunks.write_cell(&mut cell))
    }

    fn update_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.database_runtime.chunks.update_cell(&mut cell))
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        self.with_indices_ensured(self.database_runtime.chunks.remove_cell(&key))
    }
    fn upsert_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.database_runtime.chunks.upsert_cell(&mut cell))
    }
    fn compare_version_and_update_cell(
        &self,
        key: Id,
        version: u64,
        mut cell: OwnedCell,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(
            self.database_runtime
                .chunks
                .compare_version_and_update_cell(&key, version, &mut cell),
        )
    }
    fn compare_version_and_set_field(
        &self,
        key: Id,
        version: u64,
        field: u64,
        value: OwnedValue,
    ) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(
            self.database_runtime
                .chunks
                .compare_version_and_set_field(&key, version, field, value),
        )
    }
    fn count(&self) -> BoxFuture<'_, u64> {
        future::ready(self.database_runtime.chunks.count() as u64).boxed()
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
                    .chunks
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
    fn with_indices_ensured<'a, R>(&'a self, res: R) -> BoxFuture<'a, R>
    where
        R: Send + 'a,
    {
        if self.database_runtime.indexer.is_some() {
            IndexBuilder::await_indices().map(|_| res).boxed()
        } else {
            future::ready(res).boxed()
        }
    }
}
