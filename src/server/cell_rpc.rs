use crate::ram::cell::Cell;
use crate::ram::schema::{post_schema_add, post_schema_delete};
use crate::ram::types::Id;
use crate::server::NebServer;
use crate::{
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
use itertools::Itertools;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells(keys: &Vec<Id>) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_all_cells_selected(keys: &Vec<Id>, colums: &Vec<u64>, need_header: bool) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_cell_select(id: Id, fields: &Vec<u64>, need_header: bool) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells_proced(keys: &Vec<Id>, colums: &Vec<u64>, filter: &Expr, proc: &Expr) -> Vec<Result<OwnedCell, ReadError>>;
    rpc write_cell(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc update_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc compare_version_and_update_cell(key: Id, version: u64, cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc count() -> u64;
    rpc post_schema_add(schema_id: u32) -> Result<(), String>;
    rpc post_schema_delete(schema: u32) -> Result<(), String>;
}

service_with_id!(NebRPCService, DEFAULT_SERVICE_ID);

pub struct NebRPCService {
    server: Arc<NebServer>,
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<'_, Result<OwnedCell, ReadError>> {
        future::ready(self.server.chunks.read_cell(&key).map(|c| c.to_owned())).boxed()
    }
    fn read_all_cells(&self, keys: &Vec<Id>) -> BoxFuture<'_, Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| self.server.chunks.read_cell(&id).map(|c| c.to_owned()))
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
                    self.server
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
            self.server
                .chunks
                .read_selected(&id, fields.as_slice(), need_header)
                .map(|c| c.to_owned()),
        )
        .boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.write_cell(&mut cell))
    }

    fn update_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.update_cell(&mut cell))
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<'_, Result<(), WriteError>> {
        self.with_indices_ensured(self.server.chunks.remove_cell(&key))
    }
    fn upsert_cell(&self, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.upsert_cell(&mut cell))
    }
    fn compare_version_and_update_cell(&self, key: Id, version: u64, mut cell: OwnedCell) -> BoxFuture<'_, Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.compare_version_and_update_cell(&key, version, &mut cell))
    }
    fn count(&self) -> BoxFuture<'_, u64> {
        future::ready(self.server.chunks.count() as u64).boxed()
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
        let mut cells = if colums.is_empty() {
            keys.iter()
                .map(|id| self.server.chunks.read_cell(id))
                .collect_vec()
        } else {
            keys.iter()
                .map(|id| {
                    self.server
                        .chunks
                        .read_selected(id, colums.as_slice(), true)
                })
                .collect_vec()
        };
        if (!filter_empty) | (!proc_empty) {
            let mut interpreter = lisp::get_interpreter();
            let filter = filter.clone().to_sexpr();
            if !filter_empty {
                cells = cells
                    .into_iter()
                    .map(|cell_res| {
                        cell_res.and_then(|cell| {
                            unsafe {
                                interpreter.unsafe_set_global_val(&cell.data);
                            }
                            let check_res = filter.clone().eval(interpreter.get_env());
                            interpreter.unset_global_val();
                            match check_res {
                                Ok(sexp) => {
                                    if is_true(&sexp) {
                                        Ok(cell)
                                    } else {
                                        Err(ReadError::NotMatch)
                                    }
                                }
                                Err(e) => return Err(ReadError::ExecError(e)),
                            }
                        })
                    })
                    .collect();
            }
            if !proc_empty {
                let proc = proc.clone().to_sexpr();
                let cells = cells
                    .into_iter()
                    .map(|cell_res| {
                        cell_res.and_then(|cell| {
                            unsafe {
                                interpreter.unsafe_set_global_val(&cell.data);
                            }
                            let proc_res = proc.clone().eval(interpreter.get_env());
                            interpreter.unset_global_val();
                            match proc_res {
                                Ok(sexp) => {
                                    let val = sexp.owned_val().unwrap_or(OwnedValue::NA);
                                    Ok(OwnedCell {
                                        header: cell.header,
                                        data: val,
                                    })
                                }
                                Err(e) => Err(ReadError::ExecError(e)),
                            }
                        })
                    })
                    .collect();
                return future::ready(cells).boxed();
            }
        }
        let res: Vec<_> = cells
            .into_iter()
            .map(|row| row.map(|cell| cell.to_owned()))
            .collect();
        return future::ready(res).boxed();
    }

    fn post_schema_add<'a>(&'a self, schema_id: u32) -> BoxFuture<'a, Result<(), String>> {
        async move {
            let schema = self
                .server
                .neb_client
                .schema_by_id(schema_id)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(schema) = schema {
                post_schema_add(&schema, &self.server).await
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
                .server
                .neb_client
                .schema_by_id(schema_id)
                .await
                .map_err(|e| e.to_string())?;
            if let Some(schema) = schema {
                post_schema_delete(&schema, &self.server).await
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
    pub fn new(server: &Arc<NebServer>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            server: server.clone(),
        })
    }
    fn with_indices_ensured<'a, R>(&'a self, res: R) -> BoxFuture<'a, R>
    where
        R: Send + 'a,
    {
        if self.server.indexer.is_some() {
            IndexBuilder::await_indices().map(|_| res).boxed()
        } else {
            future::ready(res).boxed()
        }
    }
}
