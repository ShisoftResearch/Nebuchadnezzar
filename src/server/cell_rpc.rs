use crate::ram::types::Id;
use crate::server::NebServer;
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, WriteError},
};
use bifrost::rpc::*;
use dovahkiin::expr::serde::Expr;
use dovahkiin::expr::SExpr;
use dovahkiin::expr::symbols::utils::is_true;
use dovahkiin::integrated::lisp;
use dovahkiin::types::{OwnedValue, SharedMap, SharedValue};
use futures::future::BoxFuture;
use futures::prelude::*;

use bifrost_plugins::hash_ident;
use itertools::Itertools;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(NEB_CELL_RPC_SERVICE) as u64;

service! {
    rpc read_cell(key: Id) -> Result<OwnedCell, ReadError>;
    rpc read_all_cells(keys: Vec<Id>) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_all_cells_selected(keys: Vec<Id>, colums: Vec<u64>) -> Vec<Result<OwnedCell, ReadError>>;
    rpc read_all_cells_proced(keys: Vec<Id>, colums: Vec<u64>, filter: Expr, proc: Expr) -> Vec<Result<OwnedCell, ReadError>>;
    rpc write_cell(cell:OwnedCell) -> Result<CellHeader, WriteError>;
    rpc update_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc upsert_cell(cell: OwnedCell) -> Result<CellHeader, WriteError>;
    rpc remove_cell(key: Id) -> Result<(), WriteError>;
    rpc count() -> u64;
}

pub struct NebRPCService {
    server: Arc<NebServer>,
}

impl Service for NebRPCService {
    fn read_cell(&self, key: Id) -> BoxFuture<Result<OwnedCell, ReadError>> {
        future::ready(self.server.chunks.read_cell(&key).map(|c| c.to_owned())).boxed()
    }
    fn read_all_cells(&self, keys: Vec<Id>) -> BoxFuture<Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| self.server.chunks.read_cell(&id).map(|c| c.to_owned()))
                .collect(),
        )
        .boxed()
    }
    fn read_all_cells_selected(
        &self,
        keys: Vec<Id>,
        colums: Vec<u64>,
    ) -> BoxFuture<Vec<Result<OwnedCell, ReadError>>> {
        future::ready(
            keys.into_iter()
                .map(|id| {
                    self.server
                        .chunks
                        .read_selected(&id, colums.as_slice())
                        .map(|c| c.to_owned())
                })
                .collect(),
        )
        .boxed()
    }
    fn write_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.write_cell(&mut cell))
    }

    fn update_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.update_cell(&mut cell))
    }
    fn remove_cell(&self, key: Id) -> BoxFuture<Result<(), WriteError>> {
        self.with_indices_ensured(self.server.chunks.remove_cell(&key))
    }
    fn upsert_cell(&self, mut cell: OwnedCell) -> BoxFuture<Result<CellHeader, WriteError>> {
        self.with_indices_ensured(self.server.chunks.upsert_cell(&mut cell))
    }
    fn count(&self) -> BoxFuture<u64> {
        future::ready(self.server.chunks.count() as u64).boxed()
    }

    fn read_all_cells_proced(
        &self,
        keys: Vec<Id>,
        colums: Vec<u64>,
        filter: Expr,
        proc: Expr,
    ) -> BoxFuture<Vec<Result<OwnedCell, ReadError>>> {
        let filter_empty = filter.is_empty();
        let proc_empty = proc.is_empty();
        if filter_empty && proc_empty {
            // Fast path for naive query
            if colums.is_empty() {
                let res = keys
                    .iter()
                    .map(|id| self.server.chunks.read_cell(id).map(|c| c.to_owned()))
                    .collect_vec();
                return future::ready(res).boxed();
            } else {
                let res = keys
                    .iter()
                    .map(|id| {
                        self.server
                            .chunks
                            .read_selected(id, colums.as_slice())
                            .map(|c| c.to_owned())
                    })
                    .collect_vec();
                return future::ready(res).boxed();
            }
        };
        let mut rows = if colums.is_empty() {
            keys.into_iter()
                .map(|id| self.server.chunks.read_cell(&id))
                .map(|cell| {
                    cell.map(|cell| {
                        let mut interpreter = lisp::get_interpreter();
                        if let &SharedValue::Map(ref map) = &cell.data {
                            for (id, val) in &map.map {
                                interpreter.bind_by_id(*id, SExpr::shared_value(val.clone()));
                            }
                        }
                        interpreter.bind("data", SExpr::shared_value(cell.data.clone()));
                        (cell, interpreter)
                    })
                })
                .collect_vec()
        } else {
            keys.into_iter()
                .map(|id| self.server.chunks.read_selected(&id, colums.as_slice()))
                .map(|fields| {
                    fields.map(|fields| {
                        if let &SharedValue::Array(ref arr) = &fields.data {
                            let mut interpreter = lisp::get_interpreter();
                            debug_assert_eq!(arr.len(), colums.len());
                            for (i, col) in colums.iter().enumerate() {
                                interpreter.bind_by_id(*col, SExpr::shared_value(arr[i].clone()));
                            }
                            interpreter.bind("data", SExpr::shared_value(fields.data.clone()));
                            return (fields, interpreter);
                        } else {
                            unreachable!()
                        }
                    })
                })
                .collect_vec()
        };
        if !filter_empty {
            let filter = filter.to_sexpr();
            rows = rows.into_iter().map(|row| {
                if let Ok((cell, mut exec)) = row {
                    let check_res = filter.clone().eval(exec.get_env());
                    match check_res {
                        Ok(sexp) => {
                            if is_true(sexp) {
                                return Ok((cell, exec));
                            } else {
                                return Err(ReadError::NotMatch);
                            }
                        }
                        Err(e) => {
                            return Err(ReadError::ExecError(e))
                        }
                    }
                } else {
                    return row;
                }
            }).collect()
        }
        if !proc_empty {
            let proc = proc.to_sexpr();
            let res = rows.into_iter().map(|row| {
                match row {
                    Ok((cell, mut exec)) => {
                        let proc_res = proc.clone().eval(exec.get_env());
                        match proc_res {
                            Ok(sexp) => {
                                let val = sexp.owned_val().unwrap_or(OwnedValue::NA);
                                return Ok(OwnedCell {
                                    header: cell.header,
                                    data: val
                                });
                            }
                            Err(e) => {
                                return Err(ReadError::ExecError(e))
                            }
                        }
                    }
                    Err(e) => Err(e)
                }
            })
            .collect();
            return future::ready(res).boxed();
        } else {
            let res: Vec<_> = rows.into_iter().map(|row| {
                row.map(|(cell, _)| cell.to_owned())
            }).collect();
            return future::ready(res).boxed();
        }
    }
}

dispatch_rpc_service_functions!(NebRPCService);

impl NebRPCService {
    pub fn new(server: &Arc<NebServer>) -> Arc<NebRPCService> {
        Arc::new(NebRPCService {
            server: server.clone(),
        })
    }
    fn with_indices_ensured<'a, R>(&'a self, res: R) -> BoxFuture<R>
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
