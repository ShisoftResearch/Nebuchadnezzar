use std::{collections::HashSet, io, sync::Arc};

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::{
    expr::serde::Expr,
    types::{Id, OwnedValue},
};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::{client_by_server_name, AsyncClient},
    index::{
        full_text::BM25Hit,
        hash::get_hash_id_from_value,
        ranged::{
            client::cursor::ClientCursor,
            tree::{btree::Ordering, service::Range},
        },
        EntryKey, IndexerClients, SCHEMA_SCAN_PATT_SIZE,
    },
    query::planner::{
        build_indexed_predicate_plan, IndexedClausePlan, IndexedPredicatePlan, QueryPlanExplain,
    },
    ram::cell::{OwnedCell, ReadError},
    ram::schema::IndexType,
};

mod cursor;
mod ids;

use ids::{clause_execution_order, intersect_ids_ordered, sort_ids_by_ordering, union_ids_ordered};
pub use cursor::{DataCursor, IdCursor};

pub use crate::query::planner::{ValueRange, ValueRangeTerm};

const SCAN_BUFFER_SIZE: u16 = 64;

#[derive(Clone)]
pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
}

impl IndexedDataClient {
    pub fn new(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<RaftClient>,
    ) -> Self {
        Self {
            conshash: conshash.clone(),
            // Use 0 as server_id for query-only clients since inverted indexer won't be initialized
            index_clients: Arc::new(IndexerClients::new_query_only(
                neb_client,
                conshash,
                raft_client,
                0,
            )),
        }
    }

    /// Create IndexedDataClient with server's indexer clients (for BM25 search support)
    pub fn new_with_indexers(
        index_clients: Arc<IndexerClients>,
        conshash: Arc<ConsistentHashing>,
    ) -> Self {
        Self {
            conshash,
            index_clients,
        }
    }
    pub async fn range_index_scan<'a>(
        &'a self,
        schema: u32,
        field: u64,
        range: ValueRange,
        projection: Vec<u64>, // Column array
        selection: Expr,      // Checker expression
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor, RPCError> {
        let range = range.to_key_range(schema, field, ordering);
        let index_cursor = self
            .index_clients
            .range_seek(range, SCAN_BUFFER_SIZE, None)
            .await?;
        Ok(self
            .new_cursor(index_cursor, projection, selection, proc)
            .await)
    }
    pub async fn scan_all<'a>(
        &'a self,
        schema: u32,
        projection: Vec<u64>, // Column array
        selection: Expr,      // Checker expression
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor, RPCError> {
        let mut id_cursor = self.scan_by_expr_ids(schema, selection, ordering).await?;
        let mut ids = vec![];
        while let Some(id) = id_cursor.next().await? {
            ids.push(id);
        }
        let cells = self
            .read_cells_from_ids(&ids, &projection, &Expr::nothing(), &proc)
            .await;
        Ok(DataCursor {
            index_cursor: None,
            buffer: cells,
            projection: vec![],
            selection: Expr::nothing(),
            proc: Expr::nothing(),
            client: self.clone(),
            pos: 0,
        })
    }

    pub async fn scan_by_expr<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor, RPCError> {
        self.scan_by_expr_with_options(schema, selection, ordering, None, None)
            .await
    }

    pub async fn scan_by_expr_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: Ordering,
        order_by_field: Option<u64>,
        limit: Option<usize>,
    ) -> Result<DataCursor, RPCError> {
        let mut id_cursor = self
            .scan_by_expr_ids_with_options(schema, selection.clone(), ordering, order_by_field, limit)
            .await?;
        let mut ids = vec![];
        while let Some(id) = id_cursor.next().await? {
            ids.push(id);
        }
        let cells = self
            .read_cells_from_ids(&ids, &vec![], &Expr::nothing(), &Expr::nothing())
            .await;
        Ok(DataCursor {
            index_cursor: None,
            buffer: cells,
            projection: vec![],
            selection: Expr::nothing(),
            proc: Expr::nothing(),
            client: self.clone(),
            pos: 0,
        })
    }

    pub async fn scan_by_expr_plan(
        &self,
        schema: u32,
        selection: Expr,
        order_by_field: Option<u64>,
        limit: Option<usize>,
    ) -> Option<QueryPlanExplain> {
        self.indexed_predicate_plan(schema, &selection, order_by_field, limit)
            .await
            .map(IndexedPredicatePlan::into_explain)
    }

    pub async fn scan_by_expr_ids<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: Ordering,
    ) -> Result<IdCursor, RPCError> {
        self.scan_by_expr_ids_with_options(schema, selection, ordering, None, None)
            .await
    }

    pub async fn scan_by_expr_ids_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: Ordering,
        order_by_field: Option<u64>,
        limit: Option<usize>,
    ) -> Result<IdCursor, RPCError> {
        if let Some(field_id) = order_by_field {
            self.ensure_orderable_field(schema, field_id).await?;
        }

        let plan = self
            .indexed_predicate_plan(schema, &selection, order_by_field, limit)
            .await;
        let candidate_ids = if let Some(plan) = plan {
            if plan.is_impossible() {
                vec![]
            } else if plan.is_disjunction() {
                let mut candidate_ids = vec![];
                for candidate in plan.all() {
                    let ids = match self.execute_clause_ids(schema, candidate, ordering).await {
                        Ok(ids) => ids,
                        Err(_) => {
                            candidate_ids = self.scan_schema_ids(schema, ordering).await?;
                            break;
                        }
                    };
                    candidate_ids = union_ids_ordered(candidate_ids, &ids);
                }
                sort_ids_by_ordering(&mut candidate_ids, ordering);
                candidate_ids
            } else {
                let ordered_candidates = clause_execution_order(plan.all());
                let mut candidates = ordered_candidates.iter().copied();
                if let Some(first) = candidates.next() {
                    let mut candidate_ids =
                        match self.execute_clause_ids(schema, first, ordering).await {
                            Ok(ids) => ids,
                            Err(_) => self.scan_schema_ids(schema, ordering).await?,
                        };

                    for candidate in candidates {
                        let ids = match self.execute_clause_ids(schema, candidate, ordering).await {
                            Ok(ids) => ids,
                            Err(_) => {
                                candidate_ids = self.scan_schema_ids(schema, ordering).await?;
                                break;
                            }
                        };
                        candidate_ids = intersect_ids_ordered(candidate_ids, &ids);
                        if candidate_ids.is_empty() {
                            break;
                        }
                    }

                    if !ordered_candidates
                        .iter()
                        .any(|candidate| matches!(candidate, IndexedClausePlan::Ranged { .. }))
                    {
                        sort_ids_by_ordering(&mut candidate_ids, ordering);
                    }
                    candidate_ids
                } else {
                    self.scan_schema_ids(schema, ordering).await?
                }
            }
        } else {
            self.scan_schema_ids(schema, ordering).await?
        };

        let ordered_candidate_ids = if let Some(field_id) = order_by_field {
            self.reorder_ids_by_field(schema, field_id, &candidate_ids, ordering)
                .await?
        } else {
            candidate_ids
        };

        let mut selected_ids = self
            .filter_ids_by_selection_limit(&ordered_candidate_ids, &selection, limit)
            .await;
        if let Some(limit) = limit {
            selected_ids.truncate(limit);
        }

        Ok(IdCursor {
            buffer: selected_ids,
            pos: 0,
        })
    }

    async fn scan_schema_index<'a>(
        &'a self,
        schema: u32,
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor, RPCError> {
        let key = EntryKey::for_schema(schema);
        let index_cursor = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, ordering),
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?;
        Ok(self
            .new_cursor(index_cursor, projection, selection, proc)
            .await)
    }

    async fn scan_schema_ids(&self, schema: u32, ordering: Ordering) -> Result<Vec<Id>, RPCError> {
        let key = EntryKey::for_schema(schema);
        let Some(mut index_cursor) = self
            .index_clients
            .range_seek(
                Range::new_inclusive_opened(key, ordering),
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
            )
            .await?
        else {
            return Ok(vec![]);
        };
        let mut ids = vec![];
        loop {
            ids.extend_from_slice(index_cursor.current_block());
            if !index_cursor.next_block().await? {
                break;
            }
        }
        Ok(ids)
    }

    async fn filter_ids_by_selection_limit(
        &self,
        candidate_ids: &[Id],
        selection: &Expr,
        limit: Option<usize>,
    ) -> Vec<Id> {
        let empty_projection: Vec<u64> = vec![];
        let Some(limit) = limit else {
            let selected_cells = self
                .read_cells_from_ids(candidate_ids, &empty_projection, selection, &Expr::nothing())
                .await;
            return selected_cells.into_iter().map(|cell| cell.id()).collect_vec();
        };
        if limit == 0 || candidate_ids.is_empty() {
            return vec![];
        }

        let mut selected_ids = Vec::with_capacity(limit);
        let batch = usize::from(SCAN_BUFFER_SIZE.max(1));
        for chunk in candidate_ids.chunks(batch) {
            let selected_cells = self
                .read_cells_from_ids(chunk, &empty_projection, selection, &Expr::nothing())
                .await;
            selected_ids.extend(selected_cells.into_iter().map(|cell| cell.id()));
            if selected_ids.len() >= limit {
                selected_ids.truncate(limit);
                break;
            }
        }
        selected_ids
    }

    async fn new_cursor<'a>(
        &'a self,
        index_cursor: Option<ClientCursor>,
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
    ) -> DataCursor {
        let mut cursor = DataCursor {
            index_cursor,
            projection,
            selection,
            proc,
            client: self.clone(),
            buffer: vec![],
            pos: 0,
        };
        cursor.refresh_batch().await;
        cursor
    }

    async fn indexed_predicate_plan(
        &self,
        schema_id: u32,
        selection: &Expr,
        order_by_field: Option<u64>,
        limit: Option<usize>,
    ) -> Option<IndexedPredicatePlan> {
        if selection.is_empty() {
            return None;
        }
        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id)
            .await
            .ok()
            .flatten()?;
        let stats = self.index_clients.overall_schema_statistics(schema_id);
        build_indexed_predicate_plan(&schema, selection, stats.as_deref(), order_by_field, limit)
    }

    async fn ensure_orderable_field(&self, schema_id: u32, field_id: u64) -> Result<(), RPCError> {
        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id)
            .await
            .map_err(|e| RPCError::IOError(io::Error::new(io::ErrorKind::Other, e.to_string())))?
            .ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("schema {schema_id} not found"),
                ))
            })?;

        let orderable = schema
            .index_fields
            .get(&field_id)
            .map(|indices| indices.iter().any(|idx| matches!(idx, IndexType::Ranged)))
            .unwrap_or(false);
        if !orderable {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ORDER BY field {field_id} requires ranged index"),
            )));
        }
        Ok(())
    }

    async fn reorder_ids_by_field(
        &self,
        schema: u32,
        field_id: u64,
        ids: &[Id],
        ordering: Ordering,
    ) -> Result<Vec<Id>, RPCError> {
        let ordered_scan_ids = self
            .range_query_ids(
                schema,
                field_id,
                &ValueRange {
                    start: ValueRangeTerm::Open,
                    end: ValueRangeTerm::Open,
                },
                ordering,
            )
            .await?;
        let selected: HashSet<Id> = ids.iter().copied().collect();
        Ok(ordered_scan_ids
            .into_iter()
            .filter(|id| selected.contains(id))
            .collect())
    }

    async fn execute_clause_ids(
        &self,
        schema: u32,
        clause: &IndexedClausePlan,
        ordering: Ordering,
    ) -> Result<Vec<Id>, RPCError> {
        match clause {
            IndexedClausePlan::HashedEq { field_id, value } => {
                let ids = self.hashed_query(schema, *field_id, value).await?;
                Ok(ids.unwrap_or_default())
            }
            IndexedClausePlan::Ranged { field_id, range } => {
                self.range_query_ids(schema, *field_id, range, ordering)
                    .await
            }
        }
    }

    async fn range_query_ids(
        &self,
        schema: u32,
        field: u64,
        range: &ValueRange,
        ordering: Ordering,
    ) -> Result<Vec<Id>, RPCError> {
        let mut ids = vec![];
        let key_range = range.clone().to_key_range(schema, field, ordering);
        let Some(mut cursor) = self
            .index_clients
            .range_seek(key_range, SCAN_BUFFER_SIZE, None)
            .await?
        else {
            return Ok(ids);
        };
        loop {
            ids.extend_from_slice(cursor.current_block());
            if !cursor.next_block().await? {
                break;
            }
        }
        Ok(ids)
    }

    async fn read_cells_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
        selection: &Expr,
        proc: &Expr,
    ) -> Vec<OwnedCell> {
        let mut all_cells = vec![];
        let mut tasks = ids
            .iter()
            .enumerate()
            .filter_map(|(i, id)| self.conshash.get_server_id_by(id).map(|sid| (i, sid, *id)))
            .sorted_by_key(|(_i, sid, _id)| *sid)
            .chunk_by(|(_i, sid, _id)| *sid)
            .into_iter()
            .map(|(sid, pairs)| {
                let mut grouped_ids = vec![];
                let mut idx = vec![];
                for (i, _, id) in pairs {
                    idx.push(i);
                    grouped_ids.push(id);
                }
                let projection = projection.clone();
                let selection = selection.clone();
                let proc = proc.clone();
                let server_name = self.conshash.to_server_name(sid);
                async move {
                    match client_by_server_name(sid, server_name).await {
                        Ok(client) => {
                            let read_res = client
                                .read_all_cells_proced(&grouped_ids, &projection, &selection, &proc)
                                .await
                                .map(|cells| {
                                    cells
                                        .into_iter()
                                        .zip(idx)
                                        .filter_map(|(cell_res, original_idx)| {
                                            cell_res.ok().map(|cell| (cell, original_idx))
                                        })
                                        .collect_vec()
                                });
                            match read_res {
                                Ok(cells) => Ok(cells),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(task_res) = tasks.next().await {
            if let Ok(mut cells) = task_res {
                all_cells.append(&mut cells);
            }
        }

        all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
        all_cells.into_iter().map(|(cell, _)| cell).collect_vec()
    }

    pub fn hashed_index_id(schema: u32, field: u64, value: &OwnedValue) -> Id {
        get_hash_id_from_value(schema, field, value)
    }

    pub async fn hashed_query(
        &self,
        schema: u32,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        let index_id = Self::hashed_index_id(schema, field_id, value);
        self.index_clients
            .hashed_query(index_id, field_id, value)
            .await
    }

    pub async fn bm25_search(
        &self,
        schema: u32,
        field_id: u64,
        query: &str,
        limit: usize,
        phrase_boost: bool,
    ) -> Result<Result<Vec<BM25Hit>, ReadError>, RPCError> {
        self.index_clients
            .bm25_search(schema, field_id, query, limit, phrase_boost)
            .await
    }
}

#[cfg(test)]
mod tests;
