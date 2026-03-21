use std::{cmp::Ordering as CmpOrdering, collections::HashSet, io, sync::Arc};

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::{
    ahash::HashMap, expr::serde::Expr, types::{Id, OwnedValue, Type}
};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::{AsyncClient, SemanticHit, SimilarityHit, client_by_server_name},
    index::{
        EntryKey, IndexerClients, SCHEMA_SCAN_PATT_SIZE, embedding::EmbeddingHit, full_text::BM25Hit, vector::VectorHit, hash::{get_hash_id_from_value, get_null_hash_id}, ranged::{
            client::cursor::ClientCursor,
            tree::{btree::Ordering, service::Range},
        }
    },
    query::planner::{
        IndexedClausePlan, IndexedDisjunctPlan, IndexedPredicatePlan, QueryPlanExplain, build_indexed_predicate_plan
    },
    ram::{
        cell::{OwnedCell, ReadError},
    },
};

mod cursor;
mod ids;

pub use cursor::{AggregateResultCursor, AggregateRow, DataCursor, IdCursor};
use ids::{clause_execution_order, intersect_ids_ordered, sort_ids_by_query_order, union_ids_ordered};

pub use crate::query::planner::{ValueRange, ValueRangeTerm};

const SCAN_BUFFER_SIZE: u16 = 64;

#[derive(Clone)]
pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryHitType {
    VectorHit,
    EmbeddingHit,
    BM25Hit,
}

pub type QueryHitTable = Option<HashMap<Id, HashMap<(u64, QueryHitType), f32>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryOrdering {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    CountStar,
    CountField,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSpec {
    pub func: AggregateFunction,
    pub field_id: Option<u64>,
    pub alias: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateOrderTarget {
    GroupField(u64),
    AggregateAlias(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateOrderBy {
    pub target: AggregateOrderTarget,
    pub ordering: QueryOrdering,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateQuery {
    pub selection: Expr,
    pub group_by_fields: Vec<u64>,
    pub aggregates: Vec<AggregateSpec>,
    pub order_by: Option<AggregateOrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggregateValueType {
    Signed,
    Unsigned,
    Float,
    Scalar,
}

#[derive(Clone, Debug)]
struct ValidatedAggregateSpec {
    spec: AggregateSpec,
    value_type: Option<AggregateValueType>,
}

#[derive(Clone, Debug)]
enum AggregateState {
    CountStar {
        count: u64,
    },
    CountField {
        count: u64,
    },
    Sum {
        sum: NumericAccum,
        value_type: AggregateValueType,
        seen: bool,
    },
    Avg {
        sum: f64,
        count: u64,
    },
    Min {
        value: Option<OwnedValue>,
    },
    Max {
        value: Option<OwnedValue>,
    },
}

#[derive(Clone, Copy, Debug)]
enum NumericAccum {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

#[derive(Clone, Debug)]
struct AggregateGroupState {
    group_values: Vec<OwnedValue>,
    states: Vec<AggregateState>,
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
        ordering: QueryOrdering,
    ) -> Result<DataCursor, RPCError> {
        let mut id_cursor = self.query_ids(schema, selection, ordering).await?;
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

    pub async fn query<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
    ) -> Result<DataCursor, RPCError> {
        self.query_with_options(schema, selection, ordering, None, None, None, None)
            .await
    }

    pub async fn query_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<DataCursor, RPCError> {
        self.query_with_options_and_hits(
            schema,
            selection,
            ordering,
            order_by_field,
            distinct_fields,
            limit,
            offset,
            &mut None,
        )
        .await
    }

    pub async fn query_with_options_and_hits<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
        hit_table: &mut QueryHitTable,
    ) -> Result<DataCursor, RPCError> {
        let mut id_cursor = self
            .query_ids_with_options_and_hits(
                schema,
                selection.clone(),
                ordering,
                order_by_field,
                distinct_fields,
                limit,
                offset,
                hit_table,
            )
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

    pub async fn scan_by_expr<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
    ) -> Result<DataCursor, RPCError> {
        self.query(schema, selection, ordering).await
    }

    pub async fn scan_by_expr_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<DataCursor, RPCError> {
        self.query_with_options(
            schema,
            selection,
            ordering,
            order_by_field,
            distinct_fields,
            limit,
            offset,
        )
            .await
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

    pub async fn aggregate(
        &self,
        schema: u32,
        query: AggregateQuery,
    ) -> Result<AggregateResultCursor, RPCError> {
        if matches!(query.limit, Some(0)) {
            return Ok(AggregateResultCursor {
                buffer: vec![],
                pos: 0,
            });
        }

        let validated = self.validate_aggregate_query(schema, &query).await?;
        let mut id_cursor = self
            .query_ids_with_options(
                schema,
                query.selection.clone(),
                QueryOrdering::Asc,
                None,
                None,
                None,
                None,
            )
            .await?;
        let mut ids = vec![];
        while let Some(id) = id_cursor.next().await? {
            ids.push(id);
        }

        let required_fields = collect_aggregate_required_fields(&query.group_by_fields, &validated);
        let field_positions = required_fields
            .iter()
            .enumerate()
            .map(|(index, field_id)| (*field_id, index))
            .collect::<HashMap<_, _>>();
        let rows = self
            .read_projected_rows_from_ids(&ids, &required_fields)
            .await;

        let mut groups = HashMap::default();
        let mut states = Vec::<AggregateGroupState>::new();
        for row in rows {
            let group_values = query
                .group_by_fields
                .iter()
                .map(|field_id| {
                    field_positions
                        .get(field_id)
                        .and_then(|index| row.get(*index))
                        .cloned()
                        .unwrap_or(OwnedValue::Null)
                })
                .collect_vec();
            let group_key = serialize_group_key(&group_values);
            let group_index = if let Some(index) = groups.get(&group_key) {
                *index
            } else {
                let index = states.len();
                states.push(AggregateGroupState::new(&group_values, &validated));
                groups.insert(group_key, index);
                index
            };
            let group_state = states
                .get_mut(group_index)
                .expect("aggregate group index must exist");
            for (state, spec) in group_state.states.iter_mut().zip(validated.iter()) {
                let value = spec
                    .spec
                    .field_id
                    .and_then(|field_id| field_positions.get(&field_id).and_then(|index| row.get(*index)));
                state.accumulate(value);
            }
        }

        if states.is_empty() && query.group_by_fields.is_empty() {
            states.push(AggregateGroupState::new(&[], &validated));
        }

        let mut aggregate_rows = states
            .into_iter()
            .map(|state| state.finalize(&query.group_by_fields, &validated))
            .collect_vec();

        if let Some(order_by) = query.order_by.as_ref() {
            sort_aggregate_rows(&mut aggregate_rows, order_by);
        }

        let offset = query.offset.unwrap_or(0);
        let mut aggregate_rows = if offset >= aggregate_rows.len() {
            vec![]
        } else {
            aggregate_rows.split_off(offset)
        };
        if let Some(limit) = query.limit {
            aggregate_rows.truncate(limit);
        }

        Ok(AggregateResultCursor {
            buffer: aggregate_rows,
            pos: 0,
        })
    }

    pub async fn query_ids<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
    ) -> Result<IdCursor, RPCError> {
        self.query_ids_with_options(schema, selection, ordering, None, None, None, None)
            .await
    }

    pub async fn query_ids_and_hits<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
    ) -> Result<IdCursor, RPCError> {
        self.query_ids_with_options_and_hits(
            schema,
            selection,
            ordering,
            None,
            None,
            None,
            None,
            hit_table,
        )
            .await
    }

    pub async fn query_ids_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<IdCursor, RPCError> {
        self.query_ids_with_options_and_hits(
            schema,
            selection,
            ordering,
            order_by_field,
            distinct_fields,
            limit,
            offset,
            &mut None,
        )
        .await
    }

    pub async fn query_ids_with_options_and_hits<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
        hit_table: &mut QueryHitTable,
    ) -> Result<IdCursor, RPCError> {
        if matches!(limit, Some(0)) {
            return Ok(IdCursor {
                buffer: vec![],
                pos: 0,
            });
        }
        let effective_limit = match (limit, offset) {
            (Some(limit), Some(offset)) => Some(limit.saturating_add(offset)),
            (Some(limit), None) => Some(limit),
            (None, _) => None,
        };
        if let Some(field_id) = order_by_field {
            self.ensure_orderable_field(schema, field_id).await?;
        }
        if let Some(field_ids) = distinct_fields.as_ref() {
            self.ensure_distinct_fields(schema, field_ids).await?;
        }

        let explicit_order_by_field = order_by_field;
        let plan_limit = if explicit_order_by_field.is_some() || distinct_fields.is_some() {
            None
        } else {
            effective_limit
        };
        let plan = self
            .indexed_predicate_plan(schema, &selection, order_by_field, plan_limit)
            .await;
        let inferred_order_field = explicit_order_by_field
            .is_none()
            .then(|| plan.as_ref().and_then(Self::infer_query_order_field_from_plan))
            .flatten();
        let (candidate_ids, requires_selection_filter): (Vec<Id>, bool) = if let Some(plan) = plan {
            if plan.is_impossible() {
                (vec![], false)
            } else {
                (
                    self.execute_predicate_plan_ids(schema, &plan, ordering, hit_table)
                        .await?,
                    false,
                )
            }
        } else {
            (self.scan_schema_ids(schema, ordering).await?, true)
        };

        let ordered_candidate_ids: Vec<Id> = if let Some(field_id) = explicit_order_by_field {
            self.reorder_ids_by_field(schema, field_id, &candidate_ids, ordering)
                .await?
        } else {
            candidate_ids
        };
        let mut selected_ids = if requires_selection_filter {
            self.filter_ids_by_selection_limit(&ordered_candidate_ids, &selection, None)
                .await
        } else {
            ordered_candidate_ids
        };
        if let Some(field_id) = inferred_order_field {
            self.sort_ids_by_field_postprocessing(field_id, &mut selected_ids, ordering)
                .await;
        } else if explicit_order_by_field.is_none() {
            sort_ids_by_query_order(&mut selected_ids, ordering);
        }
        if let Some(field_ids) = distinct_fields.as_ref() {
            selected_ids = self.distinct_ids_by_fields(field_ids, selected_ids).await;
        }
        if let Some(limit) = effective_limit {
            selected_ids.truncate(limit);
        }
        let offset = offset.unwrap_or(0);
        let mut selected_ids = if offset >= selected_ids.len() {
            vec![]
        } else {
            selected_ids.split_off(offset)
        };
        if let Some(limit) = limit {
            selected_ids.truncate(limit);
        }

        Ok(IdCursor {
            buffer: selected_ids,
            pos: 0,
        })
    }

    pub async fn scan_by_expr_ids<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
    ) -> Result<IdCursor, RPCError> {
        self.query_ids(schema, selection, ordering).await
    }

    pub async fn scan_by_expr_ids_with_options<'a>(
        &'a self,
        schema: u32,
        selection: Expr,
        ordering: QueryOrdering,
        order_by_field: Option<u64>,
        distinct_fields: Option<Vec<u64>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<IdCursor, RPCError> {
        self.query_ids_with_options(
            schema,
            selection,
            ordering,
            order_by_field,
            distinct_fields,
            limit,
            offset,
        )
            .await
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

    async fn scan_schema_ids(
        &self,
        schema: u32,
        _ordering: QueryOrdering,
    ) -> Result<Vec<Id>, RPCError> {
        let key = EntryKey::for_schema(schema);
        let Some(mut index_cursor) = self
            .index_clients
            .range_seek(
                // Schema scans collect the full candidate set first. Final query ordering
                // is applied after filtering/pagination, so use a stable forward traversal
                // here instead of coupling correctness to backward cursor behavior.
                Range::new_inclusive_opened(key, Ordering::Forward),
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
                .read_cells_from_ids(
                    candidate_ids,
                    &empty_projection,
                    selection,
                    &Expr::nothing(),
                )
                .await;
            return selected_cells
                .into_iter()
                .map(|cell| cell.id())
                .collect_vec();
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

        if schema.field_by_id_path(&[field_id]).is_none() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ORDER BY field {field_id} does not exist in schema {schema_id}"),
            )));
        }
        Ok(())
    }

    async fn ensure_distinct_fields(
        &self,
        schema_id: u32,
        field_ids: &[u64],
    ) -> Result<(), RPCError> {
        if field_ids.is_empty() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DISTINCT requires at least one field",
            )));
        }
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
        for field_id in field_ids {
            if schema.field_by_id_path(&[*field_id]).is_none() {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("DISTINCT field {field_id} does not exist in schema {schema_id}"),
                )));
            }
        }
        Ok(())
    }

    async fn validate_aggregate_query(
        &self,
        schema_id: u32,
        query: &AggregateQuery,
    ) -> Result<Vec<ValidatedAggregateSpec>, RPCError> {
        if query.aggregates.is_empty() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aggregate query requires at least one aggregate",
            )));
        }

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

        for field_id in &query.group_by_fields {
            let field = schema.field_by_id_path(&[*field_id]).ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("GROUP BY field {field_id} does not exist in schema {schema_id}"),
                ))
            })?;
            if field.is_array || matches!(field.data_type, Type::Map | Type::Null | Type::NA) {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("GROUP BY field {field_id} must be a scalar field"),
                )));
            }
        }

        let mut validated = Vec::with_capacity(query.aggregates.len());
        let mut aliases = HashSet::new();
        for aggregate in &query.aggregates {
            if !aliases.insert(aggregate.alias.clone()) {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate aggregate alias {:?}", aggregate.alias),
                )));
            }
            let value_type = match aggregate.func {
                AggregateFunction::CountStar => {
                    if aggregate.field_id.is_some() {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "COUNT(*) must not specify a field",
                        )));
                    }
                    None
                }
                AggregateFunction::CountField => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "COUNT(field) requires a field",
                        ))
                    })?;
                    schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("aggregate field {field_id} does not exist in schema {schema_id}"),
                        ))
                    })?;
                    None
                }
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{:?} requires a field", aggregate.func),
                        ))
                    })?;
                    let field = schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("aggregate field {field_id} does not exist in schema {schema_id}"),
                        ))
                    })?;
                    Some(numeric_value_type(field).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} must be numeric for {:?}",
                                aggregate.func
                            ),
                        ))
                    })?)
                }
                AggregateFunction::Min | AggregateFunction::Max => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{:?} requires a field", aggregate.func),
                        ))
                    })?;
                    let field = schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("aggregate field {field_id} does not exist in schema {schema_id}"),
                        ))
                    })?;
                    if field.is_array || matches!(field.data_type, Type::Map | Type::Null | Type::NA)
                    {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("aggregate field {field_id} must be scalar for {:?}", aggregate.func),
                        )));
                    }
                    Some(AggregateValueType::Scalar)
                }
            };
            validated.push(ValidatedAggregateSpec {
                spec: aggregate.clone(),
                value_type,
            });
        }

        if let Some(order_by) = query.order_by.as_ref() {
            match &order_by.target {
                AggregateOrderTarget::GroupField(field_id) => {
                    if !query.group_by_fields.contains(field_id) {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("ORDER BY group field {field_id} is not present in GROUP BY"),
                        )));
                    }
                }
                AggregateOrderTarget::AggregateAlias(alias) => {
                    if !aliases.contains(alias) {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("ORDER BY aggregate alias {:?} does not exist", alias),
                        )));
                    }
                }
            }
        }

        Ok(validated)
    }

    async fn reorder_ids_by_field(
        &self,
        schema: u32,
        field_id: u64,
        ids: &[Id],
        ordering: QueryOrdering,
    ) -> Result<Vec<Id>, RPCError> {
        let _ = schema;
        let mut result = ids.to_vec();
        self.sort_ids_by_field(
            field_id,
            &mut result,
            ordering,
            match ordering {
                QueryOrdering::Asc => QueryOrdering::Asc,
                QueryOrdering::Desc => QueryOrdering::Desc,
            },
        )
        .await;
        Ok(result)
    }

    async fn sort_ids_by_field_postprocessing(
        &self,
        field_id: u64,
        ids: &mut [Id],
        ordering: QueryOrdering,
    ) {
        self.sort_ids_by_field(field_id, ids, ordering, QueryOrdering::Asc)
            .await;
    }

    async fn sort_ids_by_field(
        &self,
        field_id: u64,
        ids: &mut [Id],
        ordering: QueryOrdering,
        tie_break_ordering: QueryOrdering,
    ) {
        if ids.len() <= 1 {
            return;
        }

        let mut feature_by_id = HashMap::default();
        let id_list = ids.to_vec();
        match self.index_clients.neb_client.read_all_cells(&id_list).await {
            Ok(cells) => {
                for (id, cell_res) in id_list.into_iter().zip(cells) {
                    match cell_res {
                        Ok(cell) => {
                            let feature = if matches!(cell[field_id], OwnedValue::Null) {
                                None
                            } else {
                                Some(cell[field_id].feature())
                            };
                            feature_by_id.insert(id, feature);
                        }
                        Err(e) => {
                            warn!("Cell read error during sort for id {:?}: {:?}", id, e);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Bulk cell read error during sort: {:?}", e);
            }
        }

        ids.sort_unstable_by(|left, right| {
            let left_feature = feature_by_id.get(left).copied().flatten();
            let right_feature = feature_by_id.get(right).copied().flatten();
            compare_optional_features(left_feature, right_feature, ordering)
                .then_with(|| compare_ids_for_query_order(left, right, tie_break_ordering))
        });
    }

    async fn distinct_ids_by_fields(&self, field_ids: &[u64], ids: Vec<Id>) -> Vec<Id> {
        if ids.len() <= 1 {
            return ids;
        }

        let id_list = ids.clone();
        let cells = match self.index_clients.neb_client.read_all_cells(&id_list).await {
            Ok(cells) => cells,
            Err(e) => {
                warn!("Bulk cell read error during DISTINCT: {:?}", e);
                return ids;
            }
        };

        let mut seen = HashSet::new();
        let mut distinct_ids = Vec::with_capacity(ids.len());
        for (id, cell_res) in id_list.into_iter().zip(cells) {
            match cell_res {
                Ok(cell) => {
                    let key = field_ids
                        .iter()
                        .map(|field_id| cell[*field_id].clone())
                        .collect::<Vec<_>>();
                    if seen.insert(key) {
                        distinct_ids.push(id);
                    }
                }
                Err(e) => {
                    warn!("Cell read error during DISTINCT for id {:?}: {:?}", id, e);
                }
            }
        }
        distinct_ids
    }

    async fn execute_clause_ids(
        &self,
        schema: u32,
        clause: &IndexedClausePlan,
        _ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
    ) -> Result<Vec<Id>, RPCError> {
        match clause {
            IndexedClausePlan::HashedEq { field_id, value } => {
                match self.hashed_query(schema, *field_id, value).await? {
                    Ok(ids) => Ok(ids),
                    Err(_) => Ok(Vec::new()),
                }
            }
            IndexedClausePlan::NullPresence { field_id } => {
                let index_id = get_null_hash_id(schema, *field_id);
                match self
                    .index_clients
                    .hashed_client
                    .query(index_id, *field_id, &OwnedValue::Null)
                    .await?
                {
                    Ok(ids) => Ok(ids),
                    Err(_) => Ok(Vec::new()),
                }
            }
            IndexedClausePlan::Ranged { field_id, range } => {
                self.range_query_ids(schema, *field_id, range, range_index_order_for_range(range))
                    .await
            }
            IndexedClausePlan::VectorSimilarity {
                field_id,
                query,
                limit,
            } => {
                let hits = self.vector_query_hits(schema, *field_id, query.as_slice(), *limit).await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                        hit_table.entry(hit.id)
                            .or_insert_with(|| HashMap::default())
                            .entry((*field_id, QueryHitType::VectorHit))
                            .and_modify(|score| *score = score.max(hit.score))
                            .or_insert(hit.score);
                    }
                }
                Ok(hits.into_iter().map(|hit| hit.id).collect_vec())
            }
            IndexedClausePlan::EmbeddingSimilarity {
                field_id,
                query,
                limit,
            } => {
                let hits = self.embedding_query_hits(schema, *field_id, query.as_str(), *limit).await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                        hit_table.entry(hit.id)
                            .or_insert_with(|| HashMap::default())
                            .entry((*field_id, QueryHitType::EmbeddingHit))
                            .and_modify(|score| *score = score.max(hit.score))
                            .or_insert(hit.score);
                    }
                }
                Ok(hits.into_iter().map(|hit| hit.id).collect_vec())
            }
            IndexedClausePlan::FullTextMatch {
                field_id,
                query,
                limit,
                phrase_boost,
            } => {
                let hits = self.fulltext_query_hits(schema, *field_id, query.as_str(), *limit, *phrase_boost).await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                    hit_table.entry(hit.id)
                            .or_insert_with(|| HashMap::default())  
                            .entry((*field_id, QueryHitType::BM25Hit))
                            .and_modify(|score| *score = score.max(hit.score))
                            .or_insert(hit.score);
                    }
                }
                Ok(hits.into_iter().map(|hit| hit.id).collect_vec())
            }
        }
    }

    async fn execute_predicate_plan_ids(
        &self,
        schema: u32,
        plan: &IndexedPredicatePlan,
        ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
    ) -> Result<Vec<Id>, RPCError> {
        let mut all_ids = vec![];
        for disjunct in plan.disjuncts() {
            let ids = self
                .execute_disjunct_ids(schema, disjunct, ordering, hit_table)
                .await?;
            all_ids = union_ids_ordered(all_ids, &ids);
        }
        if plan.is_disjunction() {
            sort_ids_by_query_order(&mut all_ids, ordering);
        }
        Ok(all_ids)
    }

    async fn execute_disjunct_ids(
        &self,
        schema: u32,
        disjunct: &IndexedDisjunctPlan,
        ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
    ) -> Result<Vec<Id>, RPCError> {
        let mut candidate_ids = if disjunct.clauses().is_empty() {
            self.scan_schema_ids(schema, ordering).await?
        } else {
            let ordered_candidates = clause_execution_order(disjunct.clauses());
            let mut candidates = ordered_candidates.iter().copied();
            let Some(first) = candidates.next() else {
                return Ok(vec![]);
            };
            let mut candidate_ids = match self.execute_clause_ids(schema, first, ordering, hit_table).await {
                Ok(ids) => ids,
                Err(e) => {
                    if Self::is_special_clause(first) {
                        return Err(e);
                    }
                    self.scan_schema_ids(schema, ordering).await?
                }
            };

            for candidate in candidates {
                let ids = match self.execute_clause_ids(schema, candidate, ordering, hit_table).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        if Self::is_special_clause(candidate) {
                            return Err(e);
                        }
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
                sort_ids_by_query_order(&mut candidate_ids, ordering);
            }
            candidate_ids
        };

        if !disjunct.residual().is_empty() {
            candidate_ids = self
                .filter_ids_by_selection_limit(&candidate_ids, disjunct.residual(), None)
                .await;
        }
        Ok(candidate_ids)
    }

    fn is_special_clause(clause: &IndexedClausePlan) -> bool {
        match clause {
            IndexedClausePlan::VectorSimilarity { .. }
            | IndexedClausePlan::EmbeddingSimilarity { .. }
            | IndexedClausePlan::FullTextMatch { .. } => true,
            _ => false,
        }
    }

    async fn vector_query_hits(
        &self,
        schema: u32,
        field_id: u64,
        query_vector: &[f32],
        limit: usize,
    ) -> Result<Vec<VectorHit>, RPCError> {
        if !self
            .index_clients
            .vector_client
            .is_vector_search_coordinator_set()
            && !self.index_clients.vector_client.is_vector_index_core_set()
        {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                "Vector indexer core and distributed coordinator are not available",
            )));
        }
        let search_result = if self
            .index_clients
            .vector_client
            .is_vector_search_coordinator_set()
        {
            self.index_clients
                .vector_client
                .search_distributed(schema, field_id, query_vector, limit.max(1), None)
                .await
        } else {
            self.index_clients
                .vector_client
                .search(schema, field_id, query_vector, limit.max(1), None)
                .await
        };
        let hits = search_result.map_err(|e| {
            RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                format!("Vector search error: {:?}", e),
            ))
        })?;
        Ok(hits)
    }

    async fn embedding_query_hits(
        &self,
        schema: u32,
        field_id: u64,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EmbeddingHit>, RPCError> {
        if !self
            .index_clients
            .embedding_client
            .is_embedding_index_core_set()
        {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                "Embedding indexer core is not available",
            )));
        }
        let hits = self
            .index_clients
            .embedding_client
            .search(schema, field_id, query, limit.max(1))
            .await
            .map_err(|e| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Embedding search error: {:?}", e),
                ))
            })?;
        Ok(hits)
    }

    async fn fulltext_query_hits(
        &self,
        schema: u32,
        field_id: u64,
        query: &str,
        limit: usize,
        phrase_boost: bool,
    ) -> Result<Vec<BM25Hit>, RPCError> {
        let hits = self
            .bm25_search(schema, field_id, query, limit.max(1), phrase_boost)
            .await?
            .map_err(|e| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Full-text search error: {:?}", e),
                ))
            })?;
        Ok(hits)
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
                                        .filter_map(|(cell_res, original_idx)| match cell_res {
                                            Ok(cell) => Some((cell, original_idx)),
                                            Err(e) => {
                                                warn!(
                                                    "Cell read error at index {}: {:?}",
                                                    original_idx, e
                                                );
                                                None
                                            }
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
            match task_res {
                Ok(mut cells) => all_cells.append(&mut cells),
                Err(e) => {
                    warn!("Task error in read_cells_from_ids: {:?}", e);
                }
            }
        }

        all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
        all_cells.into_iter().map(|(cell, _)| cell).collect_vec()
    }

    async fn read_projected_rows_from_ids(
        &self,
        ids: &[Id],
        projection: &Vec<u64>,
    ) -> Vec<Vec<OwnedValue>> {
        let projected_cells = self
            .read_cells_from_ids(ids, projection, &Expr::nothing(), &Expr::nothing())
            .await;
        projected_cells
            .into_iter()
            .map(|cell| {
                projection
                    .iter()
                    .enumerate()
                    .map(|(index, _field_id)| cell[index].clone())
                    .collect_vec()
            })
            .collect_vec()
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

    fn infer_query_order_field_from_plan(plan: &IndexedPredicatePlan) -> Option<u64> {
        let mut field_id = None;
        for clause in plan.all() {
            let IndexedClausePlan::Ranged {
                field_id: ranged_field_id,
                ..
            } = clause
            else {
                continue;
            };
            match field_id {
                None => field_id = Some(*ranged_field_id),
                Some(existing) if existing == *ranged_field_id => {}
                Some(_) => return None,
            }
        }
        field_id
    }
}

fn query_order_to_scan_order(ordering: QueryOrdering) -> Ordering {
    match ordering {
        QueryOrdering::Asc => Ordering::Forward,
        QueryOrdering::Desc => Ordering::Backward,
    }
}

fn range_index_order_for_range(range: &ValueRange) -> Ordering {
    match (&range.start, &range.end) {
        (ValueRangeTerm::Inclusive(_) | ValueRangeTerm::Exclusive(_), _) => Ordering::Forward,
        (ValueRangeTerm::Open, ValueRangeTerm::Inclusive(_) | ValueRangeTerm::Exclusive(_)) => {
            Ordering::Backward
        }
        (ValueRangeTerm::Open, ValueRangeTerm::Open) => Ordering::Forward,
    }
}

fn compare_optional_features(
    left: Option<crate::index::Feature>,
    right: Option<crate::index::Feature>,
    ordering: QueryOrdering,
) -> CmpOrdering {
    match (left, right) {
        (Some(left), Some(right)) => match ordering {
            QueryOrdering::Asc => left.cmp(&right),
            QueryOrdering::Desc => right.cmp(&left),
        },
        (Some(_), None) => CmpOrdering::Less,
        (None, Some(_)) => CmpOrdering::Greater,
        (None, None) => CmpOrdering::Equal,
    }
}

fn compare_ids_for_query_order(left: &Id, right: &Id, ordering: QueryOrdering) -> CmpOrdering {
    match ordering {
        QueryOrdering::Asc => left.cmp(right),
        QueryOrdering::Desc => right.cmp(left),
    }
}

impl AggregateGroupState {
    fn new(group_values: &[OwnedValue], specs: &[ValidatedAggregateSpec]) -> Self {
        Self {
            group_values: group_values.to_vec(),
            states: specs.iter().map(AggregateState::from_spec).collect_vec(),
        }
    }

    fn finalize(
        self,
        group_by_fields: &[u64],
        specs: &[ValidatedAggregateSpec],
    ) -> AggregateRow {
        AggregateRow {
            group_values: group_by_fields
                .iter()
                .copied()
                .zip(self.group_values)
                .collect_vec(),
            aggregate_values: self
                .states
                .into_iter()
                .zip(specs.iter())
                .map(|(state, spec)| (spec.spec.alias.clone(), state.finalize()))
                .collect_vec(),
        }
    }
}

impl AggregateState {
    fn from_spec(spec: &ValidatedAggregateSpec) -> Self {
        match spec.spec.func {
            AggregateFunction::CountStar => Self::CountStar { count: 0 },
            AggregateFunction::CountField => Self::CountField { count: 0 },
            AggregateFunction::Sum => Self::Sum {
                sum: NumericAccum::zero(spec.value_type.expect("SUM value type validated")),
                value_type: spec.value_type.expect("SUM value type validated"),
                seen: false,
            },
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => Self::Min { value: None },
            AggregateFunction::Max => Self::Max { value: None },
        }
    }

    fn accumulate(&mut self, value: Option<&OwnedValue>) {
        match self {
            AggregateState::CountStar { count } => *count += 1,
            AggregateState::CountField { count } => {
                if value.is_some_and(|value| !is_null_like(value)) {
                    *count += 1;
                }
            }
            AggregateState::Sum {
                sum,
                value_type,
                seen,
            } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if sum.add(value_type, value) {
                        *seen = true;
                    }
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if let Some(v) = owned_value_to_f64(value) {
                        *sum += v;
                        *count += 1;
                    }
                }
            }
            AggregateState::Min { value: current } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if current
                        .as_ref()
                        .map(|existing| compare_owned_values(value, existing, QueryOrdering::Asc))
                        .unwrap_or(CmpOrdering::Less)
                        == CmpOrdering::Less
                    {
                        *current = Some(value.clone());
                    }
                }
            }
            AggregateState::Max { value: current } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if current
                        .as_ref()
                        .map(|existing| compare_owned_values(value, existing, QueryOrdering::Desc))
                        .unwrap_or(CmpOrdering::Less)
                        == CmpOrdering::Less
                    {
                        *current = Some(value.clone());
                    }
                }
            }
        }
    }

    fn finalize(self) -> OwnedValue {
        match self {
            AggregateState::CountStar { count } | AggregateState::CountField { count } => {
                OwnedValue::U64(count)
            }
            AggregateState::Sum { sum, seen, .. } => {
                if seen {
                    sum.into_owned_value()
                } else {
                    OwnedValue::Null
                }
            }
            AggregateState::Avg { sum, count } => {
                if count == 0 {
                    OwnedValue::Null
                } else {
                    OwnedValue::F64(sum / count as f64)
                }
            }
            AggregateState::Min { value } | AggregateState::Max { value } => {
                value.unwrap_or(OwnedValue::Null)
            }
        }
    }
}

impl NumericAccum {
    fn zero(value_type: AggregateValueType) -> Self {
        match value_type {
            AggregateValueType::Signed => Self::Signed(0),
            AggregateValueType::Unsigned => Self::Unsigned(0),
            AggregateValueType::Float => Self::Float(0.0),
            AggregateValueType::Scalar => unreachable!("scalar accumulator is not numeric"),
        }
    }

    fn add(&mut self, value_type: &AggregateValueType, value: &OwnedValue) -> bool {
        match (self, value_type) {
            (NumericAccum::Signed(sum), AggregateValueType::Signed) => {
                if let Some(value) = owned_value_to_i64(value) {
                    *sum = sum.saturating_add(value);
                    true
                } else {
                    false
                }
            }
            (NumericAccum::Unsigned(sum), AggregateValueType::Unsigned) => {
                if let Some(value) = owned_value_to_u64(value) {
                    *sum = sum.saturating_add(value);
                    true
                } else {
                    false
                }
            }
            (NumericAccum::Float(sum), AggregateValueType::Float) => {
                if let Some(value) = owned_value_to_f64(value) {
                    *sum += value;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn into_owned_value(self) -> OwnedValue {
        match self {
            NumericAccum::Signed(sum) => OwnedValue::I64(sum),
            NumericAccum::Unsigned(sum) => OwnedValue::U64(sum),
            NumericAccum::Float(sum) => OwnedValue::F64(sum),
        }
    }
}

fn collect_aggregate_required_fields(
    group_by_fields: &[u64],
    aggregates: &[ValidatedAggregateSpec],
) -> Vec<u64> {
    let mut fields = Vec::with_capacity(group_by_fields.len() + aggregates.len());
    for field_id in group_by_fields {
        if !fields.contains(field_id) {
            fields.push(*field_id);
        }
    }
    for aggregate in aggregates {
        if let Some(field_id) = aggregate.spec.field_id {
            if !fields.contains(&field_id) {
                fields.push(field_id);
            }
        }
    }
    fields
}

fn serialize_group_key(values: &[OwnedValue]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| format!("{values:?}"))
}

fn sort_aggregate_rows(rows: &mut [AggregateRow], order_by: &AggregateOrderBy) {
    rows.sort_unstable_by(|left, right| {
        let left_value = aggregate_row_value(left, &order_by.target);
        let right_value = aggregate_row_value(right, &order_by.target);
        compare_optional_owned_values(left_value, right_value, order_by.ordering)
            .then_with(|| compare_aggregate_group_values(left, right))
    });
}

fn compare_aggregate_group_values(left: &AggregateRow, right: &AggregateRow) -> CmpOrdering {
    for ((_, left_value), (_, right_value)) in left.group_values.iter().zip(right.group_values.iter()) {
        let ordering = compare_owned_values(left_value, right_value, QueryOrdering::Asc);
        if ordering != CmpOrdering::Equal {
            return ordering;
        }
    }
    left.group_values.len().cmp(&right.group_values.len())
}

fn aggregate_row_value<'a>(
    row: &'a AggregateRow,
    target: &AggregateOrderTarget,
) -> Option<&'a OwnedValue> {
    match target {
        AggregateOrderTarget::GroupField(field_id) => row
            .group_values
            .iter()
            .find_map(|(candidate_field_id, value)| (*candidate_field_id == *field_id).then_some(value)),
        AggregateOrderTarget::AggregateAlias(alias) => row
            .aggregate_values
            .iter()
            .find_map(|(candidate_alias, value)| (candidate_alias == alias).then_some(value)),
    }
}

fn compare_optional_owned_values(
    left: Option<&OwnedValue>,
    right: Option<&OwnedValue>,
    ordering: QueryOrdering,
) -> CmpOrdering {
    match (left, right) {
        (Some(left), Some(right)) => compare_owned_values(left, right, ordering),
        (Some(_), None) => CmpOrdering::Less,
        (None, Some(_)) => CmpOrdering::Greater,
        (None, None) => CmpOrdering::Equal,
    }
}

fn compare_owned_values(left: &OwnedValue, right: &OwnedValue, ordering: QueryOrdering) -> CmpOrdering {
    let cmp = left.partial_cmp(right).unwrap_or(CmpOrdering::Equal);
    match ordering {
        QueryOrdering::Asc => cmp,
        QueryOrdering::Desc => cmp.reverse(),
    }
}

fn is_null_like(value: &OwnedValue) -> bool {
    matches!(value, OwnedValue::Null | OwnedValue::NA)
}

fn numeric_value_type(field: &crate::ram::schema::Field) -> Option<AggregateValueType> {
    if field.is_array {
        return None;
    }
    match field.data_type {
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => Some(AggregateValueType::Signed),
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => Some(AggregateValueType::Unsigned),
        Type::F32 | Type::F64 => Some(AggregateValueType::Float),
        _ => None,
    }
}

fn owned_value_to_i64(value: &OwnedValue) -> Option<i64> {
    match value {
        OwnedValue::I8(value) => Some(*value as i64),
        OwnedValue::I16(value) => Some(*value as i64),
        OwnedValue::I32(value) => Some(*value as i64),
        OwnedValue::I64(value) => Some(*value),
        _ => None,
    }
}

fn owned_value_to_u64(value: &OwnedValue) -> Option<u64> {
    match value {
        OwnedValue::U8(value) => Some(*value as u64),
        OwnedValue::U16(value) => Some(*value as u64),
        OwnedValue::U32(value) => Some(*value as u64),
        OwnedValue::U64(value) => Some(*value),
        _ => None,
    }
}

fn owned_value_to_f64(value: &OwnedValue) -> Option<f64> {
    match value {
        OwnedValue::I8(value) => Some(*value as f64),
        OwnedValue::I16(value) => Some(*value as f64),
        OwnedValue::I32(value) => Some(*value as f64),
        OwnedValue::I64(value) => Some(*value as f64),
        OwnedValue::U8(value) => Some(*value as f64),
        OwnedValue::U16(value) => Some(*value as f64),
        OwnedValue::U32(value) => Some(*value as f64),
        OwnedValue::U64(value) => Some(*value as f64),
        OwnedValue::F32(value) => Some(*value as f64),
        OwnedValue::F64(value) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod alignment_tests;
