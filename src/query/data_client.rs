use std::{io, sync::Arc};

use bifrost::{conshash::ConsistentHashing, raft::client::AsRaftPlaneClient, rpc::RPCError};
use dovahkiin::{
    ahash::HashMap,
    expr::serde::Expr,
    types::{Id, OwnedValue},
};
use itertools::Itertools;

use crate::{
    client::AsyncClient,
    index::{
        full_text::BM25Hit, hash::get_hash_id_from_value, ranged::tree::btree::Ordering,
        IndexerClients,
    },
    query::planner::{IndexedPredicatePlan, QueryPlanExplain},
    ram::{cell::ReadError, types::index_query_scalars},
};

mod aggregate;
mod cursor;
mod execute;
mod ids;
mod plan;
mod projection;
mod read;
mod sort;

use aggregate::{
    collect_aggregate_required_fields, serialize_group_key, sort_aggregate_rows,
    AggregateGroupState,
};
pub use cursor::{AggregateResultCursor, AggregateRow, DataCursor, IdCursor};
use ids::sort_ids_by_query_order;
pub use projection::{ProjectionField, ProjectionItem, QueryResultCursor, QueryRow};

pub use crate::query::planner::{ValueRange, ValueRangeTerm};

const SCAN_BUFFER_SIZE: u16 = 64;

#[derive(Clone)]
pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
    group_name: String,
    database_name: String,
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

impl IndexedDataClient {
    fn hashed_query_rejects_value(value: &OwnedValue) -> bool {
        index_query_scalars(value).is_none()
    }

    fn hashed_query_value_kind(value: &OwnedValue) -> &'static str {
        match value {
            OwnedValue::Map(_) => "map",
            OwnedValue::Array(_) => "array",
            OwnedValue::PrimArray(_) => "primitive array",
            _ => "scalar",
        }
    }

    fn hashed_query_invalid_input_error(
        schema: u32,
        field_id: u64,
        value: &OwnedValue,
    ) -> RPCError {
        RPCError::IOError(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "hashed equality requires a scalar value or flat scalar array for schema {schema} field {field_id}, got {}",
                Self::hashed_query_value_kind(value)
            ),
        ))
    }

    fn hashed_query_index_ids(schema: u32, field: u64, value: &OwnedValue) -> Vec<Id> {
        index_query_scalars(value)
            .into_iter()
            .flatten()
            .map(|scalar| get_hash_id_from_value(schema, field, &scalar))
            .unique()
            .collect()
    }

    pub fn new<C>(
        neb_client: &Arc<AsyncClient>,
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<C>,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        Self {
            conshash: conshash.clone(),
            // Use 0 as server_id for query-only clients since inverted indexer won't be initialized
            index_clients: Arc::new(IndexerClients::new_query_only(
                neb_client,
                conshash,
                raft_client,
                0,
            )),
            group_name: neb_client.group_name().to_string(),
            database_name: neb_client.database_name().to_string(),
        }
    }

    /// Create IndexedDataClient with server's indexer clients (for BM25 search support)
    pub fn new_with_indexers(
        index_clients: Arc<IndexerClients>,
        conshash: Arc<ConsistentHashing>,
    ) -> Self {
        Self {
            group_name: index_clients.neb_client.group_name().to_string(),
            database_name: index_clients.neb_client.database_name().to_string(),
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
        projection: Vec<ProjectionField>,
    ) -> Result<QueryResultCursor, RPCError> {
        self.query_with_options(
            schema, selection, ordering, None, None, None, None, projection,
        )
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
        projection: Vec<ProjectionField>,
    ) -> Result<QueryResultCursor, RPCError> {
        let mut id_cursor = self
            .query_ids_with_options(
                schema,
                selection,
                ordering,
                order_by_field,
                distinct_fields,
                limit,
                offset,
            )
            .await?;
        let mut ids = vec![];
        while let Some(id) = id_cursor.next().await? {
            ids.push(id);
        }
        self.project_ids_to_rows(&ids, projection).await
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
        projection: Vec<ProjectionField>,
    ) -> Result<QueryResultCursor, RPCError> {
        self.query(schema, selection, ordering, projection).await
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
        projection: Vec<ProjectionField>,
    ) -> Result<QueryResultCursor, RPCError> {
        self.query_with_options(
            schema,
            selection,
            ordering,
            order_by_field,
            distinct_fields,
            limit,
            offset,
            projection,
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
        projection: Vec<ProjectionItem>,
    ) -> Result<QueryResultCursor, RPCError> {
        if matches!(query.limit, Some(0)) {
            return Ok(QueryResultCursor {
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
            group_state.accumulate_row(&row, &field_positions, &validated);
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

        Ok(self.shape_aggregate_rows(aggregate_rows, projection))
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
            schema, selection, ordering, None, None, None, None, hit_table,
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
            .then(|| {
                plan.as_ref()
                    .and_then(Self::infer_query_order_field_from_plan)
            })
            .flatten();
        let preserve_relevance_order = explicit_order_by_field.is_none()
            && plan
                .as_ref()
                .map(|plan| plan.is_pure_relevance_ranked_scan())
                .unwrap_or(false);
        let selection_requires_post_filter = !selection.is_empty();
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
            (
                self.scan_schema_ids(schema, ordering).await?,
                selection_requires_post_filter,
            )
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
        } else if explicit_order_by_field.is_none() && !preserve_relevance_order {
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

    pub fn hashed_index_id(schema: u32, field: u64, value: &OwnedValue) -> Id {
        get_hash_id_from_value(schema, field, value)
    }

    pub async fn hashed_query(
        &self,
        schema: u32,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        if Self::hashed_query_rejects_value(value) {
            return Err(Self::hashed_query_invalid_input_error(
                schema, field_id, value,
            ));
        }
        let mut matched_ids = vec![];
        for index_id in Self::hashed_query_index_ids(schema, field_id, value) {
            match self.index_clients.hashed_query(index_id, field_id, value).await? {
                Ok(ids) => matched_ids.extend(ids),
                Err(_) => {}
            }
        }
        Ok(Ok(matched_ids.into_iter().unique().collect()))
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
mod alignment_tests;
#[cfg(test)]
mod tests;
