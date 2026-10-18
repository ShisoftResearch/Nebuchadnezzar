use crate::{index::verification::IndexCheck, ram::cell::OwnedCell};
use std::io;

use bifrost::rpc::RPCError;
use dovahkiin::{
    ahash::HashMap,
    types::{Id, Map, OwnedValue},
};
use itertools::Itertools;

use crate::{
    index::{
        embedding::EmbeddingHit, full_text::BM25Hit, hash::get_null_hash_id,
        ranged::tree::btree::Ordering, vector::VectorHit,
    },
    query::planner::{IndexedClausePlan, IndexedDisjunctPlan, IndexedPredicatePlan},
};

use super::{
    ids::{
        clause_execution_order, intersect_ids_ordered, sort_ids_by_query_order, union_ids_ordered,
    },
    sort::range_index_order_for_range,
    IndexedDataClient, QueryHitTable, QueryHitType, QueryOrdering, ValueRange,
};
use crate::ram::schema::SchemaUid;

impl IndexedDataClient {
    pub(super) async fn execute_clause_ids(
        &self,
        schema: SchemaUid,
        clause: &IndexedClausePlan,
        _ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
    ) -> Result<Vec<Id>, RPCError> {
        match clause {
            IndexedClausePlan::HashedEq { field_id, value } => {
                let mut ids = Vec::new();
                for bucket in Self::hashed_query_index_ids(schema, *field_id, value) {
                    if let Ok(hits) = self
                        .index_clients
                        .hashed_client
                        .indexer
                        .candidates(bucket)
                        .await?
                    {
                        ids = union_ids_ordered(ids, &hits);
                    }
                }
                Ok(ids)
            }
            IndexedClausePlan::NullPresence { field_id } => {
                let index_id = get_null_hash_id(schema, *field_id);
                match self
                    .index_clients
                    .hashed_client
                    .indexer
                    .candidates(index_id)
                    .await?
                {
                    Ok(ids) => Ok(ids),
                    Err(_) => Ok(Vec::new()),
                }
            }
            IndexedClausePlan::Ranged { field_id, range } => {
                self.range_query_ids(schema, *field_id, range, Ordering::Forward)
                    .await
            }
            IndexedClausePlan::VectorSimilarity {
                field_id,
                query,
                limit,
            } => {
                let hits = self
                    .vector_query_hits(schema, *field_id, query.as_slice(), *limit)
                    .await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                        hit_table
                            .entry(hit.id)
                            .or_insert_with(HashMap::default)
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
                let hits = self
                    .embedding_query_hits(schema, *field_id, query.as_str(), *limit)
                    .await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                        hit_table
                            .entry(hit.id)
                            .or_insert_with(HashMap::default)
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
                let hits = self
                    .fulltext_query_hits(schema, *field_id, query.as_str(), *limit, *phrase_boost)
                    .await?;
                if let Some(hit_table) = hit_table {
                    for hit in &hits {
                        hit_table
                            .entry(hit.id)
                            .or_insert_with(HashMap::default)
                            .entry((*field_id, QueryHitType::BM25Hit))
                            .and_modify(|score| *score = score.max(hit.score))
                            .or_insert(hit.score);
                    }
                }
                Ok(hits.into_iter().map(|hit| hit.id).collect_vec())
            }
        }
    }

    pub(super) async fn execute_predicate_plan_ids(
        &self,
        schema: SchemaUid,
        plan: &IndexedPredicatePlan,
        ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
        cells: &mut HashMap<Id, OwnedCell>,
        retain_cells: bool,
        cache_fields: &[u64],
        verify: super::Verify,
    ) -> Result<Vec<Id>, RPCError> {
        let mut all_ids = vec![];
        for disjunct in plan.disjuncts() {
            let ids = self
                .execute_disjunct_ids(
                    schema,
                    disjunct,
                    ordering,
                    hit_table,
                    cells,
                    retain_cells,
                    cache_fields,
                    verify,
                )
                .await?;
            all_ids = union_ids_ordered(all_ids, &ids);
        }
        if plan.is_disjunction() && !plan.is_pure_relevance_ranked_scan() {
            sort_ids_by_query_order(&mut all_ids, ordering);
        }
        Ok(all_ids)
    }

    async fn execute_disjunct_ids(
        &self,
        schema: SchemaUid,
        disjunct: &IndexedDisjunctPlan,
        ordering: QueryOrdering,
        hit_table: &mut QueryHitTable,
        cells: &mut HashMap<Id, OwnedCell>,
        retain_cells: bool,
        cache_fields: &[u64],
        verify: super::Verify,
    ) -> Result<Vec<Id>, RPCError> {
        if verify == super::Verify::Exists
            && (!disjunct.residual().is_empty()
                || disjunct.clauses().iter().any(Self::is_special_clause))
        {
            return Err(Self::requires_predicate("residual or relevance predicate"));
        }
        let mut candidate_ids = if disjunct.clauses().is_empty() {
            self.scan_schema_ids(schema, ordering).await?
        } else {
            let ordered_candidates = clause_execution_order(disjunct.clauses());
            let mut candidates = ordered_candidates.iter().copied();
            let Some(first) = candidates.next() else {
                return Ok(vec![]);
            };
            let mut candidate_ids = match self
                .execute_clause_ids(schema, first, ordering, hit_table)
                .await
            {
                Ok(ids) => ids,
                Err(e) => {
                    if verify == super::Verify::Exists
                        || Self::is_special_clause(first)
                        || Self::is_invalid_input_error(&e)
                    {
                        return Err(e);
                    }
                    self.scan_schema_ids(schema, ordering).await?
                }
            };

            for candidate in candidates {
                let ids = match self
                    .execute_clause_ids(schema, candidate, ordering, hit_table)
                    .await
                {
                    Ok(ids) => ids,
                    Err(e) => {
                        if verify == super::Verify::Exists
                            || Self::is_special_clause(candidate)
                            || Self::is_invalid_input_error(&e)
                        {
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
                && !disjunct.is_pure_relevance_ranked_scan()
            {
                sort_ids_by_query_order(&mut candidate_ids, ordering);
            }
            candidate_ids
        };

        if verify == super::Verify::Exists
            && !crate::index::verification::control("NEB_TEST_IDCONTRACT_FORCE_PREDICATE")
        {
            let ids = self.existing_ids(&candidate_ids).await?;
            #[cfg(test)]
            let ids = if crate::index::verification::control("NEB_TEST_IDCONTRACT_DROP_ID") {
                ids.into_iter().skip(1).collect()
            } else {
                ids
            };
            return Ok(ids);
        }
        let mut checks = vec![IndexCheck::Schema(schema)];
        for clause in disjunct.clauses() {
            match clause {
                IndexedClausePlan::Ranged { field_id, range } => checks.push(IndexCheck::Ranged(
                    range
                        .clone()
                        .to_key_range(schema, *field_id, Ordering::Forward),
                )),
                IndexedClausePlan::HashedEq { field_id, value } => {
                    checks.push(IndexCheck::Hashed {
                        buckets: Self::hashed_query_index_ids(schema, *field_id, value),
                        field: *field_id,
                        value: value.clone(),
                    })
                }
                IndexedClausePlan::NullPresence { field_id } => checks.push(IndexCheck::Hashed {
                    buckets: vec![get_null_hash_id(schema, *field_id)],
                    field: *field_id,
                    value: OwnedValue::Null,
                }),
                _ => {}
            }
        }
        let mut verified_ids = Vec::new();
        for batch in candidate_ids.chunks(usize::from(super::SCAN_BUFFER_SIZE)) {
            let read = self
                .read_cells_from_ids_checked(
                    batch,
                    &vec![],
                    &dovahkiin::expr::serde::Expr::nothing(),
                    &dovahkiin::expr::serde::Expr::nothing(),
                    &checks,
                )
                .await;
            for cell in read {
                if super::read::cell_matches_selection(&cell, disjunct.residual()) {
                    verified_ids.push(cell.id());
                    if retain_cells {
                        cells.insert(cell.id(), cell);
                    } else if !cache_fields.is_empty() {
                        let mut data = dovahkiin::types::OwnedMap::new();
                        for field in cache_fields {
                            data.insert_key_id(*field, cell[*field].clone());
                        }
                        cells.insert(
                            cell.id(),
                            OwnedCell {
                                header: cell.header,
                                data: OwnedValue::Map(data),
                            },
                        );
                    }
                }
            }
        }
        candidate_ids = verified_ids;
        Ok(candidate_ids)
    }

    fn is_special_clause(clause: &IndexedClausePlan) -> bool {
        clause.uses_relevance_ranking()
    }

    fn is_invalid_input_error(err: &RPCError) -> bool {
        match err {
            RPCError::IOError(inner) => inner.kind() == io::ErrorKind::InvalidInput,
            _ => false,
        }
    }

    async fn vector_query_hits(
        &self,
        schema: SchemaUid,
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
        search_result.map_err(|e| {
            RPCError::IOError(io::Error::new(
                io::ErrorKind::Other,
                format!("Vector search error: {:?}", e),
            ))
        })
    }

    async fn embedding_query_hits(
        &self,
        schema: SchemaUid,
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
        self.index_clients
            .embedding_client
            .search(schema, field_id, query, limit.max(1))
            .await
            .map_err(|e| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Embedding search error: {:?}", e),
                ))
            })
    }

    async fn fulltext_query_hits(
        &self,
        schema: SchemaUid,
        field_id: u64,
        query: &str,
        limit: usize,
        phrase_boost: bool,
    ) -> Result<Vec<BM25Hit>, RPCError> {
        self.bm25_search(schema, field_id, query, limit.max(1), phrase_boost)
            .await?
            .map_err(|e| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Full-text search error: {:?}", e),
                ))
            })
    }

    async fn range_query_ids(
        &self,
        schema: SchemaUid,
        field: u64,
        range: &ValueRange,
        ordering: Ordering,
    ) -> Result<Vec<Id>, RPCError> {
        let mut ids = vec![];
        let key_range = range.clone().to_key_range(schema, field, ordering);
        let Some(mut cursor) = self
            .index_clients
            .range_seek(key_range, super::SCAN_BUFFER_SIZE, None)
            .await?
        else {
            return Ok(ids);
        };
        while let Some(id) = cursor.next().await? {
            ids.push(id);
        }
        Ok(ids)
    }
}
