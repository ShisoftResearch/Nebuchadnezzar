use std::io;

use bifrost::rpc::RPCError;
use dovahkiin::{
    ahash::HashMap,
    types::{Id, OwnedValue},
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
    ) -> Result<Vec<Id>, RPCError> {
        let mut all_ids = vec![];
        for disjunct in plan.disjuncts() {
            let ids = self
                .execute_disjunct_ids(schema, disjunct, ordering, hit_table)
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
    ) -> Result<Vec<Id>, RPCError> {
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
                    if Self::is_special_clause(first) || Self::is_invalid_input_error(&e) {
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
                        if Self::is_special_clause(candidate) || Self::is_invalid_input_error(&e) {
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

        if !disjunct.residual().is_empty() {
            candidate_ids = self
                .filter_ids_by_selection_limit(&candidate_ids, disjunct.residual(), None)
                .await;
        }
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
                .search_distributed(schema.get(), field_id, query_vector, limit.max(1), None)
                .await
        } else {
            self.index_clients
                .vector_client
                .search(schema.get(), field_id, query_vector, limit.max(1), None)
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
            .search(schema.get(), field_id, query, limit.max(1))
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
