use std::{cmp::Ordering as CmpOrdering, collections::HashSet};

use bifrost::rpc::RPCError;
use dovahkiin::{ahash::HashMap, types::Id};

use crate::ram::cell::OwnedCell;

use super::{IndexedDataClient, QueryOrdering};

impl IndexedDataClient {
    pub(super) async fn reorder_ids_by_field(
        &self,
        schema: u32,
        field_id: u64,
        ids: &[Id],
        ordering: QueryOrdering,
        cached: &HashMap<Id, OwnedCell>,
    ) -> Result<Vec<Id>, RPCError> {
        let _ = schema;
        let mut result = ids.to_vec();
        self.sort_ids_by_field(field_id, &mut result, ordering, ordering, cached)
            .await?;
        Ok(result)
    }

    pub(super) async fn sort_ids_by_field_postprocessing(
        &self,
        field_id: u64,
        ids: &mut [Id],
        ordering: QueryOrdering,
        cached: &HashMap<Id, OwnedCell>,
    ) -> Result<(), RPCError> {
        self.sort_ids_by_field(field_id, ids, ordering, QueryOrdering::Asc, cached)
            .await
    }

    /// One cell per id, from the cache when every id is cached, else from the
    /// owners. `None` is a cell that is gone. A failed read is an error: a sort
    /// or a DISTINCT over a partial read picks the wrong rows.
    async fn cells_for(
        &self,
        ids: &[Id],
        cached: &HashMap<Id, OwnedCell>,
    ) -> Result<Vec<Option<OwnedCell>>, RPCError> {
        if ids.iter().all(|id| cached.contains_key(id)) {
            return Ok(ids.iter().map(|id| Some(cached[id].clone())).collect());
        }
        self.read_full_cells_aligned(ids).await
    }

    async fn sort_ids_by_field(
        &self,
        field_id: u64,
        ids: &mut [Id],
        ordering: QueryOrdering,
        tie_break_ordering: QueryOrdering,
        cached: &HashMap<Id, OwnedCell>,
    ) -> Result<(), RPCError> {
        if ids.len() <= 1 {
            return Ok(());
        }

        let cells = self.cells_for(ids, cached).await?;
        let mut feature_by_id = HashMap::default();
        for (id, cell) in ids.iter().zip(cells) {
            // A cell deleted since it was selected sorts last, like a null.
            let feature = cell.and_then(|cell| match &cell[field_id] {
                dovahkiin::types::OwnedValue::Null
                | dovahkiin::types::OwnedValue::Array(_)
                | dovahkiin::types::OwnedValue::PrimArray(_)
                | dovahkiin::types::OwnedValue::Map(_) => None,
                value => Some(value.feature()),
            });
            feature_by_id.insert(*id, feature);
        }

        ids.sort_unstable_by(|left, right| {
            let left_feature = feature_by_id.get(left).copied().flatten();
            let right_feature = feature_by_id.get(right).copied().flatten();
            compare_optional_features(left_feature, right_feature, ordering)
                .then_with(|| compare_ids_for_query_order(left, right, tie_break_ordering))
        });
        Ok(())
    }

    pub(super) async fn distinct_ids_by_fields(
        &self,
        field_ids: &[u64],
        ids: Vec<Id>,
        cached: &HashMap<Id, OwnedCell>,
    ) -> Result<Vec<Id>, RPCError> {
        if ids.len() <= 1 {
            return Ok(ids);
        }

        let cells = self.cells_for(&ids, cached).await?;
        let mut seen = HashSet::new();
        let mut distinct_ids = Vec::with_capacity(ids.len());
        // A cell deleted since it was selected is not a row any more.
        for (id, cell) in ids.into_iter().zip(cells) {
            let Some(cell) = cell else { continue };
            let key = field_ids
                .iter()
                .map(|field_id| cell[*field_id].clone())
                .collect::<Vec<_>>();
            if seen.insert(key) {
                distinct_ids.push(id);
            }
        }
        Ok(distinct_ids)
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
