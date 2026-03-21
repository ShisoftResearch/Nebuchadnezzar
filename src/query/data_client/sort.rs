use std::{cmp::Ordering as CmpOrdering, collections::HashSet};

use bifrost::rpc::RPCError;
use dovahkiin::{ahash::HashMap, types::Id};

use crate::index::ranged::tree::btree::Ordering;

use super::{IndexedDataClient, QueryOrdering, ValueRange, ValueRangeTerm};

impl IndexedDataClient {
    pub(super) async fn reorder_ids_by_field(
        &self,
        schema: u32,
        field_id: u64,
        ids: &[Id],
        ordering: QueryOrdering,
    ) -> Result<Vec<Id>, RPCError> {
        let _ = schema;
        let mut result = ids.to_vec();
        self.sort_ids_by_field(field_id, &mut result, ordering, ordering)
            .await;
        Ok(result)
    }

    pub(super) async fn sort_ids_by_field_postprocessing(
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
                            let feature = if matches!(cell[field_id], dovahkiin::types::OwnedValue::Null) {
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

    pub(super) async fn distinct_ids_by_fields(&self, field_ids: &[u64], ids: Vec<Id>) -> Vec<Id> {
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
}

pub(super) fn query_order_to_scan_order(ordering: QueryOrdering) -> Ordering {
    match ordering {
        QueryOrdering::Asc => Ordering::Forward,
        QueryOrdering::Desc => Ordering::Backward,
    }
}

pub(super) fn range_index_order_for_range(range: &ValueRange) -> Ordering {
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
