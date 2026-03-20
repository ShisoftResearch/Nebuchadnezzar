use std::collections::{HashMap, HashSet};

use dovahkiin::types::Id;

use crate::query::{data_client::QueryOrdering, planner::IndexedClausePlan};

pub(super) fn intersect_ids_ordered(base: Vec<Id>, next_ids: &[Id]) -> Vec<Id> {
    let next_set: HashSet<Id> = next_ids.iter().copied().collect();
    base.into_iter()
        .filter(|id| next_set.contains(id))
        .collect()
}

pub(super) fn union_ids_ordered(mut base: Vec<Id>, next_ids: &[Id]) -> Vec<Id> {
    let mut seen: HashMap<Id, ()> = base.iter().copied().map(|id| (id, ())).collect();
    for id in next_ids {
        if !seen.contains_key(id) {
            seen.insert(*id, ());
            base.push(*id);
        }
    }
    base
}

pub(super) fn clause_execution_order(candidates: &[IndexedClausePlan]) -> Vec<&IndexedClausePlan> {
    candidates.iter().collect()
}

pub(super) fn sort_ids_by_query_order(ids: &mut [Id], ordering: QueryOrdering) {
    ids.sort_unstable();
    if matches!(ordering, QueryOrdering::Desc) {
        ids.reverse();
    }
}
