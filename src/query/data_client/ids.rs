use std::collections::HashSet;

use dovahkiin::types::Id;

use crate::{index::ranged::tree::btree::Ordering, query::planner::IndexedClausePlan};

pub(super) fn intersect_ids_ordered(base: Vec<Id>, next_ids: &[Id]) -> Vec<Id> {
    let next_set: HashSet<Id> = next_ids.iter().copied().collect();
    base.into_iter()
        .filter(|id| next_set.contains(id))
        .collect()
}

pub(super) fn clause_execution_order(candidates: &[IndexedClausePlan]) -> Vec<&IndexedClausePlan> {
    if let Some((idx, _)) = candidates
        .iter()
        .enumerate()
        .find(|(_, candidate)| matches!(candidate, IndexedClausePlan::Ranged { .. }))
    {
        let mut ordered = Vec::with_capacity(candidates.len());
        ordered.push(&candidates[idx]);
        for (i, candidate) in candidates.iter().enumerate() {
            if i != idx {
                ordered.push(candidate);
            }
        }
        return ordered;
    }
    candidates.iter().collect()
}

pub(super) fn sort_ids_by_ordering(ids: &mut [Id], ordering: Ordering) {
    ids.sort_unstable();
    if matches!(ordering, Ordering::Backward) {
        ids.reverse();
    }
}
