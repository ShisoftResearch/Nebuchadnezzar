use std::collections::HashMap;

use bifrost_hasher::hash_str;
use dovahkiin::{
    expr::serde::Expr,
    types::{OwnedPrimArray, OwnedValue},
};

use crate::{
    query::{
        cost::planner::{
            distinct_estimate_from_stats, estimate_clause_plan_cost, estimate_hashed_eq_rows,
            estimate_ranged_rows, indexed_clause_priority, PlanCost,
        },
        statistics::SchemaStatistics,
    },
    ram::{
        schema::{IndexType, Schema},
        types::index_query_scalars,
    },
};

use super::{ValueRange, ValueRangeTerm};

const DNF_CONJUNCTIONS_CAP: usize = 1024;

#[derive(Clone, Copy)]
enum ClauseOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Similar,
    TextMatch,
}

#[derive(Clone, Debug)]
pub(crate) enum IndexedClausePlan {
    HashedEq {
        field_id: u64,
        value: OwnedValue,
    },
    NullPresence {
        field_id: u64,
    },
    Ranged {
        field_id: u64,
        range: ValueRange,
    },
    VectorSimilarity {
        field_id: u64,
        query: Vec<f32>,
        limit: usize,
    },
    EmbeddingSimilarity {
        field_id: u64,
        query: String,
        limit: usize,
    },
    FullTextMatch {
        field_id: u64,
        query: String,
        limit: usize,
        phrase_boost: bool,
    },
}

const DEFAULT_SIMILARITY_LIMIT: usize = 256;
const DEFAULT_FULLTEXT_LIMIT: usize = 256;

pub(crate) struct IndexedPredicatePlan {
    candidates: Vec<IndexedClausePlan>,
    disjuncts: Vec<IndexedDisjunctPlan>,
    disjunction: bool,
    impossible: bool,
    explain: Vec<ClauseOrderExplain>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexedDisjunctPlan {
    clauses: Vec<IndexedClausePlan>,
    residual: Expr,
}

#[derive(Clone, Debug)]
pub struct ClauseOrderExplain {
    clause: IndexedClausePlan,
    estimated_rows: Option<usize>,
    effective_rows: Option<usize>,
    total_cost: Option<f64>,
    reason: &'static str,
}

#[derive(Clone, Debug)]
pub struct QueryPlanExplain {
    disjunction: bool,
    impossible: bool,
    clauses: Vec<ClauseOrderExplain>,
}

impl ClauseOrderExplain {
    pub fn reason(&self) -> &'static str {
        self.reason
    }

    pub fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }

    pub fn total_cost(&self) -> Option<f64> {
        self.total_cost
    }

    pub fn effective_rows(&self) -> Option<usize> {
        self.effective_rows
    }

    pub fn clause_kind(&self) -> &'static str {
        match self.clause {
            IndexedClausePlan::HashedEq { .. } => "hashed_eq",
            IndexedClausePlan::NullPresence { .. } => "null_presence",
            IndexedClausePlan::Ranged { .. } => "ranged",
            IndexedClausePlan::VectorSimilarity { .. } => "vector_similarity",
            IndexedClausePlan::EmbeddingSimilarity { .. } => "embedding_similarity",
            IndexedClausePlan::FullTextMatch { .. } => "fulltext_match",
        }
    }
}

impl QueryPlanExplain {
    pub fn disjunction(&self) -> bool {
        self.disjunction
    }

    pub fn impossible(&self) -> bool {
        self.impossible
    }

    pub fn clauses(&self) -> &[ClauseOrderExplain] {
        self.clauses.as_slice()
    }
}

impl IndexedPredicatePlan {
    pub(crate) fn new(
        candidates: Vec<IndexedClausePlan>,
        disjuncts: Vec<IndexedDisjunctPlan>,
        disjunction: bool,
        impossible: bool,
        explain: Vec<ClauseOrderExplain>,
    ) -> Self {
        Self {
            candidates,
            disjuncts,
            disjunction,
            impossible,
            explain,
        }
    }

    pub(crate) fn all(&self) -> &[IndexedClausePlan] {
        self.candidates.as_slice()
    }

    pub(crate) fn is_disjunction(&self) -> bool {
        self.disjunction
    }

    pub(crate) fn is_impossible(&self) -> bool {
        self.impossible
    }

    pub(crate) fn disjuncts(&self) -> &[IndexedDisjunctPlan] {
        self.disjuncts.as_slice()
    }

    pub(crate) fn is_pure_relevance_ranked_scan(&self) -> bool {
        self.disjuncts.len() == 1 && self.disjuncts[0].is_pure_relevance_ranked_scan()
    }

    #[allow(dead_code)]
    pub(crate) fn explain(&self) -> &[ClauseOrderExplain] {
        self.explain.as_slice()
    }

    pub(crate) fn into_explain(self) -> QueryPlanExplain {
        QueryPlanExplain {
            disjunction: self.disjunction,
            impossible: self.impossible,
            clauses: self.explain,
        }
    }
}

impl IndexedDisjunctPlan {
    fn new(clauses: Vec<IndexedClausePlan>, residual: Expr) -> Self {
        Self { clauses, residual }
    }

    pub(crate) fn clauses(&self) -> &[IndexedClausePlan] {
        self.clauses.as_slice()
    }

    pub(crate) fn residual(&self) -> &Expr {
        &self.residual
    }

    pub(crate) fn is_pure_relevance_ranked_scan(&self) -> bool {
        self.clauses.len() == 1 && self.clauses[0].uses_relevance_ranking()
    }
}

impl IndexedClausePlan {
    pub(crate) fn uses_relevance_ranking(&self) -> bool {
        matches!(
            self,
            IndexedClausePlan::VectorSimilarity { .. }
                | IndexedClausePlan::EmbeddingSimilarity { .. }
                | IndexedClausePlan::FullTextMatch { .. }
        )
    }
}

type ScoredClause = (
    IndexedClausePlan,
    Option<usize>,
    Option<usize>,
    Option<PlanCost>,
    &'static str,
);

pub(crate) fn build_indexed_predicate_plan(
    schema: &Schema,
    selection: &Expr,
    schema_stats: Option<&SchemaStatistics>,
    order_by_field: Option<u64>,
    limit: Option<usize>,
) -> Option<IndexedPredicatePlan> {
    if let Some(plan) =
        build_simple_disjunction_plan(schema, selection, schema_stats, order_by_field, limit)
    {
        return Some(plan);
    }

    let dnf_conjunctions = selection_to_dnf_conjunctions(selection);
    if dnf_conjunctions.is_empty() {
        return Some(IndexedPredicatePlan::new(
            vec![],
            vec![],
            false,
            true,
            vec![],
        ));
    }

    let mut disjuncts = vec![];
    let mut explain = vec![];
    for conjunction in dnf_conjunctions {
        let conjunction_selection = conjunction_expr(&conjunction);
        let normalized = normalized_indexed_candidates(schema, &conjunction_selection);
        if normalized.impossible {
            continue;
        }

        let residual_clauses = conjunction
            .iter()
            .filter(|clause| indexed_clause_candidate(schema, clause).is_none())
            .cloned()
            .collect::<Vec<_>>();
        let residual = conjunction_expr(&residual_clauses);

        let scored = score_candidates(
            normalized.candidates,
            false,
            schema_stats,
            order_by_field,
            limit,
        );
        let clauses = scored
            .iter()
            .map(|(candidate, _, _, _, _)| candidate.clone())
            .collect::<Vec<_>>();
        explain.extend(
            scored
                .iter()
                .map(
                    |(candidate, rows, effective_rows, cost, reason)| ClauseOrderExplain {
                        clause: candidate.clone(),
                        estimated_rows: *rows,
                        effective_rows: *effective_rows,
                        total_cost: cost.map(|c| c.total_cost),
                        reason: *reason,
                    },
                ),
        );

        disjuncts.push(IndexedDisjunctPlan::new(clauses, residual));
    }

    if disjuncts.is_empty() {
        return Some(IndexedPredicatePlan::new(
            vec![],
            vec![],
            false,
            true,
            vec![],
        ));
    }

    if disjuncts.iter().all(|disjunct| disjunct.clauses.is_empty()) {
        return None;
    }

    let disjunction = disjuncts.len() > 1;
    let candidates = disjuncts
        .iter()
        .flat_map(|disjunct| disjunct.clauses.iter().cloned())
        .collect::<Vec<_>>();
    Some(IndexedPredicatePlan::new(
        candidates,
        disjuncts,
        disjunction,
        false,
        explain,
    ))
}

pub(crate) fn normalize_selection_for_eval(selection: &Expr) -> Expr {
    if selection.is_empty() {
        return Expr::nothing();
    }

    let disjuncts = selection_to_dnf_conjunctions(selection);
    if disjuncts.is_empty() {
        return Expr::nothing();
    }

    let normalized_disjuncts = disjuncts
        .into_iter()
        .map(|conjunction| {
            let normalized_clauses = conjunction
                .into_iter()
                .map(normalize_clause_for_eval)
                .collect::<Vec<_>>();
            conjunction_expr(&normalized_clauses)
        })
        .collect::<Vec<_>>();

    match normalized_disjuncts.len() {
        0 => Expr::nothing(),
        1 => normalized_disjuncts
            .into_iter()
            .next()
            .unwrap_or_else(Expr::nothing),
        _ => {
            let mut exprs = vec![Expr::Symbol(hash_str("or"), "or".to_string())];
            exprs.extend(normalized_disjuncts);
            Expr::List(exprs)
        }
    }
}

fn build_simple_disjunction_plan(
    schema: &Schema,
    selection: &Expr,
    schema_stats: Option<&SchemaStatistics>,
    order_by_field: Option<u64>,
    limit: Option<usize>,
) -> Option<IndexedPredicatePlan> {
    let disjuncts = selection_disjuncts(selection)?;
    let mut disj_candidates = Vec::with_capacity(disjuncts.len());
    for disjunct in disjuncts {
        let Some(candidate) = indexed_clause_candidate(schema, disjunct) else {
            return None;
        };
        disj_candidates.push(candidate);
    }
    if disj_candidates.is_empty() {
        return None;
    }

    let scored = score_candidates(disj_candidates, true, schema_stats, order_by_field, limit);
    let explain = scored
        .iter()
        .map(
            |(candidate, rows, effective_rows, cost, reason)| ClauseOrderExplain {
                clause: candidate.clone(),
                estimated_rows: *rows,
                effective_rows: *effective_rows,
                total_cost: cost.map(|c| c.total_cost),
                reason: *reason,
            },
        )
        .collect::<Vec<_>>();
    let candidates = scored
        .into_iter()
        .map(|(candidate, _, _, _, _)| candidate)
        .collect::<Vec<_>>();
    let disjunct_plans = candidates
        .iter()
        .cloned()
        .map(|candidate| IndexedDisjunctPlan::new(vec![candidate], Expr::nothing()))
        .collect::<Vec<_>>();
    Some(IndexedPredicatePlan::new(
        candidates,
        disjunct_plans,
        true,
        false,
        explain,
    ))
}

fn score_candidates(
    candidates: Vec<IndexedClausePlan>,
    disjunction: bool,
    schema_stats: Option<&SchemaStatistics>,
    order_by_field: Option<u64>,
    limit: Option<usize>,
) -> Vec<(
    IndexedClausePlan,
    Option<usize>,
    Option<usize>,
    Option<PlanCost>,
    &'static str,
)> {
    let mut scored = candidates
        .into_iter()
        .map(|candidate| {
            let estimated_rows = estimate_candidate_rows(&candidate, schema_stats);
            let order_aligned = is_order_aligned(&candidate, order_by_field);
            let cost = estimated_rows
                .map(|rows| candidate_plan_cost(&candidate, rows, limit, order_aligned));
            let effective_rows =
                estimated_rows.map(|rows| limit.map(|l| rows.min(l.max(1))).unwrap_or(rows.max(1)));
            let reason = if cost.is_some() {
                if order_aligned && limit.is_some() {
                    "cost-model-limit-order"
                } else {
                    "cost-model"
                }
            } else {
                "heuristic"
            };
            (candidate, estimated_rows, effective_rows, cost, reason)
        })
        .collect::<Vec<_>>();

    if disjunction {
        return order_disjunction_candidates(scored, schema_stats, limit);
    }

    scored.sort_by(
        |(left, l_rows, _l_effective_rows, l_cost, _),
         (right, r_rows, _r_effective_rows, r_cost, _)| {
            if let (Some(lc), Some(rc)) = (l_cost, r_cost) {
                return lc
                    .total_cost
                    .partial_cmp(&rc.total_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| clause_priority(right).cmp(&clause_priority(left)))
                    .then_with(|| {
                        clause_selectivity_cost(left).cmp(&clause_selectivity_cost(right))
                    });
            }

            if l_cost.is_some() {
                return std::cmp::Ordering::Less;
            }
            if r_cost.is_some() {
                return std::cmp::Ordering::Greater;
            }

            if let (Some(lv), Some(rv)) = (l_rows, r_rows) {
                return lv
                    .cmp(rv)
                    .then_with(|| clause_priority(right).cmp(&clause_priority(left)))
                    .then_with(|| {
                        clause_selectivity_cost(left).cmp(&clause_selectivity_cost(right))
                    });
            }
            if l_rows.is_some() {
                return std::cmp::Ordering::Less;
            }
            if r_rows.is_some() {
                return std::cmp::Ordering::Greater;
            }

            clause_priority(right)
                .cmp(&clause_priority(left))
                .then_with(|| clause_selectivity_cost(left).cmp(&clause_selectivity_cost(right)))
        },
    );
    scored
}

fn order_disjunction_candidates(
    mut candidates: Vec<ScoredClause>,
    schema_stats: Option<&SchemaStatistics>,
    limit: Option<usize>,
) -> Vec<ScoredClause> {
    let mut ordered = Vec::with_capacity(candidates.len());
    let mut estimated_union_rows = 0usize;
    let total_rows = schema_stats.map(|stats| stats.count.max(1)).unwrap_or(1);

    while !candidates.is_empty() {
        let mut best_idx = 0usize;
        let mut best_cost = f64::INFINITY;
        for (idx, (_clause, est_rows, _effective_rows, base_cost, _reason)) in
            candidates.iter().enumerate()
        {
            let marginal_rows =
                estimate_or_marginal_rows(*est_rows, estimated_union_rows, total_rows);
            let effective_rows = limit
                .map(|l| marginal_rows.min(l.max(1)))
                .unwrap_or(marginal_rows.max(1));
            let base = base_cost.map(|cost| cost.startup_cost).unwrap_or(5.0);
            let per_row = base_cost.map(|cost| cost.per_row_cost).unwrap_or(2.0);
            let dedup_penalty = ((est_rows
                .unwrap_or(effective_rows)
                .saturating_sub(marginal_rows)) as f64)
                * 0.05;
            let effective_cost = base + (effective_rows as f64 * per_row) + dedup_penalty;
            if effective_cost < best_cost {
                best_cost = effective_cost;
                best_idx = idx;
            }
        }

        let (clause, est_rows, _effective_rows, _base_cost, _reason) =
            candidates.swap_remove(best_idx);
        let marginal_rows = estimate_or_marginal_rows(est_rows, estimated_union_rows, total_rows);
        estimated_union_rows = (estimated_union_rows + marginal_rows).min(total_rows);
        let effective_cost = candidate_plan_cost_for_or(&clause, est_rows, marginal_rows, limit);
        let effective_rows = Some(
            limit
                .map(|l| marginal_rows.min(l.max(1)))
                .unwrap_or(marginal_rows),
        );
        let reason = if effective_cost.is_some() {
            if limit.is_some() {
                "cost-model-or-limit"
            } else {
                "cost-model-or"
            }
        } else {
            "heuristic-or"
        };
        ordered.push((clause, est_rows, effective_rows, effective_cost, reason));
    }

    ordered
}

fn estimate_or_marginal_rows(
    estimated_rows: Option<usize>,
    union_rows_so_far: usize,
    total_rows: usize,
) -> usize {
    let estimated_rows = estimated_rows.unwrap_or((total_rows / 3).max(1));
    let overlap_ratio = (union_rows_so_far as f64 / total_rows as f64).clamp(0.0, 0.85);
    let marginal = ((estimated_rows as f64) * (1.0 - overlap_ratio)).ceil() as usize;
    marginal.max(1)
}

fn candidate_plan_cost_for_or(
    clause: &IndexedClausePlan,
    estimated_rows: Option<usize>,
    marginal_rows: usize,
    limit: Option<usize>,
) -> Option<PlanCost> {
    let rows = estimated_rows?;
    match clause {
        IndexedClausePlan::HashedEq { .. } => Some(estimate_clause_plan_cost(
            true,
            false,
            false,
            false,
            rows.max(marginal_rows),
            limit,
            false,
        )),
        IndexedClausePlan::NullPresence { .. } => Some(estimate_clause_plan_cost(
            true,
            false,
            false,
            false,
            rows.max(marginal_rows),
            limit,
            false,
        )),
        IndexedClausePlan::Ranged { range, .. } => Some(estimate_clause_plan_cost(
            false,
            true,
            matches!(range.start, ValueRangeTerm::Open),
            matches!(range.end, ValueRangeTerm::Open),
            rows.max(marginal_rows),
            limit,
            false,
        )),
        IndexedClausePlan::VectorSimilarity { .. }
        | IndexedClausePlan::EmbeddingSimilarity { .. }
        | IndexedClausePlan::FullTextMatch { .. } => Some(estimate_clause_plan_cost(
            false,
            false,
            false,
            false,
            rows.max(marginal_rows),
            limit,
            false,
        )),
    }
}

fn estimate_candidate_rows(
    candidate: &IndexedClausePlan,
    schema_stats: Option<&SchemaStatistics>,
) -> Option<usize> {
    let stats = schema_stats?;
    if stats.count == 0 {
        return Some(0);
    }
    match candidate {
        IndexedClausePlan::HashedEq { field_id, .. } => {
            let distinct = distinct_estimate_from_stats(stats, *field_id);
            Some(estimate_hashed_eq_rows(stats.count, distinct).estimated_rows)
        }
        IndexedClausePlan::NullPresence { field_id } => {
            let distinct = distinct_estimate_from_stats(stats, *field_id);
            Some(estimate_hashed_eq_rows(stats.count, distinct).estimated_rows)
        }
        IndexedClausePlan::Ranged { field_id, range } => {
            let histogram = stats.histogram.get(field_id).map(|h| h.as_slice());
            Some(
                estimate_ranged_rows(stats.count, histogram, &range.start, &range.end)
                    .estimated_rows,
            )
        }
        IndexedClausePlan::VectorSimilarity { .. } => Some((stats.count / 12).max(1)),
        IndexedClausePlan::EmbeddingSimilarity { .. } => Some((stats.count / 10).max(1)),
        IndexedClausePlan::FullTextMatch { .. } => Some((stats.count / 8).max(1)),
    }
}

fn is_order_aligned(candidate: &IndexedClausePlan, order_by_field: Option<u64>) -> bool {
    match (candidate, order_by_field) {
        (IndexedClausePlan::Ranged { field_id, .. }, Some(order_field)) => *field_id == order_field,
        _ => false,
    }
}

fn candidate_plan_cost(
    candidate: &IndexedClausePlan,
    estimated_rows: usize,
    limit: Option<usize>,
    order_aligned: bool,
) -> PlanCost {
    match candidate {
        IndexedClausePlan::HashedEq { .. } => {
            estimate_clause_plan_cost(true, false, false, false, estimated_rows, limit, false)
        }
        IndexedClausePlan::NullPresence { .. } => {
            estimate_clause_plan_cost(true, false, false, false, estimated_rows, limit, false)
        }
        IndexedClausePlan::Ranged { range, .. } => estimate_clause_plan_cost(
            false,
            true,
            matches!(range.start, ValueRangeTerm::Open),
            matches!(range.end, ValueRangeTerm::Open),
            estimated_rows,
            limit,
            order_aligned,
        ),
        IndexedClausePlan::VectorSimilarity { .. }
        | IndexedClausePlan::EmbeddingSimilarity { .. }
        | IndexedClausePlan::FullTextMatch { .. } => {
            estimate_clause_plan_cost(false, false, false, false, estimated_rows, limit, false)
        }
    }
}

fn clause_priority(candidate: &IndexedClausePlan) -> u8 {
    match candidate {
        IndexedClausePlan::HashedEq { .. } => {
            indexed_clause_priority(true, false, true, false, false)
        }
        IndexedClausePlan::NullPresence { .. } => {
            indexed_clause_priority(true, false, true, false, false)
        }
        IndexedClausePlan::Ranged { range, .. } => indexed_clause_priority(
            false,
            true,
            is_range_equality(range),
            matches!(range.start, ValueRangeTerm::Open),
            matches!(range.end, ValueRangeTerm::Open),
        ),
        IndexedClausePlan::VectorSimilarity { .. }
        | IndexedClausePlan::EmbeddingSimilarity { .. } => 85,
        IndexedClausePlan::FullTextMatch { .. } => 60,
    }
}

fn is_range_equality(range: &ValueRange) -> bool {
    match (&range.start, &range.end) {
        (ValueRangeTerm::Inclusive(s), ValueRangeTerm::Inclusive(e)) => s == e,
        _ => false,
    }
}

struct NormalizedCandidates {
    candidates: Vec<IndexedClausePlan>,
    impossible: bool,
}

fn normalized_indexed_candidates(schema: &Schema, selection: &Expr) -> NormalizedCandidates {
    let mut hashed_eq_by_field: HashMap<u64, OwnedValue> = HashMap::new();
    let mut range_by_field: HashMap<u64, ValueRange> = HashMap::new();
    let mut extra_candidates = vec![];

    for clause in selection_conjuncts(selection) {
        if clause_constant_truth(schema, clause) == Some(false) {
            return NormalizedCandidates {
                candidates: vec![],
                impossible: true,
            };
        }
        if clause_constant_truth(schema, clause) == Some(true) {
            continue;
        }

        let Some(candidate) = indexed_clause_candidate(schema, clause) else {
            continue;
        };
        match candidate {
            IndexedClausePlan::HashedEq { field_id, value } => {
                if let Some(existing) = hashed_eq_by_field.get(&field_id) {
                    if existing != &value {
                        return NormalizedCandidates {
                            candidates: vec![],
                            impossible: true,
                        };
                    }
                } else {
                    hashed_eq_by_field.insert(field_id, value.clone());
                }

                if let Some(existing_range) = range_by_field.get(&field_id) {
                    if !value_in_range(&value, existing_range) {
                        return NormalizedCandidates {
                            candidates: vec![],
                            impossible: true,
                        };
                    }
                }
            }
            IndexedClausePlan::NullPresence { field_id } => {
                extra_candidates.push(IndexedClausePlan::NullPresence { field_id });
            }
            IndexedClausePlan::Ranged { field_id, range } => {
                if let Some(existing_eq) = hashed_eq_by_field.get(&field_id) {
                    if !value_in_range(existing_eq, &range) {
                        return NormalizedCandidates {
                            candidates: vec![],
                            impossible: true,
                        };
                    }
                    continue;
                }

                if let Some(existing_range) = range_by_field.get(&field_id) {
                    let Some(merged) = intersect_ranges(existing_range, &range) else {
                        return NormalizedCandidates {
                            candidates: vec![],
                            impossible: true,
                        };
                    };
                    range_by_field.insert(field_id, merged);
                } else {
                    range_by_field.insert(field_id, range);
                }
            }
            IndexedClausePlan::VectorSimilarity { .. }
            | IndexedClausePlan::EmbeddingSimilarity { .. }
            | IndexedClausePlan::FullTextMatch { .. } => {
                extra_candidates.push(candidate);
            }
        }
    }

    let mut candidates = Vec::with_capacity(
        hashed_eq_by_field.len() + range_by_field.len() + extra_candidates.len(),
    );
    for (field_id, value) in hashed_eq_by_field {
        candidates.push(IndexedClausePlan::HashedEq { field_id, value });
    }
    for (field_id, range) in range_by_field {
        if !candidates
            .iter()
            .any(|candidate| matches!(candidate, IndexedClausePlan::HashedEq { field_id: id, .. } if *id == field_id))
        {
            candidates.push(IndexedClausePlan::Ranged { field_id, range });
        }
    }
    candidates.extend(extra_candidates);
    NormalizedCandidates {
        candidates,
        impossible: false,
    }
}

fn clause_constant_truth(schema: &Schema, clause: &Expr) -> Option<bool> {
    let (is_not_null, field_id) = null_check_clause(clause)?;
    let field = schema.field_by_id_path(&[field_id])?;
    if field.nullable {
        None
    } else if is_not_null {
        Some(true)
    } else {
        Some(false)
    }
}

fn clause_selectivity_cost(candidate: &IndexedClausePlan) -> u8 {
    match candidate {
        IndexedClausePlan::HashedEq { .. } => 1,
        IndexedClausePlan::NullPresence { .. } => 1,
        IndexedClausePlan::Ranged { range, .. } => {
            let start_open = matches!(range.start, ValueRangeTerm::Open);
            let end_open = matches!(range.end, ValueRangeTerm::Open);
            if is_range_equality(range) {
                2
            } else if !start_open && !end_open {
                4
            } else if !start_open || !end_open {
                8
            } else {
                16
            }
        }
        IndexedClausePlan::VectorSimilarity { .. }
        | IndexedClausePlan::EmbeddingSimilarity { .. } => 6,
        IndexedClausePlan::FullTextMatch { .. } => 10,
    }
}

fn value_in_range(value: &OwnedValue, range: &ValueRange) -> bool {
    let feature = value.shared().feature();
    let lower_ok = match &range.start {
        ValueRangeTerm::Inclusive(start) => feature >= *start,
        ValueRangeTerm::Exclusive(start) => feature > *start,
        ValueRangeTerm::Open => true,
    };
    let upper_ok = match &range.end {
        ValueRangeTerm::Inclusive(end) => feature <= *end,
        ValueRangeTerm::Exclusive(end) => feature < *end,
        ValueRangeTerm::Open => true,
    };
    lower_ok && upper_ok
}

fn intersect_ranges(left: &ValueRange, right: &ValueRange) -> Option<ValueRange> {
    let start = max_start_term(&left.start, &right.start);
    let end = min_end_term(&left.end, &right.end);

    if range_terms_conflict(&start, &end) {
        None
    } else {
        Some(ValueRange { start, end })
    }
}

fn max_start_term(left: &ValueRangeTerm, right: &ValueRangeTerm) -> ValueRangeTerm {
    use std::cmp::Ordering;
    match (left, right) {
        (ValueRangeTerm::Open, term) | (term, ValueRangeTerm::Open) => term.clone(),
        (ValueRangeTerm::Inclusive(l), ValueRangeTerm::Inclusive(r)) => {
            if l >= r {
                ValueRangeTerm::Inclusive(*l)
            } else {
                ValueRangeTerm::Inclusive(*r)
            }
        }
        (ValueRangeTerm::Exclusive(l), ValueRangeTerm::Exclusive(r)) => {
            if l >= r {
                ValueRangeTerm::Exclusive(*l)
            } else {
                ValueRangeTerm::Exclusive(*r)
            }
        }
        (ValueRangeTerm::Inclusive(l), ValueRangeTerm::Exclusive(r)) => match l.cmp(r) {
            Ordering::Less => ValueRangeTerm::Exclusive(*r),
            Ordering::Equal => ValueRangeTerm::Exclusive(*r),
            Ordering::Greater => ValueRangeTerm::Inclusive(*l),
        },
        (ValueRangeTerm::Exclusive(l), ValueRangeTerm::Inclusive(r)) => match l.cmp(r) {
            Ordering::Less => ValueRangeTerm::Inclusive(*r),
            Ordering::Equal => ValueRangeTerm::Exclusive(*l),
            Ordering::Greater => ValueRangeTerm::Exclusive(*l),
        },
    }
}

fn min_end_term(left: &ValueRangeTerm, right: &ValueRangeTerm) -> ValueRangeTerm {
    use std::cmp::Ordering;
    match (left, right) {
        (ValueRangeTerm::Open, term) | (term, ValueRangeTerm::Open) => term.clone(),
        (ValueRangeTerm::Inclusive(l), ValueRangeTerm::Inclusive(r)) => {
            if l <= r {
                ValueRangeTerm::Inclusive(*l)
            } else {
                ValueRangeTerm::Inclusive(*r)
            }
        }
        (ValueRangeTerm::Exclusive(l), ValueRangeTerm::Exclusive(r)) => {
            if l <= r {
                ValueRangeTerm::Exclusive(*l)
            } else {
                ValueRangeTerm::Exclusive(*r)
            }
        }
        (ValueRangeTerm::Inclusive(l), ValueRangeTerm::Exclusive(r)) => match l.cmp(r) {
            Ordering::Less => ValueRangeTerm::Inclusive(*l),
            Ordering::Equal => ValueRangeTerm::Exclusive(*r),
            Ordering::Greater => ValueRangeTerm::Exclusive(*r),
        },
        (ValueRangeTerm::Exclusive(l), ValueRangeTerm::Inclusive(r)) => match l.cmp(r) {
            Ordering::Less => ValueRangeTerm::Exclusive(*l),
            Ordering::Equal => ValueRangeTerm::Exclusive(*l),
            Ordering::Greater => ValueRangeTerm::Inclusive(*r),
        },
    }
}

fn range_terms_conflict(start: &ValueRangeTerm, end: &ValueRangeTerm) -> bool {
    match (start, end) {
        (ValueRangeTerm::Open, _) | (_, ValueRangeTerm::Open) => false,
        (ValueRangeTerm::Inclusive(s), ValueRangeTerm::Inclusive(e)) => s > e,
        (ValueRangeTerm::Inclusive(s), ValueRangeTerm::Exclusive(e)) => s >= e,
        (ValueRangeTerm::Exclusive(s), ValueRangeTerm::Inclusive(e)) => s >= e,
        (ValueRangeTerm::Exclusive(s), ValueRangeTerm::Exclusive(e)) => s >= e,
    }
}

fn selection_conjuncts(selection: &Expr) -> Vec<&Expr> {
    if let Expr::List(exprs) = selection {
        if exprs.is_empty() {
            return vec![];
        }
        if is_symbol_named(&exprs[0], "and") {
            return exprs.iter().skip(1).collect();
        }
    }
    vec![selection]
}

fn selection_disjuncts(selection: &Expr) -> Option<Vec<&Expr>> {
    if let Expr::List(exprs) = selection {
        if exprs.is_empty() {
            return None;
        }
        if is_symbol_named(&exprs[0], "or") {
            return Some(exprs.iter().skip(1).collect());
        }
    }
    None
}

fn selection_to_dnf_conjunctions(selection: &Expr) -> Vec<Vec<Expr>> {
    if let Some(expanded) = expand_special_clause(selection) {
        return selection_to_dnf_conjunctions(&expanded);
    }

    let Expr::List(items) = selection else {
        return vec![vec![selection.clone()]];
    };

    if items.is_empty() {
        return vec![];
    }

    if is_symbol_named(&items[0], "and") {
        let mut conjunctions = vec![Vec::new()];
        for child in items.iter().skip(1) {
            let child_dnf = selection_to_dnf_conjunctions(child);
            if child_dnf.is_empty() {
                return vec![];
            }

            let expected_len = conjunctions.len() * child_dnf.len().max(1);
            if expected_len > DNF_CONJUNCTIONS_CAP {
                return vec![conjunctions.into_iter().flatten().collect()];
            }

            let mut next = Vec::with_capacity(conjunctions.len() * child_dnf.len().max(1));
            for base in &conjunctions {
                for child_conjunction in &child_dnf {
                    let mut merged = Vec::with_capacity(base.len() + child_conjunction.len());
                    merged.extend(base.iter().cloned());
                    merged.extend(child_conjunction.iter().cloned());
                    next.push(merged);
                }
            }
            conjunctions = next;
        }
        return conjunctions;
    }

    if is_symbol_named(&items[0], "or") {
        let mut disjuncts = vec![];
        for child in items.iter().skip(1) {
            disjuncts.extend(selection_to_dnf_conjunctions(child));
        }
        return disjuncts;
    }

    if is_symbol_named(&items[0], "not") {
        if items.len() != 2 {
            return vec![vec![selection.clone()]];
        }
        return negated_selection_to_dnf_conjunctions(&items[1]);
    }

    vec![vec![selection.clone()]]
}

fn negated_selection_to_dnf_conjunctions(selection: &Expr) -> Vec<Vec<Expr>> {
    if let Some(expanded) = expand_special_clause(selection) {
        return negated_selection_to_dnf_conjunctions(&expanded);
    }

    let Expr::List(items) = selection else {
        return vec![vec![negate_expr(selection.clone())]];
    };

    if items.is_empty() {
        return vec![];
    }

    if is_symbol_named(&items[0], "not") {
        if items.len() != 2 {
            return vec![vec![negate_expr(selection.clone())]];
        }
        return selection_to_dnf_conjunctions(&items[1]);
    }

    if is_symbol_named(&items[0], "and") {
        let mut disjuncts = vec![];
        for child in items.iter().skip(1) {
            disjuncts.extend(negated_selection_to_dnf_conjunctions(child));
        }
        return disjuncts;
    }

    if is_symbol_named(&items[0], "or") {
        let mut conjunctions = vec![Vec::new()];
        for child in items.iter().skip(1) {
            let child_dnf = negated_selection_to_dnf_conjunctions(child);
            if child_dnf.is_empty() {
                return vec![];
            }

            let expected_len = conjunctions.len() * child_dnf.len().max(1);
            if expected_len > DNF_CONJUNCTIONS_CAP {
                return vec![conjunctions.into_iter().flatten().collect()];
            }

            let mut next = Vec::with_capacity(conjunctions.len() * child_dnf.len().max(1));
            for base in &conjunctions {
                for child_conjunction in &child_dnf {
                    let mut merged = Vec::with_capacity(base.len() + child_conjunction.len());
                    merged.extend(base.iter().cloned());
                    merged.extend(child_conjunction.iter().cloned());
                    next.push(merged);
                }
            }
            conjunctions = next;
        }
        return conjunctions;
    }

    if let Some(negated) = negate_comparison_expr(selection) {
        return selection_to_dnf_conjunctions(&negated);
    }

    if let Some(negated) = negate_null_check_expr(selection) {
        return selection_to_dnf_conjunctions(&negated);
    }

    vec![vec![negate_expr(selection.clone())]]
}

fn conjunction_expr(clauses: &[Expr]) -> Expr {
    match clauses.len() {
        0 => Expr::nothing(),
        1 => clauses[0].clone(),
        _ => {
            let mut exprs = vec![Expr::Symbol(hash_str("and"), "and".to_string())];
            exprs.extend_from_slice(clauses);
            Expr::List(exprs)
        }
    }
}

fn negate_expr(expr: Expr) -> Expr {
    Expr::List(vec![Expr::Symbol(hash_str("not"), "not".to_string()), expr])
}

fn negate_comparison_expr(expr: &Expr) -> Option<Expr> {
    let (op, field_id, value) = comparison_clause(expr)?;
    let field_name = expr_field_name(expr)?;

    match op {
        ClauseOp::Eq => Some(Expr::List(vec![
            Expr::Symbol(hash_str("or"), "or".to_string()),
            comparison_expr(ClauseOp::Lt, field_id, field_name.clone(), value.clone()),
            comparison_expr(ClauseOp::Gt, field_id, field_name, value),
        ])),
        ClauseOp::Ne => Some(comparison_expr(ClauseOp::Eq, field_id, field_name, value)),
        ClauseOp::Gt => Some(comparison_expr(ClauseOp::Le, field_id, field_name, value)),
        ClauseOp::Ge => Some(comparison_expr(ClauseOp::Lt, field_id, field_name, value)),
        ClauseOp::Lt => Some(comparison_expr(ClauseOp::Ge, field_id, field_name, value)),
        ClauseOp::Le => Some(comparison_expr(ClauseOp::Gt, field_id, field_name, value)),
        ClauseOp::Similar | ClauseOp::TextMatch => None,
    }
}

fn negate_null_check_expr(expr: &Expr) -> Option<Expr> {
    let (is_not_null, field_id) = null_check_clause(expr)?;
    let field_name = expr_unary_field_name(expr)?;
    Some(unary_field_expr(
        if is_not_null {
            "is-null"
        } else {
            "is-not-null"
        },
        field_id,
        field_name,
    ))
}

fn comparison_expr(op: ClauseOp, field_id: u64, field_name: String, value: OwnedValue) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str(op_name(op)), op_name(op).to_string()),
        Expr::Symbol(field_id, field_name),
        Expr::Value(value),
    ])
}

fn unary_field_expr(op_name: &str, field_id: u64, field_name: String) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str(op_name), op_name.to_string()),
        Expr::Symbol(field_id, field_name),
    ])
}

fn normalize_clause_for_eval(expr: Expr) -> Expr {
    if let Some((is_not_null, field_id)) = null_check_clause(&expr) {
        let field_name = expr_unary_field_name(&expr).unwrap_or_else(|| field_id.to_string());
        return Expr::List(vec![
            Expr::Symbol(
                hash_str(if is_not_null { "!=" } else { "=" }),
                if is_not_null { "!=" } else { "=" }.to_string(),
            ),
            Expr::Symbol(field_id, field_name),
            Expr::Value(OwnedValue::Null),
        ]);
    }

    expr
}

fn indexed_clause_candidate(schema: &Schema, clause: &Expr) -> Option<IndexedClausePlan> {
    if let Some((is_not_null, field_id)) = null_check_clause(clause) {
        let indices = schema.index_fields.get(&field_id)?;
        if !is_not_null {
            if indices.iter().any(|idx| matches!(idx, IndexType::Null)) {
                return Some(IndexedClausePlan::NullPresence { field_id });
            }
            return None;
        }
        if indices.iter().any(|idx| matches!(idx, IndexType::Ranged)) {
            return Some(IndexedClausePlan::Ranged {
                field_id,
                range: ValueRange {
                    start: ValueRangeTerm::Open,
                    end: ValueRangeTerm::Open,
                },
            });
        }
        return None;
    }

    let (op, field_id, value) = comparison_clause(clause)?;
    let indices = if let Some(indices) = schema.index_fields.get(&field_id) {
        indices
    } else if let Some(compound) = schema.compound_index_fields.get(&field_id) {
        &compound.indices
    } else {
        return None;
    };

    let supports_hashed = indices.iter().any(|idx| matches!(idx, IndexType::Hashed));
    let supports_ranged = indices.iter().any(|idx| matches!(idx, IndexType::Ranged));
    let supports_vector = indices
        .iter()
        .any(|idx| matches!(idx, IndexType::Vector(_)));
    let supports_embedding = indices
        .iter()
        .any(|idx| matches!(idx, IndexType::Embedding(_)));
    let supports_fulltext = indices.iter().any(|idx| matches!(idx, IndexType::Fulltext));

    if supports_hashed && matches!(op, ClauseOp::Eq) {
        return Some(IndexedClausePlan::HashedEq { field_id, value });
    }

    if supports_ranged {
        let range = match op {
            ClauseOp::Eq => ValueRange {
                start: ValueRangeTerm::Inclusive(value.shared().feature()),
                end: ValueRangeTerm::Inclusive(value.shared().feature()),
            },
            ClauseOp::Ne => return None,
            ClauseOp::Gt => ValueRange {
                start: ValueRangeTerm::Exclusive(value.shared().feature()),
                end: ValueRangeTerm::Open,
            },
            ClauseOp::Ge => ValueRange {
                start: ValueRangeTerm::Inclusive(value.shared().feature()),
                end: ValueRangeTerm::Open,
            },
            ClauseOp::Lt => ValueRange {
                start: ValueRangeTerm::Open,
                end: ValueRangeTerm::Exclusive(value.shared().feature()),
            },
            ClauseOp::Le => ValueRange {
                start: ValueRangeTerm::Open,
                end: ValueRangeTerm::Inclusive(value.shared().feature()),
            },
            ClauseOp::Similar | ClauseOp::TextMatch => return None,
        };
        return Some(IndexedClausePlan::Ranged { field_id, range });
    }

    if matches!(op, ClauseOp::Similar) {
        if supports_embedding {
            if let Some(query) = owned_value_string(&value) {
                return Some(IndexedClausePlan::EmbeddingSimilarity {
                    field_id,
                    query,
                    limit: DEFAULT_SIMILARITY_LIMIT,
                });
            }
        }
        if supports_vector {
            if let Some(query) = owned_value_f32_vector(&value) {
                return Some(IndexedClausePlan::VectorSimilarity {
                    field_id,
                    query,
                    limit: DEFAULT_SIMILARITY_LIMIT,
                });
            }
        }
    }

    if matches!(op, ClauseOp::TextMatch) && supports_fulltext {
        if let Some(query) = owned_value_string(&value) {
            return Some(IndexedClausePlan::FullTextMatch {
                field_id,
                query,
                limit: DEFAULT_FULLTEXT_LIMIT,
                phrase_boost: true,
            });
        }
    }

    None
}

fn comparison_clause(clause: &Expr) -> Option<(ClauseOp, u64, OwnedValue)> {
    let Expr::List(items) = clause else {
        return None;
    };
    if items.len() != 3 {
        return None;
    }

    let mut op = parse_clause_op(&items[0])?;

    if let (Some(field_id), Some(value)) = (expr_field_id(&items[1]), expr_owned_value(&items[2])) {
        return Some((op, field_id, value));
    }

    if let (Some(value), Some(field_id)) = (expr_owned_value(&items[1]), expr_field_id(&items[2])) {
        op = reverse_op(op);
        return Some((op, field_id, value));
    }

    None
}

fn null_check_clause(clause: &Expr) -> Option<(bool, u64)> {
    let Expr::List(items) = clause else {
        return None;
    };
    if items.len() != 2 {
        return None;
    }

    if is_symbol_named(&items[0], "is-null") {
        expr_field_id(&items[1]).map(|field_id| (false, field_id))
    } else if is_symbol_named(&items[0], "is-not-null") {
        expr_field_id(&items[1]).map(|field_id| (true, field_id))
    } else {
        None
    }
}

fn expand_special_clause(expr: &Expr) -> Option<Expr> {
    if let Some(expanded) = expand_in_clause(expr) {
        return Some(expanded);
    }
    if let Some(expanded) = expand_between_clause(expr) {
        return Some(expanded);
    }
    expand_array_comparison_clause(expr)
}

fn expand_array_comparison_clause(expr: &Expr) -> Option<Expr> {
    let (op, field_id, value) = comparison_clause(expr)?;
    if !matches!(
        op,
        ClauseOp::Eq | ClauseOp::Ne | ClauseOp::Gt | ClauseOp::Ge | ClauseOp::Lt | ClauseOp::Le
    ) {
        return None;
    }
    let field_name = expr_field_name(expr)?;
    let scalar_values = index_query_scalars(&value)?;

    if scalar_values.len() <= 1 {
        return None;
    }

    let junction_name = if matches!(op, ClauseOp::Ne) {
        "and"
    } else {
        "or"
    };
    let mut clauses = Vec::with_capacity(scalar_values.len() + 1);
    clauses.push(Expr::Symbol(
        hash_str(junction_name),
        junction_name.to_string(),
    ));
    for scalar_value in scalar_values {
        clauses.push(comparison_expr(
            op,
            field_id,
            field_name.clone(),
            scalar_value,
        ));
    }
    Some(Expr::List(clauses))
}

fn expand_in_clause(expr: &Expr) -> Option<Expr> {
    let Expr::List(items) = expr else {
        return None;
    };
    if items.len() < 3 || !is_symbol_named(&items[0], "in") {
        return None;
    }

    let field_expr = items[1].clone();
    let mut disjuncts = Vec::with_capacity(items.len() - 1);
    disjuncts.push(Expr::Symbol(hash_str("or"), "or".to_string()));
    for value in items.iter().skip(2) {
        disjuncts.push(Expr::List(vec![
            Expr::Symbol(hash_str("="), "=".to_string()),
            field_expr.clone(),
            value.clone(),
        ]));
    }
    Some(Expr::List(disjuncts))
}

fn expand_between_clause(expr: &Expr) -> Option<Expr> {
    let Expr::List(items) = expr else {
        return None;
    };
    if items.len() != 4 || !is_symbol_named(&items[0], "between") {
        return None;
    }

    Some(Expr::List(vec![
        Expr::Symbol(hash_str("and"), "and".to_string()),
        Expr::List(vec![
            Expr::Symbol(hash_str(">="), ">=".to_string()),
            items[1].clone(),
            items[2].clone(),
        ]),
        Expr::List(vec![
            Expr::Symbol(hash_str("<="), "<=".to_string()),
            items[1].clone(),
            items[3].clone(),
        ]),
    ]))
}

fn parse_clause_op(expr: &Expr) -> Option<ClauseOp> {
    if is_symbol_named(expr, "=") {
        Some(ClauseOp::Eq)
    } else if is_symbol_named(expr, "!=") {
        Some(ClauseOp::Ne)
    } else if is_symbol_named(expr, ">") {
        Some(ClauseOp::Gt)
    } else if is_symbol_named(expr, ">=") {
        Some(ClauseOp::Ge)
    } else if is_symbol_named(expr, "<") {
        Some(ClauseOp::Lt)
    } else if is_symbol_named(expr, "<=") {
        Some(ClauseOp::Le)
    } else if is_symbol_named(expr, "~") {
        Some(ClauseOp::Similar)
    } else if is_symbol_named(expr, "@") {
        Some(ClauseOp::TextMatch)
    } else {
        None
    }
}

fn reverse_op(op: ClauseOp) -> ClauseOp {
    match op {
        ClauseOp::Eq => ClauseOp::Eq,
        ClauseOp::Ne => ClauseOp::Ne,
        ClauseOp::Gt => ClauseOp::Lt,
        ClauseOp::Ge => ClauseOp::Le,
        ClauseOp::Lt => ClauseOp::Gt,
        ClauseOp::Le => ClauseOp::Ge,
        ClauseOp::Similar => ClauseOp::Similar,
        ClauseOp::TextMatch => ClauseOp::TextMatch,
    }
}

fn op_name(op: ClauseOp) -> &'static str {
    match op {
        ClauseOp::Eq => "=",
        ClauseOp::Ne => "!=",
        ClauseOp::Gt => ">",
        ClauseOp::Ge => ">=",
        ClauseOp::Lt => "<",
        ClauseOp::Le => "<=",
        ClauseOp::Similar => "~",
        ClauseOp::TextMatch => "@",
    }
}

fn owned_value_string(value: &OwnedValue) -> Option<String> {
    if let OwnedValue::String(query) = value {
        Some(query.clone())
    } else {
        None
    }
}

fn owned_value_f32_vector(value: &OwnedValue) -> Option<Vec<f32>> {
    match value {
        OwnedValue::PrimArray(OwnedPrimArray::F32(values)) => Some(values.clone()),
        _ => None,
    }
}

fn expr_field_id(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Symbol(id, _) | Expr::Keyword(id, _) => Some(*id),
        _ => None,
    }
}

fn expr_field_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::List(items) if items.len() == 3 => match (&items[1], &items[2]) {
            (Expr::Symbol(_, symbol), Expr::Value(_))
            | (Expr::Keyword(_, symbol), Expr::Value(_)) => Some(symbol.clone()),
            (Expr::Value(_), Expr::Symbol(_, symbol))
            | (Expr::Value(_), Expr::Keyword(_, symbol)) => Some(symbol.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn expr_unary_field_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::List(items) if items.len() == 2 => match &items[1] {
            Expr::Symbol(_, symbol) | Expr::Keyword(_, symbol) => Some(symbol.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn expr_owned_value(expr: &Expr) -> Option<OwnedValue> {
    if let Expr::Value(value) = expr {
        Some(value.clone())
    } else {
        None
    }
}

fn is_symbol_named(expr: &Expr, name: &str) -> bool {
    if let Expr::Symbol(id, symbol) = expr {
        symbol == name || *id == hash_str(name)
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dovahkiin::types::{OwnedPrimArray, OwnedValue, Type};

    use crate::{
        index::{embedding::EmbeddingModel, vector::MetricEncoding, vector::VectorIndexConfig},
        ram::schema::{Field, IndexType, Schema},
    };

    use super::*;

    #[test]
    fn plan_prefers_more_selective_hashed_clause_when_stats_available() {
        let field_a = "FIELD_A";
        let field_b = "FIELD_B";
        let schema = Schema::new_with_id(
            3001,
            "planner_stats_ordering",
            None,
            Field::new_schema(vec![
                Field::new_indexed(field_a, Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed(field_b, Type::U64, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let field_a_id = hash_str(field_a);
        let field_b_id = hash_str(field_b);
        let mut histogram = HashMap::new();
        histogram.insert(field_a_id, histogram_with_distinct(2));
        histogram.insert(field_b_id, histogram_with_distinct(80));
        let stats = SchemaStatistics {
            histogram,
            count: 10_000,
            segs: 1,
            bytes: 1,
            timestamp: 1,
        };

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("and"), "and".to_string()),
            eq_expr(field_a, 1),
            eq_expr(field_b, 1),
        ]);

        let plan =
            build_indexed_predicate_plan(&schema, &selection, Some(&stats), None, None).unwrap();
        match plan.all().first().unwrap() {
            IndexedClausePlan::HashedEq { field_id, .. } => assert_eq!(*field_id, field_b_id),
            _ => panic!("expected hashed clause"),
        }
        assert_eq!(plan.explain().len(), 2);
        assert_eq!(plan.explain()[0].reason(), "cost-model");
    }

    #[test]
    fn plan_prefers_order_aligned_ranged_clause_when_limit_present() {
        let hash_field = "HASH_F";
        let range_field = "RANGE_F";
        let schema = Schema::new_with_id(
            3002,
            "planner_limit_ordering",
            None,
            Field::new_schema(vec![
                Field::new_indexed(hash_field, Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed(range_field, Type::U64, vec![IndexType::Ranged]),
            ]),
            false,
            false,
        );

        let range_field_id = hash_str(range_field);
        let mut histogram = HashMap::new();
        histogram.insert(hash_str(hash_field), histogram_with_distinct(100));
        histogram.insert(range_field_id, histogram_with_distinct(100));
        let stats = SchemaStatistics {
            histogram,
            count: 50_000,
            segs: 1,
            bytes: 1,
            timestamp: 1,
        };

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("and"), "and".to_string()),
            eq_expr(hash_field, 7),
            Expr::List(vec![
                Expr::Symbol(hash_str(">="), ">=".to_string()),
                Expr::Symbol(range_field_id, range_field.to_string()),
                Expr::Value(OwnedValue::U64(10)),
            ]),
        ]);

        let plan = build_indexed_predicate_plan(
            &schema,
            &selection,
            Some(&stats),
            Some(range_field_id),
            Some(5),
        )
        .unwrap();
        match plan.all().first().unwrap() {
            IndexedClausePlan::Ranged { field_id, .. } => assert_eq!(*field_id, range_field_id),
            _ => panic!("expected ranged clause first"),
        }
        assert_eq!(plan.explain()[0].reason(), "cost-model-limit-order");
    }

    #[test]
    fn disjunction_plan_uses_or_cost_reasoning() {
        let field_a = "OR_A";
        let field_b = "OR_B";
        let schema = Schema::new_with_id(
            3003,
            "planner_or_costing",
            None,
            Field::new_schema(vec![
                Field::new_indexed(field_a, Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed(field_b, Type::U64, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let mut histogram = HashMap::new();
        histogram.insert(hash_str(field_a), histogram_with_distinct(200));
        histogram.insert(hash_str(field_b), histogram_with_distinct(2));
        let stats = SchemaStatistics {
            histogram,
            count: 20_000,
            segs: 1,
            bytes: 1,
            timestamp: 1,
        };

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("or"), "or".to_string()),
            eq_expr(field_a, 1),
            eq_expr(field_b, 1),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, Some(&stats), None, Some(20))
            .unwrap();
        assert!(plan.is_disjunction());
        assert_eq!(plan.explain().len(), 2);
        assert!(
            plan.explain()[0].reason() == "cost-model-or-limit"
                || plan.explain()[0].reason() == "cost-model-or"
        );
    }

    #[test]
    fn plan_supports_fulltext_operator() {
        let field = "TEXT";
        let schema = Schema::new_with_id(
            3004,
            "planner_fulltext_operator",
            None,
            Field::new_schema(vec![Field::new_indexed(
                field,
                Type::String,
                vec![IndexType::Fulltext],
            )]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("@"), "@".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::String("ranking database".to_string())),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert_eq!(plan.explain()[0].clause_kind(), "fulltext_match");
    }

    #[test]
    fn plan_supports_embedding_similarity_operator() {
        let field = "EMB";
        let schema = Schema::new_with_id(
            3005,
            "planner_embedding_operator",
            None,
            Field::new_schema(vec![Field::new_indexed(
                field,
                Type::String,
                vec![IndexType::Embedding(EmbeddingModel::default_model())],
            )]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("~"), "~".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::String("semantic query".to_string())),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert_eq!(plan.explain()[0].clause_kind(), "embedding_similarity");
    }

    #[test]
    fn plan_supports_compound_embedding_similarity_operator() {
        let compound_name = "title_body";
        let mut schema = Schema::new_with_id(
            3015,
            "planner_compound_embedding_operator",
            None,
            Field::new_schema(vec![
                Field::new_unindexed("title", Type::String),
                Field::new_unindexed("body", Type::String),
            ]),
            false,
            false,
        );
        schema.add_compound_index(
            compound_name,
            vec!["title".to_string(), "body".to_string()],
            vec![IndexType::Embedding(EmbeddingModel::default_model())],
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("~"), "~".to_string()),
            Expr::Symbol(hash_str(compound_name), compound_name.to_string()),
            Expr::Value(OwnedValue::String("semantic query".to_string())),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert_eq!(plan.explain()[0].clause_kind(), "embedding_similarity");
    }

    #[test]
    fn plan_supports_vector_similarity_operator() {
        let field = "VEC";
        let schema = Schema::new_with_id(
            3006,
            "planner_vector_operator",
            None,
            Field::new_schema(vec![Field::new_indexed(
                field,
                Type::String,
                vec![IndexType::Vector(VectorIndexConfig::new(
                    MetricEncoding::Cosine,
                ))],
            )]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("~"), "~".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::PrimArray(OwnedPrimArray::F32(vec![
                0.1, 0.2, 0.3,
            ]))),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert_eq!(plan.explain()[0].clause_kind(), "vector_similarity");
    }

    #[test]
    fn disjunction_plan_supports_fulltext_and_hashed_clauses() {
        let text_field = "TEXT";
        let tag_field = "TAG";
        let schema = Schema::new_with_id(
            3007,
            "planner_fulltext_or_hashed",
            None,
            Field::new_schema(vec![
                Field::new_indexed(text_field, Type::String, vec![IndexType::Fulltext]),
                Field::new_indexed(tag_field, Type::String, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("or"), "or".to_string()),
            Expr::List(vec![
                Expr::Symbol(hash_str("@"), "@".to_string()),
                Expr::Symbol(hash_str(text_field), text_field.to_string()),
                Expr::Value(OwnedValue::String("database ranking".to_string())),
            ]),
            eq_string_expr(tag_field, "infra"),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, Some(20)).unwrap();
        assert!(plan.is_disjunction());
        assert_eq!(plan.explain().len(), 2);
        let kinds = plan
            .explain()
            .iter()
            .map(ClauseOrderExplain::clause_kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"fulltext_match"));
        assert!(kinds.contains(&"hashed_eq"));
    }

    #[test]
    fn conjunction_plan_supports_embedding_and_hashed_clauses() {
        let emb_field = "EMB";
        let tag_field = "TAG";
        let schema = Schema::new_with_id(
            3008,
            "planner_embedding_and_hashed",
            None,
            Field::new_schema(vec![
                Field::new_indexed(
                    emb_field,
                    Type::String,
                    vec![IndexType::Embedding(EmbeddingModel::default_model())],
                ),
                Field::new_indexed(tag_field, Type::String, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("and"), "and".to_string()),
            Expr::List(vec![
                Expr::Symbol(hash_str("~"), "~".to_string()),
                Expr::Symbol(hash_str(emb_field), emb_field.to_string()),
                Expr::Value(OwnedValue::String("semantic query".to_string())),
            ]),
            eq_string_expr(tag_field, "infra"),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert!(!plan.is_disjunction());
        assert_eq!(plan.explain().len(), 2);
        let kinds = plan
            .explain()
            .iter()
            .map(ClauseOrderExplain::clause_kind)
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"embedding_similarity"));
        assert!(kinds.contains(&"hashed_eq"));
    }

    #[test]
    fn vector_similarity_requires_vector_payload() {
        let field = "VEC";
        let schema = Schema::new_with_id(
            3009,
            "planner_vector_operator_payload",
            None,
            Field::new_schema(vec![Field::new_indexed(
                field,
                Type::String,
                vec![IndexType::Vector(VectorIndexConfig::new(
                    MetricEncoding::Cosine,
                ))],
            )]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("~"), "~".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::String("not a vector".to_string())),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None);
        assert!(plan.is_none(), "vector ~ should require f32 array payload");
    }

    #[test]
    fn fulltext_match_requires_string_payload() {
        let field = "TEXT";
        let schema = Schema::new_with_id(
            3010,
            "planner_fulltext_operator_payload",
            None,
            Field::new_schema(vec![Field::new_indexed(
                field,
                Type::String,
                vec![IndexType::Fulltext],
            )]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("@"), "@".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::U64(42)),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None);
        assert!(plan.is_none(), "@ should require string payload");
    }

    #[test]
    fn nested_and_or_plans_into_multiple_disjuncts() {
        let field_a = "A";
        let field_b = "B";
        let field_c = "C";
        let schema = Schema::new_with_id(
            3011,
            "planner_nested_and_or",
            None,
            Field::new_schema(vec![
                Field::new_indexed(field_a, Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed(field_b, Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed(field_c, Type::U64, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("and"), "and".to_string()),
            eq_expr(field_a, 1),
            Expr::List(vec![
                Expr::Symbol(hash_str("or"), "or".to_string()),
                eq_expr(field_b, 2),
                eq_expr(field_c, 3),
            ]),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert!(plan.is_disjunction());
        assert_eq!(plan.disjuncts().len(), 2);
        assert!(plan
            .disjuncts()
            .iter()
            .all(|disjunct| disjunct.clauses().len() == 2));
    }

    #[test]
    fn nested_or_and_keeps_fulltext_and_hashed_clauses() {
        let text_field = "TEXT";
        let tag_field = "TAG";
        let schema = Schema::new_with_id(
            3012,
            "planner_nested_or_and_special",
            None,
            Field::new_schema(vec![
                Field::new_indexed(text_field, Type::String, vec![IndexType::Fulltext]),
                Field::new_indexed(tag_field, Type::String, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("or"), "or".to_string()),
            Expr::List(vec![
                Expr::Symbol(hash_str("and"), "and".to_string()),
                Expr::List(vec![
                    Expr::Symbol(hash_str("@"), "@".to_string()),
                    Expr::Symbol(hash_str(text_field), text_field.to_string()),
                    Expr::Value(OwnedValue::String("database ranking".to_string())),
                ]),
                eq_string_expr(tag_field, "infra"),
            ]),
            eq_string_expr(tag_field, "search"),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert!(plan.is_disjunction());
        assert_eq!(plan.disjuncts().len(), 2);
        let kinds = plan
            .all()
            .iter()
            .map(|clause| match clause {
                IndexedClausePlan::HashedEq { .. } => "hashed_eq",
                IndexedClausePlan::NullPresence { .. } => "null_presence",
                IndexedClausePlan::Ranged { .. } => "ranged",
                IndexedClausePlan::VectorSimilarity { .. } => "vector_similarity",
                IndexedClausePlan::EmbeddingSimilarity { .. } => "embedding_similarity",
                IndexedClausePlan::FullTextMatch { .. } => "fulltext_match",
            })
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"fulltext_match"));
        assert!(kinds.contains(&"hashed_eq"));
    }

    #[test]
    fn nested_or_and_keeps_embedding_and_hashed_clauses() {
        let emb_field = "EMB";
        let tag_field = "TAG";
        let schema = Schema::new_with_id(
            3013,
            "planner_nested_or_and_embedding",
            None,
            Field::new_schema(vec![
                Field::new_indexed(
                    emb_field,
                    Type::String,
                    vec![IndexType::Embedding(EmbeddingModel::default_model())],
                ),
                Field::new_indexed(tag_field, Type::String, vec![IndexType::Hashed]),
            ]),
            false,
            false,
        );

        let selection = Expr::List(vec![
            Expr::Symbol(hash_str("or"), "or".to_string()),
            Expr::List(vec![
                Expr::Symbol(hash_str("and"), "and".to_string()),
                Expr::List(vec![
                    Expr::Symbol(hash_str("~"), "~".to_string()),
                    Expr::Symbol(hash_str(emb_field), emb_field.to_string()),
                    Expr::Value(OwnedValue::String("semantic query".to_string())),
                ]),
                eq_string_expr(tag_field, "infra"),
            ]),
            eq_string_expr(tag_field, "ops"),
        ]);

        let plan = build_indexed_predicate_plan(&schema, &selection, None, None, None).unwrap();
        assert!(plan.is_disjunction());
        assert_eq!(plan.disjuncts().len(), 2);
        let kinds = plan
            .all()
            .iter()
            .map(|clause| match clause {
                IndexedClausePlan::HashedEq { .. } => "hashed_eq",
                IndexedClausePlan::NullPresence { .. } => "null_presence",
                IndexedClausePlan::Ranged { .. } => "ranged",
                IndexedClausePlan::VectorSimilarity { .. } => "vector_similarity",
                IndexedClausePlan::EmbeddingSimilarity { .. } => "embedding_similarity",
                IndexedClausePlan::FullTextMatch { .. } => "fulltext_match",
            })
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"embedding_similarity"));
        assert!(kinds.contains(&"hashed_eq"));
    }

    fn eq_expr(field: &str, value: u64) -> Expr {
        Expr::List(vec![
            Expr::Symbol(hash_str("="), "=".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::U64(value)),
        ])
    }

    fn eq_string_expr(field: &str, value: &str) -> Expr {
        Expr::List(vec![
            Expr::Symbol(hash_str("="), "=".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::String(value.to_string())),
        ])
    }

    fn histogram_with_distinct(unique: usize) -> [[u8; 8]; 101] {
        let mut histogram = [[0u8; 8]; 101];
        for (idx, slot) in histogram.iter_mut().enumerate() {
            *slot = ((idx % unique.max(1)) as u64).to_be_bytes();
        }
        histogram
    }
}
