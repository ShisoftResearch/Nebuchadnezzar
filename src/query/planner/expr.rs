use std::collections::HashMap;

use bifrost_hasher::hash_str;
use dovahkiin::{expr::serde::Expr, types::OwnedValue};

use crate::{
    query::{
        cost::planner::{
            distinct_estimate_from_stats, estimate_hashed_eq_rows, estimate_ranged_rows,
            indexed_clause_priority,
        },
        statistics::SchemaStatistics,
    },
    ram::schema::{IndexType, Schema},
};

use super::{ValueRange, ValueRangeTerm};

#[derive(Clone)]
enum ClauseOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone)]
pub(crate) enum IndexedClausePlan {
    HashedEq { field_id: u64, value: OwnedValue },
    Ranged { field_id: u64, range: ValueRange },
}

pub(crate) struct IndexedPredicatePlan {
    candidates: Vec<IndexedClausePlan>,
    disjunction: bool,
    impossible: bool,
}

impl IndexedPredicatePlan {
    pub(crate) fn new(
        candidates: Vec<IndexedClausePlan>,
        disjunction: bool,
        impossible: bool,
    ) -> Self {
        Self {
            candidates,
            disjunction,
            impossible,
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
}

pub(crate) fn build_indexed_predicate_plan(
    schema: &Schema,
    selection: &Expr,
    schema_stats: Option<&SchemaStatistics>,
) -> Option<IndexedPredicatePlan> {
    let mut candidates;
    let disjunction;
    let mut impossible = false;
    if let Some(disjuncts) = selection_disjuncts(selection) {
        let mut disj_candidates = Vec::with_capacity(disjuncts.len());
        for disjunct in disjuncts {
            let Some(candidate) = indexed_clause_candidate(schema, disjunct) else {
                return None;
            };
            disj_candidates.push(candidate);
        }
        candidates = disj_candidates;
        disjunction = true;
    } else {
        let normalized = normalized_indexed_candidates(schema, selection);
        candidates = normalized.candidates;
        impossible = normalized.impossible;
        disjunction = false;
    }
    if candidates.is_empty() && !impossible {
        return None;
    }
    candidates.sort_by(|left, right| {
        match (
            estimate_candidate_rows(left, schema_stats),
            estimate_candidate_rows(right, schema_stats),
        ) {
            (Some(l_rows), Some(r_rows)) => l_rows
                .cmp(&r_rows)
                .then_with(|| clause_priority(right).cmp(&clause_priority(left)))
                .then_with(|| clause_selectivity_cost(left).cmp(&clause_selectivity_cost(right))),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => clause_priority(right)
                .cmp(&clause_priority(left))
                .then_with(|| clause_selectivity_cost(left).cmp(&clause_selectivity_cost(right))),
        }
    });
    Some(IndexedPredicatePlan::new(
        candidates,
        disjunction,
        impossible,
    ))
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
        IndexedClausePlan::Ranged { field_id, range } => {
            let histogram = stats.histogram.get(field_id).map(|h| h.as_slice());
            Some(
                estimate_ranged_rows(stats.count, histogram, &range.start, &range.end)
                    .estimated_rows,
            )
        }
    }
}

fn clause_priority(candidate: &IndexedClausePlan) -> u8 {
    match candidate {
        IndexedClausePlan::HashedEq { .. } => {
            indexed_clause_priority(true, false, true, false, false)
        }
        IndexedClausePlan::Ranged { range, .. } => indexed_clause_priority(
            false,
            true,
            is_range_equality(range),
            matches!(range.start, ValueRangeTerm::Open),
            matches!(range.end, ValueRangeTerm::Open),
        ),
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

    for clause in selection_conjuncts(selection) {
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
        }
    }

    let mut candidates = Vec::with_capacity(hashed_eq_by_field.len() + range_by_field.len());
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
    NormalizedCandidates {
        candidates,
        impossible: false,
    }
}

fn clause_selectivity_cost(candidate: &IndexedClausePlan) -> u8 {
    match candidate {
        IndexedClausePlan::HashedEq { .. } => 1,
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

fn indexed_clause_candidate(schema: &Schema, clause: &Expr) -> Option<IndexedClausePlan> {
    let (op, field_id, value) = comparison_clause(clause)?;
    let indices = schema.index_fields.get(&field_id)?;

    let supports_hashed = indices.iter().any(|idx| matches!(idx, IndexType::Hashed));
    let supports_ranged = indices.iter().any(|idx| matches!(idx, IndexType::Ranged));

    if supports_hashed && matches!(op, ClauseOp::Eq) {
        return Some(IndexedClausePlan::HashedEq { field_id, value });
    }

    if supports_ranged {
        let range = match op {
            ClauseOp::Eq => ValueRange {
                start: ValueRangeTerm::Inclusive(value.shared().feature()),
                end: ValueRangeTerm::Inclusive(value.shared().feature()),
            },
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
        };
        return Some(IndexedClausePlan::Ranged { field_id, range });
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

fn parse_clause_op(expr: &Expr) -> Option<ClauseOp> {
    if is_symbol_named(expr, "=") {
        Some(ClauseOp::Eq)
    } else if is_symbol_named(expr, ">") {
        Some(ClauseOp::Gt)
    } else if is_symbol_named(expr, ">=") {
        Some(ClauseOp::Ge)
    } else if is_symbol_named(expr, "<") {
        Some(ClauseOp::Lt)
    } else if is_symbol_named(expr, "<=") {
        Some(ClauseOp::Le)
    } else {
        None
    }
}

fn reverse_op(op: ClauseOp) -> ClauseOp {
    match op {
        ClauseOp::Eq => ClauseOp::Eq,
        ClauseOp::Gt => ClauseOp::Lt,
        ClauseOp::Ge => ClauseOp::Le,
        ClauseOp::Lt => ClauseOp::Gt,
        ClauseOp::Le => ClauseOp::Ge,
    }
}

fn expr_field_id(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Symbol(id, _) | Expr::Keyword(id, _) => Some(*id),
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

    use dovahkiin::types::{OwnedValue, Type};

    use crate::ram::schema::{Field, IndexType, Schema};

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

        let plan = build_indexed_predicate_plan(&schema, &selection, Some(&stats)).unwrap();
        match plan.all().first().unwrap() {
            IndexedClausePlan::HashedEq { field_id, .. } => assert_eq!(*field_id, field_b_id),
            _ => panic!("expected hashed clause"),
        }
    }

    fn eq_expr(field: &str, value: u64) -> Expr {
        Expr::List(vec![
            Expr::Symbol(hash_str("="), "=".to_string()),
            Expr::Symbol(hash_str(field), field.to_string()),
            Expr::Value(OwnedValue::U64(value)),
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
