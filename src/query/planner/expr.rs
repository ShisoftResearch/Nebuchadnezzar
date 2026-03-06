use bifrost_hasher::hash_str;
use dovahkiin::{expr::serde::Expr, types::OwnedValue};

use crate::{
    query::cost::planner::indexed_clause_priority,
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
}

impl IndexedPredicatePlan {
    pub(crate) fn new(candidates: Vec<IndexedClausePlan>) -> Self {
        Self { candidates }
    }

    pub(crate) fn all(&self) -> &[IndexedClausePlan] {
        self.candidates.as_slice()
    }
}

pub(crate) fn build_indexed_predicate_plan(
    schema: &Schema,
    selection: &Expr,
) -> Option<IndexedPredicatePlan> {
    let mut candidates = indexed_clause_candidates(schema, selection);
    if candidates.is_empty() {
        return None;
    }
    candidates.sort_by_key(|candidate| std::cmp::Reverse(clause_priority(candidate)));
    Some(IndexedPredicatePlan::new(candidates))
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

fn indexed_clause_candidates(schema: &Schema, selection: &Expr) -> Vec<IndexedClausePlan> {
    selection_conjuncts(selection)
        .iter()
        .filter_map(|clause| indexed_clause_candidate(schema, clause))
        .collect()
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
