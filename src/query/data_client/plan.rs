use bifrost_hasher::hash_str;
use dovahkiin::{expr::serde::Expr, types::OwnedValue};

use crate::ram::schema::{IndexType, Schema};

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
pub(super) enum IndexedClausePlan {
    HashedEq { field_id: u64, value: OwnedValue },
    Ranged { field_id: u64, range: ValueRange },
}

pub(super) struct IndexedPredicatePlan {
    candidates: Vec<IndexedClausePlan>,
}

impl IndexedPredicatePlan {
    pub(super) fn new(candidates: Vec<IndexedClausePlan>) -> Self {
        Self { candidates }
    }

    pub(super) fn chosen(&self) -> Option<&IndexedClausePlan> {
        self.candidates
            .iter()
            .find(|candidate| matches!(candidate, IndexedClausePlan::HashedEq { .. }))
            .or_else(|| self.candidates.first())
    }
}

pub(super) fn indexed_clause_candidates(
    schema: &Schema,
    selection: &Expr,
) -> Vec<IndexedClausePlan> {
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
    if matches!(op, ClauseOp::Eq) && indices.iter().any(|idx| matches!(idx, IndexType::Hashed)) {
        return Some(IndexedClausePlan::HashedEq { field_id, value });
    }
    if indices.iter().any(|idx| matches!(idx, IndexType::Ranged)) {
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
