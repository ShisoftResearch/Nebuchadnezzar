use std::collections::HashSet;

use crate::{query::planner::ValueRangeTerm, query::statistics::SchemaStatistics};

#[derive(Debug, Clone, Copy)]
pub struct ClauseEstimate {
    pub estimated_rows: usize,
    pub confidence: f64,
    pub reason: &'static str,
}

pub fn estimate_hashed_eq_rows(
    row_count: usize,
    distinct_estimate: Option<usize>,
) -> ClauseEstimate {
    if row_count == 0 {
        return ClauseEstimate {
            estimated_rows: 0,
            confidence: 1.0,
            reason: "empty-schema",
        };
    }

    let distinct = distinct_estimate.unwrap_or(64).max(1);
    let rows = (row_count / distinct).max(1);
    ClauseEstimate {
        estimated_rows: rows,
        confidence: if distinct_estimate.is_some() {
            0.7
        } else {
            0.4
        },
        reason: if distinct_estimate.is_some() {
            "distinct-estimate"
        } else {
            "distinct-fallback"
        },
    }
}

pub fn estimate_ranged_rows(
    row_count: usize,
    histogram: Option<&[[u8; 8]]>,
    start: &ValueRangeTerm,
    end: &ValueRangeTerm,
) -> ClauseEstimate {
    if row_count == 0 {
        return ClauseEstimate {
            estimated_rows: 0,
            confidence: 1.0,
            reason: "empty-schema",
        };
    }

    let Some(histogram) = histogram else {
        return ClauseEstimate {
            estimated_rows: (row_count / 2).max(1),
            confidence: 0.3,
            reason: "histogram-missing",
        };
    };
    if histogram.len() <= 1 {
        return ClauseEstimate {
            estimated_rows: row_count,
            confidence: 0.2,
            reason: "histogram-too-small",
        };
    }

    let start_idx = range_term_pos(start, histogram, true).min(histogram.len() - 1);
    let end_idx = range_term_pos(end, histogram, false).min(histogram.len() - 1);
    let (left, right) = if start_idx <= end_idx {
        (start_idx, end_idx)
    } else {
        (end_idx, start_idx)
    };
    let width = right.saturating_sub(left).max(1);
    let denom = (histogram.len() - 1).max(1);
    let ratio = (width as f64 / denom as f64).clamp(0.0, 1.0);
    ClauseEstimate {
        estimated_rows: ((row_count as f64 * ratio).ceil() as usize).max(1),
        confidence: 0.75,
        reason: "histogram",
    }
}

pub fn distinct_estimate_from_stats(stats: &SchemaStatistics, field_id: u64) -> Option<usize> {
    let histogram = stats.histogram.get(&field_id)?;
    let distinct = histogram.iter().copied().collect::<HashSet<_>>().len();
    Some(distinct.max(1))
}

fn range_term_pos(term: &ValueRangeTerm, histogram: &[[u8; 8]], start_side: bool) -> usize {
    match term {
        ValueRangeTerm::Open => {
            if start_side {
                0
            } else {
                histogram.len() - 1
            }
        }
        ValueRangeTerm::Inclusive(val) | ValueRangeTerm::Exclusive(val) => {
            histogram.binary_search(val).unwrap_or_else(|pos| pos)
        }
    }
}

pub fn indexed_clause_priority(
    supports_hashed: bool,
    supports_ranged: bool,
    is_equality: bool,
    start_open: bool,
    end_open: bool,
) -> u8 {
    if supports_hashed && is_equality {
        return 100;
    }
    if supports_ranged {
        if is_equality {
            return 90;
        }
        if !start_open && !end_open {
            return 70;
        }
        if !start_open || !end_open {
            return 50;
        }
        return 30;
    }
    10
}
