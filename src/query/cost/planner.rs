use std::collections::HashSet;

use crate::{query::planner::ValueRangeTerm, query::statistics::SchemaStatistics};

#[derive(Debug, Clone, Copy)]
pub struct ClauseEstimate {
    pub estimated_rows: usize,
    pub confidence: f64,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub struct PlanCost {
    pub startup_cost: f64,
    pub per_row_cost: f64,
    pub estimated_rows: usize,
    pub total_cost: f64,
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

    // Equi-depth histogram: every bucket holds ~row_count/denom rows, so the
    // fraction of buckets a range spans is the fraction of rows it matches.
    //
    // The bounds must be the FULL RUN of equal keys, not the first match. A
    // heavy value repeats across many consecutive bucket boundaries -- a
    // label held by 56% of rows occupies ~56 of 100 keys -- and
    // `binary_search` lands on an arbitrary one of them, so an equality on it
    // measured one bucket instead of fifty-six: a 56x underestimate on
    // exactly the values a planner most needs to know are heavy.
    let denom = (histogram.len() - 1).max(1);
    let left = range_lower_bound(start, histogram);
    let right = range_upper_bound(end, histogram);
    // A value that falls between keys spans no bucket boundary but still
    // lies inside one bucket; a value past every key matches nothing.
    let width = if right > left {
        right - left
    } else if left >= histogram.len() {
        0
    } else {
        1
    };
    let ratio = (width as f64 / denom as f64).clamp(0.0, 1.0);
    ClauseEstimate {
        estimated_rows: ((row_count as f64 * ratio).ceil() as usize).max(1),
        confidence: 0.75,
        reason: "histogram",
    }
}

/// First key not less than the range start (or 0 for an open start).
fn range_lower_bound(term: &ValueRangeTerm, histogram: &[[u8; 8]]) -> usize {
    match term {
        ValueRangeTerm::Open => 0,
        ValueRangeTerm::Inclusive(val) => histogram.partition_point(|k| k < val),
        ValueRangeTerm::Exclusive(val) => histogram.partition_point(|k| k <= val),
    }
}

/// One past the last key not greater than the range end (or the last key
/// for an open end).
fn range_upper_bound(term: &ValueRangeTerm, histogram: &[[u8; 8]]) -> usize {
    match term {
        ValueRangeTerm::Open => histogram.len() - 1,
        ValueRangeTerm::Inclusive(val) => histogram.partition_point(|k| k <= val),
        ValueRangeTerm::Exclusive(val) => histogram.partition_point(|k| k < val),
    }
}

pub fn distinct_estimate_from_stats(stats: &SchemaStatistics, field_id: u64) -> Option<usize> {
    let histogram = stats.histogram.get(&field_id)?;
    let distinct = histogram.iter().copied().collect::<HashSet<_>>().len();
    Some(distinct.max(1))
}

pub fn estimate_clause_plan_cost(
    supports_hashed: bool,
    supports_ranged: bool,
    open_start: bool,
    open_end: bool,
    estimated_rows: usize,
    limit: Option<usize>,
    order_aligned: bool,
) -> PlanCost {
    let mut startup_cost = if supports_hashed {
        1.0
    } else if supports_ranged {
        2.0
    } else {
        4.0
    };
    let mut per_row_cost = if supports_hashed {
        1.0
    } else if supports_ranged {
        if open_start || open_end {
            1.8
        } else {
            1.2
        }
    } else {
        2.5
    };

    if order_aligned {
        startup_cost *= 0.25;
        per_row_cost *= 0.35;
    }

    let effective_rows = limit
        .map(|l| estimated_rows.min(l.max(1)))
        .unwrap_or(estimated_rows);
    let total_cost = startup_cost + (effective_rows as f64 * per_row_cost);

    PlanCost {
        startup_cost,
        per_row_cost,
        estimated_rows,
        total_cost,
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

#[cfg(test)]
mod estimate_tests {
    use super::*;

    fn k(v: u64) -> [u8; 8] {
        v.to_be_bytes()
    }

    /// An equi-depth histogram over a label column where value 6 holds 56 of
    /// 100 buckets -- the shape of `super_class_id` on the connectome, where
    /// `optic` is 56% of neurons.
    fn skewed() -> Vec<[u8; 8]> {
        let mut h = Vec::new();
        h.extend(std::iter::repeat(k(1)).take(1));
        h.extend(std::iter::repeat(k(2)).take(23));
        h.extend(std::iter::repeat(k(6)).take(56));
        h.extend(std::iter::repeat(k(7)).take(12));
        h.extend(std::iter::repeat(k(10)).take(9));
        assert_eq!(h.len(), 101);
        h
    }

    fn eq(rows: usize, h: &[[u8; 8]], v: u64) -> usize {
        estimate_ranged_rows(
            rows,
            Some(h),
            &ValueRangeTerm::Inclusive(k(v)),
            &ValueRangeTerm::Inclusive(k(v)),
        )
        .estimated_rows
    }

    #[test]
    fn equality_on_a_heavy_value_spans_its_whole_run() {
        // 56 of 100 buckets are value 6, so ~56% of rows. The old
        // first-match position measured ONE bucket here.
        let est = eq(139_255, &skewed(), 6);
        assert!(
            (70_000..=85_000).contains(&est),
            "expected ~56% of 139,255, got {est}"
        );
    }

    #[test]
    fn equality_on_a_rare_value_is_bounded_by_one_bucket() {
        // Value 1 holds one key: at most one bucket's worth of rows, never
        // the 50% fallback.
        let est = eq(139_255, &skewed(), 1);
        assert!(est <= 139_255 / 100 + 1, "got {est}");
        assert!(est >= 1);
    }

    #[test]
    fn equality_on_an_absent_value_between_keys_costs_one_bucket() {
        // 4 is not in the histogram but lies inside the run between 2 and 6:
        // it could be in that bucket, so charge one bucket, not zero and not
        // half the table.
        let est = eq(139_255, &skewed(), 4);
        assert!(est <= 139_255 / 100 + 1, "got {est}");
    }

    #[test]
    fn equality_past_every_key_matches_nothing() {
        let est = eq(139_255, &skewed(), 99);
        assert_eq!(est, 1, "floor is 1 row, not a bucket");
    }

    #[test]
    fn open_range_covers_the_table() {
        let est = estimate_ranged_rows(
            139_255,
            Some(&skewed()),
            &ValueRangeTerm::Open,
            &ValueRangeTerm::Open,
        )
        .estimated_rows;
        assert_eq!(est, 139_255);
    }

    #[test]
    fn missing_histogram_is_named_not_hidden() {
        let e = estimate_ranged_rows(
            1000,
            None,
            &ValueRangeTerm::Inclusive(k(1)),
            &ValueRangeTerm::Inclusive(k(1)),
        );
        assert_eq!(e.reason, "histogram-missing");
        assert_eq!(e.estimated_rows, 500);
    }
}
