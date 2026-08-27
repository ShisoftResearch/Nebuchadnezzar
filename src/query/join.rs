//! Streaming join operators.
//!
//! Both operators here consume [`RowSource`]s and produce one, so they
//! compose with `DataCursor` and with each other without ever holding an
//! input in full. That is the whole point: the alternative — materialise both
//! sides, then filter pairs — has no memory ceiling tied to the *answer*, only
//! to the *inputs*, and a plan that mis-estimates its inputs then has nothing
//! standing between it and the machine. Measured, before this existed: a join
//! over 69,627 x 139,255 candidate pairs to return 17 rows grew ~865 MB/s and
//! reached 466 GB before it was killed.
//!
//! Neither operator sorts, and neither spills, because neither needs to:
//!
//! - [`IndexNestedLoopJoin`] streams the outer side and probes an index per
//!   row. Memory is one outer row plus the matches for its key — bounded by
//!   the fan-out of a single key, not by either input's size. Requires an
//!   index on the inner join field, which for an equi-join on an indexed
//!   column is the normal case.
//! - [`MergeJoin`] streams two sources already ordered on the join key, which
//!   a ranged-index cursor gives for free. Memory is one duplicate run per
//!   side. Requires ordered inputs and does not verify that — see its docs.
//!
//! A hash join, which is what you reach for when the key is neither indexed
//! nor ordered, is deliberately NOT here: bounding its memory needs partition
//! spill infrastructure that does not exist yet, and adding an unbounded hash
//! build would reintroduce exactly the failure these operators exist to close.

use async_trait::async_trait;
use bifrost::rpc::RPCError;
use dovahkiin::types::{Id, OwnedValue};
use std::collections::VecDeque;

use crate::query::data_client::cursor::DataCursor;
use crate::query::data_client::IndexedDataClient;
use crate::ram::cell::OwnedCell;
use crate::ram::schema::SchemaUid;

/// A source that yields cells one at a time without holding the rest.
///
/// `?Send` deliberately. `DataCursor::next` is not `Send` because the ranged
/// index client holds a `parking_lot` read guard across an await
/// (`index/ranged/client/mod.rs:516`), so requiring `Send` here would exclude
/// the one source that matters. A query runs on one task, so the bound buys
/// nothing it costs. Worth fixing at the source: a lock held across an await
/// also blocks the executor thread and can deadlock.
#[async_trait(?Send)]
pub trait RowSource {
    async fn next_row(&mut self) -> Result<Option<OwnedCell>, RPCError>;
}

#[async_trait(?Send)]
impl RowSource for DataCursor {
    async fn next_row(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        self.next().await
    }
}

/// A source over an in-memory batch. For tests and for small inputs a caller
/// already holds; a real scan should use `DataCursor` so nothing is
/// materialised.
pub struct BatchRowSource {
    rows: VecDeque<OwnedCell>,
}

impl BatchRowSource {
    pub fn new(rows: Vec<OwnedCell>) -> Self {
        Self {
            rows: rows.into(),
        }
    }
}

#[async_trait(?Send)]
impl RowSource for BatchRowSource {
    async fn next_row(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        Ok(self.rows.pop_front())
    }
}

/// One output row: the pair that satisfied the join predicate.
#[derive(Debug, Clone)]
pub struct JoinedRow {
    pub left: OwnedCell,
    pub right: OwnedCell,
}

/// Read the join key out of a cell. A cell missing the field, or holding null
/// there, never joins — SQL's null semantics, and the useful behaviour here
/// too, since a nullable annotation column is exactly where this arises.
fn join_key(cell: &OwnedCell, field: u64) -> Option<OwnedValue> {
    match &cell[field] {
        OwnedValue::Null | OwnedValue::NA => None,
        value => Some(value.clone()),
    }
}

/// Stream the outer side; for each row, probe an index on the inner side.
///
/// Memory is one outer row plus the matches for its key. Cost is one index
/// probe per outer row, so this is the right operator when the outer side is
/// small or selective and the inner join field is indexed — which is the
/// shape almost every graph-pattern equi-join takes.
pub struct IndexNestedLoopJoin<S: RowSource> {
    outer: S,
    client: IndexedDataClient,
    inner_schema: SchemaUid,
    /// Field on the OUTER row supplying the key.
    outer_field: u64,
    /// Indexed field on the INNER schema the key is matched against.
    inner_field: u64,
    /// Matches for the outer row currently being emitted.
    pending: VecDeque<JoinedRow>,
    probes: u64,
}

impl<S: RowSource> IndexNestedLoopJoin<S> {
    pub fn new(
        outer: S,
        client: IndexedDataClient,
        inner_schema: SchemaUid,
        outer_field: u64,
        inner_field: u64,
    ) -> Self {
        Self {
            outer,
            client,
            inner_schema,
            outer_field,
            inner_field,
            pending: VecDeque::new(),
            probes: 0,
        }
    }

    /// Index probes issued so far — one per outer row that had a key.
    pub fn probes(&self) -> u64 {
        self.probes
    }

    async fn fill_from_next_outer(&mut self) -> Result<bool, RPCError> {
        loop {
            let Some(outer) = self.outer.next_row().await? else {
                return Ok(false);
            };
            let Some(key) = join_key(&outer, self.outer_field) else {
                continue; // null key joins nothing; take the next outer row
            };
            self.probes += 1;
            let ids: Vec<Id> = match self
                .client
                .hashed_query(self.inner_schema, self.inner_field, &key)
                .await?
            {
                Ok(ids) => ids,
                // A missing index bucket is an empty match, not a failure.
                Err(_) => Vec::new(),
            };
            if ids.is_empty() {
                continue;
            }
            for inner in self.client.read_cells(&ids).await {
                // Re-check the key: a hash bucket holds collisions as well as
                // matches, so the index narrows but does not decide.
                if join_key(&inner, self.inner_field).as_ref() == Some(&key) {
                    self.pending.push_back(JoinedRow {
                        left: outer.clone(),
                        right: inner,
                    });
                }
            }
            if !self.pending.is_empty() {
                return Ok(true);
            }
        }
    }
}

#[async_trait(?Send)]
impl<S: RowSource> RowSourcePairs for IndexNestedLoopJoin<S> {
    async fn next_pair(&mut self) -> Result<Option<JoinedRow>, RPCError> {
        if self.pending.is_empty() && !self.fill_from_next_outer().await? {
            return Ok(None);
        }
        Ok(self.pending.pop_front())
    }
}

/// A source of joined pairs. `?Send` for the same reason as [`RowSource`].
#[async_trait(?Send)]
pub trait RowSourcePairs {
    async fn next_pair(&mut self) -> Result<Option<JoinedRow>, RPCError>;
}

/// Merge two sources already ordered by the join key.
///
/// Memory is one duplicate run per side: equal keys must be paired with each
/// other, so a run of equal keys on the right is buffered while the matching
/// run on the left is emitted against it. For a unique or near-unique key that
/// is one row.
///
/// **Ordering is the caller's contract and is not verified.** Checking it
/// would cost a comparison per row to catch a caller error; instead
/// `unordered_input_detected` reports after the fact whether a key went
/// backwards, so a test or a debug path can assert on it without the hot path
/// paying for it.
pub struct MergeJoin<L: RowSource, R: RowSource> {
    left: L,
    right: R,
    left_field: u64,
    right_field: u64,
    left_head: Option<OwnedCell>,
    right_head: Option<OwnedCell>,
    /// The current run of equal-keyed right rows, held to pair against every
    /// left row sharing that key.
    right_run: Vec<OwnedCell>,
    right_run_key: Option<OwnedValue>,
    run_pos: usize,
    current_left: Option<OwnedCell>,
    started: bool,
    last_left_key: Option<OwnedValue>,
    unordered: bool,
}

impl<L: RowSource, R: RowSource> MergeJoin<L, R> {
    pub fn new(left: L, right: R, left_field: u64, right_field: u64) -> Self {
        Self {
            left,
            right,
            left_field,
            right_field,
            left_head: None,
            right_head: None,
            right_run: Vec::new(),
            right_run_key: None,
            run_pos: 0,
            current_left: None,
            started: false,
            last_left_key: None,
            unordered: false,
        }
    }

    /// True if a left key was observed going backwards, i.e. the caller's
    /// ordering contract was broken and results may be incomplete.
    pub fn unordered_input_detected(&self) -> bool {
        self.unordered
    }

    async fn advance_left(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        if let Some(cell) = self.left_head.take() {
            return Ok(Some(cell));
        }
        self.left.next_row().await
    }

    async fn advance_right(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        if let Some(cell) = self.right_head.take() {
            return Ok(Some(cell));
        }
        self.right.next_row().await
    }

    /// Collect the run of right rows sharing `key`, leaving the first row past
    /// the run in `right_head`.
    async fn collect_right_run(&mut self, key: &OwnedValue) -> Result<(), RPCError> {
        self.right_run.clear();
        self.run_pos = 0;
        loop {
            let Some(cell) = self.advance_right().await? else {
                break;
            };
            match join_key(&cell, self.right_field) {
                Some(k) if &k == key => self.right_run.push(cell),
                Some(k) if compare_keys(&k, key) == std::cmp::Ordering::Less => {
                    continue; // right lags; skip forward
                }
                // A null key joins nothing, so it is skipped rather than
                // treated as "past the run" -- stashing it in `right_head`
                // ended run collection early and lost every later match.
                None => continue,
                Some(_) => {
                    self.right_head = Some(cell);
                    break;
                }
            }
        }
        self.right_run_key = Some(key.clone());
        Ok(())
    }
}

/// Order join keys. `OwnedValue` is `PartialOrd`; values of different shapes
/// have no ordering between them, and are reported as `Greater` so the merge
/// treats them as "not this run" and moves on rather than pairing nonsense.
fn compare_keys(a: &OwnedValue, b: &OwnedValue) -> std::cmp::Ordering {
    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Greater)
}

#[async_trait(?Send)]
impl<L: RowSource, R: RowSource> RowSourcePairs for MergeJoin<L, R> {
    async fn next_pair(&mut self) -> Result<Option<JoinedRow>, RPCError> {
        loop {
            // Still emitting the current left row against the buffered run.
            if let Some(left) = self.current_left.clone() {
                if self.run_pos < self.right_run.len() {
                    let right = self.right_run[self.run_pos].clone();
                    self.run_pos += 1;
                    return Ok(Some(JoinedRow { left, right }));
                }
                self.current_left = None;
            }

            let Some(left) = self.advance_left().await? else {
                return Ok(None);
            };
            let Some(key) = join_key(&left, self.left_field) else {
                continue; // null key joins nothing
            };

            if let Some(previous) = &self.last_left_key {
                if compare_keys(&key, previous) == std::cmp::Ordering::Less {
                    self.unordered = true;
                }
            }
            self.last_left_key = Some(key.clone());
            self.started = true;

            // Reuse the buffered run when the key repeats; otherwise collect
            // the next one.
            if self.right_run_key.as_ref() != Some(&key) {
                self.collect_right_run(&key).await?;
            }
            self.run_pos = 0;
            if self.right_run.is_empty() {
                continue;
            }
            self.current_left = Some(left);
        }
    }
}

/// What a join's inputs and schema support, as the planner knows it.
///
/// Explicit rather than introspected: the data client has no schema handle,
/// and a planner asking for a join already knows both of these. Passing them
/// keeps the choice testable and keeps the operator layer free of a schema
/// dependency it would otherwise need only for this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinInputs {
    /// The inner side's join field carries an index that can be probed.
    pub inner_field_indexed: bool,
    /// Both inputs arrive ordered by the join key -- true for ranged-index
    /// cursors on the join field, false for anything else.
    pub inputs_ordered_on_key: bool,
}

/// How a join should be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Stream the outer side, probe the inner index per row. Bounded by the
    /// fan-out of one key.
    IndexNestedLoop,
    /// Merge two key-ordered streams. Bounded by one duplicate run.
    Merge,
    /// Neither is possible: no index to probe and no usable order. The caller
    /// must materialise, which has NO memory bound tied to the answer -- this
    /// is the case a hash join with partition spill would take over, and the
    /// reason a row budget exists in the meantime.
    Materialize,
}

impl JoinStrategy {
    /// Whether this strategy's memory is bounded by the join's fan-out rather
    /// than by its inputs.
    pub fn is_streaming(&self) -> bool {
        !matches!(self, JoinStrategy::Materialize)
    }
}

/// Choose a join strategy.
///
/// Index-nested-loop is preferred over merge when both are available: it
/// touches only the inner rows a key actually matches, whereas a merge walks
/// both inputs end to end even when the answer is tiny. That ordering matters
/// for exactly the shape that motivated this module -- a highly selective
/// outer side against a large inner one.
pub fn plan_join(inputs: JoinInputs) -> JoinStrategy {
    if inputs.inner_field_indexed {
        JoinStrategy::IndexNestedLoop
    } else if inputs.inputs_ordered_on_key {
        JoinStrategy::Merge
    } else {
        JoinStrategy::Materialize
    }
}

/// Drain a pair source. Convenience for callers that genuinely want every
/// result — it materialises, so it is for tests and small answers.
pub async fn collect_pairs<J: RowSourcePairs>(
    join: &mut J,
) -> Result<Vec<JoinedRow>, RPCError> {
    let mut out = Vec::new();
    while let Some(pair) = join.next_pair().await? {
        out.push(pair);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::SchemaVid;
    use crate::ram::types::*;

    fn cell(key_field: u64, value: OwnedValue) -> OwnedCell {
        let mut map = OwnedMap::new();
        map.insert_key_id(key_field, value);
        OwnedCell::new_with_id(SchemaVid(1), &Id::rand(), OwnedValue::Map(map))
    }

    fn null_cell(key_field: u64) -> OwnedCell {
        cell(key_field, OwnedValue::Null)
    }

    const K: u64 = 7;

    fn keys_of(pairs: &[JoinedRow]) -> Vec<(i64, i64)> {
        pairs
            .iter()
            .map(|p| {
                let l = match &p.left[K] { OwnedValue::I64(v) => *v, o => panic!("{:?}", o) };
                let r = match &p.right[K] { OwnedValue::I64(v) => *v, o => panic!("{:?}", o) };
                (l, r)
            })
            .collect()
    }

    fn src(vals: &[i64]) -> BatchRowSource {
        BatchRowSource::new(vals.iter().map(|v| cell(K, OwnedValue::I64(*v))).collect())
    }

    #[test]
    fn strategy_prefers_index_probe_over_merge() {
        // Both available: the probe touches only matching inner rows, the
        // merge walks both inputs whole.
        assert_eq!(
            plan_join(JoinInputs { inner_field_indexed: true, inputs_ordered_on_key: true }),
            JoinStrategy::IndexNestedLoop
        );
    }

    #[test]
    fn strategy_falls_back_to_merge_then_materialize() {
        assert_eq!(
            plan_join(JoinInputs { inner_field_indexed: false, inputs_ordered_on_key: true }),
            JoinStrategy::Merge
        );
        let last = plan_join(JoinInputs { inner_field_indexed: false, inputs_ordered_on_key: false });
        assert_eq!(last, JoinStrategy::Materialize);
        assert!(!last.is_streaming(), "materialise must not claim to stream");
    }

    #[tokio::test]
    async fn merge_join_pairs_equal_keys() {
        let mut j = MergeJoin::new(src(&[1, 2, 3]), src(&[2, 3, 4]), K, K);
        let pairs = collect_pairs(&mut j).await.unwrap();
        assert_eq!(keys_of(&pairs), vec![(2, 2), (3, 3)]);
        assert!(!j.unordered_input_detected());
    }

    #[tokio::test]
    async fn merge_join_emits_full_run_cross_product() {
        // Two left rows and three right rows sharing a key must produce all
        // six pairings -- the case a naive "advance both" merge gets wrong.
        let mut j = MergeJoin::new(src(&[5, 5]), src(&[5, 5, 5]), K, K);
        let pairs = collect_pairs(&mut j).await.unwrap();
        assert_eq!(pairs.len(), 6, "2x3 run must yield 6 pairs");
    }

    #[tokio::test]
    async fn merge_join_skips_non_matching_and_terminates() {
        let mut j = MergeJoin::new(src(&[1, 5, 9]), src(&[2, 6, 10]), K, K);
        assert!(collect_pairs(&mut j).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn merge_join_null_keys_never_join() {
        // Null on both sides must not pair with itself.
        let left = BatchRowSource::new(vec![null_cell(K), cell(K, OwnedValue::I64(1))]);
        let right = BatchRowSource::new(vec![null_cell(K), cell(K, OwnedValue::I64(1))]);
        let mut j = MergeJoin::new(left, right, K, K);
        let pairs = collect_pairs(&mut j).await.unwrap();
        assert_eq!(keys_of(&pairs), vec![(1, 1)]);
    }

    #[tokio::test]
    async fn merge_join_reports_unordered_input() {
        // The contract is not enforced, but a violation must be observable
        // rather than silently returning a short answer.
        let mut j = MergeJoin::new(src(&[3, 1]), src(&[1, 3]), K, K);
        let _ = collect_pairs(&mut j).await.unwrap();
        assert!(j.unordered_input_detected());
    }

    #[tokio::test]
    async fn merge_join_empty_side_terminates() {
        let mut j = MergeJoin::new(src(&[1, 2]), src(&[]), K, K);
        assert!(collect_pairs(&mut j).await.unwrap().is_empty());
        let mut j = MergeJoin::new(src(&[]), src(&[1, 2]), K, K);
        assert!(collect_pairs(&mut j).await.unwrap().is_empty());
    }

    /// The property the whole module exists for: a source that would blow up
    /// if drained is never drained. Pulling one pair must touch only as much
    /// of the input as that pair needs.
    #[tokio::test]
    async fn merge_join_is_lazy_and_does_not_drain_inputs() {
        struct Counting { inner: BatchRowSource, pulled: std::rc::Rc<std::cell::Cell<usize>> }
        #[async_trait(?Send)]
        impl RowSource for Counting {
            async fn next_row(&mut self) -> Result<Option<OwnedCell>, RPCError> {
                let r = self.inner.next_row().await?;
                if r.is_some() { self.pulled.set(self.pulled.get() + 1); }
                Ok(r)
            }
        }
        let lc = std::rc::Rc::new(std::cell::Cell::new(0));
        let rc = std::rc::Rc::new(std::cell::Cell::new(0));
        let big: Vec<i64> = (0..10_000).collect();
        let left = Counting { inner: BatchRowSource::new(big.iter().map(|v| cell(K, OwnedValue::I64(*v))).collect()), pulled: lc.clone() };
        let right = Counting { inner: BatchRowSource::new(big.iter().map(|v| cell(K, OwnedValue::I64(*v))).collect()), pulled: rc.clone() };
        let mut j = MergeJoin::new(left, right, K, K);

        let first = j.next_pair().await.unwrap().expect("one pair");
        assert_eq!(keys_of(&[first]), vec![(0, 0)]);
        // A materialising join would have pulled all 10,000 from each side.
        assert!(lc.get() <= 2, "left over-pulled: {}", lc.get());
        assert!(rc.get() <= 3, "right over-pulled: {}", rc.get());
    }
}
