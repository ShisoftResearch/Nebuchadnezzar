# Benchmarks for Nebuchadnezzar Full-Text Search

This directory contains benchmarks for the hybrid inverted indexer's indexing and search performance.

## Running Benchmarks

To run all benchmarks:

```bash
cargo bench --bench inverted_index
```

To run a specific benchmark:

```bash
cargo bench --bench inverted_index -- indexing
cargo bench --bench inverted_index -- search
cargo bench --bench inverted_index -- concurrent_indexing
cargo bench --bench inverted_index -- search_limit
```

## Benchmark Suites

### 1. `bench_indexing`
Measures indexing performance with varying document counts (10, 100, 1000, 5000 documents).

**What it measures:**
- Time to index documents of varying sizes
- Throughput (documents/second)

### 2. `bench_search`
Measures search performance with varying document counts and query types.

**What it measures:**
- Query latency for different query patterns:
  - Single word queries
  - Multi-word queries
  - Phrase queries
  - Common word queries
- Performance scaling with document count

### 3. `bench_concurrent_indexing`
Measures concurrent indexing performance.

**What it measures:**
- Throughput when indexing multiple documents concurrently
- Thread safety and contention handling

### 4. `bench_search_limit`
Measures search performance with varying result limits (1, 10, 50, 100 results).

**What it measures:**
- Impact of result limit on query performance
- Top-K retrieval efficiency

## Benchmark Results

Results are saved to `target/criterion/` directory with HTML reports. Open `target/criterion/inverted_index/index.html` to view detailed results.

## Sample Output

```
indexing/10              time:   [1.2345 ms 1.3456 ms 1.4567 ms]
indexing/100             time:   [12.345 ms 13.456 ms 14.567 ms]
indexing/1000            time:   [123.45 ms 134.56 ms 145.67 ms]
indexing/5000            time:   [1.2345 s 1.3456 s 1.4567 s]

search/10_docs/single_word    time:   [123.45 us 134.56 us 145.67 us]
search/100_docs/single_word   time:   [234.56 us 245.67 us 256.78 us]
search/1000_docs/single_word  time:   [345.67 us 356.78 us 367.89 us]
```

## Notes

- Benchmarks use realistic text samples from various domains
- Each benchmark creates its own isolated test environment
- Benchmarks use temporary directories that are cleaned up automatically
- For accurate results, run benchmarks in release mode: `cargo bench --release`

## OCC transaction benchmarks

Run the local MVCC smoke/correctness portfolio with:

```bash
cargo bench --bench occ_transactions -- --test
NEB_OCC_BENCH_LABEL=mvcc-smoke \
  cargo bench --bench occ_transactions -- --sample-size 10
```

Save a baseline with:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=occ-initial \
  cargo bench --bench occ_transactions -- --save-baseline occ-initial
```

`NEB_OCC_BENCH_BASE_PORT` moves the loopback port range used by the fixtures. The
transaction report JSON is written under `target/occ-bench/`, while Criterion's
HTML reports and baselines are written under `target/criterion/`.

The MVCC portfolio is `mvcc/non_transactional_read`,
`mvcc/non_transactional_update`, `mvcc/read_only_current`, `mvcc/rmw_one_cell`,
`mvcc/rmw_multi_cell`, `mvcc/multi_participant`, `mvcc/blind_update`,
`mvcc/blind_remove`, `mvcc/full_read`, `mvcc/selected_read`, `mvcc/head_read`,
`mvcc/partial_read`, `mvcc/history_depth_1`, `mvcc/history_depth_8`,
`mvcc/history_depth_32`, `mvcc/hot_cell_old_snapshot`,
`mvcc/history_expiration`, `mvcc/cleaner_retained_revisions`,
`mvcc/cleaner_reader_contention`, and `mvcc/hlc_contention`.

Each JSON scenario contains `committed`, `attempts`, `not_realizable`,
`logical_retries`, `waits`, `commits_per_second`, `p50_ns`, `p95_ns`, `p99_ns`,
`unexpected`, `invariants_passed`, `retained_revisions`, `retained_bytes`, and
`segment_count`. Retention is part of the workload configuration: the
`mvcc_direct_updates` fixture used by the direct non-transactional scenarios
sets `history_retention_ms=1`; the history-expiration fixture uses `50`; both
cleaner fixtures use `2,000`; and the normal history fixture uses `300,000`.
The cleaner cases rebuild real fragmentation outside the measured interval for
every counted operation. They use a dedicated ten-sample flat configuration
whose one-nanosecond warmup and measurement targets clamp Criterion to one real
operation per sample (plus one real warmup operation); the returned duration is
the observed cleaner-pass time and is never synthesized or multiplied.

Accept-grade comparisons run only on the idle primary host `192.168.10.87`, with
separate baseline and candidate worktrees. Nebuchadnezzar `a82ccd46` predates
the Task 12 benchmark schema, so its native tree cannot supply a like-for-like
20-scenario report. The baseline must pair `a82ccd46` with Bifrost `b078ce7`
and an explicitly identified benchmark-only comparable-harness backport.
Record the backport's full commit ID with the reports; a branch name or an
uncommitted patch is not sufficient provenance.

The comparable harness contains exactly these 13 non-historical scenarios:
`mvcc/non_transactional_read`, `mvcc/non_transactional_update`,
`mvcc/read_only_current`, `mvcc/rmw_one_cell`, `mvcc/rmw_multi_cell`,
`mvcc/multi_participant`, `mvcc/blind_update`, `mvcc/blind_remove`,
`mvcc/full_read`, `mvcc/selected_read`, `mvcc/head_read`,
`mvcc/partial_read`, and `mvcc/hlc_contention`. Build that harness only from
APIs shared by the two revisions, apply the same harness patch to a separate
candidate-comparison worktree, and keep these files byte-identical on both
sides:

```bash
for path in \
  benches/occ_transactions.rs \
  benches/occ_support/mod.rs \
  benches/occ_support/fixture.rs \
  benches/occ_support/metrics.rs \
  benches/occ_support/workloads.rs; do
  cmp "$BASELINE_NEB_TREE/$path" "$CANDIDATE_COMPARE_TREE/$path"
done

BASELINE_HARNESS_SHA="$(git -C "$BASELINE_NEB_TREE" rev-parse HEAD)"
git -C "$BASELINE_NEB_TREE" merge-base --is-ancestor \
  a82ccd46 "$BASELINE_HARNESS_SHA"
git -C "$BASELINE_NEB_TREE" diff --name-only \
  a82ccd46 "$BASELINE_HARNESS_SHA"
test "$(git -C "$BASELINE_BIFROST_TREE" rev-parse HEAD)" = \
  "$(git -C "$BASELINE_BIFROST_TREE" rev-parse b078ce7)"
```

Inspect the printed backport paths before running: they may contain benchmark
harness/configuration files only, never production implementation changes.
Use the same release flags, sample settings, NUMA binding, port plan, dataset,
and exact filter for both sides. For each worktree, capture runs 1 through 3
with this filter (changing only the label and recorded product/harness
revisions):

```bash
COMPARABLE_FILTER='^mvcc/(non_transactional_read|non_transactional_update|read_only_current|rmw_one_cell|rmw_multi_cell|multi_participant|blind_update|blind_remove|full_read|selected_read|head_read|partial_read|hlc_contention)$'
numactl --cpunodebind=0 --membind=0 env \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL=develop-1 \
  NEB_OCC_BENCH_REVISION="a82ccd46+harness-$BASELINE_HARNESS_SHA" \
  cargo bench --bench occ_transactions -- "$COMPARABLE_FILTER"
```

The remote six-report gate compares the three baseline and three candidate
reports from that 13-scenario comparable harness:

```bash
scripts/compare-mvcc-benchmarks.sh \
  target/occ-bench/develop-1.json \
  target/occ-bench/develop-2.json \
  target/occ-bench/develop-3.json \
  -- \
  target/occ-bench/mvcc-1.json \
  target/occ-bench/mvcc-2.json \
  target/occ-bench/mvcc-3.json
```

The seven historical scenarios—`mvcc/history_depth_1`,
`mvcc/history_depth_8`, `mvcc/history_depth_32`,
`mvcc/hot_cell_old_snapshot`, `mvcc/history_expiration`,
`mvcc/cleaner_retained_revisions`, and `mvcc/cleaner_reader_contention`—have
no honest pre-MVCC baseline. Do not copy candidate measurements, insert
synthetic values, or describe them as like-for-like. Separately, the native
candidate Task 12 worktree must run the full 20-scenario portfolio:

```bash
cargo bench --bench occ_transactions -- --test
NEB_OCC_BENCH_BASE_PORT=39400 \
NEB_OCC_BENCH_LABEL=mvcc-full-20 \
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
  cargo bench --bench occ_transactions -- '^mvcc/'
python3 -c 'import json,sys; s=json.load(open(sys.argv[1]))["scenarios"]; assert len(s)==20; assert all(v["invariants_passed"] is True and v["unexpected"] == [] for v in s.values())' \
  target/occ-bench/mvcc-full-20.json
```

That full candidate report is required coverage evidence, not an input to the
13-scenario six-report comparison. The comparator strictly rejects non-standard
JSON `NaN`, `Infinity`, and `-Infinity`, non-finite throughput or p99,
non-positive throughput, negative p99, mismatched scenario names, or unexpected
outcomes. It requires throughput CV below 5% on both sides and fails
non-historical scenarios whose median throughput declines by over 5% or median
p99 rises by over 5%.

Accurate performance work should run on the dedicated idle `192.168.10.87` host
with identical release flags, NUMA binding, port range, and dataset for baseline
and candidate; remote execution is intentionally not hardcoded in Rust.

### OCC phase profiling

`occ_phase_profile` is a non-default diagnostic feature. Its cfg gates compile
the phase clocks and counter updates out of default builds. Independently,
`scripts/check-occ-phase-profile-default.sh` builds the default release library,
resolves the exact emitted object from Cargo's JSON artifact record, and checks
the completed `nm` output for `phase_profile`-named symbols. This is a
symbol-level regression guard, not a semantic proof that no counter update code
exists.

The check requires a Unix-like environment with Bash, `awk`, `grep`, `mktemp`,
and `nm`. Set `NM` to the path of a compatible symbol dumper when `nm` is not
the desired binary.

Collect profiling data with:

```bash
NEB_OCC_BENCH_LABEL=phase-profile \
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
  cargo bench --features occ_phase_profile --bench occ_transactions -- \
  'occ/independent_rmw/1$'
```

Each phase reports `total_ns`, `invocation_count`, `ns_per_invocation`, and
`ns_per_commit`.

Coordinator barriers include participant RPC work, while participant timings are
nested diagnostics. Never sum participant time into coordinator time. Summed
phase time may exceed wall-clock time under concurrency and is not a
percentage.

If a snapshot is requested while phase guards are still active, the snapshot is
invalid and the code panics instead of publishing partial data. Treat snapshot
batches as quiescent or otherwise serialize them.

The controlled profiling portfolio is exactly `independent_rmw/1`,
`hot_rmw/8`, `hot_rmw/32`, `multi_cell/8`, `multi_participant/1`, and
`multi_participant/4`, run serially on `192.168.10.87` with NUMA node 0
binding.

Generated profiling artifacts stay under `target/` and are not committed.
