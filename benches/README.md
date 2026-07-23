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

Run a local smoke pass with:

```bash
NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
NEB_OCC_BENCH_LABEL=smoke \
  cargo bench --bench occ_transactions -- --test
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

Each scenario reports attempts, commits, `NotRealizable` outcomes, logical
retries, p50/p95/p99 latency, and unexpected errors/invariant failures. A change
is accepted when targeted stable throughput or p95 improves by at least 5%, the
geometric-mean/aggregate throughput does not decline, secondary throughput is no
worse than 3%, secondary p95 is no worse than 5%, unexpected errors remain zero,
and all correctness suites pass.

Accurate performance work should run on a dedicated idle host. This project's
controlled loop uses `192.168.10.17` with identical NUMA binding for every
baseline and candidate; remote execution is intentionally not hardcoded in Rust.

### OCC phase profiling

`occ_phase_profile` is a non-default diagnostic feature. Default builds contain
no phase clocks or counter updates. Run
`scripts/check-occ-phase-profile-default.sh` to build the default release
library object under `target/release/deps/` and confirm that no
`phase_profile` symbols are present.

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
`multi_participant/4`, run serially on `192.168.10.17` with NUMA node 0
binding.

Generated profiling artifacts stay under `target/` and are not committed.
