# Task 12 Baseline Cleanup Benchmark Isolation

## Context

The first accept-grade `develop-1` run on `192.168.10.87` stopped in
`mvcc/multi_participant` with
`DMCommitError(CheckFailed(CannotEnd))`. The failing transaction performed no
storage mutation. The baseline `cell_meta_cleanup` implementation can check an
unowned `CellMeta`, release its lock, and then remove the map entry after a new
transaction has acquired that same `Arc`. Commit subsequently resolves a fresh
unowned entry and rejects the orphaned prepared transaction. The MVCC branch
fixes this race in Task 10, but changing the baseline product would invalidate
an exact develop-versus-feature comparison.

## Decision

The byte-identical comparison harness will keep one untimed read-only sentinel
transaction open on the three-server cluster fixture while Criterion runs.
Before the benchmark group starts, the harness will:

1. choose and transactionally seed one dedicated sentinel cell per participant;
2. begin one read-only transaction;
3. issue a selected `score` read for every sentinel cell; and
4. retain the transaction ID until the benchmark group finishes.

On the baseline, selected reads register participant transaction state and
anchor `txns_sorted` below all later hot-cell metadata. Cleanup therefore does
not consider the benchmark cells stale. The candidate receives the identical
public transaction calls, but its product-level cleanup fix remains active.
After Criterion finishes, the harness aborts the sentinel transaction before
shutting down the cluster fixture.

## Constraints

- The baseline remains product commit
  `a82ccd46fa6c63ddaf0cd921fc1a09ea33dec539` with Bifrost
  `b078ce7ae4ec0808b76eb13ab14c6966f6147688`.
- The candidate remains product commit
  `97f957e28925d3b0235049aec25237511bf85540` with Bifrost
  `0a53f1951d6f0d216364f87265620bb9d47ab85c`.
- The five comparison harness files remain byte-identical.
- Sentinel seed, read, retention, and abort are outside returned benchmark
  elapsed time.
- Sentinel IDs are distinct from every measured workload ID.
- All 13 nonhistorical scenarios remain present.
- The measured multi-participant transaction still executes begin, reads,
  updates, distributed prepare/commit, and distributed end.
- No product source, distributed phase, invariant, retry rule, or comparator
  threshold changes.

## Verification

The failed remote run is the behavioral RED case. A structural RED/GREEN check
will additionally require dedicated sentinel IDs, setup before
`group.bench_function`, and abort after `group.finish`.

Both baseline and candidate must then pass:

1. scoped rustfmt;
2. local `cargo bench --bench occ_transactions -- --test`;
3. strict exact-13 JSON validation;
4. byte-identity and clean-scope checks; and
5. an accept-grade filtered `mvcc/multi_participant` run on
   `192.168.10.87`.

Only after those gates pass may the three baseline and three candidate
accept-grade portfolio runs restart.
