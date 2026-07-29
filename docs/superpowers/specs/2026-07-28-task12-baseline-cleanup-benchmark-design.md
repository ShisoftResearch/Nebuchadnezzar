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

The first amendment protected only the three-server cluster fixture. Its
filtered baseline and candidate `mvcc/multi_participant` gates passed, but the
next full baseline run stopped with the same fail-safe error in
`mvcc/rmw_multi_cell`. That workload uses the separate single-server
transaction fixture. The second failure confirms that the isolation must cover
each fixture that executes transactions, not only the fixture where the race
was first observed.

## Decision

The byte-identical comparison harness will keep one untimed read-only sentinel
transaction open on every transactional fixture while Criterion runs:

- the single-server transaction fixture used by current reads, RMW, blind
  update, and blind remove;
- the single-server projected-read fixture used by full, selected, head, and
  mixed reads; and
- the three-server cluster fixture used by the distributed RMW.

The direct non-transactional fixture and the in-process HLC workload need no
sentinel because neither participates in transaction metadata cleanup.

Before the benchmark group starts, the harness will perform the same lifecycle
for each transactional fixture:

1. choose and transactionally seed one dedicated sentinel cell per participant;
2. begin one read-only transaction;
3. issue a selected `score` read for every sentinel cell; and
4. retain the transaction ID until the benchmark group finishes.

On the baseline, selected reads register participant transaction state and
anchor `txns_sorted` below all later hot-cell metadata. Cleanup therefore does
not consider that fixture's benchmark cells stale. The candidate receives the
identical public transaction calls, but its product-level cleanup fix remains
active. After Criterion finishes, the harness aborts every sentinel transaction
before shutting down its fixture.

The existing `hold_cleanup_floor` and `release_cleanup_floor` fixture methods
remain the single implementation mechanism. Single-server fixtures pass one
sentinel ID; the cluster passes one ID per server. Sentinel setup, retention,
and release are not added to any `iter_custom` closure and are therefore
outside returned benchmark elapsed time.

The transaction sentinel starts its ID search at `9_800_000`, the
projected-read sentinel at `10_100_000`, and the existing cluster sentinels at
`9_500_000 + participant_index * 10_000`. These ranges do not overlap any
measured ID range.

## Alternatives considered

- A sentinel on every transactional fixture is the selected approach. It
  directly covers the scope of the baseline cleanup mechanism with a small,
  identical, public-API-only harness change.
- Recreating a fixture per scenario would isolate state more aggressively but
  would add substantial setup complexity and would no longer exercise the
  existing portfolio lifecycle.
- Patching or disabling cleanup in the baseline product would remove the race
  at its source but would stop measuring the exact develop product revision.

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
- The transaction, projected-read, and cluster sentinels use separate ID
  ranges and retain independent transaction IDs.
- All 13 nonhistorical scenarios remain present.
- The measured multi-participant transaction still executes begin, reads,
  updates, distributed prepare/commit, and distributed end.
- No product source, distributed phase, invariant, retry rule, or comparator
  threshold changes.

## Verification

The two preserved failed remote runs are the behavioral RED cases. A structural
RED/GREEN check will additionally require dedicated sentinel IDs, setup before
`group.bench_function`, and abort after `group.finish`.

Both baseline and candidate must then pass:

1. scoped rustfmt;
2. local `cargo bench --bench occ_transactions -- --test`;
3. strict exact-13 JSON validation;
4. byte-identity and clean-scope checks; and
5. one accept-grade filtered run per side containing exactly
   `mvcc/rmw_multi_cell`, `mvcc/partial_read`, and
   `mvcc/multi_participant` on `192.168.10.87`.

Each filtered report must contain exactly its requested scenario set, positive
commits, passing invariants, and no unexpected outcomes. The baseline failure
logs and partial reports for both the unprotected cluster fixture and the
unprotected single-server transaction fixture remain preserved as behavioral
RED evidence.

Only after those gates pass may the three baseline and three candidate
accept-grade portfolio runs restart.
