# OCC Repeatable-Read Snapshot Pinning

## Design revision (2026-07-23): shape-gated, not size-gated

This design was refined during implementation. The trigger for pinning is the read
**shape**, not the cell **size**. Pinning exists so a version stays stable for a *later*
read within the same transaction; the reads that need that are the **partial** ones
(`head`/`read_selected`), which may be followed by a full read. A full `read` never needs
it — the coordinator has the whole cell and caches it as today.

Final approach:
- The participant pins the version it serves on a `head`/`read_selected` (partial) read and
  serves that transaction's later reads of the cell from the pin; a full `read` does not
  pin.
- The coordinator issues **shape-specific** participant RPCs (`head`→`head`,
  `selected`→`read_selected`, full→`read`), caches the small results, and serves a later
  full read from the pin. A `head` on a large cell therefore transfers only the header,
  never the whole cell — that is the `projected_reads` win.
- No cell-format change and no read-RPC return-type change. The only protocol tweak is a
  `pin: bool` on the `head` RPC so the blind-update version-observation path
  (`observe_head_from_site`) passes `false` and stays pin-free.
- There is no size threshold, size probe, or response envelope. The sections below that
  describe a size threshold, a `cell_stored_len` probe, or an enriched
  `{pinned, full_small_cell}` response envelope are **superseded** by this revision; they
  remain for historical context.

Trade-off: a *transactional partial* read of a *small* cell also pins (briefly, cheap).
Non-transactional reads and the common small-cell *full* read (rmw, multi_cell) are
untouched, so the zero-overhead-on-non-transactional property holds and there is no
small-cell regression in the portfolio.

## Summary

Repeatable reads in the current OCC implementation are bought by cloning the
entire cell into the coordinator's private transaction memory on first access,
even when the caller only needs a header or a projected field. For large cells
this whole-cell fetch, transfer, and clone dominates read latency and is the
principal cause of the `projected_reads` regression (~12x slower than
`develop`).

This design removes that clone for large cells without changing any isolation
guarantee. A transactional read of a large cell records the version's storage
**location plus a held `SegmentReferenceGuard`** instead of copying its bytes.
The storage engine is copy-on-write, so the read version's bytes are immutable;
the guard prevents the cleaner and tiered eviction from reclaiming them.
Repeatability is then provided by immutability-plus-pinning rather than by
copying. The whole-cell transfer is deferred until (and only if) an actual full
read occurs, and is served from the pinned version.

This is the read-side half of MVCC with none of the hard parts: no version
index, no visibility rule, no garbage-collection redesign. Each transaction
needs only the single version it read at first access, pinned in place. Full
MVCC (a later increment) generalizes "one pinned version per cell" into "a
pinned version chain plus a visibility rule," reusing the pin and the
participant read-set lifecycle built here.

## Context

The repeatable-read OCC implementation is committed on
`feature/repeatable-read-occ`. Today's read path:

- The coordinator's `read_from_site` fetches the full cell from the participant
  and stores the owned cell in the transaction's `txn.data` cache.
- Later `head`, `read_selected`, and full `read` calls are served from that
  cached owned cell, which guarantees repeatable full/selected/header reads and
  repeatable absence.
- The cached version also feeds read certification: the observed version is
  recorded as `CellExpectation::Present(version)` and certified at prepare.

The engine that makes a cheaper path possible:

- Updates are copy-on-write: `Chunks::update_cell_by` writes a new cell at a new
  location, atomically flips the `cell_index` pointer, and marks the old entry
  dead. Old-version bytes are never mutated in place; they remain valid until the
  cleaner reclaims the dead space.
- Removes write a tombstone and mark the old entry dead.
- `SegmentReferenceGuard` pins a segment so the cleaner cannot reclaim it and
  tiered storage cannot evict it. The commit/abort paths already use it to keep
  rollback cells alive.

Because old versions are immutable until reclaimed, and reclamation is
preventable with an existing primitive, a transaction can hold a stable
reference to the exact version it read without copying the bytes.

## Goals

- Eliminate the whole-cell fetch and clone that provides repeatable reads, for
  cells above a configurable size threshold.
- Preserve every immutable correctness property of the repeatable-read OCC
  contract, unchanged.
- Improve `projected_reads` (`head`, `selected`, `mixed`) throughput and p95
  latency without regressing any other portfolio scenario.
- Keep small-cell and non-transactional behavior byte-for-byte identical to
  today.
- Leave the pin and participant read-set machinery reusable by a later MVCC
  increment.

## Non-Goals

- Building a version index, a snapshot visibility rule, or multi-version garbage
  collection. Those are later MVCC increments.
- Changing the prepare, commit, or abort protocols, or the certification logic.
  (A new lightweight read-only pin release is added; it is not a prepare and
  does not alter those protocols. See the read-only pin lifecycle.)
- Optimizing small-cell reads, where the clone is already cheap.
- Changing the on-mmap cell format or cell header.
- Changing non-transactional cell RPCs.

## Immutable Correctness Contract (unchanged)

This increment must not change any property of the repeatable-read OCC contract.
In particular:

1. The transaction's first observation of a cell remains its repeatable snapshot
   for later full, selected, and header reads.
2. An observed missing cell remains missing within the transaction.
3. Every read dependency that influences a write is certified at prepare.
4. Inserts certify absence; updates and removes certify the expected present
   version.
5. Two transactions derived from the same cell version cannot both commit
   conflicting writes.
6. The guarantee holds across coordinators with concurrent vector clocks.
7. Participant expectations are validated before any participant mutation.
8. Storage mutations remain conditional on the certified version or absence.
9. Certification ownership is held through commit or rollback and released only
   by its owner.
10. Prepare votes settle before failure cleanup.
11. A successful prepare whose response is not delivered is rolled back.
12. Once abort is accepted, commit stays illegal; partial abort failures stay
    retryable.
13. Read-only transactions continue to avoid distributed prepare. (A pin release
    is not a prepare; transactions that pin nothing contact no participant at
    completion, exactly as today.)

## Design

### Size gate

A tunable threshold (`NEB_TXN_READ_PIN_BYTES`, default a small multiple of the
cell-header size on the order of a few kilobytes) decides the read path per
cell:

- Serialized cell size at or below the threshold: the current
  clone-into-`txn.data` path is used, unchanged.
- Serialized cell size above the threshold: the pin-and-defer path is used.

Because sub-threshold cells take the exact current path, small-cell scenarios,
blind mutation, hot-cell, multi-cell, and multi-participant workloads composed
of small counter cells are provably unaffected. Only large-cell reads change.

### Participant-side pinned read-set

Each participant maintains, per transaction, a pinned read-set:

```
txn -> { cell_id -> { location, version, guard: SegmentReferenceGuard } }
```

On a large-cell read for a transaction, the participant:

1. Resolves the current version of the cell (as today).
2. Acquires a `SegmentReferenceGuard` on the segment holding that version.
3. Records `{location, version, guard}` in the transaction's pinned read-set.
4. Returns only what the caller asked for (header for `head`, projected fields
   for `read_selected`, full bytes for a full `read`).

Subsequent reads of the same cell by the same transaction are served from the
pinned `location`, not from `cell_index`, so a concurrent update that advances
the current version does not change what the transaction observes. The pinned
bytes are immutable (copy-on-write) and cannot be reclaimed (guard held), so all
later reads of that cell within the transaction are repeatable and consistent.

### Coordinator-side deferred fetch and small-result cache

For a pinned large cell the coordinator's `txn.data` entry holds
`{version, header, cached projections, pin reference}` instead of the full owned
cell. Reads are served as follows:

- `head`: served from the cached header after the first access; no full transfer
  ever occurs for a header-only usage.
- `read_selected`: served from a cached projection when present; otherwise one
  RPC projects it from the pinned version and caches the result.
- Full `read`: one RPC fetches the full bytes from the pinned version, done once,
  only when a full read is actually requested.

A transaction that only ever performs `head` and `selected` reads on a large
cell never transfers or clones the whole cell. Read-your-writes is unchanged: a
buffered write shadows the pinned read exactly as it shadows a cached cell
today.

**Read-response protocol (decided 2026-07-23).** The coordinator cannot know a
cell's size before reading it, and the participant read RPCs must therefore
carry the size decision back. Each participant read response is enriched to a
small envelope carrying the requested shape, the version, a `pinned: bool`, and
— for `head`/`read_selected` on a **small** cell — the full cell as well
(`full_small_cell`). A small cell is thus fetched whole on its first access (the
coordinator caches it and serves every shape locally, preserving today's
single-RPC, consistent small-cell behavior); a large cell returns only the
requested shape with `pinned = true`, and the coordinator defers the full-cell
transfer until an actual full read. This avoids a repeatable-read violation that
a header-only response would cause for a small cell updated between a `head` and
a later full read.

### Certification (unchanged)

The coordinator records the observed version as `CellExpectation::Present`
exactly as today and certifies it at prepare. The pin is orthogonal to
certification; it provides repeatability of the read value, not conflict
detection. Write-skew prevention and gates 3 through 5 are untouched.

### Read-only pin lifecycle

Pins are memory-retention references, not correctness locks. Losing a pin
release is fail-safe: it only delays reclamation, never affects correctness or
isolation.

- A read-write transaction already contacts each participant at commit or abort;
  its pins are dropped there, alongside the existing segment-guard cleanup.
- A read-only transaction that pinned large cells sends a lightweight release to
  the participants holding its pins on completion. The participant's existing
  stale-transaction cleanup is the backstop if that release is lost.
- A read-only transaction that pinned nothing (all reads sub-threshold) keeps
  today's zero-participant-contact completion, preserving property 13.

### Concurrency and correctness

- **Concurrent transactional overwrite.** An update writes a new version
  elsewhere and marks the old dead; the pinned version's bytes are unchanged and
  unreclaimable. The reader keeps seeing its version.
- **Concurrent non-transactional overwrite.** Same: a non-transactional
  `update_cell_by` is copy-on-write and marks the old dead; the guard prevents
  the cleaner from reclaiming the pinned version.
- **Remove.** A remove tombstones and marks the old dead; the pinned version
  survives, so repeatable reads and repeatable absence both hold.
- **Cleaner and tiered eviction.** Both honor `SegmentReferenceGuard`, so
  neither reclaims nor evicts a pinned version.
- **Certification races** are unchanged, because certification still runs
  against the current stored version at prepare.

## Testing and Acceptance

Correctness gates (must pass before acceptance), run as in the OCC optimization
loop:

```
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
cargo check --lib
```

New focused tests:

- A large-cell transaction observes the same header, selected fields, and full
  value across repeated reads while a concurrent transaction updates the cell to
  a new version.
- The same, with a concurrent non-transactional overwrite plus a forced cleaner
  pass, proving the pinned version is neither mutated nor reclaimed.
- Repeatable absence: a pinned large cell removed by a concurrent transaction
  still reads as its pre-remove snapshot within the pinning transaction.
- A header-only large-cell transaction performs no full-cell transfer
  (instrumented assertion on bytes transferred or cells cloned).
- Certification still aborts a read-write transaction whose pinned cell advanced
  since it was read.
- Read-only pins are released on completion, and stale-cleanup reclaims a pin
  whose release was dropped.

Benchmark acceptance (same policy as the OCC optimization loop, on the dedicated
host with NUMA binding):

- `projected_reads` (`head`, `selected`, `mixed`) improves throughput or p95 by
  at least 5% at CV at most 5%.
- No stable secondary scenario loses more than 3% throughput or gains more than
  5% p95. Small-cell scenarios are expected to be unchanged because they take
  the current path.
- All workload invariants pass and all unexpected-outcome lists are empty.

## Roadmap: path to full MVCC

This increment is the first slice of a snapshot-isolation-with-MVCC-reads
direction whose target is snapshot reads plus kept read certification (so every
current gate, including write-skew prevention, is preserved while reads get
cheaper). Later increments, each its own spec:

- **Version index.** Generalize the single pinned version into a side index
  `cell_id -> ordered [{version, commit_ts, location}]`, populated at commit,
  with no cell-format change and pay-per-use cost (zero on cells never accessed
  transactionally).
- **Snapshot visibility.** Assign each transaction a snapshot `S` (begin clock)
  and resolve the highest version with `commit_ts <= S` under the existing
  total order (causal `Before` with `deterministic_cmp` tie-break), giving a
  consistent, repeatable, cross-server cut.
- **Snapshot-gated GC.** Retain versions until the oldest active snapshot passes
  them; reclaim below the watermark, integrated with the cleaner.

## Risks and Mitigations

- **Pinning blocks the cleaner** for a transaction's lifetime on the pinned
  segments. Mitigated by the size gate (only large cells pin) and by read-only
  release plus stale-cleanup. If long transactions over large cells prove to
  pin too much, the threshold is tunable upward.
- **Participant read routing** must serve a pinned cell's later reads from the
  pin, not from `cell_index`. This is the one participant-path change; it is
  covered by the repeatable-read-under-concurrent-update tests.
- **Engine under active repair.** This increment touches the transaction read
  path and the pin lifecycle, not the cleaner internals or the cell format, so
  it stays clear of the storage-engine areas currently being repaired.

## Benchmark results: 2026-07-23, host 192.168.10.239 (Genoa, 192 cores, 1 NUMA node)

The dedicated benchmark host `192.168.10.17` was unreachable, so measurement ran
on `192.168.10.239` — a shared machine (load average 1-4 during runs), which
caps result quality: only `projected_reads/head` and `selected` achieved CV <= 5%;
the write scenarios were noise-dominated there.

Baseline = pre-pinning branch HEAD (`4ac86c69`), candidate = the pinning feature
after the review fixes (`0c99f28e`, `adaced90`). Both built and run identically,
NUMA-bound, sequentially.

Interleaved A/B x3 (means, CV in parentheses):

| Scenario | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| `occ/projected_reads/head` | 30,760 (1.5%) | 32,769 (1.5%) | **+6.5%** |
| `occ/projected_reads/mixed` | 32,859 (18.9%) | 24,621 (16.6%) | **-25.1%** |
| `occ/independent_rmw/1` | 37,138 (34.6%) | 43,528 (9.4%) | +17.2% (noise) |
| `occ/hot_rmw/8` | 25,514 (9.3%) | 29,173 (16.4%) | +14.3% (noise) |

Single-process solo runs of `projected_reads` alone showed a larger head win
(+71.8%, 49.6k vs 28.9k, p95 -29.5%) and `selected` at -1%; `blind_update/1`
and `multi_cell/8` were unchanged (-1%, within noise).

Interpretation:

- The head win is real and mechanistic: header reads no longer materialize the
  64 KiB payload (the review fix replacing `read_cell` with
  `cell_location_and_version` in `ensure_read_pin` is what unlocked it).
- The mixed regression is real and **structural**: a partial read followed by a
  full read costs two RPCs and two parses under deferral, versus the baseline's
  one prefetching RPC and one parse. In-process, where transfer is nearly free,
  deferral cannot beat prefetching on that pattern. The win deferral was
  designed for — avoiding an expensive large-cell *network* transfer — is not
  exercised by this loopback/in-process benchmark.
- `selected` is a wash because projecting a field from a large map still walks
  the stored layout; an early-exit field reader is a possible follow-up.
- No write-path scenario shows a detectable regression once measured
  interleaved; pin bookkeeping is skipped entirely on unpinned paths.

Against the acceptance policy (target >= +5%, no secondary scenario worse than
-3%): `head` clears the target bar, but `mixed` fails the secondary bar by a
wide margin, so the feature as a whole does not pass the in-process gate. A
formal verdict for networked deployments would need a multi-host benchmark, and
stable write-scenario confirmation needs the quiet dedicated host.
