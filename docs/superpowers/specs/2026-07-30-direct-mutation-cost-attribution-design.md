# Direct-Mutation Cost Attribution and Optimization Design

## Status

Proposed. No production source has been changed under this design yet.

This design continues the approved
`2026-07-29-nontransaction-mvcc-cost-isolation-design.md`. That correction made
the direct data path history-free, but Task 7 remote acceptance rejected the
performance restoration: isolated matched `non_transactional_write` reproduced
a `-9.76%` candidate throughput loss, and the follow-up RCA demonstrated two
remaining candidate-only costs during direct-only churn:

1. every direct mutation obtains `revision_ts` through `HlcSource::try_now`
   (one `SystemTime::now()` plus an `AcqRel` compare-exchange loop on one
   server-wide atomic), where the pre-MVCC path incremented a per-cell version
   and read a cached coarse clock; and
2. the cleaner classifies every physical entry through MVCC-aware machinery
   (`HistoryIndex` probes, `(Id, revision_ts)` deduplication, revision sorting,
   and exclusive segment acquisition) even when the chunk has never created a
   revision chain.

This document defines how the two costs are measured separately, and specifies
the production designs for each: an amortized leased revision allocator and a
chain-count-gated cleaner fast path. Each is optimized, benchmarked, and
committed independently.

## Reproduction

Protocol: identical to the Task 7 isolated matched write classification.
Host `192.168.10.87`, `numactl --cpunodebind=0 --membind=0`, base port 39400,
shared build target `target/occ-bench/direct-build`, one command at a time,
exact filter `^mvcc/non_transactional_write$`, and the frozen Task 7 disposable
heads:

- baseline `468a52ae4fe8d848f7f5fe5f308f2785c492cc63`
  (+ bifrost `b078ce7a`), no features;
- candidate `ed8ee6c6d22566f416e0dc7bb6aa5ceb77582b7b`
  (+ bifrost `0a53f195`), `--features mvcc_revision_api`.

Both trees were re-verified clean and detached at those heads with the five
authoritative shared-source SHA-256 hashes intact before execution.

Reproduction population (2026-07-30, labels `direct-*-repro-write-{1..3}`,
executed serially base-1..3 then mvcc-1..3; every run correctness-clean with
positive commits, `attempts >= committed`, all invariants passed, and empty
`unexpected`):

| Run | Throughput (commits/s) | p50 | p99 |
| --- | ---: | ---: | ---: |
| base-1 | 1,045,745.610 | 841 ns | 2,511 ns |
| base-2 | 1,125,057.663 | 750 ns | 2,270 ns |
| base-3 | 1,115,596.230 | 780 ns | 2,220 ns |
| mvcc-1 | 985,826.025 | 880 ns | 2,349 ns |
| mvcc-2 | 956,081.260 | 910 ns | 2,460 ns |
| mvcc-3 | 1,000,748.666 | 870 ns | 2,380 ns |

Population statistics:

- baseline throughput median `1,115,596.230/s`, CV `3.95%`;
- candidate throughput median `985,826.025/s`, CV `2.32%`;
- **throughput median delta `-11.63%`**, p50 median delta `+12.82%`,
  p99 median delta `+4.85%`.

Both populations satisfy the CV <= 5% requirement, so the reproduction is
conclusive and consistent with the Task 7 isolated pair (`-9.76%`) and the
original 3x3 write median (`-13.88%`). The shape — a stable per-operation
cost increase (p50 up ~100 ns at ~850 ns medians) rather than tail collapse —
matches a fixed foreground cost per mutation plus background interference.
Remote evidence: `direct-{base,mvcc}-repro-write-{1..3}.{json,log}` under the
respective Task 7 acceptance checkouts on `192.168.10.87`.

## Measured outcomes and decisions

All variants measured on `192.168.10.87` with the Task 7 protocol, isolated
`^mvcc/non_transactional_write$`, three runs each, compared against two
candidate control re-runs (`998,241/s` and `991,502/s`, a `0.68%` spread
confirming host stability; median `994,872/s` used as the contemporaneous
candidate). Baseline median `1,115,596/s`. Every run correctness-clean.

| Variant | Median | CV | vs candidate | vs baseline | Gap recovered |
| --- | ---: | ---: | ---: | ---: | ---: |
| D1 shared chunk counter | 1,046,429/s | 1.91% | +5.18% | -6.20% | +42.7% |
| D2 cleaner probes gated | 989,159/s | 1.87% | -0.91% | -11.33% | -7.7% |
| D3 D1 + D2 | 1,059,669/s | 3.96% | +6.51% | -5.01% | +53.7% |
| D4 per-thread counters | 1,048,823/s | 1.90% | +5.42% | -5.99% | +44.7% |

Three decisions follow directly:

1. **Revision generation is the only confirmed lever** (+5.18%). Production
   design 1 proceeds.
2. **Production design 2 (cleaner fast path) is REJECTED by measurement.**
   D2 measured `-0.91%` — indistinguishable from zero. The RCA's 28%-vs-18%
   cleaner cycle share does not convert into foreground throughput because
   that work runs on cleaner threads that do not contend with the writer.
   The chain-count gate is correct and passes every local cleaner, recovery,
   and MVCC test, but it buys nothing on this workload and must not be
   committed on the strength of a cycle-share statistic. It may be revisited
   only if the erratic mutation scenarios show it helps them.
3. **Per-thread sharding is unnecessary** (D4 beats D1 by `+0.23%`, inside
   noise). The contended resource was the *server-wide* `HlcSource` — one
   `SystemTime::now()` plus an `AcqRel` CAS shared by every chunk and every
   operation — not the per-chunk counter, which at roughly 250k ops/s per
   chunk is effectively uncontended. Production design 1 therefore uses a
   single per-chunk lease with a plain `fetch_add`, with no thread-local
   sub-leases, no per-thread slot assignment, and none of the timestamp-waste
   or thread-churn concerns that sharding would introduce. This is a simpler
   design reached by measurement rather than assumption.

**Residual.** Even the best variant remains `-5.0%` to `-6.0%` below baseline,
so roughly half the reproduced regression has no identified mechanism. No
further production code should be written against guesses; the next step is a
fresh profile of the direct write path, plus a full seven-scenario portfolio to
establish current per-workload numbers with valid CVs.

## Demonstrated candidate-only mechanisms

Read from the frozen sources, three concrete mechanisms distinguish the
candidate's direct-only execution from the baseline's:

### M1: per-mutation full-HLC revision allocation (foreground)

`Chunk::next_revision_ts` calls `HlcSource::try_now` on the **one server-wide**
`Arc<HlcSource>` shared by all chunks. Each call performs:

- `SystemTime::now()` + checked packing;
- a load plus `compare_exchange_weak(AcqRel)` retry loop on the shared
  `ts: AtomicU64`.

Under many concurrent writers this is a contended cache line bouncing between
cores on every mutation, plus a wall-clock read. The baseline stamped
`version + 1` (register arithmetic on the already-loaded old header) and read a
coarse cached wall clock with one relaxed load. The RCA sampled
`HlcSource::try_now` children at 2.44% of user cycles, but sampling attributes
only in-line work; the coherence stall lands in the caller. The p50 increase
in the reproduction pair is consistent with a fixed per-operation cost.

### M2: MVCC-aware cleaner classification (background, same socket)

During direct-only churn the `HistoryIndex` map is empty, yet
`CombinedCleaner::collect_and_deduplicate_entries` and `Chunk::live_entries`
per physical entry:

- probe `chunk.history.is_live_at` (16-byte `Id` hash + map probe);
- deduplicate through a `HashMap<RevisionKey, _>` keyed by 24-byte
  `(Id, revision_ts)` (baseline: `u64` hash key);
- sort retained entries by `(revision_ts, size)`;

and `execute_combine_phases` per relocation:

- calls `chunk.history.relocate` (guaranteed `LostRace` map miss);
- then `compare_exchange_current_only_address` under the cell-index guard.

The RCA measured cleaner-related command share at 28.02% of sampled user
cycles versus 18.36% on the baseline. The cleaner runs on its own threads, but
on the same NUMA node it steals cycles, memory bandwidth, and cell-index mutex
slots from the foreground.

### M3: whole-combine exclusive segment acquisition (background, blocking)

`SegmentCandidate::new` now takes **exclusive** segment references for the
entire combine (required so MVCC historical materialization can never read a
segment being reclaimed), and `select_candidate_segments` lost the baseline's
`no_references()` prefilter. The baseline held only shared references, so
foreground reads of source segments proceeded during a combine; the candidate
makes them fail-and-retry until the combine finishes.

M3 cannot affect a pure-insert workload (fresh writes never read source
segments), so it is not the primary `non_transactional_write` mechanism. It is
the leading hypothesis for the five mutation scenarios whose candidate CV
exceeded 55% with p99 spikes up to +480% (`update`, `remove`, `upsert`,
`conditional_update`, `delete_recreate`): those workloads read existing cells,
and a read landing on a cleaner-held segment stalls. M3 is measured under the
cleaner-isolation experiments below and its remedy belongs to the cleaner
tranche.

## Attribution experiments

Goal: attribute the write regression to M1 versus M2/M3 with disposable,
benchmark-only candidate variants before any production change. All runs use
the Task 7 protocol on `192.168.10.87`, isolated
`^mvcc/non_transactional_write$` first (the one stable-CV regression), matched
pairs, serialized, one variant changed at a time.

Each variant is a disposable commit on top of the frozen candidate disposable
head; product trees on `develop`/`feature/point-cell-mvcc` are not touched.
Variants alter product code inside a disposable tree strictly for measurement;
they are never merged, and their numbers gate nothing except which production
design is pursued first.

- **D1 — revision generation nulled.** Candidate + `Chunk::next_revision_ts`
  (both impls) replaced by a relaxed `fetch_add(1)` on a process-local
  `AtomicU64` seeded once from the HLC at startup. Strictly increasing and
  unique, so the benchmark's correctness invariants hold; recovery/floor
  semantics are irrelevant inside one measured process. Removes the wall-clock
  read and the shared CAS loop.
  `delta(D1, candidate)` = M1's foreground share.
- **D2 — cleaner MVCC work nulled.** Candidate + chain-emptiness gate exactly
  as specified in the production cleaner design below (probe skipped when the
  chunk has never created a chain), plus the restored `no_references()`-style
  busy-segment prefilter before exclusive acquisition. History remains off the
  direct path, so during this workload the gate is always taken.
  `delta(D2, candidate)` = M2 (+ any M3) share.
- **D3 — both.** Additivity check: `D3 - candidate ≈ (D1 - candidate) +
  (D2 - candidate)` within noise, and `D3` versus baseline shows the residual
  regression (if any) not explained by M1/M2/M3.

Decision rule: one matched pair per variant for signal; any variant showing
`>= 2%` recovery graduates to a 3-run population with CV <= 5% before its
production counterpart is implemented. If D1 and D2 together fail to recover
at least half of the reproduced regression, stop and re-profile before writing
production code.

## Production design 1: leased revision allocation

### Requirements (unchanged from the handoff)

- nonzero, strictly increasing `revision_ts` per cell and per source;
- direct and transactional revisions remain comparable in one clock domain;
- transaction/recovery floors are observed;
- restart recovery selects the greatest `revision_ts` and advances the HLC
  beyond it before accepting writes;
- exhaustion and logical-bit overflow use checked arithmetic and refuse writes
  rather than wrapping or reusing timestamps;
- not an unchecked counter, and direct timestamps are not removed.

### Prior art: the Percolator timestamp oracle

Peng and Dabek, *Large-scale Incremental Processing Using Distributed
Transactions and Notifications* (OSDI 2010), §"Timestamps", solves the same
problem — every operation contacting one strictly-increasing timestamp
authority — with the same technique. The oracle "periodically allocates a
range of timestamps by writing the highest allocated timestamp to stable
storage", after which it "can satisfy future requests strictly from memory",
and on restart "timestamps will jump forward to the maximum allocated
timestamp (but will never go backwards)". A single oracle machine serves
around two million timestamps per second.

Three properties of that design are adopted here deliberately:

1. **Reserve by advancing the authority first.** The expensive durable step
   records the *end* of the range before any timestamp inside it is issued, so
   a crash can only waste timestamps, never reuse one. Our analogue is the
   `AcqRel` CAS that moves the shared `HlcSource` to `range.end` before
   `try_lease` returns; the authority a restart re-derives from (the maximum
   recovered `revision_ts`, which recovery already advances the HLC beyond) is
   likewise never moved backwards. Abandoned lease remainders are Percolator's
   "jump forward", not a defect.
2. **Self-tuning batch size.** Percolator keeps "only one pending RPC to the
   oracle" per worker, so batching grows automatically with load. Our `refill`
   mutex has exactly this shape: one refill is in flight at a time and every
   thread that arrives during it consumes the resulting range, so contention
   increases the effective batch instead of the cost. This is why the refill
   path is a mutex plus re-check rather than a lock-free retry loop.
3. **Batching does not weaken the timestamp guarantee.** Strict increase and
   uniqueness come from the reservation itself, not from per-request
   allocation.

The scoping difference matters and is deliberate. Percolator's timestamps
*are* its snapshot-isolation mechanism, so its oracle must preserve the
external-consistency property that a read at `T_R` sees every write committed
before `T_R`. Our leased timestamps are issued only to **direct,
non-transactional** mutations, which by the approved isolation contract have
no isolation or safety relationship with transactions. Transactional
timestamps keep coming from unbatched `try_now`/`try_observe`, so prepare
clock merging, commit-timestamp allocation, and `establish_recovery_floor` are
untouched. We apply the batching only where the weaker property is
contractually free.

### Required safety rule: the assigned-revision watermark

Percolator's "jump forward, never backwards" framing exposes a hazard the
first draft of this design missed, and it must be fixed before implementation.

Because a lease advances the shared clock immediately but is consumed over the
following microseconds, a direct mutation can carry a `revision_ts` *below* a
transaction that was stamped after the lease was taken. For direct updates and
direct deletes this is already contained: both compare against `previous`, the
current header's `revision_ts`, which any transactional write to that cell
would have advanced, so the existing check forces a refill. Insert is not
contained. A transaction deletes cell X at ts 900; a direct insert of X then
consumes leased value 850 into an empty slot (`previous == 0`); recovery
selects the greatest `revision_ts` per id and picks the ts-900 tombstone, so
the inserted cell disappears on restart.

The brief permits mixed-API use to have no isolation guarantee but requires
durable recovery to remain deterministic and correct, so a lost durable insert is a real defect, not an accepted mixing artifact.

Naming: this is a **lost durable insert**, not a resurrection. The deleted
cell correctly stays deleted; it is the newer insert that vanishes because a
higher-timestamped tombstone outranks it. Verified against
`src/ram/recovery.rs`, where selection is purely
`candidate.revision_ts > existing.revision_ts` with no cell-versus-tombstone
preference, and equal timestamps of differing kind are rejected as corruption.
The window is confined to one process lifetime: recovery tracks
`max_revision_ts` and advances the HLC beyond it, so a restart cannot inherit
a lagging allocator.

Fix: each `Chunk` keeps `max_assigned_revision_ts: AtomicU64`, updated with a
relaxed `fetch_max` wherever a transaction installs an assigned revision (cell
or tombstone) and by recovery when it selects a winner. A direct insert into
an empty slot requires its value to exceed that watermark in addition to
`previous`; failing that, it refills with `try_observe(watermark)` and retries.
This is both necessary (recovery has no other guard) and sufficient (the
tombstone's timestamp is in the watermark before the insert can consume a
leased value, so the insert is forced above it).

Cost accounting: the direct path adds one relaxed load of a line that is
written only by transactional installs and recovery — in a pure direct
workload it is never invalidated, so it stays shared-clean in every core's
cache. The transactional path adds one relaxed `fetch_max` to operations that
already perform chain installation and distributed coordination. This keeps
the direct hot path at one chunk-local `fetch_add` plus two compares while
closing the hazard globally rather than per-cell.

### Design

Bifrost gains a checked range-reservation primitive on `HlcSource`:

```rust
/// Reserve `span` consecutive timestamps. Advances the source to the end of
/// the reserved range, so every later try_now()/try_observe() result is
/// strictly greater than every leased value.
pub fn try_lease(&self, span: u64) -> Result<HlcLeaseRange, HlcError> {
    debug_assert!(span > 0);
    let phys = Self::packed_phys_ms_checked()?;
    let mut current = self.ts.load(Ordering::Relaxed);
    loop {
        let start = current.max(phys);
        let end = start.checked_add(span).ok_or(HlcError::Exhausted)?;
        match self
            .ts
            .compare_exchange_weak(current, end, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => return Ok(HlcLeaseRange { start, end }),
            Err(actual) => current = actual,
        }
    }
}
```

Leased values are `start+1 ..= end`: all nonzero, unique, strictly increasing,
and — because the reservation advances the shared source to `end` — strictly
less than every subsequently issued transaction ID, commit timestamp, observe
result, and recovery floor. Comparability with transactional revisions is
therefore preserved by construction, and "HLC advancement beyond the maximum
recovered timestamp" continues to hold: any durably written leased value is
`<= ts` at the moment it was handed out, and restart recovery already advances
the clock beyond the maximum recovered `revision_ts`.

Nebuchadnezzar gives each `Chunk` a lock-free allocator refilled from leases:

```rust
pub struct RevisionAllocator {
    clock: Arc<HlcSource>,
    /// Next candidate value; values above `end` are discarded mid-refill.
    next: AtomicU64,
    /// Inclusive end of the currently installed lease.
    end: AtomicU64,
    refill: Mutex<()>,
}
```

- **take(floor)** — `let v = next.fetch_add(1, Ordering::Relaxed) + 1;`
  if `v <= end.load(Acquire)` and `v > floor`, return `v`. Otherwise enter
  the refill path. `floor` is `previous` for updates and deletes, and
  `max(previous, max_assigned_revision_ts)` for an insert into an empty slot,
  per the watermark rule above.
- **refill(floor)** — under the `refill` mutex, re-check (another thread may
  have refilled), then `clock.try_observe(floor)?` if the floor was
  the failure cause, then `clock.try_lease(SPAN)?` and publish
  `end.store(range.end, Release); next.store(range.start, Release)` in an
  order that makes concurrent `take` calls either use the new range or retry.
  Failure maps to `WriteError::RevisionClockExhausted` exactly as today.
- The per-cell strict-increase check (`v > previous`, where `previous` is the
  replaced header's `revision_ts`) is retained verbatim; a leased value below
  a remote-stamped predecessor triggers one refill-with-observe, then errors
  only on true exhaustion — strictly more forgiving than today's immediate
  error, never less safe.
- Inserts additionally observe `max_assigned_revision_ts` as described in the
  watermark rule, which is what prevents a durable insert from being
  lost to a transaction's higher-timestamped tombstone.

Hot-path cost: one relaxed `fetch_add` on a chunk-local line plus two compares.
The wall-clock read and the server-wide contended CAS are paid once per `SPAN`
allocations. Initial `SPAN` candidate: 1024 (~1 ms of headroom at the observed
~1M ops/s), selected by microbenchmark across {256, 1024, 4096}.

Semantic deltas, stated explicitly:

- **Bounded wall-clock lag.** A leased value can lag physical time by the time
  taken to consume the span (µs–ms under load). After an idle period a stale
  lease can hand out a timestamp minted earlier. This affects nothing
  correctness-bearing: per-cell ordering is enforced by the `previous` check,
  retention/expiration use monotonic elapsed time, recovery compares only
  stored values, and cleaner temperature is a heuristic. If wall anchoring is
  wanted, the allocator can force a refill when the leased value's `wall_ms`
  trails a cached coarse clock by more than a configured bound; this is an
  optional hardening, not a correctness need.
- **Timestamp gaps.** Unconsumed lease remainders are abandoned on refill or
  restart. Gaps were always possible (failed writes, restarts); nothing
  consumes `revision_ts` density.
- **Logical-bit overflow.** Reservation is plain checked addition on the packed
  `u64`; logical bits carrying into physical bits behaves exactly as the
  existing single-step HLC and exhaustion (`u64` overflow, 48-bit physical
  packing overflow) refuses writes via `HlcError::Exhausted`.

Transactional paths are untouched: coordinators still use
`try_now`/`try_observe`; prepare clock merging, commit-timestamp allocation,
`establish_recovery_floor`, and recovery HLC advancement are unchanged.

Fallback design (if leasing microbenchmarks poorly or review rejects the lag):
keep per-mutation allocation but make it cheaper inside Bifrost — a background
ticker caches packed physical milliseconds in an `AtomicU64` so `try_now`
becomes `fetch_add(1)` + rare CAS-raise when behind the cached clock. This
removes the wall-clock read but keeps the shared-line contention; it is
strictly weaker than leasing and is only pursued if D1 shows the wall-clock
read, not the sharing, dominates.

### Tests

- lease ranges are disjoint, nonzero, strictly increasing, and every
  subsequent `try_now`/`try_observe` exceeds every leased value;
- `take` after an observed remote floor returns values above the floor;
- a leased value `<= previous` refills once with observe and then returns a
  strictly greater value;
- exhaustion at `u64::MAX` and at 48-bit physical packing refuses the lease;
- concurrent `take`/`refill` never return a duplicate or non-increasing value
  (loom-style or stress test);
- direct mutations remain strictly increasing across lease refills;
- **lost-insert regression**: a transaction deletes a cell at a high
  timestamp, a direct insert of the same id then runs against a deliberately
  stale lease, and after restart recovery the cell is present — this test must
  fail without the `max_assigned_revision_ts` watermark and pass with it;
- the watermark is advanced by assigned-revision cell installs, assigned
  tombstone installs, and recovery winner selection;
- existing recovery tests (`recovery advances HLC beyond maximum recovered
  timestamp`, greatest-`revision_ts` winner selection) stay green;
- history-bypass counters from the isolation design stay zero on direct paths.

## Root cause: cleaner exclusivity livelock (confirmed by profile)

The full-portfolio remeasure (2026-07-30, 3-run baseline population, CVs
1.25%-4.62%) showed the true mutation regressions had been hidden by the
Task 7 high-CV populations: candidate run 1 measured `upsert -43.9%`
(p99 2,420 -> 5,440 ns), `update -17.3%`, `write -11.4%`, `read +3.2%`, with
candidate scenario runtime exploding (baseline full portfolio ~4 minutes;
candidate exceeded 30). Throughput degrades as each scenario runs — Criterion
sizes iterations from a fast warmup, then collection crawls.

A matched local profile (`perf record -F 999`, isolated
`^mvcc/non_transactional_upsert$`, frozen heads `468a52ae` / `ed8ee6c6`,
`tiered_memory` on as in every benchmark build since it is a default feature)
reproduced the collapse locally (baseline 2.47M ops/s; candidate warm-up fast,
then collapse) and attributed it:

| Population | Thread group | Share of all sampled cycles |
| --- | --- | ---: |
| baseline | workload thread | 98.92% |
| baseline | all cleaner threads | ~1.0% |
| candidate | `combine-update-` pool | **90.34%** |
| candidate | `cleaner-clean-t` pool | 7.20% |
| candidate | workload thread | **2.30%** |

Inside the candidate `combine-update-` pool, **86.33% of all machine cycles
are `lightning::spin_hint::Backoff::spin`**, adjacent to
`Chunk::compare_exchange_current_address` and cell-index `WordMutexGuard::
try_lock`, with kernel `sched_yield` frames showing spin escalation.

### Mechanism

1. `tiered_memory` (default feature, so active in every benchmark) makes every
   guard over a present cell take a shared segment reference:
   `CellGuard::from_guard` -> `seg.incr_references()`. Every update/upsert/
   remove of an existing cell references the old cell's segment for the guard
   lifetime.
2. The MVCC cleaner replaced the baseline's shared-reference combine with
   whole-combine exclusivity: `SegmentCandidate::new` ->
   `obtain_exclusive_references()`, held across copy, archive, and the entire
   relocation phase.
3. `Segment::incr_references` deliberately bails out when the segment is
   exclusively held — the comment on it documents the inverted lock order
   ("cleaners obtain segment lock first, then cell locks, while normal
   operations obtain cell lock then segment counter") that the bail-out
   converts from deadlock into retry.
4. The result under mutation churn is livelock: foreground threads loop
   `lock_or_insert_cell` -> lock cell word -> `incr_references` fails against
   cleaner exclusivity -> unlock -> `Backoff::spin` -> retry, while the
   cleaner's relocation pool loops `try_lock` on those same hammered cell
   words to publish relocated addresses. Both sides burn full CPU; the
   workload thread gets 2.3% of the machine.
5. Second-order: foreground references also defeat the cleaner's
   `CAS(0 -> EXCLUSIVE)` at selection, so combining is delayed while dead
   space from the mutation churn accumulates; the allocation path then runs
   `Cleaner::clean` inline (`try_acquire`'s emergency and threshold GC),
   putting combine work on the foreground thread itself — the profile shows
   `CombinedCleaner::combine_segments` frames inside the workload thread.

This single mechanism explains the portfolio shape exactly: `upsert`
(maximum existing-cell churn) collapses; `update`, `remove`,
`conditional_update`, `delete_recreate` (all guard existing cells) regress
and show unstable CV — their throughput depends on how often a combine window
overlaps the timed interval; `write` inserts into empty slots, whose guards
take no segment reference, so it pays only revision generation and ambient
cleaner cycles; `read` creates no dead space, leaves the cleaner idle, and is
unaffected. It also explains why experiment D2 measured null on `write`:
`write` structurally cannot enter the livelock, and D2's probe-gating did not
touch the exclusivity window at all.

The Task 7 remote thread dump during the aborted candidate upsert rerun
(combine threads consuming CPU while the benchmark crawled at 116% total) is
the same signature.

### Why exclusivity cannot simply be reverted

`Segment::mem_drop` returns the address range to the allocator
unconditionally; it is not reference-safe. Historical MVCC materialization
resolves a raw address from a revision node and then acquires a segment
reference — between resolve and acquire, a freed segment would be a
use-after-free. Whole-combine exclusivity was the correction for that class
of race; the defect is its **window**, not its existence.

## Production design 3: narrow the cleaner exclusivity window

Restore the baseline's concurrency during the expensive phases and confine
exclusivity to the only step that needs it — returning memory:

1. **Selection and copy under shared references.** `SegmentCandidate::new`
   returns to `incr_references()` (shared), exactly as the baseline, keeping
   the cold/hot locking as-is. Foreground guards coexist with the cleaner
   throughout collection, copy, archive, and relocation. The lock-order
   inversion disappears: when the relocation pool takes a cell word lock, the
   foreground's `incr_references` on that segment succeeds, so nobody spins.
2. **Relocation exactly as today.** `history.relocate` first, mirror CAS under
   the cell word lock second, reverse rollback on inconsistency — unchanged.
   These already tolerate concurrent readers; they were designed for it.
3. **Exclusive only to free.** After all relocations for a source segment
   have been published, the cleaner releases its shared reference and attempts
   `CAS(0 -> EXCLUSIVE)` in a bounded backoff loop. New foreground guards no
   longer route into the segment (the cell index and history nodes point at
   the destinations), so remaining references are short-lived materializations
   draining out; the existing reader protocol (acquire reference -> recheck
   node location -> retry at the new address) means any reader that lands on
   the drained segment retries harmlessly. Once exclusivity is obtained, the
   segment is removed and `mem_drop` runs — the memory-free instant keeps the
   exact protection it has today.
4. **Fail-safe retention.** If references do not drain within the bounded
   wait, the segment is retained for the next cleaner cycle (space is
   reclaimed later, never unsafely). This is the same fail-safe direction the
   reconciliation error path already takes.

MVCC guarantees are unaffected: pending/committed visibility, snapshot
retention, relocation reconciliation, durability ordering, tombstone
watermarks, and recovery are untouched; the only change is *when* the cleaner
excludes readers — at the free, not during the work.

Acceptance for this tranche: the full seven-scenario portfolio with both-side
CV <= 5%, no stable regression, plus the transactional smoke; the cleaner
race suite and the tiered-memory suites must stay green. The livelock's
signature (combine-pool spin share) should drop to baseline levels in a
post-fix profile.

## Production design 2: chain-count-gated cleaner fast path (REJECTED BY MEASUREMENT)

> Measured at `-0.91%` on the isolated write scenario. Retained below as the
> record of a tested and refuted hypothesis. Do not implement without new
> evidence from the erratic mutation scenarios.

### Key invariant

In the isolated architecture, a revision chain is only ever created by seeding
the **current** cell-index target (lazy conversion) or by installing a new
assigned-revision head. Therefore a physical address that, at one observed
moment, is neither the current cell-index target nor referenced by any
existing chain can never later become chain-referenced. Chains are never
removed from `HistoryIndex.chains` once created. This yields a safe monotone
gate:

> If `HistoryIndex` contains zero chains at the moment an entry is probed,
> that entry's history liveness is definitively `false`.

### Design

`HistoryIndex` gains a `chain_count: AtomicUsize` incremented inside
`get_or_create_chain_with` under the existing `chain_creation` mutex (and by
the recovery-undo seeding path, which uses the same installer), exposed as:

```rust
#[inline]
pub fn has_chains(&self) -> bool {
    self.chain_count.load(Ordering::Acquire) != 0
}
```

Gated call sites, each probing the counter at classification time (per probe,
never cached across a pass — a start-of-pass snapshot would race with lazy
conversion of a head that is subsequently superseded mid-pass):

- `collect_and_deduplicate_entries`:
  `let history_live = chunk.history.has_chains() && chunk.history.is_live_at(...)`
  for both cells and tombstones. Raw cells then classify purely by
  `cell_index` ownership; raw tombstones purely by the direct sequence
  watermark, exactly as the handoff requires.
- `Chunk::live_entries`: same gate for both entry types.
- `execute_combine_phases` relocation: `chunk.history.relocate` stays
  **unconditional**. The relocation of a current head races with lazy
  conversion (which chains that very head); the existing order — history
  relocate first, then the cell-index mirror CAS under the guard — is what
  closes that race, and a pre-lock counter read would reopen it. A relocate
  against a chunk with no chains is a single map miss; collection, not
  relocation, is the per-entry hot path. If D2 later shows relocate misses
  still matter, the gate may be re-checked inside
  `compare_exchange_current_only_address` while the cell-index guard is held,
  as a separate measured change.
- `select_candidate_segments`: restore the baseline's cheap busy-segment
  prefilter (skip a segment whose reference word is nonzero) before attempting
  `obtain_exclusive_references`. This only skips segments whose exclusive
  acquisition would fail anyway; acquisition itself remains exclusive, so MVCC
  historical materialization safety is unchanged.

Safety argument for the gate, spelled out:

1. Probe reads `chain_count == 0` at time T. No chain exists at T, so no
   revision node references the probed address at T ⇒ `history_live` false is
   exact, not approximate.
2. Could the address become chain-referenced after T? Chain creation at
   T' > T seeds the cell-index target *as of T'*. If the probed address is the
   current target at T, the collection classifies it live via
   `current_index_target` regardless of the gate — covered. If it is not the
   current target at T, it can never again become a target or a seed — its
   liveness is permanently false — covered.
3. MVCC tombstones exist only inside chains; with zero chains, tombstone
   history-liveness is definitively false, and direct tombstones keep the
   sequence-watermark rule untouched.
4. Once `chain_count` becomes nonzero it never returns to zero, so mixed-mode
   chunks permanently use the full history-aware path: transactional
   revisions retain history-aware liveness, relocation reconciliation,
   durability, snapshot retention, and recovery behavior with zero change.

What this deliberately does NOT do: no per-Id gating, no skipping of
`history.relocate`, no weakening of exclusive segment acquisition, no change
to `RelocateResult` handling, dead-space accounting, or the direct-tombstone
watermark.

### Tests

- direct-only churn combine performs zero `is_live_at` map probes
  (test-only counter) while producing byte-identical retained-entry sets to
  the ungated path on the same fixture;
- creating one chain (a single transactional read of a direct head) flips the
  chunk to the full path permanently — probes resume;
- race regression: a lazy conversion racing a combine of the converted head's
  segment (before-relocate hook) still relocates the chain node and reconciles
  the mirror — this test pins the unconditional `history.relocate`;
- the existing cleaner suite (watermark survival/expiry, MVCC cell/tombstone
  relocation, pending/abort/compensation survival, lost-race single
  dead-accounting) stays green;
- busy-segment prefilter: a segment holding an active shared reference is not
  selected, and is selected on a later pass after release.

## Execution order and acceptance

1. Complete the 3x3 isolated-write reproduction population (CV <= 5% required
   for a conclusive baseline).
2. Run D1, D2, D3 disposable attribution pairs; graduate winners to 3-run
   populations.
3. Implement, test, and commit the winning production tranche first (one
   hypothesis at a time; separate commits for revision generation and cleaner
   isolation; Bifrost `try_lease` lands as its own Bifrost commit).
4. Gate each tranche on: local serial correctness suites (cell, history,
   cleaner, recovery, undo_log, data_site, occ_tests), then the complete
   matched non-transactional portfolio on `192.168.10.87` (all seven
   scenarios), then the 11-scenario transactional smoke on the candidate.
5. Keep a tranche only if it improves a stable scenario by more than 1% with
   both-side CV <= 5% and no stable-workload throughput or p99 regression.
   Noisy results are inconclusive and decide nothing.
6. The five high-CV mutation scenarios are re-examined after the cleaner
   tranche (M3 prefilter) lands; if their CV remains above 5%, exclusivity
   windowing (holding exclusivity per relocation batch rather than per
   combine) becomes the next measured hypothesis — as its own design.

Out of scope, unchanged from the parent designs: indexes/ranges/predicates,
mixed-API isolation guarantees, distributed phase changes, pending-revision
visibility, snapshot-retention rules, and backward compatibility.
