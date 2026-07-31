# Point-Cell MVCC Retrospective

Date: 2026-08-02
Status: attempt concluded; branch preserved unmerged as `feature/point-cell-mvcc`
Companion design: `2026-07-25-point-cell-mvcc-design.md`

## Decision

Point-cell MVCC will not replace the repeatable-read OCC engine. The branch
is pushed as a reference implementation; the transferable work was extracted
to `feature/occ-baseline-backports` and merged (cleaner restoration stack,
tombstone accounting fix, off-foreground progress-gated cleaning, adaptive
combine victim bar) together with the bifrost checked-HLC upgrade.

## What was built and proven correct

Per-cell revision chains keyed by HLC `revision_ts`; strict-`<` snapshot
visibility with pending-revision invisibility until distributed end;
Percolator-style leased revision allocation with an assigned-revision
watermark protecting recovery; chain-aware cleaner (bloom classification,
newest-ts ceilings, dead-extent bitmaps); committed-current mirror fast
path; deferred abort compensation materialized from the pinned chain
predecessor. All correctness gates held throughout: hundreds of local
tests, invariant-checked benchmark runs, recovery and corruption suites.

## Final measured position (fresh interleaved 4x4, acceptance host)

Rich projected reads at or above OCC (selected +13%, partial parity,
head parity); cheap point reads -7..-12%; write transactions -20..-29%
with p99 +30..55%. Non-transactional operations at parity by design.

## Why it did not work out

1. **The write tax is structural, not incidental.** Roughly 6us/cell of
   every transactional commit is chain maintenance: revision node
   allocation, validated chain publication, retirement scheduling. It is
   distributed across many small costs (map probes, epoch pins, small
   allocations) with no single hot function, so it shrinks only by
   removing steps. Five gated optimization tranches (v25-v28, v31)
   removed every redundant step that preserved the contracts; what
   remains is the price of keeping history.

2. **The expected read win had no mechanism to exploit.** The OCC
   engine's point reads are already lock-free: transaction owner locks
   gate other prepares, never plain reads, so readers never blocked on
   writers to begin with (verified: zero waits under saturated write
   contention, tails identical). MVCC therefore cannot make point reads
   faster here; under write pressure it makes them ~20% slower because
   chain-bearing cells cost more to probe. MVCC's genuine read value on
   this architecture is consistency semantics only: coherent multi-cell
   snapshots, repeatable non-fuzzy reads, invisibility of in-flight
   writes. Those were not the goal.

3. **Deliberate contracts bounded further recovery.** The coordinator's
   resolvability contract (every abort discoverable for 300s, two-phase
   decision publication) and the rollback-on-undelivered-response
   guarantee (post-commit compensation) are load-bearing, test-encoded
   design choices. Each blocks an otherwise-large optimization (read-only
   decision exemption; fused sole-participant commit+end, built and
   parked on the branch). Honoring them was correct; it also capped the
   ceiling.

## Lessons

- **Measure the mechanism before promising the win.** The 1.5x read
  expectation assumed reader-blocking that this engine never had. One
  contended-workload measurement would have priced the whole attempt
  before implementation started.
- **On this rig, 3-run populations cannot support claims under ~5%.**
  Cross-population medians wander +-3..10%; two "regressions" chased this
  campaign were baseline-side outlier draws. Real effects (remove -9.9%,
  read_only -6%) reproduced across 5-run populations with p99 signatures
  and mechanisms; noise did not. Gate rule going forward: 5 runs for any
  claim under 5%, and always interleave fresh populations for both sides.
- **Distinguish restoration from improvement.** Most of Phase A repaired
  regressions the MVCC branch itself introduced (cleaner livelock,
  classification cost). Only genuinely new mechanisms (dead-extent
  bitmap, accounting fix, off-foreground cleaning) were portable to the
  baseline - and they were, netting +3..+10% on direct mutations.
- **Background cleaning needs both a pressure ladder and progress
  gating.** Wake-only collapses bimodally under full-rate inserts;
  fill-based pacing taxes static near-full working sets with futile
  passes; victim selection that relocates soon-to-die cells fights the
  foreground. The merged design (progress-gated two-tier pacing +
  pressure-adaptive victim bar) is the distilled answer.
- **Fail-safe re-verification accumulates.** The commit path had grown
  six chain/head verification walks per write where one or two carried
  the guarantees; each removal needed an explicit invariant argument, and
  all of them survive on the branch as worked examples.
- **Volatile and durable paths deserve separate cost models.** Several
  wins (force-sync elision, deferred compensation, replay-lock skip) were
  free on volatile deployments and impossible on durable ones; gating
  them on storage configuration kept both honest.

## Where the reference lives

Branch `feature/point-cell-mvcc` (tip: blind-op deferred observation).
Unlanded but built, on the branch's session records: fused
sole-participant commit+end (semantics trade), chain-handle threading
(gate-ambiguous at n=3). The optimization ledger with every gate
population is in the branch worktree's local `.superpowers/sdd/progress.md`.
