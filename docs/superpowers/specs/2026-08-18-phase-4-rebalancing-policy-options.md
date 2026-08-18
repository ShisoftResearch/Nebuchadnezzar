# Phase 4: who decides to rebalance, and on what evidence

**Status:** decision document. Nothing here is implemented. The *mechanism* is
finished and merged; what is missing is the *policy* on top of it, and the
policy is the part that should not be chosen by whoever happens to be writing
the code that week.

**Date:** 2026-08-18

---

## 1. What already exists

Everything below is merged on `develop` in bifrost + Neb and has tests.

| capability | entry point | notes |
|---|---|---|
| move one slot | `migration::migrate_slot` / `reclaim_donor_copy` | table entry is the only commit point |
| move many slots, one donor → one recipient | `migration::reshard_slots` | spawned fan-out, deferred drops |
| empty a member | `migration::drain::drain_member` | convergent loop, reports stranded slots |
| reassign slots holding nothing | bifrost `reassign_slots` | one raft command per batch, no data moves |
| ask whether a member holds anything | `migration::drain::owns_nothing` | reads the SM, not a report |
| refuse a write for a slot you no longer own | `WriteError::NotSlotOwner(owner)` | client follows the redirect once |

Measured costs, all on .239 in release:

- reshard, no tier pressure: **952 MB/s**.
- reshard, tier limit at ¼ of the payload: **35–62 MB/s** — the transfer becomes
  disk-bound the moment the working set does not fit. *A rebalancing policy that
  ignores this will schedule work that takes 20× its estimate.*
- drain, 1024 slots × 64 cells: **1.1 s** at concurrency 32 (was 8.9 s before the
  spawned fan-out), **73 s** at concurrency 1.
- receiving a migration costs the recipient no more hot tier than writing the
  same volume ordinarily (4128 MB vs 4096 MB against a 4 GB limit).

## 2. The six decisions

### 2.1 Who runs the balancer?

| option | consequence |
|---|---|
| **A. the raft leader of the slot SM** *(recommended)* | one balancer by construction, no extra election, it already has the authoritative table |
| B. every member, first-writer-wins per slot | matches how adoption works today, but two members can pick *different* destinations for the same overloaded donor and thrash |
| C. an operator command only | zero risk, zero benefit; this is the status quo |

Recommendation: **A**, with C always available. Leadership already exists and is
already the thing that serialises table writes.

### 2.2 What triggers a rebalance?

- a member **joins** — it owns nothing (that is the orphaning fix), so without a
  trigger a new machine stays empty forever. This is the one case where doing
  nothing is clearly wrong.
- a member **leaves** — already covered by `drain_member`, operator-invoked.
- **imbalance** crosses a threshold — needs §2.3.
- **pressure** on one member (disk, tier, request rate).

Recommendation: implement **join** first and stop there for one release. It is
the case with an unambiguous right answer, and it is the case the campaign was
started for.

### 2.3 What does "balanced" mean?

This is the real question, and the honest answer today is *we cannot measure it*.

| metric | available now? |
|---|---|
| slots per member | yes, free (`slots_owned_by`) |
| cells per member | only by enumerating — one pass over a member's whole index |
| **bytes** per member | **no** |
| hot-tier bytes per member | yes-ish (`settle_bulk_receive` reports hot bytes) |
| request rate per slot | no |

Balancing on **slot count** is nearly free and nearly meaningless: a slot is a
locality, and localities are not uniformly full — a hub vertex's container and
its adjacency live in one slot on purpose. Balancing 32768 slots evenly can
leave one member with ten times the bytes.

**This is the gap Phase 4 has to close first.** A per-slot byte counter,
maintained on write rather than computed on demand, is the prerequisite; the
[[statistics-refresh-wedge]] lesson applies directly — it must be a counter
updated on the write path and read by a sweeper, never an O(cells) scan invoked
inline.

### 2.4 How much movement is allowed at once?

`MigrationPlan::concurrent_slots` already bounds in-flight data to
`permits × batch_cells`. What a policy adds is a *rate*: how much of the cluster
may be in `Migrating` at any moment.

Recommendation: a single number, "at most N slots migrating cluster-wide"
(default small, e.g. 16), enforced at the SM by refusing `begin_slot_migration`
beyond it. Enforcing it in the state machine rather than in the balancer means
an operator's manual reshard and the balancer cannot together exceed the budget.

### 2.5 What stops it?

Interlocks worth having before any automatic movement:

1. no rebalancing while any member is unreachable or behind on the ring;
2. no rebalancing during recovery;
3. **hysteresis** — a member that just joined should be filled once, not
   re-balanced every time the ring twitches;
4. a kill switch that survives restart (a flag in the SM, not a CLI argument).

### 2.6 Does the recipient get a say?

Today it cannot refuse. A member that is over its tier limit, low on disk, or
already receiving should be able to answer "not now" and have the balancer pick
someone else. This is small and worth doing with the trigger.

## 3. Recommended shape for the first version

> On the leader, when a member joins and the cluster has been stable for T:
> compute each member's share by **bytes** (once §2.3 exists), move slots from
> the largest holder to the new member, at most 16 slots migrating cluster-wide,
> stopping when the new member is within X% of the mean or a recipient declines.
> Everything else stays operator-invoked.

Deliberately excluded from version one: load-based balancing, continuous
rebalancing, and anything that moves data because a *request rate* changed.
Those need evidence the system does not currently collect, and each of them can
be added on top of the same trigger once it does.

## 4. Blocking risk

**Do not enable automatic rebalancing while task #65 is open.** A reshard run
against a tier limit smaller than the data lost 129 of 1048576 cells silently —
the donor's read returned `CellDoesNotExisted` for cells whose index entry
pointed at zeroed memory, and the reshard reported `0 failures`. It is
intermittent and it is a tier bug rather than a migration one (the same run with
a tier limit above the payload lost nothing), but the consequence is specific to
moving data: an operator-invoked migration is something a human is watching,
whereas an automatic one loses cells at 3 a.m. and tells nobody.

The cheap mitigation, worth doing regardless of the root cause: a migration must
**verify** before it drops. The reclaim already knows exactly which ids the
recipient confirmed; the missing step is refusing to commit a slot whose
transferred count does not match its enumerated count, instead of reporting
success with a shortfall.
