# Phase 4: the node manager — decisions taken

**Status:** decision record. Supersedes the options in
`2026-08-18-phase-4-rebalancing-policy-options.md`, which stays as the
reasoning behind these choices.

**Date:** 2026-08-19
**Decided by:** the repository owner, in discussion.

---

## 0. What changed since the options doc

The options doc's §4 says in bold: *"Do not enable automatic rebalancing while
task #65 is open"*, because a reshard against a tier limit smaller than the data
silently lost 129 of 1048576 cells.

**That blocker is closed, and two more were found underneath it:**

| task | fix | what it was |
|---|---|---|
| #65 | `a5911452` | the write path ignored `incr_references()`'s refusal and appended into a segment being `madvise`d away |
| #70 | `d8b0039c` | archiving a settled-cold segment wrote its patchwork image over its own backup |
| #71 | `c35193ef` | promotion released its exclusive guard before restoring, so the cold-budget sweeper and `archive` could reach into the window |

The scenario the doc called blocking — reshard with the tier limit at ¼ of the
payload — now runs **4194304 cells, 0 vanished, 0 failures, across 6 contended
copies plus 4 single-copy runs**. Ten clean runs of the case that used to lose
cells.

**And the doc under-states what already exists.** It asks for "a migration must
verify before it drops" as future work. That is in the code:
`reclaim_slot_confirmed` (`src/migration.rs`) drops a cell only when the
recipient demonstrably holds it *at a version not older than the donor's*,
carries over anything stale or missing, and retains the donor's copy on any
error it cannot interpret.

The real gap is narrower: **a shortfall is counted and logged at `info`, not
escalated.** `retained > 0` is fine when a human is watching an operator-invoked
reshard. It is not fine for a balancer at 3 a.m.

## 1. The decisions

| question | decision |
|---|---|
| **v1 scope** | **Join-only auto-fill.** The leader fills a newly joined member; everything else stays operator-invoked. It is the one trigger with an unambiguous right answer: a joiner owns nothing *by design* (that is the orphaning fix), so without it a new machine stays empty forever. |
| **Balance metric** | **Per-slot byte counters, built first.** Maintained on the write path, read by a sweeper. Never an inline scan. |
| **Recipient veto** | **Yes, with the trigger.** A member over its tier limit, low on disk, or already receiving can answer "not now" and the balancer picks someone else. |
| **Who runs it** | The raft leader of the slot SM. One balancer by construction, no new election, it already holds the authoritative table. |
| **Rate limit** | One cluster-wide cap on slots in `Migrating`, enforced **at the state machine**, so the balancer and an operator's manual reshard cannot jointly exceed it. |
| **Interlocks** | No rebalancing while a member is unreachable or in recovery; hysteresis so a fresh joiner is filled once rather than re-balanced on every ring twitch; a kill switch stored in the SM so it survives restart. |
| **Escalation** | `retained > 0` from any reclaim is a warning **and stops the balancer**. |

Deliberately excluded from v1: imbalance triggers, continuous rebalancing, and
anything that moves data because a *request rate* changed. Each can be added on
top of the same trigger once the evidence to justify it is collected.

## 2. Shape: a node manager, owning the node lifecycle

Not a "balancer" bolted on the side — a component that owns
**join → fill → steady → drain → leave**. The drain half already exists
(`migration::drain::drain_member`); pairing it with fill under one component is
what makes "balancing" stop being a separate concept.

### Where it lives

Split on the rule the owner set during Phase 1 — *general-purpose clustering
belongs in bifrost*, which is why the slot SM is generic and carries no
dovahkiin dependency. The same line applies:

- **bifrost — the coordinator.** Leadership gate, membership subscription
  (`conshash::server_changed` already fires on join and leave), the migration
  budget, the kill switch, hysteresis, the decision loop. It knows **slots and
  members**.
- **Neb — moving and measuring.** `migrate_slot`/`reshard_slots` and the
  per-slot byte counters. It knows **cells**, and bifrost must not.
- **The seam is a trait**: move these slots A→B, report your load, admit or
  decline an incoming transfer. The third method is where the recipient veto
  lives.

### Three properties inherited from work already done

1. **A leadership change mid-migration is safe.** The table entry is already the
   only commit point, so an interrupted migration is correct-and-unfinished. The
   new leader re-observes and continues; no recovery protocol is needed.
2. **A stale leader cannot over-issue.** The budget is enforced at the state
   machine, not in the balancer, so a member that has not yet noticed it lost
   leadership still cannot exceed the cap.
3. **Concurrent proposals are harmless.** Adoption is first-writer-wins per slot.

### The trap to design against from the start

**It must be scoped per group, not a process global.** This codebase has a
recurring failure mode in exactly this shape: raft's `CALLBACK` is
process-global, service ids had to be scoped by `(group, database)`, and
bifrost's suite once hung 79 minutes on unscoped one-shot test hooks. A node
manager instantiated once per process would put two databases on one host into a
fight over the same table.

## 3. Build order

The metric gates the policy, so it goes first — and it is independently useful,
because "how much does each member hold" is a question operators need answered
whether or not anything automatic ever runs.

1. **Per-slot byte counters.** Updated on write/remove, read by a sweeper.
   The [[statistics-refresh-wedge]] rule is binding: an inline O(cells) refresh
   on the write path froze shard workers for 25+ minutes and drove every P2AB
   wedge. Counter on the write path, aggregation on a sweeper thread, never a
   poll that can block a shard worker.
2. **Load reporting + `retained > 0` escalation.** Expose per-member holdings;
   make a shortfall loud. Both are useful to an operator immediately and are
   prerequisites for anything automatic.
3. **The SM-side budget and kill switch** in bifrost — refuse
   `begin_slot_migration` beyond the cap, and honour a stored disable flag.
4. **The node manager itself**: leadership gate, membership subscription,
   hysteresis, and the fill decision.
5. **The recipient veto**, wired into the fill decision.

Costs to keep in view while writing the policy: reshard runs at **952 MB/s with
no tier pressure and 35–62 MB/s under it**. A policy that ignores that will
schedule work that takes 20× its estimate.
