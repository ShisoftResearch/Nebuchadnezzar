# Ranged index: robustness architecture plan

Written 2026-08-31, at the end of the soak campaign that followed the
BANC import debugging nights. Input evidence: eight distinct defects
found and fixed in one day (`ddd49c85..1d8440b8` plus bifrost
`506eb00`), five of them delete-triggered, three of them in the split
ROLLBACK path alone, every one of them living in the seam between two
subsystems that were each locally correct. This plan is about removing
the seams, not patching them faster.

The diagnosis in one sentence: the index's correctness is a conjunction
of invariants spread across six subsystems (B-link tree, leaf chain,
shared deletion set, write-back hub, placement SM, DistTree
boundary/marker layer), and nothing -- no type, no latch, no protocol
-- enforces the conjunction; it lives in comments.

Proposals are ordered by leverage. 1-3 are structural; 4-6 harden and
verify. Each names what it retires.

> **STATUS 2026-09-01: proposals 1-5 are implemented and 6 is running.**
> 1 (copy splits, `a16fd84d`) passed a full 3-hour soak and the legacy
> shared-leaf path is deleted (`c0595b56`, net -1,056 lines). 2 shipped
> in its minimal form (`94959498`: one-way detach stamps; the full
> owner-generation scheme is still the answer when migration work
> touches page ownership again). 3 shipped (`d30cf5e5`: per-tree
> tombstone journal on the existing checkpoint). 4 shipped
> (`docs/tla/CopySplit.tla`) and immediately earned itself: it found a
> LIVE bug in the copy split as first landed -- the copy sharing the
> source's deletion set -- in six states (`bd6382d3`). 5 shipped
> (`539dd292`: raw per-tree audit, run by the soak every 10 minutes).
>
> Two further defects fell out of doing the work: `retain` skipped
> every leaf past a gap pivot (latent for years, load-bearing the
> moment the copy split made retain its commit point), and `tree_stats`
> panicked its caller on a racing unload.

---

## 1. Disjoint tree ownership: retire shared-leaf splits

**The single largest bug factory in the index's history is pages shared
between trees.** The spine split moves subtrees by pointer, so source
and target share leaf objects, the seam must be severed durably in the
right order, recovery needs bounded loads that stop at foreign pages,
rollback must reabsorb, and the write-back queue can hold orphans. That
complexity produced: the TB16 MissingPage corpses, fd24832b's orphaned
half, the reabsorb right-edge violation, the orphaned-queue-ref
tombstone consumption, `recover_bounded`'s truncation logic,
`reconcile_split_marker`'s chain surgery, and `relink_page_next`.

**The optimization it buys is not worth it.** Measured (release, this
machine): moving 500K keys by full leaf rebuild costs **3.6-4.2ms**;
the spine split costs 0.35ms. Both run under a frozen tree. A depth-3
production split moves ~2M keys, so the copy costs ~15-20ms of freeze
-- invisible next to the seconds-long marker windows the seam barrier
already imposes, and next to the *minutes* the shared-leaf failure
modes have cost in debugging nights.

**Proposal:** a split builds the target from COPIES (the leaf-rebuild
path, which already exists and is the fallback today), and the source
is **not mutated at all** until the commit point. Commit = placement
flip + source truncation (retain below pivot, which also already
exists). Abort = drop the target; the source was never touched -- no
reabsorb, no right-edge reopening, no seam severing, no orphaned
shared pages, no bounded recovery, no marker reconciliation surgery.

Rules that fall out:
- **No page is ever reachable from two trees.** An ownership question
  ("whose page is this?") always has exactly one answer.
- Target pages are fresh ids in the target's slot; the source's
  persisted chain is never cut mid-split (truncation happens at commit,
  as one durable retain).
- `recover_bounded`, `walk_chain_page_ids`, `relink_page_next`, and
  the uncommitted-split arm of `reconcile_split_marker` retire.
- The rollback path retires entirely. Three of this campaign's eight
  bugs become unrepresentable.

Migration: the leaf-rebuild `split_off` and `retain` are the surviving
primitives, both battle-tested. The spine split and its simulation stay
available behind a flag for one release for A/B, then delete.

## 2. Page owner-generation tags: background work validates ownership

Even with disjoint trees, pages get detached (clear, retain, drop,
future migration) while the write-back hub, the cleaner, and any future
background actor hold owned refs to them. Today a detached page still
ACTS -- the resurrection bug was `remove_contains` on an orphan
consuming live tombstones.

**Proposal:** every `Node` carries an `owner: AtomicU64` stamped with
its tree's generation; structural detachment bumps the page's stamp to
a tombstone value (or the tree bumps its generation on surgery). Every
background touch (`build_cell`, deletion-lane processing, future
compactors) captures the expected stamp at enqueue and **compares
before acting; mismatch = skip**. One atomic store at detach, one load
at flush. This turns the entire orphan-work class -- past and future --
into no-ops, instead of relying on each surgery site remembering to
drain (the current fix) or neuter.

## 3. Durable tombstones: a deletion journal per tree

The deletion set is memory-only and shared by a split family. Two
consequences: any tree reload resurrects every uncompacted delete (the
pending-drop bug made this reachable MID-RUN; a genuine restart hits it
by design), and the delete's durability story is "whenever the page
next flushes", invisibly.

**Proposal:** a small per-tree tombstone journal cell, written by the
balancer's existing 60s checkpoint (append keys tombstoned since the
last checkpoint; drop entries whose pages have flushed -- compaction
already knows). Loads read the journal after the chain. Sizing: the
soak's worst case held ~50K uncompacted tombstones across 500+ trees --
per-tree journals are hundreds of bytes to a few KB. This closes
resurrect-on-reload completely and gives deletes a bounded durability
window (the checkpoint interval) instead of an unbounded one.

Cheaper interim if the journal waits: the index scrub grows a
"deletes" mode (it is insert-only repair today), and every reload path
logs at WARN what it may have resurrected (`caf09d6d` already added
this for hydrate).

## 4. The split lifecycle as an explicit, modeled state machine

The split today is a straight-line function whose failure arms each
hand-roll their own undo, coordinated by advisory flags
(`migration.is_some()`, `balancer_stopped`, `barrier_failed`). Most of
its historical bugs were transitions that half-happened.

**Proposal:** name the states -- Idle, Frozen, Built, Published,
Committed, Loaded (and, until proposal 1 lands, RolledBack) -- put the
transition rules in one place, and extend `docs/tla/StructuralSplit.tla`
to cover the full lifecycle including the background actors (the
flusher as a process, not an assumption). Every model in `docs/tla/`
has either found its bug or proven its fix; the split deserves the
full treatment, not just the freeze.

## 5. Observability of the invariants themselves

- **Scrub duplicate detector:** the single-copy-per-key invariant is
  load-bearing (the resurrection needed a double copy to forge) and
  currently unobservable -- client cursors dedup ids BY DESIGN, so a
  duplicate is invisible to every scan. The scrub should walk trees raw
  and count copies per key.
- **Dedup-drop telemetry:** the client cursor counts cross-block dedup
  drops; a nonzero rate in a steady state is a duplicate alarm.
- Keep the named-spin discipline (every unbounded loop counts and
  warns) and the `UNDELETE`/reload WARN tripwires added this campaign
  -- they are how the last three theories were eliminated in hours
  instead of days.

## 6. The soak and the fuzzer are part of the architecture

The import-shaped tests were green for years while eight
delete/storm-triggered defects sat reachable. The conjunction of
invariants is only visible under mixed workloads with failure
injection:

- `test_soak_ranged_index` (verified inserts + verified deletes + exact
  stripe audits + global roams + store-full storms) runs nightly at 1h,
  and 3h before any release touching `index/ranged`.
- The crash-churn fuzzer gains deletes, so tombstone durability
  (proposal 3) gets the same treatment inserts got.
- The regression-panic hook stays: `all` for short stress runs,
  `initial` for soaks.

---

## What this buys, against the campaign's own record

| defect (this campaign) | with 1 | with 2 | with 3 |
|---|---|---|---|
| off-page seek positioning | unaffected (fixed at source) | - | - |
| from_root null latch | unaffected (fixed) | - | - |
| runtime driver orphan | unaffected (bifrost/loops fixed) | - | - |
| rollback right-edge bounds | **unrepresentable** | - | - |
| scan truncation past emptied tree | unaffected (fixed) | - | - |
| statistical len() gate | unaffected (fixed) | - | - |
| pending-drop tree reload | reload becomes safe | - | **resurrection impossible** |
| orphaned-queue tombstone theft | **unrepresentable** | **no-op'd** | harmless |

Proposal 1 should land first: it deletes more code than it adds, both
of its primitives already exist and are tested, and it converts the
index's most dangerous operation into "build a copy, flip a pointer,
drop on failure".

---

## Finding #9 (soak attempt 12, recorded for follow-up)

Under PERMANENT store overload -- the workload's target exceeding
capacity for the final hour -- the write-back backlog grows without
bound: abandoned batches re-queue, new dirty pages keep arriving, and
RSS climbed 5GB -> 17GB over ~40 minutes while correctness held (912
exact audits clean, zero violations). A real deployment needs
back-pressure here: cap the retry lane and surface a hard "store full,
shedding index persistence" state instead of converting overload into
memory growth. Orthogonal to proposals 1-3; belongs with the
crash-safety work.
