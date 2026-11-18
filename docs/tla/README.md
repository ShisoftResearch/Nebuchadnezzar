# TLA+ models

Models of the concurrent protocols in this repository, written when an
interleaving bug resisted reproduction. They are checked with TLC:

```sh
tlc -workers 4 -config <Config>.cfg <Model>.tla
```

## `SegmentTier.tla` — the hot/cold segment tier

Covers `ram/segs.rs`, `ram/tiered/eviction.rs` and `ram/tiered/promotion.rs`:
the `tiered_lock` states, the reference protocol, block residency, and what a
read returns from a mapping whose pages have been dropped (zeros, silently —
which is why this class of bug is so quiet).

Written for task **#71**: two cells of 4.2M reading back as `Id(0)` from a
segment reporting `HOT, not dirty, offset INSIDE the written range, promoted
at ...`. Six 16 GiB reproduction runs found nothing; TLC found two distinct
counterexamples in under a second.

| config | `Fixed` | result |
|---|---|---|
| `SegmentTier.cfg` | FALSE | **`Recoverable` violated, a nine-state trace.** A bystander `archive()` lands while a promotion holds the segment `COLD\|LOCKING`. `is_settled_cold()` is false there, so the d8b0039c guard does not fire, and the patchwork image is written over the one good backup. The cell is then gone from memory *and* disk. |
| `NoBystander.cfg` | FALSE | **`HotReadable` violated, a twelve-state trace**, with no bystander archiver at all. `try_reclaim_resident_blocks` is gated on `is_cold()`, which is true mid-promotion, and the promoter had already released its exclusive reference — so the sweeper takes the segment and `madvise`s away the image the promotion just restored. `set_hot()` then publishes the hole. |
| `Fixed.cfg` | TRUE | **No error.** Exhaustive: 42 distinct states, depth 17. |
| `Sanity.cfg` | TRUE | `ReachesAPromotedHotSegment` is violated **on purpose**. It asserts the interesting state is unreachable, so its counterexample is a full evict → fault → promote round trip. If this one ever *passes*, the model has gone vacuous and the clean run above means nothing. |

The three modelled fixes, all in the code now:

1. `promote_segment` holds its `SegmentExclusiveRefGuard` across the whole
   restore. It used to bind it inside the acquisition loop, so `break` dropped
   it before a single byte was read.
2. `try_reclaim_resident_blocks` requires `is_settled_cold()`, not `is_cold()`.
3. `archive()` refuses on `image_is_partial()` — the actual question — rather
   than on `is_settled_cold()`, which was a proxy for it that missed the
   promotion window.

### Reading the model against the code

`mem[s]` is page *content*, not a residency bit: `"z"` is what a read returns
after `MADV_DONTNEED`, because the mapping survives. Promotion's restore is
modelled slot by slot, since an 8 MiB `ptr::copy_nonoverlapping` is not atomic
against a concurrent `madvise` — that is what turns a whole-segment wipe into
the two-cell hole that was actually measured.

`Recoverable` is the bytes existing *somewhere*; `HotReadable` is the bytes
being in memory once the segment reads as hot, because at that point a reader
goes straight to the mapping and there is nowhere else to look.

## TxnChainRecovery — the head-pool / bracket-chain commit protocol

Models the settled Phase 6a design (docs/crash-safety-plan.md): chain
members with volatile/durable states, the settled commit sequencing
(members durable → COMMIT written → COMMIT durable → ack), physical
abort by rewind, the cleaner (dissolving members to plain data and
dropping old COMMIT records), the decided-watermark, and crash+recovery
with the reduction rule.

| Config | Gates | Expected |
|---|---|---|
| `TxnChainFixed.cfg` | both on | **Passes.** NoPartialInstall, AckedSurvives, MarkIsDecided, InstalledWasCommitted. |
| `TxnChainNoCleanerGate.cfg` | cleaner gate off | `InstalledWasCommitted` violated: the cleaner dissolves an undecided member into plain data, which recovery installs at face value. |
| `TxnChainNoWatermarkGate.cfg` | watermark gate off | `InstalledWasCommitted` violated: the mark covers an undecided transaction and the marked rule surfaces its members — a COMPLETE install of an uncommitted transaction, which is why NoPartialInstall alone cannot catch it. |
| `TxnChainSanity.cfg` | both on | `NeverAckedThenRecovered` violated **on purpose** — its counterexample is a full write → sync → commit → ack → crash → install round trip. If this ever passes, the model has gone vacuous. |

Two lessons TLC taught while the model was being written, both real:

1. **The rewind path must key on transaction state, never on the log.**
   Guarding abort on "no COMMIT record present" let a committed, acked
   transaction abort after the cleaner legitimately dropped its record
   below the watermark.
2. **"Installed implies committed" is a separate invariant from "no
   partial install."** The watermark-gate bug installs transactions
   atomically — completely, and completely wrongly.

## `BLinkSeek.tla` — the B-link seek's off-page positioning

Covers the READ path of `src/index/ranged/tree/btree` (`search.rs
search_node`, `node.rs key_at_right_node`, `cursor.rs initialize` /
`load_following_page`) against concurrent leaf front-inserts and leaf
splits, with the reader allowed to enter the leaf level left of its
target (internal separator lag, which B-link readers must tolerate).

Written for the 2026-08-31 "fresh root descent regressed" give-up storms
(`docs/RANGED_INDEX_OPEN_ISSUES.md` Issue 1): seeks returning keys
ordered BEFORE their seek key, 65 fresh descents in a row, ~8,700
give-ups per import — including on a fresh store with ZERO structural
splits, so plain insert+seek concurrency had to be sufficient. It is.

The defect: when a search key lands past every key of a page
(`pos == n.len`), the old code built an empty-snapshot cursor whose
`initialize()` later yields the FIRST KEY OF THE NEXT PAGE, read at a
later time and never compared against the seek key. A front-insert
landing in the sibling between the (unvalidated, tearable) peek that
decided "don't slide right" and that deferred read hands the seek a key
below its target. The fix continues the descent at the sibling through
the existing retry channel, so a key is only ever yielded by a validated
lower-bound search on the node that answers.

| config | semantics | result |
|---|---|---|
| `BLinkSeekBuggyOrganic.cfg` | old, honest peeks | **`SeekGE` violated, five states**: enter left of target, honest "sibling starts past K" peek, off-page, front-insert lands below K in the sibling, deferred follow read yields it. No tearing needed. |
| `BLinkSeekBuggyTorn.cfg` | old, torn peeks | **`SeekGE` violated** (also without the racing insert). |
| `BLinkSeekFixed.cfg` | new, torn peeks | **No error.** Exhaustive: 3,463 distinct states, depth 10, with torn peeks, concurrent inserts and splits all enabled. |
| `BLinkSeekReach.cfg` | new | `OffPageUnreachable` violated **on purpose** — its counterexample walks the off-page path. If this one ever passes, the model has gone vacuous and the clean run above means nothing. |

## `CopySplit.tla` — the copy-based split lifecycle, with the flusher

Models the split of `docs/ranged-index-robustness-plan.md` proposal 1
(implemented in `a16fd84d`): build a copy of the live keys at or past
the pivot on fresh pages, publish, flip placement, then retain on the
source; abort before the flip drains and drops the copy. The write-back
flusher is a MODELED PROCESS, not an assumption, because every
resurrection of the 2026-08-31 soak campaign was `remove_contains`
pairing a page's key against a deletion set while that key existed in
two places.

Freeze fidelity is what makes it interesting: the source is frozen for
the whole window, but the copy starts serving at the placement flip and
is NOT frozen, so deletes of moved keys land during the commit->retain
window while the source still physically holds those keys.

| config | toggles | result |
|---|---|---|
| `CopySplitFixed.cfg` | disjoint sets, filtered copy, detach on abort | **No error.** Exhaustive: 630 distinct states. |
| `CopySplitShared.cfg` | copy SHARES the source's deletion set | **`NoResurrection` violated in six states** — and it caught a live bug in the first implementation. Commit; a delete of a moved key routes to the (unfrozen) copy and tombstones the shared set; the source flushes a page it has not yet retained, pairs the key against that tombstone, consumes it, and the copy's key is visible again. Fixed by giving the copy its own set. |
| `CopySplitUnfiltered.cfg` | copy walk does not skip tombstoned keys | **`NoResurrection` violated.** A key tombstoned before the split exists twice with one tombstone; whichever page flushes first frees the other copy. |
| `CopySplitNoDetach.cfg` | aborted copy's pages still accept flushes | **`NoResurrection` violated** — the abandoned pages consume live tombstones (the campaign's `1d8440b8`/`94959498` shape). |
| `CopySplitReach.cfg` | fixed | `LivenessSanity` violated **on purpose**: its counterexample is a delete landing on the copy inside the commit->retain window. If this passes, the model is vacuous. |

The lesson generalizes past this split: **shared mutable state between
two trees is only safe while their PAGES are shared.** The shared
deletion set was correct for the shared-leaf design and became a defect
the moment ownership was made disjoint — an invariant that changed
meaning under a refactor, which is exactly the class of thing a model
catches and a test suite does not.

## `HeadWordLock.tla` — cell words, write heads, the GC lock

Covers the lock order of one chunk in `ram/chunk.rs`: the word lock of each
cell (the mutex bit of its cell-index word), the head pool (one owner per
head), the GC lock and transaction leases. The processes are head-first
writers (writes, upserts, CAS), word-first updaters (`update_cell_by`),
deletes, the cleaner and a transaction. `HeadWordLock.py` writes one MC
module per scenario, runs TLC and compares each result with the expected
one:

```sh
TLC=tlc python3 HeadWordLock.py --out /some/scratch/dir
```

| scenario | result |
|---|---|
| `old_k1`, `old_k2` | Deadlock. The delete order before e6a64539: the delete holds the word and waits for a head, the head owner waits for the word. |
| `cur_*` | No error. The e6a64539 order: the delete releases the word before it waits for a head. |
| `new_*` | No error. The current order: the delete reserves its tombstone under the word that checked the cell if a head is free; the try has a bound, then the e6a64539 path runs. |
| `ctl_new_unbounded_try` | Deadlock, on purpose: the try without a bound is the old order again. |
| `ctl_new_no_recheck` | `AppendUnderBoth` violated, on purpose: a word taken again after a wait must check the cell again. |
| `full_old`, `full_cur`, `full_new` | No error. At a full store an absent cell or a false predicate gives "absent", never "no space". |
| `new_u_bounded_c1`, `new_u_bounded_c1_k3` | No error. A word-first updater holds the word and waits for a head while every head owner waits for that word (cycle C1). The updater refuses at the head-queue deadline and releases the word. |
| `cur_u_unbounded`, `new_u_unbounded` | Deadlock, on purpose: C1 without that deadline, which was the code before the C1 fix. |
| `ctl_cas_unbounded_try` | Deadlock, on purpose: the CAS free-head try without its bound. |
| `full_cas` | No error. At a full store a CAS on an absent cell (a lost CAS in the model) reports a mismatch, never "no space". |
| `cas_wordfast_*` | No error. The current CAS order: check the version, write and publish under one word hold if a head is free; else the bounded try ends, the word is released, and the CAS checks again under a new hold after it gets a head. |

## `SealReclaim.tla` — the seal job and the cleaner

Covers one rotated segment in `ram/chunk.rs` (`finish_rotated_head`) and
`ram/cleaner` (`Protection::blocks_cleaner`, `SegmentCandidate::for_cleaner`).
The seal job drains the segment, syncs its WAL and archives it. A mostly
dead segment first waits for the cleaner for a bounded number of attempts.
The archive and the cleaner claim exclude each other.

Two writers touch the segment. The first write is in flight at the rotation,
and the drain waits for it. The second write is a late writer. It loaded the
head before the rotation and owns it later, so it can start after the drain
while the journal gate is open. The D46 review found this case (F1): with
`e07b3561` the cleaner removed a synced segment under such a write, and the
acknowledged cell was lost. The fix is `CloseGate`: the seal job closes the
journal gate before the drain, and the gate never reopens
(`Protection::rotated`). The model also has the WAL syncer, which needs
`Protection::job`, and the other cleaner checks (owned head, pending journals,
`wal_sync_queued`). The constant `OtherChecks` switches these checks off, to
show what the seal rule alone guarantees.
`SealReclaim.py` writes one cfg per scenario, runs TLC and compares each
result with the expected one:

```sh
TLC=tlc python3 SealReclaim.py --out /some/scratch/dir
```

The model does not cover the gate reopen; the Rust control `gate_reopen` covers it.

| scenario | result |
|---|---|
| `synced_wait` | No error. The current rule: the cleaner may take a seal-pending segment after the drain and the WAL sync; the gate is closed before the drain; the seal job waits for the cleaner at most `Waits` times, then archives. 67 distinct states. |
| `synced_live` | No error. A segment that is not mostly dead is archived without a wait. |
| `synced_rule_only` | No error. The same as `synced_wait`, with no other cleaner check: the seal rule and the gate close are enough. |
| `d20` | No error. The rule of 15370976: the cleaner never takes a seal-pending segment. |
| `ctl_synced_no_close` | `NoLostLateWrite` violated, on purpose: the `synced` rule without `CloseGate` (`e07b3561`). A late writer appends after the drain, and the cleaner removes the segment without the write. |
| `ctl_none` | `NoLostLateWrite` violated, on purpose: no rule and no gate close (`441c1ff7`). |
| `ctl_no_gate`, `ctl_no_gate_wait` | `NoLostWrite` violated, on purpose: without a rule and without other cleaner checks, the cleaner copies the segment before its in-flight write lands. |
| `ctl_unbounded_wait` | `SealEnds` violated, on purpose: without the bound, a seal job waits forever for a cleaner that never reclaims. |

## `PublicationOrder.tla` — what a reader outside a transaction sees

Covers `DataManager::apply_commit_ops` (`publication_order`) and
`DataManager::rollback` in `src/server/transactions/data_site.rs`. A site
publishes the cells of a transaction one at a time, at prepare, and a reader
that takes no part in the transaction reads one cell at a time. The model is
general: each cell may refer to one other cell, and the store holds no
dangling reference before and after the transaction.

Written for the 2026-11-14 FlyWire v783 import: 1 to 3 edge groups failed per
run, and a reader of the same list logged `Cannot find next key` and returned
the list before the gap. The apply published cells in id order, so a list
head could name its new segment before the segment existed; the rollback
walked the same order.

| config | orders | result |
|---|---|---|
| `PublicationOrderFixed.cfg` | new, changed, removed; undo removed, changed, new | **No error.** 741,046 distinct states (3 cells). |
| `PublicationOrderFixedN4.cfg` | the same, 4 cells | **No error.** 2,608,105 distinct states. |
| `PublicationOrderReaderCommit.cfg` | the same, a walking reader, a commit with no removal | **No error.** 141,901 distinct states. |
| `PublicationOrderIdOrder.cfg` | any order (the code before) | `ReachableIntact` violated in 2 states. |
| `PublicationOrderIdOrderNoRemove.cfg` | any apply order, no removal, no abort | `ReaderNeverMissing` violated: the order alone breaks readers. |
| `PublicationOrderUndoExistingFirst.cfg` | undo changed and removed cells in id order, then new | `ReachableIntact` violated: a changed cell gets its reference to a removed cell back before that cell. The first draft of the fix did this. |
| `PublicationOrderTwoSites.cfg` | the fixed orders, cells on 2 sites | `ReachableIntact` violated, by design: each site publishes on its own. |
| `PublicationOrderReaderAbort.cfg` | the fixed orders, a reader, an abort | `ReaderNeverMissing` violated, by design: a reader already past the changed cell meets a created cell that the rollback removed. |
| `PublicationOrderReaderWithRemove.cfg` | the fixed orders, a reader, removals | `ReaderNeverMissing` violated, by design: the same for a cell the commit removes. |
| `PublicationOrderSanity.cfg` | fixed | `Sanity` violated **on purpose**: a partly published transaction is reachable, so the passes above are not vacuous. |

What the fix gives, then: on one site, no cell that a reader can reach refers
to a missing cell at any step of the apply or the rollback. What it does not
give: a reader already in flight can meet a removed cell, and a transaction
over two sites is published on each site on its own. A reader that walks a
structure outside a transaction must report a missing cell (Morpheus:
`IdListError::MissingSegment`), never a shorter answer.

Not modelled, and not covered by the guarantee:

- A restore that fails. `rollback` records the cell in `unrestored` and goes on
  with the next class, and `retry_restoration` later restores the `unrestored`
  cells in id order. A failed restore of a removed cell can thus leave a
  restored cell that refers to it until the retry succeeds.
- The Thomas write rule skip in `apply_commit_ops`: a skipped write of a new
  cell together with an applied update that names it would leave a dangling
  reference. Hypothesis (not verified): this cannot happen, because the cell
  locks are held from prepare and the commit timestamp follows the prepare
  reply.

## `OccCommit.tla` — the OCC transaction, the commit, the fence and recovery

Covers the client retry loop (`src/client/mod.rs` `transaction`), the
coordinator (`src/server/transactions/manager.rs`: `do_prepare`,
`sites_commit`, `sites_end`, `end_sites`, `abort_sites`), the participant
(`src/server/transactions/data_site.rs`: `prepare`, `prepare_read`,
`apply_commit_ops`, `end_transaction_locked`, `close_committed_brackets`,
`abort_locked`, `presume_abort`, `finish_aborted`, the lease sweep and the
participant fence `terminate_in_doubt` / `decide_in_doubt` / `fence_here`),
a site crash with the recovery of `src/server/mod.rs`
(`resolve_in_doubt_after_recovery`, `discard_in_doubt`,
`apply_resolved_commit`), and one plain write (`src/server/cell_rpc.rs`
`write_cell`). Each action names its source line. Time is abstract: every
timeout can fire when its guard in the code holds. RPCs are lost before or
after the target. The coordinator can stop at any step. A site crash is a
process crash: appended records survive, memory is lost.

Workloads (2 cells, `x` on site 1, `y` on site 2): `LU` two read-modify-write
transactions on `x`; `WS` write skew; `MS` two transactions that read and
write both cells; `M1` and `R1` one transaction over two sites (`R1` writes
only `x`, so site 2 is a read-only part); `RO` a writer and a read-only
transaction. `Retry` adds a second attempt of client 1.

Invariants: `Atomic` (no participant commits while another aborts, over every
decision a site ever recorded), `ToldCommittedTrue` and `ToldFailedTrue` (the
outcome told to the client is the real one; `unknown` is always allowed),
`OneCommitPerClient` (a retried closure takes effect once), `SerialRW` (the
committed read-write transactions read as in a serial order), `FinalSerial`,
`QuiescentNoStuckLock`, `QuiescentNoLock`, `QuiescentNoObject`, and
`NoLeftovers`: when nothing more happens, the store is a serial history of
the committed transactions (no cell of an uncommitted transaction), every
committed transaction is complete on every participant, and no lock and no
participant object is left. `PlainKept`: an acknowledged plain write is not
lost. `NoLeftoverLocks`: the lock and object part of `NoLeftovers`.

Fix toggles: A `FixEndAborted`, B `FixStaleSweep`, C `FixReadReclaim`, D
`FixPendingUnknown`, E `FixDurableDecision`, F `FixNoReclaimDecided`, G
`FixPlainRespectsLocks`, F3 `FixApplyFenced`, F4 `FixROValidate`.
`PlainAtomic` (TRUE in the code) makes the check of fix G and the plain
write one step; FALSE is a control in which the write checks and then
writes. `PlainPrepareWindow` (TRUE in the code since the word-lock check)
also admits a plain write while the owner of the cell is Prepared and not
applied: the code reads the lock flag under the cell word lock, and a write
of an absent cell has no word that orders it with a prepare that expects
the cell absent. The apply checks the version again, so such a write makes
the transaction fail, not leave anything. A, B, D, F, G, F3 and F4 are in the code (`fix/neb-txn`); C and E
are rejected candidates.

Two invariants were added with the fixes: `NoApplyAfterFence` (F3: no apply
runs on an object that a participant fenced; a ghost field `lf` of the
object marks it) and `NoLeftoversPlain` (with a plain write: at the end no
cell holds a write of a transaction that is not committed, and each
committed transaction is complete on every participant).

### The shipped configs

Every `OccShipped*` config has the fixes that are in the code on, and every
fault on: lost RPCs, coordinator stop, site crash and recovery, restore and
apply failure, refused close, stale reclaim, timeouts.

| config | workload | invariants | result | distinct states | depth |
|---|---|---|---|---|---|
| `OccShippedM1.cfg` | M1 | Atomic, ToldCommittedTrue, ToldFailedTrue, OneCommitPerClient, SerialRW, NoLeftovers, NoApplyAfterFence, QuiescentNoStuckLock | no error | 90,198 | 27 |
| `OccShippedM1Crash2.cfg` | M1, 2 crashes | the same | no error | 154,059 | 28 |
| `OccShippedM1Plain.cfg` | M1 + plain write | Atomic, ToldCommittedTrue, ToldFailedTrue, PlainKept, NoLeftoverLocks, NoLeftoversPlain, NoApplyAfterFence | no error | 199,887 | 28 |
| `OccShippedR1.cfg` | R1 | as M1 | no error | 2,658 | 17 |
| `OccShippedLU.cfg` | LU + retry | as M1 | no error | 153,830 | 30 |
| `OccShippedRO.cfg` | RO | as M1 + SerialAll | no error | 2,107,014 | 33 |
| `OccShippedWS.cfg` | WS | as M1 | no error | 9,769,496 | 33 |
| `OccShippedMS.cfg` | MS | as M1 | incomplete: no error in 30,737,605 distinct states at BFS level 23; the 12 GB scope stopped TLC with 11,411,626 states on the queue | 30,737,605 (partial) | – |

Controls and sanity for the new fixes (each is violated on purpose):

| config | what it shows | result | distinct states | depth |
|---|---|---|---|---|
| `OccShippedM1PlainWord.cfg` | `OccShippedM1Plain` with fix G as the word-lock check (`PlainPrepareWindow`): a plain write is also admitted while the owner of `x` is Prepared and not applied | no error | 210,861 | 28 |
| `OccShippedPlainSplitWord.cfg` | the same with a plain write that checks, then writes | `NoLeftoverLocks` violated | 25,099 | 15 |
| `OccShippedNoFenceCheck.cfg` | shipped without F3: a delayed apply runs on a fenced site | `NoApplyAfterFence` violated | 2,535 | 13 |
| `OccShippedPlainSplit.cfg` | shipped with a plain write that checks, then writes | `NoLeftoverLocks` violated | 26,423 | 15 |
| `OccShippedNoROValidate.cfg` | shipped without F4 | `SerialAll` violated | 22,004 | 14 |
| `OccROSkewFix.cfg` | `OccROSkew` with F4 only | no error | 10,809 | 30 |
| `OccSanityRO.cfg` | a validated read-only transaction commits and the system settles | `SanityNoROCommit` violated | 647,931 | 21 |
| `OccSanityStaleEnd.cfg` | fix B ends a Prepared write participant (no lease: it did not apply) while the slow coordinator has not sent it the apply | `SanityStaleEndBeforeApply` violated | 198 | 8 |

Fix B and a slow coordinator: a participant becomes `Prepared` at its
prepare and gets its writes and its leases only at the apply (`commit` RPC),
so every `Prepared` write participant holds no lease. Its yes vote is the
apply, not the prepare: the coordinator sends `end` (the decision) only
after every apply answered Success (`sites_commit`). After fix B ended the
object, a late apply finds it Aborted or gone and is refused, so the
coordinator can never decide COMMIT for it. `OccSanityStaleEnd` shows that
the model reaches this case; the `OccShipped*` configs, in which time is
abstract (a coordinator can be slow beyond any bound), pass `Atomic` and
`NoLeftovers` with it.

The configs of the table above this section were checked again with the new
constants (`PlainAtomic = TRUE`, `FixApplyFenced = FALSE`, `FixROValidate =
FALSE`), so they still model the code before the fixes. Their state counts
change by the ghost field `lf` (for example `OccSanity` 4,924, `OccROSkew`
387, `OccM1Faults` 15,171, `OccM1PlainFixGAll` 293,829); each result is the
same.

| config | expected | result | distinct states | depth |
|---|---|---|---|---|
| `OccSanity.cfg` | `SanityNoMultiCommit` violated on purpose | violated | 4,752 | 15 |
| `OccSanityFence.cfg` | `SanityNoFenceAbort` violated on purpose | violated | 65 | 13 |
| `OccSanityRetry.cfg` | `SanityNoRetryCommit` violated on purpose | violated | 573 | 17 |
| `OccControlNoFence.cfg` | control: presumed abort without the fence (before D29) | `Atomic` violated | 55 | 13 |
| `OccControlLU.cfg` | control: no version check | `SerialRW` violated (lost update) | 255 | 17 |
| `OccLU.cfg` | the code, lost-update workload, retry, stale reclaim, timeouts | no error | 801 | 27 |
| `OccWS.cfg` | the code, write skew | no error | 11,167 | 31 |
| `OccMS.cfg` | the code, two 2-site transactions | no error | 556,859 | 47 |
| `OccRO.cfg` | the code, a read-only transaction | no error | 6,649 | 27 |
| `OccROSkew.cfg` | read-only transactions have no snapshot (by design) | `SerialAll` violated | 428 | 15 |
| `OccLUFaultsCode.cfg` | the code, every fault, no crash: failure, once, atomic, serial | no error | 55,205 | 29 |
| `OccM1Faults.cfg` | the code, every fault | F1: `ToldCommittedTrue` violated | 13,447 | 18 |
| `OccR1F1.cfg` | the same, 1 writing site | F1 | 719 | 15 |
| `OccLUFaults.cfg` | the same, 2 transactions and a retry | F1 | 1,296 | 10 |
| `OccM1Leak.cfg` | coordinator stops | F2: `QuiescentNoLock` violated | 10 | 6 |
| `OccR1Lock.cfg` | lost RPCs only | F2 | 101 | 10 |
| `OccR1Faults.cfg` | A, every fault | F2 | 24 | 7 |
| `OccM1CrashCode.cfg` | A, crash, refused close | F5: `ToldCommittedTrue` violated | 23,456 | 19 |
| `OccM1Crash2Code.cfg` | A, 2 crashes, refused close | F5 | 7,768 | 19 |
| `OccLUCrashAll.cfg` | A, B, D, every fault, crash | F7: `SerialRW` violated | 138,315 | 23 |
| `OccM1Plain.cfg` | A, B, D, plain write | F6: `NoLeftoverLocks` violated | 862 | 15 |
| `OccM1PlainCrash.cfg` | the same, crash | F6 | 2,668 | 15 |
| `OccPlainKept.cfg` | plain write while in doubt after a restart | F6: `PlainKept` violated | 9,500 | 18 |
| `OccM1FaultsFixA.cfg` | A | no error | 33,983 | 26 |
| `OccR1F1FixA.cfg` | A | no error | 732 | 16 |
| `OccLUFaultsFixA.cfg` | A | no error | 53,137 | 29 |
| `OccWSFaults.cfg` | A, every fault | no error | 1,954,335 | 32 |
| `OccR1FaultsFixB.cfg` | A, B | no error | 1,087 | 16 |
| `OccM1LeakFixB.cfg` | A, B, every fault | no error | 34,135 | 26 |
| `OccWSFaultsFixB.cfg` | A, B, every fault, quiescence set | no error | 3,765,680 | 32 |
| `OccR1FaultsFixC.cfg` | A, C, safety set | no error | 732 | 16 |
| `OccM1Crash.cfg` | A, B, D, crash, `NoLeftovers` | no error | 30,984 | 26 |
| `OccM1CrashAll.cfg` | A, B, D, every fault, crash, `NoLeftovers` | no error | 90,998 | 27 |
| `OccR1CrashAll.cfg` | the same, 1 writing site | no error | 2,658 | 17 |
| `OccWSCrash.cfg` | A, B, D, write skew, crash, `NoLeftovers` | no error | 3,571,950 | 30 |
| `OccM1CrashFixD.cfg` | A, B, D, 2 crashes, refused close, `NoLeftovers` | no error | 67,368 | 27 |
| `OccM1CrashFixE.cfg` | A, B, E, the same | no error | 64,169 | 27 |
| `OccLUCrashFixF.cfg` | A, B, D, F, every fault, crash, `NoLeftovers` | no error | 153,830 | 30 |
| `OccM1PlainFixG.cfg` | A, B, D, G, plain write, crash | no error | 12,982 | 26 |
| `OccM1PlainFixGAll.cfg` | A, B, D, F, G, every fault, crash, plain write | no error | 202,151 | 28 |

The findings F1 to F7 and the fix designs are in the model-check findings
file of agent A (`/mnt/ext/morpheus-fixes/model-check/findings/A_neb_occ.md`).

Not modelled: power loss (unsynced records lost), the status-cell release
and the eviction of the decision map, creation and removal of cells, index
entries (a cell value stands for the cell and its entries), more than one
cell per site, the read pins, the slot seal and the write fence. Clocks are
taken as causal. With causal clocks the Thomas write rule skip in
`apply_commit_ops` cannot fire for a transaction that holds the lock: the
previous writer applied before this transaction's prepare took the lock, the
site observed that writer's clock, and the coordinator observed the site's
clock in the prepare reply before it stamped the commit (Read, not checked).

## `IndexRepair.tla` — the repair of a failed index task

A Neb copy of the `WriteIndex` model of Morpheus (agent D, Morpheus
`docs/tla/WriteIndex.tla` at `424ac459`), with the repair that is in the
code (`src/index/repair.rs`, finding D-F11). One cell, transactional and
plain writes, index tasks that can fail, a crash with the recovery
reconcile, the repair worker as 4 separate steps (take the generation, read
the cell, re-assert its entries, remove the record if the generation did
not change), and C3: a query of a schema with a pending repair answers
`IndexRepairPending`. `NoSilentMiss`: at a quiet moment a query for the
cell's value finds the cell or answers an error. `RepairEnds` (liveness,
`FairSpec`): every repair record is worked off.

| config | expected | result | distinct states | depth |
|---|---|---|---|---|
| `IndexRepairShipped.cfg` | pass | no error | 26,236 | 22 |
| `IndexRepairLive.cfg` | pass (`RepairEnds`) | no error | 26,236 | 22 |
| `IndexRepairNoRefusal.cfg` | the repair without C3: a query misses the row while the repair is pending | `NoSilentMiss` violated | 158 | 4 |
| `IndexRepairCode.cfg` | the code before the fix | `NoSilentMiss` violated | 99 | 4 |
| `IndexRepairNoGen.cfg` | the worker removes a record without the generation check | `NoSilentMiss` violated | 10,210 | 11 |
| `IndexRepairNoReconcile.cfg` | a crash loses the records and the reconcile is off | `NoSilentMiss` violated | 73 | 3 |
| `IndexRepairSanity.cfg` | `SanityRepaired` violated on purpose | violated | 2,687 | 8 |

C3 is checked by every index reader of the process: `IndexedDataClient` and
every ranged seek (`RangedIndexerClient::seek`, so also `client::ranged`).
The records are per process and per database; a reader in another process
does not see them, and the model has one process.

The records are memory, not a WAL record: with the recovery reconcile on (the
default), a crash needs no record (`IndexRepairShipped` has the crash on).
The worker only inserts; a stale entry stays until the full scrub prunes it,
and a query drops it when it reads the candidate cell. Not modelled:
concurrent plain writes of one cell, which can reorder their index tasks.

## `ShutdownWriteBack.tla` — client writes and the index write-back at shutdown

The graceful shutdown of one database and its ranged-index write-back
(triage 2026-11-17, D4). The flush takes the write-back barrier, publishes
the tree heads, drains again and resets the hub; the reset discards what is
still queued. Before `fix/shutdown-close-first` only the default database
closed its client writes before the flush. The loop for the other loaded
databases closed writes after the flush, so a client write (the Morpheus
sidecar rebuild on Genoa) landed after the hub reset: its index entry was
lost, and after a split a durable page could link to a page that never
landed. The fix closes the client writes of every database, and drains its
index tasks, before any flush of the loop. The loop also skips the default
database, which steps 0-1.6 already flushed and archived.

Mapping: `CLOSE_CLIENTS_FIRST` = `close_client_writes` before the flush
(`NebServer::shutdown`, step 0 and step 1.7); `Publish` = the barrier and
`publish_head` in `flush_all_trees`; `Reset` = `WriteBackHub::reset`;
`Close` = `close_writes` + `archive_all`. Pages land in any order and as
they are now (coalesced dirty pages).

| config | expected | result | distinct states | depth |
|---|---|---|---|---|
| `ShutdownWriteBackShipped.cfg` | pass (`NoMissingPage`, `IndexCoversCells`) | no error | 59 | 11 |
| `ShutdownWriteBackControlIndex.cfg` | the code before: an acked entry is lost | `IndexCoversCells` violated | 103 | 9 |
| `ShutdownWriteBackControlPage.cfg` | the code before: a page links to a page that never landed | `NoMissingPage` violated | 133 | 11 |
| `ShutdownWriteBackSanity.cfg` | violated on purpose | violated | 115 | 10 |

Test: `server::tests::a_secondary_database_written_during_shutdown_keeps_every_acked_index_entry`
writes to a secondary database during the shutdown and checks, with the
recovery scrub off, that every acknowledged cell scans after the restart.
Before the fix: 3 of 3 runs failed (255,164 of 345,138; 340,845 of 359,131;
358,090 of 400,000 entries). After the fix: 3 of 3 runs passed.
