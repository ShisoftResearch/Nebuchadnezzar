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
