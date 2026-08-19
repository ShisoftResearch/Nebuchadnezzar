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
| `SegmentTier.cfg` | FALSE | **`Recoverable` violated in 9 steps.** A bystander `archive()` lands while a promotion holds the segment `COLD\|LOCKING`. `is_settled_cold()` is false there, so the d8b0039c guard does not fire, and the patchwork image is written over the one good backup. The cell is then gone from memory *and* disk. |
| `NoBystander.cfg` | FALSE | **`HotReadable` violated in 12 steps**, with no bystander archiver at all. `try_reclaim_resident_blocks` is gated on `is_cold()`, which is true mid-promotion, and the promoter had already released its exclusive reference — so the sweeper takes the segment and `madvise`s away the image the promotion just restored. `set_hot()` then publishes the hole. |
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
