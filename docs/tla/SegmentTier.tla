------------------------------ MODULE SegmentTier ------------------------------
(***************************************************************************)
(* The hot/cold segment tier protocol of `src/ram/segs.rs`,                 *)
(* `src/ram/tiered/eviction.rs` and `src/ram/tiered/promotion.rs`.          *)
(*                                                                         *)
(* Written to explain a measured failure (task #71): after a promotion, two *)
(* cells of one segment read back as zeros while the segment reported HOT   *)
(* and not dirty, and every other cell in the same segment was intact.      *)
(*                                                                         *)
(* Modelling choices, each traceable to the code:                          *)
(*                                                                         *)
(*  - `st` is the segment's `tiered_lock`. The four reachable values are    *)
(*    HOT, HOT|LOCKING, COLD, COLD|LOCKING. `is_hot()` masks the locking    *)
(*    bit off (HOT_COLD_MASK = 0x7F), so "hotL" still reads as hot; and     *)
(*    `is_settled_cold()` is the exact equality st = COLD.                  *)
(*                                                                         *)
(*  - `refs` is the reference counter. "excl" is EXCLUSIVE_REF_COUNT, which *)
(*    `incr_references` refuses to compete with.                            *)
(*                                                                         *)
(*  - `mem[s]` is the CONTENT of a page/block of the segment's anonymous    *)
(*    mapping: "d" for the cell's bytes, "z" for what a read returns after  *)
(*    MADV_DONTNEED. That is the whole reason this class of bug is silent:   *)
(*    the mapping survives, so a stale pointer reads zeros, not a fault.    *)
(*                                                                         *)
(*  - Promotion's restore is modelled slot by slot, because the failure to  *)
(*    explain is a PARTIAL hole. `ptr::copy_nonoverlapping` of 8 MiB is not *)
(*    atomic against a concurrent madvise.                                  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Slots,            \* the pages (or backup blocks) of one segment
          SettledColdGuard, \* TRUE models the d8b0039c archive refusal (#70)
          Bystanders,       \* TRUE lets a thread that owns nothing call archive()
          Fixed             \* TRUE models the #71 fix, all three parts of it

Content == {"d", "z"}       \* cell bytes / zeros

VARIABLES
    st,       \* tiered_lock
    refs,     \* shared reference count
    excl,     \* EXCLUSIVE_REF_COUNT is held
    mem,      \* [Slots -> Content]  what the mapping currently reads as
    bak,      \* [Slots -> Content]  what the backup file holds
    hasBak,   \* has a backup ever been written
    res,      \* SUBSET Slots: block_residency's present set
    dirty,    \* dirty_seq > archived_seq
    partial,  \* BlockResidency.pages_dropped: the mapping is not a whole image
    copied,   \* promotion's memcpy progress
    pc        \* per-thread program counter

vars == <<st, refs, excl, mem, bak, hasBak, res, dirty, partial, copied, pc>>

Threads == {"evict", "promote", "reclaim", "fault", "archive"}

IsHot         == st \in {"hot", "hotL"}
IsCold        == st \in {"cold", "coldL"}
IsSettledCold == st = "cold"

TypeOK ==
    /\ st \in {"hot", "hotL", "cold", "coldL"}
    /\ refs \in 0..2
    /\ excl \in BOOLEAN
    /\ (excl => refs = 0)
    /\ mem \in [Slots -> Content]
    /\ bak \in [Slots -> Content]
    /\ hasBak \in BOOLEAN
    /\ res \subseteq Slots
    /\ dirty \in BOOLEAN
    /\ partial \in BOOLEAN
    /\ copied \subseteq Slots

Init ==
    /\ st = "hot"            \* a full, live, never-yet-archived segment
    /\ refs = 0
    /\ excl = FALSE
    /\ mem = [s \in Slots |-> "d"]
    /\ bak = [s \in Slots |-> "z"]
    /\ hasBak = FALSE
    /\ res = {}
    /\ dirty = TRUE
    /\ partial = FALSE
    /\ copied = {}
    /\ pc = [t \in Threads |-> "idle"]

(***************************************************************************)
(* Shared helpers                                                           *)
(***************************************************************************)

\* SegmentExclusiveRefGuard::new -- CAS 0 -> EXCLUSIVE
CanTakeExclusive == refs = 0 /\ ~excl

\* Segment::archive(). Reads the segment's MEMORY, so a non-resident page is
\* archived as zeros. This is what d8b0039c guards against for settled-cold
\* segments; eviction archives with the locking bit set and is still allowed.
DoArchive ==
    /\ bak' = mem
    /\ hasBak' = TRUE
    /\ dirty' = FALSE

\* The pre-fix guard tests the segment's STATE; the fixed one tests the actual
\* question, whether the mapping still holds a whole image. They differ in
\* exactly one window -- COLD|LOCKING, where a promotion lives.
ArchiveAllowed ==
    IF Fixed THEN ~partial
             ELSE ~(SettledColdGuard /\ IsSettledCold)

(***************************************************************************)
(* Evictor: tiered/eviction.rs evict_segment                                *)
(* Holds the exclusive guard across the WHOLE function.                     *)
(***************************************************************************)

EvictStart ==
    /\ pc["evict"] = "idle"
    /\ st = "hot"
    /\ CanTakeExclusive
    /\ excl' = TRUE
    /\ st' = "coldL"                      \* lock_hot_to_cold
    /\ pc' = [pc EXCEPT !["evict"] = "archive"]
    /\ UNCHANGED <<refs, mem, bak, hasBak, res, dirty, partial, copied>>

EvictArchive ==
    /\ pc["evict"] = "archive"
    /\ IF dirty /\ ArchiveAllowed THEN DoArchive ELSE UNCHANGED <<bak, hasBak, dirty>>
    /\ pc' = [pc EXCEPT !["evict"] = "setcold"]
    /\ UNCHANGED <<st, refs, excl, mem, res, partial, copied>>

\* set_cold() clears the locking bits BEFORE the pages are dropped, so there is
\* a window where the segment is settled-cold and still fully resident.
EvictSetCold ==
    /\ pc["evict"] = "setcold"
    /\ hasBak                             \* set_cold panics without a backup
    /\ st' = "cold"
    /\ pc' = [pc EXCEPT !["evict"] = "free"]
    /\ UNCHANGED <<refs, excl, mem, bak, hasBak, res, dirty, partial, copied>>

EvictFree ==
    /\ pc["evict"] = "free"
    /\ mem' = [s \in Slots |-> "z"]       \* free_memory: MADV_DONTNEED
    /\ res' = {}
    /\ partial' = TRUE
    /\ excl' = FALSE
    /\ pc' = [pc EXCEPT !["evict"] = "idle"]
    /\ UNCHANGED <<st, refs, bak, hasBak, dirty, copied>>

(***************************************************************************)
(* Promoter: tiered/promotion.rs promote_segment                             *)
(*                                                                          *)
(* NOTE the guard lifetime. In the code the guard is bound INSIDE the loop:  *)
(*                                                                          *)
(*     loop {                                                               *)
(*         let _exclusive_guard = SegmentExclusiveRefGuard::new(segment)..;  *)
(*         if segment.is_hot() { return; }                                   *)
(*         if segment.lock_cold() { break; }   // <-- guard dropped here     *)
(*     }                                                                    *)
(*     ... open, read, decompress, memcpy, set_hot ...                       *)
(*                                                                          *)
(* so the whole restore runs with refs back at 0.                            *)
(***************************************************************************)

PromoteTakeGuard ==
    /\ pc["promote"] = "idle"
    /\ ~IsHot
    /\ CanTakeExclusive
    /\ excl' = TRUE
    /\ pc' = [pc EXCEPT !["promote"] = "lockcold"]
    /\ UNCHANGED <<st, refs, mem, bak, hasBak, res, dirty, partial, copied>>

PromoteLockCold ==
    /\ pc["promote"] = "lockcold"
    /\ IF st = "cold"
         THEN /\ st' = "coldL"
              /\ copied' = {}
              /\ pc' = [pc EXCEPT !["promote"] = "restore"]
         ELSE /\ UNCHANGED <<st, copied>>
              /\ pc' = [pc EXCEPT !["promote"] = "idle"]
    \* Pre-fix, the guard was bound inside the loop and `break` dropped it, so
    \* the whole restore below ran unprotected. Fixed, it escapes the loop.
    /\ excl' = Fixed
    /\ UNCHANGED <<refs, mem, bak, hasBak, res, dirty, partial>>

\* The memcpy, one page at a time.
PromoteRestoreSlot(s) ==
    /\ pc["promote"] = "restore"
    /\ s \notin copied
    /\ mem' = [mem EXCEPT ![s] = bak[s]]
    /\ copied' = copied \cup {s}
    /\ pc' = IF copied \cup {s} = Slots
               THEN [pc EXCEPT !["promote"] = "sethot"]
               ELSE pc
    /\ UNCHANGED <<st, refs, excl, bak, hasBak, res, dirty, partial>>

\* mark_image_restored() then set_hot(), in that order: nothing may observe a
\* hot segment that is still flagged as a patchwork.
PromoteSetHot ==
    /\ pc["promote"] = "sethot"
    /\ st' = "hot"
    /\ partial' = FALSE
    /\ res' = {}
    /\ excl' = FALSE                      \* the guard is released here, not earlier
    /\ pc' = [pc EXCEPT !["promote"] = "idle"]
    /\ UNCHANGED <<refs, mem, bak, hasBak, dirty, copied>>

(***************************************************************************)
(* Cold-budget reclaimer: Segment::try_reclaim_resident_blocks               *)
(* Gated on is_cold(), which is TRUE while a promotion is in flight.         *)
(***************************************************************************)

ReclaimBlocks ==
    /\ pc["reclaim"] = "idle"
    /\ IF Fixed THEN IsSettledCold        \* the fix: settled-cold only
                ELSE IsCold                \* is_cold(): true for "cold" AND "coldL"
    /\ res # {}                            \* block_resident_bytes() > 0
    /\ CanTakeExclusive
    /\ mem' = [s \in Slots |-> "z"]       \* madvise_free(addr, SEGMENT_SIZE)
    /\ res' = {}
    /\ partial' = TRUE
    /\ UNCHANGED <<st, refs, excl, bak, hasBak, dirty, copied, pc>>

(***************************************************************************)
(* Cold reader: Segment::fault_in_block_for, reached from CellGuard::        *)
(* from_guard's cold path, which pins the segment first.                    *)
(***************************************************************************)

FaultInBlock(s) ==
    /\ pc["fault"] = "idle"
    /\ IsCold
    /\ ~excl
    /\ hasBak
    /\ s \notin res
    /\ mem' = [mem EXCEPT ![s] = bak[s]]
    /\ res' = res \cup {s}
    /\ UNCHANGED <<st, refs, excl, bak, hasBak, dirty, partial, copied, pc>>

(***************************************************************************)
(* An archive that does not own the segment: the combine cleaner archives    *)
(* destinations, and the statistics/flush paths archive dirty segments.      *)
(***************************************************************************)

BystanderArchive ==
    /\ Bystanders
    /\ pc["archive"] = "idle"
    /\ ArchiveAllowed
    /\ DoArchive
    /\ UNCHANGED <<st, refs, excl, mem, res, partial, copied, pc>>

Next ==
    \/ EvictStart \/ EvictArchive \/ EvictSetCold \/ EvictFree
    \/ PromoteTakeGuard \/ PromoteLockCold \/ PromoteSetHot
    \/ \E s \in Slots: PromoteRestoreSlot(s)
    \/ ReclaimBlocks
    \/ \E s \in Slots: FaultInBlock(s)
    \/ BystanderArchive

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The properties. Every cell of this segment is live and indexed, so:      *)
(*                                                                         *)
(*  HotReadable -- once the segment reads as hot, a reader goes straight to *)
(*    memory and there is nowhere else to look. This is the #71 fingerprint:*)
(*    "segment N is HOT, not dirty, the address is INSIDE the written range,*)
(*    found Id(0)".                                                        *)
(*                                                                         *)
(*  Recoverable -- the bytes exist SOMEWHERE. Losing this is #70.          *)
(***************************************************************************)

HotReadable == IsHot => \A s \in Slots: mem[s] = "d"

Recoverable == \A s \in Slots: mem[s] = "d" \/ (hasBak /\ bak[s] = "d")

(***************************************************************************)
(* Deliberately FALSE. A "no error found" result is only worth something if *)
(* the interesting states are reachable at all, so this is checked as an    *)
(* invariant and is EXPECTED to be violated: its counterexample is a full   *)
(* evict-fault-promote round trip. If it ever passes, the model has gone    *)
(* vacuous and the clean runs above mean nothing.                           *)
(***************************************************************************)
ReachesAPromotedHotSegment == ~(st = "hot" /\ hasBak /\ copied = Slots)

===============================================================================
