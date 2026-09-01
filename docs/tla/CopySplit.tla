------------------------------ MODULE CopySplit ------------------------------
(***************************************************************************)
(* The COPY-based split lifecycle (docs/ranged-index-robustness-plan.md     *)
(* proposal 1, implemented in a16fd84d) with the write-back FLUSHER as a    *)
(* modeled process rather than an assumption -- because every resurrection  *)
(* bug of the 2026-08-31 soak campaign was the flusher's tombstone pairing  *)
(* meeting a key that existed in two places.                               *)
(*                                                                         *)
(* Lifecycle: idle -> built (copy of the live keys >= pivot, on fresh       *)
(* pages) -> published -> committed (placement flip) -> retained (source    *)
(* drops the moved keys). Abort at any point before commit drains and drops *)
(* the copy; the source was never touched.                                 *)
(*                                                                         *)
(* The flusher is the danger. `build_cell` calls `remove_contains`, which   *)
(* PAIRS a page's keys against a deletion set: for every key present in     *)
(* both, it drops the key from the page AND the tombstone from the set. If  *)
(* a key exists in two pages and only one tombstone, that pairing can       *)
(* consume the tombstone against the copy that is about to die, leaving the *)
(* SURVIVING copy visible -- a resurrection.                               *)
(*                                                                         *)
(* Freeze fidelity matters here. The source is frozen (Migrating) for the   *)
(* whole window, so no delete routes to it. But at the placement flip the   *)
(* COPY starts serving [pivot, upper) and is NOT frozen -- so deletes of    *)
(* moved keys land during the commit->retain window, while the source still *)
(* physically holds those keys. That window is the interesting one.        *)
(*                                                                         *)
(* Toggles:                                                                *)
(*   Filtered      : the copy walk skips tombstoned keys (implemented).    *)
(*   SharedTombs   : the copy shares the source's deletion set (as first    *)
(*                   implemented) vs owning its own (disjoint ownership).  *)
(*   DetachOnAbort : an aborted copy's pages refuse later flushes (94959498)*)
(*   DropTombsOnRetain : retain also drops tombstones for moved keys, which *)
(*                   after a filtered copy name keys that exist nowhere.    *)
(*                                                                         *)
(* Expected results:                                                       *)
(*   CopySplitFixed    (all TRUE, SharedTombs FALSE): no error.            *)
(*   CopySplitShared   (SharedTombs TRUE): NoResurrection VIOLATED -- the   *)
(*     commit->retain window: a delete of a moved key routes to the copy    *)
(*     and tombstones the SHARED set; a source page flush pairs it against  *)
(*     the source's not-yet-retained copy, drops the tombstone, and the     *)
(*     copy's key is visible again.                                        *)
(*   CopySplitUnfiltered (Filtered FALSE): NoResurrection VIOLATED -- a key *)
(*     tombstoned BEFORE the split gets copied, so it exists twice with one *)
(*     tombstone, and whichever page flushes first frees the other copy.   *)
(*   CopySplitNoDetach (DetachOnAbort FALSE): NoResurrection VIOLATED via   *)
(*     the abandoned copy's pages (only with SharedTombs, the shape the     *)
(*     campaign actually hit).                                             *)
(*   CopySplitReach    : LivenessSanity violated ON PURPOSE -- its          *)
(*     counterexample is a full delete -> commit -> flush -> retain run. If *)
(*     it ever passes, the model has gone vacuous.                         *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS N, P, Filtered, SharedTombs, DetachOnAbort, DropTombsOnRetain

ASSUME N \in Nat /\ P \in 1..(N - 1)

Keys == 1..N
Moved == {k \in Keys : k > P}
Kept  == {k \in Keys : k <= P}

VARIABLES
    inSrc,        \* keys physically present in the source's pages
    inCopy,       \* keys physically present in the copy's pages
    srcTomb,      \* the source tree's deletion set
    copyTomb,     \* the copy tree's deletion set ({} when SharedTombs)
    phase,        \* idle | built | published | committed | retained | aborted
    deletedEver,  \* every key whose delete was acknowledged
    copyGone      \* the copy's pages were drained and detached

vars == <<inSrc, inCopy, srcTomb, copyTomb, phase, deletedEver, copyGone>>

Init ==
    /\ inSrc = Keys
    /\ inCopy = {}
    /\ srcTomb = {}
    /\ copyTomb = {}
    /\ phase = "idle"
    /\ deletedEver = {}
    /\ copyGone = FALSE

\* Which tree serves a key right now.
CopyServes(k) == /\ k \in Moved
                 /\ phase \in {"committed", "retained"}

\* The deletion set a given tree pairs against.
SrcTombSet == srcTomb
CopyTombSet == IF SharedTombs THEN srcTomb ELSE copyTomb

Visible(k) ==
    IF CopyServes(k)
    THEN k \in inCopy /\ k \notin CopyTombSet
    ELSE k \in inSrc  /\ k \notin SrcTombSet

--------------------------------------------------------------------------
(* The splitter. *)

Build ==
    /\ phase = "idle"
    /\ inCopy' = IF Filtered
                 THEN { k \in inSrc : k \in Moved /\ k \notin SrcTombSet }
                 ELSE { k \in inSrc : k \in Moved }
    /\ phase' = "built"
    /\ UNCHANGED <<inSrc, srcTomb, copyTomb, deletedEver, copyGone>>

Publish ==
    /\ phase = "built"
    /\ phase' = "published"
    /\ UNCHANGED <<inSrc, inCopy, srcTomb, copyTomb, deletedEver, copyGone>>

\* The placement flip: from here the copy serves the moved range, and it is
\* NOT frozen -- deletes of moved keys start landing on it.
Commit ==
    /\ phase = "published"
    /\ phase' = "committed"
    /\ UNCHANGED <<inSrc, inCopy, srcTomb, copyTomb, deletedEver, copyGone>>

\* Source truncation, still under the source's freeze.
Retain ==
    /\ phase = "committed"
    /\ inSrc' = { k \in inSrc : k \in Kept }
    /\ srcTomb' = IF DropTombsOnRetain
                  THEN { k \in srcTomb : k \in Kept }
                  ELSE srcTomb
    /\ phase' = "retained"
    /\ UNCHANGED <<inCopy, copyTomb, deletedEver, copyGone>>

\* Abort before the commit point: drain and drop the copy. The source was
\* never touched, so there is nothing to restore.
Abort ==
    /\ phase \in {"built", "published"}
    /\ inCopy' = IF DetachOnAbort THEN {} ELSE inCopy
    /\ copyGone' = TRUE
    /\ phase' = "aborted"
    /\ UNCHANGED <<inSrc, srcTomb, copyTomb, deletedEver>>

--------------------------------------------------------------------------
(* The client. A delete is acknowledged only against the tree that serves
   the key; the source is frozen for the whole split window, but the copy
   is live from the commit point. *)

DeleteEnabled(k) ==
    /\ Visible(k)
    /\ \/ phase \in {"idle", "retained", "aborted"}   \* no split in flight
       \/ CopyServes(k)                                \* the copy is not frozen

Delete(k) ==
    /\ DeleteEnabled(k)
    /\ IF CopyServes(k)
       THEN /\ IF SharedTombs
               THEN /\ srcTomb' = srcTomb \cup {k}
                    /\ UNCHANGED copyTomb
               ELSE /\ copyTomb' = copyTomb \cup {k}
                    /\ UNCHANGED srcTomb
       ELSE /\ srcTomb' = srcTomb \cup {k}
            /\ UNCHANGED copyTomb
    /\ deletedEver' = deletedEver \cup {k}
    /\ UNCHANGED <<inSrc, inCopy, phase, copyGone>>

--------------------------------------------------------------------------
(* The write-back flusher: build_cell -> remove_contains. It pairs a page's
   keys against ITS TREE's deletion set, dropping both sides of each match.
   Modeled per key so every interleaving is explored. *)

FlushSrc(k) ==
    /\ k \in inSrc
    /\ k \in srcTomb
    /\ inSrc' = inSrc \ {k}
    /\ srcTomb' = srcTomb \ {k}
    /\ UNCHANGED <<inCopy, copyTomb, phase, deletedEver, copyGone>>

FlushCopy(k) ==
    \* A detached page refuses background work (94959498).
    /\ ~(copyGone /\ DetachOnAbort)
    /\ k \in inCopy
    /\ k \in CopyTombSet
    /\ inCopy' = inCopy \ {k}
    /\ IF SharedTombs
       THEN /\ srcTomb' = srcTomb \ {k}
            /\ UNCHANGED copyTomb
       ELSE /\ copyTomb' = copyTomb \ {k}
            /\ UNCHANGED srcTomb
    /\ UNCHANGED <<inSrc, phase, deletedEver, copyGone>>

--------------------------------------------------------------------------
Done == phase \in {"retained", "aborted"}

Next ==
    \/ Build \/ Publish \/ Commit \/ Retain \/ Abort
    \/ \E k \in Keys : Delete(k)
    \/ \E k \in Keys : FlushSrc(k)
    \/ \E k \in Keys : FlushCopy(k)
    \/ (Done /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
(* THE property of the whole campaign: an acknowledged delete is forever.  *)
NoResurrection == \A k \in deletedEver : ~Visible(k)

(* Nothing is lost: a key never deleted stays reachable, at every state --
   including the commit->retain window, where routing must find it in the
   copy while the source still physically holds it. *)
NoLostKey == \A k \in Keys : (k \notin deletedEver) => Visible(k)

(* Coverage guard: asserts the interesting state -- a delete landing on the
   copy during the commit->retain window -- is unreachable. Expected to be
   VIOLATED; if it ever passes, the model is vacuous. *)
LivenessSanity ==
    ~(phase = "committed" /\ deletedEver \cap Moved # {})

=============================================================================
