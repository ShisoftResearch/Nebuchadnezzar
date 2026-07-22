---------------------------- MODULE DeletionReclaim ----------------------------
(***************************************************************************)
(* Models tombstone reclamation for the ranged index deletion set.         *)
(*                                                                         *)
(* State per key: `phys` (physically stored in a page) and `tomb`          *)
(* (tombstone in the DeletionSet). Visible == phys /\ ~tomb.               *)
(*                                                                         *)
(* Protocol under check: the write-back / merge path compacts a            *)
(* tombstoned key while HOLDING THE PAGE WRITE LATCH — it removes the key  *)
(* from the page and drops the tombstone before releasing the latch.       *)
(* There is no grace period and no undo.                                   *)
(*                                                                         *)
(* Modeling notes:                                                         *)
(*  - The deletion set is lock-free and does NOT respect the page latch:   *)
(*    insert's first step (deletion.remove) interleaves freely with the    *)
(*    reclaimer's set operations. Each individual set op is atomic.        *)
(*  - Operations that read or write the PAGE (delete's seek check,         *)
(*    insert's physical insert) are ordered by the latch: they cannot run  *)
(*    while the reclaimer holds it (validated reads retry, writers wait).  *)
(*                                                                         *)
(* An earlier draft used deferred reclamation with a recheck-and-undo;     *)
(* TLC found it loses inserts (the undo restores a tombstone that a        *)
(* concurrent insert had legitimately consumed). This latched protocol     *)
(* replaced it.                                                            *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS NumOps, NumUsers  \* op budget and concurrent user threads

Users == 1..NumUsers

VARIABLES
    phys, tomb,
    latched,    \* reclaimer holds the page write latch
    upc,        \* per-user state: "idle" | "i2" (mid-insert)
    tombWas,    \* per-user observation from deletion.remove
    expect,     \* ghost: visibility after the last USER-caused flip; the
                \* reclaimer must never change net visibility
    rpc,        \* reclaimer: "idle" | "unphys" | "untomb" | "release"
    opsLeft

vars == <<phys, tomb, latched, upc, tombWas, expect, rpc, opsLeft>>

Init ==
    /\ phys = TRUE /\ tomb = FALSE      \* key starts inserted and visible
    /\ latched = FALSE
    /\ upc = [u \in Users |-> "idle"] /\ tombWas = [u \in Users |-> FALSE]
    /\ expect = TRUE
    /\ rpc = "idle"
    /\ opsLeft = NumOps

--------------------------------------------------------------------------
(* User operations *)

DeleteOp(u) ==
    /\ upc[u] = "idle" /\ opsLeft > 0
    /\ ~latched                          \* the seek check reads the page
    /\ phys                              \* seek_raw found the key
    /\ ~tomb                             \* deletion.insert succeeds
    /\ tomb' = TRUE
    /\ expect' = FALSE
    /\ opsLeft' = opsLeft - 1
    /\ UNCHANGED <<phys, latched, upc, tombWas, rpc>>

\* deletion.remove — a pure set op, NOT ordered by the page latch.
InsertStep1(u) ==
    /\ upc[u] = "idle" /\ opsLeft > 0
    /\ ~(phys /\ ~tomb)                  \* only insert when not visible
    /\ tombWas' = [tombWas EXCEPT ![u] = tomb]
    /\ tomb' = FALSE
    \* resurrect case: dropping the tombstone of a present key IS the
    \* linearization point of the insert
    /\ expect' = IF phys THEN TRUE ELSE expect
    /\ upc' = [upc EXCEPT ![u] = "i2"]
    /\ opsLeft' = opsLeft - 1
    /\ UNCHANGED <<phys, latched, rpc>>

\* The physical part (resurrect check + tree.insert) touches the page.
InsertStep2(u) ==
    /\ upc[u] = "i2"
    /\ ~latched
    /\ phys' = TRUE
    /\ upc' = [upc EXCEPT ![u] = "idle"]
    \* the physical insert flips visibility only if no tombstone re-appeared
    /\ expect' = IF ~tomb THEN TRUE ELSE expect
    /\ UNCHANGED <<tomb, latched, tombWas, rpc, opsLeft>>

--------------------------------------------------------------------------
(* Reclaimer: compaction under the page write latch. The page mutation and *)
(* the set mutation are separate steps even under the latch, because only  *)
(* the page is latch-protected.                                            *)

ReclaimAcquire ==
    /\ rpc = "idle" /\ ~latched
    /\ tomb /\ phys                      \* persist saw a tombstoned key
    /\ latched' = TRUE
    /\ rpc' = "unphys"
    /\ UNCHANGED <<phys, tomb, upc, tombWas, expect, opsLeft>>

ReclaimRemovePhys ==
    /\ rpc = "unphys"
    /\ phys' = FALSE                     \* remove from the page (latched)
    /\ rpc' = "untomb"
    /\ UNCHANGED <<tomb, latched, upc, tombWas, expect, opsLeft>>

ReclaimDropTomb ==
    /\ rpc = "untomb"
    /\ tomb' = FALSE                     \* deletion.remove (atomic set op)
    /\ rpc' = "release"
    /\ UNCHANGED <<phys, latched, upc, tombWas, expect, opsLeft>>

ReclaimRelease ==
    /\ rpc = "release"
    /\ latched' = FALSE
    /\ rpc' = "idle"                     \* may run again later
    /\ UNCHANGED <<phys, tomb, upc, tombWas, expect, opsLeft>>

--------------------------------------------------------------------------

Quiescent ==
    /\ \A u \in Users : upc[u] = "idle"
    /\ rpc = "idle" /\ opsLeft = 0

Next ==
    \/ \E u \in Users : DeleteOp(u) \/ InsertStep1(u) \/ InsertStep2(u)
    \/ ReclaimAcquire \/ ReclaimRemovePhys \/ ReclaimDropTomb \/ ReclaimRelease
    \/ (Quiescent /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

Visible == phys /\ ~tomb

\* At quiescence, visibility matches the last committed user operation:
\* reclamation neither resurrects a deleted key nor hides an inserted one.
QuiescentCorrect == Quiescent => (Visible = expect)

\* The tombstone drop only ever targets a physically absent key.
DropOnlyAbsent == (rpc = "untomb") => ~phys

\* A deleted key is never visible outside an in-flight insert.
NoResurrection ==
    (~expect /\ (\A u \in Users : upc[u] = "idle") /\ rpc = "idle") => ~Visible

=============================================================================
