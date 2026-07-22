----------------------------- MODULE PersistLatch -----------------------------
(***************************************************************************)
(* Models the latch hold-and-wait interactions between the write-back      *)
(* persist path (node.rs persist -> external.rs to_cell) and the insert    *)
(* path (write_targeted / split_insert) on a chain of sibling leaf pages.  *)
(*                                                                         *)
(* Persist takes the page's WRITE latch, then resolves the ids of both     *)
(* neighbors.  A validated read (read_node) of a neighbor WAITS while that *)
(* neighbor's latch is held.  Inserts hold a page's latch while acquiring  *)
(* the latch of its RIGHT sibling (write_targeted hand-over-hand, and      *)
(* split_insert latching self.next), so the global latch order is          *)
(* left-before-right.                                                      *)
(*                                                                         *)
(* WaitLeft = TRUE  models resolving the LEFT neighbor with a waiting      *)
(* read while holding the node's own latch: a leftward wait that closes a  *)
(* cycle with the rightward waiters.  TLC finds the deadlock (persist      *)
(* holds R and waits for L to unlatch; an inserter holds L and waits to    *)
(* latch R).  This bug was introduced and then caught during the audit.    *)
(*                                                                         *)
(* WaitLeft = FALSE models the fix: the left neighbor is read without      *)
(* waiting (read_unchecked), only the rightward resolution may wait.  TLC  *)
(* verifies deadlock freedom.                                              *)
(***************************************************************************)
EXTENDS Naturals, Sequences, TLC

CONSTANTS
    NumNodes,     \* sibling chain 1..NumNodes (left to right)
    NumPersist,   \* write-back workers, each persists one page
    NumInsert,    \* inserters, each walks one hand-over-hand step right
    WaitLeft      \* TRUE = pre-fix behavior (waiting read on left neighbor)

Nodes == 1..NumNodes
PProcs == 1..NumPersist
IProcs == 1..NumInsert
Nil == 0

VARIABLES
    latch,   \* Nodes -> holder id (string) or "" if free
    ppc, ptarget,
    ipc, ipos

vars == <<latch, ppc, ptarget, ipc, ipos>>

PName(p) == <<"P", p>>
IName(p) == <<"I", p>>
Free == <<>>

Init ==
    /\ latch = [n \in Nodes |-> Free]
    /\ ppc = [p \in PProcs |-> "start"]
    \* Each persist worker targets some page with both neighbors in range.
    /\ ptarget \in [PProcs -> 2..(NumNodes - 1)]
    /\ ipc = [p \in IProcs |-> "start"]
    \* Each inserter starts its rightward walk at some non-last page.
    /\ ipos \in [IProcs -> 1..(NumNodes - 1)]

--------------------------------------------------------------------------
(* Persist worker p: latch target; wait-read right neighbor; resolve left  *)
(* neighbor (waiting or not per WaitLeft); unlatch.                        *)

PLatch(p) ==
    /\ ppc[p] = "start"
    /\ latch[ptarget[p]] = Free
    /\ latch' = [latch EXCEPT ![ptarget[p]] = PName(p)]
    /\ ppc' = [ppc EXCEPT ![p] = "readRight"]
    /\ UNCHANGED <<ptarget, ipc, ipos>>

\* read_node on the right neighbor: proceeds only when unlatched.
PReadRight(p) ==
    /\ ppc[p] = "readRight"
    /\ latch[ptarget[p] + 1] = Free
    /\ ppc' = [ppc EXCEPT ![p] = "readLeft"]
    /\ UNCHANGED <<latch, ptarget, ipc, ipos>>

PReadLeft(p) ==
    /\ ppc[p] = "readLeft"
    /\ IF WaitLeft
         THEN latch[ptarget[p] - 1] = Free    \* waiting read: the bug
         ELSE TRUE                            \* read_unchecked: never waits
    /\ ppc' = [ppc EXCEPT ![p] = "unlatch"]
    /\ UNCHANGED <<latch, ptarget, ipc, ipos>>

PUnlatch(p) ==
    /\ ppc[p] = "unlatch"
    /\ latch' = [latch EXCEPT ![ptarget[p]] = Free]
    /\ ppc' = [ppc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<ptarget, ipc, ipos>>

--------------------------------------------------------------------------
(* Inserter p: latch a page, then while holding it acquire the latch of    *)
(* its right sibling (write_targeted / split latching self.next), then     *)
(* release both.                                                           *)

ILatch(p) ==
    /\ ipc[p] = "start"
    /\ latch[ipos[p]] = Free
    /\ latch' = [latch EXCEPT ![ipos[p]] = IName(p)]
    /\ ipc' = [ipc EXCEPT ![p] = "latchRight"]
    /\ UNCHANGED <<ppc, ptarget, ipos>>

ILatchRight(p) ==
    /\ ipc[p] = "latchRight"
    /\ latch[ipos[p] + 1] = Free
    /\ latch' = [latch EXCEPT ![ipos[p] + 1] = IName(p)]
    /\ ipc' = [ipc EXCEPT ![p] = "release"]
    /\ UNCHANGED <<ppc, ptarget, ipos>>

IRelease(p) ==
    /\ ipc[p] = "release"
    /\ latch' = [latch EXCEPT ![ipos[p]] = Free, ![ipos[p] + 1] = Free]
    /\ ipc' = [ipc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<ppc, ptarget, ipos>>

--------------------------------------------------------------------------

AllDone ==
    /\ \A p \in PProcs : ppc[p] = "done"
    /\ \A p \in IProcs : ipc[p] = "done"

Next ==
    \/ \E p \in PProcs : PLatch(p) \/ PReadRight(p) \/ PReadLeft(p) \/ PUnlatch(p)
    \/ \E p \in IProcs : ILatch(p) \/ ILatchRight(p) \/ IRelease(p)
    \/ (AllDone /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

\* With TLC deadlock checking enabled, a state where no action is enabled
\* and not everything is done is reported as a deadlock.

=============================================================================
