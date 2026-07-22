---------------------------- MODULE StructuralSplit ----------------------------
(***************************************************************************)
(* Models a structural tree split (docs/structural-split-design.md): an     *)
(* oversized tree is split at a pivot by re-pointing whole leaf nodes into   *)
(* a new tree instead of copying keys, then handing each side its own root.  *)
(*                                                                         *)
(* A structural split frees no node — every leaf survives in exactly one of  *)
(* the two trees — so the reader-safety substrate (version re-check + epoch  *)
(* reclamation) is already covered by SeqlockCursor.tla / SeqlockReclaim.tla.*)
(* What is load-bearing and NEW is the precondition the design leans on: the *)
(* source must be FROZEN (no concurrent writer) for the whole split,         *)
(* enforced by the migration marker (apply_in_ranged_tree returns            *)
(* Migrating). This spec checks that precondition is actually necessary.     *)
(*                                                                         *)
(*   Frozen = TRUE  (correct): no writer runs during the split; every key    *)
(*     ends up reachable from exactly one tree on the correct side of the    *)
(*     pivot. PartitionComplete and NoLostKey hold.                          *)
(*   Frozen = FALSE (precondition violated): a writer appends a key to a     *)
(*     leaf after that leaf's tree has snapshotted its keys for publication  *)
(*     and after the source stopped covering it — the key is reachable from  *)
(*     neither tree. NoLostKey fails, showing why the split needs the freeze.*)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS N, Pivot, Frozen

ASSUME N \in Nat /\ Pivot \in 1..(N - 1)

Leaves == 1..N
Nil == 0
WKey == 100          \* a key the concurrent writer inserts (distinct from ids)

VARIABLES
    leafKeys,    \* leaf -> set of keys it physically holds
    srcCover,    \* set of leaves the source root reaches
    newCover,    \* set of leaves the new root reaches ({} until built)
    pubKeys,     \* keys the new tree published (snapshotted when spine built)
    spc,         \* splitter: "sever" | "buildNew" | "truncSrc" | "done"
    wrote

vars == <<leafKeys, srcCover, newCover, pubKeys, spc, wrote>>

Init ==
    /\ leafKeys = [l \in Leaves |-> {l}]      \* leaf l initially holds key l
    /\ srcCover = Leaves                       \* before split source covers all
    /\ newCover = {}
    /\ pubKeys = {}
    /\ spc = "sever"
    /\ wrote = FALSE

--------------------------------------------------------------------------
(* Splitter, source assumed frozen. Sever the chain, build the new spine     *)
(* (which snapshots the moved leaves' keys for the new tree's published      *)
(* head), then truncate the source spine.                                   *)

Sever ==
    /\ spc = "sever"
    /\ spc' = "buildNew"
    /\ UNCHANGED <<leafKeys, srcCover, newCover, pubKeys, wrote>>

BuildNew ==
    /\ spc = "buildNew"
    /\ newCover' = { l \in Leaves : l > Pivot }
    \* Publishing the new tree captures the current keys of the moved leaves.
    /\ pubKeys' = UNION { leafKeys[l] : l \in { m \in Leaves : m > Pivot } }
    /\ spc' = "truncSrc"
    /\ UNCHANGED <<leafKeys, srcCover, wrote>>

TruncSrc ==
    /\ spc = "truncSrc"
    /\ srcCover' = { l \in Leaves : l <= Pivot }
    /\ spc' = "done"
    /\ UNCHANGED <<leafKeys, newCover, pubKeys, wrote>>

--------------------------------------------------------------------------
(* Concurrent writer, enabled only when the source is NOT frozen. It appends *)
(* WKey to a moved leaf during the split window. If it lands after BuildNew  *)
(* (so WKey is not in pubKeys) and after TruncSrc dropped the leaf from the  *)
(* source, WKey is reachable from neither tree.                            *)
Writer ==
    /\ ~Frozen
    /\ ~wrote
    /\ spc \in {"truncSrc", "done"}
    /\ leafKeys' = [leafKeys EXCEPT ![Pivot + 1] = @ \cup {WKey}]
    /\ wrote' = TRUE
    /\ UNCHANGED <<srcCover, newCover, pubKeys, spc>>

--------------------------------------------------------------------------
Quiescent == spc = "done" /\ (Frozen \/ wrote)

Next ==
    \/ Sever \/ BuildNew \/ TruncSrc \/ Writer
    \/ (Quiescent /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
\* A key is reachable after the split iff its leaf is still covered by the
\* source, or it was captured in the new tree's published key set.
KeyReachable(k) ==
    \/ (\E l \in srcCover : k \in leafKeys[l])
    \/ (k \in pubKeys)

AllKeys == UNION { leafKeys[l] : l \in Leaves }

(* P1: at quiescence every leaf is covered by exactly one root, correct side.*)
PartitionComplete ==
    Quiescent =>
        \A l \in Leaves :
            /\ (l <= Pivot) = (l \in srcCover)
            /\ (l > Pivot)  = (l \in newCover)
            /\ (l \in srcCover) # (l \in newCover)

(* Every key that exists at quiescence is reachable from some tree. Holds    *)
(* under Frozen; the unfrozen writer's late key is lost.                    *)
NoLostKey == Quiescent => \A k \in AllKeys : KeyReachable(k)

=============================================================================
