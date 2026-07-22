---------------------------- MODULE SeqlockReclaim ----------------------------
(***************************************************************************)
(* Models the interaction between optimistic (seqlock) readers and         *)
(* reference-counted node reclamation in cell_ref.rs / node.rs.            *)
(*                                                                         *)
(* A parent node's data slot holds a ref to child O. A writer latches the  *)
(* parent, replaces the slot with child N, releases (bumping the version), *)
(* and drops its ref to O; when O's count hits zero its destruction is     *)
(* DEFERRED through an epoch: the destroy may only run once every reader   *)
(* whose pinned section began before the defer has unpinned.               *)
(*                                                                         *)
(* A reader snapshots the version, reads the slot (possibly obtaining the  *)
(* stale pointer to O), CLONES the target (a refcount increment on the     *)
(* pointee), then validates the version and retries on mismatch.           *)
(*                                                                         *)
(* Variants:                                                               *)
(*   Pinned=FALSE               : today's code. The destroy can run        *)
(*     between the slot read and the clone; the clone touches freed        *)
(*     memory. Invariant NoUseAfterFree fails.                             *)
(*   Pinned=TRUE, TryClone=FALSE: memory stays valid, but a clone can      *)
(*     increment a zero count whose destroy is already queued —            *)
(*     resurrection. The reader keeps a ref past its pin; the deferred     *)
(*     destroy then frees a live object (and the ref's later drop          *)
(*     double-frees). NoResurrection fails.                                *)
(*   Pinned=TRUE, TryClone=TRUE : increment-if-nonzero; a zero count       *)
(*     makes the reader retry. All invariants hold.                        *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS Pinned, TryClone

VARIABLES
    slot,        \* parent slot: "O" or "N"
    ver,         \* parent version word (latch modeled by writer pc)
    countO,      \* refcount of child O (writer's ref + reader clones)
    deferQ,      \* queued destructions of O (a resurrected clone's drop
                 \* can queue a second one: the double free)
    destroyCount, \* executed destructions of O
    uaf,         \* a reader touched O's memory after destruction
    wpc,         \* writer: "start" | "swapped" | "done"
    rpc,         \* reader: "idle" | "readSlot" | "cloned" | "validate" | "done"
    rptr,        \* pointer value the reader read from the slot
    rref,        \* TRUE while the reader holds a cloned ref to O
    rver,        \* reader's version snapshot
    rpinBeforeDefer  \* reader's pin began before O's defer was queued

vars == <<slot, ver, countO, deferQ, destroyCount, uaf, wpc, rpc, rptr, rref,
          rver, rpinBeforeDefer>>

Init ==
    /\ slot = "O" /\ ver = 0
    /\ countO = 1                        \* the parent slot owns one ref
    /\ deferQ = 0 /\ destroyCount = 0 /\ uaf = FALSE
    /\ wpc = "start"
    /\ rpc = "idle" /\ rptr = "none" /\ rref = FALSE /\ rver = 0
    /\ rpinBeforeDefer = FALSE

--------------------------------------------------------------------------
(* Writer: latch parent, swap slot O -> N, release (version bump), then    *)
(* drop the ref to O; count 0 queues the deferred destroy.                 *)

WriterSwap ==
    /\ wpc = "start"
    /\ slot' = "N"
    /\ ver' = ver + 1
    /\ wpc' = "swapped"
    /\ UNCHANGED <<countO, deferQ, destroyCount, uaf, rpc, rptr, rref, rver,
                   rpinBeforeDefer>>

WriterDropO ==
    /\ wpc = "swapped"
    /\ countO' = countO - 1
    /\ deferQ' = IF countO = 1 THEN deferQ + 1 ELSE deferQ
    /\ wpc' = "done"
    /\ UNCHANGED <<slot, ver, destroyCount, uaf, rpc, rptr, rref, rver,
                   rpinBeforeDefer>>

(* The deferred destroy runs only after every pin that predates the defer  *)
(* has exited (epoch grace period). Without pinning there is no such       *)
(* protection.                                                             *)
DestroyO ==
    /\ deferQ > 0
    /\ (Pinned /\ rpc \in {"readSlot", "cloned", "validate"}) => ~rpinBeforeDefer
    /\ destroyCount' = destroyCount + 1
    /\ deferQ' = deferQ - 1
    /\ UNCHANGED <<slot, ver, countO, uaf, wpc, rpc, rptr, rref,
                   rver, rpinBeforeDefer>>

--------------------------------------------------------------------------
(* Reader *)

ReaderPin ==
    /\ rpc = "idle"
    /\ rver' = ver
    /\ rptr' = slot                      \* reads the slot pointer bytes
    /\ rpinBeforeDefer' = (deferQ = 0 /\ destroyCount = 0)
    /\ rpc' = "readSlot"
    /\ UNCHANGED <<slot, ver, countO, deferQ, destroyCount, uaf, wpc, rref>>

(* Clone the pointee: a refcount RMW on its memory.                        *)
ReaderClone ==
    /\ rpc = "readSlot"
    /\ IF rptr = "O"
         THEN /\ uaf' = (uaf \/ destroyCount > 0)  \* touched O's counter word
              /\ IF TryClone
                   THEN IF countO > 0
                          THEN /\ countO' = countO + 1
                               /\ rref' = TRUE
                               /\ rpc' = "validate"
                          ELSE /\ countO' = countO   \* observed 0: retry
                               /\ rref' = FALSE
                               /\ rpc' = "idle"
                   ELSE /\ countO' = countO + 1      \* plain fetch_add
                        /\ rref' = TRUE
                        /\ rpc' = "validate"
         ELSE /\ uaf' = uaf /\ countO' = countO /\ rref' = FALSE
              /\ rpc' = "validate"
    /\ UNCHANGED <<slot, ver, deferQ, destroyCount, wpc, rver, rptr,
                   rpinBeforeDefer>>

ReaderValidate ==
    /\ rpc = "validate"
    /\ IF ver = rver
         THEN rpc' = "done"                    \* keeps rref if it has one
         ELSE rpc' = "idle"                    \* retry: drop the clone
    /\ rref' = IF ver = rver THEN rref ELSE FALSE
    /\ countO' = IF ver # rver /\ rref THEN countO - 1 ELSE countO
    \* dropping the discarded clone can hit zero and queue ANOTHER destroy
    /\ deferQ' = IF ver # rver /\ rref /\ countO = 1 THEN deferQ + 1 ELSE deferQ
    /\ UNCHANGED <<slot, ver, destroyCount, uaf, wpc, rver, rptr, rpinBeforeDefer>>

(* An unchecked read (read_unchecked handler) keeps its clone without      *)
(* version validation.                                                     *)
ReaderUncheckedKeep ==
    /\ rpc = "validate" /\ rref
    /\ rpc' = "done"
    /\ UNCHANGED <<slot, ver, countO, deferQ, destroyCount, uaf, wpc, rptr,
                   rref, rver, rpinBeforeDefer>>

(* After unpinning, a reader that kept a ref may use it at any time.       *)
ReaderUseRef ==
    /\ rpc = "done" /\ rref
    /\ uaf' = (uaf \/ destroyCount > 0)
    /\ UNCHANGED <<slot, ver, countO, deferQ, destroyCount, wpc, rpc, rptr,
                   rref, rver, rpinBeforeDefer>>

Done ==
    /\ wpc = "done" /\ rpc \in {"idle", "done"} /\ deferQ = 0
    /\ UNCHANGED vars

Next ==
    \/ WriterSwap \/ WriterDropO \/ DestroyO
    \/ ReaderPin \/ ReaderClone \/ ReaderValidate \/ ReaderUncheckedKeep
    \/ ReaderUseRef
    \/ Done

Spec == Init /\ [][Next]_vars

\* No reader step ever touches O's memory after its destruction.
NoUseAfterFree == ~uaf

\* Destruction never runs while a reader still holds a live ref to O.
NoResurrection == (destroyCount > 0) => ~rref

\* An object is destroyed at most once.
NoDoubleFree == destroyCount <= 1

=============================================================================
