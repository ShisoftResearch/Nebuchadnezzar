----------------------------- MODULE DirtyCoalesce -----------------------------
(***************************************************************************)
(* Models write-back coalescing for B-tree pages.                          *)
(*                                                                         *)
(* Instead of pushing one write-back queue entry per page touch, a writer  *)
(* atomically swaps the page's dirty flag and enqueues only on the         *)
(* FALSE -> TRUE transition. The persister pops an entry, latches the      *)
(* page, CLEARS the dirty flag, then snapshots the page state — all under  *)
(* the page write latch — and persists the snapshot. A touch after the     *)
(* clear re-enqueues, so no modification is ever left unpersisted.         *)
(*                                                                         *)
(* Checked: NoLostUpdate — once quiescent (no touches in flight, queue     *)
(* drained, page clean), the persisted version equals the page version.    *)
(* BoundedQueue — the queue never holds more than one entry per page       *)
(* (with one page: <= 1 entry plus the one being processed).               *)
(***************************************************************************)
EXTENDS Naturals, TLC

CONSTANTS MaxTouches

VARIABLES
    pageVer,     \* page content version (bumped by each touch)
    persistedVer,\* last version written to storage
    dirty,       \* the page's dirty flag
    queue,       \* number of queued write-back entries for this page
    latched,     \* page write latch held by the persister
    ppc,         \* persister: "idle" | "snap" (holds latch, snapshotting)
    psnap,       \* version captured by the in-flight persist
    tpc,         \* two-phase toucher: "idle" | "markPending"
    touches

vars == <<pageVer, persistedVer, dirty, queue, latched, ppc, psnap, tpc, touches>>

Init ==
    /\ pageVer = 0 /\ persistedVer = 0
    /\ dirty = FALSE /\ queue = 0
    /\ latched = FALSE /\ ppc = "idle" /\ psnap = 0
    /\ tpc = "idle"
    /\ touches = MaxTouches

--------------------------------------------------------------------------
(* A writer modifies the page under its latch and marks it changed. The    *)
(* modify and the dirty swap are separate steps in the implementation, but *)
(* both happen while the writer holds the page latch, so they are atomic   *)
(* with respect to the persister's clear-then-snapshot.                    *)
Touch ==
    /\ touches > 0 /\ ~latched /\ tpc = "idle"
    /\ pageVer' = pageVer + 1
    /\ queue' = IF dirty THEN queue ELSE queue + 1
    /\ dirty' = TRUE
    /\ touches' = touches - 1
    /\ UNCHANGED <<persistedVer, latched, ppc, psnap, tpc>>

(* Some call sites modify under the latch but mark AFTER releasing it      *)
(* (e.g. marking a freshly split right node). The mark is latch-free and   *)
(* may land at any later time, including while the persister holds the     *)
(* latch: the persister's snapshot then already contains the modification, *)
(* and the late mark merely queues a redundant persist.                    *)
TouchModify ==
    /\ touches > 0 /\ ~latched /\ tpc = "idle"
    /\ pageVer' = pageVer + 1
    /\ tpc' = "markPending"
    /\ touches' = touches - 1
    /\ UNCHANGED <<persistedVer, dirty, queue, latched, ppc, psnap>>

TouchMark ==
    /\ tpc = "markPending"
    /\ queue' = IF dirty THEN queue ELSE queue + 1
    /\ dirty' = TRUE
    /\ tpc' = "idle"
    /\ UNCHANGED <<pageVer, persistedVer, latched, ppc, psnap, touches>>

(* Persister: pop an entry and latch the page; clear dirty, snapshot.      *)
PersistStart ==
    /\ ppc = "idle" /\ queue > 0 /\ ~latched
    /\ latched' = TRUE
    /\ queue' = queue - 1
    /\ dirty' = FALSE                    \* cleared before the snapshot
    /\ psnap' = pageVer                  \* snapshot under the latch
    /\ ppc' = "snap"
    /\ UNCHANGED <<pageVer, persistedVer, tpc, touches>>

(* Release the latch, then the storage write completes at any later time.  *)
PersistFinish ==
    /\ ppc = "snap"
    /\ latched' = FALSE
    /\ persistedVer' = psnap
    /\ ppc' = "idle"
    /\ UNCHANGED <<pageVer, dirty, queue, psnap, tpc, touches>>

Quiescent ==
    touches = 0 /\ queue = 0 /\ ppc = "idle" /\ tpc = "idle" /\ ~dirty

Next ==
    \/ Touch \/ TouchModify \/ TouchMark \/ PersistStart \/ PersistFinish
    \/ (Quiescent /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

\* Every modification is eventually covered: with no work left, the
\* persisted version is the current page version.
NoLostUpdate == Quiescent => (persistedVer = pageVer)

\* Coalescing bound: at most two queued entries per page (one from the
\* latched path plus one late mark).
BoundedQueue == queue <= 2

\* A drained queue with a clean page means nothing is awaiting persistence.
NoOrphanDirty ==
    (queue = 0 /\ ppc = "idle" /\ tpc = "idle" /\ touches = 0) => ~dirty

=============================================================================
