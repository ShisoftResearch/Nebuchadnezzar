-------------------------- MODULE PublicationOrder --------------------------
(***************************************************************************)
(* What a reader outside a transaction sees while one transaction is      *)
(* applied and, if it aborts, rolled back.                                 *)
(*                                                                         *)
(* Covers `DataManager::apply_commit_ops` (`publication_order`) and        *)
(* `DataManager::rollback` in src/server/transactions/data_site.rs. A site *)
(* publishes the cells of a transaction one at a time, and a reader that   *)
(* does not take part in the transaction reads one cell at a time. The     *)
(* model is general: any cell can refer to one other cell (a list segment  *)
(* to the next one, an index node to a child). One reference per cell is   *)
(* enough, because the argument is per reference.                          *)
(*                                                                         *)
(* The store has no dangling reference before the transaction and none     *)
(* after it (`Intact`). The question is the steps in between.             *)
(*                                                                         *)
(* ApplyOrder:  "Any"         - any order of the cells (the id order of a   *)
(*                              site, before the fix, is one of these).    *)
(*              "Publication" - new cells, then changed cells, then        *)
(*                              removed cells; any order inside a class.   *)
(* UndoOrder:   "Any"         - any order (the id order before the fix).   *)
(*              "ExistingFirst" - changed and removed cells in any order,  *)
(*                              then new cells (a first draft of the fix). *)
(*              "Reverse"     - removed cells, then changed cells, then    *)
(*                              new cells: the reverse of "Publication".   *)
(* Each site orders only its own cells; sites run at the same time.       *)
(*                                                                         *)
(* Invariants:                                                             *)
(*   ReachableIntact - no cell that a reader can reach from a cell that   *)
(*     exists before and after the transaction refers to a missing cell.  *)
(*   ReaderNeverMissing - a reader that walks one reference per step never *)
(*     reads a missing cell. It holds for a transaction that commits and   *)
(*     removes nothing. A removal and a rollback (which removes the        *)
(*     created cells) make it fail by design: a reader that read a         *)
(*     reference before the change and follows it after the removal meets  *)
(*     the removed cell. Only a reader that checks its walk avoids that.   *)
(*   Sanity - false on purpose: a step with some cells published and some  *)
(*     not is reachable, so a pass of the others is not vacuous.           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    N,              \* Cells are 1..N.
    TwoSites,       \* FALSE: one site holds every cell. TRUE: odd and even
                    \* cells are on two sites.
    ApplyOrder,     \* "Any" or "Publication"
    UndoOrder,      \* "Any", "ExistingFirst" or "Reverse"
    AllowRemove,    \* Whether the transaction may remove cells.
    AllowAbort      \* Whether the transaction may abort after a publish.

Cells == 1..N
Nil == 0            \* "Refers to no cell".
SiteOf == [c \in Cells |-> IF TwoSites THEN c % 2 ELSE 0]

ASSUME ApplyOrder \in {"Any", "Publication"}
ASSUME UndoOrder \in {"Any", "ExistingFirst", "Reverse"}

Absent == -1       \* A missing cell: no cell value.
Value == Cells \cup {Nil}
Stores == [Cells -> Value \cup {Absent}]

Present(s, c) == s[c] # Absent

\* No present cell refers to a missing cell.
Intact(s) == \A c \in Cells : Present(s, c) => (s[c] = Nil \/ Present(s, s[c]))

VARIABLES
    before,     \* The store before the transaction.
    after,      \* The store the transaction leaves when it commits.
    store,      \* What a reader sees now.
    applied,    \* Cells the apply published.
    undone,     \* Cells the rollback restored.
    phase,      \* "apply", "undo", "done"
    reader,     \* 0 when idle, or the cell the reader reads next.
    missing     \* A reader read a missing cell.

vars == <<before, after, store, applied, undone, phase, reader, missing>>

Kind(c) ==
    IF before[c] = Absent /\ after[c] # Absent THEN "create"
    ELSE IF before[c] # Absent /\ after[c] = Absent THEN "remove"
    ELSE IF before[c] # after[c] THEN "change"
    ELSE "none"

Touched == {c \in Cells : Kind(c) # "none"}

\* Cells a reader can start from: they exist before and after.
Entry == {c \in Cells : before[c] # Absent /\ after[c] # Absent}

RECURSIVE Closure(_, _, _)
Closure(s, set, n) ==
    IF n = 0 THEN set
    ELSE Closure(s, set \cup {s[c] : c \in {x \in set : Present(s, x) /\ s[x] # Nil}}, n - 1)

Reachable(s) == Closure(s, Entry, Cardinality(Cells))

ApplyClass(c) ==
    IF ApplyOrder = "Any" THEN 0
    ELSE CASE Kind(c) = "create" -> 0 [] Kind(c) = "change" -> 1 [] OTHER -> 2

UndoClass(c) ==
    CASE UndoOrder = "Any" -> 0
      [] UndoOrder = "ExistingFirst" -> IF Kind(c) = "create" THEN 1 ELSE 0
      [] OTHER -> CASE Kind(c) = "remove" -> 0 [] Kind(c) = "change" -> 1 [] OTHER -> 2

\* `c` is next at its site: no cell of a lower class waits there.
NextOf(pending, c, class(_)) ==
    \A d \in pending : SiteOf[d] = SiteOf[c] => class(d) >= class(c)

Init ==
    /\ before \in {s \in Stores : Intact(s)}
    /\ after \in {s \in Stores :
                    /\ Intact(s)
                    /\ AllowRemove \/ \A c \in Cells : before[c] # Absent => s[c] # Absent}
    /\ store = before
    /\ applied = {}
    /\ undone = {}
    /\ phase = "apply"
    /\ reader = 0
    /\ missing = FALSE

Apply ==
    /\ phase = "apply"
    /\ \E c \in Touched \ applied :
        /\ NextOf(Touched \ applied, c, ApplyClass)
        /\ store' = [store EXCEPT ![c] = after[c]]
        /\ applied' = applied \cup {c}
    /\ UNCHANGED <<before, after, undone, phase, reader, missing>>

\* Every cell is published: the transaction is decided as committed.
Commit ==
    /\ phase = "apply"
    /\ applied = Touched
    /\ phase' = "done"
    /\ UNCHANGED <<before, after, store, applied, undone, reader, missing>>

\* An abort at any point of the apply (a later write failed, the
\* coordinator decided to abort, a site refused).
Abort ==
    /\ AllowAbort
    /\ phase = "apply"
    /\ applied # {}
    /\ phase' = "undo"
    /\ UNCHANGED <<before, after, store, applied, undone, reader, missing>>

Undo ==
    /\ phase = "undo"
    /\ \E c \in applied \ undone :
        /\ NextOf(applied \ undone, c, UndoClass)
        /\ store' = [store EXCEPT ![c] = before[c]]
        /\ undone' = undone \cup {c}
    /\ UNCHANGED <<before, after, applied, phase, reader, missing>>

Undone ==
    /\ phase = "undo"
    /\ undone = applied
    /\ phase' = "done"
    /\ UNCHANGED <<before, after, store, applied, undone, reader, missing>>

ReaderStart ==
    /\ reader = 0
    /\ \E c \in Entry : reader' = c
    /\ UNCHANGED <<before, after, store, applied, undone, phase, missing>>

\* One cell read: follow its reference, or report it missing.
ReaderStep ==
    /\ reader \in Cells
    /\ IF Present(store, reader)
         THEN /\ reader' = store[reader]
              /\ UNCHANGED missing
         ELSE /\ missing' = TRUE
              /\ reader' = 0
    /\ UNCHANGED <<before, after, store, applied, undone, phase>>

Next == Apply \/ Commit \/ Abort \/ Undo \/ Undone \/ ReaderStart \/ ReaderStep

Spec == Init /\ [][Next]_vars

ReachableIntact ==
    \A c \in Reachable(store) : Present(store, c) => (store[c] = Nil \/ Present(store, store[c]))

ReaderNeverMissing == ~missing

\* Must FAIL: a step with some, not all, cells published is reachable.
Sanity == ~(phase = "apply" /\ applied # {} /\ applied # Touched)

=============================================================================
