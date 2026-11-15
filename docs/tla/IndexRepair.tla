----------------------------- MODULE IndexRepair -----------------------------
(***************************************************************************)
(* The repair of a failed secondary-index task (model-check finding D-F11) *)
(* and the refusal of a query while a repair is pending.                   *)
(*                                                                         *)
(* A Neb copy of the WriteIndex model of Morpheus (docs/tla/WriteIndex.tla *)
(* at Morpheus 424ac459, agent D), with the repair as it is shipped:       *)
(*                                                                         *)
(*   - A failed index task records a repair for its cell in memory         *)
(*     (src/index/repair.rs `IndexRepairs::record`). A record has a        *)
(*     generation; a new failure of the same cell increments it.           *)
(*   - A worker takes the generation, then reads the cell, then re-asserts *)
(*     the entries that the cell implies (insert only), then removes the   *)
(*     record if the generation did not change. Each is its own step: the  *)
(*     worker holds no lock, so writes and their tasks interleave.         *)
(*   - C3: an index query of a schema with a pending repair answers a      *)
(*     typed error (`IndexRepairPending`), not fewer rows. The query is    *)
(*     any index reader of the process: `IndexedDataClient` and every      *)
(*     ranged seek (`RangedIndexerClient::seek`, which `client::ranged`    *)
(*     uses). The records are per process: one process is modelled.        *)
(*   - A crash loses the records; the recovery reconcile (Repair, the      *)
(*     default) re-derives the whole index before the store serves.        *)
(*                                                                         *)
(* One cell. `idx` is the set of values whose index entry names the cell.  *)
(* An entry for a value the cell does not hold is stale; a query reads the *)
(* cell of every candidate and drops it, so a stale entry costs a read and *)
(* not a row. A missing entry (the cell's value not in `idx`) costs a row. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    TaskFail,     \* an index task can fail
    MaxFails,     \* how many task failures in all
    Crash,        \* one crash can happen
    Reconcile,    \* the recovery reconcile runs (the default)
    Fix,          \* "None": the code before the fix (a failure only logs)
                  \* "Repair": the record and the worker
    GenCheck,     \* TRUE: the worker removes the record only if its
                  \* generation did not change (shipped). FALSE: control.
    QueryRefusal, \* C3: a query answers an error while a repair is pending
    Plain,        \* plain writes of the cell happen beside transactions
    MaxWrites

Vals == {0, 1, 2}

VARIABLES
    cell,       \* the published value
    decided,    \* the last committed value (what a crash recovers)
    idx,        \* index entries that name the cell
    tasks,      \* pending index tasks
    txn,        \* the transaction on the cell: [st, old, new]
    nw,         \* writes so far
    fails,      \* task failures so far
    crashed,
    debt,       \* a repair record is pending
    gen,        \* its generation
    rep         \* the worker: [st |-> "idle"] or [st, g, v]

vars == <<cell, decided, idx, tasks, txn, nw, fails, crashed, debt, gen, rep>>

NoTxn == [st |-> "none", old |-> 0, new |-> 0]
Idle == [st |-> "idle", g |-> 0, v |-> 0]

Init ==
    /\ cell = 0 /\ decided = 0 /\ idx = {0} /\ tasks = {}
    /\ txn = NoTxn /\ nw = 0 /\ fails = 0 /\ crashed = FALSE
    /\ debt = FALSE /\ gen = 0 /\ rep = Idle

Change(old, new) ==
    IF old = new THEN {} ELSE {[k |-> "rm", v |-> old], [k |-> "add", v |-> new]}

\* The transaction apply publishes the cell and queues its index tasks. The
\* index gate holds the decision and the undo until the forward tasks end
\* (data_site.rs `commit`), so the decision waits for `tasks = {}`.
Apply(new) ==
    /\ txn.st = "none" /\ nw < MaxWrites /\ tasks = {}
    /\ txn' = [st |-> "applied", old |-> cell, new |-> new]
    /\ cell' = new
    /\ tasks' = tasks \cup Change(cell, new)
    /\ nw' = nw + 1
    /\ UNCHANGED <<decided, idx, fails, crashed, debt, gen, rep>>

Commit ==
    /\ txn.st = "applied" /\ tasks = {}
    /\ decided' = cell
    /\ txn' = NoTxn
    /\ UNCHANGED <<cell, idx, tasks, nw, fails, crashed, debt, gen, rep>>

Abort ==
    /\ txn.st = "applied" /\ tasks = {}
    /\ cell' = txn.old
    /\ tasks' = tasks \cup Change(txn.new, txn.old)
    /\ txn' = NoTxn
    /\ UNCHANGED <<decided, idx, nw, fails, crashed, debt, gen, rep>>

\* A plain write: refused while a transaction holds the cell (fix G). It is
\* committed when it is written. Its request waits for its own tasks. Two
\* plain writes of one cell are taken in sequence here (`tasks = {}`):
\* concurrent plain writes of one cell can reorder their tasks in the code,
\* which is a separate question that this model does not ask.
PlainWrite(new) ==
    /\ Plain /\ txn.st = "none" /\ nw < MaxWrites /\ tasks = {}
    /\ cell' = new
    /\ decided' = new
    /\ tasks' = tasks \cup Change(cell, new)
    /\ nw' = nw + 1
    /\ UNCHANGED <<idx, txn, fails, crashed, debt, gen, rep>>

Exec(t) == IF t.k = "add" THEN idx \cup {t.v} ELSE idx \ {t.v}

RunTask(t) ==
    /\ t \in tasks
    /\ idx' = Exec(t)
    /\ tasks' = tasks \ {t}
    /\ UNCHANGED <<cell, decided, txn, nw, fails, crashed, debt, gen, rep>>

\* The indexer RPC fails. Before the fix: a warning. With the fix: a repair
\* record, or a new generation of the pending one.
FailTask(t) ==
    /\ TaskFail /\ fails < MaxFails /\ t \in tasks
    /\ tasks' = tasks \ {t}
    /\ fails' = fails + 1
    /\ debt' = (debt \/ Fix = "Repair")
    /\ gen' = IF Fix = "Repair" THEN gen + 1 ELSE gen
    /\ UNCHANGED <<cell, decided, idx, txn, nw, crashed, rep>>

\* The worker takes the generation, then reads the cell (the read comes
\* after the generation: a failure recorded before the read is covered by it).
RepairTake ==
    /\ Fix = "Repair" /\ debt /\ rep.st = "idle"
    /\ rep' = [st |-> "taken", g |-> gen, v |-> 0]
    /\ UNCHANGED <<cell, decided, idx, tasks, txn, nw, fails, crashed, debt, gen>>

RepairRead ==
    /\ rep.st = "taken"
    /\ rep' = [rep EXCEPT !.st = "read", !.v = cell]
    /\ UNCHANGED <<cell, decided, idx, tasks, txn, nw, fails, crashed, debt, gen>>

\* Re-assert the entries of the value read. Insert only: a remove of an entry
\* that the cell does not imply now could race a write that implies it.
RepairInsert ==
    /\ rep.st = "read"
    /\ idx' = idx \cup {rep.v}
    /\ rep' = [rep EXCEPT !.st = "inserted"]
    /\ UNCHANGED <<cell, decided, tasks, txn, nw, fails, crashed, debt, gen>>

\* The insert fails (the indexer is still down): the record stays. It counts
\* as a failure: `RepairEnds` holds only if the indexer works again.
RepairInsertFails ==
    /\ TaskFail /\ fails < MaxFails /\ rep.st = "read"
    /\ rep' = Idle
    /\ fails' = fails + 1
    /\ UNCHANGED <<cell, decided, idx, tasks, txn, nw, crashed, debt, gen>>

RepairClear ==
    /\ rep.st = "inserted"
    /\ debt' = IF ~GenCheck \/ gen = rep.g THEN FALSE ELSE debt
    /\ rep' = Idle
    /\ UNCHANGED <<cell, decided, idx, tasks, txn, nw, fails, crashed, gen>>

\* Crash and recovery: tasks, the undecided transaction and the repair
\* records are memory; the reconcile re-derives the index before serving.
CrashRecover ==
    /\ Crash /\ ~crashed
    /\ crashed' = TRUE
    /\ cell' = decided
    /\ tasks' = {}
    /\ txn' = NoTxn
    /\ idx' = IF Reconcile THEN {decided} ELSE idx
    /\ debt' = FALSE
    /\ rep' = Idle
    /\ UNCHANGED <<decided, nw, fails, gen>>

Next ==
    \/ \E v \in Vals : Apply(v) \/ PlainWrite(v)
    \/ \E t \in tasks : RunTask(t) \/ FailTask(t)
    \/ Commit \/ Abort
    \/ RepairTake \/ RepairRead \/ RepairInsert \/ RepairInsertFails \/ RepairClear
    \/ CrashRecover

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(RepairTake) /\ WF_vars(RepairRead)
            /\ WF_vars(RepairInsert) /\ WF_vars(RepairClear)
            /\ WF_vars(\E t \in tasks : RunTask(t)) /\ WF_vars(Commit \/ Abort)

\* No write and no index task in flight.
Quiet == txn.st = "none" /\ tasks = {}

\* What a query for the cell's value answers at a quiet moment.
QueryAnswer ==
    IF QueryRefusal /\ debt THEN "error"
    ELSE IF cell \in idx THEN "hit" ELSE "miss"

\* A query never answers fewer rows without an error: at a quiet moment the
\* query for the cell's value finds it, or answers an error.
NoSilentMiss == Quiet => QueryAnswer # "miss"

\* Every repair record is worked off (liveness, under FairSpec).
RepairEnds == debt ~> ~debt

\* Sanity, violated on purpose: a failed task is repaired and the index then
\* serves the cell's value.
SanityRepaired == ~(fails > 0 /\ ~debt /\ Quiet /\ cell \in idx /\ cell # 0
                    /\ ~crashed)
=============================================================================
