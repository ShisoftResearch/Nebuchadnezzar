----------------------------- MODULE OccCommit -----------------------------
(***************************************************************************)
(* The OCC transaction of Neb: the client retry loop, the coordinator      *)
(* (`TransactionManager`) and the participants (`DataManager`), with the   *)
(* lease sweep and the participant fence (D29) for a coordinator that      *)
(* goes silent.                                                            *)
(*                                                                         *)
(* Sources (all paths under src/):                                         *)
(*   M  = server/transactions/manager.rs                                   *)
(*   D  = server/transactions/data_site.rs                                 *)
(*   CL = client/mod.rs, CT = client/transaction.rs                        *)
(*                                                                         *)
(* What one transaction does in the code:                                  *)
(*   1. The closure reads cells. A read waits while any transaction owns   *)
(*      the cell lock (D:2755 `prepare_read`). The coordinator caches the  *)
(*      read: later reads repeat it (M:1046 `full_read`).                  *)
(*   2. `prepare` (M:2273 `do_prepare`): every site of a read or written   *)
(*      cell gets a prepare (D:9279). The site locks every cell of the     *)
(*      transaction (read and write intent), Wait-Die on a held lock,      *)
(*      and checks the cached versions. Then every site gets `commit`      *)
(*      (D:9475 -> D:3000 `apply_commit_ops`), which PUBLISHES the writes  *)
(*      and keeps the undo history. Only after that is the client told     *)
(*      that prepare succeeded.                                            *)
(*   3. `commit` (M:921 -> M:1931 `sites_end`): `end` to every site.        *)
(*      `end` (D:1464) closes the brackets, releases the locks and records *)
(*      the decision. A multi-site `end` sets `decided_commit` first.      *)
(*   4. A failure in 2 runs `abort` (M:645 -> M:1821): `abort` and then    *)
(*      `end` on every site. The client retries a NotRealizable failure    *)
(*      with a NEW transaction (CL:1389), else it returns the error.       *)
(*   5. A site whose applied transaction idles past the lease runs the     *)
(*      sweep (D:1411): a decided commit retries its close; a single-site  *)
(*      transaction is presumed aborted; a multi-site one runs the         *)
(*      participant fence (D:2083 `terminate_in_doubt`, D:2222).           *)
(*                                                                         *)
(* Abstractions (each one is the weakest behaviour the code allows):       *)
(*   - Time is not modelled. A timeout (read wait, prepare wait, lease,    *)
(*     stale lock, `finish_aborted` idle) can fire at any step at which    *)
(*     its guard in the code holds.                                        *)
(*   - Each RPC is atomic at its target. A lossy RPC is either not         *)
(*     delivered, or delivered with its reply lost.                        *)
(*   - Clocks are causal (HLC observe on every reply), so `ReadTooLate`    *)
(*     and the Thomas write rule skip in `apply_commit_ops` never fire.    *)
(*     The argument is in docs/tla/README.md.                              *)
(*   - Cell versions are the writer's tid, so no version repeats (ABA).   *)
(*   - Cells are updated, never created or removed. One cell per site.    *)
(*   - The coordinator can stop at any step. A site can crash and restart *)
(*     (`SiteCrashes`): a process crash, so appended records survive and  *)
(*     memory is lost. Power loss (unsynced records lost) is not modelled. *)
(*   - Index entries are not modelled: a cell value stands for the cell    *)
(*     and its index entries.                                              *)
(*   - The status cell is released only into the decision map, which is   *)
(*     never evicted (64 Ki entries, D:671).                               *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    Workload,     \* "LU": 2 txns read and write x (one site).
                  \* "WS": T1 reads x,y writes x; T2 reads x,y writes y.
                  \* "MS": 2 txns read and write x and y (two sites).
                  \* "M1": 1 txn reads and writes x and y.
                  \* "R1": 1 txn reads x and y, writes x.
                  \* "RO": T1 writes x and y; T2 only reads x and y.
    Retry,        \* TRUE: client 1 has a second attempt (tid 3).
    Lossy,        \* RPCs can be lost before or after the target.
    CoordCrash,   \* The coordinator can stop at any step.
    Sweep,        \* The lease sweep and the participant fence run.
    FenceInDoubt, \* TRUE: the code (D29). FALSE: control, the sweep
                  \* presumes abort for a multi-site transaction without
                  \* asking its peers (the behaviour before D29).
    RestoreFail,  \* A rollback can fail to restore a cell (D:1170).
    ApplyFail,    \* An apply can fail after a write (WAL error, D:3000).
    CloseRefuse,  \* A close of the brackets can be refused (D:1867).
    StaleReclaim, \* A prepare can reclaim a stale lock (D:9414).
    Timeouts,     \* The read wait and the prepare wait can time out.
    Certify,      \* TRUE: the code. FALSE: control, prepare and apply do
                  \* not check the cached versions (lost update).
    FixEndAborted,\* Candidate fix for F1: `end` of an aborted object
                  \* answers AlreadyAborted, not Success.
    FixStaleSweep,\* Candidate fix for F2: the sweep also ends an idle
                  \* object without leases (Started, Prepared, or a
                  \* read-only part): it releases its locks and wipes it.
    FixReadReclaim,\* Candidate fix for F2: a transactional read reclaims
                  \* a stale lock as a prepare does.
    SiteCrashes,  \* Sites can crash and restart, at most MaxCrashes times
                  \* in all (process crash: appended records survive,
                  \* memory is lost).
    MaxCrashes,
    FixPendingUnknown, \* Candidate fix for F5: the coordinator reports
                  \* DecidedPending as OutcomeUnknown, not as committed.
    FixNoReclaimDecided, \* Candidate fix for F7: a lock of a decided object
                  \* whose close is not finished is never reclaimed as stale.
    FixPlainRespectsLocks, \* Candidate fix for F6: a plain write is refused
                  \* (retryable) while a transaction owns the cell or the cell
                  \* is in doubt after a restart.
    FixDurableDecision, \* Candidate fix for F5: `decided_commit` of a
                  \* multi-site part is made durable before `end` answers;
                  \* recovery commits from it.
    PlainWrites,  \* One plain (non-transactional) write to `x`, which
                  \* ignores transaction locks (cell_rpc.rs admit_write).
    PlainAtomic,  \* TRUE: the check of fix G and the write are one step
                  \* (the write holds the cell lock from its check to its
                  \* end). FALSE: control, the write checks, then writes.
    FixApplyFenced, \* Candidate fix for F3: `commit` (the apply) refuses a
                  \* transaction that a participant fenced on this site.
    FixROValidate \* Candidate fix for F4: a read-only transaction that read
                  \* 2 or more cells validates each read at its end: the cell
                  \* has no lock owner (else wait) and still holds the version
                  \* that was read (else retry).

ASSUME Workload \in {"LU", "WS", "MS", "M1", "R1", "RO"}

Sites == {1, 2}
Cells == {"x", "y"}
SiteOf(c) == IF c = "x" THEN 1 ELSE 2
NoTxn == 0
Unread == -1

\* Tid 3 is the retry of tid 1 (same client). A retry begins later, so it is
\* younger than every tid before it (HLC `begin`, M:704). Smaller is older.
NTxn == IF Workload \in {"M1", "R1"} THEN 1 ELSE 2
Tids == IF Retry THEN (1..NTxn) \cup {3} ELSE 1..NTxn
ClientOf(t) == IF t = 3 THEN 1 ELSE t
NextAttempt(t) == IF t = 1 /\ Retry THEN 3 ELSE NoTxn

Spec1(t) == IF t = 3 THEN 1 ELSE t   \* the closure of tid t
RS(t) ==
    CASE Workload = "LU" -> {"x"}
      [] OTHER -> {"x", "y"}
WS(t) ==
    LET k == Spec1(t) IN
    CASE Workload = "LU" -> {"x"}
      [] Workload = "WS" -> IF k = 1 THEN {"x"} ELSE {"y"}
      [] Workload = "MS" -> {"x", "y"}
      [] Workload = "M1" -> {"x", "y"}
      [] Workload = "R1" -> {"x"}
      [] Workload = "RO" -> IF k = 1 THEN {"x", "y"} ELSE {}

\* The sites of the transaction: every site of a cached cell (M:1598
\* `generate_affected_objs`); participants: the sites with a write (M:1714).
SitesOf(t) == {SiteOf(c) : c \in RS(t) \cup WS(t)}
Part(t) == {SiteOf(c) : c \in WS(t)}
CellsAt(t, s) == {c \in RS(t) \cup WS(t) : SiteOf(c) = s}
WritesAt(t, s) == {c \in WS(t) : SiteOf(c) = s}
\* D:2493 `holds_multi_site_part`.
MultiPart(t, s) == s \in Part(t) /\ Cardinality(Part(t)) >= 2

NoObj == [st |-> "none", hist |-> FALSE, dc |-> FALSE, cd |-> FALSE,
          fenced |-> FALSE, unrest |-> FALSE, restored |-> FALSE,
          rec |-> FALSE, lf |-> FALSE]
\* `lf` is a ghost: the apply ran on an object that a participant had
\* fenced (F3). The plain readers of the site then see the writes of a
\* transaction that the fence decides to abort.
PlainTid == 9       \* the writer id of the plain write

VARIABLES
    val,      \* cell -> the tid of its writer (0 = initial)
    owner,    \* cell -> lock owner (D: CellMeta.owner)
    obj,      \* site -> tid -> participant object (D: Transaction)
    undo,     \* tid -> cell -> value before the apply (D: CellHistory)
    dec,      \* site -> tid -> "none" | "Committed" | "Aborted" (D: decided)
    status,   \* site -> tid -> "none" | "committing" | "committed"
              \* (D: txn_status registry, self_synced)
    pc,       \* tid -> coordinator / client phase
    cache,    \* tid -> cell -> cached read (M: DataObject)
    pend,     \* tid -> sites that have not answered the current fan-out
    res,      \* tid -> site -> answer of the current fan-out
    why,      \* tid -> "retry" | "error": the reason of an abort
    told,     \* tid -> the outcome the client got
    sw,       \* site -> tid -> step of the participant fence
    gdec,     \* ghost: site -> tid -> every decision the site ever recorded
    crashes,  \* how many site crashes happened
    plain     \* the plain write: "no", "checked" (fix G passed, the write is
              \* not done yet; PlainAtomic = FALSE only), "done", or "lost"
              \* (its site crashed before the write)

vars == <<val, owner, obj, undo, dec, status, pc, cache, pend, res, why,
          told, sw, gdec, crashes, plain>>

Init ==
    /\ val = [c \in Cells |-> 0]
    /\ owner = [c \in Cells |-> NoTxn]
    /\ obj = [s \in Sites |-> [t \in Tids |-> NoObj]]
    /\ undo = [t \in Tids |-> [c \in Cells |-> 0]]
    /\ dec = [s \in Sites |-> [t \in Tids |-> "none"]]
    /\ status = [s \in Sites |-> [t \in Tids |-> "none"]]
    /\ pc = [t \in Tids |-> IF t = 3 THEN "init" ELSE "body"]
    /\ cache = [t \in Tids |-> [c \in Cells |-> Unread]]
    /\ pend = [t \in Tids |-> {}]
    /\ res = [t \in Tids |-> [s \in Sites |-> "none"]]
    /\ why = [t \in Tids |-> "none"]
    /\ told = [t \in Tids |-> "none"]
    /\ sw = [s \in Sites |-> [t \in Tids |-> "idle"]]
    /\ gdec = [s \in Sites |-> [t \in Tids |-> {}]]
    /\ crashes = 0
    /\ plain = "no"

-----------------------------------------------------------------------------
(* Helpers on the participant side.                                        *)

\* D:999 `owner_waits_with_writes`: the lock is never reclaimed as stale.
OwnerWaitsWithWrites(o, s) ==
    /\ obj[s][o].st # "none"
    /\ (~obj[s][o].dc \/ FixNoReclaimDecided)
    /\ obj[s][o].hist
    /\ ~obj[s][o].restored

\* Release the locks of `t` on the cells of `t` at `s`, except `keep`
\* (D:2884 `attempt_lock_release`: only an owner equal to `t` is cleared).
Released(t, s, keep) ==
    [c \in Cells |-> IF SiteOf(c) = s /\ c \in CellsAt(t, s) /\ c \notin keep
                         /\ owner[c] = t
                     THEN NoTxn ELSE owner[c]]

\* The rollback of the writes of `t` at `s` (D:1170 `rollback`, D:1261).
\* `fail`: the restore of the written cell fails (one cell per site).
\* A restore also fails when the cell is not at the version the transaction
\* wrote (`restore_cell_location`, src/ram/chunk.rs:4535): a plain write
\* overwrote it.
Mismatch(t, s) == \E c \in WritesAt(t, s) : val[c] # t
RolledBack(t, s, fail) ==
    [c \in Cells |-> IF c \in WritesAt(t, s) /\ ~fail /\ val[c] = t
                     THEN undo[t][c] ELSE val[c]]

\* D:2672 `txn_outcome_with_branch`, the answer of `txn_status`.
Outcome(s, t) ==
    IF status[s][t] = "committed" THEN "Committed"
    ELSE IF status[s][t] = "committing" THEN "Committing"
    ELSE IF obj[s][t].st # "none"
         THEN IF obj[s][t].dc THEN "Committing"
              ELSE IF obj[s][t].st = "Aborted" THEN "Aborted"
              ELSE "InDoubt"
    ELSE IF dec[s][t] # "none" THEN dec[s][t]
    ELSE "Unknown"

\* D:2337 `fence_here_inner`: the answer, and whether it sets `fenced`.
FenceAnswer(s, t) ==
    IF dec[s][t] = "Committed" THEN "Committing"
    ELSE IF dec[s][t] = "Aborted" THEN "Aborted"
    ELSE IF status[s][t] # "none" THEN "Committing"
    ELSE IF obj[s][t].st = "none" THEN "Unknown"
    ELSE IF obj[s][t].dc THEN "Committing"
    ELSE IF obj[s][t].st = "Aborted" THEN "Aborted"
    ELSE "Fenced"

-----------------------------------------------------------------------------
(* The participant operations. Each is one atomic step at one site. They  *)
(* are written as relations on the primed variables they change; the      *)
(* answer goes in `r`.                                                     *)

\* D:9279 `prepare`. Stale reclaims are the separate action `Reclaim`.
PrepareAnswer(t, s) ==
    LET o == obj[s][t]
        held == {c \in CellsAt(t, s) : owner[c] \notin {NoTxn, t}}
    IN  IF o.fenced THEN "stateerr"                         \* D:9363
        ELSE IF o.st \notin {"none", "Started"} THEN "stateerr" \* D:9369
        ELSE IF held # {}
             THEN LET c == CHOOSE c \in held : TRUE IN
                  IF t > owner[c] THEN "notreal" ELSE "wait" \* D:9420-9433
        ELSE IF Certify /\ \E c \in CellsAt(t, s) : val[c] # cache[t][c]
             THEN "notreal"                                 \* D:9440
        ELSE "ok"

DoPrepare(t, s, r) ==
    /\ r = PrepareAnswer(t, s)
    /\ IF r = "ok"
       THEN /\ owner' = [c \in Cells |-> IF c \in CellsAt(t, s) THEN t
                                          ELSE owner[c]]
            /\ obj' = [obj EXCEPT ![s][t].st = "Prepared"]
       ELSE /\ owner' = owner
            \* `get_or_create_transaction` leaves a Started object behind
            \* (D:9357); only a fenced refusal wipes it (D:9468).
            /\ obj' = [obj EXCEPT ![s][t].st =
                         IF @ = "none" THEN "Started" ELSE @]
    /\ UNCHANGED <<val, undo, dec, status>>

\* D:9475 `commit` -> D:3000 `apply_commit_ops`. `fail`: a write or the
\* WAL sync fails after a write; the partial apply is rolled back
\* (D:3371 `undo_partial_apply`, D:447) and the object is Aborted.
DoApply(t, s, r, fail) ==
    LET o == obj[s][t] IN
    IF o.st = "none" THEN r = "error" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF o.st # "Prepared"
         THEN r = "error" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF \E c \in CellsAt(t, s) : owner[c] # t                \* D:3047
         THEN r = "error" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF Certify /\ \E c \in WritesAt(t, s) : val[c] # cache[t][c] \* D:3053
         THEN r = "changed" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF FixApplyFenced /\ o.fenced
         THEN r = "error" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF fail /\ WritesAt(t, s) # {}
         THEN /\ r = "error"
              /\ obj' = [obj EXCEPT ![s][t].st = "Aborted",
                                    ![s][t].hist = TRUE,
                                    ![s][t].restored = TRUE]
              /\ UNCHANGED <<val, owner, undo, dec, status>>
    ELSE /\ r = "ok"
         /\ undo' = [undo EXCEPT ![t] = [c \in Cells |->
                        IF c \in WritesAt(t, s) THEN val[c] ELSE @[c]]]
         /\ val' = [c \in Cells |-> IF c \in WritesAt(t, s) THEN t ELSE val[c]]
         /\ obj' = [obj EXCEPT ![s][t].st = "Committed",
                               ![s][t].hist = (WritesAt(t, s) # {}),
                               ![s][t].lf = (o.fenced /\ WritesAt(t, s) # {})]
         /\ UNCHANGED <<owner, dec, status>>

\* `finish_aborted` (D:1726) after `mark_restored` (D:1017), on `o2`, the
\* object after an abort.
FinishAbortedOn(t, s, o2) ==
    LET restored == o2.st = "Aborted" /\ ~o2.unrest IN
    /\ owner' = IF o2.st = "Aborted"
                THEN Released(t, s, IF o2.unrest THEN WritesAt(t, s) ELSE {})
                ELSE owner
    /\ IF o2.st = "Aborted" /\ restored
       THEN /\ obj' = [obj EXCEPT ![s][t] = NoObj]
            /\ dec' = [dec EXCEPT ![s][t] = "Aborted"]
       ELSE /\ obj' = [obj EXCEPT ![s][t] = [o2 EXCEPT !.restored = restored]]
            /\ dec' = dec

\* D:1311 `abort_locked`. Answer and the object after it; `failRestore`
\* picks a failed restore. Does not release locks.
AbortLockedAnswer(t, s) ==
    LET o == obj[s][t] IN
    IF o.st = "none" \/ o.rec THEN "notexist"
    ELSE IF o.st = "Aborted" THEN "already"                   \* D:1312
    ELSE IF o.dc THEN "committed"                             \* D:1315
    ELSE IF ~o.hist THEN "ok"                                 \* D:1324
    ELSE IF \E c \in CellsAt(t, s) : owner[c] # t THEN "cannotend" \* D:1357
    ELSE "ok"

AbortedObj(t, s, failRestore) ==
    LET o == obj[s][t] IN
    IF o.hist
    THEN [o EXCEPT !.st = "Aborted", !.unrest = failRestore \/ Mismatch(t, s)]
    ELSE [o EXCEPT !.st = "Aborted"]

\* D:9542 `abort`: abort_locked, then mark_restored.
DoAbort(t, s, r, failRestore) ==
    /\ r = AbortLockedAnswer(t, s)
    /\ IF r = "ok"
       THEN LET o2 == AbortedObj(t, s, failRestore /\ obj[s][t].hist) IN
            /\ obj' = [obj EXCEPT ![s][t] =
                         [o2 EXCEPT !.restored = ~o2.unrest]]
            /\ val' = IF obj[s][t].hist
                      THEN RolledBack(t, s, failRestore) ELSE val
       ELSE UNCHANGED <<obj, val>>
    /\ UNCHANGED <<owner, undo, dec, status>>

\* D:1647 `presume_abort` (+ `presume_abort_locked`, D:1709).
DoPresumeAbort(t, s, failRestore) ==
    LET o == obj[s][t]
        a == AbortLockedAnswer(t, s)
    IN
    IF o.st = "none" \/ o.dc
    THEN UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF o.rec
         \* A bracket in doubt after a restart is discarded (src/server/mod.rs
         \* `discard_in_doubt`): the index already shows the previous version.
         THEN /\ owner' = Released(t, s, {})
              /\ dec' = [dec EXCEPT ![s][t] = "Aborted"]
              /\ obj' = [obj EXCEPT ![s][t] = NoObj]
              /\ UNCHANGED <<val, undo, status>>
    ELSE IF a = "cannotend"
         \* D:1683 "other": record Aborted and wipe; no lock is released.
         THEN /\ dec' = [dec EXCEPT ![s][t] = "Aborted"]
              /\ obj' = [obj EXCEPT ![s][t] = NoObj]
              /\ UNCHANGED <<val, owner, undo, status>>
    ELSE LET fr == failRestore /\ o.hist /\ a = "ok"
             o2 == IF a = "ok" THEN AbortedObj(t, s, fr) ELSE o
         IN /\ val' = IF a = "ok" /\ o.hist THEN RolledBack(t, s, fr) ELSE val
            /\ FinishAbortedOn(t, s, o2)
            /\ UNCHANGED <<undo, status>>

\* D:1464 `end_transaction_locked` with D:1867 `close_committed_brackets`.
\* `refuse` picks a refused close; `mode` its kind for one site:
\* "durable", "unresolved", "retracted" (D:1938-1990).
DoEnd(t, s, r, refuse, mode, failRestore) ==
    LET o == obj[s][t] IN
    IF o.st = "none" \/ o.rec
    THEN r = "notexist" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF o.st \notin {"Aborted", "Committed"}
    THEN r = "checkfailed" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF o.st = "Aborted"
    \* D:1489-1620: release the locks of the restored cells, record Aborted,
    \* wipe if nothing waits for restoration -- and answer Success.
    THEN /\ r = IF FixEndAborted THEN "aborted" ELSE "success"
         /\ owner' = Released(t, s, IF o.unrest THEN WritesAt(t, s) ELSE {})
         /\ dec' = [dec EXCEPT ![s][t] = "Aborted"]
         /\ obj' = IF o.unrest THEN obj ELSE [obj EXCEPT ![s][t] = NoObj]
         /\ UNCHANGED <<val, undo, status>>
    ELSE IF o.fenced                                            \* D:1877
    THEN r = "checkfailed" /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    ELSE IF MultiPart(t, s)
    THEN IF refuse
         \* D:1902-1945: decided, close pending; the sweep retries it.
         THEN /\ r = "pending"
              /\ obj' = [obj EXCEPT ![s][t].dc = TRUE, ![s][t].cd = TRUE]
              /\ UNCHANGED <<val, owner, undo, dec, status>>
         ELSE /\ r = "success"
              /\ status' = [status EXCEPT ![s][t] = "committing"]
              /\ owner' = Released(t, s, {})
              /\ dec' = [dec EXCEPT ![s][t] = "Committed"]
              /\ obj' = [obj EXCEPT ![s][t] = NoObj]
              /\ UNCHANGED <<val, undo>>
    ELSE IF refuse /\ o.hist
    THEN CASE mode = "durable" ->
              /\ r = "pendingsuccess"
              /\ obj' = [obj EXCEPT ![s][t].dc = TRUE, ![s][t].cd = TRUE]
              /\ dec' = [dec EXCEPT ![s][t] = "Committed"]
              /\ UNCHANGED <<val, owner, undo, status>>
           [] mode = "unresolved" ->
              /\ r = "refusal"
              /\ obj' = [obj EXCEPT ![s][t].dc = TRUE]
              /\ UNCHANGED <<val, owner, undo, dec, status>>
           [] mode = "retracted" /\ o.dc ->
              /\ r = IF o.cd THEN "pendingsuccess" ELSE "refusal"
              /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
           [] mode = "retracted" /\ ~o.dc ->
              \* abort_locked, then RolledBack: mark_restored, finish_aborted
              \* (D:1505-1513).
              LET o2 == AbortedObj(t, s, failRestore) IN
              /\ r = "refusal"
              /\ val' = RolledBack(t, s, failRestore)
              /\ FinishAbortedOn(t, s, o2)
              /\ UNCHANGED <<undo, status>>
    ELSE /\ r = "success"
         /\ owner' = Released(t, s, {})
         /\ dec' = [dec EXCEPT ![s][t] = "Committed"]
         /\ obj' = [obj EXCEPT ![s][t] = NoObj]
         /\ UNCHANGED <<val, undo, status>>

Modes == {"durable", "unresolved", "retracted"}
Bools(flag) == IF flag THEN BOOLEAN ELSE {FALSE}

-----------------------------------------------------------------------------
(* The client and the coordinator.                                         *)

CoordUnchanged == UNCHANGED <<pc, cache, pend, res, why, told>>
SiteUnchanged == UNCHANGED <<val, owner, obj, undo, dec, status, sw>>

\* The client begins the next attempt of a client whose attempt aborted with
\* a NotRealizable reason (CL:1478-1485), or stops (bounded retries).
FinishAbortWith(t, w) ==
    LET n == NextAttempt(t)
        again == w = "retry" /\ n # NoTxn
    IN
    /\ told' = [told EXCEPT ![t] =
                  IF w = "retry" /\ n = NoTxn THEN "toomany" ELSE w]
    /\ pc' = IF again THEN [pc EXCEPT ![t] = "done", ![n] = "body"]
            ELSE [pc EXCEPT ![t] = "done"]

\* A failure: abort every site of the transaction (M:645 -> M:1821).
StartAbort(t, reason) ==
    /\ pc' = [pc EXCEPT ![t] = "abort"]
    /\ pend' = [pend EXCEPT ![t] = SitesOf(t)]
    /\ why' = [why EXCEPT ![t] = reason]
    /\ res' = [res EXCEPT ![t] = [s \in Sites |-> "none"]]

\* A transactional read (M:1046 -> D:9103, D:2755): waits while the cell
\* has an owner; the coordinator caches the value.
Read(t, c) ==
    /\ pc[t] = "body"
    /\ c \in RS(t)
    /\ cache[t][c] = Unread
    /\ IF owner[c] = NoTxn THEN TRUE
       ELSE FixReadReclaim /\ ~OwnerWaitsWithWrites(owner[c], SiteOf(c))
    /\ cache' = [cache EXCEPT ![t][c] = val[c]]
    /\ owner' = [owner EXCEPT ![c] = NoTxn]
    /\ UNCHANGED <<pc, pend, res, why, told>>
    /\ UNCHANGED <<val, obj, undo, dec, status, sw>>

\* The read waits up to `max_total_wait_ms` and returns Rejected
\* (M:1203); the client gets ReadTooLate and retries (CT:103, CL:1478).
\* Nothing was prepared, so the abort touches no site.
ReadTimeout(t) ==
    /\ Timeouts
    /\ pc[t] = "body"
    /\ \E c \in RS(t) : cache[t][c] = Unread /\ owner[c] # NoTxn
    /\ why' = [why EXCEPT ![t] = "retry"]
    /\ FinishAbortWith(t, "retry")
    /\ UNCHANGED <<cache, pend, res>>
    /\ SiteUnchanged

\* The closure returns. A read-only transaction has no affected objects
\* (M:1598): prepare, commit and end touch no site, and it commits.
BodyDone(t) ==
    /\ pc[t] = "body"
    /\ \A c \in RS(t) : cache[t][c] # Unread
    /\ IF WS(t) = {} /\ ~(FixROValidate /\ Cardinality(RS(t)) >= 2)
       THEN /\ pc' = [pc EXCEPT ![t] = "done"]
            /\ told' = [told EXCEPT ![t] = "committed"]
            /\ UNCHANGED <<pend, why>>
       ELSE IF WS(t) = {}
       THEN /\ pc' = [pc EXCEPT ![t] = "validate"]
            /\ pend' = [pend EXCEPT ![t] = SitesOf(t)]
            /\ UNCHANGED <<why, told>>
       ELSE /\ pc' = [pc EXCEPT ![t] = "prep"]
            /\ pend' = [pend EXCEPT ![t] = SitesOf(t)]
            /\ UNCHANGED <<why, told>>
    /\ res' = [res EXCEPT ![t] = [s \in Sites |-> "none"]]
    /\ UNCHANGED cache
    /\ SiteUnchanged

\* One site answers the prepare (M:1629 `site_prepare`). `Wait` keeps the
\* site pending: the coordinator backs off and asks again.
SitePrepare(t, s) ==
    /\ pc[t] = "prep"
    /\ s \in pend[t]
    /\ \E r \in {"ok", "wait", "notreal", "stateerr"},
          delivery \in (IF Lossy THEN {"ok", "lost", "noreply"} ELSE {"ok"}) :
        /\ IF delivery = "lost"
           THEN UNCHANGED <<val, owner, obj, undo, dec, status>> /\ r = "ok"
           ELSE DoPrepare(t, s, r)
        /\ IF delivery = "ok" /\ r = "wait"
           THEN UNCHANGED <<pend, res>>
           ELSE /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
                /\ res' = [res EXCEPT ![t][s] =
                              IF delivery = "ok" THEN r ELSE "err"]
    /\ UNCHANGED <<pc, cache, why, told, sw>>

\* M:1640: after a Wait the coordinator gives up at `max_total_wait_ms`.
PrepareTimeout(t, s) ==
    /\ Timeouts
    /\ pc[t] = "prep"
    /\ s \in pend[t]
    /\ PrepareAnswer(t, s) = "wait"
    /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
    /\ res' = [res EXCEPT ![t][s] = "notreal"]
    /\ UNCHANGED <<pc, cache, why, told>>
    /\ SiteUnchanged

\* M:2188 `reduce_prepare_results`, M:2326, CT:206, CL:1478.
PrepareDone(t) ==
    /\ pc[t] = "prep"
    /\ pend[t] = {}
    /\ IF \A s \in SitesOf(t) : res[t][s] = "ok"
       THEN /\ pc' = [pc EXCEPT ![t] = "apply"]
            /\ pend' = [pend EXCEPT ![t] = SitesOf(t)]
            /\ res' = [res EXCEPT ![t] = [s \in Sites |-> "none"]]
            /\ UNCHANGED why
       ELSE /\ StartAbort(t, IF \E s \in SitesOf(t) : res[t][s] = "err"
                             THEN "error"
                             ELSE IF \E s \in SitesOf(t) : res[t][s] = "notreal"
                                  THEN "retry" ELSE "error")
    /\ UNCHANGED <<cache, told>>
    /\ SiteUnchanged

\* One site applies (M:1755 `sites_commit`, still the coordinator's
\* PREPARE phase).
SiteApply(t, s) ==
    /\ pc[t] = "apply"
    /\ s \in pend[t]
    /\ \E r \in {"ok", "error", "changed"},
          fail \in Bools(ApplyFail),
          delivery \in (IF Lossy THEN {"ok", "lost", "noreply"} ELSE {"ok"}) :
        /\ IF delivery = "lost"
           THEN UNCHANGED <<val, owner, obj, undo, dec, status>> /\ r = "ok"
           ELSE DoApply(t, s, r, fail)
        /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
        /\ res' = [res EXCEPT ![t][s] = IF delivery = "ok" THEN r ELSE "err"]
    /\ UNCHANGED <<pc, cache, why, told, sw>>

\* M:2296-2318: all applied -> Prepared, the client calls commit. Else
\* abort; CellChanged is NotRealizable (CT:217) and is retried.
ApplyDone(t) ==
    /\ pc[t] = "apply"
    /\ pend[t] = {}
    /\ IF \A s \in SitesOf(t) : res[t][s] = "ok"
       THEN /\ pc' = [pc EXCEPT ![t] = "end"]
            /\ pend' = [pend EXCEPT ![t] = SitesOf(t)]
            /\ res' = [res EXCEPT ![t] = [s \in Sites |-> "none"]]
            /\ UNCHANGED why
       ELSE /\ StartAbort(t, IF \E s \in SitesOf(t) : res[t][s] = "err"
                             THEN "error"
                             ELSE IF \E s \in SitesOf(t) : res[t][s] = "changed"
                                  THEN "retry" ELSE "error")
    /\ UNCHANGED <<cache, told>>
    /\ SiteUnchanged

\* Candidate fix for F4: the end of a read-only transaction validates its
\* reads on each site (one cell per site here, so one step per cell). A cell
\* with a lock owner (also an in-doubt cell after a restart) makes the
\* validation wait. A changed version is a NotRealizable retry. A lost RPC
\* is an error to the client (nothing was written).
ROValidate(t, s) ==
    /\ pc[t] = "validate"
    /\ s \in pend[t]
    /\ \A c \in CellsAt(t, s) : owner[c] = NoTxn
    /\ \E delivery \in (IF Lossy THEN {"ok", "lost"} ELSE {"ok"}) :
        IF delivery = "lost"
        THEN /\ why' = [why EXCEPT ![t] = "error"]
             /\ FinishAbortWith(t, "error")
             /\ UNCHANGED pend
        ELSE IF \A c \in CellsAt(t, s) : val[c] = cache[t][c]
        THEN /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
             /\ UNCHANGED <<pc, why, told>>
        ELSE /\ why' = [why EXCEPT ![t] = "retry"]
             /\ FinishAbortWith(t, "retry")
             /\ UNCHANGED pend
    /\ UNCHANGED <<cache, res>>
    /\ SiteUnchanged

\* The validation waits up to `max_total_wait_ms` on a held lock.
ROValidateTimeout(t, s) ==
    /\ Timeouts
    /\ pc[t] = "validate"
    /\ s \in pend[t]
    /\ \E c \in CellsAt(t, s) : owner[c] # NoTxn
    /\ why' = [why EXCEPT ![t] = "retry"]
    /\ FinishAbortWith(t, "retry")
    /\ UNCHANGED <<cache, pend, res>>
    /\ SiteUnchanged

ROValidateDone(t) ==
    /\ pc[t] = "validate"
    /\ pend[t] = {}
    /\ pc' = [pc EXCEPT ![t] = "done"]
    /\ told' = [told EXCEPT ![t] = "committed"]
    /\ UNCHANGED <<cache, pend, res, why>>
    /\ SiteUnchanged

\* The prepare reply to the client is lost, or the commit request is not
\* sent (CT:282 CommitNotApplied): the client aborts and returns the error.
\* The prepare lifecycle task also aborts an undelivered success (M:512).
ClientGivesUp(t) ==
    /\ Lossy
    /\ pc[t] = "end"
    /\ pend[t] = SitesOf(t)
    /\ \A s \in Sites : res[t][s] = "none"
    /\ StartAbort(t, "error")
    /\ UNCHANGED <<cache, told>>
    /\ SiteUnchanged

\* One site answers `end` for the commit (M:1931 `sites_end`).
SiteEnd(t, s) ==
    /\ pc[t] = "end"
    /\ s \in pend[t]
    /\ \E r \in {"success", "pending", "pendingsuccess", "refusal",
                 "checkfailed", "notexist", "aborted"},
          refuse \in Bools(CloseRefuse), mode \in Modes,
          fr \in Bools(RestoreFail),
          delivery \in (IF Lossy THEN {"ok", "lost", "noreply"} ELSE {"ok"}) :
        /\ IF delivery = "lost"
           THEN UNCHANGED <<val, owner, obj, undo, dec, status>> /\ r = "success"
           ELSE DoEnd(t, s, r, refuse, mode, fr)
        /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
        /\ res' = [res EXCEPT ![t][s] = IF delivery = "ok" THEN r ELSE "err"]
    /\ UNCHANGED <<pc, cache, why, told, sw>>

\* M:1985-2058 (multi-site) and M:2061 `end_sites` (one writer), with the
\* client mapping CT:234-305.
EndVerdict(t) ==
    IF Cardinality(Part(t)) >= 2
    THEN IF \E s \in Part(t) : res[t][s] = "success" THEN "committed"
         ELSE IF \E s \in Part(t) : res[t][s] \in {"pending", "refusal"}
              THEN IF FixPendingUnknown THEN "unknown"
                   ELSE "committed"              \* DecidedPending
         ELSE "unknown"
    ELSE LET w == CHOOSE s \in Part(t) : TRUE IN
         IF res[t][w] \in {"success", "pendingsuccess"} THEN "committed"
         ELSE "unknown"

EndDone(t) ==
    /\ pc[t] = "end"
    /\ pend[t] = {}
    /\ \E s \in Sites : res[t][s] # "none"
    /\ pc' = [pc EXCEPT ![t] = "done"]
    /\ told' = [told EXCEPT ![t] = EndVerdict(t)]
    /\ UNCHANGED <<cache, pend, res, why>>
    /\ SiteUnchanged

\* One site gets `abort` (M:1821 `abort_sites`, first fan-out). A site that
\* answers Success or AlreadyAborted gets `end` next (M:1853-1880).
SiteAbort(t, s) ==
    /\ pc[t] = "abort"
    /\ s \in pend[t]
    /\ \E r \in {"ok", "already", "committed", "cannotend", "notexist"},
          fr \in Bools(RestoreFail),
          delivery \in (IF Lossy THEN {"ok", "lost", "noreply"} ELSE {"ok"}) :
        /\ IF delivery = "lost"
           THEN UNCHANGED <<val, owner, obj, undo, dec, status>> /\ r = "ok"
           ELSE DoAbort(t, s, r, fr)
        /\ res' = [res EXCEPT ![t][s] =
                      IF delivery = "ok" /\ r \in {"ok", "already"}
                      THEN "needend" ELSE "aborted"]
        /\ pend' = [pend EXCEPT ![t] = @ \ {s}]
    /\ UNCHANGED <<pc, cache, why, told, sw>>

\* The second fan-out of `abort_sites`: `end` on the aborted site.
SiteAbortEnd(t, s) ==
    /\ pc[t] = "abort"
    /\ res[t][s] = "needend"
    /\ \E r \in {"success", "pending", "pendingsuccess", "refusal",
                 "checkfailed", "notexist", "aborted"},
          fr \in Bools(RestoreFail),
          delivery \in (IF Lossy THEN {"ok", "lost"} ELSE {"ok"}) :
        /\ IF delivery = "lost"
           THEN UNCHANGED <<val, owner, obj, undo, dec, status>> /\ r = "success"
           ELSE DoEnd(t, s, r, FALSE, "durable", fr)
        /\ res' = [res EXCEPT ![t][s] = "aborted"]
    /\ UNCHANGED <<pc, cache, pend, why, told, sw>>

AbortDone(t) ==
    /\ pc[t] = "abort"
    /\ pend[t] = {}
    /\ \A s \in Sites : res[t][s] # "needend"
    /\ FinishAbortWith(t, why[t])
    /\ UNCHANGED <<cache, pend, res, why>>
    /\ SiteUnchanged

\* The coordinator stops. The client of a commit in flight is told
\* OutcomeUnknown (CT:300); any earlier step gives it an RPC error.
Crash(t) ==
    /\ CoordCrash
    /\ pc[t] \in {"prep", "apply", "end", "abort", "validate"}
    /\ pc' = [pc EXCEPT ![t] = "done"]
    /\ told' = [told EXCEPT ![t] = IF pc[t] = "end" THEN "unknown" ELSE "error"]
    /\ UNCHANGED <<cache, pend, res, why>>
    /\ SiteUnchanged

-----------------------------------------------------------------------------
(* The participant's background work (D:731 loop, D:1411).                 *)

\* A transaction that holds write heads (`transactions_with_idle_leases`):
\* applied, not closed, not rolled back.
Candidate(s, t) == obj[s][t].st = "Committed" /\ obj[s][t].hist
Peer(t, s) == CHOOSE p \in Part(t) : p # s

\* D:2083 `terminate_in_doubt` for one candidate, and D:1430 presume_abort
\* for one that is not held.
SweepStart(s, t) ==
    /\ Sweep
    /\ Candidate(s, t)
    /\ sw[s][t] = "idle"
    /\ \E r \in {"success", "pending", "pendingsuccess", "refusal",
                 "checkfailed", "notexist", "aborted"},
          refuse \in Bools(CloseRefuse), mode \in Modes,
          fr \in Bools(RestoreFail) :
        CASE obj[s][t].dc ->
               \* D:2101: decided commit, retry the close (`end_from_sweep`).
               DoEnd(t, s, r, refuse, mode, fr) /\ UNCHANGED sw
          [] Part(t) \ {s} = {} \/ ~FenceInDoubt ->
               \* D:2128: one site, nobody to ask: presumed abort.
               DoPresumeAbort(t, s, fr) /\ UNCHANGED sw
          [] OTHER ->
               /\ sw' = [sw EXCEPT ![s][t] = "status"]
               /\ UNCHANGED <<val, owner, obj, undo, dec, status>>
    /\ CoordUnchanged

\* D:2264-2295: ask the peer. Committed decides commit; Committing or no
\* answer waits; anything else goes on to the fence.
SweepStatus(s, t) ==
    /\ sw[s][t] = "status"
    /\ \E answer \in {Outcome(Peer(t, s), t)} \cup
                     (IF Lossy THEN {"silent"} ELSE {}) :
        CASE answer = "Committed" ->
               \* D:2131-2140: state Committed, the fence is cleared.
               /\ obj' = IF obj[s][t].st # "none"
                         THEN [obj EXCEPT ![s][t].st = "Committed",
                                          ![s][t].fenced = FALSE]
                         ELSE obj
               /\ sw' = [sw EXCEPT ![s][t] = "commit"]
          [] answer \in {"Committing", "silent"} ->
               /\ sw' = [sw EXCEPT ![s][t] = "idle"]
               /\ UNCHANGED obj
          [] OTHER ->
               /\ sw' = [sw EXCEPT ![s][t] = "fenceSelf"]
               /\ UNCHANGED obj
    /\ UNCHANGED <<val, owner, undo, dec, status>>
    /\ CoordUnchanged

\* D:2155: `end_from_sweep` after a Commit decision.
SweepCommit(s, t) ==
    /\ sw[s][t] = "commit"
    /\ sw' = [sw EXCEPT ![s][t] = "idle"]
    /\ \E r \in {"success", "pending", "pendingsuccess", "refusal",
                 "checkfailed", "notexist", "aborted"},
          refuse \in Bools(CloseRefuse), mode \in Modes,
          fr \in Bools(RestoreFail) :
        IF obj[s][t].rec
        \* `apply_resolved_commit` (src/server/mod.rs): a synced bracket
        \* with the cells and the status cell.
        \* It installs the cells of the transaction over the current index.
        THEN /\ owner' = Released(t, s, {})
             /\ dec' = [dec EXCEPT ![s][t] = "Committed"]
             /\ status' = [status EXCEPT ![s][t] = "committed"]
             /\ obj' = [obj EXCEPT ![s][t] = NoObj]
             /\ val' = [c \in Cells |-> IF c \in WritesAt(t, s) THEN t ELSE val[c]]
             /\ UNCHANGED undo
        ELSE DoEnd(t, s, r, refuse, mode, fr)
    /\ CoordUnchanged

\* D:2300: fence this site.
SweepFenceSelf(s, t) ==
    /\ sw[s][t] = "fenceSelf"
    /\ LET a == FenceAnswer(s, t) IN
       /\ obj' = IF a = "Fenced" THEN [obj EXCEPT ![s][t].fenced = TRUE]
                 ELSE obj
       /\ sw' = [sw EXCEPT ![s][t] = IF a = "Committing" THEN "idle"
                                     ELSE "fencePeer"]
    /\ UNCHANGED <<val, owner, undo, dec, status>>
    /\ CoordUnchanged

\* D:2304-2320: fence the peer (D:9607 `fence`).
SweepFencePeer(s, t) ==
    /\ sw[s][t] = "fencePeer"
    /\ LET p == Peer(t, s)
           a == FenceAnswer(p, t)
       IN \E delivery \in (IF Lossy THEN {"ok", "lost", "noreply"} ELSE {"ok"}) :
          /\ obj' = IF a = "Fenced" /\ delivery # "lost"
                    THEN [obj EXCEPT ![p][t].fenced = TRUE] ELSE obj
          /\ sw' = [sw EXCEPT ![s][t] =
                      IF delivery # "ok" \/ a = "Committing" THEN "idle"
                      ELSE "abort"]
    /\ UNCHANGED <<val, owner, undo, dec, status>>
    /\ CoordUnchanged

\* InDoubtDecision::Abort, then `presume_abort` (D:1430).
SweepAbort(s, t) ==
    /\ sw[s][t] = "abort"
    /\ sw' = [sw EXCEPT ![s][t] = "idle"]
    /\ \E fr \in Bools(RestoreFail) : DoPresumeAbort(t, s, fr)
    /\ CoordUnchanged

\* D:1787 `retry_restoration`.
RetryRestore(s, t) ==
    /\ Sweep
    /\ obj[s][t].st = "Aborted"
    /\ obj[s][t].unrest
    /\ ~Mismatch(t, s)
    /\ val' = [c \in Cells |-> IF c \in WritesAt(t, s) THEN undo[t][c]
                               ELSE val[c]]
    /\ obj' = [obj EXCEPT ![s][t].unrest = FALSE]
    /\ UNCHANGED <<owner, undo, dec, status, sw>>
    /\ CoordUnchanged

\* D:1758 `finish_aborted_transactions`: an aborted object whose `end` never
\* came is finished once it is idle.
FinishAborted(s, t) ==
    /\ Sweep
    /\ obj[s][t].st = "Aborted"
    /\ ~obj[s][t].unrest
    /\ FinishAbortedOn(t, s, obj[s][t])
    /\ UNCHANGED <<val, undo, status, sw>>
    /\ CoordUnchanged

\* Candidate fix for F2: an idle object that holds no leases is ended: its
\* locks go, and a participant that never applied records Aborted (its
\* apply can no longer happen: `commit` finds no object, D:9494).
StaleObjSweep(s, t) ==
    /\ FixStaleSweep
    /\ Sweep
    /\ \/ obj[s][t].st \in {"Started", "Prepared"}
       \/ obj[s][t].st = "Committed" /\ ~obj[s][t].hist /\ ~obj[s][t].dc
    /\ owner' = Released(t, s, {})
    /\ dec' = IF obj[s][t].st = "Prepared" /\ s \in Part(t)
              THEN [dec EXCEPT ![s][t] = "Aborted"] ELSE dec
    /\ obj' = [obj EXCEPT ![s][t] = NoObj]
    /\ UNCHANGED <<val, undo, status, sw>>
    /\ CoordUnchanged

\* The COMMIT of the status cell is synced (D:2501, TLA+ F1).
Sync(s, t) ==
    /\ status[s][t] = "committing"
    /\ status' = [status EXCEPT ![s][t] = "committed"]
    /\ UNCHANGED <<val, owner, obj, undo, dec, sw>>
    /\ CoordUnchanged

\* D:9414: a prepare of another transaction reclaims a stale lock whose
\* owner does not wait with writes. The age (30 s) is abstract.
Reclaim(c) ==
    /\ StaleReclaim
    /\ owner[c] # NoTxn
    /\ ~OwnerWaitsWithWrites(owner[c], SiteOf(c))
    /\ \E t \in Tids : /\ t # owner[c]
                       /\ pc[t] = "prep"
                       /\ SiteOf(c) \in pend[t]
                       /\ c \in RS(t) \cup WS(t)
    /\ owner' = [owner EXCEPT ![c] = NoTxn]
    /\ UNCHANGED <<val, obj, undo, dec, status, sw>>
    /\ CoordUnchanged

\* A site crashes and restarts (process crash). Memory is lost: objects,
\* locks, the decision map, the fence step. On restart (src/server/mod.rs
\* `resolve_in_doubt_after_recovery`): a bracket with a COMMIT is committed;
\* a bracket without one of a single-site transaction is discarded; one of a
\* multi-site transaction is in doubt (`rec`), its cells blocked
\* (`is_in_doubt_cell`), and the participant fence resolves it. A bracket of
\* an abort whose restore failed was retired, not rewound, and recovery
\* discards it.
DurableDecided(s, t) == FixDurableDecision /\ obj[s][t].dc /\ MultiPart(t, s)
InDoubtAfter(s, t) == obj[s][t].st = "Committed" /\ obj[s][t].hist
                      /\ MultiPart(t, s) /\ ~DurableDecided(s, t)
DurableSingle(s, t) == obj[s][t].st = "Committed" /\ obj[s][t].hist
                       /\ ~MultiPart(t, s) /\ obj[s][t].cd
Discarded(s, t) ==
    \/ obj[s][t].st = "Committed" /\ obj[s][t].hist /\ ~MultiPart(t, s)
       /\ ~obj[s][t].cd
    \/ obj[s][t].st = "Aborted" /\ obj[s][t].unrest
SiteCrash(s) ==
    /\ SiteCrashes
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ obj' = [obj EXCEPT ![s] = [t \in Tids |->
                 IF InDoubtAfter(s, t)
                 THEN [NoObj EXCEPT !.st = "Committed", !.hist = TRUE,
                                    !.rec = TRUE]
                 \* The decided part keeps its object: the sweep retries its close.
                 ELSE IF DurableDecided(s, t)
                 THEN [NoObj EXCEPT !.st = "Committed", !.hist = TRUE,
                                    !.dc = TRUE, !.cd = TRUE]
                 ELSE NoObj]]
    \* A discarded bracket and a bracket in doubt are out of the cell index:
    \* the index shows the previous version, unless a newer version (a
    \* plain write) replaced the transaction's version.
    /\ val' = [c \in Cells |->
                 LET Gone(w) == w \in Tids /\ c \in WritesAt(w, s)
                                /\ (Discarded(s, w) \/ InDoubtAfter(s, w))
                     v1 == IF Gone(val[c]) THEN undo[val[c]][c] ELSE val[c]
                 IN  IF SiteOf(c) # s THEN val[c]
                     ELSE IF Gone(v1) THEN undo[v1][c] ELSE v1]
    /\ owner' = [c \in Cells |->
                 IF SiteOf(c) # s THEN owner[c]
                 ELSE IF \E t \in Tids : (InDoubtAfter(s, t) \/ DurableDecided(s, t))
                                          /\ c \in WritesAt(t, s)
                      THEN CHOOSE t \in Tids : (InDoubtAfter(s, t) \/ DurableDecided(s, t))
                                               /\ c \in WritesAt(t, s)
                      ELSE NoTxn]
    \* The decision map is memory; a durable COMMIT is a fact on disk.
    /\ dec' = [dec EXCEPT ![s] = [t \in Tids |->
                 IF dec[s][t] = "Committed" \/ DurableSingle(s, t) THEN "Committed"
                 ELSE IF Discarded(s, t) THEN "Aborted" ELSE "none"]]
    /\ sw' = [sw EXCEPT ![s] = [t \in Tids |-> "idle"]]
    \* A plain write in flight on the crashed site is never done and never
    \* acknowledged.
    /\ plain' = IF plain = "checked" /\ s = SiteOf("x") THEN "lost" ELSE plain
    /\ UNCHANGED <<undo, status>>
    /\ CoordUnchanged

\* A plain write of `x` (src/server/cell_rpc.rs:693, `admit_write` checks
\* the fence, the slot seal and the owner member, not the transaction
\* lock).
PlainWrite ==
    /\ PlainWrites
    /\ plain = "no"
    /\ FixPlainRespectsLocks => owner["x"] = NoTxn
    /\ IF PlainAtomic
       THEN /\ plain' = "done"
            /\ val' = [val EXCEPT !["x"] = PlainTid]
       ELSE /\ plain' = "checked"
            /\ UNCHANGED val
    /\ UNCHANGED <<owner, obj, undo, dec, status, sw, crashes>>
    /\ CoordUnchanged

\* The write of a plain write that checked first (PlainAtomic = FALSE).
PlainApply ==
    /\ plain = "checked"
    /\ plain' = "done"
    /\ val' = [val EXCEPT !["x"] = PlainTid]
    /\ UNCHANGED <<owner, obj, undo, dec, status, sw, crashes>>
    /\ CoordUnchanged

-----------------------------------------------------------------------------

NextCore ==
    \/ \E t \in Tids :
        \/ \E c \in Cells : Read(t, c)
        \/ ReadTimeout(t) \/ BodyDone(t)
        \/ PrepareDone(t) \/ ApplyDone(t) \/ EndDone(t) \/ AbortDone(t)
        \/ ClientGivesUp(t) \/ Crash(t) \/ ROValidateDone(t)
        \/ \E s \in Sites :
            \/ SitePrepare(t, s) \/ PrepareTimeout(t, s) \/ SiteApply(t, s)
            \/ SiteEnd(t, s) \/ SiteAbort(t, s) \/ SiteAbortEnd(t, s)
            \/ SweepStart(s, t) \/ SweepStatus(s, t) \/ SweepCommit(s, t)
            \/ SweepFenceSelf(s, t) \/ SweepFencePeer(s, t)
            \/ SweepAbort(s, t)
            \/ RetryRestore(s, t) \/ FinishAborted(s, t) \/ Sync(s, t)
            \/ StaleObjSweep(s, t)
            \/ ROValidate(t, s) \/ ROValidateTimeout(t, s)
    \/ \E c \in Cells : Reclaim(c)

Next ==
    /\ \/ NextCore /\ UNCHANGED <<crashes, plain>>
       \/ \E s \in Sites : SiteCrash(s)
       \/ PlainWrite \/ PlainApply
    /\ gdec' = [s \in Sites |-> [t \in Tids |->
                  gdec[s][t] \cup (IF dec'[s][t] = "none" THEN {}
                                   ELSE {dec'[s][t]})]]

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants.                                                             *)

\* The transaction is committed at a site: an `end` closed it there, or the
\* site is decided COMMIT (no abort path can run any more, D:520).
CommittedAt(s, t) == "Committed" \in gdec[s][t] \/ obj[s][t].dc
RealCommitted(t) ==
    \/ \E s \in Part(t) : CommittedAt(s, t)
    \/ WS(t) = {} /\ told[t] = "committed"
RealAborted(t) == \E s \in Part(t) : "Aborted" \in gdec[s][t]

\* All or nothing across the participants.
Atomic == \A t \in Tids : ~((\E s \in Part(t) : CommittedAt(s, t))
                            /\ RealAborted(t))

\* A client told COMMITTED: no participant aborted or rolled it back.
ToldCommittedTrue ==
    \A t \in Tids : (told[t] = "committed" /\ WS(t) # {}) =>
        \A s \in Part(t) : "Aborted" \notin gdec[s][t] /\ obj[s][t].st # "Aborted"

\* A client told that the attempt failed: it is not committed anywhere.
ToldFailedTrue ==
    \A t \in Tids : told[t] \in {"retry", "error", "toomany"} =>
        ~RealCommitted(t)

\* A retried closure takes effect at most once.
OneCommitPerClient == Retry => ~(RealCommitted(1) /\ RealCommitted(3))

RECURSIVE LastW(_, _, _)
LastW(f, i, c) ==
    IF i = 1 THEN 0
    ELSE IF c \in WS(f[i - 1]) THEN f[i - 1] ELSE LastW(f, i - 1, c)

Perms(S) == {f \in [1..Cardinality(S) -> S] :
               \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}
ReadsOK(f) == \A i \in DOMAIN f : \A c \in RS(f[i]) :
                  cache[f[i]][c] = LastW(f, i, c)
FinalOK(f) == \A c \in Cells : val[c] = LastW(f, Cardinality(DOMAIN f) + 1, c)

CommittedRW == {t \in Tids : RealCommitted(t) /\ WS(t) # {}}
CommittedAll == {t \in Tids : RealCommitted(t)}

\* The committed read-write transactions read as in some serial order.
SerialRW == \E f \in Perms(CommittedRW) : ReadsOK(f)
\* The same with the read-only transactions (no snapshot: by design).
SerialAll == \E f \in Perms(CommittedAll) : ReadsOK(f)

\* Nothing more happens: every client is done, no site holds work.
Quiescent ==
    /\ \A t \in Tids : pc[t] \in {"done", "init"}
    /\ \A s \in Sites, t \in Tids :
        /\ ~Candidate(s, t)
        \* An aborted object that can never be restored (a plain write
        \* changed its cell) is not work any more: it is a leftover.
        /\ ~(obj[s][t].st = "Aborted" /\ ~(obj[s][t].unrest /\ Mismatch(t, s)))
        /\ ~ENABLED StaleObjSweep(s, t)
        /\ status[s][t] # "committing"
        /\ sw[s][t] = "idle"
    /\ plain # "checked"

\* At the end the store holds exactly a serial history of the committed
\* transactions: no lost write, no write of an aborted transaction.
FinalSerial == Quiescent =>
    \E f \in Perms(CommittedRW) : ReadsOK(f) /\ FinalOK(f)
\* At the end no lock blocks a later transaction for good.
QuiescentNoStuckLock == Quiescent =>
    \A c \in Cells : owner[c] = NoTxn
                     \/ ~OwnerWaitsWithWrites(owner[c], SiteOf(c))
\* Stricter: at the end no lock is held at all (a held, reclaimable lock
\* blocks readers for the 30 s stale-lock timeout).
QuiescentNoLock == Quiescent => \A c \in Cells : owner[c] = NoTxn
\* At the end no participant object is left.
QuiescentNoObject == Quiescent =>
    \A s \in Sites, t \in Tids : obj[s][t].st = "none"

\* Every committed transaction is complete on every participant.
CompleteCommit ==
    \A t \in Tids : (\E s \in Part(t) : "Committed" \in gdec[s][t]) =>
        \A s \in Part(t) : "Committed" \in gdec[s][t]
\* The user requirement: all or nothing, and no leftover. At the end the store
\* is a serial history of the committed transactions (no cell of an
\* uncommitted transaction), each committed transaction is complete on every
\* participant, and no lock and no participant object is left.
NoLeftovers == Quiescent =>
    /\ \E f \in Perms(CommittedRW) : ReadsOK(f) /\ FinalOK(f)
    /\ CompleteCommit
    /\ \A c \in Cells : owner[c] = NoTxn
    /\ \A s \in Sites, t \in Tids : obj[s][t].st = "none"
\* With one plain write of `x`: the acknowledged plain write is not lost. It
\* is the value of `x`, or a committed transaction read it before it wrote.
PlainKept == (Quiescent /\ plain = "done") =>
    \/ val["x"] = PlainTid
    \/ \E t \in CommittedRW : "x" \in WS(t) /\ cache[t]["x"] = PlainTid
\* With a plain write: at the end no cell holds a write of a transaction
\* that is not committed, and each committed transaction is complete on
\* every participant. (`NoLeftovers` with the plain write as a committed
\* writer; the serial-order part does not apply to a plain write.)
NoLeftoversPlain == Quiescent =>
    /\ \A c \in Cells : val[c] \in {0, PlainTid} \cup CommittedRW
    /\ CompleteCommit
\* With a plain write: no lock and no object is left.
NoLeftoverLocks == Quiescent =>
    /\ \A c \in Cells : owner[c] = NoTxn
    /\ \A s \in Sites, t \in Tids : obj[s][t].st = "none"

\* F3: no apply runs on an object that a participant fenced.
NoApplyAfterFence == \A s \in Sites, t \in Tids : ~obj[s][t].lf

\* Sanity, violated on purpose: the sweep of fix B ended a Prepared write
\* participant (it recorded Aborted and its object is gone) while the
\* coordinator has not sent the apply to it yet -- the slow coordinator. The
\* apply that comes later finds no object, so the coordinator cannot decide
\* COMMIT (it decides only after every apply succeeded); `Atomic` and
\* `NoLeftovers` of the shipped configs cover what follows.
SanityStaleEndBeforeApply ==
    ~\E s \in Sites, t \in Tids :
        /\ s \in Part(t) /\ "Aborted" \in gdec[s][t] /\ obj[s][t].st = "none"
        /\ pc[t] = "apply" /\ s \in pend[t]

\* Sanity, violated on purpose: a read-only transaction that read 2 cells
\* commits after its validation, and the system settles.
SanityNoROCommit ==
    ~\E t \in Tids : WS(t) = {} /\ told[t] = "committed" /\ Quiescent
                    /\ \E w \in Tids : RealCommitted(w) /\ WS(w) # {}

\* Sanity, violated on purpose: a multi-site transaction commits and the
\* system settles.
SanityNoMultiCommit ==
    ~\E t \in Tids : Cardinality(Part(t)) >= 2 /\ told[t] = "committed"
                     /\ Quiescent
\* Sanity, violated on purpose: the participant fence aborts a transaction
\* that a peer applied.
SanityNoFenceAbort ==
    ~\E s \in Sites, t \in Tids : sw[s][t] = "abort"
\* Sanity: the retry of client 1 commits.
SanityNoRetryCommit == ~(Retry /\ RealCommitted(3))

=============================================================================
