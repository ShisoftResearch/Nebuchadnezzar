--------------------------- MODULE TxnChainRecovery ---------------------------
(***************************************************************************)
(* Models the head-pool / bracket-chain transaction design's RECOVERY      *)
(* REDUCTION and its two protective gates (docs/crash-safety-plan.md,      *)
(* "Phase 6a SETTLED").                                                    *)
(*                                                                         *)
(* A transaction writes a known set of chain MEMBERS (segments), then a    *)
(* COMMIT record naming all of them (the manifest). Durability is modeled  *)
(* in three levels: "A"bsent, "V"olatile (written, not fsynced),           *)
(* "D"urable. A crash drops V to A. The commit sequencing under test is    *)
(* the settled one: members all durable -> COMMIT written -> COMMIT        *)
(* durable -> ack.                                                         *)
(*                                                                         *)
(* The CLEANER may compact ("dissolve") a member: its bracket structure    *)
(* disappears and its entries become PLAIN DATA that recovery installs at  *)
(* face value. It may likewise drop the COMMIT record of an old            *)
(* transaction. Both are gated on the transaction being below the decided  *)
(* WATERMARK; the watermark may advance only past decided transactions.   *)
(*                                                                         *)
(* RecoveryOutcome encodes the reduction:                                  *)
(*   surfaces(m) == dissolved \/ (durable /\ (commit durable \/ marked))   *)
(*   all surface -> "all", none -> "none", otherwise -> "BAD".             *)
(*                                                                         *)
(* The gates are CONSTANTS so the buggy configurations can disable them    *)
(* and MUST then produce counterexamples — see the cfg table in README.    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS CleanerGate, WatermarkGate

Txns == {"t1", "t2"}
\* t1 chains across three members (multi-segment, multi-chunk folded in);
\* t2 is the common single-bracket case.
WriteSet == [t \in Txns |-> IF t = "t1" THEN {"a", "b", "c"} ELSE {"x"}]

VARIABLES
    mem,       \* [t][m] -> "A" | "V" | "D" | "X" (X = dissolved to plain data)
    com,       \* [t]    -> "A" | "V" | "D"        (the COMMIT record)
    wasCommitted, \* [t] -> BOOLEAN. GHOST: the decision became durable at
               \* some point. The durable EVIDENCE (com) may later be
               \* dropped by the cleaner once the watermark covers the
               \* transaction -- that is the watermark's purpose -- but the
               \* FACT is permanent, and the invariants must be stated
               \* against the fact, not the evidence.
    acked,     \* [t]    -> BOOLEAN (client told committed)
    aborted,   \* [t]    -> BOOLEAN (decision was abort; physical rewind)
    dead,      \* [t]    -> BOOLEAN (discarded by a recovery; terminal)
    mark,      \* subset of Txns below the decided watermark
    outcome,   \* [t]    -> "na" | "none" | "all" | "BAD" (last recovery)
    badInstall, \* GHOST: some recovery installed a transaction that was
               \* never committed. NoPartialInstall alone misses this: with
               \* the watermark gate off, a marked-but-undecided transaction
               \* installs COMPLETELY (the marked rule surfaces its members),
               \* which is atomic -- and atomically wrong.
    ackLost    \* GHOST: some recovery resolved an acked transaction to
               \* anything other than "all". Checked at recovery time,
               \* because `outcome` alone goes stale: a crash before a
               \* transaction starts leaves outcome="none", and an ack
               \* arriving later must not be judged against it.

vars == <<mem, com, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

Decided(t) == wasCommitted[t] \/ aborted[t] \/ dead[t]

Init ==
    /\ mem = [t \in Txns |-> [m \in WriteSet[t] |-> "A"]]
    /\ com = [t \in Txns |-> "A"]
    /\ wasCommitted = [t \in Txns |-> FALSE]
    /\ acked = [t \in Txns |-> FALSE]
    /\ aborted = [t \in Txns |-> FALSE]
    /\ dead = [t \in Txns |-> FALSE]
    /\ mark = {}
    /\ outcome = [t \in Txns |-> "na"]
    /\ ackLost = FALSE
    /\ badInstall = FALSE

(* Apply loop: write a member (journaled, not yet fsynced). *)
WriteMember(t, m) ==
    /\ ~aborted[t] /\ ~dead[t] /\ com[t] = "A"
    /\ mem[t][m] = "A"
    /\ mem' = [mem EXCEPT ![t][m] = "V"]
    /\ UNCHANGED <<com, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

(* First fsync wave: every member of the write set becomes durable. *)
SyncMembers(t) ==
    /\ ~aborted[t] /\ ~dead[t] /\ com[t] = "A"
    /\ \A m \in WriteSet[t] : mem[t][m] # "A"
    /\ mem' = [mem EXCEPT ![t] = [m \in WriteSet[t] |-> "D"]]
    /\ UNCHANGED <<com, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

(* The decision, commit side: COMMIT may be written only once every
   member is durable — the settled sequencing. *)
WriteCommit(t) ==
    /\ ~aborted[t] /\ ~dead[t] /\ com[t] = "A"
    /\ \A m \in WriteSet[t] : mem[t][m] = "D"
    /\ com' = [com EXCEPT ![t] = "V"]
    /\ UNCHANGED <<mem, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

SyncCommit(t) ==
    /\ com[t] = "V"
    /\ com' = [com EXCEPT ![t] = "D"]
    /\ wasCommitted' = [wasCommitted EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<mem, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

Ack(t) ==
    /\ com[t] = "D" /\ ~dead[t] /\ ~acked[t]
    /\ acked' = [acked EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<mem, com, wasCommitted, aborted, dead, mark, outcome, ackLost, badInstall>>

(* The decision, abort side: ownership means nobody wrote after the
   bracket, so it physically vanishes. Only before a commit decision.
   GUARDED ON THE FACT (wasCommitted), NOT THE EVIDENCE (com): TLC found
   that guarding on `com = "A"` lets a committed, acked transaction
   "abort" after the cleaner legitimately drops its COMMIT record below
   the watermark. The implementation lesson is real: the rewind path
   must key on transaction state, never on the log's contents. *)
Abort(t) ==
    /\ ~wasCommitted[t] /\ com[t] = "A" /\ ~aborted[t] /\ ~dead[t]
    /\ mem' = [mem EXCEPT ![t] = [m \in WriteSet[t] |-> "A"]]
    /\ aborted' = [aborted EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<com, wasCommitted, acked, dead, mark, outcome, ackLost, badInstall>>

(* Cleaner: dissolve a durable member into plain data, or drop an old
   COMMIT record. Gated on the watermark unless disabled. *)
Dissolve(t, m) ==
    /\ CleanerGate => t \in mark
    /\ mem[t][m] = "D"
    /\ mem' = [mem EXCEPT ![t][m] = "X"]
    /\ UNCHANGED <<com, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

DropCommit(t) ==
    /\ CleanerGate => t \in mark
    /\ com[t] = "D"
    /\ com' = [com EXCEPT ![t] = "A"]
    /\ UNCHANGED <<mem, wasCommitted, acked, aborted, dead, mark, outcome, ackLost, badInstall>>

(* The watermark advances only past decided transactions (gated). *)
Advance(t) ==
    /\ WatermarkGate => Decided(t)
    /\ t \notin mark
    /\ mark' = mark \cup {t}
    /\ UNCHANGED <<mem, com, wasCommitted, acked, aborted, dead, outcome, ackLost, badInstall>>

(* Crash + recovery, atomically. Volatile state is lost; the reduction
   decides each transaction from what remains. *)
Surfaces(memT, comT, marked, m) ==
    \/ memT[m] = "X"
    \/ /\ memT[m] = "D"
       /\ (comT = "D" \/ marked)

CrashRecover ==
    /\ \E t \in Txns : TRUE  \* always enabled; kept explicit for readability
    /\ LET memC == [t \in Txns |->
                       [m \in WriteSet[t] |-> IF mem[t][m] = "V" THEN "A" ELSE mem[t][m]]]
           comC == [t \in Txns |-> IF com[t] = "V" THEN "A" ELSE com[t]]
           surf == [t \in Txns |-> {m \in WriteSet[t] : Surfaces(memC[t], comC[t], t \in mark, m)}]
           verdict == [t \in Txns |->
                          IF surf[t] = WriteSet[t] THEN "all"
                          ELSE IF surf[t] = {} THEN "none"
                          ELSE "BAD"]
       IN /\ outcome' = verdict
          /\ ackLost' = (ackLost \/ \E t \in Txns : acked[t] /\ verdict[t] # "all")
          /\ badInstall' = (badInstall \/ \E t \in Txns : verdict[t] = "all" /\ ~wasCommitted[t])
          \* Discard: a transaction that did not fully surface loses its
          \* remaining durable members and is terminal.
          /\ mem' = [t \in Txns |->
                        IF verdict[t] = "all" THEN memC[t]
                        ELSE [m \in WriteSet[t] |-> "A"]]
          /\ com' = comC
          \* Terminal only for transactions that had actually begun:
          \* discarding a never-started transaction would forbid it from
          \* ever running after any crash, shrinking the explored space.
          /\ dead' = [t \in Txns |->
                         dead[t] \/ (verdict[t] # "all"
                                     /\ ~aborted[t]
                                     /\ (com[t] # "A" \/ wasCommitted[t]
                                         \/ \E m \in WriteSet[t] : mem[t][m] # "A"))]
    /\ UNCHANGED <<wasCommitted, acked, aborted, mark>>

Next ==
    \/ \E t \in Txns : \E m \in WriteSet[t] : WriteMember(t, m) \/ Dissolve(t, m)
    \/ \E t \in Txns : SyncMembers(t) \/ WriteCommit(t) \/ SyncCommit(t)
                       \/ Ack(t) \/ Abort(t) \/ DropCommit(t) \/ Advance(t)
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* A recovery never installs part of a transaction.
NoPartialInstall == \A t \in Txns : outcome[t] # "BAD"

\* An acknowledged commit survives every crash (checked at recovery
\* time via the ghost, because `outcome` goes stale across txn starts).
AckedSurvives == ~ackLost

\* Nothing installs without a durable commit decision behind it.
InstalledWasCommitted == ~badInstall

\* The watermark never covers an undecided transaction.
MarkIsDecided == \A t \in Txns : t \in mark => Decided(t)

\* Vacuity guard (Sanity cfg): asserts commit-with-crash is unreachable.
\* It MUST be violated — its counterexample is a full write -> sync ->
\* commit -> ack -> crash -> "all" round trip. If this ever PASSES, the
\* model has gone vacuous and the clean runs above prove nothing.
NeverAckedThenRecovered == \A t \in Txns : ~(acked[t] /\ outcome[t] = "all")

================================================================================
