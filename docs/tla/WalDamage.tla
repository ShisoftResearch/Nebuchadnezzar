---- MODULE WalDamage ----
(***************************************************************************)
(* One WAL file of one segment: appends, syncs that race with tail         *)
(* rewinds, a crash that keeps any subset of the unsynced records, one     *)
(* damage at rest, and the decision of recovery.                           *)
(*                                                                         *)
(* A position in `file` stands for a record. A position in `durable` or    *)
(* `known` counts records from the start of the file.                      *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    N,              \* Records one run may append.
    Proof,          \* Evidence recovery accepts: see ProvedSynced.
    SizeCheck,      \* TRUE: the file size is fixed, so a cut is visible.
    CheckRewind,    \* TRUE: a sync that raced a rewind publishes no length.
    Shape,          \* TRUE: a full record with a bad CRC cannot be a tear.
    K               \* Acked records a loss bound allows.

ASSUME Proof \in {"None", "Watermark", "Commit", "Both"}

VARIABLES
    file,       \* Sequence of records, as the file now holds them.
    durable,    \* Truth: records that a completed sync covers.
    known,      \* What the writer may copy into a record: never above durable.
    acked,      \* Records acknowledged as durable to a client.
    syncEnd,    \* Length an in-flight sync covers; -1 when none runs.
    syncRaw,    \* Length that sync read when it started.
    phase,      \* run, stopped, damaged, done.
    damage,     \* none, bytes, cut.
    verdict,    \* none, accept, stop.
    kept,       \* Records an accepting recovery keeps.
    proven      \* Ghost: records the file proves synced before the damage.

vars == <<file, durable, known, acked, syncEnd, syncRaw, phase, damage,
          verdict, kept, proven>>

Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

Record(w, commit) == [w |-> w, commit |-> commit, bad |-> "no"]

Init ==
    /\ file = <<>>
    /\ durable = 0 /\ known = 0 /\ acked = 0
    /\ syncEnd = -1 /\ syncRaw = -1
    /\ phase = "run" /\ damage = "none" /\ verdict = "none" /\ kept = 0
    /\ proven = 0

AppendPlain ==
    /\ phase = "run" /\ Len(file) < N
    /\ file' = Append(file, Record(known, FALSE))
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, phase, damage,
                   verdict, kept, proven>>

\* A COMMIT follows the prepare sync, so everything before it is durable.
AppendCommit ==
    /\ phase = "run" /\ Len(file) < N /\ Len(file) > 0
    /\ durable = Len(file)
    /\ file' = Append(file, Record(known, TRUE))
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, phase, damage,
                   verdict, kept, proven>>

SyncStart ==
    /\ phase = "run" /\ syncEnd = -1 /\ durable < Len(file)
    /\ syncEnd' = Len(file) /\ syncRaw' = Len(file)
    /\ UNCHANGED <<file, durable, known, acked, phase, damage, verdict, kept, proven>>

SyncDone ==
    /\ phase = "run" /\ syncEnd >= 0
    /\ durable' = Max(durable, syncEnd)
    /\ known' = Max(known, IF CheckRewind THEN syncEnd ELSE syncRaw)
    /\ syncEnd' = -1 /\ syncRaw' = -1
    /\ UNCHANGED <<file, acked, phase, damage, verdict, kept, proven>>

\* A refused transaction's private tail goes, durably. Acked records stay.
Rewind(p) ==
    /\ phase = "run" /\ acked <= p /\ p < Len(file)
    /\ file' = SubSeq(file, 1, p)
    /\ durable' = Min(durable, p)
    /\ known' = Min(known, p)
    /\ syncEnd' = IF syncEnd >= 0 THEN Min(syncEnd, p) ELSE -1
    /\ UNCHANGED <<acked, syncRaw, phase, damage, verdict, kept, proven>>

Ack ==
    /\ phase = "run" /\ acked < durable
    /\ acked' = durable
    /\ UNCHANGED <<file, durable, known, syncEnd, syncRaw, phase, damage,
                   verdict, kept, proven>>

\* The disk keeps any prefix past `durable`, and any subset of it can be
\* torn, because unsynced pages reach the disk in any order.
Crash ==
    /\ phase = "run"
    /\ \E cut \in durable..Len(file), torn \in SUBSET ((durable + 1)..Len(file)):
        /\ file' = [i \in 1..cut |->
                      [file[i] EXCEPT !.bad = IF i \in torn THEN "torn" ELSE "no"]]
        /\ proven' = LET good == {i \in 1..cut: i \notin torn}
                     IN  IF good = {} THEN 0
                         ELSE Max(0, CHOOSE m \in {file[i].w: i \in good}:
                                       \A i \in good: file[i].w <= m)
    /\ phase' = "stopped"
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, damage, verdict,
                   kept>>

\* A flipped bit keeps the full record; only its CRC fails.
DamageFlip ==
    /\ phase = "stopped"
    /\ \E i \in 1..Len(file):
        /\ file' = [file EXCEPT ![i].bad = "flip"]
    /\ phase' = "damaged" /\ damage' = "flip"
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, verdict, kept, proven>>

\* A zeroed block looks the same as a tear.
DamageZero ==
    /\ phase = "stopped"
    /\ \E i \in 1..Len(file):
        /\ file' = [file EXCEPT ![i].bad = "torn"]
    /\ phase' = "damaged" /\ damage' = "zero"
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, verdict, kept, proven>>

DamageCut ==
    /\ phase = "stopped" /\ Len(file) > 0
    /\ \E c \in 0..(Len(file) - 1):
        /\ file' = SubSeq(file, 1, c)
    /\ phase' = "damaged" /\ damage' = "cut"
    /\ UNCHANGED <<durable, known, acked, syncEnd, syncRaw, verdict, kept, proven>>

Good(i) == file[i].bad = "no"

FirstBad ==
    IF \E i \in 1..Len(file): ~Good(i)
    THEN CHOOSE i \in 1..Len(file):
            ~Good(i) /\ \A j \in 1..(i - 1): Good(j)
    ELSE 0

\* Does a valid record after position x prove that x was synced?
ProvedSynced(x) ==
    \E i \in (x + 1)..Len(file):
        /\ Good(i)
        /\ \/ Proof \in {"Watermark", "Both"} /\ file[i].w >= x
           \/ Proof \in {"Commit", "Both"} /\ file[i].commit

Recover ==
    /\ phase \in {"stopped", "damaged"}
    /\ LET x == FirstBad
           visibleCut == SizeCheck /\ damage = "cut"
           shapeProof == Shape /\ \E i \in 1..Len(file): file[i].bad = "flip"
       IN  IF visibleCut \/ shapeProof \/ (x > 0 /\ ProvedSynced(x))
           THEN verdict' = "stop" /\ kept' = 0
           ELSE verdict' = "accept"
                /\ kept' = IF x = 0 THEN Len(file) ELSE x - 1
    /\ phase' = "done"
    /\ UNCHANGED <<file, durable, known, acked, syncEnd, syncRaw, damage, proven>>

Done ==
    /\ phase = "done"
    /\ UNCHANGED vars

Next ==
    \/ AppendPlain \/ AppendCommit \/ SyncStart \/ SyncDone \/ Ack \/ Crash
    \/ DamageFlip \/ DamageZero \/ DamageCut \/ Recover \/ Done
    \/ \E p \in 0..N: Rewind(p)

Spec == Init /\ [][Next]_vars

\* The writer never claims more than a sync covers.
KnownIsSafe == phase = "run" => known <= durable

\* A crash alone must never stop the start.
NoFalseStop ==
    (phase = "done" /\ damage = "none") => verdict = "accept"

\* A crash alone must never lose an acknowledged record.
CrashKeepsAcked ==
    (phase = "done" /\ damage = "none") => kept >= acked

\* Recovery may lose at most K acked records with no stop, per damage kind.
LossBound(kind) ==
    (phase = "done" /\ damage = kind /\ verdict = "accept") => acked - kept <= K

\* Silent loss stays past what surviving records proved: the final sync group.
Window(kind) == (phase = "done" /\ damage = kind /\ verdict = "accept") => kept >= proven
WindowFlip == Window("flip")
WindowZero == Window("zero")
WindowCut == Window("cut")

BoundFlip == LossBound("flip")
BoundZero == LossBound("zero")
BoundCut == LossBound("cut")
====
