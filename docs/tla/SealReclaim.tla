----------------------------- MODULE SealReclaim -----------------------------
(***************************************************************************)
(* The hand-off of one rotated segment between its seal job and the        *)
(* cleaner, in `src/ram/chunk.rs` (`finish_rotated_head`, `SealQueue`) and *)
(* `src/ram/cleaner` (`Protection`, `SegmentCandidate::for_cleaner`).       *)
(*                                                                         *)
(* The seal job takes a job count, waits for the writes of the segment to  *)
(* land (the drain), syncs the WAL and then archives the segment. A        *)
(* mostly dead segment can instead wait for the cleaner for a bounded      *)
(* number of attempts. The archive and the cleaner claim exclude each      *)
(* other (`maintenance` and `claim_cleaner` set the same CLEANING bit).    *)
(*                                                                         *)
(* Two writers touch the segment:                                          *)
(*  - `w` is in flight at the rotation. The drain waits for it.            *)
(*  - `late` loaded the head before the rotation and owns it only later    *)
(*    (`try_acquire_in_class`: slot load, `segs.get`, `try_own`,           *)
(*    `incr_references`, `begin_pending_journal`, `try_acquire`). It can   *)
(*    start while the journal gate is open, also after the drain. D46      *)
(*    review F1: with e07b3561 the cleaner removed a synced segment under  *)
(*    such a write, and the acknowledged cell was lost.                    *)
(*                                                                         *)
(* `Gate` is the rule that keeps the cleaner away from a seal-pending      *)
(* segment:                                                                *)
(*   "d20"    the cleaner never takes a seal-pending segment (15370976).   *)
(*   "synced" the cleaner takes it after the drain and the WAL sync.       *)
(*   "none"   no rule (441c1ff7). A control.                               *)
(*                                                                         *)
(* `CloseGate` is the fix: the seal job closes the journal gate before the *)
(* drain, and the gate never reopens (`Protection::rotated`). A late       *)
(* writer is then refused.                                                 *)
(*                                                                         *)
(* `OtherChecks` gives the cleaner the checks of the code besides the seal *)
(* rule: no owned head, no pending journal (claim and removal), and no     *)
(* queued WAL sync (`wal_sync_queued`). The WAL syncer needs               *)
(* `Protection::job`, so it cannot run while the cleaner holds its claim.  *)
(* With `OtherChecks` false, the model shows what the seal rule alone      *)
(* guarantees.                                                             *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Gate, Wait, Bounded, Waits, Dead, CloseGate, OtherChecks

VARIABLES w, late, seal, job, pend, synced, waits, archiving, cleaning,
          copied, copiedLate, retired, gate, queued

vars == <<w, late, seal, job, pend, synced, waits, archiving, cleaning,
          copied, copiedLate, retired, gate, queued>>

Init ==
    /\ w = "pending" /\ late = "none" /\ seal = "queued" /\ job = FALSE
    /\ pend = TRUE /\ synced = FALSE /\ waits = 0 /\ archiving = FALSE
    /\ cleaning = FALSE /\ copied = "none" /\ copiedLate = FALSE
    /\ retired = FALSE /\ gate = "open" /\ queued = FALSE

BlocksCleaner ==
    CASE Gate = "d20"    -> pend
      [] Gate = "synced" -> pend /\ ~synced
      [] Gate = "none"   -> FALSE

Busy == w = "pending" \/ late = "pending"

\* What the cleaner reads besides the seal rule (owner, pending journals,
\* wal_sync_queued).
Quiet == OtherChecks => (~Busy /\ ~queued)

WLand ==
    /\ w = "pending" /\ w' = "landed"
    /\ UNCHANGED <<late, seal, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

\* The late writer owns the segment and claims a journal slot. It needs an
\* open gate, and a segment that is not yet gone.
LStart ==
    /\ late = "none" /\ gate = "open" /\ seal = "queued"
    /\ late' = "pending"
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

\* It journals, publishes and releases; its WAL sync acknowledges it.
LLand ==
    /\ late = "pending" /\ late' = "landed" /\ queued' = TRUE
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate>>

\* sync_dirty_segment needs Protection::job, so not while the cleaner claims.
Syncer ==
    /\ queued /\ ~cleaning /\ ~archiving /\ queued' = FALSE
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate>>

SJob ==
    /\ seal = "queued" /\ ~retired /\ ~job /\ ~cleaning /\ ~archiving
    /\ job' = TRUE
    /\ UNCHANGED <<w, late, seal, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

\* Drain and WAL sync. With CloseGate the gate is shut first, so the drain
\* also covers a late writer that got in before it, and none starts after it.
SDrainSync ==
    /\ job /\ ~synced /\ ~Busy
    /\ (CloseGate => gate = "closed")
    /\ synced' = TRUE
    /\ UNCHANGED <<w, late, seal, job, pend, waits, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

SCloseGate ==
    /\ CloseGate /\ job /\ ~synced /\ gate = "open"
    /\ gate' = "closed"
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, queued>>

WaitsForCleaner == Wait /\ Dead /\ (~Bounded \/ waits < Waits)

SWait ==
    /\ job /\ synced /\ WaitsForCleaner
    /\ job' = FALSE
    /\ waits' = IF Bounded THEN waits + 1 ELSE waits
    /\ UNCHANGED <<w, late, seal, pend, synced, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

\* archive_inner closes the gate when it takes the claim.
SArchiveTry ==
    /\ job /\ synced /\ ~WaitsForCleaner
    /\ job' = FALSE
    /\ archiving' = ~cleaning
    /\ gate' = IF ~cleaning THEN "closed" ELSE gate
    /\ UNCHANGED <<w, late, seal, pend, synced, waits, cleaning, copied, copiedLate, retired, queued>>

\* drain_pending_journals, then the image (with the late write) and seal.
SArchiveEnd ==
    /\ archiving /\ late # "pending"
    /\ archiving' = FALSE /\ seal' = "archived" /\ pend' = FALSE
    /\ UNCHANGED <<w, late, job, synced, waits, cleaning, copied, copiedLate, retired, gate, queued>>

SGone ==
    /\ seal = "queued" /\ retired /\ ~job
    /\ seal' = "gone"
    /\ UNCHANGED <<w, late, job, pend, synced, waits, archiving, cleaning, copied, copiedLate, retired, gate, queued>>

\* for_cleaner: rule, head owned, pending journals; then the claim.
CClaim ==
    /\ ~retired /\ ~cleaning /\ ~job /\ ~archiving /\ ~BlocksCleaner /\ Quiet
    /\ cleaning' = TRUE
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, copied, copiedLate, retired, gate, queued>>

CRelease ==
    /\ cleaning /\ copied = "none"
    /\ cleaning' = FALSE
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, copied, copiedLate, retired, gate, queued>>

\* live_entries: what the index points at now.
CCopy ==
    /\ cleaning /\ copied = "none"
    /\ copied' = IF w = "landed" THEN "full" ELSE "partial"
    /\ copiedLate' = (late = "landed")
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, cleaning, retired, gate, queued>>

\* remove_segment_for_cleaner re-checks owner and pending journals only.
CRetire ==
    /\ cleaning /\ copied # "none" /\ Quiet
    /\ retired' = TRUE /\ cleaning' = FALSE
    /\ UNCHANGED <<w, late, seal, job, pend, synced, waits, archiving, copied, copiedLate, gate, queued>>

Done ==
    /\ seal \in {"archived", "gone"} /\ ~cleaning /\ ~Busy
    /\ UNCHANGED vars

Next == WLand \/ LStart \/ LLand \/ Syncer \/ SJob \/ SCloseGate \/ SDrainSync \/ SWait
        \/ SArchiveTry \/ SArchiveEnd \/ SGone \/ CClaim \/ CRelease \/ CCopy
        \/ CRetire \/ Done

Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(WLand) /\ WF_vars(LLand) /\ SF_vars(Syncer) /\ WF_vars(SDrainSync) /\ WF_vars(SWait)
    /\ WF_vars(SCloseGate) /\ WF_vars(SArchiveEnd) /\ WF_vars(SGone)
    /\ SF_vars(SJob) /\ SF_vars(SArchiveTry)
    /\ WF_vars(CCopy) /\ WF_vars(CRetire)

TypeOK ==
    /\ w \in {"pending", "landed"}
    /\ late \in {"none", "pending", "landed"}
    /\ seal \in {"queued", "archived", "gone"}
    /\ copied \in {"none", "partial", "full"}
    /\ gate \in {"open", "closed"}
    /\ waits \in Nat

\* The cleaner copied the segment only after the in-flight write landed.
NoLostWrite == copied # "partial"
\* An acknowledged late write is never in a retired segment without a copy.
NoLostLateWrite == (retired /\ late = "landed") => copiedLate
\* The archive, the cleaner and the job exclude each other.
Exclusive == ~(archiving /\ cleaning) /\ ~(job /\ cleaning)
\* An archive never runs before the drain and the WAL sync.
ArchiveAfterSync == archiving => synced
\* The seal job ends: the segment is archived or reclaimed.
SealEnds == <>(seal \in {"archived", "gone"})
=============================================================================
