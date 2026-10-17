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
(* `Gate` is the rule that keeps the cleaner away from a seal-pending      *)
(* segment:                                                                *)
(*   "d20"    the cleaner never takes a seal-pending segment (15370976).   *)
(*   "synced" the cleaner takes it after the drain and the WAL sync.       *)
(*   "none"   no rule (441c1ff7). A control.                               *)
(*                                                                         *)
(* The model gives the cleaner no other check (head ownership, pending     *)
(* journals, references). So it shows what the seal rule alone guarantees. *)
(* One write of the segment is still in flight when the segment rotates.   *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Gate, Wait, Bounded, Waits, Dead

VARIABLES
    w,          \* "pending" until the in-flight write lands, then "landed"
    seal,       \* "queued", "archived" or "gone"
    job,        \* the seal job holds a job count
    pend,       \* Protection::seal_pending
    synced,     \* Protection::synced: drained and the WAL synced
    waits,      \* Protection::reclaim_waits
    archiving,  \* the archive holds the maintenance claim
    cleaning,   \* the cleaner holds its claim
    copied,     \* what the cleaner copied: "none", "partial" or "full"
    retired     \* the cleaner retired the segment

vars == <<w, seal, job, pend, synced, waits, archiving, cleaning, copied, retired>>

Init ==
    /\ w = "pending"
    /\ seal = "queued"
    /\ job = FALSE
    /\ pend = TRUE
    /\ synced = FALSE
    /\ waits = 0
    /\ archiving = FALSE
    /\ cleaning = FALSE
    /\ copied = "none"
    /\ retired = FALSE

BlocksCleaner ==
    CASE Gate = "d20"    -> pend
      [] Gate = "synced" -> pend /\ ~synced
      [] Gate = "none"   -> FALSE

\* The in-flight write lands. The drain waits for it.
WLand ==
    /\ w = "pending"
    /\ w' = "landed"
    /\ UNCHANGED <<seal, job, pend, synced, waits, archiving, cleaning, copied, retired>>

\* Protection::job: refused while the cleaner or an archive holds CLEANING.
SJob ==
    /\ seal = "queued" /\ ~retired /\ ~job /\ ~cleaning /\ ~archiving
    /\ job' = TRUE
    /\ UNCHANGED <<w, seal, pend, synced, waits, archiving, cleaning, copied, retired>>

\* Drain (SegmentExclusiveRefGuard) and force_wal_sync, once.
SDrainSync ==
    /\ job /\ ~synced /\ w = "landed"
    /\ synced' = TRUE
    /\ UNCHANGED <<w, seal, job, pend, waits, archiving, cleaning, copied, retired>>

WaitsForCleaner == Wait /\ Dead /\ (~Bounded \/ waits < Waits)

\* Leave the synced segment to the cleaner: release the job and retry later.
SWait ==
    /\ job /\ synced /\ WaitsForCleaner
    /\ job' = FALSE
    /\ waits' = IF Bounded THEN waits + 1 ELSE waits
    /\ UNCHANGED <<w, seal, pend, synced, archiving, cleaning, copied, retired>>

\* Release the job, then archive_rotated takes the maintenance claim. A
\* cleaner claim in between refuses the archive, and the job is retried.
SArchiveTry ==
    /\ job /\ synced /\ ~WaitsForCleaner
    /\ job' = FALSE
    /\ archiving' = ~cleaning
    /\ UNCHANGED <<w, seal, pend, synced, waits, cleaning, copied, retired>>

SArchiveEnd ==
    /\ archiving
    /\ archiving' = FALSE
    /\ seal' = "archived"
    /\ pend' = FALSE
    /\ UNCHANGED <<w, job, synced, waits, cleaning, copied, retired>>

\* A retried job finds no segment.
SGone ==
    /\ seal = "queued" /\ retired /\ ~job
    /\ seal' = "gone"
    /\ UNCHANGED <<w, job, pend, synced, waits, archiving, cleaning, copied, retired>>

\* SegmentCandidate::for_cleaner: the rule, then claim_cleaner (activity 0).
CClaim ==
    /\ ~retired /\ ~cleaning /\ ~job /\ ~archiving /\ ~BlocksCleaner
    /\ cleaning' = TRUE
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, copied, retired>>

CRelease ==
    /\ cleaning /\ copied = "none"
    /\ cleaning' = FALSE
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, copied, retired>>

CCopy ==
    /\ cleaning /\ copied = "none"
    /\ copied' = IF w = "landed" THEN "full" ELSE "partial"
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, cleaning, retired>>

CRetire ==
    /\ cleaning /\ copied # "none"
    /\ retired' = TRUE
    /\ cleaning' = FALSE
    /\ UNCHANGED <<w, seal, job, pend, synced, waits, archiving, copied>>

Done ==
    /\ seal \in {"archived", "gone"} /\ ~cleaning
    /\ UNCHANGED vars

SealJob == SJob \/ SDrainSync \/ SWait \/ SArchiveTry \/ SArchiveEnd \/ SGone
Cleaner == CClaim \/ CRelease \/ CCopy \/ CRetire

Next == WLand \/ SealJob \/ Cleaner \/ Done

\* The cleaner may never reclaim; it may claim and release again and again.
\* So the seal job needs strong fairness for the steps a claim can disable.
Spec ==
    /\ Init /\ [][Next]_vars
    /\ WF_vars(WLand) /\ WF_vars(SDrainSync) /\ WF_vars(SWait)
    /\ WF_vars(SArchiveEnd) /\ WF_vars(SGone)
    /\ SF_vars(SJob) /\ SF_vars(SArchiveTry)
    /\ WF_vars(CCopy) /\ WF_vars(CRetire)

TypeOK ==
    /\ w \in {"pending", "landed"}
    /\ seal \in {"queued", "archived", "gone"}
    /\ copied \in {"none", "partial", "full"}
    /\ waits \in Nat

\* The cleaner copied the segment only after its last write landed.
NoLostWrite == copied # "partial"

\* The archive, the cleaner and the job exclude each other.
Exclusive ==
    /\ ~(archiving /\ cleaning)
    /\ ~(job /\ cleaning)

\* An archive never runs before the drain and the WAL sync.
ArchiveAfterSync == archiving => synced

\* The seal job ends: the segment is archived or reclaimed.
SealEnds == <>(seal \in {"archived", "gone"})
=============================================================================
