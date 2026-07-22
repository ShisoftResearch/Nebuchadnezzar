---------------------------- MODULE SeqlockCursor ----------------------------
(***************************************************************************)
(* Models the optimistic read protocol of the B+ tree nodes                *)
(* (src/index/ranged/tree/btree/node.rs, read_node) together with the      *)
(* cursor advance step (cursor.rs).                                        *)
(*                                                                         *)
(* A node carries a version word; writers latch the node and bump the      *)
(* version on release (NodeWriteGuard::drop increments even when nothing   *)
(* was mutated).  read_node snapshots the version, runs a closure over the *)
(* node data, re-reads the version, and RE-RUNS THE CLOSURE if it changed. *)
(*                                                                         *)
(* The pre-audit cursor mutated its own position (self.index += 1) inside  *)
(* that closure, so a retry advanced the position twice and the scan       *)
(* skipped a key.  The fixed cursor computes the new position in a local,  *)
(* and commits it only after validation.                                   *)
(*                                                                         *)
(* Check with Buggy = TRUE  : invariant NoSkip fails (TLC shows the trace).*)
(* Check with Buggy = FALSE : NoSkip holds.                                *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    Keys,       \* number of keys on the page, scanned in order 1..Keys
    MaxWrites,  \* bound on writer critical sections (version bumps)
    Buggy       \* TRUE models the pre-audit cursor

VARIABLES
    version,    \* node version word (latch flag modeled separately)
    latched,    \* TRUE while a writer holds the node latch
    writes,     \* number of completed writer critical sections
    idx,        \* cursor position committed so far (0 = before first key)
    tmpIdx,     \* candidate position computed inside the closure (fixed variant)
    readV,      \* version snapshot taken at closure entry
    pc,         \* reader state: "ready" | "body" | "validate" | "done"
    emitted     \* sequence of key positions the scan has yielded

vars == <<version, latched, writes, idx, tmpIdx, readV, pc, emitted>>

Init ==
    /\ version = 0 /\ latched = FALSE /\ writes = 0
    /\ idx = 0 /\ tmpIdx = 0 /\ readV = 0
    /\ pc = "ready" /\ emitted = <<>>

(* Writer: take the latch, then release it, bumping the version.  Content   *)
(* changes are irrelevant here -- a no-op write (e.g. re-inserting an       *)
(* existing key) already bumps the version in the implementation.           *)
WriterAcquire ==
    /\ ~latched /\ writes < MaxWrites
    /\ latched' = TRUE
    /\ UNCHANGED <<version, writes, idx, tmpIdx, readV, pc, emitted>>

WriterRelease ==
    /\ latched
    /\ latched' = FALSE
    /\ version' = version + 1
    /\ writes' = writes + 1
    /\ UNCHANGED <<idx, tmpIdx, readV, pc, emitted>>

(* Reader: one next() call = StartRead ; Body ; Validate.                   *)
StartRead ==
    /\ pc = "ready" /\ idx < Keys
    /\ ~latched                       \* spins while the latch is held
    /\ readV' = version
    /\ pc' = "body"
    /\ UNCHANGED <<version, latched, writes, idx, tmpIdx, emitted>>

Body ==
    /\ pc = "body"
    /\ IF Buggy
         THEN /\ idx' = idx + 1       \* mutates committed state inside the closure
              /\ tmpIdx' = tmpIdx
         ELSE /\ tmpIdx' = idx + 1    \* pure: candidate only
              /\ idx' = idx
    /\ pc' = "validate"
    /\ UNCHANGED <<version, latched, writes, readV, emitted>>

Validate ==
    /\ pc = "validate"
    /\ IF version = readV
         THEN \* validation succeeded: yield the key at the position
              /\ IF Buggy
                   THEN /\ emitted' = IF idx <= Keys
                                        THEN Append(emitted, idx)
                                        ELSE emitted   \* ran off the page
                        /\ idx' = idx /\ tmpIdx' = tmpIdx
                   ELSE /\ emitted' = Append(emitted, tmpIdx)
                        /\ idx' = tmpIdx /\ tmpIdx' = tmpIdx
              /\ pc' = IF (IF Buggy THEN idx ELSE tmpIdx) >= Keys
                         THEN "done" ELSE "ready"
         ELSE \* version changed: re-run the closure from the top
              /\ pc' = "ready"
              /\ UNCHANGED <<idx, tmpIdx, emitted>>
    /\ UNCHANGED <<version, latched, writes, readV>>

ReaderDone ==
    /\ pc = "ready" /\ idx >= Keys
    /\ pc' = "done"
    /\ UNCHANGED <<version, latched, writes, idx, tmpIdx, readV, emitted>>

Terminated ==
    /\ pc = "done" /\ (writes = MaxWrites \/ ~latched)
    /\ UNCHANGED vars

Next ==
    \/ WriterAcquire \/ WriterRelease
    \/ StartRead \/ Body \/ Validate \/ ReaderDone
    \/ Terminated

Spec == Init /\ [][Next]_vars

(* The scan must yield exactly the keys 1..n in order, for some prefix n.   *)
NoSkip == \A i \in 1..Len(emitted) : emitted[i] = i

=============================================================================
