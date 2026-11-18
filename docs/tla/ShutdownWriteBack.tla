--------------------------- MODULE ShutdownWriteBack ---------------------------
(***************************************************************************)
(* The graceful shutdown of ONE database of a Neb server and the ranged   *)
(* index write-back of that database (triage 2026-11-17, FINDINGS.md D4). *)
(*                                                                         *)
(* Code: Neb develop 2afbab9a (CLOSE_CLIENTS_FIRST = FALSE for the loop  *)
(* over the other loaded databases) and fix/shutdown-close-first (TRUE for *)
(* every database).                                                       *)
(*  - server/mod.rs NebServer::shutdown step 0: the DEFAULT database       *)
(*    closes client writes (close_client_writes) before its tree flush.    *)
(*    Step 1.7, the loop for the other databases: before the fix it        *)
(*    flushed their trees and closed writes only after the flush; with the *)
(*    fix it closes their client writes and drains their index tasks       *)
(*    before any of their flushes.                                         *)
(*  - tree/service.rs:1231-1312 flush_all_trees: wait for the write-back   *)
(*    barrier, publish the tree heads, drain again, then reset the hub.    *)
(*  - btree/storage.rs:840-899 reset: the queued page writes are DISCARDED;*)
(*    a write enqueued after the reset waits for a worker that is gone.    *)
(*  - A ranged insert changes the in-memory page and queues its image; a  *)
(*    split queues the new right page and the left page that links to it  *)
(*    as two images (btree/mod.rs:204 mark_changed per node, Hypothesis:  *)
(*    no order between the two).                                           *)
(*  - The next start loads the tree from the durable head and pages; a    *)
(*    missing page refuses the tree (tree/service.rs:462-478). The scrub   *)
(*    (index/scrub.rs:370) re-inserts entries of durable cells.            *)
(*                                                                         *)
(* Abstraction: keys k0..k2; page 1 holds at most 2 keys; the third key   *)
(* splits into page 2. A cell write is durable when acknowledged; its      *)
(* index entry is durable when an image of its page that holds it lands.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    CLOSE_CLIENTS_FIRST   \* TRUE: client writes close before the flush (default
                          \* database; the fix for the others); FALSE: the loop for
                          \* the other databases (code)

Keys == {"k1", "k2"}
NoPage == [keys |-> {}, link |-> FALSE, present |-> FALSE]
Img(ks, l) == [keys |-> ks, link |-> l, present |-> TRUE]

VARIABLES
    cells,      \* durable acknowledged data cells (keys)
    mem,        \* [1..2 -> image]: the in-memory tree pages
    queue,      \* set of page ids whose image is queued
    durable,    \* [1..2 -> image]: durable page cells
    hub,        \* "alive" | "reset"
    phase,      \* "run" | "stopping" | "published" | "drained" | "reset" | "closed" | "down"
    clientOpen, \* client writes are accepted
    written     \* keys a client has written (at most once each)

vars == <<cells, mem, queue, durable, hub, phase, clientOpen, written>>

Init ==
    /\ cells = {"k0"} /\ written = {}
    /\ mem = [p \in 1..2 |-> IF p = 1 THEN Img({"k0"}, FALSE) ELSE NoPage]
    /\ durable = mem
    /\ queue = {}
    /\ hub = "alive" /\ phase = "run" /\ clientOpen = TRUE

\* A client write: the cell is durable at once, the entry goes into the
\* in-memory page, and the page image(s) are queued.
ClientWrite(k) ==
    /\ clientOpen /\ k \notin written
    /\ written' = written \cup {k}
    /\ cells' = cells \cup {k}
    /\ IF Cardinality(mem[1].keys) < 2
       THEN /\ mem' = [mem EXCEPT ![1] = Img(mem[1].keys \cup {k}, mem[1].link)]
            /\ queue' = queue \cup {1}
       ELSE /\ mem' = [mem EXCEPT ![1] = Img(mem[1].keys, TRUE),
                                  ![2] = Img(mem[2].keys \cup {k}, FALSE)]
            /\ queue' = queue \cup {1, 2}
    /\ UNCHANGED <<durable, hub, phase, clientOpen>>

\* A write-back worker lands one queued page, as the page is now (the
\* dirty pages coalesce, Neb DirtyCoalesce.tla); the pages land in any order.
Land ==
    /\ hub = "alive"
    /\ \E q \in queue :
        /\ durable' = [durable EXCEPT ![q] = mem[q]]
        /\ queue' = queue \ {q}
    /\ UNCHANGED <<cells, mem, hub, phase, clientOpen, written>>

Stop ==
    /\ phase = "run"
    /\ phase' = "stopping"
    /\ clientOpen' = IF CLOSE_CLIENTS_FIRST THEN FALSE ELSE clientOpen
    /\ UNCHANGED <<cells, mem, queue, durable, hub, written>>

\* Barrier, then the head publish (the head names page 1).
Publish ==
    /\ phase = "stopping" /\ queue = {}
    /\ phase' = "published"
    /\ UNCHANGED <<cells, mem, queue, durable, hub, clientOpen, written>>

Drain ==
    /\ phase = "published" /\ queue = {}
    /\ phase' = "drained"
    /\ UNCHANGED <<cells, mem, queue, durable, hub, clientOpen, written>>

\* The hub reset: the workers stop, the queue is discarded.
Reset ==
    /\ phase = "drained"
    /\ phase' = "reset" /\ hub' = "reset" /\ queue' = {}
    /\ UNCHANGED <<cells, mem, durable, clientOpen, written>>

\* close_writes + archive: no client write lands after this.
Close ==
    /\ phase = "reset"
    /\ phase' = "closed" /\ clientOpen' = FALSE
    /\ UNCHANGED <<cells, mem, queue, durable, hub, written>>

Exit ==
    /\ phase = "closed"
    /\ phase' = "down"
    /\ UNCHANGED <<cells, mem, queue, durable, hub, clientOpen, written>>

Next == (\E k \in Keys : ClientWrite(k)) \/ Land \/ Stop \/ Publish \/ Drain \/ Reset
        \/ Close \/ Exit
Spec == Init /\ [][Next]_vars

DurableEntries == durable[1].keys \cup (IF durable[1].link THEN durable[2].keys ELSE {})

\* The next start can load the tree: a durable page never links to a page
\* that is not durable (else MissingPage refuses the tree).
NoMissingPage == phase = "down" => (durable[1].link => durable[2].present)

\* Without the scrub, every acknowledged cell has its index entry after
\* the shutdown. The default scrub (repair) restores the entries at the
\* next start, at the cost of a full cell walk (64 s for 15.9 M cells).
IndexCoversCells == phase = "down" => cells \subseteq DurableEntries

\* SANITY (violated on purpose): no client write reaches the index.
SanityNoWriteIndexed == phase = "down" => DurableEntries = {"k0"}
=============================================================================
