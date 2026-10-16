---------------------------- MODULE HeadWordLock ----------------------------
(***************************************************************************)
(* Lock protocol of the Neb write path in one chunk (fix/seal-compose).    *)
(*                                                                         *)
(* Resources: one word lock per cell (the mutex bit of the cell index      *)
(* word), K head slots (each head has at most one owner), the GC lock.     *)
(*                                                                         *)
(* Process kinds:                                                          *)
(*  "W" head-first writer (write_cell, upsert, update_cell, CAS today):    *)
(*      own a head, then wait for the word, append, release.               *)
(*      WAlgoOf(p) = "wordfast" models the CAS candidate: word first, a bounded *)
(*      try for a head, else release the word and run the head-first path. *)
(*  "U" word-first updater (update_cell_by, CellGuard updates): hold the   *)
(*      word, then wait for a head. UBounded models the head-queue         *)
(*      deadline near the wall; away from the wall the wait has no bound   *)
(*      when every slot is owned (the spin in try_acquire_claim).          *)
(*  "D" non-transactional delete (remove_cell, remove_cell_by):            *)
(*      DeleteAlgo = "old" (before e6a64539): word, then wait for a head.  *)
(*                 = "cur" (e6a64539 .. 9020529d): word to check, release, *)
(*                   wait for a head, take the word again, check again.    *)
(*                 = "new" (D35): word, check, bounded try for a head      *)
(*                   while the word is held; on failure release the word  *)
(*                   and continue with the "cur" path.                     *)
(*  "C" cleaner: hold the GC lock, wait for a cell word. Heads are taken   *)
(*      only with try_own, which never waits, so heads are not modeled.    *)
(*  "T" transaction: lease a head (never the last usable one), then wait   *)
(*      for the GC lock (transaction apply), then the cell word.           *)
(*                                                                         *)
(* StoreFull = TRUE: no claim can get space (no room in any head, no       *)
(* segment to allocate, GC frees nothing).                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Procs, Heads, NoOne,
          KindOf(_), CellOf(_), PredOf(_), Exists0(_),
          DeleteAlgo, WAlgoOf(_), UBounded, StoreFull,
          FastBounded,   \* control: FALSE removes the bound of the fast try
          SlowRecheck    \* control: FALSE removes the check under the second word

Cells == {CellOf(p) : p \in Procs}

VARIABLES pc, word, head, leased, gc, exists, result, myHead, sawExists
vars == <<pc, word, head, leased, gc, exists, result, myHead, sawExists>>

Free(h) == head[h] = NoOne
Spare(h) == Cardinality({g \in Heads : ~leased[g]}) >= 2 /\ ~leased[h]

Init ==
  /\ pc = [p \in Procs |-> "start"]
  /\ word = [c \in Cells |-> NoOne]
  /\ head = [h \in Heads |-> NoOne]
  /\ leased = [h \in Heads |-> FALSE]
  /\ gc = NoOne
  /\ exists = [c \in Cells |-> Exists0(c)]
  /\ result = [p \in Procs |-> "none"]
  /\ myHead = [p \in Procs |-> NoOne]
  /\ sawExists = [p \in Procs |-> FALSE]

Finish(p, r) == /\ pc' = [pc EXCEPT ![p] = "done"]
                /\ result' = [result EXCEPT ![p] = r]

Goto(p, l) == pc' = [pc EXCEPT ![p] = l]

WordFree(p) == word[CellOf(p)] = NoOne
SetWord(p) == word' = [word EXCEPT ![CellOf(p)] = p]
TakeWord(p) == WordFree(p) /\ SetWord(p)
DropWord(p) == word' = [word EXCEPT ![CellOf(p)] = NoOne]

TakeHead(p) == /\ ~StoreFull
               /\ \E h \in Heads : /\ Free(h)
                                   /\ head' = [head EXCEPT ![h] = p]
                                   /\ myHead' = [myHead EXCEPT ![p] = h]
DropHead(p) == /\ head' = [head EXCEPT ![myHead[p]] = NoOne]
               /\ myHead' = [myHead EXCEPT ![p] = NoOne]

Ok(p) == exists[CellOf(p)] /\ PredOf(p)

-----------------------------------------------------------------------------
(* W: head-first writer; WAlgoOf(p) = "wordfast" adds the bounded word-first try. *)
WStart(p) ==
  /\ KindOf(p) = "W" /\ pc[p] = "start"
  /\ IF WAlgoOf(p) = "wordfast"
       THEN /\ WordFree(p)
            /\ sawExists' = [sawExists EXCEPT ![p] = Ok(p)]
            /\ IF Ok(p)
                 THEN /\ SetWord(p) /\ Goto(p, "w_fast") /\ UNCHANGED result
                 ELSE /\ UNCHANGED word /\ Finish(p, "mismatch")
            /\ UNCHANGED <<head, myHead, leased, gc, exists>>
       ELSE \/ /\ StoreFull /\ Finish(p, "nospace")
               /\ UNCHANGED <<word, head, myHead, leased, gc, exists, sawExists>>
            \/ /\ TakeHead(p) /\ Goto(p, "w_word")
               /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>

WFast(p) ==
  /\ KindOf(p) = "W" /\ pc[p] = "w_fast"
  /\ \/ /\ TakeHead(p) /\ Goto(p, "w_append")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>
     \/ /\ FastBounded \/ StoreFull
        /\ DropWord(p) /\ Goto(p, "w_slow")
        /\ UNCHANGED <<head, myHead, leased, gc, exists, result, sawExists>>

WSlow(p) ==
  /\ KindOf(p) = "W" /\ pc[p] = "w_slow"
  /\ \/ /\ StoreFull /\ Finish(p, "nospace")
        /\ UNCHANGED <<word, head, myHead, leased, gc, exists, sawExists>>
     \/ /\ TakeHead(p) /\ Goto(p, "w_word")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>

WWord(p) ==
  /\ KindOf(p) = "W" /\ pc[p] = "w_word"
  /\ WordFree(p)
  /\ sawExists' = [sawExists EXCEPT ![p] = Ok(p)]
  /\ IF WAlgoOf(p) = "wordfast" /\ ~Ok(p)
       THEN /\ DropHead(p) /\ Finish(p, "mismatch") /\ UNCHANGED word
       ELSE /\ SetWord(p) /\ Goto(p, "w_append") /\ UNCHANGED <<head, myHead, result>>
  /\ UNCHANGED <<leased, gc, exists>>

WAppend(p) ==   \* OPT 44dd119d: the head is released at the journal append
  /\ KindOf(p) = "W" /\ pc[p] = "w_append"
  /\ word[CellOf(p)] = p /\ head[myHead[p]] = p
  /\ DropHead(p) /\ Goto(p, "w_unlock")
  /\ exists' = [exists EXCEPT ![CellOf(p)] = TRUE]
  /\ UNCHANGED <<word, leased, gc, result, sawExists>>

WUnlock(p) ==
  /\ KindOf(p) = "W" /\ pc[p] = "w_unlock"
  /\ DropWord(p) /\ Finish(p, "ok")
  /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>

-----------------------------------------------------------------------------
(* U: word-first updater. *)
UStart(p) ==
  /\ KindOf(p) = "U" /\ pc[p] = "start"
  /\ WordFree(p)
  /\ IF exists[CellOf(p)]
       THEN SetWord(p) /\ Goto(p, "u_head") /\ UNCHANGED result
       ELSE UNCHANGED word /\ Finish(p, "absent")
  /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>

UHead(p) ==
  /\ KindOf(p) = "U" /\ pc[p] = "u_head"
  /\ \/ /\ TakeHead(p) /\ Goto(p, "u_append")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>
     \/ /\ UBounded \/ StoreFull
        /\ DropWord(p) /\ Finish(p, "nospace")
        /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>

UAppend(p) ==
  /\ KindOf(p) = "U" /\ pc[p] = "u_append"
  /\ DropHead(p) /\ Goto(p, "u_unlock")
  /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>

UUnlock(p) ==
  /\ KindOf(p) = "U" /\ pc[p] = "u_unlock"
  /\ DropWord(p) /\ Finish(p, "ok")
  /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>

-----------------------------------------------------------------------------
(* D: non-transactional delete. *)
DStart(p) ==
  /\ KindOf(p) = "D" /\ pc[p] = "start"
  /\ WordFree(p)
  /\ sawExists' = [sawExists EXCEPT ![p] = Ok(p)]
  /\ IF ~Ok(p)
       THEN /\ UNCHANGED word /\ Finish(p, "absent")
       ELSE CASE DeleteAlgo = "old" -> SetWord(p) /\ Goto(p, "d_head_locked") /\ UNCHANGED result
              [] DeleteAlgo = "cur" -> UNCHANGED word /\ Goto(p, "d_head") /\ UNCHANGED result
              [] DeleteAlgo = "new" -> SetWord(p) /\ Goto(p, "d_fast") /\ UNCHANGED result
  /\ UNCHANGED <<head, myHead, leased, gc, exists>>

DHeadLocked(p) ==   \* "old": wait for a head while the word is held
  /\ KindOf(p) = "D" /\ pc[p] = "d_head_locked"
  /\ \/ /\ StoreFull /\ DropWord(p) /\ Finish(p, "nospace")
        /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>
     \/ /\ TakeHead(p) /\ Goto(p, "d_append")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>

DFast(p) ==         \* "new": bounded try while the word is held
  /\ KindOf(p) = "D" /\ pc[p] = "d_fast"
  /\ \/ /\ TakeHead(p) /\ Goto(p, "d_append")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>
     \/ /\ FastBounded \/ StoreFull
        /\ DropWord(p) /\ Goto(p, "d_head")
        /\ UNCHANGED <<head, myHead, leased, gc, exists, result, sawExists>>

DHead(p) ==         \* slow path: no word held while it waits
  /\ KindOf(p) = "D" /\ pc[p] = "d_head"
  /\ \/ /\ StoreFull /\ Finish(p, "nospace")
        /\ UNCHANGED <<word, head, myHead, leased, gc, exists, sawExists>>
     \/ /\ TakeHead(p) /\ Goto(p, "d_word2")
        /\ UNCHANGED <<word, leased, gc, exists, result, sawExists>>

DWord2(p) ==
  /\ KindOf(p) = "D" /\ pc[p] = "d_word2"
  /\ WordFree(p)
  /\ IF SlowRecheck
       THEN /\ sawExists' = [sawExists EXCEPT ![p] = Ok(p)]
            /\ IF Ok(p)
                 THEN SetWord(p) /\ Goto(p, "d_append") /\ UNCHANGED <<head, myHead, result>>
                 ELSE UNCHANGED word /\ DropHead(p) /\ Finish(p, "absent")
       ELSE SetWord(p) /\ Goto(p, "d_append") /\ UNCHANGED <<head, myHead, result, sawExists>>
  /\ UNCHANGED <<leased, gc, exists>>

DAppend(p) ==
  /\ KindOf(p) = "D" /\ pc[p] = "d_append"
  /\ DropHead(p) /\ Goto(p, "d_unlock")
  /\ exists' = [exists EXCEPT ![CellOf(p)] = FALSE]
  /\ UNCHANGED <<word, leased, gc, result, sawExists>>

DUnlock(p) ==
  /\ KindOf(p) = "D" /\ pc[p] = "d_unlock"
  /\ DropWord(p) /\ Finish(p, "ok")
  /\ UNCHANGED <<head, myHead, leased, gc, exists, sawExists>>

-----------------------------------------------------------------------------
(* C: cleaner. *)
CStart(p) ==
  /\ KindOf(p) = "C" /\ pc[p] = "start"
  /\ gc = NoOne /\ gc' = p /\ Goto(p, "c_word")
  /\ UNCHANGED <<word, head, myHead, leased, exists, result, sawExists>>

CWord(p) ==   \* relocate the cell under its word; take and release in one step
  /\ KindOf(p) = "C" /\ pc[p] = "c_word"
  /\ word[CellOf(p)] = NoOne /\ Goto(p, "c_end")
  /\ UNCHANGED <<word, head, myHead, leased, gc, exists, result, sawExists>>

CEnd(p) ==
  /\ KindOf(p) = "C" /\ pc[p] = "c_end"
  /\ gc' = NoOne /\ Finish(p, "ok")
  /\ UNCHANGED <<word, head, myHead, leased, exists, sawExists>>

-----------------------------------------------------------------------------
(* T: transaction with a leased head. *)
TStart(p) ==
  /\ KindOf(p) = "T" /\ pc[p] = "start"
  /\ \/ /\ StoreFull \/ Cardinality(Heads) < 2
        /\ Finish(p, "nospace")
        /\ UNCHANGED <<word, head, myHead, leased, gc, exists, sawExists>>
     \/ /\ ~StoreFull
        /\ \E h \in Heads :
             /\ Free(h) /\ Spare(h)
             /\ head' = [head EXCEPT ![h] = p]
             /\ myHead' = [myHead EXCEPT ![p] = h]
             /\ leased' = [leased EXCEPT ![h] = TRUE]
        /\ Goto(p, "t_gc")
        /\ UNCHANGED <<word, gc, exists, result, sawExists>>

TGc(p) ==
  /\ KindOf(p) = "T" /\ pc[p] = "t_gc"
  /\ gc = NoOne /\ gc' = p /\ Goto(p, "t_word")
  /\ UNCHANGED <<word, head, myHead, leased, exists, result, sawExists>>

TWord(p) ==
  /\ KindOf(p) = "T" /\ pc[p] = "t_word"
  /\ TakeWord(p) /\ Goto(p, "t_end")
  /\ UNCHANGED <<head, myHead, leased, gc, exists, result, sawExists>>

TEnd(p) ==    \* decision: write into the leased head, end the lease
  /\ KindOf(p) = "T" /\ pc[p] = "t_end"
  /\ DropWord(p) /\ DropHead(p) /\ gc' = NoOne
  /\ leased' = [leased EXCEPT ![myHead[p]] = FALSE]
  /\ exists' = [exists EXCEPT ![CellOf(p)] = TRUE]
  /\ Finish(p, "ok")
  /\ UNCHANGED sawExists

-----------------------------------------------------------------------------
Step(p) ==
  \/ WStart(p) \/ WFast(p) \/ WSlow(p) \/ WWord(p) \/ WAppend(p) \/ WUnlock(p)
  \/ UStart(p) \/ UHead(p) \/ UAppend(p) \/ UUnlock(p)
  \/ DStart(p) \/ DHeadLocked(p) \/ DFast(p) \/ DHead(p) \/ DWord2(p)
  \/ DAppend(p) \/ DUnlock(p)
  \/ CStart(p) \/ CWord(p) \/ CEnd(p)
  \/ TStart(p) \/ TGc(p) \/ TWord(p) \/ TEnd(p)

AllDone == \A p \in Procs : pc[p] = "done"
Terminated == AllDone /\ UNCHANGED vars

Next == (\E p \in Procs : Step(p)) \/ Terminated
Spec == Init /\ [][Next]_vars /\ \A p \in Procs : WF_vars(Step(p))

-----------------------------------------------------------------------------
(* Properties. *)
TypeOK ==
  /\ word \in [Cells -> Procs \cup {NoOne}]
  /\ head \in [Heads -> Procs \cup {NoOne}]
  /\ gc \in Procs \cup {NoOne}

\* A tombstone or a write is appended only by the owner of the word and the head,
\* and a tombstone only for a cell that exists and satisfies the predicate.
AppendUnderBoth ==
  \A p \in Procs :
    /\ pc[p] \in {"d_append", "w_append"} =>
         word[CellOf(p)] = p /\ myHead[p] # NoOne /\ head[myHead[p]] = p
    /\ pc[p] = "d_append" => exists[CellOf(p)] /\ PredOf(p)

\* Result codes of a delete (and of the CAS candidate) follow the last check made
\* under the word: an absent cell or a false predicate is never "nospace", even
\* at a full store; "ok" only for a cell that existed.
ResultCodes ==
  \A p \in Procs : KindOf(p) \in {"D"} =>
    /\ result[p] = "nospace" => sawExists[p]
    /\ result[p] = "absent" => ~sawExists[p]
    /\ result[p] = "ok" => sawExists[p]

\* The lease rule keeps one unleased head.
LeaseSpare == Cardinality({h \in Heads : leased[h]}) <= Cardinality(Heads) - 1

Termination == <>AllDone
=============================================================================
