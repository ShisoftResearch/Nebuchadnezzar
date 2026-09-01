------------------------------ MODULE BLinkSeek ------------------------------
(***************************************************************************)
(* Models the READ path of the B-link tree leaf level                      *)
(* (src/index/ranged/tree/btree/search.rs `search_node`, node.rs           *)
(* `key_at_right_node`, cursor.rs `initialize`/`load_following_page`)      *)
(* against concurrent leaf inserts and leaf splits.                        *)
(*                                                                         *)
(* The question: can a fresh seek(K) yield a key ordered BEFORE K?        *)
(* Production said yes -- the "fresh root descent regressed, 65 times in   *)
(* a row" give-up storms of 2026-08-31 (`server_final4.log`: 8,700         *)
(* give-ups on a fresh store with ZERO structural splits, so plain         *)
(* insert+seek concurrency suffices).                                      *)
(*                                                                         *)
(* What is modeled and why:                                                *)
(*                                                                         *)
(* - One leaf level. The internal levels only choose the entry page, and   *)
(*   B-link readers must tolerate entering left of the target (separator   *)
(*   lag), so the reader nondeterministically starts at the target page    *)
(*   or any page to its left.                                              *)
(*                                                                         *)
(* - `read_node` closures are seqlock-validated against ONE node, so one   *)
(*   closure evaluation is a single atomic action on that node's CURRENT   *)
(*   state.  The sibling peek inside `key_at_right_node` (`peek_data`)     *)
(*   reads the right sibling's memory with NO validation: `Torn = TRUE`    *)
(*   lets that peek return an arbitrary key (a `copy_within` mid-memmove   *)
(*   under the sibling's latch tears the prefix-compressed first key).     *)
(*   Even with `Torn = FALSE` the peek is only atomic WITH the deciding    *)
(*   closure, not with the later follow -- which is the organic race.      *)
(*                                                                         *)
(* - Writers: `Insert(k)` places k in the page owning k (atomic: the       *)
(*   write path holds the page latch).  `Split(p)` halves a full page      *)
(*   into a fresh right sibling (atomic to validated readers: the split    *)
(*   holds the page, sibling and parent latches throughout).               *)
(*                                                                         *)
(* - OldSemantics = TRUE models the pre-fix off-page fallthrough: when     *)
(*   the search key lands past every key of the page (`pos == n.len`),    *)
(*   `search_node` built an empty-snapshot cursor whose `initialize()`     *)
(*   then yields the FIRST KEY OF THE NEXT PAGE READ AT A LATER TIME,      *)
(*   with no comparison against the seek key.                              *)
(*                                                                         *)
(*   OldSemantics = FALSE models the fix: an off-page position CONTINUES   *)
(*   THE DESCENT at the sibling (`Err(follow)` retry), so the only way to  *)
(*   yield a key is a validated lower-bound search on the node that        *)
(*   answers.                                                              *)
(*                                                                         *)
(* Expected results:                                                       *)
(*   BLinkSeekBuggyOrganic (Old=TRUE,  Torn=FALSE): SeekGE VIOLATED.       *)
(*     The trace: reader enters left of the target with K in the           *)
(*     sibling's range but below its first key; the peek honestly says     *)
(*     "sibling starts past K" so the reader stays and goes off-page; a    *)
(*     front-insert lands a key < K in the sibling; the follow read then   *)
(*     yields it.                                                          *)
(*   BLinkSeekBuggyTorn    (Old=TRUE,  Torn=TRUE):  SeekGE VIOLATED        *)
(*     (additionally without needing the racing insert).                   *)
(*   BLinkSeekFixed        (Old=FALSE, Torn=TRUE):  no error, even with    *)
(*     torn peeks and concurrent inserts and splits.                       *)
(*   BLinkSeekReach        (Old=FALSE, Torn=TRUE):  OffPageUnreachable is  *)
(*     violated ON PURPOSE -- its counterexample walks the off-page path.  *)
(*     If this one ever passes, the model has gone vacuous and the clean   *)
(*     run above means nothing.                                            *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS OldSemantics, Torn

Nil == 0
Inf == 99
MaxNodes == 4
NodeIds == 1..MaxNodes
LeafCap == 2
KeyUniverse == 1..6
NoKey == 0

(***************************************************************************)
(* nodes[n] = [live, keys (ascending seq), lo, rb, right]                  *)
(* Page n owns keys in [lo, rb).  The chain is ordered by bounds.          *)
(***************************************************************************)
VARIABLES nodes, rpc, rcur, rfollow, rkey, rres, roffpage

vars == <<nodes, rpc, rcur, rfollow, rkey, rres, roffpage>>

AscSeq(s) == \A i \in 1..(Len(s) - 1) : s[i] < s[i + 1]

InsertSorted(s, k) ==
    LET n == Len(s)
        pos == CHOOSE i \in 1..(n + 1) :
                   /\ \A j \in 1..(i - 1) : s[j] < k
                   /\ \A j \in i..n : s[j] > k
    IN SubSeq(s, 1, pos - 1) \o <<k>> \o SubSeq(s, pos, n)

\* Initial tree: n1 = [lo 0, rb 4) holding <<2>>, n2 = [4, Inf) holding
\* <<6>>. Key 5 seeks can enter at n1 (separator lag) with 5 inside n2's
\* range but below n2's first key -- the smallest interesting shape.
InitNodes ==
    [n \in NodeIds |->
        CASE n = 1 -> [live |-> TRUE, keys |-> <<2>>, lo |-> 0, rb |-> 4, right |-> 2]
          [] n = 2 -> [live |-> TRUE, keys |-> <<6>>, lo |-> 4, rb |-> Inf, right |-> Nil]
          [] OTHER -> [live |-> FALSE, keys |-> <<>>, lo |-> 0, rb |-> 0, right |-> Nil]]

LivePages == {n \in NodeIds : nodes[n].live}

Owner(k) == CHOOSE n \in LivePages : nodes[n].lo <= k /\ k < nodes[n].rb

PresentKeys == UNION {{nodes[n].keys[i] : i \in 1..Len(nodes[n].keys)} : n \in LivePages}

\* Pages at or left of k's owner: valid descent entry points under
\* separator lag.
EntryPages(k) == {n \in LivePages : nodes[n].lo <= nodes[Owner(k)].lo}

Init ==
    /\ nodes = InitNodes
    /\ rpc = "start"
    /\ rcur = Nil
    /\ rfollow = Nil
    /\ rkey = NoKey
    /\ rres = NoKey
    /\ roffpage = FALSE

(***************************************************************************)
(* Writers                                                                 *)
(***************************************************************************)

Insert(k) ==
    /\ k \notin PresentKeys
    /\ LET p == Owner(k) IN
        /\ Len(nodes[p].keys) < LeafCap
        /\ nodes' = [nodes EXCEPT ![p].keys = InsertSorted(nodes[p].keys, k)]
    /\ UNCHANGED <<rpc, rcur, rfollow, rkey, rres, roffpage>>

\* Split a full page: upper half moves to a fresh right sibling. Atomic to
\* validated readers (the code holds page + sibling + parent latches for
\* the whole relink).
Split(p) ==
    /\ nodes[p].live
    /\ Len(nodes[p].keys) = LeafCap
    /\ \E m \in NodeIds :
        /\ ~nodes[m].live
        /\ LET half == LeafCap \div 2
               pivot == nodes[p].keys[half + 1]
           IN nodes' = [nodes EXCEPT
                ![m] = [live |-> TRUE,
                        keys |-> SubSeq(nodes[p].keys, half + 1, LeafCap),
                        lo |-> pivot,
                        rb |-> nodes[p].rb,
                        right |-> nodes[p].right],
                ![p].keys = SubSeq(nodes[p].keys, 1, half),
                ![p].rb = pivot,
                ![p].right = m]
    /\ UNCHANGED <<rpc, rcur, rfollow, rkey, rres, roffpage>>

(***************************************************************************)
(* Reader: one seek(K), Forward                                            *)
(***************************************************************************)

Begin ==
    /\ rpc = "start"
    /\ \E k \in KeyUniverse : \E p \in EntryPages(k) :
        /\ rkey' = k
        /\ rcur' = p
        /\ rpc' = "descend"
    /\ UNCHANGED <<nodes, rfollow, rres, roffpage>>

\* What the unvalidated peek of the sibling's first key can report.
\* Torn adds arbitrary values (mid-memmove garbage); otherwise the peek
\* reads the sibling's true current state (the race is that the LATER
\* follow read sees a different state).
PeekValues(s) ==
    LET truth == IF Len(nodes[s].keys) = 0 THEN Inf ELSE nodes[s].keys[1]
    IN IF Torn THEN {truth} \cup KeyUniverse \cup {Inf} ELSE {truth}

LowerBound(s, k) ==
    IF \E i \in 1..Len(s) : s[i] >= k
    THEN s[CHOOSE i \in 1..Len(s) : s[i] >= k /\ \A j \in 1..(i - 1) : s[j] < k]
    ELSE NoKey

\* One `read_node` closure evaluation on the current page: atomic on this
\* page's state; the sibling peek happens inside it (against the sibling's
\* current-or-torn state).
Descend ==
    /\ rpc = "descend"
    /\ LET p == nodes[rcur] IN
       IF ~p.live
       THEN \* a condemned page: the code retries through its forward ref;
            \* condemned pages do not arise in this model's actions, but
            \* keep the arm total.
            /\ rpc' = "done"
            /\ UNCHANGED <<nodes, rcur, rfollow, rkey, rres, roffpage>>
       ELSE IF Len(p.keys) = 0
       THEN \* is_empty(): slide right unconditionally (code peeks only for
            \* is_none)
            IF p.right = Nil
            THEN /\ rpc' = "done"
                 /\ UNCHANGED <<nodes, rcur, rfollow, rkey, rres, roffpage>>
            ELSE /\ rcur' = p.right
                 /\ UNCHANGED <<nodes, rpc, rfollow, rkey, rres, roffpage>>
       ELSE IF p.rb <= rkey /\ p.right # Nil
       THEN \* key_at_right_node: peek the sibling (unvalidated) and decide
            \E f \in PeekValues(p.right) :
                IF f <= rkey
                THEN \* slide right: continue the descent at the sibling
                     /\ rcur' = p.right
                     /\ UNCHANGED <<nodes, rpc, rfollow, rkey, rres, roffpage>>
                ELSE \* stay: all keys here are < rb <= K, so the search
                     \* lands off-page
                     IF OldSemantics
                     THEN /\ rfollow' = p.right
                          /\ rpc' = "follow"
                          /\ roffpage' = TRUE
                          /\ UNCHANGED <<nodes, rcur, rkey, rres>>
                     ELSE /\ rcur' = p.right
                          /\ roffpage' = TRUE
                          /\ UNCHANGED <<nodes, rpc, rfollow, rkey, rres>>
       ELSE \* search this page: lower bound of K
            LET hit == LowerBound(p.keys, rkey) IN
            IF hit # NoKey
            THEN /\ rres' = hit
                 /\ rpc' = "done"
                 /\ UNCHANGED <<nodes, rcur, rfollow, rkey, roffpage>>
            ELSE \* off-page: K lies in this page's gap (or past its end)
                 IF p.right = Nil
                 THEN /\ rpc' = "done"
                      /\ UNCHANGED <<nodes, rcur, rfollow, rkey, rres, roffpage>>
                 ELSE IF OldSemantics
                 THEN /\ rfollow' = p.right
                      /\ rpc' = "follow"
                      /\ roffpage' = TRUE
                      /\ UNCHANGED <<nodes, rcur, rkey, rres>>
                 ELSE /\ rcur' = p.right
                      /\ roffpage' = TRUE
                      /\ UNCHANGED <<nodes, rpc, rfollow, rkey, rres>>

\* OLD semantics only: the deferred read of the follow page
\* (cursor initialize -> load_following_page), a SEPARATE validated read
\* at a later time, yielding that page's first key with no comparison
\* against the seek key.
Follow ==
    /\ rpc = "follow"
    /\ LET s == nodes[rfollow] IN
       IF ~s.live \/ Len(s.keys) = 0
       THEN IF s.right = Nil
            THEN /\ rpc' = "done"
                 /\ UNCHANGED <<nodes, rcur, rfollow, rkey, rres, roffpage>>
            ELSE /\ rfollow' = s.right
                 /\ UNCHANGED <<nodes, rpc, rcur, rkey, rres, roffpage>>
       ELSE /\ rres' = s.keys[1]
            /\ rpc' = "done"
            /\ UNCHANGED <<nodes, rcur, rfollow, rkey, roffpage>>

Done ==
    /\ rpc = "done"
    /\ UNCHANGED vars

Next ==
    \/ \E k \in KeyUniverse : Insert(k)
    \/ \E p \in NodeIds : Split(p)
    \/ Begin
    \/ Descend
    \/ Follow
    \/ Done

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* THE seek contract: a Forward seek never yields a key before its seek key.
SeekGE == (rpc = "done" /\ rres # NoKey) => rres >= rkey

\* Structural sanity of the leaf level, whatever the writers did.
ChainOrdered ==
    \A n \in LivePages :
        /\ AscSeq(nodes[n].keys)
        /\ \A i \in 1..Len(nodes[n].keys) :
             nodes[n].lo <= nodes[n].keys[i] /\ nodes[n].keys[i] < nodes[n].rb
        /\ nodes[n].right # Nil =>
             /\ nodes[nodes[n].right].live
             /\ nodes[nodes[n].right].lo = nodes[n].rb

\* Coverage guard: asserts the off-page path is never taken. Expected to be
\* VIOLATED (see BLinkSeekReach.cfg); a pass means the model went vacuous.
OffPageUnreachable == ~roffpage

=============================================================================
