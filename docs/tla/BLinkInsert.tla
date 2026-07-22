----------------------------- MODULE BLinkInsert -----------------------------
(***************************************************************************)
(* Models the concurrent insert protocol of the B-link tree in             *)
(* src/index/ranged/tree/btree (insert.rs, node.rs write_targeted,         *)
(* external.rs split_insert), focusing on the race between a leaf split    *)
(* at the top level and a concurrent root installation.                    *)
(*                                                                         *)
(* Latches are modeled per node; the root-versioning latch serializes root *)
(* installations.  Lock-free descent is modeled by reading `root` and the  *)
(* routing tables without latches, so a process can act on a stale view.   *)
(*                                                                         *)
(* Buggy = TRUE models the pre-audit `check_root_modification`: when the   *)
(* split node no longer matches the current root, the pivot is inserted    *)
(* into the current root level AND a new root is stacked on top (the       *)
(* function's "handled" result was discarded), giving the new right node   *)
(* two parents; when first keys collide, the fix-up is skipped entirely    *)
(* and a spurious root level is stacked.                                   *)
(*                                                                         *)
(* Buggy = FALSE models `apply_top_level_split` after the fix: install a   *)
(* new root only when the split node is still the root (pointer identity), *)
(* otherwise release the latches and insert the pivot at the current       *)
(* root's level.                                                           *)
(*                                                                         *)
(* Stale = TRUE starts from a two-leaf tree with both processes primed     *)
(* with a stale "the leaf is the root" view, the smallest state exhibiting *)
(* the double-insert.  Stale = FALSE grows the tree from empty with        *)
(* NumProcs concurrent inserters.                                          *)
(*                                                                         *)
(* Expected results:                                                       *)
(*   Buggy=FALSE           : Ordered, SearchAll, SingleParent, HeightOK,   *)
(*                           no deadlock.                                  *)
(*   Buggy=TRUE Stale=TRUE : SingleParent fails (double insert).           *)
(*   Buggy=TRUE Stale=FALSE: HeightOK fails (spurious root level).         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS NumProcs, Buggy, Stale, NumMergers

Nil == 0
Inf == 99
LeafCap == 2
MaxNodes == 8
NodeIds == 1..MaxNodes
Procs == 1..NumProcs

\* Distinct key per process. The seeded scenario aims one process at each
\* pre-built full leaf.
KeyOf(p) == IF Stale THEN (IF p = 1 THEN 0 ELSE 7) ELSE p

Mergers == 1..NumMergers

VARIABLES
    nodes,     \* NodeIds -> [typ, keys, ptrs, right, rb, latch]
    root,      \* current root node id
    rootVer,   \* owner of the root-versioning latch (Nil if free)
    pc, cur, rootSeen, via, pivot, newRight, tgt,
    mpc, mroot, \* bulk-merger state: pc and the root it built against
    inserted   \* keys visible in the tree; searches must find all of them

vars == <<nodes, root, rootVer, pc, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

FreeNode == [typ |-> "free", keys |-> <<>>, ptrs |-> <<>>,
             right |-> Nil, rb |-> Inf, latch |-> Nil]

Alloc == CHOOSE n \in NodeIds : nodes[n].typ = "free"

Range(s) == {s[i] : i \in 1..Len(s)}

InsertSorted(s, k) ==
    LET cnt == Len(SelectSeq(s, LAMBDA x : x < k))
    IN SubSeq(s, 1, cnt) \o <<k>> \o SubSeq(s, cnt + 1, Len(s))

\* Child to descend into: keys equal to the pivot route right (InNode::search
\* returns mid + 1 on equality).
ChildFor(nd, k) ==
    LET cnt == Len(SelectSeq(nd.keys, LAMBDA x : x <= k))
    IN nd.ptrs[cnt + 1]

\* B-link search: follow the right link when the key is at or past the
\* right bound, else descend.
RECURSIVE ContainsF(_, _, _)
ContainsF(n, k, fuel) ==
    IF fuel = 0 \/ n = Nil THEN FALSE
    ELSE LET nd == nodes[n] IN
         IF nd.rb <= k
         THEN ContainsF(nd.right, k, fuel - 1)
         ELSE IF nd.typ = "leaf"
              THEN k \in Range(nd.keys)
              ELSE ContainsF(ChildFor(nd, k), k, fuel - 1)

Contains(k) == ContainsF(root, k, 2 * MaxNodes)

RECURSIVE HeightF(_, _)
HeightF(n, fuel) ==
    IF fuel = 0 \/ n = Nil THEN 0
    ELSE LET nd == nodes[n] IN
         IF nd.typ = "leaf" THEN 1 ELSE 1 + HeightF(nd.ptrs[1], fuel - 1)

--------------------------------------------------------------------------

OrganicInit ==
    /\ nodes = [n \in NodeIds |->
                  IF n = 1
                  THEN [typ |-> "leaf", keys |-> <<>>, ptrs |-> <<>>,
                        right |-> Nil, rb |-> Inf, latch |-> Nil]
                  ELSE FreeNode]
    /\ root = 1
    /\ pc = [p \in Procs |-> "start"]
    /\ cur = [p \in Procs |-> 1]
    /\ rootSeen = [p \in Procs |-> 1]
    /\ via = [p \in Procs |-> FALSE]
    /\ inserted = {}

\* Two full leaves under an internal root; both processes hold a stale
\* pre-root-install view (as if they descended before the root appeared).
SeededInit ==
    /\ nodes = [n \in NodeIds |->
                  IF n = 1 THEN [typ |-> "leaf", keys |-> <<1, 2>>, ptrs |-> <<>>,
                                 right |-> 2, rb |-> 3, latch |-> Nil]
                  ELSE IF n = 2 THEN [typ |-> "leaf", keys |-> <<3, 4>>, ptrs |-> <<>>,
                                      right |-> Nil, rb |-> Inf, latch |-> Nil]
                  ELSE IF n = 3 THEN [typ |-> "internal", keys |-> <<3>>, ptrs |-> <<1, 2>>,
                                      right |-> Nil, rb |-> Inf, latch |-> Nil]
                  ELSE FreeNode]
    /\ root = 3
    /\ pc = [p \in Procs |-> "latchLeaf"]
    /\ cur = [p \in Procs |-> p]        \* process p starts latched onto leaf p
    /\ rootSeen = [p \in Procs |-> p]
    /\ via = [p \in Procs |-> FALSE]    \* both believe their leaf is the root
    /\ inserted = {1, 2, 3, 4}

Init ==
    /\ IF Stale THEN SeededInit ELSE OrganicInit
    /\ rootVer = Nil
    /\ pivot = [p \in Procs |-> 0]
    /\ newRight = [p \in Procs |-> Nil]
    /\ tgt = [p \in Procs |-> Nil]
    /\ mpc = [m \in Mergers |-> "build"]
    /\ mroot = [m \in Mergers |-> Nil]

--------------------------------------------------------------------------

\* Lock-free read of the root pointer.
Start(p) ==
    /\ pc[p] = "start"
    /\ rootSeen' = [rootSeen EXCEPT ![p] = root]
    /\ cur' = [cur EXCEPT ![p] = root]
    /\ via' = [via EXCEPT ![p] = nodes[root].typ = "internal"]
    /\ pc' = [pc EXCEPT ![p] = "descend"]
    /\ UNCHANGED <<nodes, root, rootVer, pivot, newRight, tgt, mpc, mroot, inserted>>

\* Latch-free descent to the leaf level.
Descend(p) ==
    /\ pc[p] = "descend"
    /\ IF nodes[cur[p]].typ = "internal"
       THEN /\ cur' = [cur EXCEPT ![p] = ChildFor(nodes[cur[p]], KeyOf(p))]
            /\ pc' = pc
       ELSE /\ cur' = cur
            /\ pc' = [pc EXCEPT ![p] = "latchLeaf"]
    /\ UNCHANGED <<nodes, root, rootVer, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

LatchLeaf(p) ==
    /\ pc[p] = "latchLeaf"
    /\ nodes[cur[p]].latch = Nil
    /\ nodes' = [nodes EXCEPT ![cur[p]].latch = p]
    /\ pc' = [pc EXCEPT ![p] = "moveRight"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* write_targeted: hand-over-hand walk right while the key is out of bound.
MoveRight(p) ==
    /\ pc[p] = "moveRight"
    /\ LET nd == nodes[cur[p]] IN
       IF nd.rb <= KeyOf(p)
       THEN /\ nodes[nd.right].latch = Nil
            /\ nodes' = [nodes EXCEPT ![nd.right].latch = p, ![cur[p]].latch = Nil]
            /\ cur' = [cur EXCEPT ![p] = nd.right]
            /\ pc' = pc
       ELSE /\ nodes' = nodes /\ cur' = cur
            /\ pc' = [pc EXCEPT ![p] = "leafOp"]
    /\ UNCHANGED <<root, rootVer, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* Plain insert, or split when full (ExtNode::insert / split_insert).
LeafOp(p) ==
    /\ pc[p] = "leafOp"
    /\ LET nd == nodes[cur[p]]
           k == KeyOf(p)
       IN IF Len(nd.keys) < LeafCap
          THEN /\ nodes' = [nodes EXCEPT ![cur[p]].keys = InsertSorted(nd.keys, k),
                                         ![cur[p]].latch = Nil]
               /\ inserted' = inserted \cup {k}
               /\ pc' = [pc EXCEPT ![p] = "done"]
               /\ UNCHANGED <<newRight, pivot>>
          ELSE LET fresh == Alloc
                   comb == InsertSorted(nd.keys, k)     \* LeafCap + 1 keys
                   piv == comb[2]
               IN /\ nodes' = [nodes EXCEPT
                        ![fresh] = [typ |-> "leaf",
                                    keys |-> SubSeq(comb, 2, Len(comb)),
                                    ptrs |-> <<>>,
                                    right |-> nd.right, rb |-> nd.rb,
                                    latch |-> Nil],
                        ![cur[p]].keys = SubSeq(comb, 1, 1),
                        ![cur[p]].right = fresh,
                        ![cur[p]].rb = piv]
                  /\ newRight' = [newRight EXCEPT ![p] = fresh]
                  /\ pivot' = [pivot EXCEPT ![p] = piv]
                  /\ inserted' = inserted \cup {k}
                  /\ pc' = [pc EXCEPT ![p] = IF via[p] THEN "parentLatch" ELSE "rootVerAcq"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, tgt, mpc, mroot>>

\* Split of a non-top leaf: latch the internal node the descent came through
\* (possibly stale), walk right by the pivot, insert. The leaf latch is held
\* throughout, as in the implementation.
ParentLatch(p) ==
    /\ pc[p] = "parentLatch"
    /\ nodes[rootSeen[p]].latch = Nil
    /\ nodes' = [nodes EXCEPT ![rootSeen[p]].latch = p]
    /\ tgt' = [tgt EXCEPT ![p] = rootSeen[p]]
    /\ pc' = [pc EXCEPT ![p] = "parentMove"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>

ParentMove(p) ==
    /\ pc[p] = "parentMove"
    /\ LET nd == nodes[tgt[p]] IN
       IF nd.rb <= pivot[p]
       THEN /\ nodes[nd.right].latch = Nil
            /\ nodes' = [nodes EXCEPT ![nd.right].latch = p, ![tgt[p]].latch = Nil]
            /\ tgt' = [tgt EXCEPT ![p] = nd.right]
            /\ pc' = pc
       ELSE /\ nodes' = nodes /\ tgt' = tgt
            /\ pc' = [pc EXCEPT ![p] = "parentInsert"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>

InsertPivot(n, piv, child) ==
    LET nd == nodes[n]
        cnt == Len(SelectSeq(nd.keys, LAMBDA x : x < piv))
    IN [nd EXCEPT !.keys = InsertSorted(nd.keys, piv),
                  !.ptrs = SubSeq(nd.ptrs, 1, cnt + 1) \o <<child>>
                           \o SubSeq(nd.ptrs, cnt + 2, Len(nd.ptrs))]

ParentInsert(p) ==
    /\ pc[p] = "parentInsert"
    /\ nodes' = [nodes EXCEPT
          ![tgt[p]] = [InsertPivot(tgt[p], pivot[p], newRight[p]) EXCEPT !.latch = Nil],
          ![cur[p]].latch = Nil]
    /\ pc' = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* Top-level split: acquire the root-versioning latch first.
RootVerAcq(p) ==
    /\ pc[p] = "rootVerAcq"
    /\ rootVer = Nil
    /\ rootVer' = p
    /\ pc' = [pc EXCEPT ![p] = "applyTop"]
    /\ UNCHANGED <<nodes, root, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

InstallRoot(p) ==
    LET fresh == Alloc IN
    /\ nodes' = [nodes EXCEPT
          ![fresh] = [typ |-> "internal", keys |-> <<pivot[p]>>,
                      ptrs |-> <<root, newRight[p]>>, right |-> Nil,
                      rb |-> Inf, latch |-> Nil],
          ![cur[p]].latch = Nil]
    /\ root' = fresh
    /\ rootVer' = Nil
    /\ pc' = [pc EXCEPT ![p] = "done"]

FirstKeyOf(n) == IF Len(nodes[n].keys) = 0 THEN 0 ELSE nodes[n].keys[1]

ApplyTop(p) ==
    /\ pc[p] = "applyTop"
    /\ IF Buggy
       THEN IF FirstKeyOf(root) # FirstKeyOf(cur[p])
            THEN \* fix-up path, latches kept, root creation NOT suppressed
                 /\ tgt' = [tgt EXCEPT ![p] = root]
                 /\ pc' = [pc EXCEPT ![p] = "bugLatchRoot"]
                 /\ UNCHANGED <<nodes, root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>
            ELSE \* first keys collide: stack a new root over the current one
                 /\ InstallRoot(p)
                 /\ UNCHANGED <<cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>
       ELSE IF root = cur[p] \/ nodes[root].typ = "leaf"
            THEN /\ InstallRoot(p)
                 /\ UNCHANGED <<cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>
            ELSE \* the tree grew above us: release both latches, then place
                 \* the pivot at the current root's level
                 /\ nodes' = [nodes EXCEPT ![cur[p]].latch = Nil]
                 /\ rootVer' = Nil
                 /\ pc' = [pc EXCEPT ![p] = "fixReadRoot"]
                 /\ UNCHANGED <<root, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* Fixed fix-up: non-atomic read of the root pointer, then latch and walk.
FixReadRoot(p) ==
    /\ pc[p] = "fixReadRoot"
    /\ tgt' = [tgt EXCEPT ![p] = root]
    /\ pc' = [pc EXCEPT ![p] = "fixLatch"]
    /\ UNCHANGED <<nodes, root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>

FixLatch(p) ==
    /\ pc[p] = "fixLatch"
    /\ nodes[tgt[p]].latch = Nil
    /\ nodes' = [nodes EXCEPT ![tgt[p]].latch = p]
    /\ pc' = [pc EXCEPT ![p] = "fixMove"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

FixMove(p) ==
    /\ pc[p] = "fixMove"
    /\ LET nd == nodes[tgt[p]] IN
       IF nd.rb <= pivot[p]
       THEN /\ nodes[nd.right].latch = Nil
            /\ nodes' = [nodes EXCEPT ![nd.right].latch = p, ![tgt[p]].latch = Nil]
            /\ tgt' = [tgt EXCEPT ![p] = nd.right]
            /\ pc' = pc
       ELSE /\ nodes' = nodes /\ tgt' = tgt
            /\ pc' = [pc EXCEPT ![p] = "fixInsert"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>

FixInsert(p) ==
    /\ pc[p] = "fixInsert"
    /\ nodes' = [nodes EXCEPT
          ![tgt[p]] = [InsertPivot(tgt[p], pivot[p], newRight[p]) EXCEPT !.latch = Nil]]
    /\ pc' = [pc EXCEPT ![p] = "done"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* Buggy fix-up: leaf latch and root-versioning latch stay held.
BugLatchRoot(p) ==
    /\ pc[p] = "bugLatchRoot"
    /\ nodes[tgt[p]].latch = Nil
    /\ nodes' = [nodes EXCEPT ![tgt[p]].latch = p]
    /\ pc' = [pc EXCEPT ![p] = "bugMove"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

BugMove(p) ==
    /\ pc[p] = "bugMove"
    /\ LET nd == nodes[tgt[p]] IN
       IF nd.rb <= pivot[p]
       THEN /\ nodes[nd.right].latch = Nil
            /\ nodes' = [nodes EXCEPT ![nd.right].latch = p, ![tgt[p]].latch = Nil]
            /\ tgt' = [tgt EXCEPT ![p] = nd.right]
            /\ pc' = pc
       ELSE /\ nodes' = nodes /\ tgt' = tgt
            /\ pc' = [pc EXCEPT ![p] = "bugInsert"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, mpc, mroot, inserted>>

BugInsert(p) ==
    /\ pc[p] = "bugInsert"
    /\ nodes' = [nodes EXCEPT
          ![tgt[p]] = [InsertPivot(tgt[p], pivot[p], newRight[p]) EXCEPT !.latch = Nil]]
    /\ pc' = [pc EXCEPT ![p] = "bugInstall"]
    /\ UNCHANGED <<root, rootVer, cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

\* The discarded "handled" result: the caller stacks a new root anyway.
BugInstall(p) ==
    /\ pc[p] = "bugInstall"
    /\ InstallRoot(p)
    /\ UNCHANGED <<cur, rootSeen, via, pivot, newRight, tgt, mpc, mroot, inserted>>

(* Bulk merger m: models merge_with_keys_ root growth. Build against the
   observed root without holding shared latches, then install under the
   root-versioning latch only if the root has not moved; otherwise rebuild. *)

MBuild(m) ==
    /\ mpc[m] = "build"
    /\ mroot' = [mroot EXCEPT ![m] = root]
    /\ mpc' = [mpc EXCEPT ![m] = "install"]
    /\ UNCHANGED <<nodes, root, rootVer, pc, cur, rootSeen, via, pivot, newRight, tgt, inserted>>

MInstall(m) ==
    /\ mpc[m] = "install"
    /\ rootVer = Nil
    /\ IF root = mroot[m]
         THEN LET fresh == Alloc IN
              /\ nodes' = [nodes EXCEPT
                    ![fresh] = [typ |-> "internal", keys |-> <<>>, ptrs |-> <<root>>,
                                right |-> Nil, rb |-> Inf, latch |-> Nil]]
              /\ root' = fresh
              /\ mpc' = [mpc EXCEPT ![m] = "done"]
              /\ UNCHANGED <<rootVer, pc, cur, rootSeen, via, pivot, newRight, tgt, mroot, inserted>>
         ELSE /\ mpc' = [mpc EXCEPT ![m] = "build"]
              /\ UNCHANGED <<nodes, root, rootVer, pc, cur, rootSeen, via, pivot, newRight, tgt, mroot, inserted>>

AllDone ==
    /\ \A m \in Mergers : mpc[m] = "done"
    /\ \A p \in Procs : pc[p] = "done"

Next ==
    \/ \E p \in Procs :
         Start(p) \/ Descend(p) \/ LatchLeaf(p) \/ MoveRight(p) \/ LeafOp(p)
         \/ ParentLatch(p) \/ ParentMove(p) \/ ParentInsert(p)
         \/ RootVerAcq(p) \/ ApplyTop(p)
         \/ FixReadRoot(p) \/ FixLatch(p) \/ FixMove(p) \/ FixInsert(p)
         \/ BugLatchRoot(p) \/ BugMove(p) \/ BugInsert(p) \/ BugInstall(p)
    \/ \E m \in Mergers : MBuild(m) \/ MInstall(m)
    \/ (AllDone /\ UNCHANGED vars)

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------

\* Every key ever made visible must remain findable from the root (B-link
\* searches may traverse right links).
SearchAll == \A k \in inserted : Contains(k)

\* Keys inside every live node are strictly increasing and below the node's
\* right bound; sibling chains are ordered.
Ordered ==
    \A n \in NodeIds :
        nodes[n].typ \in {"leaf", "internal"} =>
            /\ \A i \in 1..(Len(nodes[n].keys) - 1) :
                   nodes[n].keys[i] < nodes[n].keys[i + 1]
            /\ \A i \in 1..Len(nodes[n].keys) : nodes[n].keys[i] < nodes[n].rb
            /\ (nodes[n].right # Nil =>
                   nodes[nodes[n].right].rb >= nodes[n].rb)

\* No node may be referenced as a child by more than one internal slot.
SingleParent ==
    \A n \in NodeIds :
        nodes[n].typ \in {"leaf", "internal"} =>
            Cardinality({<<m, i>> \in NodeIds \X (1..MaxNodes) :
                           /\ nodes[m].typ = "internal"
                           /\ i <= Len(nodes[m].ptrs)
                           /\ nodes[m].ptrs[i] = n}) <= 1

\* With <= NumProcs + 4 keys and leaves of two keys, one internal level is
\* always sufficient; extra levels are the spurious-root symptom.
HeightOK == HeightF(root, MaxNodes) <= 2

NeverFix == \A p \in Procs : pc[p] # "fixInsert"

=============================================================================
