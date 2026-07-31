# Compact Cell Id Design (64-bit, Allocated-First)

Date: 2026-08-02
Status: proposed; supersedes the 128-bit content-hashed id scheme
Related: `2026-08-02-point-cell-mvcc-retrospective.md` (allocator machinery
provenance), `2026-07-23-hlc-txn-id-design.md`

## Motivation

Cells reference other cells by id, and references dominate footprint in
graph workloads: an adjacency of degree d costs 16d bytes under the
128-bit scheme, typically an order of magnitude more than the referenced
headers themselves. Halving the id halves reference memory, doubles
effective cache lines per traversal hop, improves ranged-index fanout,
and shrinks every RPC that carries neighbor batches.

The 128-bit width exists to make *content-hashed* ids (`Id::from_obj`,
two independent 64-bit hashes) collision-safe without coordination. The
premise of this design is to stop paying for collision resistance with
width and instead obtain uniqueness *by construction* through allocation
— using the leased-block allocator discipline this codebase already
built, broke, fixed, and validated for revision timestamps.

## Non-goals

- No change to transaction, MVCC-history, or cleaner semantics.
- No dual-width runtime: one id format per store generation.
- No preservation of hash-derived addressing for large populations
  (full-text terms, hash-index buckets); those migrate to allocated ids.

## Id layout

64 bits, one class tag:

```
allocated:  [ tag=0 : 1 ][ locality : 15 ][ origin : 12 ][ sequence : 36 ]
hashed:     [ tag=1 : 1 ][ hash                                     : 63 ]
```

- **tag** — separates the allocated and hashed namespaces by
  construction; the two classes can never collide with each other.
- **locality** (allocated only) — the placement affinity key; 32k
  routing buckets. Free-form with respect to uniqueness.
- **origin** — an allocator identity (a consumable lease slot, not a
  permanent server identity); 4096 slots.
- **sequence** — per-origin counter; 2^36 ≈ 68.7B ids per slot.

Lifetime allocated-id budget: 2^48 ≈ 281T ids (locality excluded from
uniqueness). At a sustained cluster-wide 1M allocations/s that is ~9
years; the split is a format-version constant and `locality` can shrink
to 12 bits (2.2 quadrillion ids) if a deployment needs more. Sentinels:
all-zero and all-one ids are reserved and never issued (today's
`unit_id`/`max_id`).

## Generation modes

**Explicit affinity.** A creator that wants co-location copies the
locality bits of the anchor id: `new_id.locality = anchor.locality`.
This reproduces the current "same hi bits, same partition" idiom
(vertex + satellite cells) with a 15-bit affinity key. Uniqueness is
untouched because it rests entirely on `(origin, sequence)`.

**Default (uniform).** Unaffiliated cells derive locality from a
multiply-xor-shift mixer over `(origin << 36) | sequence`, taking the
top 15 bits. Statistically uniform placement, deterministic (same
allocation order → same ids → reproducible tests and benchmarks), no
RNG state in the allocation hot path, and immune to cloned-seed
clumping in forked workers.

**Hashed (restricted).** Keyed singletons may use `tag=1` with a 63-bit
hash of `(schema, key)`. Routing uses the full id; hashed ids carry no
locality bits — anything needing both a key and affinity takes an
allocated id plus a key-index entry. Population budget: ≤ ~10^7 ids per
collision domain (p ≈ n²/2^64; 10^7 → ~5·10^-6). Current `from_obj`
consumers must be inventoried; the full-text term/segment ids and
hash-index buckets exceed the budget and must move to allocated
internal ids with their own term→bucket resolution.

## Uniqueness

Three guarantees, one per boundary:

1. **Allocated class — impossible by construction**, given:
   - *Single ownership of each origin slot.* Slot assignment goes
     through consensus (raft membership). Slots carry an **epoch**;
     replacing a presumed-dead holder bumps the epoch, and an allocator
     with a stale epoch refuses to issue (zombie fencing). Retired or
     exhausted slots are never re-leased.
   - *Durable-lease-before-issue.* An allocator persists a block lease
     (origin, block end, epoch) and only then issues ids from the
     block. Every issued id is therefore ≤ its origin's durable lease
     end at all times. Abandoned block tails become gaps; gaps are
     legal. This is the revision-allocator discipline, including both
     of its known-and-fixed failure modes: the counter-rewind race
     (monotone `fetch_max`, never `store`) and the recovery-floor rule
     (the floor comes from the durable lease record, never from
     scanning stored cells, because issued-but-unwritten ids exist).
2. **Hashed class — bounded and detected, never silent.** Hashed ids
   are keyed cells and keyed cells store their key. On id-matching
   insert, compare keys: equal → legitimate upsert; different → an
   explicit collision error (with an allocated-id + key-index fallback
   available to the caller). One comparison on keyed inserts converts
   the residual probability into a loud, recoverable failure.
3. **Cross-class — the tag bit.** Namespaces are disjoint by layout.

## Restart and recovery

Governing rule: **the lease record must be at least as durable as
anything that can reference the ids.** References live on other
servers, so block leases are replicated consensus state (one raft entry
per block), not local disk. One consensus round per ~2^20-id block
amortizes to noise.

Restart sequence:

1. Rejoin membership; raft confirms the origin slot and bumps its
   epoch (fencing any warm zombie holding the old block).
2. Read the origin's highest durable lease end from replicated state.
3. During the recovery scan that already runs, compute the maximum
   sequence actually present in recovered cells for this origin;
   `floor = max(lease_end, scanned_max + guard)`. Redundant when the
   invariant held; insurance against operator-error restores. (Same
   belt-and-suspenders as `establish_recovery_floor`.)
4. Lease a fresh block above the floor; serve from memory.

Failure matrix covered: crash mid-block (tail gap); crash between
lease-persist and first issue (whole-block gap); local disk loss with
cluster alive (replicated lease survives; new epoch fences the old
identity); zombie resurrection (epoch check); full-cluster restore
(leases restored from the consensus snapshot, which the durability
ordering keeps no staler than data; step 3 catches violations).
Volatile deployments use the same path — distinguishing "nothing can
reference me" buys nothing and would add a mode.

## Routing, placement, and skew

Consistent hashing over the 15-bit locality bucket for allocated ids
(standard vnode granularity), full-id hashing for hashed ids.
Intra-server chunk selection keys off the same locality prefix,
mirroring today's `higher`-based chunk routing. The in-chunk cell index
becomes exact: its 64-bit key now *is* the id, removing the current
soft exposure where distinct 128-bit ids share `lower`.

Co-location is indivisible by construction: one affinity group cannot
be split across servers, so a mega-group caps at single-server
capacity. Escape hatch: group owners may spill overflow members to
default-routed ids at the application layer (e.g., an adjacency
continuation cell chain whose continuations are uniform-routed).

Side benefits recorded for later exploitation: co-located transactions
become single-participant (the case the parked fused commit+end
optimization on `feature/point-cell-mvcc` serves), and sequential
allocation clusters ranged-index inserts.

## API and migration

`Id` is a public 128-bit type across Dovahkiin, Nebuchadnezzar, and
Morpheus. Two strategies:

- **(a) Big-bang type change (recommended).** New 64-bit `Id` in
  Dovahkiin, mechanical propagation through both dependents, new
  storage format version, offline reload for existing data. Right for
  the project's current stage; avoids carrying two representations.
- **(b) Transitional embedding.** Encode the new id in `lower` with
  `higher = 0` to stage application-level changes before the format
  flip. Only worth it if (a) must be split across releases; none of
  the density wins land until the format changes.

Prerequisites either way: inventory and migrate every `Id::from_obj`
call site (keyed cells → hashed class with collision detection;
full-text and hash-index internals → allocated ids); replace
`is_greater_than` uses with plain `Ord` (the current implementation is
a broken partial order and must not survive the migration).

## Testing plan

- Allocator unit battery ported from `ram::revision` tests: strict
  monotonicity, concurrent takers never collide or regress (run under
  load — its ancestor's race only surfaced under suite pressure),
  floor-above-lease refill, exhaustion refusal.
- Origin lifecycle: consensus single-ownership, epoch fencing rejects
  stale issuers, slot exhaustion leases a fresh slot, retired slots
  never re-lease.
- Restart suites: every row of the failure matrix, including a
  deliberately stale-snapshot restore proving the scan-floor catches it.
- Hashed-class collision detection: same-id-different-key insert errors
  loudly; upsert path unaffected.
- Mixer distribution test: chi-squared uniformity over locality buckets
  at realistic allocation counts; determinism across replays.
- Benchmarks: bytes-per-edge on a Morpheus dataset before/after (the
  number that justifies the surgery), plus the standard portfolio gated
  under the campaign rules — 5-run populations for any claim under 5%,
  fresh interleaved baselines both sides.

## Open questions

1. Final bit split (15/12/36 proposed) — decide against Morpheus's
   projected group counts and per-origin allocation rates.
2. Complete `from_obj` consumer inventory and per-consumer migration
   assignments (hashed-with-budget vs allocated-with-index).
3. Morpheus adjacency encoding on top of 64-bit ids (delta/varint over
   sorted co-located neighbors could compound the win) — separate doc.
4. Migration mode (a) vs (b) and the reload tooling story.
