# Crash-Safety Campaign: no corruption, atomic transactions

Goal: a kill -9 or power loss at any instant leaves the store in a state
that recovery turns into: (a) no corrupt or torn data served, ever; (b)
every transaction either fully applied or fully rolled back; (c) losses
confined to the explicitly contracted window (see Contract below), and
never silent.

Grounded in a full audit of the WAL/recovery/undo paths (2026-08-22).
Sixteen crash windows were identified with file:line evidence; they are
numbered #1–#16 throughout and listed in the appendix.

## The contract (Phase 0 — decide, then everything serves it)

- **Corruption: zero tolerance.** After any crash, recovery either serves a
  record byte-identical to what was written, or refuses it loudly
  (truncated tail / quarantined file), never silently serves garbage.
- **Transactions: atomic and durable at ack.** When `commit` returns
  success, every write of that transaction is fsync-durable and recovery
  will keep all of them; when it doesn't, recovery rolls all of them back.
  The commit point is one durable record.
- **Non-transactional writes: bounded-window group commit** (status quo,
  ≤ `WAL_SYNC_INTERVAL_MS` = 10ms / 1MB) — DECIDED 2026-08-22 (user):
  losing the un-fsync'd tail on power loss is contracted, but the tail
  must be cleanly absent, never torn, and a swallowed journal failure
  (#3) is a contract violation regardless.
- **Indexes: rebuildable, never authoritative.** An index store running
  with `wal_storage: None` (#4) must be provably reconstructible from
  data; a reindex/scrub tool makes that promise executable.

## Phase 1 — Recovery correctness (the corruption class)

The theme: what recovery reads must be self-validating.

1. **WAL per-record framing with CRC** (#6). Today a WAL record is a raw
   byte-copy of the segment append region with no magic/CRC/LSN; a torn
   interior (header landed, body zeros) replays as a valid cell with a
   zeroed header. New WAL file format: version magic in the file header;
   each record framed `[len][segment_offset][crc32c][payload]`. Recovery
   verifies CRC, and truncates at the first bad frame (tail rule) —
   scanning stops cleanly instead of ingesting garbage. Old-format files
   remain readable for one release (version byte dispatch).
2. **Never resume a WAL-recovered segment as append head** (#8). The
   torn-tail desync — memory `append_header` rewound past the tail while
   the WAL reopens in append mode — silently breaks the positional
   invariant for every post-restart write to that segment. Fix: recovery
   marks recovered segments non-head (sealed-for-append); writes start a
   fresh segment. The `segment_offset` field in the new frame (1.) makes
   the invariant checkable, not just assumed.
3. **Undo log per-record CRC + truncate-at-tear** (#12). Today one garbage
   `txn_id_len` aborts `recover()` entirely and the server boots with NO
   rollback — the worst possible failure mode for the atomicity story.
   CRC each record; a bad frame truncates the tail; anything before it
   still rolls back.
4. **Quarantine, never panic** (#9). A backup CRC mismatch currently
   `panic!`s, which bypasses the existing quarantine + WAL-twin fallback.
   Convert to an error that flows into that machinery; a store with a
   quarantined segment boots degraded and says so.
5. **Directory fsync discipline** (#14). Backup dir fsync becomes
   mandatory (result checked) BEFORE the WAL unlink; WAL dir fsync'd after
   the unlink. Otherwise a crash can lose a whole segment that both files
   covered moments earlier.
6. **Deterministic version tie-break** (#7) — replace `seg_id` scan-order
   ties with (version, seq_id) chronology — and **use the tombstone's
   `segment_seq_id`** (#13) so a tombstone is scoped to the incarnation it
   deleted (kills a resurrection class).

## Phase 2 — Transaction atomicity, single node

The undo log is already the strongest log in the system (per-record
fsync, version-verified rollback, boot-time application). The breakage is
sequencing around it:

1. **Commit syncs the right segments, then the marker, then acks.**
   Today (#11) commit fsyncs segments derived from the OLD cell addresses
   of updates/removes only — pure-insert transactions fsync *nothing*
   (their writes were `skip_sync` (#5) and never enqueued). Fix: track the
   head segment of every entry the txn APPENDED (insert/update/tombstone);
   at commit, `force_wal_sync` that set; only then write the commit
   marker; only then return `Success`.
2. **Close the marker gap** (#10). Commit is acked in the `commit` RPC but
   the marker is written in the later `end` RPC — a crash between the two
   rolls back an acknowledged commit. With (1.) the marker write moves
   into the commit path itself, before the ack. `end` becomes bookkeeping.
3. **Rollback idempotency under crash-during-rollback.** Recovery
   rollback re-journals through the normal write path; a second crash
   mid-rollback must converge. The version-verified restore should give
   this already — prove it with a targeted test, don't assume.
4. **Perf gate:** marker-fsync-per-commit is new latency. Batch marker
   fsyncs (group commit on the undo log) if the A/B shows it matters.
   Measured A/B on .239 before/after is mandatory (lightning-no-regression
   discipline applies to this path too).

## Phase 3 — The write path's swallowed failures

1. **Seal-race root cause** (#15, the standing `WAL_JOURNAL_FAILURES`
   generator, ~40 hits/17M-edge import). Archive deliberately does not
   drain writers, so an in-flight `PendingEntry` can land after seal +
   WAL unlink and its journal write is refused — the entry then has no
   durable home. Fix at the source: a `PendingEntry` holds a journal
   guard (per-segment in-flight count); seal/WAL-unlink waits for the
   count to drain (bounded, microseconds — the entries are already
   allocated) instead of racing it. The containment (counter + error log)
   stays as a tripwire that should then read zero.
2. **Journal failure must fail the write** (#3). If `write_wal` errors
   for any *other* reason, the caller must see it (or the entry must be
   re-homed to a fresh segment), not a counter increment after an `Ok`.
3. **Index-visibility ordering** (#2): cell visible in `cell_index`
   before its WAL record exists. Under the bounded-window contract this
   is acceptable for non-txn writes (same window), but the txn path must
   journal before prepare-success. Audit and pin with a test.

## Phase 4 — Integrity of what's already durable

1. **Per-block CRC in the backup format** (#16). The whole-file CRC is
   verified only on full-file recovery reads; cold per-block reads verify
   nothing (the code comment claiming otherwise is wrong). Format bump:
   CRC32C per block in the block index. Old files: whole-file CRC on
   first open, then trust.
2. **Cell-header CRC** — the `cell.rs:484` TODO. With WAL frames, backup
   whole-file + per-block CRCs in place, decide whether cell-level CRC
   still buys anything (it catches in-memory scribbles, not just disk) —
   measure the write-path cost, then decide.
3. **`wal_storage: None` audit** (#4): enumerate every embedded store
   running without WAL, prove each is rebuildable, and wire the rebuild.
4. **The reindex/scrub tool** (booked since the durability overhaul,
   never built): offline pass that walks chunk data, rebuilds ranged
   index entries, verifies sidecar family completeness. This is the
   backstop that turns any future index-corruption bug from an outage
   into a maintenance command.

## Phase 5 — Verification: make crashes boring

1. **Crash-churn fuzzer, delete-heavy + transactional lanes.** The
   existing fuzzer (insert-heavy, 2×24 cycles clean) never deletes and
   never runs transactions — this week proved deletions are where
   untested bugs live. New lanes: mixed insert/update/delete/txn workload,
   kill -9 at randomized points (including inside commit, inside archive,
   inside rollback), then full invariant check: per-schema scan counts,
   no resurrections, no torn cells, every txn all-or-nothing (writes
   tagged by txn id in cell payloads so atomicity is checkable from data).
2. **File-mutilation injection**: after a kill, randomly truncate/zero
   the tail (and random interiors) of WAL/undo/backup files before
   restart — direct exercise of every CRC/truncation/quarantine path from
   Phase 1. Cheap, no root needed, catches what fsync-timing kills can't.
3. **TLA+ for the commit protocol** (docs/tla/ is the home, tlc
   installed): model prepare/commit/marker/crash interleavings for the
   Phase 2 design BEFORE building it; guard against vacuous passes.
4. **Acceptance:** overnight kill-churn at FlyWire scale on .239 (import
   + crash loop + probes), plus the standard suite gates. Target: zero
   integrity violations across ≥100 crash cycles, WAL_JOURNAL_FAILURES=0.

## Phase 6 — Distributed atomicity (design first, build later)

Single-node atomicity (Phases 2–3) is the foundation. Multi-node 2PC has
a known-open hole: the coordinator (`manager.rs`) keeps NO durable state,
so a coordinator crash between prepare-success and commit fan-out leaves
participants in-doubt with no resolution protocol. Plan: TLA+ model of
coordinator-decision durability (plane/raft-backed decision record vs
participant-led termination protocol), then implement. Explicitly out of
scope until the single-node story is proven by Phase 5 — DECIDED
2026-08-22 (user): design-only this campaign.

### Phase 6a SETTLED (2026-08-23): the head-pool / bracket-chain design

Everything below this header is the FINAL design, settled with the user
over 2026-08-22/23. The subsections after it (stamps, per-entry local
sequences, the three-pass counting recovery, the earlier bracket
discussion) are kept as the decision record but are SUPERSEDED where
they conflict. What carries over unchanged: the decided-watermark in
file headers, the cleaner rule bound to it, and the COMMIT cross-chunk
manifest check.

THE CORE: each chunk has a POOL of head segments per class
(regular/blob) instead of one. Whoever writes holds a head exclusively;
hold DURATION is the only difference between writer kinds:

    plain write   one cell
    batch         one run
    transaction   its whole bracket chain, UNTIL THE 2PC DECISION

Single writer per segment means a crash can only tear a segment's TAIL,
and a torn tail (CRC fail) simply means end-of-segment -- the case
recovery has always handled. Mid-segment holes are impossible BY
CONSTRUCTION. The journal (WAL append, not fsync) happens INSIDE the
ownership window, so each segment's WAL is offset-ordered and
prefix-complete; group commit is unchanged.

TRANSACTIONS. The write set is exactly known at apply time (ops list +
write plans), so the footprint per chunk is EXACT, not an estimate:

- Fits the claimed head's remainder: single bracket
  `BEGIN .. entries ..`, held until the decision. The common case.
- Larger: PRE-ALLOCATE the whole chain of fresh segments up front.
  Allocation failure happens before any byte is written. Big
  transactions barely touch the pool.

CHAIN LAYOUT (the user's fixed-tail-link design):

    part 1 (pooled head): [others][BEGIN(T)][entries][zeros..][TXN_CONT @ end]
    parts 2..k-1 (fresh): [entries...........][zeros........][TXN_CONT @ end]
    part k (fresh, open): [entries][COMMIT(T, manifest)][free -> pool]

`TXN_CONT(txn, prev_seq)` ALWAYS occupies the last 24 bytes of a full
chain segment; plain zeros pad from the last entry that fit. The fit
check reserves those 24 bytes whenever more entries remain. Zeros are
safe here precisely because the fixed-position link bounds the segment
from the outside -- the ambiguity that required PADDING entries in
shared segments does not exist. seq ids, never segment ids (seq is the
identity that survives recovery).

Recovery reads the fixed tail of each file: chain membership is O(1)
per segment, no scan -- and an aborted or torn chain's segments are
DISCARDED WITHOUT EVER BEING SCANNED. Part k carries no link; the
COMMIT manifest (chunk -> [seq1..seqk], exact from pre-allocation) is
the authoritative chain order, links are the cross-check.

DECISION:
- COMMIT: fsync all chain WALs -> write COMMIT into each chunk's tail
  bracket -> fsync those -> ack. Bracket committed iff it ends with its
  COMMIT (single-chunk fully local); multi-chunk: any COMMIT found +
  every manifest member present, else loud discard (safe: ack had not
  happened).
- ABORT: physically vanish. Part 1 rewinds to its pre-transaction
  cursor (ownership means nobody wrote after it; journaled padding over
  the bracket, cursor back); fresh parts recycle whole. No undo log, no
  junk, no cleaner debt. Chained and unchained alike.
- CRASH before decision: chain has no COMMIT -> discarded, parts
  unscanned. Same outcome as abort.
- IN-DOUBT (coordinator gone): heads are hostages; after a timeout,
  release and fall back to discard-unless-COMMIT semantics for that
  transaction. Timeout policy is a real open knob, coupled to the
  Phase 6 termination-protocol gap.

POOL ACCOUNTING IS CONSERVED: commit-with-chain swaps the now-full
part-1 head out (to seal/archive) and the half-used tail in; plain
commit returns the same head fuller; abort restores the exact
pre-transaction state. The pool never shrinks and has no refill path.
A recycled tail carries a committed bracket with ordinary entries after
its COMMIT -- the mirror of part 1, already a handled shape.

WHAT THIS RETIRES: the undo log from the write path, per-entry
transaction stamps, the flag bit, local sequences, and the END entry
entirely (it only existed for release-before-decision). Entry types:
BEGIN, TXN_CONT, COMMIT (+ PADDING, already landed).

COSTS, honestly: K x 8 MiB x chunks x classes resident (K elastic with
a cap); K WAL fds per chunk (multiplied by dynamic databases -- the
EMFILE lesson applies); the pool caps SMALL-transaction concurrency per
chunk at K / decision-latency (big ones pre-allocate outside the pool);
padding waste bounded by one entry per chain boundary. Footnote: the
O(1) tail read works on backups (block index reaches the last block);
WAL frames are forward-only, but chain middles are full the moment they
are written, so they archive almost immediately -- crash-fresh chains
in WALs get scanned normally.

VERIFICATION, done 2026-08-23:

TLA+: docs/tla/TxnChainRecovery.tla (`8c8bc38d`). Fixed config passes
four invariants; disabling either the cleaner gate or the watermark
gate violates InstalledWasCommitted (both gates proven load-bearing);
the Sanity config fails on purpose (vacuity guard). Implementation
lessons from TLC: the abort/rewind path must key on TRANSACTION STATE,
never on the presence of a COMMIT record (the cleaner legitimately
drops it below the watermark); and "installed implies committed" is a
separate invariant from "no partial install", because the
watermark-gate failure mode installs uncommitted transactions
COMPLETELY.

CONTENTION SPIKE (.239, 192 cores, ~/poolspike/results.txt): the pool
is viable, and the spike corrected the baseline mental model. The
"shared cursor" baseline modeled holds (journal work) as parallel --
but TODAY's system serializes ALL journal writes to a head on that
segment's file_state mutex (WAL_LOCK_CONTENDED exists precisely because
of this), so today is really K=1 per chunk for the hold. Correctly
read:

- Pool ceiling = K / hold. Measured: K=16, hold=2us -> ~6.1M ops/s per
  chunk (76% of theoretical). Today's equivalent ceiling: 1/hold ~
  0.5M. The pool is ~16x today PER CHUNK on the axis that matters.
- Real per-chunk load at import peak is ~6K entries/s (380K/s across
  64 chunks): the pool ceiling leaves 300-1000x headroom even at K=4.
- With K >= active writers, the acquire path is UNCONTENDED and beats
  the shared CAS by an order of magnitude (affinity scanning works).
- K=4 degrades badly at 96-192 threads (contended slot scan); K must
  be elastic, 8-16 under load. Confirms the elastic-K decision.

Caveat: allocator-only microbench. The binding gate remains the full
import A/B when the implementation lands, per house rule.


### Phase 6a: retire the undo log from the write path (Zen-style)

Prompted by the user pointing at Zen (Liu, Chen & Chen, VLDB 14(5), 2021),
whose LP ("Last Persisted") bit is the MSB of a 63-bit per-tuple Tx-CTS:
a transaction persists its tuples, fences once, then sets LP on the LAST
tuple. Recovery groups tuples by Tx-CTS and treats a group as committed
iff one member carries LP; otherwise the previous versions (never
overwritten) stand. No logs, no checkpoints.

What we take, and what changes because we are NOT on PMEM:

- TAKE the architecture: stamp transactional entries with a commit
  timestamp, keep the commit evidence in the SAME log as the data, and
  fall back to the previous version. That is what removes the undo log's
  marker, its separate file, its second fsync, and the ordering
  dependency between them.
- DROP the bit as the mechanism. On PMEM the LP bit exists to avoid one
  extra 64B line write; in an append log an extra record costs nothing we
  were not already fsyncing. Commit evidence is a COMMIT ENTRY, not a
  stolen bit -- and a record can carry what a bit cannot (below).
- REPLACE ordering with completeness. Zen's sfence guarantees
  data-before-LP; fsync gives no such ordering, since until it returns any
  subset may be durable. So the commit record names the transaction's
  ENTRY COUNT and recovery accepts it only on finding all N. The CRC
  record framing from Phase 1.1 is what makes that count trustworthy.
- ADD a global reduction. Zen's per-thread regions make a local scan
  conclusive; our transactions scatter across chunks, so the commit set
  must be reduced across all of them. Recovery already does a global
  second pass for tombstones, so the shape exists.

DECIDED 2026-08-22 (user): the stamp does NOT widen the entry header.
Growing the 8-byte header to 16 would charge every cell and tombstone
(~29 GB at TB19 scale) for a minority of writes -- the same arithmetic
that kept the checksum out of CellHeader. Instead one spare bit in the
type byte marks "carries a Tx-CTS", and only tagged entries pay 8 bytes,
in their CONTENT:

    bits 0..4    entry type        (UNDECIDED/CELL/TOMBSTONE, + COMMIT)
    bits 4..8    flags             (bit 4 = carries Tx-CTS)
    bits 8..32   content checksum  (unchanged; covers the stamp for free)

Non-transactional writes -- the bulk, imports included -- are untouched.
Alignment holds (8-byte stamp, 8-byte aligned entries). The cost is that
`content_pos` becomes flag-dependent, so every site decoding content from
an entry address must agree; a missed one reads a cell header out of a
Tx-CTS, which is why entry checksum verification wants to be live on
those paths while the change beds in.

DECIDED 2026-08-22 (user): 8-byte LOCAL COMMIT SEQUENCE per transactional
entry, flagged by one spare bit in the type byte; the full 16-byte
`bifrost::hlc::Hlc` recorded ONCE PER TRANSACTION in the COMMIT entry.
Non-transactional writes pay nothing.

Considered and REJECTED: bracketing the transaction with BEGIN/COMMIT
entries so membership is positional and entries carry nothing. It is
attractive -- zero bytes per entry, no flag bit, and it would have kept
`content_pos` uniform, removing the stamped design's sharpest hazard --
and it is reservable in principle, since `try_acquire_run` already claims
a contiguous span and transactional writes are applied in one loop.

It loses on holes. Reservation advances the append cursor BEFORE any
bytes are written, so a crash in that window leaves a zero gap that no
BEGIN yet describes. Writing BEGIN first narrows the window but cannot
close it. And a hole in the MIDDLE of a segment is not like a hole at the
tail: the forward scan stops dead there, losing every entry other writers
appended after it. Bracketing turns a microsecond-wide, one-entry hazard
into a transaction-wide one. Secondarily, the cleaner would have to skip
any segment containing an undecided bracket, coupling reclamation to
transaction lifetime at segment granularity.

Rejected earlier in the same discussion: 16 bytes per entry (doubles a
permanent per-cell cost for identity needed once per transaction -- 29 GB
vs 58 GB on a 3.6B-cell all-transactional store); a squeezed 8-byte HLC
(32 bits of `ts` cannot hold a usable millisecond range alongside the
16-bit logical counter); `(node32, per-node seq32)` (globally unique in 8
bytes and viable, but coordinator sequences are not mutually comparable,
so the watermark becomes a per-node vector inside a fixed-size file
header). Node-id truncation to 32 bits is itself fine and DETECTABLE at
join -- check the truncated id against current members, re-salt on
collision -- far lighter than a dense member-id scheme.

FUTURE COMPRESSION, not for v1: only transactions ABOVE the watermark
need distinguishing, so a 2-byte slot suffices if reuse is forbidden
while a slot's transactions remain above it (29 GB -> ~7 GB in the
all-transactional case). Deferred because it couples correctness to
watermark LIVENESS: a stalled watermark stops issuing slots and stalls
transactions, where the 8-byte version merely gets slower. An 8-BIT slot
would cap in-flight undecided transactions at 256 and throttle a
192-core box; 16 bits is the floor for that variant.

IMPLEMENTATION HAZARD to pin with tests: `content_pos` becomes
flag-dependent, so every site decoding content from an entry address must
agree. A missed one reads a cell header out of a transaction id. Keep
entry checksum verification live on those paths while the change beds in.

The cleaner constraint this creates has teeth: a version whose only
successor is an UNDECIDED transaction must not be reclaimed, or the
fallback target is gone, and commit evidence must survive until its
transaction's entries are superseded. Bound both with a durable "every
transaction below CTS X is decided" watermark -- Zen's global minimum
Tx-CTS.

Portability: the entry stamp is the durable investment and is identical
on either memory technology. Only the evidence form changes -- a COMMIT
entry on block storage, Zen's LP bit if the store ever moves to PMEM.

#### Recovery with commit entries

Three passes, and only the first touches data -- reading the files stays
exactly as expensive as it is today.

PASS 1, scan (parallel per chunk, as now). Untagged entries install into
the cell index immediately, unchanged. Tagged entries do NOT install:
they go on a per-chunk pending list keyed by Tx-CTS, while each chunk
accumulates `cts -> entries_seen` and any COMMIT entries it finds.
Tagged tombstones defer too -- an uncommitted delete must not delete.

PASS 2, global reduction (metadata only, cheap). Merge the per-chunk
maps. The rule:

    committed  iff  a COMMIT entry exists for this CTS
                AND entries found across ALL chunks == the count it declares

No COMMIT entry means not committed -- presumed abort, and sound because
the commit entry is written last and fsynced, so its absence means no
success was reported. A COMMIT entry with a short count means the fsync
did not finish; that completeness check is what stands in for the
data-before-commit ordering an sfence would give on PMEM.

PASS 3, install (parallel again). Committed transactions' pending
entries install with the normal version reconciliation. Uncommitted ones
are skipped, which by itself leaves the previous version standing --
that version was installed in pass 1 and nothing higher supersedes it.
Skipped entries are marked dead so the cleaner reclaims them.

THE WATERMARK IS LOAD-BEARING FOR CORRECTNESS, NOT SPEED. A durable
"every transaction below CTS X is decided" mark lets entries below it
install directly with no counting. That is not just a way to bound the
pending set: the cleaner legitimately compacts away superseded entries
of old committed transactions, so re-running the count check on them
would find fewer entries than declared and wrongly abort a transaction
that committed long ago. Hence the count rule applies ONLY above the
watermark, and the cleaner's rule is its mirror -- do not touch entries
above it, and neutralise aborted entries before advancing past them.

THE COMMIT ENTRY CARRIES: the CTS, the entry count, and a per-chunk
breakdown. The breakdown costs a few bytes and turns a silent failure
loud: a missing chunk file reports "expected 12 entries in chunk 7,
scanned 0" instead of quietly counting short and aborting a transaction
that really committed.

WHAT THIS RETIRES: the undo log leaves the write path entirely.
Uncommitted inserts are never installed, so there is nothing to delete;
uncommitted updates and removes fall back to the prior version the
append-only store still holds. No markers, no separate file, no second
fsync, no cross-file ordering dependency.

THE WATERMARK LIVES IN THE FILE HEADERS, STAMPED AT SEAL (decided
2026-08-22, user; supersedes the COMMIT_WATERMARK entry sketched first).

Recovery ALREADY parses these headers before touching content --
`declared_used_len` reads the backup header without decompressing, and
`is_framed` reads the WAL header before any record scan. So a watermark
field arrives in pass 0 for free: no targeted pre-pass, no double
parsing, no guessing which chunk holds the newest. Recovery takes the max
across discovered files. It also removes two rules the entry form needed:
no cleaner retention rule (the header dies with its file), and no
"references no cell, looks like garbage" special case.

Safety: each header records the watermark that was durable when that file
was sealed, and decidedness is permanent, so a value true at seal time
stays true. Max-over-headers can never exceed the true watermark; if the
file carrying the highest stamp is reclaimed, the max drops -- staler,
still safe.

Cost: freshness is bounded by SEAL cadence, not watermark cadence -- a
live head's WAL header was written at creation, so the freshest stamp
comes from the last seal. Self-limiting, though: few seals means few
writes means few transactions above the watermark to count. An idle store
has both an old watermark and almost nothing above it.

Format mechanics:
- WAL: the header already carries a version byte and 3 reserved bytes.
  Bump v1 -> v2, 16 -> 24 bytes, watermark in the tail; v1 files read as
  watermark 0, safe by construction.
- BACKUP: BLOCK_HEADER_SIZE is 20 with fixed offsets, so growing it needs
  a magic bump (NEB\x03 -> NEB\x04); old backups report no watermark.
  This is the one that matters long-term, because WAL files are DELETED
  at archive -- a store recovered from backups alone would otherwise
  carry no watermark at all and count everything forever.
- CONSOLIDATE: Phase 4.1 already wants a backup format bump for per-block
  CRCs. Do both in one version increment, not two. They are
  complementary: per-block CRCs make cold reads verifiable, the watermark
  makes recovery cheap.

Kept in reserve: a COMMIT_WATERMARK entry is the only way to advance the
mark WITHOUT sealing a segment. Not needed while staleness self-limits,
but that is the reason to keep the idea rather than delete it.

SEPARATE FINDING, worth fixing on its own (surfaced while evaluating
bracketing): A MID-IMAGE HOLE TRUNCATES RECOVERY TODAY. `try_acquire`
advances the append cursor before the entry bytes are written, so a crash
in that window leaves a gap with no WAL record. The framed WAL rebuilds
the image by offset, so later records land correctly past the gap -- but
`scan_segment_from_data` then walks the image FORWARD and stops at the
zeros, discarding entries that were fully durable. The window is small
(one entry, microseconds) but it is not zero, and the loss is unbounded:
everything after the hole in that segment.

The fix falls out of the framing already landed: recovery can iterate the
VERIFIED WAL RECORDS directly -- each declares its own `seg_offset` and
length -- instead of forward-parsing the reconstructed image. That is
immune to holes by construction. Backups still need the sequential scan
(they carry no frames), but a backup is written from a complete image, so
it has no holes to begin with.

TO MODEL IN TLA+ BEFORE ANY CODE:
1. Safety: an installed transaction is exactly one whose commit was
   reported; never a partial install.
2. No resurrection: skipping an uncommitted entry leaves precisely the
   prior version, including across tombstones.
3. Idempotence: recovery re-run after a crash mid-recovery converges.
4. Watermark advance: no interleaving of cleaner, abort neutralisation
   and watermark advance can leave a committed transaction unprovable
   or an aborted one installed.
5. The distributed generalisation: "all N of my entries present" becomes
   "every participant reports its entries present" -- Parallel Commits,
   reusing this machinery rather than adding a second commit path.

## Sequencing and dependencies

Phase 1 and Phase 3.1 first (they close active corruption/loss windows and
the seal race is already firing 40×/import). Phase 2 rides on Phase 1.3.
Phase 5's fuzzer lanes get built EARLY (right after Phase 1) so every
subsequent phase lands with its crash-lane already watching. Phases 4 and
6 trail.

## Appendix: the 16 audited crash windows

| # | Window | Where |
|---|---|---|
| 1 | Non-txn write acked ≤10ms before fsync (contracted) | chunk.rs:2591, segs.rs:69 |
| 2 | Cell visible in index before WAL record written | chunk.rs:1442 vs 2570 |
| 3 | WAL journal failure swallowed after ack | chunk.rs:2581 |
| 4 | `wal_storage: None` stores acked with zero journaling | file_manager.rs:94 |
| 5 | Transactional writes never enqueue a sync | chunk.rs:2591, segs.rs:2054 |
| 6 | Torn WAL interior replays as valid cell (no CRC) | recovery.rs:699-729 |
| 7 | Version ties resolved by seg_id scan order | recovery.rs:227, 759 |
| 8 | Torn tail ⇒ WAL/segment offset desync on resumed head | recovery.rs:438, chunk.rs:1203 |
| 9 | Backup CRC mismatch panics, bypassing quarantine | compression.rs:529 |
| 10 | Committed txn rolled back (marker in later `end` RPC) | data_site.rs:1173 vs 4263 |
| 11 | Commit fsyncs wrong segment; pure inserts fsync nothing | data_site.rs:1154, 1045, 1135 |
| 12 | Torn undo-log tail silently disables ALL rollback | undo_log.rs:888, mod.rs:1594 |
| 13 | Tombstone seq_id recorded but unused in recovery | recovery.rs:794-807 |
| 14 | Dir fsyncs best-effort/one-sided around archive+unlink | segs.rs:1913, 1978 |
| 15 | Archive doesn't drain writers ⇒ sealed-seg straggler homeless | segs.rs:1598, 2024 |
| 16 | Cold per-block reads bypass the only real CRC | compression.rs:487-510 |

### Windows 17-19: found by the fuzzer AFTER the audit (2026-08-23/24)

The audit was static; these three were found by running the thing. All are
fixed, with non-vacuous regression tests.

| # | Window | Where | Fix |
|---|---|---|---|
| 17 | Abandoned image mutated in place breaks its content checksum, so the scan stops there and DISCARDS the rest of the segment | cell.rs `abandon_entry_version`, recovery.rs verify | `1dbdea86` — retire an entry by stamping CHECKSUMMED padding over its span; never edit published content |
| 18 | One damaged entry ends the segment walk, so mid-segment damage costs every entry behind it | recovery.rs `verify_entry_at == Some(false)` | `855890e0` — resync onto a successor that vouches for itself; unreadable tails still stop |
| 19 | Replaying a placement panics the server when the target is unknown (any address change makes every placement unknown) | ranged/sm.rs `load_sub_tree` | warn + leave unloaded, retried |

Also fixed in bifrost, same hunt: replay deferred forever at holes the WAL
legitimately contains (entries skipped for non-recoverable state machines),
wedging every state machine on the plane behind the first hole — which lost
the ranged index's placements wholesale (`tree placement was not found`).
bifrost `66a2606`.

**The lesson that generalises past these three.** Every one of them turned a
LOCAL fault into a TOTAL one: one entry cost a segment, one segment cost an
index, one missing log id cost a plane, one unknown server cost the process.
The store had every byte on disk in all four cases. When adding recovery
code, the question is not only "is this fault handled" but "what is the
blast radius when it is" — and the answer must be the smallest unit that
actually failed.

**And the testing lesson.** The pre-existing test for abandoned images put
the ghost LAST in the segment, where truncating at it costs nothing: it
passed for four months while the bug was live. A test for damaged or
retired entries MUST place live entries AFTER them, or it asserts nothing.

