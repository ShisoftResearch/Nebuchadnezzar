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

OPEN: 8-byte local commit sequence vs the full 16-byte HLC. Recovery only
needs to group entries and order watermarks, which 8 bytes does for
decades at a million transactions a second. The full HLC is only needed
if the distributed protocol later resolves in-doubt transactions by
reading a participant's store directly.

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
