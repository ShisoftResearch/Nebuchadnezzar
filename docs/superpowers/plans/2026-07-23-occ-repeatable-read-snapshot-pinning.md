# OCC Repeatable-Read Snapshot Pinning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** For cells above a size threshold, make transactional repeatable reads hold the version by location + `SegmentReferenceGuard` instead of cloning the whole cell, deferring the full-cell transfer until an actual full read.

**Architecture:** The engine is copy-on-write, so a read version's bytes are immutable until reclaimed; a held segment guard prevents reclamation and eviction. The participant keeps a per-transaction pinned read-set and serves that transaction's later reads of a pinned cell from the pinned location; the coordinator caches only the small result (header/projection) and fetches the full bytes lazily. Certification and every distributed phase are unchanged. Small cells and non-transactional paths are untouched.

**Tech Stack:** Rust, tokio, bifrost RPC (`service!` macro), the `neb` RAM engine (`chunk.rs`, `segs.rs`), the OCC transaction layer (`manager.rs` coordinator, `data_site.rs` participant).

**Spec:** `docs/superpowers/specs/2026-07-23-occ-repeatable-read-snapshot-pinning-design.md`

---

## File Structure

- `src/server/transactions/manager.rs` — coordinator. `WaitConfig`/new read-pin config, `DataObject` gains a pinned-read representation, `read_cached_full_cell` gains the size-gate branch, coordinator `head`/`read`/`read_selected` serve from the small-result cache or fetch the pinned version, read-only pin release on commit/abort.
- `src/server/transactions/data_site.rs` — participant. `service!` gains pinned-version read RPCs + a `release_read_pins` RPC; `DataManager` gains a per-transaction pinned read-set; read handlers pin large cells and serve from the pin; `end`/`abort`/stale-cleanup drop pins.
- `src/ram/chunk.rs` — one read helper to read/head/project a cell from a raw pinned location (mirrors the existing `SharedCellData::from_chunk_raw` usage).
- Tests live in the existing `#[cfg(test)] mod tests` of `data_site.rs` and `occ_tests.rs`.
- Benchmark: existing `benches/occ_transactions.rs` `projected_reads` group is the acceptance scenario; no new bench code.

## Conventions

- Correctness gate suite (run before accepting any phase):

```
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo check --lib
```

- Commit after each task with the shown message.
- Pick an unused loopback port for any new server test (existing tests use 127.0.0.1:52xx/53xx; use 5373+).

---

## Phase 0: Read-pin configuration + size decision

### Task 0.1: Read-pin threshold config

**Files:**
- Modify: `src/server/transactions/manager.rs` (near `WaitConfig`, struct at line 206)
- Test: same file's `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn read_pin_threshold_defaults_and_env_override() {
    // Default when unset.
    std::env::remove_var("NEB_TXN_READ_PIN_BYTES");
    assert_eq!(read_pin_threshold_bytes(), DEFAULT_READ_PIN_BYTES);
    // Override.
    std::env::set_var("NEB_TXN_READ_PIN_BYTES", "65536");
    assert_eq!(read_pin_threshold_bytes(), 65536);
    // Invalid falls back to default.
    std::env::set_var("NEB_TXN_READ_PIN_BYTES", "not-a-number");
    assert_eq!(read_pin_threshold_bytes(), DEFAULT_READ_PIN_BYTES);
    std::env::remove_var("NEB_TXN_READ_PIN_BYTES");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib server::transactions::manager::tests::read_pin_threshold_defaults_and_env_override -- --test-threads=1`
Expected: FAIL — `read_pin_threshold_bytes`/`DEFAULT_READ_PIN_BYTES` not found.

- [ ] **Step 3: Implement**

```rust
/// Cells whose serialized size exceeds this are read via pin-and-defer instead
/// of being cloned into the coordinator's transaction cache. A few KiB by
/// default: large enough that small counter cells keep the current clone path.
pub const DEFAULT_READ_PIN_BYTES: usize = 4096;

pub fn read_pin_threshold_bytes() -> usize {
    std::env::var("NEB_TXN_READ_PIN_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_READ_PIN_BYTES)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib server::transactions::manager::tests::read_pin_threshold_defaults_and_env_override -- --test-threads=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/manager.rs
git commit -m "feat(txn): add read-pin size threshold config"
```

### Task 0.2: Cell-size measurement helper (participant side)

The participant decides pin-vs-clone from the stored cell's size. Reuse the entry length already available when a cell is read. Add a `head`-cheap size probe on `Chunks` that returns the stored cell's total entry byte length without materializing the value.

**Files:**
- Modify: `src/ram/chunk.rs` (near `head_cell`, line 617 / 1656)
- Test: `src/ram/chunk.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test** — seed a small and a large cell, assert `cell_stored_len` orders them and exceeds/undershoots a threshold. (Model the seeding on the existing chunk tests that call `write_cell`.)

```rust
#[test]
fn cell_stored_len_reflects_payload_size() {
    let chunks = create_test_chunks(); // existing helper in this module
    let small = /* build a small OwnedCell via existing test helpers */;
    let large = /* build an OwnedCell with a multi-KiB payload */;
    let (mut small, mut large) = (small, large);
    chunks.write_cell(&mut small).unwrap();
    chunks.write_cell(&mut large).unwrap();
    let s = chunks.cell_stored_len(&small.id()).unwrap();
    let l = chunks.cell_stored_len(&large.id()).unwrap();
    assert!(l > s);
    assert!(l > 4096);
}
```

- [ ] **Step 2: Run — expect FAIL** (`cell_stored_len` not found).

- [ ] **Step 3: Implement** `Chunks::cell_stored_len(&self, key: &Id) -> Result<usize, ReadError>` and the per-chunk `Chunk::cell_stored_len`. Mirror `head_cell`'s locate path, but instead of decoding the header, read the entry length via `Entry::decode_from(cell_location, ...)` (as done in `update_cell_by` at line 848-851 where `entry.content_length` is captured) and return `content_length as usize`.

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/ram/chunk.rs
git commit -m "feat(ram): add cheap stored-cell length probe"
```

---

## Phase 1: Participant pinned read-set + serve-from-pin

### Task 1.1: Per-transaction pinned read-set state

**Files:**
- Modify: `src/server/transactions/data_site.rs` (the `Transaction` struct used by `DataManager`, and `DataManager` fields)

- [ ] **Step 1: Write the failing test** — a unit test that inserts a pin into a transaction's read-set and reads it back.

```rust
#[test]
fn pinned_read_set_records_and_returns_entry() {
    let entry = PinnedRead { location: 0x1000, version: 7 };
    let mut set = PinnedReadSet::default();
    set.insert(Id::new(0, 42), entry, /* guard */ None);
    assert_eq!(set.get(&Id::new(0, 42)).map(|p| p.version), Some(7));
    assert!(set.get(&Id::new(0, 99)).is_none());
}
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** a `PinnedRead { location: usize, version: u64 }` and a `PinnedReadSet` holding `HashMap<Id, (PinnedRead, Option<SegmentReferenceGuard>)>` with `insert`/`get`/`drain`. Add a `pinned_reads: PinnedReadSet` field to the participant `Transaction` struct (init in `create_transaction`, line 325). The guard is `Option` so the unit test can exercise the map without a real segment.

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/data_site.rs
git commit -m "feat(txn): add participant per-transaction pinned read-set"
```

### Task 1.2: Raw-location read helper

**Files:**
- Modify: `src/ram/chunk.rs`

- [ ] **Step 1: Write the failing test** — write a cell, capture its address via `read_cell(...).cell_guard().get_ptr()`, then read it back through the new by-address helper and assert equal data; then update the cell and confirm the by-address read still returns the OLD value (copy-on-write).

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** `Chunks::read_cell_at(&self, key: &Id, location: usize) -> Result<OwnedCell, ReadError>` (and `head_at`, `read_selected_at`) that build a `SharedCellData` from the raw location — mirror `update_cell_by` at line 839: `SharedCellData::from_chunk_raw(hash, location, chunk)` — and materialize header / projection / full cell from it. Use `locate_chunk_by_partition(key.higher)` (line 1627) to get the chunk.

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/ram/chunk.rs
git commit -m "feat(ram): read a cell version from a raw pinned location"
```

### Task 1.3: Pin large cells on participant read; serve subsequent reads from the pin

**Files:**
- Modify: `src/server/transactions/data_site.rs` — `read`, `head`, `read_selected` handlers (lines 3367-3415), and the `prepare_read` gate (unchanged).

- [ ] **Step 1: Write the failing integration test** (`#[tokio::test(flavor = "multi_thread")]`, model setup on `prepare_rejects_a_stale_present_version`): seed a large cell; open a participant transaction; `head` it (creating a pin); externally `update_cell` the cell to a new version; `read` it again in the same transaction and assert the returned value/version is the PINNED (pre-update) one, not the new current.

- [ ] **Step 2: Run — expect FAIL** (today's read returns current, not pinned).

- [ ] **Step 3: Implement.** In each read handler, after `prepare_read` succeeds:
  - If the transaction already has a pin for this id: serve `head`/`selected`/full from `read_cell_at`/`head_at`/`read_selected_at` at the pinned location.
  - Else, probe `cell_stored_len(id)`. If `> read_pin_threshold_bytes()`: acquire a `SegmentReferenceGuard` on the cell's segment (mirror `acquire_segment_guard` at line 441 using `get_cell_segment_info` + the current cell address), record `PinnedRead{location, version}` + guard in the transaction's `pinned_reads`, and serve the requested projection from that location. If `<=` threshold: current behavior (read current, no pin).
  - Ensure a participant `Transaction` exists for read-only transactions that pin (call `get_or_create_transaction`).

- [ ] **Step 4: Run — expect PASS**, plus the full gate suite.

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/data_site.rs src/ram/chunk.rs
git commit -m "feat(txn): pin large-cell reads and serve them from the pinned version"
```

---

## Phase 2: Coordinator size-gate + deferred full fetch

### Task 2.1: DataObject pinned representation

**Files:**
- Modify: `src/server/transactions/manager.rs` — `DataObject` (line 223).

- [ ] **Step 1: Write the failing test** — construct a `DataObject` in the pinned variant (header cached, no full cell) and assert accessors return the header and report "full not yet materialized".

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — add fields to `DataObject` for the pinned case: `pinned: bool`, `header: Option<CellHeader>`, `projections: HashMap<Vec<u64>, OwnedValue>` (or reuse an existing projection cache type), keeping `cell: Option<OwnedCell>` as the materialized-full slot. Do not change the small-cell path (still sets `cell`).

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/manager.rs
git commit -m "feat(txn): add coordinator pinned-read data-object representation"
```

### Design decision (2026-07-23): enriched read responses

The coordinator must choose "cache the whole cell (small) vs. defer + cache only small
results (large)," but only learns a cell's size from the participant, and today's read
RPCs carry no size signal and never return a full cell alongside a header. A header-only
RPC for a small cell would also break repeatable reads (a `head` then a later full `read`,
with a concurrent update in between, would see two versions, since small cells are not
pinned). Resolution: **enrich the participant read responses** so each carries the
requested shape, the version, a `pinned: bool`, and — for `head`/`read_selected` on a
**small** cell — the full cell too, so the coordinator caches it once and serves all shapes
locally (today's consistent, single-RPC small-cell behavior). Large cells return only the
requested shape + `pinned = true`. This splits original Task 2.2 into 2.2a (participant
envelope, behavior-preserving) and 2.2b (coordinator deferral logic, the win).

### Task 2.2a: Enriched participant read-response envelope (behavior-preserving)

**Files:**
- Modify: `src/server/transactions/data_site.rs` — the `service!` read RPC return types (`read`, `read_selected`, `head`), their handlers, and the `ensure_read_pin`/read paths added in 1.3.
- Modify: `src/server/transactions/manager.rs` — the coordinator call sites of those RPCs, to extract the existing shape from the new envelope (behavior UNCHANGED this task).

- [ ] **Step 1: Write the failing test** — a participant `head`/`read_selected`/`read` returns the new envelope with `pinned` set correctly (true for a large cell, false for a small one) and, for `head`/`read_selected` on a small cell, `full_small_cell` populated; for a large cell `full_small_cell` is `None`.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** the envelope types and change the three RPCs to return them:
  - `head` → `TxnExecResult<HeadReply, ReadError>` where `HeadReply { header: CellHeader, pinned: bool, full_small_cell: Option<OwnedCell> }`.
  - `read_selected` → `TxnExecResult<SelectedReply, ReadError>` where `SelectedReply { selected: OwnedCell, pinned: bool, full_small_cell: Option<OwnedCell> }`.
  - `read` → `TxnExecResult<ReadReply, ReadError>` where `ReadReply { cell: OwnedCell, pinned: bool }`.
  Handlers set `pinned` from whether `ensure_read_pin` pinned the cell; for a small cell (`ensure_read_pin` returned `None`) on `head`/`read_selected`, also read and attach the full current cell as `full_small_cell`. Update the coordinator call sites to just extract `header`/`selected`/`cell` and ignore the new fields for now — **behavior must be identical to before this task.**

- [ ] **Step 4: Run — expect PASS**, plus `cargo check --lib`.

- [ ] **Step 5: Commit** `git commit -m "feat(txn): enrich participant read responses with pinned/full-small-cell\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"`

### Task 2.2b: Coordinator size-gate + deferred full fetch (the win)

**Files:**
- Modify: `src/server/transactions/manager.rs` — `read_cached_full_cell` (804), `read_from_site` (861), `head`/`read`/`read_selected` service methods (397-456), using the `DataObject.pinned` cache from Task 2.1.

- [ ] **Step 1: Write the failing integration test** (`occ_tests.rs`): a transaction does `head` then `read_selected` on a large cell; assert both succeed and are consistent, and assert (via an instrumentation counter on the participant full-`read` handler) that **no full-cell fetch** occurred for a header+selected-only transaction; then a full `read` triggers exactly one full fetch equal to the pinned version. Also assert a **small** cell read as `head` then full stays consistent under a concurrent update (served from the coordinator's cached full cell).

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** the coordinator branch on the envelope's `pinned`:
  - `pinned = false` (small): behavior as today — cache the full cell (`full_small_cell` for head/selected, or the returned `cell` for a full read) into `DataObject.cell`; serve all shapes locally.
  - `pinned = true` (large): set `DataObject.pinned = Some(..)`; cache the header (`head`) / projection (`read_selected`) in the pinned cache; do NOT materialize the full cell. A full `read` fetches from the participant (served from the pin) once and stores it in `DataObject.cell`. Repeated head/selected serve from the pinned cache.
  Keep `CellExpectation::Present(version)` certification recording exactly as today.

- [ ] **Step 4: Run — expect PASS**, plus the full gate suite.

- [ ] **Step 5: Commit** `git commit -m "feat(txn): size-gate coordinator reads and defer full-cell fetch\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"`

---

## Phase 3: Pin lifecycle (release + backstop)

### Task 3.1: Drop pins on end/abort

**Files:**
- Modify: `src/server/transactions/data_site.rs` — `end` (3576+), `abort` (3499+), and `cell_meta_cleanup`/stale wipe.

- [ ] **Step 1: Write the failing test** — a read-write transaction pins a large cell, commits/aborts; assert the participant transaction's `pinned_reads` is empty afterward and the segment reference count returned to its pre-pin value.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** — in `end` and `abort`, `drain()` the transaction's `pinned_reads` (dropping the guards). Confirm `wipe_out_transaction` also drops any residual pins for stale transactions.

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/data_site.rs
git commit -m "feat(txn): release read pins on end/abort and stale cleanup"
```

### Task 3.2: Read-only pin release RPC + coordinator call

**Files:**
- Modify: `src/server/transactions/data_site.rs` — add `rpc release_read_pins(tid: TxnId) -> DataSiteResponse<()>` to `service!` (line 237) and a handler that drains that transaction's pins. Model the handler on `end`'s participant lookup.
- Modify: `src/server/transactions/manager.rs` — read-only commit/abort path calls `release_read_pins` on each participant that holds pins for the transaction (track which participants were pinned in the coordinator transaction state).

- [ ] **Step 1: Write the failing test** — a read-only transaction pins a large cell across a distinct participant, then commits; assert the participant's pins are released and no `prepare` was issued (property 13 preserved). Also test that if the release is skipped, stale-cleanup eventually reclaims the pin.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement** the RPC + handler + coordinator call. The release is not a prepare; read-only transactions that pinned nothing send nothing.

- [ ] **Step 4: Run — expect PASS**, plus the full gate suite.

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/data_site.rs src/server/transactions/manager.rs
git commit -m "feat(txn): release read-only pins via lightweight RPC with stale backstop"
```

---

## Phase 4: Correctness tests

### Task 4.1: Repeatability under concurrent transactional + non-transactional overwrite + cleaner

**Files:**
- Test: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Write the tests** (each `#[tokio::test(flavor = "multi_thread")]`):
  1. Large-cell transaction reads header/selected/full; a concurrent transaction updates the cell to a new version; all reads in the first transaction stay consistent with its pinned version.
  2. Same, but the concurrent overwrite is a **non-transactional** `update_cell` and a cleaner pass is forced; assert the pinned version is neither mutated nor reclaimed and the first transaction still reads it.
  3. Repeatable absence: a pinned large cell is removed by a concurrent transaction; the pinning transaction still reads its pre-remove snapshot.
  4. Certification still aborts: a read-write transaction reads a large cell, the cell advances, and prepare rejects it.

- [ ] **Step 2-4: Run each — they must PASS on the implemented code** (they are the acceptance criteria; if any fails, fix the implementation, not the test).

- [ ] **Step 5: Commit**

```bash
git add src/server/transactions/occ_tests.rs
git commit -m "test(txn): repeatable-read pinning correctness under concurrency"
```

---

## Phase 5: Benchmark + acceptance

### Task 5.1: Measure against baseline

- [ ] **Step 1:** On the dedicated host, NUMA-pinned, save an `occ-initial`-equivalent baseline for the current branch HEAD before this work (or reuse the existing saved baseline), then run the `projected_reads` group with this branch:

```
NEB_OCC_BENCH_LABEL=read-pin-candidate \
  numactl --cpunodebind=0 --membind=0 \
  cargo bench --bench occ_transactions -- 'occ/projected_reads/'
```

- [ ] **Step 2:** Confirm acceptance: `projected_reads` head/selected/mixed improves throughput or p95 by >=5% at CV <=5%; no secondary scenario regresses >3% throughput or >5% p95; all invariants pass and unexpected lists are empty. Repeat noisy scenarios up to three times per the OCC loop rules.

- [ ] **Step 3:** Record the before/after table in a results note and in the accepted commit message body, matching the OCC optimization-loop convention.

- [ ] **Step 4: Commit** the results note only (no code):

```bash
git add docs/superpowers/specs/2026-07-23-occ-repeatable-read-snapshot-pinning-design.md
git commit -m "docs: record read-pin benchmark results"
```

---

## Self-Review Notes

- Every spec section maps to a phase: size gate → 0.1; participant pin-set + serve-from-pin → 1.x; coordinator defer + small-result cache → 2.x; read-only lifecycle → 3.x; correctness (CoW immutability, non-txn overwrite, cleaner, certification) → 4.1; acceptance → 5.1.
- Two storage helpers (`cell_stored_len`, `read_cell_at`/`head_at`/`read_selected_at`) are grounded by reference to the verified `update_cell_by` pattern (`Entry::decode_from` at chunk.rs:848, `SharedCellData::from_chunk_raw` at chunk.rs:839); confirm the exact `from_chunk_raw` signature (`src/ram/cell.rs:343`) when implementing Task 1.2/1.3.
- Certification is never modified; `CellExpectation::Present(version)` recording is preserved in Task 2.2.
- Property 13 is preserved: only pinning read-only transactions send a release, which is not a prepare.
