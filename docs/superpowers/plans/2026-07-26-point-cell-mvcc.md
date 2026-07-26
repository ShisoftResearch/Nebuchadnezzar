# Point-Cell MVCC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded point-cell MVCC snapshots while retaining full-read OCC
validation, preventing lost updates and point-read write skew, and preserving
the existing distributed prepare/commit/end protocol.

**Architecture:** Replace the persisted cell version and cached wall-clock
fields with one `revision_ts: u64`, then add a chunk-owned
`LinkedRingBufferList` revision chain whose nodes point to immutable cell or
tombstone entries. Transactions resolve `TxnId.ts` snapshots through those
chains, writers certify every point observation, participants install one
shared commit timestamp as pending, and `end` promotes it before releasing
ownership. Cleaner relocation changes node addresses by CAS; recovery restores
only current revisions and seeds the HLC before compensating incomplete writes.

**Tech Stack:** Rust 2021, Bifrost HLC, Lightning
`LinkedRingBufferList`/`PtrHashMap`, atomics, parking_lot, Tokio RPC, Criterion,
append-only segments, WAL and undo logging.

## Global Constraints

- Point cells only; index, range, predicate, and phantom semantics remain
  outside the MVCC isolation guarantee.
- A revision is visible exactly when `revision_ts < TxnId.ts`; equality is
  invisible.
- Every point read made by a writing transaction is automatically certified.
- Read-only transactions do not run distributed prepare.
- Every cell changed by one distributed transaction uses the same
  `commit_hlc.ts`.
- Distinct logical revisions of one cell have strictly increasing timestamps.
- Cell headers and tombstone payloads remain exactly 32 bytes.
- `version`, `__header.ver`, `CellVersionMismatch`, and compare-version APIs are
  removed without compatibility aliases.
- Existing storage directories, WAL, undo logs, RPC clients, and binaries are
  not backward compatible.
- Non-transactional operations remain functional but have no isolation or
  safety guarantee relative to transactional operations.
- History is volatile across restart and retained for a tunable five minutes by
  default (`300_000` milliseconds).
- Transactions retain revision identity or owned results, never raw cell
  addresses or transaction-lifetime segment guards.
- A reproducible regression greater than 5% on a non-historical hot path must
  be investigated, replaced, or explicitly approved.
- Accept-grade performance runs use `192.168.10.87`; local runs are acceptable
  for workloads that do not need substantial RAM.
- No distributed transaction phase may be removed.

## File and Responsibility Map

- `../bifrost/src/hlc.rs` — checked HLC advancement and explicit exhaustion.
- `src/ram/cell.rs` — 32-byte revision header, encoding, decoding, and revision
  mismatch errors.
- `src/ram/tombstone.rs` — 32-byte revision tombstone.
- `src/ram/history.rs` — chunk-owned revision nodes, version chains, snapshot
  selection, expiration queue, and dead-location notifications.
- `src/ram/chunk.rs` — revision allocation, mutation publication, snapshot
  materialization, chain ownership, and current-index mirroring.
- `src/ram/segs.rs` — fallible short shared leases and exclusive cleaner
  acquisition.
- `src/ram/cleaner/{mod.rs,combine.rs}` — retain and relocate all live
  `(CellId, revision_ts)` entries.
- `src/ram/recovery.rs` — timestamp ordering, identical cleaner-copy handling,
  current-chain reconstruction, recovery maximum, and snapshot floor.
- `src/server/mod.rs` — runtime HLC injection and retention configuration.
- `src/server/transactions/mod.rs` — revision-aware expectations, observed-read
  RPC payloads, commit timestamp payloads, and resolution types.
- `src/server/transactions/manager.rs` — fixed snapshots, read dependency
  retention, shared commit timestamp allocation, and coordinator decisions.
- `src/server/transactions/data_site.rs` — snapshot reads, OCC certification,
  pending installs, promotion, compensation, and stale-owner resolution.
- `src/server/transactions/undo_log.rs` — new revision-based durable undo
  format and compensating recovery.
- `src/client/transaction.rs` — `SnapshotTooOld` propagation without changing
  the point-read `Option` convenience API.
- `src/exec/adapter/mod.rs` — `__header.ts: U64(revision_ts)` and removal of
  `__header.ver`.
- `src/index/hash/mod.rs`, `src/server/cell_rpc.rs`, and callers — direct rename
  from version-based conditional operations to revision-based operations.
- `src/ram/tests/*.rs`, `src/server/transactions/{tests,occ_tests}.rs`, and
  inline module tests — correctness and race coverage.
- `benches/occ_transactions.rs`, `benches/occ_support/*.rs`, and
  `benches/README.md` — current, historical, retention, cleaner, HLC, and
  distributed benchmark coverage.

---

### Task 1: Make Bifrost HLC Advancement Checked

**Files:**

- Modify: `../bifrost/src/hlc.rs`

**Interfaces:**

- Produces:
  `HlcSource::try_now() -> Result<Hlc, HlcError>` and
  `HlcSource::try_observe(Hlc) -> Result<Hlc, HlcError>`.
- Preserves: `now()` and `observe()` as checked, fail-fast convenience methods
  for existing non-write call sites; correctness-sensitive Nebuchadnezzar write
  paths switch to the fallible methods in later tasks.

- [ ] **Step 1: Write failing overflow tests**

Add these tests inside `../bifrost/src/hlc.rs`:

```rust
#[test]
fn checked_advance_refuses_local_wrap() {
    let source = HlcSource::new(7);
    source.ts.store(u64::MAX, Ordering::Relaxed);

    assert_eq!(source.try_now(), Err(HlcError::Exhausted));
    assert_eq!(source.ts.load(Ordering::Relaxed), u64::MAX);
}

#[test]
fn checked_observe_refuses_remote_wrap() {
    let source = HlcSource::new(7);

    assert_eq!(
        source.try_observe(Hlc {
            ts: u64::MAX,
            node: 9,
        }),
        Err(HlcError::Exhausted)
    );
    assert_eq!(source.ts.load(Ordering::Relaxed), 0);
}
```

- [ ] **Step 2: Run the tests and verify the missing API failure**

Run:

```bash
cargo test --manifest-path ../bifrost/Cargo.toml hlc::tests::checked_ -- --nocapture
```

Expected: compilation fails because `HlcError`, `try_now`, and `try_observe`
do not exist.

- [ ] **Step 3: Implement checked advancement**

Replace unchecked arithmetic in `HlcSource::advance` with:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlcError {
    Exhausted,
}

impl HlcSource {
    fn advance_checked(&self, floor: u64) -> Result<Hlc, HlcError> {
        let phys = Self::packed_phys_ms_checked()?;
        let floor_next = floor.checked_add(1).ok_or(HlcError::Exhausted)?;
        let mut current = self.ts.load(Ordering::Relaxed);
        loop {
            let local_next = current.checked_add(1).ok_or(HlcError::Exhausted)?;
            let next = local_next.max(floor_next).max(phys);
            match self.ts.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(Hlc {
                        ts: next,
                        node: self.node,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn packed_phys_ms_checked() -> Result<u64, HlcError> {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        if ms > (u64::MAX >> LOGICAL_BITS) {
            return Err(HlcError::Exhausted);
        }
        Ok(ms << LOGICAL_BITS)
    }

    pub fn try_now(&self) -> Result<Hlc, HlcError> {
        self.advance_checked(0)
    }

    pub fn try_observe(&self, remote: Hlc) -> Result<Hlc, HlcError> {
        self.advance_checked(remote.ts)
    }

    pub fn now(&self) -> Hlc {
        self.try_now().expect("HLC timestamp space exhausted")
    }

    pub fn observe(&self, remote: Hlc) -> Hlc {
        self.try_observe(remote)
            .expect("HLC timestamp space exhausted")
    }
}
```

- [ ] **Step 4: Run Bifrost HLC tests**

Run:

```bash
cargo test --manifest-path ../bifrost/Cargo.toml hlc::tests -- --nocapture
```

Expected: all HLC tests pass, including both exhaustion tests.

- [ ] **Step 5: Commit the Bifrost change**

```bash
git -C ../bifrost add src/hlc.rs
git -C ../bifrost commit -m "fix(hlc): refuse timestamp overflow"
```

### Task 2: Replace Persisted Versions with Revision Timestamps

**Files:**

- Modify: `src/ram/cell.rs`
- Modify: `src/ram/tombstone.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/recovery.rs`
- Modify: `src/ram/cleaner/combine.rs`
- Modify: `src/server/mod.rs`
- Modify: `src/server/transactions/mod.rs`
- Modify: `src/server/transactions/{data_site,manager,undo_log}.rs`
- Modify: `src/server/cell_rpc.rs`
- Modify: `src/index/hash/mod.rs`
- Modify: `src/exec/adapter/mod.rs`
- Modify: all Rust tests containing `header.version`, `header.timestamp`,
  `CellVersionMismatch`, or `compare_version`

**Interfaces:**

- Produces:
  `CellHeader { revision_ts, flags, schema, partition, hash }`.
- Produces:
  `Tombstone { segment_seq_id, revision_ts, partition, hash }`.
- Produces:
  `Chunks::next_revision_ts(previous: u64) -> Result<u64, WriteError>`.
- Produces:
  `compare_revision_and_update_cell` and
  `compare_revision_and_set_field`.

- [ ] **Step 1: Add layout and monotonic-allocation tests**

Add to the inline cell tests in `src/ram/cell.rs`:

```rust
#[test]
fn revision_header_is_exactly_32_bytes() {
    assert_eq!(std::mem::size_of::<CellHeader>(), 32);
    let id = Id::new(11, 22);
    let header = CellHeader::new(7, &id);
    assert_eq!(header.revision_ts, 0);
    assert_eq!(header.flags, 0);
    assert_eq!(header.schema, 7);
    assert_eq!(header.id(), id);
}
```

Add to `src/ram/tombstone.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_tombstone_is_exactly_32_bytes() {
        assert_eq!(TOMBSTONE_SIZE, 32);
        assert_eq!(std::mem::size_of::<Tombstone>(), 32);
    }
}
```

Add a chunk test in `src/ram/tests/chunk.rs` that creates a chunk with a shared
test HLC, writes twice, and asserts `second.revision_ts > first.revision_ts`.

- [ ] **Step 2: Run the layout tests and verify failure**

Run:

```bash
cargo test --lib ram::cell::tests::revision_header_is_exactly_32_bytes
cargo test --lib ram::tombstone::tests::revision_tombstone_is_exactly_32_bytes
```

Expected: compilation fails because the revision fields do not exist.

- [ ] **Step 3: Replace the header and tombstone layouts**

Use these definitions and serialization order:

```rust
#[repr(C)]
#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default)]
pub struct CellHeader {
    pub revision_ts: u64,
    pub flags: u32,
    pub schema: u32,
    pub partition: u64,
    pub hash: u64,
}

impl CellHeader {
    pub fn new(schema: u32, id: &Id) -> Self {
        Self {
            revision_ts: 0,
            flags: 0,
            schema,
            partition: id.higher,
            hash: id.lower,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Tombstone {
    pub segment_seq_id: u64,
    pub revision_ts: u64,
    pub partition: u64,
    pub hash: u64,
}
```

Encode cell fields at offsets `0, 8, 12, 16, 24`. Rename
`cell_version_from_*` helpers to `cell_revision_ts_from_*`. Change
`minimal_header_from_chunk_raw` to load `schema` from offset `12`.

- [ ] **Step 4: Inject the server HLC and expose a tunable retention value**

Add to `ServerOptions`:

```rust
pub history_retention_ms: u64,
```

Use `300_000` in every in-repository `ServerOptions` literal. Add a
`revision_clock: Arc<HlcSource>` field to `Chunks` and `Chunk`. Add an explicit
server constructor:

```rust
pub fn new_with_recovery_and_clock(
    count: usize,
    size: usize,
    meta: Arc<ServerMeta>,
    index_builder: Option<Arc<IndexBuilder>>,
    backup_storage: Option<String>,
    wal_storage: Option<String>,
    tiered_manager: Option<Arc<TieredMemoryManager>>,
    enable_recovery: bool,
    raft_storage: Option<String>,
    revision_clock: Arc<HlcSource>,
    history_retention_ms: u64,
) -> Arc<Chunks>
```

Keep test constructors concise by delegating `Chunks::new` and
`Chunks::new_with_recovery` to this constructor with
`Arc::new(HlcSource::new(0))` and `300_000`. The server must call the explicit
constructor with its process-wide `hlc.clone()` and
`effective_opts.history_retention_ms`.

Implement allocation as:

```rust
fn next_revision_ts(&self, previous: u64) -> Result<u64, WriteError> {
    let next = self
        .revision_clock
        .try_now()
        .map_err(|_| WriteError::RevisionClockExhausted)?
        .ts;
    if next <= previous {
        return Err(WriteError::RevisionClockExhausted);
    }
    Ok(next)
}
```

- [ ] **Step 5: Convert write encoding and conditional APIs**

Replace `WriteToChunkResult` with:

```rust
pub struct WriteToChunkResult {
    pub revision_ts: u64,
    pub addr: usize,
}
```

Make `OwnedCell::write_to_chunk_with` accept an assigned `revision_ts: u64`
and write it unchanged. Direct insert/update/remove obtains a timestamp through
`next_revision_ts`; later transactional tasks use the assigned-revision
variants.

Rename the error and methods:

```rust
CellRevisionMismatch,

pub fn compare_revision_and_update_cell(
    &self,
    hash: u64,
    revision_ts: u64,
    cell: &mut OwnedCell,
) -> Result<CellHeader, WriteError>

pub fn compare_revision_and_set_field(
    &self,
    hash: u64,
    revision_ts: u64,
    field: u64,
    value: OwnedValue,
) -> Result<CellHeader, WriteError>
```

Update cell RPC, hash-index callers, transaction structures, undo fields, and
tests directly; do not add aliases.

- [ ] **Step 6: Convert recovery, cleaner, and query metadata mechanically**

Recovery and cleaner must compare `revision_ts`. At this task boundary recovery
may retain its existing `>=` tie behavior; Task 6 replaces it with the complete
identical-copy rule.

Replace query metadata construction with:

```rust
header_map.insert("ts", OwnedValue::U64(header.revision_ts));
header_map.insert("sch", OwnedValue::U32(header.schema));
```

and:

```rust
header_map.insert("ts", SharedValue::U64(&header.revision_ts));
header_map.insert("sch", SharedValue::U32(&header.schema));
```

Do not insert `"ver"`.

- [ ] **Step 7: Prove the version API is gone**

Run:

```bash
rg -n "header\\.version|header\\.timestamp|CellVersionMismatch|compare_version|cell_version_from|__header\\.ver" src benches
```

Expected: no matches.

Run:

```bash
cargo check --lib
cargo test --lib ram::tests::cell -- --test-threads=1
cargo test --lib ram::tests::chunk -- --test-threads=1
```

Expected: all commands pass.

- [ ] **Step 8: Commit the revision-layout migration**

```bash
git add src benches
git commit -m "refactor(storage): replace cell versions with revision timestamps"
```

### Task 3: Add the Chunk-Owned Revision Chain

**Files:**

- Create: `src/ram/history.rs`
- Modify: `src/ram/mod.rs`
- Modify: `src/ram/chunk.rs`

**Interfaces:**

- Produces: `RevisionNode`, `RevisionChain`, `HistoryIndex`,
  `RevisionState`, `SnapshotRevision`, and `DeadRevision`.
- Consumes: aligned immutable entry addresses and `history_retention_ms`.

- [ ] **Step 1: Write failing chain-order, visibility, and expiration tests**

Put unit tests in `src/ram/history.rs`:

```rust
fn node(
    revision_ts: u64,
    state: RevisionState,
    location: usize,
) -> Arc<RevisionNode> {
    Arc::new(RevisionNode::new(revision_ts, state, location, 64))
}

#[test]
fn strict_snapshot_selects_newest_committed_revision_below_boundary() {
    let chain = RevisionChain::new();
    chain.push_front(node(300, RevisionState::CommittedPresent, 0x3000));
    chain.push_front(node(400, RevisionState::CommittedPresent, 0x4000));

    assert_eq!(chain.resolve(400).revision_ts(), Some(300));
    assert_eq!(chain.resolve(401).revision_ts(), Some(400));
}

#[test]
fn pending_head_makes_first_read_wait() {
    let chain = RevisionChain::new();
    chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
    chain.push_front(node(200, RevisionState::PendingPresent, 0x2000));

    assert!(matches!(chain.resolve(300), SnapshotRevision::Wait));
}

#[test]
fn aborted_revision_is_never_selected() {
    let chain = RevisionChain::new();
    chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
    chain.push_front(node(200, RevisionState::Aborted, 0x2000));

    assert_eq!(chain.resolve(250).revision_ts(), Some(100));
}

#[test]
fn pruned_suffix_reports_snapshot_too_old() {
    let chain = RevisionChain::new();
    chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
    chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
    chain.expire_oldest_for_test();

    assert!(matches!(chain.resolve(150), SnapshotRevision::TooOld));
    assert_eq!(chain.resolve(201).revision_ts(), Some(200));
}
```

The local `node` helper must create aligned addresses and `entry_size: 64`.

- [ ] **Step 2: Run the history tests and verify module failure**

Run:

```bash
cargo test --lib ram::history::tests -- --nocapture
```

Expected: compilation fails because `ram::history` does not exist.

- [ ] **Step 3: Implement atomic node state and location**

Use the low three bits of aligned entry addresses:

```rust
const STATE_MASK: usize = 0b111;
const LOCATION_MASK: usize = !STATE_MASK;

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionState {
    PendingPresent = 0,
    PendingDeleted = 1,
    CommittedPresent = 2,
    CommittedDeleted = 3,
    Aborted = 4,
    Expired = 5,
}

pub struct RevisionNode {
    pub revision_ts: u64,
    state_and_location: AtomicUsize,
    pub entry_size: u32,
    retire_deadline_ms: AtomicU64,
}

impl RevisionNode {
    pub fn new(
        revision_ts: u64,
        state: RevisionState,
        location: usize,
        entry_size: u32,
    ) -> Self {
        assert_eq!(location & STATE_MASK, 0);
        Self {
            revision_ts,
            state_and_location: AtomicUsize::new(location | state as usize),
            entry_size,
            retire_deadline_ms: AtomicU64::new(0),
        }
    }

    pub fn load(&self) -> (RevisionState, usize) {
        let raw = self.state_and_location.load(Ordering::Acquire);
        (
            RevisionState::from_tag(raw & STATE_MASK),
            raw & LOCATION_MASK,
        )
    }
}

impl RevisionState {
    fn from_tag(tag: usize) -> Self {
        match tag {
            0 => Self::PendingPresent,
            1 => Self::PendingDeleted,
            2 => Self::CommittedPresent,
            3 => Self::CommittedDeleted,
            4 => Self::Aborted,
            5 => Self::Expired,
            invalid => panic!("invalid revision state tag: {invalid}"),
        }
    }
}
```

Tags `6` and `7` therefore fail closed as corrupted in-memory metadata.

- [ ] **Step 4: Implement the newest-to-oldest chain**

Use:

```rust
pub struct RevisionChain {
    revisions: LinkedRingBufferList<Option<Arc<RevisionNode>>, 32>,
    truncated_before_ts: AtomicU64,
}

pub enum SnapshotRevision {
    Present(Arc<RevisionNode>),
    Deleted(Arc<RevisionNode>),
    NeverExisted,
    Wait,
    TooOld,
}

impl RevisionChain {
    pub fn new() -> Self {
        Self {
            revisions: LinkedRingBufferList::new(),
            truncated_before_ts: AtomicU64::new(0),
        }
    }

    pub fn push_front(&self, node: Arc<RevisionNode>) {
        self.revisions.push_front(Some(node));
    }
}

impl SnapshotRevision {
    #[cfg(test)]
    fn revision_ts(&self) -> Option<u64> {
        match self {
            Self::Present(node) | Self::Deleted(node) => Some(node.revision_ts),
            Self::NeverExisted | Self::Wait | Self::TooOld => None,
        }
    }
}
```

`resolve(snapshot_ts)` iterates from the front. It returns `Wait` upon an
unresolved pending node, skips aborted and expired nodes, and selects the first
committed node whose `revision_ts < snapshot_ts`. If no committed node matches
and `snapshot_ts <= truncated_before_ts`, return `TooOld`; otherwise return
`NeverExisted`.

- [ ] **Step 5: Implement per-chunk ownership and expiration**

Use:

```rust
pub struct HistoryIndex {
    chains: PtrHashMap<Id, Arc<RevisionChain>>,
    expirations: LinkedRingBufferList<Option<ExpirationRecord>, 64>,
    dead: LinkedRingBufferList<Option<DeadRevision>, 64>,
    retention_ms: u64,
    recovery_floor: AtomicU64,
    stopped: AtomicBool,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[derive(Clone)]
struct ExpirationRecord {
    chain: Arc<RevisionChain>,
    node: Arc<RevisionNode>,
    deadline_ms: u64,
}

#[derive(Clone, Default)]
pub struct DeadRevision {
    pub location: usize,
    pub entry_size: u32,
}
```

Construct `HistoryIndex` in `Arc`, start one weak-reference worker per chunk,
and use a process-monotonic `OnceLock<Instant>` millisecond counter. Retirement
pushes an expiration record; the current node is never retired. The worker
marks due nodes expired, pushes their current location to `dead`, pops only
contiguous expired/aborted list suffixes, and advances
`truncated_before_ts` to the oldest remaining revision timestamp.

`Chunk::drain_history_dead` locates each segment and calls
`mark_dead_entry_with_size`. Cleaner invokes it before selecting segments.

- [ ] **Step 6: Run history unit tests and chunk construction tests**

Run:

```bash
cargo test --lib ram::history::tests -- --test-threads=1
cargo test --lib ram::tests::chunk -- --test-threads=1
```

Expected: all tests pass and every constructed chunk owns exactly one running
history worker.

- [ ] **Step 7: Commit the history core**

```bash
git add src/ram/history.rs src/ram/mod.rs src/ram/chunk.rs
git commit -m "feat(mvcc): add chunk-owned revision chains"
```

### Task 4: Publish Immutable Revisions and Resolve Point Snapshots

**Files:**

- Modify: `src/ram/history.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/cell.rs`
- Modify: `src/ram/tombstone.rs`
- Modify: `src/ram/segs.rs`
- Modify: `src/ram/tests/cell.rs`
- Modify: `src/ram/tests/chunk.rs`

**Interfaces:**

- Produces:
  `RevisionWrite { revision_ts, visibility }`,
  `InstalledRevision`, and `SnapshotRead<T>`.
- Produces:
  `write_cell_at_revision`, `update_cell_at_revision`,
  `remove_cell_at_revision`, `promote_revision`, and `abort_revision`.
- Produces snapshot methods for full, selected, header, and partial point reads.

- [ ] **Step 1: Write failing storage MVCC tests**

Add tests that use deterministic assigned timestamps:

```rust
#[test]
fn snapshot_reads_old_address_after_current_update() {
    let chunks = test_chunks();
    let id = Id::new(1, 91);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(
            &mut first,
            RevisionWrite::committed(100),
        )
        .unwrap();
    let old_address = chunks.address_of(&id);

    let mut second = test_cell(id, 20);
    chunks
        .update_cell_at_revision(
            &mut second,
            RevisionWrite::committed(200),
        )
        .unwrap();

    let selected = chunks.read_cell_snapshot(&id, 150).unwrap();
    let SnapshotRead::Present(cell) = selected else {
        panic!("snapshot should select the old cell");
    };
    assert_eq!(cell.header.revision_ts, 100);
    assert_eq!(chunks.history_location(&id, 100), Some(old_address));
}

#[test]
fn delete_and_recreate_preserve_revision_aware_absence() {
    let chunks = test_chunks();
    let id = Id::new(1, 92);
    let mut first = test_cell(id, 10);
    chunks
        .write_cell_at_revision(&mut first, RevisionWrite::committed(100))
        .unwrap();
    chunks
        .remove_cell_at_revision(&id, RevisionWrite::committed(200))
        .unwrap();

    assert!(matches!(
        chunks.read_cell_snapshot(&id, 250).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));

    let mut second = test_cell(id, 30);
    chunks
        .write_cell_at_revision(&mut second, RevisionWrite::committed(300))
        .unwrap();
    assert!(matches!(
        chunks.read_cell_snapshot(&id, 250).unwrap(),
        SnapshotRead::Absent(Some(200))
    ));
}
```

- [ ] **Step 2: Run the tests and verify missing mutation APIs**

Run:

```bash
cargo test --lib ram::tests::cell::snapshot_reads_old_address_after_current_update
cargo test --lib ram::tests::cell::delete_and_recreate_preserve_revision_aware_absence
```

Expected: compilation fails because the assigned-revision and snapshot APIs do
not exist.

- [ ] **Step 3: Add explicit mutation and installation types**

Use:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallVisibility {
    Pending,
    Committed,
}

#[derive(Debug, Clone, Copy)]
pub struct RevisionWrite {
    pub revision_ts: u64,
    pub visibility: InstallVisibility,
}

impl RevisionWrite {
    pub fn pending(revision_ts: u64) -> Self {
        Self {
            revision_ts,
            visibility: InstallVisibility::Pending,
        }
    }

    pub fn committed(revision_ts: u64) -> Self {
        Self {
            revision_ts,
            visibility: InstallVisibility::Committed,
        }
    }
}

#[derive(Clone)]
pub struct InstalledRevision {
    pub id: Id,
    pub node: Arc<RevisionNode>,
}

pub enum SnapshotRead<T> {
    Present(T),
    Absent(Option<u64>),
    Wait,
}
```

- [ ] **Step 4: Publish the predecessor and new head under the cell lock**

For update and delete:

1. Read the current header and address while holding the existing cell-index
   guard.
2. Reject `revision_ts <= current.revision_ts`.
3. Ensure the current revision has a chain node.
4. Write the new immutable cell or tombstone.
5. Push its node to the front using pending or committed state.
6. Update or remove the cell-index mirror.
7. Retire the predecessor only after the new node is installed.
8. Stop calling `mark_dead_entry_with_cell` for retained predecessors.

For insert, certify that the current logical history state is deleted or never
existed before publishing. A current tombstone remains the chain head and is
not expired.

`promote_revision` atomically transitions pending-present/deleted to the
corresponding committed state. `abort_revision` transitions pending to aborted
without changing its timestamp.

- [ ] **Step 5: Add snapshot materialization with short leases**

First add the fallible lease used by historical reads:

```rust
impl SegmentReferenceGuard {
    pub fn try_new(segment: lightning::aarc::Arc<Segment>) -> Option<Self> {
        if !segment.incr_references() {
            return None;
        }
        Some(Self { segment })
    }

    pub fn new(segment: lightning::aarc::Arc<Segment>) -> Self {
        Self::try_new(segment).expect("segment is exclusively referenced")
    }
}
```

Implement:

```rust
pub fn read_cell_snapshot(
    &self,
    key: &Id,
    snapshot_ts: u64,
) -> Result<SnapshotRead<OwnedCell>, ReadError>

pub fn read_selected_snapshot(
    &self,
    key: &Id,
    snapshot_ts: u64,
    fields: &[u64],
) -> Result<SnapshotRead<OwnedCell>, ReadError>

pub fn head_snapshot(
    &self,
    key: &Id,
    snapshot_ts: u64,
) -> Result<SnapshotRead<CellHeader>, ReadError>

pub fn read_partial_raw_snapshot(
    &self,
    key: &Id,
    snapshot_ts: u64,
    offset: usize,
    len: usize,
) -> Result<SnapshotRead<Vec<u8>>, ReadError>
```

Use the existing cell-index path when a current present head is directly
visible. Historical reads load the node address, call the fallible short shared
lease, recheck state and address, materialize, and drop the lease.
They retry when cleaner relocation wins. Map `SnapshotRevision::TooOld` to
`ReadError::SnapshotTooOld`.

- [ ] **Step 6: Test every point read shape and pending state**

Add assertions that full, selected, header, and partial reads select revision
`100` after revision `200` becomes current. Add:

```rust
assert!(matches!(
    chunks.read_cell_snapshot(&id, 300).unwrap(),
    SnapshotRead::Wait
));
chunks.promote_revision(&installed).unwrap();
assert!(matches!(
    chunks.read_cell_snapshot(&id, 300).unwrap(),
    SnapshotRead::Present(_)
));
```

Run:

```bash
cargo test --lib ram::tests::cell -- --test-threads=1
cargo test --lib ram::tests::chunk -- --test-threads=1
```

Expected: all point snapshot, direct mutation, and legacy functional tests pass.

- [ ] **Step 7: Commit immutable publication and snapshot reads**

```bash
git add src/ram
git commit -m "feat(mvcc): publish and read immutable point revisions"
```

### Task 5: Make Cleaner Relocation History-Safe

**Files:**

- Modify: `src/ram/segs.rs`
- Modify: `src/ram/cleaner/mod.rs`
- Modify: `src/ram/cleaner/combine.rs`
- Modify: `src/ram/cleaner/tests.rs`
- Modify: `src/ram/history.rs`
- Modify: `src/ram/chunk.rs`

**Interfaces:**

- Consumes:
  `SegmentReferenceGuard::try_new(AArc<Segment>) -> Option<Self>` from Task 4.
- Produces:
  `HistoryIndex::is_live_at(Id, revision_ts, addr)` and
  `HistoryIndex::relocate(Id, revision_ts, old, new)`.
- Changes `SegmentCandidate` to own exclusive reference state.

- [ ] **Step 1: Write failing lease and relocation race tests**

Add a segment regression test for the Task 4 lease:

```rust
#[test]
fn shared_reference_fails_while_exclusive_guard_is_held() {
    let segment = test_segment();
    let exclusive = SegmentExclusiveRefGuard::new(&segment).unwrap();

    assert!(SegmentReferenceGuard::try_new(segment.clone()).is_none());
    drop(exclusive);
    assert!(SegmentReferenceGuard::try_new(segment).is_some());
}
```

Add cleaner tests that create two retained revisions, combine the source
segments, and assert both nodes point to readable destination addresses. Add a
race test in which a historical reader either completes on the source or
retries on the destination, never dereferencing a freed segment.

- [ ] **Step 2: Run focused tests and verify failure**

Run:

```bash
cargo test --lib ram::cleaner::tests -- --test-threads=1
cargo test --lib ram::segs::tests::shared_reference_fails_while_exclusive_guard_is_held
```

Expected: the historical cleaner relocation tests fail; the lease regression
test passes.

- [ ] **Step 3: Make `SegmentCandidate` truly exclusive**

Replace ordinary reference increment/decrement with:

```rust
impl SegmentCandidate {
    pub fn new(segment: &lightning::aarc::Arc<Segment>) -> Option<Self> {
        if !segment.obtain_exclusive_references() {
            return None;
        }
        if !segment.lock_hot() {
            segment.release_exclusive_references();
            return None;
        }
        Some(Self {
            segment: segment.clone(),
        })
    }
}

impl Drop for SegmentCandidate {
    fn drop(&mut self) {
        self.segment.set_hot();
        self.segment.release_exclusive_references();
    }
}
```

Remove the racy `no_references()` precondition from cleaner selection; failed
exclusive acquisition already skips the segment.

- [ ] **Step 4: Retain and relocate logical revisions**

Change cleaner identity from cell hash to:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct RevisionKey {
    id: Id,
    revision_ts: u64,
}
```

Collect a cell or tombstone only when it is either the current cell-index target
or `history.is_live_at(key, entry_addr)`. Preserve all live keys, not only the
largest timestamp per hash. Sort by `(revision_ts, entry_size)` descending.

After copying, call:

```rust
match chunk
    .history
    .relocate(id, revision_ts, old_addr, new_addr)
{
    RelocateResult::HistoricalMoved => {}
    RelocateResult::CurrentPresentMoved => {
        chunk.compare_exchange_current_address(id.lower, old_addr, new_addr);
    }
    RelocateResult::LostRace => {
        chunk.mark_dead_entry_with_size(new_addr, entry_size, &new_segment);
    }
}
```

Tombstones receive relocation mappings as well as cells.

- [ ] **Step 5: Run cleaner, tiered, and race tests**

Run:

```bash
cargo test --lib ram::cleaner -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
cargo test --lib ram::history -- --test-threads=1
```

Expected: all pass; no test uses a transaction-lifetime segment guard.

- [ ] **Step 6: Commit cleaner integration**

```bash
git add src/ram
git commit -m "feat(mvcc): relocate retained revisions safely"
```

### Task 6: Recover Current Revisions and Seed the Clock

**Files:**

- Modify: `src/ram/recovery.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/ram/history.rs`
- Modify: `src/server/mod.rs`

**Interfaces:**

- Produces:
  `RecoverySummary { max_revision_ts: u64 }`.
- Produces:
  `Chunks::establish_recovery_floor() -> Result<u64, WriteError>`.
- Consumes the shared server HLC installed in Task 2.

- [ ] **Step 1: Write failing recovery-order tests**

Add recovery tests for:

```rust
#[test]
fn recovery_selects_largest_revision_across_cell_and_tombstone() {
    let recovered = recover_fixture(vec![
        stored_cell(100, "old"),
        stored_tombstone(200),
        stored_cell(300, "new"),
    ]);
    assert_eq!(recovered.current_revision_ts(), 300);
    assert_eq!(recovered.current_value(), Some("new"));
}

#[test]
fn recovery_accepts_identical_cleaner_copies() {
    let bytes = encoded_cell(200, "same");
    let recovered = recover_fixture(vec![bytes.clone(), bytes]);
    assert_eq!(recovered.current_revision_ts(), 200);
}

#[test]
fn recovery_rejects_conflicting_equal_timestamps() {
    let err = recover_fixture_result(vec![
        encoded_cell(200, "left"),
        encoded_cell(200, "right"),
    ])
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn recovery_starts_with_no_historical_coverage() {
    let chunks = recover_single_current(200);
    let floor = chunks.establish_recovery_floor().unwrap();
    assert!(floor > 200);
    assert_eq!(
        chunks.read_cell_snapshot(&test_id(), 150).unwrap_err(),
        ReadError::SnapshotTooOld
    );
}
```

- [ ] **Step 2: Run the tests and verify current recovery behavior fails**

Run:

```bash
cargo test --lib ram::recovery::tests::recovery_selects_largest_revision
cargo test --lib ram::recovery::tests::recovery_accepts_identical_cleaner_copies
cargo test --lib ram::recovery::tests::recovery_rejects_conflicting_equal_timestamps
cargo test --lib ram::recovery::tests::recovery_starts_with_no_historical_coverage
```

Expected: at least the duplicate-conflict and recovery-floor tests fail.

- [ ] **Step 3: Replace temporary version maps with sharded candidates**

Use:

```rust
#[derive(Clone, Copy)]
enum RecoveredKind {
    Present,
    Deleted,
}

#[derive(Clone, Copy)]
struct RecoveryCandidate {
    entry_addr: usize,
    entry_size: u32,
    revision_ts: u64,
    kind: RecoveredKind,
    segment_seq_id: u64,
}

struct RecoveryCandidates {
    shards: Vec<Mutex<HashMap<Id, RecoveryCandidate>>>,
    max_revision_ts: AtomicU64,
}
```

On a larger timestamp, replace the candidate. On an equal timestamp, require
the same kind, entry size, and byte-identical logical entry; then choose the
candidate with larger `(segment_seq_id, entry_addr)` for deterministic
recovery. Equal timestamp with different kind or bytes returns
`io::ErrorKind::InvalidData`.

- [ ] **Step 4: Rebuild only current history nodes**

After all scans complete:

- present candidate: populate `cell_index` and one committed-present chain node;
- deleted candidate: leave `cell_index` empty and create one committed-deleted
  chain node;
- every non-selected physical entry: account it as dead;
- do not link older physical entries as history.

Return:

```rust
pub struct RecoverySummary {
    pub max_revision_ts: u64,
}
```

- [ ] **Step 5: Advance HLC before undo and set the floor after undo**

Immediately after segment recovery:

```rust
chunks
    .revision_clock()
    .try_observe(Hlc {
        ts: recovery.max_revision_ts,
        node: chunks.revision_clock().node(),
    })
    .map_err(|_| recovery_clock_exhausted())?;
```

Run undo recovery next. Only after compensation completes:

```rust
let recovery_floor = chunks.establish_recovery_floor()?;
info!("MVCC recovery snapshot floor: {}", recovery_floor);
```

`establish_recovery_floor` obtains a fresh checked HLC and stores its `ts` into
every chunk history index. Reads with `snapshot_ts < recovery_floor` return
`SnapshotTooOld`.

- [ ] **Step 6: Run recovery suites**

Run:

```bash
cargo test --lib ram::recovery -- --test-threads=1
cargo test --lib server::transactions::undo_log -- --test-threads=1
```

Expected: recovery selects the latest revision, accepts exact cleaner copies,
rejects conflicts, seeds the clock, and exposes no pre-restart history.

- [ ] **Step 7: Commit recovery changes**

```bash
git add src/ram/recovery.rs src/ram/chunk.rs src/ram/history.rs src/server/mod.rs
git commit -m "feat(mvcc): recover current revisions and seed HLC"
```

### Task 7: Replace Transaction Read Pins with Snapshot Observations

**Files:**

- Modify: `src/server/transactions/mod.rs`
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/client/transaction.rs`
- Modify: `src/server/transactions/tests.rs`
- Modify: `src/server/transactions/occ_tests.rs`

**Interfaces:**

- Produces:
  `CellExpectation::Present(u64)` and
  `CellExpectation::Absent(Option<u64>)`.
- Produces:
  `ObservedPoint<T> { value: Option<T>, expectation: CellExpectation }`.
- Consumes the four storage snapshot read methods from Task 4.
- Removes `PinnedRead`, `PinnedReadSet`, pin release RPCs, and coordinator pin
  caches.

- [ ] **Step 1: Write failing fixed-snapshot and absence tests**

Add transaction tests:

```rust
#[tokio::test]
async fn transaction_reads_revision_older_than_current_head() {
    let fixture = transaction_fixture().await;
    let tid = fixture.begin_before_update().await;
    fixture.direct_update_after(tid.ts).await;

    let read = fixture.txn.read(tid, fixture.id).await.unwrap().unwrap();
    assert!(read.header.revision_ts < tid.ts);
    assert_eq!(fixture.participant_pin_count(tid), 0);
}

#[tokio::test]
async fn deleted_snapshot_carries_exact_tombstone_revision() {
    let fixture = transaction_fixture().await;
    let tid = fixture.begin_after_delete(200).await;

    assert!(fixture.txn.read(tid, fixture.id).await.unwrap().is_none());
    assert_eq!(
        fixture.coordinator_expectation(tid, fixture.id),
        CellExpectation::Absent(Some(200))
    );
}
```

Add a mixed-shape repeat test: selected, head, and full reads must all report
the same `revision_ts` after current advances. Add a read-your-writes case:

```rust
let tid = txn.begin().await.unwrap().unwrap();
let before = accepted_cell(txn.read(tid, id).await.unwrap().unwrap());
let mut buffered = before.clone();
buffered.data["score"] = OwnedValue::U64(99);
assert!(matches!(
    txn.update(tid, buffered).await.unwrap().unwrap(),
    TxnExecResult::Accepted(())
));
assert_eq!(
    score_of(&accepted_cell(txn.read(tid, id).await.unwrap().unwrap())),
    99
);
```

Also retain the existing assertion that a transaction with only point reads
clears its coordinator cache locally and creates no participant prepare state.

- [ ] **Step 2: Run the tests and verify pin/current-read behavior fails**

Run:

```bash
cargo test --lib server::transactions::occ_tests::transaction_reads_revision_older_than_current_head -- --test-threads=1
cargo test --lib server::transactions::occ_tests::deleted_snapshot_carries_exact_tombstone_revision -- --test-threads=1
```

Expected: the old path rejects the late read or loses the tombstone timestamp.

- [ ] **Step 3: Add revision-aware observed payloads**

Use:

```rust
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum CellExpectation {
    Present(u64),
    Absent(Option<u64>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObservedPoint<T> {
    pub value: Option<T>,
    pub expectation: CellExpectation,
}
```

Change participant point read RPC payloads to
`TxnExecResult<ObservedPoint<T>, ReadError>`. The transaction manager stores
`expectation` and translates `value: None` back into the existing public
`Option<T>` behavior.

- [ ] **Step 4: Make participant reads use `TxnId.ts`**

Replace `prepare_read` timestamp-order rejection with:

```rust
fn prepare_read<T: Send>(
    &self,
    clock: &Hlc,
    tid: &TxnId,
    id: &Id,
) -> Result<(), BoxFuture<'_, DataSiteResponse<TxnExecResult<T, ReadError>>>>
where
    T: 'static + Clone,
{
    self.update_clock(*clock);
    let meta_ref = self.cell_meta_mutex(id);
    let mut meta = meta_ref.lock();
    if meta.owner.is_some() {
        return Err(self.response_with(TxnExecResult::Wait));
    }
    if meta.read < *tid {
        meta.read = *tid;
    }
    Ok(())
}
```

After this ownership check, call the storage snapshot API with `tid.ts`. Map
`SnapshotRead::Wait` to `TxnExecResult::Wait`, present to
`Present(revision_ts)`, deleted to `Absent(Some(delete_ts))`, and never existed
to `Absent(None)`. Do not use `max(clock, tid)` for visibility.

- [ ] **Step 5: Delete transaction-lifetime pinning**

Remove:

- participant `PinnedRead`, `PinnedReadSet`, and `segment_guards` used for
  repeatable reads;
- coordinator `PinnedReadCache`, `pinned_servers`, release queue, and flusher;
- `release_read_pins` RPC;
- by-address transaction RPC logic.

Keep the storage by-address helpers private for history materialization and
cleaner tests.

- [ ] **Step 6: Propagate `SnapshotTooOld`**

Add `ReadError::SnapshotTooOld`. In `src/client/transaction.rs`, map it to the
existing `TxnError::ReadError(ReadError::SnapshotTooOld)`. Never translate it
to `None`, the current value, or `Rejected`.

- [ ] **Step 7: Run read-shape and no-pin suites**

Run:

```bash
cargo test --lib server::transactions::data_site -- --test-threads=1
cargo test --lib server::transactions::manager -- --test-threads=1
cargo test --lib server::transactions::occ_tests -- --test-threads=1
```

Expected: snapshot and repeated point reads pass; searches for `PinnedRead`,
`pinned_servers`, and `release_read_pins` return no matches.

- [ ] **Step 8: Commit snapshot transaction reads**

```bash
git add src/server/transactions src/client/transaction.rs
git commit -m "feat(mvcc): resolve transaction point reads by snapshot"
```

### Task 8: Certify Every Read and Install One Shared Pending Commit

**Files:**

- Modify: `src/server/transactions/mod.rs`
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/occ_tests.rs`

**Interfaces:**

- Produces explicit commit RPC input:
  `commit(commit_hlc: Hlc, tid: TxnId, cells: Vec<CommitOp>)`.
- Stores `InstalledRevision` handles in the participant transaction.
- Consumes `RevisionWrite::pending(commit_hlc.ts)` and promotion from Task 4.

- [ ] **Step 1: Write failing safety and timestamp tests**

Add:

```rust
#[tokio::test]
async fn full_read_validation_prevents_point_write_skew() {
    let fixture = two_cell_fixture(1, 1).await;
    let left = fixture.begin().await;
    let right = fixture.begin().await;

    fixture.read_both(left).await;
    fixture.read_both(right).await;
    fixture.set_first_zero(left).await;
    fixture.set_second_zero(right).await;

    let results = futures::future::join(
        fixture.prepare(left),
        fixture.prepare(right),
    )
    .await;
    assert_eq!(usize::from(results.0.is_ok()) + usize::from(results.1.is_ok()), 1);
}

#[tokio::test]
async fn all_participants_install_the_same_commit_timestamp() {
    let fixture = distributed_fixture().await;
    let tid = fixture.update_one_cell_per_server().await;
    fixture.prepare_and_commit(tid).await.unwrap();

    let revisions = fixture.current_revision_timestamps().await;
    assert!(!revisions.is_empty());
    assert!(revisions.iter().all(|ts| *ts == revisions[0]));
}

#[tokio::test]
async fn equal_commit_timestamp_is_invisible_to_snapshot() {
    let fixture = transaction_fixture().await;
    fixture.install_committed_revision(200).await;
    assert_eq!(fixture.snapshot_value(200).await, fixture.old_value());
}
```

- [ ] **Step 2: Run tests and verify current commit clocks fail**

Run:

```bash
cargo test --lib server::transactions::occ_tests::full_read_validation_prevents_point_write_skew -- --test-threads=1
cargo test --lib server::transactions::occ_tests::all_participants_install_the_same_commit_timestamp -- --test-threads=1
cargo test --lib server::transactions::occ_tests::equal_commit_timestamp_is_invisible_to_snapshot -- --test-threads=1
```

Expected: shared timestamp or fixed-snapshot assertions fail.

- [ ] **Step 3: Preserve every read dependency for writers**

Keep `generate_affected_objs` behavior:

```rust
fn generate_affected_objs(&self, txn: &mut TxnGuard) {
    let has_writes = txn.data.values().any(|object| object.changed);
    txn.affected_objects = if has_writes {
        txn.data
            .drain()
            .fold(BTreeMap::new(), |mut grouped, (id, object)| {
                grouped
                    .entry(object.server)
                    .or_insert_with(BTreeMap::new)
                    .insert(id, object);
                grouped
            })
    } else {
        txn.data.clear();
        BTreeMap::new()
    };
}
```

Prepare operations include unchanged reads as `PrepareIntent::Read`; commit
payloads still include only changed objects.

- [ ] **Step 4: Validate exact current logical expectations**

Add this mapping inside `DataManager`, keeping transaction types out of the RAM
layer:

```rust
fn current_expectation(&self, id: &Id) -> CellExpectation {
    match self.chunks().current_revision(id) {
        Some(CurrentRevision::Present(ts)) => CellExpectation::Present(ts),
        Some(CurrentRevision::Deleted(ts)) => CellExpectation::Absent(Some(ts)),
        None => CellExpectation::Absent(None),
    }
}
```

Participant prepare acquires all canonical owners first and then compares every
operation against `current_expectation`. An unchanged read receives the same
ownership and validation as a write dependency. No storage mutation occurs
until all comparisons pass.

- [ ] **Step 5: Allocate and distribute one commit HLC**

After all prepare responses have been observed:

```rust
let commit_hlc = self
    .deps
    .hlc
    .try_now()
    .map_err(|_| TMError::ClockExhausted)?;
let sites_commit_result = self
    .sites_commit(&tid, commit_hlc, affected_objs, &data_sites)
    .await?;
```

Pass `commit_hlc` unchanged to every participant. Remove `effective_ts` and the
Thomas-write skip from participant commit. Reject any assigned timestamp that
does not strictly exceed the current revision of a written cell.

- [ ] **Step 6: Install pending and promote during `end`**

Add to participant `Transaction`:

```rust
installed: BTreeMap<Id, InstalledRevision>,
commit_hlc: Option<Hlc>,
```

Commit installs each mutation with `RevisionWrite::pending(commit_hlc.ts)` and
keeps the cell owner. It acknowledges only after the history chain and current
cell-index mirror agree.

In `end`, while ownership is still held:

```rust
for installed in txn.installed.values() {
    self.chunks().promote_revision(installed)?;
}
```

Only after all promotions succeed may `attempt_lock_release` clear owners.
Already-selected old owned results remain valid; first reads see `Wait` until
promotion and release.

- [ ] **Step 7: Run OCC and distributed visibility tests**

Run:

```bash
cargo test --lib server::transactions::occ_tests -- --test-threads=1
cargo test --lib server::transactions::tests -- --test-threads=1
```

Expected: lost-update, write-skew, shared-timestamp, equality-boundary, and
partial-visibility tests pass.

- [ ] **Step 8: Commit OCC pending installs**

```bash
git add src/server/transactions
git commit -m "feat(mvcc): certify reads and install shared pending commits"
```

### Task 9: Make Undo and Abort Produce New Compensating Revisions

**Files:**

- Modify: `src/server/transactions/undo_log.rs`
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/ram/chunk.rs`
- Modify: `src/server/transactions/occ_tests.rs`

**Interfaces:**

- Replaces undo `version` with `installed_revision_ts` and
  `prior_revision_ts`.
- Produces:
  `Chunks::compensate(InstalledRevision, Option<OwnedCell>)`.
- Consumes checked local revision allocation and abort-state transitions.

- [ ] **Step 1: Write failing compensation and durability tests**

Add:

```rust
#[tokio::test]
async fn aborted_update_restores_content_with_newer_revision() {
    let fixture = transaction_fixture().await;
    fixture.seed_revision(100, "A").await;
    let tid = fixture.install_pending_update(200, "B").await;
    fixture.abort(tid).await.unwrap();

    let current = fixture.current_cell().await;
    assert_eq!(current.value(), "A");
    assert!(current.header.revision_ts > 200);
    assert!(!fixture.history_is_visible(200).await);
}

#[test]
fn undo_bytes_record_installed_and_prior_revisions() {
    let entry = UndoLogEntry::new_restore(
        test_hlc(10, 1),
        Id::new(1, 2),
        UndoOpType::Update,
        200,
        100,
        0,
        9,
        64,
    );
    let decoded = UndoLogEntry::from_bytes(&entry.to_bytes().unwrap())
        .unwrap()
        .0;
    assert_eq!(decoded.installed_revision_ts, 200);
    assert_eq!(decoded.prior_revision_ts, Some(100));
}
```

Add a durability hook that pauses immediately before storage mutation and
asserts the undo entry is recoverable from disk at that point.

Use the same test-hook pattern already present in `data_site.rs`:

```rust
#[cfg(test)]
pub(crate) fn install_before_storage_mutation_pause(
    tid: TxnId,
    id: Id,
) -> BeforeStorageMutationHandle
```

`BeforeStorageMutationHandle::wait_until_entered().await` fires after
`write_undo_entry` returns and before the `Chunks` mutation method is called;
`release()` permits the mutation.

- [ ] **Step 2: Run the tests and verify old rollback semantics fail**

Run:

```bash
cargo test --lib server::transactions::occ_tests::aborted_update_restores_content_with_newer_revision -- --test-threads=1
cargo test --lib server::transactions::undo_log::tests::undo_bytes_record_installed_and_prior_revisions
```

Expected: the undo layout and compensation assertions fail.

- [ ] **Step 3: Replace the undo format without a legacy decoder**

Use:

```rust
pub struct UndoLogEntry {
    pub txn_id: TxnId,
    pub cell_id: Id,
    pub op_type: UndoOpType,
    pub installed_revision_ts: u64,
    pub prior_revision_ts: Option<u64>,
    pub chunk_id: u64,
    pub seq_id: u64,
    pub cell_offset: u64,
}
```

Encode `prior_revision_ts` as a `u64`, using zero for `None`; persisted revision
timestamps are nonzero. Do not recognize the old record length or field order.

- [ ] **Step 4: Write undo before every mutation can become durable**

For insert, log `installed_revision_ts=commit_ts` and
`prior_revision_ts=None`. For update/remove, capture the old immutable address,
log both timestamps and its stable `chunk_id/seq_id/offset`, flush and
`sync_data`, then perform the storage mutation. Any undo write failure aborts
the commit before that mutation.

Do not merely log and continue after an error.

- [ ] **Step 5: Implement compensation**

While the transaction still owns the cell:

```rust
pub fn compensate(
    &self,
    installed: &InstalledRevision,
    prior: Option<OwnedCell>,
) -> Result<InstalledRevision, WriteError> {
    self.abort_revision(installed)?;
    let compensation_ts = self.next_revision_ts(installed.node.revision_ts)?;
    match prior {
        Some(mut cell) => self.upsert_cell_at_revision(
            &mut cell,
            RevisionWrite::committed(compensation_ts),
        ),
        None => self.remove_cell_at_revision(
            &installed.id,
            RevisionWrite::committed(compensation_ts),
        ),
    }
}
```

An aborted insert produces a newer committed tombstone. An aborted update or
delete produces newer restored content. Validate that current logical revision
still equals `installed_revision_ts`; otherwise return a rollback failure
without overwriting a later successful revision.

- [ ] **Step 6: Apply the same rule during startup undo recovery**

Recovery must compare the recovered current logical revision with
`installed_revision_ts`, call
`invalidate_recovered_revision(id, installed_revision_ts)` to transition the
otherwise committed one-node recovered head to aborted, and create the same
newer compensation. This recovery-only invalidation rejects any timestamp
other than the exact undo entry. Repeated recovery is idempotent because a
later compensation no longer equals the failed installed timestamp.

- [ ] **Step 7: Run undo, abort, and crash recovery tests**

Run:

```bash
cargo test --lib server::transactions::undo_log -- --test-threads=1
cargo test --lib server::transactions::occ_tests -- --test-threads=1
cargo test --lib ram::recovery -- --test-threads=1
```

Expected: insert/update/delete compensation, durability ordering, and
idempotence tests pass.

- [ ] **Step 8: Commit revision-based compensation**

```bash
git add src/server/transactions src/ram/chunk.rs
git commit -m "feat(mvcc): compensate aborted installs with newer revisions"
```

### Task 10: Resolve Stale Owners Instead of Reclaiming Them

**Files:**

- Modify: `src/server/transactions/mod.rs`
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/occ_tests.rs`

**Interfaces:**

- Produces coordinator RPC:
  `resolve(tid: TxnId) -> Result<TxnResolution, TMError>`.
- Produces bounded coordinator decision records.
- Produces test-only
  `DataManager::new_with_lock_timeout(runtime, hlc, lock_timeout_ms)`.
- Consumes idempotent participant `abort` and `end`.

- [ ] **Step 1: Write failing stale-owner safety tests**

Add:

```rust
#[tokio::test]
async fn stale_pending_owner_is_not_cleared_by_age() {
    let fixture = distributed_fixture_with_short_lock_timeout().await;
    let paused = fixture.pause_after_first_participant_install().await;
    fixture.advance_past_lock_timeout().await;

    assert_eq!(fixture.read_new_transaction(paused.cell).await, TxnExecResult::Wait);
    assert_eq!(fixture.owner(paused.cell), Some(paused.owner));
}

#[tokio::test]
async fn stale_owner_resolution_finishes_known_commit() {
    let fixture = distributed_fixture_with_short_lock_timeout().await;
    let paused = fixture.pause_before_end().await;
    fixture.record_coordinator_commit(paused.tid).await;
    fixture.trigger_resolution(paused.owner).await;

    assert_eq!(fixture.owner(paused.cell), None);
    assert_eq!(fixture.current_value(paused.cell).await, paused.new_value);
}

#[tokio::test]
async fn unknown_resolution_keeps_owner_and_data_hidden() {
    let fixture = distributed_fixture_with_short_lock_timeout().await;
    let paused = fixture.install_pending_without_decision().await;
    fixture.trigger_resolution(paused.owner).await;

    assert_eq!(fixture.owner(paused.cell), Some(paused.owner));
    assert_eq!(fixture.read_new_transaction(paused.cell).await, TxnExecResult::Wait);
}
```

- [ ] **Step 2: Run tests and verify blind timeout reclamation**

Run:

```bash
cargo test --lib server::transactions::occ_tests::stale_pending_owner_is_not_cleared_by_age -- --test-threads=1
cargo test --lib server::transactions::occ_tests::stale_owner_resolution_finishes_known_commit -- --test-threads=1
cargo test --lib server::transactions::occ_tests::unknown_resolution_keeps_owner_and_data_hidden -- --test-threads=1
```

Expected: the first test exposes the current blind `meta.owner = None` path.

- [ ] **Step 3: Add explicit decisions and bounded retention**

Use:

```rust
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub enum TxnResolution {
    Commit(Hlc),
    Abort,
    InProgress,
    Unknown,
}

struct DecisionRecord {
    resolution: TxnResolution,
    expires_at_ms: i64,
}
```

Keep decisions for `300_000` milliseconds after coordinator transaction
cleanup. Record `Commit(commit_hlc)` after every participant acknowledges its
pending install; record `Abort` when abort becomes the coordinator decision.
The `resolve` RPC returns the active transaction decision first, then the
bounded completed decision, then `Unknown`.

Store `lock_timeout_ms: i64` in `DataManager`. Production `DataManager::new`
passes `30_000`; the test-only constructor accepts a shorter positive value so
the stale-owner tests use real Tokio time without modifying the system clock.

- [ ] **Step 4: Queue asynchronous participant resolution**

When prepare encounters an owner older than `LOCK_TIMEOUT_MS`, do not mutate
`meta.owner`. Push the owner into a deduplicating resolution queue and return
the normal Wait-Die result.

The resolver obtains the coordinator transaction-manager client from
`owner.coordinator_id`, calls `resolve(owner.tid)`, and:

- `Commit(commit_hlc)`: verify the participant installed that timestamp, then
  run idempotent local `end`;
- `Abort`: run idempotent local `abort`, then `end`;
- `InProgress`, `Unknown`, or RPC failure: retain ownership and retry with
  backoff.

Never infer abort from age or network failure.

- [ ] **Step 5: Run resolution and distributed visibility suites**

Run:

```bash
cargo test --lib server::transactions::occ_tests -- --test-threads=1
cargo test --lib server::transactions::data_site -- --test-threads=1
```

Expected: stale owners either resolve from an explicit decision or remain
safely unavailable.

- [ ] **Step 6: Commit transaction resolution**

```bash
git add src/server/transactions
git commit -m "fix(mvcc): resolve stale transaction owners safely"
```

### Task 11: Complete Point API and Correctness Integration

**Files:**

- Modify: `src/server/cell_rpc.rs`
- Modify: `src/client/transaction.rs`
- Modify: `src/index/hash/mod.rs`
- Modify: `src/exec/adapter/mod.rs`
- Modify: `src/ram/tests/{cell,chunk}.rs`
- Modify: `src/server/transactions/{tests,occ_tests,corruption_tests}.rs`
- Modify: `src/query/data_client/tests.rs`

**Interfaces:**

- Consumes all prior revision-based storage and transaction interfaces.
- Produces no compatibility aliases.

- [ ] **Step 1: Add public behavior tests**

Cover:

```rust
assert_eq!(header_value["ts"], OwnedValue::U64(header.revision_ts));
assert!(header_map.get("ver").is_none());
```

Add conditional mutation tests proving that the exact revision succeeds once
and a stale revision returns `CellRevisionMismatch`. Add client tests proving
`SnapshotTooOld` is distinguishable from ordinary absence.

- [ ] **Step 2: Run the focused API tests and fix every direct caller**

Run:

```bash
cargo test --lib exec::adapter -- --test-threads=1
cargo test --lib client::tests -- --test-threads=1
cargo test --lib index::hash -- --test-threads=1
```

Expected: all pass with revision names and U64 timestamp metadata.

- [ ] **Step 3: Run an exhaustive forbidden-name scan**

Run:

```bash
rg -n "header\\.version|header\\.timestamp|CellVersionMismatch|compare_version|cell_version|current_version|old_version|new_version|__header\\.ver|PinnedRead|release_read_pins|effective_ts" src benches
```

Expected: no matches. Historical prose in affected comments must also use
“revision” so future maintainers do not reintroduce the removed counter.

- [ ] **Step 4: Run point-cell correctness gates**

Run:

```bash
cargo check --lib
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram:: -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
```

Expected: all pass.

- [ ] **Step 5: Commit API and integration cleanup**

```bash
git add src
git commit -m "refactor(api): expose revision-based point cell semantics"
```

### Task 12: Extend the Benchmark Portfolio and Enforce the Regression Gate

**Files:**

- Modify: `benches/occ_transactions.rs`
- Modify: `benches/occ_support/fixture.rs`
- Modify: `benches/occ_support/workloads.rs`
- Modify: `benches/occ_support/metrics.rs`
- Modify: `benches/README.md`
- Create: `scripts/compare-mvcc-benchmarks.sh`

**Interfaces:**

- Produces Criterion scenarios and JSON reports for current and historical
  point operations.
- Uses `192.168.10.87` as the accept-grade host.

- [ ] **Step 1: Add benchmark smoke tests before scenarios**

Add unit tests proving the workload matrix contains:

```rust
const REQUIRED_MVCC_SCENARIOS: &[&str] = &[
    "mvcc/non_transactional_read",
    "mvcc/non_transactional_update",
    "mvcc/read_only_current",
    "mvcc/rmw_one_cell",
    "mvcc/rmw_multi_cell",
    "mvcc/multi_participant",
    "mvcc/blind_update",
    "mvcc/blind_remove",
    "mvcc/full_read",
    "mvcc/selected_read",
    "mvcc/head_read",
    "mvcc/partial_read",
    "mvcc/history_depth_1",
    "mvcc/history_depth_8",
    "mvcc/history_depth_32",
    "mvcc/hot_cell_old_snapshot",
    "mvcc/history_expiration",
    "mvcc/cleaner_retained_revisions",
    "mvcc/cleaner_reader_contention",
    "mvcc/hlc_contention",
];
```

The smoke test invokes one iteration per scenario and requires zero unexpected
outcomes.

- [ ] **Step 2: Run benchmark test mode and verify missing scenarios**

Run:

```bash
cargo bench --bench occ_transactions -- --test
```

Expected: the required scenario inventory test fails before registration.

- [ ] **Step 3: Implement the workload matrix**

Reuse `OccFixture` for transactional cases. Add a direct client workload for
non-transactional reads/updates, deterministic helpers that create chain
depths `1`, `8`, and `32`, a retention fixture with
`history_retention_ms=50`, and a cleaner fixture with enough segments to force
combine.

For every scenario record:

```rust
pub struct ScenarioSummary {
    pub committed: u64,
    pub attempts: u64,
    pub not_realizable: u64,
    pub logical_retries: u64,
    pub waits: u64,
    pub commits_per_second: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub unexpected: Vec<String>,
    pub invariants_passed: bool,
    pub retained_revisions: u64,
    pub retained_bytes: u64,
    pub segment_count: u64,
}
```

Keep existing OCC counters and phase profiling intact.

- [ ] **Step 4: Add a strict comparison script**

`scripts/compare-mvcc-benchmarks.sh` accepts three baseline reports, a `--`
separator, and three candidate reports. It requires matching scenario names,
rejects any nonempty `unexpected` list, calculates the across-run coefficient
of variation for throughput, and prints median throughput and p99 deltas. It
exits nonzero when either side has throughput CV at or above 5%, or when a
non-historical scenario has a reproducible median throughput loss greater than
5% or median p99 increase greater than 5%.

The command contract is:

```bash
scripts/compare-mvcc-benchmarks.sh \
  target/occ-bench/develop-1.json \
  target/occ-bench/develop-2.json \
  target/occ-bench/develop-3.json \
  -- \
  target/occ-bench/mvcc-1.json \
  target/occ-bench/mvcc-2.json \
  target/occ-bench/mvcc-3.json
```

- [ ] **Step 5: Run local smoke and correctness checks**

Run:

```bash
cargo bench --bench occ_transactions -- --test
NEB_OCC_BENCH_LABEL=mvcc-smoke cargo bench --bench occ_transactions -- --sample-size 10
```

Expected: all scenario invariants pass and
`target/occ-bench/mvcc-smoke.json` is produced.

- [ ] **Step 6: Run accept-grade comparison on `192.168.10.87`**

On an idle host, use separate Nebuchadnezzar and Bifrost worktrees so the
baseline uses the Bifrost revision referenced by `a82ccd46` while the candidate
uses Task 1's checked-HLC commit. Use identical release flags, NUMA binding,
port range, and dataset for both revisions:

```bash
numactl --cpunodebind=0 --membind=0 env \
  NEB_OCC_BENCH_LABEL=develop-1 \
  NEB_OCC_BENCH_REVISION=a82ccd46 \
  cargo bench --bench occ_transactions -- --save-baseline develop
```

Repeat with labels `develop-2` and `develop-3`, then run the candidate:

```bash
numactl --cpunodebind=0 --membind=0 env \
  NEB_OCC_BENCH_LABEL=mvcc-1 \
  NEB_OCC_BENCH_REVISION="$(git rev-parse HEAD)" \
  cargo bench --bench occ_transactions -- --save-baseline mvcc
```

Repeat with labels `mvcc-2` and `mvcc-3`. Require coefficient of variation
below 5% before applying the regression rule. Investigate any stable hot-path
regression above 5%; do not weaken read validation, snapshot visibility,
reclamation safety, or a distributed phase to recover performance.

- [ ] **Step 7: Update benchmark documentation and commit**

Document the scenario commands, JSON fields, retention settings, and primary
host `192.168.10.87`. Do not commit `target/`, Criterion output, `perf.data`, or
host configuration.

```bash
git add benches scripts/compare-mvcc-benchmarks.sh
git commit -m "bench(mvcc): add correctness and performance portfolio"
```

### Task 13: Run the Full Acceptance Gate

**Files:**

- Modify only files required by failures attributable to the MVCC branch.
- Record accepted benchmark results in:
  `docs/superpowers/specs/2026-07-25-point-cell-mvcc-design.md`

**Interfaces:**

- Consumes all implementation tasks.
- Produces a clean, verified branch ready for review.

- [ ] **Step 1: Run formatting and compile checks**

```bash
cargo fmt --all -- --check
cargo check --lib
```

Expected: both pass.

- [ ] **Step 2: Run transaction, storage, recovery, and tiered suites**

```bash
cargo test --lib server::transactions -- --test-threads=1
cargo test --lib ram:: -- --test-threads=1
cargo test --lib ram::tiered -- --test-threads=1
```

Expected: all pass.

- [ ] **Step 3: Run existing index regression suites**

```bash
cargo test --lib index::full_text -- --test-threads=1
cargo test --lib index::ranged -- --test-threads=1
```

Expected: all pass. Index results do not gain MVCC isolation, but existing
functionality must not regress.

- [ ] **Step 4: Run benchmark smoke and compare accepted reports**

```bash
cargo bench --bench occ_transactions -- --test
scripts/compare-mvcc-benchmarks.sh \
  target/occ-bench/develop-1.json \
  target/occ-bench/develop-2.json \
  target/occ-bench/develop-3.json \
  -- \
  target/occ-bench/mvcc-1.json \
  target/occ-bench/mvcc-2.json \
  target/occ-bench/mvcc-3.json
```

Expected: invariants pass and the script exits zero.

- [ ] **Step 5: Record evidence and commit**

Append the exact tested commit IDs, commands, scenario deltas, coefficients of
variation, and host `192.168.10.87` to the design specification.

```bash
git add docs/superpowers/specs/2026-07-25-point-cell-mvcc-design.md
git commit -m "docs(mvcc): record implementation verification"
```

- [ ] **Step 6: Verify the final repository state**

```bash
git status --short
git log --oneline --decorate -15
```

Expected: the worktree is clean and the ordered commits show HLC safety,
revision layout, history, snapshot reads, cleaner, recovery, transaction
integration, compensation, resolution, API cleanup, benchmarks, and recorded
verification.
