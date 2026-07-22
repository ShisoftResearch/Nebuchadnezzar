# Repeatable-Read OCC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore version-certified OCC so transactional reads are repeatable and two updates derived from the same cell version cannot both commit.

**Architecture:** The transaction manager keeps one complete cell snapshot plus an explicit present-version or absent expectation for every dependency. Read-write transactions send those expectations to each data site during prepare; data sites acquire short-lived certification ownership with a total Wait-Die priority, validate headers without retaining storage read guards, and keep ownership through commit. Commit uses the certified version again for conditional storage mutation as a defense against non-transactional interference.

**Tech Stack:** Rust 2021, Tokio, Bifrost RPC/vector clocks, parking_lot metadata locks, Neb chunk versioned mutation APIs, Cargo tests.

---

## File Structure

- Modify `src/server/transactions/mod.rs`: define expectation, prepare-intent, prepare-operation, and transaction-priority wire/value types.
- Modify `src/server/transactions/manager.rs`: unify transactional reads around the full-cell cache, cache absence, discover versions for blind mutations, retain read dependencies for read-write transactions, and send prepare operations.
- Modify `src/server/transactions/data_site.rs`: use total-priority owners, certify expectations during prepare, retain certified expectations locally, and conditionally apply commits.
- Create `src/server/transactions/occ_tests.rs`: focused behavioral regression tests for repeatable reads, lost updates, concurrent vector clocks, and prepare cleanup.
- Modify `README.md`: advertise repeatable-read OCC instead of read committed.
- Modify `agent_docs/POSIX_FUSE_BACKEND_GUIDE.md`: correct the isolation description and retain the warning that Neb is not an external lock/lease authority.

### Task 1: Define OCC expectations and total transaction priority

**Files:**
- Modify: `src/server/transactions/mod.rs`
- Test: `src/server/transactions/mod.rs`

- [ ] **Step 1: Write failing unit tests for causal and concurrent priorities**

Add these tests to the existing transaction module:

```rust
#[cfg(test)]
mod occ_type_tests {
    use super::*;
    use std::cmp::Ordering;

    fn clock(entries: &[(u64, u64)]) -> TxnId {
        StandardVectorClock::from_vec(entries.to_vec())
    }

    #[test]
    fn txn_priority_preserves_causal_order() {
        let older = TxnPriority::new(clock(&[(1, 1)]), 9);
        let younger = TxnPriority::new(clock(&[(1, 2)]), 9);
        assert_eq!(older.compare_age(&younger), Ordering::Less);
        assert_eq!(younger.compare_age(&older), Ordering::Greater);
    }

    #[test]
    fn txn_priority_totally_orders_concurrent_clocks_by_coordinator() {
        let left = TxnPriority::new(clock(&[(1, 1)]), 10);
        let right = TxnPriority::new(clock(&[(2, 1)]), 20);
        assert_eq!(left.compare_age(&right), Ordering::Less);
        assert_eq!(right.compare_age(&left), Ordering::Greater);
    }
}
```

- [ ] **Step 2: Run the tests to verify RED**

Run:

```bash
cargo test --lib txn_priority_ -- --nocapture
```

Expected: compilation fails because `TxnPriority` does not exist.

- [ ] **Step 3: Add explicit expectation and priority types**

In `src/server/transactions/mod.rs`, add:

```rust
use bifrost::vector_clock::{Relation, StandardVectorClock};
use std::cmp::Ordering;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum CellExpectation {
    Present(u64),
    Absent,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, Eq, PartialEq)]
pub enum PrepareIntent {
    Read,
    Write,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct PrepareOp {
    pub id: Id,
    pub expectation: CellExpectation,
    pub intent: PrepareIntent,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct TxnPriority {
    pub tid: TxnId,
    pub coordinator_id: u64,
}

impl TxnPriority {
    pub fn new(tid: TxnId, coordinator_id: u64) -> Self {
        Self { tid, coordinator_id }
    }

    pub fn compare_age(&self, other: &Self) -> Ordering {
        match self.tid.relation(&other.tid) {
            Relation::Before => Ordering::Less,
            Relation::After => Ordering::Greater,
            Relation::Equal | Relation::Concurrent => self
                .coordinator_id
                .cmp(&other.coordinator_id)
                .then_with(|| {
                    bifrost::utils::serde::serialize(&self.tid)
                        .cmp(&bifrost::utils::serde::serialize(&other.tid))
                }),
        }
    }
}
```

Do not implement `Ord` for `TxnPriority`; callers must use the explicit age comparison so the code cannot accidentally inherit `VectorClock::Ord`, which treats concurrent clocks as equal.

- [ ] **Step 4: Run the focused tests to verify GREEN**

Run:

```bash
cargo test --lib txn_priority_ -- --nocapture
```

Expected: both priority tests pass.

- [ ] **Step 5: Commit the type foundation**

```bash
git add src/server/transactions/mod.rs
git commit -m "feat(txn): define OCC certification types"
```

### Task 2: Make every transactional read use one cached snapshot

**Files:**
- Create: `src/server/transactions/occ_tests.rs`
- Modify: `src/server/transactions/mod.rs`
- Modify: `src/server/transactions/manager.rs`

- [ ] **Step 1: Register the focused OCC test module**

Add beside the other test modules in `src/server/transactions/mod.rs`:

```rust
#[cfg(test)]
mod occ_tests;
```

- [ ] **Step 2: Add test helpers and failing repeatable-read tests**

Create `src/server/transactions/occ_tests.rs` with a dedicated server helper and these behaviors:

```rust
use super::*;
use crate::ram::cell::{OwnedCell, ReadError};
use crate::ram::schema::Schema;
use crate::ram::tests::default_fields;
use crate::ram::types::{Id, OwnedMap, OwnedValue};
use crate::server::{NebServer, ServerOptions, Service};
use bifrost_hasher::hash_str;
use dovahkiin::types::Map;
use std::sync::Arc;

async fn start_occ_server(address: &str, group: &str) -> Arc<NebServer> {
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: crate::ram::segs::SEGMENT_SIZE,
            db_size: crate::ram::segs::SEGMENT_SIZE,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: false,
            services: vec![Service::Cell, Service::Transaction],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address.to_string(),
        &group.to_string(),
        async |_| {},
    )
    .await
    .unwrap()
}

fn install_schema(server: &Arc<NebServer>) -> Schema {
    let schema = Schema::new_with_id(
        901,
        "occ_test",
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    schema
}

fn counter_cell(schema: u32, id: Id, score: u64) -> OwnedCell {
    let mut data = OwnedMap::new();
    data.insert(&"id".to_string(), OwnedValue::I64(id.lower as i64));
    data.insert(&"score".to_string(), OwnedValue::U64(score));
    data.insert(&"name".to_string(), OwnedValue::String(format!("v{score}")));
    OwnedCell::new_with_id(schema, &id, OwnedValue::Map(data))
}

async fn txn_client(address: &str, group: &str) -> Arc<manager::AsyncServiceClient> {
    new_async_client_for_database(&address.to_string(), group, group)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_full_read_uses_first_snapshot() {
    let server = start_occ_server("127.0.0.1:5320", "occ_repeatable_full").await;
    let schema = install_schema(&server);
    let id = Id::new(0, 1);
    let mut initial = counter_cell(schema.id, id, 0);
    server.chunks().write_cell(&mut initial).unwrap();
    let client = txn_client("127.0.0.1:5320", "occ_repeatable_full").await;
    let tid = client.begin().await.unwrap().unwrap();

    let first = client.read(tid.clone(), id).await.unwrap().unwrap().unwrap();
    let mut external = counter_cell(schema.id, id, 9);
    server.chunks().update_cell(&mut external).unwrap();
    let second = client.read(tid.clone(), id).await.unwrap().unwrap().unwrap();

    assert_eq!(first.header.version, second.header.version);
    assert_eq!(second.data["score"].u64(), Some(&0));
    client.abort(tid).await.unwrap().unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_missing_read_caches_absence() {
    let server = start_occ_server("127.0.0.1:5321", "occ_repeatable_absent").await;
    let schema = install_schema(&server);
    let id = Id::new(0, 2);
    let client = txn_client("127.0.0.1:5321", "occ_repeatable_absent").await;
    let tid = client.begin().await.unwrap().unwrap();

    assert!(matches!(
        client.read(tid.clone(), id).await.unwrap().unwrap(),
        TxnExecResult::Error(ReadError::CellDoesNotExisted)
    ));
    let mut inserted = counter_cell(schema.id, id, 1);
    server.chunks().write_cell(&mut inserted).unwrap();
    assert!(matches!(
        client.read(tid.clone(), id).await.unwrap().unwrap(),
        TxnExecResult::Error(ReadError::CellDoesNotExisted)
    ));

    client.abort(tid).await.unwrap().unwrap();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repeatable_selected_and_head_share_full_snapshot() {
    let server = start_occ_server("127.0.0.1:5322", "occ_repeatable_projection").await;
    let schema = install_schema(&server);
    let id = Id::new(0, 3);
    let mut initial = counter_cell(schema.id, id, 0);
    server.chunks().write_cell(&mut initial).unwrap();
    let client = txn_client("127.0.0.1:5322", "occ_repeatable_projection").await;
    let tid = client.begin().await.unwrap().unwrap();

    let selected = client
        .read_selected(tid.clone(), id, vec![hash_str("score")])
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let head = client.head(tid.clone(), id).await.unwrap().unwrap().unwrap();
    let mut external = counter_cell(schema.id, id, 9);
    server.chunks().update_cell(&mut external).unwrap();
    let full = client.read(tid.clone(), id).await.unwrap().unwrap().unwrap();

    assert_eq!(selected.header.version, head.version);
    assert_eq!(head.version, full.header.version);
    assert_eq!(selected.data[0].u64(), Some(&0));
    assert_eq!(full.data["score"].u64(), Some(&0));
    client.abort(tid).await.unwrap().unwrap();
    server.shutdown().await;
}
```

- [ ] **Step 3: Run repeatability tests to verify RED**

Run:

```bash
cargo test --lib repeatable_ -- --nocapture --test-threads=1
```

Expected: missing-cell and selected/head repeatability fail because those observations are not cached consistently.

- [ ] **Step 4: Replace ambiguous versions with explicit expectations**

Change `DataObject` in `manager.rs` to:

```rust
#[derive(Clone, Debug)]
struct DataObject {
    server: u64,
    cell: Option<OwnedCell>,
    expectation: CellExpectation,
    changed: bool,
    new: bool,
}
```

When `read_from_site` accepts a cell, store:

```rust
DataObject {
    server: server_id,
    expectation: CellExpectation::Present(cell.header.version),
    cell: Some(cell),
    new: false,
    changed: false,
}
```

When it receives `TxnExecResult::Error(ReadError::CellDoesNotExisted)`, insert:

```rust
txn.data.insert(
    *id,
    DataObject {
        server: server_id,
        cell: None,
        expectation: CellExpectation::Absent,
        new: false,
        changed: false,
    },
);
```

Then return the original missing-cell result.

- [ ] **Step 5: Route selected and header reads through the full-cell cache**

Extract the existing selected-field projection into:

```rust
fn select_from_cell(
    &self,
    cell: &OwnedCell,
    fields: &[u64],
) -> Result<OwnedCell, ReadError> {
    let schema = self
        .deps
        .schemas()
        .get(&cell.header.schema)
        .ok_or(ReadError::SchemaDoesNotExisted(cell.header.schema))?;
    let map = match &cell.data {
        OwnedValue::Map(map) => map,
        _ => return Err(ReadError::CellTypeIsNotMapForSelect),
    };
    let values = fields
        .iter()
        .map(|field| {
            schema
                .id_index
                .get(field)
                .map(|path| path.iter().map(|id| *id as u64).collect_vec())
                .map(|path| map.get_in_by_ids(path.iter()).clone())
                .unwrap_or(OwnedValue::Null)
        })
        .collect();
    Ok(OwnedCell {
        header: cell.header,
        data: OwnedValue::Array(values),
    })
}
```

Make `head` and `read_selected` acquire a mutable transaction guard and call the same full-cell cache/fetch helper used by `read`. Map an accepted full cell to `cell.header` or `select_from_cell`. Remove `read_selected_from_site`. Retain the remote header helper for blind mutation discovery, rename it `observe_head_from_site`, and remove its unused `_server_id` and `_txn` parameters. Keep the data-site RPCs unchanged for compatibility.

- [ ] **Step 6: Run repeatability tests to verify GREEN**

Run:

```bash
cargo test --lib repeatable_ -- --nocapture --test-threads=1
```

Expected: all repeatable-read tests pass.

- [ ] **Step 7: Commit the snapshot behavior**

```bash
git add src/server/transactions/mod.rs src/server/transactions/manager.rs src/server/transactions/occ_tests.rs
git commit -m "feat(txn): cache repeatable cell snapshots"
```

### Task 3: Retain OCC dependencies and version blind mutations

**Files:**
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Add failing manager tests for dependency retention**

Add private-module tests in `manager.rs` that construct a `Transaction` with one unchanged read and one changed update, call `generate_affected_objs`, and assert both objects remain in the same participant map. Add a second test asserting a read-only transaction produces an empty participant map.

The core assertions are:

```rust
manager.generate_affected_objs(&mut txn_guard);
let participant = txn_guard.affected_objects.get(&server_id).unwrap();
assert!(participant.contains_key(&read_id));
assert!(participant.contains_key(&write_id));
```

and:

```rust
manager.generate_affected_objs(&mut read_only_guard);
assert!(read_only_guard.affected_objects.is_empty());
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test --lib affected_objs_ -- --nocapture
```

Expected: the mixed transaction test fails because unchanged reads are filtered out.

- [ ] **Step 3: Retain the complete dependency set only for read-write transactions**

Replace `generate_affected_objs` with:

```rust
fn generate_affected_objs(&self, txn: &mut TxnGuard) {
    let has_writes = txn.data.values().any(|data_obj| data_obj.changed);
    let mut affected_objs = AffectedObjs::new();
    if has_writes {
        for (id, data_obj) in txn.data.drain() {
            affected_objs
                .entry(data_obj.server)
                .or_insert_with(BTreeMap::new)
                .insert(id, data_obj);
        }
    } else {
        txn.data.clear();
    }
    txn.affected_objects = affected_objs;
}
```

- [ ] **Step 4: Add failing manager tests for blind update/remove discovery**

Add manager-module tests that seed a cell, invoke `update` or `remove` without a preceding transactional read, then inspect the live transaction entry and assert:

```rust
let txn = runtime.txn_manager().unwrap().get_transaction(&tid).unwrap();
let guard = txn.lock().await;
assert_eq!(
    guard.data.get(&id).unwrap().expectation,
    CellExpectation::Present(original_version)
);
assert!(guard.data.get(&id).unwrap().changed);
```

Name the tests `blind_mutation_records_update_version` and `blind_mutation_records_remove_version`.

- [ ] **Step 5: Discover a base version for blind update/remove**

Add a manager helper that performs the existing retry/backoff header RPC and returns the header without adding a user-visible read result:

```rust
async fn observe_version(
    &self,
    server: &Arc<data_site::AsyncServiceClient>,
    tid: &TxnId,
    id: Id,
) -> Result<TxnExecResult<u64, ReadError>, TMError> {
    match self.observe_head_from_site(server, tid, &id).await? {
        TxnExecResult::Accepted(header) => Ok(TxnExecResult::Accepted(header.version)),
        TxnExecResult::Error(error) => Ok(TxnExecResult::Error(error)),
        TxnExecResult::Rejected => Ok(TxnExecResult::Rejected),
        TxnExecResult::Wait => unreachable!("head_from_site retries waits"),
        TxnExecResult::StateError(state) => Ok(TxnExecResult::StateError(state)),
    }
}
```

Give `observe_head_from_site` this exact signature:

```rust
async fn observe_head_from_site(
    &self,
    server: &Arc<data_site::AsyncServiceClient>,
    tid: &TxnId,
    id: &Id,
) -> Result<TxnExecResult<CellHeader, ReadError>, TMError>
```

In `update` and `remove`, when no cache entry exists, await `observe_version` and insert `CellExpectation::Present(version)` before marking the object changed. Convert `ReadError::CellDoesNotExisted` to `WriteError::CellDoesNotExisted` and other read errors to `WriteError::ReadError(error)`.

For `write`, use these rules:

```rust
match txn.data.get_mut(&id) {
    Some(data) if matches!(data.expectation, CellExpectation::Present(_)) => {
        Ok(TxnExecResult::Error(WriteError::CellAlreadyExisted))
    }
    Some(data) => {
        data.cell = Some(cell);
        data.expectation = CellExpectation::Absent;
        data.changed = true;
        data.new = true;
        Ok(TxnExecResult::Accepted(()))
    }
    None => {
        txn.data.insert(id, DataObject {
            server: server_id,
            cell: Some(cell),
            expectation: CellExpectation::Absent,
            changed: true,
            new: true,
        });
        Ok(TxnExecResult::Accepted(()))
    }
}
```

- [ ] **Step 6: Run manager-focused tests**

Run:

```bash
cargo test --lib affected_objs_ -- --nocapture
cargo test --lib blind_mutation_records_ -- --nocapture --test-threads=1
```

Expected: dependency-retention and expectation-recording tests pass; end-to-end stale prepare tests are still RED because prepare accepts only IDs.

- [ ] **Step 7: Commit dependency tracking**

```bash
git add src/server/transactions/manager.rs src/server/transactions/occ_tests.rs
git commit -m "feat(txn): retain OCC dependency expectations"
```

### Task 4: Certify versions during deadlock-free prepare

**Files:**
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/server/transactions/data_site.rs`
- Test: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Add failing data-site certification tests**

In the existing `data_site.rs` test module, import `Schema`, `default_fields`, `OwnedMap`, and `OwnedValue`, then add this helper:

```rust
fn seed_data_site_cell(server: &Arc<NebServer>, id: Id) -> u64 {
    let schema = Schema::new_with_id(
        902,
        "occ_data_site",
        None,
        default_fields(),
        false,
        false,
    );
    server.meta().schemas.debug_only_new_schema(schema.clone());
    let mut data = OwnedMap::new();
    data.insert(&"id".to_string(), OwnedValue::I64(id.lower as i64));
    data.insert(&"score".to_string(), OwnedValue::U64(0));
    data.insert(&"name".to_string(), OwnedValue::String("initial".to_string()));
    let mut cell = OwnedCell::new_with_id(schema.id, &id, OwnedValue::Map(data));
    server.chunks().write_cell(&mut cell).unwrap().version
}

#[tokio::test(flavor = "multi_thread")]
async fn prepare_rejects_a_stale_present_version() {
    let address = "127.0.0.1:5323";
    let group = "occ_stale_present";
    let server = start_transaction_test_server(address, group).await;
    let manager = data_manager_for_database(&server, address, group).await;
    let id = Id::new(0, 10);
    let version = seed_data_site_cell(&server, id);
    let tid = StandardVectorClock::from_vec(vec![(11, 1)]);
    let response = <DataManager as Service>::prepare(
        &manager,
        11,
        tid.clone(),
        tid,
        vec![PrepareOp {
            id,
            expectation: CellExpectation::Present(version + 1),
            intent: PrepareIntent::Write,
        }],
    )
    .await;
    assert_eq!(response.payload, DMPrepareResult::NotRealizable);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepare_rejects_a_present_cell_when_absence_was_observed() {
    let address = "127.0.0.1:5324";
    let group = "occ_stale_absent";
    let server = start_transaction_test_server(address, group).await;
    let manager = data_manager_for_database(&server, address, group).await;
    let id = Id::new(0, 11);
    seed_data_site_cell(&server, id);
    let tid = StandardVectorClock::from_vec(vec![(11, 1)]);
    let response = <DataManager as Service>::prepare(
        &manager,
        11,
        tid.clone(),
        tid,
        vec![PrepareOp {
            id,
            expectation: CellExpectation::Absent,
            intent: PrepareIntent::Write,
        }],
    )
    .await;
    assert_eq!(response.payload, DMPrepareResult::NotRealizable);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_clock_wait_die_has_one_younger_requester() {
    let address = "127.0.0.1:5325";
    let group = "occ_total_wait_die";
    let server = start_transaction_test_server(address, group).await;
    let manager = data_manager_for_database(&server, address, group).await;
    let id = Id::new(0, 12);
    let version = seed_data_site_cell(&server, id);
    let older = StandardVectorClock::from_vec(vec![(11, 1)]);
    let younger = StandardVectorClock::from_vec(vec![(22, 1)]);
    let op = PrepareOp {
        id,
        expectation: CellExpectation::Present(version),
        intent: PrepareIntent::Write,
    };

    let first = <DataManager as Service>::prepare(
        &manager,
        11,
        older.clone(),
        older.clone(),
        vec![op.clone()],
    )
    .await;
    let second = <DataManager as Service>::prepare(
        &manager,
        22,
        younger.clone(),
        younger,
        vec![op],
    )
    .await;

    assert_eq!(first.payload, DMPrepareResult::Success);
    assert_eq!(second.payload, DMPrepareResult::NotRealizable);
    server.shutdown().await;
}
```

- [ ] **Step 2: Run certification tests to verify RED**

Run:

```bash
cargo test --lib prepare_rejects_ -- --nocapture --test-threads=1
cargo test --lib concurrent_clock_wait_die_ -- --nocapture --test-threads=1
```

Expected: compilation fails because data-site prepare still accepts `Vec<Id>`.

- [ ] **Step 3: Store priority owners and certified operations**

Change data-site structures to:

```rust
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnPriority>,
    lock_acquired_at: Option<i64>,
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    certified: BTreeMap<Id, PrepareOp>,
    coordinator_id: Option<u64>,
    last_activity: i64,
    history: CommitHistory,
    segment_guards: Vec<SegmentReferenceGuard>,
}
```

Initialize the new fields in `create_transaction`.

- [ ] **Step 4: Change the prepare RPC and manager payload**

Change the data-site service declaration and implementation to:

```rust
rpc prepare(
    coordinator_id: u64,
    clock: StandardVectorClock,
    tid: TxnId,
    ops: Vec<PrepareOp>
) -> DataSiteResponse<DMPrepareResult>;
```

In manager `site_prepare`, map the sorted `BTreeMap` directly:

```rust
let ops = objs
    .iter()
    .map(|(id, data)| PrepareOp {
        id: *id,
        expectation: data.expectation.clone(),
        intent: if data.changed {
            PrepareIntent::Write
        } else {
            PrepareIntent::Read
        },
    })
    .collect::<Vec<_>>();
```

- [ ] **Step 5: Implement owner conflict handling with total priority**

Inside data-site prepare, build:

```rust
let requester = TxnPriority::new(tid.clone(), coordinator_id);
```

For each sorted operation, resolve the metadata mutex and apply:

```rust
if let Some(owner) = &meta.owner {
    if owner != &requester {
        if lock_is_stale {
            meta.owner = None;
            meta.lock_acquired_at = None;
        } else if requester.compare_age(owner).is_gt() {
            return self.response_with(DMPrepareResult::NotRealizable);
        } else {
            return self.response_with(DMPrepareResult::Wait);
        }
    }
}
```

Remove `tid < meta.read` and `tid < meta.write` as correctness gates. Keep the timestamp fields for current cleanup/diagnostic behavior until a separate cleanup removes them.

- [ ] **Step 6: Validate expectations without retaining storage read guards**

Add:

```rust
fn expectation_matches(&self, op: &PrepareOp) -> bool {
    match (&op.expectation, self.chunks().head_cell(&op.id)) {
        (CellExpectation::Present(expected), Ok(header)) => header.version == *expected,
        (CellExpectation::Absent, Err(ReadError::CellDoesNotExisted)) => true,
        _ => false,
    }
}
```

While holding the sorted metadata mutex guards, validate every operation. If any mismatch occurs, return `NotRealizable`; no owner marker has been published yet. If all match, set every owner to `requester`, store the operations in `txn.certified`, record the coordinator, and mark the transaction prepared.

Do not call `location_for_read` and do not store storage read guards.

- [ ] **Step 7: Update lock release to compare full owners**

Build the expected owner from the data-site transaction's stored coordinator ID and require exact `TxnPriority` equality before clearing `meta.owner`. Adjust log messages to include both TID and coordinator.

- [ ] **Step 8: Run certification and deadlock tests to verify GREEN**

Run:

```bash
cargo test --lib prepare_rejects_ -- --nocapture --test-threads=1
cargo test --lib concurrent_clock_wait_die_ -- --nocapture --test-threads=1
cargo test --lib server::transactions::data_site::tests -- --nocapture --test-threads=1
```

Expected: stale expectations are rejected, concurrent clocks have a single total priority, and existing data-site cleanup tests pass.

- [ ] **Step 9: Commit prepare certification**

```bash
git add src/server/transactions/manager.rs src/server/transactions/data_site.rs src/server/transactions/occ_tests.rs
git commit -m "feat(txn): certify OCC versions during prepare"
```

### Task 5: Apply only the certified version and prove lost updates are rejected

**Files:**
- Modify: `src/server/transactions/data_site.rs`
- Modify: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Add the failing lost-update integration test**

Add a test that begins T1 and T2, reads counter `0` in both, writes `1` in both, then prepares T1 followed by T2:

```rust
assert_eq!(
    client.prepare(t1.clone()).await.unwrap().unwrap(),
    TMPrepareResult::Success
);
assert_eq!(client.commit(t1).await.unwrap().unwrap(), EndResult::Success);
assert_eq!(
    client.prepare(t2).await.unwrap().unwrap(),
    TMPrepareResult::DMPrepareError(DMPrepareResult::NotRealizable)
);
assert_eq!(
    server.chunks().read_cell(&id).unwrap().data["score"].u64(),
    Some(&1)
);
```

Add a retry transaction that reads `1`, writes `2`, commits, and asserts the final value is `2`.

Use this assertion after retry commit:

```rust
let final_cell = server.chunks().read_cell(&id).unwrap();
assert_eq!(final_cell.data["score"].u64(), Some(&2));
```

- [ ] **Step 2: Add a concurrent-vector-clock data-site test**

Using a single `DataManager`, construct:

```rust
let t1 = StandardVectorClock::from_vec(vec![(11, 1)]);
let t2 = StandardVectorClock::from_vec(vec![(22, 1)]);
assert_eq!(t1.relation(&t2), Relation::Concurrent);
```

Have both expectations reference the same initial version. Commit the first prepared operation, end it, and assert the second prepare returns `NotRealizable`. This proves correctness does not depend on `<` between vector clocks.

Add a direct data-site test named `commit_rejects_change_after_certification`. Seed a cell, prepare `Present(initial_version)`, mutate the cell directly, then invoke data-site commit with `CommitOp::Update(desired_cell)`:

```rust
let prepare = <DataManager as Service>::prepare(
    &manager,
    41,
    tid.clone(),
    tid.clone(),
    vec![PrepareOp {
        id,
        expectation: CellExpectation::Present(initial_version),
        intent: PrepareIntent::Write,
    }],
)
.await;
assert_eq!(prepare.payload, DMPrepareResult::Success);

let mut external = desired_cell.clone();
let mut external_data = external.data.Map().unwrap().clone();
external_data.insert(
    &"score".to_string(),
    OwnedValue::U64(50),
);
external.data = OwnedValue::Map(external_data);
server.chunks().update_cell(&mut external).unwrap();

let commit = <DataManager as Service>::commit(
    &manager,
    tid.clone(),
    tid,
    vec![CommitOp::Update(desired_cell)],
)
.await;
assert_eq!(commit.payload, DMCommitResult::CellChanged(id));
```

- [ ] **Step 3: Run lost-update tests and verify the defensive check is RED**

Run:

```bash
cargo test --lib lost_update_ -- --nocapture --test-threads=1
cargo test --lib concurrent_vector_clock_stale_update_ -- --nocapture --test-threads=1
cargo test --lib commit_rejects_change_after_certification -- --nocapture --test-threads=1
```

Expected: the normal and concurrent-clock lost-update tests pass through prepare certification; `commit_rejects_change_after_certification` fails because update/remove still adopt the commit-time current version rather than the certified version.

- [ ] **Step 4: Use certified versions in update and remove**

Add a helper on the data-site transaction:

```rust
fn certified_version(txn: &Transaction, id: &Id) -> Option<u64> {
    match txn.certified.get(id).map(|op| &op.expectation) {
        Some(CellExpectation::Present(version)) => Some(*version),
        _ => None,
    }
}
```

For `CommitOp::Update`, obtain the expected version before calling `update_cell_by` and use:

```rust
if cell_to_update.header.version == expected_version {
    old_cell_ref = Some((*cell_to_update).to_owned().into_ref());
    cell.header.version = expected_version;
    Some(cell)
} else {
    None
}
```

For `CommitOp::Remove`, call `remove_cell_by` with:

```rust
|current| current.header.version == expected_version
```

Map `UserCanceledUpdate`, `DeletionPredictionFailed`, and `CellVersionMismatch` to `DMCommitResult::CellChanged(id)`. For `CommitOp::Write`, require a certified `Absent` expectation; an unexpected existing cell also returns `CellChanged`.

- [ ] **Step 5: Validate commit intents against certified intents**

Before applying each operation, verify that read operations have `PrepareIntent::Read` and mutations have `PrepareIntent::Write`. A missing or mismatched certification returns `CheckFailed(CheckError::CannotEnd)` without applying that operation.

- [ ] **Step 6: Run all OCC tests to verify GREEN**

Run:

```bash
cargo test --lib server::transactions::occ_tests -- --nocapture --test-threads=1
```

Expected: repeatable reads, blind conflicts, standard lost updates, and concurrent-vector-clock stale updates all pass.

- [ ] **Step 7: Commit conditional application**

```bash
git add src/server/transactions/data_site.rs src/server/transactions/occ_tests.rs
git commit -m "fix(txn): prevent stale certified writes"
```

### Task 6: Make distributed prepare failure cleanup race-free

**Files:**
- Modify: `src/server/transactions/manager.rs`
- Modify: `src/server/transactions/occ_tests.rs`

- [ ] **Step 1: Add a failing prepare aggregation unit test**

Extract result reduction into a pure helper and test that it consumes all results while retaining the first failure:

```rust
#[test]
fn prepare_result_reduction_waits_for_all_and_returns_first_failure() {
    let results = vec![
        Ok(DMPrepareResult::Success),
        Ok(DMPrepareResult::NotRealizable),
        Ok(DMPrepareResult::Success),
    ];
    assert_eq!(
        TransactionManager::reduce_prepare_results(results).unwrap(),
        DMPrepareResult::NotRealizable
    );
}
```

- [ ] **Step 2: Run the reducer test to verify RED**

Run:

```bash
cargo test --lib prepare_result_reduction_ -- --nocapture
```

Expected: compilation fails because the reducer does not exist.

- [ ] **Step 3: Await every launched prepare before returning a failure**

Change `sites_prepare` from early return inside `while let` to collection and reduction:

```rust
let results: Vec<Result<DMPrepareResult, TMError>> = prepare_futures.collect().await;
Self::reduce_prepare_results(results)
```

Implement:

```rust
fn reduce_prepare_results<I>(results: I) -> Result<DMPrepareResult, TMError>
where
    I: IntoIterator<Item = Result<DMPrepareResult, TMError>>,
{
    let mut first_failure = None;
    let mut first_error = None;
    for result in results {
        match result {
            Ok(DMPrepareResult::Success) => {}
            Ok(other) if first_failure.is_none() => first_failure = Some(other),
            Ok(_) => {}
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        Err(error)
    } else {
        Ok(first_failure.unwrap_or(DMPrepareResult::Success))
    }
}
```

The public `prepare` wrapper already calls `abort` on any non-success, so after all prepare RPCs settle the abort reaches every participant in `affected_objects`.

- [ ] **Step 4: Make data-site reads stateless for read-only transactions**

Remove `get_or_create_transaction` from `prepare_read`. The read path should update the clock, inspect `meta.owner`, retry while a certified commit owns the cell, and read committed storage without creating participant transaction state. Transaction-manager state remains authoritative for whether a client may issue another read.

Add a test:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn read_only_transaction_creates_no_data_site_transaction() {
    let address = "127.0.0.1:5326";
    let group = "occ_stateless_read";
    let server = start_transaction_test_server(address, group).await;
    let manager = data_manager_for_database(&server, address, group).await;
    let id = Id::new(0, 13);
    seed_data_site_cell(&server, id);
    let tid = StandardVectorClock::from_vec(vec![(31, 1)]);

    let response = <DataManager as Service>::read(
        &manager,
        31,
        tid.clone(),
        tid.clone(),
        id,
    )
    .await;

    assert!(matches!(response.payload, TxnExecResult::Accepted(_)));
    assert!(manager.find_transaction(&tid).is_none());
    server.shutdown().await;
}
```

- [ ] **Step 5: Run cleanup and race tests**

Run:

```bash
cargo test --lib prepare_result_reduction_ -- --nocapture
cargo test --lib read_only_transaction_ -- --nocapture --test-threads=1
cargo test --lib prepare_failure_racing_ -- --nocapture --test-threads=1
cargo test --lib concurrent_end_calls_ -- --nocapture --test-threads=1
```

Expected: all pass without leaked manager/data-site transaction entries or locks.

- [ ] **Step 6: Commit distributed cleanup changes**

```bash
git add src/server/transactions/manager.rs src/server/transactions/data_site.rs src/server/transactions/occ_tests.rs
git commit -m "fix(txn): settle prepare votes before cleanup"
```

### Task 7: Update isolation documentation and run full verification

**Files:**
- Modify: `README.md`
- Modify: `agent_docs/POSIX_FUSE_BACKEND_GUIDE.md`
- Test: transaction and library test suites

- [ ] **Step 1: Update the public isolation description**

Change the README feature bullet to:

```markdown
* Version-certified OCC transactions with repeatable cell reads
```

Change the POSIX guide statement to:

```markdown
Neb provides repeatable cell reads and version-certified optimistic commits. It does not provide predicate/range phantom protection or full external linearizability.
```

Retain the guide's warning that multi-host POSIX leases and locks need a separate coordination authority.

- [ ] **Step 2: Run formatting and static checks**

Run:

```bash
cargo fmt --all -- --check
cargo check --lib
```

Expected: both exit successfully with no formatting diff or compilation errors.

- [ ] **Step 3: Run the focused transaction suite**

Run:

```bash
cargo test --lib server::transactions -- --nocapture --test-threads=1
```

Expected: all transaction manager, data-site, OCC, undo-log, and existing transaction tests pass.

- [ ] **Step 4: Run broader affected suites**

Run:

```bash
cargo test --lib ram::tiered::tests -- --test-threads=1
cargo test --lib index::full_text -- --test-threads=1
```

Expected: tiered-memory transactional tests and transaction-backed index tests pass. If the suites require unavailable external resources, record the exact skipped command and error; do not claim it passed.

- [ ] **Step 5: Inspect the final diff and isolation claims**

Run:

```bash
git diff --check
rg -n "read committed|strict serializ|lineariz|no lost update" README.md agent_docs src/server/transactions
git status --short
```

Expected: no whitespace errors; transaction comments and docs describe version-certified OCC accurately; unrelated pre-existing B-tree changes remain untouched.

- [ ] **Step 6: Commit documentation and final verification state**

```bash
git add README.md agent_docs/POSIX_FUSE_BACKEND_GUIDE.md
git commit -m "docs: describe repeatable-read OCC transactions"
```

- [ ] **Step 7: Request code review**

Invoke the `requesting-code-review` skill and review the complete range from the first implementation commit through `HEAD`, focusing on:

- stale-version rejection before writes;
- total Wait-Die ordering for concurrent vector clocks;
- lock release on every failure path;
- transaction-local absence caching;
- defensive conditional mutation and rollback behavior;
- no accidental inclusion of unrelated workspace changes.
