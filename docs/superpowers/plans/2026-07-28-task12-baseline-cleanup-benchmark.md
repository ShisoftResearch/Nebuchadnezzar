# Task 12 Baseline Cleanup Benchmark Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the exact develop baseline stable under the distributed benchmark without patching product code or weakening the 13-scenario regression gate.

**Architecture:** Add an untimed, read-only sentinel transaction to the shared three-server comparison fixture. Selected reads of one dedicated cell per participant keep the baseline cleanup watermark older than measured hot-cell metadata; the harness aborts the sentinel after Criterion and applies the same calls byte-for-byte to both products.

**Tech Stack:** Rust 2021, Bifrost RPC, Nebuchadnezzar transaction client, Criterion, Python JSON validation, Git worktrees.

## Global Constraints

- Modify only the five comparison harness files in the disposable baseline and candidate trees.
- Keep the final five files byte-identical across both trees.
- Do not change either product parent or dependency revision.
- Keep all 13 nonhistorical scenarios and all distributed phases.
- Exclude sentinel setup and teardown from measured elapsed.
- Run heavy commands sequentially.
- Use local execution for correctness and `192.168.10.87` only for benchmarks.

---

### Task 1: Anchor the Cluster Cleanup Watermark

**Files:**

- Modify: `benches/occ_support/fixture.rs`
- Modify: `benches/occ_transactions.rs`
- Verify unchanged identity: `benches/occ_support/mod.rs`
- Verify unchanged identity: `benches/occ_support/metrics.rs`
- Verify unchanged identity: `benches/occ_support/workloads.rs`

**Interfaces:**

- Produces:
  `OccFixture::hold_cleanup_floor(&self, ids: &[Id]) -> TxnId`.
- Produces:
  `OccFixture::release_cleanup_floor(&self, tid: TxnId)`.
- Consumes the existing transaction client `begin`, `read_selected`, and
  `abort` RPCs.

- [ ] **Step 1: Run the structural RED check**

Run a bounded Python source check that requires:

```text
hold_cleanup_floor
release_cleanup_floor
cluster_cleanup_floor_ids
cluster_cleanup_floor_tid
group.finish()
release_cleanup_floor(cluster_cleanup_floor_tid)
```

It must also verify the release call occurs after `group.finish()` and before
`shutdown_fixture(&runtime, cluster_fixture)`.

Expected: exit 1 because the current harness has no sentinel transaction.

- [ ] **Step 2: Add the fixture lifecycle**

Import `TxnId` and add:

```rust
pub async fn hold_cleanup_floor(&self, ids: &[Id]) -> TxnId {
    assert_eq!(
        ids.len(),
        self.servers.len(),
        "cleanup floor requires one sentinel per participant"
    );
    let tid = self
        .txn
        .begin()
        .await
        .expect("begin cleanup-floor RPC")
        .expect("begin cleanup-floor transaction");
    for id in ids {
        let result = self
            .txn
            .read_selected(tid, *id, vec![bifrost_hasher::hash_str("score")])
            .await;
        assert!(
            matches!(result, Ok(Ok(TxnExecResult::Accepted(_)))),
            "selected cleanup-floor read for {id:?}: {result:?}"
        );
    }
    tid
}

pub async fn release_cleanup_floor(&self, tid: TxnId) {
    let result = self.txn.abort(tid).await;
    assert!(
        matches!(result, Ok(Ok(AbortResult::Success(None)))),
        "release cleanup-floor transaction: {result:?}"
    );
}
```

- [ ] **Step 3: Install dedicated cluster sentinels**

In `compatible_portfolio`, derive one ID per cluster server from a distinct
`9_500_000 + index * 10_000` start, seed them transactionally, and retain:

```rust
let cluster_cleanup_floor_tid = runtime.block_on(
    cluster_fixture.hold_cleanup_floor(cluster_cleanup_floor_ids.as_ref()),
);
```

Create the floor before Criterion registration. After `group.finish()`, call:

```rust
runtime.block_on(
    cluster_fixture.release_cleanup_floor(cluster_cleanup_floor_tid),
);
```

Drop `cluster_cleanup_floor_ids` before unwrapping and shutting down the
cluster fixture.

- [ ] **Step 4: Run GREEN and local gates**

Re-run the structural check; expected exit 0. Then, sequentially in baseline
and candidate:

```bash
rustfmt --edition 2021 --check \
  benches/occ_transactions.rs \
  benches/occ_support/mod.rs \
  benches/occ_support/fixture.rs \
  benches/occ_support/metrics.rs \
  benches/occ_support/workloads.rs

NEB_OCC_BENCH_LABEL=<distinct-label> \
NEB_OCC_BENCH_REVISION=<exact-product-and-harness> \
  cargo bench --bench occ_transactions -- --test
```

Require exactly 13 scenarios, `invariants_passed == true`,
`unexpected == []`, positive committed counts, and attempts not below commits.
Compare all five files with `cmp`.

- [ ] **Step 5: Commit both disposable trees**

Commit the same final harness state in each tree:

```bash
git add benches/occ_transactions.rs benches/occ_support/fixture.rs
git commit -m "fix(bench): isolate baseline cleanup race"
```

- [ ] **Step 6: Run the remote behavioral GREEN gate**

Transfer the two new harness commits without pushing. Preserve the failed
`develop-1` report and log under a failure-evidence directory. On
`192.168.10.87`, sequentially run the exact filtered scenario for baseline and
candidate:

```bash
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR=<shared-task12-target> \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL=<distinct-label> \
  NEB_OCC_BENCH_REVISION=<exact-product-harness-and-bifrost> \
  cargo bench --bench occ_transactions -- 'mvcc/multi_participant'
```

Require a clean JSON report containing exactly
`mvcc/multi_participant`, with positive commits and no unexpected outcomes.

- [ ] **Step 7: Review and resume acceptance**

Append the RED/GREEN, local, remote, identity, and commit evidence to the Task
12 compatibility report. Obtain a fresh task review. Only a READY verdict may
restart `develop-1..3` and `mvcc-1..3`.
