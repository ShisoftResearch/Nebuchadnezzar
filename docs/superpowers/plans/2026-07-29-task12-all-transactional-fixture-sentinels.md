# Task 12 All-Transactional-Fixture Sentinels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the byte-identical comparison harness so every fixture that executes transactions retains an untimed cleanup-floor sentinel, then resume the exact develop-versus-MVCC performance comparison.

**Architecture:** Reuse `OccFixture::hold_cleanup_floor` and `OccFixture::release_cleanup_floor`; add one sentinel transaction to the single-server transaction fixture, one to the single-server projected-read fixture, and retain the existing one-per-participant cluster sentinel. Apply the same public harness calls byte-for-byte to baseline and candidate, validate locally, transfer without pushing, run a three-scenario remote behavioral gate, then restart the serialized three-by-three portfolio.

**Tech Stack:** Rust 2021, Tokio, Criterion, Nebuchadnezzar transaction RPCs, shell/JQ validation, Git bundles, Linux `numactl`.

## Global Constraints

- Work only in the disposable comparison trees:
  - baseline: `/home/shisoft/Dropbox/Code/OSS Projects/Nebuchadnezzar-task12-compare/baseline/Nebuchadnezzar`
  - candidate: `/home/shisoft/Dropbox/Code/OSS Projects/Nebuchadnezzar-task12-compare/candidate/Nebuchadnezzar`
- Start from baseline `13879887f52031b595c9e051085e82b29dd99f19` and candidate `721b105b0002b6898af00d85f1e7af40ef7442f1`.
- Keep the baseline product at `a82ccd46fa6c63ddaf0cd921fc1a09ea33dec539` with Bifrost `b078ce7ae4ec0808b76eb13ab14c6966f6147688`.
- Keep the candidate product at `97f957e28925d3b0235049aec25237511bf85540` with Bifrost `0a53f1951d6f0d216364f87265620bb9d47ab85c`.
- Keep Dovahkiin at `98cf1fb` on both sides.
- Modify only `benches/occ_transactions.rs`; the existing cleanup-floor methods in `benches/occ_support/fixture.rs` are already sufficient.
- Keep these five harness files byte-identical:
  - `benches/occ_transactions.rs`
  - `benches/occ_support/mod.rs`
  - `benches/occ_support/fixture.rs`
  - `benches/occ_support/metrics.rs`
  - `benches/occ_support/workloads.rs`
- Do not modify product source, dependency pointers, scenario bodies, retry rules, invariants, distributed phases, sample settings, or comparator thresholds.
- Sentinel setup, hold, release, and drops remain outside every `iter_custom` closure and outside returned benchmark elapsed time.
- Run tests and debugging locally. Use `192.168.10.87` only for benchmarks.
- Run one heavy command at a time.
- Transfer comparison commits with Git bundles; do not push.
- Preserve both remote failed-run evidence directories.
- Never edit, stage, or reformat the eleven user-owned ranged-index files in the native MVCC worktree.

---

### Task 1: Protect, verify, and commit every transactional fixture

**Files:**
- Modify: `benches/occ_transactions.rs`
- Reuse unchanged: `benches/occ_support/fixture.rs`

**Interfaces:**
- Consumes: `OccFixture::hold_cleanup_floor(&self, ids: &[Id]) -> TxnId`
- Consumes: `OccFixture::release_cleanup_floor(&self, tid: TxnId)`
- Produces: retained `transaction_cleanup_floor_tid`, `projected_cleanup_floor_tid`, and existing `cluster_cleanup_floor_tid`

- [ ] **Step 1: Verify the starting commits and clean disposable trees**

Run in each comparison tree:

```bash
git rev-parse HEAD
git status --short
```

Expected: baseline prints `13879887f52031b595c9e051085e82b29dd99f19`,
candidate prints `721b105b0002b6898af00d85f1e7af40ef7442f1`, and both status outputs are
empty.

- [ ] **Step 2: Run the structural RED check in the baseline**

```bash
rg -q 'transaction_cleanup_floor_tid' benches/occ_transactions.rs &&
rg -q 'projected_cleanup_floor_tid' benches/occ_transactions.rs &&
rg -q '9_800_000' benches/occ_transactions.rs &&
rg -q '10_100_000' benches/occ_transactions.rs
```

Expected: exit 1 because only the cluster cleanup floor exists.

- [ ] **Step 3: Add the transaction-fixture cleanup floor**

Immediately after the existing seed of `transaction_ids`, add:

```rust
    let transaction_cleanup_floor_ids = Arc::new(transaction_fixture.ids_for_server(
        transaction_server_id,
        1,
        9_800_000,
    ));
    seed(
        &runtime,
        &transaction_fixture,
        transaction_cleanup_floor_ids.as_ref(),
        0,
        false,
    );
    let transaction_cleanup_floor_tid = runtime.block_on(
        transaction_fixture.hold_cleanup_floor(transaction_cleanup_floor_ids.as_ref()),
    );
```

- [ ] **Step 4: Add the projected-read-fixture cleanup floor**

Immediately after the existing seed of `projected_ids`, add:

```rust
    let projected_cleanup_floor_ids = Arc::new(projected_fixture.ids_for_server(
        projected_fixture.servers[0].server_id,
        1,
        10_100_000,
    ));
    seed(
        &runtime,
        &projected_fixture,
        projected_cleanup_floor_ids.as_ref(),
        0,
        false,
    );
    let projected_cleanup_floor_tid = runtime.block_on(
        projected_fixture.hold_cleanup_floor(projected_cleanup_floor_ids.as_ref()),
    );
```

- [ ] **Step 5: Release and drop every cleanup floor after Criterion**

Replace the single release following `group.finish()` with:

```rust
    runtime.block_on(cluster_fixture.release_cleanup_floor(cluster_cleanup_floor_tid));
    runtime.block_on(projected_fixture.release_cleanup_floor(projected_cleanup_floor_tid));
    runtime.block_on(transaction_fixture.release_cleanup_floor(transaction_cleanup_floor_tid));

    drop(hlc_source);
    drop(cluster_cleanup_floor_ids);
    drop(projected_cleanup_floor_ids);
    drop(transaction_cleanup_floor_ids);
```

Keep the existing measured-ID drops and fixture shutdowns after this block.

- [ ] **Step 6: Run the structural GREEN and formatting checks**

```bash
rg -q 'transaction_cleanup_floor_tid' benches/occ_transactions.rs &&
rg -q 'projected_cleanup_floor_tid' benches/occ_transactions.rs &&
rg -q '9_800_000' benches/occ_transactions.rs &&
rg -q '10_100_000' benches/occ_transactions.rs

rustfmt --edition 2021 --check \
  benches/occ_transactions.rs \
  benches/occ_support/mod.rs \
  benches/occ_support/fixture.rs \
  benches/occ_support/metrics.rs \
  benches/occ_support/workloads.rs

git diff --check
git diff --name-only
```

Expected: all commands exit 0; the only modified path is
`benches/occ_transactions.rs`.

- [ ] **Step 7: Apply the identical three-hunk change to candidate**

Use `apply_patch` in the candidate tree. Insert this transaction-fixture block
after the existing seed of `transaction_ids`:

```rust
    let transaction_cleanup_floor_ids = Arc::new(transaction_fixture.ids_for_server(
        transaction_server_id,
        1,
        9_800_000,
    ));
    seed(
        &runtime,
        &transaction_fixture,
        transaction_cleanup_floor_ids.as_ref(),
        0,
        false,
    );
    let transaction_cleanup_floor_tid = runtime.block_on(
        transaction_fixture.hold_cleanup_floor(transaction_cleanup_floor_ids.as_ref()),
    );
```

Insert this projected-read-fixture block after the existing seed of
`projected_ids`:

```rust
    let projected_cleanup_floor_ids = Arc::new(projected_fixture.ids_for_server(
        projected_fixture.servers[0].server_id,
        1,
        10_100_000,
    ));
    seed(
        &runtime,
        &projected_fixture,
        projected_cleanup_floor_ids.as_ref(),
        0,
        false,
    );
    let projected_cleanup_floor_tid = runtime.block_on(
        projected_fixture.hold_cleanup_floor(projected_cleanup_floor_ids.as_ref()),
    );
```

Replace the single release after `group.finish()` with:

```rust
    runtime.block_on(cluster_fixture.release_cleanup_floor(cluster_cleanup_floor_tid));
    runtime.block_on(projected_fixture.release_cleanup_floor(projected_cleanup_floor_tid));
    runtime.block_on(transaction_fixture.release_cleanup_floor(transaction_cleanup_floor_tid));

    drop(hlc_source);
    drop(cluster_cleanup_floor_ids);
    drop(projected_cleanup_floor_ids);
    drop(transaction_cleanup_floor_ids);
```

Expected: candidate modifies only `benches/occ_transactions.rs`.

- [ ] **Step 8: Prove byte identity**

```bash
for path in \
  benches/occ_transactions.rs \
  benches/occ_support/mod.rs \
  benches/occ_support/fixture.rs \
  benches/occ_support/metrics.rs \
  benches/occ_support/workloads.rs
do
  cmp --silent "baseline/Nebuchadnezzar/$path" \
    "candidate/Nebuchadnezzar/$path"
done
```

Expected: exit 0 with no output.

**Files:**
- Test: `benches/occ_transactions.rs`
- Generated, do not commit:
  - `target/occ-bench/task12-all-fixtures-baseline-local.json`
  - `target/occ-bench/task12-all-fixtures-candidate-local.json`

**Verification interfaces:**
- Consumes: the three retained cleanup-floor transaction IDs introduced in
  Steps 3-5
- Produces: two clean exact-13 local reports and one commit per comparison side

- [ ] **Step 9: Run the baseline local test-mode benchmark**

```bash
NEB_OCC_BENCH_LABEL=task12-all-fixtures-baseline-local \
NEB_OCC_BENCH_REVISION='product-a82ccd46fa6c63ddaf0cd921fc1a09ea33dec539+harness-all-fixtures+bifrost-b078ce7ae4ec0808b76eb13ab14c6966f6147688' \
  cargo bench --bench occ_transactions -- --test
```

Expected: exit 0 and Criterion `Success` for all 13 scenarios.

- [ ] **Step 10: Strictly validate the baseline JSON**

```bash
jq -e '
  (.scenarios | length == 13) and
  (all(.scenarios[];
    .committed > 0 and
    .attempts >= .committed and
    .invariants_passed == true and
    (.unexpected | length) == 0
  ))
' target/occ-bench/task12-all-fixtures-baseline-local.json
```

Expected: `true`, exit 0.

- [ ] **Step 11: Run the candidate local test-mode benchmark**

```bash
NEB_OCC_BENCH_LABEL=task12-all-fixtures-candidate-local \
NEB_OCC_BENCH_REVISION='product-97f957e28925d3b0235049aec25237511bf85540+harness-all-fixtures+bifrost-0a53f1951d6f0d216364f87265620bb9d47ab85c' \
  cargo bench --bench occ_transactions -- --test
```

Expected: exit 0 and Criterion `Success` for all 13 scenarios.

- [ ] **Step 12: Strictly validate the candidate JSON**

```bash
jq -e '
  (.scenarios | length == 13) and
  (all(.scenarios[];
    .committed > 0 and
    .attempts >= .committed and
    .invariants_passed == true and
    (.unexpected | length) == 0
  ))
' target/occ-bench/task12-all-fixtures-candidate-local.json
```

Expected: `true`, exit 0.

- [ ] **Step 13: Recheck formatting, scope, and byte identity**

Run the Task 1 Step 6 checks in both trees and the Task 1 Step 8 identity loop.
Expected: all exit 0; each tree modifies exactly
`benches/occ_transactions.rs`.

- [ ] **Step 14: Commit baseline**

```bash
git add benches/occ_transactions.rs
git commit -m "fix(bench): protect every transactional fixture"
```

Expected: one-file commit whose parent is
`13879887f52031b595c9e051085e82b29dd99f19`.

- [ ] **Step 15: Commit candidate**

```bash
git add benches/occ_transactions.rs
git commit -m "fix(bench): protect every transactional fixture"
```

Expected: one-file commit whose parent is
`721b105b0002b6898af00d85f1e7af40ef7442f1`.

- [ ] **Step 16: Audit both commits**

```bash
git show --check --stat --oneline HEAD
git diff-tree --no-commit-id --name-only -r HEAD
git status --short
```

Expected: no whitespace errors, exactly one changed path
(`benches/occ_transactions.rs`), and empty status.

---

### Task 2: Review and transfer the harness commits

**Files:**
- Review: the Task 1 commit on each comparison side
- Generated, do not commit:
  - `transfer/baseline-all-fixtures.bundle`
  - `transfer/candidate-all-fixtures.bundle`

**Interfaces:**
- Consumes: clean reviewed commits from Task 1
- Produces: remote detached worktrees at those exact commits

- [ ] **Step 1: Obtain an independent READY review**

The reviewer must verify:

- exact one-file scope on each side;
- byte identity of all five harness files;
- one sentinel per single-server transactional fixture;
- one sentinel per cluster participant;
- setup before Criterion registration;
- releases after `group.finish()` and before shutdown;
- no measured scenario or distributed phase changed; and
- both strict exact-13 local reports are clean.

Expected: READY with no Critical, Important, or Minor findings.

- [ ] **Step 2: Create and verify Git bundles**

In baseline:

```bash
git bundle create ../../transfer/baseline-all-fixtures.bundle HEAD
git bundle verify ../../transfer/baseline-all-fixtures.bundle
```

In candidate:

```bash
git bundle create ../../transfer/candidate-all-fixtures.bundle HEAD
git bundle verify ../../transfer/candidate-all-fixtures.bundle
```

Expected: each bundle contains the corresponding exact `HEAD` and verifies.

- [ ] **Step 3: Transfer bundles without pushing**

```bash
scp transfer/baseline-all-fixtures.bundle \
  transfer/candidate-all-fixtures.bundle \
  '192.168.10.87:/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/transfer/'
```

Expected: exit 0.

- [ ] **Step 4: Fetch distinct refs and update clean remote worktrees**

After confirming `git status --short` is empty in both remote task worktrees,
run:

```bash
git -C '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/baseline/Nebuchadnezzar' \
  fetch \
  '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/transfer/baseline-all-fixtures.bundle' \
  HEAD:refs/task12/baseline-all-fixtures
git -C '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/baseline/Nebuchadnezzar' \
  checkout --detach refs/task12/baseline-all-fixtures

git -C '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/candidate/Nebuchadnezzar' \
  fetch \
  '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/transfer/candidate-all-fixtures.bundle' \
  HEAD:refs/task12/candidate-all-fixtures
git -C '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/candidate/Nebuchadnezzar' \
  checkout --detach refs/task12/candidate-all-fixtures
```

Expected: remote `rev-parse HEAD` matches the two reviewed commits and both
statuses remain empty.

---

### Task 3: Run the remote three-fixture behavioral GREEN gate

**Files:**
- Generated, do not commit:
  - baseline `target/occ-bench/develop-all-fixtures-check.json`
  - candidate `target/occ-bench/mvcc-all-fixtures-check.json`
  - remote logs with matching labels

**Interfaces:**
- Consumes: exact remote commits from Task 2
- Produces: two exact-three clean reports that authorize full portfolio runs

- [ ] **Step 1: Confirm the host is idle**

```bash
ssh 192.168.10.87 \
  'pgrep -af "cargo bench|occ_transactions|rustc" || true; df -h /home/shisoft'
```

Expected: no benchmark/compiler process and sufficient free disk.

- [ ] **Step 2: Run the baseline gate**

From the remote baseline worktree:

```bash
set -o pipefail
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR='/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-build' \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL=develop-all-fixtures-check \
  NEB_OCC_BENCH_REVISION="product-a82ccd46fa6c63ddaf0cd921fc1a09ea33dec539+harness-$(git rev-parse HEAD)+bifrost-b078ce7ae4ec0808b76eb13ab14c6966f6147688" \
  cargo bench --bench occ_transactions -- \
  '^mvcc/(rmw_multi_cell|partial_read|multi_participant)$' \
  2>&1 | tee \
  '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/logs/develop-all-fixtures-check.log'
```

Expected: exit 0 after all three scenarios complete.

- [ ] **Step 3: Strictly validate the baseline gate**

```bash
jq -e '
  (.scenarios | keys | sort) ==
    (["mvcc/multi_participant", "mvcc/partial_read", "mvcc/rmw_multi_cell"] | sort)
  and
  all(.scenarios[];
    .committed > 0 and
    .attempts >= .committed and
    .invariants_passed == true and
    (.unexpected | length) == 0
  )
' target/occ-bench/develop-all-fixtures-check.json
```

Expected: `true`, exit 0.

- [ ] **Step 4: Run the candidate gate**

From the remote candidate worktree:

```bash
set -o pipefail
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR='/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-build' \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL=mvcc-all-fixtures-check \
  NEB_OCC_BENCH_REVISION="product-97f957e28925d3b0235049aec25237511bf85540+harness-$(git rev-parse HEAD)+bifrost-0a53f1951d6f0d216364f87265620bb9d47ab85c" \
  cargo bench --bench occ_transactions -- \
  '^mvcc/(rmw_multi_cell|partial_read|multi_participant)$' \
  2>&1 | tee \
  '/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/logs/mvcc-all-fixtures-check.log'
```

Expected: exit 0 after all three scenarios complete.

- [ ] **Step 5: Strictly validate the candidate gate**

```bash
jq -e '
  (.scenarios | keys | sort) ==
    (["mvcc/multi_participant", "mvcc/partial_read", "mvcc/rmw_multi_cell"] | sort)
  and
  all(.scenarios[];
    .committed > 0 and
    .attempts >= .committed and
    .invariants_passed == true and
    (.unexpected | length) == 0
  )
' target/occ-bench/mvcc-all-fixtures-check.json
```

Expected: `true`, exit 0.

- [ ] **Step 6: Append RED/GREEN evidence and obtain READY review**

Append the second preserved baseline failure and the new exact-three GREEN
results to `.superpowers/sdd/task-12-compat-harness-report.md`. A fresh reviewer
must verify revision provenance, full logs, exact-three JSON, preserved failure
evidence, and unchanged distributed phases.

Expected: READY before any six-run acceptance sequence starts.

---

### Task 4: Restart the accept-grade comparison

**Files:**
- Generated remotely, do not commit:
  - `target/occ-bench/develop-{1,2,3}.json`
  - `target/occ-bench/mvcc-{1,2,3}.json`
  - logs with matching labels
- Use unchanged: `scripts/compare-mvcc-benchmarks.sh`

**Interfaces:**
- Consumes: READY behavioral gate from Task 3
- Produces: stable three-by-three throughput and p99 comparison

- [ ] **Step 1: Run three serialized baseline portfolios**

Run the following command three times from the remote baseline worktree,
setting `RUN_LABEL` to `develop-1`, `develop-2`, and `develop-3` in three
separate command invocations:

```bash
RUN_LABEL=develop-1
set -o pipefail
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR='/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-build' \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL="$RUN_LABEL" \
  NEB_OCC_BENCH_REVISION="product-a82ccd46fa6c63ddaf0cd921fc1a09ea33dec539+harness-$(git rev-parse HEAD)+bifrost-b078ce7ae4ec0808b76eb13ab14c6966f6147688" \
  cargo bench --bench occ_transactions -- \
  '^mvcc/(non_transactional_read|non_transactional_update|read_only_current|rmw_one_cell|rmw_multi_cell|multi_participant|blind_update|blind_remove|full_read|selected_read|head_read|partial_read|hlc_contention)$' \
  2>&1 | tee \
  "/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/logs/$RUN_LABEL.log"
```

Expected: each process exits 0 and writes exactly 13 clean scenarios.

- [ ] **Step 2: Run three serialized candidate portfolios**

Run the following command three times from the remote candidate worktree,
setting `RUN_LABEL` to `mvcc-1`, `mvcc-2`, and `mvcc-3` in three separate
command invocations:

```bash
RUN_LABEL=mvcc-1
set -o pipefail
numactl --cpunodebind=0 --membind=0 env \
  CARGO_TARGET_DIR='/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-build' \
  NEB_OCC_BENCH_BASE_PORT=39400 \
  NEB_OCC_BENCH_LABEL="$RUN_LABEL" \
  NEB_OCC_BENCH_REVISION="product-97f957e28925d3b0235049aec25237511bf85540+harness-$(git rev-parse HEAD)+bifrost-0a53f1951d6f0d216364f87265620bb9d47ab85c" \
  cargo bench --bench occ_transactions -- \
  '^mvcc/(non_transactional_read|non_transactional_update|read_only_current|rmw_one_cell|rmw_multi_cell|multi_participant|blind_update|blind_remove|full_read|selected_read|head_read|partial_read|hlc_contention)$' \
  2>&1 | tee \
  "/home/shisoft/Code/OSS Projects/Nebuchadnezzar/target/occ-bench/task12-compare/logs/$RUN_LABEL.log"
```

Expected: each process exits 0 and writes exactly 13 clean scenarios.

- [ ] **Step 3: Strictly validate all six reports**

For every report, require:

```jq
(.scenarios | length == 13) and
all(.scenarios[];
  .committed > 0 and
  .attempts >= .committed and
  .invariants_passed == true and
  (.unexpected | length) == 0
)
```

Expected: all six checks print `true` and exit 0.

- [ ] **Step 4: Run the strict comparator**

Copy the six JSON reports to the native MVCC worktree without changing source,
then run:

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

Expected: matching scenario inventory is accepted. Record every CV,
throughput delta, and p99 delta. Do not weaken correctness or remove a
distributed phase in response to a regression.

- [ ] **Step 5: Record Task 12 evidence**

Append exact revisions, commands, report validation, comparator output, and
the measured regression decision to the Task 12 evidence report. Generated
JSON, Criterion output, logs, bundles, and host configuration remain
uncommitted.

Expected: Task 12 has reproducible correctness and performance evidence, after
which the existing Task 13 local acceptance plan resumes.
