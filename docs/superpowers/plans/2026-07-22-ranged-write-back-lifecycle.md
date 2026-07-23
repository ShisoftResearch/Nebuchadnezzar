# Ranged Write-Back Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep ranged B-tree write-back workers alive for the process lifetime so runtime teardown and individual server shutdown cannot strand migrations or discard another server's queued writes.

**Architecture:** Replace caller-runtime `tokio::spawn` workers and mutable global start/stop flags with a process-owned Tokio runtime retained in a `OnceLock`. The existing process-global queue and ordered completion watermark remain unchanged; server shutdown waits for its snapshot barrier but no longer stops or resets the shared manager.

**Tech Stack:** Rust 2021, Tokio runtime builder, `std::sync::OnceLock`, crossbeam queue, existing ranged migration pressure tests, Cargo test harness.

---

## File map

- Modify `src/index/ranged/tree/btree/storage.rs`: process-level runtime manager, durable worker loops, test probe handling, and lifecycle regression test.
- Modify `src/index/ranged/tree/btree/external.rs`: add a `cfg(test)` probe entry used only by the lifecycle test.
- Modify `src/server/mod.rs`: preserve the shutdown write-back barrier while removing destructive process-global reset.

### Task 1: Process-owned write-back manager

**Files:**
- Modify: `src/index/ranged/tree/btree/storage.rs`
- Modify: `src/index/ranged/tree/btree/external.rs`
- Modify: `src/server/mod.rs`

- [ ] **Step 1: Add the test-only probe queue entry**

In `src/index/ranged/tree/btree/external.rs`, extend `ChangingNode` with:

```rust
pub enum ChangingNode {
    DeletedWithClient(Id, Arc<AsyncClient>),
    Modified(NodeModified),
    #[cfg(test)]
    Probe(std::sync::mpsc::Sender<()>),
}
```

- [ ] **Step 2: Write the failing process-lifetime test**

At the end of `src/index/ranged/tree/btree/storage.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn write_back_manager_outlives_the_runtime_that_initialized_it() {
        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create caller runtime");
        caller_runtime.block_on(async {
            ensure_write_back_runtime();
            tokio::task::yield_now().await;
        });
        drop(caller_runtime);

        let (probe_sender, probe_receiver) = mpsc::channel();
        let change_id = external::CHANGE_COUNTER.fetch_add(1, Ordering::Relaxed);
        external::CHANGED_NODES.push((change_id, external::ChangingNode::Probe(probe_sender)));

        probe_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("process write-back manager should survive caller runtime teardown");
    }
}
```

- [ ] **Step 3: Run the focused test to verify RED**

Run:

```bash
cargo test index::ranged::tree::btree::storage::tests::write_back_manager_outlives_the_runtime_that_initialized_it -- --exact --nocapture
```

Expected: compilation fails because `ensure_write_back_runtime` does not exist and the old worker match does not handle `ChangingNode::Probe`.

- [ ] **Step 4: Introduce process-owned runtime state**

In `storage.rs`, replace the atomic imports and worker-control globals with:

```rust
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

struct WriteBackRuntime {
    _runtime: tokio::runtime::Runtime,
}

static WRITE_BACK_RUNTIME: OnceLock<WriteBackRuntime> = OnceLock::new();

lazy_static! {
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(usize::MAX);
    static ref OUT_OF_ORDER_COMPLETIONS: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
}
```

Delete `WB_STARTED` and `WB_SHOULD_STOP`. Keep `write_back_worker_count` and
`record_completed_change` unchanged.

- [ ] **Step 5: Implement durable worker loops and one-time initialization**

Add after `record_completed_change`:

```rust
async fn run_write_back_worker(worker_id: usize) {
    debug!("B-tree write-back worker {} started", worker_id);
    loop {
        match external::CHANGED_NODES.pop() {
            Some((id, changing)) => {
                match changing {
                    external::ChangingNode::Modified(modified) => {
                        modified
                            .node
                            .persist(&modified.deletion, &modified.client)
                            .await;
                    }
                    external::ChangingNode::DeletedWithClient(id, client) => {
                        if let Err(error) = client.remove_cell(id).await {
                            warn!(
                                "B-tree write-back could not delete external node {:?}: {:?}",
                                id, error
                            );
                        }
                    }
                    #[cfg(test)]
                    external::ChangingNode::Probe(sender) => {
                        let _ = sender.send(());
                    }
                }
                record_completed_change(id);
            }
            None => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
}

fn ensure_write_back_runtime() -> &'static WriteBackRuntime {
    WRITE_BACK_RUNTIME.get_or_init(|| {
        let worker_count = write_back_worker_count();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_count)
            .thread_name("neb-btree-write-back")
            .enable_all()
            .build()
            .expect("create ranged B-tree write-back runtime");
        for worker_id in 0..worker_count {
            runtime.spawn(run_write_back_worker(worker_id));
        }
        WriteBackRuntime { _runtime: runtime }
    })
}
```

- [ ] **Step 6: Replace caller-runtime startup and remove mutable reset state**

Replace `start_external_nodes_write_back` with:

```rust
pub fn start_external_nodes_write_back(_client: &Arc<client::AsyncClient>) {
    let _ = ensure_write_back_runtime();
}
```

Delete the complete `reset_write_back_state` function. At the start of
`wait_until_updated`, replace the `WB_STARTED` check with:

```rust
if WRITE_BACK_RUNTIME.get().is_none() {
    return;
}
```

- [ ] **Step 7: Make individual server shutdown a barrier, not a global reset**

In `NebServer::shutdown` in `src/server/mod.rs`, retain:

```rust
crate::index::ranged::tree::btree::storage::wait_until_updated().await;
info!("B-tree nodes write-back completed");
```

Delete only this call and its obsolete comments:

```rust
crate::index::ranged::tree::btree::storage::reset_write_back_state().await;
```

Do not change the order of index draining, LSM flushing, write-back waiting, WAL sync,
archival, Raft shutdown, or RPC shutdown.

- [ ] **Step 8: Run the focused test to verify GREEN**

Run:

```bash
cargo fmt --all
cargo test index::ranged::tree::btree::storage::tests::write_back_manager_outlives_the_runtime_that_initialized_it -- --exact --nocapture
```

Expected: one lifecycle test passes within five seconds after dropping the caller runtime.

- [ ] **Step 9: Verify each pressure test remains correct alone**

Run:

```bash
timeout 600 cargo test index::ranged::tests::general -- --exact --nocapture
timeout 600 cargo test index::ranged::tests::migration_stress_insert_only -- --exact --nocapture
```

Expected: each command exits zero with one passing test.

- [ ] **Step 10: Verify the same-process regression is fixed in both modes**

Run:

```bash
timeout 600 cargo test index::ranged::tests:: -- --test-threads=1 --nocapture
timeout 600 cargo test index::ranged::tests:: -- --nocapture
```

Expected: both commands exit zero; the serial command no longer returns the pre-fix exit
`124`, both active pressure tests pass, and the 30-minute soak remains ignored.

- [ ] **Step 11: Review and commit the production diff**

Run:

```bash
git diff --check
git diff -- src/index/ranged/tree/btree/storage.rs src/index/ranged/tree/btree/external.rs src/server/mod.rs
git status --short
```

Confirm no split, routing, migration, storage-format, OCC, Bifrost, or Dovahkiin code changed.
Then commit:

```bash
git add src/index/ranged/tree/btree/storage.rs src/index/ranged/tree/btree/external.rs src/server/mod.rs
git commit -m "fix(ranged): own write-back workers at process scope"
```

### Task 2: Clean-baseline regression gate

**Files:**
- Verify only; no source changes expected.

- [ ] **Step 1: Run formatting and compile gates**

Run:

```bash
cargo fmt --check
cargo build
git diff --check
```

Expected: all commands exit zero. Existing repository warnings may remain, but there are no
new errors.

- [ ] **Step 2: Run the complete test suite with a hard bound**

Run:

```bash
timeout 1200 cargo test
```

Expected: command exits zero, including `index::ranged::tests::general` and
`index::ranged::tests::migration_stress_insert_only`; there is no `tree is migrating` retry
exhaustion and no command timeout.

- [ ] **Step 3: Re-run the original serial regression after the full suite**

Run:

```bash
timeout 600 cargo test index::ranged::tests:: -- --test-threads=1 --nocapture
```

Expected: both pressure tests pass in the same process.

- [ ] **Step 4: Confirm branch hygiene**

Run:

```bash
git status --short --branch
git log -3 --oneline
```

Expected: the feature worktree is clean and the lifecycle repair commit follows the design
and plan commits. Do not commit generated `target` artifacts or test logs.
