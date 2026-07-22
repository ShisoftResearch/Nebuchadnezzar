use super::external;
use crate::client;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;

lazy_static! {
    // Initialize to MAX to indicate "nothing processed yet"
    // This prevents the race where counter=1, progress=0 looks like "operation 0 done"
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(usize::MAX);
    // Number of live write-back workers. Workers are tokio tasks: they die
    // with the runtime that spawned them (e.g. when a test's runtime is torn
    // down), so liveness must be tracked, not just "started once". Each
    // worker holds a WorkerAlive guard whose Drop decrements this counter —
    // including on task cancellation — letting a later server instance
    // respawn workers on its own runtime.
    static ref WB_ALIVE: AtomicUsize = AtomicUsize::new(0);
    // Guards against two threads spawning worker fleets concurrently.
    static ref WB_SPAWNING: AtomicBool = AtomicBool::new(false);
    // Flag to signal the write-back tasks to stop
    static ref WB_SHOULD_STOP: AtomicBool = AtomicBool::new(false);
    // Track completed operations that finished ahead of earlier queue ids.
    static ref OUT_OF_ORDER_COMPLETIONS: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
}

fn write_back_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

// Decrements the live-worker count when a worker ends for any reason,
// including cancellation when its runtime is dropped.
struct WorkerAlive;

impl WorkerAlive {
    fn register() -> Self {
        WB_ALIVE.fetch_add(1, Ordering::AcqRel);
        WorkerAlive
    }
}

impl Drop for WorkerAlive {
    fn drop(&mut self) {
        WB_ALIVE.fetch_sub(1, Ordering::AcqRel);
    }
}

// Records the change id when dropped. Progress advances only through
// contiguous ids, so an id popped from the queue MUST be recorded even if
// persisting it panics or the worker task is cancelled mid-await; a single
// unrecorded id stalls wait_until_updated forever.
struct CompletionGuard {
    id: usize,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        record_completed_change(self.id);
    }
}

fn record_completed_change(id: usize) {
    let mut completions = OUT_OF_ORDER_COMPLETIONS
        .lock()
        .expect("write-back completion lock poisoned");
    let current = CHANGE_PROGRESS.load(Ordering::Acquire);
    let expected = if current == usize::MAX {
        0
    } else {
        current + 1
    };

    if id == expected {
        let mut new_progress = id;
        loop {
            let next_expected = new_progress + 1;
            if completions.remove(&next_expected) {
                new_progress = next_expected;
            } else {
                break;
            }
        }
        CHANGE_PROGRESS.store(new_progress, Ordering::Release);
    } else if id > expected {
        completions.insert(id);
    }
}

async fn process_change(changing: external::ChangingNode) {
    match changing {
        external::ChangingNode::Modified(modified) => {
            modified
                .node
                .persist(&modified.deletion, &modified.client)
                .await;
        }
        external::ChangingNode::DeletedWithClient(cell_id, client) => {
            match client.remove_cell(cell_id).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(
                        "write-back: cell removal rejected for {:?}: {:?}",
                        cell_id, e
                    );
                }
                Err(e) => {
                    error!(
                        "write-back: cell removal RPC error for {:?}: {:?}",
                        cell_id, e
                    );
                }
            }
        }
    }
}

pub fn start_external_nodes_write_back(_client: &Arc<client::AsyncClient>) {
    // Respawn whenever no worker is alive (first start, or the previous
    // fleet died with its runtime). WB_SPAWNING keeps concurrent callers
    // from spawning two fleets.
    if WB_ALIVE.load(Ordering::Acquire) > 0 {
        return;
    }
    if WB_SPAWNING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    if WB_ALIVE.load(Ordering::Acquire) > 0 {
        WB_SPAWNING.store(false, Ordering::SeqCst);
        return;
    }

    // Reset the stop flag
    WB_SHOULD_STOP.store(false, Ordering::SeqCst);

    for worker_id in 0..write_back_worker_count() {
        let alive = WorkerAlive::register();
        tokio::spawn(async move {
            let _alive = alive;
            debug!("B-tree write-back worker {} started", worker_id);
            loop {
                if WB_SHOULD_STOP.load(Ordering::SeqCst) {
                    debug!("B-tree write-back worker {} stopping", worker_id);
                    break;
                }

                match external::CHANGED_NODES.pop() {
                    Some((id, changing)) => {
                        let _completion = CompletionGuard { id };
                        if let Err(e) =
                            AssertUnwindSafe(process_change(changing)).catch_unwind().await
                        {
                            let msg = e
                                .downcast_ref::<&str>()
                                .map(|s| s.to_string())
                                .or_else(|| e.downcast_ref::<String>().cloned())
                                .unwrap_or_else(|| "<non-string panic>".to_string());
                            error!(
                                "write-back worker {}: persisting change {} panicked: {}",
                                worker_id, id, msg
                            );
                        }
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                }
            }
            debug!("B-tree write-back worker {} stopped", worker_id);
        });
    }
    WB_SPAWNING.store(false, Ordering::SeqCst);
}

/// Reset write-back state for server restart
/// This should be called during server shutdown after wait_until_updated()
pub async fn reset_write_back_state() {
    // Signal the task to stop
    WB_SHOULD_STOP.store(true, Ordering::SeqCst);

    // Wait briefly to allow workers to see the stop signal and exit.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Reset progress to initial state
    CHANGE_PROGRESS.store(usize::MAX, Ordering::SeqCst);
    OUT_OF_ORDER_COMPLETIONS
        .lock()
        .expect("write-back completion lock poisoned")
        .clear();

    // Reset the counter
    external::CHANGE_COUNTER.store(0, Ordering::SeqCst);

    // Drain any remaining items from the queue (they should already be processed)
    while external::CHANGED_NODES.pop().is_some() {}

    debug!("B-tree write-back state reset");
}

pub async fn wait_until_updated() {
    let counter = external::CHANGE_COUNTER.load(Ordering::Acquire);
    if counter == 0 {
        return;
    }
    let newest = counter - 1; // The ID of the most recent operation (fetch_add returns old value)
    let progress = CHANGE_PROGRESS.load(Ordering::Acquire);

    // If progress is MAX (initial value), nothing has been processed yet
    // If progress < newest, there are pending operations
    let has_pending = progress == usize::MAX || progress < newest;
    if !has_pending {
        return;
    }
    if WB_ALIVE.load(Ordering::Acquire) == 0 {
        warn!(
            "wait_until_updated: {} change(s) pending but no write-back worker is alive; \
             returning without waiting",
            if progress == usize::MAX {
                newest + 1
            } else {
                newest - progress
            }
        );
        return;
    }

    let ops_remaining = if progress == usize::MAX {
        newest + 1 // All operations are pending
    } else {
        newest - progress
    };
    debug!("Waiting storage, {} ops to go", ops_remaining);
    loop {
        let current = CHANGE_PROGRESS.load(Ordering::Acquire);
        // If current is still MAX, nothing processed yet
        // Otherwise, wait until current >= newest
        if current != usize::MAX && current >= newest {
            break;
        }
        if WB_ALIVE.load(Ordering::Acquire) == 0 {
            warn!("wait_until_updated: write-back workers died while waiting; giving up");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    debug!("Write back updated, {} cells", ops_remaining);
}
