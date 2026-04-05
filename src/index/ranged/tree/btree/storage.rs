use super::external;
use crate::client;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

lazy_static! {
    // Initialize to MAX to indicate "nothing processed yet"
    // This prevents the race where counter=1, progress=0 looks like "operation 0 done"
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(usize::MAX);
    // Flag to indicate whether the write-back task is running
    static ref WB_STARTED: AtomicBool = AtomicBool::new(false);
    // Flag to signal the write-back task to stop
    static ref WB_SHOULD_STOP: AtomicBool = AtomicBool::new(false);
    // Track completed operations that finished ahead of earlier queue ids.
    static ref OUT_OF_ORDER_COMPLETIONS: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());
}

fn write_back_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(2, 8))
        .unwrap_or(4)
}

fn record_completed_change(id: usize) {
    let mut completions = OUT_OF_ORDER_COMPLETIONS
        .lock()
        .expect("write-back completion lock poisoned");
    let current = CHANGE_PROGRESS.load(Ordering::Acquire);
    let expected = if current == usize::MAX { 0 } else { current + 1 };

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

pub fn start_external_nodes_write_back(_client: &Arc<client::AsyncClient>) {
    // Check if already started using atomic compare-exchange
    if WB_STARTED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        // Already started, nothing to do
        return;
    }

    // Reset the stop flag
    WB_SHOULD_STOP.store(false, Ordering::SeqCst);

    for worker_id in 0..write_back_worker_count() {
        tokio::spawn(async move {
            debug!("B-tree write-back worker {} started", worker_id);
            loop {
                if WB_SHOULD_STOP.load(Ordering::SeqCst) {
                    debug!("B-tree write-back worker {} stopping", worker_id);
                    break;
                }

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
                                let _ = client.remove_cell(id).await.unwrap();
                            }
                        }
                        record_completed_change(id);
                    }
                    None => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                }
            }
            debug!("B-tree write-back worker {} stopped", worker_id);
        });
    }
}

/// Reset write-back state for server restart
/// This should be called during server shutdown after wait_until_updated()
pub async fn reset_write_back_state() {
    // Signal the task to stop
    WB_SHOULD_STOP.store(true, Ordering::SeqCst);

    // Wait briefly to allow workers to see the stop signal and exit.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Reset the started flag so a new task can be started on restart
    WB_STARTED.store(false, Ordering::SeqCst);

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
    if !WB_STARTED.load(Ordering::Acquire) {
        return;
    }
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    debug!("Write back updated, {} cells", ops_remaining);
}
