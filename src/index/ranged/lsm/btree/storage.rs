use super::external;
use crate::client;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

lazy_static! {
    // Initialize to MAX to indicate "nothing processed yet"
    // This prevents the race where counter=1, progress=0 looks like "operation 0 done"
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(usize::MAX);
    // Flag to indicate whether the write-back task is running
    static ref WB_STARTED: AtomicBool = AtomicBool::new(false);
    // Flag to signal the write-back task to stop
    static ref WB_SHOULD_STOP: AtomicBool = AtomicBool::new(false);
}

pub fn start_external_nodes_write_back(client: &Arc<client::AsyncClient>) {
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

    let client = client.clone();
    tokio::spawn(async move {
        debug!("B-tree write-back task started");
        loop {
            // Check if we should stop
            if WB_SHOULD_STOP.load(Ordering::SeqCst) {
                debug!("B-tree write-back task stopping");
                break;
            }

            while let Some((id, changing)) = external::CHANGED_NODES.pop() {
                match changing {
                    external::ChangingNode::Modified(modified) => {
                        modified.node.persist(&modified.deletion, &client).await;
                    }
                    external::ChangingNode::Deleted(id) => {
                        let _ = client.remove_cell(id).await.unwrap();
                    }
                }
                CHANGE_PROGRESS.store(id, Ordering::Release);

                // Check if we should stop between operations
                if WB_SHOULD_STOP.load(Ordering::SeqCst) {
                    debug!("B-tree write-back task stopping (mid-operation)");
                    break;
                }
            }

            if WB_SHOULD_STOP.load(Ordering::SeqCst) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        debug!("B-tree write-back task stopped");
    });
}

/// Reset write-back state for server restart
/// This should be called during server shutdown after wait_until_updated()
pub async fn reset_write_back_state() {
    // Signal the task to stop
    WB_SHOULD_STOP.store(true, Ordering::SeqCst);

    // Wait briefly to allow the old task to see the stop signal and exit
    // The task checks every 500ms, so wait a bit longer
    tokio::time::sleep(Duration::from_millis(600)).await;

    // Reset the started flag so a new task can be started on restart
    WB_STARTED.store(false, Ordering::SeqCst);

    // Reset progress to initial state
    CHANGE_PROGRESS.store(usize::MAX, Ordering::SeqCst);

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
