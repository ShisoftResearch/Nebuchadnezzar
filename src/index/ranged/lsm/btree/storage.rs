use super::external;
use crate::client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static mut WB_STARTED: bool = false;
lazy_static! {
    // Initialize to MAX to indicate "nothing processed yet"
    // This prevents the race where counter=1, progress=0 looks like "operation 0 done"
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(usize::MAX);
}

pub fn start_external_nodes_write_back(client: &Arc<client::AsyncClient>) {
    let client = client.clone();
    tokio::spawn(async move {
        loop {
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
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
    unsafe {
        WB_STARTED = true;
    }
}

pub async fn wait_until_updated() {
    unsafe {
        if !WB_STARTED {
            return;
        }
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
        newest + 1  // All operations are pending
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
