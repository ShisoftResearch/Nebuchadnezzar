use super::external;
use crate::client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static mut WB_STARTED: bool = false;
lazy_static! {
    pub static ref CHANGE_PROGRESS: AtomicUsize = AtomicUsize::new(0);
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
    let newest = counter - 1; // fetch add
    let ops = newest - CHANGE_PROGRESS.load(Ordering::Acquire);
    if ops == 0 {
        return;
    }
    debug!("Waiting storage, {} ops to go", ops);
    loop {
        let current = CHANGE_PROGRESS.load(Ordering::Acquire);
        if current >= newest {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    debug!("Write back updated, {} cells", ops);
}
