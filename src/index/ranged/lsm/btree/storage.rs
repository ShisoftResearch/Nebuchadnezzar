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
            while let Ok((id, changing)) = external::CHANGED_NODES.pop() {
                match changing {
                    external::ChangingNode::Modified(modified) => {
                        modified.node.persist(&modified.deletion, &client).await;
                    }
                    external::ChangingNode::Deleted(id) => {
                        let _ = client.remove_cell(id).await.unwrap();
                    }
                }
                CHANGE_PROGRESS.store(id, Ordering::Relaxed);
            }
            tokio::time::delay_for(Duration::from_millis(500)).await;
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
    let newest = external::CHANGE_COUNTER.load(Ordering::Acquire) - 1; // fetch add
    loop {
        let current = CHANGE_PROGRESS.load(Ordering::Relaxed);
        if current >= newest {
            break;
        }
        tokio::time::delay_for(Duration::from_millis(500)).await;
    }
    debug!("Write back updated");
}
