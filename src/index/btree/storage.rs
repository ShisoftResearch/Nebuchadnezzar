use crate::client;
use crate::index::btree::external;
use std::sync::Arc;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};

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
                        client.remove_cell(id).await.unwrap().unwrap();
                    }
                }
                CHANGE_PROGRESS.store(id, Ordering::Relaxed);
            }
            tokio::time::delay_for(Duration::from_millis(500)).await;
        }
    });
}

pub async fn wait_until_updated() {
    let newest = external::CHANGE_COUNTER.load(Ordering::Acquire);
    loop {
        if CHANGE_PROGRESS.load(Ordering::Relaxed) >= newest {
            break;
        }
        tokio::time::delay_for(Duration::from_millis(500)).await;
    }
}