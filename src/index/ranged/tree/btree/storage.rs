use super::external;
use crate::client;
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

pub fn start_external_nodes_write_back(_client: &Arc<client::AsyncClient>) {
    let _ = ensure_write_back_runtime();
}

pub async fn wait_until_updated() {
    if WRITE_BACK_RUNTIME.get().is_none() {
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
