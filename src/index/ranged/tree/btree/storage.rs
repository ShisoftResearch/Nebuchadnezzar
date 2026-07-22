use super::external;
use crate::client;
use crossbeam::queue::SegQueue;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::FutureExt;

// Write-back state is scoped per server instance, keyed by the identity of
// the server's AsyncClient. Sharing one process-wide queue meant two servers
// in one process waited on each other's progress and a dead fleet poisoned
// every later instance.
pub struct WriteBackHub {
    queue: SegQueue<(usize, external::ChangingNode)>,
    counter: AtomicUsize,
    // usize::MAX = nothing processed yet (avoids counter=1/progress=0
    // looking like "operation 0 done").
    progress: AtomicUsize,
    // Number of live workers. Workers are tokio tasks: they die with the
    // runtime that spawned them, so liveness is tracked with RAII guards and
    // a dead fleet is respawned by the next ensure_workers call.
    alive: AtomicUsize,
    spawning: AtomicBool,
    should_stop: AtomicBool,
    // Completions that finished ahead of earlier queue ids.
    completions: Mutex<BTreeSet<usize>>,
}

lazy_static! {
    static ref HUBS: Mutex<Vec<(Weak<client::AsyncClient>, Arc<WriteBackHub>)>> =
        Mutex::new(Vec::new());
}

fn write_back_worker_count() -> usize {
    // Scale with the machine: page persistence (to_cell serialization plus a
    // cell upsert) is the drain bottleneck, and a fixed cap of 8 leaves a
    // large host almost idle while a big write-back backlog clears. Use about
    // a quarter of the cores, bounded to keep cell-store contention sane, and
    // allow an explicit override for tuning.
    if let Ok(v) = std::env::var("NEB_WRITEBACK_WORKERS") {
        if let Ok(n) = v.parse::<usize>() {
            return n.max(1);
        }
    }
    std::thread::available_parallelism()
        .map(|n| (n.get() / 4).clamp(4, 64))
        .unwrap_or(4)
}

/// The hub owning write-back state for the server behind `client`.
pub fn hub_for(client: &Arc<client::AsyncClient>) -> Arc<WriteBackHub> {
    let mut hubs = HUBS.lock().expect("write-back hub registry poisoned");
    hubs.retain(|(c, _)| c.strong_count() > 0);
    for (c, hub) in hubs.iter() {
        if let Some(existing) = c.upgrade() {
            if Arc::ptr_eq(&existing, client) {
                return hub.clone();
            }
        }
    }
    let hub = Arc::new(WriteBackHub {
        queue: SegQueue::new(),
        counter: AtomicUsize::new(0),
        progress: AtomicUsize::new(usize::MAX),
        alive: AtomicUsize::new(0),
        spawning: AtomicBool::new(false),
        should_stop: AtomicBool::new(false),
        completions: Mutex::new(BTreeSet::new()),
    });
    hubs.push((Arc::downgrade(client), hub.clone()));
    hub
}

fn all_hubs() -> Vec<Arc<WriteBackHub>> {
    HUBS.lock()
        .expect("write-back hub registry poisoned")
        .iter()
        .map(|(_, hub)| hub.clone())
        .collect()
}

// Decrements the live-worker count when a worker ends for any reason,
// including cancellation when its runtime is dropped.
struct WorkerAlive {
    hub: Arc<WriteBackHub>,
}

impl Drop for WorkerAlive {
    fn drop(&mut self) {
        self.hub.alive.fetch_sub(1, Ordering::AcqRel);
    }
}

// Records the change id when dropped. Progress advances only through
// contiguous ids, so an id popped from the queue MUST be recorded even if
// persisting it panics or the worker task is cancelled mid-await; a single
// unrecorded id stalls wait_until_updated forever.
struct CompletionGuard {
    hub: Arc<WriteBackHub>,
    id: usize,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.hub.record_completed(self.id);
    }
}

impl WriteBackHub {
    pub fn push(&self, changing: external::ChangingNode) {
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        self.queue.push((id, changing));
    }

    fn record_completed(&self, id: usize) {
        let mut completions = self
            .completions
            .lock()
            .expect("write-back completion lock poisoned");
        let current = self.progress.load(Ordering::Acquire);
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
            self.progress.store(new_progress, Ordering::Release);
        } else if id > expected {
            completions.insert(id);
        }
    }

    /// Spawn the worker fleet on the caller's runtime if none is alive.
    pub fn ensure_workers(self: &Arc<Self>) {
        if self.alive.load(Ordering::Acquire) > 0 {
            return;
        }
        if self
            .spawning
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        if self.alive.load(Ordering::Acquire) > 0 {
            self.spawning.store(false, Ordering::SeqCst);
            return;
        }
        self.should_stop.store(false, Ordering::SeqCst);
        for worker_id in 0..write_back_worker_count() {
            self.alive.fetch_add(1, Ordering::AcqRel);
            let hub = self.clone();
            tokio::spawn(async move {
                let _alive = WorkerAlive { hub: hub.clone() };
                debug!("B-tree write-back worker {} started", worker_id);
                loop {
                    if hub.should_stop.load(Ordering::SeqCst) {
                        debug!("B-tree write-back worker {} stopping", worker_id);
                        break;
                    }
                    match hub.queue.pop() {
                        Some((id, changing)) => {
                            let _completion = CompletionGuard {
                                hub: hub.clone(),
                                id,
                            };
                            if let Err(e) = AssertUnwindSafe(process_change(changing))
                                .catch_unwind()
                                .await
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
        self.spawning.store(false, Ordering::SeqCst);
    }

    pub async fn wait_until_updated(&self) {
        let counter = self.counter.load(Ordering::Acquire);
        if counter == 0 {
            return;
        }
        let newest = counter - 1;
        let progress = self.progress.load(Ordering::Acquire);
        let has_pending = progress == usize::MAX || progress < newest;
        if !has_pending {
            return;
        }
        if self.alive.load(Ordering::Acquire) == 0 {
            warn!(
                "wait_until_updated: {} change(s) pending but no write-back worker \
                 is alive on this hub; returning without waiting",
                if progress == usize::MAX {
                    newest + 1
                } else {
                    newest - progress
                }
            );
            return;
        }
        loop {
            let current = self.progress.load(Ordering::Acquire);
            if current != usize::MAX && current >= newest {
                break;
            }
            if self.alive.load(Ordering::Acquire) == 0 {
                warn!("wait_until_updated: write-back workers died while waiting; giving up");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn reset(&self) {
        self.should_stop.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        self.progress.store(usize::MAX, Ordering::SeqCst);
        self.completions
            .lock()
            .expect("write-back completion lock poisoned")
            .clear();
        self.counter.store(0, Ordering::SeqCst);
        while self.queue.pop().is_some() {}
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

pub fn start_external_nodes_write_back(client: &Arc<client::AsyncClient>) {
    hub_for(client).ensure_workers();
}

/// Wait until every live hub has drained its pending changes.
pub async fn wait_until_updated() {
    for hub in all_hubs() {
        hub.wait_until_updated().await;
    }
}

/// Reset write-back state for server restart.
/// This should be called during server shutdown after wait_until_updated().
pub async fn reset_write_back_state() {
    for hub in all_hubs() {
        hub.reset().await;
    }
    HUBS.lock()
        .expect("write-back hub registry poisoned")
        .clear();
    debug!("B-tree write-back state reset");
}
