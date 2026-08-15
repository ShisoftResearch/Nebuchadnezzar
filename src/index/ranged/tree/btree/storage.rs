use super::external;
use crate::client;
use crossbeam::queue::SegQueue;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;


// Write-back state is scoped per server instance, keyed by the identity of
// the server's AsyncClient. Sharing one process-wide queue meant two servers
// in one process waited on each other's progress and a dead fleet poisoned
// every later instance.
pub struct WriteBackHub {
    // Page upserts. Drained in batches by any worker.
    queue: SegQueue<(usize, external::ChangingNode)>,
    // Page deletions, fenced: a deletion enqueued at id D executes only
    // once progress covers every id before D — i.e. every modification
    // (and earlier deletion) enqueued before it is durable. Without the
    // fence, a batch of multi-worker drains could persist a page's removal
    // while the link rewrite that unlinks it was still queued; a crash in
    // that window left the on-disk chain pointing at deleted pages (TB16's
    // genesis tree, 3M keys stranded behind a mid-chain hole).
    deletions: Mutex<std::collections::VecDeque<(usize, crate::ram::types::Id, Arc<client::AsyncClient>)>>,
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
    // Latched when a worker abandons cells it could not persist. The ids
    // still complete (so waiters resolve), but the barrier must report
    // failure: those pages are NOT durable and nothing may publish a head
    // that names them. Cleared by reset().
    barrier_failed: AtomicBool,
    // Completions that finished ahead of earlier queue ids.
    completions: Mutex<BTreeSet<usize>>,
}

// Bounded so a permanently failing store cannot wedge shutdown: ~200
// attempts at 100ms is ~20s of genuine transient-failure tolerance.
const WRITE_BACK_MAX_ATTEMPTS: u32 = 200;

// Crash-forensics tap, disabled unless NEB_WB_ID_LOG names a file: appends
// one line per page-cell event in the flush stream (built / applied /
// rejected / deleted), so a corpse's broken chain can be diffed against
// exactly what the flusher persisted -- never-written vs written-then-lost
// vs linked-too-early. Diagnostic only; does not alter flush behavior.
fn wb_id_log(line: std::fmt::Arguments<'_>) {
    use std::io::Write;
    lazy_static! {
        static ref WB_ID_LOG: Option<Mutex<std::fs::File>> = {
            std::env::var("NEB_WB_ID_LOG").ok().and_then(|path| {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .ok()
                    .map(Mutex::new)
            })
        };
    }
    if let Some(file) = WB_ID_LOG.as_ref() {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut file = file.lock().expect("wb id log poisoned");
        let _ = writeln!(file, "{} {} {}", ms, std::process::id(), line);
    }
}

macro_rules! wb_id_log {
    ($($arg:tt)*) => {
        if std::env::var_os("NEB_WB_ID_LOG").is_some() {
            wb_id_log(format_args!($($arg)*));
        }
    };
}

lazy_static! {
    static ref HUBS: Mutex<Vec<(Weak<client::AsyncClient>, Arc<WriteBackHub>)>> =
        Mutex::new(Vec::new());
}

fn write_back_worker_count() -> usize {
    // ONE worker, deliberately. Crash consistency of the page chain rests on
    // the flush stream being a single total order whose every prefix is
    // referentially closed (dependency pull below). Parallel workers flush
    // batches in arbitrary relative order, so a cancelled fleet could
    // persist a page whose next pointer names a page another worker never
    // got to — the TB17 corpse (a fresh split's link landing without the
    // new page). The override exists for experiments only; anything > 1
    // trades that guarantee away.
    if let Ok(v) = std::env::var("NEB_WRITEBACK_WORKERS") {
        if let Ok(n) = v.parse::<usize>() {
            return n.max(1);
        }
    }
    1
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
        deletions: Mutex::new(std::collections::VecDeque::new()),
        counter: AtomicUsize::new(0),
        progress: AtomicUsize::new(usize::MAX),
        alive: AtomicUsize::new(0),
        spawning: AtomicBool::new(false),
        should_stop: AtomicBool::new(false),
        barrier_failed: AtomicBool::new(false),
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
        match changing {
            external::ChangingNode::DeletedWithClient(cid, cl) => {
                // Ids for deletions are drawn from the same counter as
                // modifications, so "progress reached id-1" means every
                // change enqueued before this deletion is durable.
                let mut deletions = self
                    .deletions
                    .lock()
                    .expect("write-back deletion lane poisoned");
                let id = self.counter.fetch_add(1, Ordering::Relaxed);
                deletions.push_back((id, cid, cl));
            }
            changing => {
                let id = self.counter.fetch_add(1, Ordering::Relaxed);
                self.queue.push((id, changing));
            }
        }
    }

    // Deletions whose fence is satisfied: every id before theirs is done.
    fn take_ready_deletions(
        &self,
    ) -> Vec<(usize, crate::ram::types::Id, Arc<client::AsyncClient>)> {
        let progress = self.progress.load(Ordering::Acquire);
        let mut deletions = self
            .deletions
            .lock()
            .expect("write-back deletion lane poisoned");
        let mut ready = Vec::new();
        while let Some(&(id, _, _)) = deletions.front() {
            let fenced = id == 0 || (progress != usize::MAX && progress >= id - 1);
            if !fenced {
                break;
            }
            ready.push(deletions.pop_front().unwrap());
        }
        ready
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
                // Group commit: drain a batch of dirty pages, serialize them
                // (each under its own node latch), and flush the whole batch
                // in one upsert_all_cells RPC — amortizing per-cell location
                // lookup and RPC round-trips across the batch.
                // One ordered flusher means batch size IS throughput: each
                // batch costs one upsert RPC, so a small batch caps the
                // drain rate and lets dirty pages pile up in memory (16k
                // pages queued in a 2s churn burst before this was raised).
                // Dirty pages are coalesced, so a large batch is bounded by
                // the working set, not by the write rate.
                const BATCH: usize = 4096;
                loop {
                    if hub.should_stop.load(Ordering::SeqCst) {
                        debug!("B-tree write-back worker {} stopping", worker_id);
                        break;
                    }
                    // CompletionGuards are held for the whole batch so the
                    // progress chain still advances even if the task is
                    // cancelled mid-flush (drop records the id).
                    let mut guards: Vec<CompletionGuard> = Vec::new();
                    let mut cells: Vec<crate::ram::cell::OwnedCell> = Vec::new();
                    let mut batch_client: Option<Arc<client::AsyncClient>> = None;
                    // Node addresses already serialized into THIS batch. Any
                    // image of a page id in the batch satisfies referential
                    // closure for pages that link to it; the refs keep those
                    // node objects alive so the addresses stay unambiguous.
                    let mut built_addrs: std::collections::HashSet<usize> =
                        std::collections::HashSet::new();
                    let mut built_refs: Vec<super::NodeCellRef> = Vec::new();
                    for _ in 0..BATCH {
                        let Some((id, changing)) = hub.queue.pop() else {
                            break;
                        };
                        let guard = CompletionGuard {
                            hub: hub.clone(),
                            id,
                        };
                        match changing {
                            external::ChangingNode::Modified(m) => {
                                if batch_client.is_none() {
                                    batch_client = Some(m.client.clone());
                                }
                                // Referential closure: a page whose next
                                // pointer names a page with NO on-disk image
                                // yet must never land first -- the reader
                                // would walk into a hole. Naming a merely
                                // dirty page is fine (its older image is
                                // still readable), so the chain we must
                                // order on is the never-persisted one, which
                                // is bounded by recent splits rather than by
                                // the whole dirty working set. Deepest first,
                                // and never truncated: a cut anywhere in this
                                // chain leaves its tail naming a hole (the
                                // 1024-cap version produced exactly that
                                // corpse at crash-churn cycle 6).
                                let mut chain = Vec::new();
                                let mut cursor = m.node.clone();
                                let mut seen = std::collections::HashSet::new();
                                while seen.insert(cursor.address()) {
                                    match cursor.unpersisted_next_ref() {
                                        Some(next) => {
                                            chain.push(cursor);
                                            cursor = next;
                                        }
                                        None => break,
                                    }
                                }
                                chain.push(cursor);
                                if chain.len() > 1 {
                                    debug!(
                                        "write-back: pulled {} unpersisted forward sibling(s) \
                                         ahead of their referrer",
                                        chain.len() - 1
                                    );
                                }
                                let mut built_this_entry: Vec<super::NodeCellRef> = Vec::new();
                                while let Some(node) = chain.pop() {
                                    if built_addrs.contains(&node.address()) {
                                        continue;
                                    }
                                    let built = std::panic::catch_unwind(AssertUnwindSafe(|| {
                                        node.build_cell(&m.deletion)
                                    }));
                                    match built {
                                        Ok(Some(cell)) => {
                                            wb_id_log!(
                                                "built id={} change={}",
                                                cell.id().bits(),
                                                id
                                            );
                                            cells.push(cell);
                                            built_addrs.insert(node.address());
                                            built_refs.push(node.clone());
                                            built_this_entry.push(node);
                                        }
                                        Ok(None) => {}
                                        Err(_) => error!(
                                            "write-back worker {}: build_cell panicked for change {}",
                                            worker_id, id
                                        ),
                                    }
                                }
                                // Close the walk/build window. The chain walk
                                // above and each node's snapshot are separate
                                // latch sections, so a concurrent split can
                                // land between them: the snapshot then names a
                                // freshly linked page the walk never saw, and
                                // that page's own queue entry sits BEHIND this
                                // batch -- a kill in the gap leaves a durable
                                // referrer naming a page with no image. Re-read
                                // every built node's CURRENT next: a
                                // still-unpersisted successor missing from this
                                // batch is serialized now and PREPENDED, so it
                                // lands before every referrer. (A split landing
                                // after a snapshot is harmless -- that snapshot
                                // still names the old, persisted sibling.)
                                for node in built_this_entry {
                                    let mut cur = node;
                                    let mut late_seen = std::collections::HashSet::new();
                                    while let Some(next) = cur.unpersisted_next_ref() {
                                        if built_addrs.contains(&next.address())
                                            || !late_seen.insert(next.address())
                                        {
                                            break;
                                        }
                                        let built =
                                            std::panic::catch_unwind(AssertUnwindSafe(|| {
                                                next.build_cell(&m.deletion)
                                            }));
                                        match built {
                                            Ok(Some(cell)) => {
                                                debug!(
                                                    "write-back: serialized late-linked \
                                                     successor {:?} ahead of its referrer",
                                                    cell.id()
                                                );
                                                wb_id_log!(
                                                    "built id={} change={} late-linked",
                                                    cell.id().bits(),
                                                    id
                                                );
                                                cells.insert(0, cell);
                                                built_addrs.insert(next.address());
                                                built_refs.push(next.clone());
                                            }
                                            Ok(None) => {}
                                            Err(_) => error!(
                                                "write-back worker {}: build_cell panicked for \
                                                 late-linked successor of change {}",
                                                worker_id, id
                                            ),
                                        }
                                        cur = next;
                                    }
                                }
                            }
                            // Deletions never enter this queue; they live in
                            // the fenced deletion lane.
                            external::ChangingNode::DeletedWithClient(cid, cl) => {
                                error!(
                                    "write-back worker {}: deletion {:?} found in the \
                                     modification lane; executing unfenced",
                                    worker_id, cid
                                );
                                let _ = cl.remove_cell(cid).await;
                            }
                        }
                        guards.push(guard);
                    }

                    let flushed_mods = !guards.is_empty();
                    if !cells.is_empty() {
                        let client = batch_client.expect("cells present implies a client");
                        // Retry transient failures INLINE, before this batch's
                        // completion guards drop. A rejected upsert used to be
                        // warn-and-forget -- but the node's dirty flag was
                        // already cleared at build time, so unless a later
                        // insert happened to touch that exact page again its
                        // current image was never flushed at all: under
                        // chunk-full pressure (CannotAllocateSpace) whole
                        // batches of page images silently evaporated, and a
                        // later crash recovered the pages at whatever version
                        // last succeeded. Retrying inline keeps the flush
                        // stream's total order (single worker, nothing later
                        // lands first), resends the SAME built images (a
                        // rebuilt snapshot could name forward siblings that
                        // are not yet in the flush stream, breaking prefix
                        // closure), and keeps the deletion fence honest: the
                        // guards for these ids are still held, so no fenced
                        // deletion can run ahead of a link rewrite that is
                        // still being retried.
                        let mut pending: Vec<crate::ram::cell::OwnedCell> = cells;
                        let mut attempt: u32 = 0;
                        loop {
                            attempt += 1;
                            let pending_len = pending.len();
                            let round = pending.clone();
                            let mut retry: Vec<crate::ram::cell::OwnedCell> = Vec::new();
                            match client.upsert_all_cells(round).await {
                                Ok(results) => {
                                    for (r, cell) in
                                        results.into_iter().zip(pending.into_iter())
                                    {
                                        match r {
                                            Ok(_) => {
                                                wb_id_log!(
                                                    "applied id={}",
                                                    cell.id().bits()
                                                );
                                            }
                                            Err(
                                                crate::ram::cell::WriteError::CannotAllocateSpace
                                                | crate::ram::cell::WriteError::BatchAborted,
                                            ) => {
                                                retry.push(cell);
                                            }
                                            Err(e) => {
                                                // Non-transient rejection: retrying
                                                // cannot fix it; keep the historical
                                                // warn-and-drop but say so loudly.
                                                wb_id_log!(
                                                    "rejected id={} err={:?}",
                                                    cell.id().bits(),
                                                    e
                                                );
                                                warn!(
                                                    "write-back batch: cell update rejected \
                                                     (not retryable): {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "write-back batch upsert RPC error (attempt {}): {:?}; \
                                         retrying the whole batch",
                                        attempt, e
                                    );
                                    retry = pending;
                                }
                            }
                            if retry.is_empty() {
                                break;
                            }
                            if hub.should_stop.load(Ordering::SeqCst) {
                                warn!(
                                    "write-back worker {} stopping with {} unflushed cell(s) \
                                     after {} attempt(s); shutdown reset owns the state now",
                                    worker_id,
                                    retry.len(),
                                    attempt
                                );
                                break;
                            }
                            if attempt % 40 == 0 {
                                error!(
                                    "write-back batch still failing after {} attempts; {} cell(s) \
                                     pending (store out of space?)",
                                    attempt,
                                    retry.len()
                                );
                            }
                            // A permanently failing store (out of space, a
                            // rejected schema) must not wedge the process:
                            // retrying forever holds the drain barrier open
                            // and a graceful shutdown never returns. Give up
                            // loudly instead -- the guards below record
                            // completion, so wait_until_updated resolves and
                            // reports an UNESTABLISHED barrier, which every
                            // publisher already treats as "do not publish
                            // heads". Data is not lost silently: the pages
                            // stay dirty in memory and the durable metadata
                            // still names the last consistent chain.
                            if attempt >= WRITE_BACK_MAX_ATTEMPTS {
                                error!(
                                    "write-back worker {} ABANDONING {} cell(s) after {} \
                                     attempts; the drain barrier will report failure and no \
                                     head pointer will be published over these pages",
                                    worker_id,
                                    retry.len(),
                                    attempt
                                );
                                hub.barrier_failed.store(true, Ordering::SeqCst);
                                // Stop the worker outright. Continuing would
                                // let later pages land on top of the hole
                                // these abandoned cells leave, and a referrer
                                // above a hole is exactly the unreadable
                                // chain this design exists to prevent.
                                hub.should_stop.store(true, Ordering::SeqCst);
                                break;
                            }
                            pending = retry;
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                    // Modification ids complete here, advancing the fence.
                    drop(guards);

                    // Deletions run only once every change enqueued before
                    // them is durable, so a crash can never persist a page
                    // removal ahead of the link rewrite that unlinks it.
                    let ready = hub.take_ready_deletions();
                    let flushed_dels = !ready.is_empty();
                    for (id, cid, cl) in ready {
                        let _guard = CompletionGuard {
                            hub: hub.clone(),
                            id,
                        };
                        match cl.remove_cell(cid).await {
                            Ok(Ok(())) => {
                                wb_id_log!("deleted id={} change={}", cid.bits(), id);
                            }
                            Ok(Err(e)) => {
                                warn!("write-back: cell removal rejected for {:?}: {:?}", cid, e)
                            }
                            Err(e) => {
                                error!("write-back: cell removal RPC error for {:?}: {:?}", cid, e)
                            }
                        }
                    }

                    if !flushed_mods && !flushed_dels {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                }
                debug!("B-tree write-back worker {} stopped", worker_id);
            });
        }
        self.spawning.store(false, Ordering::SeqCst);
    }

    /// Returns true when every change enqueued before the call is durable.
    /// False means the barrier could NOT be established (no live workers, or
    /// workers died mid-wait) — callers publishing head pointers or
    /// archiving segments must treat false as a hard failure, not a flush.
    pub async fn wait_until_updated(&self) -> bool {
        if self.barrier_failed.load(Ordering::SeqCst) {
            warn!("wait_until_updated: a worker abandoned unpersistable cells; barrier NOT established");
            return false;
        }
        let counter = self.counter.load(Ordering::Acquire);
        if counter == 0 {
            return true;
        }
        let newest = counter - 1;
        let progress = self.progress.load(Ordering::Acquire);
        let has_pending = progress == usize::MAX || progress < newest;
        if !has_pending {
            return true;
        }
        if self.alive.load(Ordering::Acquire) == 0 {
            warn!(
                "wait_until_updated: {} change(s) pending but no write-back worker \
                 is alive on this hub; barrier NOT established",
                if progress == usize::MAX {
                    newest + 1
                } else {
                    newest - progress
                }
            );
            return false;
        }
        loop {
            if self.barrier_failed.load(Ordering::SeqCst) {
                warn!("wait_until_updated: worker abandoned cells mid-wait; barrier NOT established");
                return false;
            }
            let current = self.progress.load(Ordering::Acquire);
            if current != usize::MAX && current >= newest {
                return true;
            }
            if self.alive.load(Ordering::Acquire) == 0 {
                warn!(
                    "wait_until_updated: write-back workers died while waiting; \
                     barrier NOT established"
                );
                return false;
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
        self.barrier_failed.store(false, Ordering::SeqCst);
        // Anything still queued here is a page write that will NEVER become
        // durable, while a head pointer published during shutdown may
        // already name it. That is the MissingPage corpse, so say so
        // loudly rather than dropping the work in silence.
        let mut discarded_pages = 0usize;
        while self.queue.pop().is_some() {
            discarded_pages += 1;
        }
        let discarded_deletes = {
            let mut deletions = self
                .deletions
                .lock()
                .expect("write-back deletion lane poisoned");
            let n = deletions.len();
            deletions.clear();
            n
        };
        if discarded_pages > 0 || discarded_deletes > 0 {
            error!(
                "write-back reset DISCARDED {} queued page write(s) and {} deletion(s); \
                 a head published during this shutdown may name a page that never landed",
                discarded_pages, discarded_deletes
            );
        }
    }
}

pub fn start_external_nodes_write_back(client: &Arc<client::AsyncClient>) {
    hub_for(client).ensure_workers();
}

/// Wait until every live hub has drained its pending changes. A hub whose
/// worker fleet died is respawned on the caller's runtime and retried once;
/// false means at least one hub could not establish the barrier and pending
/// page writes may not be durable.
pub async fn wait_until_updated() -> bool {
    let mut all_drained = true;
    for hub in all_hubs() {
        let mut drained = hub.wait_until_updated().await;
        if !drained {
            hub.ensure_workers();
            drained = hub.wait_until_updated().await;
        }
        all_drained &= drained;
    }
    all_drained
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::types::Id;
    use crate::server::{NebServer, ServerOptions, Service};

    /// The deletion lane must hold a page removal until every change
    /// enqueued before it has completed. This is the fence that keeps a
    /// crash from persisting a page delete ahead of the link rewrite that
    /// unlinks it (the TB16 mid-chain hole).
    #[tokio::test(flavor = "multi_thread")]
    async fn deletion_fence_waits_for_prior_ids() {
        let _ = env_logger::try_init();
        let server_addr = crate::utils::test_port::unique_localhost_addr();
        let server_group = "writeback_deletion_fence";
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 8 * 1024 * 1024,
                db_size: 8 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![Service::Cell],
                disable_storage_locks: true,
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await
        .unwrap();
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );

        // A fresh hub with NO workers: lane state is driven by hand.
        let hub = hub_for(&client);
        assert_eq!(hub.counter.load(Ordering::Acquire), 0);

        // id 0: a modification (represented directly through the counter —
        // the fence logic cares only about id completion order).
        let mod_id = hub.counter.fetch_add(1, Ordering::Relaxed);
        assert_eq!(mod_id, 0);

        // id 1: a deletion enqueued AFTER the modification.
        hub.push(external::ChangingNode::DeletedWithClient(
            Id::from_parts(42, 42),
            client.clone(),
        ));

        // The modification has not completed: the deletion must stay fenced.
        assert!(
            hub.take_ready_deletions().is_empty(),
            "a deletion must not run before changes enqueued ahead of it complete"
        );

        // Complete the modification; the fence opens.
        hub.record_completed(mod_id);
        let ready = hub.take_ready_deletions();
        assert_eq!(ready.len(), 1, "the fenced deletion must be released");
        assert_eq!(ready[0].1, Id::from_parts(42, 42));

        // Deletions themselves advance the progress chain once executed.
        hub.record_completed(ready[0].0);
        assert!(hub.wait_until_updated().await);

        server.shutdown().await;
    }
}
