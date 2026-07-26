use crate::ram::types::Id;
use lightning::list::LinkedRingBufferList;
use lightning::map::{Map, PtrHashMap};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const STATE_MASK: usize = 0b111;
const LOCATION_MASK: usize = !STATE_MASK;
const WORKER_MAX_PARK_MS: u64 = 50;
const HISTORY_MAP_INITIAL_CAPACITY: usize = 4_096;

static PROCESS_EPOCH: OnceLock<Instant> = OnceLock::new();

#[cfg(test)]
static ACTIVE_HISTORY_WORKERS: AtomicUsize = AtomicUsize::new(0);

fn monotonic_ms() -> u64 {
    PROCESS_EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevisionState {
    PendingPresent = 0,
    PendingDeleted = 1,
    CommittedPresent = 2,
    CommittedDeleted = 3,
    Aborted = 4,
    Expired = 5,
}

impl RevisionState {
    fn from_tag(tag: usize) -> Self {
        match tag {
            0 => Self::PendingPresent,
            1 => Self::PendingDeleted,
            2 => Self::CommittedPresent,
            3 => Self::CommittedDeleted,
            4 => Self::Aborted,
            5 => Self::Expired,
            invalid => panic!("invalid revision state tag: {invalid}"),
        }
    }

    #[inline]
    fn is_prunable(self) -> bool {
        matches!(self, Self::Aborted | Self::Expired)
    }
}

#[derive(Debug)]
pub struct RevisionNode {
    pub revision_ts: u64,
    state_and_location: AtomicUsize,
    pub entry_size: u32,
    retire_deadline_ms: AtomicU64,
}

impl RevisionNode {
    pub fn new(revision_ts: u64, state: RevisionState, location: usize, entry_size: u32) -> Self {
        assert_eq!(
            location & STATE_MASK,
            0,
            "revision entry addresses must be 8-byte aligned"
        );
        Self {
            revision_ts,
            state_and_location: AtomicUsize::new(location | state as usize),
            entry_size,
            retire_deadline_ms: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn load(&self) -> (RevisionState, usize) {
        let raw = self.state_and_location.load(Ordering::Acquire);
        (
            RevisionState::from_tag(raw & STATE_MASK),
            raw & LOCATION_MASK,
        )
    }

    pub fn compare_exchange_state(&self, expected: RevisionState, next: RevisionState) -> bool {
        let mut raw = self.state_and_location.load(Ordering::Acquire);
        loop {
            if RevisionState::from_tag(raw & STATE_MASK) != expected {
                return false;
            }
            let next_raw = (raw & LOCATION_MASK) | next as usize;
            match self.state_and_location.compare_exchange_weak(
                raw,
                next_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => raw = actual,
            }
        }
    }

    fn schedule_retirement(&self, deadline_ms: u64) -> bool {
        self.retire_deadline_ms
            .compare_exchange(0, deadline_ms.max(1), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn expire(&self) -> Option<DeadRevision> {
        let mut raw = self.state_and_location.load(Ordering::Acquire);
        loop {
            let state = RevisionState::from_tag(raw & STATE_MASK);
            if state == RevisionState::Expired {
                return None;
            }
            let location = raw & LOCATION_MASK;
            let expired_raw = location | RevisionState::Expired as usize;
            match self.state_and_location.compare_exchange_weak(
                raw,
                expired_raw,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(DeadRevision {
                        location,
                        entry_size: self.entry_size,
                    });
                }
                Err(actual) => raw = actual,
            }
        }
    }

    #[cfg(test)]
    fn set_state_tag_for_test(&self, tag: usize) {
        let location = self.state_and_location.load(Ordering::Relaxed) & LOCATION_MASK;
        self.state_and_location
            .store(location | (tag & STATE_MASK), Ordering::Release);
    }
}

pub struct RevisionChain {
    revisions: LinkedRingBufferList<Option<Arc<RevisionNode>>, 32>,
    truncated_before_ts: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpirationReadiness {
    Ready,
    Blocked,
    AlreadyRemoved,
}

impl RevisionChain {
    pub fn new() -> Self {
        Self {
            revisions: LinkedRingBufferList::new(),
            truncated_before_ts: AtomicU64::new(0),
        }
    }

    pub fn push_front(&self, node: Arc<RevisionNode>) {
        self.revisions.push_front(Some(node));
    }

    pub fn resolve(&self, snapshot_ts: u64) -> SnapshotRevision {
        for revision_ref in self.revisions.iter_front() {
            let Some(node) = revision_ref.deref().flatten() else {
                continue;
            };
            if node.revision_ts >= snapshot_ts {
                continue;
            }
            match node.load().0 {
                RevisionState::PendingPresent | RevisionState::PendingDeleted => {
                    return SnapshotRevision::Wait;
                }
                RevisionState::CommittedPresent => {
                    return SnapshotRevision::Present(node);
                }
                RevisionState::CommittedDeleted => {
                    return SnapshotRevision::Deleted(node);
                }
                RevisionState::Aborted | RevisionState::Expired => {}
            }
        }

        let truncated_before_ts = self.truncated_before_ts.load(Ordering::Acquire);
        if truncated_before_ts != 0 && snapshot_ts <= truncated_before_ts {
            SnapshotRevision::TooOld
        } else {
            SnapshotRevision::NeverExisted
        }
    }

    fn current(&self) -> Option<Arc<RevisionNode>> {
        self.revisions
            .peek_front()
            .and_then(|revision_ref| revision_ref.deref())
            .flatten()
    }

    fn is_current(&self, node: &Arc<RevisionNode>) -> bool {
        self.current()
            .is_some_and(|current| Arc::ptr_eq(&current, node))
    }

    fn oldest(&self) -> Option<Arc<RevisionNode>> {
        self.revisions
            .peek_back()
            .and_then(|revision_ref| revision_ref.deref())
            .flatten()
    }

    fn prune_retired_suffix(&self) {
        let mut pruned = false;
        loop {
            let Some(oldest) = self.oldest() else {
                break;
            };
            if self.is_current(&oldest) || !oldest.load().0.is_prunable() {
                break;
            }

            match self.revisions.pop_back() {
                Some(Some(removed)) => {
                    debug_assert!(Arc::ptr_eq(&removed, &oldest));
                    pruned = true;
                }
                Some(None) => {}
                None => break,
            }
        }

        if pruned {
            if let Some(oldest_remaining) = self.oldest() {
                self.truncated_before_ts
                    .fetch_max(oldest_remaining.revision_ts, Ordering::AcqRel);
            }
        }
    }

    fn prepare_expiration(&self, expiring: &Arc<RevisionNode>) -> ExpirationReadiness {
        let mut found_expiring = false;
        let mut blocked_by_older = false;
        for revision_ref in self.revisions.iter_back() {
            let Some(node) = revision_ref.deref().flatten() else {
                continue;
            };
            if Arc::ptr_eq(&node, expiring) {
                found_expiring = true;
                continue;
            }
            let state = node.load().0;
            if state.is_prunable() {
                continue;
            }
            if !found_expiring {
                blocked_by_older = true;
                continue;
            }
            if blocked_by_older {
                return ExpirationReadiness::Blocked;
            }
            self.truncated_before_ts
                .fetch_max(node.revision_ts, Ordering::AcqRel);
            return ExpirationReadiness::Ready;
        }
        if found_expiring {
            ExpirationReadiness::Blocked
        } else {
            ExpirationReadiness::AlreadyRemoved
        }
    }

    #[cfg(test)]
    fn expire_oldest_for_test(&self) {
        let Some(oldest) = self.oldest() else {
            return;
        };
        if self.is_current(&oldest) {
            return;
        }
        let _ = oldest.expire();
        self.prune_retired_suffix();
    }
}

impl Default for RevisionChain {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum SnapshotRevision {
    Present(Arc<RevisionNode>),
    Deleted(Arc<RevisionNode>),
    NeverExisted,
    Wait,
    TooOld,
}

impl SnapshotRevision {
    #[cfg(test)]
    fn revision_ts(&self) -> Option<u64> {
        match self {
            Self::Present(node) | Self::Deleted(node) => Some(node.revision_ts),
            Self::NeverExisted | Self::Wait | Self::TooOld => None,
        }
    }
}

#[derive(Clone)]
struct ExpirationRecord {
    chain: Arc<RevisionChain>,
    node: Arc<RevisionNode>,
    deadline_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeadRevision {
    pub location: usize,
    pub entry_size: u32,
}

pub struct HistoryIndex {
    chains: PtrHashMap<Id, Arc<RevisionChain>>,
    expirations: LinkedRingBufferList<Option<ExpirationRecord>, 64>,
    dead: LinkedRingBufferList<Option<DeadRevision>, 64>,
    retention_ms: u64,
    recovery_floor: AtomicU64,
    stopped: AtomicBool,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl HistoryIndex {
    pub fn new(retention_ms: u64) -> Arc<Self> {
        Self::new_named(None, retention_ms)
    }

    pub(crate) fn new_for_chunk(chunk_id: usize, retention_ms: u64) -> Arc<Self> {
        Self::new_named(Some(chunk_id), retention_ms)
    }

    fn new_named(chunk_id: Option<usize>, retention_ms: u64) -> Arc<Self> {
        let history = Arc::new(Self {
            chains: PtrHashMap::with_capacity(HISTORY_MAP_INITIAL_CAPACITY),
            expirations: LinkedRingBufferList::new(),
            dead: LinkedRingBufferList::new(),
            retention_ms,
            recovery_floor: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            worker: Mutex::new(None),
        });
        Self::start_worker(&history, chunk_id);
        history
    }

    fn start_worker(history: &Arc<Self>, chunk_id: Option<usize>) {
        let weak_history = Arc::downgrade(history);
        let thread_name = chunk_id
            .map(|id| format!("mvcc-history-{id}"))
            .unwrap_or_else(|| "mvcc-history".to_owned());
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                #[cfg(test)]
                let _worker_count = ActiveWorkerGuard::new();

                loop {
                    let Some(history) = weak_history.upgrade() else {
                        break;
                    };
                    if history.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    let park_ms = history.expire_due(monotonic_ms());
                    if history.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    drop(history);
                    thread::park_timeout(Duration::from_millis(park_ms));
                }
            })
            .expect("failed to start MVCC history worker");
        *history.worker.lock() = Some(handle);
    }

    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    pub fn chain(&self, id: &Id) -> Option<Arc<RevisionChain>> {
        self.chains.get(id)
    }

    pub fn get_or_create_chain(&self, id: Id) -> Arc<RevisionChain> {
        let candidate = Arc::new(RevisionChain::new());
        match self.chains.try_insert(id, candidate.clone()) {
            None => candidate,
            Some(existing) => existing,
        }
    }

    pub fn resolve(&self, id: &Id, snapshot_ts: u64) -> SnapshotRevision {
        if let Some(chain) = self.chain(id) {
            return chain.resolve(snapshot_ts);
        }
        let recovery_floor = self.recovery_floor.load(Ordering::Acquire);
        if recovery_floor != 0 && snapshot_ts <= recovery_floor {
            SnapshotRevision::TooOld
        } else {
            SnapshotRevision::NeverExisted
        }
    }

    pub fn set_recovery_floor(&self, revision_ts: u64) {
        self.recovery_floor.fetch_max(revision_ts, Ordering::AcqRel);
    }

    pub fn recovery_floor(&self) -> u64 {
        self.recovery_floor.load(Ordering::Acquire)
    }

    pub fn retire(&self, chain: &Arc<RevisionChain>, node: &Arc<RevisionNode>) -> bool {
        if chain.is_current(node) {
            return false;
        }
        let deadline_ms = monotonic_ms().saturating_add(self.retention_ms).max(1);
        if !node.schedule_retirement(deadline_ms) {
            return false;
        }
        self.expirations.push_front(Some(ExpirationRecord {
            chain: chain.clone(),
            node: node.clone(),
            deadline_ms,
        }));
        self.wake_worker();
        true
    }

    fn expire_due(&self, now_ms: u64) -> u64 {
        let mut next_park_ms = WORKER_MAX_PARK_MS;
        let mut deferred = Vec::new();
        while let Some(expiration) = self.expirations.pop_back() {
            let Some(expiration) = expiration else {
                continue;
            };
            if expiration.deadline_ms > now_ms {
                next_park_ms = next_park_ms.min(expiration.deadline_ms - now_ms);
                deferred.push(expiration);
                continue;
            }
            if !self.expire_record(&expiration) {
                deferred.push(expiration);
                next_park_ms = 1;
            }
        }
        for expiration in deferred {
            self.expirations.push_front(Some(expiration));
        }
        next_park_ms.max(1)
    }

    fn expire_record(&self, expiration: &ExpirationRecord) -> bool {
        self.expire_record_with_hook(expiration, || {})
    }

    fn expire_record_with_hook<F>(&self, expiration: &ExpirationRecord, after_expire: F) -> bool
    where
        F: FnOnce(),
    {
        if expiration.chain.is_current(&expiration.node) {
            return false;
        }
        // Ready publishes the TooOld boundary before making a suffix node
        // invisible. AlreadyRemoved means an earlier suffix prune published
        // that monotonic boundary before removing this scheduled node.
        let readiness = expiration.chain.prepare_expiration(&expiration.node);
        if matches!(readiness, ExpirationReadiness::Blocked) {
            return false;
        }
        // The retrying tagged-word CAS preserves a concurrently updated aligned
        // location and makes dead accounting single-winner.
        let dead = expiration.node.expire();
        after_expire();
        if let Some(dead) = dead {
            self.dead.push_front(Some(dead));
        }
        if matches!(readiness, ExpirationReadiness::Ready) {
            expiration.chain.prune_retired_suffix();
        }
        true
    }

    pub(crate) fn pop_dead(&self) -> Option<DeadRevision> {
        self.dead.pop_back().flatten()
    }

    fn wake_worker(&self) {
        if let Some(handle) = self.worker.lock().as_ref() {
            handle.thread().unpark();
        }
    }

    pub(crate) fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        let handle = self.worker.lock().take();
        if let Some(handle) = handle {
            handle.thread().unpark();
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }

    #[cfg(test)]
    fn expire_due_for_test(&self, now_ms: u64) {
        let _ = self.expire_due(now_ms);
    }

    #[cfg(test)]
    fn active_workers_for_test() -> usize {
        ACTIVE_HISTORY_WORKERS.load(Ordering::Acquire)
    }
}

impl Drop for HistoryIndex {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
struct ActiveWorkerGuard;

#[cfg(test)]
impl ActiveWorkerGuard {
    fn new() -> Self {
        ACTIVE_HISTORY_WORKERS.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

#[cfg(test)]
impl Drop for ActiveWorkerGuard {
    fn drop(&mut self) {
        ACTIVE_HISTORY_WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::chunk::Chunks;
    use crate::ram::segs::SEGMENT_SIZE;
    use crate::ram::types::Id;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    fn node(revision_ts: u64, state: RevisionState, location: usize) -> Arc<RevisionNode> {
        Arc::new(RevisionNode::new(revision_ts, state, location, 64))
    }

    fn wait_for_worker_count(expected: usize) {
        let timeout = Instant::now() + Duration::from_secs(2);
        while HistoryIndex::active_workers_for_test() != expected {
            assert!(
                Instant::now() < timeout,
                "history worker count did not reach {expected}; current count is {}",
                HistoryIndex::active_workers_for_test()
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn strict_snapshot_selects_newest_committed_revision_below_boundary() {
        let chain = RevisionChain::new();
        chain.push_front(node(300, RevisionState::CommittedPresent, 0x3000));
        chain.push_front(node(400, RevisionState::CommittedPresent, 0x4000));

        assert_eq!(chain.resolve(400).revision_ts(), Some(300));
        assert_eq!(chain.resolve(401).revision_ts(), Some(400));
    }

    #[test]
    fn pending_head_makes_first_read_wait() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        chain.push_front(node(200, RevisionState::PendingPresent, 0x2000));

        assert!(matches!(chain.resolve(300), SnapshotRevision::Wait));
    }

    #[test]
    fn pending_revision_at_snapshot_boundary_is_invisible() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        chain.push_front(node(200, RevisionState::PendingPresent, 0x2000));

        assert_eq!(chain.resolve(200).revision_ts(), Some(100));
    }

    #[test]
    fn aborted_revision_is_never_selected() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        chain.push_front(node(200, RevisionState::Aborted, 0x2000));

        assert_eq!(chain.resolve(250).revision_ts(), Some(100));
    }

    #[test]
    fn committed_tombstone_resolves_as_deleted() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedDeleted, 0x1000));

        assert!(matches!(chain.resolve(101), SnapshotRevision::Deleted(_)));
    }

    #[test]
    fn pruned_suffix_reports_snapshot_too_old() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        chain.expire_oldest_for_test();

        assert!(matches!(chain.resolve(150), SnapshotRevision::TooOld));
        assert!(matches!(chain.resolve(200), SnapshotRevision::TooOld));
        assert_eq!(chain.resolve(201).revision_ts(), Some(200));
    }

    #[test]
    fn untruncated_chain_reports_never_existed_before_first_revision() {
        let chain = RevisionChain::new();
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));

        assert!(matches!(chain.resolve(100), SnapshotRevision::NeverExisted));
    }

    #[test]
    fn state_transition_preserves_aligned_location() {
        let revision = node(100, RevisionState::PendingPresent, 0x1000);

        assert!(revision.compare_exchange_state(
            RevisionState::PendingPresent,
            RevisionState::CommittedPresent,
        ));
        assert_eq!(revision.load(), (RevisionState::CommittedPresent, 0x1000));
    }

    #[test]
    fn unaligned_revision_location_is_rejected() {
        assert!(catch_unwind(|| {
            RevisionNode::new(100, RevisionState::CommittedPresent, 0x1001, 64)
        })
        .is_err());
    }

    #[test]
    fn invalid_state_tag_panics_instead_of_becoming_visible() {
        let revision = node(100, RevisionState::CommittedPresent, 0x1000);
        revision.set_state_tag_for_test(6);

        assert!(catch_unwind(AssertUnwindSafe(|| revision.load())).is_err());
    }

    #[test]
    fn history_index_reuses_one_chain_per_cell() {
        let history = HistoryIndex::new(300_000);
        let id = Id::new(1, 2);

        let first = history.get_or_create_chain(id.clone());
        let second = history.get_or_create_chain(id);

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn concurrent_first_creation_returns_the_single_published_chain() {
        const THREADS: usize = 64;

        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let id = Id::new(3, 4);
        let start = Arc::new(Barrier::new(THREADS));
        let callers: Vec<_> = (0..THREADS)
            .map(|_| {
                let history = history.clone();
                let id = id.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    history.get_or_create_chain(id)
                })
            })
            .collect();
        let returned: Vec<_> = callers
            .into_iter()
            .map(|caller| caller.join().expect("chain creator panicked"))
            .collect();
        let published = history.chain(&id).expect("published chain");

        assert!(
            returned.iter().all(|chain| Arc::ptr_eq(chain, &published)),
            "every first creator must receive the map's single published chain"
        );
    }

    #[test]
    fn current_node_is_never_scheduled_for_retirement() {
        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let current = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(current.clone());

        assert!(!history.retire(&chain, &current));
        history.expire_due_for_test(u64::MAX);
        assert_eq!(current.load(), (RevisionState::CommittedPresent, 0x1000));
        assert!(history.pop_dead().is_none());
    }

    #[test]
    fn expiration_enqueues_dead_revision_once_and_prunes_suffix() {
        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(oldest.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));

        assert!(history.retire(&chain, &oldest));
        history.expire_due_for_test(u64::MAX);
        history.expire_due_for_test(u64::MAX);

        let dead = history.pop_dead().expect("expired revision must be dead");
        assert_eq!(dead.location, 0x1000);
        assert_eq!(dead.entry_size, 64);
        assert!(history.pop_dead().is_none());
        assert!(matches!(chain.resolve(150), SnapshotRevision::TooOld));
        assert_eq!(chain.resolve(201).revision_ts(), Some(200));
    }

    #[test]
    fn expiration_never_exposes_a_false_never_existed_window() {
        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(oldest.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        let expiration = ExpirationRecord {
            chain: chain.clone(),
            node: oldest,
            deadline_ms: 1,
        };

        let expired = history.expire_record_with_hook(&expiration, || {
            assert!(matches!(chain.resolve(150), SnapshotRevision::TooOld));
        });
        assert!(expired);
    }

    #[test]
    fn middle_revision_is_not_expired_before_older_suffix_is_prunable() {
        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        let middle = node(200, RevisionState::CommittedPresent, 0x2000);
        chain.push_front(middle.clone());
        chain.push_front(node(300, RevisionState::CommittedPresent, 0x3000));
        let expiration = ExpirationRecord {
            chain: chain.clone(),
            node: middle.clone(),
            deadline_ms: 1,
        };

        let expired = history.expire_record(&expiration);

        assert!(!expired);
        assert_eq!(middle.load(), (RevisionState::CommittedPresent, 0x2000));
        assert_eq!(chain.resolve(250).revision_ts(), Some(200));
        assert!(history.pop_dead().is_none());
    }

    #[test]
    fn due_middle_revision_is_requeued_until_oldest_suffix_expires() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(100, RevisionState::CommittedPresent, 0x1000);
        let middle = node(200, RevisionState::CommittedPresent, 0x2000);
        chain.push_front(oldest.clone());
        chain.push_front(middle.clone());
        chain.push_front(node(300, RevisionState::CommittedPresent, 0x3000));

        assert!(history.retire(&chain, &middle));
        assert!(history.retire(&chain, &oldest));
        history.expire_due_for_test(u64::MAX);

        assert_eq!(oldest.load().0, RevisionState::Expired);
        assert_eq!(middle.load().0, RevisionState::CommittedPresent);
        assert_eq!(chain.resolve(250).revision_ts(), Some(200));

        history.expire_due_for_test(u64::MAX);

        assert_eq!(middle.load().0, RevisionState::Expired);
        assert!(matches!(chain.resolve(250), SnapshotRevision::TooOld));
        assert_eq!(history.pop_dead().expect("oldest dead").location, 0x1000);
        assert_eq!(history.pop_dead().expect("middle dead").location, 0x2000);
        assert!(history.pop_dead().is_none());
    }

    #[test]
    fn pruned_aborted_record_completes_without_requeueing() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(200, RevisionState::CommittedPresent, 0x2000);
        let aborted = node(300, RevisionState::Aborted, 0x3000);
        chain.push_front(oldest.clone());
        chain.push_front(aborted.clone());
        chain.push_front(node(400, RevisionState::CommittedPresent, 0x4000));

        // FIFO expiration processes 200 first, whose suffix prune also removes 300.
        assert!(history.retire(&chain, &oldest));
        assert!(history.retire(&chain, &aborted));
        let first_park_ms = history.expire_due(u64::MAX);

        assert_eq!(oldest.load(), (RevisionState::Expired, 0x2000));
        assert_eq!(aborted.load(), (RevisionState::Expired, 0x3000));
        assert_eq!(first_park_ms, WORKER_MAX_PARK_MS);
        assert!(matches!(chain.resolve(350), SnapshotRevision::TooOld));
        assert_eq!(chain.resolve(401).revision_ts(), Some(400));

        let mut dead_locations = vec![
            history.pop_dead().expect("oldest dead").location,
            history.pop_dead().expect("aborted dead").location,
        ];
        dead_locations.sort_unstable();
        assert_eq!(dead_locations, vec![0x2000, 0x3000]);
        assert!(history.pop_dead().is_none());

        assert_eq!(history.expire_due(u64::MAX), WORKER_MAX_PARK_MS);
        assert!(history.pop_dead().is_none());
        assert!(history.expirations.pop_back().is_none());
    }

    #[test]
    fn worker_expires_due_retirement_without_owning_the_index() {
        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(oldest.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));

        assert!(history.retire(&chain, &oldest));
        let timeout = Instant::now() + Duration::from_secs(2);
        let dead = loop {
            if let Some(dead) = history.pop_dead() {
                break dead;
            }
            assert!(
                Instant::now() < timeout,
                "history worker did not expire a due revision"
            );
            thread::sleep(Duration::from_millis(5));
        };

        assert_eq!(oldest.load().0, RevisionState::Expired);
        assert!(matches!(chain.resolve(150), SnapshotRevision::TooOld));
        assert_eq!(
            dead,
            DeadRevision {
                location: 0x1000,
                entry_size: 64,
            }
        );
    }

    #[test]
    fn chunk_drain_marks_expired_entry_dead_exactly_once() {
        let chunks = Chunks::new_dummy(1, SEGMENT_SIZE);
        let chunk = &chunks.list[0];
        let segment = chunk
            .segs
            .get(&(chunk.get_head_seg_id() as usize))
            .expect("bootstrap segment");
        let before = segment
            .dead_space
            .load(std::sync::atomic::Ordering::Relaxed);
        let chain = Arc::new(RevisionChain::new());
        let oldest = node(100, RevisionState::CommittedPresent, segment.addr + 0x1000);
        chain.push_front(oldest.clone());
        chain.push_front(node(
            200,
            RevisionState::CommittedPresent,
            segment.addr + 0x2000,
        ));

        assert!(chunk.history.retire(&chain, &oldest));
        chunk.history.expire_due_for_test(u64::MAX);
        chunk.drain_history_dead();
        chunk.drain_history_dead();

        assert_eq!(
            segment
                .dead_space
                .load(std::sync::atomic::Ordering::Relaxed),
            before + 64
        );
    }

    #[test]
    fn chunk_construction_starts_one_weak_worker_per_chunk_and_drop_joins_them() {
        let baseline = HistoryIndex::active_workers_for_test();
        {
            let chunks = Chunks::new_dummy(2, SEGMENT_SIZE);
            wait_for_worker_count(baseline + 2);
            assert_eq!(chunks.list.len(), 2);
            for chunk in &chunks.list {
                assert_eq!(chunk.history.retention_ms(), 300_000);
            }
        }
        wait_for_worker_count(baseline);
    }
}
