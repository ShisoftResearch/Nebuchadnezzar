use crate::ram::types::Id;
use lightning::list::LinkedRingBufferList;
use lightning::map::{Map, PtrHashMap};
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const STATE_MASK: usize = 0b111;
const LOCATION_MASK: usize = !STATE_MASK;
const WORKER_MAX_PARK_MS: u64 = 50;
const EXPIRATION_WORK_BUDGET: usize = 256;
const CHAIN_CANCELLATION_CHECK_INTERVAL: usize = 64;
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

    #[inline]
    pub fn is_present(self) -> bool {
        matches!(self, Self::PendingPresent | Self::CommittedPresent)
    }

    #[inline]
    pub fn is_deleted(self) -> bool {
        matches!(self, Self::PendingDeleted | Self::CommittedDeleted)
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

    fn relocate(&self, old_location: usize, new_location: usize) -> bool {
        assert_eq!(
            new_location & STATE_MASK,
            0,
            "revision entry addresses must be 8-byte aligned"
        );
        let mut raw = self.state_and_location.load(Ordering::Acquire);
        loop {
            let state = RevisionState::from_tag(raw & STATE_MASK);
            if state == RevisionState::Expired || raw & LOCATION_MASK != old_location {
                return false;
            }
            let relocated = new_location | state as usize;
            match self.state_and_location.compare_exchange_weak(
                raw,
                relocated,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => raw = actual,
            }
        }
    }

    pub fn promote(&self) -> bool {
        match self.load().0 {
            RevisionState::PendingPresent => self.compare_exchange_state(
                RevisionState::PendingPresent,
                RevisionState::CommittedPresent,
            ),
            RevisionState::PendingDeleted => self.compare_exchange_state(
                RevisionState::PendingDeleted,
                RevisionState::CommittedDeleted,
            ),
            RevisionState::CommittedPresent
            | RevisionState::CommittedDeleted
            | RevisionState::Aborted
            | RevisionState::Expired => false,
        }
    }

    pub fn abort(&self) -> bool {
        match self.load().0 {
            RevisionState::PendingPresent => {
                self.compare_exchange_state(RevisionState::PendingPresent, RevisionState::Aborted)
            }
            RevisionState::PendingDeleted => {
                self.compare_exchange_state(RevisionState::PendingDeleted, RevisionState::Aborted)
            }
            RevisionState::CommittedPresent
            | RevisionState::CommittedDeleted
            | RevisionState::Aborted
            | RevisionState::Expired => false,
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
    #[cfg(test)]
    expiration_scan_steps: AtomicUsize,
    #[cfg(test)]
    expiration_scan_pause_at: AtomicUsize,
    #[cfg(test)]
    expiration_scan_paused: AtomicBool,
    #[cfg(test)]
    expiration_prune_steps: AtomicUsize,
    #[cfg(test)]
    expiration_prune_pause_at: AtomicUsize,
    #[cfg(test)]
    expiration_prune_paused: AtomicBool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpirationReadiness {
    Ready,
    Blocked,
    AlreadyRemoved,
    Cancelled,
}

impl RevisionChain {
    pub fn new() -> Self {
        Self {
            revisions: LinkedRingBufferList::new(),
            truncated_before_ts: AtomicU64::new(0),
            #[cfg(test)]
            expiration_scan_steps: AtomicUsize::new(0),
            #[cfg(test)]
            expiration_scan_pause_at: AtomicUsize::new(0),
            #[cfg(test)]
            expiration_scan_paused: AtomicBool::new(false),
            #[cfg(test)]
            expiration_prune_steps: AtomicUsize::new(0),
            #[cfg(test)]
            expiration_prune_pause_at: AtomicUsize::new(0),
            #[cfg(test)]
            expiration_prune_paused: AtomicBool::new(false),
        }
    }

    fn push_front(&self, node: Arc<RevisionNode>) {
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

    pub(crate) fn current(&self) -> Option<Arc<RevisionNode>> {
        self.revisions
            .peek_front()
            .and_then(|revision_ref| revision_ref.deref())
            .flatten()
    }

    fn install(
        &self,
        node: Arc<RevisionNode>,
        expected_predecessor: Option<&Arc<RevisionNode>>,
    ) -> Result<Option<Arc<RevisionNode>>, ()> {
        let predecessor = self.current();
        if predecessor
            .as_ref()
            .is_some_and(|current| current.revision_ts >= node.revision_ts)
        {
            return Err(());
        }
        let predecessor_matches = match (expected_predecessor, predecessor.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(actual)) => Arc::ptr_eq(expected, actual),
            (None, Some(_)) | (Some(_), None) => false,
        };
        if !predecessor_matches {
            return Err(());
        }
        self.push_front(node);
        Ok(predecessor)
    }

    fn find(&self, revision_ts: u64) -> Option<Arc<RevisionNode>> {
        self.revisions.iter_front().find_map(|revision_ref| {
            revision_ref
                .deref()
                .flatten()
                .filter(|node| node.revision_ts == revision_ts)
        })
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
        self.prune_retired_suffix_until(&|| false);
    }

    fn prune_retired_suffix_until<F>(&self, should_cancel: &F)
    where
        F: Fn() -> bool,
    {
        let mut pruned = false;
        let mut steps = 0;
        loop {
            if steps % CHAIN_CANCELLATION_CHECK_INTERVAL == 0 && should_cancel() {
                break;
            }
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
            steps += 1;
            #[cfg(test)]
            {
                self.expiration_prune_steps.store(steps, Ordering::Release);
                if self.expiration_prune_pause_at.load(Ordering::Acquire) == steps {
                    self.expiration_prune_paused.store(true, Ordering::Release);
                    while self.expiration_prune_pause_at.load(Ordering::Acquire) == steps {
                        thread::yield_now();
                    }
                    self.expiration_prune_paused.store(false, Ordering::Release);
                }
            }
        }

        if pruned {
            if let Some(oldest_remaining) = self.oldest() {
                self.truncated_before_ts
                    .fetch_max(oldest_remaining.revision_ts, Ordering::AcqRel);
            }
        }
    }

    fn prepare_expiration_until<F>(
        &self,
        expiring: &Arc<RevisionNode>,
        should_cancel: &F,
    ) -> ExpirationReadiness
    where
        F: Fn() -> bool,
    {
        let mut found_expiring = false;
        let mut blocked_by_older = false;
        let mut scan_steps = 0;
        if should_cancel() {
            return ExpirationReadiness::Cancelled;
        }
        for revision_ref in self.revisions.iter_back() {
            scan_steps += 1;
            #[cfg(test)]
            {
                let step = self.expiration_scan_steps.fetch_add(1, Ordering::AcqRel) + 1;
                if self.expiration_scan_pause_at.load(Ordering::Acquire) == step {
                    self.expiration_scan_paused.store(true, Ordering::Release);
                    while self.expiration_scan_pause_at.load(Ordering::Acquire) == step {
                        thread::yield_now();
                    }
                    self.expiration_scan_paused.store(false, Ordering::Release);
                }
            }
            if scan_steps % CHAIN_CANCELLATION_CHECK_INTERVAL == 0 && should_cancel() {
                return ExpirationReadiness::Cancelled;
            }
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
            match state {
                RevisionState::CommittedPresent | RevisionState::CommittedDeleted => {
                    if blocked_by_older {
                        return ExpirationReadiness::Blocked;
                    }
                    self.truncated_before_ts
                        .fetch_max(node.revision_ts, Ordering::AcqRel);
                    return ExpirationReadiness::Ready;
                }
                RevisionState::PendingPresent
                | RevisionState::PendingDeleted
                | RevisionState::Aborted
                | RevisionState::Expired => {}
            }
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

enum ExpirationAttempt {
    Completed,
    Blocked,
    Cancelled,
}

#[derive(Clone)]
struct ScheduledExpiration {
    expiration: ExpirationRecord,
    sequence: u64,
}

impl PartialEq for ScheduledExpiration {
    fn eq(&self, other: &Self) -> bool {
        self.expiration.deadline_ms == other.expiration.deadline_ms
            && self.sequence == other.sequence
    }
}

impl Eq for ScheduledExpiration {}

impl PartialOrd for ScheduledExpiration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledExpiration {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap, so reverse both keys to pop the earliest
        // deadline and then the oldest insertion first.
        other
            .expiration
            .deadline_ms
            .cmp(&self.expiration.deadline_ms)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeadRevision {
    pub location: usize,
    pub entry_size: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RelocateResult {
    HistoricalMoved,
    CurrentPresentMoved,
    LostRace,
}

pub struct HistoryIndex {
    chains: PtrHashMap<Id, Arc<RevisionChain>>,
    #[cfg(test)]
    chain_map_resolutions: AtomicUsize,
    chain_creation: Mutex<()>,
    expiration_ingress: LinkedRingBufferList<Option<ScheduledExpiration>, 64>,
    expirations: Mutex<BinaryHeap<ScheduledExpiration>>,
    expiration_sequence: AtomicU64,
    #[cfg(test)]
    expiration_checks: AtomicUsize,
    #[cfg(test)]
    worker_wakes: AtomicUsize,
    #[cfg(test)]
    pause_after_worker_scan: AtomicBool,
    #[cfg(test)]
    worker_paused_after_scan: AtomicBool,
    #[cfg(test)]
    pause_after_wake_publication: AtomicBool,
    #[cfg(test)]
    worker_paused_after_wake_publication: AtomicBool,
    dead: LinkedRingBufferList<Option<DeadRevision>, 64>,
    retention_ms: u64,
    recovery_floor: AtomicU64,
    stopped: AtomicBool,
    published_worker_wake_ms: AtomicU64,
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
            #[cfg(test)]
            chain_map_resolutions: AtomicUsize::new(0),
            chain_creation: Mutex::new(()),
            expiration_ingress: LinkedRingBufferList::new(),
            expirations: Mutex::new(BinaryHeap::new()),
            expiration_sequence: AtomicU64::new(0),
            #[cfg(test)]
            expiration_checks: AtomicUsize::new(0),
            #[cfg(test)]
            worker_wakes: AtomicUsize::new(0),
            #[cfg(test)]
            pause_after_worker_scan: AtomicBool::new(false),
            #[cfg(test)]
            worker_paused_after_scan: AtomicBool::new(false),
            #[cfg(test)]
            pause_after_wake_publication: AtomicBool::new(false),
            #[cfg(test)]
            worker_paused_after_wake_publication: AtomicBool::new(false),
            dead: LinkedRingBufferList::new(),
            retention_ms,
            recovery_floor: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            published_worker_wake_ms: AtomicU64::new(0),
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
                    // Zero means the worker is active. Producers that enqueue
                    // while no wake is published must leave an unpark token for
                    // the scan-to-park race.
                    history.published_worker_wake_ms.store(0, Ordering::Release);
                    if history.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    let park_ms = history.expire_due_until(monotonic_ms(), &|| {
                        history.stopped.load(Ordering::Acquire)
                    });
                    debug_assert!((1..=WORKER_MAX_PARK_MS).contains(&park_ms));
                    #[cfg(test)]
                    history.pause_after_worker_scan_for_test();
                    if history.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    history
                        .published_worker_wake_ms
                        .store(monotonic_ms().saturating_add(park_ms), Ordering::Release);
                    #[cfg(test)]
                    history.pause_after_wake_publication_for_test();
                    if history.stopped.load(Ordering::Acquire) {
                        history.published_worker_wake_ms.store(0, Ordering::Release);
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

    fn lookup_chain(&self, id: &Id) -> Option<Arc<RevisionChain>> {
        #[cfg(test)]
        self.chain_map_resolutions.fetch_add(1, Ordering::Relaxed);
        self.chains.get(id)
    }

    pub fn chain(&self, id: &Id) -> Option<Arc<RevisionChain>> {
        self.lookup_chain(id)
    }

    fn get_or_create_chain(&self, id: Id) -> Arc<RevisionChain> {
        self.get_or_create_chain_with(id, || Arc::new(RevisionChain::new()))
    }

    fn get_or_create_chain_with<F>(&self, id: Id, create: F) -> Arc<RevisionChain>
    where
        F: FnOnce() -> Arc<RevisionChain>,
    {
        if let Some(chain) = self.lookup_chain(&id) {
            return chain;
        }

        let _creation = self.chain_creation.lock();
        if let Some(chain) = self.lookup_chain(&id) {
            return chain;
        }

        let chain = create();
        let replaced = self.chains.insert(id, chain.clone());
        debug_assert!(
            replaced.is_none(),
            "chain creation lock must serialize every history insertion"
        );
        chain
    }

    pub fn resolve(&self, id: &Id, snapshot_ts: u64) -> SnapshotRevision {
        let recovery_floor = self.recovery_floor.load(Ordering::Acquire);
        if recovery_floor != 0 && snapshot_ts < recovery_floor {
            return SnapshotRevision::TooOld;
        }
        if let Some(chain) = self.chain(id) {
            return chain.resolve(snapshot_ts);
        }
        SnapshotRevision::NeverExisted
    }

    pub(crate) fn current(&self, id: &Id) -> Option<Arc<RevisionNode>> {
        self.chain(id).and_then(|chain| chain.current())
    }

    pub(crate) fn install(
        &self,
        id: Id,
        node: Arc<RevisionNode>,
        expected_predecessor: Option<&Arc<RevisionNode>>,
    ) -> Result<(Arc<RevisionChain>, Option<Arc<RevisionNode>>), ()> {
        let chain = self.get_or_create_chain(id);
        let predecessor = chain.install(node, expected_predecessor)?;
        Ok((chain, predecessor))
    }

    pub(crate) fn install_on_chain(
        &self,
        chain: &Arc<RevisionChain>,
        node: Arc<RevisionNode>,
        expected_predecessor: &Arc<RevisionNode>,
    ) -> Result<(), ()> {
        // The caller carries this chain from a resolution performed while it
        // holds the cell's write guard. RevisionChain::install still rechecks
        // the exact predecessor immediately before publishing the list node.
        chain.install(node, Some(expected_predecessor)).map(|_| ())
    }

    pub(crate) fn location(&self, id: &Id, revision_ts: u64) -> Option<usize> {
        self.chain(id)
            .and_then(|chain| chain.find(revision_ts))
            .map(|node| node.load().1)
    }

    pub(crate) fn is_live_at(&self, id: Id, revision_ts: u64, location: usize) -> bool {
        self.chain(&id)
            .and_then(|chain| chain.find(revision_ts))
            .is_some_and(|node| {
                let (state, actual_location) = node.load();
                state != RevisionState::Expired && actual_location == location
            })
    }

    pub(crate) fn relocate(
        &self,
        id: Id,
        revision_ts: u64,
        old_location: usize,
        new_location: usize,
    ) -> RelocateResult {
        let Some(chain) = self.chain(&id) else {
            return RelocateResult::LostRace;
        };
        let Some(node) = chain.find(revision_ts) else {
            return RelocateResult::LostRace;
        };
        if !node.relocate(old_location, new_location) {
            return RelocateResult::LostRace;
        }

        // State alone cannot recover the physical kind of an Aborted node.
        // The cleaner decodes the entry it copied and only publishes a
        // cell-index mirror for physical cells. This result reports logical
        // head position, not a fresh inference from the state tag.
        if chain.is_current(&node) {
            RelocateResult::CurrentPresentMoved
        } else {
            RelocateResult::HistoricalMoved
        }
    }

    pub fn set_recovery_floor(&self, revision_ts: u64) {
        self.recovery_floor.store(revision_ts, Ordering::Release);
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
        self.schedule_expiration(ExpirationRecord {
            chain: chain.clone(),
            node: node.clone(),
            deadline_ms,
        });
        self.wake_worker_for_deadline(deadline_ms, monotonic_ms());
        true
    }

    fn schedule_expiration(&self, expiration: ExpirationRecord) {
        let sequence = self.expiration_sequence.fetch_add(1, Ordering::Relaxed);
        self.expiration_ingress
            .push_front(Some(ScheduledExpiration {
                expiration,
                sequence,
            }));
    }

    fn expire_due(&self, now_ms: u64) -> u64 {
        self.expire_due_until(now_ms, &|| false)
    }

    fn expire_due_until<F>(&self, now_ms: u64, should_cancel: &F) -> u64
    where
        F: Fn() -> bool,
    {
        // Producers stay on the lock-free ingress path. The worker bounds both
        // ingestion and due work so a shutdown join never waits for the whole
        // retirement backlog.
        let mut ingested = Vec::with_capacity(EXPIRATION_WORK_BUDGET);
        let mut pass_cancelled = false;
        while ingested.len() < EXPIRATION_WORK_BUDGET {
            if should_cancel() {
                pass_cancelled = true;
                break;
            }
            let Some(scheduled) = self.expiration_ingress.pop_back() else {
                break;
            };
            let Some(scheduled) = scheduled else {
                continue;
            };
            #[cfg(test)]
            self.expiration_checks.fetch_add(1, Ordering::Relaxed);
            ingested.push(scheduled);
        }
        let ingress_pending = self.expiration_ingress.peek_back().is_some();

        let mut due = Vec::with_capacity(EXPIRATION_WORK_BUDGET);
        {
            let mut expirations = self.expirations.lock();
            expirations.extend(ingested);
            while due.len() < EXPIRATION_WORK_BUDGET {
                if should_cancel() {
                    pass_cancelled = true;
                    break;
                }
                let Some(next) = expirations.peek() else {
                    break;
                };
                if next.expiration.deadline_ms > now_ms {
                    break;
                }
                due.push(
                    expirations
                        .pop()
                        .expect("peeked expiration must remain queued"),
                );
            }
        }

        let due_budget_exhausted = due.len() == EXPIRATION_WORK_BUDGET;
        let mut blocked = Vec::new();
        let mut deferred = Vec::new();
        let mut due = due.into_iter();
        while let Some(mut scheduled) = due.next() {
            if should_cancel() {
                pass_cancelled = true;
                deferred.push(scheduled);
                deferred.extend(due);
                break;
            }
            #[cfg(test)]
            self.expiration_checks.fetch_add(1, Ordering::Relaxed);
            match self.expire_record_until(&scheduled.expiration, should_cancel) {
                ExpirationAttempt::Completed => {}
                ExpirationAttempt::Blocked => {
                    // A blocked middle revision must yield to later heap entries;
                    // one of those can be the older suffix record that unblocks it.
                    scheduled.expiration.deadline_ms = now_ms.saturating_add(1).max(1);
                    scheduled.sequence = self.expiration_sequence.fetch_add(1, Ordering::Relaxed);
                    blocked.push(scheduled);
                }
                ExpirationAttempt::Cancelled => {
                    pass_cancelled = true;
                    deferred.push(scheduled);
                    deferred.extend(due);
                    break;
                }
            }
        }

        let blocked_pending = !blocked.is_empty();
        let mut expirations = self.expirations.lock();
        expirations.extend(blocked);
        expirations.extend(deferred);
        if pass_cancelled || ingress_pending || due_budget_exhausted || blocked_pending {
            return 1;
        }
        expirations
            .peek()
            .map(|next| {
                next.expiration
                    .deadline_ms
                    .saturating_sub(now_ms)
                    .clamp(1, WORKER_MAX_PARK_MS)
            })
            .unwrap_or(WORKER_MAX_PARK_MS)
    }

    fn expire_record(&self, expiration: &ExpirationRecord) -> bool {
        self.expire_record_with_hook(expiration, || {})
    }

    fn expire_record_with_hook<F>(&self, expiration: &ExpirationRecord, after_expire: F) -> bool
    where
        F: FnOnce(),
    {
        matches!(
            self.expire_record_until_with_hook(expiration, &|| false, after_expire),
            ExpirationAttempt::Completed
        )
    }

    fn expire_record_until<F>(
        &self,
        expiration: &ExpirationRecord,
        should_cancel: &F,
    ) -> ExpirationAttempt
    where
        F: Fn() -> bool,
    {
        self.expire_record_until_with_hook(expiration, should_cancel, || {})
    }

    fn expire_record_until_with_hook<C, H>(
        &self,
        expiration: &ExpirationRecord,
        should_cancel: &C,
        after_expire: H,
    ) -> ExpirationAttempt
    where
        C: Fn() -> bool,
        H: FnOnce(),
    {
        if expiration.chain.is_current(&expiration.node) {
            return ExpirationAttempt::Blocked;
        }
        // Ready publishes the TooOld boundary before making a suffix node
        // invisible. AlreadyRemoved means an earlier suffix prune published
        // that monotonic boundary before removing this scheduled node.
        let readiness = expiration
            .chain
            .prepare_expiration_until(&expiration.node, should_cancel);
        match readiness {
            ExpirationReadiness::Blocked => return ExpirationAttempt::Blocked,
            ExpirationReadiness::Cancelled => return ExpirationAttempt::Cancelled,
            ExpirationReadiness::Ready | ExpirationReadiness::AlreadyRemoved => {}
        }
        // The retrying tagged-word CAS preserves a concurrently updated aligned
        // location and makes dead accounting single-winner.
        let dead = expiration.node.expire();
        after_expire();
        if let Some(dead) = dead {
            self.dead.push_front(Some(dead));
        }
        if matches!(readiness, ExpirationReadiness::Ready) {
            // The TooOld boundary, state transition, and dead accounting above
            // are complete before pruning becomes cooperatively cancellable.
            expiration.chain.prune_retired_suffix_until(should_cancel);
        }
        ExpirationAttempt::Completed
    }

    pub(crate) fn pop_dead(&self) -> Option<DeadRevision> {
        self.dead.pop_back().flatten()
    }

    fn wake_worker(&self) {
        if let Some(handle) = self.worker.lock().as_ref() {
            #[cfg(test)]
            self.worker_wakes.fetch_add(1, Ordering::AcqRel);
            handle.thread().unpark();
        }
    }

    fn wake_worker_for_deadline(&self, deadline_ms: u64, now_ms: u64) {
        // The expiration record is fully queued before this acquire. A zero or
        // stale publication means the worker is active or already due, so an
        // unpark token closes the scan-to-park race. An earlier deadline must
        // also move the worker's wake forward. Equal and later work is covered
        // by the already-published wake.
        let published_wake_ms = self.published_worker_wake_ms.load(Ordering::Acquire);
        if published_wake_ms == 0 || published_wake_ms <= now_ms || deadline_ms < published_wake_ms
        {
            self.wake_worker();
        }
    }

    #[cfg(test)]
    fn schedule_expiration_and_notify_at_for_test(
        &self,
        expiration: ExpirationRecord,
        now_ms: u64,
    ) {
        let deadline_ms = expiration.deadline_ms;
        self.schedule_expiration(expiration);
        self.wake_worker_for_deadline(deadline_ms, now_ms);
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
    pub(crate) fn expire_due_for_test(&self, now_ms: u64) {
        let _ = self.expire_due(now_ms);
    }

    #[cfg(test)]
    fn active_workers_for_test() -> usize {
        ACTIVE_HISTORY_WORKERS.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn take_expiration_checks_for_test(&self) -> usize {
        self.expiration_checks.swap(0, Ordering::AcqRel)
    }

    #[cfg(test)]
    fn pause_after_worker_scan_for_test(&self) {
        Self::pause_worker_for_test(
            &self.pause_after_worker_scan,
            &self.worker_paused_after_scan,
            &self.stopped,
        );
    }

    #[cfg(test)]
    fn pause_after_wake_publication_for_test(&self) {
        Self::pause_worker_for_test(
            &self.pause_after_wake_publication,
            &self.worker_paused_after_wake_publication,
            &self.stopped,
        );
    }

    #[cfg(test)]
    fn pause_worker_for_test(pause: &AtomicBool, paused: &AtomicBool, stopped: &AtomicBool) {
        if !pause.load(Ordering::Acquire) {
            return;
        }
        paused.store(true, Ordering::Release);
        while pause.load(Ordering::Acquire) && !stopped.load(Ordering::Acquire) {
            thread::yield_now();
        }
        paused.store(false, Ordering::Release);
    }

    #[cfg(test)]
    fn wait_for_worker_pause_for_test(&self, paused: &AtomicBool, boundary: &str) {
        let timeout = Instant::now() + Duration::from_secs(2);
        while !paused.load(Ordering::Acquire) {
            assert!(
                Instant::now() < timeout,
                "history worker did not reach its {boundary} boundary"
            );
            thread::yield_now();
        }
    }

    #[cfg(test)]
    pub(crate) fn take_chain_map_resolutions_for_test(&self) -> usize {
        self.chain_map_resolutions.swap(0, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(crate) fn revision_count_for_test(&self, id: &Id) -> usize {
        self.chain(id)
            .map(|chain| {
                chain
                    .revisions
                    .iter_front()
                    .filter(|revision_ref| revision_ref.deref().flatten().is_some())
                    .count()
            })
            .unwrap_or(0)
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

    fn expiration(
        chain: &Arc<RevisionChain>,
        revision_ts: u64,
        deadline_ms: u64,
    ) -> ExpirationRecord {
        ExpirationRecord {
            chain: chain.clone(),
            node: node(
                revision_ts,
                RevisionState::CommittedPresent,
                0x1000 + revision_ts as usize * 8,
            ),
            deadline_ms,
        }
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
        let constructions = Arc::new(AtomicUsize::new(0));
        let callers: Vec<_> = (0..THREADS)
            .map(|_| {
                let history = history.clone();
                let id = id.clone();
                let start = start.clone();
                let constructions = constructions.clone();
                thread::spawn(move || {
                    start.wait();
                    history.get_or_create_chain_with(id, || {
                        constructions.fetch_add(1, Ordering::SeqCst);
                        Arc::new(RevisionChain::new())
                    })
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
        assert_eq!(
            constructions.load(Ordering::SeqCst),
            1,
            "the serialized miss path must construct exactly one candidate"
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
    fn expiration_pass_bounds_large_future_queue_ingestion() {
        const FUTURE_EXPIRATIONS: usize = 10_000;

        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let current = node(20_000, RevisionState::CommittedPresent, 0x2000);
        chain.push_front(current);
        for revision_ts in 0..FUTURE_EXPIRATIONS as u64 {
            history.schedule_expiration(ExpirationRecord {
                chain: chain.clone(),
                node: node(revision_ts, RevisionState::CommittedPresent, 0x1000),
                deadline_ms: 100_000,
            });
        }

        assert_eq!(history.expire_due(1), 1);
        assert!(
            history.take_expiration_checks_for_test() <= EXPIRATION_WORK_BUDGET,
            "a future-only pass must bound ingress scheduling work"
        );
    }

    #[test]
    fn expiration_pass_bounds_due_work_for_prompt_shutdown() {
        const DUE_EXPIRATIONS: usize = 10_000;

        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let current = node(20_000, RevisionState::CommittedPresent, 0x2000);
        chain.push_front(current);
        for revision_ts in 0..DUE_EXPIRATIONS as u64 {
            history.schedule_expiration(ExpirationRecord {
                chain: chain.clone(),
                node: node(revision_ts, RevisionState::CommittedPresent, 0x1000),
                deadline_ms: 1,
            });
        }

        assert_eq!(history.expire_due(1), 1);
        let checks = history.take_expiration_checks_for_test();
        assert!(
            checks <= EXPIRATION_WORK_BUDGET * 2,
            "one pass inspected {checks} due expirations instead of yielding after bounded work"
        );
    }

    #[test]
    fn equal_and_later_deadlines_do_not_wake_worker_parked_until_earlier_deadline() {
        const NOW_MS: u64 = 10_000;
        const PUBLISHED_WAKE_MS: u64 = 20_000;

        let history = HistoryIndex::new(300_000);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        history
            .published_worker_wake_ms
            .store(PUBLISHED_WAKE_MS, Ordering::Release);
        history.worker_wakes.store(0, Ordering::Release);

        let chain = Arc::new(RevisionChain::new());
        let current = node(300, RevisionState::CommittedPresent, 0x3000);
        chain.push_front(current);
        for (revision_ts, deadline_ms) in [(100, PUBLISHED_WAKE_MS), (200, PUBLISHED_WAKE_MS + 1)] {
            history.schedule_expiration_and_notify_at_for_test(
                ExpirationRecord {
                    chain: chain.clone(),
                    node: node(
                        revision_ts,
                        RevisionState::CommittedPresent,
                        0x1000 + revision_ts as usize * 8,
                    ),
                    deadline_ms,
                },
                NOW_MS,
            );
        }

        assert_eq!(
            history.worker_wakes.load(Ordering::Acquire),
            0,
            "equal and later deadlines are already covered by the worker's published wake"
        );
        assert_eq!(
            history
                .expiration_ingress
                .pop_back()
                .flatten()
                .expect("equal-deadline record must be queued")
                .expiration
                .deadline_ms,
            PUBLISHED_WAKE_MS
        );
        assert_eq!(
            history
                .expiration_ingress
                .pop_back()
                .flatten()
                .expect("later-deadline record must be queued")
                .expiration
                .deadline_ms,
            PUBLISHED_WAKE_MS + 1
        );
        history.shutdown();
    }

    #[test]
    fn earlier_stale_and_active_worker_publications_request_a_wake() {
        const NOW_MS: u64 = 10_000;
        const PUBLISHED_WAKE_MS: u64 = 20_000;

        let history = HistoryIndex::new(300_000);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        history.worker_wakes.store(0, Ordering::Release);
        let chain = Arc::new(RevisionChain::new());
        chain.push_front(node(400, RevisionState::CommittedPresent, 0x4000));

        history
            .published_worker_wake_ms
            .store(PUBLISHED_WAKE_MS, Ordering::Release);
        history.schedule_expiration_and_notify_at_for_test(
            expiration(&chain, 100, PUBLISHED_WAKE_MS - 1),
            NOW_MS,
        );

        history
            .published_worker_wake_ms
            .store(NOW_MS, Ordering::Release);
        history.schedule_expiration_and_notify_at_for_test(
            expiration(&chain, 200, PUBLISHED_WAKE_MS + 1),
            NOW_MS,
        );

        history.published_worker_wake_ms.store(0, Ordering::Release);
        history.schedule_expiration_and_notify_at_for_test(
            expiration(&chain, 300, PUBLISHED_WAKE_MS + 1),
            NOW_MS,
        );

        assert_eq!(
            history.worker_wakes.load(Ordering::Acquire),
            3,
            "an earlier deadline, stale publication, and active worker each require a wake"
        );
        history.shutdown();
    }

    #[test]
    fn suppressed_equal_deadline_is_ingested_at_the_published_wake() {
        let history = HistoryIndex::new(300_000);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        let published_wake_ms = monotonic_ms().saturating_add(WORKER_MAX_PARK_MS);
        history
            .published_worker_wake_ms
            .store(published_wake_ms, Ordering::Release);
        history.worker_wakes.store(0, Ordering::Release);

        let chain = Arc::new(RevisionChain::new());
        let retired = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(retired.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        assert!(retired.schedule_retirement(published_wake_ms));
        history.schedule_expiration_and_notify_at_for_test(
            ExpirationRecord {
                chain,
                node: retired.clone(),
                deadline_ms: published_wake_ms,
            },
            published_wake_ms - 1,
        );
        assert_eq!(
            history.worker_wakes.load(Ordering::Acquire),
            0,
            "an equal deadline must not add a redundant wake"
        );

        history
            .pause_after_wake_publication
            .store(false, Ordering::Release);
        let timeout = Instant::now() + Duration::from_secs(2);
        while retired.load().0 != RevisionState::Expired {
            assert!(
                Instant::now() < timeout,
                "the worker did not ingest the suppressed record at its published wake"
            );
            thread::yield_now();
        }
        assert_eq!(
            history.pop_dead(),
            Some(DeadRevision {
                location: 0x1000,
                entry_size: 64,
            })
        );
        history.shutdown();
    }

    #[test]
    fn enqueue_after_scan_before_wake_publication_preserves_unpark_token() {
        let history = HistoryIndex::new(0);
        history
            .pause_after_worker_scan
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_scan,
            "post-scan pre-publication",
        );
        history.worker_wakes.store(0, Ordering::Release);

        let chain = Arc::new(RevisionChain::new());
        let retired = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(retired.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        assert!(history.retire(&chain, &retired));
        assert_eq!(
            history.worker_wakes.load(Ordering::Acquire),
            1,
            "an active worker must receive an unpark token before it publishes a deadline"
        );

        history
            .pause_after_worker_scan
            .store(false, Ordering::Release);
        let timeout = Instant::now() + Duration::from_secs(2);
        while retired.load().0 != RevisionState::Expired {
            assert!(
                Instant::now() < timeout,
                "the pre-publication unpark token was lost at park"
            );
            thread::yield_now();
        }
        history.shutdown();
    }

    #[test]
    fn enqueue_after_wake_publication_before_park_preserves_unpark_token() {
        let history = HistoryIndex::new(0);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        history.worker_wakes.store(0, Ordering::Release);

        let chain = Arc::new(RevisionChain::new());
        let retired = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(retired.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        assert!(history.retire(&chain, &retired));
        assert_eq!(
            history.worker_wakes.load(Ordering::Acquire),
            1,
            "an earlier deadline must unpark a worker between publication and park"
        );

        history
            .pause_after_wake_publication
            .store(false, Ordering::Release);
        let timeout = Instant::now() + Duration::from_secs(2);
        while retired.load().0 != RevisionState::Expired {
            assert!(
                Instant::now() < timeout,
                "the post-publication unpark token was lost at park"
            );
            thread::yield_now();
        }
        history.shutdown();
    }

    #[test]
    fn early_wake_clears_publication_before_the_next_scan() {
        let history = HistoryIndex::new(0);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        history
            .pause_after_worker_scan
            .store(true, Ordering::Release);

        let chain = Arc::new(RevisionChain::new());
        let retired = node(100, RevisionState::CommittedPresent, 0x1000);
        chain.push_front(retired.clone());
        chain.push_front(node(200, RevisionState::CommittedPresent, 0x2000));
        assert!(history.retire(&chain, &retired));
        history
            .pause_after_wake_publication
            .store(false, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_scan,
            "post-early-wake scan",
        );

        assert_eq!(
            history.published_worker_wake_ms.load(Ordering::Acquire),
            0,
            "a worker must stop advertising an obsolete wake while it is active"
        );
        assert_eq!(retired.load().0, RevisionState::Expired);
        history.shutdown();
    }

    #[test]
    fn shutdown_joins_promptly_from_worker_pause_boundaries_and_with_backlog() {
        const BACKLOG: usize = 10_000;

        let history = HistoryIndex::new(300_000);
        history
            .pause_after_worker_scan
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_scan,
            "post-scan pre-publication",
        );
        let started = Instant::now();
        history.shutdown();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown waited at the scan-to-publication boundary"
        );
        assert_eq!(
            history.published_worker_wake_ms.load(Ordering::Acquire),
            0,
            "a stopped worker must clear its published wake"
        );

        let history = HistoryIndex::new(300_000);
        history
            .pause_after_wake_publication
            .store(true, Ordering::Release);
        history.wait_for_worker_pause_for_test(
            &history.worker_paused_after_wake_publication,
            "published-wake pre-park",
        );
        let chain = Arc::new(RevisionChain::new());
        chain.push_front(node(
            BACKLOG as u64 + 1,
            RevisionState::CommittedPresent,
            0x2000,
        ));
        for revision_ts in 0..BACKLOG as u64 {
            history.schedule_expiration(ExpirationRecord {
                chain: chain.clone(),
                node: node(revision_ts, RevisionState::CommittedPresent, 0x1000),
                deadline_ms: u64::MAX,
            });
        }
        let started = Instant::now();
        history.shutdown();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown drained or waited behind the pending retirement backlog"
        );
        assert_eq!(
            history.published_worker_wake_ms.load(Ordering::Acquire),
            0,
            "a stopped worker must clear its published wake"
        );
    }

    #[test]
    fn retirement_records_keep_original_deadlines_and_expire_once_due() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let mut retired = Vec::new();

        for index in 0..3 {
            let chain = Arc::new(RevisionChain::new());
            let revision = node(
                100 + index,
                RevisionState::CommittedPresent,
                0x1000 + index as usize * 8,
            );
            chain.push_front(revision.clone());
            chain.push_front(node(
                200 + index,
                RevisionState::CommittedPresent,
                0x2000 + index as usize * 8,
            ));
            assert!(history.retire(&chain, &revision));
            retired.push(revision);
        }

        let deadlines: Vec<_> = retired
            .iter()
            .map(|revision| revision.retire_deadline_ms.load(Ordering::Acquire))
            .collect();
        let earliest = *deadlines.iter().min().expect("retirement deadline");
        history.expire_due_for_test(earliest - 1);
        assert!(
            retired
                .iter()
                .all(|revision| revision.load().0 == RevisionState::CommittedPresent),
            "no revision may expire before its authoritative deadline"
        );
        assert!(history.pop_dead().is_none());

        history.expire_due_for_test(*deadlines.iter().max().unwrap());
        assert!(
            retired
                .iter()
                .all(|revision| revision.load().0 == RevisionState::Expired),
            "every queued retirement record must be processed"
        );
        let mut dead_locations = Vec::new();
        while let Some(dead) = history.pop_dead() {
            dead_locations.push(dead.location);
        }
        dead_locations.sort_unstable();
        assert_eq!(dead_locations, vec![0x1000, 0x1008, 0x1010]);
        assert_eq!(
            retired
                .iter()
                .map(|revision| revision.retire_deadline_ms.load(Ordering::Acquire))
                .collect::<Vec<_>>(),
            deadlines,
            "processing must not rewrite the node's authoritative original deadline"
        );
    }

    #[test]
    fn blocked_scheduler_retry_moves_without_rewriting_original_deadline() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        chain.push_front(node(100, RevisionState::CommittedPresent, 0x1000));
        let blocked = node(200, RevisionState::CommittedPresent, 0x2000);
        chain.push_front(blocked.clone());
        chain.push_front(node(300, RevisionState::CommittedPresent, 0x3000));
        assert!(blocked.schedule_retirement(1));
        history.schedule_expiration(ExpirationRecord {
            chain,
            node: blocked.clone(),
            deadline_ms: 1,
        });

        assert_eq!(history.expire_due(1), 1);
        assert_eq!(blocked.retire_deadline_ms.load(Ordering::Acquire), 1);
        assert_eq!(
            history
                .expirations
                .lock()
                .peek()
                .expect("blocked expiration must remain scheduled")
                .expiration
                .deadline_ms,
            2,
            "blocked work must yield until now + 1 for suffix fairness"
        );
    }

    #[test]
    fn shutdown_cancels_expiration_inside_a_large_real_chain() {
        const CHAIN_LEN: usize = 10_000;
        const PAUSE_AT_STEP: usize = 128;

        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let revisions: Vec<_> = (0..CHAIN_LEN)
            .map(|index| {
                node(
                    100 + index as u64,
                    RevisionState::CommittedPresent,
                    0x1000 + index * 8,
                )
            })
            .collect();
        for revision in &revisions {
            chain.push_front(revision.clone());
        }
        chain
            .expiration_scan_pause_at
            .store(PAUSE_AT_STEP, Ordering::Release);

        assert!(history.retire(&chain, &revisions[CHAIN_LEN - 2]));
        let timeout = Instant::now() + Duration::from_secs(2);
        while !chain.expiration_scan_paused.load(Ordering::Acquire) {
            assert!(
                Instant::now() < timeout,
                "history worker did not enter the long-chain expiration scan"
            );
            thread::yield_now();
        }

        let shutdown_history = history.clone();
        let shutdown = thread::spawn(move || shutdown_history.shutdown());
        while !history.stopped.load(Ordering::Acquire) {
            assert!(
                Instant::now() < timeout,
                "shutdown did not publish its cooperative stop request"
            );
            thread::yield_now();
        }
        chain.expiration_scan_pause_at.store(0, Ordering::Release);
        shutdown.join().expect("history shutdown panicked");

        let scan_steps = chain.expiration_scan_steps.load(Ordering::Acquire);
        assert!(
            scan_steps <= PAUSE_AT_STEP + CHAIN_CANCELLATION_CHECK_INTERVAL,
            "shutdown waited while one expiration scanned {scan_steps} real chain nodes"
        );
        assert_eq!(
            revisions[CHAIN_LEN - 2].load().0,
            RevisionState::CommittedPresent,
            "cancelled preparation must not expire the scheduled revision"
        );
        assert_eq!(chain.truncated_before_ts.load(Ordering::Acquire), 0);
        assert!(history.pop_dead().is_none());
    }

    #[test]
    fn shutdown_cancels_large_suffix_prune_and_preserves_too_old_floor() {
        const CHAIN_LEN: usize = 10_000;
        const PAUSE_AFTER_POPS: usize = 128;

        let history = HistoryIndex::new(0);
        let chain = Arc::new(RevisionChain::new());
        let mut revisions: Vec<_> = (0..CHAIN_LEN - 2)
            .map(|index| {
                node(
                    100 + index as u64,
                    RevisionState::Expired,
                    0x1000 + index * 8,
                )
            })
            .collect();
        let expiring = node(
            100 + (CHAIN_LEN - 2) as u64,
            RevisionState::CommittedPresent,
            0x1000 + (CHAIN_LEN - 2) * 8,
        );
        let current = node(
            100 + (CHAIN_LEN - 1) as u64,
            RevisionState::CommittedPresent,
            0x1000 + (CHAIN_LEN - 1) * 8,
        );
        revisions.push(expiring.clone());
        revisions.push(current.clone());
        for revision in &revisions {
            chain.push_front(revision.clone());
        }
        chain
            .expiration_prune_pause_at
            .store(PAUSE_AFTER_POPS, Ordering::Release);

        assert!(history.retire(&chain, &expiring));
        let timeout = Instant::now() + Duration::from_secs(2);
        while !chain.expiration_prune_paused.load(Ordering::Acquire) {
            assert!(
                Instant::now() < timeout,
                "history worker did not enter the large suffix prune"
            );
            thread::yield_now();
        }

        let shutdown_history = history.clone();
        let shutdown = thread::spawn(move || shutdown_history.shutdown());
        while !history.stopped.load(Ordering::Acquire) {
            assert!(
                Instant::now() < timeout,
                "shutdown did not publish its cooperative stop request"
            );
            thread::yield_now();
        }
        chain.expiration_prune_pause_at.store(0, Ordering::Release);
        shutdown.join().expect("history shutdown panicked");

        let prune_steps = chain.expiration_prune_steps.load(Ordering::Acquire);
        assert!(
            prune_steps <= PAUSE_AFTER_POPS + CHAIN_CANCELLATION_CHECK_INTERVAL,
            "shutdown waited while one expiration pruned {prune_steps} real suffix nodes"
        );
        assert_eq!(expiring.load().0, RevisionState::Expired);
        assert_eq!(
            history.pop_dead(),
            Some(DeadRevision {
                location: expiring.load().1,
                entry_size: expiring.entry_size,
            }),
            "cancellation after expiration must retain dead accounting"
        );
        assert!(Arc::ptr_eq(
            &chain.current().expect("current revision must remain"),
            &current
        ));
        assert!(
            chain.truncated_before_ts.load(Ordering::Acquire) >= current.revision_ts,
            "partial pruning must retain the published TooOld boundary"
        );
        assert!(matches!(
            chain.resolve(current.revision_ts),
            SnapshotRevision::TooOld
        ));
    }

    #[test]
    fn blocked_expiration_batch_yields_to_later_unblocking_record() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let revisions: Vec<_> = (0..=EXPIRATION_WORK_BUDGET + 1)
            .map(|index| {
                node(
                    100 + index as u64,
                    RevisionState::CommittedPresent,
                    0x1000 + index * 8,
                )
            })
            .collect();
        for revision in &revisions {
            chain.push_front(revision.clone());
        }

        // Fill the first work batch with middle revisions that are blocked by
        // the oldest suffix record, then enqueue that unblocking record last.
        for revision in &revisions[1..=EXPIRATION_WORK_BUDGET] {
            history.schedule_expiration(ExpirationRecord {
                chain: chain.clone(),
                node: revision.clone(),
                deadline_ms: 1,
            });
        }
        history.schedule_expiration(ExpirationRecord {
            chain,
            node: revisions[0].clone(),
            deadline_ms: 1,
        });

        assert_eq!(history.expire_due(1), 1);
        assert_eq!(revisions[0].load().0, RevisionState::CommittedPresent);

        assert_eq!(history.expire_due(1), 1);
        assert_eq!(
            revisions[0].load().0,
            RevisionState::Expired,
            "blocked retry records must yield to the suffix record that makes progress possible"
        );
    }

    #[test]
    fn pending_boundary_that_aborts_keeps_retired_commit_visible() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let retired = node(200, RevisionState::CommittedPresent, 0x2000);
        let pending = node(300, RevisionState::PendingPresent, 0x3000);
        chain.push_front(retired.clone());
        chain.push_front(pending.clone());

        assert!(history.retire(&chain, &retired));
        assert_eq!(history.expire_due(u64::MAX), 1);

        assert_eq!(retired.load(), (RevisionState::CommittedPresent, 0x2000));
        assert_eq!(chain.truncated_before_ts.load(Ordering::Acquire), 0);
        assert!(history.pop_dead().is_none());

        assert!(
            pending.compare_exchange_state(RevisionState::PendingPresent, RevisionState::Aborted,)
        );
        assert_eq!(chain.resolve(401).revision_ts(), Some(200));
    }

    #[test]
    fn pending_boundary_that_commits_allows_retired_commit_to_expire() {
        let history = HistoryIndex::new(300_000);
        history.shutdown();
        let chain = Arc::new(RevisionChain::new());
        let retired = node(200, RevisionState::CommittedPresent, 0x2000);
        let pending = node(300, RevisionState::PendingPresent, 0x3000);
        chain.push_front(retired.clone());
        chain.push_front(pending.clone());

        assert!(history.retire(&chain, &retired));
        assert_eq!(history.expire_due(u64::MAX), 1);
        assert!(pending.compare_exchange_state(
            RevisionState::PendingPresent,
            RevisionState::CommittedPresent,
        ));

        assert_eq!(history.expire_due(u64::MAX), WORKER_MAX_PARK_MS);
        assert_eq!(retired.load(), (RevisionState::Expired, 0x2000));
        assert!(matches!(chain.resolve(300), SnapshotRevision::TooOld));
        assert_eq!(chain.resolve(301).revision_ts(), Some(300));
        assert_eq!(
            history.pop_dead(),
            Some(DeadRevision {
                location: 0x2000,
                entry_size: 64,
            })
        );
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
        assert!(history.expiration_ingress.peek_back().is_none());
        assert!(history.expirations.lock().is_empty());
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
    fn predecessor_mismatch_does_not_publish_a_partial_head() {
        let history = HistoryIndex::new(300_000);
        let id = Id::new(7, 9);
        let first = node(100, RevisionState::CommittedPresent, 0x1000);
        history.install(id, first.clone(), None).unwrap();
        let wrong_predecessor = node(100, RevisionState::CommittedPresent, 0x1800);
        let second = node(200, RevisionState::CommittedPresent, 0x2000);

        assert!(history
            .install(id, second, Some(&wrong_predecessor))
            .is_err());
        let current = history.current(&id).expect("first remains current");
        assert!(Arc::ptr_eq(&current, &first));
        assert_eq!(history.location(&id, 200), None);
    }

    #[test]
    fn one_exact_predecessor_can_publish_only_one_successor() {
        let history = HistoryIndex::new(300_000);
        let id = Id::new(7, 10);
        let first = node(100, RevisionState::CommittedPresent, 0x1000);
        history.install(id, first.clone(), None).unwrap();
        let winner = node(200, RevisionState::CommittedPresent, 0x2000);
        let stale = node(300, RevisionState::CommittedPresent, 0x3000);

        history
            .install(id, winner.clone(), Some(&first))
            .expect("the exact predecessor must accept its first successor");
        assert!(
            history.install(id, stale, Some(&first)).is_err(),
            "a stale expected predecessor must not publish after the head advances"
        );
        let current = history.current(&id).expect("winner remains current");
        assert!(Arc::ptr_eq(&current, &winner));
        assert_eq!(history.location(&id, 300), None);
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
