use super::*;
use crate::ram::cell::{InstalledRevision, OwnedCellRef, RevisionWrite};
use crate::ram::history::RevisionState;
use crate::ram::types::{FromHeader, Id, OwnedMap, OwnedPrimArray, OwnedValue};
use crate::server::DatabaseRuntime;
use crate::{
    index::builder::IndexBuilder,
    ram::cell::{CellHeader, OwnedCell, ReadError, SnapshotRead, WriteError},
};
use bifrost::hlc::Hlc;
use bifrost::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use lightning::linked_list::LinkedList;
use lightning::map::Map;
use lightning::map::PtrHashMap as LFMap;
#[cfg(test)]
use parking_lot::Condvar;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::Notify;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(TXN_DATA_MANAGER_RPC_SERVICE) as u64;

pub fn generate_scoped_service_id(group: &str, database_name: &str) -> u64 {
    hash_str(&format!(
        "TXN_DATA_MANAGER_RPC_SERVICE-{}-{}",
        group, database_name
    ))
}

// Test-only instrumentation: counts how many full-cell participant `read` RPCs
// this process has served. Coordinator tests assert that a `head`/`read_selected`
// (shape-gated partial read) does NOT transfer the whole cell, i.e. does not
// increment this counter. The increment is a cheap relaxed add always compiled
// in; only tests read it via `full_read_rpc_count`.
static FULL_READ_RPC_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Number of full-cell participant `read` RPCs served since process start.
/// Test-only accessor; used to prove header/projection-only reads never fetch
/// the whole cell.
#[cfg(test)]
pub(crate) fn full_read_rpc_count() -> usize {
    FULL_READ_RPC_COUNT.load(Relaxed)
}

const DEFAULT_LOCK_TIMEOUT_MS: i64 = 30_000;
const VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS: i64 = 300_000;
const PARTICIPANT_LIFECYCLE_SHARD_COUNT: usize = 64;
const OWNER_RESOLUTION_QUEUE_CAPACITY: usize = 256;
const OWNER_RESOLUTION_DISCOVERY_BATCH: usize = 64;
const OWNER_RESOLUTION_DISCOVERY_INTERVAL_MS: u64 = 10;
const OWNER_RESOLUTION_MIN_ATTEMPT_INTERVAL_MS: u64 = 10;

type OwnerKey = (TxnId, u64);

#[derive(Default)]
struct OwnerResolutionWorkerActivity {
    live: AtomicUsize,
}

struct OwnerResolutionWorkerGuard {
    activity: Arc<OwnerResolutionWorkerActivity>,
}

impl OwnerResolutionWorkerGuard {
    fn new(activity: Arc<OwnerResolutionWorkerActivity>) -> Self {
        activity.live.fetch_add(1, Relaxed);
        Self { activity }
    }
}

impl Drop for OwnerResolutionWorkerGuard {
    fn drop(&mut self) {
        self.activity.live.fetch_sub(1, Relaxed);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParticipantCompletionEvidence {
    outcome: TxnState,
    expires_at_ms: Option<i64>,
}

impl ParticipantCompletionEvidence {
    fn pending_retirement(outcome: TxnState) -> Self {
        Self {
            outcome,
            expires_at_ms: None,
        }
    }

    fn durable(outcome: TxnState, expires_at_ms: Option<i64>) -> Self {
        Self {
            outcome,
            expires_at_ms,
        }
    }

    fn outcome_at(&self, now_ms: i64) -> Option<TxnState> {
        self.expires_at_ms
            .is_none_or(|expires_at_ms| now_ms < expires_at_ms)
            .then_some(self.outcome)
    }
}

#[derive(Default)]
struct OwnerResolutionState {
    requested: bool,
    queued: bool,
    attempt: u32,
    retry_at_ms: i64,
}

struct OwnerEntry {
    count: usize,
    resolution: OwnerResolutionState,
    previous: Option<OwnerKey>,
    next: Option<OwnerKey>,
}

#[derive(Default)]
struct OwnerIndex {
    by_owner: HashMap<OwnerKey, OwnerEntry>,
    by_tid: HashMap<TxnId, usize>,
    head: Option<OwnerKey>,
    tail: Option<OwnerKey>,
    resolution_cursor: Option<OwnerKey>,
}

impl OwnerIndex {
    fn add(&mut self, owner: &TxnPriority, count: usize) {
        if count == 0 {
            return;
        }
        let key = (owner.tid, owner.coordinator_id);
        if let Some(entry) = self.by_owner.get_mut(&key) {
            entry.count += count;
            *self.by_tid.entry(owner.tid).or_default() += count;
            return;
        }

        let previous = self.tail;
        self.by_owner.insert(
            key,
            OwnerEntry {
                count,
                resolution: OwnerResolutionState::default(),
                previous,
                next: None,
            },
        );
        if let Some(previous) = previous {
            self.by_owner
                .get_mut(&previous)
                .expect("owner discovery tail must remain indexed")
                .next = Some(key);
        } else {
            self.head = Some(key);
        }
        self.tail = Some(key);
        self.resolution_cursor.get_or_insert(key);
        *self.by_tid.entry(owner.tid).or_default() += count;
    }

    fn remove(&mut self, owner: &TxnPriority, count: usize) {
        if count == 0 {
            return;
        }
        let key = (owner.tid, owner.coordinator_id);
        let remaining = self
            .by_owner
            .get(&key)
            .expect("owner index transition must remove an existing owner")
            .count
            .checked_sub(count)
            .expect("owner index transition must not underflow");
        if remaining > 0 {
            self.by_owner
                .get_mut(&key)
                .expect("owner index entry must remain present")
                .count = remaining;
        } else {
            let removed = self
                .by_owner
                .remove(&key)
                .expect("owner index entry must remain present");
            if let Some(previous) = removed.previous {
                self.by_owner
                    .get_mut(&previous)
                    .expect("owner discovery previous link must remain indexed")
                    .next = removed.next;
            } else {
                self.head = removed.next;
            }
            if let Some(next) = removed.next {
                self.by_owner
                    .get_mut(&next)
                    .expect("owner discovery next link must remain indexed")
                    .previous = removed.previous;
            } else {
                self.tail = removed.previous;
            }
            if self.resolution_cursor == Some(key) {
                self.resolution_cursor = removed.next.or(self.head);
            }
        }
        Self::subtract_count(&mut self.by_tid, &owner.tid, count);
    }

    fn contains_owner(&self, owner: &TxnPriority) -> bool {
        self.by_owner
            .contains_key(&(owner.tid, owner.coordinator_id))
    }

    fn request_resolution(&mut self, key: OwnerKey, now_ms: i64) -> bool {
        let Some(entry) = self.by_owner.get_mut(&key) else {
            return false;
        };
        entry.resolution.requested = true;
        if entry.resolution.queued || now_ms < entry.resolution.retry_at_ms {
            return false;
        }
        entry.resolution.queued = true;
        true
    }

    fn release_queue_slot(&mut self, key: OwnerKey) {
        if let Some(entry) = self.by_owner.get_mut(&key) {
            entry.resolution.queued = false;
        }
    }

    fn finish_resolution_attempt(&mut self, key: OwnerKey, now_ms: i64) {
        let Some(entry) = self.by_owner.get_mut(&key) else {
            return;
        };
        let backoff_ms = 10i64
            .saturating_mul(1i64 << entry.resolution.attempt.min(6))
            .min(1_000);
        entry.resolution.attempt = entry.resolution.attempt.saturating_add(1);
        entry.resolution.retry_at_ms = now_ms.saturating_add(backoff_ms);
        entry.resolution.queued = false;
    }

    fn discover_resolution_candidates(
        &mut self,
        now_ms: i64,
        visit_limit: usize,
        candidate_limit: usize,
    ) -> Vec<OwnerKey> {
        if candidate_limit == 0 {
            return Vec::new();
        }
        let visit_count = self.by_owner.len().min(visit_limit);
        let mut current = self.resolution_cursor.or(self.head);
        let mut candidates = Vec::with_capacity(visit_count.min(candidate_limit));
        for _ in 0..visit_count {
            let Some(key) = current else {
                break;
            };
            let next = self
                .by_owner
                .get(&key)
                .and_then(|entry| entry.next)
                .or(self.head);
            let entry = self
                .by_owner
                .get_mut(&key)
                .expect("owner discovery cursor must reference canonical ownership");
            if entry.resolution.requested
                && !entry.resolution.queued
                && now_ms >= entry.resolution.retry_at_ms
            {
                entry.resolution.queued = true;
                candidates.push(key);
            }
            current = next;
            if candidates.len() == candidate_limit {
                break;
            }
        }
        self.resolution_cursor = current;
        candidates
    }

    #[cfg(test)]
    fn requested_resolution_count(&self) -> usize {
        self.by_owner
            .values()
            .filter(|entry| entry.resolution.requested)
            .count()
    }

    fn subtract_count<K: Eq + Hash + Copy>(counts: &mut HashMap<K, usize>, key: &K, count: usize) {
        let remaining = counts
            .get(key)
            .copied()
            .expect("owner index transition must remove an existing owner")
            .checked_sub(count)
            .expect("owner index transition must not underflow");
        if remaining == 0 {
            counts.remove(key);
        } else {
            counts.insert(*key, remaining);
        }
    }
}

#[cfg(test)]
struct PrepareDelayState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: AtomicBool,
    released_notify: Notify,
}

#[cfg(test)]
pub(crate) struct PrepareDelayHandle {
    key: (TxnId, Id),
    state: Arc<PrepareDelayState>,
}

#[cfg(test)]
impl PrepareDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self
            .state
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for PrepareDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = prepare_delay_hooks().lock();
        let owns_registration = hooks
            .get(&self.key)
            .map(|state| Arc::ptr_eq(state, &self.state))
            .unwrap_or(false);
        if owns_registration {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static PREPARE_DELAY_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>>> =
    OnceLock::new();

#[cfg(test)]
fn prepare_delay_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>> {
    PREPARE_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_prepare_delay_for_cell(tid: TxnId, id: Id) -> PrepareDelayHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    prepare_delay_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    PrepareDelayHandle { key, state }
}

#[cfg(test)]
pub(crate) struct MetaAcquireDelayHandle {
    key: (TxnId, Id),
    state: Arc<PrepareDelayState>,
}

#[cfg(test)]
impl MetaAcquireDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self
            .state
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for MetaAcquireDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = meta_acquire_delay_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static META_ACQUIRE_DELAY_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>>> =
    OnceLock::new();

#[cfg(test)]
fn meta_acquire_delay_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>> {
    META_ACQUIRE_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_meta_acquire_delay_for_cell(tid: TxnId, id: Id) -> MetaAcquireDelayHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    meta_acquire_delay_hooks().lock().insert(key, state.clone());
    MetaAcquireDelayHandle { key, state }
}

#[cfg(test)]
pub(crate) struct CommitDelayHandle {
    key: (TxnId, Id),
    state: Arc<PrepareDelayState>,
}

#[cfg(test)]
impl CommitDelayHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        if !self
            .state
            .released
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.state.released_notify.notify_waiters();
        }
    }
}

#[cfg(test)]
impl Drop for CommitDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = commit_delay_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static COMMIT_DELAY_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>>> =
    OnceLock::new();

#[cfg(test)]
fn commit_delay_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<PrepareDelayState>>> {
    COMMIT_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_commit_delay_for_cell(tid: TxnId, id: Id) -> CommitDelayHandle {
    let key = (tid, id);
    let state = Arc::new(PrepareDelayState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: AtomicBool::new(false),
        released_notify: Notify::new(),
    });
    commit_delay_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    CommitDelayHandle { key, state }
}

#[cfg(test)]
struct BeforeStorageMutationState {
    entered: AtomicBool,
    entered_notify: Notify,
    released: Mutex<bool>,
    released_condvar: Condvar,
}

#[cfg(test)]
pub(crate) struct BeforeStorageMutationHandle {
    key: (TxnId, Id),
    state: Arc<BeforeStorageMutationState>,
}

#[cfg(test)]
impl BeforeStorageMutationHandle {
    pub(crate) async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub(crate) fn release(&self) {
        let mut released = self.state.released.lock();
        if !*released {
            *released = true;
            self.state.released_condvar.notify_all();
        }
    }
}

#[cfg(test)]
impl Drop for BeforeStorageMutationHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = before_storage_mutation_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static BEFORE_STORAGE_MUTATION_HOOKS: OnceLock<
    Mutex<BTreeMap<(TxnId, Id), Arc<BeforeStorageMutationState>>>,
> = OnceLock::new();

#[cfg(test)]
fn before_storage_mutation_hooks(
) -> &'static Mutex<BTreeMap<(TxnId, Id), Arc<BeforeStorageMutationState>>> {
    BEFORE_STORAGE_MUTATION_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_before_storage_mutation_pause(
    tid: TxnId,
    id: Id,
) -> BeforeStorageMutationHandle {
    let key = (tid, id);
    let state = Arc::new(BeforeStorageMutationState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: Mutex::new(false),
        released_condvar: Condvar::new(),
    });
    before_storage_mutation_hooks()
        .lock()
        .insert(key.clone(), state.clone());
    BeforeStorageMutationHandle { key, state }
}

#[cfg(test)]
fn pause_before_storage_mutation(tid: &TxnId, id: &Id) {
    let state = before_storage_mutation_hooks()
        .lock()
        .remove(&(tid.clone(), *id));
    let Some(state) = state else {
        return;
    };
    state
        .entered
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    let mut released = state.released.lock();
    while !*released {
        state.released_condvar.wait(&mut released);
    }
}

#[cfg(test)]
struct TxnRegistryPublishDelayHandle {
    tid: TxnId,
    state: Arc<BeforeStorageMutationState>,
}

#[cfg(test)]
impl TxnRegistryPublishDelayHandle {
    async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    fn release(&self) {
        let mut released = self.state.released.lock();
        if !*released {
            *released = true;
            self.state.released_condvar.notify_all();
        }
    }
}

#[cfg(test)]
impl Drop for TxnRegistryPublishDelayHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = txn_registry_publish_delay_hooks().lock();
        if hooks
            .get(&self.tid)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.tid);
        }
    }
}

#[cfg(test)]
static TXN_REGISTRY_PUBLISH_DELAY_HOOKS: OnceLock<
    Mutex<BTreeMap<TxnId, Arc<BeforeStorageMutationState>>>,
> = OnceLock::new();

#[cfg(test)]
fn txn_registry_publish_delay_hooks(
) -> &'static Mutex<BTreeMap<TxnId, Arc<BeforeStorageMutationState>>> {
    TXN_REGISTRY_PUBLISH_DELAY_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_txn_registry_publish_delay(tid: TxnId) -> TxnRegistryPublishDelayHandle {
    let state = Arc::new(BeforeStorageMutationState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: Mutex::new(false),
        released_condvar: Condvar::new(),
    });
    txn_registry_publish_delay_hooks()
        .lock()
        .insert(tid, state.clone());
    TxnRegistryPublishDelayHandle { tid, state }
}

#[cfg(test)]
fn pause_after_txn_registry_publish(tid: &TxnId) {
    let Some(state) = txn_registry_publish_delay_hooks().lock().remove(tid) else {
        return;
    };
    state
        .entered
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    let mut released = state.released.lock();
    while !*released {
        state.released_condvar.wait(&mut released);
    }
}

#[cfg(test)]
struct RetirementReadPauseHandle {
    key: (TxnId, bool),
    state: Arc<BeforeStorageMutationState>,
}

#[cfg(test)]
impl RetirementReadPauseHandle {
    async fn wait_until_entered(&self) {
        let notified = self.state.entered_notify.notified();
        if self.state.entered.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    fn release(&self) {
        let mut released = self.state.released.lock();
        if !*released {
            *released = true;
            self.state.released_condvar.notify_all();
        }
    }
}

#[cfg(test)]
impl Drop for RetirementReadPauseHandle {
    fn drop(&mut self) {
        self.release();
        let mut hooks = retirement_read_pause_hooks().lock();
        if hooks
            .get(&self.key)
            .is_some_and(|state| Arc::ptr_eq(state, &self.state))
        {
            hooks.remove(&self.key);
        }
    }
}

#[cfg(test)]
static RETIREMENT_READ_PAUSE_HOOKS: OnceLock<
    Mutex<BTreeMap<(TxnId, bool), Arc<BeforeStorageMutationState>>>,
> = OnceLock::new();

#[cfg(test)]
fn retirement_read_pause_hooks(
) -> &'static Mutex<BTreeMap<(TxnId, bool), Arc<BeforeStorageMutationState>>> {
    RETIREMENT_READ_PAUSE_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn install_retirement_read_pause(tid: TxnId, finalized: bool) -> RetirementReadPauseHandle {
    let key = (tid, finalized);
    let state = Arc::new(BeforeStorageMutationState {
        entered: AtomicBool::new(false),
        entered_notify: Notify::new(),
        released: Mutex::new(false),
        released_condvar: Condvar::new(),
    });
    retirement_read_pause_hooks()
        .lock()
        .insert(key, state.clone());
    RetirementReadPauseHandle { key, state }
}

#[cfg(test)]
fn pause_after_retirement_read(tid: &TxnId, finalized: bool) {
    let Some(state) = retirement_read_pause_hooks()
        .lock()
        .remove(&(*tid, finalized))
    else {
        return;
    };
    state
        .entered
        .store(true, std::sync::atomic::Ordering::SeqCst);
    state.entered_notify.notify_waiters();
    let mut released = state.released.lock();
    while !*released {
        state.released_condvar.wait(&mut released);
    }
}

#[cfg(test)]
pub(crate) struct AbortCannotEndHandle {
    key: (TxnId, Id),
}

#[cfg(test)]
impl Drop for AbortCannotEndHandle {
    fn drop(&mut self) {
        abort_cannot_end_hooks().lock().remove(&self.key);
    }
}

#[cfg(test)]
static ABORT_CANNOT_END_HOOKS: OnceLock<Mutex<BTreeMap<(TxnId, Id), bool>>> = OnceLock::new();

#[cfg(test)]
fn abort_cannot_end_hooks() -> &'static Mutex<BTreeMap<(TxnId, Id), bool>> {
    ABORT_CANNOT_END_HOOKS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn install_abort_cannot_end_for_cell(tid: TxnId, id: Id) -> AbortCannotEndHandle {
    install_abort_cannot_end_for_cell_with_mode(tid, id, false)
}

#[cfg(test)]
pub(crate) fn install_persistent_abort_cannot_end_for_cell(
    tid: TxnId,
    id: Id,
) -> AbortCannotEndHandle {
    install_abort_cannot_end_for_cell_with_mode(tid, id, true)
}

#[cfg(test)]
fn install_abort_cannot_end_for_cell_with_mode(
    tid: TxnId,
    id: Id,
    persistent: bool,
) -> AbortCannotEndHandle {
    let key = (tid, id);
    abort_cannot_end_hooks()
        .lock()
        .insert(key.clone(), persistent);
    AbortCannotEndHandle { key }
}

#[cfg(test)]
pub(crate) struct EndPromotionFailureHandle {
    key: (TxnId, Id),
}

#[cfg(test)]
impl Drop for EndPromotionFailureHandle {
    fn drop(&mut self) {
        end_promotion_failure_hooks().lock().remove(&self.key);
    }
}

#[cfg(test)]
static END_PROMOTION_FAILURE_HOOKS: OnceLock<Mutex<BTreeSet<(TxnId, Id)>>> = OnceLock::new();

#[cfg(test)]
fn end_promotion_failure_hooks() -> &'static Mutex<BTreeSet<(TxnId, Id)>> {
    END_PROMOTION_FAILURE_HOOKS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[cfg(test)]
pub(crate) fn install_end_promotion_failure(tid: TxnId, id: Id) -> EndPromotionFailureHandle {
    let key = (tid, id);
    end_promotion_failure_hooks().lock().insert(key.clone());
    EndPromotionFailureHandle { key }
}

#[cfg(test)]
fn should_fail_end_promotion(tid: &TxnId, id: &Id) -> bool {
    end_promotion_failure_hooks()
        .lock()
        .contains(&(tid.clone(), *id))
}

type CommitHistory = BTreeMap<Id, CellHistory>;
type CellMetaMutex = Arc<Mutex<CellMeta>>;
type TxnMutex = Arc<Mutex<Transaction>>;

/// Per-cell metadata for concurrency control
///
/// Implements a hybrid timestamp-ordering + lock-based protocol with Wait-Die:
/// - `read` / `write`: Track timestamps for timestamp-ordering validation
/// - `owner`: Acts as a write lock during prepare/commit phases
/// - `lock_acquired_at`: Timestamp used for diagnostics only
///
/// Wait-Die Protocol:
/// - When a transaction wants to acquire a cell already owned by another:
///   - If requester is YOUNGER (higher timestamp): DIE (abort immediately)
///   - If requester is OLDER (lower timestamp): WAIT (backoff and retry)
/// - This prevents deadlock while reducing contention on hot cells
/// - Waiters poll via backoff rather than blocking on a condition variable
///
/// Owner age never authorizes takeover. Task 10 adds explicit stale-owner
/// resolution; until then every owner participates in Wait-Die.
#[derive(Debug)]
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnPriority>, // transaction that owns the cell during prepare/commit
    lock_acquired_at: Option<i64>, // timestamp when lock was acquired (milliseconds since epoch)
}

struct Transaction {
    state: TxnState,
    affected_cells: Vec<Id>,
    certified: BTreeMap<Id, PrepareOp>,
    coordinator_id: Option<u64>,
    installed: BTreeMap<Id, InstalledRevision>,
    commit_hlc: Option<Hlc>,
    commit_ops: Option<Vec<CommitOp>>,
    installed_output_durable: bool,
    compensation_output_durable: bool,
    durable_decision: Option<TxnState>,
    last_activity: i64,
    history: CommitHistory,
}

struct CellHistory {
    cell: Option<OwnedCellRef>,
    compensation: Option<InstalledRevision>,
}

impl CellHistory {
    pub fn new(cell: Option<OwnedCellRef>) -> CellHistory {
        CellHistory {
            cell,
            compensation: None,
        }
    }
}

impl Transaction {
    fn certified_op(&self, id: &Id) -> Option<&PrepareOp> {
        self.certified.get(id)
    }

    fn certified_present_revision_ts(&self, id: &Id) -> Option<u64> {
        match self.certified_op(id).map(|op| &op.expectation) {
            Some(CellExpectation::Present(revision_ts)) => Some(*revision_ts),
            _ => None,
        }
    }

    fn certified_expects_absent_write(&self, id: &Id) -> bool {
        matches!(
            self.certified_op(id),
            Some(PrepareOp {
                intent: PrepareIntent::Write,
                expectation: CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent,
                ..
            })
        )
    }
}

pub struct DataManager {
    cells: LFMap<Id, Arc<Mutex<CellMeta>>>,
    txns: LFMap<TxnId, Arc<Mutex<Transaction>>>,
    cell_list: LinkedList<Id>,
    txns_sorted: Mutex<BTreeSet<TxnId>>,
    txn_registry_lock: Mutex<()>,
    cell_cleanup_lock: Mutex<()>,
    owner_resolution_sender: mpsc::Sender<OwnerKey>,
    owner_resolution_worker_activity: Arc<OwnerResolutionWorkerActivity>,
    owner_index: Mutex<OwnerIndex>,
    participant_lifecycle_shards: [Mutex<()>; PARTICIPANT_LIFECYCLE_SHARD_COUNT],
    participant_completions: Mutex<HashMap<TxnId, ParticipantCompletionEvidence>>,
    participant_completion_cache_ready: bool,
    #[cfg(test)]
    participant_clock_overrides: Mutex<HashMap<TxnId, i64>>,
    #[cfg(test)]
    resolution_attempts: Mutex<BTreeMap<(TxnId, u64), u64>>,
    database_runtime: Arc<DatabaseRuntime>,
    cleanup_signal: Arc<AtomicBool>,
    /// Per-server Hybrid Logical Clock source (node = server_id), shared with
    /// the coordinator-side `TransactionManager`. Stamps every participant
    /// response clock and observes the coordinator's incoming clock.
    hlc: Arc<bifrost::hlc::HlcSource>,
    lock_timeout_ms: i64,
    #[cfg(test)]
    fail_next_undo_availability: AtomicBool,
}

#[cfg(test)]
type TestDataManagerKey = (u64, String, String);

#[cfg(test)]
static TEST_DATA_MANAGERS: OnceLock<Mutex<BTreeMap<TestDataManagerKey, Weak<DataManager>>>> =
    OnceLock::new();

#[cfg(test)]
fn test_data_managers() -> &'static Mutex<BTreeMap<TestDataManagerKey, Weak<DataManager>>> {
    TEST_DATA_MANAGERS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
fn register_data_manager_for_test(manager: &Arc<DataManager>) {
    let key = (
        manager.hlc.node(),
        manager.database_runtime.group_name().to_string(),
        manager.database_runtime.database_name().to_string(),
    );
    test_data_managers()
        .lock()
        .insert(key, Arc::downgrade(manager));
}

#[cfg(test)]
pub(crate) struct ParticipantClockHandle {
    manager: Weak<DataManager>,
    tid: TxnId,
}

#[cfg(test)]
impl ParticipantClockHandle {
    pub(crate) fn advance_by(&self, delta_ms: i64) {
        if let Some(manager) = self.manager.upgrade() {
            let mut clocks = manager.participant_clock_overrides.lock();
            let now_ms = clocks
                .get_mut(&self.tid)
                .expect("participant test clock must remain installed");
            *now_ms = now_ms.saturating_add(delta_ms);
        }
    }
}

#[cfg(test)]
impl Drop for ParticipantClockHandle {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            manager.participant_clock_overrides.lock().remove(&self.tid);
        }
    }
}

#[cfg(test)]
pub(crate) fn install_participant_clock_for_test(
    server_id: u64,
    group_name: &str,
    database_name: &str,
    tid: TxnId,
    now_ms: i64,
) -> Option<ParticipantClockHandle> {
    let key = (server_id, group_name.to_string(), database_name.to_string());
    let manager = test_data_managers().lock().get(&key)?.upgrade()?;
    manager
        .participant_clock_overrides
        .lock()
        .insert(tid, now_ms);
    Some(ParticipantClockHandle {
        manager: Arc::downgrade(&manager),
        tid,
    })
}

#[cfg(test)]
pub(crate) fn participant_owner_for_test(
    server_id: u64,
    group_name: &str,
    database_name: &str,
    id: &Id,
) -> Option<TxnPriority> {
    let key = (server_id, group_name.to_string(), database_name.to_string());
    let manager = test_data_managers().lock().get(&key)?.upgrade()?;
    let cell_meta = manager.cells.get(id)?;
    let owner = cell_meta.lock().owner.clone();
    owner
}

#[cfg(test)]
pub(crate) fn participant_completion_for_test(
    server_id: u64,
    group_name: &str,
    database_name: &str,
    tid: &TxnId,
) -> Option<(TxnState, Option<i64>)> {
    let key = (server_id, group_name.to_string(), database_name.to_string());
    let manager = test_data_managers().lock().get(&key)?.upgrade()?;
    let completion = manager
        .participant_completions
        .lock()
        .get(tid)
        .copied()
        .map(|evidence| (evidence.outcome, evidence.expires_at_ms));
    completion
}

#[cfg(test)]
pub(crate) fn participant_completion_outcome_at_for_test(
    server_id: u64,
    group_name: &str,
    database_name: &str,
    tid: &TxnId,
    now_ms: i64,
) -> Option<TxnState> {
    let key = (server_id, group_name.to_string(), database_name.to_string());
    let manager = test_data_managers().lock().get(&key)?.upgrade()?;
    let outcome = manager
        .participant_completions
        .lock()
        .get(tid)
        .and_then(|evidence| evidence.outcome_at(now_ms));
    outcome
}

service! {
    rpc read(server_id: u64, clock: Hlc, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>;
    rpc read_selected(server_id: u64, clock: Hlc, tid: TxnId, id: Id, fields: Vec<u64>) -> DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>;
    rpc read_partial_raw(server_id: u64, clock: Hlc, tid: TxnId, id: Id, offset: usize, len: usize) -> DataSiteResponse<TxnExecResult<ObservedPoint<Vec<u8>>, ReadError>>;
    rpc head(server_id: u64, clock: Hlc, tid: TxnId, id: Id) -> DataSiteResponse<TxnExecResult<ObservedPoint<CellHeader>, ReadError>>;
    // two phase commit
    rpc prepare(coordinator_id: u64, clock: Hlc, tid: TxnId, ops: Vec<PrepareOp>) -> DataSiteResponse<DMPrepareResult>;
    rpc commit(commit_hlc: Hlc, tid: TxnId, cells: Vec<CommitOp>) -> DataSiteResponse<DMCommitResult>;

    // because there may be some exception on commit, abort have to handle 'committed' and 'committing' transactions
    // for committed transaction, abort need to recover the data according to it's cells history
    rpc abort(clock: Hlc, tid: TxnId) -> DataSiteResponse<AbortResult>;

    // there also should be a 'end' from transaction manager to inform data manager to clean up and release cell locks
    rpc end(clock: Hlc, tid: TxnId) -> DataSiteResponse<EndResult>;
    // Retirement is off the user-visible completion path. The prepare step
    // retains idempotence evidence until the coordinator durably acknowledges
    // it; finalize starts the participant-side expiry clock.
    rpc retire(clock: Hlc, tid: TxnId, resolution: TxnResolution) -> DataSiteResponse<EndResult>;
    rpc finalize_retirement(clock: Hlc, tid: TxnId, resolution: TxnResolution) -> DataSiteResponse<EndResult>;
}

dispatch_rpc_service_functions!(DataManager);

service_with_id!(DataManager, DEFAULT_SERVICE_ID);

impl DataManager {
    pub fn new(
        database_runtime: Arc<DatabaseRuntime>,
        hlc: Arc<bifrost::hlc::HlcSource>,
    ) -> Arc<Self> {
        Self::new_inner(database_runtime, hlc, DEFAULT_LOCK_TIMEOUT_MS)
    }

    #[cfg(test)]
    pub(crate) fn new_with_lock_timeout(
        database_runtime: Arc<DatabaseRuntime>,
        hlc: Arc<bifrost::hlc::HlcSource>,
        lock_timeout_ms: i64,
    ) -> Arc<Self> {
        Self::new_inner(database_runtime, hlc, lock_timeout_ms)
    }

    fn new_inner(
        database_runtime: Arc<DatabaseRuntime>,
        hlc: Arc<bifrost::hlc::HlcSource>,
        lock_timeout_ms: i64,
    ) -> Arc<Self> {
        assert!(lock_timeout_ms > 0, "lock timeout must be positive");
        let cleanup_signal = Arc::new(AtomicBool::new(false));
        let (owner_resolution_sender, owner_resolution_receiver) =
            mpsc::channel(OWNER_RESOLUTION_QUEUE_CAPACITY);
        let owner_resolution_worker_activity = Arc::new(OwnerResolutionWorkerActivity::default());
        let (participant_completions, participant_completion_cache_ready) =
            match database_runtime.undo_log() {
                Some(undo_log) => match undo_log.participant_completion_cache_at(get_time()) {
                    Ok(completions) => (
                        completions
                            .into_iter()
                            .map(|(tid, (outcome, expires_at_ms))| {
                                (
                                    tid,
                                    ParticipantCompletionEvidence::durable(outcome, expires_at_ms),
                                )
                            })
                            .collect(),
                        true,
                    ),
                    Err(error) => {
                        error!(
                            "Failed to rebuild participant completion cache at startup: {:?}",
                            error
                        );
                        (HashMap::new(), false)
                    }
                },
                None => (HashMap::new(), true),
            };
        let manager = Arc::new(Self {
            cells: LFMap::with_capacity(256),
            txns: LFMap::with_capacity(128),
            cell_list: LinkedList::new(),
            txns_sorted: Mutex::new(BTreeSet::new()),
            txn_registry_lock: Mutex::new(()),
            cell_cleanup_lock: Mutex::new(()),
            owner_resolution_sender,
            owner_resolution_worker_activity: owner_resolution_worker_activity.clone(),
            owner_index: Mutex::new(OwnerIndex::default()),
            participant_lifecycle_shards: std::array::from_fn(|_| Mutex::new(())),
            participant_completions: Mutex::new(participant_completions),
            participant_completion_cache_ready,
            #[cfg(test)]
            participant_clock_overrides: Mutex::new(HashMap::new()),
            #[cfg(test)]
            resolution_attempts: Mutex::new(BTreeMap::new()),
            database_runtime,
            cleanup_signal: cleanup_signal.clone(),
            hlc,
            lock_timeout_ms,
            #[cfg(test)]
            fail_next_undo_availability: AtomicBool::new(false),
        });
        #[cfg(test)]
        register_data_manager_for_test(&manager);
        Self::spawn_owner_resolution_worker(
            &manager,
            owner_resolution_receiver,
            owner_resolution_worker_activity,
        );

        let manager_ref = Arc::downgrade(&manager);
        tokio::spawn(async move {
            loop {
                let Some(manager) = manager_ref.upgrade() else {
                    break;
                };
                manager.prune_participant_completions_at(get_time());
                if cleanup_signal.load(Relaxed) {
                    manager.cell_meta_cleanup().await;
                    cleanup_signal.store(false, Relaxed);
                }
                drop(manager);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        // Spawn undo log trimming task if undo log is enabled
        if manager.undo_log().is_some() {
            let manager_ref = Arc::downgrade(&manager);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(300)).await; // Trim every 5 minutes
                    let Some(manager) = manager_ref.upgrade() else {
                        break;
                    };
                    if let Some(undo_log) = manager.undo_log() {
                        if let Err(e) = undo_log.trim_old_logs() {
                            error!("Failed to trim undo logs: {:?}", e);
                        } else {
                            debug!("Successfully trimmed old undo logs");
                        }
                    }
                }
            });
        }

        return manager;
    }

    fn spawn_owner_resolution_worker(
        manager: &Arc<Self>,
        mut receiver: mpsc::Receiver<OwnerKey>,
        activity: Arc<OwnerResolutionWorkerActivity>,
    ) {
        let manager_ref = Arc::downgrade(manager);
        tokio::spawn(async move {
            let _worker_guard = OwnerResolutionWorkerGuard::new(activity);
            let mut discovery = tokio::time::interval(Duration::from_millis(
                OWNER_RESOLUTION_DISCOVERY_INTERVAL_MS,
            ));
            discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    owner_key = receiver.recv() => {
                        let Some(owner_key) = owner_key else {
                            break;
                        };
                        tokio::time::sleep(Duration::from_millis(
                            OWNER_RESOLUTION_MIN_ATTEMPT_INTERVAL_MS,
                        ))
                        .await;
                        let Some(manager) = manager_ref.upgrade() else {
                            break;
                        };
                        let owner = TxnPriority::new(owner_key.0, owner_key.1);
                        manager.resolve_stale_owner_once(&owner).await;
                        manager
                            .owner_index
                            .lock()
                            .finish_resolution_attempt(owner_key, get_time());
                        manager.refill_owner_resolution_queue();
                    }
                    _ = discovery.tick() => {
                        let Some(manager) = manager_ref.upgrade() else {
                            break;
                        };
                        manager.refill_owner_resolution_queue();
                    }
                }
            }
        });
    }

    fn update_clock(&self, clock: Hlc) {
        self.hlc.observe(clock);
    }

    fn prune_participant_completions_at(&self, now_ms: i64) {
        self.participant_completions
            .lock()
            .retain(|_, completion| completion.outcome_at(now_ms).is_some());
    }

    #[cfg(test)]
    fn participant_time(&self, tid: &TxnId) -> i64 {
        self.participant_clock_overrides
            .lock()
            .get(tid)
            .copied()
            .unwrap_or_else(get_time)
    }

    #[cfg(not(test))]
    fn participant_time(&self, _tid: &TxnId) -> i64 {
        get_time()
    }

    fn cached_participant_completion_at(&self, tid: &TxnId, now_ms: i64) -> Option<TxnState> {
        let mut completions = self.participant_completions.lock();
        Self::cached_participant_completion_from(&mut completions, tid, now_ms)
    }

    fn cached_participant_completion_from(
        completions: &mut HashMap<TxnId, ParticipantCompletionEvidence>,
        tid: &TxnId,
        now_ms: i64,
    ) -> Option<TxnState> {
        match completions.get(tid).copied() {
            Some(completion) => match completion.outcome_at(now_ms) {
                Some(outcome) => Some(outcome),
                None => {
                    completions.remove(tid);
                    None
                }
            },
            None => None,
        }
    }

    fn record_volatile_completion(&self, tid: TxnId, outcome: TxnState) {
        let now_ms = self.participant_time(&tid);
        let mut completions = self.participant_completions.lock();
        if completions
            .get(&tid)
            .is_some_and(|completion| completion.outcome_at(now_ms).is_none())
        {
            completions.remove(&tid);
        }
        completions
            .entry(tid)
            .or_insert_with(|| ParticipantCompletionEvidence::pending_retirement(outcome));
    }

    fn record_durable_completion(&self, tid: TxnId, outcome: TxnState, expires_at_ms: Option<i64>) {
        self.participant_completions.lock().insert(
            tid,
            ParticipantCompletionEvidence::durable(outcome, expires_at_ms),
        );
    }

    fn participant_completion_evidence(
        &self,
        tid: &TxnId,
        now_ms: i64,
    ) -> std::io::Result<Option<TxnState>> {
        if !self.participant_completion_cache_ready {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "participant completion cache was not rebuilt",
            ));
        }
        Ok(self.cached_participant_completion_at(tid, now_ms))
    }

    fn participant_lifecycle_guard(&self, tid: &TxnId) -> parking_lot::MutexGuard<'_, ()> {
        let mut hasher = DefaultHasher::new();
        tid.hash(&mut hasher);
        let shard = (hasher.finish() as usize) % PARTICIPANT_LIFECYCLE_SHARD_COUNT;
        self.participant_lifecycle_shards[shard].lock()
    }

    fn end_result_from_completion_evidence(&self, tid: &TxnId) -> EndResult {
        match self.participant_completion_evidence(tid, self.participant_time(tid)) {
            Ok(Some(TxnState::Committed | TxnState::Aborted)) => EndResult::Success,
            Ok(_) => EndResult::CheckFailed(CheckError::NotExisted),
            Err(error) => {
                error!(
                    "Failed to read participant completion for transaction {:?}: {:?}",
                    tid, error
                );
                EndResult::CheckFailed(CheckError::CannotEnd)
            }
        }
    }

    fn abort_result_from_completion_evidence(&self, tid: &TxnId) -> AbortResult {
        match self.participant_completion_evidence(tid, self.participant_time(tid)) {
            Ok(Some(TxnState::Aborted)) => AbortResult::CheckFailed(CheckError::AlreadyAborted),
            Ok(Some(TxnState::Committed)) => AbortResult::CheckFailed(CheckError::AlreadyCommitted),
            Ok(_) => AbortResult::CheckFailed(CheckError::NotExisted),
            Err(error) => {
                error!(
                    "Failed to read participant completion for transaction {:?}: {:?}",
                    tid, error
                );
                AbortResult::CheckFailed(CheckError::CannotEnd)
            }
        }
    }
    #[inline]
    fn get_or_create_transaction(&self, tid: &TxnId) -> TxnMutex {
        self.get_or_create_transaction_with_status(tid).0
    }

    fn get_or_create_transaction_with_status(&self, tid: &TxnId) -> (TxnMutex, bool) {
        if let Some(txn) = self.txns.get(tid) {
            return (txn, false);
        }
        self.create_transaction_with_status(tid)
    }
    #[inline]
    fn find_transaction(&self, tid: &TxnId) -> Option<TxnMutex> {
        self.txns.get(tid)
    }

    #[cold]
    fn create_transaction_with_status(&self, tid: &TxnId) -> (TxnMutex, bool) {
        let _registry_guard = self.txn_registry_lock.lock();
        loop {
            if let Some(txn) = self.txns.get(tid) {
                return (txn, false);
            }

            let txn = Arc::new(Mutex::new(Transaction {
                state: TxnState::Started,
                affected_cells: Vec::with_capacity(8), // Pre-allocate for common case
                certified: BTreeMap::new(),
                coordinator_id: None,
                installed: BTreeMap::new(),
                commit_hlc: None,
                commit_ops: None,
                installed_output_durable: false,
                compensation_output_durable: false,
                durable_decision: None,
                last_activity: get_time(),
                history: BTreeMap::new(),
            }));

            // Publish the conservative cleanup watermark first, then expose
            // the transaction map entry. Registry removal uses the same
            // mutex, so no completed transaction can leave a ghost TID and
            // readers can never observe a map entry without sorted
            // membership.
            self.txns_sorted.lock().insert(*tid);
            match self.txns.try_insert(*tid, txn.clone()) {
                None => {
                    #[cfg(test)]
                    pause_after_txn_registry_publish(tid);
                    return (txn, true);
                }
                Some(existing) => return (existing, false),
            }
        }
    }
    fn cell_meta_mutex(&self, id: &Id) -> CellMetaMutex {
        if let Some(meta) = self.cells.get(id) {
            return meta;
        }
        let new_meta = Arc::new(Mutex::new(CellMeta {
            read: TxnId::default(),
            write: TxnId::default(),
            owner: None,
            lock_acquired_at: None,
        }));
        match self.cells.try_insert(*id, new_meta.clone()) {
            None => {
                self.cell_list.push_back(*id);
                new_meta
            }
            Some(existing) => existing,
        }
    }

    fn cell_meta_is_current(&self, id: &Id, candidate: &CellMetaMutex) -> bool {
        self.cells
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(&current, candidate))
    }
    fn response_with<T: Send>(&self, data: T) -> BoxFuture<'_, DataSiteResponse<T>>
    where
        T: 'static,
    {
        future::ready(DataSiteResponse::new(self.hlc.now(), data)).boxed()
    }

    fn owner_is_present(&self, owner: &TxnPriority) -> bool {
        self.owner_index.lock().contains_owner(owner)
    }

    fn transaction_owner_is_present(&self, tid: &TxnId) -> bool {
        self.owner_index.lock().by_tid.contains_key(tid)
    }

    fn resolution_outcome(resolution: TxnResolution) -> Option<TxnState> {
        match resolution {
            TxnResolution::Commit(_) => Some(TxnState::Committed),
            TxnResolution::Abort => Some(TxnState::Aborted),
            TxnResolution::InProgress | TxnResolution::Unknown => None,
        }
    }

    fn queue_stale_owner_resolution(&self, owner: TxnPriority) {
        let key = (owner.tid, owner.coordinator_id);
        if !self.owner_index.lock().request_resolution(key, get_time()) {
            return;
        }
        self.enqueue_owner_resolution_key(key);
    }

    fn enqueue_owner_resolution_key(&self, key: OwnerKey) {
        if self.owner_resolution_sender.try_send(key).is_err() {
            self.owner_index.lock().release_queue_slot(key);
        }
    }

    fn refill_owner_resolution_queue(&self) {
        let available_capacity = self
            .owner_resolution_sender
            .capacity()
            .min(OWNER_RESOLUTION_DISCOVERY_BATCH);
        if available_capacity == 0 {
            return;
        }
        let candidates = self.owner_index.lock().discover_resolution_candidates(
            get_time(),
            OWNER_RESOLUTION_DISCOVERY_BATCH,
            available_capacity,
        );
        for key in candidates {
            if self.owner_resolution_sender.try_send(key).is_err() {
                self.owner_index.lock().release_queue_slot(key);
            }
        }
    }

    #[cfg(test)]
    fn resolution_attempt_count_for_test(&self, owner: &TxnPriority) -> u64 {
        self.resolution_attempts
            .lock()
            .get(&(owner.tid, owner.coordinator_id))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn queued_resolution_count_for_test(&self) -> usize {
        self.owner_index.lock().requested_resolution_count()
    }

    #[cfg(test)]
    fn resolver_scheduled_storage_len_for_test(&self) -> usize {
        OWNER_RESOLUTION_QUEUE_CAPACITY - self.owner_resolution_sender.capacity()
    }

    #[cfg(test)]
    fn resolver_worker_cardinality_for_test(&self) -> usize {
        self.owner_resolution_worker_activity.live.load(Relaxed)
    }

    #[cfg(test)]
    fn total_resolution_attempt_count_for_test(&self) -> u64 {
        self.resolution_attempts.lock().values().copied().sum()
    }

    #[cfg(test)]
    fn note_resolution_attempt(&self, owner: &TxnPriority) {
        *self
            .resolution_attempts
            .lock()
            .entry((owner.tid, owner.coordinator_id))
            .or_default() += 1;
    }

    #[cfg(not(test))]
    #[inline]
    fn note_resolution_attempt(&self, _owner: &TxnPriority) {}

    async fn resolve_stale_owner_once(&self, owner: &TxnPriority) -> bool {
        self.note_resolution_attempt(owner);

        if !self.owner_is_present(owner) {
            return true;
        }
        let Some(coordinator) = self.database_runtime.txn_manager() else {
            return false;
        };
        let resolution = match coordinator.resolve_at_coordinator(owner).await {
            Ok(resolution) => resolution,
            Err(error) => {
                debug!(
                    "Could not resolve stale owner {:?} at coordinator {}: {:?}",
                    owner, owner.coordinator_id, error
                );
                return false;
            }
        };

        match resolution {
            TxnResolution::Commit(commit_hlc) => {
                let exact_install = self.find_transaction(&owner.tid).is_some_and(|txn| {
                    let txn = txn.lock();
                    txn.state == TxnState::Committed
                        && txn.coordinator_id == Some(owner.coordinator_id)
                        && txn.commit_hlc == Some(commit_hlc)
                        && self.installed_revisions_agree(&txn)
                });
                if !exact_install {
                    return !self.owner_is_present(owner);
                }

                let result = <DataManager as Service>::end(self, self.hlc.now(), owner.tid)
                    .await
                    .payload;
                matches!(result, EndResult::Success) || !self.owner_is_present(owner)
            }
            TxnResolution::Abort => {
                let abort = <DataManager as Service>::abort(self, self.hlc.now(), owner.tid)
                    .await
                    .payload;
                if !matches!(
                    abort,
                    AbortResult::Success(None)
                        | AbortResult::CheckFailed(CheckError::AlreadyAborted)
                ) {
                    return !self.owner_is_present(owner);
                }

                let end = <DataManager as Service>::end(self, self.hlc.now(), owner.tid)
                    .await
                    .payload;
                matches!(end, EndResult::Success) || !self.owner_is_present(owner)
            }
            TxnResolution::InProgress | TxnResolution::Unknown => false,
        }
    }

    fn retire_participant_evidence(
        &self,
        tid: &TxnId,
        resolution: TxnResolution,
        finalized: bool,
    ) -> EndResult {
        let Some(outcome) = Self::resolution_outcome(resolution) else {
            return EndResult::CheckFailed(CheckError::CannotEnd);
        };
        let _lifecycle_guard = self.participant_lifecycle_guard(tid);
        if self.find_transaction(tid).is_some() || self.transaction_owner_is_present(tid) {
            return EndResult::CheckFailed(CheckError::CannotEnd);
        }

        let Some(undo_log) = self.undo_log() else {
            let now_ms = self.participant_time(tid);
            let mut completions = self.participant_completions.lock();
            let completion = completions.get(tid).copied();
            if completion.is_some_and(|completion| completion.outcome != outcome) {
                return EndResult::CheckFailed(CheckError::CannotEnd);
            }
            if !finalized {
                return if completion
                    .and_then(|completion| completion.outcome_at(now_ms))
                    .is_some()
                {
                    EndResult::Success
                } else {
                    EndResult::CheckFailed(CheckError::NotExisted)
                };
            }
            if completion.is_some_and(|completion| {
                completion
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| now_ms < expires_at_ms)
            }) {
                return EndResult::Success;
            }
            completions.insert(
                *tid,
                ParticipantCompletionEvidence::durable(
                    outcome,
                    Some(now_ms.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS)),
                ),
            );
            return EndResult::Success;
        };

        let retirement = match undo_log.participant_retirement(tid) {
            Ok(retirement) => retirement,
            Err(error) => {
                error!(
                    "Failed to read participant retirement for {:?}: {:?}",
                    tid, error
                );
                return EndResult::CheckFailed(CheckError::CannotEnd);
            }
        };
        if let Some(record) = retirement.as_ref() {
            if record.outcome != outcome {
                return EndResult::CheckFailed(CheckError::CannotEnd);
            }
            if record.finalized && (!finalized || self.participant_time(tid) < record.expires_at_ms)
            {
                self.record_durable_completion(*tid, outcome, Some(record.expires_at_ms));
                return EndResult::Success;
            }
            if !record.finalized && !finalized {
                self.record_durable_completion(*tid, outcome, None);
                return EndResult::Success;
            }
        } else {
            let completion = match undo_log.participant_completion(tid) {
                Ok(completion) => completion,
                Err(error) => {
                    error!(
                        "Failed to read participant completion for retirement {:?}: {:?}",
                        tid, error
                    );
                    return EndResult::CheckFailed(CheckError::CannotEnd);
                }
            };
            if completion.is_some_and(|completion| completion != outcome)
                || (!finalized && completion != Some(outcome))
                || (finalized && undo_log.has_active_undo(tid))
            {
                return EndResult::CheckFailed(CheckError::CannotEnd);
            }
        }

        #[cfg(test)]
        pause_after_retirement_read(tid, finalized);
        let expires_at_ms = self
            .participant_time(tid)
            .saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS);
        let result = if finalized {
            undo_log.finalize_participant_retirement(tid, outcome, expires_at_ms)
        } else {
            undo_log.write_participant_retirement(tid, outcome, 0)
        };
        match result {
            Ok(()) => {
                self.record_durable_completion(*tid, outcome, finalized.then_some(expires_at_ms));
                EndResult::Success
            }
            Err(error) => {
                error!(
                    "Failed to persist participant retirement for {:?}: {:?}",
                    tid, error
                );
                EndResult::CheckFailed(CheckError::CannotEnd)
            }
        }
    }

    #[cfg(test)]
    fn take_matching_prepare_delay(
        tid: &TxnId,
        prepared_ops: &[PrepareOp],
    ) -> Option<Arc<PrepareDelayState>> {
        let mut hooks = prepare_delay_hooks().lock();
        let delayed_key = prepared_ops
            .iter()
            .map(|op| (tid.clone(), op.id))
            .find(|key| hooks.contains_key(key))?;
        hooks.remove(&delayed_key)
    }

    #[cfg(test)]
    fn take_matching_meta_acquire_delay(
        tid: &TxnId,
        prepared_ops: &[PrepareOp],
    ) -> Option<Arc<PrepareDelayState>> {
        let mut hooks = meta_acquire_delay_hooks().lock();
        let delayed_key = prepared_ops
            .iter()
            .map(|op| (tid.clone(), op.id))
            .find(|key| hooks.contains_key(key))?;
        hooks.remove(&delayed_key)
    }

    #[cfg(test)]
    fn take_matching_abort_cannot_end(tid: &TxnId, affected_cells: &[Id]) -> bool {
        let mut hooks = abort_cannot_end_hooks().lock();
        let Some((key, persistent)) = affected_cells
            .iter()
            .map(|id| (tid.clone(), *id))
            .find_map(|key| hooks.get(&key).copied().map(|persistent| (key, persistent)))
        else {
            return false;
        };
        if !persistent {
            hooks.remove(&key);
        }
        true
    }

    #[cfg(test)]
    async fn await_prepare_delay(state: &Arc<PrepareDelayState>) {
        state
            .entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.entered_notify.notify_waiters();
        let notified = state.released_notify.notified();
        if state.released.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    #[cfg(test)]
    fn take_matching_commit_delay(
        tid: &TxnId,
        cells: &[CommitOp],
    ) -> Option<Arc<PrepareDelayState>> {
        let mut hooks = commit_delay_hooks().lock();
        let delayed_key = cells
            .iter()
            .filter_map(|op| Self::commit_op_cell_id(op).ok())
            .map(|id| (*tid, id))
            .find(|key| hooks.contains_key(key))?;
        hooks.remove(&delayed_key)
    }

    fn canonical_prepare_ops(ops: Vec<PrepareOp>) -> Result<Vec<PrepareOp>, DMPrepareResult> {
        let mut by_id = BTreeMap::new();
        for op in ops {
            if by_id.insert(op.id, op).is_some() {
                return Err(DMPrepareResult::NotRealizable);
            }
        }
        if by_id.is_empty() {
            return Err(DMPrepareResult::NotRealizable);
        }
        Ok(by_id.into_values().collect())
    }

    fn current_expectation(&self, id: &Id) -> Result<CellExpectation, ReadError> {
        match self.chunks().head_snapshot(id, u64::MAX)? {
            SnapshotRead::Present(header) => Ok(CellExpectation::Present(header.revision_ts)),
            SnapshotRead::Absent(tombstone_revision_ts) => {
                Ok(CellExpectation::Absent(tombstone_revision_ts))
            }
            SnapshotRead::Wait => Err(ReadError::NotMatch),
        }
    }

    fn expectation_matches(op: &PrepareOp, current: &CellExpectation) -> bool {
        match (&op.expectation, op.intent) {
            (CellExpectation::UnobservedAbsent, PrepareIntent::Write) => {
                matches!(current, CellExpectation::Absent(_))
            }
            (CellExpectation::UnobservedAbsent, _) => false,
            (expected, _) => expected == current,
        }
    }

    fn prepare_expectation_matches(&self, op: &PrepareOp) -> bool {
        self.current_expectation(&op.id)
            .is_ok_and(|current| Self::expectation_matches(op, &current))
    }

    #[inline]
    fn chunks(&self) -> &Arc<crate::ram::chunk::Chunks> {
        self.database_runtime.chunks()
    }

    #[inline]
    fn undo_log(&self) -> Option<&Arc<super::undo_log::UndoLogger>> {
        self.database_runtime.undo_log()
    }

    #[cfg(test)]
    fn fail_next_undo_availability_for_test(&self) {
        self.fail_next_undo_availability
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn rollback(
        &self,
        history: &mut CommitHistory,
        installed_revisions: &BTreeMap<Id, InstalledRevision>,
    ) -> Vec<RollbackFailure> {
        let mut failures = Vec::new();
        for (id, history) in history.iter_mut() {
            debug!("ROLLING BACK {:?}", id);
            let install_result = if history.compensation.is_none() {
                installed_revisions
                    .get(id)
                    .ok_or(WriteError::CellRevisionMismatch)
                    .and_then(|installed| {
                        self.chunks().compensate(
                            installed,
                            history.cell.as_ref().map(|cell| cell.clone_referred()),
                        )
                    })
                    .map(|compensation| {
                        history.compensation = Some(compensation);
                    })
            } else {
                Ok(())
            };
            let result = install_result.and_then(|()| {
                let compensation = history
                    .compensation
                    .as_ref()
                    .expect("successful compensation install retains its exact handle");
                self.chunks()
                    .force_sync_installed_revisions([compensation])
                    .map_err(|error| WriteError::DurabilityFailure(error.to_string()))
            });
            let error = result.err();
            if let Some(error) = error {
                failures.push(RollbackFailure { id: *id, error });
            }
        }
        failures
    }
    #[inline]
    fn guarded_txn_cell_ids(txn: &Transaction) -> Vec<Id> {
        txn.affected_cells
            .iter()
            .copied()
            .chain(txn.certified.keys().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
    #[inline]
    fn wipe_out_transaction(&self, tid: &TxnId, expected: &TxnMutex) -> bool {
        let _registry_guard = self.txn_registry_lock.lock();
        if !self
            .txns
            .get(tid)
            .is_some_and(|current| Arc::ptr_eq(&current, expected))
        {
            return false;
        }
        let _ = self.txns.remove(tid);
        self.txns_sorted.lock().remove(tid);
        true
    }
    async fn cell_meta_cleanup(&self) {
        let _cleanup_guard = self.cell_cleanup_lock.lock();
        let oldest_transaction = {
            self.txns_sorted
                .lock()
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(|| self.hlc.now())
        };
        let mut cells_to_evict = Vec::new();

        // First pass: collect cells with their metadata (while holding locks)
        for cell_id_ref in self.cell_list.iter_front() {
            let cell_id = cell_id_ref.deref();
            if let Some(cell_meta) = self.cells.get(&cell_id) {
                let meta = cell_meta.lock();
                if meta.write < oldest_transaction && meta.read < oldest_transaction {
                    cells_to_evict.push((cell_id_ref, cell_meta.clone()));
                } else {
                    // Cells are ordered by activity, stop at first active one
                    break;
                }
            } else {
                // Cell not in map but in list - just remove from list
                cell_id_ref.remove();
            }
        }

        // Second pass: re-check and remove (prevents TOCTOU race)
        for (cell_id_ref, cell_meta) in cells_to_evict {
            let cell_id = cell_id_ref.deref();

            // Keep the candidate locked through identity validation and removal.
            // A caller that fetched this Arc first either stamps it before this
            // check, or observes that it became orphaned and retries the lookup.
            let meta = cell_meta.lock();
            if self.cell_meta_is_current(&cell_id, &cell_meta)
                && meta.write < oldest_transaction
                && meta.read < oldest_transaction
                && meta.owner.is_none()
            {
                self.cells.remove(&cell_id);
                cell_id_ref.remove();
            }
        }
    }
    fn prepare_read<T: Send>(
        &self,
        clock: &Hlc,
        tid: &TxnId,
        id: &Id,
    ) -> Result<(), BoxFuture<'_, DataSiteResponse<TxnExecResult<T, ReadError>>>>
    where
        T: 'static + Clone,
    {
        self.update_clock(*clock);
        loop {
            let meta_ref = self.cell_meta_mutex(id);
            let mut meta = meta_ref.lock();
            if !self.cell_meta_is_current(id, &meta_ref) {
                drop(meta);
                continue;
            }
            if meta.owner.is_some() {
                debug!(
                    "-> READ {:?} WAITING for {:?} to finish commit on cell {:?}",
                    tid, &meta.owner, id
                );
                return Err(self.response_with(TxnExecResult::Wait));
            }

            if meta.read < *tid {
                meta.read = *tid;
            }
            return Ok(());
        }
    }

    fn observed_snapshot<T, F>(
        snapshot: Result<SnapshotRead<T>, ReadError>,
        revision_ts: F,
    ) -> TxnExecResult<ObservedPoint<T>, ReadError>
    where
        T: Send + Clone,
        F: FnOnce(&T) -> u64,
    {
        match snapshot {
            Ok(SnapshotRead::Present(value)) => {
                let expectation = CellExpectation::Present(revision_ts(&value));
                TxnExecResult::Accepted(ObservedPoint {
                    value: Some(value),
                    expectation,
                })
            }
            Ok(SnapshotRead::Absent(delete_revision_ts)) => {
                TxnExecResult::Accepted(ObservedPoint {
                    value: None,
                    expectation: CellExpectation::Absent(delete_revision_ts),
                })
            }
            Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
            Err(error) => TxnExecResult::Error(error),
        }
    }

    fn warn_on_index_wait_results<I>(&self, tid: &TxnId, results: I)
    where
        I: IntoIterator<
            Item = Result<Result<(), crate::index::builder::IndexError>, tokio::task::JoinError>,
        >,
    {
        for result in results {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(
                        "Index task failed during transaction commit {:?}: {:?}",
                        tid, error
                    );
                }
                Err(error) => {
                    warn!(
                        "Index task join failed during transaction commit {:?}: {:?}",
                        tid, error
                    );
                }
            }
        }
    }

    fn exact_slice_eq<T>(left: &[T], right: &[T], elements_equal: impl Fn(&T, &T) -> bool) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| elements_equal(left, right))
    }

    fn exact_owned_map_eq(left: &OwnedMap, right: &OwnedMap) -> bool {
        left.fields == right.fields
            && left.map.len() == right.map.len()
            && left.map.iter().all(|(key, left_value)| {
                right
                    .map
                    .get(key)
                    .is_some_and(|right_value| Self::exact_owned_value_eq(left_value, right_value))
            })
    }

    fn exact_owned_primitive_array_eq(left: &OwnedPrimArray, right: &OwnedPrimArray) -> bool {
        match left {
            OwnedPrimArray::Bool(left) => {
                matches!(right, OwnedPrimArray::Bool(right) if left == right)
            }
            OwnedPrimArray::Char(left) => {
                matches!(right, OwnedPrimArray::Char(right) if left == right)
            }
            OwnedPrimArray::I8(left) => {
                matches!(right, OwnedPrimArray::I8(right) if left == right)
            }
            OwnedPrimArray::I16(left) => {
                matches!(right, OwnedPrimArray::I16(right) if left == right)
            }
            OwnedPrimArray::I32(left) => {
                matches!(right, OwnedPrimArray::I32(right) if left == right)
            }
            OwnedPrimArray::I64(left) => {
                matches!(right, OwnedPrimArray::I64(right) if left == right)
            }
            OwnedPrimArray::U8(left) => {
                matches!(right, OwnedPrimArray::U8(right) if left == right)
            }
            OwnedPrimArray::U16(left) => {
                matches!(right, OwnedPrimArray::U16(right) if left == right)
            }
            OwnedPrimArray::U32(left) => {
                matches!(right, OwnedPrimArray::U32(right) if left == right)
            }
            OwnedPrimArray::U64(left) => {
                matches!(right, OwnedPrimArray::U64(right) if left == right)
            }
            OwnedPrimArray::F32(left) => matches!(right, OwnedPrimArray::F32(right)
                if Self::exact_slice_eq(left, right, |left, right| left.to_bits() == right.to_bits())),
            OwnedPrimArray::F64(left) => matches!(right, OwnedPrimArray::F64(right)
                if Self::exact_slice_eq(left, right, |left, right| left.to_bits() == right.to_bits())),
            OwnedPrimArray::Pos2d32(left) => matches!(right, OwnedPrimArray::Pos2d32(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
            })),
            OwnedPrimArray::Pos2d64(left) => matches!(right, OwnedPrimArray::Pos2d64(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
            })),
            OwnedPrimArray::Pos3d32(left) => matches!(right, OwnedPrimArray::Pos3d32(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()
            })),
            OwnedPrimArray::Pos3d64(left) => matches!(right, OwnedPrimArray::Pos3d64(right)
            if Self::exact_slice_eq(left, right, |left, right| {
                left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()
            })),
            OwnedPrimArray::Id(left) => {
                matches!(right, OwnedPrimArray::Id(right) if left == right)
            }
            OwnedPrimArray::String(left) => {
                matches!(right, OwnedPrimArray::String(right) if left == right)
            }
            OwnedPrimArray::Bytes(left) => {
                matches!(right, OwnedPrimArray::Bytes(right) if left.iter().map(|value| &value.data).eq(right.iter().map(|value| &value.data)))
            }
            OwnedPrimArray::SmallBytes(left) => {
                matches!(right, OwnedPrimArray::SmallBytes(right) if left.iter().map(|value| &value.data).eq(right.iter().map(|value| &value.data)))
            }
        }
    }

    fn exact_owned_value_eq(left: &OwnedValue, right: &OwnedValue) -> bool {
        match left {
            OwnedValue::Bool(left) => matches!(right, OwnedValue::Bool(right) if left == right),
            OwnedValue::Char(left) => matches!(right, OwnedValue::Char(right) if left == right),
            OwnedValue::I8(left) => matches!(right, OwnedValue::I8(right) if left == right),
            OwnedValue::I16(left) => matches!(right, OwnedValue::I16(right) if left == right),
            OwnedValue::I32(left) => matches!(right, OwnedValue::I32(right) if left == right),
            OwnedValue::I64(left) => matches!(right, OwnedValue::I64(right) if left == right),
            OwnedValue::U8(left) => matches!(right, OwnedValue::U8(right) if left == right),
            OwnedValue::U16(left) => matches!(right, OwnedValue::U16(right) if left == right),
            OwnedValue::U32(left) => matches!(right, OwnedValue::U32(right) if left == right),
            OwnedValue::U64(left) => matches!(right, OwnedValue::U64(right) if left == right),
            OwnedValue::F32(left) => {
                matches!(right, OwnedValue::F32(right) if left.to_bits() == right.to_bits())
            }
            OwnedValue::F64(left) => {
                matches!(right, OwnedValue::F64(right) if left.to_bits() == right.to_bits())
            }
            OwnedValue::Pos2d32(left) => matches!(right, OwnedValue::Pos2d32(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()),
            OwnedValue::Pos2d64(left) => matches!(right, OwnedValue::Pos2d64(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()),
            OwnedValue::Pos3d32(left) => matches!(right, OwnedValue::Pos3d32(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()),
            OwnedValue::Pos3d64(left) => matches!(right, OwnedValue::Pos3d64(right)
                if left.x.to_bits() == right.x.to_bits()
                    && left.y.to_bits() == right.y.to_bits()
                    && left.z.to_bits() == right.z.to_bits()),
            OwnedValue::Id(left) => matches!(right, OwnedValue::Id(right) if left == right),
            OwnedValue::String(left) => {
                matches!(right, OwnedValue::String(right) if left == right)
            }
            OwnedValue::Bytes(left) => {
                matches!(right, OwnedValue::Bytes(right) if left.data == right.data)
            }
            OwnedValue::SmallBytes(left) => {
                matches!(right, OwnedValue::SmallBytes(right) if left.data == right.data)
            }
            OwnedValue::Map(left) => {
                matches!(right, OwnedValue::Map(right) if Self::exact_owned_map_eq(left, right))
            }
            OwnedValue::Array(left) => matches!(right, OwnedValue::Array(right)
                if Self::exact_slice_eq(left, right, Self::exact_owned_value_eq)),
            OwnedValue::PrimArray(left) => matches!(right, OwnedValue::PrimArray(right)
                if Self::exact_owned_primitive_array_eq(left, right)),
            OwnedValue::Null => matches!(right, OwnedValue::Null),
            OwnedValue::NA => matches!(right, OwnedValue::NA),
        }
    }

    fn commit_cells_equal(left: &OwnedCell, right: &OwnedCell) -> bool {
        left.header.revision_ts == right.header.revision_ts
            && left.header.flags == right.header.flags
            && left.header.schema == right.header.schema
            && left.header.partition == right.header.partition
            && left.header.hash == right.header.hash
            && Self::exact_owned_value_eq(&left.data, &right.data)
    }

    fn commit_ops_equal(left: &CommitOp, right: &CommitOp) -> bool {
        match (left, right) {
            (CommitOp::Write(left), CommitOp::Write(right))
            | (CommitOp::Update(left), CommitOp::Update(right)) => {
                Self::commit_cells_equal(left, right)
            }
            (CommitOp::Remove(left), CommitOp::Remove(right)) => left == right,
            (CommitOp::Read(left_id, left_revision), CommitOp::Read(right_id, right_revision)) => {
                left_id == right_id && left_revision == right_revision
            }
            (CommitOp::None, CommitOp::None) => true,
            _ => false,
        }
    }

    fn canonical_commit_ops(mut cells: Vec<CommitOp>) -> Result<Vec<CommitOp>, DMCommitResult> {
        let mut keyed = Vec::with_capacity(cells.len());
        for cell in cells.drain(..) {
            let id = Self::commit_op_cell_id(&cell).map_err(DMCommitResult::CheckFailed)?;
            keyed.push((id, cell));
        }
        keyed.sort_by_key(|(id, _)| *id);
        Ok(keyed.into_iter().map(|(_, cell)| cell).collect())
    }

    fn commit_requests_equal(left: &[CommitOp], right: &[CommitOp]) -> bool {
        left.len() == right.len()
            && left
                .iter()
                .zip(right)
                .all(|(left, right)| Self::commit_ops_equal(left, right))
    }

    fn apply_commit_ops(
        &self,
        txn_lock: &TxnMutex,
        tid: &TxnId,
        commit_hlc: Hlc,
        cells: Vec<CommitOp>,
    ) -> DMCommitResult {
        let cells = match Self::canonical_commit_ops(cells) {
            Ok(cells) => cells,
            Err(result) => return result,
        };
        let mut txn = txn_lock.lock();
        txn.last_activity = get_time();
        match txn.state {
            TxnState::Started => {
                return DMCommitResult::CheckFailed(CheckError::NotCommitted);
            }
            TxnState::Aborted => {
                return DMCommitResult::CheckFailed(CheckError::AlreadyAborted);
            }
            TxnState::Committed => {
                if txn.commit_hlc == Some(commit_hlc)
                    && txn
                        .commit_ops
                        .as_ref()
                        .is_some_and(|stored| Self::commit_requests_equal(stored, &cells))
                    && self.installed_revisions_agree(&txn)
                {
                    return DMCommitResult::Success;
                }
                return DMCommitResult::CheckFailed(CheckError::AlreadyCommitted);
            }
            TxnState::Cleanup => {
                return DMCommitResult::CheckFailed(CheckError::AlreadyCleanup);
            }
            TxnState::Prepared => {}
        };

        let undo_available = self.undo_log().is_some();
        #[cfg(test)]
        let undo_available = {
            let failure_injected = self
                .fail_next_undo_availability
                .swap(false, std::sync::atomic::Ordering::SeqCst);
            undo_available && !failure_injected
        };
        if self.chunks().durable_storage_configured()
            && (!undo_available || !self.chunks().wal_storage_configured())
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }

        if let Err(result) = Self::validate_commit_subset(&txn, &cells) {
            return result;
        }
        if let Err(result) = Self::validate_commit_payload(&txn, &cells) {
            return result;
        }
        if let Err(result) = Self::validate_commit_hlc(&txn, tid, commit_hlc) {
            return result;
        }
        if txn
            .commit_hlc
            .is_some_and(|installed_hlc| installed_hlc != commit_hlc)
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if txn
            .commit_ops
            .as_ref()
            .is_some_and(|stored| !Self::commit_requests_equal(stored, &cells))
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }

        let Some(coordinator_id) = txn.coordinator_id else {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        };
        let expected_owner = TxnPriority::new(tid.clone(), coordinator_id);
        let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
        let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
        for cell_id in &guarded_cell_ids {
            cell_mutexes.push(self.cell_meta_mutex(cell_id));
        }
        let mut cell_guards = Vec::with_capacity(cell_mutexes.len());
        for cell_mutex in &cell_mutexes {
            cell_guards.push(cell_mutex.lock());
        }
        if cell_guards
            .iter()
            .any(|meta| meta.owner.as_ref() != Some(&expected_owner))
        {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if let Err(result) = self.validate_commit_storage_state(&txn, &cells) {
            return result;
        }

        txn.commit_hlc = Some(commit_hlc);
        txn.commit_ops = Some(cells.clone());
        crate::ram::chunk::set_transaction_context(true);
        let mut write_error: Option<(Id, WriteError)> = None;
        let mut commit_failure: Option<DMCommitResult> = None;
        {
            for cell_op in cells {
                let cell_id = Self::commit_op_cell_id(&cell_op)
                    .expect("commit payload validation rejects non-mutation ops");
                if txn.installed.contains_key(&cell_id) {
                    continue;
                }

                let meta_index = guarded_cell_ids
                    .binary_search(&cell_id)
                    .expect("prepared cell metadata must exist");
                let meta = &mut cell_guards[meta_index];
                match cell_op {
                    CommitOp::Write(mut cell) => {
                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_write(
                                tid.clone(),
                                cell_id,
                                commit_hlc.ts,
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry for new cell: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, &cell_id);
                        }
                        match self.chunks().write_cell_at_revision(
                            &mut cell,
                            RevisionWrite::pending(commit_hlc.ts),
                        ) {
                            Ok(installed) => {
                                txn.history.insert(cell_id, CellHistory::new(None));
                                txn.installed.insert(cell_id, installed);
                                meta.write = commit_hlc;
                            }
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        };
                    }
                    CommitOp::Remove(ref cell_id) => {
                        let expected_revision_ts = txn
                            .certified_present_revision_ts(cell_id)
                            .expect("commit payload validation requires present certification");

                        let old_cell = {
                            let shared_cell = match self.chunks().read_cell(cell_id) {
                                Ok(cell) => cell,
                                Err(read_error) => {
                                    write_error =
                                        Some((*cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            };
                            if shared_cell.header.revision_ts != expected_revision_ts {
                                write_error = Some((*cell_id, WriteError::CellRevisionMismatch));
                                break;
                            }
                            shared_cell.to_owned()
                        };

                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                *cell_id,
                                super::undo_log::UndoOpType::Remove,
                                commit_hlc.ts,
                                old_cell.clone(),
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, cell_id);
                        }

                        match self
                            .chunks()
                            .remove_cell_at_revision(cell_id, RevisionWrite::pending(commit_hlc.ts))
                        {
                            Ok(installed) => {
                                txn.history
                                    .insert(*cell_id, CellHistory::new(Some(old_cell.into_ref())));
                                txn.installed.insert(*cell_id, installed);
                                meta.write = commit_hlc;
                            }
                            Err(error) => {
                                write_error = Some((*cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::Update(mut cell) => {
                        let cell_id = cell.id();
                        let expected_revision_ts = txn
                            .certified_present_revision_ts(&cell_id)
                            .expect("commit payload validation requires present certification");

                        let old_cell = {
                            let shared_cell = match self.chunks().read_cell(&cell_id) {
                                Ok(cell) => cell,
                                Err(read_error) => {
                                    write_error =
                                        Some((cell_id, WriteError::ReadError(read_error)));
                                    break;
                                }
                            };
                            if shared_cell.header.revision_ts != expected_revision_ts {
                                write_error = Some((cell_id, WriteError::CellRevisionMismatch));
                                break;
                            }
                            shared_cell.to_owned()
                        };

                        if let Some(undo_log) = self.undo_log() {
                            let undo_entry = super::undo_log::UndoLogEntry::new_restore(
                                tid.clone(),
                                cell_id,
                                super::undo_log::UndoOpType::Update,
                                commit_hlc.ts,
                                old_cell.clone(),
                            );
                            if let Err(error) = undo_log.write_undo_entry(undo_entry) {
                                error!("Failed to write undo log entry: {:?}", error);
                                commit_failure =
                                    Some(DMCommitResult::CheckFailed(CheckError::CannotEnd));
                                break;
                            }
                            #[cfg(test)]
                            pause_before_storage_mutation(tid, &cell_id);
                        }
                        match self.chunks().update_cell_at_revision(
                            &mut cell,
                            RevisionWrite::pending(commit_hlc.ts),
                        ) {
                            Ok(installed) => {
                                txn.history
                                    .insert(cell_id, CellHistory::new(Some(old_cell.into_ref())));
                                txn.installed.insert(cell_id, installed);
                                meta.write = commit_hlc;
                            }
                            Err(error) => {
                                write_error = Some((cell_id, error));
                                break;
                            }
                        }
                    }
                    CommitOp::Read(_, _) | CommitOp::None => {
                        unreachable!("commit payload validation rejects non-mutation ops")
                    }
                }
            }
        }
        txn.last_activity = get_time();
        crate::ram::chunk::set_transaction_context(false);

        if let Some(failure) = commit_failure {
            return failure;
        }
        if let Some((id, error)) = write_error {
            return Self::map_commit_write_error(&txn, id, error);
        }
        if !self.installed_revisions_agree(&txn) {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        if let Err(error) = self
            .chunks()
            .force_sync_installed_revisions(txn.installed.values())
        {
            error!(
                "Failed to sync exact installed output before participant commit success for transaction {:?}: {:?}",
                tid, error
            );
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        txn.installed_output_durable = true;

        txn.state = TxnState::Committed;

        DMCommitResult::Success
    }

    fn validate_commit_hlc(
        txn: &Transaction,
        tid: &TxnId,
        commit_hlc: Hlc,
    ) -> Result<(), DMCommitResult> {
        if commit_hlc.ts <= tid.ts
            || txn.certified.values().any(|op| {
                let certified_revision = match op.expectation {
                    CellExpectation::Present(revision_ts)
                    | CellExpectation::Absent(Some(revision_ts)) => Some(revision_ts),
                    CellExpectation::Absent(None) | CellExpectation::UnobservedAbsent => None,
                };
                certified_revision.is_some_and(|revision_ts| commit_hlc.ts <= revision_ts)
            })
        {
            Err(DMCommitResult::CheckFailed(CheckError::CannotEnd))
        } else {
            Ok(())
        }
    }

    fn installed_revision_agrees(&self, installed: &InstalledRevision, commit_hlc: Hlc) -> bool {
        if installed.node.revision_ts != commit_hlc.ts {
            return false;
        }
        let (state, location) = installed.node.load();
        if self.chunks().history_location(&installed.id, commit_hlc.ts) != Some(location) {
            return false;
        }
        match state {
            RevisionState::PendingPresent | RevisionState::CommittedPresent => {
                self.chunks().head_cell(&installed.id).is_ok_and(|header| {
                    Id::from_header(&header) == installed.id && header.revision_ts == commit_hlc.ts
                })
            }
            RevisionState::PendingDeleted | RevisionState::CommittedDeleted => matches!(
                self.chunks().head_cell(&installed.id),
                Err(ReadError::CellDoesNotExisted)
            ),
            RevisionState::Aborted | RevisionState::Expired => false,
        }
    }

    fn installed_revisions_agree(&self, txn: &Transaction) -> bool {
        let write_ids: Vec<_> = txn
            .certified
            .iter()
            .filter(|(_, op)| op.intent == PrepareIntent::Write)
            .map(|(id, _)| *id)
            .collect();
        if write_ids.is_empty() {
            return txn.installed.is_empty();
        }
        let Some(commit_hlc) = txn.commit_hlc else {
            return false;
        };
        write_ids.into_iter().all(|id| {
            txn.installed
                .get(&id)
                .is_some_and(|installed| self.installed_revision_agrees(installed, commit_hlc))
        })
    }

    fn validate_commit_subset(txn: &Transaction, cells: &[CommitOp]) -> Result<(), DMCommitResult> {
        let prepared_cells_num = txn.certified.len();
        let arrived_cells_num = cells.len();
        if arrived_cells_num > prepared_cells_num {
            return Err(DMCommitResult::CheckFailed(
                CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
            ));
        }

        let mut committed_cell_ids = BTreeSet::new();
        for op in cells {
            let cell_id = match Self::commit_op_cell_id(op) {
                Ok(cell_id) => cell_id,
                Err(error) => return Err(DMCommitResult::CheckFailed(error)),
            };
            if !txn.certified.contains_key(&cell_id) || !committed_cell_ids.insert(cell_id) {
                return Err(DMCommitResult::CheckFailed(
                    CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
                ));
            }
        }

        if txn.certified.iter().any(|(cell_id, op)| {
            op.intent == PrepareIntent::Write && !committed_cell_ids.contains(cell_id)
        }) {
            return Err(DMCommitResult::CheckFailed(
                CheckError::CellNumberDoesNotMatch(prepared_cells_num, arrived_cells_num),
            ));
        }

        Ok(())
    }

    fn validate_commit_payload(
        txn: &Transaction,
        cells: &[CommitOp],
    ) -> Result<(), DMCommitResult> {
        for op in cells {
            let cell_id =
                Self::commit_op_cell_id(op).map_err(|error| DMCommitResult::CheckFailed(error))?;
            let Some(certified) = txn.certified_op(&cell_id) else {
                return Err(DMCommitResult::CheckFailed(CheckError::CannotEnd));
            };

            let valid = match op {
                CommitOp::Write(_) => {
                    certified.intent == PrepareIntent::Write
                        && matches!(
                            certified.expectation,
                            CellExpectation::Absent(_) | CellExpectation::UnobservedAbsent
                        )
                }
                CommitOp::Update(_) | CommitOp::Remove(_) => {
                    certified.intent == PrepareIntent::Write
                        && matches!(certified.expectation, CellExpectation::Present(_))
                }
                CommitOp::Read(_, _) | CommitOp::None => false,
            };

            if !valid {
                return Err(DMCommitResult::CheckFailed(CheckError::CannotEnd));
            }
        }

        Ok(())
    }

    fn validate_commit_storage_state(
        &self,
        txn: &Transaction,
        cells: &[CommitOp],
    ) -> Result<(), DMCommitResult> {
        for op in cells {
            let cell_id = Self::commit_op_cell_id(op).map_err(DMCommitResult::CheckFailed)?;
            if txn.installed.contains_key(&cell_id) {
                continue;
            }
            let certified = txn
                .certified_op(&cell_id)
                .ok_or(DMCommitResult::CheckFailed(CheckError::CannotEnd))?;
            if !self
                .current_expectation(&cell_id)
                .is_ok_and(|current| Self::expectation_matches(certified, &current))
            {
                return Err(DMCommitResult::CellChanged(cell_id));
            }
        }

        Ok(())
    }

    fn map_commit_write_error(txn: &Transaction, id: Id, error: WriteError) -> DMCommitResult {
        if matches!(error, WriteError::DurabilityFailure(_)) {
            return DMCommitResult::CheckFailed(CheckError::CannotEnd);
        }
        let expected_present = txn.certified_present_revision_ts(&id).is_some();
        let expected_absent_write = txn.certified_expects_absent_write(&id);
        let is_cell_changed = match &error {
            WriteError::DeletionPredictionFailed
            | WriteError::UserCanceledUpdate
            | WriteError::CellRevisionMismatch => true,
            WriteError::CellDoesNotExisted
            | WriteError::ReadError(ReadError::CellDoesNotExisted) => expected_present,
            WriteError::CellAlreadyExisted => expected_absent_write,
            _ => false,
        };

        if is_cell_changed {
            DMCommitResult::CellChanged(id)
        } else {
            DMCommitResult::WriteError(id, error)
        }
    }

    fn commit_op_cell_id(op: &CommitOp) -> Result<Id, CheckError> {
        match op {
            CommitOp::Write(cell) | CommitOp::Update(cell) => Ok(cell.id()),
            CommitOp::Remove(id) | CommitOp::Read(id, _) => Ok(*id),
            CommitOp::None => Err(CheckError::CannotEnd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::cell::OwnedCell;
    use crate::ram::cleaner::combine;
    use crate::ram::durable_fs::fail_next_directory_sync_for_test;
    use crate::ram::schema::Schema;
    use crate::ram::segs::{SegmentExclusiveRefGuard, SEGMENT_SIZE};
    use crate::ram::tests::default_fields;
    use crate::ram::types::{Id, OwnedMap, OwnedPrimArray, OwnedValue, Pos3d32};
    use crate::server::transactions::test_hlc;
    use crate::server::{NebServer, ServerOptions, Service as NebService};
    use bifrost::rpc::DEFAULT_CLIENT_POOL;
    use dovahkiin::types::Map as OwnedMapTrait;
    use futures::future::join_all;
    use lightning::map::Map as LFMapTrait;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn scoped_data_manager_service_ids_differ_between_databases() {
        let group = "group_a";
        assert_ne!(
            generate_scoped_service_id(group, "db_a"),
            generate_scoped_service_id(group, "db_b")
        );
    }

    #[test]
    fn canonical_commit_identity_covers_kind_full_id_header_and_payload() {
        let first_id = Id::new(7, 11);
        let second_id = Id::new(8, 11);
        let mut first = counter_cell(990, first_id, 1, "first");
        first.header.revision_ts = 17;
        first.header.flags = 3;
        let second = counter_cell(990, second_id, 2, "second");
        let expected = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(second.clone()),
            CommitOp::Update(first.clone()),
        ])
        .unwrap();
        let reordered = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(first.clone()),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(DataManager::commit_requests_equal(&expected, &reordered));

        let changed_kind = DataManager::canonical_commit_ops(vec![
            CommitOp::Remove(first_id),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(
            &expected,
            &changed_kind
        ));

        let changed_id = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(counter_cell(990, Id::new(9, 11), 1, "first")),
            CommitOp::Update(second.clone()),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(&expected, &changed_id));

        let mut header_variants = Vec::new();
        for mutate in [
            |cell: &mut OwnedCell| cell.header.revision_ts += 1,
            |cell: &mut OwnedCell| cell.header.flags += 1,
            |cell: &mut OwnedCell| cell.header.schema += 1,
        ] {
            let mut changed = first.clone();
            mutate(&mut changed);
            header_variants.push(changed);
        }
        for changed in header_variants {
            let changed_header = DataManager::canonical_commit_ops(vec![
                CommitOp::Update(changed),
                CommitOp::Update(second.clone()),
            ])
            .unwrap();
            assert!(!DataManager::commit_requests_equal(
                &expected,
                &changed_header
            ));
        }

        let changed_payload = DataManager::canonical_commit_ops(vec![
            CommitOp::Update(counter_cell(990, first_id, 99, "changed")),
            CommitOp::Update(second),
        ])
        .unwrap();
        assert!(!DataManager::commit_requests_equal(
            &expected,
            &changed_payload
        ));
    }

    fn commit_identity_for_values(left: OwnedValue, right: OwnedValue) -> bool {
        let id = Id::new(7, 12);
        let left = DataManager::canonical_commit_ops(vec![CommitOp::Update(
            OwnedCell::new_with_id(990, &id, left),
        )])
        .unwrap();
        let right = DataManager::canonical_commit_ops(vec![CommitOp::Update(
            OwnedCell::new_with_id(990, &id, right),
        )])
        .unwrap();
        DataManager::commit_requests_equal(&left, &right)
    }

    #[test]
    fn commit_identity_accepts_same_nan_bits_and_rejects_changed_payload_bits() {
        let nan_bits = 0x7ff8_0000_0000_0042;
        let same_left = OwnedValue::F64(f64::from_bits(nan_bits));
        let same_right = OwnedValue::F64(f64::from_bits(nan_bits));
        assert!(
            commit_identity_for_values(same_left, same_right),
            "the same NaN payload bits are the same request"
        );

        assert!(
            !commit_identity_for_values(
                OwnedValue::F64(f64::from_bits(nan_bits)),
                OwnedValue::F64(f64::from_bits(nan_bits + 1)),
            ),
            "different NaN payload bits are different requests"
        );
    }

    #[test]
    fn commit_identity_distinguishes_signed_zero() {
        assert!(!commit_identity_for_values(
            OwnedValue::F32(0.0),
            OwnedValue::F32(-0.0),
        ));
    }

    #[test]
    fn commit_identity_compares_complete_ordered_map_fields() {
        let mut left = OwnedMap::new();
        left.insert("score", OwnedValue::U64(9));
        left.insert("name", OwnedValue::String("cell".to_string()));
        let mut right = left.clone();
        right.fields = vec!["renamed".to_string()];

        assert!(!commit_identity_for_values(
            OwnedValue::Map(left.clone()),
            OwnedValue::Map(right),
        ));

        let mut reverse_inserted = OwnedMap::new();
        reverse_inserted.insert("name", OwnedValue::String("cell".to_string()));
        reverse_inserted.insert("score", OwnedValue::U64(9));
        reverse_inserted.fields = left.fields.clone();
        assert!(
            commit_identity_for_values(OwnedValue::Map(left), OwnedValue::Map(reverse_inserted)),
            "map entry iteration order is not request identity"
        );
    }

    #[test]
    fn commit_identity_applies_bit_exact_comparison_recursively() {
        let nan_bits = 0x7fc0_0042;
        let nested = OwnedValue::Array(vec![OwnedValue::Map(OwnedMap {
            map: [(
                17,
                OwnedValue::PrimArray(OwnedPrimArray::Pos3d32(vec![Pos3d32 {
                    x: f32::from_bits(nan_bits),
                    y: 0.0,
                    z: -0.0,
                }])),
            )]
            .into_iter()
            .collect(),
            fields: vec!["position".to_string()],
        })]);
        assert!(
            commit_identity_for_values(nested.clone(), nested.clone()),
            "bit-identical nested NaNs must compare equal"
        );

        let changed = OwnedValue::Array(vec![OwnedValue::Map(OwnedMap {
            map: [(
                17,
                OwnedValue::PrimArray(OwnedPrimArray::Pos3d32(vec![Pos3d32 {
                    x: f32::from_bits(nan_bits),
                    y: -0.0,
                    z: -0.0,
                }])),
            )]
            .into_iter()
            .collect(),
            fields: vec!["position".to_string()],
        })]);
        assert!(
            !commit_identity_for_values(nested, changed),
            "nested primitive-array element order and float bits are request identity"
        );
    }

    #[cfg(feature = "occ_phase_profile")]
    #[test]
    fn participant_profile_covers_every_existing_protocol_boundary() {
        let source = include_str!("data_site.rs");
        let (before_tests, after_test_marker) = source
            .split_once("\n#[cfg(test)]\nmod tests {")
            .expect("data_site.rs should contain the private test module");
        let (_, after_tests) = after_test_marker
            .split_once("\n}\n\nimpl Service for DataManager {")
            .expect("data_site.rs should resume production code after the private test module");
        let production_source =
            format!("{before_tests}\n\nimpl Service for DataManager {{{after_tests}");
        let end_method_tail = production_source
            .split_once("\n    fn end(")
            .map(|(_, end_method_tail)| end_method_tail)
            .expect("expected data_site.rs to define Service::end");
        let async_move_offset = end_method_tail
            .find("async move {")
            .expect("expected Service::end to return an async cleanup future");
        let before_async_move = &end_method_tail[..async_move_offset];
        let async_move_and_after = &end_method_tail[async_move_offset..];

        for phase in [
            "Phase::ParticipantPrepare",
            "Phase::ParticipantCommit",
            "Phase::ParticipantAbort",
            "Phase::ParticipantEnd",
        ] {
            assert!(
                production_source.contains(phase),
                "missing participant guard for {phase}"
            );
        }

        assert!(
            before_async_move.contains("let phase_guard =")
                && before_async_move.contains("Phase::ParticipantEnd"),
            "expected Service::end to bind a named participant end guard before its async cleanup future",
        );
        assert!(
            async_move_and_after.contains(
                "async move {\n            #[cfg(feature = \"occ_phase_profile\")]\n            let _phase_guard = phase_guard;"
            ),
            "expected Service::end async cleanup future to retain the participant end guard",
        );
    }

    #[test]
    fn dropped_prepare_delay_handle_removes_its_registration() {
        let id = Id::new(0, 99012);
        let tid = test_hlc(12, 31);
        let key = (tid.clone(), id);
        {
            let _handle = install_prepare_delay_for_cell(tid, id);
            assert!(prepare_delay_hooks().lock().contains_key(&key));
        }

        assert!(
            !prepare_delay_hooks().lock().contains_key(&key),
            "dropping the handle should remove its global registration"
        );
    }

    async fn start_transaction_test_server(
        address: &str,
        group: &str,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell, NebService::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &address.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap()
    }

    async fn start_durable_transaction_test_server(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
    ) -> Arc<crate::server::NebServer> {
        start_durable_transaction_test_server_with_chunk_size(
            address,
            group,
            temp_dir,
            SEGMENT_SIZE,
        )
        .await
    }

    async fn start_durable_transaction_test_server_with_chunk_size(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
        chunk_size: usize,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size,
                db_size: chunk_size,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: None,
                wal_storage: Some(temp_dir.path().join("wal").to_string_lossy().into_owned()),
                undo_log_storage: Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell, NebService::Transaction],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &address.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap()
    }

    async fn start_backup_without_wal_test_server(
        address: &str,
        group: &str,
        temp_dir: &TempDir,
    ) -> Arc<crate::server::NebServer> {
        NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: SEGMENT_SIZE,
                db_size: SEGMENT_SIZE,
                history_retention_ms: 300_000,
                tiered_config: None,
                backup_storage: Some(
                    temp_dir
                        .path()
                        .join("backup")
                        .to_string_lossy()
                        .into_owned(),
                ),
                wal_storage: None,
                undo_log_storage: Some(temp_dir.path().join("undo").to_string_lossy().into_owned()),
                raft_storage: None,
                index_enabled: false,
                services: vec![NebService::Cell],
                enable_recovery: false,
                disable_storage_locks: true,
            },
            &address.to_string(),
            &group.to_string(),
            async |_| {},
        )
        .await
        .unwrap()
    }

    async fn data_manager_for_database(
        server: &Arc<NebServer>,
        address: &str,
        database_name: &str,
    ) -> Arc<DataManager> {
        let runtime = server
            .ensure_database_runtime(database_name)
            .await
            .expect("database runtime");
        DataManager::new(runtime, server.hlc.clone())
    }

    async fn data_site_client_for_database(
        address: &str,
        group: &str,
        database_name: &str,
    ) -> Arc<AsyncServiceClient> {
        let client = DEFAULT_CLIENT_POOL.get(&address.to_string()).await.unwrap();
        AsyncServiceClient::new_with_service_id(
            generate_scoped_service_id(group, database_name),
            &client,
        )
    }

    fn install_prepare_test_schema(runtime: &Arc<crate::server::DatabaseRuntime>) -> Schema {
        let schema = Schema::new_with_id(
            990,
            &String::from("txn_prepare_cert"),
            None,
            default_fields(),
            false,
            false,
        );
        runtime.meta().schemas.debug_only_new_schema(schema.clone());
        schema
    }

    fn counter_cell(schema_id: u32, id: Id, score: u64, name: &str) -> OwnedCell {
        let mut data = OwnedMap::new();
        data.insert(&String::from("id"), OwnedValue::I64(id.lower as i64));
        data.insert(&String::from("score"), OwnedValue::U64(score));
        data.insert(&String::from("name"), OwnedValue::String(name.to_string()));
        OwnedCell::new_with_id(schema_id, &id, OwnedValue::Map(data))
    }

    fn seed_cell_revision(
        runtime: &Arc<crate::server::DatabaseRuntime>,
        schema_id: u32,
        id: Id,
        score: u64,
        revision_steps: usize,
    ) -> u64 {
        let mut cell = counter_cell(schema_id, id, score, "prepare-seed-0");
        let mut header = runtime.chunks().write_cell(&mut cell).unwrap();
        for step in 0..revision_steps {
            let next_score = score + step as u64 + 1;
            let mut updated = counter_cell(
                schema_id,
                id,
                next_score,
                &format!("prepare-seed-{}", step + 1),
            );
            header = runtime.chunks().update_cell(&mut updated).unwrap();
        }
        header.revision_ts
    }

    fn prepare_local_txn(manager: &Arc<DataManager>, tid: &TxnId, affected_cells: Vec<Id>) {
        let owner = TxnPriority::new(tid.clone(), 0);
        let txn_lock = manager.get_or_create_transaction(tid);
        let mut txn = txn_lock.lock();
        txn.state = TxnState::Prepared;
        txn.certified = affected_cells
            .iter()
            .copied()
            .map(|id| {
                (
                    id,
                    PrepareOp {
                        id,
                        expectation: CellExpectation::Absent(None),
                        intent: PrepareIntent::Write,
                    },
                )
            })
            .collect();
        txn.affected_cells = txn.certified.keys().copied().collect();
        txn.coordinator_id = Some(0);
        txn.history.clear();
        txn.last_activity = get_time();
        drop(txn);

        let lock_time = get_time();
        let owner_count = affected_cells.len();
        for id in affected_cells {
            let meta = manager.cell_meta_mutex(&id);
            let mut meta = meta.lock();
            meta.owner = Some(owner.clone());
            meta.lock_acquired_at = Some(lock_time);
        }
        manager.owner_index.lock().add(&owner, owner_count);
    }

    async fn prepare_ops_local(
        manager: &Arc<DataManager>,
        coordinator_id: u64,
        tid: &TxnId,
        ops: Vec<PrepareOp>,
    ) -> DMPrepareResult {
        <DataManager as Service>::prepare(manager, coordinator_id, tid.clone(), tid.clone(), ops)
            .await
            .payload
    }

    async fn commit_ops_local(
        manager: &Arc<DataManager>,
        tid: &TxnId,
        ops: Vec<CommitOp>,
    ) -> DMCommitResult {
        <DataManager as Service>::commit(manager, manager.hlc.now(), tid.clone(), ops)
            .await
            .payload
    }

    async fn abort_and_end_local(manager: &Arc<DataManager>, tid: &TxnId) {
        let abort = <DataManager as Service>::abort(manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort, AbortResult::Success(None));
        let end = <DataManager as Service>::end(manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
    }

    async fn assert_delayed_duplicate_prepare_cannot_resurrect_completion(
        manager: Arc<DataManager>,
        runtime: Arc<DatabaseRuntime>,
        cell_id: Id,
    ) {
        let schema = install_prepare_test_schema(&runtime);
        let revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision),
            intent: PrepareIntent::Read,
        };
        assert_eq!(
            prepare_ops_local(&manager, 77, &tid, vec![op.clone()]).await,
            DMPrepareResult::Success
        );

        let delay = install_prepare_delay_for_cell(tid, cell_id);
        let duplicate_manager = manager.clone();
        let duplicate =
            tokio::spawn(
                async move { prepare_ops_local(&duplicate_manager, 77, &tid, vec![op]).await },
            );
        delay.wait_until_entered().await;

        assert_eq!(
            <DataManager as Service>::commit(&manager, manager.hlc.now(), tid, vec![])
                .await
                .payload,
            DMCommitResult::Success
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        delay.release();

        assert_eq!(
            duplicate.await.unwrap(),
            DMPrepareResult::StateError(TxnState::Committed)
        );
        assert!(manager.find_transaction(&tid).is_none());
        assert!(
            manager
                .cells
                .get(&cell_id)
                .is_some_and(|meta| meta.lock().owner.is_none()),
            "the delayed duplicate must not republish a read-only owner"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_stale_present_version() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5323";
        let group = "txn_data_site_prepare_stale_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99001);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 3, 0);
        let tid = test_hlc(1, 11);

        let result = client
            .prepare(
                11,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(revision_ts + 1),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_snapshot_read_waits_for_owner_before_timestamp_rejection() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5390";
        let group = "txn_data_site_snapshot_owner_wait";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99024);

        let mut cell = counter_cell(schema.id, cell_id, 1, "owner-wait-seed");
        runtime.chunks().write_cell(&mut cell).unwrap();

        let reader_tid = test_hlc(cell.header.revision_ts + 1, 51);
        let writer_tid = test_hlc(reader_tid.ts + 1, 52);
        let meta = manager.cell_meta_mutex(&cell_id);
        {
            let mut meta = meta.lock();
            meta.write = writer_tid;
            meta.owner = Some(TxnPriority::new(writer_tid, 52));
            meta.lock_acquired_at = Some(get_time());
        }

        let response =
            <DataManager as Service>::read(&manager, 51, reader_tid, reader_tid, cell_id)
                .await
                .payload;
        assert!(matches!(response, TxnExecResult::Wait));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn point_read_shapes_share_snapshot_without_participant_state() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5362";
        let group = "txn_data_site_snapshot_shapes";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99022);

        let mut first = counter_cell(schema.id, cell_id, 300, "A");
        let first_revision_ts = runtime.chunks().write_cell(&mut first).unwrap().revision_ts;
        let tid = manager.hlc.now();
        let mut second = counter_cell(schema.id, cell_id, 400, "B");
        runtime.chunks().update_cell(&mut second).unwrap();

        let head = <DataManager as Service>::head(&manager, 43, tid, tid, cell_id)
            .await
            .payload
            .unwrap();
        let selected = <DataManager as Service>::read_selected(
            &manager,
            43,
            tid,
            tid,
            cell_id,
            vec![hash_str("score")],
        )
        .await
        .payload
        .unwrap();
        let full = <DataManager as Service>::read(&manager, 43, tid, tid, cell_id)
            .await
            .payload
            .unwrap();
        let partial =
            <DataManager as Service>::read_partial_raw(&manager, 43, tid, tid, cell_id, 0, 8)
                .await
                .payload
                .unwrap();

        let expectation = CellExpectation::Present(first_revision_ts);
        assert_eq!(head.expectation, expectation);
        assert_eq!(selected.expectation, expectation);
        assert_eq!(full.expectation, expectation);
        assert_eq!(partial.expectation, expectation);
        assert_eq!(head.value.unwrap().revision_ts, first_revision_ts);
        assert_eq!(full.value.unwrap().header.revision_ts, first_revision_ts);
        assert!(partial.value.is_some());
        assert!(manager.find_transaction(&tid).is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_a_present_cell_when_absence_was_observed() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5324";
        let group = "txn_data_site_prepare_present_vs_absent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99002);
        seed_cell_revision(&runtime, schema.id, cell_id, 4, 0);
        let tid = test_hlc(1, 12);

        let result = client
            .prepare(
                12,
                tid.clone(),
                tid.clone(),
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
            .unwrap()
            .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_present_expectation_when_cell_is_missing_without_publishing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5326";
        let group = "txn_data_site_prepare_missing_present";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99003);
        let tid = test_hlc(1, 13);

        let result = <DataManager as Service>::prepare(
            &manager,
            13,
            tid.clone(),
            tid.clone(),
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(1),
                intent: PrepareIntent::Read,
            }],
        )
        .await
        .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_ne!(txn.state, TxnState::Prepared);
        assert!(txn.affected_cells.is_empty());
        drop(txn);

        let meta = manager.cell_meta_mutex(&cell_id);
        let meta = meta.lock();
        assert!(meta.owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_duplicate_prepare_ops_without_publishing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5327";
        let group = "txn_data_site_prepare_duplicate_ops";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99004);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 5, 0);
        let tid = test_hlc(1, 14);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let result = <DataManager as Service>::prepare(
            &manager,
            14,
            tid.clone(),
            tid.clone(),
            vec![op.clone(), op],
        )
        .await
        .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        let meta = manager.cell_meta_mutex(&cell_id);
        assert!(meta.lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_rejects_empty_prepare_ops_without_creating_transaction_state() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5345";
        let group = "txn_data_site_prepare_empty_ops";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = test_hlc(1, 19);

        assert!(manager.find_transaction(&tid).is_none());

        let result =
            <DataManager as Service>::prepare(&manager, 19, tid.clone(), tid.clone(), vec![])
                .await
                .payload;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.cells.len(), 0);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_rejects_different_ops_without_overwriting_original_prepare() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5328";
        let group = "txn_data_site_prepare_retry_ops_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 99006);
        let cell_b = Id::new(0, 99007);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 7, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 8, 0);
        let tid = test_hlc(1, 15);
        let coordinator_id = 15;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let op_a = PrepareOp {
            id: cell_a,
            expectation: CellExpectation::Present(version_a),
            intent: PrepareIntent::Write,
        };
        let op_b = PrepareOp {
            id: cell_b,
            expectation: CellExpectation::Present(version_b),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_a.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_b.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::StateError(TxnState::Prepared));

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_a]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_a), Some(&op_a));
        assert_eq!(txn.certified.get(&cell_b), None);
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_a).lock().owner,
            Some(requester)
        );
        assert_eq!(
            manager
                .cells
                .get(&cell_b)
                .map(|meta| meta.lock().owner.clone()),
            None
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_a).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_rejects_different_coordinator_without_state_or_owner_change() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5329";
        let group = "txn_data_site_prepare_retry_coordinator_mismatch";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99008);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 9, 0);
        let tid = test_hlc(1, 16);
        let original_coordinator = 16;
        let requester = TxnPriority::new(tid.clone(), original_coordinator);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            original_coordinator,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        let second = <DataManager as Service>::prepare(
            &manager,
            99,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::StateError(TxnState::Prepared));

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(original_coordinator));
        assert_eq!(txn.affected_cells, vec![cell_id]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_id), Some(&op));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(requester)
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_accepts_exact_same_payload_and_reacquires_missing_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5343";
        let group = "txn_data_site_prepare_retry_idempotent";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 99009);
        let cell_b = Id::new(0, 99010);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 10, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 11, 0);
        let tid = test_hlc(1, 17);
        let coordinator_id = 17;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let op_a = PrepareOp {
            id: cell_a,
            expectation: CellExpectation::Present(version_a),
            intent: PrepareIntent::Write,
        };
        let op_b = PrepareOp {
            id: cell_b,
            expectation: CellExpectation::Present(version_b),
            intent: PrepareIntent::Read,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_b.clone(), op_a.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        {
            let meta = manager.cell_meta_mutex(&cell_a);
            let mut meta = meta.lock();
            meta.owner = None;
            meta.lock_acquired_at = None;
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op_a.clone(), op_b.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::Success);

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_a, cell_b]);
        assert_eq!(txn.certified.len(), 2);
        assert_eq!(txn.certified.get(&cell_a), Some(&op_a));
        assert_eq!(txn.certified.get(&cell_b), Some(&op_b));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_a).lock().owner,
            Some(requester.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_b).lock().owner,
            Some(requester)
        );

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_a).lock().owner.is_none());
        assert!(manager.cell_meta_mutex(&cell_b).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_retry_exact_payload_does_not_blindly_succeed_with_foreign_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5344";
        let group = "txn_data_site_prepare_retry_foreign_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99011);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 12, 0);
        let tid = test_hlc(1, 18);
        let coordinator_id = 18;
        let requester = TxnPriority::new(tid.clone(), coordinator_id);
        let foreign_owner = TxnPriority::new(test_hlc(1, 1), 1);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(foreign_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            coordinator_id,
            tid.clone(),
            tid.clone(),
            vec![op.clone()],
        )
        .await
        .payload;

        assert_eq!(second, DMPrepareResult::NotRealizable);

        let txn = manager.get_or_create_transaction(&tid);
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert_eq!(txn.coordinator_id, Some(coordinator_id));
        assert_eq!(txn.affected_cells, vec![cell_id]);
        assert_eq!(txn.certified.len(), 1);
        assert_eq!(txn.certified.get(&cell_id), Some(&op));
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(foreign_owner)
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(requester);
            meta.lock_acquired_at = Some(get_time());
        }

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_clock_wait_die_has_one_younger_requester() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5325";
        let group = "txn_data_site_prepare_wait_die_clock";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let client = data_site_client_for_database(address, group, group).await;
        let cell_id = Id::new(0, 99005);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 6, 0);
        let older_tid = test_hlc(1, 11);
        let younger_tid = test_hlc(1, 22);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = client
            .prepare(11, older_tid.clone(), older_tid.clone(), vec![op.clone()])
            .await
            .unwrap()
            .payload;
        let second = client
            .prepare(22, younger_tid.clone(), younger_tid.clone(), vec![op])
            .await
            .unwrap()
            .payload;

        assert_eq!(first, DMPrepareResult::Success);
        assert_eq!(second, DMPrepareResult::NotRealizable);
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_get_transaction_returns_single_shared_entry_per_tid() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5290";
        let group = "txn_data_site_same_tid";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();

        let results = join_all((0..32).map(|_| {
            let manager = manager.clone();
            let tid = tid.clone();
            async move {
                let txn = manager.get_or_create_transaction(&tid);
                Arc::as_ptr(&txn) as usize
            }
        }))
        .await;

        let first = results[0];
        assert!(
            results.iter().all(|ptr| *ptr == first),
            "all concurrent lookups of the same txn id should return the same transaction entry"
        );
        assert_eq!(manager.txns.len(), 1);
        assert_eq!(manager.txns_sorted.lock().len(), 1);
        assert!(manager.txns.get(&tid).is_some());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transaction_registry_publish_is_atomic_with_end_cleanup() {
        let address = "127.0.0.1:5398";
        let group = "txn_data_site_registry_publish_cleanup";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();
        let publish_delay = install_txn_registry_publish_delay(tid);

        let creator_manager = manager.clone();
        let creator = std::thread::spawn(move || {
            creator_manager
                .get_or_create_transaction_with_status(&tid)
                .1
        });
        publish_delay.wait_until_entered().await;

        let published = manager
            .find_transaction(&tid)
            .expect("the transaction map entry should be published");
        published.lock().state = TxnState::Aborted;

        let end_manager = manager.clone();
        let mut end_task = tokio::spawn(async move {
            <DataManager as Service>::end(&end_manager, tid, tid)
                .await
                .payload
        });
        let early_end = tokio::time::timeout(Duration::from_millis(100), &mut end_task).await;
        let end_was_blocked = early_end.is_err();

        publish_delay.release();
        let was_created = creator.join().expect("creator should not panic");
        assert!(was_created);
        let end_result = match early_end {
            Ok(result) => result.expect("end task should not panic"),
            Err(_) => end_task.await.expect("end task should not panic"),
        };

        assert!(
            end_was_blocked,
            "end must not remove a transaction while its map and sorted-index publication is partial"
        );
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.find_transaction(&tid).is_none());
        assert!(
            !manager.txns_sorted.lock().contains(&tid),
            "end must not leave a ghost TID pinning the CellMeta cleanup watermark"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn read_only_transaction_creates_no_data_site_transaction() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5358";
        let group = "txn_data_site_read_only_stateless";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99011);
        seed_cell_revision(&runtime, schema.id, cell_id, 7, 0);
        let tid = test_hlc(1, 31);

        let response =
            <DataManager as Service>::read(&manager, 31, tid.clone(), tid.clone(), cell_id)
                .await
                .payload;

        assert!(matches!(response, TxnExecResult::Accepted(_)));
        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_read_only_point_shape_is_participant_stateless() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5391";
        let group = "txn_data_site_all_point_shapes_stateless";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99025);
        seed_cell_revision(&runtime, schema.id, cell_id, 7, 0);
        let tid = manager.hlc.now();

        let head = <DataManager as Service>::head(&manager, 31, tid, tid, cell_id)
            .await
            .payload;
        assert!(matches!(head, TxnExecResult::Accepted(_)));

        let selected = <DataManager as Service>::read_selected(
            &manager,
            31,
            tid,
            tid,
            cell_id,
            vec![hash_str("score")],
        )
        .await
        .payload;
        assert!(matches!(selected, TxnExecResult::Accepted(_)));

        let partial =
            <DataManager as Service>::read_partial_raw(&manager, 31, tid, tid, cell_id, 0, 8)
                .await
                .payload;
        assert!(matches!(partial, TxnExecResult::Accepted(_)));

        assert!(manager.find_transaction(&tid).is_none());
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_cleans_up_many_active_transactions_without_leaking_bookkeeping() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5291";
        let group = "txn_data_site_cleanup";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tids = (0..64).map(|_| manager.hlc.now()).collect::<Vec<_>>();

        for tid in &tids {
            let txn = manager.get_or_create_transaction(tid);
            txn.lock().state = TxnState::Committed;
        }
        assert_eq!(manager.txns.len(), tids.len());
        assert_eq!(manager.txns_sorted.lock().len(), tids.len());

        let results = join_all(tids.iter().cloned().map(|tid| {
            let manager = manager.clone();
            async move {
                <DataManager as Service>::end(&manager, tid.clone(), tid)
                    .await
                    .payload
            }
        }))
        .await;

        for result in results {
            assert_eq!(result, EndResult::Success);
        }
        assert_eq!(manager.txns.len(), 0);
        assert!(manager.txns_sorted.lock().is_empty());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ending_one_database_transaction_does_not_wipe_same_tid_in_another_database() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5292";
        let group = "txn_data_site_multidb";
        let server = start_transaction_test_server(address, group).await;
        let default_manager = data_manager_for_database(&server, address, group).await;
        let analytics_manager = data_manager_for_database(&server, address, "analytics").await;
        let shared_tid = default_manager.hlc.now();

        default_manager
            .get_or_create_transaction(&shared_tid)
            .lock()
            .state = TxnState::Committed;
        analytics_manager
            .get_or_create_transaction(&shared_tid)
            .lock()
            .state = TxnState::Committed;

        let end_result =
            <DataManager as Service>::end(&default_manager, shared_tid.clone(), shared_tid.clone())
                .await
                .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(default_manager.txns.get(&shared_tid).is_none());
        assert!(!default_manager.txns_sorted.lock().contains(&shared_tid));

        assert!(
            analytics_manager.txns.get(&shared_tid).is_some(),
            "ending one database transaction must not remove another database's transaction entry"
        );
        assert!(
            analytics_manager.txns_sorted.lock().contains(&shared_tid),
            "ending one database transaction must not remove another database's sorted bookkeeping"
        );

        let analytics_end = <DataManager as Service>::end(
            &analytics_manager,
            shared_tid.clone(),
            shared_tid.clone(),
        )
        .await
        .payload;
        assert_eq!(analytics_end, EndResult::Success);
        assert!(analytics_manager.txns.get(&shared_tid).is_none());
        assert!(!analytics_manager.txns_sorted.lock().contains(&shared_tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_end_calls_on_same_transaction_cleanup_idempotently() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5297";
        let group = "txn_data_site_end_race";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();
        manager.get_or_create_transaction(&tid).lock().state = TxnState::Aborted;

        let end_clock = manager.hlc.now();
        let left_manager = manager.clone();
        let right_manager = manager.clone();
        let left_tid = tid.clone();
        let right_tid = tid.clone();
        let left_clock = end_clock.clone();
        let right_clock = end_clock.clone();

        let (left, right) = tokio::join!(
            async move {
                <DataManager as Service>::end(&left_manager, left_clock, left_tid)
                    .await
                    .payload
            },
            async move {
                <DataManager as Service>::end(&right_manager, right_clock, right_tid)
                    .await
                    .payload
            }
        );

        for result in [left, right] {
            assert_eq!(result, EndResult::Success);
        }

        assert!(manager.txns.get(&tid).is_none());
        assert!(!manager.txns_sorted.lock().contains(&tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_rejects_non_mutation_ops_without_panicking() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5299";
        let group = "txn_data_site_commit_variant_validation";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let affected_id = Id::new(0, 8101);

        for ops in [vec![CommitOp::Read(affected_id, 1)], vec![CommitOp::None]] {
            let tid = manager.hlc.now();
            prepare_local_txn(&manager, &tid, vec![affected_id]);

            let result =
                <DataManager as Service>::commit(&manager, manager.hlc.now(), tid.clone(), ops)
                    .await
                    .payload;
            assert_eq!(result, DMCommitResult::CheckFailed(CheckError::CannotEnd));

            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Prepared);
            assert_eq!(txn.affected_cells, vec![affected_id]);
            assert!(txn.history.is_empty());
        }

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_requires_certified_writes_and_allows_read_only_empty_commits() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5287";
        let group = "txn_data_site_commit_subset_validation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let write_id = Id::new(0, 8201);
        let read_id = Id::new(0, 8202);
        let read_only_id = Id::new(0, 8203);
        let unprepared = Id::new(0, 8204);
        let write_version = seed_cell_revision(&runtime, schema.id, write_id, 7, 0);
        let read_version = seed_cell_revision(&runtime, schema.id, read_id, 9, 0);
        let read_only_version = seed_cell_revision(&runtime, schema.id, read_only_id, 11, 0);

        let read_only_tid = manager.hlc.now();
        let read_only_prepare = prepare_ops_local(
            &manager,
            0,
            &read_only_tid,
            vec![PrepareOp {
                id: read_only_id,
                expectation: CellExpectation::Present(read_only_version),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(read_only_prepare, DMPrepareResult::Success);

        let read_only_before = runtime
            .chunks()
            .read_cell(&read_only_id)
            .unwrap()
            .to_owned();
        let read_only_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            read_only_tid.clone(),
            vec![],
        )
        .await
        .payload;
        assert_eq!(read_only_result, DMCommitResult::Success);
        assert_eq!(
            manager.txns.get(&read_only_tid).unwrap().lock().state,
            TxnState::Committed
        );
        let read_only_after = runtime
            .chunks()
            .read_cell(&read_only_id)
            .unwrap()
            .to_owned();
        assert_eq!(read_only_after.data, read_only_before.data);
        assert_eq!(
            read_only_after.header.revision_ts,
            read_only_before.header.revision_ts
        );

        let empty_tid = manager.hlc.now();
        let empty_prepare = prepare_ops_local(
            &manager,
            0,
            &empty_tid,
            vec![
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: read_id,
                    expectation: CellExpectation::Present(read_version),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(empty_prepare, DMPrepareResult::Success);

        let empty_before_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let empty_before_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        let empty_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            empty_tid.clone(),
            vec![],
        )
        .await
        .payload;
        assert_eq!(
            empty_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 0))
        );
        assert_eq!(
            manager.txns.get(&empty_tid).unwrap().lock().state,
            TxnState::Prepared
        );
        let empty_after_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let empty_after_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        assert_eq!(empty_after_write.data, empty_before_write.data);
        assert_eq!(
            empty_after_write.header.revision_ts,
            empty_before_write.header.revision_ts
        );
        assert_eq!(empty_after_read.data, empty_before_read.data);
        assert_eq!(
            empty_after_read.header.revision_ts,
            empty_before_read.header.revision_ts
        );
        assert!(manager
            .txns
            .get(&empty_tid)
            .unwrap()
            .lock()
            .history
            .is_empty());
        abort_and_end_local(&manager, &empty_tid).await;

        let partial_tid = manager.hlc.now();
        let partial_prepare = prepare_ops_local(
            &manager,
            0,
            &partial_tid,
            vec![
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_version),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: read_id,
                    expectation: CellExpectation::Present(read_version),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(partial_prepare, DMPrepareResult::Success);

        let partial_before_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let partial_before_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        let partial_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            partial_tid.clone(),
            vec![CommitOp::Read(read_id, read_version)],
        )
        .await
        .payload;
        assert_eq!(
            partial_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 1))
        );
        assert_eq!(
            manager.txns.get(&partial_tid).unwrap().lock().state,
            TxnState::Prepared
        );
        let partial_after_write = runtime.chunks().read_cell(&write_id).unwrap().to_owned();
        let partial_after_read = runtime.chunks().read_cell(&read_id).unwrap().to_owned();
        assert_eq!(partial_after_write.data, partial_before_write.data);
        assert_eq!(
            partial_after_write.header.revision_ts,
            partial_before_write.header.revision_ts
        );
        assert_eq!(partial_after_read.data, partial_before_read.data);
        assert_eq!(
            partial_after_read.header.revision_ts,
            partial_before_read.header.revision_ts
        );
        assert!(manager
            .txns
            .get(&partial_tid)
            .unwrap()
            .lock()
            .history
            .is_empty());
        abort_and_end_local(&manager, &partial_tid).await;

        let extra_tid = manager.hlc.now();
        prepare_local_txn(&manager, &extra_tid, vec![write_id, read_id]);
        let extra_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            extra_tid.clone(),
            vec![CommitOp::Remove(unprepared)],
        )
        .await
        .payload;
        assert_eq!(
            extra_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 1))
        );
        assert_eq!(
            manager.txns.get(&extra_tid).unwrap().lock().state,
            TxnState::Prepared
        );

        let duplicate_tid = manager.hlc.now();
        prepare_local_txn(&manager, &duplicate_tid, vec![write_id, read_id]);
        let duplicate_result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            duplicate_tid.clone(),
            vec![CommitOp::Remove(write_id), CommitOp::Remove(write_id)],
        )
        .await
        .payload;
        assert_eq!(
            duplicate_result,
            DMCommitResult::CheckFailed(CheckError::CellNumberDoesNotMatch(2, 2))
        );
        assert_eq!(
            manager.txns.get(&duplicate_tid).unwrap().lock().state,
            TxnState::Prepared
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_validation_rejects_missing_coordinator_without_mutating_storage() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5347";
        let group = "txn_data_site_commit_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8204);
        let initial_score = 13;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let tid = manager.hlc.now();
        let expected_owner = TxnPriority::new(tid.clone(), 0);

        prepare_local_txn(&manager, &tid, vec![cell_id]);
        {
            let txn_lock = manager.txns.get(&tid).expect("txn should exist");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = None;
            txn.certified.insert(
                cell_id,
                PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision_ts),
                    intent: PrepareIntent::Write,
                },
            );
        }

        let result = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                99,
                "counter_missing_coordinator_updated",
            ))],
        )
        .await
        .payload;

        assert_eq!(result, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), initial_score);
        assert_eq!(persisted.header.revision_ts, initial_revision_ts);

        let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn_lock.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(expected_owner)
        );

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = Some(0);
        }

        let abort_result = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(abort_result, AbortResult::Success(None));
        let end_result = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end_result, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_only_lock_timeout_constructor_uses_a_short_positive_timeout() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5345";
        let group = "txn_data_site_short_lock_timeout";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();

        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);

        assert_eq!(manager.lock_timeout_ms, 5);
        assert_eq!(
            DataManager::new(runtime.clone(), server.hlc.clone()).lock_timeout_ms,
            30_000,
            "production stale-owner probing must keep the 30-second timeout"
        );
        for invalid_timeout in [0, -1] {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe({
                    let runtime = runtime.clone();
                    let hlc = server.hlc.clone();
                    move || {
                        DataManager::new_with_lock_timeout(runtime, hlc, invalid_timeout);
                    }
                }))
                .is_err(),
                "{invalid_timeout}ms must be rejected instead of causing immediate stale probing"
            );
        }
        server.shutdown().await;
    }

    #[test]
    fn participant_completion_retention_uses_the_exact_logical_deadline() {
        assert_eq!(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS, 300_000);
        let accepted_at_ms: i64 = 10_000;
        let pending = ParticipantCompletionEvidence::pending_retirement(TxnState::Committed);
        assert_eq!(pending.outcome_at(i64::MAX), Some(TxnState::Committed));
        let finalized = ParticipantCompletionEvidence::durable(
            TxnState::Committed,
            Some(accepted_at_ms.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS)),
        );
        assert_eq!(
            finalized.outcome_at(accepted_at_ms + 299_999),
            Some(TxnState::Committed)
        );
        assert_eq!(finalized.outcome_at(accepted_at_ms + 300_000), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identical_prepare_retry_preserves_original_owner_age() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5378";
        let group = "txn_data_site_prepare_retry_owner_age";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 8);
        let cell_id = Id::new(0, 90506);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 17, 0);
        let tid = manager.hlc.now();
        let owner = TxnPriority::new(tid, server.server_id);
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &tid, vec![op.clone()]).await,
            DMPrepareResult::Success
        );
        let first_acquired_at = manager
            .cell_meta_mutex(&cell_id)
            .lock()
            .lock_acquired_at
            .expect("successful prepare must timestamp the new owner");
        runtime
            .undo_log()
            .unwrap()
            .write_coordinator_abort_decision(&tid, &[server.server_id])
            .unwrap();

        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(3)).await;
            assert_eq!(
                prepare_ops_local(&manager, server.server_id, &tid, vec![op.clone()]).await,
                DMPrepareResult::Success
            );
            assert_eq!(
                manager.cell_meta_mutex(&cell_id).lock().lock_acquired_at,
                Some(first_acquired_at),
                "sub-timeout identical retries must not renew the stale-owner clock"
            );
        }

        let contender = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![op.clone()]).await,
            DMPrepareResult::NotRealizable,
            "the younger contender still queues explicit resolution before wait-die rejection"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !manager.owner_is_present(&owner)
                    && manager.resolution_attempt_count_for_test(&owner) > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("original-age timeout must queue and converge the durable Abort decision");
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![op]).await,
            DMPrepareResult::Success,
            "the contender must acquire after explicit resolution releases the stale owner"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn volatile_end_evidence_waits_for_local_retirement_finalize() {
        let address = "127.0.0.1:5379";
        let group = "txn_data_site_volatile_retirement_handshake";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();

        manager.get_or_create_transaction(&tid).lock().state = TxnState::Aborted;
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        let pending = manager
            .participant_completions
            .lock()
            .get(&tid)
            .copied()
            .expect("end must publish participant completion evidence");
        assert_eq!(
            pending.expires_at_ms, None,
            "owner clear must not start the volatile completion TTL"
        );
        assert_eq!(
            pending.outcome_at(get_time().saturating_add(300_001)),
            Some(TxnState::Aborted),
            "lost end responses remain retryable before retirement finalize"
        );

        assert_eq!(
            <DataManager as Service>::retire(
                &manager,
                manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload,
            EndResult::Success
        );
        let accepted_before = get_time();
        assert_eq!(
            <DataManager as Service>::finalize_retirement(
                &manager,
                manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload,
            EndResult::Success
        );
        let accepted_after = get_time();
        let finalized = manager
            .participant_completions
            .lock()
            .get(&tid)
            .copied()
            .expect("finalize must retain volatile participant evidence");
        let expires_at_ms = finalized
            .expires_at_ms
            .expect("finalize must start the participant-local TTL");
        assert!(
            expires_at_ms
                >= accepted_before.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS)
                && expires_at_ms
                    <= accepted_after.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS),
            "participant finalize must derive expiry from local acceptance"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_volatile_tid_is_not_evidence_and_completed_abort_retries_exactly() {
        let address = "127.0.0.1:5373";
        let group = "txn_data_site_volatile_completion_evidence";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let unknown_tid = manager.hlc.now();

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), unknown_tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::NotExisted)
        );
        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), unknown_tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::NotExisted)
        );
        let targeted_expiry = get_time();
        manager.participant_completions.lock().insert(
            unknown_tid,
            ParticipantCompletionEvidence::durable(TxnState::Aborted, Some(targeted_expiry)),
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), unknown_tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::NotExisted),
            "evidence expires at the strict logical deadline"
        );

        let targeted_tid = manager.hlc.now();
        let unrelated_expired_tid = manager.hlc.now();
        let mut targeted_cache = HashMap::from([
            (
                targeted_tid,
                ParticipantCompletionEvidence::durable(TxnState::Aborted, Some(targeted_expiry)),
            ),
            (
                unrelated_expired_tid,
                ParticipantCompletionEvidence::durable(TxnState::Committed, Some(targeted_expiry)),
            ),
        ]);
        assert_eq!(
            DataManager::cached_participant_completion_from(
                &mut targeted_cache,
                &targeted_tid,
                targeted_expiry,
            ),
            None
        );
        assert!(
            targeted_cache.contains_key(&unrelated_expired_tid),
            "ordinary evidence lookup must expire only its requested TID, not scan the cache"
        );

        manager.participant_completions.lock().insert(
            unrelated_expired_tid,
            ParticipantCompletionEvidence::durable(TxnState::Committed, Some(targeted_expiry)),
        );
        manager.prune_participant_completions_at(targeted_expiry);
        assert!(
            !manager
                .participant_completions
                .lock()
                .contains_key(&unrelated_expired_tid),
            "the bounded background maintenance path removes unrelated expired evidence"
        );

        let aborted_tid = manager.hlc.now();
        manager.get_or_create_transaction(&aborted_tid).lock().state = TxnState::Aborted;
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), aborted_tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), aborted_tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::AlreadyAborted)
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), aborted_tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delayed_duplicate_prepare_cannot_resurrect_volatile_completion() {
        let address = "127.0.0.1:5374";
        let group = "txn_data_site_volatile_delayed_prepare";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let manager = data_manager_for_database(&server, address, group).await;

        assert_delayed_duplicate_prepare_cannot_resurrect_completion(
            manager,
            runtime,
            Id::new(0, 90502),
        )
        .await;

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn delayed_duplicate_prepare_cannot_resurrect_durable_completion() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5375";
        let group = "txn_data_site_durable_delayed_prepare";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = data_manager_for_database(&server, address, group).await;

        assert_delayed_duplicate_prepare_cannot_resurrect_completion(
            manager,
            runtime,
            Id::new(0, 90503),
        )
        .await;

        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_owner_resolver_uses_one_worker_and_bounded_scheduling_storage() {
        let address = "127.0.0.1:5491";
        let group = "txn_data_site_bounded_owner_resolver";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let manager = DataManager::new_with_lock_timeout(runtime, server.hlc.clone(), 5);
        let unavailable_coordinator = server.server_id.wrapping_add(1_000_000);

        for sequence in 0..2_048u64 {
            let owner = TxnPriority::new(
                test_hlc(90_000 + sequence, unavailable_coordinator),
                unavailable_coordinator,
            );
            manager.owner_index.lock().add(&owner, 1);
            manager.queue_stale_owner_resolution(owner);
        }

        assert!(
            manager.resolver_scheduled_storage_len_for_test() <= 256,
            "ready/delay scheduling storage must remain at its fixed capacity"
        );
        tokio::time::timeout(Duration::from_millis(250), async {
            while manager.resolver_worker_cardinality_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the one configured resolver worker must start");
        assert_eq!(
            manager.resolver_worker_cardinality_for_test(),
            1,
            "all stale owners must share one central resolver worker"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_owner_resolver_globally_bounds_immediate_rpc_attempt_rate() {
        let address = "127.0.0.1:5492";
        let group = "txn_data_site_bounded_owner_rpc_rate";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let manager = DataManager::new_with_lock_timeout(runtime, server.hlc.clone(), 5);
        let unavailable_coordinator = server.server_id.wrapping_add(1_000_001);

        for sequence in 0..512u64 {
            let owner = TxnPriority::new(
                test_hlc(100_000 + sequence, unavailable_coordinator),
                unavailable_coordinator,
            );
            manager.owner_index.lock().add(&owner, 1);
            manager.queue_stale_owner_resolution(owner);
        }

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            manager.total_resolution_attempt_count_for_test() <= 64,
            "a stale-owner flood must not issue one immediate RPC per owner"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_owner_resolver_fairly_reaches_late_work_after_decision_appears() {
        let address = "127.0.0.1:5493";
        let group = "txn_data_site_fair_owner_resolver";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let first_unknown_owner =
            TxnPriority::new(test_hlc(110_000, server.server_id), server.server_id);

        for sequence in 0..1_024u64 {
            let owner = TxnPriority::new(
                test_hlc(110_000 + sequence, server.server_id),
                server.server_id,
            );
            manager.owner_index.lock().add(&owner, 1);
            manager.queue_stale_owner_resolution(owner);
        }

        let coordinator = runtime.txn_manager().expect("transaction manager").clone();
        let tail_tid =
            <super::super::manager::TransactionManager as super::super::manager::Service>::begin(
                &coordinator,
            )
            .await
            .unwrap();
        let tail_owner = TxnPriority::new(tail_tid, server.server_id);
        let tail_cell = Id::new(0, 90520);
        {
            let txn_lock = manager.get_or_create_transaction(&tail_tid);
            let mut txn = txn_lock.lock();
            txn.state = TxnState::Prepared;
            txn.certified.insert(
                tail_cell,
                PrepareOp {
                    id: tail_cell,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                },
            );
            txn.affected_cells = vec![tail_cell];
            txn.coordinator_id = Some(server.server_id);
            txn.last_activity = get_time();
        }
        {
            let meta = manager.cell_meta_mutex(&tail_cell);
            let mut meta = meta.lock();
            meta.owner = Some(tail_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }
        manager.owner_index.lock().add(&tail_owner, 1);
        manager.queue_stale_owner_resolution(tail_owner.clone());

        let first_attempt = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if manager.resolution_attempt_count_for_test(&tail_owner) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            first_attempt.is_ok(),
            "rotating discovery must fairly reach late canonical work; total attempts={}, scheduled={}, pending={}, cursor={:?}",
            manager.total_resolution_attempt_count_for_test(),
            manager.resolver_scheduled_storage_len_for_test(),
            manager.queued_resolution_count_for_test(),
            manager.owner_index.lock().resolution_cursor,
        );
        assert!(
            manager.owner_is_present(&tail_owner),
            "InProgress must retain the exact late owner"
        );

        assert_eq!(
            <super::super::manager::TransactionManager as super::super::manager::Service>::abort(
                &coordinator,
                tail_tid,
            )
            .await
            .unwrap(),
            AbortResult::Success(None)
        );
        let convergence = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if !manager.owner_is_present(&tail_owner) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
        assert!(
            convergence.is_ok(),
            "late work must converge after an explicit decision appears; tail attempts={}, total attempts={}, scheduled={}, pending={}",
            manager.resolution_attempt_count_for_test(&tail_owner),
            manager.total_resolution_attempt_count_for_test(),
            manager.resolver_scheduled_storage_len_for_test(),
            manager.queued_resolution_count_for_test(),
        );
        assert!(
            manager.owner_is_present(&first_unknown_owner),
            "Unknown must remain unresolved while other work converges"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_data_manager_terminates_background_workers_without_a_strong_arc_leak() {
        let address = "127.0.0.1:5494";
        let group = "txn_data_site_owner_resolver_drop";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let manager = DataManager::new_with_lock_timeout(runtime, server.hlc.clone(), 5);
        let manager_ref = Arc::downgrade(&manager);
        let worker_activity = manager.owner_resolution_worker_activity.clone();
        tokio::time::timeout(Duration::from_millis(250), async {
            while worker_activity.live.load(Relaxed) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the central resolver worker must start");

        drop(manager);
        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if manager_ref.upgrade().is_none() && worker_activity.live.load(Relaxed) == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background workers must not retain DataManager indefinitely");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_owner_resolution_finishes_exact_known_commit() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5347";
        let group = "txn_data_site_stale_commit_resolution";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let cell_id = Id::new(0, 8208);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 21, 0);
        let tid = server.hlc.try_now().unwrap();
        let owner = TxnPriority::new(tid, server.server_id);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &tid, vec![prepare_op.clone()]).await,
            DMPrepareResult::Success
        );
        let commit_hlc = server.hlc.try_now().unwrap();
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    22,
                    "stale-commit-resolution",
                ))],
            )
            .await
            .payload,
            DMCommitResult::Success
        );
        assert!(
            matches!(
                runtime.chunks().head_snapshot(&cell_id, u64::MAX).unwrap(),
                SnapshotRead::Wait
            ),
            "the installed revision must remain pending before explicit resolution"
        );
        runtime
            .undo_log()
            .expect("durable coordinator undo log")
            .write_coordinator_commit_decision(&tid, commit_hlc, &[server.server_id])
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        let contender = server.hlc.try_now().unwrap();
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![prepare_op],).await,
            DMPrepareResult::NotRealizable
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.cell_meta_mutex(&cell_id).lock().owner.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("known Commit resolution must release the stale owner");
        assert_ne!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner),
            "the exact stale owner must be resolved"
        );
        match runtime
            .chunks()
            .read_cell_snapshot(&cell_id, u64::MAX)
            .unwrap()
        {
            SnapshotRead::Present(cell) => {
                assert_eq!(*cell.data["score"].u64().unwrap(), 22);
            }
            other => panic!("known Commit must promote the pending value, got {other:?}"),
        }

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_unknown_owner_is_retained_retried_and_deduplicated() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5348";
        let group = "txn_data_site_stale_unknown_resolution";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let cell_id = Id::new(0, 8209);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 31, 0);
        let tid = server.hlc.try_now().unwrap();
        let owner = TxnPriority::new(tid, server.server_id);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &tid, vec![prepare_op.clone()]).await,
            DMPrepareResult::Success
        );
        let commit_hlc = server.hlc.try_now().unwrap();
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    32,
                    "stale-unknown-resolution",
                ))],
            )
            .await
            .payload,
            DMCommitResult::Success
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
        for _ in 0..8 {
            let contender = server.hlc.try_now().unwrap();
            assert_eq!(
                prepare_ops_local(
                    &manager,
                    server.server_id,
                    &contender,
                    vec![prepare_op.clone()],
                )
                .await,
                DMPrepareResult::NotRealizable
            );
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.resolution_attempt_count_for_test(&owner) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("Unknown resolution must be retried");
        assert_eq!(
            manager.queued_resolution_count_for_test(),
            1,
            "repeated stale prepares must share one owner resolver"
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone()),
            "Unknown must retain the exact owner"
        );
        assert!(
            matches!(
                runtime.chunks().head_snapshot(&cell_id, u64::MAX).unwrap(),
                SnapshotRead::Wait
            ),
            "Unknown must keep the pending revision hidden"
        );

        runtime
            .undo_log()
            .expect("durable coordinator undo log")
            .write_coordinator_commit_decision(&tid, commit_hlc, &[server.server_id])
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.cell_meta_mutex(&cell_id).lock().owner.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a later exact Commit decision must finish the retry loop");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_in_progress_owner_is_retained_until_coordinator_aborts() {
        let address = "127.0.0.1:5440";
        let group = "txn_data_site_stale_in_progress";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let coordinator = runtime.txn_manager().expect("transaction manager").clone();
        let tid =
            <super::manager::TransactionManager as super::manager::Service>::begin(&coordinator)
                .await
                .unwrap();
        let owner = TxnPriority::new(tid, server.server_id);
        let cell_id = Id::new(0, 8213);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 71, 0);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &tid, vec![prepare_op.clone()]).await,
            DMPrepareResult::Success
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        let contender = server.hlc.try_now().unwrap();
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![prepare_op]).await,
            DMPrepareResult::NotRealizable
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.resolution_attempt_count_for_test(&owner) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("InProgress resolution must be retried");
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone()),
            "InProgress must never clear the owner"
        );

        assert_eq!(
            <super::manager::TransactionManager as super::manager::Service>::abort(
                &coordinator,
                tid,
            )
            .await
            .unwrap(),
            AbortResult::Success(None)
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.cell_meta_mutex(&cell_id).lock().owner.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a later explicit Abort decision must release the owner");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolution_rpc_failure_retains_owner_until_a_later_explicit_decision() {
        let address = "127.0.0.1:5441";
        let group = "txn_data_site_resolution_rpc_failure";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let coordinator = runtime.txn_manager().expect("transaction manager").clone();
        let tid =
            <super::manager::TransactionManager as super::manager::Service>::begin(&coordinator)
                .await
                .unwrap();
        let owner = TxnPriority::new(tid, server.server_id);
        let cell_id = Id::new(0, 8214);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 81, 0);

        assert_eq!(
            prepare_ops_local(
                &manager,
                server.server_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        coordinator.fail_next_resolution_request_for_test();
        assert!(
            !manager.resolve_stale_owner_once(&owner).await,
            "a failed coordinator request must remain retryable"
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone()),
            "RPC failure must retain the exact owner"
        );

        assert_eq!(
            <super::manager::TransactionManager as super::manager::Service>::abort(
                &coordinator,
                tid,
            )
            .await
            .unwrap(),
            AbortResult::Success(None)
        );
        assert!(
            manager.resolve_stale_owner_once(&owner).await,
            "the later explicit Abort decision must finish resolution"
        );
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unavailable_remote_coordinator_is_retried_without_owner_reclamation() {
        let address = "127.0.0.1:5442";
        let group = "txn_data_site_unavailable_remote_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let unavailable_coordinator = server.server_id.wrapping_add(1_000_000);
        let tid = server.hlc.try_now().unwrap();
        let owner = TxnPriority::new(tid, unavailable_coordinator);
        let cell_id = Id::new(0, 8215);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 91, 0);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(
                &manager,
                unavailable_coordinator,
                &tid,
                vec![prepare_op.clone()],
            )
            .await,
            DMPrepareResult::Success
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        let contender = server.hlc.try_now().unwrap();
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![prepare_op]).await,
            DMPrepareResult::NotRealizable
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.resolution_attempt_count_for_test(&owner) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("an unavailable coordinator must remain on the retry queue");
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone()),
            "network unavailability must never infer Abort or clear ownership"
        );
        assert_eq!(manager.queued_resolution_count_for_test(), 1);

        assert_eq!(
            <DataManager as Service>::abort(&manager, server.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, server.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_resolution_timestamp_mismatch_retains_owner_and_pending_value() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5349";
        let group = "txn_data_site_stale_commit_mismatch";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let cell_id = Id::new(0, 8210);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 41, 0);
        let tid = server.hlc.try_now().unwrap();
        let owner = TxnPriority::new(tid, server.server_id);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &tid, vec![prepare_op.clone()]).await,
            DMPrepareResult::Success
        );
        let installed_hlc = server.hlc.try_now().unwrap();
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                installed_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    42,
                    "stale-commit-mismatch",
                ))],
            )
            .await
            .payload,
            DMCommitResult::Success
        );
        let mismatched_hlc = server.hlc.try_now().unwrap();
        assert_ne!(installed_hlc, mismatched_hlc);
        runtime
            .undo_log()
            .expect("durable coordinator undo log")
            .write_coordinator_commit_decision(&tid, mismatched_hlc, &[server.server_id])
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;
        let contender = server.hlc.try_now().unwrap();
        assert_eq!(
            prepare_ops_local(&manager, server.server_id, &contender, vec![prepare_op],).await,
            DMPrepareResult::NotRealizable
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.resolution_attempt_count_for_test(&owner) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("mismatched Commit resolution must retry");
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner),
            "a mismatched Commit timestamp must retain ownership"
        );
        assert!(
            matches!(
                runtime.chunks().head_snapshot(&cell_id, u64::MAX).unwrap(),
                SnapshotRead::Wait
            ),
            "a mismatched Commit timestamp must keep installed output hidden"
        );

        runtime
            .undo_log()
            .expect("durable coordinator undo log")
            .write_coordinator_commit_decision(&tid, installed_hlc, &[server.server_id])
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if manager.cell_meta_mutex(&cell_id).lock().owner.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("a later exact Commit decision must finish the retry loop");

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_abort_resolution_compensates_then_ends_idempotently() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5350";
        let group = "txn_data_site_stale_abort_resolution";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new_with_lock_timeout(runtime.clone(), server.hlc.clone(), 5);
        let cell_id = Id::new(0, 8211);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 51, 0);
        let tid = server.hlc.try_now().unwrap();
        let owner = TxnPriority::new(tid, server.server_id);

        assert_eq!(
            prepare_ops_local(
                &manager,
                server.server_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                server.hlc.try_now().unwrap(),
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    52,
                    "stale-abort-resolution",
                ))],
            )
            .await
            .payload,
            DMCommitResult::Success
        );
        let undo_log = runtime.undo_log().expect("durable coordinator undo log");
        undo_log
            .write_coordinator_abort_decision(&tid, &[server.server_id])
            .unwrap();
        undo_log.fail_next_abort_marker_for_test();

        assert!(
            !manager.resolve_stale_owner_once(&owner).await,
            "a failed durable end must retain the resolver for retry"
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone()),
            "abort compensation alone must not release the owner"
        );
        let compensated = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*compensated.data["score"].u64().unwrap(), 51);
        assert!(compensated.header.revision_ts > initial_revision_ts);
        let compensation_ts = compensated.header.revision_ts;

        assert!(
            manager.resolve_stale_owner_once(&owner).await,
            "AlreadyAborted followed by an idempotent end must finish resolution"
        );
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*retained.data["score"].u64().unwrap(), 51);
        assert_eq!(
            retained.header.revision_ts, compensation_ts,
            "retry must not install duplicate compensation"
        );
        assert_eq!(
            undo_log.participant_completion(&tid).unwrap(),
            Some(TxnState::Aborted),
            "participant completion evidence must make end restart-idempotent"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, server.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn participant_retirement_prepare_and_finalize_are_durable_and_idempotent() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5351";
        let group = "txn_data_site_participant_retirement";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8212);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 61, 0);
        let tid = server.hlc.try_now().unwrap();

        assert_eq!(
            prepare_ops_local(
                &manager,
                server.server_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            <DataManager as Service>::abort(&manager, server.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, server.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        for _ in 0..2 {
            assert_eq!(
                <DataManager as Service>::retire(
                    &manager,
                    server.hlc.now(),
                    tid,
                    TxnResolution::Abort,
                )
                .await
                .payload,
                EndResult::Success
            );
        }
        assert_eq!(
            runtime
                .undo_log()
                .unwrap()
                .participant_retirement(&tid)
                .unwrap(),
            Some(undo_log::ParticipantRetirementRecord {
                outcome: TxnState::Aborted,
                expires_at_ms: 0,
                finalized: false,
            })
        );

        let accepted_before = get_time();
        assert_eq!(
            <DataManager as Service>::finalize_retirement(
                &manager,
                server.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload,
            EndResult::Success
        );
        let accepted_after = get_time();
        let first_finalized = runtime
            .undo_log()
            .unwrap()
            .participant_retirement(&tid)
            .unwrap()
            .expect("finalize must persist participant retirement");
        assert_eq!(first_finalized.outcome, TxnState::Aborted);
        assert!(first_finalized.finalized);
        assert!(
            first_finalized.expires_at_ms
                >= accepted_before.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS)
                && first_finalized.expires_at_ms
                    <= accepted_after.saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS),
            "participant expiry must use its local finalize-acceptance clock"
        );

        assert_eq!(
            <DataManager as Service>::finalize_retirement(
                &manager,
                server.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload,
            EndResult::Success
        );
        assert_eq!(
            runtime
                .undo_log()
                .unwrap()
                .participant_retirement(&tid)
                .unwrap(),
            Some(first_finalized),
            "idempotent finalize retry must preserve the first accepted TTL"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn participant_finalize_ttl_starts_after_delayed_local_acceptance_under_clock_skew() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5382";
        let group = "txn_data_site_retirement_delay_and_skew";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();
        runtime
            .undo_log()
            .unwrap()
            .write_abort_marker(&tid)
            .unwrap();
        manager.record_durable_completion(tid, TxnState::Aborted, None);
        assert_eq!(
            <DataManager as Service>::retire(
                &manager,
                manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload,
            EndResult::Success
        );

        let coordinator_send_ms = get_time();
        let participant_send_ms = coordinator_send_ms.saturating_add(10_000_000);
        let simulated_network_delay_ms = 120_000;
        let participant_clock = install_participant_clock_for_test(
            server.server_id,
            group,
            group,
            tid,
            participant_send_ms,
        )
        .expect("durable participant manager must be registered");
        let pause = install_retirement_read_pause(tid, true);
        let finalize_manager = manager.clone();
        let finalize = tokio::spawn(async move {
            <DataManager as Service>::finalize_retirement(
                &finalize_manager,
                finalize_manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload
        });
        pause.wait_until_entered().await;
        participant_clock.advance_by(simulated_network_delay_ms);
        pause.release();
        assert_eq!(finalize.await.unwrap(), EndResult::Success);

        let finalized = runtime
            .undo_log()
            .unwrap()
            .participant_retirement(&tid)
            .unwrap()
            .expect("participant-local finalize evidence");
        assert_eq!(
            finalized.expires_at_ms,
            participant_send_ms
                .saturating_add(simulated_network_delay_ms)
                .saturating_add(VOLATILE_PARTICIPANT_COMPLETION_RETENTION_MS),
            "network delay and coordinator/participant clock skew must not shorten the local acceptance interval"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn participant_retirement_rmw_is_serialized_and_monotonic_per_tid() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5380";
        let group = "txn_data_site_retirement_serialization";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let manager = data_manager_for_database(&server, address, group).await;
        let tid = manager.hlc.now();
        runtime
            .undo_log()
            .unwrap()
            .write_abort_marker(&tid)
            .unwrap();
        manager.record_durable_completion(tid, TxnState::Aborted, None);

        let pause = install_retirement_read_pause(tid, false);
        let old_manager = manager.clone();
        let old_retire = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(<DataManager as Service>::retire(
                &old_manager,
                old_manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            ))
            .payload
        });
        pause.wait_until_entered().await;

        let finalize_manager = manager.clone();
        let mut finalize = tokio::spawn(async move {
            <DataManager as Service>::finalize_retirement(
                &finalize_manager,
                finalize_manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload
        });
        let finalized_while_old_read_was_paused =
            tokio::time::timeout(Duration::from_millis(100), &mut finalize)
                .await
                .ok();
        pause.release();

        assert_eq!(old_retire.await.unwrap(), EndResult::Success);
        let finalize_result = match finalized_while_old_read_was_paused {
            Some(result) => result.unwrap(),
            None => finalize.await.unwrap(),
        };
        assert_eq!(finalize_result, EndResult::Success);
        let retirement = runtime
            .undo_log()
            .unwrap()
            .participant_retirement(&tid)
            .unwrap()
            .expect("retirement evidence must remain present");
        assert!(
            retirement.finalized,
            "an older retirement prepare may never overwrite accepted finalize evidence"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn expired_proof_refinalize_is_atomic_with_same_tid_prepare() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5381";
        let group = "txn_data_site_refinalize_prepare_serialization";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let coordinator_id = server.server_id;
        let cell_id = Id::new(0, 90507);
        let revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 71, 0);
        let tid = manager.hlc.now();
        let undo = runtime.undo_log().unwrap();
        undo.write_abort_marker(&tid).unwrap();
        let expired_at_ms = get_time().saturating_sub(1);
        undo.finalize_participant_retirement(&tid, TxnState::Aborted, expired_at_ms)
            .unwrap();
        manager.record_durable_completion(tid, TxnState::Aborted, Some(expired_at_ms));

        let pause = install_retirement_read_pause(tid, true);
        let finalize_manager = manager.clone();
        let finalize = tokio::spawn(async move {
            <DataManager as Service>::finalize_retirement(
                &finalize_manager,
                finalize_manager.hlc.now(),
                tid,
                TxnResolution::Abort,
            )
            .await
            .payload
        });
        pause.wait_until_entered().await;

        let prepare_manager = manager.clone();
        let mut prepare = tokio::spawn(async move {
            prepare_ops_local(
                &prepare_manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(revision_ts),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut prepare)
                .await
                .is_err(),
            "same-TID prepare must synchronize with retirement re-finalization"
        );

        pause.release();
        assert_eq!(finalize.await.unwrap(), EndResult::Success);
        assert_eq!(
            prepare.await.unwrap(),
            DMPrepareResult::StateError(TxnState::Aborted)
        );
        assert!(manager.find_transaction(&tid).is_none());
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_does_not_reclaim_aged_foreign_owner() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5346";
        let group = "txn_data_site_aged_owner_wait_die";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8205);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 21, 0);
        let t1 = test_hlc(1, 21);
        let t2 = test_hlc(1, 22);
        let t1_owner = TxnPriority::new(t1.clone(), 21);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let first = <DataManager as Service>::prepare(
            &manager,
            21,
            t1.clone(),
            t1.clone(),
            vec![prepare_op.clone()],
        )
        .await
        .payload;
        assert_eq!(first, DMPrepareResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone())
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.lock_acquired_at = Some(get_time() - DEFAULT_LOCK_TIMEOUT_MS - 1);
        }

        let second = <DataManager as Service>::prepare(
            &manager,
            22,
            t2.clone(),
            t2.clone(),
            vec![prepare_op],
        )
        .await
        .payload;
        assert_eq!(second, DMPrepareResult::NotRealizable);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone()),
            "owner age must not permit blind takeover"
        );

        let abort_t2 = <DataManager as Service>::abort(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(abort_t2, AbortResult::Success(None));
        let end_t2 = <DataManager as Service>::end(&manager, t2.clone(), t2.clone())
            .await
            .payload;
        assert_eq!(end_t2, EndResult::Success);
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner)
        );

        let abort_t1 = <DataManager as Service>::abort(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(abort_t1, AbortResult::Success(None));
        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_validation_rejects_missing_coordinator_without_rollback() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5348";
        let group = "txn_data_site_abort_missing_coordinator";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8206);
        let initial_score = 41;
        let committed_score = 55;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let tid = test_hlc(1, 23);
        let owner = TxnPriority::new(tid.clone(), 23);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let prepare = <DataManager as Service>::prepare(
            &manager,
            23,
            tid.clone(),
            tid.clone(),
            vec![prepare_op],
        )
        .await
        .payload;
        assert_eq!(prepare, DMPrepareResult::Success);

        let commit = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            tid.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                committed_score,
                "counter_abort_missing_coordinator_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.revision_ts;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_revision_ts);

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert!(txn.history.contains_key(&cell_id));
            txn.coordinator_id = None;
        }

        let abort = <DataManager as Service>::abort(&manager, tid.clone(), tid.clone())
            .await
            .payload;

        assert_eq!(abort, AbortResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), committed_score);
        assert_eq!(persisted.header.revision_ts, committed_version);

        let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn_lock.lock();
        assert_eq!(txn.state, TxnState::Committed);
        assert!(txn.history.contains_key(&cell_id));
        drop(txn);

        assert_eq!(manager.cell_meta_mutex(&cell_id).lock().owner, Some(owner));

        {
            let txn_lock = manager.txns.get(&tid).expect("txn should remain tracked");
            let mut txn = txn_lock.lock();
            txn.coordinator_id = Some(23);
        }

        let end = <DataManager as Service>::end(&manager, tid.clone(), tid.clone())
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_validation_rejects_stale_committed_owner_before_rollback() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5349";
        let group = "txn_data_site_abort_stale_owner";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8207);
        let initial_score = 61;
        let committed_score = 77;
        let initial_revision_ts =
            seed_cell_revision(&runtime, schema.id, cell_id, initial_score, 0);
        let t1 = test_hlc(1, 24);
        let t1_owner = TxnPriority::new(t1.clone(), 24);
        let t2_owner = TxnPriority::new(test_hlc(1, 25), 25);
        let t1_prepare = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        let prepare_t1 = <DataManager as Service>::prepare(
            &manager,
            24,
            t1.clone(),
            t1.clone(),
            vec![t1_prepare],
        )
        .await
        .payload;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = <DataManager as Service>::commit(
            &manager,
            manager.hlc.now(),
            t1.clone(),
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                committed_score,
                "counter_abort_stale_owner_commit",
            ))],
        )
        .await
        .payload;
        assert_eq!(commit_t1, DMCommitResult::Success);

        let committed = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        let committed_version = committed.header.revision_ts;
        assert_eq!(*committed.data["score"].u64().unwrap(), committed_score);
        assert!(committed_version > initial_revision_ts);

        {
            let txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert!(txn.history.contains_key(&cell_id));
        }

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            // Simulate stale-owner replacement directly. A real prepare must not
            // observe or certify t1's pending revision before end promotes it.
            meta.owner = Some(t2_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        let abort_t1 = <DataManager as Service>::abort(&manager, t1.clone(), t1.clone())
            .await
            .payload;

        assert_eq!(abort_t1, AbortResult::CheckFailed(CheckError::CannotEnd));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), committed_score);
        assert_eq!(persisted.header.revision_ts, committed_version);

        let t1_txn_lock = manager.txns.get(&t1).expect("t1 should remain tracked");
        let t1_txn = t1_txn_lock.lock();
        assert_eq!(t1_txn.state, TxnState::Committed);
        assert!(t1_txn.history.contains_key(&cell_id));
        drop(t1_txn);

        assert_ne!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t1_owner.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(t2_owner.clone())
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(t1_owner);
            meta.lock_acquired_at = Some(get_time());
        }

        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_vector_clock_stale_update_rejected_after_committed_peer_changes_version() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5351";
        let group = "txn_data_site_concurrent_stale_update";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8210);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 0, 0);
        let t1 = test_hlc(1, 11);
        let t2 = test_hlc(1, 22);
        let prepare_op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision_ts),
            intent: PrepareIntent::Write,
        };

        // (Dropped the `t1.relation(&t2) == Concurrent` precondition: HLC is a
        // total order with no `Concurrent` relation. The behaviour under test —
        // a stale-versioned update from a peer being rejected after another peer
        // commits a new revision_ts — does not depend on the two tids' relative age.)

        let prepare_t1 = prepare_ops_local(&manager, 11, &t1, vec![prepare_op.clone()]).await;
        assert_eq!(prepare_t1, DMPrepareResult::Success);

        let commit_t1 = commit_ops_local(
            &manager,
            &t1,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                1,
                "counter_concurrent_stale_t1",
            ))],
        )
        .await;
        assert_eq!(commit_t1, DMCommitResult::Success);

        let end_t1 = <DataManager as Service>::end(&manager, t1.clone(), t1.clone())
            .await
            .payload;
        assert_eq!(end_t1, EndResult::Success);

        let prepare_t2 = prepare_ops_local(&manager, 22, &t2, vec![prepare_op]).await;
        assert_eq!(prepare_t2, DMPrepareResult::NotRealizable);

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 1);
        assert!(persisted.header.revision_ts > initial_revision_ts);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_rejects_change_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5352";
        let group = "txn_data_site_commit_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8211);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = test_hlc(1, 31);

        let prepare = prepare_ops_local(
            &manager,
            31,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 7, "counter_commit_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.revision_ts > initial_revision_ts);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                99,
                "counter_commit_after_certification",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 7);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_remove_rejects_change_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5353";
        let group = "txn_data_site_remove_rechecks_certified_version";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8212);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 2, 0);
        let tid = test_hlc(1, 32);

        let prepare = prepare_ops_local(
            &manager,
            32,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 8, "counter_remove_external");
        let external_header = runtime.chunks().update_cell(&mut external).unwrap();
        assert!(external_header.revision_ts > initial_revision_ts);

        let commit = commit_ops_local(&manager, &tid, vec![CommitOp::Remove(cell_id)]).await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 8);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_update_rejects_missing_after_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5356";
        let group = "txn_data_site_update_rechecks_missing_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8216);
        let initial_revision_ts = seed_cell_revision(&runtime, schema.id, cell_id, 6, 0);
        let tid = test_hlc(1, 35);

        let prepare = prepare_ops_local(
            &manager,
            35,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Present(initial_revision_ts),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        runtime
            .chunks()
            .remove_cell_by(&cell_id, |_| true)
            .expect("external direct removal should succeed");

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Update(counter_cell(
                schema.id,
                cell_id,
                18,
                "counter_update_after_external_remove",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));
        assert!(
            matches!(
                runtime.chunks().read_cell(&cell_id),
                Err(ReadError::CellDoesNotExisted)
            ),
            "cell should remain missing after stale certified update"
        );

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_write_rejects_insert_after_absent_certification() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5354";
        let group = "txn_data_site_write_rechecks_absent_certification";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 8213);
        let tid = test_hlc(1, 33);

        let prepare = prepare_ops_local(
            &manager,
            33,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::Absent(None),
                intent: PrepareIntent::Write,
            }],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let mut external = counter_cell(schema.id, cell_id, 5, "counter_write_external");
        let external_header = runtime.chunks().write_cell(&mut external).unwrap();

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![CommitOp::Write(counter_cell(
                schema.id,
                cell_id,
                11,
                "counter_write_after_absent_prepare",
            ))],
        )
        .await;
        assert_eq!(commit, DMCommitResult::CellChanged(cell_id));

        let persisted = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*persisted.data["score"].u64().unwrap(), 5);
        assert_eq!(persisted.header.revision_ts, external_header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_prevalidates_full_payload_before_any_storage_mutation() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5355";
        let group = "txn_data_site_commit_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 8214);
        let cell_b = Id::new(0, 8215);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 3, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 4, 0);
        let tid = test_hlc(1, 34);

        let prepare = prepare_ops_local(
            &manager,
            34,
            &tid,
            vec![
                PrepareOp {
                    id: cell_a,
                    expectation: CellExpectation::Present(version_a),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: cell_b,
                    expectation: CellExpectation::Present(version_b),
                    intent: PrepareIntent::Read,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let before_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let before_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(schema.id, cell_a, 13, "counter_prevalidate_a")),
                CommitOp::Update(counter_cell(schema.id, cell_b, 14, "counter_prevalidate_b")),
            ],
        )
        .await;

        assert_eq!(commit, DMCommitResult::CheckFailed(CheckError::CannotEnd));

        let after_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let after_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        assert_eq!(after_a.data, before_a.data);
        assert_eq!(after_a.header.revision_ts, before_a.header.revision_ts);
        assert_eq!(after_b.data, before_b.data);
        assert_eq!(after_b.header.revision_ts, before_b.header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_prevalidates_current_storage_state_before_partial_write() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5357";
        let group = "txn_data_site_commit_storage_prevalidation";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_a = Id::new(0, 8217);
        let cell_b = Id::new(0, 8218);
        let version_a = seed_cell_revision(&runtime, schema.id, cell_a, 15, 0);
        let version_b = seed_cell_revision(&runtime, schema.id, cell_b, 25, 0);
        let tid = test_hlc(1, 36);

        let prepare = prepare_ops_local(
            &manager,
            36,
            &tid,
            vec![
                PrepareOp {
                    id: cell_a,
                    expectation: CellExpectation::Present(version_a),
                    intent: PrepareIntent::Write,
                },
                PrepareOp {
                    id: cell_b,
                    expectation: CellExpectation::Present(version_b),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::Success);

        let before_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let before_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();

        let mut external_b = counter_cell(schema.id, cell_b, 26, "counter_prevalidate_external_b");
        let external_b_header = runtime.chunks().update_cell(&mut external_b).unwrap();
        assert!(external_b_header.revision_ts > version_b);

        let commit = commit_ops_local(
            &manager,
            &tid,
            vec![
                CommitOp::Update(counter_cell(
                    schema.id,
                    cell_a,
                    115,
                    "counter_prevalidate_storage_a",
                )),
                CommitOp::Update(counter_cell(
                    schema.id,
                    cell_b,
                    126,
                    "counter_prevalidate_storage_b",
                )),
            ],
        )
        .await;

        assert_eq!(commit, DMCommitResult::CellChanged(cell_b));

        let after_a = runtime.chunks().read_cell(&cell_a).unwrap().to_owned();
        let after_b = runtime.chunks().read_cell(&cell_b).unwrap().to_owned();
        assert_eq!(after_a.data, before_a.data);
        assert_eq!(after_a.header.revision_ts, before_a.header.revision_ts);
        assert_eq!(after_b.data, external_b.data);
        assert_eq!(after_b.header.revision_ts, external_b_header.revision_ts);
        assert_ne!(after_b.header.revision_ts, before_b.header.revision_ts);

        let txn = manager.txns.get(&tid).expect("txn should remain tracked");
        let txn = txn.lock();
        assert_eq!(txn.state, TxnState::Prepared);
        assert!(txn.history.is_empty());
        drop(txn);

        abort_and_end_local(&manager, &tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stamped_meta_evicted_after_prepare_prefetch_is_retried_not_orphaned() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5372";
        let group = "txn_ds_cleanup_insert_race";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let insert_tid = manager.hlc.now();
        let insert_id = Id::new(0, 90501);
        let prefetched = manager.cell_meta_mutex(&insert_id);
        {
            let mut meta = prefetched.lock();
            meta.read = test_hlc(1, 7);
            meta.write = test_hlc(1, 7);
        }
        let pause = install_meta_acquire_delay_for_cell(insert_tid, insert_id);
        let prepare_manager = manager.clone();
        let prepare_task = tokio::spawn(async move {
            prepare_ops_local(
                &prepare_manager,
                0,
                &insert_tid,
                vec![PrepareOp {
                    id: insert_id,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                }],
            )
            .await
        });
        pause.wait_until_entered().await;

        manager.cell_meta_cleanup().await;
        assert!(
            manager.cells.get(&insert_id).is_none(),
            "the stamped prefetched candidate must be evicted during the pause"
        );
        pause.release();
        assert_eq!(prepare_task.await.unwrap(), DMPrepareResult::Success);

        let visible = manager.cells.get(&insert_id).expect("retried current meta");
        assert!(!Arc::ptr_eq(&visible, &prefetched));
        assert_eq!(
            visible.lock().owner,
            Some(TxnPriority::new(insert_tid, 0)),
            "prepare must own the replacement map entry, never the orphaned Arc"
        );

        abort_and_end_local(&manager, &insert_tid).await;
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unobserved_absence_requires_write_intent() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5503";
        let group = "txn_data_site_unobserved_absence_intent";
        let server = start_transaction_test_server(address, group).await;
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99033);
        let tid = manager.hlc.now();

        let result = prepare_ops_local(
            &manager,
            41,
            &tid,
            vec![PrepareOp {
                id: cell_id,
                expectation: CellExpectation::UnobservedAbsent,
                intent: PrepareIntent::Read,
            }],
        )
        .await;

        assert_eq!(result, DMPrepareResult::NotRealizable);
        assert!(manager.cell_meta_mutex(&cell_id).lock().owner.is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_certifies_exact_tombstone_never_absence_and_full_id() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5397";
        let group = "txn_data_site_exact_absence";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let deleted_id = Id::new(0, 99030);
        let never_id = Id::new(0, 99031);
        let colliding_id = Id::new(1, deleted_id.lower);

        let deleted_revision = seed_cell_revision(&runtime, schema.id, deleted_id, 1, 0);
        runtime.chunks().remove_cell(&deleted_id).unwrap();
        let tombstone_revision = match manager.current_expectation(&deleted_id).unwrap() {
            CellExpectation::Absent(Some(revision_ts)) => revision_ts,
            other => panic!("expected exact tombstone, got {other:?}"),
        };
        assert!(tombstone_revision > deleted_revision);

        let wrong_tid = manager.hlc.now();
        let wrong = prepare_ops_local(
            &manager,
            41,
            &wrong_tid,
            vec![PrepareOp {
                id: deleted_id,
                expectation: CellExpectation::Absent(Some(tombstone_revision - 1)),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(wrong, DMPrepareResult::NotRealizable);
        assert!(manager.cell_meta_mutex(&deleted_id).lock().owner.is_none());

        let exact_tid = manager.hlc.now();
        let exact = prepare_ops_local(
            &manager,
            42,
            &exact_tid,
            vec![
                PrepareOp {
                    id: deleted_id,
                    expectation: CellExpectation::Absent(Some(tombstone_revision)),
                    intent: PrepareIntent::Read,
                },
                PrepareOp {
                    id: never_id,
                    expectation: CellExpectation::Absent(None),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(exact, DMPrepareResult::Success);
        abort_and_end_local(&manager, &exact_tid).await;

        let collision_tid = manager.hlc.now();
        let collision = prepare_ops_local(
            &manager,
            43,
            &collision_tid,
            vec![PrepareOp {
                id: colliding_id,
                expectation: CellExpectation::Present(tombstone_revision),
                intent: PrepareIntent::Read,
            }],
        )
        .await;
        assert_eq!(collision, DMPrepareResult::NotRealizable);
        assert!(manager
            .cell_meta_mutex(&colliding_id)
            .lock()
            .owner
            .is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partial_point_observation_is_a_prepare_certificate() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5398";
        let group = "txn_data_site_partial_certificate";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let read_id = Id::new(0, 99032);
        let write_id = Id::new(0, 99033);
        let read_revision = seed_cell_revision(&runtime, schema.id, read_id, 1, 0);
        let write_revision = seed_cell_revision(&runtime, schema.id, write_id, 2, 0);
        let tid = manager.hlc.now();

        let observed =
            <DataManager as Service>::read_partial_raw(&manager, 44, tid, tid, read_id, 0, 8)
                .await
                .payload
                .unwrap();
        assert_eq!(
            observed.expectation,
            CellExpectation::Present(read_revision)
        );

        let mut external = counter_cell(schema.id, read_id, 7, "partial-cert-external");
        runtime.chunks().update_cell(&mut external).unwrap();

        let prepare = prepare_ops_local(
            &manager,
            44,
            &tid,
            vec![
                PrepareOp {
                    id: read_id,
                    expectation: observed.expectation,
                    intent: PrepareIntent::Read,
                },
                PrepareOp {
                    id: write_id,
                    expectation: CellExpectation::Present(write_revision),
                    intent: PrepareIntent::Write,
                },
            ],
        )
        .await;
        assert_eq!(prepare, DMPrepareResult::NotRealizable);
        assert!(manager.cell_meta_mutex(&read_id).lock().owner.is_none());
        assert!(manager.cell_meta_mutex(&write_id).lock().owner.is_none());

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_installs_pending_retries_same_hlc_and_end_promotes() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5399";
        let group = "txn_data_site_pending_commit";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99034);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let op = PrepareOp {
            id: cell_id,
            expectation: CellExpectation::Present(initial_revision),
            intent: PrepareIntent::Write,
        };
        assert_eq!(
            prepare_ops_local(&manager, 45, &tid, vec![op]).await,
            DMPrepareResult::Success
        );

        let commit_hlc = manager.hlc.now();
        let commit_op = CommitOp::Update(counter_cell(schema.id, cell_id, 9, "pending-commit-new"));
        let first =
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()])
                .await
                .payload;
        assert_eq!(first, DMCommitResult::Success);

        let txn = manager.txns.get(&tid).unwrap();
        {
            let txn = txn.lock();
            assert_eq!(txn.commit_hlc, Some(commit_hlc));
            assert_eq!(txn.installed.len(), 1);
            assert_eq!(
                txn.installed[&cell_id].node.load().0,
                RevisionState::PendingPresent
            );
        }
        assert!(matches!(
            runtime
                .chunks()
                .read_cell_snapshot(&cell_id, u64::MAX)
                .unwrap(),
            SnapshotRead::Wait
        ));
        let reader_tid = manager.hlc.now();
        assert!(matches!(
            <DataManager as Service>::read(&manager, 46, reader_tid, reader_tid, cell_id,)
                .await
                .payload,
            TxnExecResult::Wait
        ));

        let retry =
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()])
                .await
                .payload;
        assert_eq!(retry, DMCommitResult::Success);
        let conflicting =
            <DataManager as Service>::commit(&manager, manager.hlc.now(), tid, vec![commit_op])
                .await
                .payload;
        assert_eq!(
            conflicting,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );

        let end = <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
            .await
            .payload;
        assert_eq!(end, EndResult::Success);
        let current = runtime
            .chunks()
            .read_cell_snapshot(&cell_id, u64::MAX)
            .unwrap();
        match current {
            SnapshotRead::Present(cell) => {
                assert_eq!(cell.header.revision_ts, commit_hlc.ts);
                assert_eq!(*cell.data["score"].u64().unwrap(), 9);
            }
            other => panic!("expected promoted commit, got {other:?}"),
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_retry_same_hlc_accepts_identical_nan_payload_bits() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5410";
        let group = "txn_data_site_commit_retry_identical_nan";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99041);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                51,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        let nan_bits = 0x7ff8_0000_0000_0042;
        let mut cell = counter_cell(schema.id, cell_id, 9, "exact-nan-payload");
        let OwnedValue::Map(data) = &mut cell.data else {
            unreachable!()
        };
        data.insert("measurement", OwnedValue::F64(f64::from_bits(nan_bits)));
        let commit_op = CommitOp::Update(cell);
        let commit_hlc = manager.hlc.now();
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op.clone()],)
                .await
                .payload,
            DMCommitResult::Success
        );
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![commit_op])
                .await
                .payload,
            DMCommitResult::Success,
            "an exact same-HLC NaN retry must be idempotently accepted"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_retry_same_hlc_requires_exact_operation_and_cell_without_mutation() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5403";
        let group = "txn_data_site_commit_retry_exact_payload";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99035);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                47,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        let commit_hlc = manager.hlc.now();
        let mut original_cell = counter_cell(schema.id, cell_id, 9, "exact-payload-original");
        let OwnedValue::Map(original_data) = &mut original_cell.data else {
            unreachable!()
        };
        original_data.insert("signed_zero", OwnedValue::F64(0.0));
        let original = CommitOp::Update(original_cell);
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![original.clone()],)
                .await
                .payload,
            DMCommitResult::Success
        );

        let txn_lock = manager.txns.get(&tid).expect("transaction should exist");
        let installed_before = txn_lock.lock().installed[&cell_id].node.load();
        let mut changed_signed_zero = original.clone();
        let CommitOp::Update(changed_cell) = &mut changed_signed_zero else {
            unreachable!()
        };
        let OwnedValue::Map(changed_data) = &mut changed_cell.data else {
            unreachable!()
        };
        changed_data
            .map
            .insert(hash_str("signed_zero"), OwnedValue::F64(-0.0));
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![changed_signed_zero],)
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        let changed = CommitOp::Update(counter_cell(
            schema.id,
            cell_id,
            10,
            "exact-payload-changed",
        ));
        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![changed])
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Remove(cell_id)],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        let mut changed_header = counter_cell(schema.id, cell_id, 9, "exact-payload-original");
        changed_header.header.flags = 1;
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(changed_header)],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            <DataManager as Service>::commit(
                &manager,
                commit_hlc,
                tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    Id::new(1, cell_id.lower),
                    9,
                    "exact-payload-original",
                ))],
            )
            .await
            .payload,
            DMCommitResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load(),
            installed_before,
            "a conflicting retry must not mutate the installed revision"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        let current = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*current.data["score"].u64().unwrap(), 9);

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_owner_loss_preserves_pending_state_and_can_retry() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5404";
        let group = "txn_data_site_end_owner_loss";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99036);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 48;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "end-owner-loss-pending",
                ))],
            )
            .await,
            DMCommitResult::Success
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = None;
            meta.lock_acquired_at = None;
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed end must preserve transaction state");
        assert_eq!(txn_lock.lock().state, TxnState::Committed);
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load().0,
            RevisionState::PendingPresent,
            "failed end must not promote before validating ownership"
        );

        {
            let meta = manager.cell_meta_mutex(&cell_id);
            let mut meta = meta.lock();
            meta.owner = Some(owner);
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        match runtime
            .chunks()
            .read_cell_snapshot(&cell_id, u64::MAX)
            .unwrap()
        {
            SnapshotRead::Present(cell) => {
                assert_eq!(*cell.data["score"].u64().unwrap(), 9);
            }
            other => panic!("expected promoted retry, got {other:?}"),
        }

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_validates_all_cell_owners_before_multi_cell_promotion() {
        let _ = env_logger::try_init();
        let address = "127.0.0.1:5405";
        let group = "txn_data_site_end_multi_cell_barrier";
        let server = start_transaction_test_server(address, group).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let first_id = Id::new(0, 99037);
        let second_id = Id::new(0, 99038);
        let first_revision = seed_cell_revision(&runtime, schema.id, first_id, 1, 0);
        let second_revision = seed_cell_revision(&runtime, schema.id, second_id, 2, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 49;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![
                    PrepareOp {
                        id: first_id,
                        expectation: CellExpectation::Present(first_revision),
                        intent: PrepareIntent::Write,
                    },
                    PrepareOp {
                        id: second_id,
                        expectation: CellExpectation::Present(second_revision),
                        intent: PrepareIntent::Write,
                    },
                ],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![
                    CommitOp::Update(counter_cell(schema.id, first_id, 11, "end-barrier-first",)),
                    CommitOp::Update(counter_cell(schema.id, second_id, 12, "end-barrier-second",)),
                ],
            )
            .await,
            DMCommitResult::Success
        );

        let foreign_owner = TxnPriority::new(manager.hlc.now(), 99);
        {
            let meta = manager.cell_meta_mutex(&second_id);
            let mut meta = meta.lock();
            meta.owner = Some(foreign_owner.clone());
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed end must preserve transaction state");
        for id in [first_id, second_id] {
            assert_eq!(
                txn_lock.lock().installed[&id].node.load().0,
                RevisionState::PendingPresent,
                "owner validation failure must precede every promotion"
            );
        }
        assert_eq!(
            manager.cell_meta_mutex(&first_id).lock().owner,
            Some(owner.clone())
        );
        assert_eq!(
            manager.cell_meta_mutex(&second_id).lock().owner,
            Some(foreign_owner)
        );

        {
            let meta = manager.cell_meta_mutex(&second_id);
            let mut meta = meta.lock();
            meta.owner = Some(owner);
            meta.lock_acquired_at = Some(get_time());
        }
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_marker_failure_keeps_pending_revision_owner_and_transaction_for_retry() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5407";
        let group = "txn_data_site_end_marker_failure";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99041);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 51;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "end-marker-failure",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let installed_location = manager.txns.get(&tid).unwrap().lock().installed[&cell_id]
            .node
            .load()
            .1;
        let installed_segment = runtime.chunks().list[0]
            .locate_segment(installed_location)
            .unwrap();
        let syncs_before_end = installed_segment.force_wal_sync_count_for_test();
        assert!(
            manager
                .txns
                .get(&tid)
                .unwrap()
                .lock()
                .installed_output_durable,
            "participant commit success must already prove exact output durability"
        );

        runtime
            .undo_log()
            .unwrap()
            .fail_next_commit_marker_for_test();
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert_eq!(
            installed_segment.force_wal_sync_count_for_test(),
            syncs_before_end,
            "end must reuse the participant commit durability proof instead of double-fsyncing"
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("marker failure must preserve transaction state");
        {
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Committed);
            assert_eq!(txn.durable_decision, None);
            assert_eq!(
                txn.installed[&cell_id].node.load().0,
                RevisionState::PendingPresent
            );
        }
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone())
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "participant marker failure must retain undo until end succeeds"
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            installed_segment.force_wal_sync_count_for_test(),
            syncs_before_end,
            "retry must continue reusing the participant commit durability proof"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_retry_after_durable_commit_completion_is_idempotently_successful() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5425";
        let group = "txn_data_site_end_response_loss_retry";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99045);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                55,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "response-was-lost-after-durable-end",
                ))],
            )
            .await,
            DMCommitResult::Success
        );

        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success,
            "a retry must use durable participant outcome evidence after live state is gone"
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn participant_commit_syncs_exact_output_before_success_without_emitting_decision() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5426";
        let group = "txn_data_site_commit_output_durability";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99046);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                56,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        let output_segment = runtime.chunks().list[0]
            .locate_segment(runtime.chunks().address_of(&cell_id))
            .unwrap();
        output_segment.fail_next_force_wal_sync_for_test();
        let update = CommitOp::Update(counter_cell(
            schema.id,
            cell_id,
            9,
            "commit-output-must-be-durable",
        ));
        let commit_hlc = manager.hlc.now();

        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![update.clone()],)
                .await
                .payload,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("output sync failure must retain participant state");
        assert_eq!(
            txn_lock.lock().installed[&cell_id].node.load().0,
            RevisionState::PendingPresent
        );
        {
            let txn = txn_lock.lock();
            assert_eq!(txn.state, TxnState::Prepared);
            assert!(!txn.installed_output_durable);
            assert_eq!(txn.durable_decision, None);
        }
        let owner = manager.cells.get(&cell_id).unwrap();
        assert_eq!(
            owner.lock().owner,
            Some(TxnPriority::new(tid, 56)),
            "retryable sync contention must retain the participant owner"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "participant commit durability is not a transaction decision"
        );
        assert_eq!(
            runtime
                .undo_log()
                .unwrap()
                .participant_completion(&tid)
                .unwrap(),
            None,
            "retryable sync contention must not emit a participant marker"
        );

        assert_eq!(
            <DataManager as Service>::commit(&manager, commit_hlc, tid, vec![update])
                .await
                .payload,
            DMCommitResult::Success
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "successful output preparation must not emit a completion marker"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wal_directory_sync_failure_rejects_participant_success_without_panicking() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5437";
        let group = "txn_data_site_wal_directory_sync_failure";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99053);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let chunk = runtime.chunks().locate_chunk_by_partition(cell_id.higher);
        let head = chunk
            .segs
            .get(&(chunk.get_head_seg_id() as usize))
            .expect("active head");
        let wal_dir = {
            let mut file_state = head.file_state.lock();
            let wal_dir = std::path::PathBuf::from(
                file_state
                    .manager
                    .wal_storage()
                    .expect("durable test WAL storage"),
            );
            drop(file_state.wal.take());
            file_state
                .manager
                .delete_wal(head.chunk_id, head.id, head.seq_id)
                .unwrap();
            wal_dir
        };
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                63,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        fail_next_directory_sync_for_test(&wal_dir);

        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    2,
                    "directory-sync-failure",
                ))],
            )
            .await,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn = manager.txns.get(&tid).expect("retry state retained");
        assert!(!txn.lock().installed_output_durable);
        assert_eq!(
            runtime
                .undo_log()
                .unwrap()
                .participant_completion(&tid)
                .unwrap(),
            None
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn in_doubt_update_does_not_pin_prior_segment_and_abort_uses_owned_prior() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5438";
        let group = "txn_data_site_in_doubt_without_segment_pin";
        let server = start_durable_transaction_test_server_with_chunk_size(
            address,
            group,
            &temp_dir,
            SEGMENT_SIZE * 5,
        )
        .await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99054);
        let prior_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let prior_segment = runtime
            .chunks()
            .locate_chunk_by_partition(cell_id.higher)
            .locate_segment(runtime.chunks().address_of(&cell_id))
            .expect("prior source segment");
        prior_segment
            .append_header
            .store(prior_segment.bound, std::sync::atomic::Ordering::Release);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                64,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(prior_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    2,
                    "in-doubt-update",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let installed_revision = runtime
            .chunks()
            .current_revision_ts(&cell_id)
            .expect("pending installed revision");
        let chunk = runtime.chunks().locate_chunk_by_partition(cell_id.higher);
        let installed_source = chunk
            .locate_segment(
                runtime
                    .chunks()
                    .history_location(&cell_id, installed_revision)
                    .expect("installed history location"),
            )
            .expect("installed source segment");

        let exclusive = SegmentExclusiveRefGuard::new(&prior_segment)
            .expect("in-doubt rollback state must not pin the prior segment");
        drop(exclusive);

        installed_source
            .append_header
            .store(installed_source.bound, std::sync::atomic::Ordering::Release);
        let filler_id = Id::new(0, 99055);
        seed_cell_revision(&runtime, schema.id, filler_id, 3, 0);
        let filler_source = chunk
            .locate_segment(runtime.chunks().address_of(&filler_id))
            .expect("filler source segment");
        filler_source
            .append_header
            .store(filler_source.bound, std::sync::atomic::Ordering::Release);
        let selected = chunk.segments();
        let source_ids: std::collections::HashSet<_> =
            selected.iter().map(|segment| segment.id).collect();
        let installed_source_location = runtime
            .chunks()
            .history_location(&cell_id, installed_revision)
            .expect("installed source location");
        chunk
            .head_seg_id
            .store(u64::MAX - 7, std::sync::atomic::Ordering::Release);
        let (_, reduced) = combine::CombinedCleaner::combine_segments(chunk, &selected)
            .expect("cleaner relocation should succeed without transaction-owned pins");
        assert!(reduced > 0);
        let relocated_location = runtime
            .chunks()
            .history_location(&cell_id, installed_revision)
            .expect("relocated installed revision");
        assert_ne!(relocated_location, installed_source_location);
        let destination = chunk
            .locate_segment(relocated_location)
            .expect("registered cleaner destination");
        assert!(!source_ids.contains(&destination.id));
        assert!(selected.iter().all(|source| !chunk.contains_seg(source.id)));
        chunk
            .head_seg_id
            .store(destination.id, std::sync::atomic::Ordering::Release);

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        let restored = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*restored.data["score"].u64().unwrap(), 1);
        assert!(restored.header.revision_ts > installed_revision);
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn durable_mutation_without_available_undo_is_rejected_before_cell_change() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5409";
        let group = "txn_data_site_durable_requires_undo";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99043);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                53,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        manager.fail_next_undo_availability_for_test();
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "must-not-be-installed",
                ))],
            )
            .await,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, initial_revision);
        assert_eq!(*retained.data["score"].u64().unwrap(), 1);

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backup_only_direct_participant_rejects_mutation_without_wal_before_any_change() {
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5424";
        let group = "txn_data_site_backup_without_wal";
        let server = start_backup_without_wal_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = DataManager::new(runtime.clone(), server.hlc.clone());
        let cell_id = Id::new(0, 99044);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        assert_eq!(
            prepare_ops_local(
                &manager,
                54,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );

        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "backup-only-must-not-install",
                ))],
            )
            .await,
            DMCommitResult::CheckFailed(CheckError::CannotEnd)
        );
        let retained = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(retained.header.revision_ts, initial_revision);
        assert_eq!(*retained.data["score"].u64().unwrap(), 1);
        assert!(
            !runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "rejection must precede undo output and every durable decision"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert!(!runtime
            .undo_log()
            .unwrap()
            .recover()
            .unwrap()
            .contains_key(&tid));

        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compensation_sync_failure_retries_the_exact_handle_without_duplicate_output() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5408";
        let group = "txn_data_site_compensation_sync_retry";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let cell_id = Id::new(0, 99042);
        let initial_revision = seed_cell_revision(&runtime, schema.id, cell_id, 1, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 52;
        let owner = TxnPriority::new(tid, coordinator_id);
        assert_eq!(
            prepare_ops_local(
                &manager,
                coordinator_id,
                &tid,
                vec![PrepareOp {
                    id: cell_id,
                    expectation: CellExpectation::Present(initial_revision),
                    intent: PrepareIntent::Write,
                }],
            )
            .await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![CommitOp::Update(counter_cell(
                    schema.id,
                    cell_id,
                    9,
                    "compensation-sync-failed-output",
                ))],
            )
            .await,
            DMCommitResult::Success
        );
        let installed_location = manager.txns.get(&tid).unwrap().lock().installed[&cell_id]
            .node
            .load()
            .1;
        runtime.chunks().list[0]
            .locate_segment(installed_location)
            .unwrap()
            .fail_next_force_wal_sync_for_test();

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::CannotEnd)
        );
        let compensation = runtime.chunks().read_cell(&cell_id).unwrap().to_owned();
        assert_eq!(*compensation.data["score"].u64().unwrap(), 1);
        let compensation_ts = compensation.header.revision_ts;
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner.clone())
        );

        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::Success(None)
        );
        assert_eq!(
            runtime
                .chunks()
                .read_cell(&cell_id)
                .unwrap()
                .header
                .revision_ts,
            compensation_ts,
            "retry must sync the retained exact compensation instead of installing another"
        );
        let compensation_location = manager.txns.get(&tid).unwrap().lock().history[&cell_id]
            .compensation
            .as_ref()
            .unwrap()
            .node
            .load()
            .1;
        let compensation_segment = runtime.chunks().list[0]
            .locate_segment(compensation_location)
            .unwrap();
        let syncs_after_abort = compensation_segment.force_wal_sync_count_for_test();
        runtime
            .undo_log()
            .unwrap()
            .fail_next_abort_marker_for_test();
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        assert_eq!(
            compensation_segment.force_wal_sync_count_for_test(),
            syncs_after_abort,
            "end must reuse the participant abort durability proof instead of double-fsyncing"
        );
        assert!(
            manager.txns.get(&tid).is_some(),
            "abort marker failure must preserve retryable transaction state"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "a failed abort marker must not suppress recovery"
        );
        assert_eq!(
            manager.cell_meta_mutex(&cell_id).lock().owner,
            Some(owner),
            "abort marker failure must preserve the owner barrier"
        );
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        assert_eq!(
            compensation_segment.force_wal_sync_count_for_test(),
            syncs_after_abort,
            "end retry must continue reusing the participant abort durability proof"
        );
        assert!(
            !runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .contains_key(&tid),
            "the synced abort marker must suppress recovery only after exact compensation sync"
        );
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_partial_promotion_failure_restores_pending_barrier_for_retry() {
        let _ = env_logger::try_init();
        let temp_dir = TempDir::new().unwrap();
        let address = "127.0.0.1:5406";
        let group = "txn_data_site_end_partial_promotion";
        let server = start_durable_transaction_test_server(address, group, &temp_dir).await;
        let runtime = server.current_database();
        let schema = install_prepare_test_schema(&runtime);
        let manager = data_manager_for_database(&server, address, group).await;
        let first_id = Id::new(0, 99039);
        let second_id = Id::new(0, 99040);
        let first_revision = seed_cell_revision(&runtime, schema.id, first_id, 1, 0);
        let second_revision = seed_cell_revision(&runtime, schema.id, second_id, 2, 0);
        let tid = manager.hlc.now();
        let coordinator_id = 50;
        let owner = TxnPriority::new(tid, coordinator_id);
        let prepare_ops = vec![
            PrepareOp {
                id: first_id,
                expectation: CellExpectation::Present(first_revision),
                intent: PrepareIntent::Write,
            },
            PrepareOp {
                id: second_id,
                expectation: CellExpectation::Present(second_revision),
                intent: PrepareIntent::Write,
            },
        ];
        assert_eq!(
            prepare_ops_local(&manager, coordinator_id, &tid, prepare_ops.clone()).await,
            DMPrepareResult::Success
        );
        assert_eq!(
            commit_ops_local(
                &manager,
                &tid,
                vec![
                    CommitOp::Update(counter_cell(
                        schema.id,
                        first_id,
                        11,
                        "partial-promotion-first",
                    )),
                    CommitOp::Update(counter_cell(
                        schema.id,
                        second_id,
                        12,
                        "partial-promotion-second",
                    )),
                ],
            )
            .await,
            DMCommitResult::Success
        );

        let delayed_prepare = install_prepare_delay_for_cell(tid, first_id);
        let duplicate_manager = manager.clone();
        let duplicate_prepare = tokio::spawn(async move {
            prepare_ops_local(&duplicate_manager, coordinator_id, &tid, prepare_ops).await
        });
        delayed_prepare.wait_until_entered().await;
        let failure = install_end_promotion_failure(tid, second_id);
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::CheckFailed(CheckError::CannotEnd)
        );
        let txn_lock = manager
            .txns
            .get(&tid)
            .expect("failed promotion must preserve transaction state");
        delayed_prepare.release();
        assert_eq!(
            duplicate_prepare.await.unwrap(),
            DMPrepareResult::StateError(TxnState::Committed)
        );
        assert!(
            manager
                .txns
                .get(&tid)
                .is_some_and(|current| Arc::ptr_eq(&current, &txn_lock)),
            "a duplicate prepare must preserve the exact live retry transaction"
        );
        assert_eq!(txn_lock.lock().state, TxnState::Committed);
        assert_eq!(
            txn_lock.lock().durable_decision,
            Some(TxnState::Committed),
            "the synced marker remains the irrevocable commit decision"
        );
        assert!(
            runtime
                .undo_log()
                .unwrap()
                .recover()
                .unwrap()
                .get(&tid)
                .is_none(),
            "the durable commit marker must suppress undo before promotion retry"
        );
        assert_eq!(
            <DataManager as Service>::abort(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            AbortResult::CheckFailed(CheckError::AlreadyCommitted)
        );
        for id in [first_id, second_id] {
            assert_eq!(
                txn_lock.lock().installed[&id].node.load().0,
                RevisionState::PendingPresent,
                "partial failure must restore the complete pending barrier"
            );
            assert_eq!(
                manager.cell_meta_mutex(&id).lock().owner,
                Some(owner.clone()),
                "partial failure must preserve every owner"
            );
        }

        drop(failure);
        assert_eq!(
            <DataManager as Service>::end(&manager, manager.hlc.now(), tid)
                .await
                .payload,
            EndResult::Success
        );
        for (id, expected_score) in [(first_id, 11), (second_id, 12)] {
            match runtime.chunks().read_cell_snapshot(&id, u64::MAX).unwrap() {
                SnapshotRead::Present(cell) => {
                    assert_eq!(*cell.data["score"].u64().unwrap(), expected_score);
                }
                other => panic!("expected promoted retry, got {other:?}"),
            }
        }

        server.shutdown().await;
    }
}

impl Service for DataManager {
    /////////////////////////////////////
    ///        Implement Services    ///
    ///////////////////////////////////

    fn read(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>> {
        FULL_READ_RPC_COUNT.fetch_add(1, Relaxed);
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().read_cell_snapshot(&id, tid.ts),
            |cell| cell.header.revision_ts,
        ))
    }
    fn read_selected(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        fields: Vec<u64>,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<OwnedCell>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().read_selected_snapshot(&id, tid.ts, &fields),
            |cell| cell.header.revision_ts,
        ))
    }
    fn head(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<CellHeader>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        self.response_with(Self::observed_snapshot(
            self.chunks().head_snapshot(&id, tid.ts),
            |header| header.revision_ts,
        ))
    }
    fn read_partial_raw(
        &self,
        _server_id: u64,
        clock: Hlc,
        tid: TxnId,
        id: Id,
        offset: usize,
        len: usize,
    ) -> BoxFuture<'_, DataSiteResponse<TxnExecResult<ObservedPoint<Vec<u8>>, ReadError>>> {
        if let Err(r) = self.prepare_read(&clock, &tid, &id) {
            return r;
        }
        let payload = match self
            .chunks()
            .read_partial_raw_snapshot(&id, tid.ts, offset, len)
        {
            Ok(SnapshotRead::Present(value)) => match self.chunks().head_snapshot(&id, tid.ts) {
                Ok(SnapshotRead::Present(header)) => TxnExecResult::Accepted(ObservedPoint {
                    value: Some(value),
                    expectation: CellExpectation::Present(header.revision_ts),
                }),
                Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
                Err(error) => TxnExecResult::Error(error),
                Ok(SnapshotRead::Absent(_)) => TxnExecResult::Error(ReadError::NotMatch),
            },
            Ok(SnapshotRead::Absent(delete_revision_ts)) => {
                TxnExecResult::Accepted(ObservedPoint {
                    value: None,
                    expectation: CellExpectation::Absent(delete_revision_ts),
                })
            }
            Ok(SnapshotRead::Wait) => TxnExecResult::Wait,
            Err(error) => TxnExecResult::Error(error),
        };
        self.response_with(payload)
    }
    fn prepare(
        &self,
        coordinator_id: u64,
        clock: Hlc,
        tid: TxnId,
        ops: Vec<PrepareOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMPrepareResult>> {
        debug!("PREPARE FOR {:?}, {} ops", &tid, ops.len());
        async move {
            #[cfg(feature = "occ_phase_profile")]
            let _phase_guard =
                super::phase_profile::guard(super::phase_profile::Phase::ParticipantPrepare);
            self.update_clock(clock);

            let prepared_ops = match Self::canonical_prepare_ops(ops) {
                Ok(prepared_ops) => prepared_ops,
                Err(result) => return self.response_with(result).await,
            };
            #[cfg(test)]
            if let Some(state) = Self::take_matching_prepare_delay(&tid, &prepared_ops) {
                Self::await_prepare_delay(&state).await;
            }

            let prepared_ops_by_id: BTreeMap<Id, PrepareOp> =
                prepared_ops.iter().cloned().map(|op| (op.id, op)).collect();
            let requester = TxnPriority::new(tid.clone(), coordinator_id);

            // Reject known-complete or non-identical live retries before
            // minting any CellMeta entries. The checks inside the acquisition
            // loop remain authoritative: end or another prepare may race this
            // fail-fast snapshot.
            match self.participant_completion_evidence(&tid, self.participant_time(&tid)) {
                Ok(Some(state @ (TxnState::Committed | TxnState::Aborted))) => {
                    return self
                        .response_with(DMPrepareResult::StateError(state))
                        .await;
                }
                Ok(Some(_)) => {
                    return self.response_with(DMPrepareResult::NotRealizable).await;
                }
                Ok(None) => {}
                Err(error) => {
                    error!(
                        "Failed to check participant completion before prepare {:?}: {:?}",
                        tid, error
                    );
                    return self.response_with(DMPrepareResult::NotRealizable).await;
                }
            }
            let live_rejection = self.find_transaction(&tid).and_then(|txn_lock| {
                let txn = txn_lock.lock();
                self.txns
                    .get(&tid)
                    .is_some_and(|current| Arc::ptr_eq(&current, &txn_lock))
                    .then(|| match txn.state {
                        TxnState::Started => None,
                        TxnState::Prepared
                            if txn.coordinator_id == Some(coordinator_id)
                                && txn.certified == prepared_ops_by_id =>
                        {
                            None
                        }
                        state => Some(DMPrepareResult::StateError(state)),
                    })
                    .flatten()
            });
            if let Some(rejection) = live_rejection {
                return self.response_with(rejection).await;
            }
            let mut prefetched_cell_mutexes = Some(
                prepared_ops
                    .iter()
                    .map(|op| self.cell_meta_mutex(&op.id))
                    .collect::<Vec<_>>(),
            );
            #[cfg(test)]
            if let Some(state) = Self::take_matching_meta_acquire_delay(&tid, &prepared_ops) {
                Self::await_prepare_delay(&state).await;
            }

            let result = {
                // Serialize the authoritative completion check, transaction
                // publication, and owner acquisition with retirement
                // re-finalization for this TID.
                let _lifecycle_guard = self.participant_lifecycle_guard(&tid);
                'result: loop {
                    let (txn_lock, created) = self.get_or_create_transaction_with_status(&tid);
                    let mut txn = txn_lock.lock();
                    if !self
                        .txns
                        .get(&tid)
                        .is_some_and(|current| Arc::ptr_eq(&current, &txn_lock))
                    {
                        drop(txn);
                        match self
                            .participant_completion_evidence(&tid, self.participant_time(&tid))
                        {
                            Ok(Some(state @ (TxnState::Committed | TxnState::Aborted))) => {
                                break 'result DMPrepareResult::StateError(state);
                            }
                            Ok(_) => continue 'result,
                            Err(error) => {
                                error!(
                                    "Failed to check participant completion before prepare {:?}: {:?}",
                                    tid, error
                                );
                                break 'result DMPrepareResult::NotRealizable;
                            }
                        }
                    }
                    match self.participant_completion_evidence(
                        &tid,
                        self.participant_time(&tid),
                    ) {
                        Ok(Some(state @ (TxnState::Committed | TxnState::Aborted))) => {
                            if created {
                                self.wipe_out_transaction(&tid, &txn_lock);
                            }
                            break 'result DMPrepareResult::StateError(state);
                        }
                        Ok(Some(_)) => break 'result DMPrepareResult::NotRealizable,
                        Ok(None) => {}
                        Err(error) => {
                            error!(
                                "Failed to check participant completion before prepare {:?}: {:?}",
                                tid, error
                            );
                            break 'result DMPrepareResult::NotRealizable;
                        }
                    }
                    match txn.state {
                        TxnState::Started => {}
                        TxnState::Prepared => {
                            if txn.coordinator_id != Some(coordinator_id)
                                || txn.certified != prepared_ops_by_id
                            {
                                break 'result DMPrepareResult::StateError(TxnState::Prepared);
                            }
                        }
                        _ => break 'result DMPrepareResult::StateError(txn.state),
                    }

                    'acquire: loop {
                        let mut cell_mutexes = prefetched_cell_mutexes.take().unwrap_or_else(|| {
                            Vec::with_capacity(prepared_ops.len())
                        });
                        let mut cell_guards = Vec::with_capacity(prepared_ops.len());

                        if cell_mutexes.is_empty() && !prepared_ops.is_empty() {
                            for op in &prepared_ops {
                                cell_mutexes.push(self.cell_meta_mutex(&op.id));
                            }
                        }
                        for cell_mutex in &cell_mutexes {
                            cell_guards.push(cell_mutex.lock());
                        }
                        if prepared_ops
                            .iter()
                            .zip(&cell_mutexes)
                            .any(|(op, meta)| !self.cell_meta_is_current(&op.id, meta))
                        {
                            drop(cell_guards);
                            continue 'acquire;
                        }

                        for meta in &cell_guards {
                            if let Some(owner) = meta.owner.clone() {
                                if owner != requester {
                                    let lock_age = meta
                                        .lock_acquired_at
                                        .map(|acquired| get_time() - acquired)
                                        .unwrap_or(0);
                                    if lock_age > self.lock_timeout_ms {
                                        self.queue_stale_owner_resolution(owner.clone());
                                    }

                                    if requester.compare_age(&owner).is_gt() {
                                        debug!(
                                            "PREPARE Wait-Die: younger txn {:?} aborted, cell owned by older {:?} (lock age: {}ms)",
                                            requester, owner, lock_age
                                        );
                                        break 'result DMPrepareResult::NotRealizable;
                                    } else {
                                        debug!(
                                            "PREPARE Wait-Die: older txn {:?} waits for younger owner {:?} (lock age: {}ms)",
                                            requester, owner, lock_age
                                        );
                                        break 'result DMPrepareResult::Wait;
                                    }
                                }
                            }
                        }

                        let lock_time = get_time();
                        let mut owner_index = self.owner_index.lock();
                        let mut newly_acquired = 0;
                        for meta in &mut cell_guards {
                            if meta.owner.is_none() {
                                meta.owner = Some(requester.clone());
                                meta.lock_acquired_at = Some(lock_time);
                                newly_acquired += 1;
                            }
                        }
                        owner_index.add(&requester, newly_acquired);

                        for op in &prepared_ops {
                            if !self.prepare_expectation_matches(op) {
                                debug!(
                                    "PREPARE expectation mismatch for {:?} on cell {:?}: {:?}",
                                    requester, op.id, op
                                );
                                let mut released = 0;
                                for meta in &mut cell_guards {
                                    if meta.owner.as_ref() == Some(&requester) {
                                        meta.owner = None;
                                        meta.lock_acquired_at = None;
                                        released += 1;
                                    }
                                }
                                owner_index.remove(&requester, released);
                                break 'result DMPrepareResult::NotRealizable;
                            }
                        }

                        txn.certified = prepared_ops_by_id;
                        txn.affected_cells = txn.certified.keys().copied().collect();
                        txn.coordinator_id = Some(coordinator_id);
                        txn.state = TxnState::Prepared;
                        txn.last_activity = get_time();
                        debug!("SITE PREPARE SUCCESSFUL FOR {:?}", requester);
                        break 'result DMPrepareResult::Success;
                    }
                }
            };

            self.response_with(result).await
        }
        .boxed()
    }
    fn commit(
        &self,
        commit_hlc: Hlc,
        tid: TxnId,
        cells: Vec<CommitOp>,
    ) -> BoxFuture<'_, DataSiteResponse<DMCommitResult>> {
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantCommit);
        self.update_clock(commit_hlc);

        #[cfg(test)]
        if let Some(state) = Self::take_matching_commit_delay(&tid, &cells) {
            return async move {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = phase_guard;
                Self::await_prepare_delay(&state).await;
                let payload = match self.find_transaction(&tid) {
                    Some(txn_lock) => self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells),
                    None => DMCommitResult::CheckFailed(CheckError::NotExisted),
                };
                DataSiteResponse::new(self.hlc.now(), payload)
            }
            .boxed();
        }

        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(DMCommitResult::CheckFailed(CheckError::NotExisted));
        };

        if self.database_runtime.indexer().is_some() {
            let tid_for_logs = tid.clone();
            let scoped_commit = IndexBuilder::with_request_index_scope({
                let txn_lock = txn_lock.clone();
                let tid = tid.clone();
                move || self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells)
            });
            return async move {
                #[cfg(feature = "occ_phase_profile")]
                let _phase_guard = phase_guard;
                let (payload, request_results) = scoped_commit.await;
                let pending_results = IndexBuilder::await_indices().await;
                self.warn_on_index_wait_results(
                    &tid_for_logs,
                    request_results
                        .into_iter()
                        .chain(pending_results.into_iter()),
                );
                DataSiteResponse::new(self.hlc.now(), payload)
            }
            .boxed();
        }

        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard = phase_guard;
        self.response_with(self.apply_commit_ops(&txn_lock, &tid, commit_hlc, cells))
    }
    fn abort(&self, clock: Hlc, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<AbortResult>> {
        debug!(">> ABORT {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let _phase_guard =
            super::phase_profile::guard(super::phase_profile::Phase::ParticipantAbort);
        self.update_clock(clock);
        let Some(txn_lock) = self.find_transaction(&tid) else {
            return self.response_with(self.abort_result_from_completion_evidence(&tid));
        };
        let mut txn = txn_lock.lock();
        match self.txns.get(&tid) {
            Some(current) if Arc::ptr_eq(&current, &txn_lock) => {}
            None => {
                drop(txn);
                return self.response_with(self.abort_result_from_completion_evidence(&tid));
            }
            Some(_) => {
                return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
            }
        }
        if txn.durable_decision == Some(TxnState::Committed) {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyCommitted));
        }
        if txn.state == TxnState::Aborted {
            return self.response_with(AbortResult::CheckFailed(CheckError::AlreadyAborted));
        }
        #[cfg(test)]
        if Self::take_matching_abort_cannot_end(&tid, &txn.affected_cells) {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        if txn.history.is_empty() {
            txn.last_activity = get_time();
            txn.compensation_output_durable = true;
            txn.state = TxnState::Aborted;
            drop(txn);
            return self.response_with(AbortResult::Success(None));
        }

        let Some(coordinator_id) = txn.coordinator_id else {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        };
        let expected_owner = TxnPriority::new(tid.clone(), coordinator_id);
        let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
        let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
        for cell_id in &guarded_cell_ids {
            cell_mutexes.push(self.cell_meta_mutex(cell_id));
        }
        let mut cell_guards = Vec::with_capacity(cell_mutexes.len());
        for cell_mutex in &cell_mutexes {
            cell_guards.push(cell_mutex.lock());
        }
        if cell_guards
            .iter()
            .any(|meta| meta.owner.as_ref() != Some(&expected_owner))
        {
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        let rollback_failures = {
            debug!(
                ">>>>>>>>>> ROLLING BACK FOR {:?} CELLS {:?}",
                txn.history.len(),
                tid
            );
            let Transaction {
                history, installed, ..
            } = &mut *txn;
            let failures = self.rollback(history, installed);
            if failures.len() == 0 {
                None
            } else {
                Some(failures)
            }
        };
        if let Some(failures) = rollback_failures {
            for failure in failures {
                error!(
                    "Compensation for cell {:?} in transaction {:?} failed: {:?}",
                    failure.id, tid, failure.error
                );
            }
            txn.last_activity = get_time();
            return self.response_with(AbortResult::CheckFailed(CheckError::CannotEnd));
        }

        txn.last_activity = get_time();
        txn.compensation_output_durable = true;
        txn.state = TxnState::Aborted;

        drop(cell_guards);
        drop(txn);

        self.response_with(AbortResult::Success(None))
    }
    fn end(&self, clock: Hlc, tid: TxnId) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        debug!(">> END {:?}", tid);
        #[cfg(feature = "occ_phase_profile")]
        let phase_guard = super::phase_profile::guard(super::phase_profile::Phase::ParticipantEnd);
        self.update_clock(clock);

        let result = 'end: {
            let Some(txn_lock) = self.find_transaction(&tid) else {
                return self.response_with(self.end_result_from_completion_evidence(&tid));
            };
            let mut txn = txn_lock.lock();
            match self.txns.get(&tid) {
                Some(current) if Arc::ptr_eq(&current, &txn_lock) => {}
                None => {
                    drop(txn);
                    return self.response_with(self.end_result_from_completion_evidence(&tid));
                }
                Some(_) => break 'end EndResult::CheckFailed(CheckError::CannotEnd),
            }
            if !(txn.state == TxnState::Aborted || txn.state == TxnState::Committed) {
                break 'end EndResult::CheckFailed(CheckError::CannotEnd);
            }

            let guarded_cell_ids = Self::guarded_txn_cell_ids(&txn);
            let expected_owner = if guarded_cell_ids.is_empty() {
                None
            } else {
                let Some(coordinator_id) = txn.coordinator_id else {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                };
                Some(TxnPriority::new(tid, coordinator_id))
            };
            let mut cell_mutexes = Vec::with_capacity(guarded_cell_ids.len());
            for cell_id in &guarded_cell_ids {
                let Some(cell_mutex) = self.cells.get(cell_id) else {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                };
                cell_mutexes.push(cell_mutex);
            }
            let mut cell_guards: Vec<_> = cell_mutexes.iter().map(|cell| cell.lock()).collect();
            if let Some(expected_owner) = expected_owner.as_ref() {
                if cell_guards
                    .iter()
                    .any(|meta| meta.owner.as_ref() != Some(expected_owner))
                {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            if txn.state == TxnState::Committed {
                if !txn.installed.is_empty() && !txn.installed_output_durable {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if !self.installed_revisions_agree(&txn) {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if txn.installed.values().any(|installed| {
                    !matches!(
                        installed.node.load().0,
                        RevisionState::PendingPresent
                            | RevisionState::PendingDeleted
                            | RevisionState::CommittedPresent
                            | RevisionState::CommittedDeleted
                    )
                }) {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }
            if txn.state == TxnState::Aborted {
                if !txn.history.is_empty() && !txn.compensation_output_durable {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
                if txn
                    .history
                    .values()
                    .any(|history| history.compensation.is_none())
                {
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            if txn.durable_decision.is_none() {
                if let Some(undo_log) = self.undo_log() {
                    let log_result = match txn.state {
                        TxnState::Committed => undo_log.write_commit_marker(&tid),
                        TxnState::Aborted => undo_log.write_abort_marker(&tid),
                        _ => Ok(()),
                    };
                    if let Err(error) = log_result {
                        error!("Failed to write transaction completion marker: {:?}", error);
                        break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                    }
                    txn.durable_decision = Some(txn.state);
                }
            }

            if txn.state == TxnState::Committed {
                let mut promoted = Vec::new();
                let mut promotion_failed = false;
                for installed in txn.installed.values() {
                    match installed.node.load().0 {
                        state @ (RevisionState::PendingPresent | RevisionState::PendingDeleted) => {
                            #[cfg(test)]
                            if should_fail_end_promotion(&tid, &installed.id) {
                                promotion_failed = true;
                                break;
                            }
                            if self.chunks().promote_revision(installed).is_err() {
                                promotion_failed = true;
                                break;
                            }
                            let committed = match state {
                                RevisionState::PendingPresent => RevisionState::CommittedPresent,
                                RevisionState::PendingDeleted => RevisionState::CommittedDeleted,
                                _ => unreachable!(),
                            };
                            promoted.push((installed.node.clone(), state, committed));
                        }
                        RevisionState::CommittedPresent | RevisionState::CommittedDeleted => {}
                        RevisionState::Aborted | RevisionState::Expired => unreachable!(
                            "installed revision states were prevalidated before promotion"
                        ),
                    }
                }
                if promotion_failed || !self.installed_revisions_agree(&txn) {
                    for (node, pending, committed) in promoted.into_iter().rev() {
                        if !node.compare_exchange_state(committed, pending) {
                            error!("Could not restore pending promotion barrier for {:?}", tid);
                        }
                    }
                    break 'end EndResult::CheckFailed(CheckError::CannotEnd);
                }
            }

            debug!(
                "AFFECTED: {}, {:?}, {:?}",
                guarded_cell_ids.len(),
                txn.state,
                tid
            );

            if let Some(expected_owner) = expected_owner.as_ref() {
                let mut owner_index = self.owner_index.lock();
                let mut released = 0;
                for meta in &mut cell_guards {
                    if meta.owner.as_ref() == Some(expected_owner) {
                        meta.owner = None;
                        meta.lock_acquired_at = None;
                        released += 1;
                    }
                }
                owner_index.remove(expected_owner, released);
            } else {
                for meta in &mut cell_guards {
                    meta.owner = None;
                    meta.lock_acquired_at = None;
                }
            }

            if self.undo_log().is_none() {
                self.record_volatile_completion(tid, txn.state);
            } else {
                self.record_durable_completion(tid, txn.state, None);
            }
            self.wipe_out_transaction(&tid, &txn_lock);
            drop(txn);
            self.cleanup_signal.store(true, Relaxed);
            debug!("ENDED: {:?} after atomic owner barrier", tid);
            EndResult::Success
        };
        async move {
            #[cfg(feature = "occ_phase_profile")]
            let _phase_guard = phase_guard;
            self.response_with(result).await
        }
        .boxed()
    }

    fn retire(
        &self,
        clock: Hlc,
        tid: TxnId,
        resolution: TxnResolution,
    ) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        self.update_clock(clock);
        self.response_with(self.retire_participant_evidence(&tid, resolution, false))
    }

    fn finalize_retirement(
        &self,
        clock: Hlc,
        tid: TxnId,
        resolution: TxnResolution,
    ) -> BoxFuture<'_, DataSiteResponse<EndResult>> {
        self.update_clock(clock);
        self.response_with(self.retire_participant_evidence(&tid, resolution, true))
    }
}
