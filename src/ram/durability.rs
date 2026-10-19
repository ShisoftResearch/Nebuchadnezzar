//! Per-file durable receipts. Sync workers own completion after caller cancellation.
use crate::ram::cell::WriteError;
use futures::channel::oneshot;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::io;
use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DurabilityError {
    Sync {
        chunk: usize,
        segment: u64,
        sequence: u64,
        message: String,
    },
    WalMissing {
        chunk: usize,
        segment: u64,
        sequence: u64,
    },
    Rewound {
        chunk: usize,
        segment: u64,
        sequence: u64,
        expected_epoch: u64,
        actual_epoch: u64,
    },
    Rpc {
        message: String,
    },
    WorkerStopped,
}

pub static ACKNOWLEDGED_WRITES: AtomicU64 = AtomicU64::new(0);
pub static NAMESPACE_SYNCS: AtomicU64 = AtomicU64::new(0);
pub static FILE_SYNCS: AtomicU64 = AtomicU64::new(0);
pub static FAILED_SEGMENTS: AtomicU64 = AtomicU64::new(0);
pub static SYNC_FAILURES: AtomicU64 = AtomicU64::new(0);
pub static GROUP_SYNCS: AtomicU64 = AtomicU64::new(0);

static POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .thread_name(|n| format!("durable-sync-{n}"))
        .build()
        .expect("create durable sync pool")
});

// Durations are sums across calls/files, not a partition of concurrent wall time.
pub static APPEND_NS: AtomicU64 = AtomicU64::new(0);
pub static QUEUE_NS: AtomicU64 = AtomicU64::new(0);
pub static SYNC_NS: AtomicU64 = AtomicU64::new(0);
pub static BARRIER_NS: AtomicU64 = AtomicU64::new(0);
pub static BARRIER_SYNC_NS: AtomicU64 = AtomicU64::new(0);
pub static RELEASE_NS: AtomicU64 = AtomicU64::new(0);
pub static GROUP_BYTES: AtomicU64 = AtomicU64::new(0);
pub static WAIT_CALLS: AtomicU64 = AtomicU64::new(0);
pub static COMPLETED_WAITS: AtomicU64 = AtomicU64::new(0);
pub static BATCH_RECEIPTS_TAKEN: AtomicU64 = AtomicU64::new(0);

fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}
struct Completion {
    result: Result<(), DurabilityError>,
    started: Instant,
    finished: Instant,
}
struct Waiter {
    offset: u64,
    sender: oneshot::Sender<Completion>,
}
struct Progress {
    file: Option<std::sync::Weak<File>>,
    durable: u64,
    epoch: u64,
    directory: Option<std::path::PathBuf>,
    namespaced: bool,
    failed: Option<DurabilityError>,
    running: bool,
    requested: Option<(u64, u64)>,
    released: u64,
    waiters: Vec<Waiter>,
    #[cfg(test)]
    fail_sync: bool,
    #[cfg(test)]
    gate: Option<Arc<std::sync::Barrier>>,
    #[cfg(test)]
    capture_gate: Option<Arc<std::sync::Barrier>>,
}

pub struct DurableFile {
    chunk: usize,
    segment: u64,
    sequence: u64,
    // Highest complete prefix observed by a durable call or a sync.
    end: AtomicU64,
    epoch: AtomicU64,
    failed: AtomicBool,
    progress: Mutex<Progress>,
}

#[derive(Clone)]
pub struct Receipt {
    pub file: Arc<DurableFile>,
    pub offset: u64,
    pub epoch: u64,
    pub(crate) descriptor: Option<Arc<File>>,
}

impl Receipt {
    /// The same coverage test without the WAL descriptor.
    ///
    /// A holder that waits for a sync but never syncs by itself must not keep
    /// the file open: the segment may archive and unlink its WAL meanwhile.
    pub(crate) fn token(&self) -> Receipt {
        Receipt {
            file: self.file.clone(),
            offset: self.offset,
            epoch: self.epoch,
            descriptor: None,
        }
    }
}
struct Touched {
    file: Arc<DurableFile>,
    // Pin once on first touch. Rotation may unlink/drop the segment before the
    // batch ends. The receipt offset itself is read only after all appends.
    descriptor: Option<Arc<File>>,
}

impl DurableFile {
    pub fn new(chunk: usize, segment: u64, sequence: u64) -> Arc<Self> {
        Arc::new(Self {
            chunk,
            segment,
            sequence,
            end: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            progress: Mutex::new(Progress {
                file: None,
                durable: 0,
                epoch: 0,
                directory: None,
                namespaced: false,
                failed: None,
                running: false,
                requested: None,
                released: 0,
                waiters: Vec::new(),
                #[cfg(test)]
                fail_sync: false,
                #[cfg(test)]
                gate: None,
                #[cfg(test)]
                capture_gate: None,
            }),
        })
    }

    // Called under the append lock, before the first frame.
    pub fn open(&self, file: &Arc<File>, header_end: u64, directory: &str) -> io::Result<()> {
        let mut p = self.progress.lock();
        if p.file.is_none() {
            p.file = Some(Arc::downgrade(file));
            p.directory = Some(std::path::PathBuf::from(directory));
            self.end.store(header_end, Release);
            p.durable = (crate::ram::wal_format::FILE_HEADER_SIZE as u64).min(header_end);
            p.released = p.durable;
        }
        Ok(())
    }

    pub fn appended(self: &Arc<Self>, end: u64, descriptor: Option<&Arc<File>>) {
        let tracking = capturing();
        #[cfg(test)]
        let tracking = tracking || std::env::var_os("NEB_TEST_TRACK_DEFAULT_RECEIPTS").is_some();
        if !tracking {
            return;
        }
        self.end.store(end, Release);
        if !capturing() {
            return;
        }
        if BATCH_FILES
            .try_with(|batch| {
                let mut batch = batch.borrow_mut();
                if !batch.iter().any(|r| Arc::ptr_eq(&r.file, self)) {
                    batch.push(Touched {
                        file: self.clone(),
                        descriptor: descriptor.cloned(),
                    });
                }
            })
            .is_ok()
        {
            return;
        }
        RECEIPTS.with(|scope| {
            if let Some(receipts) = scope.borrow_mut().as_mut() {
                if let Some(receipt) = receipts.iter_mut().find(|r| Arc::ptr_eq(&r.file, self)) {
                    receipt.offset = end;
                    receipt.epoch = self.epoch.load(Acquire);
                } else {
                    receipts.push(Receipt {
                        file: self.clone(),
                        offset: end,
                        epoch: self.epoch.load(Acquire),
                        descriptor: descriptor.cloned(),
                    });
                }
            }
        });
    }

    pub fn observe(&self, end: u64) {
        self.end.store(end, Release);
    }

    pub fn failure(&self) -> Option<DurabilityError> {
        if self.failed.load(Acquire) {
            self.progress.lock().failed.clone()
        } else {
            None
        }
    }

    fn rewound(&self, expected_epoch: u64, actual_epoch: u64) -> DurabilityError {
        DurabilityError::Rewound {
            chunk: self.chunk,
            segment: self.segment,
            sequence: self.sequence,
            expected_epoch,
            actual_epoch,
        }
    }

    pub fn truncate(&self, end: u64) {
        let mut p = self.progress.lock();
        p.epoch += 1;
        self.epoch.store(p.epoch, Release);
        #[cfg(test)]
        let refuse_stale = std::env::var_os("NEB_TEST_DISABLE_TRUNCATE_FAILURE").is_none();
        #[cfg(not(test))]
        let refuse_stale = true;
        if refuse_stale {
            let error = self.rewound(p.epoch - 1, p.epoch);
            for waiter in p.waiters.drain(..) {
                let now = Instant::now();
                let _ = waiter.sender.send(Completion {
                    result: Err(error.clone()),
                    started: now,
                    finished: now,
                });
            }
        }
        self.end.store(end, Release);
        p.durable = p.durable.min(end);
        p.released = p.released.min(end);
    }
    pub fn snapshot(&self) -> (u64, u64) {
        let p = self.progress.lock();
        (self.end(), p.epoch)
    }
    pub fn covers_receipt(&self, offset: u64, epoch: u64) -> bool {
        let p = self.progress.lock();
        p.epoch == epoch && p.failed.is_none() && p.namespaced && p.durable >= offset
    }

    pub fn covers(&self, offset: u64) -> bool {
        let p = self.progress.lock();
        p.failed.is_none() && p.namespaced && p.durable >= offset
    }

    pub fn watermark(&self) -> u64 {
        self.progress.lock().durable
    }
    pub fn end(&self) -> u64 {
        self.end.load(Acquire)
    }

    pub fn complete(&self, end: u64, epoch: u64, result: &io::Result<()>) {
        self.publish(end, epoch, result, None);
    }
    fn publish(
        &self,
        end: u64,
        epoch: u64,
        result: &io::Result<()>,
        timing: Option<(Instant, Instant)>,
    ) {
        #[cfg(test)]
        if result.is_err() && std::env::var_os("NEB_TEST_DISABLE_FAILURE_MARK").is_some() {
            return;
        }
        FILE_SYNCS.fetch_add(1, Relaxed);
        let mut p = self.progress.lock();
        if let Err(error) = result {
            SYNC_FAILURES.fetch_add(1, Relaxed);
            if p.failed.is_none() {
                FAILED_SEGMENTS.fetch_add(1, Relaxed);
                self.failed.store(true, Release);
                error!(
                    "WAL sync failed: chunk={} segment={} sequence={}: {error}",
                    self.chunk, self.segment, self.sequence
                );
            }
            p.failed.get_or_insert_with(|| DurabilityError::Sync {
                chunk: self.chunk,
                segment: self.segment,
                sequence: self.sequence,
                message: error.to_string(),
            });
        } else if p.failed.is_none() && p.epoch == epoch {
            self.end.fetch_max(end, Release);
            p.durable = p.durable.max(end);
            p.released = p.released.max(end);
        }
        let failed = p.failed.clone();
        let durable = if p.namespaced {
            p.durable.min(p.released)
        } else {
            0
        };
        let (started, finished) = timing.unwrap_or_else(|| {
            let now = Instant::now();
            (now, now)
        });
        let mut pending = Vec::new();
        for waiter in p.waiters.drain(..) {
            if failed.is_some() || waiter.offset <= durable {
                let result = failed.clone().map_or(Ok(()), Err);
                let _ = waiter.sender.send(Completion {
                    result,
                    started,
                    finished,
                });
            } else if !waiter.sender.is_canceled() {
                pending.push(waiter);
            }
        }
        p.waiters = pending;
    }

    /// Queue a decided prefix even when no acknowledgement waiter remains.
    pub(crate) fn request_sync(self: &Arc<Self>, offset: u64, epoch: u64) {
        let mut progress = self.progress.lock();
        if progress.failed.is_some()
            || progress.epoch != epoch
            || (progress.namespaced && progress.durable >= offset)
        {
            return;
        }
        let Some(descriptor) = progress.file.as_ref().and_then(std::sync::Weak::upgrade) else {
            return;
        };
        let requested = progress
            .requested
            .map_or(offset, |(old, _)| old.max(offset));
        progress.requested = Some((requested, epoch));
        if !progress.running {
            progress.running = true;
            let file = self.clone();
            POOL.spawn(move || file.run(descriptor));
        }
    }

    pub async fn wait(
        self: &Arc<Self>,
        offset: u64,
        epoch: u64,
    ) -> Result<std::time::Duration, DurabilityError> {
        WAIT_CALLS.fetch_add(1, Relaxed);
        let queued = Instant::now();
        let receiver = {
            let mut p = self.progress.lock();
            if let Some(error) = &p.failed {
                return Err(error.clone());
            }
            #[cfg(test)]
            let refuse_stale = std::env::var_os("NEB_TEST_DISABLE_TRUNCATE_FAILURE").is_none();
            #[cfg(not(test))]
            let refuse_stale = true;
            if refuse_stale && epoch != p.epoch {
                return Err(self.rewound(epoch, p.epoch));
            }
            let descriptor = p.file.as_ref().and_then(std::sync::Weak::upgrade);
            if descriptor.is_none() {
                return Err(DurabilityError::WalMissing {
                    chunk: self.chunk,
                    segment: self.segment,
                    sequence: self.sequence,
                });
            }
            if offset <= p.durable.min(p.released) && p.namespaced {
                return Ok(std::time::Duration::ZERO);
            }
            let (sender, receiver) = oneshot::channel();
            p.waiters.push(Waiter { offset, sender });
            if !p.running {
                p.running = true;
                let file = self.clone();
                POOL.spawn(move || file.run(descriptor.unwrap()));
            }
            receiver
        };
        let completion = receiver.await.map_err(|_| DurabilityError::WorkerStopped)?;
        QUEUE_NS.fetch_add(
            nanos(completion.started.saturating_duration_since(queued)),
            Relaxed,
        );
        RELEASE_NS.fetch_add(nanos(completion.finished.elapsed()), Relaxed);
        COMPLETED_WAITS.fetch_add(1, Relaxed);
        completion.result.map(|()| {
            completion
                .finished
                .saturating_duration_since(completion.started.max(queued))
        })
    }

    fn run(self: Arc<Self>, file: Arc<File>) {
        loop {
            #[cfg(test)]
            {
                let gate = self.progress.lock().capture_gate.take();
                if let Some(gate) = gate {
                    gate.wait();
                }
            }
            let (end, epoch, bytes) = {
                let mut p = self.progress.lock();
                p.waiters.retain(|w| !w.sender.is_canceled());
                if p.requested.is_some_and(|(offset, epoch)| {
                    epoch != p.epoch || (p.namespaced && p.durable >= offset)
                }) {
                    p.requested = None;
                }
                if p.failed.is_some() || (p.waiters.is_empty() && p.requested.is_none()) {
                    p.running = false;
                    return;
                }
                // All pending prefixes collapse into this highest appended end.
                // Writes during this sync accumulate for the next iteration.
                let end = self.end();
                #[cfg(test)]
                let end = if std::env::var_os("NEB_TEST_SPLIT_PREFIXES").is_some() {
                    p.waiters
                        .iter()
                        .map(|w| w.offset)
                        .chain(p.requested.map(|(offset, _)| offset))
                        .min()
                        .unwrap()
                } else {
                    end
                };
                (end, p.epoch, end.saturating_sub(p.released))
            };
            #[cfg(test)]
            {
                let gate = self.progress.lock().gate.take();
                if let Some(gate) = gate {
                    gate.wait();
                }
            }
            #[cfg(test)]
            loop {
                let directory = self.progress.lock().directory.clone();
                let paused = directory.is_some_and(|directory| {
                    PAUSED_RECEIPT_SYNCS
                        .lock()
                        .iter()
                        .any(|prefix| directory.starts_with(prefix))
                });
                if !paused {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let started = Instant::now();
            GROUP_SYNCS.fetch_add(1, Relaxed);
            GROUP_BYTES.fetch_add(bytes, Relaxed);
            #[cfg(test)]
            let result = if self.progress.lock().fail_sync {
                Err(io::Error::from_raw_os_error(libc::EIO))
            } else {
                file.sync_data()
            };
            #[cfg(not(test))]
            let result = file.sync_data();
            let result = result.and_then(|()| self.publish_namespace());
            #[cfg(test)]
            if result.is_ok() {
                let directory = self.progress.lock().directory.clone();
                if let Some(directory) = directory {
                    crate::ram::segs::power_loss_model::record_synced_len(
                        &Some(
                            directory
                                .join(format!(
                                    "{}-{}-{}.nlog",
                                    self.chunk, self.segment, self.sequence
                                ))
                                .to_string_lossy()
                                .to_string(),
                        ),
                        end,
                    );
                }
            }
            let finished = Instant::now();
            SYNC_NS.fetch_add(nanos(finished.duration_since(started)), Relaxed);
            self.publish(end, epoch, &result, Some((started, finished)));
        }
    }

    pub(crate) fn publish_namespace(&self) -> io::Result<()> {
        let directory = {
            let p = self.progress.lock();
            if p.namespaced {
                return Ok(());
            }
            p.directory.clone().expect("an open WAL has a directory")
        };
        for path in directory.ancestors() {
            if path.as_os_str().is_empty() {
                continue;
            }
            File::open(path)?.sync_all()?;
            NAMESPACE_SYNCS.fetch_add(1, Relaxed);
        }
        self.progress.lock().namespaced = true;
        Ok(())
    }

    #[cfg(test)]
    pub fn test_fail_sync(&self) -> bool {
        self.progress.lock().fail_sync
    }
    #[cfg(test)]
    pub fn test_gate_pending(&self) -> bool {
        self.progress.lock().gate.is_some()
    }
    #[cfg(test)]
    pub fn inject_failure(&self) {
        self.progress.lock().fail_sync = true;
    }
    #[cfg(test)]
    pub fn set_gate(&self, gate: Arc<std::sync::Barrier>) {
        self.progress.lock().gate = Some(gate);
    }
    #[cfg(test)]
    pub fn set_capture_gate(&self, gate: Arc<std::sync::Barrier>) {
        self.progress.lock().capture_gate = Some(gate);
    }
    #[cfg(test)]
    pub fn pending_waiters(&self) -> usize {
        self.progress.lock().waiters.len()
    }
}

thread_local! {
    static RECEIPTS: RefCell<Option<Vec<Receipt>>> = const { RefCell::new(None) };
    static CAPTURING: Cell<bool> = const { Cell::new(false) };
}
tokio::task_local! {
    static BATCH_FILES: RefCell<Vec<Touched>>;
    static BATCH_HAS_DERIVED: Cell<bool>;
}
pub fn batch_active() -> bool {
    BATCH_FILES.try_with(|_| ()).is_ok()
}

/// Batch captures already retain their file once. Per-cell certification defers.
pub fn defer_to_batch(receipts: &mut Vec<Receipt>) -> bool {
    if batch_active() {
        debug_assert!(receipts.is_empty());
        true
    } else {
        false
    }
}

pub fn note_derived_writes() {
    BATCH_HAS_DERIVED.with(|derived| derived.set(true));
}

pub async fn wait_uncovered(receipts: Vec<Receipt>) -> Result<(), WriteError> {
    wait(
        receipts
            .into_iter()
            .filter(|r| !r.file.covers_receipt(r.offset, r.epoch))
            .collect(),
    )
    .await
}

#[cfg(test)]
pub async fn batch<T>(
    op: impl std::future::Future<Output = Vec<Result<T, WriteError>>>,
) -> Vec<Result<T, WriteError>> {
    batch_with_derived(op, futures::future::ready(Ok(()))).await
}

pub async fn batch_with_derived<T>(
    op: impl std::future::Future<Output = Vec<Result<T, WriteError>>>,
    certify_derived: impl std::future::Future<Output = Result<(), WriteError>>,
) -> Vec<Result<T, WriteError>> {
    let (mut results, receipts, derived) = BATCH_HAS_DERIVED
        .scope(
            Cell::new(false),
            BATCH_FILES.scope(RefCell::new(Vec::new()), async move {
                let start = Instant::now();
                let results = op.await;
                APPEND_NS.fetch_add(nanos(start.elapsed()), Relaxed);
                let files = BATCH_FILES.with(|batch| batch.take());
                let receipts = files
                    .into_iter()
                    .map(|touched| {
                        BATCH_RECEIPTS_TAKEN.fetch_add(1, Relaxed);
                        let (offset, epoch) = touched.file.snapshot();
                        Receipt {
                            offset,
                            epoch,
                            file: touched.file,
                            descriptor: touched.descriptor,
                        }
                    })
                    .collect();
                (results, receipts, BATCH_HAS_DERIVED.with(Cell::get))
            }),
        )
        .await;
    #[cfg(test)]
    let bypass = std::env::var_os("NEB_TEST_DISABLE_BATCH_WAIT").is_some();
    #[cfg(not(test))]
    let bypass = false;
    let durability = if bypass {
        Ok(())
    } else if derived {
        match certify_derived.await {
            Ok(()) => wait_uncovered(receipts).await,
            Err(error) => Err(error),
        }
    } else {
        wait(receipts).await
    };
    if let Err(error) = durability {
        for result in &mut results {
            if result.is_ok() {
                *result = Err(error.clone());
            }
        }
    }
    ACKNOWLEDGED_WRITES.fetch_add(results.iter().filter(|r| r.is_ok()).count() as u64, Relaxed);
    results
}

/// Capture synchronous appends. No thread-local state crosses an await.
pub fn capture<T>(op: impl FnOnce() -> T) -> (T, Vec<Receipt>) {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CAPTURING.with(|c| c.set(self.0));
        }
    }
    let _reset = Reset(CAPTURING.with(|c| c.replace(true)));
    if batch_active() {
        return (op(), Vec::new());
    }
    struct Scope(Option<Vec<Receipt>>);
    impl Drop for Scope {
        fn drop(&mut self) {
            RECEIPTS.with(|r| *r.borrow_mut() = self.0.take());
        }
    }
    let _scope = Scope(RECEIPTS.with(|r| r.replace(Some(Vec::new()))));
    let start = Instant::now();
    let result = op();
    APPEND_NS.fetch_add(nanos(start.elapsed()), Relaxed);
    let receipts = RECEIPTS.with(|r| r.borrow_mut().take().unwrap());
    (result, receipts)
}

pub async fn wait(receipts: Vec<Receipt>) -> Result<(), WriteError> {
    #[cfg(test)]
    if std::env::var_os("NEB_TEST_DISABLE_DURABLE_WAIT").is_some() {
        return Ok(());
    }
    let start = Instant::now();
    let results = futures::future::join_all(receipts.into_iter().map(|r| async move {
        let result = r.file.wait(r.offset, r.epoch).await;
        drop(r.descriptor);
        result
    }))
    .await;
    BARRIER_NS.fetch_add(nanos(start.elapsed()), Relaxed);
    let mut critical_service = std::time::Duration::ZERO;
    for result in results {
        critical_service = critical_service.max(result.map_err(WriteError::Durability)?);
    }
    BARRIER_SYNC_NS.fetch_add(nanos(critical_service), Relaxed);
    Ok(())
}
pub fn capturing() -> bool {
    CAPTURING.with(Cell::get)
}

impl std::fmt::Display for DurabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DurabilityError {}

/// Test hook: hold the receipt syncs of every WAL file under a directory
/// prefix, as a site whose group sync has not run yet.
#[cfg(test)]
static PAUSED_RECEIPT_SYNCS: parking_lot::Mutex<Vec<std::path::PathBuf>> =
    parking_lot::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn pause_receipt_syncs_under(prefix: &std::path::Path, paused: bool) {
    let mut held = PAUSED_RECEIPT_SYNCS.lock();
    held.retain(|existing| existing != prefix);
    if paused {
        held.push(prefix.to_path_buf());
    }
}
