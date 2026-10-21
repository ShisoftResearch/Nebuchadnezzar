//! Lock diagnostics. Disabled builds expand directly to the original expression.

#[cfg(not(feature = "lock_profile"))]
#[macro_export]
macro_rules! profile_lock {
    ($label:ident, $acquire:expr) => {
        $acquire
    };
}

#[cfg(feature = "lock_profile")]
#[macro_export]
macro_rules! profile_lock {
    ($label:ident, $acquire:expr) => {{
        let timer =
            $crate::utils::lock_profile::Wait::new($crate::utils::lock_profile::labels::$label);
        let guard = $acquire;
        timer.acquired(guard)
    }};
}

#[cfg(feature = "lock_profile")]
#[allow(non_upper_case_globals)]
pub mod labels {
    include!("lock_profile_labels.rs");
}

#[cfg(feature = "lock_profile")]
mod enabled {
    use super::labels;
    use std::ops::{Deref, DerefMut};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
    use std::time::{Duration, Instant};

    const SHARDS: usize = 64;

    #[derive(Default)]
    struct Counters {
        acquired: AtomicU64,
        released: AtomicU64,
        wait_ns: AtomicU64,
        hold_ns: AtomicU64,
        max_wait_ns: AtomicU64,
        max_hold_ns: AtomicU64,
    }

    // Thread stripes prevent the profiler from adding a single hot counter.
    #[repr(align(64))]
    struct Stripe([Counters; labels::NAMES.len()]);
    static COUNTERS: [Stripe; SHARDS] = [const {
        Stripe(
            [const {
                Counters {
                    acquired: AtomicU64::new(0),
                    released: AtomicU64::new(0),
                    wait_ns: AtomicU64::new(0),
                    hold_ns: AtomicU64::new(0),
                    max_wait_ns: AtomicU64::new(0),
                    max_hold_ns: AtomicU64::new(0),
                }
            }; labels::NAMES.len()],
        )
    }; SHARDS];
    static STARTED: AtomicBool = AtomicBool::new(false);

    fn counters(label: usize) -> &'static Counters {
        let shard = std::thread::current().id().as_u64().get() as usize % SHARDS;
        &COUNTERS[shard].0[label]
    }

    /// Write cumulative counters. Rows with no acquisitions remain in the output.
    /// A live snapshot is approximate; a quiescent snapshot has exact totals.
    pub fn snapshot() -> serde_json::Value {
        let rows: Vec<_> = labels::NAMES
            .iter()
            .enumerate()
            .map(|(label, name)| {
                let mut acquired = 0;
                let mut released = 0;
                let mut wait_ns = 0;
                let mut hold_ns = 0;
                let mut max_wait_ns = 0;
                let mut max_hold_ns = 0;
                for stripe in &COUNTERS {
                    let c = &stripe.0[label];
                    acquired += c.acquired.load(Relaxed);
                    released += c.released.load(Relaxed);
                    wait_ns += c.wait_ns.load(Relaxed);
                    hold_ns += c.hold_ns.load(Relaxed);
                    max_wait_ns = max_wait_ns.max(c.max_wait_ns.load(Relaxed));
                    max_hold_ns = max_hold_ns.max(c.max_hold_ns.load(Relaxed));
                }
                serde_json::json!({"id": name, "acquired": acquired, "released": released,
                "wait_ns": wait_ns, "hold_ns": hold_ns,
                "max_wait_ns": max_wait_ns, "max_hold_ns": max_hold_ns})
            })
            .collect();
        serde_json::json!({"pid": std::process::id(), "rows": rows})
    }

    fn write_snapshot(final_sample: bool) {
        let Ok(prefix) = std::env::var("NEB_LOCK_PROFILE") else {
            return;
        };
        let suffix = if final_sample { "final" } else { "live" };
        let path = format!("{prefix}.{}.{suffix}.json", std::process::id());
        let temporary = format!("{path}.new");
        if let Err(error) = std::fs::write(&temporary, snapshot().to_string())
            .and_then(|_| std::fs::rename(&temporary, &path))
        {
            eprintln!("lock profile export {path}: {error}");
        }
    }

    extern "C" fn at_exit() {
        write_snapshot(true);
    }

    fn start_export() {
        if !STARTED.load(Relaxed) && !STARTED.swap(true, Relaxed) {
            // The callback reads only atomics and writes its own output file.
            unsafe {
                libc::atexit(at_exit);
            }
            if std::env::var_os("NEB_LOCK_PROFILE").is_some() {
                std::thread::Builder::new()
                    .name("lock-profile".into())
                    .spawn(|| loop {
                        std::thread::sleep(Duration::from_secs(1));
                        write_snapshot(false);
                    })
                    .expect("start lock profile export");
            }
        }
    }

    pub struct Wait {
        label: usize,
        started: Instant,
    }
    impl Wait {
        pub fn new(label: usize) -> Self {
            start_export();
            Self {
                label,
                started: Instant::now(),
            }
        }
        pub fn acquired<G>(self, guard: G) -> Guard<G> {
            let now = Instant::now();
            let elapsed = now.duration_since(self.started).as_nanos() as u64;
            let c = counters(self.label);
            c.wait_ns.fetch_add(elapsed, Relaxed);
            c.max_wait_ns.fetch_max(elapsed, Relaxed);
            c.acquired.fetch_add(1, Relaxed);
            Guard {
                guard,
                label: self.label,
                held: now,
            }
        }
    }

    pub struct Guard<G> {
        guard: G,
        label: usize,
        held: Instant,
    }
    impl<G: Deref> Deref for Guard<G> {
        type Target = G::Target;
        fn deref(&self) -> &Self::Target {
            &self.guard
        }
    }
    impl<G: DerefMut> DerefMut for Guard<G> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.guard
        }
    }
    impl<G> Guard<G> {
        /// Exclude a condvar sleep from hold time and count reacquisition as wait.
        pub fn suspend<R>(&mut self, wait: impl FnOnce(&mut G) -> R) -> R {
            self.record_hold();
            let started = Instant::now();
            let result = wait(&mut self.guard);
            self.held = Instant::now();
            let elapsed = self.held.duration_since(started).as_nanos() as u64;
            let c = counters(self.label);
            c.wait_ns.fetch_add(elapsed, Relaxed);
            c.max_wait_ns.fetch_max(elapsed, Relaxed);
            c.acquired.fetch_add(1, Relaxed);
            result
        }
        fn record_hold(&self) {
            let elapsed = self.held.elapsed().as_nanos() as u64;
            let c = counters(self.label);
            c.hold_ns.fetch_add(elapsed, Relaxed);
            c.max_hold_ns.fetch_max(elapsed, Relaxed);
            c.released.fetch_add(1, Relaxed);
        }
    }
    impl<G> Drop for Guard<G> {
        fn drop(&mut self) {
            self.record_hold();
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn contended_guard_counts_wait_and_hold() {
            let lock = parking_lot::Mutex::new(0);
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            std::thread::scope(|scope| {
                let mut guard = crate::profile_lock!(TEST, lock.lock());
                scope.spawn(|| {
                    let timer = Wait::new(labels::TEST);
                    ready_tx.send(()).unwrap();
                    let mut guard = timer.acquired(lock.lock());
                    *guard += 1;
                });
                ready_rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(20));
                *guard += 1;
                drop(guard);
            });
            assert_eq!(*lock.lock(), 2);
            let row = &snapshot()["rows"][labels::TEST];
            assert_eq!(row["acquired"], 2);
            assert_eq!(row["released"], 2);
            assert!(row["wait_ns"].as_u64().unwrap() >= 10_000_000);
            assert!(row["hold_ns"].as_u64().unwrap() >= 10_000_000);
        }
    }
}

#[cfg(feature = "lock_profile")]
pub use enabled::*;

#[cfg(all(test, not(feature = "lock_profile")))]
mod disabled_tests {
    #[test]
    fn disabled_macro_keeps_the_original_guard_type() {
        let lock = parking_lot::Mutex::new(0);
        // This label does not exist. A disabled build must not resolve it.
        let mut guard: parking_lot::MutexGuard<'_, i32> =
            crate::profile_lock!(NOT_A_PROFILE_LABEL, lock.lock());
        *guard += 1;
        drop(guard);
        assert_eq!(*lock.lock(), 1);
    }
}
